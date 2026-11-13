#[macro_use]
extern crate log;

use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};

use anyhow::anyhow;
use channel::ChannelMessage;
#[cfg(all(feature = "experimental", target_os = "linux"))]
use hdl::BleAdvertiser;
use hdl::MDnsDiscovery;
use once_cell::sync::Lazy;
use rand::distr::Alphanumeric;
use rand::Rng;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[cfg(feature = "experimental")]
use crate::hdl::BleListener;
use crate::hdl::MDnsServer;
use crate::manager::TcpServer;
use crate::utils::RemoteDeviceInfo;

pub mod channel;
mod errors;
mod hdl;
mod manager;
#[cfg(all(feature = "networkmanager", target_os = "linux"))]
pub mod nm;
mod utils;

pub use hdl::info::{TransferMetadata, WifiInfo, WifiSecurity};
pub use hdl::{EndpointInfo, OutboundPayload, State, TextPayloadType, Visibility};
pub use manager::SendInfo;
pub use utils::DeviceType;

pub mod sharing_nearby {
    include!(concat!(env!("OUT_DIR"), "/sharing.nearby.rs"));
}

pub mod securemessage {
    include!(concat!(env!("OUT_DIR"), "/securemessage.rs"));
}

pub mod securegcm {
    include!(concat!(env!("OUT_DIR"), "/securegcm.rs"));
}

pub mod location_nearby_connections {
    include!(concat!(env!("OUT_DIR"), "/location.nearby.connections.rs"));
}

const MAX_DEVICE_NAME_BYTES: usize = 255;

fn truncate_utf8(mut s: String, max: usize) -> String {
    if s.len() > max {
        let mut end = max;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
    }
    s
}

static CUSTOM_DOWNLOAD: Lazy<RwLock<Option<PathBuf>>> = Lazy::new(|| RwLock::new(None));

#[derive(Debug)]
pub struct RQS {
    tracker: Option<TaskTracker>,
    ctoken: Option<CancellationToken>,
    // Discovery token is different than ctoken because he is on his own
    // - can be cancelled while the ctoken is still active
    discovery_ctk: Option<CancellationToken>,

    // Used to trigger a change in the mDNS visibility (and later on, BLE)
    pub visibility_sender: Arc<Mutex<watch::Sender<Visibility>>>,
    visibility_receiver: watch::Receiver<Visibility>,

    // Only used to send the info "a nearby device is sharing"
    ble_sender: broadcast::Sender<()>,

    network_sender: broadcast::Sender<()>,

    device_name: Option<String>,
    device_type: DeviceType,

    port_number: Option<u32>,

    pub message_sender: broadcast::Sender<ChannelMessage>,
}

impl Default for RQS {
    fn default() -> Self {
        Self::new(Visibility::Visible, None, None)
    }
}

impl RQS {
    pub fn new(
        visibility: Visibility,
        port_number: Option<u32>,
        download_path: Option<PathBuf>,
    ) -> Self {
        let mut guard = CUSTOM_DOWNLOAD.write().unwrap();
        *guard = download_path;

        let (message_sender, _) = broadcast::channel(50);
        let (ble_sender, _) = broadcast::channel(5);
        let (network_sender, _) = broadcast::channel(5);

        // Define default visibility as per the args inside the new()
        let (visibility_sender, visibility_receiver) = watch::channel(Visibility::Invisible);
        let _ = visibility_sender.send(visibility);

        Self {
            tracker: None,
            ctoken: None,
            discovery_ctk: None,
            visibility_sender: Arc::new(Mutex::new(visibility_sender)),
            visibility_receiver,
            ble_sender,
            network_sender,
            device_name: None,
            device_type: DeviceType::Laptop,
            port_number,
            message_sender,
        }
    }

    pub async fn run(
        &mut self,
    ) -> Result<(mpsc::Sender<SendInfo>, broadcast::Receiver<()>), anyhow::Error> {
        let tracker = TaskTracker::new();
        let ctoken = CancellationToken::new();
        self.tracker = Some(tracker.clone());
        self.ctoken = Some(ctoken.clone());

        let endpoint_id: Vec<u8> = rand::rng()
            .sample_iter(Alphanumeric)
            .take(4)
            .map(u8::from)
            .collect();
        let local_device = RemoteDeviceInfo {
            name: match &self.device_name {
                Some(n) => n.clone(),
                None => sys_metrics::host::get_hostname()?,
            },
            device_type: self.device_type.clone(),
        };
        let tcp_listener =
            TcpListener::bind(format!("0.0.0.0:{}", self.port_number.unwrap_or(0))).await?;
        let binded_addr = tcp_listener.local_addr()?;
        info!("TcpListener on: {}", binded_addr);

        // MPSC for the TcpServer
        let send_channel = mpsc::channel(10);
        // Start TcpServer in own "task"
        let mut server = TcpServer::new(
            endpoint_id[..4].try_into()?,
            local_device.clone(),
            tcp_listener,
            self.message_sender.clone(),
            send_channel.1,
        )?;
        let ctk = ctoken.clone();
        tracker.spawn(async move { server.run(ctk).await });

        #[cfg(feature = "experimental")]
        {
            // Don't threat BleListener error as fatal, it's a nice to have.
            if let Ok(ble) = BleListener::new(self.ble_sender.clone()).await {
                let ctk = ctoken.clone();
                tracker.spawn(async move { ble.run(ctk).await });
            }
        }

        // Start MDnsServer in own "task"
        let mut mdns = MDnsServer::new(
            endpoint_id[..4].try_into()?,
            binded_addr.port(),
            &local_device,
            self.ble_sender.subscribe(),
            self.network_sender.subscribe(),
            self.visibility_sender.clone(),
            self.visibility_receiver.clone(),
        )?;
        let ctk = ctoken.clone();
        tracker.spawn(async move { mdns.run(ctk).await });

        #[cfg(all(feature = "networkmanager", target_os = "linux"))]
        {
            let ctk = ctoken.clone();
            let network_sender = self.network_sender.clone();
            tracker.spawn(async move { nm::forward_network_changes(network_sender, ctk).await });
        }

        tracker.close();

        Ok((send_channel.0, self.ble_sender.subscribe()))
    }

    pub fn discovery(
        &mut self,
        sender: broadcast::Sender<EndpointInfo>,
    ) -> Result<(), anyhow::Error> {
        let tracker = self
            .tracker
            .as_ref()
            .ok_or_else(|| anyhow!("The service wasn't first started"))?;

        let ctk = CancellationToken::new();
        self.discovery_ctk = Some(ctk.clone());

        #[cfg(all(feature = "experimental", target_os = "linux"))]
        {
            let ctk_blea = ctk.clone();
            tracker.spawn(async move {
                let blea = match BleAdvertiser::new().await {
                    Ok(b) => b,
                    Err(e) => {
                        error!("Couldn't init BleAdvertiser: {}", e);
                        return;
                    }
                };

                if let Err(e) = blea.run(ctk_blea).await {
                    error!("Couldn't start BleAdvertiser: {}", e);
                }
            });
        }

        let discovery = MDnsDiscovery::new(sender, self.network_sender.subscribe())?;
        tracker.spawn(async move { discovery.run(ctk.clone()).await });

        Ok(())
    }

    pub fn stop_discovery(&mut self) {
        if let Some(discovert_ctk) = &self.discovery_ctk {
            discovert_ctk.cancel();
            self.discovery_ctk = None;
        }
    }

    /// Name and type announced to peers (default: hostname, laptop); applies on the next `run`.
    pub fn set_device(&mut self, name: Option<String>, device_type: DeviceType) {
        self.device_name = name.map(|n| truncate_utf8(n, MAX_DEVICE_NAME_BYTES));
        self.device_type = device_type;
    }

    /// Re-announces the mDNS service on the current interfaces; call after a network change.
    pub fn network_changed(&self) {
        let _ = self.network_sender.send(());
    }

    pub fn change_visibility(&mut self, nv: Visibility) {
        self.visibility_sender
            .lock()
            .unwrap()
            .send_modify(|state| *state = nv);
    }

    pub async fn stop(&mut self) {
        self.stop_discovery();

        if let Some(ctoken) = &self.ctoken {
            ctoken.cancel();
        }

        if let Some(tracker) = &self.tracker {
            tracker.wait().await;
        }

        self.ctoken = None;
        self.tracker = None;
    }

    // Setting None here will resume the default settings
    pub fn set_download_path(&self, p: Option<PathBuf>) {
        debug!("Setting the download path to {:?}", p);
        let mut guard = CUSTOM_DOWNLOAD.write().unwrap();
        *guard = p;
    }
}
