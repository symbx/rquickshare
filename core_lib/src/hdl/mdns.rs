use std::sync::{Arc, Mutex};
use std::time::Duration;

use mdns_sd::{AddrType, ServiceDaemon, ServiceInfo};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::broadcast::Receiver;
use tokio::sync::watch;
use tokio::time::{interval_at, Instant};
use tokio_util::sync::CancellationToken;
use ts_rs::TS;

use crate::utils::{gen_mdns_endpoint_info, gen_mdns_name, RemoteDeviceInfo};

const INNER_NAME: &str = "MDnsServer";
const TICK_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, TS)]
#[ts(export)]
pub enum Visibility {
    Visible = 0,
    Invisible = 1,
    Temporarily = 2,
}

#[allow(dead_code)]
impl Visibility {
    pub fn from_raw_value(value: u64) -> Self {
        match value {
            0 => Visibility::Visible,
            1 => Visibility::Invisible,
            2 => Visibility::Temporarily,
            _ => unreachable!(),
        }
    }
}

pub struct MDnsServer {
    daemon: ServiceDaemon,
    service_info: ServiceInfo,
    registered: bool,
    ble_receiver: Receiver<()>,
    network_receiver: Receiver<()>,
    visibility_sender: Arc<Mutex<watch::Sender<Visibility>>>,
    visibility_receiver: watch::Receiver<Visibility>,
}

impl MDnsServer {
    pub fn new(
        endpoint_id: [u8; 4],
        service_port: u16,
        local_device: &RemoteDeviceInfo,
        ble_receiver: Receiver<()>,
        network_receiver: Receiver<()>,
        visibility_sender: Arc<Mutex<watch::Sender<Visibility>>>,
        visibility_receiver: watch::Receiver<Visibility>,
    ) -> Result<Self, anyhow::Error> {
        let service_info = Self::build_service(endpoint_id, service_port, local_device)?;

        Ok(Self {
            daemon: ServiceDaemon::new()?,
            service_info,
            registered: false,
            ble_receiver,
            network_receiver,
            visibility_sender,
            visibility_receiver,
        })
    }

    pub async fn run(&mut self, ctk: CancellationToken) -> Result<(), anyhow::Error> {
        info!("{INNER_NAME}: service starting");
        let mut monitor = self.daemon.monitor()?;
        let mut visibility = *self.visibility_receiver.borrow();
        let mut interval = interval_at(Instant::now() + TICK_INTERVAL, TICK_INTERVAL);

        loop {
            tokio::select! {
                _ = ctk.cancelled() => {
                    info!("{INNER_NAME}: tracker cancelled, breaking");
                    break;
                }
                r = monitor.recv_async() => {
                    match r {
                        Ok(_) => continue,
                        Err(err) => return Err(err.into()),
                    }
                },
                _ = self.visibility_receiver.changed() => {
                    visibility = *self.visibility_receiver.borrow_and_update();

                    debug!("{INNER_NAME}: visibility changed: {visibility:?}");
                    if visibility == Visibility::Visible {
                        self.register()?;
                    } else if visibility == Visibility::Invisible {
                        self.unregister().await?;
                    } else if visibility == Visibility::Temporarily {
                        self.register()?;
                        interval.reset();
                    }
                }
                _ = self.ble_receiver.recv() => {
                    if visibility == Visibility::Invisible {
                        continue;
                    }

                    debug!("{INNER_NAME}: ble_receiver: got event");
                    if visibility == Visibility::Visible || visibility == Visibility::Temporarily {
                        // Android can sometime not see the mDNS service if the service
                        // was running BEFORE Android started the Discovery phase for QuickShare.
                        // So resend a broadcast if there's a android device sending.
                        self.daemon.register_resend(self.service_info.get_fullname())?;
                    } else {
                        self.register()?;
                    }
                },
                r = self.network_receiver.recv() => {
                    if let Err(RecvError::Closed) = r {
                        continue;
                    }
                    info!("{INNER_NAME}: network changed, restarting mDNS daemon");
                    self.restart_daemon().await?;
                    monitor = self.daemon.monitor()?;
                }
                _ = interval.tick() => {
                    if visibility != Visibility::Temporarily {
                        continue;
                    }

                    self.unregister().await?;
                    let _ = self.visibility_sender.lock().unwrap().send(Visibility::Invisible);
                }
            }
        }

        self.unregister().await?;
        Ok(())
    }

    /// New daemon binds the current interfaces at once; the old one rescans only every 30s.
    async fn restart_daemon(&mut self) -> Result<(), anyhow::Error> {
        let registered = self.registered;
        self.unregister().await?;
        let _ = self.daemon.shutdown();
        self.daemon = ServiceDaemon::new()?;
        if registered {
            self.register()?;
        }
        Ok(())
    }

    fn register(&mut self) -> Result<(), anyhow::Error> {
        self.daemon.register(self.service_info.clone())?;
        self.registered = true;
        Ok(())
    }

    async fn unregister(&mut self) -> Result<(), anyhow::Error> {
        if !std::mem::take(&mut self.registered) {
            return Ok(());
        }
        let receiver = self.daemon.unregister(self.service_info.get_fullname())?;
        if let Ok(event) = receiver.recv_async().await {
            debug!("{INNER_NAME}: service unregistered: {event:?}");
        }
        Ok(())
    }

    fn build_service(
        endpoint_id: [u8; 4],
        service_port: u16,
        local_device: &RemoteDeviceInfo,
    ) -> Result<ServiceInfo, anyhow::Error> {
        let name = gen_mdns_name(endpoint_id);
        let hostname = sys_metrics::host::get_hostname()?;
        info!("Broadcasting as {:?} on host {hostname}", local_device.name);
        let endpoint_info =
            gen_mdns_endpoint_info(local_device.device_type.clone() as u8, &local_device.name);

        let properties = [("n", endpoint_info)];
        let si = ServiceInfo::new(
            "_FC9F5ED42C8A._tcp.local.",
            &name,
            &hostname,
            "",
            service_port,
            &properties[..],
        )?
        .enable_addr_auto(AddrType::V4);

        Ok(si)
    }
}
