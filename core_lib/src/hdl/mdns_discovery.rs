use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};

use mdns_sd::{ServiceDaemon, ServiceEvent};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use ts_rs::TS;

use crate::utils::{is_not_self_ip, parse_mdns_endpoint_info};
use crate::DeviceType;

#[derive(Debug, Clone, Default, Deserialize, Serialize, TS)]
#[ts(export)]
pub struct EndpointInfo {
    pub fullname: String,
    pub id: String,
    pub name: Option<String>,
    pub ip: Option<String>,
    pub port: Option<String>,
    pub rtype: Option<DeviceType>,
    pub present: Option<bool>,
}

/// Prefers IPv4; IPv6 only when routable without a scope id (not link-local).
fn pick_address(addresses: &HashSet<IpAddr>) -> Option<IpAddr> {
    let usable = addresses.iter().filter(|ip| is_not_self_ip(ip));
    let mut v6 = None;
    for ip in usable {
        match ip {
            IpAddr::V4(_) => return Some(*ip),
            IpAddr::V6(v) if !v.is_loopback() && (v.segments()[0] & 0xffc0) != 0xfe80 => {
                v6.get_or_insert(*ip);
            }
            IpAddr::V6(_) => {}
        }
    }
    v6
}

const SERVICE_TYPE: &str = "_FC9F5ED42C8A._tcp.local.";

pub struct MDnsDiscovery {
    daemon: ServiceDaemon,
    sender: broadcast::Sender<EndpointInfo>,
    network_receiver: broadcast::Receiver<()>,
}

impl MDnsDiscovery {
    pub fn new(
        sender: broadcast::Sender<EndpointInfo>,
        network_receiver: broadcast::Receiver<()>,
    ) -> Result<Self, anyhow::Error> {
        let daemon = ServiceDaemon::new()?;

        Ok(Self {
            daemon,
            sender,
            network_receiver,
        })
    }

    pub async fn run(mut self, ctk: CancellationToken) -> Result<(), anyhow::Error> {
        info!("MDnsDiscovery: service starting");

        let mut receiver = self.daemon.browse(SERVICE_TYPE)?;

        // Map with fullname as key and EndpointInfo as value
        let mut cache: HashMap<String, EndpointInfo> = HashMap::new();

        loop {
            tokio::select! {
                _ = ctk.cancelled() => {
                    info!("MDnsDiscovery: tracker cancelled, breaking");
                    break;
                }
                r = self.network_receiver.recv() => {
                    if let Err(broadcast::error::RecvError::Closed) = r {
                        continue;
                    }
                    info!("MDnsDiscovery: network changed, restarting browse");
                    let _ = self.daemon.shutdown();
                    self.daemon = ServiceDaemon::new()?;
                    receiver = self.daemon.browse(SERVICE_TYPE)?;
                    for (_, ei) in cache.drain() {
                        let _ = self.sender.send(EndpointInfo {
                            id: ei.id,
                            ..Default::default()
                        });
                    }
                }
                r = receiver.recv_async() => {
                    match r {
                        Ok(event) => {
                            match event {
                                ServiceEvent::ServiceResolved(info) => {
                                    let port = info.get_port();

                                    let Some(ip) = pick_address(info.get_addresses()) else {
                                        continue;
                                    };

                                    // Decode the "n" text properties
                                    let n = match info.get_property("n") {
                                        Some(_n) => _n,
                                        None => continue,
                                    };

                                    // Parse the endpoint info
                                    let (dt, dn) = match parse_mdns_endpoint_info(n.val_str()) {
                                        Ok(r) => r,
                                        Err(_) => continue
                                    };

                                    let ip_port = SocketAddr::new(ip, port).to_string();
                                    let fullname = info.get_fullname().to_string();
                                    if TcpStream::connect(&ip_port).await.is_ok() {
                                        let ei = EndpointInfo {
                                            fullname: fullname.clone(),
                                            id: ip_port,
                                            name: Some(dn),
                                            ip: Some(ip.to_string()),
                                            port: Some(port.to_string()),
                                            rtype: Some(dt),
                                            present: Some(true),
                                        };
                                        info!("ServiceResolved: Resolved a new service: {:?}", ei);
                                        cache.insert(fullname.clone(), ei.clone());
                                        let _ = self.sender.send(ei);
                                    }
                                }
                                ServiceEvent::ServiceRemoved(_, fullname) => {
                                    trace!("ServiceRemoved: checking if should remove {}", fullname);
                                    // Only remove if it has not been seen in the last cleanup_threshold
                                    let should_remove = cache.get(&fullname).map(|ei| ei.id.clone());

                                    if let Some(id) = should_remove {
                                        info!("ServiceRemoved: Remove a previous service: {}", fullname);
                                        cache.remove(&fullname);
                                        let _ = self.sender.send(EndpointInfo {
                                            id,
                                            ..Default::default()
                                        });
                                    }
                                }
                                ServiceEvent::SearchStarted(_) | ServiceEvent::SearchStopped(_) => {}
                                _ => {}
                            }
                        },
                        Err(err) => error!("MDnsDiscovery: error: {}", err),
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(ips: &[&str]) -> HashSet<IpAddr> {
        ips.iter().map(|s| s.parse().unwrap()).collect()
    }

    #[test]
    fn prefers_ipv4() {
        let ips = set(&["2001:db8::5", "192.0.2.7"]);
        assert_eq!(pick_address(&ips), Some("192.0.2.7".parse().unwrap()));
    }

    #[test]
    fn falls_back_to_routable_ipv6() {
        let ips = set(&["fe80::1", "2001:db8::5"]);
        assert_eq!(pick_address(&ips), Some("2001:db8::5".parse().unwrap()));
        assert_eq!(pick_address(&set(&["fe80::1"])), None);
    }
}
