use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast::Sender;
use tokio::sync::mpsc::Receiver;
use tokio_util::sync::CancellationToken;
use ts_rs::TS;

use crate::channel::{ChannelDirection, ChannelMessage};
use crate::errors::AppError;
use crate::hdl::transport::Transport;
use crate::hdl::{InboundRequest, OutboundPayload, OutboundRequest, State};
use crate::utils::RemoteDeviceInfo;

const INNER_NAME: &str = "TcpServer";

#[derive(Debug, Deserialize, Serialize, TS)]
#[ts(export)]
pub struct SendInfo {
    pub id: String,
    pub name: String,
    pub addr: String,
    pub ob: OutboundPayload,
}

pub struct TcpServer {
    endpoint_id: [u8; 4],
    tcp_listener: TcpListener,
    sender: Sender<ChannelMessage>,
    connect_receiver: Receiver<SendInfo>,
}

impl TcpServer {
    pub fn new(
        endpoint_id: [u8; 4],
        tcp_listener: TcpListener,
        sender: Sender<ChannelMessage>,
        connect_receiver: Receiver<SendInfo>,
    ) -> Result<Self, anyhow::Error> {
        Ok(Self {
            endpoint_id,
            tcp_listener,
            sender,
            connect_receiver,
        })
    }

    pub async fn run(&mut self, ctk: CancellationToken) -> Result<(), anyhow::Error> {
        info!("{INNER_NAME}: service starting");

        loop {
            let cctk = ctk.clone();

            tokio::select! {
                _ = ctk.cancelled() => {
                    info!("{INNER_NAME}: tracker cancelled, breaking");
                    break;
                }
                Some(i) = self.connect_receiver.recv() => {
                    info!("{INNER_NAME}: connect_receiver: got {:?}", i);
                    if let Err(e) = self.connect(cctk, i).await {
                        error!("{INNER_NAME}: error sending: {}", e.to_string());
                    }
                }
                r = self.tcp_listener.accept() => {
                    match r {
                        Ok((socket, remote_addr)) => {
                            trace!("{INNER_NAME}: new client: {remote_addr}");
                            tokio::spawn(run_inbound(socket, remote_addr.to_string(), self.sender.clone()));
                        },
                        Err(err) => {
                            error!("{INNER_NAME}: error accepting: {}", err);
                            break;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// To be called inside a separate task if we want to handle concurrency
    pub async fn connect(&self, ctk: CancellationToken, si: SendInfo) -> Result<(), anyhow::Error> {
        debug!("{INNER_NAME}: Connecting to: {}", si.addr);
        let socket = TcpStream::connect(si.addr.clone()).await?;
        run_outbound(self.endpoint_id, socket, si, self.sender.clone(), ctk).await
    }
}

/// Drives an incoming session on an accepted transport until it ends.
pub(crate) async fn run_inbound(
    socket: impl Transport + 'static,
    id: String,
    sender: Sender<ChannelMessage>,
) {
    let mut ir = InboundRequest::new(socket, id.clone(), sender.clone());

    loop {
        match ir.handle().await {
            Ok(_) => {}
            Err(e) => match e.downcast_ref() {
                Some(AppError::NotAnError) => break,
                None => {
                    if ir.state.state == State::Initial {
                        break;
                    }

                    if ir.state.state != State::Finished {
                        let _ = sender.send(ChannelMessage {
                            id,
                            direction: ChannelDirection::LibToFront,
                            state: Some(State::Disconnected),
                            ..Default::default()
                        });
                    }
                    error!(
                        "{INNER_NAME}: error while handling client: {e} ({:?})",
                        ir.state.state
                    );
                    break;
                }
            },
        }
    }
}

/// Drives an outgoing session on a connected transport until it ends or `ctk` is cancelled.
pub(crate) async fn run_outbound(
    endpoint_id: [u8; 4],
    socket: impl Transport + 'static,
    si: SendInfo,
    sender: Sender<ChannelMessage>,
    ctk: CancellationToken,
) -> Result<(), anyhow::Error> {
    let mut or = OutboundRequest::new(
        endpoint_id,
        socket,
        si.id,
        sender.clone(),
        si.ob,
        RemoteDeviceInfo {
            device_type: crate::DeviceType::Unknown,
            name: si.name,
        },
    );

    // Send connection request
    or.send_connection_request().await?;
    // Send UKEY init
    or.send_ukey2_client_init().await?;

    loop {
        tokio::select! {
            _ = ctk.cancelled() => {
                info!("{INNER_NAME}: tracker cancelled, breaking");
                break;
            },
            r = or.handle() => {
                if let Err(e) = r {
                    match e.downcast_ref() {
                        Some(AppError::NotAnError) => break,
                        None => {
                            if or.state.state == State::Initial {
                                break;
                            }

                            if or.state.state != State::Finished && or.state.state != State::Cancelled {
                                let _ = sender.send(ChannelMessage {
                                    id: si.addr,
                                    direction: ChannelDirection::LibToFront,
                                    state: Some(State::Disconnected),
                                    ..Default::default()
                                });
                            }
                            error!("{INNER_NAME}: error while handling client: {e} ({:?})", or.state.state);
                            break;
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::broadcast;

    use super::*;
    use crate::channel::ChannelAction;

    #[tokio::test]
    async fn session_runs_over_in_memory_transport() {
        let (inbound_io, outbound_io) = tokio::io::duplex(64 * 1024);
        let (recv_tx, mut recv_events) = broadcast::channel(50);
        let (send_tx, _) = broadcast::channel(50);

        tokio::spawn(run_inbound(inbound_io, "duplex".into(), recv_tx.clone()));
        tokio::spawn(run_outbound(
            *b"TEST",
            outbound_io,
            SendInfo {
                id: "duplex".into(),
                name: "receiver".into(),
                addr: "duplex".into(),
                ob: OutboundPayload::Text("over a duplex pipe".into()),
            },
            send_tx,
            CancellationToken::new(),
        ));

        let payload = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let msg = recv_events.recv().await.unwrap();
                if msg.direction != ChannelDirection::LibToFront {
                    continue;
                }
                match msg.state {
                    Some(State::WaitingForUserConsent) => {
                        recv_tx
                            .send(ChannelMessage {
                                id: msg.id,
                                direction: ChannelDirection::FrontToLib,
                                action: Some(ChannelAction::AcceptTransfer),
                                ..Default::default()
                            })
                            .unwrap();
                    }
                    Some(State::Finished) => return msg.meta.unwrap().text_payload,
                    _ => {}
                }
            }
        })
        .await
        .expect("session over duplex did not finish");

        assert_eq!(payload.as_deref(), Some("over a duplex pipe"));
    }
}
