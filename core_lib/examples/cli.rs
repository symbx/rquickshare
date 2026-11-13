use std::collections::VecDeque;
use std::io::BufRead;

use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage};
use rqs_lib::{DeviceType, EndpointInfo, OutboundPayload, SendInfo, State, Visibility, RQS};
use tokio::sync::{broadcast, mpsc};

const USAGE: &str = "\
Usage:
  cli recv [--port <port>] [--dir <download dir>] [--name <device name>] [--auto-accept] [--import-wifi]
  cli discover
  cli send <ip:port> --file <path>...
  cli send <ip:port> --text <text>
  cli send <ip:port> --url <url>
  cli send <ip:port> --wifi [<ssid> [<password>]]   (no ssid: active Wi-Fi, --features networkmanager)
  cli nm-wifi    (--features networkmanager) print active Wi-Fi credentials
  cli nm-watch   (--features networkmanager) print network status changes

--import-wifi saves received Wi-Fi networks to NetworkManager (--features networkmanager)

Logs: RUST_LOG=rqs_lib=debug (default: warn)";

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "warn,mdns_sd=off");
    }
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("recv") => recv(&args[1..]).await,
        Some("discover") => discover().await,
        Some("send") => send(&args[1..]).await,
        #[cfg(feature = "networkmanager")]
        Some("nm-wifi") => nm_wifi().await,
        #[cfg(feature = "networkmanager")]
        Some("nm-watch") => nm_watch().await,
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    }
}

fn usage_error(msg: &str) -> ! {
    eprintln!("error: {msg}\n\n{USAGE}");
    std::process::exit(2);
}

fn print_meta(msg: &ChannelMessage) {
    let Some(meta) = &msg.meta else { return };
    if let Some(src) = &meta.source {
        println!("  from: {} ({:?})", src.name, src.device_type);
    }
    if let Some(pin) = &meta.pin_code {
        println!("  pin:  {pin}");
    }
    if let Some(files) = &meta.files {
        for f in files {
            println!("  file: {f}");
        }
    }
    if let Some(desc) = &meta.text_description {
        println!("  desc: {desc}");
    }
}

async fn recv(args: &[String]) -> Result<(), anyhow::Error> {
    let mut port = None;
    let mut dir = None;
    let mut auto_accept = false;
    let mut import_wifi = false;
    let mut name = None;
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--port" => {
                let p = it
                    .next()
                    .unwrap_or_else(|| usage_error("--port needs a value"));
                port = Some(p.parse().unwrap_or_else(|_| usage_error("invalid port")));
            }
            "--dir" => {
                let d = it
                    .next()
                    .unwrap_or_else(|| usage_error("--dir needs a value"));
                let d = std::path::PathBuf::from(d);
                if !d.is_dir() {
                    usage_error(&format!("not a directory: {}", d.display()));
                }
                dir = Some(d);
            }
            "--name" => {
                name = Some(
                    it.next()
                        .unwrap_or_else(|| usage_error("--name needs a value"))
                        .clone(),
                );
            }
            "--auto-accept" => auto_accept = true,
            "--import-wifi" => import_wifi = true,
            other => usage_error(&format!("unknown argument: {other}")),
        }
    }

    let mut rqs = RQS::new(Visibility::Visible, port, dir);
    rqs.set_device(name, DeviceType::Laptop);
    rqs.run().await?;
    let mut events = rqs.message_sender.subscribe();
    let control = rqs.message_sender.clone();
    println!(
        "Visible, waiting for transfers{} (Ctrl+C to quit)",
        port.map(|p| format!(" on port {p}")).unwrap_or_default()
    );

    // Detached OS thread reads stdin so a pending read never blocks runtime shutdown
    let (line_tx, mut line_rx) = mpsc::channel::<String>(8);
    std::thread::spawn(move || {
        for line in std::io::stdin().lock().lines().map_while(Result::ok) {
            if line_tx.blocking_send(line).is_err() {
                break;
            }
        }
    });

    let mut pending: VecDeque<String> = VecDeque::new();
    let mut last_state: std::collections::HashMap<String, State> = Default::default();

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            Some(line) = line_rx.recv() => {
                let Some(id) = pending.pop_front() else { continue };
                let action = if line.trim().eq_ignore_ascii_case("y") {
                    ChannelAction::AcceptTransfer
                } else {
                    ChannelAction::RejectTransfer
                };
                println!("[{id}] {action:?}");
                let _ = control.send(ChannelMessage {
                    id,
                    direction: ChannelDirection::FrontToLib,
                    action: Some(action),
                    ..Default::default()
                });
                if let Some(next) = pending.front() {
                    println!("[{next}] Accept? [y/N]");
                }
            }
            r = events.recv() => {
                let msg = match r {
                    Ok(m) => m,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(e) => return Err(e.into()),
                };
                if msg.direction != ChannelDirection::LibToFront {
                    continue;
                }
                let Some(state) = msg.state.clone() else { continue };
                if last_state.get(&msg.id) == Some(&state) {
                    continue;
                }
                last_state.insert(msg.id.clone(), state.clone());

                match state {
                    State::WaitingForUserConsent => {
                        println!("[{}] Incoming transfer", msg.id);
                        print_meta(&msg);
                        if auto_accept {
                            let _ = control.send(ChannelMessage {
                                id: msg.id.clone(),
                                direction: ChannelDirection::FrontToLib,
                                action: Some(ChannelAction::AcceptTransfer),
                                ..Default::default()
                            });
                            println!("[{}] AcceptTransfer (auto)", msg.id);
                        } else {
                            if pending.is_empty() {
                                println!("[{}] Accept? [y/N]", msg.id);
                            }
                            pending.push_back(msg.id.clone());
                        }
                    }
                    State::Finished => {
                        println!("[{}] Finished", msg.id);
                        if let Some(meta) = &msg.meta {
                            if let Some(t) = &meta.text_type {
                                println!("  {t:?}: {}", meta.text_payload.as_deref().unwrap_or(""));
                            }
                            if let Some(w) = &meta.wifi {
                                println!(
                                    "  wifi: ssid={:?} security={:?} hidden={} password={:?}",
                                    w.ssid, w.security, w.hidden, w.password
                                );
                                if import_wifi {
                                    import(w).await;
                                }
                            }
                            if let Some(dest) = &meta.destination {
                                println!("  saved to: {dest}");
                            }
                        }
                        last_state.remove(&msg.id);
                    }
                    State::Rejected | State::Cancelled | State::Disconnected => {
                        println!("[{}] {state:?}", msg.id);
                        pending.retain(|p| p != &msg.id);
                        last_state.remove(&msg.id);
                    }
                    other => println!("[{}] {other:?}", msg.id),
                }
            }
        }
    }

    rqs.stop().await;
    Ok(())
}

async fn discover() -> Result<(), anyhow::Error> {
    let mut rqs = RQS::new(Visibility::Invisible, None, None);
    rqs.run().await?;
    let (tx, mut rx) = broadcast::channel::<EndpointInfo>(32);
    rqs.discovery(tx)?;
    println!("Discovering (Ctrl+C to quit)");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            r = rx.recv() => {
                let ei = match r {
                    Ok(e) => e,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(e) => return Err(e.into()),
                };
                if ei.present == Some(false) {
                    println!("- {} gone", ei.id);
                    continue;
                }
                println!(
                    "+ {} {:?} {}:{} (id {})",
                    ei.name.as_deref().unwrap_or("?"),
                    ei.rtype,
                    ei.ip.as_deref().unwrap_or("?"),
                    ei.port.as_deref().unwrap_or("?"),
                    ei.id,
                );
            }
        }
    }

    rqs.stop().await;
    Ok(())
}

async fn send(args: &[String]) -> Result<(), anyhow::Error> {
    let addr = args
        .first()
        .unwrap_or_else(|| usage_error("missing <ip:port>"))
        .clone();
    let ob = match args.get(1).map(String::as_str) {
        Some("--file") if args.len() > 2 => {
            let files = args[2..]
                .iter()
                .map(|f| {
                    std::fs::canonicalize(f)
                        .unwrap_or_else(|e| usage_error(&format!("{f}: {e}")))
                        .to_string_lossy()
                        .into_owned()
                })
                .collect();
            OutboundPayload::Files(files)
        }
        Some("--text") if args.len() == 3 => OutboundPayload::Text(args[2].clone()),
        Some("--url") if args.len() == 3 => OutboundPayload::Url(args[2].clone()),
        Some("--wifi") if args.len() == 2 => OutboundPayload::Wifi(active_wifi().await?),
        Some("--wifi") if args.len() <= 4 => {
            let password = args.get(3).cloned();
            OutboundPayload::Wifi(rqs_lib::WifiInfo {
                ssid: args[2].clone(),
                security: if password.is_some() {
                    rqs_lib::WifiSecurity::WpaPsk
                } else {
                    rqs_lib::WifiSecurity::Open
                },
                password,
                hidden: false,
            })
        }
        _ => usage_error("expected --file <path>..., --text <text>, --url <url> or --wifi"),
    };

    let mut rqs = RQS::new(Visibility::Invisible, None, None);
    let (send_tx, _) = rqs.run().await?;
    let mut events = rqs.message_sender.subscribe();

    send_tx
        .send(SendInfo {
            id: "cli".into(),
            name: addr.clone(),
            addr: addr.clone(),
            ob,
        })
        .await?;
    println!("Connecting to {addr}, accept on the receiving device");

    let mut last: Option<State> = None;
    let mut last_pct = 0;
    let mut pin_shown = false;
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            r = events.recv() => {
                let msg = match r {
                    Ok(m) => m,
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(e) => return Err(e.into()),
                };
                if msg.direction != ChannelDirection::LibToFront {
                    continue;
                }
                let Some(state) = msg.state.clone() else { continue };

                if !pin_shown {
                    if let Some(pin) = msg.meta.as_ref().and_then(|m| m.pin_code.as_ref()) {
                        println!("pin: {pin}");
                        pin_shown = true;
                    }
                }
                if state == State::SendingFiles {
                    if let Some(meta) = &msg.meta {
                        let pct = (meta.ack_bytes * 100).checked_div(meta.total_bytes).unwrap_or(0);
                        if pct >= last_pct + 10 {
                            last_pct = pct;
                            println!("  {pct}% ({}/{} bytes)", meta.ack_bytes, meta.total_bytes);
                        }
                    }
                }
                if last.as_ref() == Some(&state) {
                    continue;
                }
                last = Some(state.clone());

                match state {
                    State::Finished | State::Rejected | State::Cancelled | State::Disconnected => {
                        println!("{state:?}");
                        break;
                    }
                    other => println!("{other:?}"),
                }
            }
        }
    }

    rqs.stop().await;
    Ok(())
}

#[cfg(feature = "networkmanager")]
async fn import(wifi: &rqs_lib::WifiInfo) {
    let result = async {
        rqs_lib::nm::NetworkManager::system()
            .await?
            .import_wifi(wifi)
            .await
    }
    .await;
    match result {
        Ok(path) => println!("  imported to NetworkManager: {}", path.as_str()),
        Err(e) => println!("  import failed: {e:#}"),
    }
}

#[cfg(not(feature = "networkmanager"))]
async fn import(_wifi: &rqs_lib::WifiInfo) {
    println!("  import skipped: built without --features networkmanager");
}

#[cfg(feature = "networkmanager")]
async fn nm_wifi() -> Result<(), anyhow::Error> {
    let nm = rqs_lib::nm::NetworkManager::system().await?;
    println!("{:#?}", nm.status().await?);
    match nm.active_wifi().await? {
        Some(w) => println!("{w:#?}"),
        None => println!("no active Wi-Fi connection"),
    }
    Ok(())
}

#[cfg(feature = "networkmanager")]
async fn nm_watch() -> Result<(), anyhow::Error> {
    use futures::StreamExt;

    let nm = rqs_lib::nm::NetworkManager::system().await?;
    println!("{:?}", nm.status().await?);
    let changes = nm.changes().await?;
    futures::pin_mut!(changes);
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            Some(status) = changes.next() => println!("{status:?}"),
        }
    }
    Ok(())
}

#[cfg(feature = "networkmanager")]
async fn active_wifi() -> Result<rqs_lib::WifiInfo, anyhow::Error> {
    rqs_lib::nm::NetworkManager::system()
        .await?
        .active_wifi()
        .await?
        .ok_or_else(|| anyhow::anyhow!("no active Wi-Fi connection"))
}

#[cfg(not(feature = "networkmanager"))]
async fn active_wifi() -> Result<rqs_lib::WifiInfo, anyhow::Error> {
    usage_error("--wifi without <ssid> needs --features networkmanager")
}
