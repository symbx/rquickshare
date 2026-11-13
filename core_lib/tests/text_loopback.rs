use std::time::Duration;

use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage};
use rqs_lib::{OutboundPayload, SendInfo, State, Visibility, WifiInfo, WifiSecurity, RQS};

async fn transfer(port: u32, ob: OutboundPayload) -> ChannelMessage {
    let mut receiver = RQS::new(Visibility::Invisible, Some(port), None);
    receiver.run().await.unwrap();
    let mut sender = RQS::new(Visibility::Invisible, None, None);
    let (send_tx, _) = sender.run().await.unwrap();

    let mut events = receiver.message_sender.subscribe();
    let control = receiver.message_sender.clone();

    send_tx
        .send(SendInfo {
            id: "loopback".into(),
            name: "receiver".into(),
            addr: format!("127.0.0.1:{port}"),
            ob,
        })
        .await
        .unwrap();

    let finished = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let msg = events.recv().await.unwrap();
            if msg.direction != ChannelDirection::LibToFront {
                continue;
            }
            match msg.state {
                Some(State::WaitingForUserConsent) => {
                    control
                        .send(ChannelMessage {
                            id: msg.id.clone(),
                            direction: ChannelDirection::FrontToLib,
                            action: Some(ChannelAction::AcceptTransfer),
                            ..Default::default()
                        })
                        .unwrap();
                }
                Some(State::Finished) => return msg,
                _ => {}
            }
        }
    })
    .await
    .expect("transfer did not finish in time");

    sender.stop().await;
    receiver.stop().await;
    finished
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_text() {
    let text = "hello \u{10} from rqs_lib — ünïcödé";
    let msg = transfer(47913, OutboundPayload::Text(text.into())).await;
    let meta = msg.meta.unwrap();
    assert!(matches!(
        meta.text_type,
        Some(rqs_lib::TextPayloadType::Text)
    ));
    assert_eq!(meta.text_payload.as_deref(), Some(text));
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_url() {
    let url = "https://example.com/path?q=1";
    let msg = transfer(47914, OutboundPayload::Url(url.into())).await;
    let meta = msg.meta.unwrap();
    assert!(matches!(
        meta.text_type,
        Some(rqs_lib::TextPayloadType::Url)
    ));
    assert_eq!(meta.text_payload.as_deref(), Some(url));
}

#[tokio::test(flavor = "multi_thread")]
async fn sends_wifi() {
    let wifi = WifiInfo {
        ssid: "Home Net".into(),
        security: WifiSecurity::WpaPsk,
        password: Some("correct horse battery".into()),
        hidden: true,
    };
    let msg = transfer(47915, OutboundPayload::Wifi(wifi.clone())).await;
    let meta = msg.meta.unwrap();
    assert!(matches!(
        meta.text_type,
        Some(rqs_lib::TextPayloadType::Wifi)
    ));
    assert_eq!(meta.wifi, Some(wifi));
}

#[tokio::test(flavor = "multi_thread")]
async fn sender_sees_rejection() {
    let port = 47916;
    let mut receiver = RQS::new(Visibility::Invisible, Some(port), None);
    receiver.run().await.unwrap();
    let mut sender = RQS::new(Visibility::Invisible, None, None);
    let (send_tx, _) = sender.run().await.unwrap();

    let mut recv_events = receiver.message_sender.subscribe();
    let mut send_events = sender.message_sender.subscribe();
    let control = receiver.message_sender.clone();

    send_tx
        .send(SendInfo {
            id: "reject".into(),
            name: "receiver".into(),
            addr: format!("127.0.0.1:{port}"),
            ob: OutboundPayload::Text("unwanted".into()),
        })
        .await
        .unwrap();

    tokio::spawn(async move {
        loop {
            let msg = recv_events.recv().await.unwrap();
            if msg.direction == ChannelDirection::LibToFront
                && msg.state == Some(State::WaitingForUserConsent)
            {
                control
                    .send(ChannelMessage {
                        id: msg.id,
                        direction: ChannelDirection::FrontToLib,
                        action: Some(ChannelAction::RejectTransfer),
                        ..Default::default()
                    })
                    .unwrap();
            }
        }
    });

    let end = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let msg = send_events.recv().await.unwrap();
            if msg.direction != ChannelDirection::LibToFront {
                continue;
            }
            if let Some(s @ (State::Rejected | State::Disconnected | State::Finished)) = msg.state {
                return s;
            }
        }
    })
    .await
    .expect("sender did not see the end of the transfer");

    sender.stop().await;
    receiver.stop().await;
    assert_eq!(end, State::Rejected);
}
