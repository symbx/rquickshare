use std::time::Duration;

use rqs_lib::channel::{ChannelAction, ChannelDirection, ChannelMessage};
use rqs_lib::{OutboundPayload, SendInfo, State, Visibility, RQS};

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
