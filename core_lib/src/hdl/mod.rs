use std::collections::HashMap;

use p256::{PublicKey, SecretKey};
use serde::{Deserialize, Serialize};
use ts_rs::TS;

use self::info::{InternalFileInfo, TransferMetadata};
use crate::location_nearby_connections::bandwidth_upgrade_negotiation_frame::EventType as BwuEvent;
use crate::location_nearby_connections::{
    offline_frame, v1_frame, BandwidthUpgradeNegotiationFrame, OfflineFrame, V1Frame,
};
use crate::securegcm::ukey2_client_init::CipherCommitment;
use crate::utils::RemoteDeviceInfo;

/// Sharing frames that carry nothing the transfer flow needs; unknown types decode as UnknownFrameType.
pub(crate) fn is_ignorable_sharing_frame(v1: &crate::sharing_nearby::V1Frame) -> bool {
    use crate::sharing_nearby::v1_frame::FrameType;
    matches!(
        v1.r#type(),
        FrameType::CertificateInfo
            | FrameType::ProgressUpdate
            | FrameType::Bindings
            | FrameType::UnknownFrameType
    )
}

/// Refuses offered upgrade paths like Nearby's `ForBwuFailure`, so the peer keeps the current connection.
pub(crate) fn upgrade_reply(v1: &V1Frame) -> Option<OfflineFrame> {
    if let Some(retry) = &v1.bandwidth_upgrade_retry {
        debug!(
            "Ignoring BANDWIDTH_UPGRADE_RETRY: supported_medium={:?} is_request={:?}",
            retry.supported_medium().collect::<Vec<_>>(),
            retry.is_request
        );
        return None;
    }

    let bwu = v1.bandwidth_upgrade_negotiation.as_ref()?;
    if bwu.event_type() != BwuEvent::UpgradePathAvailable {
        debug!("Ignoring bandwidth upgrade event: {:?}", bwu.event_type());
        return None;
    }

    info!(
        "Refusing bandwidth upgrade to {:?}",
        bwu.upgrade_path_info.as_ref().map(|i| i.medium())
    );
    Some(OfflineFrame {
        version: Some(offline_frame::Version::V1.into()),
        v1: Some(V1Frame {
            r#type: Some(v1_frame::FrameType::BandwidthUpgradeNegotiation.into()),
            bandwidth_upgrade_negotiation: Some(BandwidthUpgradeNegotiationFrame {
                event_type: Some(BwuEvent::UpgradeFailure.into()),
                upgrade_path_info: bwu.upgrade_path_info.clone(),
                ..Default::default()
            }),
            ..Default::default()
        }),
    })
}

#[cfg(feature = "experimental")]
mod ble;
#[cfg(feature = "experimental")]
pub use ble::*;
#[cfg(all(feature = "experimental", target_os = "linux"))]
mod blea;
#[cfg(all(feature = "experimental", target_os = "linux"))]
pub use blea::*;
mod inbound;
pub use inbound::*;
pub(crate) mod info;
mod mdns_discovery;
pub use mdns_discovery::*;
mod mdns;
pub use mdns::*;
mod outbound;
pub use outbound::*;
pub(crate) mod transport;

#[allow(dead_code)]
#[derive(Debug, Clone, Default, Serialize, Deserialize, TS, PartialEq)]
#[ts(export)]
pub enum State {
    #[default]
    Initial,
    ReceivedConnectionRequest,
    SentUkeyServerInit,
    SentUkeyClientInit,
    SentUkeyClientFinish,
    SentPairedKeyEncryption,
    ReceivedUkeyClientFinish,
    SentConnectionResponse,
    SentPairedKeyResult,
    SentIntroduction,
    ReceivedPairedKeyResult,
    WaitingForUserConsent,
    ReceivingFiles,
    SendingFiles,
    Disconnected,
    Rejected,
    Cancelled,
    Finished,
}

#[derive(Debug, Default)]
pub struct InnerState {
    pub id: String,
    pub server_seq: i32,
    pub client_seq: i32,
    pub encryption_done: bool,

    // Subject to be used-facing for progress, ...
    pub state: State,
    pub remote_device_info: Option<RemoteDeviceInfo>,
    pub pin_code: Option<String>,
    pub transfer_metadata: Option<TransferMetadata>,
    pub transferred_files: HashMap<i64, InternalFileInfo>,

    // Everything needed for encryption/decryption/verif
    pub cipher_commitment: Option<CipherCommitment>,
    pub private_key: Option<SecretKey>,
    pub public_key: Option<PublicKey>,
    pub server_init_data: Option<Vec<u8>>,
    pub client_init_msg_data: Option<Vec<u8>>,
    pub ukey_client_finish_msg_data: Option<Vec<u8>>,
    pub decrypt_key: Option<Vec<u8>>,
    pub recv_hmac_key: Option<Vec<u8>>,
    pub encrypt_key: Option<Vec<u8>>,
    pub send_hmac_key: Option<Vec<u8>>,

    // Used to handle/track ingress transfer
    pub text_payload: Option<TextPayloadInfo>,
    // pub text_payload_id: i64,
    // pub text_is_url: bool,
    // pub wifi_ssid: Option<String>,
    pub payload_buffers: HashMap<i64, Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum TextPayloadInfo {
    Url(i64),
    Text(i64),
    Wifi((i64, String)),
}

#[derive(Debug, Clone, Serialize, Deserialize, TS)]
pub enum TextPayloadType {
    Url,
    Text,
    Wifi,
}

impl TextPayloadInfo {
    fn get_i64_value(&self) -> i64 {
        match self {
            TextPayloadInfo::Url(value)
            | TextPayloadInfo::Text(value)
            | TextPayloadInfo::Wifi((value, _)) => value.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::location_nearby_connections::bandwidth_upgrade_negotiation_frame::upgrade_path_info::Medium;
    use crate::location_nearby_connections::bandwidth_upgrade_negotiation_frame::UpgradePathInfo;
    use crate::location_nearby_connections::BandwidthUpgradeRetryFrame;

    fn bwu_frame(event: BwuEvent, info: Option<UpgradePathInfo>) -> V1Frame {
        V1Frame {
            r#type: Some(v1_frame::FrameType::BandwidthUpgradeNegotiation.into()),
            bandwidth_upgrade_negotiation: Some(BandwidthUpgradeNegotiationFrame {
                event_type: Some(event.into()),
                upgrade_path_info: info,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn refuses_offered_upgrade_path() {
        let info = UpgradePathInfo {
            medium: Some(Medium::WifiHotspot.into()),
            ..Default::default()
        };
        let reply = upgrade_reply(&bwu_frame(
            BwuEvent::UpgradePathAvailable,
            Some(info.clone()),
        ))
        .expect("reply expected");

        let v1 = reply.v1.unwrap();
        assert_eq!(
            v1.r#type(),
            v1_frame::FrameType::BandwidthUpgradeNegotiation
        );
        let bwu = v1.bandwidth_upgrade_negotiation.unwrap();
        assert_eq!(bwu.event_type(), BwuEvent::UpgradeFailure);
        assert_eq!(bwu.upgrade_path_info, Some(info));
    }

    #[test]
    fn ignores_other_upgrade_events() {
        assert!(upgrade_reply(&bwu_frame(BwuEvent::SafeToClosePriorChannel, None)).is_none());
        assert!(upgrade_reply(&bwu_frame(BwuEvent::UpgradeFailure, None)).is_none());
    }

    #[test]
    fn ignores_upgrade_retry() {
        let v1 = V1Frame {
            r#type: Some(v1_frame::FrameType::BandwidthUpgradeRetry.into()),
            bandwidth_upgrade_retry: Some(BandwidthUpgradeRetryFrame {
                supported_medium: vec![3, 8],
                is_request: Some(true),
            }),
            ..Default::default()
        };
        assert!(upgrade_reply(&v1).is_none());
    }
}
