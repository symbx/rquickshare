use std::fs::File;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use ts_rs::TS;

use super::TextPayloadType;
use crate::sharing_nearby::wifi_credentials_metadata::SecurityType;
use crate::utils::RemoteDeviceInfo;

#[derive(Debug)]
pub struct InternalFileInfo {
    pub payload_id: i64,
    pub file_url: PathBuf,
    pub bytes_transferred: i64,
    pub total_size: i64,
    pub file: Option<File>,
}

#[derive(Debug, Clone, Default, Deserialize, Serialize, TS)]
#[ts(export)]
pub struct TransferMetadata {
    pub id: String,
    pub source: Option<RemoteDeviceInfo>,
    pub pin_code: Option<String>,

    pub destination: Option<String>,
    pub files: Option<Vec<String>>,

    pub text_type: Option<TextPayloadType>,
    pub text_description: Option<String>,
    pub text_payload: Option<String>,
    pub wifi: Option<WifiInfo>,

    pub total_bytes: u64,
    pub ack_bytes: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, TS)]
#[ts(export)]
pub enum WifiSecurity {
    #[default]
    Unknown,
    Open,
    WpaPsk,
    Wep,
}

impl From<SecurityType> for WifiSecurity {
    fn from(value: SecurityType) -> Self {
        match value {
            SecurityType::UnknownSecurityType => Self::Unknown,
            SecurityType::Open => Self::Open,
            SecurityType::WpaPsk => Self::WpaPsk,
            SecurityType::Wep => Self::Wep,
        }
    }
}

/// Shared Wi-Fi network; `password` and `hidden` are known once the credentials payload arrived.
#[derive(Debug, Clone, Default, PartialEq, Deserialize, Serialize, TS)]
#[ts(export)]
pub struct WifiInfo {
    pub ssid: String,
    pub security: WifiSecurity,
    pub password: Option<String>,
    pub hidden: bool,
}
