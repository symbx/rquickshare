//! NetworkManager client (system D-Bus): Wi-Fi import, active Wi-Fi secrets, network-change watching.

use std::collections::HashMap;

use anyhow::{anyhow, bail, Result};
use futures::{future, Stream, StreamExt};
use zbus::proxy::CacheProperties;
use zbus::zvariant::{OwnedObjectPath, OwnedValue, Value};

use crate::{WifiInfo, WifiSecurity};

/// NetworkManager connection settings (`a{sa{sv}}`).
pub type ConnectionSettings = HashMap<String, HashMap<String, OwnedValue>>;

const SERVICE: &str = "org.freedesktop.NetworkManager";
const WIRELESS: &str = "802-11-wireless";
const WIRELESS_SECURITY: &str = "802-11-wireless-security";
const NM_STATE_CONNECTED_LOCAL: u32 = 50;

#[zbus::proxy(
    interface = "org.freedesktop.NetworkManager",
    default_service = "org.freedesktop.NetworkManager",
    default_path = "/org/freedesktop/NetworkManager"
)]
trait Manager {
    #[zbus(signal)]
    fn state_changed(&self, state: u32) -> zbus::Result<()>;

    #[zbus(property, name = "State")]
    fn nm_state(&self) -> zbus::Result<u32>;

    #[zbus(property)]
    fn primary_connection(&self) -> zbus::Result<OwnedObjectPath>;

    #[zbus(property)]
    fn active_connections(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
}

#[zbus::proxy(
    interface = "org.freedesktop.NetworkManager.Settings",
    default_service = "org.freedesktop.NetworkManager",
    default_path = "/org/freedesktop/NetworkManager/Settings"
)]
trait Settings {
    fn add_connection(&self, connection: &ConnectionSettings) -> zbus::Result<OwnedObjectPath>;

    fn list_connections(&self) -> zbus::Result<Vec<OwnedObjectPath>>;
}

#[zbus::proxy(
    interface = "org.freedesktop.NetworkManager.Settings.Connection",
    default_service = "org.freedesktop.NetworkManager"
)]
trait SettingsConnection {
    fn get_settings(&self) -> zbus::Result<ConnectionSettings>;

    fn get_secrets(&self, setting_name: &str) -> zbus::Result<ConnectionSettings>;

    fn update(&self, properties: &ConnectionSettings) -> zbus::Result<()>;
}

#[zbus::proxy(
    interface = "org.freedesktop.NetworkManager.Connection.Active",
    default_service = "org.freedesktop.NetworkManager"
)]
trait ActiveConnection {
    #[zbus(property)]
    fn connection(&self) -> zbus::Result<OwnedObjectPath>;

    #[zbus(property)]
    fn id(&self) -> zbus::Result<String>;

    #[zbus(property)]
    fn uuid(&self) -> zbus::Result<String>;

    #[zbus(property, name = "Type")]
    fn connection_type(&self) -> zbus::Result<String>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NetworkStatus {
    /// NetworkManager state is at least CONNECTED_LOCAL.
    pub connected: bool,
    pub primary_id: Option<String>,
    pub primary_uuid: Option<String>,
    pub primary_type: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NetworkManager {
    conn: zbus::Connection,
}

impl NetworkManager {
    pub async fn system() -> Result<Self> {
        Ok(Self {
            conn: zbus::Connection::system().await?,
        })
    }

    pub fn with_connection(conn: zbus::Connection) -> Self {
        Self { conn }
    }

    /// Saves the network as a connection profile, updating an existing profile with the same SSID.
    pub async fn import_wifi(&self, wifi: &WifiInfo) -> Result<OwnedObjectPath> {
        if wifi.password.is_none() && wifi.security != WifiSecurity::Open {
            bail!("Wi-Fi password for {:?} not received", wifi.ssid);
        }

        let settings = SettingsProxy::new(&self.conn).await?;
        for path in settings.list_connections().await? {
            let profile = self.settings_connection(&path).await?;
            let Ok(mut current) = profile.get_settings().await else {
                continue;
            };
            if wifi_ssid(&current).as_deref() == Some(wifi.ssid.as_str()) {
                apply_wifi(&mut current, wifi)?;
                profile.update(&current).await?;
                return Ok(path);
            }
        }

        Ok(settings.add_connection(&new_wifi_settings(wifi)?).await?)
    }

    /// Credentials of the active Wi-Fi connection, preferring the primary one; reading secrets needs polkit permission.
    pub async fn active_wifi(&self) -> Result<Option<WifiInfo>> {
        let nm = self.manager().await?;
        let primary = nm.primary_connection().await?;
        let mut actives = nm.active_connections().await?;
        actives.sort_by_key(|p| *p != primary);

        for path in actives {
            let active = self.active_connection(&path).await?;
            if active.connection_type().await? != WIRELESS {
                continue;
            }
            let profile = self
                .settings_connection(&active.connection().await?)
                .await?;
            let settings = profile.get_settings().await?;
            let secrets = if settings.contains_key(WIRELESS_SECURITY) {
                profile.get_secrets(WIRELESS_SECURITY).await?
            } else {
                ConnectionSettings::new()
            };
            return wifi_from_settings(&settings, &secrets).map(Some);
        }
        Ok(None)
    }

    pub async fn status(&self) -> Result<NetworkStatus> {
        let nm = self.manager().await?;
        let mut status = NetworkStatus {
            connected: nm.nm_state().await? >= NM_STATE_CONNECTED_LOCAL,
            ..Default::default()
        };

        let primary = nm.primary_connection().await?;
        if primary.as_str() != "/" {
            if let Ok(active) = self.active_connection(&primary).await {
                status.primary_id = active.id().await.ok();
                status.primary_uuid = active.uuid().await.ok();
                status.primary_type = active.connection_type().await.ok();
            }
        }
        Ok(status)
    }

    /// Yields the new status whenever the global state or the primary connection changes (not the current one).
    pub async fn changes(&self) -> Result<impl Stream<Item = NetworkStatus> + Send + 'static> {
        let nm = ManagerProxy::new(&self.conn).await?;
        let states = nm.receive_state_changed().await?.map(|_| ());
        let primary = nm.receive_primary_connection_changed().await.map(|_| ());
        let this = self.clone();
        let current = self.status().await.ok();

        Ok(futures::stream::select(states, primary)
            .then(move |_| {
                let this = this.clone();
                async move { this.status().await }
            })
            .filter_map(|r| future::ready(r.map_err(|e| warn!("NetworkManager status: {e}")).ok()))
            .scan(current, |last: &mut Option<NetworkStatus>, status| {
                let changed = last.as_ref() != Some(&status);
                if changed {
                    *last = Some(status.clone());
                }
                future::ready(Some(changed.then_some(status)))
            })
            .filter_map(future::ready))
    }

    async fn manager(&self) -> zbus::Result<ManagerProxy<'static>> {
        ManagerProxy::builder(&self.conn)
            .cache_properties(CacheProperties::No)
            .build()
            .await
    }

    async fn settings_connection(
        &self,
        path: &OwnedObjectPath,
    ) -> zbus::Result<SettingsConnectionProxy<'static>> {
        SettingsConnectionProxy::builder(&self.conn)
            .destination(SERVICE)?
            .path(path.clone())?
            .cache_properties(CacheProperties::No)
            .build()
            .await
    }

    async fn active_connection(
        &self,
        path: &OwnedObjectPath,
    ) -> zbus::Result<ActiveConnectionProxy<'static>> {
        ActiveConnectionProxy::builder(&self.conn)
            .destination(SERVICE)?
            .path(path.clone())?
            .cache_properties(CacheProperties::No)
            .build()
            .await
    }
}

const SETTLE: std::time::Duration = std::time::Duration::from_secs(1);

/// Signals `sender` once NetworkManager settles on a connected state after a change.
pub(crate) async fn forward_network_changes(
    sender: tokio::sync::broadcast::Sender<()>,
    ctk: tokio_util::sync::CancellationToken,
) {
    let changes = match NetworkManager::system().await {
        Ok(nm) => nm.changes().await,
        Err(e) => Err(e),
    };
    let changes = match changes {
        Ok(c) => c,
        Err(e) => {
            warn!("NetworkManager unavailable, network changes not tracked: {e}");
            return;
        }
    };
    futures::pin_mut!(changes);

    loop {
        let mut last = tokio::select! {
            _ = ctk.cancelled() => return,
            s = changes.next() => match s {
                Some(s) => s,
                None => return,
            },
        };
        loop {
            tokio::select! {
                _ = ctk.cancelled() => return,
                s = tokio::time::timeout(SETTLE, changes.next()) => match s {
                    Ok(Some(s)) => last = s,
                    Ok(None) => return,
                    Err(_) => break,
                },
            }
        }
        debug!("Network status: {last:?}");
        if last.connected {
            let _ = sender.send(());
        }
    }
}

fn owned<'a>(value: impl Into<Value<'a>>) -> Result<OwnedValue> {
    Ok(OwnedValue::try_from(value.into())?)
}

fn get_str(setting: &HashMap<String, OwnedValue>, key: &str) -> Option<String> {
    setting
        .get(key)
        .and_then(|v| v.downcast_ref::<&str>().ok())
        .map(str::to_owned)
}

fn wifi_ssid(settings: &ConnectionSettings) -> Option<String> {
    let ssid = settings.get(WIRELESS)?.get("ssid")?;
    let bytes = Vec::<u8>::try_from(ssid.try_clone().ok()?).ok()?;
    Some(String::from_utf8_lossy(&bytes).into_owned())
}

/// WEP key type: 1 for a raw 40/104-bit key (ASCII or hex), 2 for a passphrase.
fn wep_key_type(key: &str) -> u32 {
    let hex = key.chars().all(|c| c.is_ascii_hexdigit());
    match key.len() {
        5 | 13 => 1,
        10 | 26 if hex => 1,
        _ => 2,
    }
}

fn new_wifi_settings(wifi: &WifiInfo) -> Result<ConnectionSettings> {
    let mut settings = ConnectionSettings::new();
    settings.insert(
        "connection".into(),
        HashMap::from([
            ("id".into(), owned(wifi.ssid.as_str())?),
            ("type".into(), owned(WIRELESS)?),
            ("uuid".into(), owned(uuid::Uuid::new_v4().to_string())?),
        ]),
    );
    settings.insert(
        WIRELESS.into(),
        HashMap::from([("mode".into(), owned("infrastructure")?)]),
    );
    apply_wifi(&mut settings, wifi)?;
    Ok(settings)
}

fn apply_wifi(settings: &mut ConnectionSettings, wifi: &WifiInfo) -> Result<()> {
    let wireless = settings.entry(WIRELESS.into()).or_default();
    wireless.insert("ssid".into(), owned(wifi.ssid.as_bytes().to_vec())?);
    wireless.insert("hidden".into(), owned(wifi.hidden)?);
    wireless.remove("security");
    settings.remove(WIRELESS_SECURITY);
    settings.remove("802-1x");

    let password = wifi.password.as_deref().unwrap_or_default();
    let security_setting = match wifi.effective_security() {
        WifiSecurity::Open => return Ok(()),
        WifiSecurity::WpaPsk => HashMap::from([
            ("key-mgmt".into(), owned("wpa-psk")?),
            ("psk".into(), owned(password)?),
        ]),
        WifiSecurity::Wep => HashMap::from([
            ("key-mgmt".into(), owned("none")?),
            ("wep-key0".into(), owned(password)?),
            ("wep-key-type".into(), owned(wep_key_type(password))?),
        ]),
        WifiSecurity::Unknown => unreachable!(),
    };
    settings.insert(WIRELESS_SECURITY.into(), security_setting);
    Ok(())
}

fn wifi_from_settings(
    settings: &ConnectionSettings,
    secrets: &ConnectionSettings,
) -> Result<WifiInfo> {
    let wireless = settings
        .get(WIRELESS)
        .ok_or_else(|| anyhow!("not a Wi-Fi connection"))?;
    let ssid = wifi_ssid(settings).ok_or_else(|| anyhow!("Wi-Fi connection without SSID"))?;
    let hidden = wireless
        .get("hidden")
        .and_then(|v| v.downcast_ref::<bool>().ok())
        .unwrap_or(false);

    let security = settings.get(WIRELESS_SECURITY);
    let secret = secrets.get(WIRELESS_SECURITY);
    let (security, password) = match security.and_then(|s| get_str(s, "key-mgmt")).as_deref() {
        None | Some("owe") => (WifiSecurity::Open, None),
        Some("wpa-psk" | "sae") => (WifiSecurity::WpaPsk, secret.and_then(|s| get_str(s, "psk"))),
        Some("none") => {
            let idx = security
                .and_then(|s| s.get("wep-tx-keyidx"))
                .and_then(|v| v.downcast_ref::<u32>().ok())
                .unwrap_or(0);
            (
                WifiSecurity::Wep,
                secret.and_then(|s| get_str(s, &format!("wep-key{idx}"))),
            )
        }
        Some(other) => bail!("unsupported Wi-Fi key management {other:?}"),
    };
    if security != WifiSecurity::Open && password.is_none() {
        bail!("secret of {ssid:?} not available (no permission or stored by a secret agent)");
    }

    Ok(WifiInfo {
        ssid,
        security,
        password,
        hidden,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wifi(security: WifiSecurity, password: Option<&str>) -> WifiInfo {
        WifiInfo {
            ssid: "Home".into(),
            security,
            password: password.map(str::to_owned),
            hidden: true,
        }
    }

    fn secrets_of(settings: &ConnectionSettings) -> ConnectionSettings {
        settings
            .iter()
            .filter(|(k, _)| k.as_str() == WIRELESS_SECURITY)
            .map(|(k, v)| {
                (
                    k.clone(),
                    v.iter()
                        .map(|(k, v)| (k.clone(), v.try_clone().unwrap()))
                        .collect(),
                )
            })
            .collect()
    }

    #[test]
    fn wpa_psk_round_trip() {
        let original = wifi(WifiSecurity::WpaPsk, Some("secret123"));
        let settings = new_wifi_settings(&original).unwrap();
        assert_eq!(
            get_str(&settings["connection"], "type").as_deref(),
            Some(WIRELESS)
        );
        assert_eq!(
            get_str(&settings[WIRELESS_SECURITY], "key-mgmt").as_deref(),
            Some("wpa-psk")
        );
        assert_eq!(
            wifi_from_settings(&settings, &secrets_of(&settings)).unwrap(),
            original
        );
    }

    #[test]
    fn open_network_has_no_security() {
        let settings = new_wifi_settings(&wifi(WifiSecurity::Unknown, Some(""))).unwrap();
        assert!(!settings.contains_key(WIRELESS_SECURITY));
        let read = wifi_from_settings(&settings, &ConnectionSettings::new()).unwrap();
        assert_eq!(read.security, WifiSecurity::Open);
        assert_eq!(read.password, None);
    }

    #[test]
    fn wep_key_types() {
        let settings = new_wifi_settings(&wifi(WifiSecurity::Wep, Some("abcde"))).unwrap();
        let sec = &settings[WIRELESS_SECURITY];
        assert_eq!(get_str(sec, "key-mgmt").as_deref(), Some("none"));
        assert_eq!(sec["wep-key-type"].downcast_ref::<u32>().unwrap(), 1);
        assert_eq!(wep_key_type("0123456789"), 1);
        assert_eq!(wep_key_type("long passphrase"), 2);
        assert_eq!(
            wifi_from_settings(&settings, &secrets_of(&settings))
                .unwrap()
                .password
                .as_deref(),
            Some("abcde")
        );
    }

    #[test]
    fn reimport_replaces_enterprise_security() {
        let mut settings = new_wifi_settings(&wifi(WifiSecurity::WpaPsk, Some("old"))).unwrap();
        settings.insert("802-1x".into(), HashMap::new());
        apply_wifi(&mut settings, &wifi(WifiSecurity::WpaPsk, Some("new"))).unwrap();
        assert!(!settings.contains_key("802-1x"));
        assert_eq!(
            get_str(&settings[WIRELESS_SECURITY], "psk").as_deref(),
            Some("new")
        );
    }

    #[test]
    fn missing_secret_is_an_error() {
        let settings = new_wifi_settings(&wifi(WifiSecurity::WpaPsk, Some("x"))).unwrap();
        assert!(wifi_from_settings(&settings, &ConnectionSettings::new()).is_err());
    }
}
