//! Env-var parsing for `bins/kaas/main.rs`.
//!
//! All knobs are env-only — no flag parser. Names are stable
//! (`KAAS_*`) so the chart's env block doesn't churn
//! between flavours.

use std::env;
use std::path::PathBuf;

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("KAAS_LISTENERS: {0}")]
    Listeners(serde_json::Error),
    #[error("KAAS_LISTENERS empty — set at least one entry")]
    NoListeners,
    #[error("KAAS_BROKER_ID: {0}")]
    BrokerId(std::num::ParseIntError),
    #[error("KAAS_FLUSH_INTERVAL_MESSAGES: {0}")]
    FlushInterval(std::num::ParseIntError),
    #[error("KAAS_RETENTION_CHECK_INTERVAL: {0}")]
    RetentionCheckInterval(std::num::ParseIntError),
    #[error("KAAS_TXN_NUM_SLOTS: {0}")]
    TxnNumSlots(std::num::ParseIntError),
    #[error("KAAS_MAX_MESSAGE_BYTES: {0}")]
    MaxMessageBytes(std::num::ParseIntError),
    #[error("KAAS_FSYNC_MAX_LATENCY_MS: {0}")]
    FsyncMaxLatency(std::num::ParseIntError),
    #[error("{0}")]
    LogDirs(String),
}

/// JSON entry in `KAAS_LISTENERS`. Mirrors the Helm chart's
/// listener array shape (gh #126). Two shapes are accepted:
///
/// * **Chart shape** (Strimzi-style; what `kaas.listenersJSON`
///   in `deploy/helm/kaas/templates/_helpers.tpl` emits):
///   `{"name":"plain","port":9092,"type":"internal","tls":false,
///     "authentication":{"type":"none"}}`. The `port` is expanded
///   into `0.0.0.0:<port>`; `tls: true` is upgraded to a
///   [`TlsConfig`] populated from `KAAS_TLS_CERT_FILE` /
///   `KAAS_TLS_KEY_FILE` (falls back to `/tls/tls.crt` +
///   `/tls/tls.key`, matching the chart's Secret mount path).
///   `authentication.type` maps into `authentication_type`.
/// * **Internal shape** (test fixtures + backward-compat):
///   `{"name":"plain","addr":"0.0.0.0:9092","tls":{...},
///     "authenticationType":"scram-sha-512"}`.
///
/// Both shapes fold into the same struct via the custom
/// [`Deserialize`] impl below.
#[derive(Debug, Clone)]
pub struct ListenerEntry {
    pub name: String,
    /// `host:port`. Use `0.0.0.0:9092` to bind all interfaces.
    pub addr: String,
    /// Optional advertised host (defaults to listener `addr`'s host).
    /// Phase 5 wires the per-broker external hostname template here.
    pub advertised_host: Option<String>,
    /// Optional TLS config. `None` ↔ plaintext listener.
    pub tls: Option<TlsConfig>,
    /// Optional SASL mechanism / mTLS mode. `None` ↔ "none"
    /// (anonymous listener). Recognised values:
    /// `"scram-sha-512"`, `"plain"`, `"mtls"`, `"oauth"`.
    pub authentication_type: Option<String>,
    /// OAuth validation config; `Some` iff `authentication_type` is
    /// `"oauth"` (gh #42). The required fields are enforced at parse
    /// time so a broken oauth listener fails boot, not first auth.
    pub oauth: Option<OauthListenerConfig>,
}

/// Strimzi `KafkaListenerAuthenticationOAuth` field names, carried
/// verbatim from the chart through `KAAS_LISTENERS`.
#[derive(Debug, Clone)]
pub struct OauthListenerConfig {
    pub valid_issuer_uri: String,
    pub jwks_endpoint_uri: String,
    pub user_name_claim: Option<String>,
    pub fallback_user_name_claim: Option<String>,
    pub check_audience: bool,
    pub client_id: Option<String>,
    /// Default 300 (Strimzi's `jwksRefreshSeconds` default).
    pub jwks_refresh_seconds: u64,
    pub max_seconds_without_reauthentication: Option<u64>,
}

impl<'de> Deserialize<'de> for ListenerEntry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;

        #[derive(Deserialize)]
        struct Raw {
            name: String,
            #[serde(default)]
            addr: Option<String>,
            #[serde(default)]
            port: Option<u16>,
            #[serde(default)]
            #[serde(rename = "advertisedHost", alias = "advertised_host")]
            advertised_host: Option<String>,
            #[serde(default)]
            tls: Option<TlsField>,
            #[serde(default)]
            authentication: Option<AuthField>,
            #[serde(default, rename = "authenticationType", alias = "authentication_type")]
            authentication_type: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TlsField {
            Bool(bool),
            Config(TlsConfig),
        }

        #[derive(Deserialize)]
        struct AuthField {
            #[serde(rename = "type")]
            ty: String,
            #[serde(flatten)]
            oauth: OauthRaw,
        }

        /// All-optional capture of the oauth fields so a partial
        /// block yields a *named* missing-field error below instead
        /// of silently deserializing to nothing.
        #[derive(Deserialize, Default)]
        #[serde(rename_all = "camelCase")]
        struct OauthRaw {
            valid_issuer_uri: Option<String>,
            jwks_endpoint_uri: Option<String>,
            user_name_claim: Option<String>,
            fallback_user_name_claim: Option<String>,
            #[serde(default)]
            check_audience: bool,
            client_id: Option<String>,
            jwks_refresh_seconds: Option<u64>,
            max_seconds_without_reauthentication: Option<u64>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let addr = match (raw.addr, raw.port) {
            (Some(a), _) => a,
            (None, Some(p)) => format!("0.0.0.0:{p}"),
            (None, None) => {
                return Err(D::Error::custom(
                    "listener needs either `addr` (host:port) or `port` (int)",
                ));
            }
        };

        let tls = match raw.tls {
            None => None,
            Some(TlsField::Bool(false)) => None,
            Some(TlsField::Bool(true)) => {
                // Chart-shape boolean: resolve cert paths from the
                // Secret-mount env vars the chart populates. Same
                // paths for every TLS listener.
                let cert = std::env::var("KAAS_TLS_CERT_FILE")
                    .unwrap_or_else(|_| "/tls/tls.crt".to_owned());
                let key = std::env::var("KAAS_TLS_KEY_FILE")
                    .unwrap_or_else(|_| "/tls/tls.key".to_owned());
                let client_ca = std::env::var("KAAS_TLS_CLIENT_CA_FILE")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .map(PathBuf::from);
                Some(TlsConfig {
                    cert_path: PathBuf::from(cert),
                    key_path: PathBuf::from(key),
                    client_ca_path: client_ca,
                })
            }
            Some(TlsField::Config(cfg)) => Some(cfg),
        };

        let (authentication_type, oauth_raw) = match raw.authentication {
            Some(a) => (raw.authentication_type.or(Some(a.ty)), Some(a.oauth)),
            None => (raw.authentication_type, None),
        };
        let authentication_type = authentication_type.filter(|s| s != "none");

        // gh #42: an oauth listener's required fields fail the parse
        // (and therefore boot) rather than falling back to the
        // anonymous engine at runtime.
        let oauth = if authentication_type.as_deref() == Some("oauth") {
            let o = oauth_raw.unwrap_or_default();
            let require = |field: Option<String>, name: &str| {
                field.filter(|s| !s.is_empty()).ok_or_else(|| {
                    D::Error::custom(format!(
                        "listener {:?}: authentication.type oauth requires {name}",
                        raw.name
                    ))
                })
            };
            let cfg = OauthListenerConfig {
                valid_issuer_uri: require(o.valid_issuer_uri, "validIssuerUri")?,
                jwks_endpoint_uri: require(o.jwks_endpoint_uri, "jwksEndpointUri")?,
                user_name_claim: o.user_name_claim,
                fallback_user_name_claim: o.fallback_user_name_claim,
                check_audience: o.check_audience,
                client_id: o.client_id,
                jwks_refresh_seconds: o.jwks_refresh_seconds.unwrap_or(300),
                max_seconds_without_reauthentication: o.max_seconds_without_reauthentication,
            };
            if cfg.check_audience && cfg.client_id.is_none() {
                return Err(D::Error::custom(format!(
                    "listener {:?}: checkAudience requires clientId",
                    raw.name
                )));
            }
            Some(cfg)
        } else {
            None
        };

        Ok(Self {
            name: raw.name,
            addr,
            advertised_host: raw.advertised_host,
            tls,
            authentication_type,
            oauth,
        })
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct TlsConfig {
    #[serde(rename = "certPath")]
    pub cert_path: PathBuf,
    #[serde(rename = "keyPath")]
    pub key_path: PathBuf,
    /// `Some` ↔ require client cert (mTLS); the file is the trust
    /// anchor used to verify the client.
    #[serde(default, rename = "clientCaPath")]
    pub client_ca_path: Option<PathBuf>,
}

#[derive(Debug)]
pub struct Cli {
    /// Malformed or out-of-range env values `from_env` replaced with a
    /// default (gh #281). `from_env` runs before the log subscriber
    /// exists, so the binary logs these once tracing is up — a knob that
    /// parses wrong must say so, not silently do something else.
    pub config_warnings: Vec<String>,
    pub listeners: Vec<ListenerEntry>,
    pub data_dir: Option<PathBuf>,
    /// `KAAS_CLUSTER_DIR` — where cluster-wide coordination state
    /// (assignment.json, txn_state/, __consumer_offsets/, credentials,
    /// fence + marker queues, producer_ids/) lives. Unset → the
    /// legacy layout, `<data_dir>/__cluster`. The chart sets this to
    /// the control-plane volume's mount when `storage.controlPlane`
    /// is enabled (gh #221 phase 1) so a runaway topic filling the
    /// data volume can't take the control plane down with it.
    pub cluster_dir: Option<PathBuf>,
    /// `KAAS_LOG_DIRS` — named pool log dirs beyond the default
    /// `data_dir` (gh #221 phase 2). Chart-emitted JSON:
    /// `[{"name":"fast","path":"/vols/fast","defaultEligible":true}]`.
    /// Empty in the classic single-volume layout.
    pub log_dirs: Vec<kaas_storage::LogDirInfo>,
    pub flush_interval_messages: i64,
    /// `KAAS_RETENTION_CHECK_INTERVAL` — how often the retention
    /// cleaner sweeps (gh #250). Mirrors Apache's
    /// `log.retention.check.interval.ms`, same 5-minute default.
    /// `0` disables the sweep entirely.
    pub retention_check_interval_secs: u64,
    pub cluster_id: String,
    pub broker_id: i32,
    pub topics_seed: String,
    pub log_level: String,
    /// `true` disables every auth/authz/quota path —
    /// `AllowAllAuthorizer` + `NoQuotaChecker` regardless of listener
    /// config. Dev-mode default.
    pub auth_disabled: bool,
    /// `""` (default) → `AllowAllAuthorizer`. `"simple"` → ACL-based
    /// `AclEngine`. Mirrors Strimzi `authorization.type`.
    pub authorization_type: String,
    /// `User:foo,User:bar`-shape comma list. Wraps the chosen
    /// authorizer in `SuperUserAuthorizer` for early-allow.
    pub super_users: Vec<String>,
    /// Apache `ssl.principal.mapping.rules` (gh #43, KIP-371).
    /// Empty → CN unchanged.
    pub ssl_principal_mapping_rules: String,
    /// `KAAS_AUTO_CREATE_TOPICS_ENABLE` — Apache
    /// `auto.create.topics.enable` (gh #109, gh #242). When set, a
    /// Metadata request that names an unknown topic *and* carries
    /// `allowAutoTopicCreation` mints a `KafkaTopic` CR and answers
    /// `LEADER_NOT_AVAILABLE` so the client retries. Defaults to
    /// Apache's `true`; inert without a CR writer (dev mode).
    pub auto_create_topics: bool,
    /// `KAAS_NUM_PARTITIONS` — Apache `num.partitions`: the partition
    /// count of an auto-created topic, and of a CreateTopics request
    /// that asks for the default with `-1` (gh #280). Apache's default
    /// is 1.
    pub num_partitions: i32,
    /// `KAAS_REQUIRE_SASL` (gh #251, re-port of the Go broker's
    /// `SKAFKA_REQUIRE_SASL`) — when true, listeners with no
    /// `authentication.type` get the real SASL engine instead of the
    /// anonymous allow-all one, closing the anonymous-fallthrough
    /// hole cluster-wide. `KAAS_AUTH_DISABLED=true` outranks it.
    pub require_sasl: bool,
    /// `KAAS_TXN_NUM_SLOTS` (gh #255) — slot-shard count for the txn
    /// state store. `0` (default) = the store's built-in default
    /// (50, Apache's `transaction.state.log.num.partitions`).
    /// Changing it on a live cluster re-shards txn ownership — drain
    /// in-flight transactions first.
    pub txn_num_slots: usize,
    /// `KAAS_MAX_MESSAGE_BYTES` (gh #253) — Apache `message.max.bytes`,
    /// the broker-wide cap on one Produce record batch. Same 1 MiB +
    /// header default as Apache (1048588).
    pub max_message_bytes: usize,
    /// `KAAS_FSYNC_MAX_LATENCY_MS` (gh #256, re-port of gh #95) —
    /// watchdog deadline for one log fsync; a commit cycle that
    /// stalls longer fails the append instead of wedging the
    /// partition. `0` disables the watchdog. Default 30 000 ms.
    pub fsync_max_latency_ms: u64,
}

impl Cli {
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config_warnings = Vec::new();
        let listeners_json =
            env::var("KAAS_LISTENERS").unwrap_or_else(|_| default_listeners().to_owned());
        let mut listeners: Vec<ListenerEntry> =
            serde_json::from_str(&listeners_json).map_err(ConfigError::Listeners)?;
        if listeners.is_empty() {
            return Err(ConfigError::NoListeners);
        }
        // Derive per-listener advertised_host from the StatefulSet
        // env vars when the chart-shape JSON didn't include one.
        // Without this Metadata responses fall back to 127.0.0.1
        // and clients bootstrap onto the wrong endpoint.
        if let Some(host) = derive_advertised_host() {
            for l in listeners.iter_mut() {
                if l.advertised_host.is_none() {
                    l.advertised_host = Some(host.clone());
                }
            }
        }

        let data_dir = env::var("KAAS_DATA_DIR").ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(PathBuf::from(s))
            }
        });

        let cluster_dir = env::var("KAAS_CLUSTER_DIR").ok().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(PathBuf::from(s))
            }
        });

        let log_dirs =
            kaas_storage::parse_log_dirs_json(&env::var("KAAS_LOG_DIRS").unwrap_or_default())
                .map_err(ConfigError::LogDirs)?;

        let flush_interval_messages = env::var("KAAS_FLUSH_INTERVAL_MESSAGES")
            .ok()
            .map(|s| s.parse::<i64>())
            .transpose()
            .map_err(ConfigError::FlushInterval)?
            .unwrap_or(1);

        // Seconds, not a duration string: this is a coarse sweep
        // cadence and the chart emits a plain integer.
        let retention_check_interval_secs = env::var("KAAS_RETENTION_CHECK_INTERVAL")
            .ok()
            .map(|s| s.parse::<u64>())
            .transpose()
            .map_err(ConfigError::RetentionCheckInterval)?
            .unwrap_or(300);

        let cluster_id = env::var("KAAS_CLUSTER_ID").unwrap_or_else(|_| "kaas-rust-dev".to_owned());

        // Explicit KAAS_BROKER_ID wins; otherwise derive the
        // StatefulSet ordinal from MY_POD_NAME ("kaas-2" → 2).
        // A StatefulSet can't template
        // per-pod env, so without this every replica boots as
        // broker 0 and the whole cluster elects/renews under one
        // identity. Dev mode (neither var set) stays broker 0.
        let broker_id = match env::var("KAAS_BROKER_ID").ok().filter(|s| !s.is_empty()) {
            Some(s) => s.parse::<i32>().map_err(ConfigError::BrokerId)?,
            None => env::var("MY_POD_NAME")
                .ok()
                .filter(|s| !s.is_empty())
                .and_then(|pod| pod.rsplit('-').next()?.parse::<i32>().ok())
                .unwrap_or(0),
        };

        let topics_seed = env::var("KAAS_TOPICS").unwrap_or_default();
        // gh #254: the chart's observability.logs.level reaches the
        // broker as KAAS_LOG_LEVEL (the operator already honours it);
        // an explicit RUST_LOG still wins for ad-hoc debugging.
        let log_level = env::var("RUST_LOG")
            .or_else(|_| env::var("KAAS_LOG_LEVEL"))
            .ok()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "info".to_owned());

        let auth_disabled = parse_bool_env("KAAS_AUTH_DISABLED", false, &mut config_warnings);
        let authorization_type = env::var("KAAS_AUTHORIZATION_TYPE").unwrap_or_default();
        let super_users = env::var("KAAS_SUPER_USERS")
            .unwrap_or_default()
            .split(',')
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        let ssl_principal_mapping_rules =
            env::var("KAAS_SSL_PRINCIPAL_MAPPING_RULES").unwrap_or_default();

        // Apache's defaults: auto.create.topics.enable=true,
        // num.partitions=1. A non-positive or unparseable partition
        // count degrades to 1 rather than failing boot — the chart
        // always sets it, so a bad value is an operator typo and a
        // dead broker is the worse outcome — but it is reported.
        let auto_create_topics =
            parse_bool_env("KAAS_AUTO_CREATE_TOPICS_ENABLE", true, &mut config_warnings);
        let num_partitions = parse_num_partitions(
            env::var("KAAS_NUM_PARTITIONS").ok().as_deref(),
            &mut config_warnings,
        );

        let require_sasl = parse_bool_env("KAAS_REQUIRE_SASL", false, &mut config_warnings);

        // Unlike num_partitions above, a bad value here fails boot:
        // these three change durability/limit semantics, and silently
        // running with the default when the operator asked for
        // something else is the gh #251/#255 dead-knob class of bug.
        let txn_num_slots = env::var("KAAS_TXN_NUM_SLOTS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse::<usize>())
            .transpose()
            .map_err(ConfigError::TxnNumSlots)?
            .unwrap_or(0);

        let max_message_bytes = env::var("KAAS_MAX_MESSAGE_BYTES")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse::<usize>())
            .transpose()
            .map_err(ConfigError::MaxMessageBytes)?
            .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES);

        let fsync_max_latency_ms = env::var("KAAS_FSYNC_MAX_LATENCY_MS")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(|s| s.trim().parse::<u64>())
            .transpose()
            .map_err(ConfigError::FsyncMaxLatency)?
            .unwrap_or(30_000);

        Ok(Self {
            config_warnings,
            listeners,
            data_dir,
            cluster_dir,
            log_dirs,
            flush_interval_messages,
            retention_check_interval_secs,
            cluster_id,
            broker_id,
            topics_seed,
            log_level,
            auth_disabled,
            authorization_type,
            super_users,
            ssl_principal_mapping_rules,
            auto_create_topics,
            num_partitions,
            require_sasl,
            txn_num_slots,
            max_message_bytes,
            fsync_max_latency_ms,
        })
    }

    /// The effective cluster-state directory: `KAAS_CLUSTER_DIR` when
    /// set, else the legacy `<data_dir>/__cluster`. `None` only in
    /// dev mode (no data dir either) — callers fall back to their
    /// tmp-dir shims there.
    pub fn resolved_cluster_dir(&self) -> Option<PathBuf> {
        self.cluster_dir
            .clone()
            .or_else(|| self.data_dir.as_ref().map(|d| d.join("__cluster")))
    }
}

/// Apache's `message.max.bytes` default: 1 MiB of records plus the
/// record-batch header overhead. The produce handler reads it back
/// through [`Cli::max_message_bytes`].
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_048_588;

/// `default` when unset or empty. A value that is neither true-ish nor
/// false-ish is reported in `warnings`, naming the value used, and also
/// yields `default`: a typo like `flase` used to read as `false`
/// silently (gh #281).
/// `KAAS_NUM_PARTITIONS`: unset is Apache's default, 1. A non-positive
/// or unparseable value also yields 1, and is reported.
fn parse_num_partitions(raw: Option<&str>, warnings: &mut Vec<String>) -> i32 {
    let Some(s) = raw else { return 1 };
    match s.trim().parse::<i32>() {
        Ok(n) if n > 0 => n,
        _ => {
            warnings.push(format!(
                "KAAS_NUM_PARTITIONS={s:?} is not a positive integer; using 1"
            ));
            1
        }
    }
}

fn parse_bool_env(name: &str, default: bool, warnings: &mut Vec<String>) -> bool {
    let Ok(raw) = env::var(name) else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" => default,
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => {
            warnings.push(format!(
                "{name}={raw:?} is not a boolean (true/false, 1/0, yes/no, on/off); using the default, {default}"
            ));
            default
        }
    }
}

fn default_listeners() -> &'static str {
    r#"[{"name":"internal","addr":"0.0.0.0:9092"}]"#
}

/// Build a StatefulSet-shaped FQDN
/// `<MY_POD_NAME>.<KAAS_HEADLESS_SVC>.<KAAS_NAMESPACE>.svc.cluster.local`
/// when all three env vars are set. Returns `None` in local-dev
/// mode so listeners fall back to their bind IP.
fn derive_advertised_host() -> Option<String> {
    let pod = env::var("MY_POD_NAME").ok().filter(|s| !s.is_empty())?;
    let svc = env::var("KAAS_HEADLESS_SVC")
        .ok()
        .filter(|s| !s.is_empty())?;
    let ns = env::var("KAAS_NAMESPACE").ok().filter(|s| !s.is_empty())?;
    Some(format!("{pod}.{svc}.{ns}.svc.cluster.local"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_listener_parses() {
        let v: Vec<ListenerEntry> = serde_json::from_str(default_listeners()).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].name, "internal");
        assert!(v[0].advertised_host.is_none());
        assert!(v[0].tls.is_none());
        assert!(v[0].authentication_type.is_none());
    }

    #[test]
    fn json_with_advertised_host_round_trips() {
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{"name":"x","addr":"0.0.0.0:9094","advertised_host":"broker-0.cluster.local"}]"#,
        )
        .unwrap();
        assert_eq!(
            v[0].advertised_host.as_deref(),
            Some("broker-0.cluster.local")
        );
    }

    #[test]
    fn chart_shape_parses() {
        // Exact JSON the Helm chart's kaas.listenersJSON helper
        // emits (deploy/helm/kaas/templates/_helpers.tpl).
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[
                {"name":"plain","port":9092,"type":"internal","tls":false,"authentication":{"type":"none"}},
                {"name":"authed","port":9095,"type":"internal","tls":false,"authentication":{"type":"scram-sha-512"}}
            ]"#,
        )
        .unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0].name, "plain");
        assert_eq!(v[0].addr, "0.0.0.0:9092");
        assert!(v[0].tls.is_none());
        assert!(v[0].authentication_type.is_none()); // "none" folds to None
        assert_eq!(v[1].addr, "0.0.0.0:9095");
        assert_eq!(v[1].authentication_type.as_deref(), Some("scram-sha-512"));
    }

    #[test]
    fn oauth_listener_parses_with_strimzi_field_names() {
        std::env::set_var("KAAS_TLS_CERT_FILE", "/tls/tls.crt");
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{
                "name":"oauth","port":9096,"type":"internal","tls":true,
                "authentication":{
                    "type":"oauth",
                    "validIssuerUri":"https://login.microsoftonline.com/tenant/v2.0",
                    "jwksEndpointUri":"https://login.microsoftonline.com/tenant/discovery/v2.0/keys",
                    "userNameClaim":"sub",
                    "maxSecondsWithoutReauthentication":3600
                }
            }]"#,
        )
        .unwrap();
        assert_eq!(v[0].authentication_type.as_deref(), Some("oauth"));
        let o = v[0].oauth.as_ref().unwrap();
        assert_eq!(
            o.valid_issuer_uri,
            "https://login.microsoftonline.com/tenant/v2.0"
        );
        assert_eq!(
            o.jwks_endpoint_uri,
            "https://login.microsoftonline.com/tenant/discovery/v2.0/keys"
        );
        assert_eq!(o.user_name_claim.as_deref(), Some("sub"));
        assert_eq!(o.jwks_refresh_seconds, 300, "default applies");
        assert_eq!(o.max_seconds_without_reauthentication, Some(3600));
        assert!(!o.check_audience);
    }

    #[test]
    fn oauth_listener_missing_required_field_fails_parse() {
        // jwksEndpointUri absent → boot-time error naming the field,
        // not a silent fall-through to the anonymous engine.
        let err = serde_json::from_str::<Vec<ListenerEntry>>(
            r#"[{
                "name":"oauth","port":9096,"type":"internal","tls":true,
                "authentication":{"type":"oauth","validIssuerUri":"https://x/v2.0"}
            }]"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("jwksEndpointUri"), "{err}");
    }

    #[test]
    fn oauth_check_audience_requires_client_id() {
        let err = serde_json::from_str::<Vec<ListenerEntry>>(
            r#"[{
                "name":"oauth","port":9096,"type":"internal","tls":true,
                "authentication":{
                    "type":"oauth",
                    "validIssuerUri":"https://x/v2.0",
                    "jwksEndpointUri":"https://x/keys",
                    "checkAudience":true
                }
            }]"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("clientId"), "{err}");
    }

    #[test]
    fn non_oauth_listener_ignores_oauth_fields() {
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{"name":"authed","port":9095,"type":"internal","tls":false,
                 "authentication":{"type":"scram-sha-512"}}]"#,
        )
        .unwrap();
        assert!(v[0].oauth.is_none());
    }

    #[test]
    fn chart_shape_tls_true_resolves_from_env() {
        // Chart emits `tls: true` for the TLS listener; the
        // deserializer resolves cert paths from env-var overrides.
        std::env::set_var("KAAS_TLS_CERT_FILE", "/tls/tls.crt");
        std::env::set_var("KAAS_TLS_KEY_FILE", "/tls/tls.key");
        std::env::remove_var("KAAS_TLS_CLIENT_CA_FILE");

        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{"name":"tls","port":9093,"type":"internal","tls":true,"authentication":{"type":"none"}}]"#,
        )
        .unwrap();
        let tls = v[0].tls.as_ref().unwrap();
        assert_eq!(tls.cert_path, PathBuf::from("/tls/tls.crt"));
        assert_eq!(tls.key_path, PathBuf::from("/tls/tls.key"));
        assert!(tls.client_ca_path.is_none());
    }

    #[test]
    fn listener_with_tls_and_auth_parses() {
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[{
                "name": "authed",
                "addr": "0.0.0.0:9095",
                "tls": {
                    "certPath": "/etc/kaas/tls.crt",
                    "keyPath":  "/etc/kaas/tls.key",
                    "clientCaPath": "/etc/kaas/ca.crt"
                },
                "authenticationType": "mtls"
            }]"#,
        )
        .unwrap();
        let tls = v[0].tls.as_ref().unwrap();
        assert_eq!(tls.cert_path, PathBuf::from("/etc/kaas/tls.crt"));
        assert!(tls.client_ca_path.is_some());
        assert_eq!(v[0].authentication_type.as_deref(), Some("mtls"));
    }

    /// The exact shape the chart renders for a per-listener `clientCA`
    /// (`kaas.listenersJSON`): the mtls listener carries the object form
    /// of `tls` with its own clientCaPath, next to a SCRAM listener that
    /// keeps the boolean form. Client-cert verification is armed from
    /// clientCaPath per listener, so this is what keeps SCRAM/OAuth
    /// clients from being asked for a certificate.
    #[test]
    fn chart_per_listener_client_ca_shape_parses() {
        let v: Vec<ListenerEntry> = serde_json::from_str(
            r#"[
              {"authentication":{"type":"scram-sha-512"},"name":"scram","port":9095,"tls":true,"type":"internal"},
              {"authentication":{"type":"mtls"},"name":"mtls","port":9093,
               "tls":{"certPath":"/tls/tls.crt","clientCaPath":"/tls-client-ca/mtls/ca.crt","keyPath":"/tls/tls.key"},
               "type":"internal"}
            ]"#,
        )
        .unwrap();
        assert_eq!(v[0].authentication_type.as_deref(), Some("scram-sha-512"));
        assert!(v[0].tls.is_some());
        let mtls = v[1].tls.as_ref().unwrap();
        assert_eq!(v[1].authentication_type.as_deref(), Some("mtls"));
        assert_eq!(mtls.cert_path, PathBuf::from("/tls/tls.crt"));
        assert_eq!(
            mtls.client_ca_path.as_deref(),
            Some(std::path::Path::new("/tls-client-ca/mtls/ca.crt"))
        );
    }

    /// gh #292: `KAAS_NUM_PARTITIONS` unset is Apache's default, 1; a
    /// non-positive or unparseable value degrades to 1 and is reported.
    #[test]
    fn num_partitions_defaults_to_one_and_reports_bad_values() {
        let mut w = Vec::new();
        assert_eq!(parse_num_partitions(None, &mut w), 1);
        assert_eq!(parse_num_partitions(Some(" 6 "), &mut w), 6);
        assert!(w.is_empty(), "{w:?}");
        for bad in ["0", "-3", "six", ""] {
            let mut w = Vec::new();
            assert_eq!(parse_num_partitions(Some(bad), &mut w), 1, "{bad:?}");
            assert_eq!(w.len(), 1, "{bad:?} must be reported");
        }
    }

    /// gh #281: a value that is neither true-ish nor false-ish is
    /// reported and falls back to the caller's default, instead of
    /// silently reading as `false`. Each case uses its own variable name:
    /// the environment is process-global and tests run in parallel.
    #[test]
    fn parse_bool_env_reports_values_it_cannot_read() {
        let mut w = Vec::new();
        for (name, val, want) in [
            ("KAAS_TEST_GH281_TRUE", "On", true),
            ("KAAS_TEST_GH281_FALSE", " no ", false),
            ("KAAS_TEST_GH281_EMPTY", "", true),
        ] {
            std::env::set_var(name, val);
            assert_eq!(parse_bool_env(name, true, &mut w), want, "{name}={val:?}");
        }
        assert!(w.is_empty(), "{w:?}");
        assert!(!parse_bool_env("KAAS_TEST_GH281_UNSET", false, &mut w));

        std::env::set_var("KAAS_TEST_GH281_TYPO", "flase");
        assert!(parse_bool_env("KAAS_TEST_GH281_TYPO", true, &mut w));
        assert_eq!(w.len(), 1);
        assert!(w[0].ends_with("using the default, true"), "{w:?}");
        assert!(
            w[0].contains("KAAS_TEST_GH281_TYPO") && w[0].contains("flase"),
            "{w:?}"
        );
    }
}
