//! API-key router.
//!
//! Routes an incoming `(header, body)` pair to the right handler.
//! Contract: unsupported key →
//! `UNSUPPORTED_VERSION` (35); version out-of-range → also 35, except
//! for key 18 (`ApiVersions`) which clamps to the broker's max so old
//! clients can still negotiate.
//!
//! Phase 3 pre-auth gate is stubbed open — Phase 4 wires
//! `AuthEngineSelector` and the pre-SASL key allowlist.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::selector::AuthEngineSelector;
use kaas_codec::api::registry;
use kaas_codec::headers::HeaderVersion;
use kaas_codec::primitives::write_i16;
use kaas_codec::RequestHeader;
use parking_lot::Mutex;
use thiserror::Error;

use crate::connstate::ConnState;

/// Apache wire error code for "unsupported API version".
pub const ERR_UNSUPPORTED_VERSION: i16 = 35;

/// Apache wire error code for "cluster authorization failed". The
/// dispatcher's pre-auth gate returns this when the connection hasn't
/// completed SASL and the requested API is not in the pre-SASL
/// allowlist.
pub const ERR_CLUSTER_AUTHORIZATION_FAILED: i16 = 31;
/// KIP-368: what an expired session answers with until the client
/// re-authenticates.
pub const ERR_SASL_AUTHENTICATION_FAILED: i16 = 58;

/// API keys allowed before SASL completes — handshake (17),
/// ApiVersions (18), and authenticate (36).
pub const PRE_AUTH_KEYS: &[i16] = &[17, 18, 36];

pub fn is_pre_auth(api_key: i16) -> bool {
    PRE_AUTH_KEYS.contains(&api_key)
}

#[derive(Debug, Error)]
pub enum HandlerError {
    #[error(transparent)]
    Codec(#[from] kaas_codec::CodecError),
    #[error(transparent)]
    Storage(#[from] kaas_storage::StorageError),
    #[error("handler: {0}")]
    Other(String),
}

#[async_trait]
pub trait Handler: Send + Sync + 'static {
    /// Decode the request body, do the work, return the encoded
    /// response body (NOT including the response header). The
    /// dispatcher prepends the correlation_id + maybe tagged-fields
    /// block via [`Connection::write_response`].
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError>;
}

struct HandlerSlot {
    handler: Arc<dyn Handler>,
    min: i16,
    max: i16,
}

/// Vec-of-Option indexed by API key. Apache reserves 0–~70; sizing to
/// 96 leaves headroom without forcing a HashMap allocation on the hot
/// path.
pub struct Dispatcher {
    slots: Vec<Option<HandlerSlot>>,
    /// Per-listener `AuthEngine` lookup. `None` ↔ pre-auth gate is
    /// open (dev/test). The gate consults this on every request,
    /// asking the listener's engine whether it requires SASL before
    /// non-pre-auth APIs are allowed through.
    engines: Option<Arc<dyn AuthEngineSelector>>,
}

impl std::fmt::Debug for Dispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let registered: Vec<usize> = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|_| i))
            .collect();
        f.debug_struct("Dispatcher")
            .field("registered_keys", &registered)
            .field("engines_wired", &self.engines.is_some())
            .finish()
    }
}

const SLOT_COUNT: usize = 96;

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl Dispatcher {
    pub fn new() -> Self {
        let mut slots = Vec::with_capacity(SLOT_COUNT);
        for _ in 0..SLOT_COUNT {
            slots.push(None);
        }
        Self {
            slots,
            engines: None,
        }
    }

    /// Wire the per-listener auth selector. Call BEFORE serving
    /// requests; the gate reads the field on the hot path so a
    /// nil-to-non-nil transition mid-flight would race.
    pub fn with_auth(mut self, engines: Arc<dyn AuthEngineSelector>) -> Self {
        self.engines = Some(engines);
        self
    }

    pub fn set_auth(&mut self, engines: Arc<dyn AuthEngineSelector>) {
        self.engines = Some(engines);
    }

    pub fn register(&mut self, api_key: i16, min: i16, max: i16, handler: Arc<dyn Handler>) {
        // `usize::MAX` on Err is a sentinel that the assert below
        // catches together with the "≥ SLOT_COUNT" case.
        let idx = usize::try_from(api_key).unwrap_or(usize::MAX);
        assert!(
            idx < SLOT_COUNT,
            "Dispatcher::register: api_key out of range: {api_key} (slot count {SLOT_COUNT})"
        );
        assert!(min <= max, "registering api_key {api_key} with min > max");
        self.slots[idx] = Some(HandlerSlot { handler, min, max });
    }

    /// Dispatch one request. Always returns `(body, header_version)` —
    /// errors land in the body as a wire error_code (the
    /// error-response pattern). Truly fatal conditions (panic, etc.)
    /// are the connection task's problem, not ours.
    pub async fn dispatch(
        &self,
        conn: &Mutex<ConnState>,
        header: RequestHeader,
        body: Bytes,
    ) -> (BytesMut, HeaderVersion) {
        let started = std::time::Instant::now();
        let api_key = header.api_key;
        let spec = registry::lookup(api_key);
        let out = self.dispatch_inner(conn, header, body, spec, api_key).await;
        // gh #217: a completed dispatch proves the main runtime is still
        // scheduling tasks, so refresh the liveness timestamp here — a
        // broker saturated with produce/fetch work stays `healthy`
        // *because* it is serving, not in spite of it. Only a true wedge
        // (no completions here AND the 1 s idle tick starved) goes stale.
        // Cost is one atomic store per request; negligible on the hot path.
        kaas_observability::record_main_tick();
        // Label by numeric api_key to cap cardinality — the API name
        // is one lookup away in dashboards. Cross-hairs with the
        // `request.latency` histogram (workspace metric name).
        kaas_observability::metrics::global()
            .request_latency
            .record(
                started.elapsed().as_secs_f64(),
                &[kaas_observability::KeyValue::new(
                    "api_key",
                    i64::from(api_key),
                )],
            );
        out
    }

    async fn dispatch_inner(
        &self,
        conn: &Mutex<ConnState>,
        header: RequestHeader,
        body: Bytes,
        spec: Option<&'static registry::ApiSpec>,
        api_key: i16,
    ) -> (BytesMut, HeaderVersion) {
        // Pre-auth gate (gh #124). When `engines` is wired and the
        // listener's engine requires pre-auth, every non-pre-auth API
        // is rejected until SASL completes. mTLS sets sasl_done=true
        // at handshake time so the same gate works for cert clients.
        if let Some(sel) = self.engines.as_ref() {
            let (listener_name, sasl_done, session_expired) = {
                let cs = conn.lock();
                (
                    cs.listener_name.clone(),
                    cs.sasl_done,
                    cs.session_deadline
                        .is_some_and(|d| std::time::Instant::now() >= d),
                )
            };
            let eng = sel.for_listener(&listener_name);
            if eng.requires_pre_auth() && !sasl_done && !is_pre_auth(api_key) {
                return error_body(spec, header.api_version, ERR_CLUSTER_AUTHORIZATION_FAILED);
            }
            // KIP-368: past the advertised re-auth deadline only the
            // SASL/ApiVersions APIs are served — the client owes a
            // fresh token before anything else goes through.
            if session_expired && !is_pre_auth(api_key) {
                return error_body(spec, header.api_version, ERR_SASL_AUTHENTICATION_FAILED);
            }
        }

        let slot_idx = match usize::try_from(api_key) {
            Ok(i) if i < SLOT_COUNT => i,
            _ => return error_body(spec, header.api_version, ERR_UNSUPPORTED_VERSION),
        };
        let Some(slot) = self.slots[slot_idx].as_ref() else {
            return error_body(spec, header.api_version, ERR_UNSUPPORTED_VERSION);
        };

        // Version negotiation. ApiVersions (key 18) is the documented
        // exception: when the client sends an unknown version, we
        // respond using OUR max version so the client can still
        // discover the supported range.
        let version = if header.api_version < slot.min || header.api_version > slot.max {
            if api_key == 18 {
                slot.max
            } else {
                return error_body(spec, header.api_version, ERR_UNSUPPORTED_VERSION);
            }
        } else {
            header.api_version
        };

        match slot.handler.handle(conn, version, body).await {
            Ok(resp_body) => {
                let hv = response_header_version(spec, version);
                (resp_body, hv)
            }
            Err(err) => {
                tracing::warn!(api_key, api_version = header.api_version, %err, "handler error");
                // Handler-side failure: give the client back an
                // UNSUPPORTED_VERSION sentinel. Cleaner-grained
                // mapping (per-API error codes) is the handler's
                // responsibility — they should encode the right error
                // shape themselves and return Ok.
                error_body(spec, header.api_version, ERR_UNSUPPORTED_VERSION)
            }
        }
    }
}

fn response_header_version(
    spec: Option<&'static registry::ApiSpec>,
    version: i16,
) -> HeaderVersion {
    spec.map(|s| s.response_header(version))
        .unwrap_or(HeaderVersion::V0)
}

fn error_body(
    spec: Option<&'static registry::ApiSpec>,
    api_version: i16,
    error_code: i16,
) -> (BytesMut, HeaderVersion) {
    // Error response: just the error_code as the body. The
    // dispatcher's caller prepends the correlation_id + maybe
    // tagged-fields. The header version is derived from the spec
    // (or V0 if the key was unknown).
    let hv = response_header_version(spec, api_version);
    let mut body = BytesMut::with_capacity(2);
    write_i16(&mut body, error_code);
    (body, hv)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::str::FromStr;

    struct EchoHandler;

    #[async_trait]
    impl Handler for EchoHandler {
        async fn handle(
            &self,
            _conn: &Mutex<ConnState>,
            version: i16,
            body: Bytes,
        ) -> Result<BytesMut, HandlerError> {
            // Body = `[version:i16][body bytes]`.
            let mut out = BytesMut::with_capacity(2 + body.len());
            write_i16(&mut out, version);
            out.extend_from_slice(&body);
            Ok(out)
        }
    }

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn header(api_key: i16, api_version: i16) -> RequestHeader {
        RequestHeader {
            api_key,
            api_version,
            correlation_id: 1,
            client_id: None,
        }
    }

    #[tokio::test]
    async fn dispatch_known_key_routes_to_handler() {
        let mut d = Dispatcher::new();
        d.register(18, 0, 4, Arc::new(EchoHandler));
        let (body, hv) = d
            .dispatch(&conn(), header(18, 3), Bytes::from_static(b"hello"))
            .await;
        // version 3 echoed, then payload.
        assert_eq!(&body[..2], &3i16.to_be_bytes());
        assert_eq!(&body[2..], b"hello");
        // ApiVersions response header is always V0 (the documented exception).
        assert!(matches!(hv, HeaderVersion::V0));
    }

    /// gh #272: a legacy (non-flexible) version gets the bare v0
    /// response header; only flexible versions carry tagged fields.
    #[tokio::test]
    async fn legacy_versions_get_a_v0_response_header() {
        let mut d = Dispatcher::new();
        d.register(0, 3, 9, Arc::new(EchoHandler));
        let (_, legacy) = d.dispatch(&conn(), header(0, 8), Bytes::new()).await;
        assert_eq!(legacy, HeaderVersion::V0, "Produce v8 is not flexible");
        let (_, flexible) = d.dispatch(&conn(), header(0, 9), Bytes::new()).await;
        assert_eq!(flexible, HeaderVersion::V1, "Produce v9 is flexible");
    }

    #[tokio::test]
    async fn dispatch_refreshes_main_liveness() {
        // gh #217: a completed dispatch must refresh the main-runtime
        // liveness signal, so a busy-but-serving runtime stays `healthy`
        // instead of being falsely evicted under produce load. Even the
        // fast unknown-key error path counts as progress.
        let d = Dispatcher::new();
        let _ = d.dispatch(&conn(), header(99, 0), Bytes::new()).await;
        assert!(
            kaas_observability::main_alive(),
            "dispatch should refresh main-runtime liveness"
        );
    }

    #[tokio::test]
    async fn dispatch_unknown_key_returns_unsupported_version() {
        let d = Dispatcher::new();
        let (body, _) = d.dispatch(&conn(), header(99, 0), Bytes::new()).await;
        assert_eq!(body.len(), 2);
        assert_eq!(&body[..], &ERR_UNSUPPORTED_VERSION.to_be_bytes());
    }

    #[tokio::test]
    async fn dispatch_out_of_range_version_returns_unsupported_version_for_non_api_versions() {
        let mut d = Dispatcher::new();
        // Registered Produce key 0 with range 3..=9; ask for v0.
        d.register(0, 3, 9, Arc::new(EchoHandler));
        let (body, _) = d.dispatch(&conn(), header(0, 0), Bytes::new()).await;
        assert_eq!(&body[..], &ERR_UNSUPPORTED_VERSION.to_be_bytes());
    }

    #[tokio::test]
    async fn dispatch_api_versions_clamps_unsupported_version_to_max() {
        let mut d = Dispatcher::new();
        d.register(18, 0, 4, Arc::new(EchoHandler));
        let (body, _) = d.dispatch(&conn(), header(18, 99), Bytes::new()).await;
        // Echoed version is the clamped max (4).
        assert_eq!(&body[..2], &4i16.to_be_bytes());
    }
}
