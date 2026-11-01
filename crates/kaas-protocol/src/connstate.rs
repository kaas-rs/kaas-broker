//! Per-connection mutable state.
//!
//! Carries authentication progress (`principal`, `sasl_done`,
//! `sasl_state`), the picked SASL mechanism from a prior
//! `SaslHandshake`, the per-listener tag the dispatcher's pre-auth
//! gate reads, and a TLS flag for the PLAIN-over-plain check.
//!
//! Lives behind `Arc<Mutex<ConnState>>` so handlers can mutate
//! `sasl_done` mid-connection without re-threading the state through
//! the dispatcher signature.

use std::net::SocketAddr;

use kaas_auth::engine::SaslExchange;
pub use kaas_auth::Principal;

#[derive(Debug)]
pub struct ConnState {
    /// Free-form listener tag from `ListenerConfig::name`. The
    /// metadata handler keys per-listener port lookups on this
    /// string (Phase 5 gh #125; the dispatcher's pre-auth gate in
    /// Phase 4 uses it to look up the per-listener engine).
    pub listener_name: String,
    pub peer_addr: SocketAddr,
    /// `true` if the connection was accepted on a TLS listener.
    /// SASL PLAIN over a non-TLS connection is rejected by the
    /// `SaslAuthenticate` handler with `NETWORK_EXCEPTION` (13).
    pub is_tls: bool,
    /// Authenticated principal. `None` until SASL/mTLS completes;
    /// anonymous listeners may stamp `Some(Principal::anonymous())`
    /// at connection time.
    pub principal: Option<Principal>,
    /// `true` after a successful `SaslAuthenticate` (or after the
    /// mTLS handshake on a mTLS listener).
    pub sasl_done: bool,
    /// SASL mechanism picked by the prior `SaslHandshake`. The
    /// `SaslAuthenticate` handler reads this to instantiate the
    /// right exchange on the first authenticate call.
    pub sasl_mechanism: Option<String>,
    /// Per-connection SASL exchange that survives across
    /// `SaslAuthenticate` calls. Lives here so a multi-step
    /// mechanism (SCRAM) doesn't need a side-channel state map.
    pub sasl_state: Option<Box<dyn SaslExchange>>,
    /// KIP-368 session deadline. `Some` when the completed SASL
    /// exchange advertised a bounded `session_lifetime_ms`
    /// (OAUTHBEARER with `maxSecondsWithoutReauthentication`); past
    /// it the dispatcher serves only the SASL APIs until the client
    /// re-authenticates. `None` = unbounded session.
    pub session_deadline: Option<std::time::Instant>,
    /// What the request loop does with the response of the request being
    /// handled. A handler sets it; the loop takes it (resetting it to
    /// [`Reply::Send`]) after every dispatch.
    pub reply: Reply,
}

/// How the request loop answers one request (gh #200).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Reply {
    /// Write the response. Every API but `acks=0` Produce.
    #[default]
    Send,
    /// Write nothing: the client doesn't expect a response. Apache's
    /// `acks=0` Produce, when every partition succeeded. A response the
    /// Java client isn't waiting for gets matched against its next
    /// in-flight request and fails the correlation-id check.
    None,
    /// Write nothing and close the connection. Apache's `acks=0` Produce
    /// with any partition error: the disconnect is the only way to tell
    /// the producer, which then refreshes its metadata.
    Close,
}

impl ConnState {
    pub fn new(listener_name: impl Into<String>, peer_addr: SocketAddr) -> Self {
        Self {
            listener_name: listener_name.into(),
            peer_addr,
            is_tls: false,
            principal: None,
            sasl_done: false,
            sasl_mechanism: None,
            sasl_state: None,
            session_deadline: None,
            reply: Reply::Send,
        }
    }
}
