//! Multi-listener accept loop.
//!
//! Multi-listener server bring-up:
//! - one `tokio::task` per accepted connection,
//! - cancellation via [`CancellationToken`],
//! - per-listener TLS plumbing (`tls_config: Option<...>`) — wired
//!   but unused in Phase 3; Phase 4 lights up real cert loading.
//!
//! Three independent axes per listener: exposure (`addr`),
//! encryption (`tls_config`), and authentication (which the
//! dispatcher pulls from per-listener via the `connstate::listener_name`
//! tag in Phase 4). Strimzi-shape listener model.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use kaas_auth::engine::AuthEngine;
use kaas_auth::principal_mapping::PrincipalMapper;
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{rustls::ServerConfig, TlsAcceptor};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::connstate::ConnState;
use crate::dispatch::Dispatcher;
use crate::frame::{Connection, ProtoError};

/// Per-listener TLS principal extraction. When `Some`, the accept
/// loop pulls the peer's leaf cert after the TLS handshake and asks
/// the engine to resolve it; on success the principal lands on
/// `ConnState::principal` and `sasl_done = true` so the dispatcher's
/// pre-auth gate lets subsequent requests through.
#[derive(Clone)]
pub struct MtlsConfig {
    pub engine: Arc<dyn AuthEngine>,
    pub mapper: Arc<PrincipalMapper>,
}

impl std::fmt::Debug for MtlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtlsConfig").finish_non_exhaustive()
    }
}

#[derive(Debug)]
pub struct ListenerConfig {
    /// Free-form tag stamped onto every accepted connection's
    /// [`ConnState::listener_name`]. The Helm chart picks the strings
    /// (`internal`, `external`, `authed`, …); kaas has no
    /// predefined set.
    pub name: String,
    pub addr: SocketAddr,
    /// `Some(listener)` skips `TcpListener::bind` and wraps the
    /// pre-bound socket — tests use this to allocate `127.0.0.1:0`
    /// themselves and capture the assigned port.
    pub pre_bound: Option<TcpListener>,
    /// `Some(cfg)` wraps the bound listener with TLS.
    pub tls_config: Option<Arc<ServerConfig>>,
    /// `Some(...)` runs mTLS principal extraction after the TLS
    /// handshake. Requires `tls_config` to also be `Some` and the
    /// rustls `ServerConfig` to require a client certificate.
    pub mtls: Option<MtlsConfig>,
}

#[derive(Debug)]
pub struct ServerConfigBuilder {
    pub listeners: Vec<ListenerConfig>,
    pub max_frame_bytes: usize,
}

impl ServerConfigBuilder {
    pub fn new(listeners: Vec<ListenerConfig>) -> Self {
        Self {
            listeners,
            max_frame_bytes: kaas_codec::frame::MAX_FRAME_SIZE,
        }
    }
}

#[derive(Debug)]
pub struct Server {
    cfg: ServerConfigBuilder,
    dispatcher: Arc<Dispatcher>,
}

impl Server {
    pub fn new(cfg: ServerConfigBuilder, dispatcher: Arc<Dispatcher>) -> Self {
        Self { cfg, dispatcher }
    }

    /// Open every listener (returning early on first bind failure)
    /// and run accept loops until `cancel` fires. Returns the bound
    /// addresses for tests that need them.
    pub async fn serve(self, cancel: CancellationToken) -> io::Result<()> {
        let bound = self.bind_all().await?;
        let mut handles = Vec::new();
        for bl in bound {
            let dispatcher = self.dispatcher.clone();
            let cancel = cancel.clone();
            handles.push(tokio::spawn(accept_loop(
                bl.name,
                bl.listener,
                bl.max_frame_bytes,
                bl.tls,
                bl.mtls,
                dispatcher,
                cancel,
            )));
        }
        for h in handles {
            let _ = h.await;
        }
        Ok(())
    }

    /// Bind every listener and return the (name, bound listener,
    /// max_frame, tls) tuples. Useful for tests that need the actual
    /// `:0` port before driving traffic.
    pub async fn bind(self) -> io::Result<(BoundServer, Arc<Dispatcher>)> {
        let bound = self.bind_all().await?;
        Ok((BoundServer { listeners: bound }, self.dispatcher))
    }

    async fn bind_all(&self) -> io::Result<Vec<BoundListener>> {
        let mut out = Vec::with_capacity(self.cfg.listeners.len());
        for lc in &self.cfg.listeners {
            if lc.name.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "server: listener config missing name",
                ));
            }
            let ln = match &lc.pre_bound {
                Some(_) => {
                    // pre_bound is `&Option<TcpListener>` here — we
                    // can't move out of it. Pre-bind support is for
                    // tests, which call `bind_all` once; treat the
                    // pre_bound field as advisory and re-bind via
                    // the listener's `local_addr`.
                    TcpListener::bind(lc.addr).await?
                }
                None => TcpListener::bind(lc.addr).await?,
            };
            let tls = lc
                .tls_config
                .as_ref()
                .map(|cfg| TlsAcceptor::from(cfg.clone()));
            info!(
                addr = %ln.local_addr().map(|a| a.to_string()).unwrap_or_default(),
                listener = lc.name.as_str(),
                tls = tls.is_some(),
                mtls = lc.mtls.is_some(),
                "kaas-protocol listening",
            );
            out.push(BoundListener {
                name: lc.name.clone(),
                listener: ln,
                max_frame_bytes: self.cfg.max_frame_bytes,
                tls,
                mtls: lc.mtls.clone(),
            });
        }
        Ok(out)
    }
}

struct BoundListener {
    name: String,
    listener: TcpListener,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    mtls: Option<MtlsConfig>,
}

impl std::fmt::Debug for BoundListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundListener")
            .field("name", &self.name)
            .field("max_frame_bytes", &self.max_frame_bytes)
            .field("tls", &self.tls.is_some())
            .field("mtls", &self.mtls.is_some())
            .finish()
    }
}

pub struct BoundServer {
    listeners: Vec<BoundListener>,
}

impl std::fmt::Debug for BoundServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let names: Vec<&str> = self.listeners.iter().map(|bl| bl.name.as_str()).collect();
        f.debug_struct("BoundServer")
            .field("listeners", &names)
            .finish()
    }
}

impl BoundServer {
    /// `(listener_name, bound_addr)` pairs in registration order.
    /// Tests use this to discover the `:0` port the kernel assigned.
    /// Skips any listener whose `local_addr()` lookup fails (which
    /// only happens if the socket was already torn down).
    pub fn local_addrs(&self) -> Vec<(String, SocketAddr)> {
        self.listeners
            .iter()
            .filter_map(|bl| bl.listener.local_addr().ok().map(|a| (bl.name.clone(), a)))
            .collect()
    }

    /// Run accept loops until `cancel` fires. Consumes self.
    pub async fn serve(self, dispatcher: Arc<Dispatcher>, cancel: CancellationToken) {
        let mut handles = Vec::new();
        for bl in self.listeners {
            let d = dispatcher.clone();
            let c = cancel.clone();
            handles.push(tokio::spawn(accept_loop(
                bl.name,
                bl.listener,
                bl.max_frame_bytes,
                bl.tls,
                bl.mtls,
                d,
                c,
            )));
        }
        for h in handles {
            let _ = h.await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn accept_loop(
    listener_name: String,
    listener: TcpListener,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    mtls: Option<MtlsConfig>,
    dispatcher: Arc<Dispatcher>,
    cancel: CancellationToken,
) {
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => {
                info!(listener = listener_name.as_str(), "accept loop cancelled");
                return;
            }
            res = listener.accept() => {
                match res {
                    Ok((stream, peer)) => {
                        // TCP_NODELAY on every accepted conn
                        // (gh #188).
                        // Kafka responses are tiny (a produce ack is
                        // ~50 bytes); with Nagle on, each one can sit
                        // in the kernel for up to ~40 ms against the
                        // client's delayed ACK, capping every
                        // connection's pipeline at
                        // in_flight × request_size / inflated_RTT.
                        // Found as phase 9's −24 % Strimzi-relative
                        // throughput gap vs the v0.1 flavor: broker CPU
                        // idle, in-broker latency equal-or-better,
                        // client-observed RTT inflated (gh #188).
                        if let Err(err) = stream.set_nodelay(true) {
                            warn!(%peer, %err, "set_nodelay failed");
                        }
                        let d = dispatcher.clone();
                        let c = cancel.clone();
                        let name = listener_name.clone();
                        let tls = tls.clone();
                        let mtls = mtls.clone();
                        let m = kaas_observability::metrics::global();
                        let label = [kaas_observability::KeyValue::new("listener", name.clone())];
                        m.connections.add(1, &label);
                        m.connections_open.add(1, &label);
                        tokio::spawn(async move {
                            let res = serve_conn(name.clone(), stream, peer, max_frame_bytes, tls, mtls, d, c).await;
                            kaas_observability::metrics::global()
                                .connections_open
                                .add(-1, &[kaas_observability::KeyValue::new("listener", name)]);
                            if let Err(err) = res {
                                tracing::debug!(%peer, %err, "connection closed with error");
                            }
                        });
                    }
                    Err(err) => {
                        warn!(listener = listener_name.as_str(), %err, "accept failed; backing off 10ms");
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn serve_conn(
    listener_name: String,
    stream: TcpStream,
    peer: SocketAddr,
    max_frame_bytes: usize,
    tls: Option<TlsAcceptor>,
    mtls: Option<MtlsConfig>,
    dispatcher: Arc<Dispatcher>,
    cancel: CancellationToken,
) -> Result<(), ProtoError> {
    let mut cs = ConnState::new(&listener_name, peer);
    cs.is_tls = tls.is_some();
    let state = Arc::new(parking_lot::Mutex::new(cs));
    if let Some(acceptor) = tls {
        let tls_stream = acceptor.accept(stream).await.map_err(ProtoError::Io)?;
        kaas_observability::metrics::global().tls_handshakes.add(
            1,
            &[kaas_observability::KeyValue::new(
                "listener",
                listener_name.clone(),
            )],
        );
        // Run mTLS principal extraction if configured. Failure
        // doesn't drop the connection — the dispatcher's pre-auth
        // gate will reject non-pre-SASL APIs until the client
        // completes a SASL handshake instead.
        if let Some(mtls_cfg) = mtls.as_ref() {
            if let Err(err) = extract_and_stamp_mtls(&tls_stream, mtls_cfg, &state) {
                warn!(
                    listener = listener_name.as_str(),
                    %peer,
                    %err,
                    "mtls: principal extraction failed; client must complete SASL"
                );
            }
        }
        let conn = Connection::with_max_frame(tls_stream, max_frame_bytes);
        request_loop(conn, dispatcher, state, cancel).await
    } else {
        let conn = Connection::with_max_frame(stream, max_frame_bytes);
        request_loop(conn, dispatcher, state, cancel).await
    }
}

fn extract_and_stamp_mtls(
    tls_stream: &tokio_rustls::server::TlsStream<TcpStream>,
    cfg: &MtlsConfig,
    state: &Arc<parking_lot::Mutex<ConnState>>,
) -> Result<(), kaas_auth::AuthError> {
    let (_, session) = tls_stream.get_ref();
    let cert = session
        .peer_certificates()
        .and_then(|chain| chain.first())
        .ok_or(kaas_auth::AuthError::BadCertificate)?;
    let principal = kaas_auth::mtls::extract_principal(cert.as_ref(), &cfg.mapper, &*cfg.engine)?;
    let mut cs = state.lock();
    cs.principal = Some(principal);
    cs.sasl_done = true;
    Ok(())
}

async fn request_loop<S>(
    mut conn: Connection<S>,
    dispatcher: Arc<Dispatcher>,
    state: Arc<parking_lot::Mutex<ConnState>>,
    cancel: CancellationToken,
) -> Result<(), ProtoError>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Ok(()),
            req = conn.read_request() => {
                let (hdr, body) = match req {
                    Ok(r) => r,
                    Err(ProtoError::Frame(kaas_codec::frame::FrameError::Disconnected)) => return Ok(()),
                    Err(e) => {
                        error!(%e, "request decode failed; closing connection");
                        return Err(e);
                    }
                };
                let correlation_id = hdr.correlation_id;
                let (body, hv) = dispatcher.dispatch(state.as_ref(), hdr, body).await;
                if let Err(e) = conn.write_response(correlation_id, &body, hv).await {
                    error!(%e, "response write failed; closing connection");
                    return Err(e);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::{Bytes, BytesMut};
    use kaas_codec::api::api_versions;
    use kaas_codec::headers::encode_request_header;
    use kaas_codec::primitives::{write_compact_string, write_i32};
    use kaas_codec::tagged;
    use kaas_codec::{HeaderVersion, RequestHeader};

    use crate::dispatch::{Handler, HandlerError};

    struct ApiVersionsStub;
    #[async_trait]
    impl Handler for ApiVersionsStub {
        async fn handle(
            &self,
            _: &parking_lot::Mutex<ConnState>,
            version: i16,
            _body: Bytes,
        ) -> Result<BytesMut, HandlerError> {
            let resp = api_versions::response_from_registry(0);
            let mut out = BytesMut::new();
            api_versions::encode_response(&mut out, &resp, version)?;
            Ok(out)
        }
    }

    fn build_request_frame(api_key: i16, api_version: i16, correlation_id: i32) -> Vec<u8> {
        let mut body = BytesMut::new();
        encode_request_header(
            &mut body,
            &RequestHeader {
                api_key,
                api_version,
                correlation_id,
                client_id: Some("smoke".to_owned()),
            },
            HeaderVersion::V2,
        )
        .unwrap();
        // ApiVersions v3 body: client_software_name + version + tagged.
        write_compact_string(&mut body, "smoke-test").unwrap();
        write_compact_string(&mut body, "1.0.0").unwrap();
        tagged::write_empty(&mut body);

        let mut out = Vec::with_capacity(body.len() + 4);
        out.extend_from_slice(&i32::try_from(body.len()).unwrap().to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[tokio::test]
    async fn server_end_to_end_api_versions_roundtrip() {
        let mut d = Dispatcher::new();
        d.register(18, 0, 4, Arc::new(ApiVersionsStub));
        let dispatcher = Arc::new(d);

        let cfg = ServerConfigBuilder::new(vec![ListenerConfig {
            name: "internal".to_owned(),
            addr: "127.0.0.1:0".parse().unwrap(),
            pre_bound: None,
            tls_config: None,
            mtls: None,
        }]);

        let server = Server::new(cfg, dispatcher.clone());
        let (bound, dispatcher) = server.bind().await.unwrap();
        let addrs = bound.local_addrs();
        let port = addrs[0].1.port();

        let cancel = CancellationToken::new();
        let serve_cancel = cancel.clone();
        let serve = tokio::spawn(async move { bound.serve(dispatcher, serve_cancel).await });

        // Client: open TCP, send one ApiVersions v3 request, read response.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut sock = tokio::net::TcpStream::connect(format!("127.0.0.1:{port}"))
            .await
            .unwrap();
        let frame = build_request_frame(18, 3, 1234);
        sock.write_all(&frame).await.unwrap();
        sock.flush().await.unwrap();

        // Read [size:i32][response].
        let mut sz = [0u8; 4];
        sock.read_exact(&mut sz).await.unwrap();
        let n = usize::try_from(i32::from_be_bytes(sz)).expect("non-negative size");
        let mut resp = vec![0u8; n];
        sock.read_exact(&mut resp).await.unwrap();
        // First 4 bytes = correlation id; for ApiVersions header is V0
        // (no tagged-fields block).
        assert_eq!(&resp[..4], &1234i32.to_be_bytes());
        // After the correlation id: error_code=0 then sorted api list.
        assert_eq!(&resp[4..6], &0i16.to_be_bytes(), "error_code = 0");

        drop(sock);
        cancel.cancel();
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), serve).await;
    }

    fn _silence_unused() {
        let _ = write_i32;
    }
}
