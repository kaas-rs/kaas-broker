//! Listener TLS that follows its files on disk (gh #343).
//!
//! cert-manager renews a certificate by rewriting its Secret, and the kubelet
//! swaps the mounted files in place. A listener that read them once at startup
//! would serve the old certificate until it restarted, and fail every handshake
//! once that one expired. [`TlsReloader`] polls a listener's files and swaps a
//! new [`ServerConfig`] into its [`ListenerTls`], which the accept loop reads
//! for each new connection. Connections already open keep the session they
//! negotiated.
//!
//! It polls rather than watching: a Secret volume updates through a `..data`
//! symlink swap, which a watch on the files themselves can miss, and the
//! kubelet takes a minute or so to update the volume anyway. It compares
//! content, not timestamps, and loads a change only once two polls in a row
//! read the same content, so a poll that lands between the certificate and the
//! key being replaced doesn't take the pair for a mismatch.
//!
//! New files that don't load (bad PEM, a key that doesn't match the
//! certificate, a missing file) are refused: the listener keeps serving what it
//! has, and says so once for each distinct content it refuses.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use kaas_observability::KeyValue;
use sha2::{Digest, Sha256};
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{RootCertStore, ServerConfig};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use x509_parser::prelude::{FromDer, X509Certificate};

/// Where a listener's TLS material lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsPaths {
    pub cert: PathBuf,
    pub key: PathBuf,
    /// `Some` requires a client certificate signed by one of the CAs in this
    /// file (mTLS).
    pub client_ca: Option<PathBuf>,
}

/// Why a listener's TLS files didn't load.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct TlsError(String);

/// A listener's TLS config, as of now. The accept loop takes
/// [`ListenerTls::current`] once per connection, so a swap reaches the next
/// handshake and leaves established connections alone.
#[derive(Debug, Clone)]
pub struct ListenerTls(Arc<ArcSwap<ServerConfig>>);

impl ListenerTls {
    pub fn current(&self) -> Arc<ServerConfig> {
        self.0.load_full()
    }

    fn replace(&self, cfg: Arc<ServerConfig>) {
        self.0.store(cfg);
    }
}

/// A fixed config, for a listener nothing reloads.
impl From<Arc<ServerConfig>> for ListenerTls {
    fn from(cfg: Arc<ServerConfig>) -> Self {
        Self(Arc::new(ArcSwap::new(cfg)))
    }
}

/// What one [`TlsReloader::poll`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// The files hold what's being served, or what was already refused.
    Unchanged,
    /// The files changed since the last poll. They're loaded if the next poll
    /// reads the same content.
    Settling,
    /// New connections get the new files' config.
    Reloaded,
    /// The new files didn't load, so the listener keeps serving the old ones.
    Refused,
}

/// Polls one listener's TLS files and keeps its [`ListenerTls`] current.
#[derive(Debug)]
pub struct TlsReloader {
    listener: String,
    paths: TlsPaths,
    tls: ListenerTls,
    /// Digest of the files being served.
    serving: [u8; 32],
    /// A change the last poll read, loaded if this poll reads it again.
    settling: Option<[u8; 32]>,
    /// The last content that failed to load, so it's reported once.
    refused: Option<[u8; 32]>,
}

impl TlsReloader {
    /// Load a listener's files. An error here fails startup.
    pub fn load(listener: &str, paths: TlsPaths) -> Result<Self, TlsError> {
        let read = read_files(&paths);
        let cfg = read.files.and_then(|f| build(&f, &paths))?;
        info!(
            listener,
            certificate = %describe(&paths),
            "tls: certificate loaded"
        );
        Ok(Self {
            listener: listener.to_owned(),
            paths,
            tls: ListenerTls::from(Arc::new(cfg)),
            serving: read.digest,
            settling: None,
            refused: None,
        })
    }

    /// The handle to give the listener.
    pub fn tls(&self) -> ListenerTls {
        self.tls.clone()
    }

    /// Read the files once and act on what changed.
    pub fn poll(&mut self) -> PollOutcome {
        let read = read_files(&self.paths);
        if read.digest == self.serving {
            self.settling = None;
            self.refused = None;
            return PollOutcome::Unchanged;
        }
        if Some(read.digest) == self.refused {
            self.settling = None;
            return PollOutcome::Unchanged;
        }
        if self.settling != Some(read.digest) {
            self.settling = Some(read.digest);
            return PollOutcome::Settling;
        }
        self.settling = None;

        let listener = self.listener.as_str();
        let outcome = match read.files.and_then(|f| build(&f, &self.paths)) {
            Ok(cfg) => {
                self.tls.replace(Arc::new(cfg));
                self.serving = read.digest;
                self.refused = None;
                info!(
                    listener,
                    certificate = %describe(&self.paths),
                    "tls: certificate files changed; new connections get the new certificate"
                );
                PollOutcome::Reloaded
            }
            Err(err) => {
                self.refused = Some(read.digest);
                warn!(
                    listener,
                    %err,
                    "tls: new certificate files don't load; still serving the previous certificate"
                );
                PollOutcome::Refused
            }
        };
        kaas_observability::metrics::global().cert_reloads.add(
            1,
            &[
                KeyValue::new("listener", self.listener.clone()),
                KeyValue::new(
                    "result",
                    if outcome == PollOutcome::Reloaded {
                        "ok"
                    } else {
                        "error"
                    },
                ),
            ],
        );
        outcome
    }

    /// Poll every `interval` until `cancel` fires. The files are a few
    /// kilobytes on a Secret's tmpfs, so they're read in place, as the
    /// credential reloader reads its files.
    pub async fn run(mut self, interval: Duration, cancel: CancellationToken) {
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                () = tokio::time::sleep(interval) => {}
            }
            self.poll();
        }
    }
}

struct Files {
    cert: Vec<u8>,
    key: Vec<u8>,
    client_ca: Option<Vec<u8>>,
}

/// The files as one poll read them, or why it couldn't, and a digest of that.
/// A read error gets a digest too, so a missing file is reported once rather
/// than on every poll.
struct Read {
    digest: [u8; 32],
    files: Result<Files, TlsError>,
}

fn read_files(paths: &TlsPaths) -> Read {
    let files = read_file(&paths.cert).and_then(|cert| {
        Ok(Files {
            cert,
            key: read_file(&paths.key)?,
            client_ca: paths.client_ca.as_deref().map(read_file).transpose()?,
        })
    });
    let mut h = Sha256::new();
    match &files {
        Ok(f) => {
            for part in [Some(&f.cert), Some(&f.key), f.client_ca.as_ref()] {
                match part {
                    Some(bytes) => {
                        h.update([1]);
                        h.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
                        h.update(bytes);
                    }
                    None => h.update([0]),
                }
            }
        }
        Err(err) => {
            h.update([2]);
            h.update(err.0.as_bytes());
        }
    }
    Read {
        digest: h.finalize().into(),
        files,
    }
}

fn read_file(path: &Path) -> Result<Vec<u8>, TlsError> {
    std::fs::read(path).map_err(|e| TlsError(format!("reading {}: {e}", path.display())))
}

fn build(files: &Files, paths: &TlsPaths) -> Result<ServerConfig, TlsError> {
    let certs = parse_certs(&files.cert, &paths.cert)?;
    let key = parse_key(&files.key, &paths.key)?;
    let builder = ServerConfig::builder();
    let builder = match (&files.client_ca, &paths.client_ca) {
        (Some(ca), Some(ca_path)) => {
            let mut roots = RootCertStore::empty();
            for cert in parse_certs(ca, ca_path)? {
                roots.add(cert).map_err(|e| {
                    TlsError(format!(
                        "installing client CA from {}: {e}",
                        ca_path.display()
                    ))
                })?;
            }
            let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
                .build()
                .map_err(|e| TlsError(format!("building client certificate verifier: {e}")))?;
            builder.with_client_cert_verifier(verifier)
        }
        _ => builder.with_no_client_auth(),
    };
    // Refuses a key that doesn't match the certificate.
    builder.with_single_cert(certs, key).map_err(|e| {
        TlsError(format!(
            "certificate {} with key {}: {e}",
            paths.cert.display(),
            paths.key.display()
        ))
    })
}

fn parse_certs(pem: &[u8], path: &Path) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs = rustls_pemfile::certs(&mut &pem[..])
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| TlsError(format!("parsing certificate in {}: {e}", path.display())))?;
    if certs.is_empty() {
        return Err(TlsError(format!(
            "no PEM certificates found in {}",
            path.display()
        )));
    }
    Ok(certs)
}

fn parse_key(pem: &[u8], path: &Path) -> Result<PrivateKeyDer<'static>, TlsError> {
    rustls_pemfile::private_key(&mut &pem[..])
        .map_err(|e| TlsError(format!("parsing private key in {}: {e}", path.display())))?
        .ok_or_else(|| TlsError(format!("no private key in {}", path.display())))
}

/// The served certificate's serial and expiry, for the log. Read again from
/// the file: it only runs after a load succeeded, so a failure here can only
/// mean the file changed in between, and the next poll picks that up.
fn describe(paths: &TlsPaths) -> String {
    let Ok(Ok(leaf)) = read_file(&paths.cert).map(|pem| parse_certs(&pem, &paths.cert)) else {
        return "unreadable".to_owned();
    };
    let Some(der) = leaf.first() else {
        return "unreadable".to_owned();
    };
    match X509Certificate::from_der(der.as_ref()) {
        Ok((_, cert)) => format!(
            "serial {}, expires {}",
            cert.raw_serial_as_string(),
            cert.validity().not_after
        ),
        Err(_) => "unparsable".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::symlink;

    use bytes::{Bytes, BytesMut};
    use kaas_codec::api::api_versions;
    use kaas_codec::headers::encode_request_header;
    use kaas_codec::primitives::write_compact_string;
    use kaas_codec::{tagged, HeaderVersion, RequestHeader};
    use rcgen::{BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::ClientConfig;
    use tokio_rustls::TlsConnector;

    use crate::dispatch::{Dispatcher, Handler, HandlerError};
    use crate::server::{ListenerConfig, Server, ServerConfigBuilder};

    fn provider() {
        let _ = tokio_rustls::rustls::crypto::ring::default_provider().install_default();
    }

    struct Ca {
        cert: rcgen::Certificate,
        key: KeyPair,
    }

    fn ca(name: &str) -> Ca {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, name);
        let cert = params.self_signed(&key).unwrap();
        Ca { cert, key }
    }

    /// A leaf signed by `ca`: (certificate PEM, key PEM, certificate DER).
    fn leaf(ca: &Ca, client: bool) -> (String, String, Vec<u8>) {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "leaf");
        if client {
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        }
        let cert = params.signed_by(&key, &ca.cert, &ca.key).unwrap();
        (cert.pem(), key.serialize_pem(), cert.der().to_vec())
    }

    /// A Secret volume as the kubelet lays it out: the files are symlinks
    /// through `..data`, which is itself a symlink to a timestamped directory,
    /// and an update writes a new directory and renames a new `..data` over
    /// the old one.
    struct SecretVolume {
        dir: tempfile::TempDir,
        generation: u32,
    }

    impl SecretVolume {
        fn new(files: &[(&str, &str)]) -> Self {
            let mut v = Self {
                dir: tempfile::tempdir().unwrap(),
                generation: 0,
            };
            v.update(files);
            for (name, _) in files {
                symlink(format!("..data/{name}"), v.dir.path().join(name)).unwrap();
            }
            v
        }

        fn update(&mut self, files: &[(&str, &str)]) {
            self.generation += 1;
            let data = format!("..2026_11_08_{:02}", self.generation);
            std::fs::create_dir(self.dir.path().join(&data)).unwrap();
            for (name, content) in files {
                std::fs::write(self.dir.path().join(&data).join(name), content).unwrap();
            }
            let tmp = self.dir.path().join("..data_tmp");
            symlink(&data, &tmp).unwrap();
            std::fs::rename(&tmp, self.dir.path().join("..data")).unwrap();
        }

        fn paths(&self, client_ca: bool) -> TlsPaths {
            TlsPaths {
                cert: self.dir.path().join("tls.crt"),
                key: self.dir.path().join("tls.key"),
                client_ca: client_ca.then(|| self.dir.path().join("ca.crt")),
            }
        }
    }

    struct ApiVersions;

    #[async_trait::async_trait]
    impl Handler for ApiVersions {
        async fn handle(
            &self,
            _: &parking_lot::Mutex<crate::ConnState>,
            version: i16,
            _: Bytes,
        ) -> Result<BytesMut, HandlerError> {
            let mut out = BytesMut::new();
            api_versions::encode_response(
                &mut out,
                &api_versions::response_from_registry(0),
                version,
            )?;
            Ok(out)
        }
    }

    /// A broker listener on loopback, through the real accept loop, answering
    /// ApiVersions.
    async fn listener(tls: ListenerTls) -> (std::net::SocketAddr, CancellationToken) {
        let mut d = Dispatcher::new();
        d.register(18, 0, 4, Arc::new(ApiVersions));
        let cfg = ServerConfigBuilder::new(vec![ListenerConfig {
            name: "tls".to_owned(),
            addr: "127.0.0.1:0".parse().unwrap(),
            pre_bound: None,
            tls_config: Some(tls),
            mtls: None,
        }]);
        let (bound, dispatcher) = Server::new(cfg, Arc::new(d)).bind().await.unwrap();
        let addr = bound.local_addrs()[0].1;
        let cancel = CancellationToken::new();
        let stop = cancel.clone();
        tokio::spawn(async move { bound.serve(dispatcher, stop).await });
        (addr, cancel)
    }

    type Client = tokio_rustls::client::TlsStream<tokio::net::TcpStream>;

    /// Connect and complete one request, which is where TLS 1.3 reports a
    /// client certificate the server refused.
    async fn connect(
        addr: std::net::SocketAddr,
        trust: &Ca,
        client_cert: Option<&(String, String, Vec<u8>)>,
    ) -> std::io::Result<Client> {
        let mut roots = RootCertStore::empty();
        roots.add(trust.cert.der().clone()).unwrap();
        let builder = ClientConfig::builder().with_root_certificates(roots);
        let cfg = match client_cert {
            Some((cert, key, _)) => builder
                .with_client_auth_cert(
                    parse_certs(cert.as_bytes(), Path::new("client")).unwrap(),
                    parse_key(key.as_bytes(), Path::new("client")).unwrap(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        let sock = tokio::net::TcpStream::connect(addr).await?;
        let mut s = TlsConnector::from(Arc::new(cfg))
            .connect(ServerName::try_from("localhost").unwrap(), sock)
            .await?;
        echo(&mut s).await?;
        Ok(s)
    }

    /// One ApiVersions v3 round trip.
    async fn echo(s: &mut Client) -> std::io::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut body = BytesMut::new();
        encode_request_header(
            &mut body,
            &RequestHeader {
                api_key: 18,
                api_version: 3,
                correlation_id: 7,
                client_id: Some("tls-test".to_owned()),
            },
            HeaderVersion::V2,
        )
        .unwrap();
        write_compact_string(&mut body, "tls-test").unwrap();
        write_compact_string(&mut body, "1").unwrap();
        tagged::write_empty(&mut body);
        s.write_all(&u32::try_from(body.len()).unwrap().to_be_bytes())
            .await?;
        s.write_all(&body).await?;
        let mut size = [0u8; 4];
        s.read_exact(&mut size).await?;
        let mut resp = vec![0u8; usize::try_from(u32::from_be_bytes(size)).unwrap()];
        s.read_exact(&mut resp).await?;
        assert_eq!(&resp[..4], &7i32.to_be_bytes(), "correlation id");
        Ok(())
    }

    fn served(s: &Client) -> Vec<u8> {
        s.get_ref().1.peer_certificates().unwrap()[0].to_vec()
    }

    #[tokio::test]
    async fn a_rotated_certificate_reaches_new_connections_and_spares_open_ones() {
        provider();
        let ca = ca("ca");
        let a = leaf(&ca, false);
        let b = leaf(&ca, false);
        let mut vol = SecretVolume::new(&[("tls.crt", &a.0), ("tls.key", &a.1)]);
        let mut r = TlsReloader::load("internal", vol.paths(false)).unwrap();
        let (addr, stop) = listener(r.tls()).await;

        let mut open = connect(addr, &ca, None).await.unwrap();
        assert_eq!(served(&open), a.2);
        assert_eq!(r.poll(), PollOutcome::Unchanged);

        vol.update(&[("tls.crt", &b.0), ("tls.key", &b.1)]);
        assert_eq!(r.poll(), PollOutcome::Settling);
        assert_eq!(
            served(&connect(addr, &ca, None).await.unwrap()),
            a.2,
            "nothing is loaded until a second poll reads the same files"
        );
        assert_eq!(r.poll(), PollOutcome::Reloaded);
        assert_eq!(served(&connect(addr, &ca, None).await.unwrap()), b.2);
        assert_eq!(r.poll(), PollOutcome::Unchanged);

        echo(&mut open).await.unwrap();
        assert_eq!(served(&open), a.2, "an open connection keeps its session");
        stop.cancel();
    }

    #[tokio::test]
    async fn files_that_dont_load_leave_the_old_certificate_serving() {
        provider();
        let ca = ca("ca");
        let a = leaf(&ca, false);
        let b = leaf(&ca, false);
        let c = leaf(&ca, false);
        let mut vol = SecretVolume::new(&[("tls.crt", &a.0), ("tls.key", &a.1)]);
        let mut r = TlsReloader::load("internal", vol.paths(false)).unwrap();
        let (addr, stop) = listener(r.tls()).await;

        // B's certificate with A's key, then a certificate that isn't PEM.
        for bad in [
            [("tls.crt", b.0.as_str()), ("tls.key", a.1.as_str())],
            [("tls.crt", "not a certificate"), ("tls.key", a.1.as_str())],
        ] {
            vol.update(&bad);
            assert_eq!(r.poll(), PollOutcome::Settling);
            assert_eq!(r.poll(), PollOutcome::Refused);
            assert_eq!(
                r.poll(),
                PollOutcome::Unchanged,
                "refused once, not every poll"
            );
            assert_eq!(served(&connect(addr, &ca, None).await.unwrap()), a.2);
        }

        vol.update(&[("tls.crt", &c.0), ("tls.key", &c.1)]);
        assert_eq!(r.poll(), PollOutcome::Settling);
        assert_eq!(r.poll(), PollOutcome::Reloaded);
        assert_eq!(served(&connect(addr, &ca, None).await.unwrap()), c.2);
        stop.cancel();
    }

    #[tokio::test]
    async fn a_missing_file_is_refused_and_its_return_is_not() {
        provider();
        let ca = ca("ca");
        let a = leaf(&ca, false);
        let vol = SecretVolume::new(&[("tls.crt", &a.0), ("tls.key", &a.1)]);
        let mut r = TlsReloader::load("internal", vol.paths(false)).unwrap();
        let (addr, stop) = listener(r.tls()).await;

        let key = vol.dir.path().join("..data/tls.key");
        std::fs::remove_file(&key).unwrap();
        assert_eq!(r.poll(), PollOutcome::Settling);
        assert_eq!(r.poll(), PollOutcome::Refused);
        assert_eq!(served(&connect(addr, &ca, None).await.unwrap()), a.2);

        // The same key coming back is what's already served: nothing to load.
        std::fs::write(&key, &a.1).unwrap();
        assert_eq!(r.poll(), PollOutcome::Unchanged);
        stop.cancel();
    }

    #[tokio::test]
    async fn a_poll_between_the_certificate_and_the_key_changing_refuses_nothing() {
        provider();
        let ca = ca("ca");
        let a = leaf(&ca, false);
        let b = leaf(&ca, false);
        let vol = SecretVolume::new(&[("tls.crt", &a.0), ("tls.key", &a.1)]);
        let mut r = TlsReloader::load("internal", vol.paths(false)).unwrap();

        // A writer that replaces the files one at a time, in place.
        let data = vol.dir.path().join("..data");
        std::fs::write(data.join("tls.crt"), &b.0).unwrap();
        assert_eq!(r.poll(), PollOutcome::Settling);
        std::fs::write(data.join("tls.key"), &b.1).unwrap();
        assert_eq!(r.poll(), PollOutcome::Settling);
        assert_eq!(r.poll(), PollOutcome::Reloaded);
    }

    #[tokio::test]
    async fn a_rotated_client_ca_changes_which_client_certificates_get_in() {
        provider();
        let server_ca = ca("server-ca");
        let old_ca = ca("old-clients-ca");
        let new_ca = ca("new-clients-ca");
        let server = leaf(&server_ca, false);
        let old_client = leaf(&old_ca, true);
        let new_client = leaf(&new_ca, true);
        let (old_ca_pem, new_ca_pem) = (old_ca.cert.pem(), new_ca.cert.pem());

        let mut vol = SecretVolume::new(&[
            ("tls.crt", &server.0),
            ("tls.key", &server.1),
            ("ca.crt", &old_ca_pem),
        ]);
        let mut r = TlsReloader::load("mtls", vol.paths(true)).unwrap();
        let (addr, stop) = listener(r.tls()).await;

        assert!(connect(addr, &server_ca, Some(&old_client)).await.is_ok());
        assert!(connect(addr, &server_ca, Some(&new_client)).await.is_err());
        assert!(
            connect(addr, &server_ca, None).await.is_err(),
            "a client certificate is required"
        );

        vol.update(&[
            ("tls.crt", &server.0),
            ("tls.key", &server.1),
            ("ca.crt", &new_ca_pem),
        ]);
        assert_eq!(r.poll(), PollOutcome::Settling);
        assert_eq!(r.poll(), PollOutcome::Reloaded);
        assert!(connect(addr, &server_ca, Some(&new_client)).await.is_ok());
        assert!(connect(addr, &server_ca, Some(&old_client)).await.is_err());
        stop.cancel();
    }

    #[tokio::test]
    async fn the_poll_loop_picks_up_a_rotation_on_its_own() {
        provider();
        let ca = ca("ca");
        let a = leaf(&ca, false);
        let b = leaf(&ca, false);
        let mut vol = SecretVolume::new(&[("tls.crt", &a.0), ("tls.key", &a.1)]);
        let r = TlsReloader::load("internal", vol.paths(false)).unwrap();
        let (addr, stop) = listener(r.tls()).await;
        tokio::spawn(r.run(Duration::from_millis(20), stop.clone()));

        vol.update(&[("tls.crt", &b.0), ("tls.key", &b.1)]);
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if served(&connect(addr, &ca, None).await.unwrap()) == b.2 {
                break;
            }
            assert!(tokio::time::Instant::now() < deadline, "never reloaded");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        stop.cancel();
    }
}
