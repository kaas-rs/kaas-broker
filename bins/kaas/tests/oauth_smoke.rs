//! End-to-end OAUTHBEARER smoke (gh #42): TLS listener → SaslHandshake
//! advertising only OAUTHBEARER → RFC 7628 initial response carrying an
//! ES256 JWT validated against a JWKS served by a mock issuer →
//! Produce unblocked; plus the two-step failure path for a bad token.
//!
//! Same framework-free pattern as `auth_smoke.rs`, with two additions:
//! the connection is real rustls (OAUTHBEARER is refused over
//! plaintext, so the smoke has to speak TLS to exercise the happy
//! path), and the JWKS travels over HTTP from a wiremock issuer the
//! way the production fetch loop pulls it from EntraID.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::as_conversions,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use bytes::{Bytes, BytesMut};
use kaas_auth::selector::PerListenerAuthEngine;
use kaas_auth::{
    AllowAllAuthorizer, AuthEngine, Authorizer, NoQuotaChecker, OauthConfig, OauthEngine,
    OauthValidator, QuotaChecker,
};
use kaas_broker::{
    ApiVersionsHandler, Broker, MetadataHandler, ProduceHandler, SaslAuthenticateHandler,
    SaslHandshakeHandler, TopicMeta, TopicRegistry,
};
use kaas_codec::api::common::{
    write_array_len, write_nullable_bytes, write_nullable_str, write_str,
};
use kaas_codec::api::{sasl_authenticate, sasl_handshake};
use kaas_codec::headers::{encode_request_header, HeaderVersion};
use kaas_codec::primitives::{write_compact_bytes, write_i16, write_i32};
use kaas_codec::tagged;
use kaas_codec::RequestHeader;
use kaas_protocol::{Dispatcher, ListenerConfig, Server, ServerConfigBuilder};
use kaas_storage::{MemoryStorage, StorageEngine};
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const ISSUER: &str = "https://issuer.smoke.test/v2.0";

// --- Mock OIDC issuer: ES256 keypair + JWKS + token minting ---

struct Issuer {
    kp: EcdsaKeyPair,
    rng: SystemRandom,
}

impl Issuer {
    fn new() -> Self {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let kp = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        Self { kp, rng }
    }

    fn jwks(&self) -> String {
        let point = self.kp.public_key().as_ref();
        let x = URL_SAFE_NO_PAD.encode(&point[1..33]);
        let y = URL_SAFE_NO_PAD.encode(&point[33..65]);
        format!(
            r#"{{"keys":[{{"kty":"EC","crv":"P-256","kid":"smoke-1","use":"sig","x":"{x}","y":"{y}"}}]}}"#
        )
    }

    fn mint(&self, iss: &str, sub: &str, ttl_secs: i64) -> String {
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        )
        .unwrap();
        let claims = serde_json::json!({
            "iss": iss, "sub": sub, "aud": "kaas", "exp": now + ttl_secs, "iat": now,
        });
        let header = r#"{"alg":"ES256","kid":"smoke-1"}"#;
        let msg = format!(
            "{}.{}",
            URL_SAFE_NO_PAD.encode(header.as_bytes()),
            URL_SAFE_NO_PAD.encode(claims.to_string().as_bytes())
        );
        let sig = self.kp.sign(&self.rng, msg.as_bytes()).unwrap();
        format!("{msg}.{}", URL_SAFE_NO_PAD.encode(sig.as_ref()))
    }
}

// --- Broker bring-up (TLS listener named "oauth") ---

/// Same call bins/kaas/src/main.rs makes at boot: with kube's
/// hyper-rustls enabling aws-lc-rs alongside our ring feature, rustls
/// can't auto-pick a provider. It has to run before the first TLS
/// client or server is built in the process — reqwest included — so call
/// it first in any test that touches TLS. The tests share one process
/// and run in parallel, so relying on another test to have installed it
/// passes or panics ("No provider set") depending on scheduling.
fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

async fn spawn_oauth_broker(
    validator: Arc<OauthValidator>,
) -> (
    CancellationToken,
    u16,
    rustls::pki_types::CertificateDer<'static>,
) {
    install_crypto_provider();
    let engine: Arc<dyn AuthEngine> = Arc::new(OauthEngine::new(validator));
    let mut sel = PerListenerAuthEngine::new(engine.clone());
    sel.insert("oauth", engine);
    let engines = Arc::new(sel);

    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let registry = Arc::new(TopicRegistry::new());
    registry.insert(TopicMeta {
        name: "beats".to_owned(),
        partition_count: 1,
        topic_id: [0; 16],
    });
    let authorizer: Arc<dyn Authorizer> = Arc::new(AllowAllAuthorizer);
    let quotas: Arc<dyn QuotaChecker> = Arc::new(NoQuotaChecker);
    let broker = Arc::new(Broker::with_auth(
        storage,
        registry,
        "oauth-smoke",
        0,
        authorizer,
        quotas,
    ));

    let listeners = vec![kaas_broker::ListenerEntry {
        name: "oauth".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None, // ListenerEntry.tls feeds Metadata, not the test socket
        authentication_type: Some("oauth".to_owned()),
        oauth: None,
    }];

    let mut d = Dispatcher::new();
    d.register(0, 3, 9, Arc::new(ProduceHandler::new(broker.clone())));
    d.register(
        3,
        1,
        10,
        Arc::new(MetadataHandler::new(broker.clone(), &listeners)),
    );
    d.register(
        17,
        0,
        1,
        Arc::new(SaslHandshakeHandler::new(engines.clone())),
    );
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(
        36,
        0,
        2,
        Arc::new(SaslAuthenticateHandler::new(engines.clone())),
    );
    d.set_auth(engines);
    let dispatcher = Arc::new(d);

    // Self-signed listener cert, trusted by the test client only.
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let cert_der = cert.cert.der().clone();
    let key_der =
        rustls::pki_types::PrivateKeyDer::try_from(cert.key_pair.serialize_der()).unwrap();
    let server_tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .unwrap();

    let cfg = ServerConfigBuilder::new(vec![ListenerConfig {
        name: "oauth".to_owned(),
        addr: "127.0.0.1:0".parse().unwrap(),
        pre_bound: None,
        tls_config: Some(Arc::new(server_tls)),
        mtls: None,
    }]);
    let server = Server::new(cfg, dispatcher.clone());
    let (bound, dispatcher) = server.bind().await.unwrap();
    let port = bound.local_addrs()[0].1.port();
    let cancel = CancellationToken::new();
    let serve_cancel = cancel.clone();
    tokio::spawn(async move { bound.serve(dispatcher, serve_cancel).await });
    (cancel, port, cert_der)
}

async fn tls_connect(
    port: u16,
    cert: &rustls::pki_types::CertificateDer<'static>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.clone()).unwrap();
    let cc = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(cc));
    let tcp = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    connector
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").unwrap(),
            tcp,
        )
        .await
        .unwrap()
}

// --- Frame helpers (same shapes as auth_smoke.rs, generic over TLS) ---

async fn send<S: AsyncRead + AsyncWrite + Unpin>(sock: &mut S, body: &[u8]) -> Vec<u8> {
    let len = i32::try_from(body.len()).unwrap();
    sock.write_all(&len.to_be_bytes()).await.unwrap();
    sock.write_all(body).await.unwrap();
    sock.flush().await.unwrap();

    let mut sz = [0u8; 4];
    sock.read_exact(&mut sz).await.unwrap();
    let n = i32::from_be_bytes(sz) as usize;
    let mut buf = vec![0u8; n];
    sock.read_exact(&mut buf).await.unwrap();
    buf
}

fn header(api_key: i16, version: i16, correlation_id: i32, hv: HeaderVersion) -> BytesMut {
    let mut w = BytesMut::new();
    encode_request_header(
        &mut w,
        &RequestHeader {
            api_key,
            api_version: version,
            correlation_id,
            client_id: Some("oauth-smoke".to_owned()),
        },
        hv,
    )
    .unwrap();
    w
}

fn handshake_frame(mechanism: &str, correlation_id: i32) -> Vec<u8> {
    let mut w = header(17, 1, correlation_id, HeaderVersion::V1);
    write_str(&mut w, mechanism, false).unwrap();
    w.to_vec()
}

fn authenticate_frame_v2(payload: &[u8], correlation_id: i32) -> Vec<u8> {
    let mut w = header(36, 2, correlation_id, HeaderVersion::V2);
    write_compact_bytes(&mut w, payload).unwrap();
    tagged::write_empty(&mut w);
    w.to_vec()
}

fn produce_frame_v9(topic: &str, records: Bytes, correlation_id: i32) -> Vec<u8> {
    let mut w = header(0, 9, correlation_id, HeaderVersion::V2);
    write_nullable_str(&mut w, None, true).unwrap();
    write_i16(&mut w, -1);
    write_i32(&mut w, 1000);
    write_array_len(&mut w, 1, true).unwrap();
    write_str(&mut w, topic, true).unwrap();
    write_array_len(&mut w, 1, true).unwrap();
    write_i32(&mut w, 0);
    write_nullable_bytes(&mut w, Some(&records), true).unwrap();
    tagged::write_empty(&mut w);
    tagged::write_empty(&mut w);
    tagged::write_empty(&mut w);
    w.to_vec()
}

fn build_record_batch(num_records: i32, size: usize) -> Bytes {
    let body_size = size - 12;
    let mut buf = vec![0u8; size];
    let body_len_i32 = i32::try_from(body_size).unwrap();
    buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
    buf[16] = 2;
    let last_offset_delta = num_records - 1;
    buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    buf[35..43].copy_from_slice(&100i64.to_be_bytes());
    buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    Bytes::from(buf)
}

fn strip_response_header(body: &[u8], hv: HeaderVersion) -> Bytes {
    let skip = match hv {
        HeaderVersion::V0 => 4,
        HeaderVersion::V1 | HeaderVersion::V2 => 5,
    };
    Bytes::copy_from_slice(&body[skip..])
}

fn initial_response(token: &str) -> Vec<u8> {
    let mut v = Vec::new();
    v.extend_from_slice(b"n,,");
    v.push(0x01);
    v.extend_from_slice(format!("auth=Bearer {token}").as_bytes());
    v.push(0x01);
    v.push(0x01);
    v
}

fn validator_with(max_reauth: Option<u64>) -> Arc<OauthValidator> {
    Arc::new(OauthValidator::new(OauthConfig {
        valid_issuer_uri: ISSUER.to_owned(),
        jwks_endpoint_uri: String::new(), // filled by the mock server per test
        user_name_claim: None,
        fallback_user_name_claim: None,
        check_audience: false,
        client_id: None,
        jwks_refresh_seconds: 300,
        max_seconds_without_reauthentication: max_reauth,
    }))
}

// --- Tests ---

#[tokio::test]
async fn oauthbearer_end_to_end_over_tls() {
    // Before the JWKS fetch below: reqwest builds a rustls client.
    install_crypto_provider();
    let issuer = Issuer::new();

    // The JWKS travels over HTTP exactly like the production fetch
    // loop pulls it from the IdP.
    let mock = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/discovery/keys"))
        .respond_with(ResponseTemplate::new(200).set_body_string(issuer.jwks()))
        .mount(&mock)
        .await;

    let validator = validator_with(Some(3600));
    let jwks_body = reqwest::get(format!("{}/discovery/keys", mock.uri()))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert_eq!(validator.install_jwks(&jwks_body).unwrap(), 1);

    let (cancel, port, cert) = spawn_oauth_broker(validator).await;
    let mut sock = tls_connect(port, &cert).await;

    // 1. Produce before SASL → CLUSTER_AUTHORIZATION_FAILED (31).
    let records = build_record_batch(1, 80);
    let resp = send(&mut sock, &produce_frame_v9("beats", records.clone(), 1)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    assert_eq!(i16::from_be_bytes([body[0], body[1]]), 31);

    // 2. Handshake: only OAUTHBEARER advertised.
    let resp = send(&mut sock, &handshake_frame("OAUTHBEARER", 2)).await;
    let mut body = strip_response_header(&resp, HeaderVersion::V0);
    let hs = sasl_handshake::decode_response(&mut body, 1).unwrap();
    assert_eq!(hs.error_code, 0);
    assert_eq!(hs.mechanisms, vec!["OAUTHBEARER"]);

    // 3. Authenticate with a valid token.
    let token = issuer.mint(ISSUER, "spn-canary", 3600);
    let resp = send(
        &mut sock,
        &authenticate_frame_v2(&initial_response(&token), 3),
    )
    .await;
    let mut body = strip_response_header(&resp, HeaderVersion::V2);
    let auth = sasl_authenticate::decode_response(&mut body, 2).unwrap();
    assert_eq!(auth.error_code, 0, "{:?}", auth.error_message);
    assert!(auth.auth_bytes.is_empty(), "success has no server message");
    assert!(
        auth.session_lifetime_ms > 0 && auth.session_lifetime_ms <= 3_600_000,
        "KIP-368 lifetime advertised: {}",
        auth.session_lifetime_ms
    );

    // 4. Produce now goes through (partition-level error 0).
    let resp = send(&mut sock, &produce_frame_v9("beats", records, 4)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    assert_ne!(
        i16::from_be_bytes([body[0], body[1]]),
        31,
        "produce after OAUTHBEARER must not be auth-gated"
    );

    cancel.cancel();
}

#[tokio::test]
async fn oauthbearer_bad_token_fails_after_challenge_roundtrip() {
    let issuer = Issuer::new();
    let validator = validator_with(None);
    validator.install_jwks(&issuer.jwks()).unwrap();
    let (cancel, port, cert) = spawn_oauth_broker(validator).await;
    let mut sock = tls_connect(port, &cert).await;

    let resp = send(&mut sock, &handshake_frame("OAUTHBEARER", 1)).await;
    let mut body = strip_response_header(&resp, HeaderVersion::V0);
    assert_eq!(
        sasl_handshake::decode_response(&mut body, 1)
            .unwrap()
            .error_code,
        0
    );

    // Wrong-issuer token: signature is fine, `iss` is not.
    let token = issuer.mint("https://evil.test", "spn-canary", 3600);
    let resp = send(
        &mut sock,
        &authenticate_frame_v2(&initial_response(&token), 2),
    )
    .await;
    let mut body = strip_response_header(&resp, HeaderVersion::V2);
    let auth = sasl_authenticate::decode_response(&mut body, 2).unwrap();
    assert_eq!(auth.error_code, 0, "failure is a challenge first");
    assert_eq!(&auth.auth_bytes[..], b"{\"status\":\"invalid_token\"}");

    // Ack the challenge → 58, and the connection stays gated.
    let resp = send(&mut sock, &authenticate_frame_v2(&[0x01], 3)).await;
    let mut body = strip_response_header(&resp, HeaderVersion::V2);
    let auth = sasl_authenticate::decode_response(&mut body, 2).unwrap();
    assert_eq!(auth.error_code, 58);

    let records = build_record_batch(1, 80);
    let resp = send(&mut sock, &produce_frame_v9("beats", records, 4)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    assert_eq!(
        i16::from_be_bytes([body[0], body[1]]),
        31,
        "failed auth leaves the pre-auth gate armed"
    );

    cancel.cancel();
}
