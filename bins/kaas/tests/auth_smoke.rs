//! End-to-end auth smoke: SCRAM-SHA-512 handshake → authenticate
//! → Produce gated by ACLs → quota throttle observed in the
//! response. Drives an in-process broker over real TCP.
//!
//! No rdkafka — same framework-free pattern as `smoke.rs`. Phase 8's
//! parity validation work brings in rdkafka for cross-client wire
//! verification; this test pins the broker-side wire shape today.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::as_conversions,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kaas_auth::credentials::{ScramCreds, TestCred};
use kaas_auth::engine::RealAuthEngine;
use kaas_auth::selector::PerListenerAuthEngine;
use kaas_auth::{
    AclEngine, AuthEngine, Authorizer, CredentialLoader, PrincipalMapper, QuotaChecker,
    QuotaEnforcer, Quotas,
};
use kaas_broker::topic_cr_writer::{ConfigOpWithValue, TopicCRWriter, TopicWriteError};
use kaas_broker::{
    ApiVersionsHandler, Broker, FetchHandler, InitProducerIdHandler, ListOffsetsHandler,
    ListenerEntry, MetadataHandler, ProduceHandler, SaslAuthenticateHandler, SaslHandshakeHandler,
    TopicMeta, TopicRegistry,
};
use kaas_codec::api::common::{
    write_array_len, write_nullable_bytes, write_nullable_str, write_str,
};
use kaas_codec::api::{metadata, produce, sasl_authenticate, sasl_handshake};
use kaas_codec::headers::{encode_request_header, HeaderVersion};
use kaas_codec::primitives::{write_bytes, write_compact_bytes, write_i16, write_i32};
use kaas_codec::tagged;
use kaas_codec::RequestHeader;
use kaas_protocol::{Dispatcher, ListenerConfig, Server, ServerConfigBuilder};
use kaas_storage::{MemoryStorage, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

// --- SCRAM client helpers (mirrors the test code in kaas_auth::scram). ---

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha512};

fn hmac_sha512(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut mac = <Hmac<Sha512> as Mac>::new_from_slice(key).unwrap();
    mac.update(msg);
    mac.finalize().into_bytes().to_vec()
}

fn pbkdf2_hmac_sha512(password: &[u8], salt: &[u8], iters: u32, out: &mut [u8]) {
    assert!(out.len() <= 64);
    let mut salt_block = salt.to_vec();
    salt_block.extend_from_slice(&1u32.to_be_bytes());
    let mut u = hmac_sha512(password, &salt_block);
    let mut t = u.clone();
    for _ in 1..iters {
        u = hmac_sha512(password, &u);
        for (ti, ui) in t.iter_mut().zip(u.iter()) {
            *ti ^= *ui;
        }
    }
    out.copy_from_slice(&t[..out.len()]);
}

fn build_scram_creds(password: &str, salt: &[u8], iterations: i32) -> ScramCreds {
    let mut salted = vec![0u8; 64];
    pbkdf2_hmac_sha512(password.as_bytes(), salt, iterations as u32, &mut salted);
    let client_key = hmac_sha512(&salted, b"Client Key");
    let server_key = hmac_sha512(&salted, b"Server Key");
    let stored_key = Sha512::digest(&client_key).to_vec();
    ScramCreds {
        stored_key,
        server_key,
        salt: salt.to_vec(),
        iterations,
    }
}

fn client_proof(password: &str, salt: &[u8], iterations: i32, auth_message: &str) -> Vec<u8> {
    let mut salted = vec![0u8; 64];
    pbkdf2_hmac_sha512(password.as_bytes(), salt, iterations as u32, &mut salted);
    let client_key = hmac_sha512(&salted, b"Client Key");
    let stored_key = Sha512::digest(&client_key);
    let client_sig = hmac_sha512(&stored_key, auth_message.as_bytes());
    client_key
        .iter()
        .zip(client_sig.iter())
        .map(|(k, s)| k ^ s)
        .collect()
}

fn parse_attrs(s: &str) -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    for part in s.split(',') {
        if part.len() < 2 || part.as_bytes()[1] != b'=' {
            continue;
        }
        m.insert(part[..1].to_owned(), part[2..].to_owned());
    }
    m
}

// --- Setup / dispatcher ---

fn loader_with_alice() -> Arc<CredentialLoader> {
    let salt = b"smoketestsalt";
    let creds = build_scram_creds("hunter2", salt, 4096);
    let loader = CredentialLoader::new("/tmp/auth-smoke-test");
    loader.install_for_test(vec![TestCred {
        username: "alice".to_owned(),
        auth_type: "scram-sha-512".to_owned(),
        scram: Some(creds),
        quotas: Some(Quotas {
            producer_max_byte_rate_per_broker: Some(1_000),
            consumer_max_byte_rate_per_broker: Some(1_000),
            request_percentage: None,
        }),
        ..TestCred::default()
    }]);
    Arc::new(loader)
}

fn build_auth(
    creds: Arc<CredentialLoader>,
) -> (
    Arc<PerListenerAuthEngine>,
    Arc<dyn Authorizer>,
    Arc<dyn QuotaChecker>,
) {
    let mapper = Arc::new(PrincipalMapper::default());
    let real: Arc<dyn AuthEngine> = Arc::new(RealAuthEngine::new(creds.clone(), mapper));
    let mut sel = PerListenerAuthEngine::new(real.clone());
    sel.insert("authed", real);
    let engines = Arc::new(sel);

    // ACL: User:alice can Write on topic "ok" only.
    let acls = AclEngine::new("/tmp/auth-smoke-acl");
    acls.install_for_test(
        r#"{"acls":[{"principal":"User:alice",
            "resource":{"type":"topic","name":"ok","patternType":"literal"},
            "operations":["Write"],"permission":"Allow"}]}"#,
    );
    let authorizer: Arc<dyn Authorizer> = Arc::new(acls);
    let quotas: Arc<dyn QuotaChecker> = Arc::new(QuotaEnforcer::new(creds));
    (engines, authorizer, quotas)
}

fn broker_with(
    topics: &[(&str, i32)],
    authorizer: Arc<dyn Authorizer>,
    quotas: Arc<dyn QuotaChecker>,
) -> Arc<Broker> {
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let registry = Arc::new(TopicRegistry::new());
    for (n, p) in topics {
        registry.insert(TopicMeta {
            name: (*n).to_owned(),
            partition_count: *p,
            topic_id: [0; 16],
        });
    }
    Arc::new(Broker::with_auth(
        engine, registry, "smoke", 0, authorizer, quotas,
    ))
}

fn build_dispatcher(
    broker: Arc<Broker>,
    listeners: &[ListenerEntry],
    engines: Arc<PerListenerAuthEngine>,
) -> Arc<Dispatcher> {
    build_dispatcher_with(broker, listeners, engines, false)
}

fn build_dispatcher_with(
    broker: Arc<Broker>,
    listeners: &[ListenerEntry],
    engines: Arc<PerListenerAuthEngine>,
    auto_create_topics: bool,
) -> Arc<Dispatcher> {
    let mut d = Dispatcher::new();
    d.register(0, 3, 9, Arc::new(ProduceHandler::new(broker.clone())));
    d.register(1, 4, 12, Arc::new(FetchHandler::new(broker.clone())));
    d.register(2, 1, 7, Arc::new(ListOffsetsHandler::new(broker.clone())));
    d.register(
        3,
        1,
        10,
        Arc::new(
            MetadataHandler::new(broker.clone(), listeners).with_auto_create(auto_create_topics, 3),
        ),
    );
    d.register(
        17,
        0,
        1,
        Arc::new(SaslHandshakeHandler::new(engines.clone())),
    );
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(22, 0, 4, Arc::new(InitProducerIdHandler::new(broker)));
    d.register(
        36,
        0,
        2,
        Arc::new(SaslAuthenticateHandler::new(engines.clone())),
    );
    d.set_auth(engines);
    Arc::new(d)
}

async fn spawn_server(dispatcher: Arc<Dispatcher>) -> (CancellationToken, u16) {
    let cfg = ServerConfigBuilder::new(vec![ListenerConfig {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".parse().unwrap(),
        pre_bound: None,
        tls_config: None,
        mtls: None,
    }]);
    let server = Server::new(cfg, dispatcher.clone());
    let (bound, dispatcher) = server.bind().await.unwrap();
    let port = bound.local_addrs()[0].1.port();
    let cancel = CancellationToken::new();
    let serve_cancel = cancel.clone();
    tokio::spawn(async move { bound.serve(dispatcher, serve_cancel).await });
    (cancel, port)
}

// --- Request encoders (kept small; we only need the SASL + produce shapes) ---

async fn send(sock: &mut TcpStream, body: &[u8]) -> Vec<u8> {
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
            client_id: Some("smoke".to_owned()),
        },
        hv,
    )
    .unwrap();
    w
}

fn handshake_frame(mechanism: &str, correlation_id: i32) -> Vec<u8> {
    let mut w = header(17, 1, correlation_id, HeaderVersion::V1);
    // body: non-flexible string
    write_str(&mut w, mechanism, false).unwrap();
    w.to_vec()
}

fn authenticate_frame_v2(payload: &[u8], correlation_id: i32) -> Vec<u8> {
    let mut w = header(36, 2, correlation_id, HeaderVersion::V2);
    write_compact_bytes(&mut w, payload).unwrap();
    tagged::write_empty(&mut w);
    w.to_vec()
}

fn authenticate_frame_v1(payload: &[u8], correlation_id: i32) -> Vec<u8> {
    let mut w = header(36, 1, correlation_id, HeaderVersion::V1);
    write_bytes(&mut w, payload).unwrap();
    w.to_vec()
}

fn produce_frame_v9(topic: &str, records: Bytes, correlation_id: i32) -> Vec<u8> {
    let mut w = header(0, 9, correlation_id, HeaderVersion::V2);
    // body (v9 flexible): transactional_id (nullable compact), acks (i16),
    // timeout_ms (i32), topic_data array, tag.
    write_nullable_str(&mut w, None, true).unwrap();
    write_i16(&mut w, -1); // acks
    write_i32(&mut w, 1000); // timeout_ms
    write_array_len(&mut w, 1, true).unwrap();
    write_str(&mut w, topic, true).unwrap();
    write_array_len(&mut w, 1, true).unwrap();
    write_i32(&mut w, 0); // partition_index
    write_nullable_bytes(&mut w, Some(&records), true).unwrap();
    tagged::write_empty(&mut w); // partition tag
    tagged::write_empty(&mut w); // topic tag
    tagged::write_empty(&mut w); // request tag
    w.to_vec()
}

fn build_record_batch(num_records: i32, size: usize) -> Bytes {
    // Same shape as the smoke.rs helper, padded to `size` bytes
    // total so we can drive quota math.
    let body_size = size - 12;
    let mut buf = vec![0u8; size];
    let body_len_i32 = i32::try_from(body_size).unwrap();
    buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
    buf[16] = 2; // magic = 2
    let last_offset_delta = num_records - 1;
    buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    buf[35..43].copy_from_slice(&100i64.to_be_bytes());
    buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    Bytes::from(buf)
}

// Strip the response header (correlation_id [+ tagged]) from a body.
fn strip_response_header(body: &[u8], hv: HeaderVersion) -> Bytes {
    let skip = match hv {
        HeaderVersion::V0 => 4,
        HeaderVersion::V1 | HeaderVersion::V2 => 5, // correlation + 1-byte empty tagged
    };
    Bytes::copy_from_slice(&body[skip..])
}

// --- Tests ---

#[tokio::test]
async fn scram_handshake_then_authenticate_unblocks_produce() {
    let creds = loader_with_alice();
    let (engines, authorizer, quotas) = build_auth(creds);
    let broker = broker_with(&[("ok", 1)], authorizer, quotas);
    let listeners = vec![ListenerEntry {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: Some("scram-sha-512".to_owned()),
        oauth: None,
    }];
    let dispatcher = build_dispatcher(broker, &listeners, engines);
    let (cancel, port) = spawn_server(dispatcher).await;
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // 1. Produce BEFORE SASL — expect CLUSTER_AUTHORIZATION_FAILED (31).
    let records = build_record_batch(1, 80);
    let resp = send(&mut sock, &produce_frame_v9("ok", records.clone(), 1)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    // The error_body wraps the first i16 with the cluster_auth err code.
    assert_eq!(
        i16::from_be_bytes([body[0], body[1]]),
        31,
        "produce before SASL must be denied with CLUSTER_AUTHORIZATION_FAILED"
    );

    // 2. SaslHandshake → SCRAM-SHA-512.
    let resp = send(&mut sock, &handshake_frame("SCRAM-SHA-512", 2)).await;
    let body = strip_response_header(&resp, HeaderVersion::V0);
    let mut b = body;
    let r = sasl_handshake::decode_response(&mut b, 1).unwrap();
    assert_eq!(r.error_code, 0);

    // 3. SaslAuthenticate — client-first.
    let client_first = b"n,,n=alice,r=clientnonce";
    let resp = send(&mut sock, &authenticate_frame_v2(client_first, 3)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = sasl_authenticate::decode_response(&mut b, 2).unwrap();
    assert_eq!(r.error_code, 0);
    let server_first = std::str::from_utf8(&r.auth_bytes).unwrap().to_owned();

    // 4. Build the proof + client-final.
    let attrs = parse_attrs(&server_first);
    let full_nonce = attrs.get("r").unwrap();
    let salt_b64 = attrs.get("s").unwrap();
    let salt = BASE64.decode(salt_b64).unwrap();
    let iterations: i32 = attrs.get("i").unwrap().parse().unwrap();
    let client_final_without_proof = format!("c=biws,r={full_nonce}");
    let auth_message = format!("n=alice,r=clientnonce,{server_first},{client_final_without_proof}");
    let proof = client_proof("hunter2", &salt, iterations, &auth_message);
    let client_final = format!("{client_final_without_proof},p={}", BASE64.encode(&proof));

    let resp = send(
        &mut sock,
        &authenticate_frame_v2(client_final.as_bytes(), 4),
    )
    .await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = sasl_authenticate::decode_response(&mut b, 2).unwrap();
    assert_eq!(r.error_code, 0, "client-final must succeed");

    // 5. Produce to "ok" — ACL allows it, throttle 0.
    let resp = send(&mut sock, &produce_frame_v9("ok", records.clone(), 5)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = produce::decode_response(&mut b, 9).unwrap();
    assert_eq!(
        r.responses[0].partition_responses[0].error_code, 0,
        "Write on allowed topic must succeed"
    );

    // 6. Produce to "denied", a topic the broker doesn't know and alice
    // may not write: TOPIC_AUTHORIZATION_FAILED (29). The ACL is checked
    // before existence, as in Apache, so the error never confirms whether
    // a topic exists (gh #282; it used to answer 3 here).
    let resp = send(&mut sock, &produce_frame_v9("denied", records.clone(), 6)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = produce::decode_response(&mut b, 9).unwrap();
    assert_eq!(r.responses[0].partition_responses[0].error_code, 29);

    cancel.cancel();
}

#[tokio::test]
async fn legacy_sasl_authenticate_response_has_no_tagged_header_byte() {
    // gh #272: a non-flexible response header is the bare correlation id.
    // kaas used to append a tagged-fields byte to every legacy-version
    // response, so a client reading SaslAuthenticate v1 the way Apache
    // frames it took that byte for the first half of the error code.
    let creds = loader_with_alice();
    let (engines, authorizer, quotas) = build_auth(creds);
    let broker = broker_with(&[("ok", 1)], authorizer, quotas);
    let listeners = vec![ListenerEntry {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: Some("scram-sha-512".to_owned()),
        oauth: None,
    }];
    let dispatcher = build_dispatcher(broker, &listeners, engines);
    let (cancel, port) = spawn_server(dispatcher).await;
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    let resp = send(&mut sock, &handshake_frame("SCRAM-SHA-512", 1)).await;
    let mut b = strip_response_header(&resp, HeaderVersion::V0);
    assert_eq!(
        sasl_handshake::decode_response(&mut b, 1)
            .unwrap()
            .error_code,
        0
    );

    let resp = send(
        &mut sock,
        &authenticate_frame_v1(b"n,,n=alice,r=clientnonce", 2),
    )
    .await;
    assert_eq!(&resp[..4], &2i32.to_be_bytes(), "correlation id");
    let mut b = strip_response_header(&resp, HeaderVersion::V0);
    let r = sasl_authenticate::decode_response(&mut b, 1).unwrap();
    assert_eq!(r.error_code, 0);
    assert!(
        r.auth_bytes.starts_with(b"r=clientnonce"),
        "server-first must follow the error fields directly, got {:?}",
        String::from_utf8_lossy(&r.auth_bytes)
    );
    assert!(b.is_empty(), "no bytes may trail the v1 body");

    cancel.cancel();
}

#[tokio::test]
async fn acl_denies_unconfigured_topic() {
    // Add a second topic the broker knows about but ACL doesn't grant.
    let creds = loader_with_alice();
    let (engines, authorizer, quotas) = build_auth(creds);
    let broker = broker_with(&[("ok", 1), ("locked", 1)], authorizer, quotas);
    let listeners = vec![ListenerEntry {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: Some("scram-sha-512".to_owned()),
        oauth: None,
    }];
    let dispatcher = build_dispatcher(broker, &listeners, engines);
    let (cancel, port) = spawn_server(dispatcher).await;
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // Run the SASL exchange (same as above, factored if we add more
    // tests).
    drive_scram_to_done(&mut sock).await;

    // Produce to "locked" — ACL doesn't grant Write → 29.
    let records = build_record_batch(1, 80);
    let resp = send(&mut sock, &produce_frame_v9("locked", records, 10)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = produce::decode_response(&mut b, 9).unwrap();
    assert_eq!(
        r.responses[0].partition_responses[0].error_code, 29,
        "locked topic must return TOPIC_AUTHORIZATION_FAILED"
    );

    cancel.cancel();
}

#[tokio::test]
async fn produce_exceeds_quota_returns_throttle() {
    let creds = loader_with_alice();
    let (engines, authorizer, quotas) = build_auth(creds);
    let broker = broker_with(&[("ok", 1)], authorizer, quotas);
    let listeners = vec![ListenerEntry {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: Some("scram-sha-512".to_owned()),
        oauth: None,
    }];
    let dispatcher = build_dispatcher(broker, &listeners, engines);
    let (cancel, port) = spawn_server(dispatcher).await;
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    drive_scram_to_done(&mut sock).await;

    // Drain the 1000 B/s producer quota.
    let big_records = build_record_batch(1, 2_000);
    let _ = send(&mut sock, &produce_frame_v9("ok", big_records.clone(), 20)).await;
    // Second back-to-back drain must report a positive throttle.
    let resp = send(&mut sock, &produce_frame_v9("ok", big_records, 21)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = produce::decode_response(&mut b, 9).unwrap();
    assert!(
        r.throttle_time_ms > 0,
        "second drain at 2000 B over 1000 B/s quota must throttle, got {}",
        r.throttle_time_ms
    );

    cancel.cancel();
}

/// Drive a fresh socket from anonymous to sasl_done = true. Shared
/// helper for ACL/quota tests.
async fn drive_scram_to_done(sock: &mut TcpStream) {
    // Handshake.
    let _ = send(sock, &handshake_frame("SCRAM-SHA-512", 100)).await;
    // Client-first.
    let resp = send(sock, &authenticate_frame_v2(b"n,,n=alice,r=nc", 101)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = sasl_authenticate::decode_response(&mut b, 2).unwrap();
    let server_first = std::str::from_utf8(&r.auth_bytes).unwrap().to_owned();
    let attrs = parse_attrs(&server_first);
    let full_nonce = attrs.get("r").unwrap();
    let salt_b64 = attrs.get("s").unwrap();
    let salt = BASE64.decode(salt_b64).unwrap();
    let iter: i32 = attrs.get("i").unwrap().parse().unwrap();
    let cf = format!("c=biws,r={full_nonce}");
    let am = format!("n=alice,r=nc,{server_first},{cf}");
    let proof = client_proof("hunter2", &salt, iter, &am);
    let final_msg = format!("{cf},p={}", BASE64.encode(&proof));
    let resp = send(sock, &authenticate_frame_v2(final_msg.as_bytes(), 102)).await;
    let body = strip_response_header(&resp, HeaderVersion::V2);
    let mut b = body;
    let r = sasl_authenticate::decode_response(&mut b, 2).unwrap();
    assert_eq!(r.error_code, 0, "SASL must complete");
}

/// Records every `create_topic` call, so a test can see whether
/// Metadata minted a `KafkaTopic` CR.
#[derive(Debug, Default)]
struct RecordingCrWriter {
    created: parking_lot::Mutex<Vec<(String, i32)>>,
}

#[async_trait::async_trait]
impl TopicCRWriter for RecordingCrWriter {
    async fn create_topic(
        &self,
        name: &str,
        partitions: i32,
        _configs: &[(String, String)],
    ) -> Result<(), TopicWriteError> {
        self.created.lock().push((name.to_owned(), partitions));
        Ok(())
    }
    async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
        unreachable!("Metadata never expands a topic")
    }
    async fn update_topic_config(
        &self,
        _: &str,
        _: &[ConfigOpWithValue],
    ) -> Result<(), TopicWriteError> {
        unreachable!("Metadata never edits topic config")
    }
    async fn delete_topic(&self, _: &str) -> Result<(), TopicWriteError> {
        unreachable!("Metadata never deletes a topic")
    }
    async fn set_partition_log_dir(&self, _: &str, _: i32, _: &str) -> Result<(), TopicWriteError> {
        unreachable!("Metadata never moves a partition")
    }
}

/// Metadata v9 for one topic, asking the broker to auto-create it.
fn metadata_frame_v9_auto_create(topic: &str, correlation_id: i32) -> Vec<u8> {
    let mut w = header(3, 9, correlation_id, HeaderVersion::V2);
    write_array_len(&mut w, 1, true).unwrap();
    write_str(&mut w, topic, true).unwrap();
    tagged::write_empty(&mut w); // topic tag
    w.extend_from_slice(&[1, 0, 0]); // allow_auto_topic_creation, both include_* flags
    tagged::write_empty(&mut w); // request tag
    w.to_vec()
}

/// gh #244: auto-creation's `Create` check is the guard whose regression
/// is a security bug. Metadata would mint topics CreateTopics refuses.
/// The handler unit test pins its logic against a stub authorizer; this
/// drives it over TCP as a SCRAM-authenticated principal against the
/// real ACL engine, which is where `Write` implies `Describe` but not
/// `Create`.
#[tokio::test]
async fn metadata_auto_create_requires_the_create_acl() {
    let creds = loader_with_alice();
    let (engines, _, quotas) = build_auth(creds);
    let acls = AclEngine::new("/tmp/auth-smoke-auto-create-acl");
    acls.install_for_test(
        r#"{"acls":[
            {"principal":"User:alice",
             "resource":{"type":"topic","name":"writable","patternType":"literal"},
             "operations":["Write"],"permission":"Allow"},
            {"principal":"User:alice",
             "resource":{"type":"topic","name":"creatable","patternType":"literal"},
             "operations":["Create","Describe"],"permission":"Allow"}]}"#,
    );
    let broker = broker_with(&[], Arc::new(acls), quotas);
    let writer = Arc::new(RecordingCrWriter::default());
    broker.install_cr_writer(writer.clone());
    let listeners = vec![ListenerEntry {
        name: "authed".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: Some("scram-sha-512".to_owned()),
        oauth: None,
    }];
    let dispatcher = build_dispatcher_with(broker, &listeners, engines, true);
    let (cancel, port) = spawn_server(dispatcher).await;
    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
    drive_scram_to_done(&mut sock).await;

    let mut topic_error = async |topic: &str, correlation_id: i32| {
        let resp = send(
            &mut sock,
            &metadata_frame_v9_auto_create(topic, correlation_id),
        )
        .await;
        let mut b = strip_response_header(&resp, HeaderVersion::V2);
        let r = metadata::decode_response(&mut b, 9).unwrap();
        assert_eq!(r.topics.len(), 1);
        assert_eq!(r.topics[0].name, topic);
        r.topics[0].error_code
    };

    // Describe (implied by Write) but no Create: refused, nothing minted.
    assert_eq!(topic_error("writable", 30).await, 29);
    // No ACL at all: refused at Describe, before Create is considered.
    assert_eq!(topic_error("ungranted", 31).await, 29);
    assert!(
        writer.created.lock().is_empty(),
        "a principal without Create must not mint a KafkaTopic CR: {:?}",
        writer.created.lock()
    );

    // Create granted: the CR is minted and the client told to retry.
    assert_eq!(
        topic_error("creatable", 32).await,
        5,
        "LEADER_NOT_AVAILABLE"
    );
    assert_eq!(*writer.created.lock(), [("creatable".to_owned(), 3)]);

    cancel.cancel();
}
