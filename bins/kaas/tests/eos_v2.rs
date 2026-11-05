//! gh #171 — hand-rolled KIP-447 EOS round trip.
//!
//! Drives the full transactional path against an in-process server
//! over real TCP, using the kaas-codec primitives directly (no
//! rdkafka / franz-rs dependency — same convention as
//! `tests/smoke.rs`).
//!
//! Two scenarios:
//!
//! - `commit`: InitProducerId → AddPartitionsToTxn → Produce (txn
//!   batch) → AddOffsetsToTxn → TxnOffsetCommit → EndTxn(commit).
//!   Fetch with `isolation_level = 1` (read_committed) sees the
//!   records, `AbortedTransactions[]` is empty.
//! - `abort`: same up to and including AddOffsetsToTxn /
//!   TxnOffsetCommit, then EndTxn(commit=false). Fetch with
//!   `isolation_level = 1` sees `AbortedTransactions[]` populated
//!   with the txn's `(producer_id, first_offset)` so the client
//!   can filter.
//!
//! End-to-end check that workstreams A–G + gh #170 + gh #175 + gh
//! #176 compose: the dispatcher routes keys 22, 24, 25, 26, 28;
//! TxnStateStore transitions and persists; the EndTxn same-broker
//! fast path writes COMMIT/ABORT control batches via
//! `build_control_batch`; Fetch honours `isolation_level` and
//! computes LSO + AbortedTransactions[] from the partition's
//! `open_txns` / `aborted_txns` indexes.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::as_conversions,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::too_many_arguments
)]

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kaas_auth::{AllowAllAuthorizer, Authorizer, NoQuotaChecker, QuotaChecker};
use kaas_broker::{
    AddOffsetsToTxnHandler, AddPartitionsToTxnHandler, ApiVersionsHandler, Broker, EndTxnHandler,
    FetchHandler, InitProducerIdHandler, ListOffsetsHandler, ListenerEntry, MetadataHandler,
    ProduceHandler, TopicMeta, TopicRegistry, TxnOffsetCommitHandler,
};
use kaas_codec::api::common::{
    write_array_len, write_nullable_bytes, write_nullable_str, write_str,
};
use kaas_codec::api::{
    add_offsets_to_txn, add_partitions_to_txn, end_txn, fetch, init_producer_id, produce,
    txn_offset_commit,
};
use kaas_codec::headers::{encode_request_header, HeaderVersion};
use kaas_codec::primitives::{write_i16, write_i32, write_i64, write_i8};
use kaas_codec::tagged;
use kaas_codec::RequestHeader;
use kaas_coordinator::{
    offset_store::OffsetStore, FnLookup, LocalGroupSource, LocalTxnSource, Manager, TxnOffsetHook,
    TxnStateStore,
};
use kaas_protocol::{Dispatcher, ListenerConfig, Server, ServerConfigBuilder};
use kaas_storage::{DiskStorageEngine, PartitionConfig, RealFs, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

// ---------------------------------------------------------------------------
// Fixture: broker + dispatcher with the full Phase 6 surface wired.
// ---------------------------------------------------------------------------

fn build_test_broker(topic: &str, partitions: i32, data_dir: &std::path::Path) -> Arc<Broker> {
    // DiskStorageEngine, not MemoryStorage — the txn-index +
    // LSO + aborted-list bookkeeping (gh #176) lives in
    // `Partition`, which only the disk engine instantiates.
    // MemoryStorage uses the trait's default LSO == HWM and an
    // empty aborts list, which would silently break the abort
    // assertion below.
    let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
        Arc::new(RealFs::new()),
        data_dir.to_path_buf(),
        PartitionConfig::default(),
    ));
    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: topic.to_owned(),
        partition_count: partitions,
        topic_id: [0; 16],
    });
    let authorizer: Arc<dyn Authorizer> = Arc::new(AllowAllAuthorizer);
    let quotas: Arc<dyn QuotaChecker> = Arc::new(NoQuotaChecker);
    Arc::new(Broker::with_auth(
        engine, topics, "kaas-eos", 0, authorizer, quotas,
    ))
}

fn install_phase6_surface(broker: &Arc<Broker>) -> (tempfile::TempDir, Arc<Manager>) {
    let tmp = tempfile::tempdir().unwrap();

    // Consumer-group manager + offset store (needed by
    // TxnOffsetCommit's group-coord gate and the offset hook).
    let offsets = Arc::new(OffsetStore::new(tmp.path()));
    let lookup = Arc::new(FnLookup::new(|_| None));
    let manager = Manager::new(
        "kaas-eos",
        offsets,
        lookup,
        LocalGroupSource::new("kaas-eos"),
    );
    manager.set_txn_assignment_source(LocalTxnSource::new("kaas-eos"));
    broker.install_coord_manager(manager.clone());

    // Transactional state store + offset hook.
    let cluster_dir = tmp.path().join("__cluster");
    std::fs::create_dir_all(&cluster_dir).unwrap();
    let txn_state = Arc::new(TxnStateStore::open(&cluster_dir, 0).unwrap());
    let hook: Arc<dyn TxnOffsetHook> = manager.clone();
    txn_state.set_offset_hook(hook);
    broker.install_txn_state(txn_state);

    // No MarkerQueue / FenceLog in single-broker dev mode: the
    // EndTxn same-broker fast path writes markers locally because
    // coord.leader_for returns None (treated as self).

    (tmp, manager)
}

fn build_dispatcher(broker: Arc<Broker>, listeners: &[ListenerEntry]) -> Arc<Dispatcher> {
    let mut d = Dispatcher::new();
    d.register(0, 3, 9, Arc::new(ProduceHandler::new(broker.clone())));
    d.register(1, 4, 12, Arc::new(FetchHandler::new(broker.clone())));
    d.register(2, 1, 7, Arc::new(ListOffsetsHandler::new(broker.clone())));
    d.register(
        3,
        1,
        10,
        Arc::new(MetadataHandler::new(broker.clone(), listeners)),
    );
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(
        22,
        0,
        4,
        Arc::new(InitProducerIdHandler::new(broker.clone())),
    );
    d.register(
        24,
        0,
        3,
        Arc::new(AddPartitionsToTxnHandler::new(broker.clone())),
    );
    d.register(
        25,
        0,
        3,
        Arc::new(AddOffsetsToTxnHandler::new(broker.clone())),
    );
    d.register(26, 0, 3, Arc::new(EndTxnHandler::new(broker.clone())));
    d.register(28, 0, 3, Arc::new(TxnOffsetCommitHandler::new(broker)));
    Arc::new(d)
}

// ---------------------------------------------------------------------------
// Wire helpers.
// ---------------------------------------------------------------------------

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

/// Skip the response header: i32 correlation_id + (flexible only)
/// an empty tagged block (single 0 byte uvarint).
fn skip_response_header(body: &[u8], hv: HeaderVersion) -> &[u8] {
    let mut off = 4;
    if !matches!(hv, HeaderVersion::V0) {
        off += 1;
    }
    &body[off..]
}

fn header(api_key: i16, version: i16, correlation_id: i32) -> RequestHeader {
    RequestHeader {
        api_key,
        api_version: version,
        correlation_id,
        client_id: Some("eos-smoke".to_owned()),
    }
}

// --- request builders ---

fn init_producer_id_request(corr: i32, txn_id: &str, timeout_ms: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(22, 4, corr), HeaderVersion::V2).unwrap();
    write_nullable_str(&mut body, Some(txn_id), true).unwrap();
    write_i32(&mut body, timeout_ms);
    write_i64(&mut body, -1); // producer_id (v3+)
    write_i16(&mut body, -1); // producer_epoch (v3+)
    tagged::write_empty(&mut body);
    body.to_vec()
}

fn add_partitions_to_txn_request(
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partitions: &[i32],
) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(24, 3, corr), HeaderVersion::V2).unwrap();
    write_str(&mut body, txn_id, true).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    write_array_len(&mut body, partitions.len(), true).unwrap();
    for p in partitions {
        write_i32(&mut body, *p);
    }
    tagged::write_empty(&mut body); // topic tag
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

fn add_offsets_to_txn_request(
    corr: i32,
    txn_id: &str,
    pid: i64,
    epoch: i16,
    group_id: &str,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(25, 3, corr), HeaderVersion::V2).unwrap();
    write_str(&mut body, txn_id, true).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_str(&mut body, group_id, true).unwrap();
    tagged::write_empty(&mut body);
    body.to_vec()
}

fn txn_offset_commit_request(
    corr: i32,
    txn_id: &str,
    group_id: &str,
    pid: i64,
    epoch: i16,
    topic: &str,
    partition: i32,
    offset: i64,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(28, 3, corr), HeaderVersion::V2).unwrap();
    write_str(&mut body, txn_id, true).unwrap();
    write_str(&mut body, group_id, true).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_i32(&mut body, -1); // generation_id
    write_str(&mut body, "", true).unwrap(); // member_id
    write_nullable_str(&mut body, None, true).unwrap(); // group_instance_id
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    write_array_len(&mut body, 1, true).unwrap();
    write_i32(&mut body, partition);
    write_i64(&mut body, offset);
    write_i32(&mut body, -1); // committed_leader_epoch (v2+)
    write_nullable_str(&mut body, None, true).unwrap(); // committed_metadata
    tagged::write_empty(&mut body); // partition tag
    tagged::write_empty(&mut body); // topic tag
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

fn end_txn_request(corr: i32, txn_id: &str, pid: i64, epoch: i16, commit: bool) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(26, 3, corr), HeaderVersion::V2).unwrap();
    write_str(&mut body, txn_id, true).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_i8(&mut body, if commit { 1 } else { 0 });
    tagged::write_empty(&mut body);
    body.to_vec()
}

fn produce_txn_request(
    corr: i32,
    txn_id: &str,
    topic: &str,
    partition: i32,
    batch: &[u8],
) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(0, 9, corr), HeaderVersion::V2).unwrap();
    write_nullable_str(&mut body, Some(txn_id), true).unwrap();
    write_i16(&mut body, -1); // acks
    write_i32(&mut body, 1000); // timeout_ms
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    write_array_len(&mut body, 1, true).unwrap();
    write_i32(&mut body, partition);
    write_nullable_bytes(&mut body, Some(batch), true).unwrap();
    tagged::write_empty(&mut body); // partition tag
    tagged::write_empty(&mut body); // topic tag
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

fn fetch_v12_request(
    corr: i32,
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    isolation_level: i8,
) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(&mut body, &header(1, 12, corr), HeaderVersion::V2).unwrap();
    write_i32(&mut body, -1); // replica_id
    write_i32(&mut body, 0); // max_wait_ms
    write_i32(&mut body, 1); // min_bytes
    write_i32(&mut body, 1024 * 1024); // max_bytes
    write_i8(&mut body, isolation_level);
    write_i32(&mut body, 0); // session_id
    write_i32(&mut body, -1); // session_epoch
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    write_array_len(&mut body, 1, true).unwrap();
    write_i32(&mut body, partition);
    write_i32(&mut body, -1); // current_leader_epoch
    write_i64(&mut body, fetch_offset);
    write_i32(&mut body, -1); // last_fetched_epoch
    write_i64(&mut body, 0); // log_start_offset
    write_i32(&mut body, 64 * 1024); // partition_max_bytes
    tagged::write_empty(&mut body); // partition tag
    tagged::write_empty(&mut body); // topic tag
    write_array_len(&mut body, 0, true).unwrap(); // forgotten topics
    write_str(&mut body, "", true).unwrap(); // rack_id
    tagged::write_empty(&mut body);
    body.to_vec()
}

/// Build a transactional v2 RecordBatch carrying `num_records`
/// records under (pid, epoch). attributes bit 4 set (transactional);
/// base_sequence = 0 = first batch of the txn.
fn build_txn_batch(pid: i64, epoch: i16, num_records: i32) -> Bytes {
    let body_size = 49 + 16 * usize::try_from(num_records).unwrap();
    let total = 12 + body_size;
    let mut buf = vec![0u8; total];
    buf[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset (broker rewrites)
    let body_len_i32 = i32::try_from(body_size).unwrap();
    buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
    buf[16] = 2; // magic
                 // attributes: bit 4 = transactional.
    buf[21..23].copy_from_slice(&0x0010i16.to_be_bytes());
    let last_offset_delta = num_records - 1;
    buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    buf[35..43].copy_from_slice(&1_700_000_000_000i64.to_be_bytes());
    buf[43..51].copy_from_slice(&pid.to_be_bytes());
    buf[51..53].copy_from_slice(&epoch.to_be_bytes());
    // base_sequence stays 0 (first batch).
    // A record count matching lastOffsetDelta (INVALID_RECORD otherwise,
    // gh #305).
    buf[57..61].copy_from_slice(&num_records.to_be_bytes());
    // gh #228: the recovery scan verifies the v2 CRC, so a synthetic
    // batch has to carry a real one or a reopen treats it as bit-rot.
    let crc = kaas_codec::crc::compute(&buf[21..]);
    buf[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(buf)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

struct Harness {
    sock: TcpStream,
    cancel: CancellationToken,
    server_task: tokio::task::JoinHandle<()>,
    broker: Arc<Broker>,
    _tmp: tempfile::TempDir,
    _cluster_tmp: tempfile::TempDir,
    _manager: Arc<Manager>,
}

impl Harness {
    async fn boot(topic: &str) -> Self {
        let listeners = vec![ListenerEntry {
            name: "internal".to_owned(),
            addr: "127.0.0.1:0".to_owned(),
            advertised_host: Some("127.0.0.1".to_owned()),
            tls: None,
            authentication_type: None,
            oauth: None,
        }];
        // One tempdir holds both the disk engine's segments and the
        // cluster dir (txn_state, offsets, etc.). Lives for the
        // harness's lifetime via Harness._tmp.
        let tmp = tempfile::tempdir().unwrap();
        let broker = build_test_broker(topic, 1, tmp.path());
        let (cluster_tmp, manager) = install_phase6_surface(&broker);
        let dispatcher = build_dispatcher(broker.clone(), &listeners);

        let server = Server::new(
            ServerConfigBuilder::new(vec![ListenerConfig {
                name: "internal".to_owned(),
                addr: "127.0.0.1:0".parse().unwrap(),
                pre_bound: None,
                tls_config: None,
                mtls: None,
            }]),
            dispatcher,
        );
        let (bound, dispatcher) = server.bind().await.unwrap();
        let port = bound.local_addrs()[0].1.port();
        let cancel = CancellationToken::new();
        let serve_cancel = cancel.clone();
        let server_task = tokio::spawn(async move { bound.serve(dispatcher, serve_cancel).await });
        let sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        Self {
            sock,
            cancel,
            server_task,
            broker,
            _tmp: tmp,
            _cluster_tmp: cluster_tmp,
            _manager: manager,
        }
    }

    async fn shutdown(mut self) {
        drop(self.sock);
        self.cancel.cancel();
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut self.server_task).await;
        self.broker.engine.drain().await.unwrap();
    }
}

async fn run_txn_through_commit_or_abort(commit: bool) -> fetch::Response {
    let topic = "events";
    let mut h = Harness::boot(topic).await;

    // 1. InitProducerId(transactional_id=tx-1) → (pid, 0).
    let resp = send(&mut h.sock, &init_producer_id_request(1, "tx-1", 60_000)).await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let init = init_producer_id::decode_response(&mut Bytes::copy_from_slice(body), 4).unwrap();
    assert_eq!(init.error_code, 0, "InitProducerId err: {init:?}");
    assert!(init.producer_id >= 1);
    assert_eq!(init.producer_epoch, 0);
    let pid = init.producer_id;
    let epoch = init.producer_epoch;

    // 2. AddPartitionsToTxn for events/0.
    let resp = send(
        &mut h.sock,
        &add_partitions_to_txn_request(2, "tx-1", pid, epoch, topic, &[0]),
    )
    .await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let apt = add_partitions_to_txn::decode_response(&mut Bytes::copy_from_slice(body), 3).unwrap();
    for tr in &apt.results {
        for pr in &tr.partition_results {
            assert_eq!(pr.error_code, 0, "AddPartitionsToTxn err: {pr:?}");
        }
    }

    // 3. Produce a transactional batch (3 records) on events/0.
    let batch = build_txn_batch(pid, epoch, 3);
    let resp = send(
        &mut h.sock,
        &produce_txn_request(3, "tx-1", topic, 0, &batch),
    )
    .await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let pr = produce::decode_response(&mut Bytes::copy_from_slice(body), 9).unwrap();
    let part = &pr.responses[0].partition_responses[0];
    assert_eq!(part.error_code, 0, "Produce err: {pr:#?}");
    assert_eq!(part.base_offset, 0);

    // 4. AddOffsetsToTxn for consumer group "g1".
    let resp = send(
        &mut h.sock,
        &add_offsets_to_txn_request(4, "tx-1", pid, epoch, "g1"),
    )
    .await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let aot = add_offsets_to_txn::decode_response(&mut Bytes::copy_from_slice(body), 3).unwrap();
    assert_eq!(aot.error_code, 0, "AddOffsetsToTxn err: {aot:?}");

    // 5. TxnOffsetCommit: stage offset 3 for g1 on events/0.
    let resp = send(
        &mut h.sock,
        &txn_offset_commit_request(5, "tx-1", "g1", pid, epoch, topic, 0, 3),
    )
    .await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let tco = txn_offset_commit::decode_response(&mut Bytes::copy_from_slice(body), 3).unwrap();
    for tr in &tco.topics {
        for pr in &tr.partitions {
            assert_eq!(pr.error_code, 0, "TxnOffsetCommit err: {pr:?}");
        }
    }

    // 6. EndTxn(commit | abort). Same-broker fast path appends the
    //    control batch immediately + fires the offset hook.
    let resp = send(&mut h.sock, &end_txn_request(6, "tx-1", pid, epoch, commit)).await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let et = end_txn::decode_response(&mut Bytes::copy_from_slice(body), 3).unwrap();
    assert_eq!(et.error_code, 0, "EndTxn err: {et:?}");

    // 7. Fetch with isolation_level = 1 (read_committed).
    let resp = send(&mut h.sock, &fetch_v12_request(7, topic, 0, 0, 1)).await;
    let body = skip_response_header(&resp, HeaderVersion::V1);
    let fr = fetch::decode_response(&mut Bytes::copy_from_slice(body), 12).unwrap();
    h.shutdown().await;
    fr
}

#[tokio::test]
async fn eos_commit_path_records_visible_to_read_committed() {
    let fr = run_txn_through_commit_or_abort(/*commit=*/ true).await;
    let part = &fr.responses[0].partitions[0];
    assert_eq!(part.error_code, 0, "fetch err: {part:#?}");
    assert!(
        part.aborted_transactions.is_empty(),
        "commit must leave aborted list empty: {:?}",
        part.aborted_transactions
    );
    assert_eq!(
        part.last_stable_offset, part.high_watermark,
        "no in-flight txns left → LSO == HWM"
    );
    let records = part.records.as_ref().expect("records present after commit");
    assert!(
        !records.is_empty(),
        "read_committed must see the committed txn data + marker"
    );
}

#[tokio::test]
async fn eos_abort_path_populates_aborted_transactions() {
    let fr = run_txn_through_commit_or_abort(/*commit=*/ false).await;
    let part = &fr.responses[0].partitions[0];
    assert_eq!(part.error_code, 0, "fetch err: {part:#?}");
    assert_eq!(
        part.aborted_transactions.len(),
        1,
        "abort must populate the aborted-transactions list: {:?}",
        part.aborted_transactions
    );
    let aborted = &part.aborted_transactions[0];
    assert!(aborted.producer_id >= 1);
    assert_eq!(
        aborted.first_offset, 0,
        "aborted txn's data batch landed at offset 0"
    );
    assert_eq!(
        part.last_stable_offset, part.high_watermark,
        "after abort marker lands, no in-flight txn → LSO == HWM"
    );
}
