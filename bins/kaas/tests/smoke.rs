//! End-to-end smoke test: drive Produce → Fetch over real TCP
//! against an in-process server, assert byte-equal records.
//!
//! No rdkafka — this is the framework-free version of the Phase 3
//! exit criterion #6. Once rdkafka builds on the CI runners, a
//! second integration test under `#[ignore]` can swap the
//! hand-rolled producer for a real Kafka client.

// Integration tests aren't `#[cfg(test)]`, so the workspace's
// `allow-unwrap-in-tests` doesn't apply automatically. Allow at
// file scope.
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::as_conversions,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation
)]

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kaas_broker::{
    ApiVersionsHandler, Broker, DescribeClusterHandler, FetchHandler, InitProducerIdHandler,
    ListOffsetsHandler, ListenerEntry, MetadataHandler, ProduceHandler, TopicMeta, TopicRegistry,
};
use kaas_codec::api::common::{
    write_array_len, write_nullable_bytes, write_nullable_str, write_str,
};
use kaas_codec::api::{describe_cluster, fetch, metadata, produce};
use kaas_codec::headers::{encode_request_header, HeaderVersion};
use kaas_codec::primitives::{write_i16, write_i32, write_i64, write_i8};
use kaas_codec::tagged;
use kaas_codec::RequestHeader;
use kaas_protocol::{Dispatcher, ListenerConfig, Server, ServerConfigBuilder};
use kaas_storage::{MemoryStorage, StorageEngine};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::sync::CancellationToken;

/// Build a v2 RecordBatch carrying `num_records` records. Producer
/// id = -1 (non-idempotent) so the engine's dedupe gate never fires.
/// Same shape as `kaas_storage`'s test helper.
fn build_record_batch(num_records: i32, max_timestamp: i64) -> Bytes {
    let body_size = 49 + 16; // header + small synthetic records payload
    let total = 12 + body_size;
    let mut buf = vec![0u8; total];
    buf[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset (engine overwrites)
    let body_len_i32 = i32::try_from(body_size).unwrap();
    buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
    buf[16] = 2; // magic = 2 (v2)
    let last_offset_delta = num_records - 1;
    buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
    buf[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id = -1
    Bytes::from(buf)
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
        60,
        0,
        1,
        Arc::new(DescribeClusterHandler::new(broker.clone(), listeners)),
    );
    d.register(22, 0, 4, Arc::new(InitProducerIdHandler::new(broker)));
    Arc::new(d)
}

fn build_test_broker(topic: &str, partitions: i32) -> Arc<Broker> {
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: topic.to_owned(),
        partition_count: partitions,
        topic_id: [0; 16],
    });
    Arc::new(Broker::new(engine, topics, "kaas-smoke", 0))
}

/// Frame and send a request, read the response frame body. Returns
/// the response body bytes (without the size prefix).
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

fn produce_v9_request(topic: &str, partition: i32, batch: &[u8]) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(
        &mut body,
        &RequestHeader {
            api_key: 0,
            api_version: 9,
            correlation_id: 1,
            client_id: Some("smoke".to_owned()),
        },
        HeaderVersion::V2,
    )
    .unwrap();
    // request body
    write_nullable_str(&mut body, None, true).unwrap(); // transactional_id
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

fn fetch_v12_request(topic: &str, partition: i32, fetch_offset: i64) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(
        &mut body,
        &RequestHeader {
            api_key: 1,
            api_version: 12,
            correlation_id: 2,
            client_id: Some("smoke".to_owned()),
        },
        HeaderVersion::V2,
    )
    .unwrap();
    write_i32(&mut body, -1); // replica_id
    write_i32(&mut body, 0); // max_wait_ms
    write_i32(&mut body, 1); // min_bytes
    write_i32(&mut body, 1024 * 1024); // max_bytes
    write_i8(&mut body, 0); // isolation_level
    write_i32(&mut body, 0); // session_id
    write_i32(&mut body, -1); // session_epoch
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    write_array_len(&mut body, 1, true).unwrap();
    write_i32(&mut body, partition);
    write_i32(&mut body, -1); // current_leader_epoch (v9+)
    write_i64(&mut body, fetch_offset);
    write_i32(&mut body, -1); // last_fetched_epoch (v12+)
    write_i64(&mut body, 0); // log_start_offset (v5+)
    write_i32(&mut body, 64 * 1024); // partition_max_bytes
    tagged::write_empty(&mut body); // partition tag
    tagged::write_empty(&mut body); // topic tag
    write_array_len(&mut body, 0, true).unwrap(); // forgotten topics
    write_str(&mut body, "", true).unwrap(); // rack_id (v11+)
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

fn metadata_v9_request(topic: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(
        &mut body,
        &RequestHeader {
            api_key: 3,
            api_version: 9,
            correlation_id: 3,
            client_id: Some("smoke".to_owned()),
        },
        HeaderVersion::V2,
    )
    .unwrap();
    write_array_len(&mut body, 1, true).unwrap();
    write_str(&mut body, topic, true).unwrap();
    tagged::write_empty(&mut body); // topic tag
    write_i8(&mut body, 0); // allow_auto_topic_creation (v4+)
    write_i8(&mut body, 0); // include_cluster_authorized_operations (v8-10)
    write_i8(&mut body, 0); // include_topic_authorized_operations (v8+)
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

/// DescribeCluster v1. Flexible from v0, so the request header is V2
/// and the response header V1 — there is no legacy shape to fall back
/// on, which makes this the one key where a wrong registry header entry
/// misparses *every* request rather than only the new versions.
fn describe_cluster_v1_request() -> Vec<u8> {
    let mut body = BytesMut::new();
    encode_request_header(
        &mut body,
        &RequestHeader {
            api_key: 60,
            api_version: 1,
            correlation_id: 4,
            client_id: Some("smoke".to_owned()),
        },
        HeaderVersion::V2,
    )
    .unwrap();
    write_i8(&mut body, 1); // include_cluster_authorized_operations
    write_i8(&mut body, describe_cluster::endpoint_type::BROKER);
    tagged::write_empty(&mut body); // request tag
    body.to_vec()
}

fn skip_response_header(body: &[u8], hv: HeaderVersion) -> &[u8] {
    // [correlation_id:i32][maybe tagged-fields uvarint=0]
    let mut off = 4;
    if !matches!(hv, HeaderVersion::V0) {
        off += 1; // empty tagged block = single 0 byte uvarint
    }
    &body[off..]
}

#[tokio::test]
async fn produce_fetch_metadata_roundtrip() {
    let topic = "events";
    let listeners = vec![ListenerEntry {
        name: "internal".to_owned(),
        addr: "127.0.0.1:0".to_owned(),
        advertised_host: Some("127.0.0.1".to_owned()),
        tls: None,
        authentication_type: None,
        oauth: None,
    }];
    let broker = build_test_broker(topic, 1);
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

    let mut sock = TcpStream::connect(("127.0.0.1", port)).await.unwrap();

    // ---- Metadata ----
    let req = metadata_v9_request(topic);
    let resp = send(&mut sock, &req).await;
    let body = skip_response_header(&resp, HeaderVersion::V1); // metadata v9 flexible → V1 resp hdr
    let mut buf = Bytes::copy_from_slice(body);
    let md = metadata::decode_response(&mut buf, 9).unwrap();
    assert_eq!(md.brokers.len(), 1, "single broker advertised");
    // Note: the advertised port comes from `ListenerEntry.addr` at
    // handler-construction time, not from the kernel-assigned bind
    // port. The smoke test passes the listener config in via the
    // pre-bind path, so we don't assert on the value here — the
    // production cli config will always carry concrete ports.
    assert_eq!(md.topics[0].name, topic);
    assert_eq!(md.topics[0].partitions.len(), 1);

    // ---- DescribeCluster ----
    let req = describe_cluster_v1_request();
    let resp = send(&mut sock, &req).await;
    let body = skip_response_header(&resp, HeaderVersion::V1); // flexible → V1 resp hdr
    let mut buf = Bytes::copy_from_slice(body);
    let dc = describe_cluster::decode_response(&mut buf, 1).unwrap();
    assert_eq!(dc.error_code, 0, "describe cluster error: {dc:#?}");
    assert_eq!(dc.cluster_id, "kaas-smoke");
    assert_eq!(dc.brokers.len(), 1, "single broker advertised");
    assert_eq!(dc.brokers[0].broker_id, md.brokers[0].node_id);
    assert_eq!(
        dc.controller_id, md.controller_id,
        "the two discovery APIs must name the same controller"
    );
    assert!(
        dc.cluster_authorized_operations > 0,
        "asked for authorized ops under the default allow-all authorizer"
    );

    // ---- Produce ----
    let batch = build_record_batch(3, 1_700_000_000_000);
    let req = produce_v9_request(topic, 0, &batch);
    let resp = send(&mut sock, &req).await;
    let body = skip_response_header(&resp, HeaderVersion::V1); // produce v9 flexible → V1 resp hdr
    let mut buf = Bytes::copy_from_slice(body);
    let pr = produce::decode_response(&mut buf, 9).unwrap();
    let partition_resp = &pr.responses[0].partition_responses[0];
    assert_eq!(partition_resp.error_code, 0, "produce error: {pr:#?}");
    assert_eq!(partition_resp.base_offset, 0);

    // ---- Fetch ----
    let req = fetch_v12_request(topic, 0, 0);
    let resp = send(&mut sock, &req).await;
    let body = skip_response_header(&resp, HeaderVersion::V1); // fetch v12 flexible → V1 resp hdr
    let mut buf = Bytes::copy_from_slice(body);
    let fr = fetch::decode_response(&mut buf, 12).unwrap();
    assert_eq!(fr.session_id, 0, "stateless contract: session_id must be 0");
    let returned = fr.responses[0].partitions[0]
        .records
        .as_ref()
        .expect("fetched bytes present")
        .clone();
    // Records should round-trip byte-equal (the engine rewrites
    // base_offset bytes [0..8] to the actual assigned offset — for
    // the first batch base_offset is 0 == what build_record_batch
    // wrote, so the bytes match identically).
    assert_eq!(returned.len(), batch.len(), "byte-length mismatch");
    assert_eq!(returned, batch, "records byte-opacity violated");

    drop(sock);
    cancel.cancel();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), server_task).await;
    broker.engine.drain().await.unwrap();
}
