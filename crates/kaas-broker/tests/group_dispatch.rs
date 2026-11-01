//! Phase 5 §F end-to-end dispatch smoke.
//!
//! Wires the new consumer-group handlers into a single `Broker`
//! against a `LocalGroupSource`-backed `Manager`, then drives
//! `FindCoordinator → JoinGroup → SyncGroup → OffsetCommit →
//! OffsetFetch → Heartbeat → LeaveGroup → ListGroups` through the
//! handlers directly. Validates that:
//!
//! 1. Each handler decodes its codec request, talks to the
//!    `Manager`, and re-encodes a wire-shape response.
//! 2. A complete consumer-group lifecycle round-trips through the
//!    Phase-5 plumbing without any byte-level wire smoke (rdkafka
//!    coverage lands in workstream H).
//!
//! `start_paused = true` lets the rebalance timer trip on
//! demand — without it the test would wait 3 s for
//! `INITIAL_REBALANCE_DELAY_MS`.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kaas_auth::Principal;
use kaas_broker::{
    Broker, FindCoordinatorHandler, HeartbeatHandler, JoinGroupHandler, LeaveGroupHandler,
    ListGroupsHandler, OffsetCommitHandler, OffsetFetchHandler, SyncGroupHandler, TopicRegistry,
};
use kaas_codec::api::{
    find_coordinator, heartbeat, join_group, leave_group, list_groups, offset_commit, offset_fetch,
    sync_group,
};
use kaas_codec::primitives;
use kaas_codec::tagged;
use kaas_coordinator::{
    BrokerEndpoint, BrokerLookup, FnLookup, LocalGroupSource, Manager, OffsetStore,
};
use kaas_protocol::{ConnState, Handler};
use kaas_storage::{MemoryStorage, StorageEngine};
use parking_lot::Mutex;

const INITIAL_REBALANCE_DELAY: Duration = Duration::from_millis(3_100);

fn broker_with_manager(tmpdir: &std::path::Path) -> Arc<Broker> {
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let broker = Arc::new(Broker::new(
        engine,
        Arc::new(TopicRegistry::new()),
        "test",
        0,
    ));
    let offsets = Arc::new(OffsetStore::new(tmpdir));
    let lookup: Arc<dyn BrokerLookup> = Arc::new(FnLookup::new(|id: &str| {
        if id == "kaas-0" {
            Some(BrokerEndpoint {
                node_id: 0,
                host: "kaas-0.local".to_owned(),
                port: 9092,
            })
        } else {
            None
        }
    }));
    let mgr = Manager::new("kaas-0", offsets, lookup, LocalGroupSource::new("kaas-0"));
    broker.install_coord_manager(mgr);
    broker
}

fn conn() -> Mutex<ConnState> {
    let mut s = ConnState::new("internal", SocketAddr::from_str("127.0.0.1:9092").unwrap());
    // Stamp a principal so the handlers' `principal.name` is
    // populated (used as the client_id for the join request).
    s.principal = Some(Principal {
        name: "consumer-1".to_owned(),
        kind: kaas_auth::PrincipalKind::User,
    });
    Mutex::new(s)
}

#[tokio::test(start_paused = true)]
async fn full_consumer_group_lifecycle_through_handlers() {
    let tmp = tempfile::tempdir().unwrap();
    let broker = broker_with_manager(tmp.path());

    // ----- FindCoordinator (key 10) ----------------------------------
    let req = find_coordinator::Request {
        key: "g1".to_owned(),
        key_type: 0,
        coordinator_keys: Vec::new(),
    };
    let mut body = BytesMut::new();
    find_coordinator::encode_request(&mut body, &req, 3).unwrap();
    let h = FindCoordinatorHandler::new(broker.clone());
    let resp_bytes = h.handle(&conn(), 3, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let resp = find_coordinator::decode_response(&mut r, 3).unwrap();
    assert_eq!(resp.error_code, 0, "FindCoordinator must resolve self");
    assert_eq!(resp.host, "kaas-0.local");
    assert_eq!(resp.port, 9092);

    // ----- JoinGroup (key 11) ---------------------------------------
    let join_req = join_group::Request {
        group_id: "g1".to_owned(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: String::new(),
        group_instance_id: None,
        protocol_type: "consumer".to_owned(),
        protocols: vec![join_group::JoinGroupProtocol {
            name: "range".to_owned(),
            metadata: Bytes::from_static(b"meta"),
        }],
        reason: None,
    };
    let mut body = BytesMut::new();
    join_group::encode_request(&mut body, &join_req, 9).unwrap();
    let h_join = Arc::new(JoinGroupHandler::new(broker.clone()));
    let conn_arc = Arc::new(conn());
    let h_join_c = h_join.clone();
    let conn_c = conn_arc.clone();
    let body_b = body.freeze();
    let join_fut = tokio::spawn(async move { h_join_c.handle(&conn_c, 9, body_b).await.unwrap() });
    // Advance past the initial rebalance delay.
    tokio::time::sleep(INITIAL_REBALANCE_DELAY).await;
    let resp_bytes = join_fut.await.unwrap();
    let mut r = resp_bytes.freeze();
    let join_resp = join_group::decode_response(&mut r, 9).unwrap();
    assert_eq!(join_resp.error_code, 0, "JoinGroup must succeed");
    assert!(!join_resp.member_id.is_empty());
    assert_eq!(join_resp.generation_id, 1);
    assert_eq!(
        join_resp.leader, join_resp.member_id,
        "single member is leader"
    );

    // ----- SyncGroup (key 14) ---------------------------------------
    let sync_req = sync_group::Request {
        group_id: "g1".to_owned(),
        generation_id: join_resp.generation_id,
        member_id: join_resp.member_id.clone(),
        group_instance_id: None,
        protocol_type: None,
        protocol_name: None,
        assignments: vec![sync_group::SyncAssignment {
            member_id: join_resp.member_id.clone(),
            assignment: Some(Bytes::from_static(b"\x01\x02\x03")),
        }],
    };
    let mut body = BytesMut::new();
    sync_group::encode_request(&mut body, &sync_req, 4).unwrap();
    let h_sync = SyncGroupHandler::new(broker.clone());
    let resp_bytes = h_sync.handle(&conn(), 4, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let sync_resp = sync_group::decode_response(&mut r, 4).unwrap();
    assert_eq!(sync_resp.error_code, 0);
    assert_eq!(sync_resp.assignment.as_ref(), b"\x01\x02\x03");

    // ----- Heartbeat (key 12) ---------------------------------------
    let hb_req = heartbeat::Request {
        group_id: "g1".to_owned(),
        generation_id: join_resp.generation_id,
        member_id: join_resp.member_id.clone(),
        group_instance_id: None,
    };
    let mut body = BytesMut::new();
    heartbeat::encode_request(&mut body, &hb_req, 4).unwrap();
    let h_hb = HeartbeatHandler::new(broker.clone());
    let resp_bytes = h_hb.handle(&conn(), 4, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let hb_resp = heartbeat::decode_response(&mut r, 4).unwrap();
    assert_eq!(hb_resp.error_code, 0, "Heartbeat in Stable returns NONE");

    // ----- OffsetCommit (key 8) -------------------------------------
    let oc_req = offset_commit::Request {
        group_id: "g1".to_owned(),
        generation_id: join_resp.generation_id,
        member_id: join_resp.member_id.clone(),
        group_instance_id: None,
        retention_time_ms: offset_commit::DEFAULT_RETENTION_TIME_MS,
        topics: vec![offset_commit::OffsetCommitTopic {
            name: "t1".to_owned(),
            partitions: vec![offset_commit::OffsetCommitPartition {
                partition_index: 0,
                committed_offset: 42,
                committed_leader_epoch: -1,
                committed_metadata: Some("manual".to_owned()),
            }],
        }],
    };
    let mut body = BytesMut::new();
    offset_commit::encode_request(&mut body, &oc_req, 8).unwrap();
    let h_oc = OffsetCommitHandler::new(broker.clone());
    let resp_bytes = h_oc.handle(&conn(), 8, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let oc_resp = offset_commit::decode_response(&mut r, 8).unwrap();
    assert_eq!(oc_resp.topics[0].partitions[0].error_code, 0);

    // ----- OffsetFetch (key 9) --------------------------------------
    let of_req = offset_fetch::Request {
        group_id: "g1".to_owned(),
        topics: Some(vec![offset_fetch::OffsetFetchTopic {
            name: "t1".to_owned(),
            partition_indexes: vec![0],
        }]),
        groups: Vec::new(),
        require_stable: false,
    };
    let mut body = BytesMut::new();
    offset_fetch::encode_request(&mut body, &of_req, 5).unwrap();
    let h_of = OffsetFetchHandler::new(broker.clone());
    let resp_bytes = h_of.handle(&conn(), 5, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let of_resp = offset_fetch::decode_response(&mut r, 5).unwrap();
    assert_eq!(of_resp.topics[0].partitions[0].committed_offset, 42);
    assert_eq!(
        of_resp.topics[0].partitions[0].metadata.as_deref(),
        Some("manual"),
        "OffsetCommit metadata must round trip"
    );

    // ----- ListGroups (key 16) --------------------------------------
    let mut body = BytesMut::new();
    // Empty filter; encode an empty states_filter array.
    primitives::write_compact_array_len(&mut body, 0).unwrap();
    tagged::write_empty(&mut body);
    let h_lg = ListGroupsHandler::new(broker.clone());
    let resp_bytes = h_lg.handle(&conn(), 4, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let lg_resp = list_groups::decode_response(&mut r, 4).unwrap();
    assert_eq!(lg_resp.groups.len(), 1);
    assert_eq!(lg_resp.groups[0].group_id, "g1");

    // ----- LeaveGroup (key 13) --------------------------------------
    let lg_req = leave_group::Request {
        group_id: "g1".to_owned(),
        member_id: String::new(),
        members: vec![leave_group::LeaveMember {
            member_id: join_resp.member_id.clone(),
            group_instance_id: None,
            reason: None,
        }],
    };
    let mut body = BytesMut::new();
    leave_group::encode_request(&mut body, &lg_req, 4).unwrap();
    let h_leave = LeaveGroupHandler::new(broker.clone());
    let resp_bytes = h_leave.handle(&conn(), 4, body.freeze()).await.unwrap();
    let mut r = resp_bytes.freeze();
    let leave_resp = leave_group::decode_response(&mut r, 4).unwrap();
    assert_eq!(leave_resp.error_code, 0);
    assert_eq!(leave_resp.members[0].error_code, 0);
}

/// Kafka 4's `kafka-consumer-groups --describe` asks for a group's
/// offsets with a null topic list. Run it the way the live cluster
/// meets it: offsets committed, then the coordinator restarts and
/// nobody rejoins (an idle group), so the cache starts cold.
#[tokio::test]
async fn null_topic_offset_fetch_lists_every_committed_partition_after_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let broker = broker_with_manager(tmp.path());
    let commit = |topic: &str, partition: i32, offset: i64, meta: Option<&str>| {
        offset_commit::OffsetCommitTopic {
            name: topic.to_owned(),
            partitions: vec![offset_commit::OffsetCommitPartition {
                partition_index: partition,
                committed_offset: offset,
                committed_leader_epoch: -1,
                committed_metadata: meta.map(str::to_owned),
            }],
        }
    };
    let oc_req = offset_commit::Request {
        group_id: "g1".to_owned(),
        generation_id: -1,
        member_id: String::new(),
        group_instance_id: None,
        retention_time_ms: offset_commit::DEFAULT_RETENTION_TIME_MS,
        topics: vec![
            commit("t2", 1, 3, None),
            commit("t1", 2, 7, None),
            commit("t1", 0, 42, Some("m")),
        ],
    };
    let mut body = BytesMut::new();
    offset_commit::encode_request(&mut body, &oc_req, 8).unwrap();
    let resp_bytes = OffsetCommitHandler::new(broker)
        .handle(&conn(), 8, body.freeze())
        .await
        .unwrap();
    let oc_resp = offset_commit::decode_response(&mut resp_bytes.freeze(), 8).unwrap();
    assert!(oc_resp
        .topics
        .iter()
        .all(|t| t.partitions.iter().all(|p| p.error_code == 0)));

    // A fresh broker over the same offsets directory: nothing cached.
    let restarted = broker_with_manager(tmp.path());
    let h = OffsetFetchHandler::new(restarted);
    let want = |topics: &[offset_fetch::OffsetFetchTopicResponse]| {
        let got: Vec<(String, i32, i64, Option<String>)> = topics
            .iter()
            .flat_map(|t| {
                t.partitions.iter().map(|p| {
                    assert_eq!(p.error_code, 0);
                    (
                        t.name.clone(),
                        p.partition_index,
                        p.committed_offset,
                        p.metadata.clone(),
                    )
                })
            })
            .collect();
        assert_eq!(
            got,
            [
                ("t1".to_owned(), 0, 42, Some("m".to_owned())),
                ("t1".to_owned(), 2, 7, None),
                ("t2".to_owned(), 1, 3, None),
            ]
        );
    };

    // v8, the version Kafka 4's AdminClient negotiates.
    let req = offset_fetch::Request {
        groups: vec![offset_fetch::OffsetFetchGroup {
            group_id: "g1".to_owned(),
            topics: None,
        }],
        ..offset_fetch::Request::default()
    };
    let mut body = BytesMut::new();
    offset_fetch::encode_request(&mut body, &req, 8).unwrap();
    let resp_bytes = h.handle(&conn(), 8, body.freeze()).await.unwrap();
    let resp = offset_fetch::decode_response(&mut resp_bytes.freeze(), 8).unwrap();
    assert_eq!(resp.groups[0].error_code, 0);
    want(&resp.groups[0].topics);

    // The single-group form carries the same null.
    let req = offset_fetch::Request {
        group_id: "g1".to_owned(),
        topics: None,
        ..offset_fetch::Request::default()
    };
    let mut body = BytesMut::new();
    offset_fetch::encode_request(&mut body, &req, 5).unwrap();
    let resp_bytes = h.handle(&conn(), 5, body.freeze()).await.unwrap();
    let resp = offset_fetch::decode_response(&mut resp_bytes.freeze(), 5).unwrap();
    assert_eq!(resp.error_code, 0);
    want(&resp.topics);
}
