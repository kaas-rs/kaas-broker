//! gh #268: topic discovery and the consumer-group APIs under
//! `authorization: simple`.
//!
//! Reproduces the live-cluster observation with the real ACL engine
//! rather than a test double: `bob-mtls` holds exactly one rule, `topic
//! audit-log: Write, Describe`, and `alice` owns the consumer group
//! `payments-app` with a committed offset on `payments`. Every test is
//! one row of the issue's table — bob asks for something his ACLs don't
//! grant, and the assertion is Apache Kafka 3.7's answer.
//!
//! Where the missing gate has a side effect beyond the error code (an
//! offset overwritten, a member evicted, a group deleted), the test
//! also checks the state the denial should have protected.
//!
//! The other principals guard the fix from the opposite side. `carol`
//! is a Strimzi-shaped consumer holding only `Read` grants, which work
//! only through Apache's `Read` ⇒ `Describe` implication. `ops` and
//! `auditor` exercise the per-topic and cluster-wide halves.
//!
//! The filtering rows also check which denials get *reported*. A
//! listing that leaves a topic or group out is answering the request,
//! so it must check through `authorize_quietly`; the live canary,
//! allowed 1 of 9 topics, logged 8 `acl: denied` warnings on every
//! metadata refresh until it did. [`Audited`] records every denial that
//! came through the reporting `authorize` instead.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use kaas_auth::{
    AclEngine, Authorizer, NoQuotaChecker, Operation, Principal, PrincipalKind, Resource,
};
use kaas_broker::{
    Broker, DeleteGroupsHandler, DescribeGroupsHandler, FindCoordinatorHandler, HeartbeatHandler,
    JoinGroupHandler, LeaveGroupHandler, ListGroupsHandler, ListOffsetsHandler, ListenerEntry,
    MetadataHandler, OffsetCommitHandler, OffsetDeleteHandler, OffsetFetchHandler,
    SyncGroupHandler, TopicMeta, TopicRegistry,
};
use kaas_codec::api::common::{write_array_len, write_str};
use kaas_codec::api::{
    delete_groups, describe_groups, find_coordinator, heartbeat, join_group, leave_group,
    list_groups, list_offsets, metadata, offset_commit, offset_delete, offset_fetch, sync_group,
};
use kaas_codec::primitives::{write_i32, write_i64, write_i8, write_uvarint};
use kaas_codec::tagged;
use kaas_coordinator::{
    BrokerEndpoint, BrokerLookup, FnLookup, LocalGroupSource, LocalTxnSource, Manager, OffsetStore,
};
use kaas_protocol::{ConnState, Handler};
use kaas_storage::{MemoryStorage, StorageEngine};
use parking_lot::Mutex;

const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;
const ERR_TXN_ID_AUTHZ_FAILED: i16 = 53;

const INITIAL_REBALANCE_DELAY: Duration = Duration::from_millis(3_100);

const BOB: &str = "bob-mtls";
const ALICE: &str = "alice";
const CAROL: &str = "carol";
const OPS: &str = "ops";
const AUDITOR: &str = "auditor";
const GROUP: &str = "payments-app";
const ALICE_OFFSET: i64 = 42;

/// `bob-mtls` is verbatim from the issue. `alice` is the other team:
/// everything on topics and groups, so her setup keeps working once
/// the handlers start asking. `carol` consumes `payments` in alice's
/// group with `Read` alone; `ops` may delete the group's offsets for
/// `payments` only; `auditor` holds `Describe` on the cluster.
const ACLS: &str = r#"{"acls":[
    {"principal":"User:bob-mtls",
     "resource":{"type":"topic","name":"audit-log","patternType":"literal"},
     "operations":["Write","Describe"],"permission":"Allow"},
    {"principal":"User:alice",
     "resource":{"type":"topic","name":"*","patternType":"literal"},
     "operations":["All"],"permission":"Allow"},
    {"principal":"User:alice",
     "resource":{"type":"group","name":"*","patternType":"literal"},
     "operations":["All"],"permission":"Allow"},
    {"principal":"User:carol",
     "resource":{"type":"topic","name":"payments","patternType":"literal"},
     "operations":["Read"],"permission":"Allow"},
    {"principal":"User:carol",
     "resource":{"type":"group","name":"payments-app","patternType":"literal"},
     "operations":["Read"],"permission":"Allow"},
    {"principal":"User:ops",
     "resource":{"type":"topic","name":"payments","patternType":"literal"},
     "operations":["Read"],"permission":"Allow"},
    {"principal":"User:ops",
     "resource":{"type":"group","name":"payments-app","patternType":"literal"},
     "operations":["Delete"],"permission":"Allow"},
    {"principal":"User:auditor",
     "resource":{"type":"cluster","name":"kafka-cluster","patternType":"literal"},
     "operations":["Describe"],"permission":"Allow"}
]}"#;

struct Cluster {
    _tmp: tempfile::TempDir,
    broker: Arc<Broker>,
    audit: Arc<Audited>,
}

/// The real ACL engine, plus a record of the denials that went through
/// the reporting entry point (`kind/name`, in order).
#[derive(Debug)]
struct Audited {
    acls: AclEngine,
    reported: Mutex<Vec<String>>,
}

impl Authorizer for Audited {
    fn authorize(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool {
        let allowed = self.acls.authorize(principal, resource, op);
        if !allowed {
            self.reported
                .lock()
                .push(format!("{}/{}", resource.kind.as_str(), resource.name));
        }
        allowed
    }

    fn authorize_quietly(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool {
        self.acls.authorize_quietly(principal, resource, op)
    }
}

impl Cluster {
    fn reported_denials(&self) -> Vec<String> {
        self.audit.reported.lock().clone()
    }
}

/// Alice's membership in `payments-app`, as her client knows it.
struct Member {
    id: String,
    generation: i32,
}

fn cluster() -> Cluster {
    let tmp = tempfile::tempdir().unwrap();
    let acls = AclEngine::new(tmp.path().join("acls.json"));
    acls.install_for_test(ACLS);
    let audit = Arc::new(Audited {
        acls,
        reported: Mutex::new(Vec::new()),
    });

    let topics = Arc::new(TopicRegistry::new());
    for (name, partition_count) in [("audit-log", 1), ("payments", 3), ("orders", 2)] {
        topics.insert(TopicMeta {
            name: name.to_owned(),
            partition_count,
            topic_id: [0; 16],
        });
    }
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let broker = Arc::new(Broker::with_auth(
        engine,
        topics,
        "test",
        0,
        audit.clone(),
        Arc::new(NoQuotaChecker),
    ));

    let lookup: Arc<dyn BrokerLookup> = Arc::new(FnLookup::new(|id: &str| {
        (id == "kaas-0").then(|| BrokerEndpoint {
            node_id: 0,
            host: "kaas-0.local".to_owned(),
            port: 9093,
        })
    }));
    let mgr = Manager::new(
        "kaas-0",
        Arc::new(OffsetStore::new(tmp.path())),
        lookup,
        LocalGroupSource::new("kaas-0"),
    );
    mgr.set_txn_assignment_source(LocalTxnSource::new("kaas-0"));
    broker.install_coord_manager(mgr);

    Cluster {
        _tmp: tmp,
        broker,
        audit,
    }
}

fn conn(user: &str) -> Mutex<ConnState> {
    let mut s = ConnState::new("mtls", SocketAddr::from_str("127.0.0.1:9093").unwrap());
    s.principal = Some(Principal {
        name: user.to_owned(),
        kind: PrincipalKind::User,
    });
    Mutex::new(s)
}

async fn call(h: &impl Handler, user: &str, version: i16, body: BytesMut) -> Bytes {
    h.handle(&conn(user), version, body.freeze())
        .await
        .unwrap()
        .freeze()
}

/// Alice joins `payments-app`, syncs, and commits `payments/0`.
async fn alice_joins_and_commits(c: &Cluster) -> Member {
    let req = join_group::Request {
        group_id: GROUP.to_owned(),
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
    join_group::encode_request(&mut body, &req, 9).unwrap();
    let h = JoinGroupHandler::new(c.broker.clone());
    let join = tokio::spawn(async move { call(&h, ALICE, 9, body).await });
    tokio::time::sleep(INITIAL_REBALANCE_DELAY).await;
    let resp = join_group::decode_response(&mut join.await.unwrap(), 9).unwrap();
    assert_eq!(resp.error_code, 0, "alice's JoinGroup");
    let alice = Member {
        id: resp.member_id,
        generation: resp.generation_id,
    };

    let req = sync_group::Request {
        group_id: GROUP.to_owned(),
        generation_id: alice.generation,
        member_id: alice.id.clone(),
        group_instance_id: None,
        protocol_type: None,
        protocol_name: None,
        assignments: vec![sync_group::SyncAssignment {
            member_id: alice.id.clone(),
            assignment: Some(Bytes::from_static(b"payments-0,1,2")),
        }],
    };
    let mut body = BytesMut::new();
    sync_group::encode_request(&mut body, &req, 4).unwrap();
    let h = SyncGroupHandler::new(c.broker.clone());
    let resp = sync_group::decode_response(&mut call(&h, ALICE, 4, body).await, 4).unwrap();
    assert_eq!(resp.error_code, 0, "alice's SyncGroup");

    let resp = offset_commit_as(c, ALICE, &alice, &[("payments", ALICE_OFFSET)]).await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code, 0,
        "alice's OffsetCommit"
    );
    alice
}

/// Alice's app stops: the group goes `Empty` but keeps its offsets —
/// the shape `DeleteGroups` and `OffsetDelete` act on.
async fn alice_leaves(c: &Cluster, alice: &Member) {
    let resp = leave_group_as(c, ALICE, alice).await;
    assert_eq!(resp.error_code, 0, "alice's LeaveGroup");
}

/// Alice's committed `payments/0`, read the way her own client would.
async fn alices_offset(c: &Cluster) -> i64 {
    alices_offset_for(c, "payments").await
}

async fn alices_offset_for(c: &Cluster, topic: &str) -> i64 {
    let resp = offset_fetch_as(c, ALICE, &[topic]).await;
    assert_eq!(resp.error_code, 0, "alice's OffsetFetch");
    resp.topics[0].partitions[0].committed_offset
}

// --- request builders shared by bob and alice ------------------------

fn metadata_request(topics: Option<&[&str]>) -> BytesMut {
    let mut w = BytesMut::new();
    match topics {
        // A null list is "all topics" — what AdminClient.listTopics
        // (hence `kafka-topics.sh --list`) sends.
        None => write_uvarint(&mut w, 0),
        Some(names) => {
            write_array_len(&mut w, names.len(), true).unwrap();
            for n in names {
                write_str(&mut w, n, true).unwrap();
                tagged::write_empty(&mut w);
            }
        }
    }
    write_i8(&mut w, 0); // allow_auto_topic_creation
    write_i8(&mut w, 0); // include_cluster_authorized_operations
    write_i8(&mut w, 0); // include_topic_authorized_operations
    tagged::write_empty(&mut w);
    w
}

async fn metadata_as(c: &Cluster, user: &str, topics: Option<&[&str]>) -> metadata::Response {
    let listeners = [ListenerEntry {
        name: "mtls".to_owned(),
        addr: "0.0.0.0:9093".to_owned(),
        advertised_host: None,
        tls: None,
        authentication_type: None,
        oauth: None,
    }];
    let h = MetadataHandler::new(c.broker.clone(), &listeners);
    let mut out = call(&h, user, 9, metadata_request(topics)).await;
    metadata::decode_response(&mut out, 9).unwrap()
}

async fn list_offsets_as(c: &Cluster, user: &str, topic: &str) -> list_offsets::Response {
    let mut w = BytesMut::new();
    write_i32(&mut w, -1); // replica_id
    write_i8(&mut w, 0); // isolation_level
    write_array_len(&mut w, 1, true).unwrap();
    write_str(&mut w, topic, true).unwrap();
    write_array_len(&mut w, 1, true).unwrap();
    write_i32(&mut w, 0); // partition_index
    write_i32(&mut w, -1); // current_leader_epoch
    write_i64(&mut w, -1); // latest
    tagged::write_empty(&mut w);
    tagged::write_empty(&mut w);
    tagged::write_empty(&mut w);
    let h = ListOffsetsHandler::new(c.broker.clone());
    let mut out = call(&h, user, 6, w).await;
    list_offsets::decode_response(&mut out, 6).unwrap()
}

async fn offset_fetch_as(c: &Cluster, user: &str, topics: &[&str]) -> offset_fetch::Response {
    let req = offset_fetch::Request {
        group_id: GROUP.to_owned(),
        topics: Some(
            topics
                .iter()
                .map(|t| offset_fetch::OffsetFetchTopic {
                    name: (*t).to_owned(),
                    partition_indexes: vec![0],
                })
                .collect(),
        ),
        groups: Vec::new(),
        require_stable: false,
    };
    let mut body = BytesMut::new();
    offset_fetch::encode_request(&mut body, &req, 5).unwrap();
    let h = OffsetFetchHandler::new(c.broker.clone());
    let mut out = call(&h, user, 5, body).await;
    offset_fetch::decode_response(&mut out, 5).unwrap()
}

async fn offset_commit_as(
    c: &Cluster,
    user: &str,
    member: &Member,
    offsets: &[(&str, i64)],
) -> offset_commit::Response {
    let req = offset_commit::Request {
        group_id: GROUP.to_owned(),
        generation_id: member.generation,
        member_id: member.id.clone(),
        group_instance_id: None,
        retention_time_ms: offset_commit::DEFAULT_RETENTION_TIME_MS,
        topics: offsets
            .iter()
            .map(|(topic, offset)| offset_commit::OffsetCommitTopic {
                name: (*topic).to_owned(),
                partitions: vec![offset_commit::OffsetCommitPartition {
                    partition_index: 0,
                    committed_offset: *offset,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                }],
            })
            .collect(),
    };
    let mut body = BytesMut::new();
    offset_commit::encode_request(&mut body, &req, 8).unwrap();
    let h = OffsetCommitHandler::new(c.broker.clone());
    let mut out = call(&h, user, 8, body).await;
    offset_commit::decode_response(&mut out, 8).unwrap()
}

async fn find_coordinator_as(
    c: &Cluster,
    user: &str,
    key: &str,
    key_type: i8,
) -> find_coordinator::Response {
    let req = find_coordinator::Request {
        key: key.to_owned(),
        key_type,
        coordinator_keys: Vec::new(),
    };
    let mut body = BytesMut::new();
    find_coordinator::encode_request(&mut body, &req, 3).unwrap();
    let h = FindCoordinatorHandler::new(c.broker.clone());
    let mut out = call(&h, user, 3, body).await;
    find_coordinator::decode_response(&mut out, 3).unwrap()
}

async fn leave_group_as(c: &Cluster, user: &str, member: &Member) -> leave_group::Response {
    let req = leave_group::Request {
        group_id: GROUP.to_owned(),
        member_id: String::new(),
        members: vec![leave_group::LeaveMember {
            member_id: member.id.clone(),
            group_instance_id: None,
            reason: None,
        }],
    };
    let mut body = BytesMut::new();
    leave_group::encode_request(&mut body, &req, 4).unwrap();
    let h = LeaveGroupHandler::new(c.broker.clone());
    let mut out = call(&h, user, 4, body).await;
    leave_group::decode_response(&mut out, 4).unwrap()
}

async fn describe_group_as(c: &Cluster, user: &str) -> describe_groups::DescribedGroup {
    describe_group_with_ops_as(c, user, false).await
}

async fn describe_group_with_ops_as(
    c: &Cluster,
    user: &str,
    include_authorized_operations: bool,
) -> describe_groups::DescribedGroup {
    let req = describe_groups::Request {
        groups: vec![GROUP.to_owned()],
        include_authorized_operations,
    };
    let mut body = BytesMut::new();
    describe_groups::encode_request(&mut body, &req, 5).unwrap();
    let h = DescribeGroupsHandler::new(c.broker.clone());
    let mut out = call(&h, user, 5, body).await;
    describe_groups::decode_response(&mut out, 5)
        .unwrap()
        .groups
        .remove(0)
}

async fn list_groups_as(c: &Cluster, user: &str) -> Vec<String> {
    let mut body = BytesMut::new();
    list_groups::encode_request(&mut body, &list_groups::Request::default(), 4).unwrap();
    let h = ListGroupsHandler::new(c.broker.clone());
    let resp = list_groups::decode_response(&mut call(&h, user, 4, body).await, 4).unwrap();
    assert_eq!(resp.error_code, 0);
    resp.groups.into_iter().map(|g| g.group_id).collect()
}

async fn offset_delete_as(c: &Cluster, user: &str, topics: &[&str]) -> offset_delete::Response {
    let req = offset_delete::Request {
        group_id: GROUP.to_owned(),
        topics: topics
            .iter()
            .map(|t| offset_delete::OffsetDeleteTopic {
                name: (*t).to_owned(),
                partitions: vec![0],
            })
            .collect(),
    };
    let mut body = BytesMut::new();
    offset_delete::encode_request(&mut body, &req, 0).unwrap();
    let h = OffsetDeleteHandler::new(c.broker.clone());
    offset_delete::decode_response(&mut call(&h, user, 0, body).await, 0).unwrap()
}

async fn heartbeat_as(c: &Cluster, user: &str, member: &Member) -> heartbeat::Response {
    let req = heartbeat::Request {
        group_id: GROUP.to_owned(),
        generation_id: member.generation,
        member_id: member.id.clone(),
        group_instance_id: None,
    };
    let mut body = BytesMut::new();
    heartbeat::encode_request(&mut body, &req, 4).unwrap();
    let h = HeartbeatHandler::new(c.broker.clone());
    let mut out = call(&h, user, 4, body).await;
    heartbeat::decode_response(&mut out, 4).unwrap()
}

// --- control: bob's one grant still works ----------------------------

/// Guards the fix against over-denying: `Describe` on `audit-log` is
/// what bob does hold, so discovery of that one topic must succeed.
#[tokio::test(start_paused = true)]
async fn bob_still_sees_the_topic_he_may_describe() {
    let c = cluster();
    let resp = metadata_as(&c, BOB, Some(&["audit-log"])).await;
    assert_eq!(resp.topics[0].error_code, 0);
    assert_eq!(resp.topics[0].partitions.len(), 1);

    let resp = list_offsets_as(&c, BOB, "audit-log").await;
    assert_eq!(resp.topics[0].partitions[0].error_code, 0);
}

// --- topic discovery -------------------------------------------------

/// The live observation: `kafka-topics.sh --list` as bob. Strimzi
/// answers `audit-log`; Apache drops undescribable topics from an
/// all-topics request silently, it doesn't error.
#[tokio::test(start_paused = true)]
async fn metadata_all_topics_lists_only_what_bob_may_describe() {
    let c = cluster();
    let resp = metadata_as(&c, BOB, None).await;
    let mut names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["audit-log"], "topic list leaked to bob");
    assert_eq!(c.reported_denials(), Vec::<String>::new());
}

/// A named topic without `Describe` answers 29 whether or not it
/// exists, so the error code doesn't reveal existence either.
#[tokio::test(start_paused = true)]
async fn metadata_named_topic_without_describe_is_29_existing_or_not() {
    let c = cluster();
    let resp = metadata_as(&c, BOB, Some(&["payments", "no-such-topic"])).await;
    for t in &resp.topics {
        assert_eq!(t.error_code, ERR_TOPIC_AUTHZ_FAILED, "topic {}", t.name);
        assert!(t.partitions.is_empty(), "partitions of {} leaked", t.name);
    }
    // Named topics are a refusal, so unlike the listing they report.
    assert_eq!(
        c.reported_denials(),
        ["topic/payments", "topic/no-such-topic"]
    );
}

#[tokio::test(start_paused = true)]
async fn list_offsets_without_describe_is_29() {
    let c = cluster();
    let resp = list_offsets_as(&c, BOB, "payments").await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        ERR_TOPIC_AUTHZ_FAILED
    );
}

// --- consumer-group APIs ---------------------------------------------

#[tokio::test(start_paused = true)]
async fn offset_fetch_of_another_group_is_30() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let resp = offset_fetch_as(&c, BOB, &["payments"]).await;
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert!(
        resp.topics
            .iter()
            .flat_map(|t| &t.partitions)
            .all(|p| p.committed_offset != ALICE_OFFSET),
        "alice's committed offset leaked to bob"
    );
}

#[tokio::test(start_paused = true)]
async fn offset_commit_into_another_group_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;

    let resp = offset_commit_as(&c, BOB, &alice, &[("payments", 0)]).await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        ERR_GROUP_AUTHZ_FAILED
    );
    assert_eq!(
        alices_offset(&c).await,
        ALICE_OFFSET,
        "bob rewound alice's group"
    );
}

#[tokio::test(start_paused = true)]
async fn find_coordinator_for_another_group_is_30() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let resp = find_coordinator_as(&c, BOB, GROUP, 0).await;
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert!(resp.host.is_empty(), "coordinator routing revealed to bob");
}

#[tokio::test(start_paused = true)]
async fn find_coordinator_for_a_txn_id_without_describe_is_53() {
    let c = cluster();
    let resp = find_coordinator_as(&c, BOB, "payments-txn", 1).await;
    assert_eq!(resp.error_code, ERR_TXN_ID_AUTHZ_FAILED);
    assert!(resp.host.is_empty(), "coordinator routing revealed to bob");
}

#[tokio::test(start_paused = true)]
async fn join_another_group_is_30() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let req = join_group::Request {
        group_id: GROUP.to_owned(),
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
    join_group::encode_request(&mut body, &req, 9).unwrap();
    let h = JoinGroupHandler::new(c.broker.clone());
    // Ungated, bob's join parks in the rebalance it forced on alice's
    // group; paused time runs that out instead of hanging the test.
    let mut out = tokio::time::timeout(Duration::from_secs(120), call(&h, BOB, 9, body))
        .await
        .expect("bob's JoinGroup is still parked in a rebalance of alice's group");
    let resp = join_group::decode_response(&mut out, 9).unwrap();
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
}

#[tokio::test(start_paused = true)]
async fn sync_another_group_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;

    let req = sync_group::Request {
        group_id: GROUP.to_owned(),
        generation_id: alice.generation,
        member_id: alice.id.clone(),
        group_instance_id: None,
        protocol_type: None,
        protocol_name: None,
        assignments: Vec::new(),
    };
    let mut body = BytesMut::new();
    sync_group::encode_request(&mut body, &req, 4).unwrap();
    let h = SyncGroupHandler::new(c.broker.clone());
    let resp = sync_group::decode_response(&mut call(&h, BOB, 4, body).await, 4).unwrap();
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert!(
        resp.assignment.is_empty(),
        "alice's assignment leaked to bob"
    );
}

#[tokio::test(start_paused = true)]
async fn heartbeat_into_another_group_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;

    let resp = heartbeat_as(&c, BOB, &alice).await;
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
}

#[tokio::test(start_paused = true)]
async fn leave_group_on_another_groups_member_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;

    let resp = leave_group_as(&c, BOB, &alice).await;
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert_eq!(
        heartbeat_as(&c, ALICE, &alice).await.error_code,
        0,
        "bob evicted alice from her own group"
    );
}

#[tokio::test(start_paused = true)]
async fn describe_another_group_is_30() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let group = describe_group_as(&c, BOB).await;
    assert_eq!(group.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert!(group.members.is_empty(), "alice's membership leaked to bob");
}

/// gh #284: DescribeGroups v3+ `authorized_operations` is Apache's
/// bitfield over the GROUP resource's supported operations {READ(3),
/// DELETE(6), DESCRIBE(8)}, computed only when asked and only for a
/// group that described successfully. The literals are Apache's, not
/// derived from the handler's own table, so drift in it can't hide.
#[tokio::test(start_paused = true)]
async fn describe_group_authorized_operations_are_apaches_bitfield() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    // All on the group: READ | DELETE | DESCRIBE.
    let g = describe_group_with_ops_as(&c, ALICE, true).await;
    assert_eq!(g.error_code, 0);
    assert_eq!(g.authorized_operations, 328);
    // Read alone, which implies Describe.
    let g = describe_group_with_ops_as(&c, CAROL, true).await;
    assert_eq!(g.error_code, 0);
    assert_eq!(g.authorized_operations, 264);
    // Delete alone, which implies Describe too.
    let g = describe_group_with_ops_as(&c, OPS, true).await;
    assert_eq!(g.error_code, 0);
    assert_eq!(g.authorized_operations, 320);

    // Not asked: INT_MIN ("not computed"), never 0 ("nothing allowed").
    let g = describe_group_with_ops_as(&c, ALICE, false).await;
    assert_eq!(g.authorized_operations, i32::MIN);
    // A denied group keeps the default even when asked.
    let g = describe_group_with_ops_as(&c, BOB, true).await;
    assert_eq!(g.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert_eq!(g.authorized_operations, i32::MIN);

    // The bits carol and ops lack are clear bits, not audited denials;
    // only bob's named request is reported.
    assert_eq!(c.reported_denials().len(), 1, "{:?}", c.reported_denials());
}

/// Without `Describe` on the cluster, Apache lists only the groups the
/// principal may `Describe` — a filter, not an error.
#[tokio::test(start_paused = true)]
async fn list_groups_hides_groups_bob_may_not_describe() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let ids = list_groups_as(&c, BOB).await;
    assert!(ids.is_empty(), "groups leaked to bob: {ids:?}");
    // Neither the cluster-wide shortcut nor the per-group filter is a
    // refusal.
    assert_eq!(c.reported_denials(), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn delete_another_group_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;
    alice_leaves(&c, &alice).await;

    let req = delete_groups::Request {
        group_names: vec![GROUP.to_owned()],
    };
    let mut body = BytesMut::new();
    delete_groups::encode_request(&mut body, &req, 2).unwrap();
    let h = DeleteGroupsHandler::new(c.broker.clone());
    let resp = delete_groups::decode_response(&mut call(&h, BOB, 2, body).await, 2).unwrap();
    assert_eq!(resp.results[0].error_code, ERR_GROUP_AUTHZ_FAILED);
    assert_eq!(
        alices_offset(&c).await,
        ALICE_OFFSET,
        "bob deleted alice's group"
    );
}

#[tokio::test(start_paused = true)]
async fn offset_delete_in_another_group_is_30() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;
    alice_leaves(&c, &alice).await;

    let resp = offset_delete_as(&c, BOB, &["payments"]).await;
    assert_eq!(resp.error_code, ERR_GROUP_AUTHZ_FAILED);
    assert_eq!(
        alices_offset(&c).await,
        ALICE_OFFSET,
        "bob deleted alice's committed offset"
    );
}

// --- the fix must not over-deny --------------------------------------

/// The common Strimzi consumer shape (the live `throttled-user` has
/// it): `Read` on the topic and the group, no explicit `Describe`.
/// Every `Describe` gate below passes only through Apache's
/// `Read` ⇒ `Describe` implication.
#[tokio::test(start_paused = true)]
async fn a_read_only_consumer_still_discovers_and_fetches() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    let resp = metadata_as(&c, CAROL, Some(&["payments"])).await;
    assert_eq!(resp.topics[0].error_code, 0, "Metadata");
    let resp = list_offsets_as(&c, CAROL, "payments").await;
    assert_eq!(resp.topics[0].partitions[0].error_code, 0, "ListOffsets");
    let resp = find_coordinator_as(&c, CAROL, GROUP, 0).await;
    assert_eq!(resp.error_code, 0, "FindCoordinator");
    let resp = offset_fetch_as(&c, CAROL, &["payments"]).await;
    assert_eq!(resp.error_code, 0, "OffsetFetch");
    assert_eq!(resp.topics[0].partitions[0].committed_offset, ALICE_OFFSET);
    assert_eq!(
        describe_group_as(&c, CAROL).await.error_code,
        0,
        "DescribeGroups"
    );
    assert_eq!(list_groups_as(&c, CAROL).await, [GROUP], "ListGroups");
}

/// Group access doesn't extend to topics: a commit or fetch for a
/// topic carol can't `Read` / `Describe` is refused per topic while
/// the rest of the request goes through.
#[tokio::test(start_paused = true)]
async fn offset_commit_and_fetch_deny_per_topic() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;

    let resp = offset_commit_as(&c, CAROL, &alice, &[("payments", 50), ("orders", 7)]).await;
    for t in &resp.topics {
        let want = if t.name == "orders" {
            ERR_TOPIC_AUTHZ_FAILED
        } else {
            0
        };
        assert_eq!(t.partitions[0].error_code, want, "commit {}", t.name);
    }
    assert_eq!(alices_offset(&c).await, 50, "the authorized topic commits");
    assert_eq!(
        alices_offset_for(&c, "orders").await,
        -1,
        "the denied topic must not commit"
    );

    let resp = offset_fetch_as(&c, CAROL, &["payments", "orders"]).await;
    assert_eq!(resp.error_code, 0);
    for t in &resp.topics {
        let want = if t.name == "orders" {
            ERR_TOPIC_AUTHZ_FAILED
        } else {
            0
        };
        assert_eq!(t.partitions[0].error_code, want, "fetch {}", t.name);
    }
}

#[tokio::test(start_paused = true)]
async fn offset_delete_denies_per_topic() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;
    let resp = offset_commit_as(&c, ALICE, &alice, &[("orders", 9)]).await;
    assert_eq!(resp.topics[0].partitions[0].error_code, 0);
    alice_leaves(&c, &alice).await;

    let resp = offset_delete_as(&c, OPS, &["payments", "orders"]).await;
    assert_eq!(resp.error_code, 0);
    for t in &resp.topics {
        let want = if t.name == "orders" {
            ERR_TOPIC_AUTHZ_FAILED
        } else {
            0
        };
        assert_eq!(t.partitions[0].error_code, want, "topic {}", t.name);
    }
    assert_eq!(
        alices_offset(&c).await,
        -1,
        "the authorized topic is deleted"
    );
    assert_eq!(
        alices_offset_for(&c, "orders").await,
        9,
        "the denied topic keeps its offset"
    );
}

/// Apache's ListGroups shortcut: `Describe` on the cluster lists every
/// group. It grants nothing on the groups themselves.
#[tokio::test(start_paused = true)]
async fn cluster_describe_lists_every_group_but_describes_none() {
    let c = cluster();
    alice_joins_and_commits(&c).await;

    assert_eq!(list_groups_as(&c, AUDITOR).await, [GROUP]);
    assert_eq!(
        describe_group_as(&c, AUDITOR).await.error_code,
        ERR_GROUP_AUTHZ_FAILED
    );
}

/// The all-topics OffsetFetch (Kafka 4's `--describe`) filters like
/// Metadata's list-all: topics the principal can't describe are left
/// out, not answered with 29.
#[tokio::test(start_paused = true)]
async fn offset_fetch_all_topics_leaves_out_undescribable_topics() {
    let c = cluster();
    let alice = alice_joins_and_commits(&c).await;
    let resp = offset_commit_as(&c, ALICE, &alice, &[("orders", 9)]).await;
    assert_eq!(resp.topics[0].partitions[0].error_code, 0);

    let req = offset_fetch::Request {
        groups: vec![offset_fetch::OffsetFetchGroup {
            group_id: GROUP.to_owned(),
            topics: None,
        }],
        ..offset_fetch::Request::default()
    };
    let fetch_all = |user: &'static str| {
        let c = &c;
        let req = req.clone();
        async move {
            let mut body = BytesMut::new();
            offset_fetch::encode_request(&mut body, &req, 8).unwrap();
            let h = OffsetFetchHandler::new(c.broker.clone());
            let group = offset_fetch::decode_response(&mut call(&h, user, 8, body).await, 8)
                .unwrap()
                .groups
                .remove(0);
            assert_eq!(group.error_code, 0, "{user}");
            group.topics.into_iter().map(|t| t.name).collect::<Vec<_>>()
        }
    };
    assert_eq!(fetch_all(ALICE).await, ["orders", "payments"]);
    assert_eq!(fetch_all(CAROL).await, ["payments"]);
    assert_eq!(c.reported_denials(), Vec::<String>::new());
}
