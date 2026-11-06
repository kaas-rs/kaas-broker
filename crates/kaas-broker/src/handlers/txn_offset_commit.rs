//! TxnOffsetCommit handler (key 28, v0–v3).
//!
//! Stages the consumer-group offset commit in the [`OffsetStore`]'s
//! pending layer keyed by `(group_id, producer_id)`. Pending offsets
//! are **not** visible to `OffsetFetch` until the txn coordinator
//! fires the offset hook from `EndTxn(commit = true)` — see
//! [`OffsetStore::store_pending`] / `commit_pending` / `discard_pending`.
//!
//! Routing: this handler runs on the **group** coordinator (not the
//! txn coordinator). Cross-broker case (group coord ≠ txn coord on
//! `EndTxn`) is currently handled by the same-broker fast path
//! when they coincide; the gh #114 cross-broker `WriteTxnMarkers`
//! RPC completes the loop for the general case.
//!
//! [`OffsetStore`]: kaas_coordinator::OffsetStore
//! [`OffsetStore::store_pending`]: kaas_coordinator::OffsetStore::store_pending

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::txn_offset_commit;
use kaas_coordinator::offset_key;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_INVALID_GROUP_ID: i16 = 24;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;
const ERR_TXN_ID_AUTHZ_FAILED: i16 = 53;

#[derive(Debug)]
pub struct TxnOffsetCommitHandler {
    broker: Arc<Broker>,
}

impl TxnOffsetCommitHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for TxnOffsetCommitHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = txn_offset_commit::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        // gh #199 ACL gates, before the coordinator gate reveals
        // routing: `Write` on the txn id, `Read` on the group (same
        // pair AddOffsetsToTxn checks — this is its delivery leg).
        if !self.broker.authorizer.authorize(
            &principal,
            &Resource::transactional_id(&req.transactional_id),
            Operation::Write,
        ) {
            return write_response(version, &req, |_, _| ERR_TXN_ID_AUTHZ_FAILED);
        }
        if !self.broker.authorizer.authorize(
            &principal,
            &Resource::group(&req.group_id),
            Operation::Read,
        ) {
            return write_response(version, &req, |_, _| ERR_GROUP_AUTHZ_FAILED);
        }
        // Offsets staged for a group that can't be stored could never be
        // committed, and the transaction could never finish (gh #318).
        if !kaas_coordinator::group_id_fits(&req.group_id) {
            return write_response(version, &req, |_, _| ERR_INVALID_GROUP_ID);
        }

        // Group-coordinator gate. Mirrors OffsetCommit: when no
        // Manager is wired, fall through with NOT_COORDINATOR so the
        // client retries via FindCoordinator.
        let mgr = match self.broker.coord_manager() {
            Some(m) => m,
            None => {
                return write_response(version, &req, |_, _| ERR_NOT_COORDINATOR);
            }
        };
        if !mgr.is_coordinator(&req.group_id) {
            return write_response(version, &req, |_, _| ERR_NOT_COORDINATOR);
        }

        // gh #199 per-topic gate — Apache's shape: unauthorized topics
        // answer `TOPIC_AUTHORIZATION_FAILED`, authorized ones commit.
        let denied: std::collections::HashSet<&str> = req
            .topics
            .iter()
            .filter(|t| {
                !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::topic(&t.name),
                    Operation::Read,
                )
            })
            .map(|t| t.name.as_str())
            .collect();

        // Flatten (topic, partition) → offset_key. Mirrors the
        // OffsetCommit handler's transform so the offset store sees
        // the same key shape regardless of which API staged it.
        // A topic or partition that doesn't exist is Apache's 3, as in
        // OffsetCommit, and isn't staged (gh #320).
        let known = |topic: &str, partition: i32| {
            self.broker
                .topics
                .get(topic)
                .is_some_and(|m| (0..m.partition_count).contains(&partition))
        };
        let mut offsets: HashMap<String, i64> = HashMap::new();
        let mut metadata: HashMap<String, String> = HashMap::new();
        for t in &req.topics {
            if denied.contains(t.name.as_str()) {
                continue;
            }
            for p in &t.partitions {
                if known(&t.name, p.partition_index) {
                    let key = offset_key(&t.name, p.partition_index);
                    offsets.insert(key.clone(), p.committed_offset);
                    // As for OffsetCommit: empty clears (gh #326).
                    metadata.insert(key, p.committed_metadata.clone().unwrap_or_default());
                }
            }
        }
        if !offsets.is_empty() {
            mgr.offsets.store_pending_with_metadata(
                &req.group_id,
                req.producer_id,
                offsets,
                metadata,
            );
        }

        write_response(version, &req, |topic, partition| {
            if denied.contains(topic) {
                ERR_TOPIC_AUTHZ_FAILED
            } else if !known(topic, partition) {
                ERR_UNKNOWN_TOPIC_OR_PARTITION
            } else {
                0
            }
        })
    }
}

fn write_response(
    version: i16,
    req: &txn_offset_commit::Request,
    code_for: impl Fn(&str, i32) -> i16,
) -> Result<BytesMut, HandlerError> {
    let topics = req
        .topics
        .iter()
        .map(|t| txn_offset_commit::ResponseTopic {
            name: t.name.clone(),
            partitions: t
                .partitions
                .iter()
                .map(|p| txn_offset_commit::ResponsePartition {
                    partition_index: p.partition_index,
                    error_code: code_for(&t.name, p.partition_index),
                })
                .collect(),
        })
        .collect();
    let resp = txn_offset_commit::Response {
        throttle_time_ms: 0,
        topics,
    };
    let mut out = BytesMut::new();
    txn_offset_commit::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::{offset_store::OffsetStore, FnLookup, LocalGroupSource, Manager};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_manager() -> (tempfile::TempDir, Arc<Broker>, Arc<Manager>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(crate::topic_registry::TopicMeta {
            name: "t".to_owned(),
            partition_count: 2,
            topic_id: [0; 16],
        });
        let b = Arc::new(Broker::new(engine, topics, "test", 0));
        let offsets = Arc::new(OffsetStore::new(tmp.path()));
        let lookup = Arc::new(FnLookup::new(|_| None));
        let mgr = Manager::new("kaas-0", offsets, lookup, LocalGroupSource::new("kaas-0"));
        b.install_coord_manager(mgr.clone());
        (tmp, b, mgr)
    }

    fn encode_request(req: &txn_offset_commit::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::{write_array_len, write_nullable_str, write_str};
        use kaas_codec::primitives::{write_i16, write_i32, write_i64};
        use kaas_codec::tagged;
        let flexible = version >= txn_offset_commit::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_str(&mut w, &req.group_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        if version >= 3 {
            write_i32(&mut w, req.generation_id);
            write_str(&mut w, &req.member_id, flexible).unwrap();
            write_nullable_str(&mut w, req.group_instance_id.as_deref(), flexible).unwrap();
        }
        write_array_len(&mut w, req.topics.len(), flexible).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partitions.len(), flexible).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, p.partition_index);
                write_i64(&mut w, p.committed_offset);
                if version >= 2 {
                    write_i32(&mut w, p.committed_leader_epoch);
                }
                write_nullable_str(&mut w, p.committed_metadata.as_deref(), flexible).unwrap();
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &TxnOffsetCommitHandler,
        req: &txn_offset_commit::Request,
    ) -> txn_offset_commit::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        txn_offset_commit::decode_response(&mut r, 3).unwrap()
    }

    fn broker_with_manager_and_auth(
        auth: Arc<dyn kaas_auth::Authorizer>,
    ) -> (tempfile::TempDir, Arc<Broker>, Arc<Manager>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        for name in ["t", "secret"] {
            topics.insert(crate::topic_registry::TopicMeta {
                name: name.to_owned(),
                partition_count: 1,
                topic_id: [0; 16],
            });
        }
        let b = Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            auth,
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        let offsets = Arc::new(OffsetStore::new(tmp.path()));
        let lookup = Arc::new(FnLookup::new(|_| None));
        let mgr = Manager::new("kaas-0", offsets, lookup, LocalGroupSource::new("kaas-0"));
        b.install_coord_manager(mgr.clone());
        (tmp, b, mgr)
    }

    fn request_for(topics: Vec<txn_offset_commit::Topic>) -> txn_offset_commit::Request {
        txn_offset_commit::Request {
            transactional_id: "tx-1".into(),
            group_id: "g1".into(),
            producer_id: 42,
            producer_epoch: 0,
            generation_id: 5,
            member_id: "m1".into(),
            group_instance_id: None,
            topics,
        }
    }

    fn topic(name: &str) -> txn_offset_commit::Topic {
        txn_offset_commit::Topic {
            name: name.into(),
            partitions: vec![txn_offset_commit::Partition {
                partition_index: 0,
                committed_offset: 7,
                committed_leader_epoch: -1,
                committed_metadata: None,
            }],
        }
    }

    /// gh #326: a transactional commit carries its metadata, which used to be
    /// dropped, leaving whatever an earlier commit had stored.
    #[tokio::test]
    async fn staged_metadata_reaches_the_commit() {
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(kaas_auth::AllowAllAuthorizer));
        let h = TxnOffsetCommitHandler::new(b);
        let mut with_metadata = topic("t");
        with_metadata.partitions[0].committed_metadata = Some("txn".to_owned());
        let resp = call(&h, &request_for(vec![with_metadata])).await;
        assert_eq!(resp.topics[0].partitions[0].error_code, 0);
        mgr.offsets.commit_pending("g1", 42).unwrap();
        let spec = kaas_coordinator::FetchSpec {
            topic: "t".to_owned(),
            partitions: vec![0],
        };
        let stored = mgr
            .offsets
            .fetch_metadata("g1", std::slice::from_ref(&spec));
        assert_eq!(
            stored.get(&offset_key("t", 0)).map(String::as_str),
            Some("txn")
        );
    }

    /// gh #320: a topic or partition that doesn't exist answers Apache's 3 and
    /// isn't staged, while the rest of the request still is.
    #[tokio::test]
    async fn an_unknown_topic_or_partition_is_3_and_is_not_staged() {
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(kaas_auth::AllowAllAuthorizer));
        let h = TxnOffsetCommitHandler::new(b);
        let mut out_of_range = topic("t");
        out_of_range.partitions[0].partition_index = 9;
        let req = request_for(vec![topic("t"), topic("nope"), out_of_range]);
        let resp = call(&h, &req).await;
        let codes: Vec<i16> = resp
            .topics
            .iter()
            .map(|t| t.partitions[0].error_code)
            .collect();
        assert_eq!(codes, [0, 3, 3]);
        let staged = mgr.offsets.pending_for("g1", 42).unwrap();
        assert_eq!(staged.len(), 1, "{staged:?}");
        assert!(staged.contains_key(&offset_key("t", 0)));
    }

    /// gh #320: the ACL comes first, so a principal without rights can't use
    /// the answer to learn which topics exist.
    #[tokio::test]
    async fn a_denied_unknown_topic_is_29_not_3() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b, _mgr) = broker_with_manager_and_auth(Arc::new(DenyNamedAuthorizer("ghost")));
        let h = TxnOffsetCommitHandler::new(b);
        let resp = call(&h, &request_for(vec![topic("ghost")])).await;
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            ERR_TOPIC_AUTHZ_FAILED
        );
    }

    /// gh #318: a group id too long to store answers 24 and stages nothing.
    /// These offsets could never be committed, so the transaction could never
    /// finish: EndTxn answered 15 forever, and only a restart cleared it.
    #[tokio::test]
    async fn a_group_id_too_long_to_store_is_24_and_stages_nothing() {
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(kaas_auth::AllowAllAuthorizer));
        let h = TxnOffsetCommitHandler::new(b);
        let mut req = request_for(vec![topic("t")]);
        req.group_id = "a".repeat(247);
        let resp = call(&h, &req).await;
        assert_eq!(resp.topics[0].partitions[0].error_code, 24);
        assert!(mgr.offsets.pending_for(&req.group_id, 42).is_none());

        req.group_id = "a".repeat(246);
        let resp = call(&h, &req).await;
        assert_eq!(resp.topics[0].partitions[0].error_code, 0);
        assert!(mgr.offsets.pending_for(&req.group_id, 42).is_some());
    }

    /// gh #199: no `Write` on the txn id -> 53 on every partition.
    #[tokio::test]
    async fn denied_txn_id_returns_authz_failed() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(DenyNamedAuthorizer("tx-1")));
        let h = TxnOffsetCommitHandler::new(b);
        let resp = call(&h, &request_for(vec![topic("t")])).await;
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            ERR_TXN_ID_AUTHZ_FAILED
        );
        assert!(mgr.offsets.pending_for("g1", 42).is_none());
    }

    /// gh #199: no `Read` on the group -> 30 on every partition.
    #[tokio::test]
    async fn denied_group_returns_group_authz_failed() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(DenyNamedAuthorizer("g1")));
        let h = TxnOffsetCommitHandler::new(b);
        let resp = call(&h, &request_for(vec![topic("t")])).await;
        assert_eq!(
            resp.topics[0].partitions[0].error_code,
            ERR_GROUP_AUTHZ_FAILED
        );
        assert!(mgr.offsets.pending_for("g1", 42).is_none());
    }

    /// gh #199: a denied topic answers 29 while the authorized one is
    /// staged — Apache's per-topic filter shape.
    #[tokio::test]
    async fn denied_topic_is_rejected_and_the_rest_staged() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b, mgr) = broker_with_manager_and_auth(Arc::new(DenyNamedAuthorizer("secret")));
        let h = TxnOffsetCommitHandler::new(b);
        let resp = call(&h, &request_for(vec![topic("t"), topic("secret")])).await;
        for t in &resp.topics {
            let want = if t.name == "secret" {
                ERR_TOPIC_AUTHZ_FAILED
            } else {
                0
            };
            assert_eq!(t.partitions[0].error_code, want, "topic {}", t.name);
        }
        let staged = mgr
            .offsets
            .pending_for("g1", 42)
            .expect("authorized topic must stage");
        assert!(
            staged.keys().all(|k| k.starts_with("t")),
            "denied topic must not stage: {staged:?}"
        );
    }

    #[tokio::test]
    async fn happy_path_stages_pending_offsets() {
        let (_t, b, mgr) = broker_with_manager();
        let h = TxnOffsetCommitHandler::new(b);
        let req = txn_offset_commit::Request {
            transactional_id: "tx-1".into(),
            group_id: "g1".into(),
            producer_id: 42,
            producer_epoch: 0,
            generation_id: 5,
            member_id: "m1".into(),
            group_instance_id: None,
            topics: vec![txn_offset_commit::Topic {
                name: "t".into(),
                partitions: vec![
                    txn_offset_commit::Partition {
                        partition_index: 0,
                        committed_offset: 100,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    },
                    txn_offset_commit::Partition {
                        partition_index: 1,
                        committed_offset: 200,
                        committed_leader_epoch: -1,
                        committed_metadata: None,
                    },
                ],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.topics {
            for pr in &tr.partitions {
                assert_eq!(pr.error_code, 0);
            }
        }
        // Pending offsets must NOT be visible to a regular OffsetFetch
        // until commit_pending fires (gh #27 contract).
        let pending = mgr.offsets.pending_for("g1", 42).expect("pending entry");
        assert_eq!(pending.get(&offset_key("t", 0)), Some(&100));
        assert_eq!(pending.get(&offset_key("t", 1)), Some(&200));
    }

    #[tokio::test]
    async fn no_manager_returns_not_coordinator() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        let h = TxnOffsetCommitHandler::new(b);
        let req = txn_offset_commit::Request {
            transactional_id: "tx-1".into(),
            group_id: "g1".into(),
            producer_id: 42,
            producer_epoch: 0,
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: vec![txn_offset_commit::Topic {
                name: "t".into(),
                partitions: vec![txn_offset_commit::Partition {
                    partition_index: 0,
                    committed_offset: 1,
                    committed_leader_epoch: -1,
                    committed_metadata: None,
                }],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.topics {
            for pr in &tr.partitions {
                assert_eq!(pr.error_code, ERR_NOT_COORDINATOR);
            }
        }
    }
}
