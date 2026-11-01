//! AddPartitionsToTxn handler (key 24, v0–v3).
//!
//! v0–v3 has no top-level `ErrorCode` field. A top-level rejection
//! (empty txn id / wrong coordinator / store not yet wired / store
//! error) is repeated across **every** partition in the response.
//! The Java client picks any one — they're all the same code.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::add_partitions_to_txn;
use kaas_coordinator::{TxnStateError, TxnStateStore, TxnTopic};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_INVALID_REQUEST: i16 = 42;
const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
const ERR_INVALID_PRODUCER_ID_MAPPING: i16 = 49;
const ERR_PRODUCER_FENCED: i16 = 90;
const ERR_CONCURRENT_TRANSACTIONS: i16 = 51;
const ERR_INVALID_TXN_STATE: i16 = 48;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_TXN_ID_AUTHZ_FAILED: i16 = 53;
const ERR_OPERATION_NOT_ATTEMPTED: i16 = 55;

#[derive(Debug)]
pub struct AddPartitionsToTxnHandler {
    broker: Arc<Broker>,
}

impl AddPartitionsToTxnHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for AddPartitionsToTxnHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = add_partitions_to_txn::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        let resp = match self.classify(&principal, &req) {
            Some(code) => build_response(&req, code),
            None => match self.authorize_topics(&principal, &req) {
                Some(resp) => resp,
                None => {
                    let code = match self.broker.txn_state() {
                        Some(store) => self.commit(&store, &req),
                        // classify already verified Some(_); this arm only
                        // fires under a concurrent uninstall (no current path).
                        None => ERR_COORDINATOR_NOT_AVAILABLE,
                    };
                    build_response(&req, code)
                }
            },
        };

        let mut out = BytesMut::new();
        add_partitions_to_txn::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl AddPartitionsToTxnHandler {
    /// Top-level validation that doesn't need the store. Returns
    /// `Some(error_code)` to short-circuit; `None` means "request
    /// is valid, delegate to the store".
    fn classify(&self, principal: &Principal, req: &add_partitions_to_txn::Request) -> Option<i16> {
        if req.transactional_id.is_empty() {
            return Some(ERR_INVALID_REQUEST);
        }
        // gh #199 ACL gate, before routing is revealed.
        if !self.broker.authorizer.authorize(
            principal,
            &Resource::transactional_id(&req.transactional_id),
            Operation::Write,
        ) {
            return Some(ERR_TXN_ID_AUTHZ_FAILED);
        }
        if !self.broker.owns_txn(&req.transactional_id) {
            return Some(ERR_NOT_COORDINATOR);
        }
        if self.broker.txn_state().is_none() {
            // Boot window: store not yet wired.
            // COORDINATOR_NOT_AVAILABLE is retryable on the Java
            // client; INVALID_PRODUCER_ID_MAPPING would be terminal.
            return Some(ERR_COORDINATOR_NOT_AVAILABLE);
        }
        None
    }

    /// gh #199 per-topic gate — Apache's shape: if any topic in the
    /// request lacks `Write`, NOTHING is added to the transaction;
    /// denied topics answer `TOPIC_AUTHORIZATION_FAILED` and the rest
    /// `OPERATION_NOT_ATTEMPTED`. Returns `None` when fully authorized.
    fn authorize_topics(
        &self,
        principal: &Principal,
        req: &add_partitions_to_txn::Request,
    ) -> Option<add_partitions_to_txn::Response> {
        let denied: std::collections::HashSet<&str> = req
            .topics
            .iter()
            .filter(|t| {
                !self.broker.authorizer.authorize(
                    principal,
                    &Resource::topic(&t.name),
                    Operation::Write,
                )
            })
            .map(|t| t.name.as_str())
            .collect();
        if denied.is_empty() {
            return None;
        }
        let results = req
            .topics
            .iter()
            .map(|t| {
                let code = if denied.contains(t.name.as_str()) {
                    ERR_TOPIC_AUTHZ_FAILED
                } else {
                    ERR_OPERATION_NOT_ATTEMPTED
                };
                add_partitions_to_txn::TopicResult {
                    name: t.name.clone(),
                    partition_results: t
                        .partitions
                        .iter()
                        .map(|p| add_partitions_to_txn::PartitionResult {
                            partition_index: *p,
                            error_code: code,
                        })
                        .collect(),
                }
            })
            .collect();
        Some(add_partitions_to_txn::Response {
            throttle_time_ms: 0,
            results,
        })
    }

    fn commit(&self, store: &Arc<TxnStateStore>, req: &add_partitions_to_txn::Request) -> i16 {
        let additions: Vec<TxnTopic> = req
            .topics
            .iter()
            .map(|t| TxnTopic {
                topic: t.name.clone(),
                partitions: t.partitions.clone(),
            })
            .collect();
        match store.add_partitions(
            &req.transactional_id,
            req.producer_id,
            req.producer_epoch,
            &additions,
            now_ms(),
        ) {
            Ok(()) => 0,
            Err(e) => map_store_error(&e),
        }
    }
}

fn build_response(
    req: &add_partitions_to_txn::Request,
    err_code: i16,
) -> add_partitions_to_txn::Response {
    let results = req
        .topics
        .iter()
        .map(|t| add_partitions_to_txn::TopicResult {
            name: t.name.clone(),
            partition_results: t
                .partitions
                .iter()
                .map(|p| add_partitions_to_txn::PartitionResult {
                    partition_index: *p,
                    error_code: err_code,
                })
                .collect(),
        })
        .collect();
    add_partitions_to_txn::Response {
        throttle_time_ms: 0,
        results,
    }
}

fn map_store_error(err: &TxnStateError) -> i16 {
    match err {
        TxnStateError::EmptyTxnId => ERR_INVALID_REQUEST,
        TxnStateError::UnknownProducer => ERR_INVALID_PRODUCER_ID_MAPPING,
        TxnStateError::EpochFenced => ERR_PRODUCER_FENCED,
        TxnStateError::Concurrent => ERR_CONCURRENT_TRANSACTIONS,
        TxnStateError::InvalidState => ERR_INVALID_TXN_STATE,
        TxnStateError::Io(_) | TxnStateError::Decode(_) => ERR_COORDINATOR_NOT_AVAILABLE,
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        b.install_txn_state(Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap()));
        (tmp, b)
    }

    fn encode_request(req: &add_partitions_to_txn::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::{write_array_len, write_str};
        use kaas_codec::primitives::{write_i16, write_i32, write_i64};
        use kaas_codec::tagged;
        let flexible = version >= add_partitions_to_txn::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_str(&mut w, &req.transactional_id, flexible).unwrap();
        write_i64(&mut w, req.producer_id);
        write_i16(&mut w, req.producer_epoch);
        write_array_len(&mut w, req.topics.len(), flexible).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partitions.len(), flexible).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, *p);
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
        h: &AddPartitionsToTxnHandler,
        req: &add_partitions_to_txn::Request,
    ) -> add_partitions_to_txn::Response {
        let body = encode_request(req, 3);
        let out = h.handle(&conn(), 3, body).await.unwrap();
        let mut r = out.freeze();
        add_partitions_to_txn::decode_response(&mut r, 3).unwrap()
    }

    fn broker_with_txn_and_auth(
        auth: Arc<dyn kaas_auth::Authorizer>,
    ) -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            auth,
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        b.install_txn_state(Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap()));
        (tmp, b)
    }

    /// gh #199: no `Write` on the txn id -> 53 on every partition.
    #[tokio::test]
    async fn denied_txn_id_returns_authz_failed() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b) = broker_with_txn_and_auth(Arc::new(DenyNamedAuthorizer("tx-1")));
        let h = AddPartitionsToTxnHandler::new(b);
        let req = add_partitions_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: 1,
            producer_epoch: 0,
            topics: vec![add_partitions_to_txn::Topic {
                name: "t".into(),
                partitions: vec![0],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.results {
            for pr in &tr.partition_results {
                assert_eq!(pr.error_code, ERR_TXN_ID_AUTHZ_FAILED);
            }
        }
    }

    /// gh #199: a denied topic fails the whole request — Apache's
    /// shape: denied topics answer 29, the rest 55, and NOTHING is
    /// added to the transaction.
    #[tokio::test]
    async fn denied_topic_fails_all_and_commits_nothing() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let (_t, b) = broker_with_txn_and_auth(Arc::new(DenyNamedAuthorizer("secret")));
        let store = b.txn_state().unwrap();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 42).unwrap();
        let h = AddPartitionsToTxnHandler::new(b);
        let req = add_partitions_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: pid,
            producer_epoch: epoch,
            topics: vec![
                add_partitions_to_txn::Topic {
                    name: "t".into(),
                    partitions: vec![0],
                },
                add_partitions_to_txn::Topic {
                    name: "secret".into(),
                    partitions: vec![0],
                },
            ],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.results {
            let want = if tr.name == "secret" {
                ERR_TOPIC_AUTHZ_FAILED
            } else {
                ERR_OPERATION_NOT_ATTEMPTED
            };
            for pr in &tr.partition_results {
                assert_eq!(pr.error_code, want, "topic {}", tr.name);
            }
        }
        let snap = store.snapshot();
        assert!(
            snap["tx-1"].partitions.is_empty(),
            "denied request must not add partitions to the txn"
        );
    }

    #[tokio::test]
    async fn empty_txn_id_returns_invalid_request_per_partition() {
        let (_t, b) = broker_with_txn();
        let h = AddPartitionsToTxnHandler::new(b);
        let req = add_partitions_to_txn::Request {
            transactional_id: String::new(),
            producer_id: 1,
            producer_epoch: 0,
            topics: vec![add_partitions_to_txn::Topic {
                name: "t".into(),
                partitions: vec![0, 1],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.results {
            for pr in &tr.partition_results {
                assert_eq!(pr.error_code, ERR_INVALID_REQUEST);
            }
        }
    }

    #[tokio::test]
    async fn unknown_producer_maps_to_invalid_producer_id_mapping() {
        let (_t, b) = broker_with_txn();
        let h = AddPartitionsToTxnHandler::new(b);
        let req = add_partitions_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: 999,
            producer_epoch: 0,
            topics: vec![add_partitions_to_txn::Topic {
                name: "t".into(),
                partitions: vec![0],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.results {
            for pr in &tr.partition_results {
                assert_eq!(pr.error_code, ERR_INVALID_PRODUCER_ID_MAPPING);
            }
        }
    }

    #[tokio::test]
    async fn happy_path_with_pre_allocated_producer() {
        let (_t, b) = broker_with_txn();
        // Pre-allocate a PID via the store so add_partitions matches.
        let store = b.txn_state().unwrap();
        let next_pid = || 42_i64;
        let (pid, epoch) = store.get_or_allocate("tx-1", next_pid).unwrap();

        let h = AddPartitionsToTxnHandler::new(b);
        let req = add_partitions_to_txn::Request {
            transactional_id: "tx-1".into(),
            producer_id: pid,
            producer_epoch: epoch,
            topics: vec![add_partitions_to_txn::Topic {
                name: "t".into(),
                partitions: vec![0, 1],
            }],
        };
        let resp = call(&h, &req).await;
        for tr in &resp.results {
            for pr in &tr.partition_results {
                assert_eq!(pr.error_code, 0);
            }
        }
    }
}
