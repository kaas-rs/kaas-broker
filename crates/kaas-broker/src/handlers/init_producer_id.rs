//! InitProducerId handler (key 22).
//!
//! InitProducerId (API key 22) with the transactional path wired in.
//!
//! Non-transactional (`transactional_id: None`) — hand out a fresh
//! PID with `epoch = 0` from the broker's monotonic counter. Same as
//! Phase 3.
//!
//! Transactional (`transactional_id: Some(_)`):
//! 0. gh #199 ACL gate — `Write` on the `TransactionalId` resource,
//!    else `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53). Checked
//!    before routing so an unauthorized client learns nothing about
//!    coordinator placement.
//! 1. gh #91 routing gate — if [`Broker::owns_txn`] says no, return
//!    `NOT_COORDINATOR` (16) so the Java client `markCoordinatorUnknown`
//!    + re-FindCoordinator path lands on the right broker.
//! 2. gh #22 rejoin contract — if a [`TxnStateStore`] is installed,
//!    call [`TxnStateStore::init_producer_id`]. First call returns a
//!    fresh PID with `epoch = 0`; every subsequent call returns the
//!    SAME PID with `epoch += 1`.
//! 3. Without a store (boot window / dev mode), fall back to fresh
//!    PID + a one-shot `tracing::warn!` (graceful degradation). A zombie writer could still squeak under
//!    its old (PID, epoch) in that window.
//!
//! KIP-360 (gh #201): a v3+ request's `(producer_id, producer_epoch)`
//! is the caller's claimed identity. A half-set pair is
//! `INVALID_REQUEST` on both paths, as in Apache's `KafkaApis`; on the
//! transactional path the store fences a stale claim with
//! `PRODUCER_FENCED` rather than handing it a newer epoch. A rejoin
//! that finds an `Ongoing` transaction aborts it first — fence the
//! partitions, write the ABORT markers — and answers
//! `CONCURRENT_TRANSACTIONS`, so the producer retries into a clean
//! entry. The idempotent path ignores the claim, as Apache does.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::init_producer_id;
use kaas_coordinator::{InitPidOutcome, PendingMarkerDispatch, TxnStateError, TxnStateStore};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

/// `NOT_COORDINATOR` — gh #91 routing miss.
pub const ERR_NOT_COORDINATOR: i16 = 16;
/// `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` — gh #199 ACL gate.
pub const ERR_TXN_ID_AUTHZ_FAILED: i16 = 53;
/// `COORDINATOR_NOT_AVAILABLE` — txn store not yet wired.
pub const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
/// `PRODUCER_FENCED` — epoch mismatch.
pub const ERR_PRODUCER_FENCED: i16 = 90;
/// `INVALID_TXN_STATE` — defensive; unreachable for InitProducerId
/// today but kept in the mapping for forward compat.
pub const ERR_INVALID_TXN_STATE: i16 = 48;
/// `CONCURRENT_TRANSACTIONS` — markers in flight, or an `Ongoing`
/// transaction this request just aborted. Retriable.
pub const ERR_CONCURRENT_TRANSACTIONS: i16 = 51;
/// `INVALID_REQUEST` — exactly one of the v3+ claimed
/// `producer_id` / `producer_epoch` is the `-1` sentinel.
pub const ERR_INVALID_REQUEST: i16 = 42;

#[derive(Debug)]
pub struct InitProducerIdHandler {
    broker: Arc<Broker>,
}

impl InitProducerIdHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for InitProducerIdHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = init_producer_id::decode_request(&mut body, version)?;
        let claimed = claimed_identity(&req);

        let mut resp = match req.transactional_id.as_deref() {
            None | Some("") if claimed.is_err() => error_response(ERR_INVALID_REQUEST),
            None | Some("") => {
                // Idempotent (non-transactional) — no per-key state,
                // every broker can answer locally. Deliberately NOT
                // gated on IDEMPOTENT_WRITE: the Java client enables
                // idempotence by default since 3.0, and Apache itself
                // relaxed the gate (KIP-679) so a topic-WRITE-only
                // principal can still produce. The real enforcement
                // point is Produce's per-topic Write check.
                init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: self.broker.next_producer_id(),
                    producer_epoch: 0,
                }
            }
            Some(txn_id) => {
                // gh #199: Apache gates the transactional path on
                // WRITE for the TransactionalId resource, before any
                // coordinator routing is revealed.
                let principal = principal_from(conn);
                if !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::transactional_id(txn_id),
                    Operation::Write,
                ) {
                    error_response(ERR_TXN_ID_AUTHZ_FAILED)
                } else {
                    match claimed {
                        Ok(expected) => {
                            self.handle_transactional(txn_id, req.transaction_timeout_ms, expected)
                                .await
                        }
                        Err(code) => error_response(code),
                    }
                }
            }
        };

        resp.error_code = super::producer_fenced_for(resp.error_code, version, 4);
        let mut out = BytesMut::new();
        init_producer_id::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl InitProducerIdHandler {
    async fn handle_transactional(
        &self,
        txn_id: &str,
        timeout_ms: i32,
        expected: Option<(i64, i16)>,
    ) -> init_producer_id::Response {
        // gh #91 routing — reject on the wrong broker before
        // touching the PID counter or the store.
        if !self.broker.owns_txn(txn_id) {
            return error_response(ERR_NOT_COORDINATOR);
        }
        let store = match self.broker.txn_state() {
            Some(s) => s,
            None => {
                // No store wired (boot window or dev mode):
                // hand out a fresh PID, log once.
                // The producer can make progress but the gh #22
                // fence-on-rejoin contract is silently disabled
                // this connection.
                tracing::warn!(
                    txn_id = %txn_id,
                    "InitProducerId received transactional.id but no TxnStateStore is wired; \
                     epoch fence on rejoin disabled (gh #22)",
                );
                return init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: self.broker.next_producer_id(),
                    producer_epoch: 0,
                };
            }
        };
        let broker_for_alloc = self.broker.clone();
        let outcome = store.init_producer_id(txn_id, timeout_ms, expected, move || {
            broker_for_alloc.next_producer_id()
        });
        match outcome {
            Ok(InitPidOutcome::Ready { pid, epoch }) => {
                // `epoch == 0` covers two cases — first-ever alloc, and
                // post-exhaustion rotation to a fresh PID — neither
                // needs fencing (no earlier (pid, epoch) state exists,
                // or the PID itself has changed).
                if epoch > 0 {
                    self.fence(pid, epoch);
                }
                init_producer_id::Response {
                    throttle_time_ms: 0,
                    error_code: 0,
                    producer_id: pid,
                    producer_epoch: epoch,
                }
            }
            Ok(InitPidOutcome::AbortingOngoing(abort)) => {
                self.abort_fenced_txn(&store, abort).await;
                error_response(ERR_CONCURRENT_TRANSACTIONS)
            }
            Err(err) => error_response(map_store_error(&err)),
        }
    }

    /// gh #30: fence locally + broadcast to peers via the outbound
    /// FenceLog so their FenceWatcher applies it within ~2s.
    fn fence(&self, pid: i64, epoch: i16) {
        self.broker.engine.fence_producer_epoch(pid, epoch);
        if let Some(log) = self.broker.fence_log() {
            if let Err(err) = log.append(pid, epoch) {
                tracing::warn!(
                    pid,
                    epoch,
                    %err,
                    "InitProducerId: outbound FenceLog append failed; \
                     peer brokers will not see this epoch bump until \
                     a future bump succeeds (zombie window on cross-broker partitions)",
                );
            }
        }
    }

    /// Finish the abort a rejoin started on an `Ongoing` transaction
    /// (gh #201): fence the old session, then write its ABORT markers
    /// at the bumped epoch and complete — EndTxn's prepare → dispatch →
    /// complete order (gh #225).
    ///
    /// The fence comes first so the outgoing session can't land
    /// another data batch behind the marker. Inline dispatch is only
    /// for latency: the entry is already `PrepareAbort` with its list
    /// intact, so a failure here is left to the marker reconcile, and
    /// the producer's retry reads `CONCURRENT_TRANSACTIONS` until it
    /// lands.
    async fn abort_fenced_txn(&self, store: &TxnStateStore, abort: PendingMarkerDispatch) {
        self.fence(abort.pid, abort.epoch);
        let dispatched = crate::txn_markers::dispatch_markers(
            &self.broker,
            &abort.txn_id,
            abort.pid,
            abort.epoch,
            false,
            &abort.partitions,
        )
        .await;
        if let Err(code) = dispatched {
            tracing::warn!(
                txn_id = %abort.txn_id,
                producer_id = abort.pid,
                error_code = code,
                "InitProducerId: ABORT dispatch for the fenced transaction incomplete; \
                 left in PrepareAbort for the marker reconcile (gh #225)",
            );
            return;
        }
        if let Err(err) = store.complete_end_txn(&abort.txn_id, abort.pid, abort.epoch, false) {
            tracing::warn!(
                txn_id = %abort.txn_id,
                %err,
                "InitProducerId: ABORT markers placed but completing the fenced \
                 transaction failed; the marker reconcile will retry",
            );
        }
    }
}

/// KIP-360 claimed identity, checked as Apache's `KafkaApis` does: both
/// fields `-1` is no claim, exactly one `-1` is malformed. v0–v2
/// decode both as `-1`.
fn claimed_identity(req: &init_producer_id::Request) -> Result<Option<(i64, i16)>, i16> {
    match (req.producer_id, req.producer_epoch) {
        (-1, -1) => Ok(None),
        (-1, _) | (_, -1) => Err(ERR_INVALID_REQUEST),
        (pid, epoch) => Ok(Some((pid, epoch))),
    }
}

fn error_response(code: i16) -> init_producer_id::Response {
    init_producer_id::Response {
        throttle_time_ms: 0,
        error_code: code,
        producer_id: -1,
        producer_epoch: -1,
    }
}

fn map_store_error(err: &TxnStateError) -> i16 {
    match err {
        TxnStateError::EmptyTxnId => ERR_INVALID_REQUEST,
        TxnStateError::UnknownProducer => 49, // INVALID_PRODUCER_ID_MAPPING
        TxnStateError::EpochFenced => ERR_PRODUCER_FENCED,
        TxnStateError::Concurrent => ERR_CONCURRENT_TRANSACTIONS,
        TxnStateError::InvalidState => ERR_INVALID_TXN_STATE,
        // Persistence failure — surface as COORDINATOR_NOT_AVAILABLE so the
        // client retries instead of silently bypassing the fence.
        TxnStateError::Io(_) | TxnStateError::Decode(_) => ERR_COORDINATOR_NOT_AVAILABLE,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::TxnStateStore;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker() -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ))
    }

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>) {
        let tmp = tempfile::tempdir().unwrap();
        let b = broker();
        let store = Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap());
        b.install_txn_state(store);
        (tmp, b)
    }

    fn encode_request(req: &init_producer_id::Request, version: i16) -> Bytes {
        use kaas_codec::api::common::write_nullable_str;
        use kaas_codec::primitives::{write_i16, write_i32, write_i64};
        use kaas_codec::tagged;
        let flexible = version >= init_producer_id::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        write_i32(&mut w, req.transaction_timeout_ms);
        if version >= 3 {
            write_i64(&mut w, req.producer_id);
            write_i16(&mut w, req.producer_epoch);
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    async fn call(
        h: &InitProducerIdHandler,
        txn_id: Option<&str>,
        timeout_ms: i32,
    ) -> init_producer_id::Response {
        call_claiming(h, txn_id, timeout_ms, -1, -1).await
    }

    /// As [`call`], carrying a KIP-360 claimed identity.
    async fn call_claiming(
        h: &InitProducerIdHandler,
        txn_id: Option<&str>,
        timeout_ms: i32,
        producer_id: i64,
        producer_epoch: i16,
    ) -> init_producer_id::Response {
        let req = init_producer_id::Request {
            transactional_id: txn_id.map(str::to_owned),
            transaction_timeout_ms: timeout_ms,
            producer_id,
            producer_epoch,
        };
        let body = encode_request(&req, 4);
        let out = h.handle(&conn(), 4, body).await.unwrap();
        let mut r = out.freeze();
        init_producer_id::decode_response(&mut r, 4).unwrap()
    }

    #[tokio::test]
    async fn transactional_denied_returns_txn_id_authz_failed() {
        // gh #199: `Write` on the TransactionalId resource gates the
        // transactional path, before any coordinator routing.
        use crate::handlers::test_authz::DenyAllAuthorizer;
        use kaas_auth::NoQuotaChecker;
        let engine: Arc<dyn kaas_storage::StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(NoQuotaChecker),
        ));
        let h = InitProducerIdHandler::new(b);
        let resp = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(resp.error_code, ERR_TXN_ID_AUTHZ_FAILED);
        assert_eq!(resp.producer_id, -1);
    }

    #[tokio::test]
    async fn idempotent_path_is_not_gated() {
        // Deliberate (KIP-679): the Java client turns idempotence on
        // by default, so the idempotent InitProducerId stays open even
        // under a deny-all authorizer — Produce's per-topic Write
        // check is the enforcement point.
        use crate::handlers::test_authz::DenyAllAuthorizer;
        use kaas_auth::NoQuotaChecker;
        let engine: Arc<dyn kaas_storage::StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(NoQuotaChecker),
        ));
        let h = InitProducerIdHandler::new(b);
        let resp = call(&h, None, 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
    }

    #[tokio::test]
    async fn non_transactional_returns_fresh_pid() {
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, None, 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[tokio::test]
    async fn transactional_without_store_falls_back_to_fresh_pid() {
        // Broker without txn_state installed: fallback —
        // fresh PID + warn, error_code 0.
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
        assert_eq!(resp.producer_epoch, 0);
    }

    #[tokio::test]
    async fn transactional_rejoin_bumps_epoch() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        let r1 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r1.error_code, 0);
        assert_eq!(r1.producer_epoch, 0);
        let pid = r1.producer_id;
        let r2 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r2.error_code, 0);
        assert_eq!(r2.producer_id, pid, "rejoin must return same PID");
        assert_eq!(r2.producer_epoch, 1);
        let r3 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r3.producer_epoch, 2);
    }

    #[tokio::test]
    async fn distinct_txn_ids_get_distinct_pids() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        let a = call(&h, Some("tx-a"), 60_000).await;
        let bb = call(&h, Some("tx-b"), 60_000).await;
        assert_ne!(a.producer_id, bb.producer_id);
    }

    #[tokio::test]
    async fn rejoin_appends_to_fence_log_for_broadcast() {
        use kaas_coordinator::FenceLog;
        let (_t, b) = broker_with_txn();
        let fence_dir = tempfile::tempdir().unwrap();
        let log = Arc::new(FenceLog::open(fence_dir.path(), "kaas-0").unwrap());
        b.install_fence_log(log.clone());

        let h = InitProducerIdHandler::new(b);
        let r1 = call(&h, Some("tx-1"), 60_000).await;
        // First call: epoch=0 → no fence broadcast.
        assert_eq!(r1.producer_epoch, 0);
        assert!(log.snapshot().is_empty(), "epoch=0 must not broadcast");

        // Rejoin: epoch=1 → broadcast appended.
        let r2 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r2.producer_epoch, 1);
        let snap = log.snapshot();
        assert_eq!(snap.get(&r2.producer_id), Some(&1));

        // Second rejoin: epoch=2 → overwrites the prior entry.
        let r3 = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(r3.producer_epoch, 2);
        assert_eq!(log.snapshot().get(&r3.producer_id), Some(&2));
    }

    #[tokio::test]
    async fn empty_string_transactional_id_is_non_transactional() {
        // KIP-98 client convention: Some("") is equivalent to None.
        let h = InitProducerIdHandler::new(broker());
        let resp = call(&h, Some(""), 0).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.producer_id >= 1);
    }

    /// gh #201: Apache's `KafkaApis` rejects a half-set claim before
    /// either path runs.
    #[tokio::test]
    async fn half_set_claim_is_invalid_request() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        for txn_id in [None, Some("tx-1")] {
            assert_eq!(
                call_claiming(&h, txn_id, 60_000, -1, 3).await.error_code,
                ERR_INVALID_REQUEST
            );
            assert_eq!(
                call_claiming(&h, txn_id, 60_000, 5, -1).await.error_code,
                ERR_INVALID_REQUEST
            );
        }
    }

    /// The idempotent path ignores the claim, as Apache does: there is
    /// no coordinator state to check it against.
    #[tokio::test]
    async fn idempotent_path_ignores_a_claim() {
        let h = InitProducerIdHandler::new(broker());
        let resp = call_claiming(&h, None, 0, 12_345, 7).await;
        assert_eq!(resp.error_code, 0);
        assert_ne!(resp.producer_id, 12_345);
        assert_eq!(resp.producer_epoch, 0);
    }

    /// gh #201: a zombie re-initialising under its old identity used to
    /// be handed a fresh valid epoch, fencing the live session. It must
    /// be told PRODUCER_FENCED, and the live epoch must not move.
    #[tokio::test]
    async fn stale_claim_is_producer_fenced() {
        let (_t, b) = broker_with_txn();
        let store = b.txn_state().unwrap();
        let h = InitProducerIdHandler::new(b);
        let zombie = call(&h, Some("tx-1"), 60_000).await;
        let live = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(live.producer_epoch, zombie.producer_epoch + 1);

        let resp = call_claiming(
            &h,
            Some("tx-1"),
            60_000,
            zombie.producer_id,
            zombie.producer_epoch,
        )
        .await;
        assert_eq!(resp.error_code, ERR_PRODUCER_FENCED);
        assert_eq!(resp.producer_id, -1);
        assert_eq!(store.snapshot()["tx-1"].epoch, live.producer_epoch);
    }

    /// The live session's own claim bumps it (KIP-360), and a retry of
    /// that request gets the same answer.
    #[tokio::test]
    async fn current_claim_bumps_and_its_retry_is_stable() {
        let (_t, b) = broker_with_txn();
        let h = InitProducerIdHandler::new(b);
        let r0 = call(&h, Some("tx-1"), 60_000).await;
        let claim = (r0.producer_id, r0.producer_epoch);
        let r1 = call_claiming(&h, Some("tx-1"), 60_000, claim.0, claim.1).await;
        assert_eq!(r1.error_code, 0);
        assert_eq!(r1.producer_id, r0.producer_id);
        assert_eq!(r1.producer_epoch, r0.producer_epoch + 1);
        let retry = call_claiming(&h, Some("tx-1"), 60_000, claim.0, claim.1).await;
        assert_eq!(retry.error_code, 0);
        assert_eq!(retry.producer_epoch, r1.producer_epoch);
    }

    /// gh #201: a rejoin that finds an `Ongoing` transaction aborts it —
    /// fences the old session, writes its ABORT marker, completes — and
    /// answers CONCURRENT_TRANSACTIONS. The retry then gets a clean
    /// entry. Before the fix the new session inherited the open
    /// transaction, partitions and all.
    #[tokio::test]
    async fn rejoin_mid_ongoing_aborts_it_and_answers_concurrent() {
        use kaas_coordinator::{FenceLog, TxnState, TxnTopic};
        let (_t, b) = broker_with_txn();
        let fence_dir = tempfile::tempdir().unwrap();
        let log = Arc::new(FenceLog::open(fence_dir.path(), "kaas-0").unwrap());
        b.install_fence_log(log.clone());
        let store = b.txn_state().unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        let h = InitProducerIdHandler::new(b.clone());

        let old = call(&h, Some("tx-1"), 60_000).await;
        store
            .add_partitions(
                "tx-1",
                old.producer_id,
                old.producer_epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        let hwm_before = b.engine.high_watermark("t", 0).unwrap();

        let resp = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(resp.error_code, ERR_CONCURRENT_TRANSACTIONS);
        assert_eq!(resp.producer_id, -1, "the fence epoch is never handed out");

        let fence_epoch = old.producer_epoch + 1;
        assert_eq!(
            log.snapshot().get(&old.producer_id),
            Some(&fence_epoch),
            "the old session must be fenced across brokers too"
        );
        assert!(
            b.engine.high_watermark("t", 0).unwrap() > hwm_before,
            "the ABORT marker must land"
        );
        let entry = &store.snapshot()["tx-1"];
        assert_eq!(entry.state, TxnState::CompleteAbort);
        assert_eq!(entry.epoch, fence_epoch);
        assert!(entry.partitions.is_empty());

        let retry = call(&h, Some("tx-1"), 60_000).await;
        assert_eq!(retry.error_code, 0);
        assert_eq!(retry.producer_id, old.producer_id);
        assert_eq!(retry.producer_epoch, fence_epoch + 1);
    }
}
