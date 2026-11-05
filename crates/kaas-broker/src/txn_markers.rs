//! COMMIT / ABORT marker dispatch, shared by the `EndTxn` handler and
//! the marker reconcile (gh #225).
//!
//! A transaction that has been *prepared* owes every partition it
//! touched a control batch. There are two ways that debt gets paid:
//!
//! - **Retry** — the producer re-sends `EndTxn`, the handler re-drives
//!   the same dispatch set, and completes on success.
//! - **Reconcile** — [`reconcile_pending_markers`] sweeps transactions
//!   stuck in `Prepare*` and drives them to completion.
//!
//! Both are needed, and neither subsumes the other. Retry alone leaves
//! a transaction prepared forever when the producer crashes, gives up,
//! or is fenced — and a transaction the *reaper* prepared (a timeout)
//! has no producer to retry at all. Reconcile alone would make every
//! commit wait a full sweep interval. So the handler dispatches inline
//! for latency and the sweep is the backstop that guarantees
//! completion, which is the "idempotent and driven to completion by
//! retry/reconcile" half of NFS substrate rule 2.
//!
//! Until the debt is paid a `read_committed` consumer cannot advance
//! past the transaction's first offset on any of those partitions, so
//! "eventually" is load-bearing: an unpaid marker pins the LSO for
//! every record written after it, not just the transaction's own.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use kaas_coordinator::{MarkerEntry, TxnStateStore, TxnTopic};

use crate::broker::Broker;
use crate::control_batch::build_control_batch;

/// Retriable wire code returned when a marker could not be placed.
/// The caller must leave the transaction in `Prepare*`.
pub const ERR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
const ACKS_ALL: i16 = -1;

/// Place a COMMIT / ABORT marker for every partition in `partitions`.
///
/// `Ok(())` means every marker is durable — locally appended with
/// `acks = -1`, or written to the peer's marker-queue inbox on the
/// shared PVC, from where that peer's `MarkerWatcher` retries until it
/// applies. Only then may the caller complete the transaction.
///
/// `Err(code)` is a retriable wire code; the caller must leave the
/// transaction prepared so the dispatch set survives for another
/// attempt.
pub async fn dispatch_markers(
    broker: &Arc<Broker>,
    txn_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    partitions: &[TxnTopic],
) -> Result<(), i16> {
    if partitions.is_empty() {
        return Ok(());
    }

    // Group partitions by which broker leads them. Same-broker
    // partitions are written locally (low latency); peer-broker
    // partitions go through the marker_queue (gh #175 file-queue
    // dispatch). Coordinator-less dev mode treats every partition
    // as same-broker.
    let mut by_target: HashMap<Option<String>, Vec<(String, i32)>> = HashMap::new();
    let coord = broker.coordinator();
    for TxnTopic { topic, partitions } in partitions {
        for &p in partitions {
            let leader = match coord.as_ref() {
                None => None, // dev mode: everything is local
                Some(c) => match c.leader_for(topic, p) {
                    Some(leader) => Some(leader),
                    None => {
                        // Unassigned with a coordinator installed. Treating
                        // this as local created the partition to write the
                        // marker, conjuring a directory for a topic that no
                        // longer exists (gh #314). A topic that's gone has
                        // nothing to mark. Anything else (the watch hasn't
                        // listed yet, or the assignment hasn't caught up to
                        // the topic) stays prepared, and the reconcile retries.
                        let exists = broker
                            .topics
                            .get(topic)
                            .is_some_and(|m| (0..m.partition_count).contains(&p));
                        if broker.topics.is_synced() && !exists {
                            tracing::info!(
                                topic,
                                partition = p,
                                "txn marker skipped: the partition no longer exists"
                            );
                            continue;
                        }
                        return Err(ERR_COORDINATOR_NOT_AVAILABLE);
                    }
                },
            };
            by_target
                .entry(leader)
                .or_default()
                .push((topic.clone(), p));
        }
    }

    let self_id = broker.self_id.as_str();
    // Splits: (local writes, per-target queue entries).
    let mut local_partitions: Vec<(String, i32)> = Vec::new();
    let mut queued: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for (target, parts) in by_target {
        match target {
            None => local_partitions.extend(parts), // dev mode only, see above
            Some(id) if id == self_id => local_partitions.extend(parts),
            Some(id) => {
                queued.entry(id).or_default().extend(parts);
            }
        }
    }

    // Same-broker write — happens before the queue write so a
    // crash mid-dispatch still leaves the local marker in place.
    if !local_partitions.is_empty() {
        write_local_markers(
            broker,
            producer_id,
            producer_epoch,
            commit,
            &local_partitions,
        )
        .await?;
    }

    // Cross-broker dispatch via the shared-PVC queue. Receiver's
    // MarkerWatcher picks it up within ~2 s and applies it on the
    // peer leader (gh #175).
    if !queued.is_empty() {
        enqueue_cross_broker_markers(broker, txn_id, producer_id, producer_epoch, commit, &queued)?;
    }
    Ok(())
}

/// Append the control batch to every locally-led partition.
///
/// A failed append aborts the whole dispatch: the caller leaves the
/// txn in `Prepare*` so a later attempt re-appends. Re-appending a
/// marker an earlier attempt already landed writes a duplicate control
/// batch, which is wasteful but not incorrect — control batches carry
/// no sequence numbers and consumers don't react to duplicate markers
/// (the same property `MarkerWatcher` already relies on).
async fn write_local_markers(
    broker: &Arc<Broker>,
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    partitions: &[(String, i32)],
) -> Result<(), i16> {
    let batch = Bytes::from(build_control_batch(
        producer_id,
        producer_epoch,
        commit,
        // CoordinatorEpoch — Apache populates it from the txn
        // coordinator's lease epoch. Phase 6 doesn't track that
        // distinctly from the assignment epoch; 0 keeps the wire
        // shape valid (consumers don't act on the field).
        0,
    ));
    for (topic, p) in partitions {
        let epoch = broker
            .coordinator()
            .and_then(|c| c.current_epoch(topic, *p))
            .unwrap_or_else(|| broker.local_lease.current_epoch());
        let _ = broker.engine.create_partition(topic, *p).await;
        if let Err(err) = broker
            .engine
            .append(topic, *p, epoch, ACKS_ALL, batch.clone())
            .await
        {
            tracing::warn!(
                topic,
                partition = p,
                %err,
                "txn marker append failed; transaction stays in Prepare state \
                 for retry / reconcile (gh #225)",
            );
            return Err(ERR_COORDINATOR_NOT_AVAILABLE);
        }
    }
    Ok(())
}

/// Hand peer-led partitions to the shared-PVC marker queue.
///
/// A successful `enqueue` is the durability boundary for a peer
/// partition: the entry is on the shared volume, and the peer's
/// `MarkerWatcher` retries application until it succeeds
/// (`ApplyOutcome::Retry` keeps the file). So enqueue success is
/// enough to complete the transaction; only enqueue *failure* is fatal
/// to this attempt.
fn enqueue_cross_broker_markers(
    broker: &Arc<Broker>,
    txn_id: &str,
    producer_id: i64,
    producer_epoch: i16,
    commit: bool,
    queued: &HashMap<String, Vec<(String, i32)>>,
) -> Result<(), i16> {
    let queue = match broker.marker_queue() {
        Some(q) => q,
        None => {
            // Before gh #225 this returned silently and EndTxn still
            // answered success, so peer partitions kept their LSO
            // pinned with nothing left to notice it.
            tracing::warn!(
                txn_id,
                "cross-broker markers needed but no MarkerQueue is wired; \
                 refusing to complete the transaction",
            );
            return Err(ERR_COORDINATOR_NOT_AVAILABLE);
        }
    };
    for (target_broker, parts) in queued {
        // Pack into TxnTopic so the schema matches what
        // MarkerWatcher applies on the other side.
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for (topic, p) in parts {
            by_topic.entry(topic.clone()).or_default().push(*p);
        }
        let partitions: Vec<TxnTopic> = by_topic
            .into_iter()
            .map(|(topic, partitions)| TxnTopic { topic, partitions })
            .collect();
        let entry = MarkerEntry {
            transactional_id: txn_id.to_owned(),
            producer_id,
            producer_epoch,
            commit,
            coordinator_epoch: 0,
            partitions,
        };
        if let Err(err) = queue.enqueue(target_broker, &entry) {
            tracing::warn!(
                target = %target_broker,
                txn_id,
                %err,
                "marker queue enqueue failed; transaction stays in Prepare \
                 state for retry / reconcile (gh #225)"
            );
            return Err(ERR_COORDINATOR_NOT_AVAILABLE);
        }
    }
    Ok(())
}

/// Drive every transaction stuck in `Prepare*` to completion.
///
/// Returns the number of transactions completed on this pass. A
/// transaction whose dispatch still fails is left prepared and retried
/// on the next pass — deliberately unbounded, because the alternative
/// (giving up) is a permanently pinned LSO on every partition it
/// touched.
///
/// `owns_txn` must be `Some` in multi-broker deployments: a slot file
/// has exactly one legal writer (NFS substrate rule 3).
///
/// The `Sync` bound is what lets the returned future be `Send`, so the
/// caller can drive this from a spawned task — `&T` is `Send` only when
/// `T: Sync`, and the closure is held across the dispatch awaits.
pub async fn reconcile_pending_markers(
    broker: &Arc<Broker>,
    store: &TxnStateStore,
    owns_txn: Option<&(dyn Fn(&str) -> bool + Sync)>,
) -> usize {
    let pending = store.pending_marker_dispatches(owns_txn);
    if pending.is_empty() {
        return 0;
    }
    let mut completed = 0;
    for p in pending {
        if let Err(code) =
            dispatch_markers(broker, &p.txn_id, p.pid, p.epoch, p.commit, &p.partitions).await
        {
            tracing::warn!(
                txn_id = %p.txn_id,
                producer_id = p.pid,
                error_code = code,
                "txn marker reconcile: dispatch still failing; \
                 leaving prepared for the next pass (gh #225)",
            );
            continue;
        }
        match store.complete_end_txn(&p.txn_id, p.pid, p.epoch, p.commit) {
            Ok(()) => {
                completed += 1;
                tracing::info!(
                    txn_id = %p.txn_id,
                    producer_id = p.pid,
                    commit = p.commit,
                    "txn marker reconcile: markers placed, transaction completed",
                );
            }
            Err(err) => {
                tracing::warn!(
                    txn_id = %p.txn_id,
                    %err,
                    "txn marker reconcile: markers placed but completing the \
                     transaction failed; will retry",
                );
            }
        }
    }
    completed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_coordinator::TxnState;
    use kaas_storage::{MemoryStorage, StorageEngine};

    fn broker_with_txn() -> (tempfile::TempDir, Arc<Broker>, Arc<TxnStateStore>) {
        let tmp = tempfile::tempdir().unwrap();
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        let store = Arc::new(TxnStateStore::open(tmp.path(), 0).unwrap());
        b.install_txn_state(store.clone());
        (tmp, b, store)
    }

    /// gh #225: a transaction the reaper timed out has no producer left
    /// to retry its EndTxn, so the reconcile is the only thing that can
    /// ever place its ABORT markers. Without it the partition's LSO
    /// stays pinned for good.
    #[tokio::test]
    async fn reconcile_completes_a_reaper_prepared_abort() {
        let (_t, b, store) = broker_with_txn();
        let (pid, epoch) = store
            .get_or_allocate_with_timeout("tx-1", 1_000, || 1)
            .unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                10_000,
            )
            .unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        let hwm_before = b.engine.high_watermark("t", 0).unwrap();

        // Reaper times it out — prepares, does not complete.
        assert_eq!(store.abort_overdue(20_000).len(), 1);
        assert_eq!(store.snapshot()["tx-1"].state, TxnState::PrepareAbort);

        let completed = reconcile_pending_markers(&b, &store, None).await;
        assert_eq!(completed, 1);

        // ABORT marker landed and the txn is finished.
        assert!(
            b.engine.high_watermark("t", 0).unwrap() > hwm_before,
            "reconcile must append the ABORT marker"
        );
        let snap = store.snapshot();
        assert_eq!(snap["tx-1"].state, TxnState::CompleteAbort);
        assert!(snap["tx-1"].partitions.is_empty());

        // Converged — a second pass finds nothing owing.
        assert_eq!(reconcile_pending_markers(&b, &store, None).await, 0);
    }

    /// The other rescue path: the EndTxn handler prepared, dispatch
    /// failed, and the producer never came back.
    #[tokio::test]
    async fn reconcile_completes_a_stranded_commit() {
        let (_t, b, store) = broker_with_txn();
        let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
        store
            .add_partitions(
                "tx-1",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        // Handler prepared but never completed (dispatch had failed).
        store.prepare_end_txn("tx-1", pid, epoch, true).unwrap();

        assert_eq!(reconcile_pending_markers(&b, &store, None).await, 1);
        assert_eq!(
            store.snapshot()["tx-1"].state,
            TxnState::CompleteCommit,
            "a stranded commit must be driven to completion"
        );
    }

    /// A slot file has one legal writer (NFS substrate rule 3), so the
    /// sweep must respect the ownership gate.
    #[tokio::test]
    async fn reconcile_honours_the_ownership_gate() {
        let (_t, b, store) = broker_with_txn();
        let (pid, epoch) = store.get_or_allocate("tx-theirs", || 1).unwrap();
        store
            .add_partitions(
                "tx-theirs",
                pid,
                epoch,
                &[TxnTopic {
                    topic: "t".into(),
                    partitions: vec![0],
                }],
                100,
            )
            .unwrap();
        b.engine.create_partition("t", 0).await.unwrap();
        store
            .prepare_end_txn("tx-theirs", pid, epoch, true)
            .unwrap();

        let owns = |id: &str| id == "tx-mine";
        assert_eq!(reconcile_pending_markers(&b, &store, Some(&owns)).await, 0);
        assert_eq!(
            store.snapshot()["tx-theirs"].state,
            TxnState::PrepareCommit,
            "another broker's transaction must be left alone"
        );
    }

    /// gh #314: with a coordinator installed, an unassigned partition is not
    /// "local". Writing its marker created the partition, conjuring a directory
    /// for a topic that doesn't exist, and with a path-like name, one outside
    /// the data dir. A topic that's gone has nothing to mark; otherwise the
    /// transaction stays prepared for the reconcile.
    #[tokio::test]
    async fn an_unassigned_partition_is_never_written_locally() {
        use crate::handlers::test_cluster;
        use crate::topic_registry::{TopicMeta, TopicRegistry};
        use kaas_storage::{DiskStorageEngine, Fs, PartitionConfig, RealFs, StorageEngine};

        let tmp = tempfile::tempdir().unwrap();
        let data = tmp.path().join("deep/data");
        let cluster = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
            fs,
            data.clone(),
            PartitionConfig::default(),
        ));
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "real".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let b = Arc::new(Broker::new(engine, topics, "test", 0));
        test_cluster::install(&b, cluster.path(), &[]);
        let dispatch = |topic: &str, partition: i32| {
            let b = b.clone();
            let parts = vec![TxnTopic {
                topic: topic.to_owned(),
                partitions: vec![partition],
            }];
            async move { dispatch_markers(&b, "tx", 7, 0, true, &parts).await }
        };

        // Before the watch has listed, absence means nothing: retry.
        assert!(dispatch("gone", 0).await.is_err());
        b.topics.mark_synced();
        // Listed and absent: gone, so there's nothing to mark.
        for (topic, partition) in [
            ("gone", 0),
            ("../../escaped", 0),
            ("real", 99),
            ("real", -7),
        ] {
            assert_eq!(dispatch(topic, partition).await, Ok(()), "{topic}");
        }
        // It exists, and the assignment hasn't caught up: retry.
        assert!(dispatch("real", 0).await.is_err());

        assert!(!tmp.path().join("escaped").exists());
        assert!(!data.join("gone").exists());
        assert!(!data.join("real").exists());
    }
}
