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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use kaas_coordinator::{MarkerEntry, TxnStateStore, TxnTopic};

use crate::broker::Broker;
use crate::control_batch::build_control_batch;
use crate::marker_watcher::{ApplyOutcome, MarkerApplier};

/// Does this broker coordinate a transactional id? See
/// [`reconcile_pending_markers`].
pub type OwnsTxn = Arc<dyn Fn(&str) -> bool + Send + Sync>;

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
    let mut local_partitions: Vec<(String, i32)> = Vec::new();
    let mut queued: HashMap<String, Vec<(String, i32)>> = HashMap::new();
    for TxnTopic { topic, partitions } in partitions {
        for &p in partitions {
            match route(broker, topic, p) {
                Route::Local => local_partitions.push((topic.clone(), p)),
                Route::Peer(id) => queued.entry(id).or_default().push((topic.clone(), p)),
                Route::Gone => tracing::info!(
                    topic,
                    partition = p,
                    "txn marker skipped: the partition no longer exists"
                ),
                Route::Unknown => return Err(ERR_COORDINATOR_NOT_AVAILABLE),
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
        // A durable file write per peer (gh #328).
        let (b, id) = (broker.clone(), txn_id.to_owned());
        broker
            .coordinator_io(move || {
                enqueue_cross_broker_markers(&b, &id, producer_id, producer_epoch, commit, &queued)
            })
            .await
            .unwrap_or(Err(ERR_COORDINATOR_NOT_AVAILABLE))?;
    }
    Ok(())
}

/// Where a partition's marker goes.
#[derive(Debug, PartialEq, Eq)]
enum Route {
    /// This broker leads it, or there's no coordinator (dev mode).
    Local,
    /// Another broker leads it.
    Peer(String),
    /// Its topic, or the partition, no longer exists: nothing to mark.
    Gone,
    /// Nobody leads it yet: try again later.
    Unknown,
}

/// The one routing rule for a marker, for the coordinator's dispatch and for
/// a peer forwarding what it found in its inbox (gh #316).
///
/// An unassigned partition is never local with a coordinator installed.
/// Treating it as local created the partition to write the marker, conjuring
/// a directory for a topic that no longer exists (gh #314). It's gone only
/// once the topic watch has listed and the partition isn't there. Anything
/// else (the watch hasn't listed yet, or the assignment hasn't caught up to
/// the topic, or this broker has just started and loaded none) is unknown.
fn route(broker: &Broker, topic: &str, partition: i32) -> Route {
    let Some(c) = broker.coordinator() else {
        return Route::Local;
    };
    match c.leader_for(topic, partition) {
        Some(id) if id == c.self_id() => Route::Local,
        Some(id) => Route::Peer(id),
        None => {
            let exists = broker
                .topics
                .get(topic)
                .is_some_and(|m| (0..m.partition_count).contains(&partition));
            if broker.topics.is_synced() && !exists {
                Route::Gone
            } else {
                Route::Unknown
            }
        }
    }
}

/// The production [`MarkerApplier`]: what a broker does with an entry in its
/// marker-queue inbox (gh #175).
///
/// A partition it leads gets the control batch, appended with `acks = -1`.
/// One that moved since the entry was written is forwarded to the broker that
/// leads it now, and one nobody leads yet is kept for the next tick (gh
/// #316). Dropping either lost an acknowledged commit, or let a
/// `read_committed` consumer read an aborted record, since `EndTxn` had
/// already answered and nothing would send the marker again.
#[derive(Debug)]
pub struct BrokerMarkerApplier {
    broker: Arc<Broker>,
}

impl BrokerMarkerApplier {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl MarkerApplier for BrokerMarkerApplier {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
        let batch = Bytes::from(build_control_batch(
            entry.producer_id,
            entry.producer_epoch,
            entry.commit,
            entry.coordinator_epoch,
        ));
        let mut forward: BTreeMap<String, BTreeMap<String, Vec<i32>>> = BTreeMap::new();
        let mut keep: BTreeMap<String, Vec<i32>> = BTreeMap::new();
        for TxnTopic { topic, partitions } in &entry.partitions {
            for &p in partitions {
                match route(&self.broker, topic, p) {
                    Route::Local => {
                        if let Err(err) = append_marker(&self.broker, topic, p, &batch).await {
                            tracing::warn!(
                                topic,
                                partition = p,
                                %err,
                                "MarkerWatcher: control-batch append failed; keeping \
                                 it for the next tick"
                            );
                            keep.entry(topic.clone()).or_default().push(p);
                        }
                    }
                    Route::Peer(id) => forward
                        .entry(id)
                        .or_default()
                        .entry(topic.clone())
                        .or_default()
                        .push(p),
                    Route::Gone => tracing::info!(
                        topic,
                        partition = p,
                        pid = entry.producer_id,
                        "MarkerWatcher: marker skipped: the partition no longer exists"
                    ),
                    Route::Unknown => {
                        tracing::debug!(
                            topic,
                            partition = p,
                            pid = entry.producer_id,
                            "MarkerWatcher: no leader for the partition yet; keeping its \
                             marker for the next tick"
                        );
                        keep.entry(topic.clone()).or_default().push(p);
                    }
                }
            }
        }
        let topics = |m: BTreeMap<String, Vec<i32>>| {
            m.into_iter()
                .map(|(topic, partitions)| TxnTopic { topic, partitions })
                .collect::<Vec<_>>()
        };
        ApplyOutcome {
            forward: forward.into_iter().map(|(id, m)| (id, topics(m))).collect(),
            keep: topics(keep),
        }
    }
}

/// Append one marker to a partition this broker leads, at its epoch.
async fn append_marker(
    broker: &Broker,
    topic: &str,
    partition: i32,
    batch: &Bytes,
) -> Result<(), kaas_storage::StorageError> {
    let epoch = broker
        .coordinator()
        .and_then(|c| c.current_epoch(topic, partition))
        .unwrap_or_else(|| broker.local_lease.current_epoch());
    let _ = broker.engine.create_partition(topic, partition).await;
    broker
        .engine
        .append(topic, partition, epoch, ACKS_ALL, batch.clone())
        .await
        .map(|_| ())
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
        if let Err(err) = append_marker(broker, topic, *p, &batch).await {
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
pub async fn reconcile_pending_markers(
    broker: &Arc<Broker>,
    store: &Arc<TxnStateStore>,
    owns_txn: Option<OwnsTxn>,
) -> usize {
    let swept = store.clone();
    let pending = broker
        .coordinator_io(move || match &owns_txn {
            Some(owns) => swept.pending_marker_dispatches(Some(&**owns)),
            None => swept.pending_marker_dispatches(None),
        })
        .await
        .unwrap_or_default();
    if pending.is_empty() {
        return 0;
    }
    let mut completed = 0;
    for p in pending {
        // Offsets before markers, or every pass over a failing write would
        // re-append a control batch to each partition (gh #318).
        let (id, pid, epoch, commit) = (p.txn_id.clone(), p.pid, p.epoch, p.commit);
        let settled = broker
            .txn_store_io(store, move |s| s.settle_offsets(&id, pid, epoch, commit))
            .await;
        if let Err(err) = settled {
            tracing::warn!(
                txn_id = %p.txn_id,
                %err,
                "txn marker reconcile: offsets not settled; leaving prepared for the next pass",
            );
            continue;
        }
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
        let id = p.txn_id.clone();
        let finished = broker
            .txn_store_io(store, move |s| s.complete_end_txn(&id, pid, epoch, commit))
            .await;
        match finished {
            Ok(()) => {
                completed += 1;
                tracing::info!(
                    txn_id = %p.txn_id,
                    producer_id = p.pid,
                    commit = p.commit,
                    "txn marker reconcile: transaction completed",
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

/// One tick of the timeout reaper and the marker reconcile. Returns how many
/// overdue transactions it prepared to abort, and how many it completed.
///
/// `Broker::owns_txn` gates both (gh #91). A txn slot file has exactly one legal
/// writer, its coordinator, so an ungated sweep had every broker
/// read-modify-writing the same slot on the shared volume (NFS substrate rule
/// 3). It degrades in the safe direction at both edges: it answers `true` when
/// no Coordinator is installed (dev / single-broker), so nothing stops being
/// swept, and in cluster mode `false` for everything until the first
/// `assignment.json` load, which delays a sweep by one 1 s poll rather than
/// skipping it.
pub async fn sweep_transactions(
    broker: &Arc<Broker>,
    store: &Arc<TxnStateStore>,
    now_ms: i64,
) -> (usize, usize) {
    let gate = broker.clone();
    let owns: OwnsTxn = Arc::new(move |id: &str| gate.owns_txn(id));
    let (swept, owned) = (store.clone(), owns.clone());
    // Reads every slot file under the store's mutex (gh #328).
    let aborted = broker
        .coordinator_io(move || swept.abort_overdue_owned(now_ms, Some(&*owned)).len())
        .await
        .unwrap_or_default();
    let completed = reconcile_pending_markers(broker, store, Some(owns)).await;
    (aborted, completed)
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

    /// gh #318: a prepared transaction whose offset write keeps failing, and
    /// whose producer is gone. Each pass leaves it prepared and places no
    /// marker, then finishes it unaided once the write works. Both with a data
    /// partition and offsets-only, which has no markers at all.
    #[tokio::test]
    async fn reconcile_retries_a_failing_offset_write_and_places_no_markers_until_it_lands() {
        struct Hook(std::sync::atomic::AtomicBool);
        impl kaas_coordinator::TxnOffsetHook for Hook {
            fn on_end_txn(&self, _: &str, _: i64, _: bool) -> std::io::Result<()> {
                if self.0.load(std::sync::atomic::Ordering::SeqCst) {
                    return Err(std::io::Error::other("disk full"));
                }
                Ok(())
            }
        }
        for with_partition in [true, false] {
            let (_t, b, store) = broker_with_txn();
            let hook = Arc::new(Hook(std::sync::atomic::AtomicBool::new(true)));
            store.set_offset_hook(hook.clone());
            let (pid, epoch) = store.get_or_allocate("tx-1", || 1).unwrap();
            store
                .add_offsets_to_txn("tx-1", pid, epoch, "g", 100)
                .unwrap();
            if with_partition {
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
            }
            store.prepare_end_txn("tx-1", pid, epoch, true).unwrap();
            let hwm = || b.engine.high_watermark("t", 0).unwrap_or(0);
            let before = hwm();

            for _ in 0..3 {
                assert_eq!(reconcile_pending_markers(&b, &store, None).await, 0);
                assert_eq!(store.snapshot()["tx-1"].state, TxnState::PrepareCommit);
                assert_eq!(hwm(), before, "a marker was placed");
            }

            hook.0.store(false, std::sync::atomic::Ordering::SeqCst);
            assert_eq!(reconcile_pending_markers(&b, &store, None).await, 1);
            assert_eq!(store.snapshot()["tx-1"].state, TxnState::CompleteCommit);
            assert_eq!(hwm(), before + i64::from(with_partition));
            assert_eq!(reconcile_pending_markers(&b, &store, None).await, 0);
        }
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

        let owns: OwnsTxn = Arc::new(|id: &str| id == "tx-mine");
        assert_eq!(reconcile_pending_markers(&b, &store, Some(owns)).await, 0);
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

    /// A transactional data batch from `pid`: the control batch's bytes with
    /// the control bit cleared.
    fn txn_batch(pid: i64) -> Bytes {
        let mut b = build_control_batch(pid, 0, true, 0);
        b[21..23].copy_from_slice(&0x10_i16.to_be_bytes());
        let crc = kaas_codec::crc::compute(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    /// Broker `kaas-<n>` on `cluster`, with its own memory engine, topic `t`
    /// (2 partitions) listed, and the shared marker queue wired.
    fn peer(n: i32, cluster: &std::path::Path) -> Arc<Broker> {
        listed_peer(n, cluster, true)
    }

    /// [`peer`], with the topic watch done listing or not.
    fn listed_peer(n: i32, cluster: &std::path::Path, synced: bool) -> Arc<Broker> {
        use crate::topic_registry::TopicMeta;
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 2,
            topic_id: [0; 16],
        });
        if synced {
            topics.mark_synced();
        }
        let b = Arc::new(Broker::new(engine, topics, "test", n));
        b.install_marker_queue(kaas_coordinator::MarkerQueue::open(cluster).unwrap());
        b
    }

    fn watcher(b: &Arc<Broker>) -> crate::marker_watcher::MarkerWatcher {
        crate::marker_watcher::MarkerWatcher::new(
            b.marker_queue().unwrap(),
            b.self_id.clone(),
            Arc::new(BrokerMarkerApplier::new(b.clone())),
        )
    }

    fn t(partitions: Vec<i32>) -> TxnTopic {
        TxnTopic {
            topic: "t".to_owned(),
            partitions,
        }
    }

    /// gh #316: each partition of an entry gets one of four fates. Led here:
    /// marked. Led elsewhere: forwarded. Exists but nobody leads it: kept.
    /// Gone: dropped, but only once the topic watch has listed.
    #[tokio::test]
    async fn the_applier_marks_forwards_keeps_and_drops() {
        use crate::handlers::test_cluster::{self, PEER, SELF};
        let cluster = tempfile::tempdir().unwrap();
        for synced in [true, false] {
            let b = listed_peer(0, cluster.path(), synced);
            test_cluster::install(&b, cluster.path(), &[("t", 0, SELF), ("t", 1, PEER)]);
            let entry = MarkerEntry {
                transactional_id: "tx".to_owned(),
                producer_id: 7,
                producer_epoch: 0,
                commit: true,
                coordinator_epoch: 0,
                partitions: vec![
                    t(vec![0, 1]),
                    TxnTopic {
                        topic: "gone".to_owned(),
                        partitions: vec![0],
                    },
                ],
            };
            let out = BrokerMarkerApplier::new(b.clone()).apply(&entry).await;
            assert_eq!(out.forward, [(PEER.to_owned(), vec![t(vec![1])])]);
            let gone = TxnTopic {
                topic: "gone".to_owned(),
                partitions: vec![0],
            };
            if synced {
                assert!(out.keep.is_empty(), "{:?}", out.keep);
            } else {
                assert_eq!(
                    out.keep,
                    [gone],
                    "absent means nothing before the watch lists"
                );
            }
            assert_eq!(b.engine.high_watermark("t", 0).unwrap(), 1, "marked here");

            // t/1 moves to nobody: it exists, so its marker is kept.
            test_cluster::write(cluster.path(), 2, &[SELF, PEER], &[("t", 0, SELF)]);
            assert!(b.coordinator().unwrap().apply_if_new());
            let out = BrokerMarkerApplier::new(b.clone())
                .apply(&MarkerEntry {
                    partitions: vec![t(vec![1])],
                    ..entry
                })
                .await;
            assert_eq!(
                out,
                ApplyOutcome {
                    forward: Vec::new(),
                    keep: vec![t(vec![1])],
                }
            );
        }
    }

    /// gh #316, the issue's sequences: the coordinator queued the marker for
    /// the partition's old leader, and the partition moved before that broker
    /// read it. The old leader dropped it, so an acknowledged commit stayed
    /// invisible to `read_committed` for good, and an abort's records were
    /// never marked aborted. Now it's forwarded and the new leader writes it.
    #[tokio::test]
    async fn a_marker_queued_for_the_old_leader_reaches_the_new_one() {
        use crate::handlers::test_cluster;
        for commit in [true, false] {
            let cluster = tempfile::tempdir().unwrap();
            let (a, b) = (peer(0, cluster.path()), peer(1, cluster.path()));
            let brokers = ["kaas-0", "kaas-1"];
            test_cluster::write(cluster.path(), 1, &brokers, &[("t", 0, "kaas-1")]);
            for x in [&a, &b] {
                let c = test_cluster::coordinator(&x.self_id, cluster.path());
                assert!(c.apply_if_new());
                x.install_coordinator(c);
            }
            // The transaction's record, in the log the new leader will serve.
            a.engine.create_partition("t", 0).await.unwrap();
            a.engine.append("t", 0, 2, -1, txn_batch(7)).await.unwrap();
            assert_eq!(a.engine.last_stable_offset("t", 0).unwrap(), 0);

            // EndTxn on kaas-0, which still sees kaas-1 leading.
            assert_eq!(
                dispatch_markers(&a, "tx", 7, 0, commit, &[t(vec![0])]).await,
                Ok(())
            );
            assert_eq!(a.marker_queue().unwrap().list("kaas-1").unwrap().len(), 1);

            // The partition moves to kaas-0 before kaas-1 reads its inbox.
            test_cluster::write(cluster.path(), 2, &brokers, &[("t", 0, "kaas-0")]);
            for x in [&a, &b] {
                assert!(x.coordinator().unwrap().apply_if_new());
            }
            watcher(&b).tick().await;
            watcher(&a).tick().await;

            assert_eq!(
                a.engine.high_watermark("t", 0).unwrap(),
                2,
                "marker written"
            );
            assert_eq!(
                a.engine.last_stable_offset("t", 0).unwrap(),
                2,
                "commit={commit}"
            );
            let aborted = a.engine.aborted_transactions_in_range("t", 0, 0, 2);
            assert_eq!(aborted.len(), usize::from(!commit), "commit={commit}");
            let q = a.marker_queue().unwrap();
            assert!(q.list("kaas-0").unwrap().is_empty());
            assert!(q.list("kaas-1").unwrap().is_empty());
        }
    }

    /// gh #316: a broker that has just started reads its inbox before it has
    /// loaded an assignment, when it leads nothing. Dropping the marker then
    /// left its partition's last stable offset pinned once it took the
    /// partition back. It's kept until a leader is known.
    #[tokio::test]
    async fn a_marker_read_before_the_first_assignment_load_is_kept() {
        use crate::handlers::test_cluster;
        let cluster = tempfile::tempdir().unwrap();
        let b = peer(1, cluster.path());
        b.install_coordinator(test_cluster::coordinator("kaas-1", cluster.path()));
        b.engine.create_partition("t", 0).await.unwrap();
        b.engine.append("t", 0, 1, -1, txn_batch(7)).await.unwrap();
        let q = b.marker_queue().unwrap();
        q.enqueue(
            "kaas-1",
            &MarkerEntry {
                transactional_id: "tx".to_owned(),
                producer_id: 7,
                producer_epoch: 0,
                commit: true,
                coordinator_epoch: 0,
                partitions: vec![t(vec![0])],
            },
        )
        .unwrap();

        let w = watcher(&b);
        w.tick().await;
        assert_eq!(q.list("kaas-1").unwrap().len(), 1, "kept, not dropped");
        assert_eq!(b.engine.high_watermark("t", 0).unwrap(), 1);

        test_cluster::write(
            cluster.path(),
            1,
            &["kaas-0", "kaas-1"],
            &[("t", 0, "kaas-1")],
        );
        assert!(b.coordinator().unwrap().apply_if_new());
        w.tick().await;
        assert!(q.list("kaas-1").unwrap().is_empty());
        assert_eq!(b.engine.last_stable_offset("t", 0).unwrap(), 2);
    }
}
