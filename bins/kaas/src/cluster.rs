//! Phase 5 cluster + consumer-group bring-up.
//!
//! Wires together the [`Manager`], [`OffsetStore`], [`Coordinator`],
//! [`TakeoverDriver`], [`GroupTakeoverDriver`], and (in single-
//! broker dev mode) the [`AssignmentLoop`] that writes the
//! authoritative `assignment.json` the Coordinator watches.
//!
//! Three modes, picked at boot:
//!
//! - **In-memory** (`MemoryStorage`) — Manager + LocalGroupSource;
//!   no Coordinator wired. Produce/Fetch fall through to the
//!   "always lead" `LocalLeaseManager` path. Consumer-group APIs
//!   (`Manager`) work fully.
//! - **Single-broker disk** (`KAAS_DATA_DIR` set, `MY_POD_NAME`
//!   unset) — adds a Coordinator + AssignmentLoop. The loop writes
//!   a self-only `assignment.json` every recompute; the Coordinator
//!   watcher picks it up and stamps ownership. Manager
//!   hot-swaps to the Coordinator-backed source via gh #92.
//! - **Cluster** (`MY_POD_NAME` set + kube client + disk) — the
//!   full multi-broker runtime: `KubeLeaseEpoch` (1 s Lease poll →
//!   stale-epoch fence), `BrokerRegistry` (EndpointSlice watch),
//!   `HeartbeatClient` (bidi stream to the Lease holder, feeds the
//!   gh #62 produce self-fence), and a `KubeLeaseElection` campaign
//!   that runs the controller stack (heartbeat gRPC server +
//!   `AssignmentLoop` + broker-set watcher + topic-change trigger)
//!   only while this broker holds the `kaas-controller` Lease.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use bytes::Bytes;
use kaas_broker::coordinator::{HeartbeatSource, LeaseEpochSource};
use kaas_broker::heartbeatpb::controller_heartbeat_server::ControllerHeartbeatServer;
use kaas_broker::heartbeatpb::BrokerStatus;
use kaas_broker::{
    build_control_batch, ApplyOutcome, Broker, Coordinator, FenceWatcher, GroupTakeoverDriver,
    HeartbeatClient, LocalHeartbeat, LocalLeaseEpoch, MarkerApplier, MarkerWatcher,
    ProducerEpochFencer, TakeoverDriver, TargetResolver,
};
use kaas_controller::{
    AssignmentLoop, AssignmentReason, BrokerSource, HeartbeatServer, HeartbeatService,
    KubeLeaseElection, LocalElection, StaticSources,
};
use kaas_coordinator::{
    fence_log_dir, BrokerEndpoint, BrokerLookup, FenceLog, FnLookup, LocalGroupSource,
    LocalTxnSource, Manager, MarkerEntry, MarkerQueue, OffsetStore, TxnOffsetHook, TxnStateStore,
};
use kaas_k8s::kube_watchers::{run_endpoint_watch, run_lease_watch, KubeLeaseEpoch};
use kaas_k8s::{BrokerIdentity, BrokerRegistry};
use kaas_storage::StorageEngine;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use kaas_broker::TopicRegistry;

/// Name of the singleton controller Lease — same object earlier
/// releases elect on, so a mixed-version rollout can't split-brain.
const CONTROLLER_LEASE_NAME: &str = "kaas-controller";

/// Cadence of the txn-timeout reaper. Matches Apache Kafka's
/// `transaction.abort.timed.out.transaction.cleanup.interval.ms`
/// default.
const TXN_REAPER_INTERVAL: Duration = Duration::from_secs(10);

/// gh #215: cadence of the takeover reconcile backstop. Fast enough to
/// un-stick a rollout within a couple of ticks, slow enough not to
/// churn — `take_over` is idempotent so an extra pass is cheap.
const TAKEOVER_RECONCILE_INTERVAL: Duration = Duration::from_secs(10);

/// What was installed. Returned so `main.rs` can drop the handles
/// on shutdown.
pub struct ClusterRuntime {
    /// Held so the Arc stays alive at least as long as `main`
    /// runs; the Broker installs and owns its own clone.
    #[allow(dead_code)]
    pub manager: Arc<Manager>,
    /// Same — Broker holds an installed Arc; this is a convenient
    /// handle for the harness.
    #[allow(dead_code)]
    pub coordinator: Option<Arc<Coordinator>>,
    /// Phase 6 transactional-state store. `Some` whenever a data
    /// dir is configured (dev `MemoryStorage` paths use a tempdir so
    /// the gh #22 rejoin contract still works under unit tests).
    /// Broker holds an installed Arc; this is a convenient handle.
    #[allow(dead_code)]
    pub txn_state: Arc<TxnStateStore>,
    /// Phase 6 outbound fence log (gh #108). Held so the Arc lives
    /// at least as long as `main`.
    #[allow(dead_code)]
    pub fence_log: Arc<FenceLog>,
    /// gh #175 cross-broker marker queue. Held for the same reason.
    #[allow(dead_code)]
    pub marker_queue: MarkerQueue,
    /// Background tasks the runtime owns. Aborted on shutdown.
    pub tasks: Vec<tokio::task::JoinHandle<()>>,
}

/// Build the consumer-group + offsets Manager and (when a data dir
/// is configured) the Coordinator + AssignmentLoop. Installs both
/// on the [`Broker`] so handlers can read them.
#[allow(clippy::too_many_arguments)]
pub fn install(
    broker: Arc<Broker>,
    topics: Arc<TopicRegistry>,
    engine: Arc<dyn StorageEngine>,
    data_dir: Option<std::path::PathBuf>,
    cluster_dir: Option<std::path::PathBuf>,
    broker_id: i32,
    cluster_id: &str,
    cancel: CancellationToken,
    kube: Option<kube::Client>,
    client_port: i32,
    topic_notify: Arc<TopicChangeNotifier>,
    txn_num_slots: usize,
) -> Result<ClusterRuntime> {
    let self_id = format!("kaas-{broker_id}");

    // Kube-backed multi-broker wiring (election, endpoint registry,
    // heartbeats) exists only when the pod identity, a kube client,
    // AND disk storage are all present. Prepared before the Manager
    // so FindCoordinator's BrokerLookup can resolve peer brokers.
    let wiring = match (&kube, &data_dir) {
        (Some(client), Some(_)) if in_cluster_pod() => Some(prepare_cluster_wiring(
            client.clone(),
            &self_id,
            client_port,
        )?),
        _ => None,
    };

    // Cluster-state root: the caller's resolved `KAAS_CLUSTER_DIR`
    // (gh #221 phase 1 — its own volume when the chart's
    // `storage.controlPlane` is enabled), falling back to the legacy
    // `<data_dir>/__cluster`; dev mode (neither) gets a tmp dir so
    // the stores still function under unit tests and single-binary
    // smoke runs.
    //
    // Offsets live HERE, not at the data-dir root — a sibling of the
    // topic dirs is exactly what the operator's orphan-topic sweep
    // reclaims, and the pre-fix layout lost committed offsets to it
    // every 5 minutes (gh #223). The legacy-dir adoption shim was
    // dropped (pre-v1 no-backcompat policy — fresh deploy across
    // gh #223).
    let cluster_dir = cluster_dir
        .or_else(|| data_dir.clone().map(|d| d.join("__cluster")))
        .unwrap_or_else(|| std::path::PathBuf::from("/tmp/kaas-cluster-mem"));
    std::fs::create_dir_all(&cluster_dir)?;

    let offsets = Arc::new(OffsetStore::new(&cluster_dir));
    let lookup: Arc<dyn BrokerLookup> = match &wiring {
        Some(w) => registry_lookup(w.registry.clone()),
        None => self_endpoint_lookup(&self_id, client_port),
    };

    // Cluster mode: Metadata advertises the live broker set instead
    // of self-only, so clients route produce/fetch to actual
    // partition leaders.
    if let Some(w) = &wiring {
        broker.install_broker_view(Arc::new(RegistryBrokerView {
            registry: w.registry.clone(),
        }));
    }
    let manager = Manager::new(
        self_id.clone(),
        offsets,
        lookup,
        LocalGroupSource::new(self_id.clone()),
    );
    // Phase 6 bootstrap: txn assignment source. Hot-swapped to the
    // Coordinator below when a data dir is configured.
    manager.set_txn_assignment_source(LocalTxnSource::new(self_id.clone()));
    broker.install_coord_manager(manager.clone());
    info!(
        broker_id = self_id.as_str(),
        cluster_id, "installed Manager (LocalGroupSource + LocalTxnSource bootstrap)"
    );

    // Phase 6 transactional-state store + fence log, rooted in the
    // same cluster-state dir resolved above (gh #22 rejoin contract
    // holds in dev mode via the tmp-dir fallback).
    // gh #255: the chart-emitted KAAS_TXN_NUM_SLOTS was ignored from
    // the Rust cutover (always the hard-coded default). 0 = store
    // default (50). Changing it on a live cluster re-shards txn
    // ownership — the values.yaml comment carries the drain warning.
    let txn_state = Arc::new(TxnStateStore::open(&cluster_dir, txn_num_slots)?);
    broker.install_txn_state(txn_state.clone());
    info!(
        slots = txn_state.num_slots(),
        cluster_dir = %cluster_dir.display(),
        "installed TxnStateStore",
    );

    // EndTxn / reaper → group-coordinator offset commit/discard.
    let hook: Arc<dyn TxnOffsetHook> = Arc::new(OffsetStoreHook {
        manager: manager.clone(),
    });
    txn_state.set_offset_hook(hook);

    let fence_dir = fence_log_dir(&cluster_dir);
    let fence_log = Arc::new(FenceLog::open(&fence_dir, &self_id)?);
    broker.install_fence_log(fence_log.clone());
    info!(path = %fence_log.path().display(), "opened FenceLog");

    let marker_queue = MarkerQueue::open(&cluster_dir)?;
    broker.install_marker_queue(marker_queue.clone());
    info!(
        inbox = %marker_queue.inbox(&self_id).display(),
        "opened MarkerQueue"
    );

    let mut tasks = Vec::new();

    // FenceWatcher: poll peer brokers' producer_fences/from-*.json
    // every 2s and dispatch new (pid, epoch) pairs into the local
    // storage engine's cross-partition fence walker (gh #170).
    let fencer: Arc<dyn ProducerEpochFencer> = Arc::new(EngineFencer {
        engine: engine.clone(),
    });
    let watcher = Arc::new(FenceWatcher::new(fence_dir, &self_id, fencer));
    let watcher_cancel = cancel.clone();
    let watcher_clone = watcher.clone();
    tasks.push(tokio::spawn(async move {
        watcher_clone.run(watcher_cancel).await;
    }));

    // gh #175 MarkerWatcher: poll the per-broker marker inbox every
    // 2 s and apply each commit/abort marker to the partitions we
    // currently lead.
    let applier: Arc<dyn MarkerApplier> = Arc::new(BrokerMarkerApplier {
        broker: broker.clone(),
    });
    let marker_watcher = Arc::new(MarkerWatcher::new(marker_queue.inbox(&self_id), applier));
    let mw_cancel = cancel.clone();
    let mw_clone = marker_watcher.clone();
    tasks.push(tokio::spawn(async move {
        mw_clone.run(mw_cancel).await;
    }));

    // Txn-timeout reaper (gh #28) + marker reconcile (gh #225). Walks
    // every slot every 10 s: prepares Ongoing entries past their
    // TransactionTimeoutMs for abort, then drives every prepared
    // transaction to Complete by placing its markers.
    let reaper_cancel = cancel.clone();
    let reaper_store = txn_state.clone();
    let reaper_broker = broker.clone();
    tasks.push(tokio::spawn(async move {
        run_txn_reaper(reaper_store, reaper_broker, reaper_cancel).await;
    }));
    let coordinator = match data_dir {
        None => {
            info!("dev mode (MemoryStorage) — Coordinator + AssignmentLoop skipped");
            None
        }
        Some(dir) => {
            // Coordinator watches <data_dir>/__cluster/assignment.json
            // via a 1 s poll. Cluster mode wires the kube-backed
            // seams (Lease-poll epoch fence + heartbeat freshness);
            // dev disk mode keeps the Local stubs.
            let (lease_src, heart_src): (Arc<dyn LeaseEpochSource>, Arc<dyn HeartbeatSource>) =
                match &wiring {
                    Some(w) => (w.lease_epoch.clone(), w.heart.clone()),
                    None => (Arc::new(LocalLeaseEpoch), Arc::new(LocalHeartbeat)),
                };
            let coordinator =
                Coordinator::new(self_id.clone(), cluster_dir.clone(), lease_src, heart_src);
            if wiring.is_some() {
                // Real heartbeat stream wired → arm the gh #62
                // produce-path self-fence (3 s staleness bound).
                coordinator.enable_self_fence();
            }
            broker.install_coordinator(coordinator.clone());

            // Hot-swap the Manager's group + txn sources to the
            // assignment-aware Coordinator (gh #92, gh #91). Without
            // the txn swap, FindCoordinator(KeyType=transaction)
            // would keep returning self for every txnID — the
            // LocalTxnSource bootstrap — instead of routing
            // through hash(txnID) % numBrokers.
            manager.set_group_assignment_source(coordinator.clone());
            manager.set_txn_assignment_source(coordinator.clone());

            // Drivers fire on every assignment apply.
            let takeover = TakeoverDriver::new(engine.clone(), self_id.clone());
            coordinator.on_assignment_change(takeover.as_handler());
            let group_takeover = GroupTakeoverDriver::new(manager.clone(), self_id.clone());
            coordinator.on_assignment_change(group_takeover.as_handler());

            // gh #215: takeover reconcile backstop. on_assignment_change
            // only re-fires take_over on an epoch change, so a transient
            // failure (the gh #203 ENOENT race) is never retried — the
            // partition stays unopened and, under honest readiness
            // (gh #208), wedges the broker NotReady and stalls the
            // rollout. This loop re-drives take_over for any
            // assigned-but-unopened partition (idempotent — NFS rule 2).
            {
                let takeover_rec = takeover.clone();
                let coord_rec = coordinator.clone();
                let cancel_rec = cancel.clone();
                tasks.push(tokio::spawn(async move {
                    let mut tick = tokio::time::interval(TAKEOVER_RECONCILE_INTERVAL);
                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                    loop {
                        tokio::select! {
                            () = cancel_rec.cancelled() => break,
                            _ = tick.tick() => {
                                if let Some(a) = coord_rec.snapshot() {
                                    let n = takeover_rec.reconcile(a.as_ref());
                                    if n > 0 {
                                        debug!(redriven = n, "takeover reconcile re-drove partitions");
                                    }
                                }
                            }
                        }
                    }
                }));
            }

            tasks.push(coordinator.spawn_watcher());
            info!(
                data_dir = %dir.display(),
                "Coordinator + takeover drivers wired"
            );

            // Controller-side assignment writing. Cluster mode: the
            // Lease election decides which broker runs the
            // AssignmentLoop (spawn_cluster_tasks); every other
            // broker just watches the shared assignment.json. Dev
            // disk mode: this broker is trivially the controller of
            // its private data_dir — run the single-broker loop.
            let is_controller = Arc::new(AtomicBool::new(wiring.is_none()));
            match wiring {
                Some(w) => spawn_cluster_tasks(
                    w,
                    dir.clone(),
                    cluster_dir.clone(),
                    self_id.clone(),
                    topics.clone(),
                    manager.clone(),
                    coordinator.clone(),
                    is_controller.clone(),
                    topic_notify.clone(),
                    cancel.clone(),
                ),
                None => {
                    if let Some(handle) = spawn_single_broker_assignment_loop(
                        dir.clone(),
                        cluster_dir.clone(),
                        self_id.clone(),
                        topics.clone(),
                        cancel.clone(),
                    )? {
                        tasks.push(handle);
                    }
                }
            }

            // Feed the Phase-10 observable gauges (is_controller,
            // assignment_version, broker counts, per-partition
            // leader/epoch/HWM) from the live runtime. Without this
            // the gauges registered at bootstrap report zero forever
            // and the Grafana headline panels flatline.
            kaas_observability::set_gauge_source(Some(Box::new(RuntimeGaugeSource {
                coordinator: coordinator.clone(),
                engine: engine.clone(),
                is_controller,
            })));

            Some(coordinator)
        }
    };

    Ok(ClusterRuntime {
        manager,
        coordinator,
        txn_state,
        fence_log,
        marker_queue,
        tasks,
    })
}

/// Bridges [`TxnOffsetHook`] (from the transactional coordinator)
/// to the consumer-group `Manager`'s `OffsetStore`. On `EndTxn`
/// commit, the txn coord fires the hook for each `(group, pid)`
/// that staged offsets via `TxnOffsetCommit`; the hook materialises
/// the pending entry into the durable offset map. On abort, it
/// discards the pending entry.
struct OffsetStoreHook {
    manager: Arc<Manager>,
}

impl TxnOffsetHook for OffsetStoreHook {
    fn on_end_txn(&self, group_id: &str, producer_id: i64, commit: bool) {
        if commit {
            if let Err(err) = self.manager.offsets.commit_pending(group_id, producer_id) {
                warn!(
                    group_id, producer_id, %err,
                    "txn offset hook: commit_pending failed; staged offsets remain pending"
                );
            }
        } else {
            self.manager.offsets.discard_pending(group_id, producer_id);
        }
    }
}

/// [`ProducerEpochFencer`] adapter that bridges the
/// [`FenceWatcher`]'s per-peer fence dispatch into the storage
/// engine's cross-partition `fence_producer_epoch` walker
/// (gh #170). Inbound peer fences are applied to every partition
/// this broker leads so a zombie batch from an old session is
/// rejected even on partitions the new session hasn't yet
/// touched (gh #30).
struct EngineFencer {
    engine: Arc<dyn StorageEngine>,
}

impl ProducerEpochFencer for EngineFencer {
    fn fence_producer_epoch(&self, pid: i64, epoch: i16) {
        self.engine.fence_producer_epoch(pid, epoch);
    }
}

/// [`MarkerApplier`] adapter that bridges the inbound marker queue
/// into the storage engine. For each `(topic, partition)` in the
/// entry that this broker currently leads, builds a control batch
/// via [`build_control_batch`] and appends it with `acks = -1`.
/// Partitions led by another broker are silently skipped — the
/// dispatcher should not have targeted us with them; logging
/// preserves the breadcrumb.
struct BrokerMarkerApplier {
    broker: Arc<Broker>,
}

#[async_trait::async_trait]
impl MarkerApplier for BrokerMarkerApplier {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
        let batch = Bytes::from(build_control_batch(
            entry.producer_id,
            entry.producer_epoch,
            entry.commit,
            entry.coordinator_epoch,
        ));
        let coord = self.broker.coordinator();
        for topic in &entry.partitions {
            for &p in &topic.partitions {
                let owns = coord.as_ref().is_none_or(|c| c.owns(&topic.topic, p));
                if !owns {
                    tracing::warn!(
                        topic = %topic.topic,
                        partition = p,
                        pid = entry.producer_id,
                        "MarkerWatcher: queue entry targeted us but we don't lead this \
                         partition; assignment view may have drifted — marker dropped"
                    );
                    continue;
                }
                let epoch = coord
                    .as_ref()
                    .and_then(|c| c.current_epoch(&topic.topic, p))
                    .unwrap_or_else(|| self.broker.local_lease.current_epoch());
                let _ = self.broker.engine.create_partition(&topic.topic, p).await;
                if let Err(err) = self
                    .broker
                    .engine
                    .append(&topic.topic, p, epoch, -1, batch.clone())
                    .await
                {
                    tracing::warn!(
                        topic = %topic.topic,
                        partition = p,
                        %err,
                        "MarkerWatcher: control-batch append failed; keeping \
                         queue file for retry next tick"
                    );
                    return ApplyOutcome::Retry;
                }
            }
        }
        ApplyOutcome::Applied
    }
}

/// Timeout reaper + marker reconcile, on the same tick.
///
/// The reaper moves overdue `Ongoing` transactions to `PrepareAbort`
/// (gh #28), and the reconcile then drives every `Prepare*` transaction
/// to `Complete*` by placing its COMMIT / ABORT markers (gh #225).
///
/// They share a tick because they are two halves of one obligation.
/// The reaper only ever *creates* marker debt — a timed-out
/// transaction has no producer left to retry its `EndTxn`, so without
/// the reconcile its partitions would keep a pinned LSO forever. The
/// reconcile also covers the transactions the `EndTxn` handler left
/// prepared because dispatch failed and the producer never came back.
async fn run_txn_reaper(
    store: Arc<TxnStateStore>,
    broker: Arc<kaas_broker::broker::Broker>,
    cancel: CancellationToken,
) {
    let mut tick = tokio::time::interval(TXN_REAPER_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = cancel.cancelled() => return,
            _ = tick.tick() => {
                // `now_ms` is wall-clock millis. UNIX_EPOCH
                // conversion can't fail in practice (it would mean
                // the system clock is pre-1970).
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
                    .unwrap_or(0);
                // Ownership gate (gh #91). A txn slot file has exactly
                // one legal writer — its coordinator — so an ungated
                // sweep had every broker read-modify-writing the same
                // slot on the shared volume (NFS substrate rule 3).
                //
                // Degrades in the safe direction at both edges:
                // `Broker::owns_txn` answers `true` when no Coordinator
                // is installed (dev / single-broker), so nothing stops
                // being swept; and in cluster mode it answers `false`
                // for everything until the first `assignment.json`
                // load, which delays a sweep by one 1 s poll rather
                // than skipping it.
                let owns = |id: &str| broker.owns_txn(id);
                let aborted = store.abort_overdue_owned(now_ms, Some(&owns));
                if !aborted.is_empty() {
                    info!(
                        count = aborted.len(),
                        "txn-timeout reaper prepared overdue Ongoing transactions for abort"
                    );
                }
                let completed = kaas_broker::txn_markers::reconcile_pending_markers(
                    &broker, &store, Some(&owns),
                )
                .await;
                if completed > 0 {
                    info!(
                        count = completed,
                        "txn marker reconcile completed prepared transactions"
                    );
                }
            }
        }
    }
}

/// Spawn an [`AssignmentLoop`] that runs in single-broker mode. The
/// loop ticks every 5 s and writes a fresh `assignment.json`
/// listing every known topic with this broker as leader. The
/// Coordinator's 1 s poll picks the file up and stamps ownership
/// on the broker.
fn spawn_single_broker_assignment_loop(
    data_dir: std::path::PathBuf,
    cluster_dir: std::path::PathBuf,
    self_id: String,
    topics: Arc<TopicRegistry>,
    cancel: CancellationToken,
) -> Result<Option<tokio::task::JoinHandle<()>>> {
    // Live topic source: reads TopicRegistry on every AssignmentLoop
    // cycle so newly-created topics (via CreateTopics + the KafkaTopic
    // CR watcher) get partitions distributed on the next 5s tick.
    // A snapshot at boot would leave every fresh topic unassigned.
    let live_topics = Arc::new(LiveTopicSource {
        registry: topics.clone(),
    });
    let broker_source = Arc::new(StaticSources {
        topics: Vec::new(),
        brokers: vec![self_id.clone()],
        groups: Vec::new(),
    });
    let loop_handle = AssignmentLoop::new(
        data_dir.clone(),
        self_id.clone(),
        live_topics,
        broker_source.clone(),
    )
    .with_cluster_dir(cluster_dir)
    .with_group_source(broker_source.clone());
    let election = LocalElection::new(self_id.clone());

    let task = tokio::spawn(async move {
        // Local election always wins immediately; the awaited
        // future returns the post-acquire epoch.
        use kaas_controller::LeaseElection;
        let epoch = election.acquire().await;
        if let Err(err) = loop_handle.start(epoch).await {
            warn!(%err, "single-broker assignment loop: initial start failed");
            return;
        }
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(5));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => return,
                _ = tick.tick() => {
                    if let Err(err) = loop_handle
                        .update_assignment(AssignmentReason::InitialRecompute)
                        .await
                    {
                        warn!(%err, "single-broker assignment loop: update failed");
                    }
                }
            }
        }
    });
    Ok(Some(task))
}

fn topic_specs_from_registry(topics: &TopicRegistry) -> Vec<kaas_controller::TopicSpec> {
    topics
        .all()
        .into_iter()
        .map(|m| kaas_controller::TopicSpec {
            name: m.name,
            partition_count: m.partition_count,
        })
        .collect()
}

/// `true` when this process runs as a StatefulSet pod (the chart
/// always sets `MY_POD_NAME` via the downward API).
fn in_cluster_pod() -> bool {
    std::env::var("MY_POD_NAME").is_ok_and(|v| !v.is_empty())
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// A positive whole number of seconds from `key`, else `default` — with
/// a warning when the variable is set but malformed or zero (gh #281).
fn env_secs(key: &str, default: Duration) -> Duration {
    let Ok(raw) = std::env::var(key) else {
        return default;
    };
    match raw.trim().parse::<u64>() {
        Ok(v) if v > 0 => Duration::from_secs(v),
        _ => {
            warn!(
                key,
                value = %raw,
                default_secs = default.as_secs(),
                "not a positive number of seconds; using the default"
            );
            default
        }
    }
}

/// The controller-Lease timings from the chart's env, refusing an
/// inconsistent set the way client-go does: the lease must outlast the
/// renew deadline, and the renew deadline the retry period, or the
/// holder loses the Lease while still renewing it. A refused set falls
/// back to the defaults as a whole (gh #281).
fn lease_timings_from_env() -> (Duration, Duration, Duration) {
    use kaas_controller::kube_election::{
        DEFAULT_LEASE_DURATION, DEFAULT_RENEW_DEADLINE, DEFAULT_RETRY_PERIOD,
    };
    let lease = env_secs(
        "KAAS_CONTROLLER_LEASE_DURATION_SECONDS",
        DEFAULT_LEASE_DURATION,
    );
    let renew = env_secs(
        "KAAS_CONTROLLER_RENEW_DEADLINE_SECONDS",
        DEFAULT_RENEW_DEADLINE,
    );
    let retry = env_secs("KAAS_CONTROLLER_RETRY_PERIOD_SECONDS", DEFAULT_RETRY_PERIOD);
    if renew < lease && retry < renew {
        return (lease, renew, retry);
    }
    warn!(
        lease_secs = lease.as_secs(),
        renew_secs = renew.as_secs(),
        retry_secs = retry.as_secs(),
        "controller-Lease timings need lease > renew deadline > retry period; using the defaults"
    );
    (
        DEFAULT_LEASE_DURATION,
        DEFAULT_RENEW_DEADLINE,
        DEFAULT_RETRY_PERIOD,
    )
}

/// Everything the multi-broker runtime needs that only exists in a
/// real cluster: pod identity, peer registry, Lease epoch source,
/// and the heartbeat client. Built once at `install` time; consumed
/// by [`spawn_cluster_tasks`].
struct ClusterWiring {
    client: kube::Client,
    identity: BrokerIdentity,
    registry: Arc<BrokerRegistry>,
    lease_epoch: Arc<KubeLeaseEpoch>,
    heart: Arc<HeartbeatClient>,
    heartbeat_bind: String,
    lease_timings: (Duration, Duration, Duration),
}

fn prepare_cluster_wiring(
    client: kube::Client,
    self_id: &str,
    client_port: i32,
) -> Result<ClusterWiring> {
    let identity = BrokerIdentity::from_env("", "", client_port)
        .map_err(|e| anyhow::anyhow!("cluster mode: broker identity: {e}"))?;
    let self_ep = kaas_k8s::BrokerEndpoint {
        node_id: identity.ordinal,
        host: identity.host.clone(),
        port: client_port,
        ready: true,
    };
    let registry = Arc::new(BrokerRegistry::new(self_ep, identity.dns.clone()));
    let lease_epoch = Arc::new(KubeLeaseEpoch::new());

    // Heartbeat client follows the Lease holder: resolver re-runs at
    // the start of every reconnect cycle, so controller failover =
    // one reconnect.
    let hb_port = env_or("KAAS_PEER_HEARTBEAT_PORT", "9094")
        .parse::<i32>()
        .unwrap_or(9094);
    let resolver: TargetResolver = Arc::new({
        let lease_epoch = lease_epoch.clone();
        let registry = registry.clone();
        let dns = identity.dns.clone();
        move || {
            let holder = lease_epoch.current_holder().filter(|h| !h.is_empty())?;
            let ord = kaas_k8s::parse_ordinal(&holder)?;
            let host = registry
                .all()
                .into_iter()
                .find(|b| b.node_id == ord)
                .map_or_else(|| dns.fqdn(ord), |b| b.host);
            Some(format!("{host}:{hb_port}"))
        }
    });
    let heart = HeartbeatClient::new(self_id).with_target_fn(resolver);

    Ok(ClusterWiring {
        client,
        identity,
        registry,
        lease_epoch,
        heart,
        heartbeat_bind: env_or("KAAS_CONTROLLER_HEARTBEAT_ADDR", "0.0.0.0:9094"),
        lease_timings: lease_timings_from_env(),
    })
}

/// FindCoordinator's peer resolution: broker-id string → live
/// endpoint from the EndpointSlice registry. Replaces the self-only
/// lookup so a client asking any broker for coordinator-of-G gets
/// routed to the actual owner.
fn registry_lookup(registry: Arc<BrokerRegistry>) -> Arc<dyn BrokerLookup> {
    Arc::new(FnLookup::new(move |broker_id: &str| {
        let ord = kaas_k8s::parse_ordinal(broker_id)?;
        registry
            .all()
            .into_iter()
            .find(|b| b.node_id == ord)
            .map(|b| BrokerEndpoint {
                node_id: b.node_id,
                host: b.host,
                port: b.port,
            })
    }))
}

/// Parking slot for the active controller's recompute trigger.
/// While this broker holds the Lease, the controller loop installs
/// an mpsc sender here; topic-watcher callbacks fire
/// [`TopicChangeNotifier::notify`], which is a no-op on
/// non-controller brokers (gh #74).
#[derive(Default)]
pub struct TopicChangeNotifier {
    tx: std::sync::Mutex<Option<tokio::sync::mpsc::Sender<AssignmentReason>>>,
}

impl TopicChangeNotifier {
    // dead_code: called from main.rs, but integration tests include
    // this module via #[path] and not all of them exercise notify.
    #[allow(dead_code)]
    pub fn notify(&self, reason: AssignmentReason) {
        let guard = self
            .tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(tx) = guard.as_ref() {
            // try_send: full channel means a recompute is already
            // queued — coalescing is fine, the loop re-reads live
            // sources on every pass.
            let _ = tx.try_send(reason);
        }
    }

    fn set(&self, tx: Option<tokio::sync::mpsc::Sender<AssignmentReason>>) {
        *self
            .tx
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = tx;
    }
}

/// Metadata's live broker catalog, backed by the EndpointSlice
/// registry (host = StatefulSet FQDN, stable across restarts).
struct RegistryBrokerView {
    registry: Arc<BrokerRegistry>,
}

impl kaas_broker::ClusterBrokerView for RegistryBrokerView {
    /// gh #249: every REGISTERED broker, fenced ones included and
    /// flagged. Callers decide: Metadata drops fenced rows (Apache
    /// omits fenced brokers too), DescribeCluster v2 reports them.
    fn brokers(&self) -> Vec<kaas_broker::BrokerNode> {
        self.registry
            .all()
            .into_iter()
            .map(|b| kaas_broker::BrokerNode {
                node_id: b.node_id,
                host: b.host,
                port: b.port,
                fenced: !b.ready,
            })
            .collect()
    }
}

/// Controller's view of the alive broker set: EndpointSlice-ready ∩
/// heartbeat-connected, falling back to registry-only while no
/// heartbeat has arrived yet (fresh controller, brokers still
/// dialing). See gh #77.
struct ClusterBrokerSource {
    registry: Arc<BrokerRegistry>,
    heart: Arc<HeartbeatServer>,
    /// The elected controller itself — always alive by definition
    /// (it holds the Lease). Guarantees the source can never yield
    /// an empty (or self-less) set, so no assignment ever unassigns
    /// the whole cluster because of a probe blip or slice hiccup.
    self_id: String,
}

impl kaas_controller::BrokerSource for ClusterBrokerSource {
    fn alive_brokers(&self) -> Vec<String> {
        // gh #208: liveness comes from the heartbeat's `healthy` bit
        // (main-runtime scheduling tasks), NOT pod EndpointSlice
        // readiness. Readiness now gates on takeover completion
        // (honest `/readyz`), so a booting broker is deliberately
        // NotReady while it recovers — using readiness here would keep
        // it out of the alive set, never assign it partitions, and
        // deadlock the rollout. Heartbeat `healthy` is true throughout
        // boot (the main runtime is running, just taking over), so a
        // booting broker is assignable; a wedged broker (main runtime
        // dead, heartbeat still alive on the control runtime) reports
        // healthy=false and drops out.
        let live = self.heart.broker_liveness();
        let registered_ready = || {
            self.registry
                .all()
                .into_iter()
                .filter(|b| b.ready)
                .map(|b| format!("kaas-{}", b.node_id))
                .collect::<Vec<_>>()
        };
        decide_alive(live, registered_ready, &self.self_id)
    }

    /// gh #249: the registered set is the EndpointSlice registry —
    /// every broker whose pod exists, ready or not. Readiness is what
    /// separates alive from fenced; presence in the slice is what
    /// separates fenced from gone.
    fn registered_brokers(&self) -> Vec<String> {
        let mut ids: Vec<String> = self
            .registry
            .all()
            .into_iter()
            .map(|b| format!("kaas-{}", b.node_id))
            .collect();
        if !ids.contains(&self.self_id) {
            ids.push(self.self_id.clone());
        }
        ids
    }

    /// Brokers that told us over the heartbeat that they are shutting
    /// down. They are already out of `alive_brokers` (see
    /// `decide_alive`), so this only decides how they are *reported*.
    fn draining_brokers(&self) -> Vec<String> {
        self.heart
            .broker_liveness()
            .into_iter()
            .filter(|b| b.draining)
            .map(|b| b.id)
            .collect()
    }
}

/// Pure alive-set policy (gh #208), split out for testing.
///
/// - No heartbeats yet → bootstrap fallback to EndpointSlice-ready
///   (`registered_ready`), same as before this change.
/// - Otherwise a connected broker is alive iff its latest heartbeat
///   reports `healthy=true`. The field is seeded from the stream's
///   first status message and trusted unconditionally — the old-image
///   `ever_healthy` guard was dropped under the pre-v1 no-backcompat
///   policy (docs/RELEASING.md); mixed-version rolling upgrades from
///   pre-`healthy` images are unsupported (fresh deploy instead).
/// - `self_id` is always included: the controller holds the Lease, so
///   it is alive by definition, and the set is never empty.
fn decide_alive(
    live: Vec<kaas_controller::BrokerLiveness>,
    registered_ready: impl FnOnce() -> Vec<String>,
    self_id: &str,
) -> Vec<String> {
    let mut alive: Vec<String> = if live.is_empty() {
        registered_ready()
    } else {
        live.into_iter()
            // gh #249: a draining broker is healthy — it is serving
            // right up until it exits — but it has announced that it
            // is leaving, so stop assigning to it now instead of
            // waiting out a heartbeat timeout after it's gone.
            .filter(|b| b.healthy && !b.draining)
            .map(|b| b.id)
            .collect()
    };
    if !alive.iter().any(|id| id == self_id) {
        alive.push(self_id.to_owned());
    }
    alive
}

/// Spawn the always-on cluster tasks (lease watch, endpoint watch,
/// heartbeat client + status pump) and the election campaign that
/// runs the controller stack while this broker holds the Lease.
///
/// Every control-plane loop runs on a **dedicated OS thread with
/// its own single-threaded tokio runtime and its own kube client**.
/// Takeover storms run seconds-long blocking NFS I/O on the main
/// runtime's workers, and a starved timer is how a healthy
/// controller's lease renew froze for 28 s and got stolen (observed
/// live). The kube
/// client must be built on that runtime too — its internal tower
/// Buffer worker is spawned onto whichever runtime creates it.
/// Only the controller stack (`run_controller`) is spawned back
/// onto the main runtime, next to the storage it drives.
#[allow(clippy::too_many_arguments)]
fn spawn_cluster_tasks(
    w: ClusterWiring,
    dir: std::path::PathBuf,
    cluster_dir: std::path::PathBuf,
    self_id: String,
    topics: Arc<TopicRegistry>,
    manager: Arc<Manager>,
    coordinator: Arc<Coordinator>,
    is_controller: Arc<AtomicBool>,
    topic_notify: Arc<TopicChangeNotifier>,
    cancel: CancellationToken,
) {
    let ns = w.identity.namespace.clone();
    let main_rt = tokio::runtime::Handle::current();

    // --- dedicated control-plane thread ---
    {
        let ns = ns.clone();
        let svc = w.identity.dns.headless_service.clone();
        let lease_epoch = w.lease_epoch.clone();
        let registry = w.registry.clone();
        let heart = w.heart.clone();
        let heartbeat_bind = w.heartbeat_bind.clone();
        let lease_timings = w.lease_timings;
        let coordinator_pump = coordinator.clone();
        let cancel = cancel.clone();
        let spawn_res = std::thread::Builder::new()
            .name("kaas-control".to_owned())
            .spawn(move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(err) => {
                        warn!(%err, "control-plane runtime build failed; multi-broker control plane DOWN");
                        return;
                    }
                };
                rt.block_on(control_plane(
                    ns,
                    svc,
                    self_id,
                    lease_epoch,
                    registry,
                    heart,
                    heartbeat_bind,
                    lease_timings,
                    dir,
                    cluster_dir,
                    topics,
                    manager,
                    coordinator_pump,
                    is_controller,
                    topic_notify,
                    cancel,
                    main_rt,
                ));
            });
        if let Err(err) = spawn_res {
            warn!(%err, "control-plane thread spawn failed; multi-broker control plane DOWN");
        }
    }

    // Readiness gate (kaas.rs/PartitionsReady): flip once the
    // first assignment.json applies, so the pod only joins the
    // Service after it knows its partition ownership. Retries with
    // backoff — needs `patch pods/status` RBAC.
    {
        let flipped = Arc::new(AtomicBool::new(false));
        let client = w.client.clone();
        let gate_ns = ns.clone();
        let pod = w.identity.pod_name.clone();
        coordinator.on_assignment_change(Arc::new(move |_prev, _next| {
            if flipped.swap(true, Ordering::Relaxed) {
                return;
            }
            let client = client.clone();
            let gate_ns = gate_ns.clone();
            let pod = pod.clone();
            tokio::spawn(async move {
                let mut delay = Duration::from_secs(1);
                loop {
                    match kaas_k8s::kube_watchers::patch_readiness(
                        client.clone(),
                        gate_ns.clone(),
                        pod.clone(),
                        true,
                    )
                    .await
                    {
                        Ok(()) => {
                            info!("readiness gate: PartitionsReady=True");
                            return;
                        }
                        Err(err) => {
                            warn!(%err, "readiness gate patch failed; retrying");
                            tokio::time::sleep(delay).await;
                            delay = (delay * 2).min(Duration::from_secs(30));
                        }
                    }
                }
            });
        }));
    }
}

/// Body of the control-plane thread: builds its own kube client,
/// then runs the lease watch, endpoint watch, heartbeat stream,
/// status pump, and the election campaign until `cancel` fires.
/// The campaign is awaited last so its release-on-shutdown runs
/// before the runtime is torn down.
#[allow(clippy::too_many_arguments)]
async fn control_plane(
    ns: String,
    headless_svc: String,
    self_id: String,
    lease_epoch: Arc<KubeLeaseEpoch>,
    registry: Arc<BrokerRegistry>,
    heart: Arc<HeartbeatClient>,
    heartbeat_bind: String,
    lease_timings: (Duration, Duration, Duration),
    dir: std::path::PathBuf,
    cluster_dir: std::path::PathBuf,
    topics: Arc<TopicRegistry>,
    manager: Arc<Manager>,
    coordinator: Arc<Coordinator>,
    is_controller: Arc<AtomicBool>,
    topic_notify: Arc<TopicChangeNotifier>,
    cancel: CancellationToken,
    main_rt: tokio::runtime::Handle,
) {
    // Own client: the main runtime's client buffers requests through
    // a worker task pinned to the main runtime — exactly the
    // starvation this thread exists to escape.
    let client = loop {
        match kube::Client::try_default().await {
            Ok(c) => break c,
            Err(err) => {
                warn!(%err, "control plane: kube client init failed; retrying in 5s");
                tokio::select! {
                    () = cancel.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(5)) => {}
                }
            }
        }
    };
    info!("control plane up on dedicated runtime");

    // 1 s Lease poll: feeds the Coordinator's stale-epoch fence AND
    // the heartbeat client's controller discovery.
    tokio::spawn(run_lease_watch(
        client.clone(),
        ns.clone(),
        CONTROLLER_LEASE_NAME.to_owned(),
        lease_epoch,
        cancel.clone(),
    ));

    // EndpointSlice watch → BrokerRegistry. The watch returns when
    // its stream ends; restart with a small delay.
    {
        let client = client.clone();
        let watch_ns = ns.clone();
        let registry = registry.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            loop {
                run_endpoint_watch(
                    client.clone(),
                    watch_ns.clone(),
                    headless_svc.clone(),
                    registry.clone(),
                    c.clone(),
                )
                .await;
                if c.is_cancelled() {
                    return;
                }
                warn!("endpoint watch stream ended; restarting in 2s");
                tokio::select! {
                    () = c.cancelled() => return,
                    () = tokio::time::sleep(Duration::from_secs(2)) => {}
                }
            }
        });
    }

    // Long-lived heartbeat stream to whoever holds the Lease.
    tokio::spawn(heart.clone().run(cancel.clone()));

    // 1 s status pump: liveness + last-seen assignment version +
    // active consumer groups (feeds the controller's GroupSource).
    {
        let heart = heart.clone();
        let c = cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = c.cancelled() => return,
                    _ = tick.tick() => {
                        let version = coordinator
                            .snapshot()
                            .map_or(0, |a| u64::try_from(a.assignment_version).unwrap_or(0));
                        // Disconnected during controller failover is
                        // normal — the run loop is already redialing.
                        let _ = heart.send(BrokerStatus {
                            broker_id: String::new(), // filled by the client
                            timestamp_ms: unix_ms(),
                            last_seen_assignment_version: version,
                            partitions: Vec::new(),
                            active_groups: manager.local_groups(),
                            // gh #208: main-runtime liveness. Read from
                            // this control-plane runtime; goes false when
                            // the main runtime wedges, so the controller
                            // reassigns our partitions.
                            healthy: kaas_observability::main_alive(),
                            draining: kaas_observability::is_draining(),
                        });
                    }
                }
            }
        });
    }

    // Election: while we hold the kaas-controller Lease, run the
    // controller stack; on loss the leader token tears it down and
    // we re-enter candidacy. Awaited (not spawned) so the campaign's
    // release-on-shutdown completes before this runtime drops.
    let election = KubeLeaseElection::new(client, ns, CONTROLLER_LEASE_NAME, self_id.clone())
        .with_timings(lease_timings.0, lease_timings.1, lease_timings.2);
    let gauge_flag = is_controller.clone();
    election
        .campaign(
            cancel,
            move |epoch, leader_token| {
                is_controller.store(true, Ordering::Relaxed);
                // The controller stack does storage I/O — it belongs
                // on the main runtime, not the control plane. It
                // gets the control-plane handle back so the
                // heartbeat SERVER (liveness signal) stays isolated.
                main_rt.spawn(run_controller(
                    epoch,
                    leader_token,
                    dir.clone(),
                    cluster_dir.clone(),
                    self_id.clone(),
                    topics.clone(),
                    registry.clone(),
                    heartbeat_bind.clone(),
                    topic_notify.clone(),
                    tokio::runtime::Handle::current(),
                ));
            },
            // Synchronous, ordered before any re-acquire — a spawned
            // watcher here raced the next stint's store(true) and
            // left the gauge stuck at 0.
            move || gauge_flag.store(false, Ordering::Relaxed),
        )
        .await;
    info!("control plane shut down");
}

/// The controller stack: heartbeat gRPC server + AssignmentLoop +
/// broker-set watcher + topic-change trigger. Runs until
/// `leader_token` fires (Lease lost or shutdown).
/// `pub(crate)` so the cluster_smoke integration test (which
/// includes this module via `#[path]`) can drive it without kube.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_controller(
    epoch: i64,
    leader_token: CancellationToken,
    dir: std::path::PathBuf,
    cluster_dir: std::path::PathBuf,
    self_id: String,
    topics: Arc<TopicRegistry>,
    registry: Arc<BrokerRegistry>,
    heartbeat_bind: String,
    topic_notify: Arc<TopicChangeNotifier>,
    ctl_rt: tokio::runtime::Handle,
) {
    let heart_srv = HeartbeatServer::new();

    // Heartbeat gRPC listener (fixed :9094 in the chart). Spawned on
    // the CONTROL-PLANE runtime: its 1 s PINGs are the liveness
    // signal every peer's read-watchdog gates on, so it must not
    // share fate with takeover/storage I/O on the main runtime — a
    // starved server made every client tear down its stream, flap
    // the broker set, and trigger the next rebalance storm. A bind
    // failure is survivable (the broker source falls back to the
    // EndpointSlice registry) but noisy on purpose.
    match heartbeat_bind.parse::<std::net::SocketAddr>() {
        Ok(addr) => {
            let svc = ControllerHeartbeatServer::new(HeartbeatService::new(heart_srv.clone()));
            let t = leader_token.clone();
            ctl_rt.spawn(async move {
                if let Err(err) = tonic::transport::Server::builder()
                    .add_service(svc)
                    .serve_with_shutdown(addr, t.cancelled())
                    .await
                {
                    warn!(%err, "controller heartbeat gRPC server exited");
                }
            });
        }
        Err(err) => {
            warn!(%err, addr = %heartbeat_bind,
                  "bad KAAS_CONTROLLER_HEARTBEAT_ADDR; heartbeat server disabled");
        }
    }

    let live_topics = Arc::new(LiveTopicSource { registry: topics });
    let broker_src = Arc::new(ClusterBrokerSource {
        registry: registry.clone(),
        heart: heart_srv.clone(),
        self_id: self_id.clone(),
    });
    let loop_handle = AssignmentLoop::new(dir, self_id, live_topics, broker_src.clone())
        .with_cluster_dir(cluster_dir)
        .with_group_source(heart_srv.clone());

    // Heartbeat grace: a fresh controller's server starts empty, and
    // balancing over whichever single broker redialed first assigns
    // the whole cluster to it — a takeover storm on every failover.
    // Give peers one reconnect cycle (backoff caps at 5 s) to show
    // up before the first recompute; the 2 s broker-set watcher
    // still handles stragglers.
    {
        let want = registry.ready_count();
        let grace_end = tokio::time::Instant::now() + Duration::from_secs(7);
        while heart_srv.connected_brokers().len() < want {
            if leader_token.is_cancelled() || tokio::time::Instant::now() >= grace_end {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        info!(
            connected = heart_srv.connected_brokers().len(),
            registered_ready = want,
            "controller: heartbeat grace complete"
        );
    }

    let version = match loop_handle.start(epoch).await {
        Ok(v) => v,
        Err(err) => {
            warn!(%err, epoch, "controller: initial assignment write failed; abdicating");
            leader_token.cancel();
            return;
        }
    };
    heart_srv.push_assignment_changed(u64::try_from(version).unwrap_or(0));
    info!(epoch, version, "controller active: assignment published");

    // Topic-change trigger (gh #74): watcher callbacks land here
    // while we hold the Lease.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<AssignmentReason>(32);
    topic_notify.set(Some(tx));

    // Broker-set watcher (gh #77): 2 s poll, recompute on any
    // join/leave. Removals take precedence for the reason label.
    let mut prev: std::collections::BTreeSet<String> =
        broker_src.alive_brokers().into_iter().collect();
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        let reason = tokio::select! {
            () = leader_token.cancelled() => break,
            r = rx.recv() => match r {
                Some(r) => Some(r),
                None => break,
            },
            _ = tick.tick() => {
                let cur: std::collections::BTreeSet<String> =
                    broker_src.alive_brokers().into_iter().collect();
                if cur == prev {
                    None
                } else {
                    let removed = prev.difference(&cur).next().is_some();
                    info!(prev = ?prev, cur = ?cur, "controller: broker set changed");
                    prev = cur;
                    Some(if removed {
                        AssignmentReason::BrokerDead
                    } else {
                        AssignmentReason::BrokerJoined
                    })
                }
            }
        };
        let Some(reason) = reason else { continue };
        match loop_handle.update_assignment(reason).await {
            Ok(v) => heart_srv.push_assignment_changed(u64::try_from(v).unwrap_or(0)),
            Err(err) => {
                warn!(%err, reason = reason.as_str(), "controller: assignment update failed");
            }
        }
    }

    topic_notify.set(None);
    heart_srv.push_leaving();
    info!("controller stack shut down (lease lost or shutdown)");
}

fn unix_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// Adapts the live cluster runtime to [`kaas_observability::GaugeSource`]
/// so the Phase-10 observable gauges sample real values on every
/// export.
struct RuntimeGaugeSource {
    coordinator: Arc<Coordinator>,
    engine: Arc<dyn StorageEngine>,
    /// Live in cluster mode (flipped by the Lease-election campaign
    /// on acquire/loss); constant `true` in single-broker dev mode
    /// where `LocalElection` always wins.
    is_controller: Arc<AtomicBool>,
}

impl kaas_observability::GaugeSource for RuntimeGaugeSource {
    fn log_dirs(&self) -> Vec<kaas_observability::LogDirCapacityGauge> {
        self.engine
            .log_dirs()
            .into_iter()
            .map(|d| {
                let (total_bytes, usable_bytes) = kaas_storage::fs_capacity(&d.path);
                kaas_observability::LogDirCapacityGauge {
                    name: d.name,
                    total_bytes,
                    usable_bytes,
                }
            })
            .collect()
    }

    fn is_controller(&self) -> i64 {
        i64::from(self.is_controller.load(Ordering::Relaxed))
    }

    fn assignment_version(&self) -> i64 {
        self.coordinator
            .snapshot()
            .map_or(0, |a| a.assignment_version)
    }

    fn broker_count_alive(&self) -> i64 {
        self.coordinator.snapshot().map_or(0, |a| {
            let alive = a
                .brokers
                .iter()
                .filter(|b| matches!(b.health, kaas_broker::BrokerHealth::Alive))
                .count();
            i64::try_from(alive).unwrap_or(i64::MAX)
        })
    }

    fn broker_count_assigned(&self) -> i64 {
        self.coordinator.snapshot().map_or(0, |a| {
            let seen: std::collections::HashSet<&str> =
                a.partitions.iter().map(|p| p.broker.as_str()).collect();
            i64::try_from(seen.len()).unwrap_or(i64::MAX)
        })
    }

    fn assignment_file_size_bytes(&self) -> i64 {
        let path = self.coordinator.assignment_path();
        std::fs::metadata(path).map_or(0, |m| i64::try_from(m.len()).unwrap_or(i64::MAX))
    }

    fn partitions(&self) -> Vec<kaas_observability::PartitionGauge> {
        let Some(snap) = self.coordinator.snapshot() else {
            return Vec::new();
        };
        let self_id = self.coordinator.self_id();
        snap.partitions
            .iter()
            .map(|p| {
                // HWM is only meaningful on the leader; a non-leader
                // read would fail or return a stale value.
                let high_watermark = if p.broker == self_id {
                    self.engine
                        .high_watermark(&p.topic, p.partition)
                        .unwrap_or(0)
                } else {
                    0
                };
                kaas_observability::PartitionGauge {
                    topic: p.topic.clone(),
                    partition: p.partition,
                    // -1 on a malformed id so the gauge flags the bug
                    // instead of silently mapping to broker 0.
                    leader_id: kaas_k8s::parse_ordinal(&p.broker).map_or(-1, i64::from),
                    epoch: i64::from(p.epoch),
                    high_watermark,
                }
            })
            .collect()
    }
}

/// Wire the `TopicRegistry` behind `TopicSource` so the assignment
/// loop always sees the current-catalog snapshot, not a boot-time
/// one. Fresh `KafkaTopic` CRs propagate: `run_topic_watch`'s apply
/// callback → `TopicRegistry::insert` → `AssignmentLoop` picks it up
/// on the next tick.
struct LiveTopicSource {
    registry: Arc<TopicRegistry>,
}

impl kaas_controller::TopicSource for LiveTopicSource {
    fn topics(&self) -> Vec<kaas_controller::TopicSpec> {
        topic_specs_from_registry(&self.registry)
    }
}

fn self_endpoint_lookup(self_id: &str, port: i32) -> Arc<dyn BrokerLookup> {
    let id = self_id.to_owned();
    // Prefer the StatefulSet FQDN — same shape the Metadata handler's
    // `advertised_host` derives via `derive_advertised_host` in
    // kaas-broker/cli.rs. Client tools (FindCoordinator response) chase
    // this host to reach the group coordinator; without it they hit
    // `<pod>.local` and NXDOMAIN. Local-dev has none of these env
    // vars set — fall through to the `.local` shape so a
    // MemoryStorage-backed dev binary still resolves.
    let advertised = {
        let pod = std::env::var("MY_POD_NAME").ok().filter(|s| !s.is_empty());
        let svc = std::env::var("KAAS_HEADLESS_SVC")
            .ok()
            .filter(|s| !s.is_empty());
        let ns = std::env::var("KAAS_NAMESPACE")
            .ok()
            .filter(|s| !s.is_empty());
        match (pod, svc, ns) {
            (Some(p), Some(s), Some(n)) => format!("{p}.{s}.{n}.svc.cluster.local"),
            _ => format!("{id}.local"),
        }
    };
    Arc::new(FnLookup::new(move |req: &str| {
        if req == id {
            Some(BrokerEndpoint {
                node_id: 0,
                host: advertised.clone(),
                port,
            })
        } else {
            None
        }
    }))
}

#[cfg(test)]
mod alive_tests {
    use super::decide_alive;
    use kaas_controller::BrokerLiveness;

    fn bl(id: &str, healthy: bool) -> BrokerLiveness {
        BrokerLiveness {
            id: id.to_owned(),
            healthy,
            draining: false,
        }
    }

    fn bl_draining(id: &str) -> BrokerLiveness {
        BrokerLiveness {
            id: id.to_owned(),
            healthy: true,
            draining: true,
        }
    }

    fn sorted(mut v: Vec<String>) -> Vec<String> {
        v.sort();
        v
    }

    #[test]
    fn bootstrap_falls_back_to_endpointslice_when_no_heartbeats() {
        let alive = decide_alive(
            vec![],
            || vec!["kaas-0".to_owned(), "kaas-1".to_owned()],
            "kaas-0",
        );
        assert_eq!(sorted(alive), vec!["kaas-0", "kaas-1"]);
    }

    #[test]
    fn booting_broker_is_alive_even_when_not_serving() {
        // Healthy (main runtime ticking) but hasn't finished takeover;
        // it is NOT EndpointSlice-ready, yet must be assignable.
        let alive = decide_alive(
            vec![bl("kaas-0", true), bl("kaas-1", true)],
            || vec!["kaas-0".to_owned()], // only kaas-0 is Ready
            "kaas-0",
        );
        assert_eq!(sorted(alive), vec!["kaas-0", "kaas-1"]);
    }

    /// gh #249: a draining broker is healthy — it serves right up
    /// until it exits — but it has announced that it is leaving, so
    /// it must stop receiving assignments immediately rather than
    /// after a heartbeat timeout has expired on a process that is
    /// already gone.
    #[test]
    fn draining_broker_leaves_the_alive_set_while_still_healthy() {
        let alive = decide_alive(
            vec![
                bl("kaas-0", true),
                bl_draining("kaas-1"),
                bl("kaas-2", true),
            ],
            Vec::new,
            "kaas-0",
        );
        assert_eq!(sorted(alive), vec!["kaas-0", "kaas-2"]);
    }

    /// The self-pin outranks draining: the controller holds the
    /// Lease, and an empty alive set would unassign the cluster.
    #[test]
    fn draining_self_is_still_kept_in_the_alive_set() {
        let alive = decide_alive(vec![bl_draining("kaas-0")], Vec::new, "kaas-0");
        assert_eq!(alive, vec!["kaas-0"]);
    }

    #[test]
    fn wedged_broker_is_evicted() {
        // `healthy` is trusted unconditionally (the old-image
        // `ever_healthy` guard was dropped — pre-v1 no-backcompat
        // policy): reporting false = evicted.
        let alive = decide_alive(
            vec![bl("kaas-0", true), bl("kaas-1", false)],
            Vec::new,
            "kaas-0",
        );
        assert_eq!(sorted(alive), vec!["kaas-0"]);
    }

    #[test]
    fn self_is_always_present() {
        // Even if the controller's own entry is missing/wedged, it
        // holds the Lease and must appear.
        let alive = decide_alive(vec![bl("kaas-1", true)], Vec::new, "kaas-0");
        assert_eq!(sorted(alive), vec!["kaas-0", "kaas-1"]);
    }
}
