//! Broker — the runtime context every handler reads from.
//!
//! Phase 3 shape: owns the storage engine, the in-memory topic
//! registry, a `LocalLeaseManager`, and a monotonic producer-id
//! counter. Phase 5 adds hot-swappable [`Coordinator`] +
//! [`Manager`] slots — `None` in dev mode (Phase-3/4 tests) so
//! handlers gracefully degrade to local-lease ownership / consumer-
//! group `NOT_COORDINATOR` retry path. `bins/kaas/main.rs` populates
//! them at boot via [`Broker::install_coord_manager`] /
//! [`Broker::install_coordinator`]. Phase 6 adds `TxnStateStore`.
//!
//! [`Coordinator`]: crate::coordinator::Coordinator
//! [`Manager`]: kaas_coordinator::Manager

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use kaas_auth::{AllowAllAuthorizer, Authorizer, NoQuotaChecker, QuotaChecker, QuotaEnforcer};
use kaas_coordinator::{FenceLog, Manager, MarkerQueue, TxnStateStore};
use kaas_storage::StorageEngine;
use parking_lot::RwLock;

use crate::coordinator::Coordinator;
use crate::local_lease::LocalLeaseManager;
use crate::topic_registry::TopicRegistry;

/// One row of the live cluster broker catalog served by Metadata and
/// DescribeCluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerNode {
    pub node_id: i32,
    /// Stable per-broker DNS name (StatefulSet FQDN in cluster mode).
    pub host: String,
    pub port: i32,
    /// gh #249: registered but not serving. Metadata omits these
    /// rows; DescribeCluster v2 reports them as `IsFenced`. Always
    /// false in dev/single-broker mode, where the only broker is
    /// self and self is never fenced.
    pub fenced: bool,
}

/// Live cluster broker catalog — backed by the EndpointSlice registry
/// in cluster mode. Defined here (not kaas-k8s) because the dependency
/// arrow points kaas-k8s → kaas-broker; the broker binary supplies the
/// registry-backed impl.
///
/// Returns **registered** brokers, fenced ones included and flagged
/// (gh #249). Filtering is the caller's call, because the two
/// consumers want opposite things: Metadata must not advertise a
/// broker that isn't serving, DescribeCluster v2 exists precisely to
/// report it.
pub trait ClusterBrokerView: Send + Sync {
    fn brokers(&self) -> Vec<BrokerNode>;
}

pub struct Broker {
    pub engine: Arc<dyn StorageEngine>,
    pub topics: Arc<TopicRegistry>,
    pub local_lease: LocalLeaseManager,
    pub cluster_id: String,
    pub broker_id: i32,
    /// Broker identity string (e.g. `"kaas-0"`). Distinct from
    /// `broker_id: i32` which is the wire-level node id; the
    /// coordinator + assignment.json use the StatefulSet pod-name
    /// shape.
    pub self_id: String,
    /// Cluster-wide authorization decision surface. `AllowAllAuthorizer`
    /// is the default when no `authorization` config is set
    /// (Strimzi-compat semantic).
    pub authorizer: Arc<dyn Authorizer>,
    /// Cluster-wide quota check. `NoQuotaChecker` is the default for
    /// anonymous-only brokers.
    pub quotas: Arc<dyn QuotaChecker>,
    /// Consumer-group + offsets coordinator. `None` until the
    /// production wiring in `bins/kaas/main.rs` installs one;
    /// handlers that read this fall back to `NOT_COORDINATOR` (16)
    /// so a client retries via FindCoordinator. Phase-3/4 tests
    /// leave it `None` and exercise only the produce/fetch surface.
    coord_manager: RwLock<Option<Arc<Manager>>>,
    /// Broker-side assignment watcher + ownership lookup + group/txn
    /// source. `None` in dev mode (`Broker::new`); production
    /// installs at boot. When `Some`, produce / fetch ownership goes
    /// through this; when `None`, the local-lease "always lead" path
    /// stays in effect — the gh #92 fallback contract.
    coordinator: RwLock<Option<Arc<Coordinator>>>,
    /// Persistent transactional-state store (Phase 6). `None` in
    /// dev mode and Phase-3/4 tests; handlers that read this fall
    /// back to fresh-PID-every-time for transactional requests and
    /// log a warning once. `bins/kaas/main.rs` installs the real
    /// store at boot.
    txn_state: RwLock<Option<Arc<TxnStateStore>>>,
    /// Outbound producer-epoch fence log (gh #108). `Some` whenever
    /// the cluster runtime is up; `InitProducerId` appends
    /// `(pid, epoch)` here on epoch bumps so peer brokers'
    /// `FenceWatcher` picks them up. `None` in pure-handler unit
    /// tests — the local engine fence still fires; only the cross-
    /// broker broadcast is skipped.
    fence_log: RwLock<Option<Arc<FenceLog>>>,
    /// Cross-broker COMMIT/ABORT marker dispatch queue (gh #175).
    /// `Some` whenever the cluster runtime is up; `EndTxn` writes
    /// one entry per peer broker that leads a participating
    /// partition. `None` in pure-handler tests — the same-broker
    /// marker fast path still fires; only the cross-broker leg is
    /// skipped.
    marker_queue: RwLock<Option<MarkerQueue>>,
    /// Phase 7 admin path: writes back to `KafkaTopic` CRs for
    /// CreatePartitions (key 37) and IncrementalAlterConfigs (key
    /// 44). `None` in dev mode and unit tests — the admin
    /// handlers map a missing writer to
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) so a client gets a
    /// clean wire response instead of a panic.
    cr_writer: RwLock<Option<Arc<dyn crate::topic_cr_writer::TopicCRWriter>>>,
    /// ACL admin path (gh #107 parity): writes back to
    /// `KafkaUser.spec.authorization.acls` for CreateAcls (30) /
    /// DeleteAcls (31) and reads for DescribeAcls (29). `None` in
    /// dev mode and unit tests — the handlers degrade to the
    /// pre-gh #107 stub behaviour (empty describe, no-op mutate).
    acl_cr_writer: RwLock<Option<Arc<dyn crate::acl_cr_writer::AclCRWriter>>>,
    /// SCRAM credential rotation path (gh #252, KIP-554): key 51
    /// patches `KafkaUser.spec.authentication.scram`. `None` in dev
    /// mode and unit tests — the handler answers per-user
    /// `CLUSTER_AUTHORIZATION_FAILED` (31), same degradation as the
    /// topic writer.
    user_cr_writer: RwLock<Option<Arc<dyn crate::user_cr_writer::UserCRWriter>>>,
    /// Concrete-typed [`QuotaEnforcer`] handle for the Phase 7
    /// admin handlers (DescribeClientQuotas / AlterClientQuotas)
    /// which need access to `set_user_quota` / `describe_user_quota`
    /// / `list_user_quotas` — methods the `QuotaChecker` trait
    /// doesn't expose. Taken from `quotas` itself
    /// ([`QuotaChecker::as_enforcer`]), so the admin APIs act on the
    /// instance that throttles; a separate install step had no caller
    /// for the life of the Rust broker (gh #260). `None` when the broker
    /// runs without a real quota enforcer (`auth.enabled=false`); the
    /// admin handlers then surface `UNSUPPORTED_VERSION` (35).
    quota_enforcer: Option<Arc<QuotaEnforcer>>,
    /// Live broker catalog for Metadata (cluster mode only). `None`
    /// keeps the single-broker "self is the whole cluster" shape.
    broker_view: RwLock<Option<Arc<dyn ClusterBrokerView>>>,
    /// gh #219: persisted, cluster-unique PID source. `None` in dev
    /// mode / unit tests (no shared volume), where `producer_id_counter`
    /// stands in — nothing on disk survives to collide with.
    producer_ids: RwLock<Option<Arc<crate::producer_id::ProducerIdAllocator>>>,
    producer_id_counter: AtomicI64,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("cluster_id", &self.cluster_id)
            .field("broker_id", &self.broker_id)
            .field("topics", &self.topics.len())
            .field(
                "producer_id_counter",
                &self.producer_id_counter.load(Ordering::Relaxed),
            )
            .finish()
    }
}

impl Broker {
    /// Build a broker with the default open-everything authorization
    /// surface (`AllowAllAuthorizer` + `NoQuotaChecker`). Tests and
    /// dev mode call this; `bins/kaas/main.rs` uses
    /// [`Broker::with_auth`] to swap in real implementations.
    pub fn new(
        engine: Arc<dyn StorageEngine>,
        topics: Arc<TopicRegistry>,
        cluster_id: impl Into<String>,
        broker_id: i32,
    ) -> Self {
        Self::with_auth(
            engine,
            topics,
            cluster_id,
            broker_id,
            Arc::new(AllowAllAuthorizer),
            Arc::new(NoQuotaChecker),
        )
    }

    pub fn with_auth(
        engine: Arc<dyn StorageEngine>,
        topics: Arc<TopicRegistry>,
        cluster_id: impl Into<String>,
        broker_id: i32,
        authorizer: Arc<dyn Authorizer>,
        quotas: Arc<dyn QuotaChecker>,
    ) -> Self {
        let broker_id_val = broker_id;
        let quota_enforcer = quotas.clone().as_enforcer();
        Self {
            engine,
            topics,
            local_lease: LocalLeaseManager,
            cluster_id: cluster_id.into(),
            broker_id,
            self_id: format!("kaas-{broker_id_val}"),
            authorizer,
            quotas,
            coord_manager: RwLock::new(None),
            coordinator: RwLock::new(None),
            txn_state: RwLock::new(None),
            fence_log: RwLock::new(None),
            marker_queue: RwLock::new(None),
            cr_writer: RwLock::new(None),
            acl_cr_writer: RwLock::new(None),
            user_cr_writer: RwLock::new(None),
            quota_enforcer,
            broker_view: RwLock::new(None),
            producer_ids: RwLock::new(None),
            // Start at 1 so 0 stays available as an "unset" sentinel
            // for clients that read uninitialised pid.
            producer_id_counter: AtomicI64::new(1),
        }
    }

    /// Install the live broker catalog (cluster mode). Metadata then
    /// advertises the whole broker set instead of self-only.
    pub fn install_broker_view(&self, v: Arc<dyn ClusterBrokerView>) {
        *self.broker_view.write() = Some(v);
    }

    /// Read the installed broker catalog. `None` in dev/single-broker
    /// mode — Metadata falls back to advertising self only.
    pub fn broker_view(&self) -> Option<Arc<dyn ClusterBrokerView>> {
        self.broker_view.read().clone()
    }

    /// Install the persisted PID allocator (gh #219). Called from
    /// `bins/kaas/main.rs` whenever a data dir is configured.
    pub fn install_producer_id_allocator(
        &self,
        alloc: Arc<crate::producer_id::ProducerIdAllocator>,
    ) {
        *self.producer_ids.write() = Some(alloc);
    }

    /// Hand out the next producer id.
    ///
    /// gh #219: with a data dir this draws from the persisted,
    /// broker-partitioned [`ProducerIdAllocator`] so a PID is never
    /// reissued — not across a restart, not across brokers. Reissuing
    /// one lands a fresh producer on a dead producer's dedupe window in
    /// `producer-state.snapshot`, which silently drops records
    /// (classified `Duplicate`) or rejects them with
    /// `OUT_OF_ORDER_SEQUENCE_NUMBER`.
    ///
    /// Without an allocator (dev mode, unit tests) it falls back to an
    /// in-memory counter: nothing persists there, so nothing collides.
    ///
    /// [`ProducerIdAllocator`]: crate::producer_id::ProducerIdAllocator
    pub fn next_producer_id(&self) -> i64 {
        if let Some(alloc) = self.producer_ids.read().clone() {
            return alloc.next();
        }
        self.producer_id_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Install the consumer-group + offsets coordinator. Called once
    /// from `bins/kaas/main.rs` at boot; tests can call it to wire
    /// a per-test [`Manager`].
    ///
    /// [`Manager`]: kaas_coordinator::Manager
    pub fn install_coord_manager(&self, mgr: Arc<Manager>) {
        *self.coord_manager.write() = Some(mgr);
    }

    /// Read the installed [`Manager`]. Returns `None` when no
    /// coordinator is wired (Phase-3/4 dev path); handlers map that
    /// to `NOT_COORDINATOR` (16) so the client retries via
    /// FindCoordinator.
    ///
    /// [`Manager`]: kaas_coordinator::Manager
    pub fn coord_manager(&self) -> Option<Arc<Manager>> {
        self.coord_manager.read().clone()
    }

    /// Install the broker-side assignment-watching [`Coordinator`].
    /// See [`Self::install_coord_manager`] for the equivalent group/
    /// offsets surface.
    pub fn install_coordinator(&self, c: Arc<Coordinator>) {
        *self.coordinator.write() = Some(c);
    }

    /// Read the installed [`Coordinator`]. Returns `None` when no
    /// coordinator is wired; produce / fetch / metadata handlers
    /// fall back to the `LocalLeaseManager` "always lead" path.
    pub fn coordinator(&self) -> Option<Arc<Coordinator>> {
        self.coordinator.read().clone()
    }

    /// Install the Phase 6 [`TxnStateStore`]. Called once from
    /// `bins/kaas/main.rs` at boot. Tests can call it directly to
    /// wire a per-test store.
    pub fn install_txn_state(&self, s: Arc<TxnStateStore>) {
        *self.txn_state.write() = Some(s);
    }

    /// Read the installed [`TxnStateStore`]. Returns `None` when no
    /// store is wired; transactional handlers fall back to either a
    /// `COORDINATOR_NOT_AVAILABLE` (15) response or — for
    /// `InitProducerId` — a fresh PID with `epoch = 0` plus a one-
    /// shot warning (graceful degradation).
    pub fn txn_state(&self) -> Option<Arc<TxnStateStore>> {
        self.txn_state.read().clone()
    }

    /// Does this broker own the txn coordinator slot for `txn_id`?
    /// Delegates to the installed [`Coordinator`] (gh #91 hash
    /// routing) when present; always returns `true` in dev mode so
    /// single-broker setups and unit tests still serve transactional
    /// requests.
    pub fn owns_txn(&self, txn_id: &str) -> bool {
        use kaas_coordinator::TxnAssignmentSource;
        match self.coordinator() {
            Some(c) => c.owns_txn(txn_id),
            None => true,
        }
    }

    /// Install the outbound [`FenceLog`]. Called once from
    /// `bins/kaas/main.rs` cluster bring-up.
    pub fn install_fence_log(&self, log: Arc<FenceLog>) {
        *self.fence_log.write() = Some(log);
    }

    /// Read the installed [`FenceLog`]. Handlers that need to
    /// broadcast a producer-epoch bump (just `InitProducerId`
    /// today) read this; `None` skips the broadcast — the local
    /// engine fence still fires, only cross-broker propagation is
    /// disabled.
    pub fn fence_log(&self) -> Option<Arc<FenceLog>> {
        self.fence_log.read().clone()
    }

    /// Install the cross-broker [`MarkerQueue`]. Called once from
    /// `bins/kaas/main.rs` cluster bring-up.
    pub fn install_marker_queue(&self, q: MarkerQueue) {
        *self.marker_queue.write() = Some(q);
    }

    /// Read the installed [`MarkerQueue`]. `EndTxn` uses this to
    /// dispatch markers to peer-broker partition leaders. `None`
    /// (handler-only tests, dev mode without a cluster runtime)
    /// skips the cross-broker leg — same-broker partitions still
    /// get markers written.
    pub fn marker_queue(&self) -> Option<MarkerQueue> {
        self.marker_queue.read().clone()
    }

    /// Install the admin-path [`TopicCRWriter`]. Called once from
    /// `bins/kaas/main.rs` cluster bring-up; tests can install a
    /// fake writer directly.
    ///
    /// [`TopicCRWriter`]: crate::topic_cr_writer::TopicCRWriter
    pub fn install_cr_writer(&self, w: Arc<dyn crate::topic_cr_writer::TopicCRWriter>) {
        *self.cr_writer.write() = Some(w);
    }

    /// Read the installed CR writer. `None` ⇒ the admin handlers
    /// (CreatePartitions, IncrementalAlterConfigs) surface
    /// `CLUSTER_AUTHORIZATION_FAILED` (31) without attempting any
    /// patch — clean wire response in dev mode.
    pub fn cr_writer(&self) -> Option<Arc<dyn crate::topic_cr_writer::TopicCRWriter>> {
        self.cr_writer.read().clone()
    }

    /// Install the ACL-path [`AclCRWriter`] (gh #107 parity). Called
    /// once from `bins/kaas/main.rs` cluster bring-up.
    ///
    /// [`AclCRWriter`]: crate::acl_cr_writer::AclCRWriter
    pub fn install_acl_cr_writer(&self, w: Arc<dyn crate::acl_cr_writer::AclCRWriter>) {
        *self.acl_cr_writer.write() = Some(w);
    }

    /// Read the installed ACL writer. `None` ⇒ DescribeAcls answers
    /// with an empty binding set and CreateAcls / DeleteAcls degrade
    /// to per-entry no-ops — the pre-gh #107 stub behaviour the
    /// kafka-compat tests rely on.
    /// Install the KafkaUser credential writer (gh #252). Cluster
    /// mode only; see the field docs for the dev-mode degradation.
    pub fn install_user_cr_writer(&self, w: Arc<dyn crate::user_cr_writer::UserCRWriter>) {
        *self.user_cr_writer.write() = Some(w);
    }

    pub fn user_cr_writer(&self) -> Option<Arc<dyn crate::user_cr_writer::UserCRWriter>> {
        self.user_cr_writer.read().clone()
    }

    pub fn acl_cr_writer(&self) -> Option<Arc<dyn crate::acl_cr_writer::AclCRWriter>> {
        self.acl_cr_writer.read().clone()
    }

    /// The quota enforcer behind [`Self::quotas`]. `None` ⇒
    /// Describe/AlterClientQuotas surface `UNSUPPORTED_VERSION` (35)
    /// without attempting any mutation.
    pub fn quota_enforcer(&self) -> Option<Arc<QuotaEnforcer>> {
        self.quota_enforcer.clone()
    }

    /// Drop everything this broker holds for a deleted topic. Called
    /// from the topic watch's delete callback, after the topic has left
    /// the registry, so no new request can touch it.
    pub async fn forget_deleted_topic(&self, topic: &str) {
        // gh #219: drop the topic's partitions from the engine
        // *without persisting*. Leaving it to the assignment recompute
        // is too late on a delete→recreate (Streams' `application-reset`
        // does exactly that): the recompute can coalesce both events
        // into no ownership change at all, leaving the recreated topic
        // served from the dead one's in-memory state — and a relinquish
        // landing after the operator re-created the directory writes the
        // dead incarnation's manifest into it.
        let dropped = self.engine.abandon_topic(topic).await;
        if dropped > 0 {
            tracing::info!(topic, dropped, "topic deleted; dropped open partitions");
        }
        // gh #240: a deleted topic's committed group offsets must go
        // with it, the way Apache tombstones them out of
        // `__consumer_offsets`. Left behind, a delete→recreate under the
        // same name (Streams' `application-reset` every run) hands the
        // new incarnation the dead one's offsets; if the new log is
        // shorter, the group resumes past its end and idles forever with
        // no error on either side. The Manager is absent in dev mode and
        // for the brief window before the cluster runtime installs it —
        // no coordinated groups exist then either.
        if let Some(mgr) = self.coord_manager() {
            let (purged, failed) = mgr.purge_topic_offsets(topic);
            if purged > 0 {
                tracing::info!(
                    topic,
                    purged,
                    "topic deleted; purged committed group offsets"
                );
            }
            if !failed.is_empty() {
                // Best-effort: the stale entries only bite a group that
                // resumes on a recreated topic of the same name.
                tracing::warn!(
                    topic,
                    groups = failed.join(","),
                    "topic deleted; could not purge offsets for some groups"
                );
            }
        }
        // gh #261: and its per-topic traffic counters, which otherwise
        // emit a dead series for every topic ever deleted (Streams'
        // reset churns through dozens), as Apache's
        // BrokerTopicStats.removeMetrics does. After the abandon, so an
        // append still in flight can't re-create the entry.
        kaas_observability::metrics::global()
            .topic_traffic
            .forget(topic);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_storage::MemoryStorage;

    fn test_broker() -> Broker {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Broker::new(engine, Arc::new(TopicRegistry::new()), "kaas-test", 0)
    }

    #[test]
    fn producer_ids_are_monotonic() {
        let b = test_broker();
        let a = b.next_producer_id();
        let c = b.next_producer_id();
        assert!(c > a);
    }

    #[test]
    fn first_producer_id_is_one() {
        let b = test_broker();
        assert_eq!(b.next_producer_id(), 1);
    }

    /// gh #261: a deleted topic's traffic counters are dropped, not left
    /// to emit a dead series for the life of the process.
    #[tokio::test]
    async fn a_deleted_topic_stops_emitting_traffic_series() {
        let b = test_broker();
        let tt = kaas_observability::metrics::global().topic_traffic.clone();
        // Unique names: the meter is process-global and tests run in
        // parallel.
        let (gone, kept) = ("gh261-deleted", "gh261-kept");
        tt.record_produce(gone, 3, 300);
        tt.record_produce(kept, 1, 100);
        assert!(tt.tracks(gone));

        b.forget_deleted_topic(gone).await;

        assert!(!tt.tracks(gone), "the deleted topic's series must go");
        assert!(tt.tracks(kept), "only the deleted topic's");
        assert_eq!(tt.produce_records(gone), 0);
    }
}
