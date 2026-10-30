//! Coordinator façade — the entry point handlers in `kaas-broker` call
//! into.
//!
//! One struct that wraps `OffsetStore`, the per-group state
//! machine map, and the hot-swappable `GroupAssignmentSource` /
//! `TxnAssignmentSource` traits. This Rust port keeps the same shape
//! and the same ownership boundaries — the source traits are
//! satisfied by `kaas-broker::Coordinator` in production (gh #92) and
//! by local stubs in tests.
//!
//! The Manager translates "do I own this group?" into a `bool` that
//! handlers map to `NOT_COORDINATOR` (16); methods that touch the
//! group state machine return either a concrete response shape or a
//! wire-level error code so the handler layer in `kaas-broker` only has
//! to glue the codec types in.

use std::collections::HashMap;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::group::{
    error_codes, Group, GroupSnapshot, GroupState, HeartbeatRequest, JoinOutcome, JoinRequest,
    SyncOutcome, SyncRequest,
};
use crate::offset_store::{FetchSpec, OffsetStore};

/// Broker-id type: a string, the StatefulSet pod
/// name (`kaas-0`, `kaas-1`, …).
pub type BrokerId = String;

/// Resolve a broker ID → (node_id, host, port) triple for the
/// `FindCoordinator` response. Returns `None` if the broker is not
/// in the current cluster view.
pub trait BrokerLookup: Send + Sync + 'static {
    fn lookup(&self, broker_id: &str) -> Option<BrokerEndpoint>;
}

#[derive(Debug, Clone)]
pub struct BrokerEndpoint {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

/// Closure-backed `BrokerLookup` implementation. Useful for tests
/// and for the dev-mode bootstrap when there's exactly one broker.
pub struct FnLookup<F: Fn(&str) -> Option<BrokerEndpoint> + Send + Sync + 'static> {
    f: F,
}

impl<F: Fn(&str) -> Option<BrokerEndpoint> + Send + Sync + 'static> std::fmt::Debug
    for FnLookup<F>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FnLookup").finish_non_exhaustive()
    }
}

impl<F: Fn(&str) -> Option<BrokerEndpoint> + Send + Sync + 'static> FnLookup<F> {
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F: Fn(&str) -> Option<BrokerEndpoint> + Send + Sync + 'static> BrokerLookup for FnLookup<F> {
    fn lookup(&self, broker_id: &str) -> Option<BrokerEndpoint> {
        (self.f)(broker_id)
    }
}

/// "Do I coordinate group G?" — answered by `kaas-broker::Coordinator`
/// (consults `assignment.json`) in production. gh #92 hot-swap shape
/// — the Manager holds the source behind an `RwLock` and swaps via
/// `set_group_assignment_source`.
pub trait GroupAssignmentSource: Send + Sync + 'static {
    fn owns_group(&self, group_id: &str) -> bool;
    fn group_coordinator(&self, group_id: &str) -> Option<BrokerId>;
    /// The assignment this source's answers are based on, as the
    /// gh #167 offset-file fence stamp. Default zero — dev/test stubs
    /// have a single writer and nothing to fence.
    fn assignment_fence(&self) -> crate::offset_store::FenceStamp {
        crate::offset_store::FenceStamp::default()
    }
}

/// gh #91 sibling of [`GroupAssignmentSource`] used by
/// `FindCoordinator(key_type=transaction)`. The Phase 5 broker wires
/// `kaas-broker::Coordinator` into both seams; Phase 6 brings up the
/// `TxnStateStore` that consumes the `Manager::wire_txn_offset_hook`
/// callback (here as a no-op until the txn handler lands).
pub trait TxnAssignmentSource: Send + Sync + 'static {
    fn owns_txn(&self, transactional_id: &str) -> bool;
    fn txn_coordinator(&self, transactional_id: &str) -> Option<BrokerId>;
}

/// Bootstrap `GroupAssignmentSource` — always reports ownership
/// (`true`) and self as coordinator. Wired at startup before the
/// real `kaas-broker::Coordinator` is up; `cluster_runtime` (Phase 5
/// §G) hot-swaps in the real source via
/// `Manager::set_group_assignment_source`.
#[derive(Debug)]
pub struct LocalGroupSource {
    pub self_id: BrokerId,
}

impl LocalGroupSource {
    pub fn new(self_id: impl Into<BrokerId>) -> Arc<Self> {
        Arc::new(Self {
            self_id: self_id.into(),
        })
    }
}

impl GroupAssignmentSource for LocalGroupSource {
    fn owns_group(&self, _group_id: &str) -> bool {
        true
    }
    fn group_coordinator(&self, _group_id: &str) -> Option<BrokerId> {
        Some(self.self_id.clone())
    }
}

/// Bootstrap `TxnAssignmentSource` — same shape as
/// [`LocalGroupSource`] for the Phase 6 txn handlers.
#[derive(Debug)]
pub struct LocalTxnSource {
    pub self_id: BrokerId,
}

impl LocalTxnSource {
    pub fn new(self_id: impl Into<BrokerId>) -> Arc<Self> {
        Arc::new(Self {
            self_id: self_id.into(),
        })
    }
}

impl TxnAssignmentSource for LocalTxnSource {
    fn owns_txn(&self, _txn_id: &str) -> bool {
        true
    }
    fn txn_coordinator(&self, _txn_id: &str) -> Option<BrokerId> {
        Some(self.self_id.clone())
    }
}

/// Per-broker coordinator state. Carries every consumer-group that
/// this broker owns under the current assignment + the offset store
/// behind the `__consumer_offsets/` directory.
pub struct Manager {
    pub broker_id: BrokerId,
    pub offsets: Arc<OffsetStore>,
    group_source: RwLock<Arc<dyn GroupAssignmentSource>>,
    txn_source: RwLock<Option<Arc<dyn TxnAssignmentSource>>>,
    lookup_broker: Arc<dyn BrokerLookup>,
    groups: DashMap<String, Arc<Group>>,
}

impl std::fmt::Debug for Manager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Manager")
            .field("broker_id", &self.broker_id)
            .field("groups", &self.groups.len())
            .finish()
    }
}

impl Manager {
    pub fn new(
        broker_id: impl Into<BrokerId>,
        offsets: Arc<OffsetStore>,
        lookup_broker: Arc<dyn BrokerLookup>,
        initial_group_source: Arc<dyn GroupAssignmentSource>,
    ) -> Arc<Self> {
        Arc::new(Self {
            broker_id: broker_id.into(),
            offsets,
            group_source: RwLock::new(initial_group_source),
            txn_source: RwLock::new(None),
            lookup_broker,
            groups: DashMap::new(),
        })
    }

    /// Atomically replace the group-assignment source. Called once
    /// from `bins/kaas/main.rs` after `kaas-broker::Coordinator`
    /// boots (gh #92 hot-swap).
    pub fn set_group_assignment_source(&self, src: Arc<dyn GroupAssignmentSource>) {
        // gh #167: the offset store's write fence follows the same
        // source, so every group-file write is stamped with the
        // assignment it was made under.
        let fence_src = src.clone();
        self.offsets
            .set_fence_source(move || fence_src.assignment_fence());
        *self.group_source.write() = src;
    }

    /// Atomically replace the txn-assignment source (Phase 6 wires
    /// the real txn store here; the Phase 5 path keeps `None`).
    pub fn set_txn_assignment_source(&self, src: Arc<dyn TxnAssignmentSource>) {
        *self.txn_source.write() = Some(src);
    }

    /// Does this broker coordinate `group_id` under the current
    /// assignment?
    pub fn is_coordinator(&self, group_id: &str) -> bool {
        self.group_source.read().owns_group(group_id)
    }

    /// IDs of every group this broker is currently the coordinator
    /// for. Order unspecified. Used by the heartbeat path to
    /// populate `BrokerStatus.active_groups` for the controller's
    /// assignment loop.
    pub fn local_groups(&self) -> Vec<String> {
        self.groups
            .iter()
            .filter(|kv| self.is_coordinator(kv.key()))
            .map(|kv| kv.key().clone())
            .collect()
    }

    /// Every group materialised in memory here, coordinated by this
    /// broker or not. Order unspecified.
    ///
    /// [`Self::local_groups`] answers "what should I report upstream";
    /// this answers "what am I holding". `GroupTakeoverDriver`'s
    /// orphan sweep needs the second question: a group this broker no
    /// longer coordinates is exactly the one to drop, and filtering by
    /// [`Self::is_coordinator`] first hides it from the sweep.
    pub fn resident_groups(&self) -> Vec<String> {
        self.groups.iter().map(|kv| kv.key().clone()).collect()
    }

    /// Drop in-memory state for a group. Called by
    /// `GroupTakeoverDriver` when the cluster controller reassigns
    /// the group to another broker. Pending offset commits remain
    /// on disk for the new coordinator to load.
    pub fn relinquish_group(&self, group_id: &str) {
        if let Some((_, g)) = self.groups.remove(group_id) {
            g.shutdown();
        }
    }

    fn get_or_create(&self, group_id: &str) -> Arc<Group> {
        if let Some(g) = self.groups.get(group_id) {
            return g.clone();
        }
        let g = Group::new(group_id);
        // Insert-then-load. A racy second caller may have inserted
        // their own Arc<Group> already — fall back to it.
        match self.groups.entry(group_id.to_owned()) {
            dashmap::mapref::entry::Entry::Occupied(o) => o.get().clone(),
            dashmap::mapref::entry::Entry::Vacant(v) => {
                v.insert(g.clone());
                let _ = self.offsets.load(group_id);
                g
            }
        }
    }

    // --- find_coordinator ---------------------------------------------

    /// Resolve one `(key, key_type)` pair into the
    /// `(node_id, host, port, error_code)` tuple
    /// [`crate::group::error_codes`] uses. `key_type` is `0` for
    /// group and `1` for transaction; anything else is
    /// `INVALID_REQUEST` (42).
    pub fn find_coordinator(&self, key: &str, key_type: i8) -> CoordinatorResolution {
        const COORD_NOT_AVAILABLE: i16 = 15;
        const INVALID_REQUEST: i16 = 42;
        let resolved = match key_type {
            0 => self.group_source.read().group_coordinator(key),
            1 => match self.txn_source.read().as_ref() {
                None => {
                    return CoordinatorResolution {
                        node_id: 0,
                        host: String::new(),
                        port: 0,
                        error_code: COORD_NOT_AVAILABLE,
                    };
                }
                Some(s) => s.txn_coordinator(key),
            },
            _ => {
                return CoordinatorResolution {
                    node_id: 0,
                    host: String::new(),
                    port: 0,
                    error_code: INVALID_REQUEST,
                };
            }
        };
        match resolved.and_then(|id| self.lookup_broker.lookup(&id)) {
            Some(ep) => CoordinatorResolution {
                node_id: ep.node_id,
                host: ep.host,
                port: ep.port,
                error_code: 0,
            },
            None => CoordinatorResolution {
                node_id: 0,
                host: String::new(),
                port: 0,
                error_code: COORD_NOT_AVAILABLE,
            },
        }
    }

    // --- group state machine entry points --------------------------------

    pub async fn join_group(&self, group_id: &str, req: JoinRequest) -> JoinOutcome {
        if !self.is_coordinator(group_id) {
            return JoinOutcome {
                error_code: 16, // NOT_COORDINATOR
                generation_id: -1,
                leader: String::new(),
                member_id: req.member_id,
                protocol_type: String::new(),
                protocol_name: String::new(),
                members: Vec::new(),
            };
        }
        let g = self.get_or_create(group_id);
        g.join(req).await
    }

    pub async fn sync_group(&self, group_id: &str, req: SyncRequest) -> SyncOutcome {
        if !self.is_coordinator(group_id) {
            return SyncOutcome {
                error_code: 16,
                ..SyncOutcome::default()
            };
        }
        let g = match self.groups.get(group_id) {
            Some(g) => g.clone(),
            None => {
                return SyncOutcome {
                    error_code: error_codes::UNKNOWN_MEMBER_ID,
                    ..SyncOutcome::default()
                };
            }
        };
        g.sync(req).await
    }

    pub fn heartbeat(&self, group_id: &str, req: HeartbeatRequest) -> i16 {
        if !self.is_coordinator(group_id) {
            return 16;
        }
        match self.groups.get(group_id) {
            Some(g) => g.heartbeat(req),
            None => error_codes::UNKNOWN_MEMBER_ID,
        }
    }

    pub fn leave_group(&self, group_id: &str, member_ids: &[String]) -> LeaveOutcome {
        if !self.is_coordinator(group_id) {
            return LeaveOutcome {
                group_error: 16,
                members: Vec::new(),
            };
        }
        let g = match self.groups.get(group_id) {
            Some(g) => g.clone(),
            None => {
                return LeaveOutcome {
                    group_error: 0,
                    members: Vec::new(),
                };
            }
        };
        LeaveOutcome {
            group_error: 0,
            members: g.leave(member_ids),
        }
    }

    // --- offsets ------------------------------------------------------

    /// Persist committed offsets for a group. The handler does the
    /// codec → `(key, value)` translation; this method writes them
    /// through to the store. Returns a tuple of `(group-level error,
    /// per-key write success)` so the handler can encode partition
    /// responses. The `metadata` map is keyed identically to
    /// `offsets` — empty strings map to the wire null sentinel.
    pub fn offset_commit(
        &self,
        group_id: &str,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) -> i16 {
        if !self.is_coordinator(group_id) {
            return 16;
        }
        match self
            .offsets
            .commit_with_metadata(group_id, offsets, metadata)
        {
            Ok(()) => 0,
            // gh #167: a fenced write means our assignment view is
            // stale and another broker coordinates this group now —
            // exactly what NOT_COORDINATOR tells the client, which
            // then re-runs FindCoordinator instead of retrying here.
            Err(e)
                if e.get_ref()
                    .is_some_and(|inner| inner.is::<crate::offset_store::FencedOffsetWrite>()) =>
            {
                16
            }
            // Best-effort: handler logs but reports success per
            // Apache's "offset commit eventual consistency".
            Err(_) => 0,
        }
    }

    /// Per-(topic, partitions) committed offsets + metadata for a
    /// group. Returns both maps; the handler joins them by
    /// `offset_key`. `None` ↔ "broker not coordinator" so the
    /// handler returns the per-partition `NOT_COORDINATOR` error.
    pub fn offset_fetch(
        &self,
        group_id: &str,
        specs: &[FetchSpec],
    ) -> Option<(HashMap<String, i64>, HashMap<String, String>)> {
        if !self.is_coordinator(group_id) {
            return None;
        }
        Some((
            self.offsets.fetch(group_id, specs),
            self.offsets.fetch_metadata(group_id, specs),
        ))
    }

    /// Drop a group's offsets + state. Returns the group-level error
    /// code per Apache's contract: `0` (success), `16`
    /// (NOT_COORDINATOR), `67` (NON_EMPTY_GROUP — Stable /
    /// PreparingRebalance / CompletingRebalance), `69`
    /// (GROUP_ID_NOT_FOUND).
    pub fn delete_group(&self, group_id: &str) -> i16 {
        const NON_EMPTY_GROUP: i16 = 67;
        const GROUP_ID_NOT_FOUND: i16 = 69;
        if !self.is_coordinator(group_id) {
            return 16;
        }
        let has_state = self.groups.contains_key(group_id);
        let has_disk = self.offsets.has_group(group_id);
        if !has_state && !has_disk {
            return GROUP_ID_NOT_FOUND;
        }
        if let Some(g) = self.groups.get(group_id) {
            match g.state() {
                GroupState::Empty | GroupState::Dead => {}
                _ => return NON_EMPTY_GROUP,
            }
        }
        // Drop in-memory state, then disk. Errors past the in-memory
        // wipe are swallowed (best-effort) — a stale file
        // on the PVC is harmless and gets re-cleaned on next
        // start-up sweep.
        if let Some((_, g)) = self.groups.remove(group_id) {
            g.shutdown();
        }
        let _ = self.offsets.delete(group_id);
        0
    }

    /// Per-(topic, partitions) offset deletion (gh #100 — OffsetDelete
    /// key 47). Returns `(group_error, key→removed bool)`.
    pub fn delete_offsets(&self, group_id: &str, keys: &[String]) -> (i16, HashMap<String, bool>) {
        if !self.is_coordinator(group_id) {
            return (16, HashMap::new());
        }
        match self.offsets.delete_partitions(group_id, keys) {
            Ok(removed) => (0, removed),
            Err(_) => (0, HashMap::new()),
        }
    }

    /// Drop every committed offset for `topic` from the groups this
    /// broker coordinates (gh #240). Returns the number of
    /// `(topic, partition)` entries removed, and the group IDs whose
    /// files could not be rewritten.
    ///
    /// Two things this deliberately does:
    ///
    /// - **Filters by coordinator ownership.** A topic-delete event
    ///   lands on *every* broker's topic watch, so an unfiltered purge
    ///   would have N brokers read-modify-writing the same group file
    ///   on the shared volume. A group's file has exactly one writer:
    ///   its coordinator (NFS substrate rule 3).
    /// - **Unions in the on-disk groups.** `self.groups` holds only the
    ///   groups this broker has materialised; a coordinated group that
    ///   nobody has touched yet still has a file, and `load` would
    ///   bring its stale entries straight back.
    pub fn purge_topic_offsets(&self, topic: &str) -> (usize, Vec<String>) {
        let mut ids = self.offsets.group_ids_on_disk();
        ids.extend(self.groups.iter().map(|kv| kv.key().clone()));
        ids.sort();
        ids.dedup();

        let mut removed = 0;
        let mut failed = Vec::new();
        for gid in ids {
            if !self.is_coordinator(&gid) {
                continue;
            }
            match self.offsets.purge_topic(&gid, topic) {
                Ok(n) => removed += n,
                Err(_) => failed.push(gid),
            }
        }
        (removed, failed)
    }

    /// Snapshot every group this broker coordinates. Used by
    /// `ListGroups` (key 16) — handler filters by `states_filter`
    /// after.
    pub fn list_groups(&self) -> Vec<GroupSnapshot> {
        self.groups
            .iter()
            .filter(|kv| self.is_coordinator(kv.key()))
            .map(|kv| kv.value().describe())
            .collect()
    }

    /// Snapshot the listed groups in `ids`. Groups not coordinated
    /// here surface as `None` so the handler can encode
    /// `NOT_COORDINATOR`; groups coordinated but unknown surface as
    /// `Some(snapshot_with_empty_state)`.
    pub fn describe_groups(&self, ids: &[String]) -> Vec<Option<GroupSnapshot>> {
        ids.iter()
            .map(|id| {
                if !self.is_coordinator(id) {
                    return None;
                }
                Some(match self.groups.get(id) {
                    Some(g) => g.describe(),
                    None => GroupSnapshot {
                        id: id.clone(),
                        state: "Empty",
                        protocol_type: String::new(),
                        protocol_name: String::new(),
                        generation_id: 0,
                        members: Vec::new(),
                    },
                })
            })
            .collect()
    }
}

/// `FindCoordinator` result — translates 1:1 to the codec response
/// shape in `kaas-codec::api::find_coordinator`. The handler picks
/// either the v0..=v3 single-coordinator form or the v4+ array
/// based on the request shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorResolution {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error_code: i16,
}

/// `LeaveGroup` result — `group_error` is the top-level error,
/// `members` is per-member `(member_id, error_code)`.
#[derive(Debug, Clone)]
pub struct LeaveOutcome {
    pub group_error: i16,
    pub members: Vec<(String, i16)>,
}

/// Compatibility re-export so handler code can build offset_key
/// strings from `(topic, partition)` without a deep import path.
pub use crate::offset_store::offset_key as build_offset_key;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::group::{JoinRequest, ProtocolMetadata};
    use crate::offset_store::offset_key;
    use bytes::Bytes;
    use std::time::Duration;

    fn lookup_self(self_id: &str, port: i32) -> Arc<dyn BrokerLookup> {
        let id = self_id.to_owned();
        Arc::new(FnLookup::new(move |req| {
            if req == id {
                Some(BrokerEndpoint {
                    node_id: 0,
                    host: format!("{id}.example.com"),
                    port,
                })
            } else {
                None
            }
        }))
    }

    fn manager(dir: &std::path::Path) -> Arc<Manager> {
        let offsets = Arc::new(OffsetStore::new(dir));
        Manager::new(
            "kaas-0",
            offsets,
            lookup_self("kaas-0", 9092),
            LocalGroupSource::new("kaas-0"),
        )
    }

    fn join_req(client_id: &str, protocol: &str) -> JoinRequest {
        JoinRequest {
            member_id: String::new(),
            group_instance_id: None,
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 30_000,
            protocol_type: "consumer".to_owned(),
            protocols: vec![ProtocolMetadata {
                name: protocol.to_owned(),
                metadata: Bytes::from_static(b"meta"),
            }],
            version: 9,
            client_id: client_id.to_owned(),
            client_host: "/127.0.0.1".to_owned(),
        }
    }

    #[test]
    fn find_coordinator_resolves_self() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        let r = m.find_coordinator("g1", 0);
        assert_eq!(r.error_code, 0);
        assert_eq!(r.host, "kaas-0.example.com");
        assert_eq!(r.port, 9092);
    }

    #[test]
    fn find_coordinator_unknown_key_type_is_invalid_request() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        let r = m.find_coordinator("g1", 7);
        assert_eq!(r.error_code, 42); // INVALID_REQUEST
    }

    #[test]
    fn find_coordinator_txn_with_no_source_is_unavailable() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        let r = m.find_coordinator("tx1", 1);
        assert_eq!(r.error_code, 15); // COORDINATOR_NOT_AVAILABLE
    }

    #[test]
    fn find_coordinator_txn_with_source_resolves() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        m.set_txn_assignment_source(LocalTxnSource::new("kaas-0"));
        let r = m.find_coordinator("tx1", 1);
        assert_eq!(r.error_code, 0);
        assert_eq!(r.host, "kaas-0.example.com");
    }

    /// gh #240: the topic-delete event lands on every broker, so the
    /// purge must touch only the groups this broker coordinates —
    /// otherwise N brokers read-modify-write the same file on the
    /// shared volume.
    #[test]
    fn purge_topic_offsets_skips_groups_this_broker_does_not_coordinate() {
        struct OwnsOnlyMine;
        impl GroupAssignmentSource for OwnsOnlyMine {
            fn owns_group(&self, group_id: &str) -> bool {
                group_id == "mine"
            }
            fn group_coordinator(&self, _group_id: &str) -> Option<BrokerId> {
                Some("kaas-0".to_owned())
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        // Both groups have a file; neither is materialised in memory.
        for g in ["mine", "theirs"] {
            let mut o = HashMap::new();
            o.insert(offset_key("gone", 0), 51_607);
            o.insert(offset_key("stays", 0), 7);
            m.offsets.commit(g, o).unwrap();
        }
        m.set_group_assignment_source(Arc::new(OwnsOnlyMine));

        let (removed, failed) = m.purge_topic_offsets("gone");
        assert_eq!(removed, 1, "purged {removed} entries");
        assert!(failed.is_empty());

        let fresh = OffsetStore::new(tmp.path());
        for (g, want) in [("mine", -1), ("theirs", 51_607)] {
            fresh.load(g).unwrap();
            let got = fresh.fetch(
                g,
                &[FetchSpec {
                    topic: "gone".to_owned(),
                    partitions: vec![0],
                }],
            );
            assert_eq!(got.get("gone/0"), Some(&want), "group {g}");
        }
    }

    #[test]
    fn not_coordinator_on_offset_commit_when_source_says_no() {
        struct DenyAll;
        impl GroupAssignmentSource for DenyAll {
            fn owns_group(&self, _group_id: &str) -> bool {
                false
            }
            fn group_coordinator(&self, _group_id: &str) -> Option<BrokerId> {
                None
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        m.set_group_assignment_source(Arc::new(DenyAll));
        let mut offsets = HashMap::new();
        offsets.insert(offset_key("t1", 0), 42);
        assert_eq!(m.offset_commit("g1", offsets, HashMap::new()), 16);
        assert_eq!(
            m.heartbeat(
                "g1",
                HeartbeatRequest {
                    member_id: "m".into(),
                    generation_id: 0,
                    group_instance_id: None,
                }
            ),
            16
        );
    }

    #[tokio::test(start_paused = true)]
    async fn join_then_sync_then_offset_commit_then_fetch_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        let m2 = m.clone();
        let join =
            tokio::spawn(async move { m2.join_group("g1", join_req("consumer-1", "range")).await });
        tokio::time::sleep(Duration::from_millis(
            crate::group::INITIAL_REBALANCE_DELAY_MS + 100,
        ))
        .await;
        let j = join.await.unwrap();
        assert_eq!(j.error_code, 0);

        // Leader supplies one assignment for itself.
        let sync = SyncRequest {
            member_id: j.member_id.clone(),
            generation_id: j.generation_id,
            assignments: vec![crate::group::SyncAssignment {
                member_id: j.member_id.clone(),
                assignment: Bytes::from_static(b"\xde\xad"),
            }],
            ..SyncRequest::default()
        };
        let s = m.sync_group("g1", sync).await;
        assert_eq!(s.error_code, 0);
        assert_eq!(s.assignment.as_ref(), b"\xde\xad");

        // Commit one offset, fetch it back.
        let mut offsets = HashMap::new();
        offsets.insert(offset_key("t1", 0), 42);
        assert_eq!(m.offset_commit("g1", offsets, HashMap::new()), 0);
        let (got_offsets, _) = m
            .offset_fetch(
                "g1",
                &[FetchSpec {
                    topic: "t1".into(),
                    partitions: vec![0],
                }],
            )
            .unwrap();
        assert_eq!(got_offsets.get("t1/0"), Some(&42));

        // local_groups reflects the active group.
        assert_eq!(m.local_groups(), vec!["g1".to_owned()]);
    }

    #[tokio::test(start_paused = true)]
    async fn delete_group_non_empty_when_state_is_stable() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        let m2 = m.clone();
        let join = tokio::spawn(async move { m2.join_group("g1", join_req("c1", "range")).await });
        tokio::time::sleep(Duration::from_millis(
            crate::group::INITIAL_REBALANCE_DELAY_MS + 100,
        ))
        .await;
        let j = join.await.unwrap();
        let _ = m
            .sync_group(
                "g1",
                SyncRequest {
                    member_id: j.member_id.clone(),
                    generation_id: j.generation_id,
                    assignments: vec![crate::group::SyncAssignment {
                        member_id: j.member_id.clone(),
                        assignment: Bytes::new(),
                    }],
                    ..SyncRequest::default()
                },
            )
            .await;
        // Group is Stable — delete must fail with NON_EMPTY_GROUP.
        assert_eq!(m.delete_group("g1"), 67);
        // After all members leave, delete succeeds.
        let _ = m.leave_group("g1", &[j.member_id]);
        assert_eq!(m.delete_group("g1"), 0);
    }

    #[test]
    fn delete_group_unknown_returns_group_id_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let m = manager(tmp.path());
        assert_eq!(m.delete_group("never-existed"), 69);
    }
}
