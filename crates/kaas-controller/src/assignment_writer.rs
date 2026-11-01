//! Atomic `assignment.json` writer + recompute loop.
//!
//! The
//! controller broker is the only writer; every other broker reads
//! the file via [`kaas_broker::Coordinator`]. The writer's job is to
//! recompute the assignment on every input change (broker join/
//! leave, topic CR change, active-group churn) and atomically
//! replace the file with the new version.
//!
//! Atomicity: tempfile + `rename`. NFSv4 guarantees same-directory
//! rename is atomic, so a crash mid-write leaves either the old or
//! the new file — never a torn JSON.
//!
//! Source-of-truth seams as traits — production wires
//! [`HeartbeatServer::active_groups`] + the topic CR watcher + the
//! K8s endpoint registry; tests pass `Vec`-backed stubs.
//!
//! [`HeartbeatServer::active_groups`]:
//!     crate::heartbeat_server::HeartbeatServer::active_groups

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use chrono::SecondsFormat;
use kaas_broker::{
    Assignment, BrokerAssignment, BrokerHealth, ConsumerGroupAssignment, PartitionAssignment,
};
use parking_lot::Mutex;
use serde::Serialize;

use crate::balancer::{balance, balance_groups, GroupSpec, TopicSpec};

/// "Tell the loop *why* it should recompute". Reasons are
/// informational — they end up on tracing spans but don't gate the
/// recompute itself.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
pub enum AssignmentReason {
    BrokerJoined,
    BrokerLeaving,
    BrokerDead,
    TopicCreated,
    TopicDeleted,
    TopicResized,
    AdminRebalance,
    InitialRecompute,
}

impl AssignmentReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BrokerJoined => "broker_joined",
            Self::BrokerLeaving => "broker_leaving",
            Self::BrokerDead => "broker_dead",
            Self::TopicCreated => "topic_created",
            Self::TopicDeleted => "topic_deleted",
            Self::TopicResized => "topic_resized",
            Self::AdminRebalance => "admin_rebalance",
            Self::InitialRecompute => "initial_recompute",
        }
    }
}

/// Live topic catalog the writer balances over.
pub trait TopicSource: Send + Sync + 'static {
    fn topics(&self) -> Vec<TopicSpec>;
}

/// Broker liveness — the alive subset the controller sees.
pub trait BrokerSource: Send + Sync + 'static {
    /// Brokers eligible to be assigned partitions and groups.
    fn alive_brokers(&self) -> Vec<String>;

    /// Every broker the cluster knows about, alive or not (gh #249):
    /// the registered set. A registered-but-not-alive broker is
    /// *fenced* — it keeps a row in `assignment.json` marked `dead`
    /// or `draining` instead of vanishing from it.
    ///
    /// Two things depend on the distinction. Fenced brokers are
    /// reportable (DescribeCluster v2 `IsFenced`), and — the
    /// load-bearing one — the coordinator hash
    /// divisor is the **full** set, so `hash(group) % n` doesn't
    /// reshuffle every group the moment one broker dies. See
    /// `kaas_broker::group_hash`.
    ///
    /// Defaults to [`Self::alive_brokers`], which is the pre-gh #249
    /// behaviour and the right answer for single-broker and test
    /// sources that have no separate notion of registration.
    fn registered_brokers(&self) -> Vec<String> {
        self.alive_brokers()
    }

    /// Registered brokers that have announced a graceful shutdown.
    /// Reported as `draining` rather than `dead`; they are expected
    /// to be excluded from [`Self::alive_brokers`] by the same
    /// source, so their work moves before they exit.
    fn draining_brokers(&self) -> Vec<String> {
        Vec::new()
    }
}

/// Consumer groups currently active in the cluster.
pub trait GroupSource: Send + Sync + 'static {
    fn active_groups(&self) -> Vec<String>;
}

/// `Vec`-backed helper that satisfies all three source traits;
/// useful for tests.
#[derive(Debug, Default, Clone)]
pub struct StaticSources {
    pub topics: Vec<TopicSpec>,
    pub brokers: Vec<String>,
    pub groups: Vec<String>,
}

impl TopicSource for StaticSources {
    fn topics(&self) -> Vec<TopicSpec> {
        self.topics.clone()
    }
}
impl BrokerSource for StaticSources {
    fn alive_brokers(&self) -> Vec<String> {
        self.brokers.clone()
    }
}
impl GroupSource for StaticSources {
    fn active_groups(&self) -> Vec<String> {
        self.groups.clone()
    }
}

/// The recompute → write → push pipeline.
///
/// Single-task ownership: all state mutation happens inside
/// [`AssignmentLoop::update_assignment`] (which currently runs the
/// recompute inline rather than via a coalescing channel because
/// production callers (`bins/kaas/main.rs`) don't generate enough
/// concurrent updates to warrant the coalescing yet. A follow-up
/// can introduce a `tokio::mpsc` queue if the call rate climbs.
pub struct AssignmentLoop<T, B, G> {
    data_dir: PathBuf,
    /// gh #221 phase 1: where `assignment.json` lives. `None` →
    /// legacy `<data_dir>/__cluster`. The broker sets this from
    /// `KAAS_CLUSTER_DIR` when the control-plane volume is split
    /// out. `data_dir` stays regardless — the epoch-floor scan walks
    /// the topic dirs, which never move.
    cluster_dir: Option<PathBuf>,
    controller_id: String,
    /// `AtomicI64` so [`Self::start`] can stamp the lease-acquire
    /// epoch after the `Arc` is already shared. Read on every
    /// recompute via [`Ordering::Relaxed`].
    controller_epoch: AtomicI64,
    topics: Arc<T>,
    brokers: Arc<B>,
    groups: Option<Arc<G>>,
    state: Mutex<LoopState>,
}

#[derive(Debug, Default)]
struct LoopState {
    current: Option<Assignment>,
    version_counter: i64,
}

impl<T, B, G> std::fmt::Debug for AssignmentLoop<T, B, G> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state = self.state.lock();
        f.debug_struct("AssignmentLoop")
            .field("data_dir", &self.data_dir)
            .field("controller_id", &self.controller_id)
            .field(
                "controller_epoch",
                &self.controller_epoch.load(Ordering::Relaxed),
            )
            .field("version_counter", &state.version_counter)
            .finish()
    }
}

impl<T, B, G> AssignmentLoop<T, B, G>
where
    T: TopicSource,
    B: BrokerSource,
    G: GroupSource,
{
    pub fn new(
        data_dir: impl Into<PathBuf>,
        controller_id: impl Into<String>,
        topics: Arc<T>,
        brokers: Arc<B>,
    ) -> Arc<Self> {
        Arc::new(Self {
            data_dir: data_dir.into(),
            cluster_dir: None,
            controller_id: controller_id.into(),
            controller_epoch: AtomicI64::new(0),
            topics,
            brokers,
            groups: None,
            state: Mutex::new(LoopState::default()),
        })
    }

    /// Override the directory holding `assignment.json` (gh #221
    /// phase 1). Call immediately after `new`, like the other
    /// builders.
    pub fn with_cluster_dir(self: Arc<Self>, dir: impl Into<PathBuf>) -> Arc<Self> {
        let mut this = self;
        if let Some(inner) = Arc::get_mut(&mut this) {
            inner.cluster_dir = Some(dir.into());
        }
        this
    }

    /// The effective cluster-state dir: the override, else the
    /// legacy `<data_dir>/__cluster`.
    fn cluster_dir(&self) -> PathBuf {
        self.cluster_dir
            .clone()
            .unwrap_or_else(|| self.data_dir.join("__cluster"))
    }

    /// Attach an optional [`GroupSource`]. Without one,
    /// `consumer_groups` stays empty on every write.
    pub fn with_group_source(self: Arc<Self>, g: Arc<G>) -> Arc<Self> {
        // Arc::get_mut is sound here because the loop hasn't been
        // shared yet — call this immediately after `new`.
        let mut this = self;
        if let Some(inner) = Arc::get_mut(&mut this) {
            inner.groups = Some(g);
        }
        this
    }

    /// Stamp the lease-acquire epoch + bootstrap from any existing
    /// `assignment.json` on disk. Returns the new file's version
    /// after the initial recompute. The epoch swap is atomic so a
    /// shared `Arc<AssignmentLoop>` is safe to start from any
    /// caller.
    pub async fn start(self: &Arc<Self>, epoch: i64) -> io::Result<i64> {
        self.controller_epoch.store(epoch, Ordering::Relaxed);
        // Bootstrap: carry the version counter forward so a
        // restarted controller doesn't rewind the sequence.
        let path = self.cluster_dir().join(Assignment::FILE_NAME);
        if let Ok(bytes) = std::fs::read(&path) {
            if let Ok(prev) = serde_json::from_slice::<Assignment>(&bytes) {
                let mut s = self.state.lock();
                if prev.assignment_version > s.version_counter {
                    s.version_counter = prev.assignment_version;
                }
                s.current = Some(prev);
            }
        }
        self.update_assignment(AssignmentReason::InitialRecompute)
            .await
    }

    /// Snapshot of the most recently written assignment. `None`
    /// before the first write.
    pub fn snapshot(&self) -> Option<Assignment> {
        self.state.lock().current.clone()
    }

    /// Recompute + write. `reason` is informational. Returns the new
    /// assignment_version.
    pub async fn update_assignment(&self, reason: AssignmentReason) -> io::Result<i64> {
        // Snapshot inputs outside the lock so the source traits'
        // own locking doesn't intersect with our `state` lock.
        let brokers = self.brokers.alive_brokers();
        // gh #249: the balancer only ever places work on ALIVE
        // brokers; `registered` widens only the reported broker list
        // (and with it the coordinator-hash divisor).
        let registered = self.brokers.registered_brokers();
        let draining = self.brokers.draining_brokers();
        let topics = self.topics.topics();
        let group_specs = self
            .groups
            .as_ref()
            .map(|g| {
                g.active_groups()
                    .into_iter()
                    .map(|id| GroupSpec { group_id: id })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        // gh #216: seed a per-partition epoch floor from the on-disk
        // manifest for any partition not already tracked in the current
        // assignment. A partition that dropped out and is being re-added
        // would otherwise reconcile as "new" and reset its leader epoch
        // to 1 — below its persisted epoch — and the storage append
        // fence would reject every write as stale. Read off the recompute
        // path (spawn_blocking) since it touches the shared volume; the
        // at-risk set is empty in steady state, so this is usually free.
        let epoch_floor = {
            let data_dir = self.data_dir.clone();
            let topic_dims: Vec<(String, i32)> = topics
                .iter()
                .map(|t| (t.name.clone(), t.partition_count))
                .collect();
            let prev_dims: Vec<(String, i32)> = self
                .snapshot()
                .map(|a| {
                    a.partitions
                        .iter()
                        .map(|p| (p.topic.clone(), p.partition))
                        .collect()
                })
                .unwrap_or_default();
            tokio::task::spawn_blocking(move || {
                compute_epoch_floor(&data_dir, &topic_dims, &prev_dims)
            })
            .await
            .unwrap_or_default()
        };

        let (assignment, version) = {
            let mut s = self.state.lock();
            let prev_parts: Option<Vec<PartitionAssignment>> =
                s.current.as_ref().map(|a| a.partitions.clone());
            let prev_groups: Option<Vec<ConsumerGroupAssignment>> =
                s.current.as_ref().map(|a| a.consumer_groups.clone());
            s.version_counter += 1;
            let version = s.version_counter;
            let now = chrono::Utc::now().to_rfc3339_opts(SecondsFormat::Nanos, true);
            // Broker rows FIRST: `balance_groups` resolves through the
            // same rows every broker will read back, so the entry it
            // mints agrees with the hash fallthrough by construction
            // (gh #248).
            let broker_entries = build_broker_entries(
                &registered,
                &brokers,
                &draining,
                &now,
                s.current.as_ref().map(|a| a.brokers.as_slice()),
            );
            let parts = balance(prev_parts.as_deref(), &brokers, &topics, &epoch_floor);
            let groups = balance_groups(prev_groups.as_deref(), &broker_entries, &group_specs);
            let a = Assignment {
                controller_epoch: self.controller_epoch.load(Ordering::Relaxed),
                assignment_version: version,
                generated_at: now.clone(),
                controller: self.controller_id.clone(),
                brokers: broker_entries,
                partitions: parts,
                consumer_groups: groups,
            };
            s.current = Some(a.clone());
            (a, version)
        };

        tracing::debug!(
            reason = reason.as_str(),
            assignment_version = version,
            partitions = assignment.partitions.len(),
            groups = assignment.consumer_groups.len(),
            "controller recompute"
        );

        let m = kaas_observability::metrics::global();
        m.assignment_changes.add(1, &[]);

        let started = std::time::Instant::now();
        let write_res = atomic_write(&self.cluster_dir(), &assignment);
        m.assignment_file_write_latency
            .record(started.elapsed().as_secs_f64(), &[]);
        m.assignment_file_writes.add(
            1,
            &[kaas_observability::KeyValue::new(
                "result",
                if write_res.is_ok() { "ok" } else { "error" },
            )],
        );
        write_res?;
        Ok(version)
    }
}

/// Read the on-disk manifest epoch for every `(topic, partition)` in
/// `topic_dims` that is NOT already tracked in `prev_dims`, returning a
/// `partition_key` → epoch floor map (gh #216). Absent, unreadable, or
/// epoch-0 manifests contribute no floor. Synchronous file I/O — call
/// via `spawn_blocking`.
fn compute_epoch_floor(
    data_dir: &Path,
    topic_dims: &[(String, i32)],
    prev_dims: &[(String, i32)],
) -> HashMap<String, u32> {
    let prev_keys: HashSet<String> = prev_dims
        .iter()
        .map(|(t, p)| kaas_broker::partition_key(t, *p))
        .collect();
    let fs = kaas_storage::RealFs;
    let mut floor = HashMap::new();
    for (name, count) in topic_dims {
        for partition in 0..*count {
            let key = kaas_broker::partition_key(name, partition);
            if prev_keys.contains(&key) {
                continue;
            }
            let dir = data_dir.join(name).join(partition.to_string());
            let epoch = match kaas_storage::manifest::read(&fs, &dir) {
                Ok(kaas_storage::manifest::ReadResult::Present(m)) => m.epoch,
                Ok(kaas_storage::manifest::ReadResult::Legacy(m)) => m.epoch,
                _ => 0,
            };
            if let Ok(e) = u32::try_from(epoch) {
                if e > 0 {
                    floor.insert(key, e);
                }
            }
        }
    }
    floor
}

/// Build the assignment's broker list: every **registered** broker,
/// each carrying the health the controller currently sees (gh #249).
///
/// Before this, the list was the alive set with every entry stamped
/// `Alive`, so a dead broker simply disappeared. Two consequences,
/// both fixed here: nothing could report a broker as fenced, and the
/// coordinator-hash divisor — documented in `kaas_broker::group_hash`
/// as "MUST be the full broker set size (including draining / dead)"
/// — silently shrank on every broker loss, reshuffling ~(N-1)/N of
/// all group and txn coordinators exactly when the cluster was
/// already degraded.
///
/// `last_seen` only advances for brokers actually seen this pass, so
/// a fenced broker's timestamp says when it was last alive rather
/// than when the controller last recomputed.
fn build_broker_entries(
    registered: &[String],
    alive: &[String],
    draining: &[String],
    now: &str,
    prev: Option<&[BrokerAssignment]>,
) -> Vec<BrokerAssignment> {
    let mut out: Vec<BrokerAssignment> = registered
        .iter()
        .map(|b| {
            let health = if draining.iter().any(|d| d == b) {
                BrokerHealth::Draining
            } else if alive.iter().any(|a| a == b) {
                BrokerHealth::Alive
            } else {
                BrokerHealth::Dead
            };
            let last_seen = if matches!(health, BrokerHealth::Dead) {
                prev.and_then(|p| p.iter().find(|e| e.id == *b))
                    .map_or_else(|| now.to_owned(), |e| e.last_seen.clone())
            } else {
                now.to_owned()
            };
            BrokerAssignment {
                id: b.clone(),
                health,
                last_seen,
            }
        })
        .collect();
    // An alive broker missing from the registered set would otherwise
    // be dropped from the list it is being assigned partitions in.
    // Can happen transiently: heartbeat-connected before its
    // EndpointSlice entry lands.
    for b in alive {
        if !out.iter().any(|e| e.id == *b) {
            out.push(BrokerAssignment {
                id: b.clone(),
                health: BrokerHealth::Alive,
                last_seen: now.to_owned(),
            });
        }
    }
    out.sort_by(|a, b| a.id.cmp(&b.id));
    out
}

/// `tmp + fsync + rename + fsync(dir)` write of `<cluster_dir>/assignment.json`.
/// Shared by the loop and by the controller-failover test harness.
/// Takes the cluster-state dir itself — callers resolve the legacy
/// `<data_dir>/__cluster` join (gh #221 phase 1).
fn atomic_write<T: Serialize>(cluster_dir: &std::path::Path, payload: &T) -> io::Result<()> {
    let dir = cluster_dir.to_path_buf();
    std::fs::create_dir_all(&dir)?;
    let final_path = dir.join(Assignment::FILE_NAME);
    let mut tmp_name = String::from(Assignment::FILE_NAME);
    tmp_name.push_str(".tmp");
    let tmp_path = dir.join(&tmp_name);
    let data = serde_json::to_vec(payload).map_err(io::Error::other)?;
    {
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        use std::io::Write;
        if let Err(e) = f.write_all(&data) {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
        if let Err(e) = f.sync_all() {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    }
    if let Err(e) = std::fs::rename(&tmp_path, &final_path) {
        let _ = std::fs::remove_file(&tmp_path);
        return Err(e);
    }
    // gh #231: on a local filesystem the rename isn't durable until the
    // directory is synced.
    kaas_storage::fs::fsync_dir(&dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topics(n: i32) -> Vec<TopicSpec> {
        (0..n)
            .map(|i| TopicSpec {
                name: format!("t{i}"),
                partition_count: 3,
            })
            .collect()
    }

    fn brokers(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("kaas-{i}")).collect()
    }

    // --- gh #249: registered vs alive vs draining ------------------

    fn health_of(entries: &[BrokerAssignment], id: &str) -> BrokerHealth {
        entries
            .iter()
            .find(|e| e.id == id)
            .map(|e| e.health)
            .expect("broker present in the reported list")
    }

    #[test]
    fn registered_but_not_alive_is_reported_dead_not_dropped() {
        let entries = build_broker_entries(
            &brokers(3),
            &["kaas-0".to_owned(), "kaas-1".to_owned()],
            &[],
            "now",
            None,
        );
        assert_eq!(entries.len(), 3, "a dead broker keeps its row");
        assert_eq!(health_of(&entries, "kaas-0"), BrokerHealth::Alive);
        assert_eq!(health_of(&entries, "kaas-1"), BrokerHealth::Alive);
        assert_eq!(health_of(&entries, "kaas-2"), BrokerHealth::Dead);
    }

    #[test]
    fn draining_outranks_alive_in_the_reported_health() {
        // The source is expected to have already dropped kaas-1 from
        // the alive set; if it hasn't, `draining` still wins, because
        // that is the more specific statement.
        let entries = build_broker_entries(
            &brokers(2),
            &["kaas-0".to_owned(), "kaas-1".to_owned()],
            &["kaas-1".to_owned()],
            "now",
            None,
        );
        assert_eq!(health_of(&entries, "kaas-1"), BrokerHealth::Draining);
    }

    /// The reason this matters beyond reporting: `group_hash` documents
    /// that the coordinator divisor MUST be the full broker set,
    /// "including draining / dead". Before gh #249 the list was the
    /// alive set, so losing one broker of three silently changed the
    /// divisor 3 → 2 and rehashed ~2/3 of all group and txn
    /// coordinators — at exactly the moment the cluster was degraded.
    #[test]
    fn broker_loss_does_not_shrink_the_coordinator_divisor() {
        let all = brokers(3);
        let healthy = build_broker_entries(&all, &all, &[], "now", None);
        let degraded = build_broker_entries(
            &all,
            &["kaas-0".to_owned(), "kaas-1".to_owned()],
            &[],
            "now",
            None,
        );
        assert_eq!(healthy.len(), degraded.len());

        // And the hash routing agrees: same preferred slot either way.
        let (names_h, alive_h) = Assignment {
            controller_epoch: 0,
            assignment_version: 1,
            generated_at: String::new(),
            controller: "kaas-0".to_owned(),
            brokers: healthy,
            partitions: vec![],
            consumer_groups: vec![],
        }
        .broker_sets();
        let (names_d, alive_d) = Assignment {
            controller_epoch: 0,
            assignment_version: 1,
            generated_at: String::new(),
            controller: "kaas-0".to_owned(),
            brokers: degraded,
            partitions: vec![],
            consumer_groups: vec![],
        }
        .broker_sets();
        assert_eq!(names_h, names_d, "divisor set is stable across the loss");
        assert_eq!(alive_h.len(), 3);
        assert_eq!(alive_d.values().filter(|v| **v).count(), 2);
    }

    /// A fenced broker's `last_seen` should say when it was last
    /// alive, not when the controller last recomputed — otherwise the
    /// field says "seen just now" about a broker that is gone.
    #[test]
    fn dead_broker_keeps_its_last_seen() {
        let prev = build_broker_entries(&brokers(2), &brokers(2), &[], "T1", None);
        let now = build_broker_entries(&brokers(2), &["kaas-0".to_owned()], &[], "T2", Some(&prev));
        assert_eq!(
            now.iter().find(|e| e.id == "kaas-1").unwrap().last_seen,
            "T1"
        );
        assert_eq!(
            now.iter().find(|e| e.id == "kaas-0").unwrap().last_seen,
            "T2"
        );
    }

    /// Transient: heartbeat-connected before the EndpointSlice entry
    /// lands. The broker is being assigned partitions, so it must
    /// appear in the list it is assigned within.
    #[test]
    fn alive_but_unregistered_broker_is_still_listed() {
        let entries = build_broker_entries(
            &["kaas-0".to_owned()],
            &["kaas-0".to_owned(), "kaas-1".to_owned()],
            &[],
            "now",
            None,
        );
        assert_eq!(entries.len(), 2);
        assert_eq!(health_of(&entries, "kaas-1"), BrokerHealth::Alive);
    }

    fn loop_with_brokers(
        dir: &std::path::Path,
        brokers_n: usize,
        topics_n: i32,
    ) -> Arc<AssignmentLoop<StaticSources, StaticSources, StaticSources>> {
        let s = Arc::new(StaticSources {
            topics: topics(topics_n),
            brokers: brokers(brokers_n),
            groups: vec!["g1".to_owned()],
        });
        let l = AssignmentLoop::new(dir, "kaas-0", s.clone(), s.clone());
        l.with_group_source(s)
    }

    #[tokio::test]
    async fn initial_recompute_writes_a_valid_file() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 2);
        let v = l.start(7).await.unwrap();
        assert_eq!(v, 1, "first write is version 1");
        let snap = l.snapshot().expect("snapshot present after start");
        assert_eq!(snap.controller_epoch, 7);
        assert_eq!(snap.controller, "kaas-0");
        assert_eq!(snap.partitions.len(), 6);
        assert_eq!(snap.consumer_groups.len(), 1);
        let path = tmp.path().join("__cluster").join(Assignment::FILE_NAME);
        let on_disk: Assignment = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(on_disk, snap);
    }

    #[tokio::test]
    async fn version_increments_on_each_update() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 2);
        let v1 = l.start(1).await.unwrap();
        let v2 = l
            .update_assignment(AssignmentReason::TopicResized)
            .await
            .unwrap();
        let v3 = l
            .update_assignment(AssignmentReason::BrokerJoined)
            .await
            .unwrap();
        assert!(v3 > v2 && v2 > v1);
    }

    #[tokio::test]
    async fn bootstraps_version_counter_from_existing_file() {
        let tmp = tempfile::tempdir().unwrap();
        // Write a fake prior assignment at version 42.
        let dir = tmp.path().join("__cluster");
        std::fs::create_dir_all(&dir).unwrap();
        let prior = Assignment {
            controller_epoch: 9,
            assignment_version: 42,
            generated_at: "2024-12-31T23:59:59Z".to_owned(),
            controller: "kaas-old".to_owned(),
            brokers: vec![],
            partitions: vec![],
            consumer_groups: vec![],
        };
        std::fs::write(
            dir.join(Assignment::FILE_NAME),
            serde_json::to_vec(&prior).unwrap(),
        )
        .unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        // Start as a fresh controller at epoch 10 — version must
        // bump beyond 42.
        let v = l.start(10).await.unwrap();
        assert_eq!(v, 43);
    }

    #[tokio::test]
    async fn rename_atomicity_no_tmp_leftover() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        l.start(0).await.unwrap();
        let dir = tmp.path().join("__cluster");
        assert!(dir.join(Assignment::FILE_NAME).exists());
        let tmp_path = dir.join(format!("{}.tmp", Assignment::FILE_NAME));
        assert!(
            !tmp_path.exists(),
            "tmp file must not survive a clean write"
        );
    }

    #[tokio::test]
    async fn snapshot_is_a_clone_not_a_reference_to_state() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 3, 1);
        l.start(0).await.unwrap();
        let snap = l.snapshot().unwrap();
        let _v = l
            .update_assignment(AssignmentReason::BrokerJoined)
            .await
            .unwrap();
        let snap2 = l.snapshot().unwrap();
        assert!(snap2.assignment_version > snap.assignment_version);
    }

    #[tokio::test]
    async fn no_brokers_yields_empty_partitions() {
        let tmp = tempfile::tempdir().unwrap();
        let l = loop_with_brokers(tmp.path(), 0, 2);
        let _v = l.start(0).await.unwrap();
        let snap = l.snapshot().unwrap();
        assert!(snap.partitions.is_empty());
        assert!(snap.consumer_groups.is_empty());
    }
}
