//! `DiskStorageEngine` — the production [`StorageEngine`] impl.
//!
//! Thin glue over [`crate::partition::Partition`]. Holds a
//! `DashMap<(String, i32), Arc<Partition>>` and routes
//! [`StorageEngine`] method calls to the relevant partition.
//!
//! Phase 2 follow-up commits on gh #157 will extend this with: the
//! topic-config hot-reload watcher (per-partition `.config.json`),
//! `OffsetForTimestamp` / `OffsetForLeaderEpoch` semantics richer than
//! the current sentinels, and the `ReadSegmentRef`-based splice path
//! that the Phase 3 Fetch handler will use for sendfile.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use dashmap::DashMap;
use parking_lot::RwLock;

use crate::engine::{
    LogDirInfo, PlacementResolver, StorageEngine, TopicIdentityResolver, TopicIncarnation,
};
use crate::errors::StorageError;
use crate::fs::Fs;
use crate::partition::{Partition, PartitionConfig};

pub struct DiskStorageEngine {
    fs: Arc<dyn Fs>,
    data_dir: PathBuf,
    /// gh #221 phase 2: additional named log dirs (pool members)
    /// beyond the default `data_dir`. Kafka vocabulary — one pool
    /// volume = one log dir (KIP-113 shape). Empty in the classic
    /// single-volume layout.
    extra_log_dirs: Vec<LogDirInfo>,
    /// Resolves `(topic, partition)` → log-dir *name*. `None` (or a
    /// resolver miss, or an unknown name) → the default log dir, so
    /// the resolver can never make a partition unopenable — worst
    /// case it lands in the legacy location.
    placement: RwLock<Option<Arc<dyn PlacementResolver>>>,
    /// Resolves a topic name → the incarnation it should be at
    /// (gh #241). `None` (no resolver installed) means every topic is
    /// [`TopicIncarnation::Unknown`], i.e. the check is inert — which
    /// is what dev mode and the unit tests want.
    identity: RwLock<Option<Arc<dyn TopicIdentityResolver>>>,
    cfg: PartitionConfig,
    partitions: DashMap<(String, i32), Arc<Partition>>,
    /// Partitions mid-move between log dirs (gh #221 phase 3).
    /// `ensure_open` refuses them so a produce/fetch can't reopen the
    /// source dir while the copy runs.
    migrating: DashMap<(String, i32), ()>,
}

/// Where [`DiskStorageEngine::locate`] found a partition's data.
struct Located {
    /// The root that holds the partition, or will hold it if it is new.
    root: PathBuf,
    /// The root the placement record names, when the broker holds a
    /// record and the data is somewhere else (gh #234).
    placed_elsewhere: Option<PathBuf>,
}

impl std::fmt::Debug for DiskStorageEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiskStorageEngine")
            .field("data_dir", &self.data_dir)
            .field("extra_log_dirs", &self.extra_log_dirs.len())
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl DiskStorageEngine {
    /// Construct a new engine rooted at `data_dir`. The directory is
    /// created on first partition open; no I/O happens here.
    pub fn new(fs: Arc<dyn Fs>, data_dir: PathBuf, cfg: PartitionConfig) -> Self {
        Self {
            fs,
            data_dir,
            extra_log_dirs: Vec::new(),
            placement: RwLock::new(None),
            identity: RwLock::new(None),
            cfg,
            partitions: DashMap::new(),
            migrating: DashMap::new(),
        }
    }

    /// Add named pool log dirs (gh #221 phase 2). Call before the
    /// engine is shared; entries named `default` are ignored (that
    /// name is reserved for `data_dir`).
    pub fn with_extra_log_dirs(mut self, dirs: Vec<LogDirInfo>) -> Self {
        self.extra_log_dirs = dirs
            .into_iter()
            .filter(|d| d.name != crate::engine::DEFAULT_LOG_DIR_NAME)
            .collect();
        self
    }

    /// Install the placement source (the broker wires the KafkaTopic
    /// registry here). Swappable at runtime — resolution happens on
    /// partition open/create, not per record.
    pub fn set_placement_resolver(&self, r: Arc<dyn PlacementResolver>) {
        *self.placement.write() = Some(r);
    }

    /// Install the topic-incarnation source (gh #241 — the broker wires
    /// the KafkaTopic registry here, same as placement). Without one
    /// the identity gate is inert.
    pub fn set_identity_resolver(&self, r: Arc<dyn TopicIdentityResolver>) {
        *self.identity.write() = Some(r);
    }

    /// gh #241: may this broker open `topic`'s directory yet?
    ///
    /// The operator reclaims a recreated topic's directory by renaming
    /// it aside (`stage_topic_dir`). That frees the live path atomically
    /// for a broker about to open it (gh #203/#220) but does nothing for
    /// one that already *has* it open — FDs follow the renamed inode, so
    /// appends keep landing in the `.deleting-*` copy while the live
    /// path serves an empty log, silently, until the copy is deleted.
    /// The cure is to not open it in the first place: a stamped-but-not-
    /// ours directory is one the operator has yet to reclaim.
    ///
    /// The `Unknown` arm is what keeps brokers runtime-independent of
    /// the operator: no CR knowledge (unreachable API server, env-var
    /// seeding, dev mode) means adopt, never block. An *unstamped*
    /// directory is likewise always adopted — matching the operator's
    /// own rule that unknown identity is never destructive.
    fn identity_permits_open(&self, topic: &str, topic_dir: &Path) -> Result<(), StorageError> {
        let expected = match self.identity.read().as_ref() {
            None => return Ok(()),
            Some(r) => r.incarnation_of(topic),
        };
        if matches!(expected, TopicIncarnation::Unknown) {
            return Ok(());
        }
        // A read error is "unknown", never "stale" — an unreadable stamp
        // must not wedge a partition.
        let stamped = match crate::topic_identity::read_topic_identity(self.fs.as_ref(), topic_dir)
        {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };
        match (expected, stamped) {
            (_, None) => Ok(()),
            (TopicIncarnation::Known(want), Some(have)) if have.topic_id == want => Ok(()),
            _ => Err(StorageError::StaleTopicDir),
        }
    }

    /// The log-dir name the placement record gives `(topic, partition)`.
    /// `None` means this broker holds no record for it yet, which is not
    /// the same as the default root: a recorded default placement is
    /// `Some("default")`.
    fn placement_of(&self, topic: &str, partition: i32) -> Option<String> {
        self.placement
            .read()
            .as_ref()
            .and_then(|r| r.log_dir_of(topic, partition))
    }

    /// Root directory of the log dir called `name`. Falls back to
    /// `data_dir` on any miss.
    fn root_named(&self, name: Option<&str>) -> PathBuf {
        name.and_then(|n| self.extra_log_dirs.iter().find(|d| d.name == n))
            .map(|d| d.path.clone())
            .unwrap_or_else(|| self.data_dir.clone())
    }

    /// Root directory hosting `(topic, partition)` under the current
    /// placement. Falls back to `data_dir` on any miss.
    fn log_dir_root(&self, topic: &str, partition: i32) -> PathBuf {
        self.root_named(self.placement_of(topic, partition).as_deref())
    }

    /// Directory hosting `(topic, partition)`'s data — **where the data
    /// actually is**, not merely where the current placement record says
    /// it should be (gh #234).
    ///
    /// Placement is eventually-consistent: a topic's partitions are
    /// routinely opened *before* the operator writes
    /// `status.volumeAssignments` (the CR-create event triggers the
    /// takeover; the status lands moments later), and pre-fix the data
    /// landed on the default root while the next restart re-derived the
    /// assigned pool root, found it empty, and created fresh 0-byte
    /// segments there — half of `kperf-bench` went invisible exactly
    /// this way. The cure is precedence, mirroring the gh #219 rule
    /// that on-disk reality outranks derived state:
    ///
    /// 1. The assigned root's dir has a `manifest.json` → assigned root
    ///    (the normal case, and the post-migration case — a log-dir
    ///    move copies the manifest to the target before the placement
    ///    record flips).
    /// 2. Otherwise, a *different* known root already holds a manifest
    ///    for this partition → that root wins. Data never moves
    ///    because a status record showed up late; only the explicit
    ///    migration path (`move_partition_to_log_dir`) relocates bytes.
    ///    This warns only when the broker holds a placement record and
    ///    it names another root. Without a record there is nothing to
    ///    contradict, and that is every boot: the takeover runs off
    ///    `assignment.json` before the topic watch has delivered
    ///    `status.volumeAssignments`, so the fallback is the default
    ///    root, and every pool-placed partition warned on every restart
    ///    (gh #271).
    /// 3. No manifest anywhere → assigned root (fresh create).
    ///
    /// When several foreign roots hold manifests (shouldn't happen —
    /// a crashed migration leaves the record pointing at the source),
    /// the default root wins if present, else the first pool root by
    /// declaration order: deterministic beats clever here.
    fn partition_dir(&self, topic: &str, partition: i32) -> PathBuf {
        let found = self.locate(topic, partition);
        if let Some(assigned) = &found.placed_elsewhere {
            tracing::warn!(
                topic,
                partition,
                assigned = %assigned.display(),
                actual = %found.root.display(),
                "partition data lives outside its assigned log dir; \
                 serving from where the data is (gh #234) — use the \
                 migration path to relocate it"
            );
        }
        found.root.join(topic).join(partition.to_string())
    }

    /// The decision behind [`Self::partition_dir`], without the log line.
    fn locate(&self, topic: &str, partition: i32) -> Located {
        let placed = self.placement_of(topic, partition);
        let assigned_root = self.root_named(placed.as_deref());
        let has_manifest = |root: &Path| {
            let manifest = root
                .join(topic)
                .join(partition.to_string())
                .join("manifest.json");
            self.fs.stat(&manifest).is_ok()
        };

        if has_manifest(&assigned_root) {
            return Located {
                root: assigned_root,
                placed_elsewhere: None,
            };
        }
        let adopted = std::iter::once(self.data_dir.clone())
            .chain(self.extra_log_dirs.iter().map(|d| d.path.clone()))
            .filter(|root| *root != assigned_root)
            .find(|root| has_manifest(root));
        match adopted {
            Some(root) => Located {
                root,
                placed_elsewhere: placed.map(|_| assigned_root),
            },
            None => Located {
                root: assigned_root,
                placed_elsewhere: None,
            },
        }
    }

    /// Directory holding `topic`'s topic-level files (`.config.json`,
    /// `.topic-id.json`) for the root that hosts `partition`. A topic
    /// spanning several volume-pool roots has one such directory per
    /// involved root, which is why this takes a partition.
    pub fn topic_dir(&self, topic: &str, partition: i32) -> PathBuf {
        self.log_dir_root(topic, partition).join(topic)
    }

    /// The filesystem this engine reads and writes through.
    pub fn fs(&self) -> &dyn Fs {
        self.fs.as_ref()
    }

    /// Engine defaults with `(topic, partition)`'s `.config.json`
    /// layered on top.
    ///
    /// Called on every partition open so a topic's `segment.bytes` /
    /// `segment.ms` actually reach the log. Before this, the engine
    /// built one `PartitionConfig` at boot and handed the same copy to
    /// every partition: a topic could ask for 16 MiB segments, have it
    /// written to `.config.json` and echoed back by DescribeConfigs,
    /// and still roll at the global 1 GiB — which in turn meant a
    /// low-volume topic had no closed segments and `retention.ms`
    /// could never delete anything.
    pub fn partition_config_for(&self, topic: &str, partition: i32) -> PartitionConfig {
        let dir = self.topic_dir(topic, partition);
        match crate::topicconfig::read_topic_config(self.fs(), &dir) {
            Ok(Some(c)) => crate::topicconfig::apply_to_partition_config(&c, &self.cfg),
            // Absent or unreadable — engine defaults, which is what
            // every partition got before this existed.
            _ => self.cfg.clone(),
        }
    }

    /// Get-or-open the partition. The race window between "no entry"
    /// and "insert" is resolved by re-checking after the open — the
    /// loser drops its partition (the active-segment FDs close in
    /// Drop, and the committer task exits when the channel sender
    /// drops). Theoretical contention only; both partitions just
    /// opened their handles, neither has written yet.
    async fn ensure_open(
        &self,
        topic: &str,
        partition: i32,
    ) -> Result<Arc<Partition>, StorageError> {
        let key = (topic.to_owned(), partition);
        if self.migrating.contains_key(&key) {
            return Err(StorageError::Migrating);
        }
        if let Some(entry) = self.partitions.get(&key) {
            return Ok(entry.value().clone());
        }
        let dir = self.partition_dir(topic, partition);
        // gh #241: refuse before `Partition::open` creates anything —
        // its prologue mkdir_all's the path, which would re-materialise
        // a directory the operator is midway through reclaiming.
        if let Some(topic_dir) = dir.parent() {
            self.identity_permits_open(topic, topic_dir)?;
        }
        let p = Arc::new(
            Partition::open(
                self.fs.clone(),
                topic.to_owned(),
                partition,
                dir,
                self.partition_config_for(topic, partition),
            )
            .await?,
        );
        match self.partitions.entry(key) {
            dashmap::Entry::Occupied(e) => Ok(e.get().clone()),
            dashmap::Entry::Vacant(e) => {
                e.insert(p.clone());
                Ok(p)
            }
        }
    }

    /// Snapshot the currently-open partition keys. Lets the retention
    /// cleaner iterate without holding DashMap shards across await
    /// points.
    pub fn iter_partition_keys(&self) -> Vec<(String, i32)> {
        self.partitions.iter().map(|kv| kv.key().clone()).collect()
    }

    /// Look up a currently-open [`Partition`] by `(topic, partition)`.
    /// Returns `None` if the partition is not open in this engine
    /// (the cleaner / compactor skip it).
    pub fn partition(&self, topic: &str, partition: i32) -> Option<Arc<Partition>> {
        self.partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().clone())
    }

    /// Close every open partition (drain committers, persist state,
    /// release FDs). Used by the SIGTERM drain path (gh #61 + gh #139).
    pub async fn relinquish_all(&self) -> Result<(), StorageError> {
        // Drain into a vec so we don't hold dashmap shards across
        // the async close. clone the Arc's; remove from map.
        let entries: Vec<((String, i32), Arc<Partition>)> = self
            .partitions
            .iter()
            .map(|kv| (kv.key().clone(), kv.value().clone()))
            .collect();
        for (key, p) in entries {
            let _ = p.close().await;
            self.partitions.remove(&key);
        }
        Ok(())
    }
}

#[async_trait]
impl StorageEngine for DiskStorageEngine {
    async fn append(
        &self,
        topic: &str,
        partition: i32,
        epoch: u32,
        acks: i16,
        batch: Bytes,
    ) -> Result<i64, StorageError> {
        let started = std::time::Instant::now();
        let batch_len = batch.len();
        // gh #218: derive the record count from the batch header
        // (`lastOffsetDelta + 1`) before the batch is consumed. This is
        // byte-opacity-safe — it reads header metadata, not record
        // contents — and matches the offset accounting `Partition::append`
        // does for the high-watermark, so the metric tracks the same
        // records the log commits. A header too short to parse falls back
        // to 1 (never a real produce batch).
        let record_count = crate::segment::parse_batch_offsets(&batch)
            .map(|(_base, last_offset_delta, _ts)| i64::from(last_offset_delta) + 1)
            .unwrap_or(1);
        let p = self.ensure_open(topic, partition).await?;
        let out = p.append(epoch, acks, batch).await;
        let m = kaas_observability::metrics::global();
        m.write_latency.record(started.elapsed().as_secs_f64(), &[]);
        // Per-topic Produce accounting: real record count + real bytes.
        if out.is_ok() {
            m.topic_traffic.record_produce(
                topic,
                record_count,
                i64::try_from(batch_len).unwrap_or(0),
            );
        }
        out
    }

    async fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError> {
        let started = std::time::Instant::now();
        let key = (topic.to_owned(), partition);
        // Read on an unknown partition returns empty (matches
        // MemoryStorage — clients receive an empty Fetch response, not
        // an error). The Arc is cloned out so no map guard is held
        // across the read.
        let Some(p) = self.partitions.get(&key).map(|e| Arc::clone(e.value())) else {
            return Ok(Bytes::new());
        };
        let out = p.read(start_offset, max_bytes).await;
        let m = kaas_observability::metrics::global();
        m.read_latency.record(started.elapsed().as_secs_f64(), &[]);
        // Count only reads of a partition this engine still holds. A
        // topic delete abandons its partitions before it drops the
        // topic's traffic series (gh #261), so a read that straddles the
        // delete must not record — that would re-create the series the
        // delete just dropped. An empty read of a held partition still
        // records, keeping an idle topic's series alive (gh #115).
        if let Ok(bytes) = &out {
            if self.partitions.contains_key(&key) {
                m.topic_traffic.record_fetch(
                    topic,
                    crate::segment::count_records(bytes),
                    i64::try_from(bytes.len()).unwrap_or(0),
                );
            }
        }
        out
    }

    fn partition_epoch(&self, topic: &str, partition: i32) -> Option<i64> {
        self.partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().epoch())
    }

    fn topic_config(&self, topic: &str) -> Option<crate::topicconfig::TopicConfigFile> {
        // Partition 0's root: the operator writes `.config.json` to
        // every root the topic touches, and partition 0's is always
        // one of them.
        let dir = self.topic_dir(topic, 0);
        crate::topicconfig::read_topic_config(self.fs(), &dir)
            .ok()
            .flatten()
    }

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        Ok(self
            .partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().high_watermark())
            .unwrap_or(0))
    }

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        Ok(self
            .partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().log_start_offset())
            .unwrap_or(0))
    }

    fn offset_for_timestamp(
        &self,
        _topic: &str,
        _partition: i32,
        _timestamp_ms: i64,
    ) -> Result<(i64, i64), StorageError> {
        // Phase 2 ships the sentinel; segment-level max_timestamp
        // tracking is in place (gh #5) but the index that maps
        // timestamps to offsets is a Phase 5 follow-up.
        Ok((-1, -1))
    }

    fn offset_for_leader_epoch(
        &self,
        _topic: &str,
        _partition: i32,
        _leader_epoch: i32,
    ) -> Result<(i32, i64), StorageError> {
        // KIP-101 lookup is Phase 5 follow-up; the segment epochs
        // are recorded but the leader-epoch cache isn't materialised
        // yet.
        Ok((-1, -1))
    }

    async fn delete_records(
        &self,
        topic: &str,
        partition: i32,
        target_offset: i64,
    ) -> Result<i64, StorageError> {
        let p = self.ensure_open(topic, partition).await?;
        p.delete_records(target_offset).await
    }

    fn fence_producer_epoch(&self, pid: i64, new_epoch: i16) {
        // Walk every open partition. Each Partition::fence_producer
        // takes its own inner mutex; the DashMap iter holds shard
        // read locks for the duration, which is fine because
        // fence_producer doesn't recursively touch the partitions
        // map. Partitions that aren't currently open on this broker
        // pick up the fence on their next take_over via the
        // producer-state snapshot path (gh #12).
        for kv in self.partitions.iter() {
            kv.value().fence_producer(pid, new_epoch);
        }
    }

    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(p) => Ok(p.last_stable_offset()),
            None => self.high_watermark(topic, partition),
        }
    }

    fn aborted_transactions_in_range(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        end_offset: i64,
    ) -> Vec<crate::txn_index::AbortedTxn> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(p) => p.aborted_in_range(start_offset, end_offset),
            None => Vec::new(),
        }
    }

    async fn create_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        // Opening is creation — Partition::open creates the dir.
        self.ensure_open(topic, partition).await?;
        Ok(())
    }

    async fn delete_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        let key = (topic.to_owned(), partition);
        if let Some((_, p)) = self.partitions.remove(&key) {
            let _ = p.close().await;
        }
        // Best-effort directory removal — leave the dir if cleanup
        // fails (segment files survive on NFS silly-rename).
        let dir = self.partition_dir(topic, partition);
        for path in self.fs.readdir(&dir).unwrap_or_default() {
            let _ = self.fs.remove(&path);
        }
        Ok(())
    }

    fn partition_size(&self, topic: &str, partition: i32) -> i64 {
        self.partitions
            .get(&(topic.to_owned(), partition))
            .map(|e| e.value().partition_size())
            .unwrap_or(0)
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn log_dirs(&self) -> Vec<LogDirInfo> {
        let mut dirs = vec![LogDirInfo {
            name: crate::engine::DEFAULT_LOG_DIR_NAME.to_owned(),
            path: self.data_dir.clone(),
            default_eligible: true,
            cordoned: false,
            labels: Default::default(),
        }];
        dirs.extend(self.extra_log_dirs.iter().cloned());
        dirs
    }

    fn partition_log_dir(&self, topic: &str, partition: i32) -> String {
        match self.placement_of(topic, partition) {
            // An unknown name resolves to the default root (see
            // `log_dir_root`), so report it as such.
            Some(n) if self.extra_log_dirs.iter().any(|d| d.name == n) => n,
            _ => crate::engine::DEFAULT_LOG_DIR_NAME.to_owned(),
        }
    }

    async fn take_over(
        &self,
        topic: &str,
        partition: i32,
        epoch: u32,
    ) -> Result<i64, StorageError> {
        // `open` recovers at whatever epoch the manifest carries;
        // `Partition::take_over` raises it to the assignment's and rolls
        // to a fresh epoch-stamped segment, which is what fences the
        // previous leader (gh #227). Idempotent at or below the current
        // epoch, so the gh #215 reconcile can re-drive it freely.
        let p = self.ensure_open(topic, partition).await?;
        p.take_over(epoch).await
    }

    async fn move_partition_to_log_dir(
        &self,
        topic: &str,
        partition: i32,
        log_dir: &str,
    ) -> Result<PathBuf, StorageError> {
        let target_root = if log_dir == crate::engine::DEFAULT_LOG_DIR_NAME {
            self.data_dir.clone()
        } else {
            self.extra_log_dirs
                .iter()
                .find(|d| d.name == log_dir)
                .map(|d| d.path.clone())
                .ok_or(StorageError::Unsupported("unknown target log dir"))?
        };
        let key = (topic.to_owned(), partition);
        let src_dir = self.partition_dir(topic, partition);
        let dst_dir = target_root.join(topic).join(partition.to_string());
        if src_dir == dst_dir {
            return Ok(src_dir);
        }
        // Single mover per partition; a concurrent request sees the
        // same retriable error producers do.
        if self.migrating.insert(key.clone(), ()).is_some() {
            return Err(StorageError::Migrating);
        }
        let moved: Result<(), StorageError> = async {
            // Close (persist manifest + producer snapshot, drop FDs)
            // exactly like a relinquish — the copy must see quiesced
            // files.
            if let Some((_, p)) = self.partitions.remove(&key) {
                let _ = p.close().await;
            }
            let src = src_dir.clone();
            let dst = dst_dir.clone();
            tokio::task::spawn_blocking(move || copy_dir_fresh(&src, &dst))
                .await
                .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;
            Ok(())
        }
        .await;
        self.migrating.remove(&key);
        moved?;
        Ok(src_dir)
    }

    async fn relinquish(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        let key = (topic.to_owned(), partition);
        if let Some((_, p)) = self.partitions.remove(&key) {
            let _ = p.close().await;
        }
        Ok(())
    }

    async fn abandon_topic(&self, topic: &str) -> usize {
        // Collect first: don't hold DashMap shards across the await.
        let keys: Vec<(String, i32)> = self
            .partitions
            .iter()
            .map(|kv| kv.key().clone())
            .filter(|(t, _)| t == topic)
            .collect();
        let mut dropped = 0;
        for key in keys {
            if let Some((_, p)) = self.partitions.remove(&key) {
                p.abandon().await;
                dropped += 1;
            }
        }
        dropped
    }

    fn open_partition_keys(&self) -> Vec<(String, i32)> {
        self.iter_partition_keys()
    }

    fn stalled_partitions(&self) -> usize {
        self.partitions
            .iter()
            .filter(|kv| kv.value().is_stalled())
            .count()
    }

    async fn drain(&self) -> Result<(), StorageError> {
        self.relinquish_all().await
    }
}

/// Fresh recursive copy for a log-dir move: any half-copied target
/// from a previous crashed attempt is discarded first, so the copy is
/// all-or-nothing from the placement record's point of view (the
/// record only flips after this returns Ok — NFS rule 2: the compound
/// op is idempotent and re-drivable).
/// Copy a partition directory to its new root. Returns only once the
/// copy is durable (gh #231): the caller flips the placement record and
/// then deletes the source, so a copy still in the page cache — ext4's
/// delayed allocation writes a fresh file's blocks seconds later — would
/// let a crash in that window lose the partition on both roots.
fn copy_dir_fresh(src: &Path, dst: &Path) -> Result<(), StorageError> {
    if dst.exists() {
        std::fs::remove_dir_all(dst)?;
    }
    if !src.exists() {
        // Empty partition (never opened): nothing to copy; the target
        // dir is created so the opener finds a valid home.
        create_dir_all_durable(dst)?;
        return Ok(());
    }
    create_dir_all_durable(dst)?;
    copy_dir_recursive(src, dst)?;
    Ok(())
}

/// Copy `src` into the existing directory `dst`, fsyncing every file and,
/// once its entries are written, `dst` itself.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    for entry in std::fs::read_dir(src)?.flatten() {
        let ty = entry.file_type()?;
        let to = dst.join(entry.file_name());
        if ty.is_dir() {
            std::fs::create_dir(&to)?;
            copy_dir_recursive(&entry.path(), &to)?;
        } else if ty.is_file() {
            std::fs::copy(entry.path(), &to)?;
            crate::fs::fsync_path(&to)?;
        }
    }
    crate::fs::fsync_dir(dst)
}

/// `create_dir_all`, then fsync the parent of every directory it had to
/// create, so the new entries survive a crash on a local filesystem.
fn create_dir_all_durable(dir: &Path) -> std::io::Result<()> {
    let mut missing = Vec::new();
    let mut cur = Some(dir);
    while let Some(d) = cur.filter(|d| !d.exists()) {
        missing.push(d.to_path_buf());
        cur = d.parent();
    }
    std::fs::create_dir_all(dir)?;
    for d in &missing {
        if let Some(parent) = d.parent() {
            crate::fs::fsync_dir(parent)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use crate::memory::MemoryStorage;
    use crate::partition::PartitionConfig;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    /// gh #231: the migration copy reproduces nested contents under a
    /// target whose ancestors don't exist yet (a topic's first partition
    /// on a new pool root), and leaves the source alone.
    #[test]
    fn copy_dir_fresh_copies_nested_contents_under_new_ancestors() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("default/t/0");
        std::fs::create_dir_all(src.join("sub")).unwrap();
        std::fs::write(src.join("manifest.json"), b"{}").unwrap();
        std::fs::write(src.join("sub/x.log"), b"abc").unwrap();
        let dst = tmp.path().join("fast/t/0");
        copy_dir_fresh(&src, &dst).unwrap();
        assert_eq!(std::fs::read(dst.join("manifest.json")).unwrap(), b"{}");
        assert_eq!(std::fs::read(dst.join("sub/x.log")).unwrap(), b"abc");
        assert!(src.join("sub/x.log").exists());
    }

    /// Build a v2 batch carrying (num_records). Producer ID = -1
    /// (non-idempotent) so the engine doesn't gate on a missing PID.
    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    #[test]
    fn fresh_engine_unknown_partition_returns_empty() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Read before any append on an unknown partition.
            let got = e.read("nope", 0, 0, 4096).await.unwrap();
            assert!(got.is_empty());
            assert_eq!(e.high_watermark("nope", 0).unwrap(), 0);
        });
    }

    #[test]
    fn append_then_read_byte_identical() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(2, 1_000))
                    .await
                    .unwrap();
            }
            assert_eq!(e.high_watermark("t", 0).unwrap(), 8);
            let got = e.read("t", 0, 0, 4096).await.unwrap();
            // Each batch contributes its full bytes; expect 4 batches.
            let one_len = build_batch(2, 1_000).len();
            assert_eq!(got.len(), one_len * 4);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn data_dir_returns_configured_path() {
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let e = DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
        assert_eq!(e.data_dir(), tmp.path());
    }

    /// Cross-engine equivalence: identical input sequence produces
    /// identical bytes on `read`. The broker-owns-offsets rewrite
    /// happens identically in both impls.
    #[test]
    fn memory_and_disk_produce_byte_equal_reads() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let mem = MemoryStorage::new();
            let disk =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());

            let sequence = [
                build_batch(3, 1_000),
                build_batch(1, 2_000),
                build_batch(5, 3_000),
                build_batch(2, 4_000),
            ];
            for b in &sequence {
                mem.append("t", 0, 0, -1, b.clone()).await.unwrap();
                disk.append("t", 0, 0, -1, b.clone()).await.unwrap();
            }

            assert_eq!(
                mem.high_watermark("t", 0).unwrap(),
                disk.high_watermark("t", 0).unwrap()
            );
            let mem_bytes = mem.read("t", 0, 0, 1 << 16).await.unwrap();
            let disk_bytes = disk.read("t", 0, 0, 1 << 16).await.unwrap();
            assert_eq!(
                mem_bytes, disk_bytes,
                "MemoryStorage and DiskStorageEngine bytes diverged"
            );
            disk.relinquish_all().await.unwrap();
        });
    }

    /// gh #218: the per-topic produce metric must count real records
    /// (derived from the batch header), not 1 per batch (appends).
    #[test]
    fn produce_records_metric_counts_records_not_batches() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Unique topic so the process-global counter isn't shared
            // with other tests appending to "t".
            let topic = "gh218-records-metric";
            let tt = kaas_observability::metrics::global().topic_traffic.clone();
            let before = tt.produce_records(topic);
            // Three appends carrying 3 + 5 + 1 = 9 records. The old code
            // counted 3 (one per batch); the fix must count 9.
            e.append(topic, 0, 0, -1, build_batch(3, 1_000))
                .await
                .unwrap();
            e.append(topic, 0, 0, -1, build_batch(5, 2_000))
                .await
                .unwrap();
            e.append(topic, 0, 0, -1, build_batch(1, 3_000))
                .await
                .unwrap();
            assert_eq!(
                tt.produce_records(topic) - before,
                9,
                "metric must count 9 records across 3 batches, not 3 batches"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    /// gh #261 follow-up: the fetch metric counts records, not reads, and
    /// a read of a partition the engine no longer holds — an abandoned
    /// topic — records nothing, so it can't re-create the traffic series
    /// a topic delete just dropped.
    #[test]
    fn fetch_metric_counts_records_and_skips_partitions_not_held() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Unique topic: the meter is process-global.
            let topic = "gh261-fetch-metric";
            let tt = kaas_observability::metrics::global().topic_traffic.clone();
            e.append(topic, 0, 0, -1, build_batch(3, 1_000))
                .await
                .unwrap();
            e.append(topic, 0, 0, -1, build_batch(4, 2_000))
                .await
                .unwrap();
            let before = tt.fetch_records(topic);
            e.read(topic, 0, 0, 1 << 20).await.unwrap();
            assert_eq!(tt.fetch_records(topic) - before, 7, "7 records, not 1 read");

            assert_eq!(e.abandon_topic(topic).await, 1);
            tt.forget(topic);
            assert!(e.read(topic, 0, 0, 1 << 20).await.unwrap().is_empty());
            assert!(
                !tt.tracks(topic),
                "a read after the abandon must not re-create the series"
            );
        });
    }

    /// Reopen test: state survives engine restart via manifest +
    /// scan_high_watermark recovery.
    #[test]
    fn reopen_recovers_state() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            {
                let e = DiskStorageEngine::new(
                    fs.clone(),
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                );
                for _ in 0..3 {
                    e.append("t", 0, 0, -1, build_batch(2, 1_000))
                        .await
                        .unwrap();
                }
                e.delete_records("t", 0, 2).await.unwrap();
                e.relinquish_all().await.unwrap();
            }
            // Reopen — HWM + log_start recovered from manifest.
            let e2 =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            // Force the partition to open.
            let _ = e2.take_over("t", 0, 0).await.unwrap();
            assert_eq!(e2.high_watermark("t", 0).unwrap(), 6);
            assert_eq!(e2.log_start_offset("t", 0).unwrap(), 2);
            e2.relinquish_all().await.unwrap();
        });
    }

    /// Phantom HWM: manifest claims HWM past actual log → reopen
    /// rewinds via scan_high_watermark.
    #[test]
    fn phantom_hwm_is_healed_on_open() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            {
                let e = DiskStorageEngine::new(
                    fs.clone(),
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                );
                for _ in 0..3 {
                    e.append("t", 0, 0, -1, build_batch(1, 1_000))
                        .await
                        .unwrap();
                }
                e.relinquish_all().await.unwrap();
            }
            // Hand-edit the manifest to claim HWM=999.
            let manifest_path = tmp.path().join("t").join("0").join("manifest.json");
            let json = std::fs::read_to_string(&manifest_path).unwrap();
            let phantom = json.replace("\"highWatermark\":3", "\"highWatermark\":999");
            assert_ne!(phantom, json, "expected the patch to take effect");
            std::fs::write(&manifest_path, phantom).unwrap();

            // Reopen — the phantom HWM should be rewound to 3.
            let e2 =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            let _ = e2.take_over("t", 0, 0).await.unwrap();
            assert_eq!(
                e2.high_watermark("t", 0).unwrap(),
                3,
                "phantom HWM should be healed by scan_high_watermark"
            );
            e2.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn arc_dyn_storage_engine_compiles() {
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let _e: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
            fs,
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
    }

    #[test]
    fn relinquish_then_reopen_via_take_over_roundtrips() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            e.append("t", 0, 0, -1, build_batch(3, 1_000))
                .await
                .unwrap();
            e.relinquish("t", 0).await.unwrap();
            // Re-take.
            let hwm = e.take_over("t", 0, 0).await.unwrap();
            assert_eq!(hwm, 3);
        });
    }

    /// gh #219: a deleted topic's partitions are dropped from the
    /// engine — all of them, and only that topic's. Anything left open
    /// keeps serving the deleted incarnation's records to a topic that
    /// was recreated under the same name.
    #[test]
    fn abandon_topic_drops_only_that_topics_partitions() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            for p in 0..3 {
                e.append("doomed", p, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            e.append("keeper", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();

            assert_eq!(e.abandon_topic("doomed").await, 3);
            assert_eq!(e.open_partition_keys(), vec![("keeper".to_string(), 0)]);
            assert_eq!(
                e.high_watermark("keeper", 0).unwrap(),
                1,
                "the surviving topic is untouched"
            );

            // Idempotent: a second delete event (or a relist retraction)
            // must not error or double-count.
            assert_eq!(e.abandon_topic("doomed").await, 0);
        });
    }

    #[test]
    fn partition_size_sums_segment_sizes() {
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e =
                DiskStorageEngine::new(fs, tmp.path().to_path_buf(), PartitionConfig::default());
            let one_len = i64::try_from(build_batch(1, 1_000).len()).unwrap();
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            assert_eq!(e.partition_size("t", 0), one_len * 4);
            e.relinquish_all().await.unwrap();
        });
    }

    /// gh #221 phase 2: the engine routes partition paths through
    /// the placement resolver; unknown names and unplaced partitions
    /// fall back to the default root.
    #[test]
    fn placement_resolver_routes_partition_dirs() {
        use crate::engine::{LogDirInfo, PlacementResolver};
        struct Fixed;
        impl PlacementResolver for Fixed {
            fn log_dir_of(&self, topic: &str, partition: i32) -> Option<String> {
                match (topic, partition) {
                    ("t", 0) => Some("fast".to_owned()),
                    ("t", 1) => Some("no-such-dir".to_owned()),
                    _ => None,
                }
            }
        }
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let default_root = tmp.path().join("data");
            let fast_root = tmp.path().join("fast");
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e = DiskStorageEngine::new(fs, default_root.clone(), PartitionConfig::default())
                .with_extra_log_dirs(vec![LogDirInfo {
                    name: "fast".to_owned(),
                    path: fast_root.clone(),
                    default_eligible: true,
                    cordoned: false,
                    labels: Default::default(),
                }]);
            e.set_placement_resolver(Arc::new(Fixed));

            e.create_partition("t", 0).await.unwrap();
            e.create_partition("t", 1).await.unwrap();
            e.create_partition("t", 2).await.unwrap();

            assert!(
                fast_root.join("t/0").is_dir(),
                "placed partition on pool root"
            );
            assert!(default_root.join("t/1").is_dir(), "unknown name falls back");
            assert!(default_root.join("t/2").is_dir(), "unplaced falls back");

            assert_eq!(e.partition_log_dir("t", 0), "fast");
            assert_eq!(e.partition_log_dir("t", 1), "default");
            assert_eq!(e.partition_log_dir("t", 2), "default");

            let dirs = e.log_dirs();
            assert_eq!(dirs.len(), 2);
            assert_eq!(dirs[0].name, "default");
            assert_eq!(dirs[1].name, "fast");
        });
    }

    /// gh #234: a topic opened before `status.volumeAssignments` lands
    /// writes to the default root; when the assignment shows up and the
    /// broker restarts, the partition must keep serving the data where
    /// it actually is — not resolve to the (empty) assigned pool root
    /// and quietly come up as a fresh 0-byte log.
    #[test]
    fn late_placement_does_not_orphan_data_on_the_default_root() {
        use crate::engine::{LogDirInfo, PlacementResolver};
        struct AssignBulk;
        impl PlacementResolver for AssignBulk {
            fn log_dir_of(&self, _topic: &str, _partition: i32) -> Option<String> {
                Some("bulk".to_owned())
            }
        }
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let default_root = tmp.path().join("data");
            let bulk_root = tmp.path().join("bulk");
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e = DiskStorageEngine::new(
                fs.clone(),
                default_root.clone(),
                PartitionConfig::default(),
            )
            .with_extra_log_dirs(vec![LogDirInfo {
                name: "bulk".to_owned(),
                path: bulk_root.clone(),
                default_eligible: true,
                cordoned: false,
                labels: Default::default(),
            }]);

            // Phase 1: no placement yet (the status write hasn't
            // landed). Data goes to the default root.
            e.create_partition("t", 0).await.unwrap();
            e.append("t", 0, 0, -1, build_batch(3, 1_000))
                .await
                .unwrap();
            let hwm = e.high_watermark("t", 0).unwrap();
            assert_eq!(hwm, 3);
            assert!(default_root.join("t/0/manifest.json").is_file());

            // Phase 2: the assignment lands; a restart relinquishes and
            // reopens every partition.
            e.relinquish_all().await.unwrap();
            e.set_placement_resolver(Arc::new(AssignBulk));
            let recovered = e.take_over("t", 0, 2).await.unwrap();
            assert_eq!(
                recovered, hwm,
                "reopen must serve the data where it is, not an empty \
                 log on the assigned pool root"
            );
            assert!(
                !bulk_root.join("t/0").exists(),
                "no empty shell may be created on the assigned root"
            );
            // Reported log dir keeps naming the assignment (that is the
            // record's job); the data path is what must not move.
            assert_eq!(e.partition_log_dir("t", 0), "bulk");
            // A known record that names another root: the one case the
            // warning is for.
            assert_eq!(e.locate("t", 0).placed_elsewhere, Some(bulk_root.clone()));

            // Phase 3: a genuinely fresh partition under the same
            // placement is created at its assigned root.
            e.create_partition("t", 1).await.unwrap();
            assert!(bulk_root.join("t/1/manifest.json").is_file());
            e.relinquish_all().await.unwrap();
        });
    }

    /// gh #234, the boot-time arm: a restarted broker takes over before
    /// its topic watch has delivered `status.volumeAssignments`, so a
    /// partition correctly placed on a pool root resolves to the default
    /// root. It must still serve the pool copy — this is the arm every
    /// restart of a pool-backed cluster relies on — and, with no record
    /// to contradict, it must do so without a warning (gh #271).
    #[test]
    fn a_restart_before_placement_is_known_serves_the_pool_copy() {
        use crate::engine::{LogDirInfo, PlacementResolver};
        struct AssignBulk;
        impl PlacementResolver for AssignBulk {
            fn log_dir_of(&self, _topic: &str, _partition: i32) -> Option<String> {
                Some("bulk".to_owned())
            }
        }
        /// The registry as it is at boot: installed, but the topic watch
        /// hasn't listed yet.
        struct NotYetKnown;
        impl PlacementResolver for NotYetKnown {
            fn log_dir_of(&self, _topic: &str, _partition: i32) -> Option<String> {
                None
            }
        }
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let default_root = tmp.path().join("data");
            let bulk_root = tmp.path().join("bulk");
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let engine = || {
                DiskStorageEngine::new(fs.clone(), default_root.clone(), PartitionConfig::default())
                    .with_extra_log_dirs(vec![LogDirInfo {
                        name: "bulk".to_owned(),
                        path: bulk_root.clone(),
                        default_eligible: true,
                        cordoned: false,
                        labels: Default::default(),
                    }])
            };

            // Before the restart: placement known, data on the pool root.
            {
                let e = engine();
                e.set_placement_resolver(Arc::new(AssignBulk));
                e.create_partition("t", 0).await.unwrap();
                e.append("t", 0, 0, -1, build_batch(3, 1_000))
                    .await
                    .unwrap();
                e.relinquish_all().await.unwrap();
            }
            assert!(bulk_root.join("t/0/manifest.json").is_file());

            // The restart: a fresh engine whose resolver knows nothing yet.
            let e = engine();
            e.set_placement_resolver(Arc::new(NotYetKnown));
            assert_eq!(e.take_over("t", 0, 2).await.unwrap(), 3);
            assert!(
                !e.read("t", 0, 0, 1 << 20).await.unwrap().is_empty(),
                "records on the pool root must still be readable"
            );
            assert!(
                !default_root.join("t/0").exists(),
                "no empty shell may be created on the default root"
            );
            let found = e.locate("t", 0);
            assert_eq!(found.root, bulk_root);
            assert_eq!(
                found.placed_elsewhere, None,
                "no placement record yet, so nothing for the warning to report"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    /// gh #221 phase 3: AlterReplicaLogDirs move — files land on the
    /// target root, the source path is returned for reclaim, and an
    /// unknown target is refused.
    #[test]
    fn move_partition_between_log_dirs() {
        use crate::engine::LogDirInfo;
        rt().block_on(async {
            let tmp = tempfile::tempdir().unwrap();
            let default_root = tmp.path().join("data");
            let fast_root = tmp.path().join("fast");
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let e = DiskStorageEngine::new(fs, default_root.clone(), PartitionConfig::default())
                .with_extra_log_dirs(vec![LogDirInfo {
                    name: "fast".to_owned(),
                    path: fast_root.clone(),
                    default_eligible: true,
                    cordoned: false,
                    labels: Default::default(),
                }]);

            e.create_partition("t", 0).await.unwrap();
            let src = default_root.join("t/0");
            std::fs::write(src.join("sentinel"), b"x").unwrap();

            let returned = e.move_partition_to_log_dir("t", 0, "fast").await.unwrap();
            assert_eq!(returned, src);
            assert!(fast_root.join("t/0/sentinel").is_file(), "files copied");
            // Source is left for the caller to reclaim after the
            // placement record flips.
            assert!(src.join("sentinel").is_file());

            // Unknown target refused.
            let err = e
                .move_partition_to_log_dir("t", 0, "nope")
                .await
                .unwrap_err();
            assert!(matches!(err, StorageError::Unsupported(_)));

            // Same-dir move is a no-op Ok.
            let same = e
                .move_partition_to_log_dir("t", 0, "default")
                .await
                .unwrap();
            assert_eq!(same, src);
        });
    }

    // --- gh #241 identity gate ---------------------------------------

    struct FixedIncarnation(TopicIncarnation);
    impl TopicIdentityResolver for FixedIncarnation {
        fn incarnation_of(&self, _topic: &str) -> TopicIncarnation {
            self.0.clone()
        }
    }

    fn engine_at(dir: &std::path::Path) -> DiskStorageEngine {
        DiskStorageEngine::new(
            Arc::new(RealFs),
            dir.to_path_buf(),
            PartitionConfig::default(),
        )
    }

    /// Stamp `<root>/<topic>/.topic-id.json` as a previous incarnation
    /// would have.
    fn stamp(root: &std::path::Path, topic: &str, id: &str) {
        let dir = root.join(topic);
        std::fs::create_dir_all(&dir).unwrap();
        crate::topic_identity::write_topic_identity(&RealFs, &dir, id).unwrap();
    }

    #[test]
    fn stale_stamp_refuses_to_open_and_creates_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        stamp(tmp.path(), "t", "old-incarnation");
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Known(
            "new-incarnation".into(),
        ))));
        rt().block_on(async {
            let err = e.take_over("t", 0, 1).await.unwrap_err();
            assert!(matches!(err, StorageError::StaleTopicDir), "got {err:?}");
        });
        // The refusal must land before `Partition::open`'s mkdir_all, or
        // it re-materialises a directory the operator is reclaiming.
        assert!(
            !tmp.path().join("t").join("0").exists(),
            "refused open still created the partition dir"
        );
    }

    /// The operator stamps the freshly reclaimed dir; the gh #215
    /// reconcile re-drives take-over and it must now succeed.
    #[test]
    fn matching_stamp_opens_after_the_reclaim_lands() {
        let tmp = tempfile::tempdir().unwrap();
        stamp(tmp.path(), "t", "old-incarnation");
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Known(
            "new-incarnation".into(),
        ))));
        rt().block_on(async {
            assert!(e.take_over("t", 0, 1).await.is_err());
            stamp(tmp.path(), "t", "new-incarnation");
            e.take_over("t", 0, 1)
                .await
                .expect("re-drive after reclaim");
        });
    }

    /// An unstamped dir is always adopted — the operator's own rule.
    /// Anything else eats every pre-stamp topic on first open.
    #[test]
    fn unstamped_dir_is_adopted() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("t")).unwrap();
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Known(
            "whatever".into(),
        ))));
        rt().block_on(async { e.take_over("t", 0, 1).await.unwrap() });
    }

    /// No CR knowledge → adopt. This is what keeps brokers serving
    /// existing topics with the API server unreachable.
    #[test]
    fn unknown_incarnation_adopts_even_a_stamped_dir() {
        let tmp = tempfile::tempdir().unwrap();
        stamp(tmp.path(), "t", "some-other-incarnation");
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Unknown)));
        rt().block_on(async { e.take_over("t", 0, 1).await.unwrap() });
    }

    /// CR seen but unstamped over a stamped dir = the operator has not
    /// reclaimed this incarnation yet. This is the actual gh #241
    /// window: the CR is recreated before its status carries an id.
    #[test]
    fn pending_incarnation_over_a_stamped_dir_waits() {
        let tmp = tempfile::tempdir().unwrap();
        stamp(tmp.path(), "t", "old-incarnation");
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Pending)));
        rt().block_on(async {
            let err = e.take_over("t", 0, 1).await.unwrap_err();
            assert!(matches!(err, StorageError::StaleTopicDir), "got {err:?}");
        });
    }

    /// A brand-new topic has no directory at all, so a not-yet-stamped
    /// CR must not block it.
    #[test]
    fn pending_incarnation_on_a_fresh_topic_opens() {
        let tmp = tempfile::tempdir().unwrap();
        let e = engine_at(tmp.path());
        e.set_identity_resolver(Arc::new(FixedIncarnation(TopicIncarnation::Pending)));
        rt().block_on(async { e.take_over("t", 0, 1).await.unwrap() });
    }

    /// No resolver installed (dev mode, unit tests) → inert.
    #[test]
    fn without_a_resolver_the_gate_is_inert() {
        let tmp = tempfile::tempdir().unwrap();
        stamp(tmp.path(), "t", "old-incarnation");
        let e = engine_at(tmp.path());
        rt().block_on(async { e.take_over("t", 0, 1).await.unwrap() });
    }
}
