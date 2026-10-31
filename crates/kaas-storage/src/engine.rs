//! The `StorageEngine` trait — the contract every storage backend
//! implements.
//!
//! Phase 2 ships two impls:
//!
//! - [`crate::memory::MemoryStorage`] — `Vec<Bytes>` per partition, used
//!   by dev mode (no `MY_POD_NAME`) and unit tests.
//! - `DiskStorageEngine` (follow-up commit) — segment files, manifest,
//!   producer snapshot, group-commit committer task, takeover.
//!
//! The trait is `dyn`-compatible (no generics, no `Self: Sized` methods,
//! async fn via `async_trait`). Every consumer holds
//! `Arc<dyn StorageEngine>`. This is the seam tests mock.
//!
//! # Byte-opacity
//!
//! `append` takes raw RecordBatch bytes as `Bytes`. The engine rewrites
//! the first 8 bytes (baseOffset) to the broker's chosen offset before
//! storing — that's "brokers own offsets", and the v2 RecordBatch CRC
//! deliberately starts at byte 21 so this overwrite doesn't invalidate
//! it. Records bytes (everything past the 61-byte header) are never
//! inspected or modified.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::Bytes;

use crate::errors::StorageError;

/// Name of the implicit log dir every engine has — the `data_dir`.
/// Pool members (gh #221 phase 2) carry chart-chosen names; this one
/// is reserved.
pub const DEFAULT_LOG_DIR_NAME: &str = "default";

/// One named log dir (gh #221 phase 2 — Kafka's KIP-113 vocabulary:
/// one pool volume = one log dir). `default_eligible` mirrors the
/// chart's `storage.pool[].defaultEligible`: whether topics without
/// an explicit volume binding may be placed here.
///
/// Serde shape matches the `KAAS_LOG_DIRS` env JSON the chart emits:
/// `[{"name":"fast","path":"/vols/fast","defaultEligible":true}]`.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogDirInfo {
    pub name: String,
    pub path: PathBuf,
    #[serde(default = "default_true")]
    pub default_eligible: bool,
    /// KIP-1066 vocabulary (Kafka 4.3): a cordoned log dir accepts no
    /// NEW partition placements — existing partitions keep serving.
    /// The drain primitive for pool shrink / volume decommission.
    #[serde(default)]
    pub cordoned: bool,
    /// gh #224: free-form labels for `volumeSelector` matching
    /// (nodeSelector vocabulary — a topic's selector must be a subset
    /// of a member's labels to match). The default data dir carries no
    /// labels.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub labels: std::collections::BTreeMap<String, String>,
}

fn default_true() -> bool {
    true
}

/// Parse the `KAAS_LOG_DIRS` env JSON (broker and operator share
/// this). Empty/absent input → empty pool (single-volume layout).
/// Entries named `default` are rejected — that name is reserved for
/// the data dir.
pub fn parse_log_dirs_json(json: &str) -> Result<Vec<LogDirInfo>, String> {
    if json.trim().is_empty() {
        return Ok(Vec::new());
    }
    let dirs: Vec<LogDirInfo> =
        serde_json::from_str(json).map_err(|e| format!("KAAS_LOG_DIRS: {e}"))?;
    if let Some(d) = dirs.iter().find(|d| d.name == DEFAULT_LOG_DIR_NAME) {
        return Err(format!(
            "KAAS_LOG_DIRS: entry '{}' uses the reserved name '{DEFAULT_LOG_DIR_NAME}'",
            d.path.display()
        ));
    }
    Ok(dirs)
}

/// KIP-827 capacity probe: `(total_bytes, usable_bytes)` of the
/// filesystem hosting `path`, or `(-1, -1)` — the wire sentinel —
/// when the statvfs fails (e.g. the sentinel `memory://` dir).
/// Feeds `DescribeLogDirs` v4 and the per-log-dir capacity gauges.
pub fn fs_capacity(path: &Path) -> (i64, i64) {
    let Ok(st) = nix::sys::statvfs::statvfs(path) else {
        return (-1, -1);
    };
    let frsize = st.fragment_size();
    let total = st.blocks().saturating_mul(frsize);
    let usable = st.blocks_available().saturating_mul(frsize);
    (
        i64::try_from(total).unwrap_or(i64::MAX),
        i64::try_from(usable).unwrap_or(i64::MAX),
    )
}

/// Resolves which log dir hosts a partition. Backed by the KafkaTopic
/// registry in production (the operator stamps placement into the CR
/// status; the topic watch feeds it here). A `None` answer — or an
/// unknown name — falls back to the default log dir, so resolution
/// can never make a partition unopenable.
pub trait PlacementResolver: Send + Sync {
    fn log_dir_of(&self, topic: &str, partition: i32) -> Option<String>;
}

/// What the broker knows about the *incarnation* of a topic name
/// (gh #241). Deliberately three-valued: "we have never heard of this
/// topic" and "we have heard of it but it has not been stamped yet"
/// must lead to opposite decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicIncarnation {
    /// No `KafkaTopic` CR has been seen for this name — env-var seeded,
    /// dev mode, or an API server we can't reach. Adopt whatever is on
    /// disk: brokers serve existing topics without the operator, and
    /// that invariant outranks this check.
    Unknown,
    /// A CR is known but carries no `Status.TopicID` yet, i.e. the
    /// operator has not reconciled *this* incarnation. A directory
    /// already stamped by a previous one is therefore un-reclaimed.
    Pending,
    /// The `Status.TopicID` of the current incarnation.
    Known(String),
}

/// Resolves the incarnation a topic name is expected to be at, so the
/// engine can refuse to open a directory the operator has not yet
/// reclaimed (gh #241). Backed by the `KafkaTopic` registry in
/// production; absent (→ [`TopicIncarnation::Unknown`]) everywhere
/// else.
pub trait TopicIdentityResolver: Send + Sync {
    fn incarnation_of(&self, topic: &str) -> TopicIncarnation;
}

#[async_trait]
pub trait StorageEngine: Send + Sync + 'static {
    /// Append a raw RecordBatch to `(topic, partition)`. The engine
    /// rewrites bytes `[0..8]` (baseOffset) to the partition's current
    /// HWM before storing.
    ///
    /// `epoch` is the leader epoch the caller believes it owns; the
    /// disk engine rejects with `EpochMismatch` if its current epoch is
    /// higher. `MemoryStorage` ignores it (single-process dev mode).
    ///
    /// `acks` matches the producer field: `-1` waits for durable
    /// storage before returning; `0` and `1` return as soon as the
    /// bytes are accepted. `MemoryStorage` is always synchronous so
    /// the distinction is moot for it.
    ///
    /// Returns the assigned `base_offset` (== HWM at the moment of
    /// append).
    async fn append(
        &self,
        topic: &str,
        partition: i32,
        epoch: u32,
        acks: i16,
        batch: Bytes,
    ) -> Result<i64, StorageError>;

    /// Read raw RecordBatch bytes starting at `start_offset` for up to
    /// `max_bytes` of payload. Returns the concatenated bytes of all
    /// batches whose last_offset is `>= start_offset`, until the cap
    /// is reached.
    async fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError>;

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError>;

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError>;

    /// gh #5: timestamp-to-offset lookup (`ListOffsets` v1+). Returns
    /// `(offset, timestamp)` for the first record at or after
    /// `timestamp_ms`. `(-1, -1)` is the documented "no matching
    /// record" sentinel.
    fn offset_for_timestamp(
        &self,
        topic: &str,
        partition: i32,
        timestamp_ms: i64,
    ) -> Result<(i64, i64), StorageError>;

    /// KIP-101 / gh #101: for a Java consumer holding `leader_epoch =
    /// E`, return `(result_epoch, end_offset)` marking the offset just
    /// past the last record at epoch `E`. `(-1, -1)` is the documented
    /// "nothing to truncate to" sentinel.
    fn offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: i32,
        leader_epoch: i32,
    ) -> Result<(i32, i64), StorageError>;

    /// gh #31: `DeleteRecords` (API key 21). Advances the partition's
    /// log start offset to at least `target_offset` so records below
    /// it become invisible to Fetch. `target_offset == -1` is the
    /// purge-to-HWM sentinel (KIP-107). Returns the new log start
    /// offset.
    async fn delete_records(
        &self,
        topic: &str,
        partition: i32,
        target_offset: i64,
    ) -> Result<i64, StorageError>;

    async fn create_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError>;

    async fn delete_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError>;

    /// Total bytes occupied by the partition's storage. `0` for unknown
    /// partitions so callers can iterate without filtering.
    fn partition_size(&self, topic: &str, partition: i32) -> i64;

    /// Engine's data directory. Advertised as the "log dir" in
    /// `DescribeLogDirs`. `MemoryStorage` returns the sentinel
    /// `memory://`.
    fn data_dir(&self) -> &Path;

    /// Every log dir this engine serves, default first (gh #221
    /// phase 2). Single-volume engines report just the data dir.
    fn log_dirs(&self) -> Vec<LogDirInfo> {
        vec![LogDirInfo {
            name: DEFAULT_LOG_DIR_NAME.to_owned(),
            path: self.data_dir().to_path_buf(),
            default_eligible: true,
            cordoned: false,
            labels: Default::default(),
        }]
    }

    /// Name of the log dir hosting `(topic, partition)` under the
    /// current placement. Feeds `DescribeLogDirs` grouping.
    fn partition_log_dir(&self, _topic: &str, _partition: i32) -> String {
        DEFAULT_LOG_DIR_NAME.to_owned()
    }

    /// Claim write ownership of `(topic, partition)` under `epoch`.
    /// On disk, this opens FDs and runs recovery; on memory storage
    /// it's a no-op that returns the current HWM.
    async fn take_over(&self, topic: &str, partition: i32, epoch: u32)
        -> Result<i64, StorageError>;

    /// Release write ownership.
    async fn relinquish(&self, topic: &str, partition: i32) -> Result<(), StorageError>;

    /// gh #221 phase 3: move a partition's files to another log dir
    /// (`AlterReplicaLogDirs`, KIP-113). Closes the partition, copies
    /// its directory to the target log dir, and returns the *source*
    /// directory (the caller reclaims it after flipping the placement
    /// record). Appends/reads during the copy fail with
    /// [`StorageError::Migrating`] — a brief, retriable window.
    async fn move_partition_to_log_dir(
        &self,
        _topic: &str,
        _partition: i32,
        _log_dir: &str,
    ) -> Result<PathBuf, StorageError> {
        Err(StorageError::Unsupported("log-dir moves"))
    }

    /// gh #219: drop every open partition of `topic` **without
    /// persisting state**, and return how many were dropped.
    ///
    /// Called when the `KafkaTopic` CR is deleted. Unlike
    /// [`StorageEngine::relinquish`] (a leadership handover — persist,
    /// then release) this is "the topic is gone": writing a manifest or
    /// producer snapshot on the way out would land the dead
    /// incarnation's state in the directory a recreated topic of the
    /// same name is about to use. Dropping the in-memory partition also
    /// forces the next `take_over` to re-open from disk, so a recreated
    /// topic can't keep serving the old one's high watermark and
    /// idempotence window.
    ///
    /// Safe to call for a topic that isn't open, and safe if the delete
    /// turns out to be spurious — nothing on disk is touched, and the
    /// takeover reconcile backstop (gh #215) re-opens anything still
    /// assigned. Default is a no-op for engines with no open handles.
    async fn abandon_topic(&self, _topic: &str) -> usize {
        0
    }

    /// Keys of the partitions this engine currently holds open — i.e.
    /// the ones it has taken over (`take_over` opened; `relinquish`
    /// closed). Used to compute the gh #208 "serving" readiness
    /// signal: a broker is serving once every partition
    /// `assignment.json` assigns to it appears here.
    ///
    /// NOTE this reflects "handles are open", not "requests are being
    /// processed" — a wedged main runtime keeps its partitions open.
    /// Wedge detection is a separate liveness signal (the main-runtime
    /// tick); this method only answers "has takeover completed".
    ///
    /// Default returns empty (a fake that never takes anything over
    /// holds nothing open).
    fn open_partition_keys(&self) -> Vec<(String, i32)> {
        Vec::new()
    }

    /// Leader epoch this engine currently holds `(topic, partition)` at,
    /// or `None` if the partition isn't open here.
    ///
    /// Lets the takeover reconcile tell "open at the assignment's epoch"
    /// apart from "open, but at a stale one" (gh #227). Those look
    /// identical through [`Self::open_partition_keys`], and conflating
    /// them strands a partition whose `take_over` failed *after* the
    /// open: it counts as open, so the reconcile skips it, and nothing
    /// re-drives it until the next assignment change.
    ///
    /// Default returns `None` — an engine that never takes anything over
    /// holds no epoch to report.
    fn partition_epoch(&self, _topic: &str, _partition: i32) -> Option<i64> {
        None
    }

    /// gh #236 — the topic's operator-materialised `.config.json`
    /// overrides, or `None` when the topic has none (or the engine
    /// keeps no per-topic config at all — memory/dev). Re-reads the
    /// file on every call, same hot-reload contract as the retention
    /// sweep's `TopicConfigPolicySource`: a CR edit is visible on the
    /// next DescribeConfigs with no restart. Not a hot path.
    fn topic_config(&self, _topic: &str) -> Option<crate::topicconfig::TopicConfigFile> {
        None
    }

    /// gh #168: how many open partitions are currently poisoned by a
    /// fsync failure (`flush_err` set, appends answering `Stalled`).
    /// Feeds `/healthz`'s `storage_stalled` so a stalled substrate is
    /// visible to ops instead of only to producers as retriable
    /// errors. Default 0 — the memory/dev engine never fsyncs.
    fn stalled_partitions(&self) -> usize {
        0
    }

    /// SIGTERM-time drain: close every open partition so the next
    /// leader doesn't hit NFS silly-rename pain on takeover
    /// (gh #61 + gh #139). Default impl is a no-op — `MemoryStorage`
    /// has nothing to release; `DiskStorageEngine` overrides.
    async fn drain(&self) -> Result<(), StorageError> {
        Ok(())
    }

    /// gh #30 / #108: walk every open partition and bump the
    /// recorded epoch for `pid` to `new_epoch`. Idempotent — no-op
    /// per-partition when the recorded epoch is already
    /// `>= new_epoch`. Called both for the in-process fence
    /// (`InitProducerId` bumped this PID's epoch on this broker)
    /// and for inbound peer fences dispatched by the
    /// `kaas-broker::FenceWatcher`.
    ///
    /// Default impl is a no-op so `MemoryStorage`, fakes, and
    /// future variants that don't track per-PID dedupe state
    /// compile without ceremony; `DiskStorageEngine` overrides.
    fn fence_producer_epoch(&self, _pid: i64, _new_epoch: i16) {}

    /// gh #176 — Last Stable Offset for `read_committed` Fetch. The
    /// lowest offset across all open transactional producers on this
    /// partition, or HWM when no txn is open. Default falls back to
    /// HWM (read_uncommitted-equivalent) so engines that don't track
    /// per-partition txn state degrade gracefully — a
    /// long-standing shortcut.
    fn last_stable_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        self.high_watermark(topic, partition)
    }

    /// gh #176 — aborted transactions whose first-offset falls in
    /// `[start_offset, end_offset)`. `read_committed` Fetch handler
    /// uses this to build the response's `AbortedTransactions[]`
    /// list. Default returns an empty list.
    fn aborted_transactions_in_range(
        &self,
        _topic: &str,
        _partition: i32,
        _start_offset: i64,
        _end_offset: i64,
    ) -> Vec<crate::txn_index::AbortedTxn> {
        Vec::new()
    }
}
