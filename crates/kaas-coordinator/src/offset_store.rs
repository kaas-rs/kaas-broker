//! Per-group committed offsets, persisted under
//! `<cluster_dir>/__consumer_offsets/<group>.json` with `tmp + rename`
//! atomicity.
//!
//! The root is the cluster-state directory (`/data/__cluster` by
//! default), NOT the data dir: a sibling of the topic dirs is exactly
//! what the operator's orphan-topic sweep reclaims, and offsets lived
//! there once — every sweep pass deleted them (gh #223). The boot-time
//! adoption shim for the pre-fix layout was dropped under the pre-v1
//! no-backcompat policy: upgrading across gh #223 is a fresh deploy.
//!
//! One on-disk schema: the gh #21 v2 envelope
//! `{"offsets":{...}, "metadata":{...}}`. The legacy v1 plain
//! `map[string]int64` fallback was dropped under the pre-v1
//! no-backcompat policy, so **every writer must go through
//! `write_group`** — serde ignores unknown fields, which means a
//! stray v1-shaped write decodes back as an empty group instead of
//! failing, silently losing every offset in it (that was
//! `delete_partitions` until gh #240).
//!
//! Three layers of state:
//!
//! 1. The visible cache + metadata maps backed by `<group>.json`.
//!    `Commit` / `commit_with_metadata` writes here; `fetch` /
//!    `fetch_metadata` reads here. A group's file is pulled in the
//!    first time any of those touches it (`ensure_loaded`), not only
//!    when the group joins: the cache is write-through, so for a cached
//!    group it is the authority, and for an uncached one the file is.
//! 2. The transactional **pending** layer keyed on `(group_id, pid)`
//!    — `store_pending` stages, `commit_pending` materialises into
//!    layer 1, `discard_pending` drops. Memory-only: an unfinished
//!    transaction reset on broker restart (matches Apache's "in-
//!    flight offsets aren't recovered" contract).
//! 3. The on-disk file. Lock is dropped before disk I/O — the
//!    "snap then write" pattern, so a concurrent `fetch` doesn't
//!    block on filesystem latency.

use std::collections::HashMap;
use std::io;
use std::path::PathBuf;

use parking_lot::{Mutex, MutexGuard, RwLock};
use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;

/// Per-(topic, partition) request shape used by `fetch` /
/// `fetch_metadata`.
#[derive(Debug, Clone)]
pub struct FetchSpec {
    pub topic: String,
    pub partitions: Vec<i32>,
}

/// Build the canonical `"topic/partition"` cache + on-disk JSON key.
/// Handlers and tests must use this exact helper so wire-level
/// `OffsetDelete` (key 47) lookups round trip against the cache
/// (gh #100).
pub fn offset_key(topic: &str, partition: i32) -> String {
    format!("{topic}/{partition}")
}

/// Split an offset key back into `(topic, partition)`, from the right
/// so a `/` in the topic can't shift the split.
pub fn parse_offset_key(key: &str) -> Option<(&str, i32)> {
    let (topic, partition) = key.rsplit_once('/')?;
    Some((topic, partition.parse().ok()?))
}

/// Does the offset key `"<topic>/<partition>"` belong to `topic`?
/// Split from the RIGHT — a topic name may itself contain `/` (the
/// SIGTERM drain parses partition keys the same way), the partition
/// suffix never does. The digit check keeps a topic literally named
/// `foo/bar` from matching a purge of `foo`.
fn key_has_topic(key: &str, topic: &str) -> bool {
    match key.rsplit_once('/') {
        Some((t, p)) => t == topic && p.parse::<i32>().is_ok(),
        None => false,
    }
}

/// gh #167 handoff fence: the assignment under which the writer
/// believed itself coordinator, compared lexicographically —
/// `controller_epoch` orders across controller reigns,
/// `assignment_version` within one. Both are monotonic, so "newer
/// assignment" is a total order. Derived `Ord` uses field order;
/// keep `controller_epoch` first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FenceStamp {
    #[serde(default)]
    pub controller_epoch: i64,
    #[serde(default)]
    pub assignment_version: i64,
}

impl FenceStamp {
    fn is_zero(&self) -> bool {
        *self == Self::default()
    }
}

/// gh #167: an offset write refused because the on-disk file was
/// stamped by a newer assignment — this broker's view of "I am the
/// coordinator" is stale, and a whole-map rewrite from its cache
/// would erase every commit the real coordinator has taken since.
/// Surfaced to clients as `NOT_COORDINATOR`.
#[derive(Debug, thiserror::Error)]
#[error("offset write fenced: on-disk assignment {on_disk:?} newer than local {local:?}")]
pub struct FencedOffsetWrite {
    pub on_disk: FenceStamp,
    pub local: FenceStamp,
}

/// gh #21 v2 envelope — the only shape written or read. Note the
/// serde default: an unrecognised payload (e.g. the old v1 plain map)
/// decodes as an empty envelope rather than erroring, which is why
/// nothing may write any other shape. The gh #167 `fence` field is
/// additive: files written before it decode as the zero stamp, which
/// any writer may overwrite.
#[derive(Debug, Serialize, Deserialize, Default)]
struct OffsetFileV2 {
    #[serde(default)]
    offsets: HashMap<String, i64>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    metadata: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "FenceStamp::is_zero")]
    fence: FenceStamp,
}

#[derive(Debug, Default)]
struct Inner {
    /// `group → offset_key → offset`.
    cache: HashMap<String, HashMap<String, i64>>,
    /// `group → offset_key → metadata`. Mirrors `cache` shape; empty
    /// strings are stored as "no entry" so the wire null sentinel
    /// round-trips.
    metadata: HashMap<String, HashMap<String, String>>,
    /// gh #27 in-flight transactional offset commits keyed on
    /// `(group_id, producer_id)`. Memory-only.
    pending: HashMap<PendingKey, PendingOffsets>,
    /// Groups whose last write failed or was fenced. The cache still holds
    /// what was last known to be on disk, but a write that failed after its
    /// rename can have left the file ahead of it, so the gh #167
    /// skip-unchanged fast path must not fire for these: "unchanged" is only
    /// unchanged relative to the cache.
    dirty: std::collections::HashSet<String>,
}

/// What `TxnOffsetCommit` staged for one `(group, producer)`.
#[derive(Debug, Default)]
struct PendingOffsets {
    offsets: HashMap<String, i64>,
    metadata: HashMap<String, String>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct PendingKey {
    group_id: String,
    producer_id: i64,
}

/// A group id too long to be a file name. Apache allows it; kaas keeps each
/// group's offsets in a file named after it, so this is a documented limit,
/// answered `INVALID_GROUP_ID` rather than a failed write that looks retriable.
#[derive(Debug, thiserror::Error)]
#[error("group id too long for a file name")]
pub struct GroupIdTooLong;

/// The longest file name, less the `.tmp` the atomic write adds.
const MAX_GROUP_FILE_NAME: usize = 255 - ".tmp".len();

/// Can this group's offsets be stored at all? Its file name has to fit, which
/// is why a longer id answers `INVALID_GROUP_ID` (24), and why no file can exist
/// for one. Anything that stages offsets checks this up front (gh #318): staged
/// offsets that could never be written would wedge their transaction.
#[must_use]
pub fn group_id_fits(group_id: &str) -> bool {
    group_file_name(group_id).len() <= MAX_GROUP_FILE_NAME
}

/// The file holding `group_id`'s offsets. Apache allows any string as a group
/// id, because it keeps them in a log; here it becomes a file name, so a
/// separator would let a group ACL matching `../acls` overwrite the cluster's
/// `acls.json` (gh #314). `/` and NUL are percent-encoded, and `%` so that the
/// encoding round-trips. Every other name's file is unchanged.
fn group_file_name(group_id: &str) -> String {
    let mut name = String::with_capacity(group_id.len() + ".json".len());
    for c in group_id.chars() {
        match c {
            '/' => name.push_str("%2F"),
            '%' => name.push_str("%25"),
            '\0' => name.push_str("%00"),
            c => name.push(c),
        }
    }
    name.push_str(".json");
    name
}

/// The inverse of [`group_file_name`], for a name with `.json` removed.
fn group_id_from_stem(stem: &str) -> String {
    let mut id = String::with_capacity(stem.len());
    let mut rest = stem;
    while let Some(at) = rest.find('%') {
        id.push_str(&rest[..at]);
        let tail = &rest[at..];
        let (decoded, used) = match tail.get(..3) {
            Some("%2F") => ('/', 3),
            Some("%25") => ('%', 3),
            Some("%00") => ('\0', 3),
            _ => ('%', 1),
        };
        id.push(decoded);
        rest = &tail[used..];
    }
    id.push_str(rest);
    id
}

pub struct OffsetStore {
    root: PathBuf,
    state: RwLock<Inner>,
    /// gh #167: where the current assignment fence comes from — the
    /// broker `Coordinator` in production (wired by
    /// `Manager::set_group_assignment_source`, same hot-swap seam as
    /// group ownership), absent in dev/tests (zero stamp: fence
    /// effectively off, matching the single-writer reality there).
    fence_source: RwLock<Option<Box<dyn Fn() -> FenceStamp + Send + Sync>>>,
    /// Does this partition exist? See [`Self::set_partition_source`].
    partition_source: RwLock<Option<PartitionSource>>,
    /// Serialises each group's writers, striped so it stays bounded. A writer
    /// holds its stripe across read cache, write file, update cache. The cache
    /// only moves after the write lands (gh #318), so without this two
    /// concurrent commits would both merge from the same old cache, and the
    /// second write would drop the first one's offsets.
    write_locks: Vec<Mutex<()>>,
}

type PartitionSource = Box<dyn Fn(&str, i32) -> bool + Send + Sync>;

/// How many stripes [`OffsetStore::write_locks`] has.
const WRITE_LOCK_STRIPES: u64 = 64;

impl std::fmt::Debug for OffsetStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OffsetStore")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl OffsetStore {
    /// `root` is the cluster-state directory; group files land at
    /// `<root>/__consumer_offsets/<group>.json`.
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            state: RwLock::new(Inner::default()),
            fence_source: RwLock::new(None),
            partition_source: RwLock::new(None),
            write_locks: (0..WRITE_LOCK_STRIPES).map(|_| Mutex::new(())).collect(),
        }
    }

    /// The write lock for `group_id`'s stripe.
    fn write_lock(&self, group_id: &str) -> MutexGuard<'_, ()> {
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        group_id.hash(&mut h);
        let stripe = usize::try_from(h.finish() % WRITE_LOCK_STRIPES).unwrap_or(0);
        self.write_locks[stripe].lock()
    }

    /// Install the gh #167 fence source. Every subsequent disk write
    /// is stamped with the assignment it was made under and refuses
    /// to overwrite a file stamped by a newer one.
    pub fn set_fence_source(&self, src: impl Fn() -> FenceStamp + Send + Sync + 'static) {
        *self.fence_source.write() = Some(Box::new(src));
    }

    /// Install the check that keeps an offset from outliving its topic (gh #335).
    ///
    /// A request checks its topic exists, then waits for the group's write lock, and
    /// a delete can land between the two: the purge visits the group and moves on, and
    /// the commit then writes an offset for a topic that's gone, which a recreate
    /// inherits. So a commit asks again under the lock. A delete takes the topic out
    /// of the registry before it purges, so one that sees the topic has the lock
    /// first and gets purged, and one that gets the lock later sees it gone.
    pub fn set_partition_source(&self, src: impl Fn(&str, i32) -> bool + Send + Sync + 'static) {
        *self.partition_source.write() = Some(Box::new(src));
    }

    /// Does `key`'s partition exist? True with no source installed, or a key this
    /// can't parse.
    fn partition_exists(&self, key: &str) -> bool {
        let source = self.partition_source.read();
        match (source.as_ref(), parse_offset_key(key)) {
            (Some(exists), Some((topic, partition))) => exists(topic, partition),
            _ => true,
        }
    }

    fn current_fence(&self) -> FenceStamp {
        self.fence_source
            .read()
            .as_ref()
            .map(|f| f())
            .unwrap_or_default()
    }

    /// The fence stamp on a group's on-disk file. `None` when the file
    /// is absent or unreadable — the fence is defense-in-depth, and
    /// refusing commits on a transient read hiccup would be a worse
    /// failure than the race it guards.
    fn on_disk_fence(&self, group_id: &str) -> Option<FenceStamp> {
        let path = self.dir().join(group_file_name(group_id));
        let data = std::fs::read(&path).ok()?;
        serde_json::from_slice::<OffsetFileV2>(&data)
            .ok()
            .map(|f| f.fence)
    }

    fn dir(&self) -> PathBuf {
        self.root.join("__consumer_offsets")
    }

    // --- pending (gh #27 transactional offsets) -----------------------

    /// Stage offsets from `TxnOffsetCommit` (key 28). They are NOT
    /// visible to `OffsetFetch` until `commit_pending` runs.
    pub fn store_pending(&self, group_id: &str, producer_id: i64, offsets: HashMap<String, i64>) {
        self.store_pending_with_metadata(group_id, producer_id, offsets, HashMap::new());
    }

    /// [`Self::store_pending`] with each offset's metadata string, which is
    /// committed along with it (gh #326). An empty value clears the entry, as
    /// in [`Self::commit_with_metadata`].
    pub fn store_pending_with_metadata(
        &self,
        group_id: &str,
        producer_id: i64,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        // Under the same lock as `purge_topic`'s drop, so an offset staged as its
        // topic is deleted is either refused here or dropped there (gh #335).
        let mut s = self.state.write();
        let slot = s.pending.entry(key).or_default();
        slot.offsets.extend(
            offsets
                .into_iter()
                .filter(|(k, _)| self.partition_exists(k)),
        );
        slot.metadata.extend(
            metadata
                .into_iter()
                .filter(|(k, _)| self.partition_exists(k)),
        );
    }

    /// Materialise the staged offsets for `(group, pid)` as committed.
    /// Called from the `EndTxn(commit)` handler. Idempotent.
    pub fn commit_pending(&self, group_id: &str, producer_id: i64) -> io::Result<()> {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        // Read under the lock, so a purge can't drop them between the read and the
        // write (gh #335).
        let _writer = self.write_lock(group_id);
        // They stay staged until the commit is durable, so a failed write can
        // be retried (gh #318). Removing them first lost them, and the caller
        // had no way to know.
        let Some((offsets, metadata)) = self
            .state
            .read()
            .pending
            .get(&key)
            .map(|p| (p.offsets.clone(), p.metadata.clone()))
        else {
            return Ok(());
        };
        self.commit_locked(group_id, offsets, metadata)?;
        self.state.write().pending.remove(&key);
        Ok(())
    }

    /// The groups holding staged offsets. A purge has to reach these too: one with
    /// no file and no members has nothing else to show for itself.
    pub fn groups_with_pending(&self) -> Vec<String> {
        let s = self.state.read();
        s.pending.keys().map(|k| k.group_id.clone()).collect()
    }

    /// Drop staged offsets for `(group, pid)` without materialising.
    /// Called from `EndTxn(abort)`. Idempotent.
    pub fn discard_pending(&self, group_id: &str, producer_id: i64) {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        self.state.write().pending.remove(&key);
    }

    /// Read-only snapshot of staged offsets for `(group, pid)`.
    /// Exposed for tests; production wires `commit_pending` /
    /// `discard_pending`. Returns `None` when no pending entry exists.
    pub fn pending_for(&self, group_id: &str, producer_id: i64) -> Option<HashMap<String, i64>> {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        self.state
            .read()
            .pending
            .get(&key)
            .map(|p| p.offsets.clone())
    }

    // --- committed --------------------------------------------------

    /// Equivalent to `commit_with_metadata(group, offsets, &empty)`.
    /// Preserved for callers that don't carry metadata (txn commit
    /// path, internal compaction paths).
    pub fn commit(&self, group_id: &str, offsets: HashMap<String, i64>) -> io::Result<()> {
        self.commit_with_metadata(group_id, offsets, HashMap::new())
    }

    /// Atomically write the committed offsets for a group + an
    /// optional per-partition metadata string (gh #21). Empty
    /// metadata values clear the entry — round-trip back as the wire
    /// null sentinel.
    pub fn commit_with_metadata(
        &self,
        group_id: &str,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) -> io::Result<()> {
        let _writer = self.write_lock(group_id);
        self.commit_locked(group_id, offsets, metadata)
    }

    /// [`Self::commit_with_metadata`] for a caller holding the group's write lock.
    fn commit_locked(
        &self,
        group_id: &str,
        mut offsets: HashMap<String, i64>,
        mut metadata: HashMap<String, String>,
    ) -> io::Result<()> {
        if !group_id_fits(group_id) {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, GroupIdTooLong));
        }
        offsets.retain(|k, _| self.partition_exists(k));
        metadata.retain(|k, _| self.partition_exists(k));
        // The merge below rewrites the whole file from the cache entry,
        // so an entry that doesn't hold the file yet would erase every
        // offset this commit doesn't name.
        self.ensure_loaded(group_id)?;
        let (merged_offsets, merged_meta, changed) = {
            let s = self.state.read();
            let mut merged = s.cache.get(group_id).cloned().unwrap_or_default();
            let mut merged_meta = s.metadata.get(group_id).cloned().unwrap_or_default();
            let mut changed = false;
            for (k, v) in offsets {
                if merged.insert(k, v) != Some(v) {
                    changed = true;
                }
            }
            for (k, v) in metadata {
                if v.is_empty() {
                    changed |= merged_meta.remove(&k).is_some();
                } else if merged_meta.insert(k, v.clone()) != Some(v) {
                    changed = true;
                }
            }
            (merged, merged_meta, changed || s.dirty.contains(group_id))
        };

        // gh #167 (write amplification): an idle consumer re-commits
        // the same offsets every interval; when the merge moved
        // nothing the on-disk file is already right, so skip the
        // rewrite entirely. Correct because the cache only holds what
        // a successful write put on disk, and the dirty set (folded
        // into `changed` above) excludes the groups where the file may
        // have moved anyway.
        if !changed {
            return Ok(());
        }

        self.write_and_cache(group_id, merged_offsets, merged_meta)
    }

    /// Write the group's file, and only then update the cache, so a failed
    /// write leaves fetches answering what's on disk (gh #318). The cache
    /// used to move first: after a failed commit a fetch answered the
    /// unwritten value, and a restart answered the old one. The caller holds
    /// [`Self::write_lock`].
    fn write_and_cache(
        &self,
        group_id: &str,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) -> io::Result<()> {
        self.write_group(group_id, offsets.clone(), metadata.clone())?;
        let mut s = self.state.write();
        s.cache.insert(group_id.to_owned(), offsets);
        s.metadata.insert(group_id.to_owned(), metadata);
        Ok(())
    }

    /// The single on-disk write path. Every writer goes through here so
    /// the file is always the v2 envelope: `decode_offsets_file` accepts
    /// v2 *only*, and serde ignores unknown fields, so a plain-map write
    /// decodes back as an EMPTY group rather than failing loudly —
    /// silent offset loss on the next coordinator load.
    fn write_group(
        &self,
        group_id: &str,
        offsets: HashMap<String, i64>,
        metadata: HashMap<String, String>,
    ) -> io::Result<()> {
        // gh #167 handoff fence: read-compare-write, same shape as the
        // partition manifest's `write_unless_superseded`. A whole-map
        // rewrite from a stale coordinator doesn't merely roll offsets
        // back — partitions committed only at the new coordinator are
        // *absent* from the stale cache, so the loser's write erases
        // them and an `auto.offset.reset=latest` consumer then skips
        // its backlog. NFS has no compare-and-swap, so a racing write
        // landing between our read and rename remains possible — the
        // same documented residual as the manifest — but the common
        // failure (a laggard broker whose assignment view is a poll
        // behind) is fenced deterministically.
        let local = self.current_fence();
        if let Some(on_disk) = self.on_disk_fence(group_id) {
            if on_disk > local {
                self.state.write().dirty.insert(group_id.to_owned());
                return Err(io::Error::other(FencedOffsetWrite { on_disk, local }));
            }
        }
        let payload = OffsetFileV2 {
            offsets,
            metadata,
            fence: local,
        };
        let name = group_file_name(group_id);
        let res = atomic_write_json(&self.dir(), &name, &payload);
        let mut s = self.state.write();
        match &res {
            Ok(()) => {
                s.dirty.remove(group_id);
            }
            Err(_) => {
                s.dirty.insert(group_id.to_owned());
            }
        }
        res
    }

    /// Committed offsets for the given `(topic, partitions[])` set.
    /// Returns `-1` for any partition without a committed offset.
    pub fn fetch(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, i64> {
        self.ensure_loaded_for_read(group_id);
        let s = self.state.read();
        let group = s.cache.get(group_id);
        let mut out = HashMap::new();
        for spec in specs {
            for &p in &spec.partitions {
                let k = offset_key(&spec.topic, p);
                let v = group.and_then(|g| g.get(&k)).copied().unwrap_or(-1);
                out.insert(k, v);
            }
        }
        out
    }

    /// Per-partition metadata blob committed alongside each offset
    /// (gh #21). Keys missing from the returned map have no metadata
    /// — the wire null sentinel.
    pub fn fetch_metadata(&self, group_id: &str, specs: &[FetchSpec]) -> HashMap<String, String> {
        self.ensure_loaded_for_read(group_id);
        let s = self.state.read();
        let group = match s.metadata.get(group_id) {
            None => return HashMap::new(),
            Some(g) => g,
        };
        let mut out = HashMap::new();
        for spec in specs {
            for &p in &spec.partitions {
                let k = offset_key(&spec.topic, p);
                if let Some(v) = group.get(&k) {
                    out.insert(k, v.clone());
                }
            }
        }
        out
    }

    /// Every committed offset for `group_id`, with its metadata — the
    /// OffsetFetch "all topics" form (a null topic list), which is how
    /// Kafka 4's `kafka-consumer-groups --describe` asks.
    pub fn fetch_all(&self, group_id: &str) -> (HashMap<String, i64>, HashMap<String, String>) {
        self.ensure_loaded_for_read(group_id);
        let s = self.state.read();
        (
            s.cache.get(group_id).cloned().unwrap_or_default(),
            s.metadata.get(group_id).cloned().unwrap_or_default(),
        )
    }

    /// Does `group_id` have committed offsets, cached or on disk? The
    /// existence test DeleteGroups needs: a group whose only trace is
    /// its file (idle since this broker became its coordinator) still
    /// exists.
    pub fn has_offsets(&self, group_id: &str) -> bool {
        self.ensure_loaded_for_read(group_id);
        self.has_group(group_id)
    }

    /// Does the in-memory cache have any offsets for `group_id`?
    pub fn has_group(&self, group_id: &str) -> bool {
        self.state.read().cache.contains_key(group_id)
    }

    /// Drop a group's offsets from cache + disk. Idempotent — deleting
    /// an unknown group is `Ok(())` so partial-delete retries from
    /// AdminClient don't surface spurious errors.
    pub fn delete(&self, group_id: &str) -> io::Result<()> {
        let _writer = self.write_lock(group_id);
        let not_found_is_fine = |r: io::Result<()>| match r {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        };
        // Disk first, and the sync whether or not this attempt did the unlink:
        // a retry of a failed sync finds nothing to unlink. On failure the
        // group is as it was, and the answer says so (gh #318).
        if group_id_fits(group_id) {
            not_found_is_fine(std::fs::remove_file(
                self.dir().join(group_file_name(group_id)),
            ))?;
        }
        not_found_is_fine(crate::atomic_write::fsync_dir(&self.dir()))?;
        let mut s = self.state.write();
        s.cache.remove(group_id);
        s.metadata.remove(group_id);
        s.dirty.remove(group_id);
        Ok(())
    }

    /// Remove specific `(topic, partition)` offset entries from a
    /// group's committed offsets (gh #100 — `OffsetDelete` key 47).
    /// Returns the set of keys actually removed; absent keys are
    /// silently ignored — wire-level `UNKNOWN_TOPIC_OR_PARTITION`
    /// mapping is the handler's job.
    pub fn delete_partitions(
        &self,
        group_id: &str,
        keys: &[String],
    ) -> io::Result<HashMap<String, bool>> {
        let _writer = self.write_lock(group_id);
        self.ensure_loaded(group_id)?;
        let (offsets, metadata, removed, dirty) = {
            let s = self.state.read();
            let Some(cached) = s.cache.get(group_id) else {
                return Ok(HashMap::new());
            };
            let mut offsets = cached.clone();
            // The metadata map mirrors the offset map; a key dropped
            // from one must go from the other or the next commit
            // rewrites orphan metadata for an offset that no longer
            // exists.
            let mut metadata = s.metadata.get(group_id).cloned().unwrap_or_default();
            let mut removed = HashMap::new();
            for k in keys {
                if offsets.remove(k).is_some() {
                    metadata.remove(k);
                    removed.insert(k.clone(), true);
                }
            }
            (offsets, metadata, removed, s.dirty.contains(group_id))
        };
        if !removed.is_empty() || dirty {
            self.write_and_cache(group_id, offsets, metadata)?;
        }
        Ok(removed)
    }

    /// Every group with an offset file on disk, whether or not this
    /// broker has materialised it in memory. A purge has to reach the
    /// un-materialised ones too: `load` pulls the file back into the
    /// cache the first time the group is touched, so anything left in
    /// the file comes straight back.
    pub fn group_ids_on_disk(&self) -> Vec<String> {
        let entries = match std::fs::read_dir(self.dir()) {
            Ok(rd) => rd,
            Err(_) => return Vec::new(),
        };
        entries
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .and_then(|n| n.strip_suffix(".json"))
                    .map(group_id_from_stem)
            })
            .collect()
    }

    /// Drop every committed offset belonging to `topic` from
    /// `group_id`, returning how many `(topic, partition)` entries went
    /// (gh #240).
    ///
    /// Apache tombstones a topic's offsets out of `__consumer_offsets`
    /// when the topic is deleted. Without that, a topic deleted and
    /// recreated under the same name hands the new incarnation the dead
    /// one's committed offsets — and if the new log is shorter, every
    /// consumer in the group parks past its end and idles forever, with
    /// no error raised on either side.
    ///
    /// A transaction's staged offsets on the topic go too, as in Apache's
    /// `GroupMetadata.removeOffsets`, or the commit would write them out afterwards
    /// (gh #335).
    pub fn purge_topic(&self, group_id: &str, topic: &str) -> io::Result<usize> {
        let _writer = self.write_lock(group_id);
        {
            let mut s = self.state.write();
            for (key, staged) in s.pending.iter_mut() {
                if key.group_id == group_id {
                    staged.offsets.retain(|k, _| !key_has_topic(k, topic));
                    staged.metadata.retain(|k, _| !key_has_topic(k, topic));
                }
            }
        }
        // Cached path. The cache only holds what reached the disk, so for a
        // group this broker coordinates it is the authority.
        let cached = {
            let s = self.state.read();
            s.cache.get(group_id).map(|g| {
                let mut offsets = g.clone();
                let mut metadata = s.metadata.get(group_id).cloned().unwrap_or_default();
                let doomed: Vec<String> = g
                    .keys()
                    .filter(|k| key_has_topic(k, topic))
                    .cloned()
                    .collect();
                for k in &doomed {
                    offsets.remove(k);
                    metadata.remove(k);
                }
                (doomed.len(), offsets, metadata)
            })
        };
        if let Some((n, offsets, metadata)) = cached {
            if n == 0 {
                return Ok(0);
            }
            self.write_and_cache(group_id, offsets, metadata)?;
            return Ok(n);
        }
        // Not in memory: rewrite the file in place. Deliberately does
        // NOT go through `load` — populating the cache here would make
        // `has_group` / `local_groups` report a group this broker never
        // materialised, which the heartbeat path feeds to the
        // controller's assignment loop.
        let Some(data) = self.read_group_file(group_id)? else {
            return Ok(0);
        };
        let (mut offsets, metadata) = decode_offsets_file(&data)?;
        let mut metadata = metadata.unwrap_or_default();
        let doomed: Vec<String> = offsets
            .keys()
            .filter(|k| key_has_topic(k, topic))
            .cloned()
            .collect();
        if doomed.is_empty() {
            return Ok(0);
        }
        for k in &doomed {
            offsets.remove(k);
            metadata.remove(k);
        }
        self.write_group(group_id, offsets, metadata)?;
        Ok(doomed.len())
    }

    /// The bytes of `group_id`'s file, or `None` if it has none. An id too long
    /// to have a file answers `None`: the read would fail with ENAMETOOLONG,
    /// which reads as a broken volume, and DeleteGroups would then answer a
    /// retriable error forever for a group that only ever joined.
    fn read_group_file(&self, group_id: &str) -> io::Result<Option<Vec<u8>>> {
        if !group_id_fits(group_id) {
            return Ok(None);
        }
        match std::fs::read(self.dir().join(group_file_name(group_id))) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Pull `group_id`'s file into the cache unless it is cached already.
    ///
    /// `load` only runs when a group joins here, so a group this broker
    /// coordinates but hasn't seen join since a restart or coordinator
    /// move — an idle group, a manual-assignment consumer, an admin
    /// tool — had no entry: fetches answered `-1`, and a commit merged
    /// into an empty entry and rewrote the file from it.
    ///
    /// Fills an absent entry only, re-checked under the write lock, so
    /// a commit racing the read is never overwritten by the older file:
    /// the cache is write-through, so whatever is cached is at least as
    /// new as the file. A missing file leaves the entry absent.
    fn ensure_loaded(&self, group_id: &str) -> io::Result<()> {
        if self.state.read().cache.contains_key(group_id) {
            return Ok(());
        }
        let Some(data) = self.read_group_file(group_id)? else {
            return Ok(());
        };
        let (offsets, metadata) = decode_offsets_file(&data)?;
        let mut s = self.state.write();
        if !s.cache.contains_key(group_id) {
            s.cache.insert(group_id.to_owned(), offsets);
            if let Some(m) = metadata {
                s.metadata.insert(group_id.to_owned(), m);
            }
        }
        Ok(())
    }

    /// [`Self::ensure_loaded`] for a caller that holds no lock. It takes the
    /// write lock if it has to read the file, or a cold fill could read the
    /// file, lose a race with a purge or a delete, and put back what they
    /// removed. The next commit would then write it out again (gh #318).
    ///
    /// An error means the group's offsets may exist but can't be read. The
    /// caller must not read that as "no offsets" (gh #326).
    pub fn check_readable(&self, group_id: &str) -> io::Result<()> {
        if self.state.read().cache.contains_key(group_id) {
            return Ok(());
        }
        let _writer = self.write_lock(group_id);
        self.ensure_loaded(group_id)
    }

    /// [`Self::check_readable`] for the read paths, which answer from whatever
    /// is cached if the file can't be read. Anything acting on the answer has
    /// checked first: see `Manager::group_load_error`.
    fn ensure_loaded_for_read(&self, group_id: &str) {
        if let Err(err) = self.check_readable(group_id) {
            tracing::warn!(group = group_id, %err, "offsets: could not load group file");
        }
    }

    /// Read a group's offsets from disk into the in-memory cache.
    /// Called when this broker becomes coordinator for the group.
    /// Tolerates both the gh #21 v2 envelope and the legacy v1 plain
    /// map.
    pub fn load(&self, group_id: &str) -> io::Result<()> {
        // This overwrites the cache, so a commit landing between the read and
        // the insert would be rolled back in memory, and by the next write on
        // disk too.
        let _writer = self.write_lock(group_id);
        let Some(data) = self.read_group_file(group_id)? else {
            return Ok(());
        };
        let (offsets, metadata) = decode_offsets_file(&data)?;
        let mut s = self.state.write();
        s.cache.insert(group_id.to_owned(), offsets);
        if let Some(m) = metadata {
            s.metadata.insert(group_id.to_owned(), m);
        }
        Ok(())
    }
}

/// Parse a `<group>.json` blob — the gh #21 v2 envelope only. The
/// legacy v1 plain-map fallback was dropped under the pre-v1
/// no-backcompat policy (every write since gh #21 emits v2).
type OffsetsAndMetadata = (HashMap<String, i64>, Option<HashMap<String, String>>);

fn decode_offsets_file(data: &[u8]) -> io::Result<OffsetsAndMetadata> {
    let v2: OffsetFileV2 = serde_json::from_slice(data).map_err(io::Error::other)?;
    let meta = if v2.metadata.is_empty() {
        None
    } else {
        Some(v2.metadata)
    };
    Ok((v2.offsets, meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &std::path::Path) -> OffsetStore {
        OffsetStore::new(dir)
    }

    fn one(topic: &str, partition: i32, offset: i64) -> HashMap<String, i64> {
        let mut m = HashMap::new();
        m.insert(offset_key(topic, partition), offset);
        m
    }

    #[test]
    fn commit_then_fetch_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 42)).unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0, 1],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&42));
        // unknown partition → -1 sentinel
        assert_eq!(got.get("t1/1"), Some(&-1));
    }

    #[test]
    fn commit_with_metadata_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut md = HashMap::new();
        md.insert(offset_key("t1", 0), "consumer-1".to_owned());
        s.commit_with_metadata("g1", one("t1", 0, 42), md).unwrap();
        let got_md = s.fetch_metadata(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got_md.get("t1/0").map(String::as_str), Some("consumer-1"));
    }

    #[test]
    fn empty_metadata_clears_entry() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut md = HashMap::new();
        md.insert(offset_key("t1", 0), "tag".to_owned());
        s.commit_with_metadata("g1", one("t1", 0, 1), md.clone())
            .unwrap();
        // Empty string clears it.
        md.insert(offset_key("t1", 0), String::new());
        s.commit_with_metadata("g1", one("t1", 0, 2), md).unwrap();
        let got_md = s.fetch_metadata(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert!(got_md.is_empty());
    }

    #[test]
    fn load_reads_v2_envelope_from_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let s1 = store(tmp.path());
        s1.commit("g1", one("t1", 0, 7)).unwrap();

        let s2 = store(tmp.path());
        s2.load("g1").unwrap();
        let got = s2.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&7));
    }

    #[test]
    fn delete_drops_cache_and_file() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 1)).unwrap();
        let path = tmp.path().join("__consumer_offsets/g1.json");
        assert!(path.exists());
        s.delete("g1").unwrap();
        assert!(!path.exists());
        assert!(!s.has_group("g1"));
        // Idempotent on missing group.
        s.delete("g1").unwrap();
    }

    #[test]
    fn delete_partitions_removes_only_requested_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut both = HashMap::new();
        both.insert(offset_key("t1", 0), 10);
        both.insert(offset_key("t1", 1), 20);
        s.commit("g1", both).unwrap();
        let removed = s
            .delete_partitions("g1", &[offset_key("t1", 0), offset_key("t1", 99)])
            .unwrap();
        assert_eq!(removed.len(), 1);
        assert_eq!(removed.get("t1/0"), Some(&true));
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0, 1],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
        assert_eq!(got.get("t1/1"), Some(&20));
    }

    #[test]
    fn pending_invisible_to_fetch_until_commit_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.store_pending("g1", 100, one("t1", 0, 555));
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
        s.commit_pending("g1", 100).unwrap();
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&555));
    }

    #[test]
    fn discard_pending_drops_unmaterialised_offsets() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.store_pending("g1", 100, one("t1", 0, 555));
        s.discard_pending("g1", 100);
        s.commit_pending("g1", 100).unwrap(); // no-op, idempotent
        let got = s.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1));
    }

    /// `delete_partitions` used to write the surviving offsets as a
    /// plain map, which is the pre-gh #21 v1 shape. `decode_offsets_file`
    /// takes v2 only and serde ignores unknown fields, so the reload
    /// didn't fail — it silently produced an EMPTY group, dropping every
    /// surviving offset the moment a coordinator loaded it.
    #[test]
    fn delete_partitions_survives_a_reload() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut both = HashMap::new();
        both.insert(offset_key("t1", 0), 10);
        both.insert(offset_key("t1", 1), 20);
        s.commit("g1", both).unwrap();
        s.delete_partitions("g1", &[offset_key("t1", 0)]).unwrap();

        let fresh = store(tmp.path());
        fresh.load("g1").unwrap();
        let got = fresh.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0, 1],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&-1), "deleted key came back");
        assert_eq!(got.get("t1/1"), Some(&20), "surviving offset was lost");
    }

    // --- gh #240 purge-on-topic-delete --------------------------------

    #[test]
    fn purge_topic_drops_only_that_topic() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut m = HashMap::new();
        m.insert(offset_key("gone", 0), 10);
        m.insert(offset_key("gone", 1), 11);
        m.insert(offset_key("stays", 0), 20);
        s.commit("g1", m).unwrap();

        assert_eq!(s.purge_topic("g1", "gone").unwrap(), 2);

        let fresh = store(tmp.path());
        fresh.load("g1").unwrap();
        let got = fresh.fetch(
            "g1",
            &[
                FetchSpec {
                    topic: "gone".to_owned(),
                    partitions: vec![0, 1],
                },
                FetchSpec {
                    topic: "stays".to_owned(),
                    partitions: vec![0],
                },
            ],
        );
        assert_eq!(got.get("gone/0"), Some(&-1));
        assert_eq!(got.get("gone/1"), Some(&-1));
        assert_eq!(got.get("stays/0"), Some(&20));
    }

    /// A coordinator that hasn't seen the group join since a restart or
    /// move must still read the committed offsets on disk, not `-1`.
    #[test]
    fn fetch_reads_a_group_never_materialised_here() {
        let tmp = tempfile::tempdir().unwrap();
        let mut md = HashMap::new();
        md.insert(offset_key("t1", 0), "m".to_owned());
        store(tmp.path())
            .commit_with_metadata("g1", one("t1", 0, 7), md)
            .unwrap();

        let fresh = store(tmp.path());
        let spec = [FetchSpec {
            topic: "t1".to_owned(),
            partitions: vec![0],
        }];
        assert_eq!(fresh.fetch("g1", &spec).get("t1/0"), Some(&7));
        assert_eq!(
            fresh
                .fetch_metadata("g1", &spec)
                .get("t1/0")
                .map(String::as_str),
            Some("m")
        );
    }

    /// The data-loss half of the cold cache: a commit merged into an
    /// empty entry and rewrote the file from it, erasing every offset
    /// it didn't name — `kafka-consumer-groups --reset-offsets --topic a`
    /// on an idle group wiped the group's offsets for every other topic.
    #[test]
    fn a_cold_commit_keeps_the_offsets_it_did_not_name() {
        let tmp = tempfile::tempdir().unwrap();
        let mut both = one("a", 0, 5);
        both.extend(one("b", 0, 9));
        store(tmp.path()).commit("g1", both).unwrap();

        store(tmp.path()).commit("g1", one("a", 0, 1)).unwrap();

        let after = store(tmp.path());
        after.load("g1").unwrap();
        let got = after.fetch(
            "g1",
            &[
                FetchSpec {
                    topic: "a".to_owned(),
                    partitions: vec![0],
                },
                FetchSpec {
                    topic: "b".to_owned(),
                    partitions: vec![0],
                },
            ],
        );
        assert_eq!(got.get("a/0"), Some(&1), "the commit lands");
        assert_eq!(got.get("b/0"), Some(&9), "the untouched topic survives");
    }

    #[test]
    fn delete_partitions_reaches_a_group_never_materialised_here() {
        let tmp = tempfile::tempdir().unwrap();
        let mut both = one("a", 0, 5);
        both.extend(one("b", 0, 9));
        store(tmp.path()).commit("g1", both).unwrap();

        let removed = store(tmp.path())
            .delete_partitions("g1", &[offset_key("a", 0)])
            .unwrap();
        assert_eq!(removed.get("a/0"), Some(&true));

        let after = store(tmp.path());
        after.load("g1").unwrap();
        let got = after.fetch(
            "g1",
            &[FetchSpec {
                topic: "b".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("b/0"), Some(&9));
    }

    /// The failure this whole fix exists for: the group has a file but
    /// this broker has never materialised it, so an in-memory-only purge
    /// misses it and `load` brings the stale offsets straight back.
    #[test]
    fn purge_topic_reaches_a_group_never_materialised_here() {
        let tmp = tempfile::tempdir().unwrap();
        store(tmp.path())
            .commit("g1", one("gone", 0, 51_607))
            .unwrap();

        let fresh = store(tmp.path());
        assert!(!fresh.has_group("g1"), "precondition: cold cache");
        assert_eq!(fresh.group_ids_on_disk(), vec!["g1".to_owned()]);
        assert_eq!(fresh.purge_topic("g1", "gone").unwrap(), 1);
        assert!(
            !fresh.has_group("g1"),
            "purge must not materialise the group — local_groups() feeds the controller"
        );

        let after = store(tmp.path());
        after.load("g1").unwrap();
        let got = after.fetch(
            "g1",
            &[FetchSpec {
                topic: "gone".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("gone/0"), Some(&-1));
    }

    #[test]
    fn purge_topic_drops_the_metadata_with_the_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut meta = HashMap::new();
        meta.insert(offset_key("gone", 0), "leader-epoch-3".to_owned());
        s.commit_with_metadata("g1", one("gone", 0, 10), meta)
            .unwrap();
        s.purge_topic("g1", "gone").unwrap();

        let fresh = store(tmp.path());
        fresh.load("g1").unwrap();
        assert!(fresh
            .fetch_metadata(
                "g1",
                &[FetchSpec {
                    topic: "gone".to_owned(),
                    partitions: vec![0],
                }]
            )
            .is_empty());
    }

    /// Keys split from the right, so a purge of `a` must not eat the
    /// offsets of a topic literally named `a/b`.
    #[test]
    fn purge_topic_splits_the_key_from_the_right() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let mut m = HashMap::new();
        m.insert(offset_key("a/b", 0), 10);
        m.insert(offset_key("a", 0), 20);
        s.commit("g1", m).unwrap();

        assert_eq!(s.purge_topic("g1", "a").unwrap(), 1);
        let got = s.fetch(
            "g1",
            &[
                FetchSpec {
                    topic: "a/b".to_owned(),
                    partitions: vec![0],
                },
                FetchSpec {
                    topic: "a".to_owned(),
                    partitions: vec![0],
                },
            ],
        );
        assert_eq!(got.get("a/b/0"), Some(&10), "sibling topic was eaten");
        assert_eq!(got.get("a/0"), Some(&-1));
    }

    #[test]
    fn purge_topic_is_a_noop_for_unknown_group_or_topic() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 10)).unwrap();
        assert_eq!(s.purge_topic("nosuchgroup", "t1").unwrap(), 0);
        assert_eq!(s.purge_topic("g1", "nosuchtopic").unwrap(), 0);
        // Idempotent: re-driven by the relist retraction path.
        assert_eq!(s.purge_topic("g1", "t1").unwrap(), 1);
        assert_eq!(s.purge_topic("g1", "t1").unwrap(), 0);
    }

    #[test]
    fn group_ids_on_disk_is_empty_without_a_directory() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(store(tmp.path()).group_ids_on_disk().is_empty());
    }

    // --- gh #167 handoff fence + write amplification ------------------

    fn stamp(epoch: i64, version: i64) -> FenceStamp {
        FenceStamp {
            controller_epoch: epoch,
            assignment_version: version,
        }
    }

    /// The gh #167 race: the old coordinator's whole-map rewrite from
    /// its stale cache must not erase commits the new coordinator has
    /// taken. The loser gets a distinguishable error (mapped to
    /// NOT_COORDINATOR upstream) and the winner's file survives.
    #[test]
    fn a_stale_coordinators_write_is_fenced_and_the_winners_offsets_survive() {
        let tmp = tempfile::tempdir().unwrap();

        // Old coordinator at assignment (3, 5) commits — its cache
        // holds t1/0=10.
        let old = store(tmp.path());
        old.set_fence_source(|| stamp(3, 5));
        old.commit("g1", one("t1", 0, 10)).unwrap();

        // Handoff: the new coordinator (same controller reign, later
        // assignment version) loads and commits fresh work.
        let new = store(tmp.path());
        new.set_fence_source(|| stamp(3, 6));
        new.load("g1").unwrap();
        new.commit("g1", one("t1", 0, 99)).unwrap();

        // The laggard, still on (3, 5), rewrites from its stale cache.
        let err = old.commit("g1", one("t1", 1, 11)).unwrap_err();
        assert!(
            err.get_ref()
                .is_some_and(|inner| inner.is::<FencedOffsetWrite>()),
            "expected FencedOffsetWrite, got {err:?}"
        );

        // Winner's commit is intact on disk.
        let fresh = store(tmp.path());
        fresh.load("g1").unwrap();
        let got = fresh.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&99), "stale write clobbered the file");
    }

    /// Controller failover orders across reigns: epoch outranks
    /// version.
    #[test]
    fn fence_orders_controller_epoch_before_version() {
        assert!(stamp(4, 0) > stamp(3, 999));
        assert!(stamp(3, 6) > stamp(3, 5));
        assert_eq!(stamp(0, 0), FenceStamp::default());
    }

    /// Pre-gh #167 files carry no stamp and decode as zero — any
    /// current coordinator may overwrite them (the rolling-upgrade
    /// path).
    #[test]
    fn an_unstamped_legacy_file_is_writable_by_any_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let legacy = store(tmp.path()); // no fence source → zero stamp
        legacy.commit("g1", one("t1", 0, 5)).unwrap();

        let s = store(tmp.path());
        s.set_fence_source(|| stamp(7, 1));
        s.load("g1").unwrap();
        s.commit("g1", one("t1", 0, 6)).unwrap();
    }

    /// gh #167 write amplification: an idle consumer re-committing
    /// identical offsets must not rewrite the file.
    #[test]
    fn an_unchanged_commit_skips_the_disk_write() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g1", one("t1", 0, 42)).unwrap();

        // Remove the file behind the store's back; an unchanged commit
        // must not bring it back (proof the write was skipped).
        let path = tmp.path().join("__consumer_offsets/g1.json");
        std::fs::remove_file(&path).unwrap();
        s.commit("g1", one("t1", 0, 42)).unwrap();
        assert!(!path.exists(), "unchanged commit still rewrote the file");

        // A moved offset writes again.
        s.commit("g1", one("t1", 0, 43)).unwrap();
        assert!(path.exists());
    }

    /// The skip must not fire while the cache is ahead of disk — a
    /// failed write marks the group dirty until a write succeeds.
    #[test]
    fn a_failed_write_disables_the_skip_until_a_write_lands() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        // Make the directory un-writable by occupying its path with a
        // file, so the first commit fails to create it.
        std::fs::write(tmp.path().join("__consumer_offsets"), b"x").unwrap();
        s.commit("g1", one("t1", 0, 42)).unwrap_err();

        // Heal the path. The identical re-commit must WRITE (dirty),
        // not skip.
        std::fs::remove_file(tmp.path().join("__consumer_offsets")).unwrap();
        s.commit("g1", one("t1", 0, 42)).unwrap();
        let fresh = store(tmp.path());
        fresh.load("g1").unwrap();
        let got = fresh.fetch(
            "g1",
            &[FetchSpec {
                topic: "t1".to_owned(),
                partitions: vec![0],
            }],
        );
        assert_eq!(got.get("t1/0"), Some(&42), "dirty group skipped its retry");
    }

    /// Make the offsets directory unusable by moving it aside and occupying
    /// its path with a file. Writes and unlinks both fail, as for root too,
    /// which a permission bit wouldn't manage. Returns a closure that heals it.
    fn break_offsets_dir(root: &std::path::Path) -> impl FnOnce() + '_ {
        let dir = root.join("__consumer_offsets");
        let aside = root.join("aside");
        std::fs::rename(&dir, &aside).unwrap();
        std::fs::write(&dir, b"x").unwrap();
        move || {
            std::fs::remove_file(&dir).unwrap();
            std::fs::rename(&aside, &dir).unwrap();
        }
    }

    fn fetch_one(s: &OffsetStore, group: &str) -> Option<i64> {
        let spec = FetchSpec {
            topic: "t1".to_owned(),
            partitions: vec![0],
        };
        s.fetch(group, std::slice::from_ref(&spec))
            .get("t1/0")
            .copied()
            .filter(|o| *o >= 0)
    }

    /// gh #318: after a failed commit a fetch answers what's on disk, not the
    /// value the client was told failed. It used to answer the unwritten
    /// value, and the old one after a restart.
    #[test]
    fn a_failed_commit_leaves_fetches_on_what_is_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g", one("t1", 0, 100)).unwrap();

        let heal = break_offsets_dir(tmp.path());
        s.commit("g", one("t1", 0, 200)).unwrap_err();
        assert_eq!(fetch_one(&s, "g"), Some(100));

        // A retry of the same value writes it: the group is dirty, and the
        // cache hasn't got it anyway.
        heal();
        s.commit("g", one("t1", 0, 200)).unwrap();
        assert_eq!(fetch_one(&s, "g"), Some(200));
        let fresh = store(tmp.path());
        assert_eq!(fetch_one(&fresh, "g"), Some(200));
    }

    /// gh #318: a write that failed after its rename can leave the file ahead
    /// of the cache. A later commit of what the cache holds must still write,
    /// or the file would keep the value the cache doesn't have.
    #[test]
    fn a_failed_write_disables_the_skip_even_for_what_the_cache_holds() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g", one("t1", 0, 100)).unwrap();
        let heal = break_offsets_dir(tmp.path());
        s.commit("g", one("t1", 0, 200)).unwrap_err();
        heal();
        // What a rename that landed before its failed sync would leave.
        OffsetStore::new(tmp.path())
            .commit("g", one("t1", 0, 200))
            .unwrap();

        s.commit("g", one("t1", 0, 100)).unwrap();
        assert_eq!(fetch_one(&store(tmp.path()), "g"), Some(100));
    }

    /// gh #318: staged offsets stay staged until their commit is durable, so
    /// the retry has something to commit.
    #[test]
    fn a_failed_commit_pending_keeps_the_offsets_staged() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g", one("t1", 0, 100)).unwrap();
        s.store_pending("g", 7, one("t1", 0, 555));

        let heal = break_offsets_dir(tmp.path());
        s.commit_pending("g", 7).unwrap_err();
        assert_eq!(fetch_one(&s, "g"), Some(100));
        assert!(s.pending_for("g", 7).is_some(), "staged offsets were lost");

        heal();
        s.commit_pending("g", 7).unwrap();
        assert!(s.pending_for("g", 7).is_none());
        assert_eq!(fetch_one(&store(tmp.path()), "g"), Some(555));
    }

    /// gh #318: a failed delete changes nothing, so the answer can be honest
    /// and the retry finds the group as it was. Both kinds of delete.
    #[test]
    fn a_failed_delete_leaves_the_group_as_it_was() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        s.commit("g", one("t1", 0, 100)).unwrap();

        let heal = break_offsets_dir(tmp.path());
        s.delete("g").unwrap_err();
        assert_eq!(fetch_one(&s, "g"), Some(100));
        assert!(s.has_offsets("g"));
        let keys = [offset_key("t1", 0)];
        s.delete_partitions("g", &keys).unwrap_err();
        assert_eq!(fetch_one(&s, "g"), Some(100));
        s.purge_topic("g", "t1").unwrap_err();
        assert_eq!(fetch_one(&s, "g"), Some(100));
        heal();

        // And it all reaches the file once it can.
        let removed = s.delete_partitions("g", &keys).unwrap();
        assert_eq!(removed.get(&keys[0]), Some(&true));
        assert_eq!(fetch_one(&store(tmp.path()), "g"), None);
        s.commit("g", one("t1", 0, 100)).unwrap();
        s.delete("g").unwrap();
        assert!(!s.has_offsets("g"));
        assert_eq!(fetch_one(&store(tmp.path()), "g"), None);
    }

    /// gh #326: staged metadata is committed with its offset, and an empty value
    /// clears what was there, as for a plain commit.
    #[test]
    fn staged_metadata_is_committed_with_its_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        let spec = FetchSpec {
            topic: "t1".to_owned(),
            partitions: vec![0],
        };
        let metadata = |s: &OffsetStore| {
            s.fetch_metadata("g", std::slice::from_ref(&spec))
                .get("t1/0")
                .cloned()
        };
        let stage = |value: &str| {
            s.store_pending_with_metadata(
                "g",
                7,
                one("t1", 0, 5),
                HashMap::from([(offset_key("t1", 0), value.to_owned())]),
            );
            s.commit_pending("g", 7).unwrap();
        };
        stage("txn");
        assert_eq!(metadata(&s).as_deref(), Some("txn"));
        assert_eq!(metadata(&store(tmp.path())).as_deref(), Some("txn"));
        stage("");
        assert_eq!(metadata(&s), None);
        assert_eq!(metadata(&store(tmp.path())), None);
    }

    /// gh #318: an id too long to have a file isn't a broken volume. Every
    /// path that reads or removes the file answers "nothing there", or
    /// DeleteGroups would answer a retriable error forever.
    #[test]
    fn a_group_id_too_long_for_a_file_has_nothing_on_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let s = store(tmp.path());
        // The directory has to exist, or these answer ENOENT before the name's
        // length matters.
        s.commit("g", one("t1", 0, 1)).unwrap();
        // Long enough for ENAMETOOLONG on the read, not just on the tmp file.
        let long = "a".repeat(300);
        assert!(!group_id_fits(&long));
        assert!(group_id_fits("g"));

        assert_eq!(fetch_one(&s, &long), None);
        assert!(!s.has_offsets(&long));
        s.load(&long).unwrap();
        assert_eq!(s.purge_topic(&long, "t1").unwrap(), 0);
        let keys = [offset_key("t1", 0)];
        assert!(s.delete_partitions(&long, &keys).unwrap().is_empty());
        s.delete(&long).unwrap();
    }

    /// gh #318: a cold fill reads the file and then fills the cache, and a
    /// purge landing in between would be undone, and written back by the next
    /// commit. So a fill waits for the group's writer. A fetch that finds the
    /// group cached doesn't, or a slow fsync would stall every read.
    #[test]
    fn a_cold_fill_waits_for_the_groups_writer_and_a_cached_read_does_not() {
        let tmp = tempfile::tempdir().unwrap();
        store(tmp.path()).commit("g", one("t1", 0, 100)).unwrap();

        type ColdRead = fn(&OffsetStore);
        let cold_reads: [ColdRead; 2] = [
            |s| {
                let _ = fetch_one(s, "g");
            },
            |s| s.load("g").unwrap(),
        ];
        for cold_read in cold_reads {
            let s = std::sync::Arc::new(store(tmp.path()));
            let writer = s.write_lock("g");
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let reader = {
                let s = s.clone();
                std::thread::spawn(move || {
                    cold_read(&s);
                    done_tx.send(()).unwrap();
                })
            };
            assert!(
                done_rx
                    .recv_timeout(std::time::Duration::from_millis(200))
                    .is_err(),
                "a cold fill didn't wait for the writer"
            );
            drop(writer);
            reader.join().unwrap();

            // Now cached, so the same read goes straight through. On a thread,
            // so a read that waits fails this rather than deadlocking the test.
            let writer = s.write_lock("g");
            let (done_tx, done_rx) = std::sync::mpsc::channel();
            let reader = {
                let s = s.clone();
                std::thread::spawn(move || done_tx.send(fetch_one(&s, "g")).unwrap())
            };
            let got = done_rx.recv_timeout(std::time::Duration::from_secs(5));
            drop(writer);
            reader.join().unwrap();
            assert_eq!(got, Ok(Some(100)), "a cached read waited for the writer");
        }
    }

    /// gh #318: the cache now moves after the write, so a group's writers are
    /// serialised. Otherwise each merges from the same old cache and the last
    /// write drops the others' offsets. Each thread commits its own partition,
    /// so all of them must survive.
    #[test]
    fn concurrent_commits_to_a_group_keep_each_others_offsets() {
        let tmp = tempfile::tempdir().unwrap();
        let s = std::sync::Arc::new(store(tmp.path()));
        let threads: Vec<_> = (0..8)
            .map(|p| {
                let s = s.clone();
                std::thread::spawn(move || s.commit("g", one("t1", p, 1)).unwrap())
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let fresh = store(tmp.path());
        let spec = FetchSpec {
            topic: "t1".to_owned(),
            partitions: (0..8).collect(),
        };
        let got = fresh.fetch("g", std::slice::from_ref(&spec));
        assert_eq!(got.values().filter(|o| **o == 1).count(), 8, "{got:?}");
    }

    /// gh #314: a group id is any string, and it becomes a file name. A
    /// traversal must stay inside the offsets directory, leave what it aimed
    /// at alone, and still work as a group: offsets read back, also from a
    /// fresh store off disk, and it is listed and deleted under its own id.
    #[test]
    fn a_group_id_cannot_escape_the_offsets_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let acls = tmp.path().join("acls.json");
        std::fs::write(&acls, b"the real acls").unwrap();
        for id in ["../acls", "../../x", "a/b", "/abs", "100%", "%2F", "a\0b"] {
            let s = OffsetStore::new(tmp.path());
            s.commit(id, HashMap::from([(offset_key("t", 0), 7)]))
                .unwrap();
            let spec = [FetchSpec {
                topic: "t".to_owned(),
                partitions: vec![0],
            }];
            let fresh = OffsetStore::new(tmp.path());
            fresh.load(id).unwrap();
            assert_eq!(
                fresh.fetch(id, &spec).get(&offset_key("t", 0)),
                Some(&7),
                "{id:?}"
            );
            assert!(fresh.group_ids_on_disk().iter().any(|g| g == id), "{id:?}");
            fresh.delete(id).unwrap();
            assert!(!OffsetStore::new(tmp.path()).has_offsets(id), "{id:?}");
        }
        assert_eq!(std::fs::read(&acls).unwrap(), b"the real acls");
        let stray: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name())
            .filter(|n| n != "acls.json" && n != "__consumer_offsets")
            .collect();
        assert!(stray.is_empty(), "{stray:?}");
    }

    /// gh #314: an ordinary group keeps its file, so an upgrade finds its
    /// offsets where they were.
    #[test]
    fn ordinary_group_ids_keep_their_file_name() {
        for id in ["g1", "my group", "app:1", "a.b-c_d", "caf\u{e9}", ".hidden"] {
            assert_eq!(group_file_name(id), format!("{id}.json"));
            assert_eq!(group_id_from_stem(id), id);
        }
    }
}
