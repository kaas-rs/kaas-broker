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

use parking_lot::RwLock;
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
    pending: HashMap<PendingKey, HashMap<String, i64>>,
    /// Groups whose cache may be ahead of disk (last write failed or
    /// was fenced). The gh #167 skip-unchanged fast path must not
    /// fire for these — an "unchanged" commit is only unchanged
    /// relative to the cache, and here the cache isn't persisted.
    dirty: std::collections::HashSet<String>,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
struct PendingKey {
    group_id: String,
    producer_id: i64,
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
}

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
        }
    }

    /// Install the gh #167 fence source. Every subsequent disk write
    /// is stamped with the assignment it was made under and refuses
    /// to overwrite a file stamped by a newer one.
    pub fn set_fence_source(&self, src: impl Fn() -> FenceStamp + Send + Sync + 'static) {
        *self.fence_source.write() = Some(Box::new(src));
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
        let path = self.dir().join(format!("{group_id}.json"));
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
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        let mut s = self.state.write();
        let slot = s.pending.entry(key).or_default();
        for (k, v) in offsets {
            slot.insert(k, v);
        }
    }

    /// Materialise the staged offsets for `(group, pid)` as committed.
    /// Called from the `EndTxn(commit)` handler. Idempotent.
    pub fn commit_pending(&self, group_id: &str, producer_id: i64) -> io::Result<()> {
        let key = PendingKey {
            group_id: group_id.to_owned(),
            producer_id,
        };
        let pending = {
            let mut s = self.state.write();
            s.pending.remove(&key)
        };
        match pending {
            None => Ok(()),
            Some(offsets) => self.commit(group_id, offsets),
        }
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
        self.state.read().pending.get(&key).cloned()
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
        // The merge below rewrites the whole file from the cache entry,
        // so an entry that doesn't hold the file yet would erase every
        // offset this commit doesn't name.
        self.ensure_loaded(group_id)?;
        let (merged_offsets, merged_meta, changed) = {
            let mut s = self.state.write();
            let mut changed = false;
            let cached = s.cache.entry(group_id.to_owned()).or_default();
            for (k, v) in offsets {
                if cached.insert(k, v) != Some(v) {
                    changed = true;
                }
            }
            let cached_meta = s.metadata.entry(group_id.to_owned()).or_default();
            for (k, v) in metadata {
                if v.is_empty() {
                    changed |= cached_meta.remove(&k).is_some();
                } else if cached_meta.insert(k, v.clone()) != Some(v) {
                    changed = true;
                }
            }
            let off = s.cache.get(group_id).cloned().unwrap_or_default();
            let meta = s.metadata.get(group_id).cloned().unwrap_or_default();
            (off, meta, changed || s.dirty.contains(group_id))
        };

        // gh #167 (write amplification): an idle consumer re-commits
        // the same offsets every interval; when the merge moved
        // nothing the on-disk file is already right, so skip the
        // rewrite entirely. Correct because the cache is write-through
        // — everything in it was persisted by a previous successful
        // write, and the dirty set (folded into `changed` above)
        // excludes the groups where that's not true.
        if !changed {
            return Ok(());
        }

        self.write_group(group_id, merged_offsets, merged_meta)
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
        let name = format!("{group_id}.json");
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
        {
            let mut s = self.state.write();
            s.cache.remove(group_id);
            s.metadata.remove(group_id);
            s.dirty.remove(group_id);
        }
        let path = self.dir().join(format!("{group_id}.json"));
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
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
        let mut removed = HashMap::new();
        self.ensure_loaded(group_id)?;
        let (snap_offsets, snap_meta) = {
            let mut s = self.state.write();
            let group = match s.cache.get_mut(group_id) {
                None => return Ok(removed),
                Some(g) => g,
            };
            for k in keys {
                if group.remove(k).is_some() {
                    removed.insert(k.clone(), true);
                }
            }
            let offsets = group.clone();
            // The metadata map mirrors the offset map; a key dropped
            // from one must go from the other or the next commit
            // rewrites orphan metadata for an offset that no longer
            // exists.
            if let Some(m) = s.metadata.get_mut(group_id) {
                for k in removed.keys() {
                    m.remove(k);
                }
            }
            let meta = s.metadata.get(group_id).cloned().unwrap_or_default();
            (offsets, meta)
        };
        self.write_group(group_id, snap_offsets, snap_meta)?;
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
                    .map(str::to_owned)
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
    pub fn purge_topic(&self, group_id: &str, topic: &str) -> io::Result<usize> {
        // Cached path. The cache is write-through on every commit, so
        // for a group this broker coordinates it is the authority.
        let cached = {
            let mut s = self.state.write();
            match s.cache.get_mut(group_id) {
                None => None,
                Some(g) => {
                    let doomed: Vec<String> = g
                        .keys()
                        .filter(|k| key_has_topic(k, topic))
                        .cloned()
                        .collect();
                    for k in &doomed {
                        g.remove(k);
                    }
                    let offsets = g.clone();
                    if let Some(m) = s.metadata.get_mut(group_id) {
                        for k in &doomed {
                            m.remove(k);
                        }
                    }
                    let meta = s.metadata.get(group_id).cloned().unwrap_or_default();
                    Some((doomed.len(), offsets, meta))
                }
            }
        };
        if let Some((n, offsets, metadata)) = cached {
            if n == 0 {
                return Ok(0);
            }
            self.write_group(group_id, offsets, metadata)?;
            return Ok(n);
        }
        // Not in memory: rewrite the file in place. Deliberately does
        // NOT go through `load` — populating the cache here would make
        // `has_group` / `local_groups` report a group this broker never
        // materialised, which the heartbeat path feeds to the
        // controller's assignment loop.
        let path = self.dir().join(format!("{group_id}.json"));
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
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
        let path = self.dir().join(format!("{group_id}.json"));
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
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

    /// [`Self::ensure_loaded`] for the read paths, which answer from
    /// whatever is cached if the file can't be read. Writers propagate
    /// the error instead: rewriting a file they couldn't read would
    /// erase it.
    fn ensure_loaded_for_read(&self, group_id: &str) {
        if let Err(err) = self.ensure_loaded(group_id) {
            tracing::warn!(group = group_id, %err, "offsets: could not load group file");
        }
    }

    /// Read a group's offsets from disk into the in-memory cache.
    /// Called when this broker becomes coordinator for the group.
    /// Tolerates both the gh #21 v2 envelope and the legacy v1 plain
    /// map.
    pub fn load(&self, group_id: &str) -> io::Result<()> {
        let path = self.dir().join(format!("{group_id}.json"));
        let data = match std::fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
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
}
