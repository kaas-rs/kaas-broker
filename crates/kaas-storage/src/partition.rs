//! Per-partition write path: mutex-guarded state, lock-free read snapshot,
//! per-partition committer task.
//!
//! Per-partition write path plus its committer task. One [`Partition`] per
//! `(topic, partition)` tuple; the engine layer ([`crate::engine`]) holds
//! a `DashMap<(String, i32), Arc<Partition>>` and routes calls.
//!
//! # Concurrency model
//!
//! - `Mutex<PartitionInner>` for the **write path**. Append, segment roll,
//!   manifest persist, producer-state mutation — all under this mutex.
//! - `ArcSwap<ReadSnapshot>` for the **read-path observation channel** —
//!   `high_watermark()` / `log_start_offset()` / `epoch()` read without
//!   ever blocking on the mutex. Preserves the gh #134 fix that kept the
//!   OTel gauge alive when a stuck NAS fsync held the partition lock for
//!   the watchdog deadline.
//! - One [`tokio::task`] per partition (the **committer**) that drains
//!   flush requests via an `mpsc::Receiver`. Each cycle runs
//!   `active.sync_log()` under `tokio::time::timeout(FsyncMaxLatency)`,
//!   updates `completed_flush_seq`, and wakes `acks=all` appenders via
//!   [`tokio::sync::Notify`].
//!
//! # Phase 2 initial slice — what's NOT here yet
//!
//! Group-commit (gh #82) — the optimization where the committer fsyncs
//! outside the partition mutex via a cloned log FD — is **not** in this
//! commit. The committer takes the inner mutex for the entire fsync
//! window, which serializes concurrent appenders for the fsync duration.
//! On tmpfs / local SSD that's fine; on NFS it loses the multi-appender
//! throughput multiplier. A follow-up commit on the same gh #156 issue
//! adds the FD-clone trick once the basic Partition is verified
//! correct.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use bytes::Bytes;
use parking_lot::Mutex;
use tokio::sync::{mpsc, Notify};
use tokio::task::JoinHandle;

use crate::errors::StorageError;
use crate::fs::Fs;
use crate::idempotence::{
    self, parse_batch_producer_info, parse_batch_txn_info, BatchProducerInfo, Outcome,
    ProducerEntry,
};
use crate::manifest::{self, Manifest, ReadResult};
use crate::producer_snapshot::{
    read_producer_snapshot, write_producer_snapshot, PRODUCER_SNAPSHOT_FILENAME,
};
use crate::recovery_checkpoint::{self, RecoveryCheckpoint, RECOVERY_CHECKPOINT_FILENAME};
use crate::segment::{self, parse_batch_offsets, ActiveSegment, SegmentMeta};
use crate::txn_index::{AbortedTxn, AbortedTxnIndex, OpenTxnIndex};
use crate::txn_index_snapshot::{
    read_txn_index_snapshot, write_txn_index_snapshot, TXN_INDEX_SNAPSHOT_FILENAME,
};

/// gh #recovery-checkpoint: the committer writes a fresh recovery
/// checkpoint once the fsynced log has grown this far past the last one.
/// Bounds crash-recovery scan to at most this many bytes; a clean close
/// checkpoints at EOF regardless, so graceful restarts re-scan nothing.
/// 64 MiB keeps checkpoint writes rare (one per 64 MiB of throughput)
/// while capping the worst-case re-scan at ~64 MiB.
const CHECKPOINT_INTERVAL_BYTES: i64 = 64 * 1024 * 1024;

/// Per-partition tuning knobs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionConfig {
    /// Roll the active segment at this size. Default 1 GiB matches
    /// Apache's `segment.bytes`.
    pub segment_bytes: u64,
    /// Roll the active segment once it has been open this long, even
    /// if it never reaches `segment_bytes`. Apache's `segment.ms`,
    /// same 7-day default. `None` = size-driven rolls only.
    ///
    /// This is what makes time-based retention work on a low-volume
    /// topic: retention only ever deletes *closed* segments, so
    /// without a time-driven roll a topic that never fills 1 GiB
    /// keeps everything forever no matter what `retention.ms` says.
    pub segment_ms: Option<u64>,
    /// Emit one sparse index entry per N bytes of log. Default 4 KiB
    /// matches Apache's `index.interval.bytes`.
    pub index_interval_bytes: u64,
    /// `log.flush.interval.messages`. Default 1 = honest acks=all on
    /// every batch; raise to amortise fsync cost over more records.
    pub flush_interval_messages: i64,
    /// Watchdog deadline for one log fsync (gh #95): a commit cycle
    /// that stalls longer fails the append instead of wedging the
    /// partition. `None` disables the watchdog (the operator's
    /// substrate is slower than any sane deadline). Default 30 s;
    /// tune via `KAAS_FSYNC_MAX_LATENCY_MS` (gh #256), `0` = None.
    pub fsync_max_latency: Option<Duration>,
}

impl Default for PartitionConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 1 << 30,
            segment_ms: Some(7 * 24 * 60 * 60 * 1000),
            index_interval_bytes: 4096,
            flush_interval_messages: 1,
            fsync_max_latency: Some(Duration::from_secs(30)),
        }
    }
}

/// Lock-free observation channel. Updated under the partition mutex
/// alongside every HWM / log_start / segment-roll change; readable
/// without taking the mutex.
#[derive(Debug, Clone)]
pub struct ReadSnapshot {
    pub high_water: i64,
    pub log_start: i64,
    pub epoch: i64,
    /// Sorted oldest-first.
    pub closed: Arc<Vec<SegmentMeta>>,
    pub active_meta: SegmentMeta,
}

/// Blocking half of [`Partition::read`] — runs on a `spawn_blocking`
/// thread (gh #209).
///
/// Two things make this O(bytes returned) rather than O(bytes in the
/// partition), and both were missing before gh #209:
///
/// 1. **Segments that cannot contain `effective_start` are skipped**
///    outright. A closed segment covers `[base_offset, next_base)`,
///    where `next_base` is the following segment's base — or the
///    active segment's base for the last closed one.
/// 2. **The offset index picks the start position.** `search_index`
///    returns a file position at or before the target offset; passing
///    `0` instead (the old behaviour) made `read_batches` walk the
///    segment from byte zero. The `.index` files were written and
///    fsynced on the append path and then never read.
fn read_snapshot(
    fs: &dyn Fs,
    snap: &ReadSnapshot,
    start_offset: i64,
    max_bytes: usize,
) -> Result<Bytes, StorageError> {
    let effective_start = start_offset.max(snap.log_start);
    let mut out = bytes::BytesMut::new();

    for (i, seg) in snap.closed.iter().enumerate() {
        if out.len() >= max_bytes {
            break;
        }
        let next_base = snap
            .closed
            .get(i + 1)
            .map_or(snap.active_meta.base_offset, |n| n.base_offset);
        if next_base <= effective_start {
            continue;
        }
        let pos = segment::search_index(fs, &seg.index_path, seg.base_offset, effective_start);
        let chunk = segment::read_batches(
            fs,
            &seg.log_path,
            pos,
            effective_start,
            max_bytes.saturating_sub(out.len()),
        )?;
        out.extend_from_slice(&chunk);
    }

    if out.len() < max_bytes {
        let pos = segment::search_index(
            fs,
            &snap.active_meta.index_path,
            snap.active_meta.base_offset,
            effective_start,
        );
        let chunk = segment::read_batches(
            fs,
            &snap.active_meta.log_path,
            pos,
            effective_start,
            max_bytes.saturating_sub(out.len()),
        )?;
        out.extend_from_slice(&chunk);
    }

    // Cap to max_bytes (read_batches may overshoot the last batch).
    if out.len() > max_bytes {
        out.truncate(max_bytes);
    }

    Ok(out.freeze())
}

/// What [`open_blocking`] recovers off disk, handed back to
/// [`Partition::open`] to assemble into a [`Partition`].
struct OpenedState {
    epoch: i64,
    hwm: i64,
    log_start: i64,
    closed: Vec<SegmentMeta>,
    active: ActiveSegment,
    producer_states: HashMap<i64, ProducerEntry>,
    open_txns: OpenTxnIndex,
    aborted_txns: AbortedTxnIndex,
}

/// Blocking prologue of [`Partition::open`] — runs on a
/// `spawn_blocking` thread (gh #210).
///
/// Every call in here is synchronous `std::io` against the partition
/// directory, and `scan_high_watermark` walks the entire active
/// segment (up to `segment.bytes`, 1 GiB by default) to find the true
/// high-watermark. That walk is inherent — the log is authoritative
/// and the manifest only best-effort — so unlike the gh #209 read
/// path there is no index to shortcut it. What was wrong was *where*
/// it ran: on a scheduler worker, it took brokers off the air at
/// every takeover. With `limits.cpu: 2` tokio sizes its pool to two
/// workers, so one NFS-backed scan starved the runtime long enough to
/// fail the readiness probe — which dropped the broker from the
/// controller's alive set, triggered reassignment, and caused *more*
/// takeovers (gh #208).
/// Number of [`open_blocking`] attempts before an ENOENT is surfaced.
const OPEN_ENOENT_ATTEMPTS: u32 = 5;
/// First backoff between those attempts; doubles each time (50 + 100 +
/// 200 + 400 = 750 ms total).
const OPEN_ENOENT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(50);

/// [`open_blocking`], retried while the directory keeps vanishing under
/// it (gh #220).
///
/// `open_blocking` starts with `mkdir_all`, so an ENOENT after that
/// point never means "this path is wrong" — it means someone removed the
/// tree between our mkdir and our first file open. In practice that
/// someone is the operator reclaiming a previous incarnation of the
/// topic: it renames the live directory aside and re-creates it, and an
/// open landing inside that window fails on a path that is about to
/// exist again.
///
/// Retrying is the right response because the whole prologue is
/// idempotent — it re-creates the directory, re-lists segments, and
/// re-scans. Failing instead pushes the problem onto callers that can
/// only translate it into a produce error or a partition that stays
/// closed until the next takeover reconcile.
fn open_blocking_retrying(fs: &dyn Fs, dir: &std::path::Path) -> Result<OpenedState, StorageError> {
    let mut backoff = OPEN_ENOENT_BACKOFF;
    for _ in 1..OPEN_ENOENT_ATTEMPTS {
        match open_blocking(fs, dir) {
            // Silent by design — this crate carries no logger, and the
            // caller already logs the error if every attempt fails.
            Err(StorageError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                std::thread::sleep(backoff);
                backoff *= 2;
            }
            other => return other,
        }
    }
    // Last attempt: whatever it returns is the answer.
    open_blocking(fs, dir)
}

fn open_blocking(fs: &dyn Fs, dir: &std::path::Path) -> Result<OpenedState, StorageError> {
    fs.mkdir_all(dir)?;

    // Manifest is the source of truth for (epoch, hwm, log_start)
    // when present; segment scan is the fallback.
    let manifest_read = manifest::read(fs, dir).map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;
    let manifest_present = !matches!(manifest_read, ReadResult::NotFound);
    let (epoch, hwm, log_start) = match manifest_read {
        ReadResult::Present(m) => (m.epoch, m.high_watermark, m.log_start_offset),
        ReadResult::Legacy(m) => (m.epoch, 0, 0),
        ReadResult::NotFound => (0, 0, 0),
    };

    // Load any existing segments.
    let mut closed = segment::list_segments(fs, dir)?;

    // gh #229: a manifest with no segments behind it describes a log
    // that is gone — a dead incarnation's relinquish landing in a
    // recreated dir, a partial restore, a half-finished
    // `remove_dir_all`. Basing the new segment at its HWM advertised
    // offsets no record backs, silently. Start over at log start the way
    // Apache does (`LogLoader` bases the new segment at the log-start
    // checkpoint and the HWM clamps to the log end), and drop the files
    // that describe the lost records: the snapshots would restore a
    // dedupe window and open-txn entries for batches that no longer
    // exist, and the recovery checkpoint would match the new segment as
    // soon as it grew past `byte_pos`, seeding the next recovery from
    // the dead log's HWM (gh #230). They go before the segment is
    // created, so a crash in between re-takes this branch next open.
    if closed.is_empty() && manifest_present {
        if hwm > log_start {
            tracing::warn!(
                dir = %dir.display(),
                manifest_high_watermark = hwm,
                log_start,
                "partition manifest claims records but no segments exist; \
                 resetting to log start (gh #229)"
            );
        }
        for name in [
            PRODUCER_SNAPSHOT_FILENAME,
            TXN_INDEX_SNAPSHOT_FILENAME,
            RECOVERY_CHECKPOINT_FILENAME,
        ] {
            match fs.remove(&dir.join(name)) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e.into()),
                _ => {}
            }
        }
    }

    // The active segment is the last one (highest base_offset);
    // if none exist yet, create at log start (0 for a fresh
    // partition) with our current epoch.
    let mut active = if let Some(last) = closed.pop() {
        let mut a = ActiveSegment::open_meta_only(last);
        a.open_handles(fs)?;
        a
    } else {
        ActiveSegment::create(fs, dir, log_start, epoch)?
    };

    // Recover the high-watermark by scanning the active segment
    // forward, stopping at the first torn batch. The log is
    // authoritative; the manifest HWM is best-effort and the scan
    // result overrides it (advancing when the manifest lagged, or
    // rewinding a phantom HWM the manifest claimed but the log
    // doesn't back).
    //
    // A recovery checkpoint that refers to *this* active segment lets
    // the scan skip the fsynced prefix: it resumes on the last batch
    // before the checkpoint's `byte_pos`, so a graceful restart
    // (checkpoint at EOF) reads one batch. The checkpoint carries no
    // offsets (gh #230): the HWM always comes from the batch headers the
    // scan reads, and the resumed scan counts only if it parses cleanly
    // at least as far as `byte_pos`. That rules out a stale checkpoint —
    // another incarnation of the segment, a copied directory — whose
    // position lands mid-batch: its scan stops early, and trusting it
    // would also have truncated real data at that position below.
    // Anything else — no checkpoint, a roll since it was written, a log
    // shorter than it, or a failed resume — scans the whole active
    // segment, which is always correct and, with a bounded segment
    // size, cheap.
    let base = active.meta.base_offset;
    let resumed = match recovery_checkpoint::read(fs, dir) {
        Some(cp) if cp.segment_base == base => {
            match (
                cp.last_batch_pos.and_then(|p| u64::try_from(p).ok()),
                u64::try_from(cp.byte_pos).ok(),
            ) {
                (Some(from), Some(upto)) if from < upto && upto <= active.log_size() => {
                    let mut f = fs.open_read(&active.meta.log_path)?;
                    let scan = segment::scan_high_watermark_from(&mut f, from, base)?;
                    (scan.valid_end >= upto).then_some(scan)
                }
                _ => None,
            }
        }
        _ => None,
    };
    let scan = match resumed {
        Some(scan) => scan,
        None => {
            let mut f = fs.open_read(&active.meta.log_path)?;
            segment::scan_high_watermark_from(&mut f, 0, base)?
        }
    };
    let hwm = scan.high_watermark;

    // gh #226: the scan stopped at the end of the last *valid* batch,
    // but `log_size` still says "whatever stat() reported", so appends
    // would resume past the torn tail — leaving
    // `[valid][garbage][new valid]`. The next recovery stops at the
    // garbage again and every record after it is gone, having been
    // acked and (at flushIntervalMessages=1) fsynced. Worse, the rewound
    // HWM re-issues offsets that were already handed out.
    //
    // Drop the tail before any write handle is used. Doing it here — in
    // open, before the partition is reachable — is what makes it safe:
    // no appender exists yet, so there is nothing to race.
    if scan.stop != segment::ScanStop::Eof {
        active.truncate_to(fs, scan.valid_end)?;
    }
    // The next checkpoint names this batch as its resume point (gh #230).
    active.set_last_batch_start(scan.last_batch_start);

    // Restore the idempotence window.
    let producer_states = read_producer_snapshot(fs, dir)
        .map_err(|e| match e {
            crate::producer_snapshot::ProducerSnapshotError::Io(e) => StorageError::Io(e),
            crate::producer_snapshot::ProducerSnapshotError::Json(e) => StorageError::Json(e),
        })?
        .map(|entries| entries.into_iter().collect::<HashMap<i64, ProducerEntry>>())
        .unwrap_or_default();

    // Restore the read_committed indexes (gh #177). Absent snapshot →
    // empty indexes, which is the pre-gh #177 behaviour for exactly one
    // restart (the first persist writes the file). Aborted entries fully
    // below log_start are dropped on the way in — DeleteRecords may have
    // advanced it since the snapshot was written.
    let (open_txns, aborted_txns) = match read_txn_index_snapshot(fs, dir).map_err(|e| match e {
        crate::txn_index_snapshot::TxnIndexSnapshotError::Io(e) => StorageError::Io(e),
        crate::txn_index_snapshot::TxnIndexSnapshotError::Json(e) => StorageError::Json(e),
    })? {
        Some((open, aborted)) => {
            let mut idx = AbortedTxnIndex::restore(aborted);
            idx.evict_below(log_start);
            (OpenTxnIndex::restore(open), idx)
        }
        None => (OpenTxnIndex::new(), AbortedTxnIndex::new()),
    };

    // Persist the manifest so the next open is fast — but never below
    // the epoch on disk (gh #235). `epoch` was read at the top of this
    // function, and everything since (the segment listing, the handle
    // opens, the high-watermark scan) has run in between. Another broker
    // completing a takeover in that window has already written a higher
    // epoch, and replaying ours over it silently unarms the fence: the
    // new leader keeps its raised epoch in memory, so the takeover
    // reconcile sees a converged partition and never re-drives it.
    //
    // Adopt whatever epoch ends up in effect. Coming up *below* the
    // durable epoch would leave this partition accepting appends the
    // fence is supposed to reject.
    let epoch = manifest::write_unless_superseded(
        fs,
        dir,
        &Manifest {
            epoch,
            high_watermark: hwm,
            log_start_offset: log_start,
        },
    )
    .map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;

    Ok(OpenedState {
        epoch,
        hwm,
        log_start,
        closed,
        active,
        producer_states,
        open_txns,
        aborted_txns,
    })
}

struct PartitionInner {
    dir: PathBuf,
    active: ActiveSegment,
    closed: Vec<SegmentMeta>,
    log_start: i64,
    high_water: i64,
    epoch: i64,

    /// Records appended since the last flush request was sent. Reset
    /// on send (not on completion) so we don't enqueue redundant
    /// flushes for the same batch tail.
    pending_flush_records: i64,
    /// gh #recovery-checkpoint: byte position of the active segment
    /// covered by the most recently written recovery checkpoint. The
    /// committer writes a new checkpoint once the fsynced log has grown
    /// [`CHECKPOINT_INTERVAL_BYTES`] past this, bounding how much of the
    /// tail a crash has to re-scan.
    last_checkpoint_byte: i64,
    /// Monotonic counter incremented every time append decides to
    /// trigger a flush. Each appender's local copy of this value is
    /// what `acks=all` waits on `completed_flush_seq` to cover.
    requested_flush_seq: u64,
    /// Updated by the committer task after each successful fsync.
    completed_flush_seq: u64,
    /// Sticky on first error. Subsequent appends return immediately
    /// with this error; the broker is expected to drop the partition.
    flush_err: Option<StorageError>,

    /// Per-PID idempotence window (gh #12). Held directly here (not
    /// behind [`crate::idempotence::ProducerStates`]) so the
    /// classify+record_accepted pair runs under the partition mutex
    /// without a nested lock.
    producer_states: HashMap<i64, ProducerEntry>,

    /// gh #176 — first offset of each currently-open transactional
    /// producer on this partition. `min(values)` is the Last Stable
    /// Offset (LSO); `read_committed` Fetch reads only up to LSO.
    /// Updated at append time alongside `producer_states`.
    open_txns: OpenTxnIndex,
    /// gh #176 — completed-but-aborted transactions whose ABORT
    /// marker is still live in the log. Drives the Fetch response's
    /// `AbortedTransactions[]` list. Evicted as `log_start` advances.
    aborted_txns: AbortedTxnIndex,
}

/// Snapshot the guarded state for publication on the lock-free read
/// channel. Called under the inner lock everywhere the write path moves
/// the high watermark, the log start, the epoch, or the segment set.
fn snapshot_of(guard: &PartitionInner) -> ReadSnapshot {
    ReadSnapshot {
        high_water: guard.high_water,
        log_start: guard.log_start,
        epoch: guard.epoch,
        closed: Arc::new(guard.closed.clone()),
        active_meta: guard.active.meta.clone(),
    }
}

struct FlushCoord {
    req_tx: mpsc::Sender<()>,
    /// Appenders waiting on `acks=-1` park here; the committer
    /// notifies after each cycle (success or failure).
    cond: Arc<Notify>,
}

/// One owned, leader-bound partition. Holds the active segment's
/// file handles per the gh #76 single-FD contract.
pub struct Partition {
    inner: Arc<Mutex<PartitionInner>>,
    snapshot: ArcSwap<ReadSnapshot>,
    flush: FlushCoord,
    committer: Mutex<Option<JoinHandle<()>>>,
    /// Swappable so a `kafka-configs.sh --alter` on `segment.bytes` /
    /// `segment.ms` takes effect on a live partition, as it does in
    /// Apache. The retention sweep re-resolves it from the topic's
    /// `.config.json` each pass.
    cfg: ArcSwap<PartitionConfig>,
    fs: Arc<dyn Fs>,
    topic: String,
    partition: i32,
}

impl std::fmt::Debug for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Partition")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("dir", &self.inner.lock().dir)
            .finish()
    }
}

impl Partition {
    /// Open (or create) a partition at `dir` and immediately take leadership:
    /// open file handles, restore producer state, start the committer task,
    /// publish the initial [`ReadSnapshot`]. After [`Partition::open`] returns,
    /// the partition is ready to accept appends.
    pub async fn open(
        fs: Arc<dyn Fs>,
        topic: String,
        partition: i32,
        dir: PathBuf,
        cfg: PartitionConfig,
    ) -> Result<Self, StorageError> {
        // gh #210: the whole recovery prologue is synchronous I/O and
        // the high-watermark scan walks up to a full segment, so it
        // runs on a blocking thread. Everything after it is cheap and
        // needs the runtime (the committer task spawn).
        let fs_open = fs.clone();
        let dir_open = dir.clone();
        let OpenedState {
            epoch,
            hwm,
            log_start,
            closed,
            active,
            producer_states,
            open_txns,
            aborted_txns,
        } = tokio::task::spawn_blocking(move || {
            open_blocking_retrying(fs_open.as_ref(), &dir_open)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;

        let inner = Arc::new(Mutex::new(PartitionInner {
            dir: dir.clone(),
            active,
            closed,
            log_start,
            high_water: hwm,
            epoch,
            pending_flush_records: 0,
            last_checkpoint_byte: 0,
            requested_flush_seq: 0,
            completed_flush_seq: 0,
            flush_err: None,
            producer_states,
            open_txns,
            aborted_txns,
        }));
        let initial_snapshot = snapshot_of(&inner.lock());

        // Flush channel — capacity 1 with coalescing semantics. If
        // multiple appenders signal a flush before the committer
        // wakes, only one wake-up happens; the committer's snapshot
        // of `requested_flush_seq` covers all of them.
        let (req_tx, req_rx) = mpsc::channel::<()>(1);
        let cond = Arc::new(Notify::new());

        let committer_handle = spawn_committer(
            inner.clone(),
            cond.clone(),
            req_rx,
            cfg.fsync_max_latency,
            fs.clone(),
        );

        Ok(Self {
            inner,
            snapshot: ArcSwap::from(Arc::new(initial_snapshot)),
            flush: FlushCoord { req_tx, cond },
            committer: Mutex::new(Some(committer_handle)),
            cfg: ArcSwap::from_pointee(cfg),
            fs,
            topic,
            partition,
        })
    }

    /// Claim write ownership at `epoch`, fencing every earlier leader
    /// out of this partition's log (gh #227). Returns the high
    /// watermark.
    ///
    /// Two things have to happen before the partition accepts a write
    /// under a new epoch, and neither used to:
    ///
    /// 1. **`epoch` becomes the partition's epoch**, so the append fence
    ///    starts rejecting a previous leader's in-flight produce
    ///    requests. The epoch previously came only from the manifest at
    ///    open and nothing ever advanced it, so the fence compared
    ///    against 0 and passed everything.
    /// 2. **The active segment rolls to a new epoch-stamped file**,
    ///    which is what the epoch-prefixed naming was introduced for.
    ///    Without the roll a new leader appends to the file the old
    ///    leader still has open, and the two interleave `write_at` calls
    ///    at their own independent `log_size` — byte-level corruption of
    ///    one shared log rather than two divergent files.
    ///
    /// The manifest is persisted before the partition accepts writes, so
    /// the bump survives a crash and a leader coming back at the old
    /// epoch cannot re-adopt the log.
    ///
    /// Idempotent: an `epoch` at or below the current one only reports
    /// the high watermark. Both callers depend on that — `on_change`
    /// re-drives every assignment change and the gh #215 reconcile
    /// re-drives on a timer.
    ///
    /// A fresh partition is created at the manifest's epoch and rolled
    /// here, costing one extra create+unlink per first takeover. That is
    /// off the hot path, and folding the epoch into `Partition::open`
    /// would push it through `create_partition`, which has no epoch to
    /// give.
    pub async fn take_over(&self, epoch: u32) -> Result<i64, StorageError> {
        let inner = self.inner.clone();
        let fs = self.fs.clone();
        let new_epoch = i64::from(epoch);
        // Same reasoning as the gh #210 open prologue: the roll fsyncs
        // the outgoing log and the persist writes two files, all
        // synchronous I/O against a possibly-stalled NAS. Runs on a
        // blocking thread so a slow takeover can't take the broker off
        // the air.
        let snapshot =
            tokio::task::spawn_blocking(move || -> Result<Option<ReadSnapshot>, StorageError> {
                let mut guard = inner.lock();
                if new_epoch <= guard.epoch {
                    return Ok(None);
                }
                // Roll first: it is the step that can fail, and an epoch
                // raised without a segment stamped to match would leave
                // the fence rejecting writes to a log the previous
                // leader still owns.
                roll_for_epoch_locked(fs.as_ref(), &mut guard, new_epoch)?;
                // Raise the epoch only once it is durable. The takeover
                // reconcile decides whether to re-drive by comparing this
                // epoch against the assignment's, so an epoch raised over
                // a manifest that still says otherwise reports a takeover
                // that never finished as complete — and nothing retries
                // it (gh #227).
                persist_state_locked(fs.as_ref(), &mut guard, new_epoch)?;
                guard.epoch = new_epoch;
                Ok(Some(snapshot_of(&guard)))
            })
            .await
            .map_err(|e| StorageError::Io(std::io::Error::other(e)))??;

        // `None` is the idempotent no-op — at or below our epoch nothing
        // moved, so leave the published snapshot alone rather than
        // churning the ArcSwap with an identical copy.
        match snapshot {
            Some(s) => {
                let hwm = s.high_water;
                self.snapshot.store(Arc::new(s));
                Ok(hwm)
            }
            None => Ok(self.high_watermark()),
        }
    }

    /// Drain the committer, persist manifest + producer snapshot one
    /// last time, close the active segment's handles. After close
    /// returns, the partition's FDs are released and a new
    /// [`Partition::open`] on the same `dir` will pick up the
    /// persisted state.
    pub async fn close(&self) -> Result<(), StorageError> {
        // Stop accepting flush requests by dropping the sender ↔
        // we can't drop self.flush.req_tx without &mut self. Instead
        // we'll close the channel by dropping all outstanding senders,
        // which requires a different approach — for now, the
        // committer is also signalled via mpsc::Sender::closed() when
        // we explicitly drop the Sender. Use a sentinel: take() the
        // JoinHandle so future closes are no-ops.
        let handle = {
            let mut guard = self.committer.lock();
            guard.take()
        };
        let Some(handle) = handle else {
            return Ok(()); // already closed
        };

        // Persist state under the lock, then close handles.
        {
            let mut guard = self.inner.lock();
            let epoch = guard.epoch;
            let persisted = persist_state_locked(self.fs.as_ref(), &mut guard, epoch)?;
            // gh #recovery-checkpoint: sync the log tail and checkpoint
            // the whole active segment as durable, so the next open
            // (this broker or the next leader, off the shared volume)
            // re-scans nothing — the graceful-restart fast path. Only
            // on a successful sync; if the log can't be flushed, skip
            // the checkpoint and let the next open fall back to a scan.
            //
            // Also skipped when a higher-epoch leader superseded us
            // (gh #227): the directory is the successor's now. The
            // checkpoint no longer carries offsets (gh #230), so a stray
            // one would cost the successor a full scan rather than its
            // HWM, but a fenced leader still has no business writing it.
            let checkpoint = if persisted && guard.active.sync_log().is_ok() {
                let durable = i64::try_from(guard.active.log_size()).unwrap_or(i64::MAX);
                guard.last_checkpoint_byte = durable;
                Some(RecoveryCheckpoint {
                    segment_base: guard.active.meta.base_offset,
                    byte_pos: durable,
                    last_batch_pos: guard
                        .active
                        .last_batch_start()
                        .and_then(|p| i64::try_from(p).ok()),
                })
            } else {
                None
            };
            let dir = guard.dir.clone();
            guard.active.close_handles();
            drop(guard);
            if let Some(cp) = checkpoint {
                let _ = recovery_checkpoint::write(self.fs.as_ref(), &dir, &cp);
            }
        }

        // Drop the channel sender via Arc/internal swap... can't,
        // since FlushCoord doesn't expose mutability. Instead we
        // signal the committer to exit by aborting the JoinHandle —
        // the committer holds no resources that need orderly cleanup
        // beyond the mutex it briefly takes.
        handle.abort();
        let _ = handle.await;
        Ok(())
    }

    /// Drop this partition **without persisting anything** (gh #219).
    ///
    /// The counterpart to [`Partition::close`], for when the topic
    /// itself is gone: stop the committer and release the FDs, but
    /// write no manifest, no producer snapshot, no recovery checkpoint.
    ///
    /// Persisting here is actively harmful. The operator reclaims a
    /// deleted topic's directory by renaming it aside, and a recreated
    /// topic gets a *fresh* directory at the same path — so a
    /// well-meaning `close()` racing that sequence lands the dead
    /// incarnation's high watermark and idempotence window in the new
    /// incarnation's directory. That is the "empty log, advanced
    /// HWM == logStartOffset" state gh #219 saw on the Streams
    /// repartition topic.
    ///
    /// Releasing the FDs still matters: on NFS an unlinked-while-open
    /// file becomes a `.nfsXXXX` entry that blocks the parent's removal
    /// (gh #76), so dropping handles promptly is what lets the staged
    /// directory actually be reclaimed.
    pub async fn abandon(&self) {
        let handle = {
            let mut guard = self.committer.lock();
            guard.take()
        };
        {
            let mut guard = self.inner.lock();
            guard.active.close_handles();
        }
        if let Some(handle) = handle {
            handle.abort();
            let _ = handle.await;
        }
    }

    /// Append a raw RecordBatch under `epoch`. Returns the assigned
    /// `base_offset` (or the cached one for an idempotent duplicate).
    pub async fn append(&self, epoch: u32, acks: i16, batch: Bytes) -> Result<i64, StorageError> {
        // Pre-lock parse so we don't hold the mutex during this work.
        let prod_info: Option<BatchProducerInfo> = if batch.len() >= 57 {
            parse_batch_producer_info(&batch).ok()
        } else {
            None
        };
        let (assigned_base, my_flush_seq, triggered_flush) = {
            let mut guard = self.inner.lock();

            // Early-fail if the partition is in a sticky stall.
            if guard.flush_err.is_some() {
                // The original cause is captured in `flush_err` for
                // diagnostics; we surface `Stalled` to the wire so
                // every dead-partition response is uniform.
                return Err(StorageError::Stalled);
            }

            // Epoch fence — caller's epoch must be ≥ our current
            // epoch; a stale epoch is rejected.
            if i64::from(epoch) < guard.epoch {
                return Err(StorageError::EpochMismatch);
            }

            // Idempotence: classify before touching the log.
            //
            // `accepted` gates the window update below. Only an
            // `Accept` may advance the dedupe window — that is
            // `record_accepted`'s documented contract, and gh #195 was
            // this call site breaking it. A COMMIT/ABORT marker carries
            // a real PID with `base_sequence == -1`, so it classifies
            // `NotIdempotent` (correctly exempt), but it was still
            // recorded, pushing `{first_seq: -1, last_seq: -1}` onto the
            // producer's window. The next data batch was then measured
            // against `last_seq + 1 == 0`, so a producer continuing its
            // sequence across a transaction boundary — which the Java
            // producer always does — got OUT_OF_ORDER_SEQUENCE_NUMBER
            // every time.
            let mut accepted = false;
            if let Some(info) = prod_info {
                match idempotence::classify(&guard.producer_states, info) {
                    Outcome::Duplicate { base_offset } => return Ok(base_offset),
                    Outcome::OutOfOrder => return Err(StorageError::OutOfOrderSequence),
                    Outcome::InvalidEpoch => return Err(StorageError::InvalidProducerEpoch),
                    Outcome::Accept => accepted = true,
                    Outcome::NotIdempotent => {}
                }
            }

            // Roll if the next append would exceed segment_bytes, or
            // if the active segment has been open longer than
            // segment_ms. The age arm is what lets a low-volume topic
            // age out at all: retention only deletes closed segments.
            let cfg = self.cfg.load();
            let batch_len_u64 = u64::try_from(batch.len()).unwrap_or(u64::MAX);
            let projected_size = guard.active.log_size().saturating_add(batch_len_u64);
            let too_big = projected_size > cfg.segment_bytes;
            // Short-circuit: the age check is the cheap-but-not-free
            // arm (a clock read, and one memoised stat per segment), so
            // skip it entirely when the size arm already decided.
            let too_old = !too_big
                && cfg
                    .segment_ms
                    .is_some_and(|ms| guard.active.age_ms(self.fs.as_ref()) >= ms);
            if (too_big || too_old) && guard.active.log_size() > 0 {
                let new_base = guard.high_water;
                let new_epoch = guard.epoch;
                let dir = guard.dir.clone();
                // Move the old active out so roll_fast can consume it.
                let old_active = std::mem::replace(
                    &mut guard.active,
                    // Placeholder — immediately overwritten below.
                    ActiveSegment::open_meta_only(SegmentMeta {
                        base_offset: 0,
                        epoch: 0,
                        size: 0,
                        log_path: dir.join("_placeholder.log"),
                        index_path: dir.join("_placeholder.index"),
                        max_timestamp: crate::segment::UNKNOWN_MAX_TIMESTAMP,
                    }),
                );
                let (new_active, tail) =
                    old_active.roll_fast(self.fs.as_ref(), &dir, new_base, new_epoch)?;
                guard.closed.push(tail.closed_meta);
                guard.active = new_active;
                // Deferred index-fsync + close runs off-lock on the
                // blocking pool.
                tokio::task::spawn_blocking(move || {
                    let _ = (tail.finalize)();
                });
            }

            // Rewrite baseOffset → current HWM. v2 CRC covers byte
            // 21 onward, so this overwrite is wire-correct.
            let assigned = guard.high_water;
            let mut owned = bytes::BytesMut::with_capacity(batch.len());
            owned.extend_from_slice(&batch);
            owned[0..8].copy_from_slice(&assigned.to_be_bytes());

            // Pre-parse the offset delta so we know how much HWM advances.
            let (_base, last_offset_delta, _max_ts) =
                parse_batch_offsets(&owned).map_err(StorageError::Io)?;

            // Append the bytes to the active log.
            guard
                .active
                .append_batch(&owned, cfg.index_interval_bytes)
                .map_err(StorageError::Io)?;

            // Advance accounting.
            guard.high_water = assigned + i64::from(last_offset_delta) + 1;
            let advanced_records = i64::from(last_offset_delta) + 1;
            guard.pending_flush_records += advanced_records;

            // Record the idempotence outcome only after the log write
            // succeeded (so a failed append doesn't poison the window),
            // and only for an `Accept` (so a marker or a non-idempotent
            // batch doesn't consume a sequence slot — gh #195).
            if accepted {
                if let Some(info) = prod_info {
                    idempotence::record_accepted(&mut guard.producer_states, info, assigned);
                }
            }

            // gh #176 — update the open + aborted-txn indexes off the
            // same batch. Same byte-opacity contract: we only read the
            // header attrs + pid + (for control batches) the key's
            // type byte.
            if let Some(txn_info) = parse_batch_txn_info(&owned) {
                if txn_info.is_transactional {
                    if txn_info.is_control {
                        // COMMIT or ABORT marker — close the open txn.
                        if let Some(first_offset) = guard.open_txns.close(txn_info.producer_id) {
                            if matches!(txn_info.control_commit, Some(false)) {
                                guard.aborted_txns.record(AbortedTxn {
                                    producer_id: txn_info.producer_id,
                                    first_offset,
                                    last_offset: assigned,
                                });
                            }
                        }
                    } else {
                        // Transactional data batch — record the first
                        // offset for this pid's current txn (no-op if
                        // already recorded).
                        guard
                            .open_txns
                            .record_data_batch(txn_info.producer_id, assigned);
                    }
                }
            }

            // Decide whether to fire a flush request.
            let trigger = cfg.flush_interval_messages > 0
                && guard.pending_flush_records >= cfg.flush_interval_messages;
            let my_seq;
            if trigger {
                guard.requested_flush_seq += 1;
                my_seq = guard.requested_flush_seq;
                guard.pending_flush_records = 0;
            } else {
                my_seq = guard.requested_flush_seq;
            }

            // Republish the read snapshot.
            self.snapshot.store(Arc::new(snapshot_of(&guard)));

            (assigned, my_seq, trigger)
        };

        if triggered_flush {
            // Coalesced: try_send fails fast if a flush is already
            // queued (capacity-1 channel). The committer picks up the
            // latest `requested_flush_seq` under the lock anyway.
            let _ = self.flush.req_tx.try_send(());
        }

        // acks == -1: wait for the fsync — but ONLY when this append
        // is the one that crossed the flush-interval threshold. This
        // mirrors the v0.1 engine's `waitForFlushIfAcksAllLocked`
        // (`triggeredFlushSeq <= 0 → return nil`): with
        // `flush_interval_messages == 1` every append triggers, so
        // every acks=all append waits — honest semantics. With the
        // interval raised (the durability/throughput dial), only the
        // crossing append pays the fsync latency; everything else
        // acks immediately, including appends landing while a flush
        // is in flight. The previous shape waited on the
        // last-requested seq from EVERY acks=all append, parking the
        // whole pipeline for the full fsync window each cycle — worth
        // −26% Strimzi-relative throughput vs the v0.1 flavor at
        // interval 10000 on NFS (phase 9 A.3 gate, gh #188).
        if acks == -1 && triggered_flush {
            self.await_flush(my_flush_seq).await?;
        }

        Ok(assigned_base)
    }

    /// Park until the committer has fsynced past `target_seq`, or the
    /// partition enters a sticky stall.
    async fn await_flush(&self, target_seq: u64) -> Result<(), StorageError> {
        loop {
            let notified = self.flush.cond.notified();
            // Check before parking — committer may have already
            // satisfied target_seq before our subscribe.
            {
                let guard = self.inner.lock();
                if guard.flush_err.is_some() {
                    return Err(StorageError::Stalled);
                }
                if guard.completed_flush_seq >= target_seq {
                    return Ok(());
                }
            }
            notified.await;
        }
    }

    /// Read raw RecordBatch bytes from `start_offset` up to
    /// `max_bytes`. Walks closed segments then the active segment;
    /// stops at the first hit that fills the cap.
    pub async fn read(&self, start_offset: i64, max_bytes: usize) -> Result<Bytes, StorageError> {
        // Capture the segment list under the snapshot (lock-free) so
        // a concurrent roll doesn't move the active segment under us.
        let snap = self.snapshot.load_full();
        let fs = self.fs.clone();
        // gh #209: segment reads are synchronous `std::io` against a
        // (typically NFS) log file, with unbounded latency. Running
        // them inline on a scheduler worker starved the whole runtime
        // — with `limits.cpu: 2` tokio sizes the pool to two workers,
        // so a single large fetch took the process off the air:
        // no accepts, no /readyz, no background tasks.
        tokio::task::spawn_blocking(move || {
            read_snapshot(fs.as_ref(), &snap, start_offset, max_bytes)
        })
        .await
        .map_err(|e| StorageError::Io(std::io::Error::other(e)))?
    }

    /// Advance the log-start offset to (at least) `target_offset`.
    /// Drops closed segments fully below the new log_start.
    /// `target_offset == -1` means "purge to HWM" (KIP-107).
    pub async fn delete_records(&self, target_offset: i64) -> Result<i64, StorageError> {
        let new_log_start = {
            let mut guard = self.inner.lock();
            let new_start = if target_offset < 0 {
                guard.high_water
            } else if target_offset > guard.high_water {
                return Err(StorageError::OffsetOutOfRange);
            } else {
                target_offset
            };
            if new_start > guard.log_start {
                guard.log_start = new_start;

                // Drop closed segments fully below the new log_start.
                // Conservatively: drop any closed seg whose
                // base_offset of the NEXT seg is <= log_start.
                // For Phase 2 simplicity we only drop a seg if its
                // size is 0 OR the next seg starts past log_start.
                // Use the next seg's base_offset as the prev seg's
                // exclusive upper bound; the last closed seg's upper
                // bound is the active seg's base_offset.
                let active_base = guard.active.meta.base_offset;
                let mut new_closed: Vec<SegmentMeta> = Vec::new();
                for (i, seg) in guard.closed.iter().enumerate() {
                    let upper = guard
                        .closed
                        .get(i + 1)
                        .map(|n| n.base_offset)
                        .unwrap_or(active_base);
                    if upper <= new_start {
                        // Entire segment below log_start — remove the
                        // file from disk (best-effort).
                        let _ = self.fs.remove(&seg.log_path);
                        let _ = self.fs.remove(&seg.index_path);
                    } else {
                        new_closed.push(seg.clone());
                    }
                }
                guard.closed = new_closed;

                // gh #177: aborted-txn entries whose span is now fully
                // below log_start can never appear in a fetchable range
                // again — drop them so the index doesn't grow with
                // abort history for the life of the leadership.
                guard.aborted_txns.evict_below(new_start);

                self.snapshot.store(Arc::new(snapshot_of(&guard)));
            }
            guard.log_start
        };
        Ok(new_log_start)
    }

    /// gh #168: is this partition's committer poisoned by a fsync
    /// failure right now? `try_lock` on purpose — a mutex held by a
    /// hung fsync must not turn the health probe into a hostage; the
    /// window where the poison is being set reads as "not stalled"
    /// for one probe tick, which is fine for an observability signal.
    pub fn is_stalled(&self) -> bool {
        self.inner
            .try_lock()
            .map(|g| g.flush_err.is_some())
            .unwrap_or(false)
    }

    /// Lock-free HWM read via the published snapshot.
    pub fn high_watermark(&self) -> i64 {
        self.snapshot.load().high_water
    }

    /// Lock-free log_start read via the published snapshot.
    pub fn log_start_offset(&self) -> i64 {
        self.snapshot.load().log_start
    }

    /// Lock-free epoch read via the published snapshot.
    pub fn epoch(&self) -> i64 {
        self.snapshot.load().epoch
    }

    /// gh #176 — Last Stable Offset for `read_committed` Fetch.
    /// The lowest offset across all currently-open transactional
    /// producers on this partition, or HWM when no txn is open.
    /// Mirrors Apache's `Log.lastStableOffset`.
    pub fn last_stable_offset(&self) -> i64 {
        let guard = self.inner.lock();
        guard
            .open_txns
            .min_open_offset()
            .unwrap_or(guard.high_water)
    }

    /// gh #176 — aborted transactions whose first-offset falls in
    /// `[start_offset, end_offset)`. Used to populate the Fetch
    /// response's `AbortedTransactions[]` list for `read_committed`
    /// consumers.
    pub fn aborted_in_range(&self, start_offset: i64, end_offset: i64) -> Vec<AbortedTxn> {
        let guard = self.inner.lock();
        guard.aborted_txns.in_range(start_offset, end_offset)
    }

    /// gh #30 / #108: bump the recorded producer epoch and clear
    /// this partition's dedupe window for `pid`. Idempotent — no-op
    /// when the recorded epoch is already `>= new_epoch`. Called by
    /// the storage engine's cross-partition `fence_producer_epoch`
    /// walker after an `InitProducerId` epoch bump, both for the
    /// local broker's in-process fence and for inbound peer fences
    /// the `kaas-broker::FenceWatcher` dispatches.
    pub fn fence_producer(&self, pid: i64, new_epoch: i16) {
        let mut guard = self.inner.lock();
        let entry = guard.producer_states.entry(pid).or_default();
        if new_epoch > entry.epoch {
            entry.epoch = new_epoch;
            entry.recent.clear();
        }
    }

    /// Sum of closed-segment sizes + active-segment size.
    pub fn partition_size(&self) -> i64 {
        let snap = self.snapshot.load();
        let total: u64 = snap
            .closed
            .iter()
            .map(|s| s.size)
            .chain(std::iter::once(snap.active_meta.size))
            .sum();
        i64::try_from(total).unwrap_or(i64::MAX)
    }

    /// Replace the tuning knobs on a live partition. The retention
    /// sweep calls this each pass with the topic's current
    /// `.config.json`, so `kafka-configs.sh --alter segment.bytes`
    /// takes effect without a restart — as it does in Apache. The
    /// append path reads the config per batch, so the next append
    /// after this call already sees it.
    pub fn set_config(&self, cfg: PartitionConfig) {
        self.cfg.store(Arc::new(cfg));
    }

    /// Current tuning knobs.
    pub fn config(&self) -> Arc<PartitionConfig> {
        self.cfg.load_full()
    }

    /// Recorded largest timestamp of each closed segment, oldest
    /// first. `-1` means "not recorded" — see
    /// [`SegmentMeta::max_timestamp`]. Test/diagnostic accessor for
    /// telling the two retention arms apart.
    pub fn closed_segment_max_timestamps(&self) -> Vec<i64> {
        self.snapshot
            .load()
            .closed
            .iter()
            .map(|s| s.max_timestamp)
            .collect()
    }

    /// Time-based retention (`retention.ms`): return the
    /// `target_offset` that [`Partition::delete_records`] should
    /// advance to in order to drop every closed segment whose records
    /// are all older than `cutoff_ms`. `None` when nothing has aged
    /// out.
    ///
    /// A segment is reapable when its **largest** timestamp is below
    /// the cutoff — never its smallest, or a segment holding one
    /// recent record among old ones would be deleted with it. Where
    /// that timestamp comes from is the interesting part, because
    /// nothing on disk records it: the manifest carries no segment
    /// list, so a segment this broker didn't roll itself has none.
    /// [`SegmentMeta::max_timestamp`] holds it for segments rolled in
    /// this process; otherwise we fall back to the log file's mtime,
    /// which is what Apache does in the same situation
    /// (`LogSegment.largestTimestamp` → `lastModified`). Both are
    /// wall-clock-comparable, and a closed segment's mtime doesn't
    /// move, so the fallback is stable across the recovery path.
    ///
    /// Walks oldest-first and stops at the first segment that is NOT
    /// reapable, rather than reaping every old segment it can find —
    /// retention advances `logStartOffset`, which must stay
    /// monotonic and contiguous. The active segment is never
    /// reclaimed here (that's `DeleteRecords`' job), so the target is
    /// capped at its base offset.
    ///
    /// Lock-free apart from the mtime `stat`, which only runs for
    /// segments whose timestamp is unknown.
    pub fn cleanup_target_for_time(&self, fs: &dyn Fs, cutoff_ms: i64) -> Option<i64> {
        let snap = self.snapshot.load();
        let mut target: Option<i64> = None;
        for (i, seg) in snap.closed.iter().enumerate() {
            let largest = if seg.max_timestamp >= 0 {
                seg.max_timestamp
            } else {
                match segment_mtime_ms(fs, &seg.log_path) {
                    Some(ms) => ms,
                    // Un-stattable: leave it alone. A segment we can't
                    // date is not a segment to delete.
                    None => break,
                }
            };
            if largest >= cutoff_ms {
                break;
            }
            target = Some(
                snap.closed
                    .get(i + 1)
                    .map(|n| n.base_offset)
                    .unwrap_or(snap.active_meta.base_offset),
            );
        }
        target
    }

    /// Size-based retention: return the `target_offset` that
    /// [`Partition::delete_records`] should advance to in order to
    /// keep the partition under `retention_bytes`. Returns `None`
    /// when no cleanup is needed.
    ///
    /// Lock-free — reads via the [`ReadSnapshot`] only. Closed
    /// segments are walked oldest-first; segments are virtually
    /// dropped while `cumulative_size <= surplus`. The returned
    /// offset is the `base_offset` of the first segment we KEEP,
    /// or the active segment's `base_offset` if all closed segments
    /// would be dropped (the active segment is never reclaimed —
    /// neither here nor by `DeleteRecords`, which also unlinks
    /// closed segments only).
    pub fn cleanup_target_for_size_bytes(&self, retention_bytes: u64) -> Option<i64> {
        let snap = self.snapshot.load();
        let active_size = snap.active_meta.size;
        let active_base = snap.active_meta.base_offset;
        let closed_sizes: Vec<u64> = snap.closed.iter().map(|s| s.size).collect();
        let total: u64 = closed_sizes.iter().sum::<u64>().saturating_add(active_size);
        if total <= retention_bytes {
            return None;
        }
        let mut surplus = total - retention_bytes;
        // Walk oldest-first.
        for (i, seg) in snap.closed.iter().enumerate() {
            if seg.size > surplus {
                // Dropping this segment would over-shoot retention.
                // Stop here — keep this segment, return its base.
                return Some(seg.base_offset);
            }
            surplus -= seg.size;
            if surplus == 0 {
                // Exactly at retention after dropping up to and
                // including segment `i`. Target = next segment's
                // base (or active if this was the last closed).
                return Some(
                    snap.closed
                        .get(i + 1)
                        .map(|n| n.base_offset)
                        .unwrap_or(active_base),
                );
            }
        }
        // Dropping all closed segments still leaves a surplus
        // (active alone exceeds retention). Cap at active_base —
        // the active segment is not reclaimed by the cleaner.
        Some(active_base)
    }
}

// ---------------------------------------------------------------------------
// Epoch-bump roll — called under the inner lock.
// ---------------------------------------------------------------------------

/// gh #227: swap the active segment for a fresh one stamped with
/// `new_epoch`, so a leader that has just been fenced can never share a
/// log file with this one. Called from [`Partition::take_over`] *before*
/// it raises `guard.epoch`, so a failure here leaves the partition
/// serving normally at its current epoch.
///
/// Two shapes, split on whether the outgoing segment holds bytes:
///
/// - **Non-empty** — an ordinary roll. The new base is the current high
///   watermark, strictly above the outgoing base, so the two files are
///   distinct in both base offset and epoch, and the outgoing segment
///   joins `closed`.
/// - **Empty** — an empty segment's high watermark *is* its base, so the
///   new file differs from the outgoing one only by its epoch prefix.
///   Nothing joins `closed` (an empty segment covers no offsets) and the
///   outgoing files are removed. The removal is best-effort: a fenced
///   leader still holding the FD turns it into an NFS silly-rename,
///   which is the desired outcome anyway — its writes land in a doomed
///   file. If the removal fails outright, [`segment::list_segments`]
///   resolves the duplicate base offset in favour of the higher epoch.
fn roll_for_epoch_locked(
    fs: &dyn Fs,
    guard: &mut PartitionInner,
    new_epoch: i64,
) -> Result<(), StorageError> {
    let dir = guard.dir.clone();
    let new_base = guard.high_water;
    let outgoing_size = guard.active.log_size();

    // Already done. A re-drive after a `take_over` that failed past the
    // roll finds the segment its own previous attempt created — same
    // base, same epoch. Detecting that the postcondition holds is much
    // safer than proceeding and compensating: `ActiveSegment::create`
    // would truncate that very file and the cleanup below would then
    // unlink the active segment out from under its own open handles.
    if guard.active.meta.epoch == new_epoch && guard.active.meta.base_offset == new_base {
        return Ok(());
    }

    // Every fallible step happens before the first mutation, so a
    // failure leaves the partition exactly as it was and the caller can
    // re-drive `take_over`. The append path's `roll_fast` can't offer
    // that — it consumes the outgoing segment — and a partition left
    // half-rolled would be serving from a segment set no epoch fully
    // describes, which no later re-drive can reason about.
    if outgoing_size > 0 {
        // Flush the outgoing log before we stop writing to it. The
        // index is deliberately not flushed: it is rebuilt on open, and
        // the whole point of this path is that a fresh leader recovers.
        guard.active.sync_log().map_err(StorageError::Io)?;
    }
    let new_active =
        ActiveSegment::create(fs, &dir, new_base, new_epoch).map_err(StorageError::Io)?;

    let mut outgoing = std::mem::replace(&mut guard.active, new_active);
    let outgoing_meta = SegmentMeta {
        size: outgoing_size,
        // Same as the size-driven roll: this is the one moment the
        // outgoing segment's largest timestamp is known without
        // re-reading it.
        max_timestamp: outgoing.max_timestamp(),
        ..outgoing.meta.clone()
    };
    outgoing.close_handles();

    if outgoing_size > 0 {
        guard.closed.push(outgoing_meta);
    } else {
        let _ = fs.remove(&outgoing_meta.log_path);
        let _ = fs.remove(&outgoing_meta.index_path);
    }

    // The checkpoint on disk describes the segment we just left, and
    // `open` rejects it on the `segment_base` mismatch. Writing a
    // replacement would be a tmp+fsync+rename — a full COMMIT on NFS —
    // for nothing: it would say `(byte_pos 0, high_watermark new_base)`,
    // which is bit-identical to what the no-checkpoint fallback already
    // derives, since a fresh segment's base offset *is* the high
    // watermark. Just forget our own position in it.
    guard.last_checkpoint_byte = 0;
    Ok(())
}

// ---------------------------------------------------------------------------
// Persist helper — called under the inner lock.
// ---------------------------------------------------------------------------

/// Persist this partition's durable state — producer snapshot, then
/// manifest — stamping the manifest with `epoch`.
///
/// `epoch` is a parameter rather than `guard.epoch` so a takeover can
/// write the epoch it is claiming *before* adopting it in memory: the
/// reported epoch must never run ahead of the durable one (gh #227).
/// Ordinary callers pass `guard.epoch`.
///
/// Returns `false` when a higher-epoch leader has superseded us, in
/// which case the manifest was left alone — the caller must not treat
/// its state as durable.
fn persist_state_locked(
    fs: &dyn Fs,
    guard: &mut PartitionInner,
    epoch: i64,
) -> Result<bool, StorageError> {
    // gh #227: a fenced leader must not write its state over its
    // successor's. `close` / `relinquish` persist whatever the partition
    // last held in memory, and a broker that lost the partition only
    // learns of it when its `assignment.json` watch catches up — by
    // which point the new leader has already raised the on-disk epoch.
    // Writing here would roll the epoch back and republish a high
    // watermark for a log this broker no longer owns.
    //
    // This guard is broader than the one inside
    // `manifest::write_unless_superseded`: it covers the producer
    // snapshot too, which carries the dedupe window and is just as
    // damaging to replay over a successor's. Re-reading is sound on NFS
    // (close-to-open gives us the latest) and cheap — the manifest is
    // persisted on open and close, never per append. Being superseded is
    // not an error; the caller still wants its file handles released.
    if let Ok(ReadResult::Present(on_disk)) = manifest::read(fs, &guard.dir) {
        if on_disk.epoch > epoch {
            return Ok(false);
        }
    }
    let snap: Vec<(i64, ProducerEntry)> = guard
        .producer_states
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    write_producer_snapshot(fs, &guard.dir, &snap).map_err(|e| match e {
        crate::producer_snapshot::ProducerSnapshotError::Io(e) => StorageError::Io(e),
        crate::producer_snapshot::ProducerSnapshotError::Json(e) => StorageError::Json(e),
    })?;
    // gh #177: the read_committed indexes ride the same triggers and
    // the same superseded-epoch guard as the producer snapshot — a
    // fenced leader's stale LSO/abort state must not replay over its
    // successor's either.
    write_txn_index_snapshot(
        fs,
        &guard.dir,
        &guard.open_txns.entries(),
        &guard.aborted_txns.snapshot(),
    )
    .map_err(|e| match e {
        crate::txn_index_snapshot::TxnIndexSnapshotError::Io(e) => StorageError::Io(e),
        crate::txn_index_snapshot::TxnIndexSnapshotError::Json(e) => StorageError::Json(e),
    })?;
    // Re-checked at the write itself, not just at the top of this
    // function: the producer-snapshot write sits in between, and on a
    // slow substrate that is enough of a gap for a takeover to land
    // (gh #235).
    let effective = manifest::write_unless_superseded(
        fs,
        &guard.dir,
        &Manifest {
            epoch,
            high_watermark: guard.high_water,
            log_start_offset: guard.log_start,
        },
    )
    .map_err(|e| match e {
        manifest::ManifestError::Io(e) => StorageError::Io(e),
        manifest::ManifestError::Json(e) => StorageError::Json(e),
        other => StorageError::Io(std::io::Error::other(other.to_string())),
    })?;
    Ok(effective == epoch)
}

// ---------------------------------------------------------------------------
// Committer task
// ---------------------------------------------------------------------------

/// gh #168: how often the committer probes a stalled substrate for
/// recovery. Chosen well above typical fsync latency so probes never
/// compete with real traffic, and low enough that a freed-up disk
/// (ENOSPC cleared, NFS back) re-arms within seconds.
const REARM_PROBE_INTERVAL: Duration = Duration::from_secs(5);

/// gh #168: recover a partition from a sticky `flush_err` instead of
/// leaving it poisoned until leadership moves or the broker restarts
/// (the pre-fix contract — "operator must restart the broker").
///
/// Two phases per attempt, because the failure modes differ:
///
/// 1. **Substrate probe, off the partition lock**: write + fsync +
///    unlink a tiny sidecar file in the partition dir. While the
///    substrate is still bad (ENOSPC, EIO, unresponsive NFS) this
///    fails or hangs *without* holding the partition mutex, so
///    appenders keep failing fast with `Stalled` rather than parking
///    behind a probe.
/// 2. **Real re-arm, under the lock**: one `sync_log` of the active
///    segment. Only on its success is `flush_err` cleared — the probe
///    file proving the filesystem answers says nothing about the
///    log's own dirty pages. The completed seq adopts the value
///    sampled under the same lock as the sync (never one sampled
///    after — the gh #232 invariant), so nothing is ever reported
///    durable that the sync didn't cover.
///
/// Known residual, deliberately out of scope: a *hung* fsync holds
/// the partition mutex from inside its `spawn_blocking`, so until the
/// kernel returns, phase 2 (and appenders) would block behind it —
/// the probe's `try_lock` dir read just keeps sleeping in that case
/// and recovery begins once the kernel call drains.
async fn probe_and_rearm(inner: &Arc<Mutex<PartitionInner>>, cond: &Arc<Notify>, fs: &Arc<dyn Fs>) {
    loop {
        tokio::time::sleep(REARM_PROBE_INTERVAL).await;

        // Phase 1 — substrate probe, no partition lock held.
        let Some(dir) = inner.try_lock().map(|g| g.dir.clone()) else {
            continue;
        };
        let fs_probe = fs.clone();
        let probe = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let path = dir.join(".fsync-probe");
            let mut f = fs_probe.create(&path)?;
            f.write_all(b"kaas fsync probe")?;
            f.sync_all()?;
            drop(f);
            let _ = fs_probe.remove(&path);
            Ok(())
        })
        .await;
        if !matches!(probe, Ok(Ok(()))) {
            continue;
        }

        // Phase 2 — the substrate answers; re-sync the log for real.
        let inner_sync = inner.clone();
        let synced = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
            let mut guard = inner_sync.lock();
            guard.active.sync_log()?;
            Ok(guard.requested_flush_seq)
        })
        .await;
        if let Ok(Ok(covered_seq)) = synced {
            {
                let mut guard = inner.lock();
                guard.flush_err = None;
                if covered_seq > guard.completed_flush_seq {
                    guard.completed_flush_seq = covered_seq;
                }
            }
            cond.notify_waiters();
            return;
        }
    }
}

fn spawn_committer(
    inner: Arc<Mutex<PartitionInner>>,
    cond: Arc<Notify>,
    mut req_rx: mpsc::Receiver<()>,
    fsync_max_latency: Option<Duration>,
    fs: Arc<dyn Fs>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while req_rx.recv().await.is_some() {
            // Capture the seq we want to satisfy.
            let target_seq = inner.lock().requested_flush_seq;
            if target_seq == inner.lock().completed_flush_seq {
                // Already satisfied by a previous cycle that
                // coalesced multiple requests.
                continue;
            }

            let inner_clone = inner.clone();
            let fs_c = fs.clone();
            let fsync_handle = tokio::task::spawn_blocking(move || -> std::io::Result<u64> {
                // Sync under the lock, then decide whether a fresh
                // recovery checkpoint is due — but write it OFF the lock
                // so the tmp+rename doesn't stall appenders.
                let (seq, checkpoint) = {
                    let mut guard = inner_clone.lock();
                    guard.active.sync_log()?;
                    let seq = guard.requested_flush_seq;
                    let durable = i64::try_from(guard.active.log_size()).unwrap_or(i64::MAX);
                    let cp = if durable.saturating_sub(guard.last_checkpoint_byte)
                        >= CHECKPOINT_INTERVAL_BYTES
                    {
                        // Advance optimistically: a failed write just
                        // defers the next attempt one interval, and
                        // recovery falls back to a full scan regardless.
                        guard.last_checkpoint_byte = durable;
                        Some((
                            RecoveryCheckpoint {
                                segment_base: guard.active.meta.base_offset,
                                byte_pos: durable,
                                last_batch_pos: guard
                                    .active
                                    .last_batch_start()
                                    .and_then(|p| i64::try_from(p).ok()),
                            },
                            guard.dir.clone(),
                        ))
                    } else {
                        None
                    };
                    (seq, cp)
                };
                if let Some((cp, dir)) = checkpoint {
                    let _ = recovery_checkpoint::write(fs_c.as_ref(), &dir, &cp);
                }
                Ok(seq)
            });

            // gh #256: `None` = watchdog disabled — await the fsync
            // however long it takes, wrapped so both arms share the
            // downstream match (Ok(join_result) ↔ "did not time out").
            let outcome = match fsync_max_latency {
                Some(deadline) => tokio::time::timeout(deadline, fsync_handle).await,
                None => Ok(fsync_handle.await),
            };
            match outcome {
                Ok(Ok(Ok(satisfied_seq))) => {
                    let mut guard = inner.lock();
                    if satisfied_seq > guard.completed_flush_seq {
                        guard.completed_flush_seq = satisfied_seq;
                    }
                    drop(guard);
                    cond.notify_waiters();
                }
                Ok(Ok(Err(io_err))) => {
                    {
                        let mut guard = inner.lock();
                        if guard.flush_err.is_none() {
                            guard.flush_err = Some(StorageError::Io(io_err));
                        }
                    }
                    cond.notify_waiters();
                    probe_and_rearm(&inner, &cond, &fs).await;
                }
                Ok(Err(_join_err)) => {
                    // spawn_blocking panicked — treat as Stalled.
                    {
                        let mut guard = inner.lock();
                        if guard.flush_err.is_none() {
                            guard.flush_err = Some(StorageError::Stalled);
                        }
                    }
                    cond.notify_waiters();
                    probe_and_rearm(&inner, &cond, &fs).await;
                }
                Err(_elapsed) => {
                    // Timeout (gh #95). The orphaned spawn_blocking
                    // task is still running; that's fine — it drains
                    // when the kernel eventually returns. We set
                    // sticky Stalled and move on.
                    {
                        let mut guard = inner.lock();
                        if guard.flush_err.is_none() {
                            guard.flush_err = Some(StorageError::Stalled);
                        }
                    }
                    cond.notify_waiters();
                    // gh #168: park in the probe loop until the
                    // substrate recovers, then clear the poison. New
                    // flush requests queue on the capacity-1 channel
                    // meanwhile; appends fail fast on `flush_err`.
                    probe_and_rearm(&inner, &cond, &fs).await;
                }
            }
        }
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// Log-file mtime as epoch milliseconds. `None` when the file can't be
/// stat'ed or its mtime predates the epoch — both mean "don't date this
/// segment", which time-based retention treats as "don't reap it".
fn segment_mtime_ms(fs: &dyn Fs, path: &std::path::Path) -> Option<i64> {
    let modified = fs.stat(path).ok()?.modified().ok()?;
    let since = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    i64::try_from(since.as_millis()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    /// [`RealFs`] with a deliberate stall on every `open_read`, to
    /// stand in for NFS latency.
    struct SlowFs {
        inner: RealFs,
        delay: Duration,
    }

    /// [`RealFs`] whose next `fail_creates` matching `create` calls fail
    /// with ENOENT — standing in for the operator renaming the topic dir
    /// aside between our `mkdir_all` and our first file open (gh #220).
    ///
    /// `only_ext` narrows which creates are eligible. Left `None` every
    /// create counts; set to `"tmp"` it hits only the
    /// `tmp + fsync + rename` writer, failing a metadata persist while
    /// leaving segment creation intact (gh #227).
    struct VanishingFs {
        inner: RealFs,
        fail_creates: std::sync::atomic::AtomicU32,
        only_ext: Option<&'static str>,
    }

    impl VanishingFs {
        fn new(fail_creates: u32) -> Self {
            Self {
                inner: RealFs::new(),
                fail_creates: std::sync::atomic::AtomicU32::new(fail_creates),
                only_ext: None,
            }
        }

        fn only_ext(mut self, ext: &'static str) -> Self {
            self.only_ext = Some(ext);
            self
        }

        fn arm(&self, n: u32) {
            self.fail_creates
                .store(n, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// [`RealFs`] that drops a higher-epoch manifest on disk the first
    /// time the recovery scan opens a `.log` — standing in for another
    /// broker completing a takeover inside `Partition::open`'s
    /// read-scan-write window (gh #235).
    struct RacingTakeoverFs {
        inner: RealFs,
        lands: Manifest,
        fired: std::sync::atomic::AtomicBool,
    }

    impl Fs for RacingTakeoverFs {
        fn open_read(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
            if p.extension().is_some_and(|e| e == "log")
                && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
            {
                if let Some(dir) = p.parent() {
                    let _ = manifest::write_unchecked(&self.inner, dir, &self.lands);
                }
            }
            self.inner.open_read(p)
        }
        fn open_write(
            &self,
            p: &std::path::Path,
            append: bool,
        ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.remove(p)
        }
        fn fsync_dir(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.fsync_dir(p)
        }
        fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    impl Fs for VanishingFs {
        fn open_read(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
            self.inner.open_read(p)
        }
        fn open_write(
            &self,
            p: &std::path::Path,
            append: bool,
        ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            let eligible = self
                .only_ext
                .is_none_or(|ext| p.extension().is_some_and(|e| e == ext));
            if eligible
                && self
                    .fail_creates
                    .fetch_update(
                        std::sync::atomic::Ordering::SeqCst,
                        std::sync::atomic::Ordering::SeqCst,
                        |n| n.checked_sub(1),
                    )
                    .is_ok()
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "no such file or directory",
                ));
            }
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.remove(p)
        }
        fn fsync_dir(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.fsync_dir(p)
        }
        fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    impl Fs for SlowFs {
        fn open_read(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
            std::thread::sleep(self.delay);
            self.inner.open_read(p)
        }
        fn open_write(
            &self,
            p: &std::path::Path,
            append: bool,
        ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &std::path::Path) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.remove(p)
        }
        fn fsync_dir(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.fsync_dir(p)
        }
        fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    /// gh #210: `Partition::open` recovers by walking the active
    /// segment, which on NFS costs seconds. It used to do that inline
    /// on a scheduler worker, and with `limits.cpu: 2` tokio only has
    /// two — so a takeover took the broker off the air entirely: no
    /// accepts, no `/readyz`, no background tasks.
    ///
    /// Both futures are `spawn`ed so they contend for the *same*
    /// single worker. Driving `open` on `block_on`'s calling thread
    /// instead would let the ticker run on the worker regardless, and
    /// the test would pass even with the blocking version.
    #[test]
    fn open_does_not_starve_the_runtime() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");

        let rt1 = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();

        let ticks = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ticker_ticks = ticks.clone();

        let p = rt1.block_on(async move {
            let ticker = tokio::spawn(async move {
                loop {
                    ticker_ticks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::task::yield_now().await;
                }
            });
            let fs: Arc<dyn Fs> = Arc::new(SlowFs {
                inner: RealFs::new(),
                delay: Duration::from_millis(40),
            });
            let opened = tokio::spawn(async move {
                Partition::open(fs, "t".to_owned(), 0, dir, PartitionConfig::default()).await
            });
            let p = opened.await.unwrap().unwrap();
            ticker.abort();
            p
        });

        let observed = ticks.load(std::sync::atomic::Ordering::Relaxed);
        assert!(
            observed > 50,
            "runtime advanced only {observed} ticks while Partition::open ran — \
             the recovery scan is blocking the scheduler worker"
        );
        drop(p);
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    /// Build a v2 batch with given (base_offset, num_records). The
    /// engine rewrites bytes [0..8] to its assigned offset.
    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2; // magic
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        // Producer ID = -1 (non-idempotent) so no classifier path.
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    /// Build a transactional v2 data batch (attributes bit 4 set).
    /// PID is encoded; base_sequence is left at 0 so the
    /// idempotence classifier accepts as a fresh first-batch.
    fn build_txn_data_batch(pid: i64, num_records: i32) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        // attributes: bit 4 (transactional) only.
        buf[21..23].copy_from_slice(&0x0010i16.to_be_bytes());
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        // baseSequence at [53..57] stays 0 = first batch of a fresh
        // PID per the idempotence classifier.
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    /// As [`build_txn_data_batch`] but with an explicit base sequence,
    /// so a test can continue a producer's sequence across a marker
    /// the way a real transactional producer does.
    fn build_txn_data_batch_seq(pid: i64, num_records: i32, base_seq: i32) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        buf[21..23].copy_from_slice(&0x0010i16.to_be_bytes());
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        buf[53..57].copy_from_slice(&base_seq.to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    /// Build a control batch (COMMIT or ABORT marker). attributes
    /// has bits 4 (transactional) | 5 (control) set; base_sequence
    /// = -1 so the idempotence classifier returns NotIdempotent
    /// (markers don't consume sequence slots).
    fn build_marker_batch(pid: i64, commit: bool) -> Bytes {
        let mut buf = vec![0u8; 70];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        // body_size = 58 (everything from attrs through the inline
        // marker record); arbitrary for these tests since we never
        // re-read records from the log.
        buf[8..12].copy_from_slice(&58i32.to_be_bytes());
        buf[16] = 2;
        buf[21..23].copy_from_slice(&0x0030i16.to_be_bytes()); // transactional | control
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        buf[53..57].copy_from_slice(&(-1i32).to_be_bytes());
        // key type at byte 69: 0=ABORT, 1=COMMIT
        buf[69] = if commit { 1 } else { 0 };
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    /// gh #195: a transactional producer continues its sequence across
    /// a COMMIT marker. The marker must not consume a sequence slot —
    /// `classify` exempts it (`base_sequence == -1` → `NotIdempotent`),
    /// but the append path recorded *every* batch into the dedupe
    /// window, marker included. That pushed `{first_seq: -1, last_seq:
    /// -1}` on, so the next data batch was measured against
    /// `last_seq + 1 == 0` and anything else came back
    /// OUT_OF_ORDER_SEQUENCE_NUMBER — at every transaction boundary.
    #[test]
    fn marker_does_not_consume_a_sequence_slot() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();

            // Transaction 1: sequences 0..=9.
            p.append(0, -1, build_txn_data_batch_seq(42, 10, 0))
                .await
                .expect("first txn batch accepted");
            // COMMIT marker for the same PID.
            p.append(0, -1, build_marker_batch(42, true))
                .await
                .expect("marker appended");

            // Transaction 2 from the same producer session: the Java
            // producer does NOT reset sequences across transactions, so
            // this continues at 10.
            let res = p.append(0, -1, build_txn_data_batch_seq(42, 10, 10)).await;
            assert!(
                res.is_ok(),
                "post-marker batch must be accepted, got {:?} — the marker \
                 consumed a sequence slot (gh #195)",
                res.err()
            );
        });
    }

    #[test]
    fn lso_equals_hwm_when_no_txn_open() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            assert!(p.aborted_in_range(0, i64::MAX).is_empty());
            p.close().await.unwrap();
        });
    }

    // --- gh #226 / gh #228: torn-tail recovery ---------------------------

    /// Append `extra` bytes of garbage to the partition's active log,
    /// mimicking a crash that left a partial batch (or a fenced writer's
    /// interleaved bytes) at the end of the file.
    fn append_garbage(dir: &std::path::Path, extra: usize) -> (std::path::PathBuf, u64) {
        let fs = RealFs::new();
        let meta = crate::segment::list_segments(&fs, dir)
            .unwrap()
            .pop()
            .unwrap();
        let before = std::fs::metadata(&meta.log_path).unwrap().len();
        let mut bytes = std::fs::read(&meta.log_path).unwrap();
        bytes.extend(std::iter::repeat_n(0xABu8, extra));
        std::fs::write(&meta.log_path, &bytes).unwrap();
        (meta.log_path, before)
    }

    /// The core gh #226 defect: records written *after* a torn tail were
    /// acked (and at flushIntervalMessages=1 fsynced) and then silently
    /// vanished on the next recovery, with their offsets re-issued.
    ///
    /// Pre-fix this test fails at the final assertion: the reopen resumed
    /// appends at the file's stat length, so the log became
    /// `[valid][garbage][new]` and the *next* scan stopped at the garbage.
    #[test]
    fn records_appended_after_a_torn_tail_survive_the_next_recovery() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let open = || {
                Partition::open(
                    fs.clone(),
                    "t".into(),
                    0,
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                )
            };

            let p = open().await.unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            assert_eq!(p.high_watermark(), 3);
            p.close().await.unwrap();

            let (log_path, clean_len) = append_garbage(tmp.path(), 40);

            // Recovery drops the garbage instead of appending past it.
            let p = open().await.unwrap();
            assert_eq!(p.high_watermark(), 3, "valid batches must survive");
            assert_eq!(
                std::fs::metadata(&log_path).unwrap().len(),
                clean_len,
                "torn tail must be truncated away"
            );

            // Write a record on top of the repaired log and make it durable.
            p.append(0, -1, build_batch(1, 2_000)).await.unwrap();
            assert_eq!(p.high_watermark(), 4);
            p.close().await.unwrap();

            let p = open().await.unwrap();
            assert_eq!(
                p.high_watermark(),
                4,
                "the record appended after repair must not be lost"
            );
        });
    }

    /// A clean log must never be touched — truncation is only ever
    /// allowed to remove bytes the scan proved unparseable.
    #[test]
    fn clean_log_is_not_truncated_on_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            p.close().await.unwrap();

            let meta = crate::segment::list_segments(&RealFs::new(), tmp.path())
                .unwrap()
                .pop()
                .unwrap();
            let len_before = std::fs::metadata(&meta.log_path).unwrap().len();

            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(p.high_watermark(), 2);
            assert_eq!(
                std::fs::metadata(&meta.log_path).unwrap().len(),
                len_before,
                "a clean log must survive reopen byte-for-byte"
            );
        });
    }

    #[test]
    fn lso_holds_at_first_txn_offset_until_commit_marker() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();

            // Non-txn batch — LSO follows HWM.
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 2);

            // Open a txn: 3 transactional records at offset 2.
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            assert_eq!(p.high_watermark(), 5);
            assert_eq!(
                p.last_stable_offset(),
                2,
                "LSO must stay at the txn's first offset while open"
            );

            // Another txn data batch for the same PID — LSO unchanged.
            // (Same-pid second batch within the same open txn.)
            // We can't easily re-use the txn classifier path here
            // because the dedupe window would reject seq=0 a second
            // time; just skip and go straight to commit.
            let marker = build_marker_batch(42, true);
            p.append(0, -1, marker).await.unwrap();
            assert_eq!(p.high_watermark(), 6);
            assert_eq!(
                p.last_stable_offset(),
                p.high_watermark(),
                "LSO catches up to HWM once commit marker lands"
            );
            assert!(
                p.aborted_in_range(0, i64::MAX).is_empty(),
                "commit doesn't populate the aborted list"
            );
            p.close().await.unwrap();
        });
    }

    #[test]
    fn abort_marker_populates_aborted_list_with_first_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            p.append(0, -1, build_marker_batch(42, false))
                .await
                .unwrap();
            let aborted = p.aborted_in_range(0, i64::MAX);
            assert_eq!(aborted.len(), 1);
            assert_eq!(aborted[0].producer_id, 42);
            assert_eq!(aborted[0].first_offset, 0);
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            // gh #277: a read_committed fetch resuming at offset 2, inside
            // the txn (data 0..=2, ABORT marker at 3), must still be told
            // it was aborted, or the consumer delivers offset 2.
            let from_inside = p.aborted_in_range(2, p.high_watermark());
            assert_eq!(from_inside.len(), 1);
            assert_eq!(from_inside[0].producer_id, 42);
            p.close().await.unwrap();
        });
    }

    /// gh #267: the takeover roll of a segment adopted at open must not
    /// date it at epoch 1970. Pre-fix, `open_meta_only` initialized the
    /// in-process largest-timestamp tracker to 0, the roll stamped that
    /// into the closed meta, and `cleanup_target_for_time` — which
    /// treats any value >= 0 as known — reaped the whole segment as
    /// "expired" on the next time-retention sweep: every rolling
    /// restart deleted up to retention.ms of fresh data within 5 min.
    #[test]
    fn a_rolled_adopted_segment_is_dated_by_mtime_not_epoch_zero() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let open = |fs: Arc<dyn Fs>| {
                Partition::open(
                    fs,
                    "t".into(),
                    0,
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                )
            };
            // Produce, then a clean close: the restart-shaped handoff.
            let p = open(fs.clone()).await.unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            p.close().await.unwrap();

            // Reopen (adopts the segment) and take over (rolls it
            // closed) — the sequence every restart runs.
            let p2 = open(fs.clone()).await.unwrap();
            p2.take_over(2).await.unwrap();
            let stamps = p2.closed_segment_max_timestamps();
            assert_eq!(stamps.len(), 1);
            assert_eq!(
                stamps[0],
                crate::segment::UNKNOWN_MAX_TIMESTAMP,
                "adopted segment rolled closed must not claim a known timestamp"
            );

            // The retention question itself: with a 7-day cutoff in the
            // past of "now", a freshly-written file must NOT be reaped.
            // Pre-fix this returned a target covering the whole segment.
            let cutoff = crate::segment::now_epoch_ms() - 7 * 24 * 3600 * 1000;
            assert_eq!(
                p2.cleanup_target_for_time(fs.as_ref(), cutoff),
                None,
                "time retention reaped a minutes-old segment (dated 1970)"
            );
            p2.close().await.unwrap();
        });
    }

    /// gh #168: a sticky `flush_err` heals once the substrate answers
    /// again — the probe loop clears the poison and appends resume,
    /// instead of the partition staying dead until a restart. Takes
    /// ~5 s (one real probe interval); the slow path is the point.
    #[test]
    fn a_transient_stall_rearms_and_appends_resume() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();

            // Poison the committer the way a transient fsync failure
            // does.
            p.inner.lock().flush_err = Some(StorageError::Stalled);
            assert!(p.is_stalled());
            assert!(matches!(
                p.append(0, -1, build_batch(1, 1_000)).await,
                Err(StorageError::Stalled)
            ));

            // Drive the probe directly (in production the committer
            // parks in it right after setting the poison). The tempdir
            // substrate is healthy, so the first attempt heals.
            probe_and_rearm(&p.inner, &p.flush.cond, &p.fs).await;
            assert!(!p.is_stalled(), "probe did not clear the poison");
            p.append(0, -1, build_batch(1, 1_000))
                .await
                .expect("appends must flow again after re-arm");
            p.close().await.unwrap();
        });
    }

    /// gh #177: both read_committed indexes survive close + reopen.
    /// Pre-fix, a restart snapped LSO to the HWM (serving a still-open
    /// transaction's records to read_committed consumers) and dropped
    /// the aborted list (serving aborted records with nothing to
    /// filter them by).
    #[test]
    fn txn_indexes_survive_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // pid 7: 3 records at 0..2, aborted (marker at 3).
            p.append(0, -1, build_txn_data_batch(7, 3)).await.unwrap();
            p.append(0, -1, build_marker_batch(7, false)).await.unwrap();
            // pid 42: 3 records at 4..6, left open across the restart.
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            assert_eq!(p.high_watermark(), 7);
            assert_eq!(p.last_stable_offset(), 4);
            p.close().await.unwrap();

            let p2 = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(p2.high_watermark(), 7);
            assert_eq!(
                p2.last_stable_offset(),
                4,
                "LSO must hold at the open txn's first offset across reopen"
            );
            let aborted = p2.aborted_in_range(0, i64::MAX);
            assert_eq!(aborted.len(), 1, "aborted list must survive reopen");
            assert_eq!(aborted[0].producer_id, 7);
            assert_eq!(aborted[0].first_offset, 0);

            // The open txn is still live state: its commit marker must
            // release the LSO exactly as if no restart had happened.
            p2.append(0, -1, build_marker_batch(42, true))
                .await
                .unwrap();
            assert_eq!(p2.last_stable_offset(), p2.high_watermark());
            p2.close().await.unwrap();
        });
    }

    /// gh #177: a snapshotted aborted entry whose span fell below
    /// log_start while the partition was closed is evicted on restore,
    /// and delete_records evicts live entries as it advances.
    #[test]
    fn aborted_entries_below_log_start_are_evicted() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_txn_data_batch(7, 3)).await.unwrap();
            p.append(0, -1, build_marker_batch(7, false)).await.unwrap();
            assert_eq!(p.aborted_in_range(0, i64::MAX).len(), 1);

            // Advance log_start past the abort marker: live eviction.
            p.delete_records(4).await.unwrap();
            assert!(
                p.aborted_in_range(0, i64::MAX).is_empty(),
                "delete_records must evict fully-covered aborted entries"
            );
            p.close().await.unwrap();

            // Restore-side eviction: plant a stale entry below the
            // persisted log_start and reopen — it must not come back.
            write_txn_index_snapshot(
                fs.as_ref(),
                tmp.path(),
                &[],
                &[crate::txn_index::AbortedTxn {
                    producer_id: 99,
                    first_offset: 0,
                    last_offset: 3,
                }],
            )
            .unwrap();
            let p2 = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert!(
                p2.aborted_in_range(0, i64::MAX).is_empty(),
                "restore must evict entries below the persisted log_start"
            );
            p2.close().await.unwrap();
        });
    }

    #[test]
    fn lso_picks_lowest_across_concurrent_open_txns() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // pid 1's first batch at offset 0
            p.append(0, -1, build_txn_data_batch(1, 2)).await.unwrap();
            // pid 2's first batch at offset 2
            p.append(0, -1, build_txn_data_batch(2, 4)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 0, "min of {{0, 2}}");

            // Commit pid 1 → LSO jumps to pid 2's first offset.
            p.append(0, -1, build_marker_batch(1, true)).await.unwrap();
            assert_eq!(p.last_stable_offset(), 2);

            // Commit pid 2 → LSO catches HWM.
            p.append(0, -1, build_marker_batch(2, true)).await.unwrap();
            assert_eq!(p.last_stable_offset(), p.high_watermark());
            p.close().await.unwrap();
        });
    }

    #[test]
    fn open_fresh_partition_returns_zero_hwm() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(p.high_watermark(), 0);
            assert_eq!(p.log_start_offset(), 0);
            assert_eq!(p.epoch(), 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn append_advances_hwm_and_returns_assigned_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            let base = p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(base, 0);
            assert_eq!(p.high_watermark(), 5);
            let base = p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            assert_eq!(base, 5);
            assert_eq!(p.high_watermark(), 8);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_waits_for_committer() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // acks=-1 must not return before the committer fsyncs.
            // Hard to assert ordering without injecting a clock, but
            // at minimum the call must not deadlock and must reflect
            // a non-zero completed_flush_seq.
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            assert!(p.inner.lock().completed_flush_seq >= 1);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_below_interval_does_not_wait_for_any_flush() {
        // gh #188: with the durability dial raised
        // (flush_interval_messages ≫ 1), an acks=all append that does
        // NOT cross the threshold must ack immediately — even if a
        // flush is pending or in flight — mirroring the v0.1 engine's
        // `triggeredFlushSeq <= 0 → no wait`. The pre-fix shape parked
        // every acks=all append on the last-requested seq, stalling
        // the whole pipeline for the fsync window each cycle.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 1_000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // Two 5-record acks=all batches: far below the interval,
            // so no flush is ever requested — the appends must return
            // without any committer round trip.
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().requested_flush_seq, 0);
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn acks_all_crossing_interval_still_waits() {
        // The other half of the gh #188 contract: the append that
        // crosses the threshold DOES wait for its fsync.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 8,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            // 5 + 5 crosses 8 → this append triggers seq 1 and must
            // not return before the committer has fsynced it.
            p.append(0, -1, build_batch(5, 1_000)).await.unwrap();
            assert!(p.inner.lock().completed_flush_seq >= 1);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn flush_interval_messages_zero_means_no_committer_trigger() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    flush_interval_messages: 0,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            // acks=0 → don't wait. With flush_interval=0 the committer
            // never fires.
            p.append(0, 0, build_batch(1, 1_000)).await.unwrap();
            assert_eq!(p.inner.lock().requested_flush_seq, 0);
            assert_eq!(p.inner.lock().completed_flush_seq, 0);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn epoch_fence_rejects_stale_writes() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // Bump the manifest's epoch manually under the lock.
            {
                let mut guard = p.inner.lock();
                guard.epoch = 5;
            }
            let err = p.append(2, -1, build_batch(1, 1_000)).await.unwrap_err();
            assert!(matches!(err, StorageError::EpochMismatch));
            p.close().await.unwrap();
        });
    }

    // --- gh #227: take_over wires the epoch end-to-end ---------------------

    /// The whole point of the fence: once a higher epoch has taken the
    /// partition over, the previous leader's in-flight produce requests
    /// are rejected instead of silently accepted.
    #[test]
    fn take_over_bumps_the_epoch_and_fences_the_previous_leader() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(4, 1_000)).await.unwrap();

            let hwm = p.take_over(3).await.unwrap();
            assert_eq!(hwm, 4);
            assert_eq!(p.epoch(), 3);

            // The old leader is still producing at its own epoch.
            let err = p.append(0, -1, build_batch(1, 1_000)).await.unwrap_err();
            assert!(matches!(err, StorageError::EpochMismatch));
            // The new one is not.
            assert_eq!(p.append(3, -1, build_batch(2, 1_000)).await.unwrap(), 4);
            p.close().await.unwrap();
        });
    }

    /// The roll is what keeps two leaders out of one file. A non-empty
    /// outgoing segment keeps its bytes and becomes a closed segment.
    #[test]
    fn take_over_rolls_to_a_new_epoch_stamped_segment() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(4, 1_000)).await.unwrap();
            let before = p.inner.lock().active.meta.clone();
            assert_eq!(before.epoch, 0);

            p.take_over(6).await.unwrap();

            let after = p.inner.lock().active.meta.clone();
            assert_ne!(
                after.log_path, before.log_path,
                "new leader must not append to the previous leader's file"
            );
            assert_eq!(after.epoch, 6);
            assert_eq!(after.base_offset, 4, "new segment starts at the HWM");
            // The outgoing segment is retained, with its records.
            assert!(fs.stat(&before.log_path).is_ok());
            assert_eq!(p.inner.lock().closed.len(), 1);
            assert_eq!(p.inner.lock().closed[0].base_offset, 0);

            // Its records are still readable across the roll.
            assert_eq!(
                u64::try_from(p.read(0, 1 << 20).await.unwrap().len()).unwrap(),
                before.size
            );
            p.close().await.unwrap();
        });
    }

    /// An empty outgoing segment has nowhere to roll *to* — its base
    /// offset is already the high watermark — so the replacement lands
    /// at the same base and the old file is reclaimed. Exactly one
    /// segment must remain, or partition open picks arbitrarily.
    #[test]
    fn take_over_over_an_empty_segment_leaves_exactly_one_file() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.take_over(7).await.unwrap();

            let segs = segment::list_segments(fs.as_ref(), tmp.path()).unwrap();
            assert_eq!(segs.len(), 1, "duplicate base offsets left on disk");
            assert_eq!(segs[0].epoch, 7);
            assert_eq!(segs[0].base_offset, 0);
            assert!(p.inner.lock().closed.is_empty());
            p.close().await.unwrap();
        });
    }

    /// `on_change` re-drives every assignment change and the gh #215
    /// reconcile re-drives on a timer, so a repeat at the same epoch has
    /// to be free — not another roll.
    #[test]
    fn take_over_at_or_below_the_current_epoch_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            p.take_over(4).await.unwrap();
            let settled = p.inner.lock().active.meta.clone();

            for epoch in [4, 1, 0] {
                assert_eq!(p.take_over(epoch).await.unwrap(), 2);
                assert_eq!(p.epoch(), 4, "epoch must never go backwards");
                assert_eq!(
                    p.inner.lock().active.meta.log_path,
                    settled.log_path,
                    "re-drive at epoch {epoch} rolled a segment"
                );
            }
            p.close().await.unwrap();
        });
    }

    /// The bump is persisted before the partition accepts writes, so a
    /// broker restarting at the old epoch cannot re-adopt the log.
    #[test]
    fn take_over_epoch_survives_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            p.take_over(9).await.unwrap();
            p.close().await.unwrap();

            let p2 = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(p2.epoch(), 9);
            assert_eq!(p2.high_watermark(), 3);
            let err = p2.append(8, -1, build_batch(1, 1_000)).await.unwrap_err();
            assert!(matches!(err, StorageError::EpochMismatch));
            p2.close().await.unwrap();
        });
    }

    /// A failed roll must leave the partition exactly as it was. The
    /// partition keeps serving either way, so a half-rolled one would
    /// hand out offsets from a segment set that no epoch describes; a
    /// re-drive can only recover a partition it finds intact.
    #[test]
    fn a_failed_take_over_leaves_the_partition_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let armed = Arc::new(VanishingFs::new(0));
            let fs: Arc<dyn Fs> = armed.clone();
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            let before = p.inner.lock().active.meta.clone();

            // The replacement segment can't be created.
            armed.arm(u32::MAX);
            let err = p.take_over(5).await.unwrap_err();
            assert!(matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::NotFound));

            assert_eq!(p.epoch(), 0, "epoch advanced without a segment to back it");
            assert_eq!(p.inner.lock().active.meta.log_path, before.log_path);
            assert!(p.inner.lock().closed.is_empty());
            // Still writable at the old epoch, and the retry succeeds.
            assert_eq!(p.append(0, -1, build_batch(1, 1_000)).await.unwrap(), 3);
            armed.arm(0);
            p.take_over(5).await.unwrap();
            assert_eq!(p.epoch(), 5);
            p.close().await.unwrap();
        });
    }

    /// gh #235: `Partition::open` reads the manifest, then runs a whole
    /// recovery (segment listing, handle opens, high-watermark scan)
    /// before writing it back. A takeover completing in that window has
    /// already raised the epoch on disk, and replaying the stale one
    /// over it unarms the fence silently — the new leader keeps its
    /// raised epoch in memory, so the takeover reconcile sees a
    /// converged partition and never re-drives it. This is the shape
    /// that left partitions one epoch behind on the cluster.
    #[test]
    fn open_does_not_replay_a_stale_epoch_over_a_concurrent_takeover() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            // Establish a partition at epoch 3 with four records.
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(4, 1_000)).await.unwrap();
            p.take_over(3).await.unwrap();
            p.close().await.unwrap();

            // Re-open while a peer's takeover lands epoch 9 mid-scan.
            let racing: Arc<dyn Fs> = Arc::new(RacingTakeoverFs {
                inner: RealFs::new(),
                lands: Manifest {
                    epoch: 9,
                    high_watermark: 4,
                    log_start_offset: 0,
                },
                fired: std::sync::atomic::AtomicBool::new(false),
            });
            let reopened = Partition::open(
                racing.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();

            let ReadResult::Present(on_disk) = manifest::read(racing.as_ref(), tmp.path()).unwrap()
            else {
                unreachable!("manifest was written above")
            };
            assert_eq!(on_disk.epoch, 9, "open replayed a stale epoch over disk");
            assert_eq!(
                reopened.epoch(),
                9,
                "open must adopt the durable epoch, not serve below it"
            );
            // Adopting it means the fence is armed, not merely recorded.
            let err = reopened
                .append(8, -1, build_batch(1, 1_000))
                .await
                .unwrap_err();
            assert!(matches!(err, StorageError::EpochMismatch));
            assert_eq!(reopened.high_watermark(), 4);
            reopened.close().await.unwrap();
        });
    }

    /// gh #227: the reported epoch must reflect *durable* state. Seen in
    /// production — the roll landed and stamped a new segment, the
    /// manifest persist hit ENOENT, and the partition then reported the
    /// new epoch from memory while the manifest still said otherwise.
    /// The takeover reconcile compares that reported epoch against the
    /// assignment to decide whether to re-drive, so a raised-but-unsaved
    /// epoch says "converged" and the partition is stranded for good.
    #[test]
    fn a_failed_persist_does_not_leave_the_epoch_raised() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            // Failing only `.tmp` creates breaks the metadata writer and
            // leaves segment creation intact — the production shape
            // exactly: the takeover roll lands, the persist does not.
            let armed = Arc::new(VanishingFs::new(0).only_ext("tmp"));
            let fs: Arc<dyn Fs> = armed.clone();
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(4, 1_000)).await.unwrap();

            armed.arm(u32::MAX);
            let err = p.take_over(6).await.unwrap_err();
            assert!(matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::NotFound));
            assert_eq!(
                p.epoch(),
                0,
                "epoch must not advance past what was persisted"
            );

            // The re-drive the reconcile will now issue has to succeed —
            // it rolls again at the same epoch, over the segment the
            // failed attempt already created.
            armed.arm(0);
            assert_eq!(p.take_over(6).await.unwrap(), 4);
            assert_eq!(p.epoch(), 6);

            // And the active segment still exists — the re-roll must not
            // have deleted the file it just created.
            let active = p.inner.lock().active.meta.clone();
            assert_eq!(active.epoch, 6);
            assert!(
                armed.stat(&active.log_path).is_ok(),
                "re-drive removed its own active segment"
            );
            // Records written before the takeover survive it.
            assert_eq!(p.high_watermark(), 4);
            p.close().await.unwrap();

            let reopened = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(reopened.epoch(), 6);
            assert_eq!(reopened.high_watermark(), 4);
            reopened.close().await.unwrap();
        });
    }

    /// A broker only learns it lost a partition when its assignment
    /// watch catches up, and it then relinquishes — persisting whatever
    /// it still holds. That write must not roll the epoch back or
    /// republish its stale high watermark over the new leader's.
    #[test]
    fn a_superseded_leaders_close_does_not_regress_the_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let old = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            old.append(0, -1, build_batch(5, 1_000)).await.unwrap();

            // The successor takes over off the shared volume.
            let new = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            new.take_over(2).await.unwrap();

            // Only now does the old leader notice and drain.
            old.close().await.unwrap();
            new.close().await.unwrap();

            // What the next open inherits is what matters.
            let reopened = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            assert_eq!(reopened.epoch(), 2, "fenced leader rolled the epoch back");
            assert_eq!(reopened.high_watermark(), 5);
            reopened.close().await.unwrap();
        });
    }

    #[test]
    fn read_returns_batches_from_start_offset() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            // Read from offset 1 — should skip the first batch.
            let got = p.read(1, 4096).await.unwrap();
            let one_len = build_batch(1, 1_000).len();
            assert_eq!(got.len(), one_len * 2);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_advances_log_start() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            let new_start = p.delete_records(3).await.unwrap();
            assert_eq!(new_start, 3);
            assert_eq!(p.log_start_offset(), 3);
            // Read clamps effective_start to log_start.
            let got = p.read(0, 4096).await.unwrap();
            let one_len = build_batch(1, 1_000).len();
            assert_eq!(got.len(), one_len * 2);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_purge_to_hwm() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..4 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            let new_start = p.delete_records(-1).await.unwrap();
            assert_eq!(new_start, 4);
            p.close().await.unwrap();
        });
    }

    #[test]
    fn delete_records_past_hwm_is_offset_out_of_range() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            let err = p.delete_records(999).await.unwrap_err();
            assert!(matches!(err, StorageError::OffsetOutOfRange));
            p.close().await.unwrap();
        });
    }

    #[test]
    fn reopen_recovers_hwm_and_log_start_from_manifest() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            // First open + append + delete_records + close.
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.delete_records(2).await.unwrap();
            p.close().await.unwrap();

            // Reopen — HWM + log_start should come from manifest.
            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.high_watermark(), 5);
            assert_eq!(p2.log_start_offset(), 2);
            p2.close().await.unwrap();
        });
    }

    /// gh #229: a manifest that outlived its segments — a dead
    /// incarnation's relinquish landing in a recreated dir, a partial
    /// restore, a half-finished `remove_dir_all` — describes a log that
    /// no longer exists. Open used to base a fresh segment at the
    /// manifest's HWM, so the partition advertised offsets it had no
    /// records for and a consumer reading from the start got nothing.
    /// Apache bases the new segment at log start and clamps the HWM to
    /// the log end; the snapshots describing the lost records go too.
    #[test]
    fn a_manifest_that_outlived_its_segments_is_not_adopted() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            // Left open, so the txn index and the producer snapshot both
            // carry an entry for records that are about to vanish.
            p.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            p.delete_records(1).await.unwrap();
            assert_eq!(p.high_watermark(), 5);
            assert_eq!(p.last_stable_offset(), 2);
            p.close().await.unwrap();

            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(segment::LOG_EXT) || name.ends_with(segment::INDEX_EXT) {
                    std::fs::remove_file(entry.path()).unwrap();
                }
            }

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.log_start_offset(), 1);
            assert_eq!(
                p2.high_watermark(),
                1,
                "the HWM must not claim records the log no longer has"
            );
            assert_eq!(
                p2.last_stable_offset(),
                1,
                "an open txn from the lost log must not hold the LSO above the HWM"
            );
            // The lost batch's sequence is not a duplicate any more: it
            // is written, at the reset log end, not echoed from the
            // dead dedupe window.
            let base = p2.append(0, -1, build_txn_data_batch(42, 3)).await.unwrap();
            assert_eq!(base, 1);
            assert_eq!(p2.high_watermark(), 4);
            p2.close().await.unwrap();
        });
    }

    /// gh #229 / gh #230: the lost log's recovery checkpoint names
    /// segment base 0 — and so does the segment the reset creates when
    /// log start is 0. Once the new log reaches the checkpoint's
    /// `byte_pos`, a crash recovery resumes there seeded with the dead
    /// log's HWM; with nothing left to scan, the seed *is* the answer.
    /// The reset has to take the checkpoint with it.
    #[test]
    fn a_reset_log_does_not_recover_from_the_lost_logs_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // Same bytes per batch as below, three times the offsets.
            for _ in 0..5 {
                p.append(0, -1, build_batch(3, 1_000)).await.unwrap();
            }
            p.close().await.unwrap(); // checkpoint at EOF: HWM 15

            for entry in std::fs::read_dir(&dir).unwrap().flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name.ends_with(segment::LOG_EXT) || name.ends_with(segment::INDEX_EXT) {
                    std::fs::remove_file(entry.path()).unwrap();
                }
            }

            let p2 = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            // Same batch count, so the new log ends at the old `byte_pos`.
            for _ in 0..5 {
                p2.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            assert_eq!(p2.high_watermark(), 5);
            // Crash: no close, so no fresh checkpoint papers over it.
            drop(p2);

            let p3 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p3.high_watermark(), 5);
            p3.close().await.unwrap();
        });
    }

    /// gh #220: an ENOENT during open means the directory is being
    /// re-created underneath us (the operator's reclaim renames it aside
    /// and mkdirs it again), never that the path is wrong — `open`
    /// mkdir_all's it first. Open must ride that out instead of handing
    /// the caller a produce error or a partition that stays closed until
    /// the next takeover reconcile.
    #[test]
    fn open_rides_out_a_vanishing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(VanishingFs::new(2));
            let p = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .expect("open should retry past a transient ENOENT");
            // And the partition is fully usable, not a husk.
            p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            assert_eq!(p.high_watermark(), 1);
        });
    }

    /// The retry is bounded — a directory that never comes back still
    /// surfaces the error rather than hanging the open forever.
    #[test]
    fn open_gives_up_on_a_permanently_missing_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(VanishingFs::new(u32::MAX));
            let err = Partition::open(fs, "t".into(), 0, dir, PartitionConfig::default())
                .await
                .expect_err("a permanent ENOENT must surface");
            assert!(matches!(err, StorageError::Io(e) if e.kind() == std::io::ErrorKind::NotFound));
        });
    }

    /// gh #219: `abandon` must not write state back. The scenario is a
    /// topic delete→recreate: the operator stages the old directory
    /// aside and a fresh one appears at the same path, so anything the
    /// dying partition persists lands in the *new* incarnation's dir
    /// (an advanced HWM over an empty log, plus a poisoned idempotence
    /// window).
    #[test]
    fn abandon_persists_nothing_into_a_recreated_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("t").join("0");
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            assert_eq!(p.high_watermark(), 5);

            // Stand in for the operator's reclaim: the directory this
            // partition was opened on is replaced by an empty one.
            std::fs::remove_dir_all(tmp.path().join("t")).unwrap();
            std::fs::create_dir_all(&dir).unwrap();

            p.abandon().await;

            let leftovers: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .flatten()
                .map(|e| e.file_name().to_string_lossy().to_string())
                .collect();
            assert!(
                leftovers.is_empty(),
                "abandon must leave the recreated dir untouched, found {leftovers:?}"
            );

            // And the recreated partition starts from zero.
            let fresh = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(fresh.high_watermark(), 0);
            assert_eq!(fresh.log_start_offset(), 0);
        });
    }

    /// gh #recovery-checkpoint: a clean close checkpoints the active
    /// segment at EOF, and reopen recovers the HWM from it.
    #[test]
    fn close_writes_checkpoint_at_eof_and_reopen_recovers() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..5 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.close().await.unwrap();

            // The checkpoint exists, sits at EOF, and names the start of
            // the last batch — one batch length before EOF.
            let cp = recovery_checkpoint::read(fs.as_ref(), &dir)
                .expect("clean close writes a recovery checkpoint");
            assert_eq!(cp.segment_base, 0);
            let one = i64::try_from(build_batch(1, 1_000).len()).unwrap();
            assert_eq!(cp.last_batch_pos, Some(cp.byte_pos - one));
            let log_len = std::fs::metadata(dir.join("00000000-00000000000000000000.log"))
                .unwrap()
                .len();
            assert_eq!(
                u64::try_from(cp.byte_pos).unwrap(),
                log_len,
                "checkpoint at EOF"
            );

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.high_watermark(), 5);
            p2.close().await.unwrap();
        });
    }

    /// The checkpoint guard: a checkpoint that names a *different*
    /// active segment is ignored, and recovery falls back to a correct
    /// full scan.
    #[test]
    fn stale_checkpoint_for_other_segment_is_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.close().await.unwrap();

            // Poison the checkpoint: claim a segment base that doesn't
            // match, with a bogus HWM. Recovery must ignore it.
            recovery_checkpoint::write(
                fs.as_ref(),
                &dir,
                &RecoveryCheckpoint {
                    segment_base: 999,
                    byte_pos: 1,
                    last_batch_pos: Some(0),
                },
            )
            .unwrap();

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(
                p2.high_watermark(),
                3,
                "fell back to a full scan, not the bogus HWM"
            );
            p2.close().await.unwrap();
        });
    }

    /// gh #230: a checkpoint that names *this* segment but positions
    /// that don't fall on its batches — another incarnation's, a copied
    /// directory's — must neither supply the HWM nor truncate the log.
    /// Before the fix recovery trusted its HWM and resumed mid-batch, so
    /// the scan "proved" garbage at that position and the torn-tail
    /// truncation cut real, acked batches off the log.
    #[test]
    fn a_checkpoint_off_the_batch_boundaries_is_ignored_and_truncates_nothing() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..4 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            p.close().await.unwrap();
            let log = dir.join("00000000-00000000000000000000.log");
            let len_before = std::fs::metadata(&log).unwrap().len();

            // Same segment base, plausible sizes, but mid-batch.
            let one = i64::try_from(build_batch(1, 1_000).len()).unwrap();
            recovery_checkpoint::write(
                fs.as_ref(),
                &dir,
                &RecoveryCheckpoint {
                    segment_base: 0,
                    byte_pos: one + 7,
                    last_batch_pos: Some(7),
                },
            )
            .unwrap();

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(
                p2.high_watermark(),
                4,
                "HWM from the batches, not the checkpoint"
            );
            assert_eq!(
                std::fs::metadata(&log).unwrap().len(),
                len_before,
                "an unverified checkpoint must not drive the torn-tail truncation"
            );
            p2.close().await.unwrap();
        });
    }

    /// gh #230: after a crash, recovery resumes on the checkpointed batch
    /// and still counts what was appended after the checkpoint.
    #[test]
    fn a_crash_after_the_checkpoint_resumes_and_keeps_later_batches() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().to_path_buf();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            }
            p.close().await.unwrap(); // checkpoint after batch 3: HWM 6
            let p = Partition::open(
                fs.clone(),
                "t".into(),
                0,
                dir.clone(),
                PartitionConfig::default(),
            )
            .await
            .unwrap();
            for _ in 0..2 {
                p.append(0, -1, build_batch(2, 1_000)).await.unwrap();
            }
            drop(p); // crash: the checkpoint still says "after batch 3"

            let p2 = Partition::open(fs, "t".into(), 0, dir.clone(), PartitionConfig::default())
                .await
                .unwrap();
            assert_eq!(p2.high_watermark(), 10);
            p2.close().await.unwrap();
        });
    }

    #[test]
    fn segment_roll_at_size_threshold() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let p = Partition::open(
                fs,
                "t".into(),
                0,
                tmp.path().to_path_buf(),
                PartitionConfig {
                    // Roll after one batch.
                    segment_bytes: one_len,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            for _ in 0..3 {
                p.append(0, -1, build_batch(1, 1_000)).await.unwrap();
            }
            // After 3 appends with segment_bytes == one batch, we
            // should have at least 1 closed segment.
            assert!(
                !p.inner.lock().closed.is_empty(),
                "expected at least one roll"
            );
            p.close().await.unwrap();
        });
    }

    #[test]
    fn high_watermark_read_is_lock_free() {
        // Smoke-test: while a background appender holds the inner
        // mutex briefly, high_watermark() returns without blocking.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let p = Arc::new(
                Partition::open(
                    fs,
                    "t".into(),
                    0,
                    tmp.path().to_path_buf(),
                    PartitionConfig::default(),
                )
                .await
                .unwrap(),
            );
            // Fire 10 concurrent reads — they must not deadlock with
            // the append path or each other.
            let mut handles = Vec::new();
            for _ in 0..10 {
                let p2 = p.clone();
                handles.push(tokio::spawn(async move { p2.high_watermark() }));
            }
            for h in handles {
                let _ = h.await.unwrap();
            }
            p.close().await.unwrap();
        });
    }
}
