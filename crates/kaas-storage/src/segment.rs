//! Segment files: filenames, [`SegmentMeta`], [`ActiveSegment`].
//!
//! Segment files: the file-I/O surface on top of the filename helpers that
//! landed in the initial slice.
//!
//! # Layout
//!
//! Each segment is a pair of files in the partition directory:
//!
//! - `{epoch:08x}-{base_offset:020d}.log`
//! - `{epoch:08x}-{base_offset:020d}.index`
//!
//! Encoding the leader epoch in the filename prevents a stale ex-leader's
//! writes from colliding with a new leader's file (the gh #75 / gh #76
//! single-writer story). Pre-Phase-4 partitions on disk use the legacy
//! unprefixed form `{base_offset:020d}.log`; [`parse_segment_stem`]
//! accepts both so an in-place upgrade Just Works.
//!
//! # Byte opacity
//!
//! [`ActiveSegment::append_batch`] writes raw RecordBatch bytes through
//! `Fs::write_at` without inspecting any record. The 43-byte head it
//! parses for `(base_offset, last_offset_delta, max_timestamp)` is
//! batch-header only — the records payload that follows is never
//! touched.

use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

use crate::fs::{FileRead, FileWrite, Fs};

/// `.log` filename suffix.
pub const LOG_EXT: &str = ".log";

/// `.index` filename suffix.
pub const INDEX_EXT: &str = ".index";

/// "Sealed-by-takeover" marker — produced by older (v0.1) releases; new
/// writes never emit it. [`list_segments`] skips files with this
/// suffix.
const SEALED_EXT: &str = ".log.sealed";

/// Wire size of one sparse-index entry: `(rel_offset:i32, file_pos:i32)`.
pub const INDEX_ENTRY_SIZE: usize = 8;

/// Wall clock as epoch milliseconds, saturating at 0 below the epoch.
pub(crate) fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Bufio window size for [`scan_high_watermark`]. 4 MiB lets one NFS
/// READ RPC carry many batches of work — without this, startup
/// becomes thousands of round-trips per partition.
const SCAN_HWM_BUF_SIZE: usize = 4 * 1024 * 1024;

/// On-disk pointer to a single segment. Owned by the partition's
/// closed-segment list and the active segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub base_offset: i64,
    pub epoch: i64,
    /// Log file size in bytes. `0` is a valid value at creation;
    /// callers update this after appends.
    pub size: u64,
    pub log_path: PathBuf,
    pub index_path: PathBuf,
    /// Largest record timestamp this segment is known to hold, or
    /// `-1` when unknown. Set when the segment is rolled closed (the
    /// active segment tracked it across its appends); `-1` for
    /// segments discovered by [`list_segments`], since nothing
    /// persists it — the manifest carries no segment list.
    ///
    /// Time-based retention treats `-1` as "fall back to file mtime",
    /// which is what Apache does when a segment has no usable
    /// timestamp (`LogSegment.largestTimestamp` → `lastModified`).
    /// A restarted broker therefore ages its inherited segments by
    /// mtime and its own by record timestamp. See
    /// [`crate::partition::Partition::cleanup_target_for_time`].
    pub max_timestamp: i64,
}

/// `max_timestamp` value meaning "not known for this segment".
pub const UNKNOWN_MAX_TIMESTAMP: i64 = -1;

/// Construct the `.log` path for `(dir, base_offset, epoch)`.
pub fn segment_log_path(dir: &Path, base_offset: i64, epoch: i64) -> PathBuf {
    let stem = epoch_prefixed_stem(base_offset, epoch);
    dir.join(format!("{stem}{LOG_EXT}"))
}

/// Construct the `.index` path matching [`segment_log_path`].
pub fn segment_index_path(dir: &Path, base_offset: i64, epoch: i64) -> PathBuf {
    let stem = epoch_prefixed_stem(base_offset, epoch);
    dir.join(format!("{stem}{INDEX_EXT}"))
}

/// Recover `(base_offset, epoch)` from a stem (filename without `.log` /
/// `.index` suffix). Accepts both layouts:
///
/// - Epoch-prefixed: `"00000005-00000000000000000123"` → `(123, 5)`
/// - Legacy: `"00000000000000000123"` → `(123, 0)`
///
/// Returns `None` for any other shape; the caller (segment listing) skips
/// unknown files.
pub fn parse_segment_stem(stem: &str) -> Option<(i64, i64)> {
    if let Some((epoch_part, base_part)) = stem.split_once('-') {
        let epoch = u32::from_str_radix(epoch_part, 16).ok()?;
        let base = base_part.parse::<i64>().ok()?;
        Some((base, i64::from(epoch)))
    } else {
        let base = stem.parse::<i64>().ok()?;
        Some((base, 0))
    }
}

fn epoch_prefixed_stem(base_offset: i64, epoch: i64) -> String {
    let epoch_u32 = i64_low_u32(epoch);
    format!("{epoch_u32:08x}-{base_offset:020}")
}

fn i64_low_u32(v: i64) -> u32 {
    let bytes = v.to_le_bytes();
    let mut lo = [0u8; 4];
    lo.copy_from_slice(&bytes[..4]);
    u32::from_le_bytes(lo)
}

// ---------------------------------------------------------------------------
// list_segments
// ---------------------------------------------------------------------------

/// Scan `dir` for segment files and return their metadata sorted by
/// `base_offset`. Skips `.log.sealed` markers and any file with an
/// unparseable stem. Used by partition open.
///
/// At most one segment is returned per `base_offset`: two files can
/// share one only when a leader epoch bump rolled over an *empty*
/// active segment (gh #227), which produces a new epoch-stamped file at
/// the same base offset as the one it supersedes. The higher epoch
/// wins. The superseded file is by construction empty at the moment of
/// the roll — a segment holding bytes rolls to a strictly higher base —
/// so anything in it was written by a leader that had already been
/// fenced, which is exactly what must not be adopted.
pub fn list_segments(fs: &dyn Fs, dir: &Path) -> io::Result<Vec<SegmentMeta>> {
    let entries = match fs.readdir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };

    let mut out: Vec<SegmentMeta> = Vec::new();
    for path in entries {
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            continue;
        };
        if !name.ends_with(LOG_EXT) || name.ends_with(SEALED_EXT) {
            continue;
        }
        // Strip `.log` (4 chars) — never `.log.sealed` (filtered above).
        let stem = &name[..name.len() - LOG_EXT.len()];
        let (base_offset, epoch) = match parse_segment_stem(stem) {
            Some(x) => x,
            None => continue,
        };
        let size = fs.stat(&path).map(|m| m.len()).unwrap_or(0);
        out.push(SegmentMeta {
            base_offset,
            epoch,
            size,
            log_path: path.clone(),
            index_path: dir.join(format!("{stem}{INDEX_EXT}")),
            // Nothing persists a segment's largest timestamp, so a
            // segment we didn't roll ourselves is unknown; time-based
            // retention falls back to mtime for it.
            max_timestamp: UNKNOWN_MAX_TIMESTAMP,
        });
    }
    // Highest epoch first within a base offset, so the dedup keeps the
    // newest leader's file.
    out.sort_by_key(|m| (m.base_offset, std::cmp::Reverse(m.epoch)));
    out.dedup_by_key(|m| m.base_offset);
    Ok(out)
}

// ---------------------------------------------------------------------------
// parse_batch_offsets
// ---------------------------------------------------------------------------

/// Pull the offset-bearing fields out of a v2 RecordBatch header.
///
/// Returns `(base_offset, last_offset_delta, max_timestamp)`. Reads
/// only the first 43 bytes of the batch — records payload is never
/// touched. Byte-opacity preserved.
pub fn parse_batch_offsets(raw: &[u8]) -> Result<(i64, i32, i64), io::Error> {
    if raw.len() < 43 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("batch too short: {} bytes (need 43)", raw.len()),
        ));
    }
    let mut o8 = [0u8; 8];
    o8.copy_from_slice(&raw[0..8]);
    let base_offset = i64::from_be_bytes(o8);

    let mut d4 = [0u8; 4];
    d4.copy_from_slice(&raw[23..27]);
    let last_offset_delta = i32::from_be_bytes(d4);

    let mut t8 = [0u8; 8];
    t8.copy_from_slice(&raw[35..43]);
    let max_timestamp = i64::from_be_bytes(t8);

    Ok((base_offset, last_offset_delta, max_timestamp))
}

// ---------------------------------------------------------------------------
// scan_high_watermark
// ---------------------------------------------------------------------------

/// Fill in a v2 batch's CRC32C field so it survives the recovery
/// scan's gh #228 verification.
///
/// Test-only: real producers compute this client-side, and a synthetic
/// batch that leaves the field zeroed is byte-for-byte indistinguishable
/// from bit-rot — which is exactly what the scan now rejects.
///
/// Exposed to other crates' tests behind the `test-helpers` feature
/// (same pattern as `kaas-auth`), so an integration test can build a
/// batch the recovery scan will accept without duplicating the CRC
/// layout — a copy of it would drift from the real one silently.
#[cfg(any(test, feature = "test-helpers"))]
pub fn stamp_crc(buf: &mut [u8]) {
    let crc = crc32c::crc32c(&buf[21..]);
    buf[17..21].copy_from_slice(&crc.to_be_bytes());
}

/// Why [`scan_high_watermark_from`] stopped walking the log.
///
/// Only [`ScanStop::Eof`] means the whole file was valid. The other two
/// say the log has a tail that must not be kept: `Torn` is a short or
/// structurally impossible batch (the classic crash-mid-write shape),
/// `CrcMismatch` is a batch whose bytes decode structurally but do not
/// match their own checksum — bit-rot, or the byte-interleaving a
/// dual-writer window produces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanStop {
    /// Clean end of file — every byte scanned was a valid batch.
    Eof,
    /// A batch was short, or its length field was structurally impossible.
    Torn,
    /// A batch failed CRC32C verification (gh #228).
    CrcMismatch,
}

/// Outcome of a log scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanResult {
    /// Next offset to write — one past the last record of the last
    /// valid batch.
    pub high_watermark: i64,
    /// Byte position just past the last *valid* batch. Anything at or
    /// after this is garbage and must be truncated before the log is
    /// appended to again (gh #226), or writes land past it and the next
    /// recovery silently drops them.
    pub valid_end: u64,
    /// Byte position where the last *valid* batch starts; `None` when the
    /// scan found none. A recovery checkpoint records this so the next
    /// recovery can resume on a batch it can verify (gh #230).
    pub last_batch_start: Option<u64>,
    /// Why the scan stopped.
    pub stop: ScanStop,
}

/// Scan a log file to find the high watermark (next offset to write).
///
/// Stops at the first malformed, truncated, or CRC-failing batch — that's
/// the post-crash partial-write boundary, and the returned HWM reflects
/// only fully-persisted batches. Read through a 4 MiB-window
/// [`BufReader`] so NFS substrates open large logs in MB/s, not
/// RPCs/s.
///
/// Returns only the high watermark; callers that intend to *write* to
/// the log need [`scan_high_watermark_from`]'s `valid_end` as well, or
/// they will append past a torn tail.
pub fn scan_high_watermark<R: Read + Seek>(f: &mut R, segment_base_offset: i64) -> io::Result<i64> {
    // Whole-segment scan: start at byte 0, seed the running HWM with
    // the segment's base offset (an empty segment has HWM == base).
    Ok(scan_high_watermark_from(f, 0, segment_base_offset)?.high_watermark)
}

/// Like [`scan_high_watermark`] but resumes a partial scan: start at
/// byte `start_byte` (a batch boundary the recovery checkpoint recorded
/// as fsynced) with the running HWM seeded to `start_hwm` (the
/// log-end-offset at that boundary). Everything before `start_byte` is
/// trusted; only the tail is read. gh #recovery-checkpoint turns
/// takeover recovery from O(active segment) into O(bytes since the last
/// checkpoint).
pub fn scan_high_watermark_from<R: Read + Seek>(
    f: &mut R,
    start_byte: u64,
    start_hwm: i64,
) -> io::Result<ScanResult> {
    f.seek(SeekFrom::Start(start_byte))?;
    let mut br = BufReader::with_capacity(SCAN_HWM_BUF_SIZE, f);

    let mut hwm = start_hwm;
    let mut valid_end = start_byte;
    let mut last_batch_start = None;
    let mut header = [0u8; 12];
    let mut body_head = [0u8; 16];

    let stop = loop {
        if br.read_exact(&mut header).is_err() {
            break ScanStop::Eof;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&header[8..12]);
        let batch_length = i32::from_be_bytes(len_bytes);
        let body_head_len_i32 = i32::try_from(body_head.len()).unwrap_or(i32::MAX);
        if batch_length < body_head_len_i32 {
            break ScanStop::Torn;
        }
        if br.read_exact(&mut body_head).is_err() {
            break ScanStop::Torn;
        }
        // Read (not discard) the rest of the body: the CRC covers it.
        let body_head_len_i32_2 = body_head_len_i32;
        let rest_i32 = batch_length - body_head_len_i32_2;
        let Ok(rest_us) = usize::try_from(rest_i32) else {
            break ScanStop::Torn;
        };
        let mut rest = vec![0u8; rest_us];
        if br.read_exact(&mut rest).is_err() {
            break ScanStop::Torn;
        }
        // gh #228: verify the batch against its own CRC32C before
        // trusting the offsets in its header. The v2 CRC field sits at
        // batch byte 17..21 and covers byte 21 to the end of the batch,
        // i.e. body_head[5..9] and body_head[9..] ++ rest here. This
        // reads no record contents — byte-opacity is preserved.
        let mut crc_bytes = [0u8; 4];
        crc_bytes.copy_from_slice(&body_head[5..9]);
        let expected = u32::from_be_bytes(crc_bytes);
        let mut digest = crc32c::crc32c(&body_head[9..]);
        digest = crc32c::crc32c_append(digest, &rest);
        if digest != expected {
            break ScanStop::CrcMismatch;
        }
        // base_offset := [0..8]; last_offset_delta := body_head[11..15].
        let mut o8 = [0u8; 8];
        o8.copy_from_slice(&header[0..8]);
        let base_offset = i64::from_be_bytes(o8);
        let mut d4 = [0u8; 4];
        d4.copy_from_slice(&body_head[11..15]);
        let last_offset_delta = i32::from_be_bytes(d4);
        hwm = base_offset + i64::from(last_offset_delta) + 1;
        // Only now is the batch known-good, so only now does the valid
        // region grow. 12 = baseOffset + batchLength fields, which
        // batch_length itself does not cover.
        last_batch_start = Some(valid_end);
        valid_end += u64::from(u32::try_from(batch_length).unwrap_or(0)) + 12;
    };
    Ok(ScanResult {
        high_watermark: hwm,
        valid_end,
        last_batch_start,
        stop,
    })
}

// ---------------------------------------------------------------------------
// search_index
// ---------------------------------------------------------------------------

/// Binary-search the index for the largest entry whose `rel_offset` is
/// `<= (target_offset - segment_base_offset)`. Returns the
/// corresponding file position, or 0 if the index is empty or the
/// target precedes every entry.
pub fn search_index_bytes(data: &[u8], segment_base_offset: i64, target_offset: i64) -> i64 {
    let n = data.len() / INDEX_ENTRY_SIZE;
    if n == 0 {
        return 0;
    }
    let Ok(target_rel) = i32::try_from(target_offset - segment_base_offset) else {
        // target_offset much larger than segment_base — clamp; we'll
        // return the last entry below.
        return read_index_entry(data, n - 1).1;
    };
    if target_rel < 0 {
        return 0;
    }
    let mut lo = 0usize;
    let mut hi = n - 1;
    let mut result = 0i64;
    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let (rel_offset, pos) = read_index_entry(data, mid);
        if rel_offset <= target_rel {
            result = pos;
            lo = mid + 1;
        } else if mid == 0 {
            break;
        } else {
            hi = mid - 1;
        }
    }
    result
}

fn read_index_entry(data: &[u8], i: usize) -> (i32, i64) {
    let off = i * INDEX_ENTRY_SIZE;
    let mut rel = [0u8; 4];
    rel.copy_from_slice(&data[off..off + 4]);
    let mut pos = [0u8; 4];
    pos.copy_from_slice(&data[off + 4..off + 8]);
    (i32::from_be_bytes(rel), i64::from(u32::from_be_bytes(pos)))
}

/// File-based variant of [`search_index_bytes`]. Reads the full index
/// into memory; the partition's hot path will swap this for an mmap'd
/// snapshot in a later commit (`#[cfg(feature = "mmap")]`).
pub fn search_index(
    fs: &dyn Fs,
    index_path: &Path,
    segment_base_offset: i64,
    target_offset: i64,
) -> i64 {
    let Ok(mut f) = fs.open_read(index_path) else {
        return 0;
    };
    let mut buf = Vec::new();
    if io::Read::read_to_end(&mut f, &mut buf).is_err() {
        return 0;
    }
    search_index_bytes(&buf, segment_base_offset, target_offset)
}

// ---------------------------------------------------------------------------
// ActiveSegment
// ---------------------------------------------------------------------------

/// The currently-open segment of a partition. Holds the log and index
/// file handles (Some when the partition is held by this broker; None
/// for followers per the gh #76 single-FD contract) plus accounting
/// state.
pub struct ActiveSegment {
    pub meta: SegmentMeta,
    log: Option<Box<dyn FileWrite>>,
    index: Option<Box<dyn FileWrite>>,
    /// Total log file size in bytes. Stays in sync with `meta.size`
    /// after every successful append.
    log_size: u64,
    /// Highest record offset appended so far. `base_offset - 1` when
    /// no records have been appended.
    last_offset: i64,
    /// `log_size` at which the last index entry was written; the next
    /// entry is emitted when `log_size - last_indexed_log_pos >=
    /// index_interval_bytes`.
    last_indexed_log_pos: u64,
    /// Byte position where the last batch in the log starts, for the
    /// recovery checkpoint (gh #230). `None` until a batch is appended or
    /// recovery reports one ([`Self::set_last_batch_start`]).
    last_batch_start: Option<u64>,
    /// Highest `max_timestamp` seen across appended batches. Used by
    /// the cleaner's retention.ms check and reported in
    /// [`SegmentMeta`] when this segment is closed.
    max_timestamp: i64,
    /// Wall clock (epoch ms) when this segment was created in *this*
    /// process, for the `segment.ms` age roll. `None` for a segment
    /// adopted from disk, where [`Self::age_ms`] falls back to the log
    /// file's mtime — the same substitute time-based retention uses
    /// for an inherited segment.
    created_at_ms: Option<i64>,
}

impl std::fmt::Debug for ActiveSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ActiveSegment")
            .field("meta", &self.meta)
            .field("log_open", &self.log.is_some())
            .field("index_open", &self.index.is_some())
            .field("log_size", &self.log_size)
            .field("last_offset", &self.last_offset)
            .field("max_timestamp", &self.max_timestamp)
            .finish()
    }
}

impl ActiveSegment {
    /// Create a fresh segment under `dir` starting at `base_offset`
    /// with leader epoch `epoch`. Opens both files truncated and
    /// returns the active segment with handles ready for append.
    pub fn create(fs: &dyn Fs, dir: &Path, base_offset: i64, epoch: i64) -> io::Result<Self> {
        fs.mkdir_all(dir)?;
        let log_path = segment_log_path(dir, base_offset, epoch);
        let index_path = segment_index_path(dir, base_offset, epoch);
        let log = fs.create(&log_path)?;
        let index = match fs.create(&index_path) {
            Ok(f) => f,
            Err(e) => {
                let _ = fs.remove(&log_path);
                return Err(e);
            }
        };
        // gh #231: persist the two new directory entries before anything
        // is appended. The log's own fsync covers its data, not its name,
        // so on a local filesystem a crash could otherwise lose a segment
        // that held acked records.
        if let Err(e) = fs.fsync_dir(dir) {
            let _ = fs.remove(&log_path);
            let _ = fs.remove(&index_path);
            return Err(e);
        }
        Ok(Self {
            meta: SegmentMeta {
                base_offset,
                epoch,
                size: 0,
                log_path,
                index_path,
                max_timestamp: UNKNOWN_MAX_TIMESTAMP,
            },
            log: Some(log),
            index: Some(index),
            log_size: 0,
            last_offset: base_offset - 1,
            last_indexed_log_pos: 0,
            last_batch_start: None,
            // gh #267: UNKNOWN, not 0 — this value gets stamped into
            // the closed SegmentMeta on roll, and the cleaner treats
            // any value >= 0 as a real timestamp. A 0 dates the
            // segment at epoch 1970, which time retention reaps on
            // its next sweep. `append_batch` only ever raises it, so
            // a segment with observed appends stamps a real value.
            max_timestamp: UNKNOWN_MAX_TIMESTAMP,
            created_at_ms: Some(now_epoch_ms()),
        })
    }

    /// Statify an existing segment from disk **without** opening file
    /// handles. Used on follower brokers and during partition open
    /// before takeover. Call [`Self::open_handles`] to materialise
    /// the handles when this broker becomes leader.
    pub fn open_meta_only(meta: SegmentMeta) -> Self {
        let log_size = meta.size;
        Self {
            meta,
            log: None,
            index: None,
            log_size,
            last_offset: 0,
            last_indexed_log_pos: 0,
            last_batch_start: None,
            // gh #267: UNKNOWN, not 0. This is the arm that ate data:
            // the takeover roll on every restart closed the adopted
            // active segment with this value stamped as its largest
            // timestamp, and a 0 reads as epoch 1970 — time retention
            // reaped the whole segment on its next sweep. UNKNOWN
            // routes the cleaner to its mtime fallback, which is the
            // documented dating for inherited segments.
            max_timestamp: UNKNOWN_MAX_TIMESTAMP,
            // Adopted from disk — this process didn't create it, so
            // `age_ms` dates it by the file instead.
            created_at_ms: None,
        }
    }

    /// How long this segment has been open, in milliseconds.
    ///
    /// Uses the in-process creation time when we have one, and the log
    /// file's mtime otherwise — a segment adopted at partition open has
    /// no creation time here, and its mtime is when it was last
    /// appended to, which is the closest thing on disk. A segment we
    /// can't date at all reports age 0, so an un-stattable file is
    /// never rolled on age (the size arm still applies).
    ///
    /// **The mtime lookup is memoised, and must stay that way.** This
    /// runs on the append path — once per produced batch — and every
    /// partition comes back from a restart or a takeover with an
    /// adopted segment, so a non-memoised version issues a `stat` per
    /// batch against the shared volume. Resolving once is also more
    /// correct: mtime moves with every append, so re-reading it would
    /// make a busy segment perpetually "just written" and it would
    /// never roll on age at all.
    pub fn age_ms(&mut self, fs: &dyn Fs) -> u64 {
        let started = match self.created_at_ms {
            Some(ms) => ms,
            None => {
                let resolved = fs
                    .stat(&self.meta.log_path)
                    .ok()
                    .and_then(|m| m.modified().ok())
                    .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                    .and_then(|d| i64::try_from(d.as_millis()).ok());
                match resolved {
                    Some(ms) => {
                        self.created_at_ms = Some(ms);
                        ms
                    }
                    // Un-stattable: don't retry per batch either.
                    // Age 0 means "never roll this on time"; the size
                    // arm still applies.
                    None => {
                        self.created_at_ms = Some(now_epoch_ms());
                        return 0;
                    }
                }
            }
        };
        u64::try_from(now_epoch_ms().saturating_sub(started)).unwrap_or(0)
    }

    /// Materialise the log + index file handles. Idempotent — safe to
    /// call when handles are already open. Called from
    /// `Partition::take_over` when this broker becomes leader.
    pub fn open_handles(&mut self, fs: &dyn Fs) -> io::Result<()> {
        if self.log.is_some() && self.index.is_some() {
            return Ok(());
        }
        if self.log.is_none() {
            self.log = Some(fs.open_write(&self.meta.log_path, false)?);
        }
        if self.index.is_none() {
            match fs.open_write(&self.meta.index_path, false) {
                Ok(f) => self.index = Some(f),
                Err(e) => {
                    self.log = None;
                    return Err(e);
                }
            }
        }
        // Refresh log_size / last_indexed_log_pos from disk in case
        // the file changed between meta-only open and now (e.g., a
        // stale leader wrote past our cached size before we took
        // over).
        if let Ok(meta) = fs.stat(&self.meta.log_path) {
            self.log_size = meta.len();
            self.meta.size = meta.len();
        }
        if let Ok(meta) = fs.stat(&self.meta.index_path) {
            // Map "N index entries" to "approx 4096*N bytes of log
            // covered". Rough estimate; an
            // exact value isn't required for appends.
            self.last_indexed_log_pos = meta.len() / 8 * 4096;
        }
        Ok(())
    }

    /// Drop everything at or after `valid_end` (gh #226).
    ///
    /// Called from partition open once the recovery scan has found the
    /// end of the last valid batch. Two effects, and the second is the
    /// load-bearing one:
    ///
    /// 1. The file shrinks, so a later scan doesn't re-encounter the
    ///    garbage.
    /// 2. `log_size` — the offset `append_batch` writes at — moves back
    ///    to the valid end. Without this, appends resume at the file's
    ///    *stat* length, i.e. past the torn region, and the next
    ///    recovery stops at the tear and silently drops every record
    ///    written after it (they were acked, and at
    ///    `flushIntervalMessages: 1` they were fsynced).
    ///
    /// An `Fs` without a real `truncate` still gets effect 2, which is
    /// what keeps the data-loss window closed; it just leaves the dead
    /// bytes on disk to be overwritten.
    pub fn truncate_to(&mut self, fs: &dyn Fs, valid_end: u64) -> io::Result<()> {
        if valid_end >= self.log_size {
            return Ok(());
        }
        match fs.truncate(&self.meta.log_path, valid_end) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::Unsupported => {}
            Err(e) => return Err(e),
        }
        self.log_size = valid_end;
        if self.last_batch_start.is_some_and(|p| p >= valid_end) {
            self.last_batch_start = None;
        }
        self.meta.size = valid_end;
        Ok(())
    }

    /// Release the log + index file descriptors. Idempotent. Called
    /// from `Partition::relinquish` when this broker loses leadership
    /// — the gh #76 contract that lets the new leader's `os.remove`
    /// actually free disk on NFS.
    pub fn close_handles(&mut self) {
        self.log = None;
        self.index = None;
    }

    /// Highest record offset appended so far; `base_offset - 1`
    /// before any appends.
    pub fn last_offset(&self) -> i64 {
        self.last_offset
    }

    /// Total log file size in bytes.
    pub fn log_size(&self) -> u64 {
        self.log_size
    }

    /// Where the last batch in the log starts (gh #230).
    pub fn last_batch_start(&self) -> Option<u64> {
        self.last_batch_start
    }

    /// Adopt the last valid batch position a recovery scan found, so the
    /// first checkpoint after open can name it.
    pub fn set_last_batch_start(&mut self, pos: Option<u64>) {
        self.last_batch_start = pos;
    }

    /// Highest `max_timestamp` seen so far.
    pub fn max_timestamp(&self) -> i64 {
        self.max_timestamp
    }

    /// Append a raw RecordBatch. Writes the bytes at the current end
    /// of the log via `write_at`, conditionally emits a sparse index
    /// entry, and updates accounting. Records bytes are NOT inspected
    /// beyond the 43-byte header; byte-opacity preserved.
    pub fn append_batch(&mut self, raw: &[u8], index_interval_bytes: u64) -> io::Result<()> {
        let (base_offset, last_offset_delta, max_timestamp) = parse_batch_offsets(raw)?;

        let log = self
            .log
            .as_deref_mut()
            .ok_or_else(|| io::Error::other("log handle closed"))?;
        log.write_at(raw, self.log_size)?;

        // Sparse index entry when threshold exceeded.
        if self.log_size - self.last_indexed_log_pos >= index_interval_bytes {
            let index = self
                .index
                .as_deref_mut()
                .ok_or_else(|| io::Error::other("index handle closed"))?;
            let rel_offset_i64 = base_offset - self.meta.base_offset;
            let rel_offset = i32::try_from(rel_offset_i64).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "rel_offset overflowed i32 — segment > 2 GiB of offsets",
                )
            })?;
            let pos_u32 = u32::try_from(self.log_size).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "log_size overflowed u32 — segment > 4 GiB",
                )
            })?;
            let mut entry = [0u8; INDEX_ENTRY_SIZE];
            entry[0..4].copy_from_slice(&rel_offset.to_be_bytes());
            entry[4..8].copy_from_slice(&pos_u32.to_be_bytes());
            io::Write::write_all(index, &entry)?;
            self.last_indexed_log_pos = self.log_size;
        }

        let raw_len = u64::try_from(raw.len()).unwrap_or(u64::MAX);
        self.last_batch_start = Some(self.log_size);
        self.log_size = self.log_size.saturating_add(raw_len);
        self.meta.size = self.log_size;
        self.last_offset = base_offset + i64::from(last_offset_delta);
        if max_timestamp > self.max_timestamp {
            self.max_timestamp = max_timestamp;
        }
        Ok(())
    }

    /// Fast half of segment roll (gh #82). Fsyncs the log so the
    /// trigger batch is durable, then closes both file handles and
    /// returns this segment's [`SegmentMeta`] alongside a fresh
    /// [`ActiveSegment`] starting at `new_base_offset`.
    ///
    /// The OLD segment's index fsync runs in
    /// [`RolledTail::finalize`] — call that off the partition mutex
    /// via `tokio::task::spawn_blocking` so the hot path doesn't pay
    /// the index-sync stall.
    pub fn roll_fast(
        mut self,
        fs: &dyn Fs,
        dir: &Path,
        new_base_offset: i64,
        epoch: i64,
    ) -> io::Result<(ActiveSegment, RolledTail)> {
        if let Some(log) = self.log.as_deref_mut() {
            log.sync_all()?;
        }
        let new_active = ActiveSegment::create(fs, dir, new_base_offset, epoch)?;

        // Move the closed handles into the tail closure for deferred
        // index fsync + close. The handle drops at the end of
        // `finalize` close the FDs.
        let mut closed_log = self.log.take();
        let mut closed_index = self.index.take();
        let closed_meta = SegmentMeta {
            size: self.log_size,
            // The only place a segment's largest timestamp is known
            // for free: we tracked it across every append.
            max_timestamp: self.max_timestamp,
            ..self.meta.clone()
        };
        let tail = RolledTail {
            closed_meta,
            finalize: Box::new(move || {
                if let Some(idx) = closed_index.as_deref_mut() {
                    let _ = idx.sync_all();
                }
                drop(closed_index.take());
                drop(closed_log.take());
                Ok(())
            }),
        };
        Ok((new_active, tail))
    }

    /// Synchronous fsync of the log file. Called from the partition
    /// committer task on its flush cycle.
    pub fn sync_log(&mut self) -> io::Result<()> {
        let started = std::time::Instant::now();
        let log = self
            .log
            .as_deref_mut()
            .ok_or_else(|| io::Error::other("log handle closed"))?;
        let out = log.sync_all();
        kaas_observability::metrics::global()
            .fsync_latency
            .record(started.elapsed().as_secs_f64(), &[]);
        out
    }
}

/// Deferred close-out work produced by [`ActiveSegment::roll_fast`].
/// Runs off the partition mutex via `spawn_blocking` so the hot path
/// doesn't pay the index-sync stall.
pub struct RolledTail {
    /// Closed segment's meta — push this onto the partition's
    /// `closed` list after the finalize closure runs (or before, if
    /// the caller prefers).
    pub closed_meta: SegmentMeta,
    /// Closure that fsyncs the index and drops the old log + index
    /// handles. Returns `io::Result<()>` so caller can log a stall;
    /// failure does NOT impair correctness (the log fsync already
    /// ran inside `roll_fast`).
    pub finalize: Box<dyn FnOnce() -> io::Result<()> + Send>,
}

impl std::fmt::Debug for RolledTail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RolledTail")
            .field("closed_meta", &self.closed_meta)
            .field("finalize", &"<closure>")
            .finish()
    }
}

// ---------------------------------------------------------------------------
// Read a closed segment's bytes (used by Read path for closed segments).
// ---------------------------------------------------------------------------

/// Read raw batch bytes from a closed segment starting at the
/// in-file position `approx_pos` (typically the result of
/// [`search_index`]). Returns up to `max_bytes` of batches that
/// start at offset `>= start_offset`. Mirrors `readBatches` in
/// `segment.go`.
pub fn read_batches(
    fs: &dyn Fs,
    log_path: &Path,
    approx_pos: i64,
    start_offset: i64,
    max_bytes: usize,
) -> io::Result<Vec<u8>> {
    let mut f: Box<dyn FileRead> = match fs.open_read(log_path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    if approx_pos > 0 {
        let pos_u64 = u64::try_from(approx_pos)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "negative seek position"))?;
        f.seek(SeekFrom::Start(pos_u64))?;
    }
    let mut out = Vec::new();
    let mut header = [0u8; 12];
    // Enough of the batch body to reach `lastOffsetDelta` (absolute
    // bytes 23..27), which is what decides whether we want the batch.
    let mut body_head = [0u8; 15];
    while out.len() < max_bytes {
        if f.read_exact(&mut header).is_err() {
            break;
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&header[8..12]);
        let batch_length = i32::from_be_bytes(len_bytes);
        if batch_length <= 0 {
            break;
        }
        let body_len = match usize::try_from(batch_length) {
            Ok(n) => n,
            Err(_) => break,
        };
        // A body too short to carry lastOffsetDelta is a torn tail.
        // (The pre-gh #209 code indexed batch[23..27] unconditionally
        // and would panic here.)
        if body_len < body_head.len() {
            break;
        }
        if f.read_exact(&mut body_head).is_err() {
            break;
        }
        let mut o8 = [0u8; 8];
        o8.copy_from_slice(&header[0..8]);
        let base_offset = i64::from_be_bytes(o8);
        let mut d4 = [0u8; 4];
        d4.copy_from_slice(&body_head[11..15]);
        let last_offset_delta = i32::from_be_bytes(d4);
        let batch_last_offset = base_offset + i64::from(last_offset_delta);
        let rest = body_len - body_head.len();

        if batch_last_offset < start_offset {
            // gh #209: seek past the body instead of reading and
            // heap-allocating it. Skipping used to cost a full read +
            // `vec![0u8; 12 + body_len]` per batch, and because `out`
            // never grew, the `out.len() < max_bytes` condition could
            // not terminate the loop — a fetch near the log head read
            // the entire segment before returning anything.
            let Ok(rest_i64) = i64::try_from(rest) else {
                break;
            };
            if f.seek(SeekFrom::Current(rest_i64)).is_err() {
                break;
            }
            continue;
        }

        let mut batch = vec![0u8; 12 + body_len];
        batch[..12].copy_from_slice(&header);
        batch[12..12 + body_head.len()].copy_from_slice(&body_head);
        if f.read_exact(&mut batch[12 + body_head.len()..]).is_err() {
            break;
        }
        out.extend_from_slice(&batch);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use proptest::prelude::*;

    // --- filename helpers (preserved from the initial slice) --------------

    #[test]
    fn epoch_prefixed_log_path_matches_go_format() {
        let dir = Path::new("/data/topic-a/0");
        let p = segment_log_path(dir, 123, 5);
        assert_eq!(
            p.to_str().unwrap(),
            "/data/topic-a/0/00000005-00000000000000000123.log"
        );
    }

    #[test]
    fn epoch_prefixed_index_path_matches_go_format() {
        let dir = Path::new("/data/topic-a/0");
        let p = segment_index_path(dir, 123, 5);
        assert_eq!(
            p.to_str().unwrap(),
            "/data/topic-a/0/00000005-00000000000000000123.index"
        );
    }

    #[test]
    fn parse_epoch_prefixed_stem() {
        assert_eq!(
            parse_segment_stem("00000005-00000000000000000123"),
            Some((123, 5))
        );
    }

    #[test]
    fn parse_legacy_stem_yields_zero_epoch() {
        assert_eq!(parse_segment_stem("00000000000000000123"), Some((123, 0)));
    }

    #[test]
    fn parse_rejects_garbage() {
        assert_eq!(parse_segment_stem("not-a-number"), None);
        assert_eq!(parse_segment_stem("XYZ-123"), None);
        assert_eq!(parse_segment_stem(""), None);
        assert_eq!(parse_segment_stem("00000001-12-34"), None);
    }

    // --- parse_batch_offsets ---------------------------------------------

    /// Hand-build a v2 batch header carrying given offset fields. The
    /// "records" payload is filler zeros — we never look at it.
    fn build_batch(
        base_offset: i64,
        last_offset_delta: i32,
        max_timestamp: i64,
        records_size: usize,
    ) -> Vec<u8> {
        let body_size = 49 + records_size; // 49 = minimum header tail
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        // magic = 2 at offset 16
        buf[16] = 2;
        // lastOffsetDelta at [23..27]
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        // maxTimestamp at [35..43]
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        buf
    }

    #[test]
    fn parse_batch_offsets_extracts_three_fields() {
        let raw = build_batch(100, 4, 1_700_000_000_000, 0);
        let (base, delta, ts) = parse_batch_offsets(&raw).unwrap();
        assert_eq!(base, 100);
        assert_eq!(delta, 4);
        assert_eq!(ts, 1_700_000_000_000);
    }

    #[test]
    fn parse_batch_offsets_rejects_short() {
        let raw = vec![0u8; 42];
        assert!(parse_batch_offsets(&raw).is_err());
    }

    // --- list_segments ----------------------------------------------------

    #[test]
    fn list_segments_sorts_by_base_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        for (base, epoch) in &[(100, 1), (0, 0), (50, 0), (200, 2)] {
            let _ = ActiveSegment::create(&fs, tmp.path(), *base, *epoch).unwrap();
        }
        let segs = list_segments(&fs, tmp.path()).unwrap();
        let bases: Vec<i64> = segs.iter().map(|s| s.base_offset).collect();
        assert_eq!(bases, vec![0, 50, 100, 200]);
    }

    /// gh #227: an epoch bump over an empty active segment creates the
    /// replacement at the same base offset and removes the old file
    /// best-effort. When that removal loses — a fenced leader still
    /// holding the FD — both files remain, and partition open must not
    /// pick between them by readdir order.
    #[test]
    fn list_segments_keeps_the_highest_epoch_at_a_shared_base_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        for epoch in &[3, 1, 2] {
            let _ = ActiveSegment::create(&fs, tmp.path(), 40, *epoch).unwrap();
        }
        let _ = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let segs = list_segments(&fs, tmp.path()).unwrap();
        assert_eq!(
            segs.iter()
                .map(|s| (s.base_offset, s.epoch))
                .collect::<Vec<_>>(),
            vec![(0, 1), (40, 3)]
        );
    }

    #[test]
    fn list_segments_skips_unparseable_files() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let _ = ActiveSegment::create(&fs, tmp.path(), 0, 0).unwrap();
        // Drop a junk file alongside.
        let _ = fs.create(&tmp.path().join("nonsense.log")).unwrap();
        let _ = fs.create(&tmp.path().join("seg.log.sealed")).unwrap();
        let segs = list_segments(&fs, tmp.path()).unwrap();
        assert_eq!(segs.len(), 1);
    }

    #[test]
    fn list_segments_on_missing_dir_is_empty() {
        let fs = RealFs::new();
        let segs = list_segments(&fs, Path::new("/this/does/not/exist")).unwrap();
        assert!(segs.is_empty());
    }

    // --- ActiveSegment append + reopen + scan ----------------------------

    #[test]
    fn create_then_append_one_batch_updates_state() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let raw = build_batch(0, 4, 1_000, 100);
        seg.append_batch(&raw, 4096).unwrap();

        let raw_len_u64 = u64::try_from(raw.len()).unwrap();
        assert_eq!(seg.log_size(), raw_len_u64);
        assert_eq!(seg.last_offset(), 4);
        assert_eq!(seg.max_timestamp(), 1_000);
        assert_eq!(seg.meta.size, raw_len_u64);
    }

    #[test]
    fn append_then_reopen_scan_recovers_hwm() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        // 3 single-record batches: offsets 0, 1, 2.
        for offset in &[0i64, 1, 2] {
            let raw = build_batch(*offset, 0, 1_000, 64);
            seg.append_batch(&raw, 4096).unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();
        drop(seg);

        // Reopen via stdlib for the scan (the scan reads through a
        // BufReader<File> regardless of the Fs trait).
        let mut f = std::fs::File::open(&log_path).unwrap();
        let hwm = scan_high_watermark(&mut f, 0).unwrap();
        assert_eq!(hwm, 3, "next offset to write after 0,1,2 is 3");
    }

    #[test]
    fn scan_high_watermark_stops_at_truncated_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        for offset in &[0i64, 1, 2] {
            seg.append_batch(&build_batch(*offset, 0, 1_000, 32), 4096)
                .unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();
        drop(seg);

        // Strip 5 bytes off the tail to simulate a torn last batch.
        let mut bytes = std::fs::read(&log_path).unwrap();
        let new_len = bytes.len() - 5;
        bytes.truncate(new_len);
        std::fs::write(&log_path, &bytes).unwrap();

        let mut f = std::fs::File::open(&log_path).unwrap();
        let hwm = scan_high_watermark(&mut f, 0).unwrap();
        assert_eq!(hwm, 2, "torn batch 2 dropped; HWM points just past batch 1");
    }

    // --- gh #228: CRC verification during recovery ----------------------

    /// Write `n` single-record batches and return (log path, byte
    /// position after each batch).
    fn log_with_batches(fs: &RealFs, dir: &Path, n: i64) -> (PathBuf, Vec<u64>) {
        let mut seg = ActiveSegment::create(fs, dir, 0, 1).unwrap();
        let mut ends = Vec::new();
        for offset in 0..n {
            seg.append_batch(&build_batch(offset, 0, 1_000, 32), 4096)
                .unwrap();
            ends.push(seg.log_size());
        }
        seg.sync_log().unwrap();
        let path = seg.meta.log_path.clone();
        seg.close_handles();
        (path, ends)
    }

    #[test]
    fn scan_reports_eof_and_full_length_on_a_clean_log() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, ends) = log_with_batches(&RealFs::new(), tmp.path(), 3);

        let mut f = std::fs::File::open(&path).unwrap();
        let r = scan_high_watermark_from(&mut f, 0, 0).unwrap();
        assert_eq!(r.stop, ScanStop::Eof);
        assert_eq!(r.high_watermark, 3);
        assert_eq!(r.valid_end, *ends.last().unwrap());
    }

    /// The gh #228 case that used to be undetectable: the batch is
    /// structurally perfect — length field intact, header parseable — but
    /// a payload byte has rotted. Pre-fix the scan trusted the header and
    /// served the corrupt record; now it stops at the batch.
    #[test]
    fn scan_stops_at_a_batch_whose_payload_fails_crc() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, ends) = log_with_batches(&RealFs::new(), tmp.path(), 3);

        // Flip a byte inside batch 2's records payload. Batch 1 ends at
        // ends[0]; the payload sits well past that batch's 61-byte header.
        let mut bytes = std::fs::read(&path).unwrap();
        let corrupt_at = usize::try_from(ends[0]).unwrap() + 70;
        bytes[corrupt_at] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let mut f = std::fs::File::open(&path).unwrap();
        let r = scan_high_watermark_from(&mut f, 0, 0).unwrap();
        assert_eq!(r.stop, ScanStop::CrcMismatch, "corruption must be detected");
        assert_eq!(
            r.high_watermark, 1,
            "only batch 0 is trustworthy, so the HWM is 1"
        );
        assert_eq!(
            r.valid_end, ends[0],
            "valid region ends where the last good batch ended"
        );
    }

    /// A corrupt *length* field that stays plausible desynchronises the
    /// walk onto a non-boundary. Without CRC checking the scan would
    /// happily parse garbage as headers and invent an arbitrary HWM.
    #[test]
    fn scan_stops_when_a_plausible_length_field_desyncs_the_walk() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, ends) = log_with_batches(&RealFs::new(), tmp.path(), 3);

        // Shrink batch 1's length by 4 bytes: still >= the 16-byte floor,
        // so the old guard passes and the next "header" is read from the
        // middle of batch 1.
        let mut bytes = std::fs::read(&path).unwrap();
        let at = usize::try_from(ends[0]).unwrap();
        let len = i32::from_be_bytes(bytes[at + 8..at + 12].try_into().unwrap());
        bytes[at + 8..at + 12].copy_from_slice(&(len - 4).to_be_bytes());
        std::fs::write(&path, &bytes).unwrap();

        let mut f = std::fs::File::open(&path).unwrap();
        let r = scan_high_watermark_from(&mut f, 0, 0).unwrap();
        assert_ne!(r.stop, ScanStop::Eof, "must not accept a desynced walk");
        assert_eq!(r.high_watermark, 1);
        assert_eq!(r.valid_end, ends[0]);
    }

    /// A torn tail (crash mid-write) still stops the scan, and now also
    /// reports where the valid bytes end so the caller can truncate.
    #[test]
    fn scan_reports_valid_end_for_a_torn_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let (path, ends) = log_with_batches(&RealFs::new(), tmp.path(), 3);

        let mut bytes = std::fs::read(&path).unwrap();
        let new_len = bytes.len() - 5;
        bytes.truncate(new_len);
        std::fs::write(&path, &bytes).unwrap();

        let mut f = std::fs::File::open(&path).unwrap();
        let r = scan_high_watermark_from(&mut f, 0, 0).unwrap();
        assert_eq!(r.stop, ScanStop::Torn);
        assert_eq!(r.high_watermark, 2);
        assert_eq!(r.valid_end, ends[1], "batch 2 is incomplete, so it's out");
    }

    // --- gh #226: truncate_to -------------------------------------------

    #[test]
    fn truncate_to_shrinks_the_file_and_the_append_position() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let (path, ends) = log_with_batches(&fs, tmp.path(), 3);

        let meta = list_segments(&fs, tmp.path()).unwrap().pop().unwrap();
        let mut seg = ActiveSegment::open_meta_only(meta);
        seg.open_handles(&fs).unwrap();
        seg.truncate_to(&fs, ends[0]).unwrap();

        assert_eq!(seg.log_size(), ends[0], "appends must resume at valid end");
        assert_eq!(std::fs::metadata(&path).unwrap().len(), ends[0]);
    }

    #[test]
    fn truncate_to_is_a_noop_at_or_past_the_current_size() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let (path, ends) = log_with_batches(&fs, tmp.path(), 2);
        let full = *ends.last().unwrap();

        let meta = list_segments(&fs, tmp.path()).unwrap().pop().unwrap();
        let mut seg = ActiveSegment::open_meta_only(meta);
        seg.truncate_to(&fs, full).unwrap();
        seg.truncate_to(&fs, full + 999).unwrap();

        assert_eq!(seg.log_size(), full);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), full);
    }

    /// gh #recovery-checkpoint: resuming the scan from a mid-log byte
    /// position (with the HWM seed for that boundary) yields the same
    /// answer as a full scan — and a checkpoint at EOF reads nothing.
    #[test]
    fn scan_from_checkpoint_matches_full_scan() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let mut pos_after_2 = 0u64;
        for offset in 0..5i64 {
            seg.append_batch(&build_batch(offset, 0, 1_000, 32), 4096)
                .unwrap();
            if offset == 2 {
                pos_after_2 = seg.log_size();
            }
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        let eof = seg.log_size();
        seg.close_handles();
        drop(seg);

        // Baseline: full scan from byte 0.
        let mut f = std::fs::File::open(&log_path).unwrap();
        let full = scan_high_watermark(&mut f, 0).unwrap();
        assert_eq!(full, 5);

        // Resume after batch 2 (HWM there = 3) → same result, tail only.
        let mut f = std::fs::File::open(&log_path).unwrap();
        let resumed = scan_high_watermark_from(&mut f, pos_after_2, 3).unwrap();
        assert_eq!(
            resumed.high_watermark, full,
            "checkpoint resume must equal full scan"
        );
        // A clean log's valid end is EOF no matter where the scan began.
        assert_eq!(resumed.valid_end, eof);
        assert_eq!(resumed.stop, ScanStop::Eof);

        // Checkpoint at EOF (HWM 5) → nothing to read, returns the seed.
        let mut f = std::fs::File::open(&log_path).unwrap();
        let at_eof = scan_high_watermark_from(&mut f, eof, 5).unwrap();
        assert_eq!(at_eof.high_watermark, 5);
        assert_eq!(at_eof.valid_end, eof);
    }

    #[test]
    fn open_handles_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        seg.open_handles(&fs).unwrap();
        seg.open_handles(&fs).unwrap();
        seg.append_batch(&build_batch(0, 0, 1_000, 32), 4096)
            .unwrap();
    }

    #[test]
    fn close_handles_then_open_handles_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        seg.append_batch(&build_batch(0, 0, 1_000, 32), 4096)
            .unwrap();
        seg.sync_log().unwrap();
        let initial_size = seg.log_size();

        seg.close_handles();
        seg.open_handles(&fs).unwrap();
        assert_eq!(seg.log_size(), initial_size);
        // Can resume appending after reopen.
        seg.append_batch(&build_batch(1, 0, 1_000, 32), 4096)
            .unwrap();
    }

    #[test]
    fn append_after_close_handles_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        seg.close_handles();
        let err = seg
            .append_batch(&build_batch(0, 0, 1_000, 32), 4096)
            .unwrap_err();
        assert!(err.to_string().contains("closed"));
    }

    // --- index entries ----------------------------------------------------

    #[test]
    fn appends_emit_index_entries_at_interval() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        // Each batch ~80 bytes; interval = 64 forces an entry per
        // batch after the first.
        for next_offset in 0..5i64 {
            let raw = build_batch(next_offset, 0, 1_000, 32);
            seg.append_batch(&raw, 64).unwrap();
        }
        seg.sync_log().unwrap();
        // Close to flush the index file via Drop.
        let idx_path = seg.meta.index_path.clone();
        seg.close_handles();
        drop(seg);
        let idx_bytes = std::fs::read(&idx_path).unwrap();
        // First batch is at log_pos=0 → no entry written (the test
        // for `log_size - last_indexed_log_pos >= interval` is false
        // before any append). Subsequent batches each trigger one
        // entry. So 4 entries expected.
        assert_eq!(idx_bytes.len() / INDEX_ENTRY_SIZE, 4);
    }

    // --- search_index_bytes ----------------------------------------------

    fn build_index_bytes(entries: &[(i32, u32)]) -> Vec<u8> {
        let mut out = Vec::with_capacity(entries.len() * INDEX_ENTRY_SIZE);
        for (rel, pos) in entries {
            out.extend_from_slice(&rel.to_be_bytes());
            out.extend_from_slice(&pos.to_be_bytes());
        }
        out
    }

    #[test]
    fn search_empty_index_returns_zero() {
        assert_eq!(search_index_bytes(&[], 0, 100), 0);
    }

    #[test]
    fn search_index_bytes_finds_largest_le() {
        // base_offset=100; relative offsets 0, 10, 20 at positions
        // 0, 80, 160.
        let data = build_index_bytes(&[(0, 0), (10, 80), (20, 160)]);
        // target 100 → rel 0 → pos 0
        assert_eq!(search_index_bytes(&data, 100, 100), 0);
        // target 115 → rel 15 → largest entry ≤ 15 is rel 10 → pos 80
        assert_eq!(search_index_bytes(&data, 100, 115), 80);
        // target 120 → rel 20 → pos 160
        assert_eq!(search_index_bytes(&data, 100, 120), 160);
        // target 200 → past last → returns last pos
        assert_eq!(search_index_bytes(&data, 100, 200), 160);
    }

    #[test]
    fn search_index_bytes_returns_zero_for_target_before_base() {
        let data = build_index_bytes(&[(0, 0), (10, 80)]);
        // target 50 < base 100 → rel negative → 0
        assert_eq!(search_index_bytes(&data, 100, 50), 0);
    }

    // --- roll_fast --------------------------------------------------------

    #[test]
    fn roll_fast_returns_fresh_active_and_tail() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        for offset in &[0i64, 1, 2] {
            seg.append_batch(&build_batch(*offset, 0, 1_000, 32), 4096)
                .unwrap();
        }
        let pre_size = seg.log_size();
        let (new_active, tail) = seg.roll_fast(&fs, tmp.path(), 3, 2).unwrap();

        // New active is empty and starts at the new base offset.
        assert_eq!(new_active.meta.base_offset, 3);
        assert_eq!(new_active.meta.epoch, 2);
        assert_eq!(new_active.log_size(), 0);
        assert_eq!(new_active.last_offset(), 2);
        // Closed meta captures pre-roll size.
        assert_eq!(tail.closed_meta.size, pre_size);
        // Tail runs cleanly.
        (tail.finalize)().unwrap();

        // After roll, list_segments sees both (closed + new active
        // file already exists on disk).
        let segs = list_segments(&fs, tmp.path()).unwrap();
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0].base_offset, 0);
        assert_eq!(segs[1].base_offset, 3);
    }

    // --- read_batches -----------------------------------------------------

    #[test]
    fn read_batches_returns_only_at_or_after_start_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let raws: Vec<Vec<u8>> = (0..5).map(|i| build_batch(i, 0, 1_000, 32)).collect();
        for r in &raws {
            seg.append_batch(r, 4096).unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();
        drop(seg);

        // Read from start_offset = 2 → should get batches 2, 3, 4.
        let max_bytes: usize = raws.iter().map(Vec::len).sum::<usize>() + 100;
        let got = read_batches(&fs, &log_path, 0, 2, max_bytes).unwrap();
        let expected: Vec<u8> = raws[2..].iter().flatten().copied().collect();
        assert_eq!(got, expected);
    }

    use std::sync::Arc;

    /// Wraps [`RealFs`], counting bytes actually pulled off disk so a
    /// test can assert read *amplification*, not just output bytes.
    struct CountingFs {
        inner: RealFs,
        bytes: Arc<std::sync::atomic::AtomicU64>,
    }

    struct CountingRead {
        inner: Box<dyn FileRead>,
        bytes: Arc<std::sync::atomic::AtomicU64>,
    }

    impl Read for CountingRead {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let n = self.inner.read(buf)?;
            self.bytes.fetch_add(
                u64::try_from(n).unwrap_or(u64::MAX),
                std::sync::atomic::Ordering::Relaxed,
            );
            Ok(n)
        }
    }
    impl Seek for CountingRead {
        fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
            self.inner.seek(pos)
        }
    }
    impl FileRead for CountingRead {}

    impl Fs for CountingFs {
        fn open_read(&self, p: &Path) -> io::Result<Box<dyn FileRead>> {
            Ok(Box::new(CountingRead {
                inner: self.inner.open_read(p)?,
                bytes: self.bytes.clone(),
            }))
        }
        fn open_write(&self, p: &Path, append: bool) -> io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.open_write(p, append)
        }
        fn create(&self, p: &Path) -> io::Result<Box<dyn crate::fs::FileWrite>> {
            self.inner.create(p)
        }
        fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> io::Result<()> {
            self.inner.fsync(f)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.inner.rename(from, to)
        }
        fn remove(&self, p: &Path) -> io::Result<()> {
            self.inner.remove(p)
        }
        fn fsync_dir(&self, p: &Path) -> io::Result<()> {
            self.inner.fsync_dir(p)
        }
        fn mkdir_all(&self, p: &Path) -> io::Result<()> {
            self.inner.mkdir_all(p)
        }
        fn readdir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
            self.inner.readdir(p)
        }
        fn stat(&self, p: &Path) -> io::Result<std::fs::Metadata> {
            self.inner.stat(p)
        }
    }

    /// gh #231: a new segment's directory entries are made durable
    /// before the segment is handed out for appends.
    #[test]
    fn create_syncs_the_directory_after_both_files() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = crate::fs::JournalFs::default();
        let _seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let ops = fs.ops.lock();
        assert_eq!(ops.len(), 3, "{ops:?}");
        assert!(
            ops[0].starts_with("create ") && ops[0].ends_with(".log"),
            "{ops:?}"
        );
        assert!(
            ops[1].starts_with("create ") && ops[1].ends_with(".index"),
            "{ops:?}"
        );
        assert_eq!(ops[2], "fsync_dir");
    }

    /// gh #209: a fetch positioned near the end of a segment must not
    /// materialise the batches it skips over. Before the fix,
    /// `read_batches` heap-allocated and read the full body of every
    /// skipped batch — and because `out` never grew while skipping,
    /// the `out.len() < max_bytes` loop condition could not terminate
    /// it, so the whole segment was read before anything was returned.
    #[test]
    fn read_batches_seeks_past_skipped_batches_instead_of_reading_them() {
        let tmp = tempfile::tempdir().unwrap();
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let fs = CountingFs {
            inner: RealFs::new(),
            bytes: counter.clone(),
        };
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        // 200 fat batches; the one we want is dead last.
        let raws: Vec<Vec<u8>> = (0..200).map(|i| build_batch(i, 0, 1_000, 4_000)).collect();
        for r in &raws {
            seg.append_batch(r, 1 << 30).unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();
        drop(seg);

        let total: usize = raws.iter().map(Vec::len).sum();
        counter.store(0, std::sync::atomic::Ordering::Relaxed);

        // Read from the very last offset, without an index hint — the
        // worst case, and exactly what kafbat-ui's `mode=LATEST` does.
        let got = read_batches(&fs, &log_path, 0, 199, total + 100).unwrap();
        assert_eq!(got, raws[199], "should return exactly the last batch");

        let read = usize::try_from(counter.load(std::sync::atomic::Ordering::Relaxed))
            .unwrap_or(usize::MAX);
        assert!(
            read < total / 4,
            "read {read} bytes to return {} — skipped bodies are still being read \
             (segment is {total} bytes)",
            got.len()
        );
    }

    /// The skip path must not corrupt the batches it does return.
    #[test]
    fn read_batches_skip_path_preserves_batch_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        // Varying body sizes so a mis-sized seek would desynchronise
        // the reader and surface as garbage.
        let raws: Vec<Vec<u8>> = (0..8)
            .map(|i| build_batch(i, 0, 1_000, 16 + (usize::try_from(i).unwrap() * 37)))
            .collect();
        for r in &raws {
            seg.append_batch(r, 1 << 30).unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();
        drop(seg);

        let total: usize = raws.iter().map(Vec::len).sum::<usize>() + 100;
        for start in 0..8i64 {
            let got = read_batches(&fs, &log_path, 0, start, total).unwrap();
            let idx = usize::try_from(start).unwrap();
            let expected: Vec<u8> = raws[idx..].iter().flatten().copied().collect();
            assert_eq!(got, expected, "mismatch reading from offset {start}");
        }
    }

    /// A torn tail too short to carry `lastOffsetDelta` must stop the
    /// scan, not panic. The pre-fix code indexed `batch[23..27]`
    /// unconditionally.
    #[test]
    fn read_batches_stops_on_body_too_short_for_last_offset_delta() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let log_path = tmp.path().join("00000000-00000000000000000000.log");
        let mut bytes = build_batch(0, 0, 1_000, 32);
        // Append a header claiming a 4-byte body, then only 4 bytes.
        bytes.extend_from_slice(&0i64.to_be_bytes());
        bytes.extend_from_slice(&4i32.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 4]);
        std::fs::write(&log_path, &bytes).unwrap();

        let got = read_batches(&fs, &log_path, 0, 0, 1 << 20).unwrap();
        assert_eq!(got, build_batch(0, 0, 1_000, 32));
    }

    #[test]
    fn read_batches_caps_at_max_bytes() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        for i in 0..5 {
            seg.append_batch(&build_batch(i, 0, 1_000, 32), 4096)
                .unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();

        // Read at most ~one batch worth.
        let got = read_batches(&fs, &log_path, 0, 0, 50).unwrap();
        // Either 0 or exactly one batch — but never more than one
        // started (we stop on the first batch that pushes us past
        // max_bytes).
        assert!(got.len() < 200, "max_bytes cap should bound the result");
    }

    // --- proptest: append → re-scan = correct HWM ------------------------

    proptest! {
        #[test]
        fn pt_append_then_scan_recovers_hwm(
            num_batches in 1usize..32,
            records_per_batch in 1i32..16,
        ) {
            let tmp = tempfile::tempdir().unwrap();
            let fs = RealFs::new();
            let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();

            // Build a sequence with monotonically advancing offsets.
            let mut next_offset = 0i64;
            for _ in 0..num_batches {
                let last_delta = records_per_batch - 1;
                let raw = build_batch(next_offset, last_delta, 1_000, 32);
                seg.append_batch(&raw, 4096).unwrap();
                next_offset += i64::from(records_per_batch);
            }
            seg.sync_log().unwrap();
            let log_path = seg.meta.log_path.clone();
            seg.close_handles();
            drop(seg);

            let mut f = std::fs::File::open(&log_path).unwrap();
            let hwm = scan_high_watermark(&mut f, 0).unwrap();
            prop_assert_eq!(hwm, next_offset);
        }
    }

    // --- byte-equality on re-open via raw read --------------------------

    #[test]
    fn append_then_read_back_is_byte_identical() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let mut seg = ActiveSegment::create(&fs, tmp.path(), 0, 1).unwrap();
        let raws: Vec<Vec<u8>> = (0..10).map(|i| build_batch(i, 0, 1_000, 64)).collect();
        for r in &raws {
            seg.append_batch(r, 4096).unwrap();
        }
        seg.sync_log().unwrap();
        let log_path = seg.meta.log_path.clone();
        seg.close_handles();

        let mut bytes = Vec::new();
        std::fs::File::open(&log_path)
            .unwrap()
            .read_to_end(&mut bytes)
            .unwrap();
        let expected: Vec<u8> = raws.iter().flatten().copied().collect();
        assert_eq!(
            bytes, expected,
            "log file bytes must equal what was written"
        );
    }
}
