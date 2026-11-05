//! Idempotent-producer state.
//!
//! The classifier
//! is a pure function: caller (Append) holds the partition mutex, the
//! classifier inspects the per-PID window, returns an [`Outcome`],
//! and the caller records the accepted batch via
//! [`record_accepted`] after the log write succeeds.
//!
//! # Window size
//!
//! Five batches per PID, matching Java's
//! `max.in.flight.requests.per.connection=5` default. Java's idempotent
//! producer caps in-flight at 5 to keep this window small.

use std::collections::HashMap;

use parking_lot::Mutex;

/// Five batches per PID. Matches Apache Kafka 3.7.
pub const RING_SIZE: usize = 5;

/// One cache slot in the per-producer window.
///
/// `first_seq..=last_seq` describes the contiguous range of record-level
/// sequence numbers in the batch. `base_offset` is what was returned
/// to the producer at accept time — replays echo the same value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecentBatch {
    pub first_seq: i32,
    pub last_seq: i32,
    pub base_offset: i64,
}

/// Per-(producerID) idempotence state.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProducerEntry {
    pub epoch: i16,
    /// Ordered oldest-first; `len <= RING_SIZE`.
    pub recent: Vec<RecentBatch>,
}

/// Fields extracted from a v2 RecordBatch header by
/// [`parse_batch_producer_info`].
///
/// `last_seq` is `first_seq + last_offset_delta` — Apache treats the
/// per-record sequence delta as identical to the per-record offset
/// delta because every v2 record is sequence-numbered in lock-step
/// with its offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchProducerInfo {
    pub producer_id: i64,
    pub epoch: i16,
    pub first_seq: i32,
    pub last_seq: i32,
}

/// Outcome of the idempotence classifier. v0.1 conflated "no
/// PID" and "transactional control batch" into `idemNotIdempotent`;
/// we keep them as one variant ([`Outcome::NotIdempotent`]) for the
/// same reason — the caller's handling is identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// `producer_id < 0` (legacy non-idempotent producer) **or**
    /// `first_seq < 0` (transactional COMMIT/ABORT marker, gh #114).
    /// Append proceeds without idempotence checks.
    NotIdempotent,
    /// Fresh, in-sequence batch. Caller appends and then calls
    /// [`record_accepted`] with the assigned `base_offset`.
    Accept,
    /// Exact in-window retry. Caller skips the append and echoes the
    /// cached `base_offset` with `error_code = 0`.
    Duplicate { base_offset: i64 },
    /// Gap detected (first batch with non-zero seq, or any batch
    /// whose `first_seq != prev.last_seq + 1`). Wire error 45.
    OutOfOrder,
    /// Batch epoch is older than the recorded epoch. Wire error 47.
    InvalidEpoch,
}

/// Parse the v2 RecordBatch header for the idempotence-relevant
/// fields. Reads only the first 57 bytes — does not touch records
/// payload. The byte-opacity invariant is preserved at the call site.
///
/// Layout (Apache Kafka v2 RecordBatch):
///
/// ```text
///   0..8   baseOffset
///   8..12  batchLength
///  12..16  partitionLeaderEpoch
///  16      magic
///  17..21  crc
///  21..23  attrs
///  23..27  lastOffsetDelta   <- used for last_seq
///  27..35  baseTimestamp
///  35..43  maxTimestamp
///  43..51  producerID
///  51..53  producerEpoch
///  53..57  baseSequence
///  57..61  numRecords
/// ```
pub fn parse_batch_producer_info(raw: &[u8]) -> Result<BatchProducerInfo, IdempotenceParseError> {
    const HEADER_END: usize = 57;
    if raw.len() < HEADER_END {
        return Err(IdempotenceParseError::BatchTooShort { got: raw.len() });
    }

    fn read_i16(b: &[u8]) -> i16 {
        let mut tmp = [0u8; 2];
        tmp.copy_from_slice(&b[..2]);
        i16::from_be_bytes(tmp)
    }
    fn read_i32(b: &[u8]) -> i32 {
        let mut tmp = [0u8; 4];
        tmp.copy_from_slice(&b[..4]);
        i32::from_be_bytes(tmp)
    }
    fn read_i64(b: &[u8]) -> i64 {
        let mut tmp = [0u8; 8];
        tmp.copy_from_slice(&b[..8]);
        i64::from_be_bytes(tmp)
    }

    let last_offset_delta = read_i32(&raw[23..27]);
    let producer_id = read_i64(&raw[43..51]);
    let epoch = read_i16(&raw[51..53]);
    let base_seq = read_i32(&raw[53..57]);

    Ok(BatchProducerInfo {
        producer_id,
        epoch,
        first_seq: base_seq,
        // `last_seq = first_seq + last_offset_delta`, wrapping past
        // i32::MAX to 0 as producers do. For PID == -1 the values are
        // arbitrary and ignored downstream.
        last_seq: increment_sequence(base_seq, last_offset_delta),
    })
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum IdempotenceParseError {
    #[error("batch too short for producer info: {got} bytes")]
    BatchTooShort { got: usize },
}

/// Fields extracted from a v2 RecordBatch header that drive the
/// per-partition transactional bookkeeping (gh #176). Read at
/// append time so the [`crate::txn_index::OpenTxnIndex`] and
/// [`crate::txn_index::AbortedTxnIndex`] can be updated under the
/// partition mutex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchTxnInfo {
    /// `attributes & 0x10 != 0` — the producer marked this batch as
    /// part of a transaction.
    pub is_transactional: bool,
    /// `attributes & 0x20 != 0` — this is a COMMIT/ABORT marker
    /// (control batch) rather than data.
    pub is_control: bool,
    pub producer_id: i64,
    /// `Some` iff `is_control`. `Some(true)` = COMMIT, `Some(false)`
    /// = ABORT. Decoded from the single control record's key (Apache
    /// `ControlRecordType.{ABORT=0, COMMIT=1}`).
    pub control_commit: Option<bool>,
}

/// Peek the v2 batch header for the txn-bookkeeping fields. Reads
/// only the bytes needed — header through producer_id, plus
/// (for control batches) one byte of the inline record's key. Does
/// not decode records payload — the byte-opacity invariant holds
/// for data batches; control batches are a single 1-record batch
/// whose key shape is part of Apache's wire format.
///
/// Returns `None` when the batch is too short to parse the header
/// (caller treats as "not a recognized v2 batch — skip the
/// bookkeeping").
pub fn parse_batch_txn_info(raw: &[u8]) -> Option<BatchTxnInfo> {
    const HEADER_END: usize = 57;
    if raw.len() < HEADER_END {
        return None;
    }
    let mut attr_bytes = [0u8; 2];
    attr_bytes.copy_from_slice(&raw[21..23]);
    let attrs = i16::from_be_bytes(attr_bytes);
    let is_transactional = (attrs & 0x10) != 0;
    let is_control = (attrs & 0x20) != 0;

    let mut pid_bytes = [0u8; 8];
    pid_bytes.copy_from_slice(&raw[43..51]);
    let producer_id = i64::from_be_bytes(pid_bytes);

    let control_commit = if is_control {
        // Control batch layout (per `crates/kaas-broker/src/control_batch.rs`):
        //   0..61   batch header (recordCount=1)
        //   61      varint bodyLen (1 byte for the standard marker)
        //   62      record attributes (i8 = 0)
        //   63      varlong timestampDelta = 0 (1 byte)
        //   64      varint offsetDelta = 0 (1 byte)
        //   65      varint keyLen = 4 (1 byte = 0x08)
        //   66..68  key version (i16 = 0)
        //   68..70  key type (i16 — 0=ABORT, 1=COMMIT)
        // Total minimum batch size = 70.
        const TYPE_HI: usize = 68;
        const TYPE_LO: usize = 69;
        if raw.len() <= TYPE_LO {
            None
        } else {
            let mut type_bytes = [0u8; 2];
            type_bytes.copy_from_slice(&raw[TYPE_HI..=TYPE_LO]);
            let control_type = i16::from_be_bytes(type_bytes);
            Some(control_type == 1)
        }
    } else {
        None
    };

    Some(BatchTxnInfo {
        is_transactional,
        is_control,
        producer_id,
        control_commit,
    })
}

// ---------------------------------------------------------------------------
// Pure classifier
// ---------------------------------------------------------------------------

/// Apache's `DefaultRecordBatch.incrementSequence`: sequences wrap from
/// i32::MAX to 0, not to i32::MIN (gh #306).
fn increment_sequence(sequence: i32, increment: i32) -> i32 {
    let next = i64::from(sequence) + i64::from(increment);
    let wrapped = if next > i64::from(i32::MAX) {
        next - i64::from(i32::MAX) - 1
    } else {
        next
    };
    i32::try_from(wrapped).unwrap_or(i32::MAX)
}

/// Apache's `ProducerAppendInfo.inSequence`.
fn in_sequence(last_seq: i32, next_seq: i32) -> bool {
    i64::from(next_seq) == i64::from(last_seq) + 1 || (next_seq == 0 && last_seq == i32::MAX)
}

/// Decide what to do with `info` given the partition's current
/// per-PID map. Pure function: no I/O, no mutation. Caller holds the
/// partition mutex throughout the `classify → record_accepted` pair.
pub fn classify(states: &HashMap<i64, ProducerEntry>, info: BatchProducerInfo) -> Outcome {
    if info.producer_id < 0 {
        return Outcome::NotIdempotent;
    }
    // gh #114: control batches (transactional COMMIT/ABORT markers)
    // carry a real PID but `base_sequence == -1`. They are exempt
    // from the dedupe window — Apache's storage layer treats them
    // as idempotence-transparent.
    if info.first_seq < 0 {
        return Outcome::NotIdempotent;
    }

    let entry = match states.get(&info.producer_id) {
        Some(e) => e,
        // No state for this PID: any sequence is accepted, as in Apache's
        // `ProducerAppendInfo.checkSequence`. A live producer legitimately
        // arrives here with a non-zero sequence once its entry has expired
        // or its records have been deleted; refusing it wedged the producer
        // on 45 (gh #306).
        None => return Outcome::Accept,
    };

    if info.epoch > entry.epoch {
        // Fresh-epoch reset (KIP-360 PID renewal). Same first-seq=0 rule.
        return if info.first_seq == 0 {
            Outcome::Accept
        } else {
            Outcome::OutOfOrder
        };
    }
    if info.epoch < entry.epoch {
        return Outcome::InvalidEpoch;
    }

    // Same epoch — dedupe against the window first.
    for rb in &entry.recent {
        if rb.first_seq == info.first_seq && rb.last_seq == info.last_seq {
            return Outcome::Duplicate {
                base_offset: rb.base_offset,
            };
        }
    }

    if entry.recent.is_empty() {
        // A cross-partition fence raises the epoch and clears the window,
        // so this is the first batch of the session after a bump. Apache
        // requires sequence 0 on an epoch change, and here the epoch change
        // has already been applied.
        return if info.first_seq == 0 {
            Outcome::Accept
        } else {
            Outcome::OutOfOrder
        };
    }

    let last = entry.recent[entry.recent.len() - 1];
    if in_sequence(last.last_seq, info.first_seq) {
        Outcome::Accept
    } else {
        Outcome::OutOfOrder
    }
}

/// Advance the per-PID window after `Append` has succeeded. Only
/// called for [`Outcome::Accept`] — dedupe and rejection paths
/// already have correct state.
pub fn record_accepted(
    states: &mut HashMap<i64, ProducerEntry>,
    info: BatchProducerInfo,
    base_offset: i64,
) {
    let entry = states.entry(info.producer_id).or_default();
    if info.epoch > entry.epoch {
        // Epoch bump — drop the old window. KIP-360 PID renewal.
        entry.epoch = info.epoch;
        entry.recent.clear();
    }
    entry.recent.push(RecentBatch {
        first_seq: info.first_seq,
        last_seq: info.last_seq,
        base_offset,
    });
    if entry.recent.len() > RING_SIZE {
        let overflow = entry.recent.len() - RING_SIZE;
        entry.recent.drain(..overflow);
    }
}

// ---------------------------------------------------------------------------
// Concurrent wrapper used by Partition.
// ---------------------------------------------------------------------------

/// Thread-safe wrapper around `HashMap<i64, ProducerEntry>`.
///
/// Phase 2 initial slice uses a single `parking_lot::Mutex` — the
/// classify/record_accepted pair runs under the partition mutex already,
/// so the inner mutex is effectively uncontended. The Phase 2 plan
/// notes `DashMap` as a potential follow-up; if benchmarks show
/// per-shard write-lock starvation on the multi-PID hot path, swap
/// the impl behind this seam.
#[derive(Debug, Default)]
pub struct ProducerStates {
    inner: Mutex<HashMap<i64, ProducerEntry>>,
}

impl ProducerStates {
    pub fn new() -> Self {
        Self::default()
    }

    /// Run the classifier against the current state.
    pub fn classify(&self, info: BatchProducerInfo) -> Outcome {
        classify(&self.inner.lock(), info)
    }

    /// Advance the per-PID window for an accepted batch.
    pub fn record_accepted(&self, info: BatchProducerInfo, base_offset: i64) {
        record_accepted(&mut self.inner.lock(), info, base_offset);
    }

    /// Phase 6: cross-broker fence broadcast (gh #108). Bumps the
    /// recorded epoch for `pid` and clears the dedupe window so a
    /// zombie batch from the old session is fenced.
    pub fn fence(&self, pid: i64, new_epoch: i16) {
        let mut guard = self.inner.lock();
        let entry = guard.entry(pid).or_default();
        if new_epoch > entry.epoch {
            entry.epoch = new_epoch;
            entry.recent.clear();
        }
    }

    /// Snapshot for the producer-state file on disk.
    pub fn snapshot(&self) -> Vec<(i64, ProducerEntry)> {
        let guard = self.inner.lock();
        guard.iter().map(|(k, v)| (*k, v.clone())).collect()
    }

    /// Restore from a producer-state snapshot. Existing state is
    /// replaced wholesale.
    pub fn restore(&self, entries: Vec<(i64, ProducerEntry)>) {
        let mut guard = self.inner.lock();
        guard.clear();
        for (pid, entry) in entries {
            guard.insert(pid, entry);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pi(pid: i64, epoch: i16, first: i32, last: i32) -> BatchProducerInfo {
        BatchProducerInfo {
            producer_id: pid,
            epoch,
            first_seq: first,
            last_seq: last,
        }
    }

    // ---- parse_batch_producer_info ---------------------------------------

    #[test]
    fn parse_rejects_short_input() {
        let bad = vec![0u8; 56];
        assert_eq!(
            parse_batch_producer_info(&bad),
            Err(IdempotenceParseError::BatchTooShort { got: 56 })
        );
    }

    #[test]
    fn parse_extracts_pid_epoch_first_last_from_v2_header() {
        let mut hdr = vec![0u8; 61];
        // lastOffsetDelta = 4 at [23..27]
        hdr[23..27].copy_from_slice(&4i32.to_be_bytes());
        // producerID = 100 at [43..51]
        hdr[43..51].copy_from_slice(&100i64.to_be_bytes());
        // producerEpoch = 7 at [51..53]
        hdr[51..53].copy_from_slice(&7i16.to_be_bytes());
        // baseSequence = 10 at [53..57]
        hdr[53..57].copy_from_slice(&10i32.to_be_bytes());

        let got = parse_batch_producer_info(&hdr).unwrap();
        assert_eq!(got.producer_id, 100);
        assert_eq!(got.epoch, 7);
        assert_eq!(got.first_seq, 10);
        assert_eq!(got.last_seq, 14, "first_seq + lastOffsetDelta");
    }

    // ---- parse_batch_txn_info --------------------------------------------

    fn make_data_batch(attrs: i16, pid: i64) -> Vec<u8> {
        let mut hdr = vec![0u8; 61];
        hdr[21..23].copy_from_slice(&attrs.to_be_bytes());
        hdr[43..51].copy_from_slice(&pid.to_be_bytes());
        hdr
    }

    fn make_control_batch(attrs: i16, pid: i64, control_type: i16) -> Vec<u8> {
        // 61-byte header + 9-byte payload through key.type.
        let mut buf = vec![0u8; 70];
        buf[21..23].copy_from_slice(&attrs.to_be_bytes());
        buf[43..51].copy_from_slice(&pid.to_be_bytes());
        buf[68..70].copy_from_slice(&control_type.to_be_bytes());
        buf
    }

    #[test]
    fn txn_info_too_short_returns_none() {
        assert!(parse_batch_txn_info(&[0u8; 56]).is_none());
    }

    #[test]
    fn txn_info_non_transactional_data_batch() {
        let raw = make_data_batch(0, 42);
        let info = parse_batch_txn_info(&raw).unwrap();
        assert!(!info.is_transactional);
        assert!(!info.is_control);
        assert_eq!(info.producer_id, 42);
        assert!(info.control_commit.is_none());
    }

    #[test]
    fn txn_info_transactional_data_batch() {
        let raw = make_data_batch(0x10, 42);
        let info = parse_batch_txn_info(&raw).unwrap();
        assert!(info.is_transactional);
        assert!(!info.is_control);
        assert_eq!(info.producer_id, 42);
        assert!(info.control_commit.is_none());
    }

    #[test]
    fn txn_info_commit_marker() {
        let raw = make_control_batch(0x30, 42, 1); // transactional | control, COMMIT
        let info = parse_batch_txn_info(&raw).unwrap();
        assert!(info.is_transactional);
        assert!(info.is_control);
        assert_eq!(info.producer_id, 42);
        assert_eq!(info.control_commit, Some(true));
    }

    #[test]
    fn txn_info_abort_marker() {
        let raw = make_control_batch(0x30, 42, 0); // transactional | control, ABORT
        let info = parse_batch_txn_info(&raw).unwrap();
        assert_eq!(info.control_commit, Some(false));
    }

    #[test]
    fn txn_info_control_batch_too_short_for_type_returns_none_commit() {
        // is_control set but batch is truncated before the type byte.
        let raw = make_data_batch(0x30, 42); // 61 bytes — type byte missing
        let info = parse_batch_txn_info(&raw).unwrap();
        assert!(info.is_control);
        assert!(
            info.control_commit.is_none(),
            "truncated control batch must not guess the type"
        );
    }

    // ---- classifier ------------------------------------------------------

    #[test]
    fn negative_producer_id_is_not_idempotent() {
        let m = HashMap::new();
        assert_eq!(classify(&m, pi(-1, 0, 0, 0)), Outcome::NotIdempotent);
    }

    #[test]
    fn control_batch_first_seq_negative_is_not_idempotent() {
        let m = HashMap::new();
        // gh #114: COMMIT/ABORT marker, PID > 0, first_seq = -1.
        assert_eq!(classify(&m, pi(42, 0, -1, -1)), Outcome::NotIdempotent);
    }

    #[test]
    fn a_pid_with_no_state_accepts_any_sequence() {
        let m = HashMap::new();
        assert_eq!(classify(&m, pi(1, 0, 0, 0)), Outcome::Accept);
        // gh #306: Apache's rule. The PID's state may have expired or been
        // deleted while the producer lives on.
        assert_eq!(classify(&m, pi(1, 0, 5, 5)), Outcome::Accept);
    }

    #[test]
    fn duplicate_in_window_returns_cached_offset() {
        let mut m = HashMap::new();
        record_accepted(&mut m, pi(1, 0, 0, 2), 100);
        record_accepted(&mut m, pi(1, 0, 3, 5), 103);
        assert_eq!(
            classify(&m, pi(1, 0, 3, 5)),
            Outcome::Duplicate { base_offset: 103 }
        );
    }

    #[test]
    fn next_in_sequence_after_window_accepts() {
        let mut m = HashMap::new();
        record_accepted(&mut m, pi(1, 0, 0, 2), 100);
        // last_seq of last entry = 2; next must be first_seq = 3.
        assert_eq!(classify(&m, pi(1, 0, 3, 5)), Outcome::Accept);
        assert_eq!(classify(&m, pi(1, 0, 7, 9)), Outcome::OutOfOrder);
    }

    #[test]
    fn older_epoch_is_invalid() {
        let mut m = HashMap::new();
        record_accepted(&mut m, pi(1, 5, 0, 2), 100);
        assert_eq!(classify(&m, pi(1, 3, 0, 0)), Outcome::InvalidEpoch);
    }

    #[test]
    fn newer_epoch_resets_window() {
        let mut m = HashMap::new();
        record_accepted(&mut m, pi(1, 5, 0, 2), 100);
        // Newer epoch must restart at first_seq = 0.
        assert_eq!(classify(&m, pi(1, 6, 0, 0)), Outcome::Accept);
        assert_eq!(classify(&m, pi(1, 6, 7, 9)), Outcome::OutOfOrder);
    }

    #[test]
    fn ring_caps_at_five_entries() {
        let mut m = HashMap::new();
        for i in 0..10 {
            // each batch is a single-record batch: first_seq == last_seq
            let first = i32::try_from(i).unwrap();
            record_accepted(&mut m, pi(1, 0, first, first), i);
        }
        let entry = m.get(&1).unwrap();
        assert_eq!(entry.recent.len(), RING_SIZE);
        // Oldest retained should be batch index 5 (we kept the LAST five).
        assert_eq!(entry.recent[0].first_seq, 5);
        assert_eq!(entry.recent[RING_SIZE - 1].first_seq, 9);
    }

    // ---- ProducerStates wrapper ------------------------------------------

    #[test]
    fn states_wrapper_roundtrips_classify_record() {
        let states = ProducerStates::new();
        assert_eq!(states.classify(pi(1, 0, 0, 2)), Outcome::Accept);
        states.record_accepted(pi(1, 0, 0, 2), 100);
        // Replay → duplicate.
        assert_eq!(
            states.classify(pi(1, 0, 0, 2)),
            Outcome::Duplicate { base_offset: 100 }
        );
        // Next-in-sequence → accept.
        assert_eq!(states.classify(pi(1, 0, 3, 5)), Outcome::Accept);
    }

    #[test]
    fn fence_bumps_epoch_and_clears_window() {
        let states = ProducerStates::new();
        states.record_accepted(pi(1, 0, 0, 2), 100);
        states.fence(1, 5);
        // After fence, the new epoch must start at first_seq=0.
        assert_eq!(states.classify(pi(1, 5, 0, 0)), Outcome::Accept);
        // Old epoch is now stale.
        assert_eq!(states.classify(pi(1, 0, 3, 5)), Outcome::InvalidEpoch);
    }

    #[test]
    fn snapshot_restore_roundtrips() {
        let states = ProducerStates::new();
        states.record_accepted(pi(1, 0, 0, 2), 100);
        states.record_accepted(pi(2, 3, 0, 4), 200);
        let snap = states.snapshot();

        let restored = ProducerStates::new();
        restored.restore(snap);
        assert_eq!(
            restored.classify(pi(1, 0, 0, 2)),
            Outcome::Duplicate { base_offset: 100 }
        );
        assert_eq!(
            restored.classify(pi(2, 3, 0, 4)),
            Outcome::Duplicate { base_offset: 200 }
        );
    }

    /// gh #306: sequences wrap from i32::MAX to 0 (Apache's
    /// `incrementSequence` and `inSequence`). `wrapping_add` went to
    /// i32::MIN, so a producer that wrapped was refused with 45 for good.
    #[test]
    fn sequences_wrap_to_zero() {
        assert_eq!(increment_sequence(i32::MAX, 1), 0);
        assert_eq!(increment_sequence(i32::MAX - 1, 4), 2);
        assert_eq!(increment_sequence(5, 2), 7);

        // Last sequence i32::MAX, next 0.
        let mut m = HashMap::new();
        record_accepted(&mut m, pi(1, 0, i32::MAX, i32::MAX), 0);
        assert_eq!(classify(&m, pi(1, 0, 0, 0)), Outcome::Accept);
        assert_eq!(classify(&m, pi(1, 0, 1, 1)), Outcome::OutOfOrder);

        // A batch straddling the wrap: base i32::MAX-1, 5 records, last 2.
        let mut m = HashMap::new();
        record_accepted(
            &mut m,
            pi(1, 0, i32::MAX - 1, increment_sequence(i32::MAX - 1, 4)),
            0,
        );
        assert_eq!(classify(&m, pi(1, 0, 3, 3)), Outcome::Accept);
    }
}
