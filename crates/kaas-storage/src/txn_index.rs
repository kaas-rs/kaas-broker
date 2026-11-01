//! Per-partition transactional bookkeeping for `read_committed`
//! Fetch (gh #176).
//!
//! Two indexes maintained at append time:
//!
//! - [`OpenTxnIndex`] tracks the **first** transactional batch
//!   offset per `(pid, current_open_txn)` on this partition.
//!   `min(values)` is the partition's **Last Stable Offset** (LSO) —
//!   `read_committed` Fetch must not return records past it.
//! - [`AbortedTxnIndex`] records every txn that ended in ABORT so
//!   the Fetch response can include `AbortedTransactions[]` for the
//!   client to filter out the aborted records.
//!
//! Both indexes are populated at append time and persisted to
//! `txn-index.snapshot` (gh #177, [`crate::txn_index_snapshot`]) on
//! the same triggers as the producer snapshot — take-over and
//! close/relinquish — then restored on `Partition::open`. Apache's
//! per-segment `.txnindex` files are the same idea at finer
//! granularity; one file per partition keeps NFS round-trips down.

use std::collections::{HashMap, VecDeque};

/// One aborted-transaction record. Apache's wire shape is
/// `(producer_id, first_offset)`; we keep `last_offset` as an
/// internal eviction key so the entry can be dropped when the
/// partition's `log_start_offset` advances past it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub producer_id: i64,
    pub first_offset: i64,
    /// The offset of the ABORT marker batch — used internally for
    /// eviction by `log_start` advance. Not emitted on the wire.
    pub last_offset: i64,
}

/// Per-partition open-transaction index. Updated at append time by
/// the storage engine; queried at fetch time by the Fetch handler
/// via `Partition::last_stable_offset`.
#[derive(Debug, Default)]
pub struct OpenTxnIndex {
    /// `pid → base_offset of the first transactional data batch on
    /// this partition since the txn opened`. Removed when the
    /// COMMIT or ABORT marker for the pid lands.
    first_offset: HashMap<i64, i64>,
}

impl OpenTxnIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the assigned base offset for `pid`'s first
    /// transactional batch since the txn opened. Subsequent batches
    /// from the same `pid` within the same txn are no-ops (the
    /// first-offset is the one LSO must respect).
    pub fn record_data_batch(&mut self, pid: i64, base_offset: i64) {
        self.first_offset.entry(pid).or_insert(base_offset);
    }

    /// Remove `pid`'s open-txn entry. Called when the COMMIT or
    /// ABORT marker for `pid` is appended. Returns the
    /// previously-recorded first offset, or `None` if `pid` had no
    /// open txn (idempotent retry, marker for a never-started txn).
    pub fn close(&mut self, pid: i64) -> Option<i64> {
        self.first_offset.remove(&pid)
    }

    /// Lowest offset across all open transactions. `None` when no
    /// txn is open — caller uses HWM as the LSO in that case.
    pub fn min_open_offset(&self) -> Option<i64> {
        self.first_offset.values().copied().min()
    }

    /// All `(pid, first_offset)` entries, for the gh #177 snapshot.
    pub fn entries(&self) -> Vec<(i64, i64)> {
        self.first_offset.iter().map(|(k, v)| (*k, *v)).collect()
    }

    /// Rebuild from a gh #177 snapshot's entries.
    pub fn restore(entries: impl IntoIterator<Item = (i64, i64)>) -> Self {
        Self {
            first_offset: entries.into_iter().collect(),
        }
    }

    /// True when no transaction is currently open on this
    /// partition. Equivalent to `min_open_offset().is_none()`.
    pub fn is_empty(&self) -> bool {
        self.first_offset.is_empty()
    }
}

/// Per-partition aborted-transaction index. Entries are emitted in
/// the Fetch response's `AbortedTransactions[]` list so clients
/// filter out the aborted records on the consumer side.
#[derive(Debug, Default)]
pub struct AbortedTxnIndex {
    /// Sorted by `first_offset` for efficient range queries on the
    /// fetch path. `VecDeque` because the common eviction shape is
    /// "drop from the front when `log_start` advances."
    entries: VecDeque<AbortedTxn>,
}

impl AbortedTxnIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a new aborted-txn entry. Maintained sorted by
    /// `first_offset` since fetches scan a contiguous range. In
    /// practice abort markers land monotonically (the marker offset
    /// is always > the txn's first offset), but we don't rely on
    /// that — partition_point handles any insertion order.
    pub fn record(&mut self, entry: AbortedTxn) {
        let pos = self
            .entries
            .partition_point(|e| e.first_offset < entry.first_offset);
        self.entries.insert(pos, entry);
    }

    /// Drop entries whose entire span is below `log_start`. Called
    /// by `DeleteRecords` and segment-retention cleanup.
    pub fn evict_below(&mut self, log_start: i64) {
        while let Some(front) = self.entries.front() {
            if front.last_offset < log_start {
                self.entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Aborted txns that overlap the fetched range `[start, end)`: the
    /// txn ends (`last_offset`, its ABORT marker) at or after `start` and
    /// begins before `end`. Populates the Fetch response's
    /// `AbortedTransactions[]`. This is Apache's
    /// `TransactionIndex.collectAbortedTxns`. Filtering on `first_offset`
    /// alone (until gh #277) dropped a txn the fetch starts inside — which
    /// every fetch cut short by `max.partition.fetch.bytes` can do — and a
    /// read_committed consumer then delivered its aborted records.
    pub fn in_range(&self, start: i64, end: i64) -> Vec<AbortedTxn> {
        self.entries
            .iter()
            .filter(|e| e.last_offset >= start && e.first_offset < end)
            .copied()
            .collect()
    }

    /// The full entry list, sorted by `first_offset` — the gh #177
    /// snapshot source (and test observability).
    pub fn snapshot(&self) -> Vec<AbortedTxn> {
        self.entries.iter().copied().collect()
    }

    /// Rebuild from a gh #177 snapshot's entries. `record` keeps the
    /// deque sorted regardless of input order.
    pub fn restore(entries: impl IntoIterator<Item = AbortedTxn>) -> Self {
        let mut idx = Self::default();
        for e in entries {
            idx.record(e);
        }
        idx
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn aborted(pid: i64, first: i64, last: i64) -> AbortedTxn {
        AbortedTxn {
            producer_id: pid,
            first_offset: first,
            last_offset: last,
        }
    }

    // --- OpenTxnIndex -----------------------------------------------------

    #[test]
    fn empty_open_index_has_no_min_offset() {
        let idx = OpenTxnIndex::new();
        assert!(idx.min_open_offset().is_none());
        assert!(idx.is_empty());
    }

    #[test]
    fn record_data_batch_remembers_first_offset_only() {
        let mut idx = OpenTxnIndex::new();
        idx.record_data_batch(42, 100);
        idx.record_data_batch(42, 200); // later batch — must be ignored
        idx.record_data_batch(42, 50); // even an earlier-arriving offset shouldn't shift
        assert_eq!(idx.min_open_offset(), Some(100));
    }

    #[test]
    fn min_open_offset_picks_lowest_across_pids() {
        let mut idx = OpenTxnIndex::new();
        idx.record_data_batch(1, 500);
        idx.record_data_batch(2, 200);
        idx.record_data_batch(3, 800);
        assert_eq!(idx.min_open_offset(), Some(200));
    }

    #[test]
    fn close_removes_entry_and_returns_first_offset() {
        let mut idx = OpenTxnIndex::new();
        idx.record_data_batch(42, 100);
        assert_eq!(idx.close(42), Some(100));
        assert_eq!(idx.close(42), None);
        assert!(idx.is_empty());
    }

    #[test]
    fn close_advances_min_offset() {
        let mut idx = OpenTxnIndex::new();
        idx.record_data_batch(1, 100);
        idx.record_data_batch(2, 200);
        assert_eq!(idx.min_open_offset(), Some(100));
        idx.close(1);
        assert_eq!(idx.min_open_offset(), Some(200));
        idx.close(2);
        assert_eq!(idx.min_open_offset(), None);
    }

    // --- AbortedTxnIndex --------------------------------------------------

    #[test]
    fn empty_aborted_index_range_is_empty() {
        let idx = AbortedTxnIndex::new();
        assert!(idx.in_range(0, i64::MAX).is_empty());
    }

    #[test]
    fn record_maintains_sorted_order_regardless_of_insertion() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(1, 500, 501));
        idx.record(aborted(2, 100, 101));
        idx.record(aborted(3, 300, 301));
        let snap = idx.snapshot();
        let offsets: Vec<i64> = snap.iter().map(|e| e.first_offset).collect();
        assert_eq!(offsets, vec![100, 300, 500]);
    }

    #[test]
    fn in_range_filters_by_first_offset() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(1, 100, 101));
        idx.record(aborted(2, 200, 201));
        idx.record(aborted(3, 300, 301));
        let got = idx.in_range(150, 250);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].producer_id, 2);
    }

    /// gh #277: a fetch that resumes inside an aborted txn still gets its
    /// abort entry; one that starts after the ABORT marker doesn't.
    #[test]
    fn in_range_includes_a_txn_the_fetch_starts_inside() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(7, 100, 150));
        let got = idx.in_range(120, 200);
        assert_eq!(got.len(), 1, "fetch from 120 lies inside txn 100..=150");
        assert_eq!(got[0].first_offset, 100);
        assert_eq!(idx.in_range(150, 200).len(), 1, "marker itself is in range");
        assert!(idx.in_range(151, 200).is_empty(), "fetch past the marker");
    }

    #[test]
    fn in_range_end_is_exclusive() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(1, 100, 101));
        // end == first_offset must NOT include the entry.
        assert!(idx.in_range(0, 100).is_empty());
        assert_eq!(idx.in_range(0, 101).len(), 1);
    }

    #[test]
    fn evict_below_drops_fully_covered_entries() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(1, 100, 110));
        idx.record(aborted(2, 200, 210));
        idx.record(aborted(3, 300, 310));
        idx.evict_below(211);
        // 1 (last 110 < 211): dropped. 2 (last 210 < 211): dropped.
        // 3 (last 310 >= 211): kept.
        let snap = idx.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].producer_id, 3);
    }

    #[test]
    fn evict_below_stops_at_first_kept_entry() {
        let mut idx = AbortedTxnIndex::new();
        idx.record(aborted(1, 100, 110));
        idx.record(aborted(2, 200, 210));
        idx.evict_below(105); // 1's last 110 >= 105 → keep both.
        assert_eq!(idx.snapshot().len(), 2);
    }
}
