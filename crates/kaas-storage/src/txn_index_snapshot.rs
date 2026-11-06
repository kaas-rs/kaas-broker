//! Per-partition transactional-index snapshot: `txn-index.snapshot`
//! (gh #177).
//!
//! Persists the two gh #176 indexes — the open-transaction first
//! offsets (LSO source) and the aborted-transaction list
//! (`AbortedTransactions[]` source) — so `read_committed` correctness
//! survives broker restart and leader takeover. Without it both
//! indexes came back empty after `Partition::open`: LSO snapped to the
//! HWM (leaking a still-open transaction's records to `read_committed`
//! consumers) and previously-aborted records were served with no
//! abort entry to filter them by.
//!
//! Same persistence contract as `producer-state.snapshot` (gh #12),
//! deliberately: written at take-over, at close/relinquish and every
//! `STATE_SNAPSHOT_INTERVAL_BYTES` of log, restored on `Partition::open`,
//! atomic via tmp + fsync + rename. And the same `covers` field: `open`
//! replays the log after that point, so a hard crash loses nothing the
//! log still holds (gh #303). A snapshot without one covers nothing, and
//! these indexes are then rebuilt from the log start.
//!
//! # Schema
//!
//! ```json
//! {
//!   "version": 1,
//!   "covers": {"offset": 130, "byte_pos": 9216, "epoch": 3},
//!   "open": [{"producer_id": 9, "first_offset": 120}],
//!   "aborted": [{"producer_id": 7, "first_offset": 40, "last_offset": 55}]
//! }
//! ```
//!
//! A snapshot from a future schema version is dropped (`Ok(None)`)
//! rather than misinterpreted — losing one restart's worth of index
//! (the pre-gh #177 status quo) beats refusing to open the partition.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;
use crate::producer_snapshot::SnapshotCoverage;
use crate::txn_index::AbortedTxn;

pub const TXN_INDEX_SNAPSHOT_FILENAME: &str = "txn-index.snapshot";
pub const TXN_INDEX_SNAPSHOT_VERSION: i64 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TxnIndexSnapshot {
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covers: Option<SnapshotCoverage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub open: Vec<OpenTxnEntry>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub aborted: Vec<AbortedTxnEntry>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct OpenTxnEntry {
    pub producer_id: i64,
    pub first_offset: i64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct AbortedTxnEntry {
    pub producer_id: i64,
    pub first_offset: i64,
    pub last_offset: i64,
}

/// The restored index contents: `(open entries, aborted entries, what they
/// cover)`.
pub type RestoredTxnIndexes = (Vec<(i64, i64)>, Vec<AbortedTxn>, Option<SnapshotCoverage>);

/// Read the txn-index snapshot for `dir`. `Ok(None)` for any of: file
/// absent, version mismatch (future schema).
pub fn read_txn_index_snapshot(
    fs: &dyn Fs,
    dir: &Path,
) -> Result<Option<RestoredTxnIndexes>, TxnIndexSnapshotError> {
    let path = dir.join(TXN_INDEX_SNAPSHOT_FILENAME);
    let mut buf = Vec::new();
    match fs.open_read(&path) {
        Ok(mut f) => {
            io::Read::read_to_end(&mut f, &mut buf)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let snap: TxnIndexSnapshot = serde_json::from_slice(&buf)?;
    if snap.version != TXN_INDEX_SNAPSHOT_VERSION {
        return Ok(None);
    }

    let open = snap
        .open
        .into_iter()
        .map(|e| (e.producer_id, e.first_offset))
        .collect();
    let aborted = snap
        .aborted
        .into_iter()
        .map(|e| AbortedTxn {
            producer_id: e.producer_id,
            first_offset: e.first_offset,
            last_offset: e.last_offset,
        })
        .collect();
    Ok(Some((open, aborted, snap.covers)))
}

/// Atomically write the txn-index snapshot. Like the producer
/// snapshot, empty indexes write a versioned skeleton rather than
/// removing the file — "no txn state" and "never snapshotted" stay
/// distinguishable.
pub fn write_txn_index_snapshot(
    fs: &dyn Fs,
    dir: &Path,
    open: &[(i64, i64)],
    aborted: &[AbortedTxn],
    covers: SnapshotCoverage,
) -> Result<(), TxnIndexSnapshotError> {
    let snap = TxnIndexSnapshot {
        version: TXN_INDEX_SNAPSHOT_VERSION,
        covers: Some(covers),
        open: open
            .iter()
            .map(|(pid, first)| OpenTxnEntry {
                producer_id: *pid,
                first_offset: *first,
            })
            .collect(),
        aborted: aborted
            .iter()
            .map(|a| AbortedTxnEntry {
                producer_id: a.producer_id,
                first_offset: a.first_offset,
                last_offset: a.last_offset,
            })
            .collect(),
    };
    atomic_write_json(fs, dir, TXN_INDEX_SNAPSHOT_FILENAME, &snap)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum TxnIndexSnapshotError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use std::io::Write;

    const COVERS: SnapshotCoverage = SnapshotCoverage {
        offset: 7,
        byte_pos: 512,
        epoch: 3,
    };

    #[test]
    fn empty_indexes_write_a_versioned_skeleton() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_txn_index_snapshot(&fs, tmp.path(), &[], &[], COVERS).unwrap();
        let body = std::fs::read_to_string(tmp.path().join(TXN_INDEX_SNAPSHOT_FILENAME)).unwrap();
        assert_eq!(
            body,
            r#"{"version":1,"covers":{"offset":7,"byte_pos":512,"epoch":3}}"#
        );
    }

    /// Written by the previous release, which has no `covers`.
    #[test]
    fn a_snapshot_without_coverage_still_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        std::fs::write(
            tmp.path().join(TXN_INDEX_SNAPSHOT_FILENAME),
            r#"{"version":1,"open":[{"producer_id":9,"first_offset":120}]}"#,
        )
        .unwrap();
        let (open, aborted, covers) = read_txn_index_snapshot(&fs, tmp.path()).unwrap().unwrap();
        assert_eq!(open, vec![(9, 120)]);
        assert!(aborted.is_empty());
        assert_eq!(covers, None);
    }

    #[test]
    fn roundtrip_preserves_both_indexes() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let open = vec![(9i64, 120i64), (11, 340)];
        let aborted = vec![
            AbortedTxn {
                producer_id: 7,
                first_offset: 40,
                last_offset: 55,
            },
            AbortedTxn {
                producer_id: 8,
                first_offset: 90,
                last_offset: 99,
            },
        ];
        write_txn_index_snapshot(&fs, tmp.path(), &open, &aborted, COVERS).unwrap();
        let (r_open, r_aborted, covers) =
            read_txn_index_snapshot(&fs, tmp.path()).unwrap().unwrap();
        assert_eq!(r_open, open);
        assert_eq!(r_aborted, aborted);
        assert_eq!(covers, Some(COVERS));
    }

    #[test]
    fn missing_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        assert!(read_txn_index_snapshot(&fs, tmp.path()).unwrap().is_none());
    }

    #[test]
    fn future_version_is_dropped_not_misinterpreted() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        {
            let mut f = fs
                .create(&tmp.path().join(TXN_INDEX_SNAPSHOT_FILENAME))
                .unwrap();
            f.write_all(br#"{"version":999,"open":[]}"#).unwrap();
        }
        assert!(read_txn_index_snapshot(&fs, tmp.path()).unwrap().is_none());
    }

    #[test]
    fn malformed_json_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        {
            let mut f = fs
                .create(&tmp.path().join(TXN_INDEX_SNAPSHOT_FILENAME))
                .unwrap();
            f.write_all(b"not json").unwrap();
        }
        assert!(matches!(
            read_txn_index_snapshot(&fs, tmp.path()).unwrap_err(),
            TxnIndexSnapshotError::Json(_)
        ));
    }
}
