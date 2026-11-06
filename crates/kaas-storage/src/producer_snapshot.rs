//! Per-partition producer-state snapshot: `producer-state.snapshot`.
//!
//! Writes the
//! [`ProducerStates`] window to disk so the idempotent-producer dedupe
//! contract survives broker restart. The snapshot lives next to the
//! partition's `manifest.json`; both go through
//! [`crate::atomic_write::atomic_write_json`] for the tmp + fsync +
//! rename dance.
//!
//! # Schema
//!
//! ```json
//! {
//!   "version": 1,
//!   "entries": [
//!     {
//!       "producer_id": 100,
//!       "epoch": 5,
//!       "recent": [
//!         {"first_seq": 0, "last_seq": 4, "base_offset": 0},
//!         {"first_seq": 5, "last_seq": 9, "base_offset": 5}
//!       ]
//!     }
//!   ]
//! }
//! ```
//!
//! Schema version is checked on read; a snapshot from a future version
//! is dropped (returns `Ok(None)`) rather than misinterpreted — we'd
//! rather lose the dedupe window for one restart than refuse to open
//! the partition.
//!
//! # A snapshot is a starting point, not the state (gh #303)
//!
//! `covers` says which part of the log the snapshot reflects.
//! `Partition::open` takes it from there and replays the batch headers
//! after that point, so an unclean stop loses nothing the log still
//! holds. A snapshot without `covers` is treated as covering nothing.
//! The field is optional so an adjacent release can read this file in
//! either direction.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;
use crate::idempotence::{ProducerEntry, RecentBatch};

pub const PRODUCER_SNAPSHOT_FILENAME: &str = "producer-state.snapshot";
pub const PRODUCER_SNAPSHOT_VERSION: i64 = 1;

/// What a snapshot's contents reflect (gh #303). Both snapshot files
/// carry it, because a crash can land between writing them.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnapshotCoverage {
    /// Every batch below this offset is in the snapshot, and replay
    /// starts here. Always a batch boundary: the high watermark when the
    /// snapshot was taken.
    pub offset: i64,
    /// Where `offset` was in the segment active at the time. Only a hint
    /// that saves walking the segment from its start: `open` checks the
    /// batch found there really is at `offset`.
    pub byte_pos: u64,
    /// The writer's leader epoch. A lower one than the manifest's is a
    /// fenced leader's write, landing over its successor's.
    pub epoch: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerSnapshot {
    pub version: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub covers: Option<SnapshotCoverage>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entries: Vec<ProducerSnapshotEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerSnapshotEntry {
    pub producer_id: i64,
    pub epoch: i16,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub recent: Vec<ProducerSnapshotBatch>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProducerSnapshotBatch {
    pub first_seq: i32,
    pub last_seq: i32,
    pub base_offset: i64,
}

/// The restored snapshot: its entries, and what they cover.
pub type RestoredProducerStates = (Vec<(i64, ProducerEntry)>, Option<SnapshotCoverage>);

/// Read the producer-state snapshot for `dir`. Returns `Ok(None)` for
/// any of: file absent, file empty, version mismatch (future schema).
pub fn read_producer_snapshot(
    fs: &dyn Fs,
    dir: &Path,
) -> Result<Option<RestoredProducerStates>, ProducerSnapshotError> {
    let path = dir.join(PRODUCER_SNAPSHOT_FILENAME);
    let mut buf = Vec::new();
    match fs.open_read(&path) {
        Ok(mut f) => {
            io::Read::read_to_end(&mut f, &mut buf)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    }

    let snap: ProducerSnapshot = serde_json::from_slice(&buf)?;
    if snap.version != PRODUCER_SNAPSHOT_VERSION {
        return Ok(None);
    }

    let covers = snap.covers;
    let mut out = Vec::with_capacity(snap.entries.len());
    for e in snap.entries {
        let recent = e
            .recent
            .into_iter()
            .map(|rb| RecentBatch {
                first_seq: rb.first_seq,
                last_seq: rb.last_seq,
                base_offset: rb.base_offset,
            })
            .collect();
        out.push((
            e.producer_id,
            ProducerEntry {
                epoch: e.epoch,
                recent,
            },
        ));
    }
    Ok(Some((out, covers)))
}

/// Atomically write the producer-state snapshot. An empty entry list
/// writes an empty snapshot rather than removing the file — the
/// explicit "this partition had producers but the state was cleared"
/// signal is useful for diagnosis.
pub fn write_producer_snapshot(
    fs: &dyn Fs,
    dir: &Path,
    states: &[(i64, ProducerEntry)],
    covers: SnapshotCoverage,
) -> Result<(), ProducerSnapshotError> {
    let snap = ProducerSnapshot {
        version: PRODUCER_SNAPSHOT_VERSION,
        covers: Some(covers),
        entries: states
            .iter()
            .map(|(pid, entry)| ProducerSnapshotEntry {
                producer_id: *pid,
                epoch: entry.epoch,
                recent: entry
                    .recent
                    .iter()
                    .map(|rb| ProducerSnapshotBatch {
                        first_seq: rb.first_seq,
                        last_seq: rb.last_seq,
                        base_offset: rb.base_offset,
                    })
                    .collect(),
            })
            .collect(),
    };
    atomic_write_json(fs, dir, PRODUCER_SNAPSHOT_FILENAME, &snap)?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ProducerSnapshotError {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use crate::idempotence::ProducerStates;
    use std::io::Write;

    const COVERS: SnapshotCoverage = SnapshotCoverage {
        offset: 7,
        byte_pos: 512,
        epoch: 3,
    };

    #[test]
    fn empty_states_writes_a_versioned_skeleton() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_producer_snapshot(&fs, tmp.path(), &[], COVERS).unwrap();
        let path = tmp.path().join(PRODUCER_SNAPSHOT_FILENAME);
        let body = std::fs::read_to_string(&path).unwrap();
        // No entries means no entries field on the wire (skip_serializing_if).
        assert_eq!(
            body,
            r#"{"version":1,"covers":{"offset":7,"byte_pos":512,"epoch":3}}"#
        );
    }

    /// The previous release wrote no `covers` and doesn't know the field.
    /// A rolling update crosses both ways, so neither may break the other.
    #[test]
    fn a_snapshot_without_coverage_still_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        std::fs::write(
            tmp.path().join(PRODUCER_SNAPSHOT_FILENAME),
            r#"{"version":1,"entries":[{"producer_id":100,"epoch":5}]}"#,
        )
        .unwrap();
        let (entries, covers) = read_producer_snapshot(&fs, tmp.path()).unwrap().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(covers, None);
    }

    #[test]
    fn json_snake_case_matches_go() {
        // producer-state.snapshot bytes are pinned to the v0.1 layout:
        // snake_case JSON keys (`producer_id`,
        // `first_seq`, …); we match via serde's default field naming
        // for `pub field_name` plus the `recent` Vec.
        let snap = ProducerSnapshot {
            version: 1,
            covers: None,
            entries: vec![ProducerSnapshotEntry {
                producer_id: 100,
                epoch: 5,
                recent: vec![ProducerSnapshotBatch {
                    first_seq: 0,
                    last_seq: 4,
                    base_offset: 0,
                }],
            }],
        };
        let json = serde_json::to_string(&snap).unwrap();
        assert_eq!(
            json,
            r#"{"version":1,"entries":[{"producer_id":100,"epoch":5,"recent":[{"first_seq":0,"last_seq":4,"base_offset":0}]}]}"#
        );
    }

    #[test]
    fn roundtrip_through_atomic_write() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();

        let states = ProducerStates::new();
        states.record_accepted(
            crate::idempotence::BatchProducerInfo {
                producer_id: 1,
                epoch: 0,
                first_seq: 0,
                last_seq: 2,
            },
            100,
        );
        states.record_accepted(
            crate::idempotence::BatchProducerInfo {
                producer_id: 2,
                epoch: 3,
                first_seq: 0,
                last_seq: 4,
            },
            200,
        );

        let snap = states.snapshot();
        write_producer_snapshot(&fs, tmp.path(), &snap, COVERS).unwrap();
        let (mut restored, covers) = read_producer_snapshot(&fs, tmp.path()).unwrap().unwrap();
        assert_eq!(covers, Some(COVERS));
        // Sort to make the equality check order-independent.
        restored.sort_by_key(|(pid, _)| *pid);
        let mut snap_sorted = snap.clone();
        snap_sorted.sort_by_key(|(pid, _)| *pid);
        assert_eq!(restored, snap_sorted);
    }

    #[test]
    fn missing_file_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        assert!(read_producer_snapshot(&fs, tmp.path()).unwrap().is_none());
    }

    #[test]
    fn future_version_is_dropped_not_misinterpreted() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let path = tmp.path().join(PRODUCER_SNAPSHOT_FILENAME);
        {
            let mut f = fs.create(&path).unwrap();
            f.write_all(br#"{"version":999,"entries":[]}"#).unwrap();
        }
        // Future schema → None, not an error.
        assert!(read_producer_snapshot(&fs, tmp.path()).unwrap().is_none());
    }

    #[test]
    fn malformed_json_is_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let path = tmp.path().join(PRODUCER_SNAPSHOT_FILENAME);
        {
            let mut f = fs.create(&path).unwrap();
            f.write_all(b"not json at all").unwrap();
        }
        assert!(matches!(
            read_producer_snapshot(&fs, tmp.path()).unwrap_err(),
            ProducerSnapshotError::Json(_)
        ));
    }
}
