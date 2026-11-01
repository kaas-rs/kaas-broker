//! Per-partition recovery checkpoint: `recovery-checkpoint.json`.
//!
//! Kafka's recovery-point idea, per partition: a durable marker that
//! says "in the active segment, everything up to `byte_pos` is fsynced,
//! and the last batch before it starts at `last_batch_pos`." On
//! takeover, recovery scans forward from that batch instead of from the
//! segment start — O(bytes since the checkpoint) instead of O(active
//! segment). A clean close writes it at EOF, so a graceful restart
//! re-reads one batch.
//!
//! It is only a hint, and recovery verifies it before using it (gh #230).
//! The checkpoint carries no offsets: it names the byte where the last
//! batch before `byte_pos` starts, recovery resumes the scan *on* that
//! batch, and the high watermark comes from the batch headers the scan
//! reads. The resumed scan is accepted only if it parses cleanly at
//! least as far as `byte_pos`, which a stale checkpoint (another
//! incarnation of the segment, a copied directory) can't fake short of
//! a CRC32C collision. Otherwise, or if the file is missing or names a
//! different active segment, recovery scans the whole active segment
//! from byte 0. So a wrong checkpoint costs time, never offsets, and
//! never drives the torn-tail truncation (gh #226) from an unverified
//! position. It goes through [`atomic_write_json`] anyway (NFS-substrate
//! rule 1) so a torn write is never even read.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

pub const RECOVERY_CHECKPOINT_FILENAME: &str = "recovery-checkpoint.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryCheckpoint {
    /// `base_offset` of the active segment this checkpoint refers to.
    /// Recovery discards the checkpoint when the current active segment
    /// has a different base — i.e. a roll happened since it was written.
    pub segment_base: i64,
    /// Byte offset within the active segment's log file up to which the
    /// data is fsynced. A resumed scan must get at least this far.
    pub byte_pos: i64,
    /// Where the last batch before `byte_pos` starts; recovery resumes
    /// the scan here. `None` when the segment held no batch, and in a
    /// checkpoint written before gh #230 (which carried a
    /// `highWatermark` instead, now ignored), so either means a full
    /// scan.
    #[serde(default)]
    pub last_batch_pos: Option<i64>,
}

/// Read the checkpoint. `None` when the file is absent or unreadable —
/// recovery then falls back to a full scan, so a missing or corrupt
/// checkpoint is never a correctness problem.
#[must_use]
pub fn read(fs: &dyn Fs, dir: &Path) -> Option<RecoveryCheckpoint> {
    let path = dir.join(RECOVERY_CHECKPOINT_FILENAME);
    let mut f = fs.open_read(&path).ok()?;
    let mut buf = Vec::new();
    io::Read::read_to_end(&mut f, &mut buf).ok()?;
    serde_json::from_slice(&buf).ok()
}

/// Write the checkpoint atomically (tmp + fsync + rename).
pub fn write(fs: &dyn Fs, dir: &Path, cp: &RecoveryCheckpoint) -> io::Result<()> {
    atomic_write_json(fs, dir, RECOVERY_CHECKPOINT_FILENAME, cp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn missing_checkpoint_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(read(&RealFs::new(), tmp.path()).is_none());
    }

    #[test]
    fn write_then_read_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let cp = RecoveryCheckpoint {
            segment_base: 100,
            byte_pos: 4096,
            last_batch_pos: Some(3000),
        };
        write(&fs, tmp.path(), &cp).unwrap();
        assert_eq!(read(&fs, tmp.path()), Some(cp));
    }

    /// A checkpoint written before gh #230 still parses; with no batch
    /// position it can only mean "scan from the start".
    #[test]
    fn a_pre_gh230_checkpoint_reads_with_no_batch_position() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(RECOVERY_CHECKPOINT_FILENAME),
            br#"{"segmentBase":0,"bytePos":4096,"highWatermark":250}"#,
        )
        .unwrap();
        let cp = read(&RealFs::new(), tmp.path()).unwrap();
        assert_eq!(cp.byte_pos, 4096);
        assert_eq!(cp.last_batch_pos, None);
    }

    #[test]
    fn corrupt_checkpoint_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join(RECOVERY_CHECKPOINT_FILENAME),
            b"not json at all",
        )
        .unwrap();
        assert!(read(&RealFs::new(), tmp.path()).is_none());
    }
}
