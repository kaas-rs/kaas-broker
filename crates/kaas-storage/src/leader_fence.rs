//! Per-partition leader fence: `leader-fence.json` (gh #164).
//!
//! A partition moves between brokers by `assignment.json`, and the broker
//! losing it only finds out when its own view of that file catches up. Until
//! then it keeps accepting `acks=all` writes. The epoch roll (gh #227) keeps
//! those writes out of the new leader's log, but each one it acknowledged
//! was lost: the new leader's recovery scan had already run, so it never saw
//! them.
//!
//! The fence closes that window with an ordering, not a timeout:
//!
//! 1. A new leader writes its epoch here, durably, **before** it scans the
//!    log it is taking over ([`crate::DiskStorageEngine`]'s `take_over`).
//! 2. Every leader reads it **after** each group-commit fsync and before it
//!    releases a single ack. An epoch above its own means it was deposed:
//!    the waiters get `EpochMismatch` (wire `NOT_LEADER_OR_FOLLOWER`,
//!    retriable) and the partition takes no more appends.
//!
//! So an ack the old leader does send was fsynced before the fence was
//! written, which was before the successor's scan, and the scan includes it.
//! The read has to reach the server for that to hold, which a fresh open
//! gives on NFS (close-to-open; a v4 OPEN resolves the name on the server).
//!
//! A separate file rather than the manifest's epoch because the manifest is
//! what `Partition::open` adopts its epoch from: written ahead of the scan,
//! it would make the new leader's `take_over` look already done, and skip
//! the roll that keeps two leaders out of one file.
//!
//! [`raise`] is read-check-write, which NFS can't make atomic. Two brokers
//! raising it at once could leave the lower epoch on disk, which takes two
//! leaders acting at once on different assignments. The assignment's epoch
//! fence makes that a stale broker racing a takeover within the few
//! milliseconds of the write, the same residual gap the manifest's
//! `write_unless_superseded` has.

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

pub const LEADER_FENCE_FILENAME: &str = "leader-fence.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct LeaderFence {
    epoch: i64,
}

/// The fenced epoch, or `None` when no leader has written one. A file that
/// exists but can't be read or parsed is an error: the caller can't tell
/// whether it still leads, so it must not ack.
pub fn read(fs: &dyn Fs, dir: &Path) -> io::Result<Option<i64>> {
    let mut f = match fs.open_read(&dir.join(LEADER_FENCE_FILENAME)) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut buf = Vec::new();
    io::Read::read_to_end(&mut f, &mut buf)?;
    let fence: LeaderFence =
        serde_json::from_slice(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Some(fence.epoch))
}

/// Raise the fence to `epoch`, durably (tmp + fsync + rename + dir fsync).
/// Never lowers it: at or below what is there, it writes nothing. A file
/// that doesn't parse is replaced, since a takeover is exactly what decides
/// who leads.
pub fn raise(fs: &dyn Fs, dir: &Path, epoch: i64) -> io::Result<()> {
    match read(fs, dir) {
        Ok(Some(current)) if current >= epoch => return Ok(()),
        Err(e) if e.kind() != io::ErrorKind::InvalidData => return Err(e),
        _ => {}
    }
    atomic_write_json(fs, dir, LEADER_FENCE_FILENAME, &LeaderFence { epoch })
}

/// Whether the fence on disk says a leader at `epoch` has been deposed.
pub fn superseded(fs: &dyn Fs, dir: &Path, epoch: i64) -> io::Result<Option<i64>> {
    Ok(read(fs, dir)?.filter(|fenced| *fenced > epoch))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn no_fence_reads_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert_eq!(read(&RealFs::new(), tmp.path()).unwrap(), None);
        assert_eq!(superseded(&RealFs::new(), tmp.path(), 0).unwrap(), None);
    }

    #[test]
    fn raise_never_lowers_the_fence() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        raise(&fs, tmp.path(), 5).unwrap();
        raise(&fs, tmp.path(), 3).unwrap();
        assert_eq!(read(&fs, tmp.path()).unwrap(), Some(5));
        raise(&fs, tmp.path(), 7).unwrap();
        assert_eq!(read(&fs, tmp.path()).unwrap(), Some(7));
    }

    #[test]
    fn only_a_higher_epoch_supersedes() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        raise(&fs, tmp.path(), 5).unwrap();
        assert_eq!(superseded(&fs, tmp.path(), 4).unwrap(), Some(5));
        assert_eq!(superseded(&fs, tmp.path(), 5).unwrap(), None);
        assert_eq!(superseded(&fs, tmp.path(), 6).unwrap(), None);
    }

    #[test]
    fn an_unparseable_fence_is_an_error_until_a_takeover_replaces_it() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        std::fs::write(tmp.path().join(LEADER_FENCE_FILENAME), b"{not json").unwrap();
        assert_eq!(
            read(&fs, tmp.path()).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        raise(&fs, tmp.path(), 2).unwrap();
        assert_eq!(read(&fs, tmp.path()).unwrap(), Some(2));
    }
}
