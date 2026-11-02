//! Outbound producer-epoch fence log (gh #108 phase 2).
//!
//!
//! When this broker is the txn coordinator for `transactional.id`
//! and `InitProducerId` bumps the epoch, every partition this
//! broker leads is fenced in-process. Partitions led by *other*
//! brokers see no signal until the new session writes there — a
//! zombie batch from the old session can sneak in during that
//! window. Phase 2 of gh #108 closes the gap: every fence event is
//! written to this broker's outbound file under
//! `<data_dir>/__cluster/producer_fences/from-<broker_id>.json` and
//! peer brokers' [`FenceWatcher`] picks it up.
//!
//! On-disk shape: a JSON object mapping stringified `pid → epoch`.
//! Stringified because `serde_json` (like the v0.1 encoder)
//! doesn't support non-string map keys.
//!
//! Atomic `tmp + fsync + rename` per Append. Idempotent — appending
//! an epoch `<=` the recorded one is a no-op.
//!
//! [`FenceWatcher`]: ../../../kaas_broker/fence_watcher/struct.FenceWatcher.html

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

use crate::atomic_write::atomic_write_json;

/// Conventional directory inside `<data_dir>/__cluster/` where all
/// brokers publish their outbound fence files.
pub const FENCE_DIR_NAME: &str = "producer_fences";

/// Per-broker outbound producer-fence log.
pub struct FenceLog {
    path: PathBuf,
    mu: Mutex<()>,
}

impl std::fmt::Debug for FenceLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FenceLog")
            .field("path", &self.path)
            .finish()
    }
}

impl FenceLog {
    /// Open (and create if missing) the per-broker fence log at
    /// `dir/from-<broker_id>.json`. `dir` is conventionally
    /// `<data_dir>/__cluster/producer_fences/` — use
    /// [`fence_log_dir`] to derive it from the cluster dir.
    pub fn open(dir: &Path, broker_id: &str) -> io::Result<Self> {
        if broker_id.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fence log: empty broker id",
            ));
        }
        crate::atomic_write::create_dir_all_durable(dir)?;
        let path = dir.join(format!("from-{broker_id}.json"));
        Ok(Self {
            path,
            mu: Mutex::new(()),
        })
    }

    /// Record `(pid, epoch)`. If the file already has a higher-or-
    /// equal epoch for this pid, no-op. Atomic tmp + rename so peers
    /// reading mid-write see the prior consistent state, never a
    /// half-written file.
    pub fn append(&self, pid: i64, epoch: i16) -> io::Result<()> {
        let _g = self.mu.lock();
        let mut state = load_state(&self.path)?;
        let key = pid.to_string();
        if let Some(&existing) = state.get(&key) {
            if existing >= epoch {
                return Ok(());
            }
        }
        state.insert(key, epoch);
        let parent = self
            .path
            .parent()
            .ok_or_else(|| io::Error::other("fence log path has no parent"))?;
        let name = self
            .path
            .file_name()
            .and_then(|n| n.to_str())
            .ok_or_else(|| io::Error::other("fence log path has non-utf8 file name"))?;
        atomic_write_json(parent, name, &state)?;
        Ok(())
    }

    /// Copy of the current `(pid → epoch)` map. Tests use this to
    /// inspect the on-disk shape without poking into private fields.
    pub fn snapshot(&self) -> HashMap<i64, i16> {
        let _g = self.mu.lock();
        let state = load_state(&self.path).unwrap_or_default();
        state
            .into_iter()
            .filter_map(|(k, v)| k.parse::<i64>().ok().map(|pid| (pid, v)))
            .collect()
    }

    /// On-disk path. Used by the peer [`FenceWatcher`] to know which
    /// file to skip (don't re-apply our own outbound entries).
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Conventional directory for fence files: `<cluster_dir>/producer_fences/`.
pub fn fence_log_dir(cluster_dir: &Path) -> PathBuf {
    cluster_dir.join(FENCE_DIR_NAME)
}

fn load_state(path: &Path) -> io::Result<HashMap<String, i16>> {
    match fs::read(path) {
        Ok(data) if data.is_empty() => Ok(HashMap::new()),
        Ok(data) => serde_json::from_slice(&data).map_err(io::Error::other),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(HashMap::new()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_then_snapshot_roundtrip() {
        let tmp = tempfile::tempdir().unwrap();
        let log = FenceLog::open(tmp.path(), "kaas-0").unwrap();
        log.append(42, 3).unwrap();
        log.append(99, 1).unwrap();
        let snap = log.snapshot();
        assert_eq!(snap.get(&42), Some(&3));
        assert_eq!(snap.get(&99), Some(&1));
    }

    #[test]
    fn append_lower_or_equal_epoch_is_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let log = FenceLog::open(tmp.path(), "kaas-0").unwrap();
        log.append(42, 5).unwrap();
        log.append(42, 5).unwrap(); // equal — no-op
        log.append(42, 3).unwrap(); // lower — no-op
        assert_eq!(log.snapshot().get(&42), Some(&5));
    }

    #[test]
    fn append_higher_epoch_overwrites() {
        let tmp = tempfile::tempdir().unwrap();
        let log = FenceLog::open(tmp.path(), "kaas-0").unwrap();
        log.append(42, 3).unwrap();
        log.append(42, 7).unwrap();
        assert_eq!(log.snapshot().get(&42), Some(&7));
    }

    #[test]
    fn empty_broker_id_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let err = FenceLog::open(tmp.path(), "").unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn file_name_matches_broker_id() {
        let tmp = tempfile::tempdir().unwrap();
        let log = FenceLog::open(tmp.path(), "kaas-3").unwrap();
        assert_eq!(log.path().file_name().unwrap(), "from-kaas-3.json");
    }

    #[test]
    fn snapshot_on_missing_file_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let log = FenceLog::open(tmp.path(), "kaas-0").unwrap();
        // No append — snapshot should be empty, not error.
        assert!(log.snapshot().is_empty());
    }

    #[test]
    fn fence_log_dir_under_cluster() {
        let cluster = Path::new("/data/__cluster");
        assert_eq!(
            fence_log_dir(cluster),
            Path::new("/data/__cluster/producer_fences")
        );
    }
}
