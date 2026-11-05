//! The `tmp + fsync + rename + fsync(dir)` writer for the coordinator's
//! JSON files: `kaas_storage::atomic_write` over the real filesystem, so
//! there is one implementation of the NFS substrate's persist rule and its
//! ordering tests cover every caller (gh #289).

use std::io;
use std::path::Path;

use kaas_storage::fs::RealFs;

pub use kaas_storage::fs::{create_dir_all_durable, fsync_dir};

/// Atomically write the JSON-serialised payload to `dir/name`.
pub fn atomic_write_json<T: serde::Serialize>(
    dir: &Path,
    name: &str,
    payload: &T,
) -> io::Result<()> {
    kaas_storage::atomic_write::atomic_write_json(&RealFs::new(), dir, name, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_existing_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        atomic_write_json(tmp.path(), "x.json", &serde_json::json!({"v": 1})).unwrap();
        atomic_write_json(tmp.path(), "x.json", &serde_json::json!({"v": 2})).unwrap();

        let data = std::fs::read_to_string(tmp.path().join("x.json")).unwrap();
        assert!(data.contains("\"v\":2"));
        assert!(!tmp.path().join("x.json.tmp").exists());
    }
}
