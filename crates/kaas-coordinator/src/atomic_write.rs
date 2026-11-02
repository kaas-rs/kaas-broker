//! `tmp + fsync + rename` writer for `__consumer_offsets/<group>.json`.
//!
//! Mirrors the dance in `kaas-storage::atomic_write` but stays
//! standalone — the coordinator owns one JSON file per group and
//! doesn't need the storage engine's `Fs` trait abstraction. Same
//! NFSv4-class atomicity guarantee: same-directory rename is atomic,
//! so a crash mid-write leaves either the old file or the new one,
//! never a torn payload.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

/// Atomically write the JSON-serialised payload to `dir/name`.
///
/// 1. `mkdir_all(dir)`, syncing each new directory into its parent (gh #289)
/// 2. JSON-encode payload
/// 3. Write into `dir/<name>.tmp`
/// 4. `fsync` the tmp file
/// 5. `rename(<name>.tmp, <name>)`
/// 6. `fsync` the directory, so the rename is durable (gh #231)
pub fn atomic_write_json<T: serde::Serialize>(
    dir: &Path,
    name: &str,
    payload: &T,
) -> io::Result<()> {
    create_dir_all_durable(dir)?;
    let data = serde_json::to_vec(payload).map_err(io::Error::other)?;

    let mut tmp_name = String::from(name);
    tmp_name.push_str(".tmp");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(name);

    {
        let mut f: File = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&tmp_path)?;
        if let Err(e) = f.write_all(&data) {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
        if let Err(e) = f.sync_all() {
            let _ = fs::remove_file(&tmp_path);
            return Err(e);
        }
    }

    if let Err(e) = fs::rename(&tmp_path, &final_path) {
        let _ = fs::remove_file(&tmp_path);
        return Err(e);
    }

    fsync_dir(dir)
}

/// Sync directory `dir` so a rename into it survives a crash on a local
/// filesystem (gh #231); a twin of `kaas_storage::fs::fsync_dir`, kept
/// here because this writer stays independent of the storage crate.
/// `EINVAL` and `EBADF` (a filesystem that can't fsync a directory) count
/// as done, as in PostgreSQL's `fsync_fname` (gh #289).
pub(crate) fn fsync_dir(dir: &Path) -> io::Result<()> {
    /// `EBADF` on Linux and the BSDs.
    const EBADF: i32 = 9;
    match File::open(dir)?.sync_all() {
        Err(e) if e.kind() == io::ErrorKind::InvalidInput || e.raw_os_error() == Some(EBADF) => {
            Ok(())
        }
        r => r,
    }
}

/// `create_dir_all`, then sync each directory it had to create into its
/// parent (gh #289), so a new `txn_state/`, `marker_queue/to-<n>/` or
/// fence-log directory survives a crash on a local filesystem along with
/// the files later renamed into it. A twin of
/// `kaas_storage::fs::create_dir_all_durable`.
pub fn create_dir_all_durable(dir: &Path) -> io::Result<()> {
    let mut missing = Vec::new();
    let mut cur = Some(dir);
    while let Some(d) = cur.filter(|d| !d.as_os_str().is_empty() && !d.exists()) {
        missing.push(d.to_path_buf());
        cur = d.parent();
    }
    fs::create_dir_all(dir)?;
    for d in &missing {
        if let Some(parent) = d.parent().filter(|q| !q.as_os_str().is_empty()) {
            fsync_dir(parent)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replaces_existing_atomically() {
        let tmp = tempfile::tempdir().unwrap();
        atomic_write_json(tmp.path(), "x.json", &serde_json::json!({"v": 1})).unwrap();
        atomic_write_json(tmp.path(), "x.json", &serde_json::json!({"v": 2})).unwrap();

        let data = fs::read_to_string(tmp.path().join("x.json")).unwrap();
        assert!(data.contains("\"v\":2"));
        assert!(!tmp.path().join("x.json.tmp").exists());
    }
}
