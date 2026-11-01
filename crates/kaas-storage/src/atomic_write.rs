//! `tmp + fsync + rename` writer for partition metadata files.
//!
//! NFSv4 guarantees
//! that a same-directory rename is atomic, so a crash mid-write
//! leaves either the old file or the new one — never a torn JSON
//! payload.
//!
//! All three persisted JSON files in kaas go through here:
//! `manifest.json`, `producer-state.snapshot`, and the operator-
//! written `.config.json`.

use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;

use crate::fs::{fsync_path, Fs};

/// Atomically write a serde-serialisable payload to `dir/name`.
///
/// 1. `mkdir_all(dir)`
/// 2. JSON-encode payload
/// 3. Write into `dir/<name>.tmp`
/// 4. `fsync` the tmp file
/// 5. `rename(<name>.tmp, <name>)`
/// 6. `fsync` the directory, so the rename itself is durable (gh #231)
///
/// On any failure after the tmp file is opened, the tmp file is
/// removed before the error is propagated so a partial write never
/// leaks.
pub fn atomic_write_json<T: Serialize>(
    fs: &dyn Fs,
    dir: &Path,
    name: &str,
    payload: &T,
) -> io::Result<()> {
    atomic_write_json_inner(fs, dir, name, payload, false)
}

/// Same as [`atomic_write_json`] but pretty-printed (2-space indent +
/// newline-terminated). The operator-side `credentials.json` /
/// `acls.json` writers use this so the on-disk format stays
/// byte-equal to the v0.1 pretty-printed output — humans inspect
/// these files and the existing tooling expects multi-line layout.
pub fn atomic_write_json_pretty<T: Serialize>(
    fs: &dyn Fs,
    dir: &Path,
    name: &str,
    payload: &T,
) -> io::Result<()> {
    atomic_write_json_inner(fs, dir, name, payload, true)
}

fn atomic_write_json_inner<T: Serialize>(
    fs: &dyn Fs,
    dir: &Path,
    name: &str,
    payload: &T,
    pretty: bool,
) -> io::Result<()> {
    fs.mkdir_all(dir)?;
    let data = if pretty {
        let mut v = serde_json::to_vec_pretty(payload).map_err(io::Error::other)?;
        // The v0.1 writer didn't add a trailing newline either,
        // so don't append one — keep the output byte-identical.
        v.shrink_to_fit();
        v
    } else {
        serde_json::to_vec(payload).map_err(io::Error::other)?
    };

    let mut tmp_name = String::from(name);
    tmp_name.push_str(".tmp");
    let tmp_path = dir.join(&tmp_name);
    let final_path = dir.join(name);

    // Step 3: write the bytes.
    {
        let mut f = fs.create(&tmp_path)?;
        if let Err(e) = f.write_all(&data) {
            let _ = fs.remove(&tmp_path);
            return Err(e);
        }
        if let Err(e) = f.flush() {
            let _ = fs.remove(&tmp_path);
            return Err(e);
        }
    }

    // Step 4: fsync the tmp file via the path-based helper. The trait-
    // object route (`Fs::fsync`) is intentionally no-op on `RealFs`
    // because the trait hides the concrete `std::fs::File`. See the
    // comment above `fs::fsync_path`.
    if let Err(e) = fsync_path(&tmp_path) {
        let _ = fs.remove(&tmp_path);
        return Err(e);
    }

    // Step 5: rename.
    if let Err(e) = fs.rename(&tmp_path, &final_path) {
        let _ = fs.remove(&tmp_path);
        return Err(e);
    }

    // Step 6: the rename is a directory-entry change, and on a local
    // filesystem only a directory fsync makes it durable. A failure here
    // leaves the new file in place but not known to survive a crash, so
    // it is reported, not swallowed.
    fs.fsync_dir(dir)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;
    use serde::Deserialize;
    use std::io::Read;

    #[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Sample {
        a: i64,
        b: String,
    }

    /// gh #231: the directory is fsynced, and only after the rename —
    /// before it there is nothing new to make durable.
    #[test]
    fn the_directory_is_synced_after_the_rename() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = crate::fs::JournalFs::default();
        let s = Sample {
            a: 1,
            b: "x".into(),
        };
        atomic_write_json(&fs, tmp.path(), "m.json", &s).unwrap();
        assert_eq!(
            *fs.ops.lock(),
            ["create m.json.tmp", "rename m.json", "fsync_dir"]
        );
    }

    #[test]
    fn roundtrip_writes_and_reads() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let payload = Sample {
            a: 42,
            b: "kaas".into(),
        };
        atomic_write_json(&fs, tmp.path(), "thing.json", &payload).unwrap();

        let mut buf = String::new();
        fs.open_read(&tmp.path().join("thing.json"))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        let got: Sample = serde_json::from_str(&buf).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn replacing_an_existing_file_is_atomic_in_observable_state() {
        // After atomic_write_json returns successfully, the destination
        // file exists and the .tmp file does not.
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        atomic_write_json(
            &fs,
            tmp.path(),
            "x.json",
            &Sample {
                a: 1,
                b: "v1".into(),
            },
        )
        .unwrap();
        atomic_write_json(
            &fs,
            tmp.path(),
            "x.json",
            &Sample {
                a: 2,
                b: "v2".into(),
            },
        )
        .unwrap();

        assert!(fs.exists(&tmp.path().join("x.json")));
        assert!(!fs.exists(&tmp.path().join("x.json.tmp")));

        let mut buf = String::new();
        fs.open_read(&tmp.path().join("x.json"))
            .unwrap()
            .read_to_string(&mut buf)
            .unwrap();
        let got: Sample = serde_json::from_str(&buf).unwrap();
        assert_eq!(
            got,
            Sample {
                a: 2,
                b: "v2".into()
            }
        );
    }

    #[test]
    fn mkdir_all_creates_parent_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let nested = tmp.path().join("topic-a/partition-0");
        atomic_write_json(
            &fs,
            &nested,
            "manifest.json",
            &Sample {
                a: 5,
                b: "deep".into(),
            },
        )
        .unwrap();
        assert!(fs.exists(&nested.join("manifest.json")));
    }
}
