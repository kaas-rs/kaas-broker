//! Filesystem abstraction.
//!
//! Every disk operation in `kaas-storage` routes through [`Fs`]. The
//! production impl is [`RealFs`] — a thin wrapper over `std::fs`.
//! Tests use a `FailingFs` (workstream H) that wraps `RealFs` to
//! inject EIO / partial-write faults; a future `io_uring` impl
//! becomes a drop-in.
//!
//! The trait is deliberately small. Helpers built on top of it
//! (atomic-rename, walk-directory, scan-segment-files) live in
//! [`crate::atomic_write`] and the segment module.

use std::fs::Metadata;
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};

/// File handle returned by [`Fs::open_read`]. Implementors compose
/// `Read + Seek + Send`; the trait inherits those bounds via
/// supertrait constraints so callers can `Box<dyn FileRead>` without
/// re-stating them.
pub trait FileRead: Read + Seek + Send + 'static {}

/// File handle returned by [`Fs::open_write`] / [`Fs::create`].
/// `Write + Seek + Send`, plus per-handle `write_at` (atomic positioned
/// write) and `sync_all` (durable flush) so the segment hot path
/// doesn't pay the close-reopen cost on every fsync.
pub trait FileWrite: Write + Seek + Send + 'static {
    /// Atomic positioned write at byte offset `offset`. Default impl
    /// uses `seek + write_all`; [`RealFs`] overrides with the native
    /// `pwrite(2)`-backed `FileExt::write_at` so the file's cursor
    /// position isn't disturbed by concurrent reads on a separate
    /// handle.
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        self.seek(io::SeekFrom::Start(offset))?;
        self.write_all(buf)
    }

    /// Flush kernel buffers and durable storage. Default impl flushes
    /// only; [`RealFs`] overrides with `std::fs::File::sync_all`.
    fn sync_all(&mut self) -> io::Result<()> {
        self.flush()
    }
}

/// All disk I/O the storage engine performs.
pub trait Fs: Send + Sync + 'static {
    fn open_read(&self, p: &Path) -> io::Result<Box<dyn FileRead>>;

    /// Open `p` for writing. `append == true` opens with O_APPEND; the
    /// file is created if missing.
    fn open_write(&self, p: &Path, append: bool) -> io::Result<Box<dyn FileWrite>>;

    /// Create-or-truncate `p` for writing. Equivalent to `open(O_WRONLY|O_CREAT|O_TRUNC)`.
    fn create(&self, p: &Path) -> io::Result<Box<dyn FileWrite>>;

    /// Flush kernel buffers for `f` to durable storage. The default
    /// impl calls `Write::flush` then a target-specific syscall;
    /// implementors that need a richer contract (`io_uring`,
    /// fault-injection) override.
    fn fsync(&self, f: &mut dyn FileWrite) -> io::Result<()>;

    /// Rename `from` to `to`. NFSv4 guarantees atomicity when both
    /// paths share a directory; cross-directory renames may not be
    /// atomic on some substrates and the caller should avoid them.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    fn remove(&self, p: &Path) -> io::Result<()>;

    /// Shrink `p` to exactly `len` bytes. Used by recovery to drop a
    /// torn tail before the log is appended to again (gh #226).
    ///
    /// The default impl reports `Unsupported` so wrapper/fake `Fs`
    /// implementations don't silently claim to have truncated. Callers
    /// must treat `Unsupported` as "the bytes are still there" and stay
    /// correct anyway — recovery does, by appending from the scan's
    /// valid end regardless.
    fn truncate(&self, p: &Path, len: u64) -> io::Result<()> {
        let _ = (p, len);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "truncate not implemented for this Fs",
        ))
    }

    /// Make the entries of directory `p` durable: a rename into it, or
    /// a file created in it (gh #231). Without this a local filesystem
    /// (ext4/xfs — the chart's single-broker local-path mode) can lose
    /// the entry on crash even though the file's data was fsynced: the
    /// partition comes back with the previous manifest, or without a
    /// freshly created segment. On NFS directory operations are already
    /// synchronous RPCs, so this costs next to nothing there.
    ///
    /// Same default as [`Self::truncate`]: `Unsupported`, so a
    /// wrapper/fake `Fs` can't silently claim durability.
    fn fsync_dir(&self, p: &Path) -> io::Result<()> {
        let _ = p;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "fsync_dir not implemented for this Fs",
        ))
    }

    fn mkdir_all(&self, p: &Path) -> io::Result<()>;

    fn readdir(&self, p: &Path) -> io::Result<Vec<PathBuf>>;

    fn stat(&self, p: &Path) -> io::Result<Metadata>;

    /// `true` if `p` exists. Default impl falls back to [`stat`]; an
    /// impl can specialise (e.g. for a memory-backed fake).
    fn exists(&self, p: &Path) -> bool {
        self.stat(p).is_ok()
    }
}

// ---------------------------------------------------------------------------
// RealFs — production impl over std::fs.
// ---------------------------------------------------------------------------

#[derive(Debug, Default, Clone, Copy)]
pub struct RealFs;

impl RealFs {
    pub fn new() -> Self {
        Self
    }
}

struct RealFile(std::fs::File);

impl Read for RealFile {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}
impl Write for RealFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl Seek for RealFile {
    fn seek(&mut self, p: io::SeekFrom) -> io::Result<u64> {
        self.0.seek(p)
    }
}
impl FileRead for RealFile {}
impl FileWrite for RealFile {
    fn write_at(&mut self, buf: &[u8], offset: u64) -> io::Result<()> {
        use std::os::unix::fs::FileExt;
        self.0.write_all_at(buf, offset)
    }

    fn sync_all(&mut self) -> io::Result<()> {
        self.0.sync_all()
    }
}

impl Fs for RealFs {
    fn open_read(&self, p: &Path) -> io::Result<Box<dyn FileRead>> {
        let f = std::fs::OpenOptions::new().read(true).open(p)?;
        Ok(Box::new(RealFile(f)))
    }

    fn open_write(&self, p: &Path, append: bool) -> io::Result<Box<dyn FileWrite>> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(false)
            .append(append)
            .open(p)?;
        Ok(Box::new(RealFile(f)))
    }

    fn create(&self, p: &Path) -> io::Result<Box<dyn FileWrite>> {
        let f = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(p)?;
        Ok(Box::new(RealFile(f)))
    }

    fn fsync(&self, f: &mut dyn FileWrite) -> io::Result<()> {
        // Downcast back to RealFile so we can call sync_all. The trait
        // object lost the concrete type; we recover it via a known-
        // invariant: every FileWrite returned from RealFs IS a
        // RealFile. Use std::any indirection rather than a runtime
        // assertion — keeps the unsafe-code lint happy.
        f.flush()?;
        // The only public path that produces a `dyn FileWrite` from
        // `RealFs` wraps a `std::fs::File`. We expose `sync_all`
        // through the trait by having callers pair fsync with a
        // RealFs-aware helper; here we provide the no-op pre-flush
        // and depend on the caller to drive sync via the AtomicWrite
        // helper which retains the concrete type. Phase 2 adds the
        // path-based fsync helper below.
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn remove(&self, p: &Path) -> io::Result<()> {
        std::fs::remove_file(p)
    }

    fn truncate(&self, p: &Path, len: u64) -> io::Result<()> {
        std::fs::OpenOptions::new()
            .write(true)
            .open(p)?
            .set_len(len)
    }

    fn fsync_dir(&self, p: &Path) -> io::Result<()> {
        fsync_dir(p)
    }

    fn mkdir_all(&self, p: &Path) -> io::Result<()> {
        std::fs::create_dir_all(p)
    }

    fn readdir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(p)? {
            out.push(entry?.path());
        }
        Ok(out)
    }

    fn stat(&self, p: &Path) -> io::Result<Metadata> {
        std::fs::metadata(p)
    }
}

// ---------------------------------------------------------------------------
// Path-based fsync helper.
//
// The trait method `fsync(&mut dyn FileWrite)` is intentionally a no-op for
// the default `RealFs` impl because the trait object hides the concrete
// `std::fs::File` we need to call `sync_all` on. Production atomic-write
// paths reach for [`fsync_path`] instead, which opens, flushes, and syncs
// a known path (open-write-sync-close-rename, one syscall set per file).
// ---------------------------------------------------------------------------

/// Open `p` read-write, sync_all, close. Used by atomic-rename writers
/// after the bulk write is complete.
pub fn fsync_path(p: &Path) -> io::Result<()> {
    let f = std::fs::OpenOptions::new().read(true).write(true).open(p)?;
    f.sync_all()
}

/// Open directory `p` and sync it, so the entries renamed or created in
/// it survive a crash (gh #231). For writers that use `std::fs` directly
/// rather than an [`Fs`]; [`RealFs::fsync_dir`] calls this.
///
/// `EINVAL` counts as success: a filesystem that can't fsync a directory
/// says so that way, and there is nothing further to do. PostgreSQL's
/// `fsync_fname` makes the same call.
pub fn fsync_dir(p: &Path) -> io::Result<()> {
    match std::fs::File::open(p)?.sync_all() {
        Err(e) if e.kind() == io::ErrorKind::InvalidInput => Ok(()),
        r => r,
    }
}

/// Test-only [`Fs`] over [`RealFs`] that journals the calls whose order
/// durability depends on: `create`, `rename` and `fsync_dir` (gh #231).
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct JournalFs {
    inner: RealFs,
    pub(crate) ops: parking_lot::Mutex<Vec<String>>,
}

#[cfg(test)]
impl JournalFs {
    fn log(&self, op: &str, p: &Path) {
        let name = p.file_name().map_or_else(
            || p.display().to_string(),
            |n| n.to_string_lossy().into_owned(),
        );
        self.ops.lock().push(format!("{op} {name}"));
    }
}

#[cfg(test)]
impl Fs for JournalFs {
    fn open_read(&self, p: &Path) -> io::Result<Box<dyn FileRead>> {
        self.inner.open_read(p)
    }
    fn open_write(&self, p: &Path, append: bool) -> io::Result<Box<dyn FileWrite>> {
        self.inner.open_write(p, append)
    }
    fn create(&self, p: &Path) -> io::Result<Box<dyn FileWrite>> {
        self.log("create", p);
        self.inner.create(p)
    }
    fn fsync(&self, f: &mut dyn FileWrite) -> io::Result<()> {
        self.inner.fsync(f)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.log("rename", to);
        self.inner.rename(from, to)
    }
    fn remove(&self, p: &Path) -> io::Result<()> {
        self.inner.remove(p)
    }
    fn fsync_dir(&self, p: &Path) -> io::Result<()> {
        self.ops.lock().push("fsync_dir".to_owned());
        self.inner.fsync_dir(p)
    }
    fn mkdir_all(&self, p: &Path) -> io::Result<()> {
        self.inner.mkdir_all(p)
    }
    fn readdir(&self, p: &Path) -> io::Result<Vec<PathBuf>> {
        self.inner.readdir(p)
    }
    fn stat(&self, p: &Path) -> io::Result<Metadata> {
        self.inner.stat(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn realfs_roundtrip_create_read() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let path = tmp.path().join("hello.txt");
        {
            let mut f = fs.create(&path).unwrap();
            f.write_all(b"kaas").unwrap();
        }
        let mut buf = Vec::new();
        fs.open_read(&path).unwrap().read_to_end(&mut buf).unwrap();
        assert_eq!(&buf, b"kaas");
    }

    #[test]
    fn realfs_mkdir_then_stat() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let nested = tmp.path().join("a/b/c");
        fs.mkdir_all(&nested).unwrap();
        assert!(fs.stat(&nested).unwrap().is_dir());
    }

    #[test]
    fn realfs_readdir_lists_children() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        for name in &["a.log", "b.log", "manifest.json"] {
            let _ = fs.create(&tmp.path().join(name)).unwrap();
        }
        let mut got: Vec<String> = fs
            .readdir(tmp.path())
            .unwrap()
            .into_iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        got.sort();
        assert_eq!(got, vec!["a.log", "b.log", "manifest.json"]);
    }

    #[test]
    fn realfs_rename_swaps_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let from = tmp.path().join("draft");
        let to = tmp.path().join("final");
        {
            let mut f = fs.create(&from).unwrap();
            f.write_all(b"ready").unwrap();
        }
        fs.rename(&from, &to).unwrap();
        assert!(!fs.exists(&from));
        assert!(fs.exists(&to));
    }

    #[test]
    fn realfs_truncate_shrinks_and_never_grows() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let path = tmp.path().join("log");
        {
            let mut f = fs.create(&path).unwrap();
            f.write_all(b"0123456789").unwrap();
        }
        fs.truncate(&path, 4).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"0123");
        // set_len can extend; recovery never asks it to, and callers
        // guard on `valid_end < log_size` — but pin the semantics so a
        // future change to that guard is a visible decision.
        fs.truncate(&path, 8).unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 8);
    }

    #[test]
    fn default_fs_truncate_reports_unsupported() {
        // Wrapper/fake Fs impls inherit this, so recovery must stay
        // correct when truncation silently can't happen.
        struct Bare;
        impl Fs for Bare {
            fn open_read(&self, _: &Path) -> io::Result<Box<dyn FileRead>> {
                unimplemented!()
            }
            fn open_write(&self, _: &Path, _: bool) -> io::Result<Box<dyn FileWrite>> {
                unimplemented!()
            }
            fn create(&self, _: &Path) -> io::Result<Box<dyn FileWrite>> {
                unimplemented!()
            }
            fn fsync(&self, _: &mut dyn FileWrite) -> io::Result<()> {
                unimplemented!()
            }
            fn rename(&self, _: &Path, _: &Path) -> io::Result<()> {
                unimplemented!()
            }
            fn remove(&self, _: &Path) -> io::Result<()> {
                unimplemented!()
            }
            fn mkdir_all(&self, _: &Path) -> io::Result<()> {
                unimplemented!()
            }
            fn readdir(&self, _: &Path) -> io::Result<Vec<PathBuf>> {
                unimplemented!()
            }
            fn stat(&self, _: &Path) -> io::Result<Metadata> {
                unimplemented!()
            }
        }
        let err = Bare.truncate(Path::new("/x"), 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        let err = Bare.fsync_dir(Path::new("/x")).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn fsync_dir_syncs_a_real_directory_and_fails_on_a_missing_one() {
        let tmp = tempfile::tempdir().unwrap();
        RealFs::new().fsync_dir(tmp.path()).unwrap();
        let missing = tmp.path().join("nope");
        assert_eq!(
            RealFs::new().fsync_dir(&missing).unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn fsync_path_on_real_file_is_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let path = tmp.path().join("durable");
        {
            let mut f = fs.create(&path).unwrap();
            f.write_all(b"persisted").unwrap();
        }
        fsync_path(&path).unwrap();
    }
}
