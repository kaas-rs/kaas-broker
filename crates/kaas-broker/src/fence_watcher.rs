//! Cross-broker producer-epoch fence watcher (gh #108 phase 2).
//!
//! Polls `<data_dir>/__cluster/producer_fences/` every 2 s, reads
//! every peer broker's `from-<id>.json`, and for each
//! `(pid, epoch)` pair that's newer than what we've already
//! applied calls [`ProducerEpochFencer::fence_producer_epoch`].
//! Self-loop avoidance: the file named `from-<our broker id>.json`
//! is skipped — we already applied those fences in-process when
//! `InitProducerId` ran.
//!
//! **Why poll, not `notify`?** The Phase 5 acknowledgement applies
//! here too: Linux `inotify` does not fire for changes made by
//! other NFS clients, so a `notify`-driven watcher would silently
//! miss every peer broker's writes on a shared-RWX volume. The
//! 2 s mtime poll is the load-bearing mechanism; see gh #166 for
//! the doc gap.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

/// Hook the watcher calls to apply a `(pid, epoch)` fence into the
/// local storage view. Production wires this to the storage engine's
/// per-partition `producer_states::fence` walker so a zombie batch
/// from the old session is rejected even on partitions the new
/// session hasn't yet touched. Tests substitute a capturing impl.
///
/// Same shape as the in-process callback `InitProducerIdHandler`
/// uses for the local fence (workstream C — currently a TODO until
/// the engine grows a cross-partition `fence_producer_epoch`).
pub trait ProducerEpochFencer: Send + Sync + 'static {
    fn fence_producer_epoch(&self, pid: i64, epoch: i16);
}

/// Default poll interval: 2 s.
pub const DEFAULT_POLL: Duration = Duration::from_secs(2);

/// Reads every peer broker's outbound fence file and dispatches new
/// entries to the wired [`ProducerEpochFencer`]. Designed to be
/// `tokio::spawn`ed under a cluster-runtime cancellation token.
pub struct FenceWatcher {
    dir: PathBuf,
    self_file: String,
    fencer: Arc<dyn ProducerEpochFencer>,
    poll: Duration,
    /// Per-peer `(pid → highest epoch already applied)` cache so
    /// the engine isn't re-fenced for the same value every tick.
    applied: Mutex<HashMap<String, HashMap<i64, i16>>>,
}

impl std::fmt::Debug for FenceWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FenceWatcher")
            .field("dir", &self.dir)
            .field("self_file", &self.self_file)
            .field("poll", &self.poll)
            .finish()
    }
}

impl FenceWatcher {
    /// `dir` is the conventional `producer_fences/` directory;
    /// `self_broker_id` derives the file name to skip
    /// (`from-<self_broker_id>.json`).
    pub fn new(dir: PathBuf, self_broker_id: &str, fencer: Arc<dyn ProducerEpochFencer>) -> Self {
        Self {
            dir,
            self_file: format!("from-{self_broker_id}.json"),
            fencer,
            poll: DEFAULT_POLL,
            applied: Mutex::new(HashMap::new()),
        }
    }

    /// Override the poll interval. Test-only — production keeps the
    /// 2 s default.
    pub fn with_poll(mut self, d: Duration) -> Self {
        self.poll = d;
        self
    }

    /// Drive the polling loop until `cancel` fires. Ticks once on
    /// entry so a peer's pre-existing file (e.g. after our own
    /// restart) is applied without waiting a full interval.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        self.tick_off_workers().await;
        let mut t = interval(self.poll);
        // Skip the immediate tick `interval` always yields first.
        t.tick().await;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = t.tick() => self.tick_off_workers().await,
            }
        }
    }

    /// [`Self::tick`] on the blocking pool (gh #332). It reads files, and applying
    /// a fence takes every open partition's mutex, which a committer holds across
    /// an fsync.
    async fn tick_off_workers(self: &Arc<Self>) {
        let me = self.clone();
        if let Err(err) = tokio::task::spawn_blocking(move || me.tick()).await {
            tracing::warn!(%err, "fence watcher: a pass panicked; retrying next poll");
        }
    }

    /// Single synchronous pass. Exposed so integration tests can
    /// drive the watcher deterministically.
    pub fn tick(&self) {
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(
                    dir = %self.dir.display(),
                    %e,
                    "fence watcher: scanning the cross-broker fence directory failed; \
                     peer producer-epoch bumps will not propagate this cycle",
                );
                return;
            }
        };
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name = match name.to_str() {
                Some(n) => n,
                None => continue,
            };
            if !name.starts_with("from-") || !name.ends_with(".json") {
                continue;
            }
            if name == self.self_file {
                continue;
            }
            self.apply_peer(name);
        }
    }

    fn apply_peer(&self, name: &str) {
        let path = self.dir.join(name);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(
                    file = %path.display(),
                    %e,
                    "fence watcher: reading peer broker's fence file failed; \
                     this broker may miss a producer-epoch bump until the next tick",
                );
                return;
            }
        };
        if data.is_empty() {
            return;
        }
        let peer_state: HashMap<String, i16> = match serde_json::from_slice(&data) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    file = %path.display(),
                    %e,
                    "fence watcher: decoding peer broker's fence-state JSON failed; \
                     file likely mid-write or corrupted; will retry next tick",
                );
                return;
            }
        };

        let mut pending: Vec<(i64, i16)> = Vec::new();
        {
            let applied = self.applied.lock();
            let last = applied.get(name);
            for (k, &epoch) in &peer_state {
                let pid: i64 = match k.parse() {
                    Ok(p) => p,
                    Err(_) => continue,
                };
                if let Some(&existing) = last.and_then(|l| l.get(&pid)) {
                    if existing >= epoch {
                        continue;
                    }
                }
                pending.push((pid, epoch));
            }
        }
        // Recorded only once applied, so a pass that dies partway retries what it
        // didn't reach on the next poll. A fence is idempotent. Recording first lost
        // those for good (gh #332 verification).
        for (pid, epoch) in pending {
            self.fencer.fence_producer_epoch(pid, epoch);
            self.applied
                .lock()
                .entry(name.to_owned())
                .or_default()
                .insert(pid, epoch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[derive(Default)]
    struct Capturing {
        calls: parking_lot::Mutex<Vec<(i64, i16)>>,
    }

    impl ProducerEpochFencer for Capturing {
        fn fence_producer_epoch(&self, pid: i64, epoch: i16) {
            self.calls.lock().push((pid, epoch));
        }
    }

    fn write_peer(dir: &Path, name: &str, state: &HashMap<i64, i16>) {
        // Stringified keys to match the on-disk shape.
        let stringy: HashMap<String, i16> =
            state.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        let data = serde_json::to_vec(&stringy).unwrap();
        fs::write(dir.join(name), data).unwrap();
    }

    #[test]
    fn applies_peer_fences() {
        let tmp = tempfile::tempdir().unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        let mut peer = HashMap::new();
        peer.insert(42, 3);
        peer.insert(99, 1);
        write_peer(tmp.path(), "from-kaas-1.json", &peer);
        w.tick();
        let mut calls = capturing.calls.lock().clone();
        calls.sort();
        assert_eq!(calls, vec![(42, 3), (99, 1)]);
    }

    #[test]
    fn skips_self_file() {
        let tmp = tempfile::tempdir().unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        let mut state = HashMap::new();
        state.insert(42, 3);
        write_peer(tmp.path(), "from-kaas-0.json", &state); // self
        write_peer(tmp.path(), "from-kaas-1.json", &state); // peer
        w.tick();
        // Only the peer entry fires.
        assert_eq!(capturing.calls.lock().len(), 1);
    }

    #[test]
    fn dedupe_across_ticks() {
        let tmp = tempfile::tempdir().unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        let mut state = HashMap::new();
        state.insert(42, 3);
        write_peer(tmp.path(), "from-kaas-1.json", &state);
        w.tick();
        w.tick();
        assert_eq!(capturing.calls.lock().len(), 1);
    }

    #[test]
    fn higher_epoch_after_first_apply_fires_once_more() {
        let tmp = tempfile::tempdir().unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        let mut state = HashMap::new();
        state.insert(42, 3);
        write_peer(tmp.path(), "from-kaas-1.json", &state);
        w.tick();
        state.insert(42, 5); // bump
        write_peer(tmp.path(), "from-kaas-1.json", &state);
        w.tick();
        let calls = capturing.calls.lock().clone();
        assert_eq!(calls, vec![(42, 3), (42, 5)]);
    }

    #[test]
    fn missing_dir_is_silent_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(missing, "kaas-0", capturing.clone());
        w.tick(); // must not panic / error
        assert!(capturing.calls.lock().is_empty());
    }

    #[test]
    fn corrupt_peer_file_is_skipped() {
        let tmp = tempfile::tempdir().unwrap();
        fs::write(tmp.path().join("from-kaas-1.json"), b"\xffnot-json").unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        w.tick();
        assert!(capturing.calls.lock().is_empty());
    }

    #[test]
    fn ignores_files_not_matching_pattern() {
        let tmp = tempfile::tempdir().unwrap();
        // Wrong prefix / suffix.
        let mut state = HashMap::new();
        state.insert(42, 3);
        write_peer(tmp.path(), "from-kaas-1.json", &state);
        let stringy: HashMap<String, i16> =
            state.iter().map(|(k, v)| (k.to_string(), *v)).collect();
        fs::write(
            tmp.path().join("notes.txt"),
            serde_json::to_vec(&stringy).unwrap(),
        )
        .unwrap();
        let capturing = Arc::new(Capturing::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", capturing.clone());
        w.tick();
        assert_eq!(capturing.calls.lock().len(), 1);
    }

    /// gh #332: on this one-thread runtime the sleep can't end if the pass is
    /// waiting on the worker, so the fencer gives up after 5 s and says so.
    #[tokio::test]
    async fn applying_a_fence_stays_off_the_workers() {
        struct Stalled {
            reached: std::sync::atomic::AtomicBool,
            release: parking_lot::Mutex<std::sync::mpsc::Receiver<()>>,
            parked: std::sync::atomic::AtomicBool,
        }
        impl ProducerEpochFencer for Stalled {
            fn fence_producer_epoch(&self, _: i64, _: i16) {
                self.reached
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                let released = self.release.lock().recv_timeout(Duration::from_secs(5));
                if released.is_err() {
                    self.parked.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        write_peer(tmp.path(), "from-kaas-1.json", &HashMap::from([(42, 3)]));
        let (release, released) = std::sync::mpsc::channel();
        let fencer = Arc::new(Stalled {
            reached: Default::default(),
            release: parking_lot::Mutex::new(released),
            parked: Default::default(),
        });
        let w = Arc::new(FenceWatcher::new(
            tmp.path().to_path_buf(),
            "kaas-0",
            fencer.clone(),
        ));
        let cancel = CancellationToken::new();
        let task = tokio::spawn(w.run(cancel.clone()));

        tokio::time::sleep(Duration::from_millis(100)).await;
        let reached = fencer.reached.load(std::sync::atomic::Ordering::SeqCst);
        let _ = release.send(());
        cancel.cancel();
        task.await.unwrap();

        assert!(
            !fencer.parked.load(std::sync::atomic::Ordering::SeqCst),
            "applying a fence parked the scheduler worker"
        );
        assert!(reached, "the fence never got applied: proves nothing");
    }

    /// A pass that dies partway retries what it didn't apply. They were recorded
    /// before being applied, so the rest of that pass was lost for good, while the
    /// log said it would retry.
    #[test]
    #[allow(clippy::panic)]
    fn a_pass_that_dies_retries_what_it_did_not_apply() {
        #[derive(Default)]
        struct PanicsOnce {
            panicked: std::sync::atomic::AtomicBool,
            calls: parking_lot::Mutex<Vec<(i64, i16)>>,
        }
        impl ProducerEpochFencer for PanicsOnce {
            fn fence_producer_epoch(&self, pid: i64, epoch: i16) {
                if !self
                    .panicked
                    .swap(true, std::sync::atomic::Ordering::SeqCst)
                {
                    panic!("the first fence fails");
                }
                self.calls.lock().push((pid, epoch));
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        write_peer(
            tmp.path(),
            "from-kaas-1.json",
            &HashMap::from([(1, 1), (2, 1), (3, 1)]),
        );
        let fencer = Arc::new(PanicsOnce::default());
        let w = FenceWatcher::new(tmp.path().to_path_buf(), "kaas-0", fencer.clone());

        let died = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| w.tick()));
        assert!(died.is_err());
        w.tick();

        let mut calls = fencer.calls.lock().clone();
        calls.sort();
        assert_eq!(calls, vec![(1, 1), (2, 1), (3, 1)], "a fence was lost");

        // Applied now, so another pass does nothing.
        w.tick();
        assert_eq!(fencer.calls.lock().len(), 3);
    }
}
