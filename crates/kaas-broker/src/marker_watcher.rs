//! Inbound COMMIT / ABORT marker dispatcher.
//!
//! Polls `<data_dir>/__cluster/marker_queue/to-<self_broker_id>/`
//! every 2 s and applies each marker entry as a control-batch
//! append on the partitions this broker leads (gh #175).
//!
//! Kaas's answer to Apache's `WriteTxnMarkers` RPC. Where Apache
//! uses a Kafka-wire client from the txn coordinator to each peer
//! leader, kaas writes JSON files into a per-target inbox on the
//! shared PVC and the leader reads + applies them. No new transport
//! beyond what `FenceWatcher` already established.
//!
//! Idempotency: the file is deleted after successful application.
//! A duplicate application (file replayed after a crash) just
//! appends an extra control batch — wasteful, not incorrect
//! (control batches don't carry sequence numbers and consumers
//! don't react to duplicate markers).
//!
//! The [`MarkerApplier`] seam exists for tests; production wires the
//! storage-engine adapter that builds the control batch via
//! [`crate::control_batch::build_control_batch`] and appends with
//! `acks = -1`.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kaas_coordinator::MarkerEntry;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

/// Default poll cadence — matches `FenceWatcher::DEFAULT_POLL`.
pub const DEFAULT_POLL: Duration = Duration::from_secs(2);

/// Applies a [`MarkerEntry`] to local storage. The production impl
/// (in `bins/kaas/src/cluster.rs`) builds a control batch via
/// [`crate::control_batch::build_control_batch`] and appends it to
/// every `(topic, partition)` this broker leads.
///
/// Returns the file's fate:
/// - [`ApplyOutcome::Applied`] — every owned partition was written;
///   the watcher deletes the queue file.
/// - [`ApplyOutcome::Retry`] — applier wants the file kept (e.g.,
///   transient storage error). Watcher leaves it in place.
#[async_trait]
pub trait MarkerApplier: Send + Sync + 'static {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome;
}

/// What the watcher does with a queue file after handing it to the
/// applier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApplyOutcome {
    /// Delete the file — work done, idempotency layer relies on
    /// at-most-once deletion.
    #[default]
    Applied,
    /// Leave the file in place; the watcher will retry it on the
    /// next tick.
    Retry,
}

/// Polls the per-broker marker inbox and dispatches each entry into
/// the wired [`MarkerApplier`].
pub struct MarkerWatcher {
    inbox: PathBuf,
    applier: Arc<dyn MarkerApplier>,
    poll: Duration,
}

impl std::fmt::Debug for MarkerWatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarkerWatcher")
            .field("inbox", &self.inbox)
            .field("poll", &self.poll)
            .finish()
    }
}

impl MarkerWatcher {
    pub fn new(inbox: PathBuf, applier: Arc<dyn MarkerApplier>) -> Self {
        Self {
            inbox,
            applier,
            poll: DEFAULT_POLL,
        }
    }

    /// Override the poll interval. Test-only.
    pub fn with_poll(mut self, d: Duration) -> Self {
        self.poll = d;
        self
    }

    /// Drive the polling loop until `cancel` fires. Ticks once on
    /// entry so a pre-existing queue file (e.g. after restart)
    /// is applied without waiting a full interval.
    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        self.tick().await;
        let mut t = interval(self.poll);
        // Skip the immediate first tick `interval` always yields.
        t.tick().await;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return,
                _ = t.tick() => self.tick().await,
            }
        }
    }

    /// Single synchronous-style pass. Exposed so integration tests
    /// can drive the watcher deterministically.
    pub async fn tick(&self) {
        let entries = match fs::read_dir(&self.inbox) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(
                    inbox = %self.inbox.display(),
                    %e,
                    "marker watcher: scanning inbox failed; \
                     cross-broker markers will not propagate this cycle"
                );
                return;
            }
        };

        // Collect first so the read_dir handle drops before any
        // async applier work — avoids holding the directory iterator
        // across an await.
        let mut files: Vec<PathBuf> = Vec::new();
        for e in entries.flatten() {
            if !e.file_name().to_string_lossy().ends_with(".json") {
                continue;
            }
            files.push(e.path());
        }

        for path in files {
            self.process_one(&path).await;
        }
    }

    async fn process_one(&self, path: &std::path::Path) {
        let data = match fs::read(path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
            Err(e) => {
                tracing::warn!(
                    file = %path.display(),
                    %e,
                    "marker watcher: read failed; will retry next tick"
                );
                return;
            }
        };
        // The writer is tmp + fsync + rename, so a `.json` file here is complete.
        // One that doesn't decode never will, so it's set aside rather than
        // retried and logged every 2 s for ever (gh #320). The name is kept for
        // whoever looks into it, and it no longer matches the `.json` filter.
        let entry: MarkerEntry = match serde_json::from_slice(&data) {
            Ok(e) => e,
            Err(e) => {
                let mut aside = path.as_os_str().to_owned();
                aside.push(".bad");
                match fs::rename(path, &aside) {
                    Ok(()) => tracing::error!(
                        file = %path.display(),
                        %e,
                        "marker watcher: undecodable queue entry set aside as .bad; a \
                         transaction marker may be lost, and a partition's LSO with it"
                    ),
                    Err(rename) => tracing::warn!(
                        file = %path.display(),
                        %e,
                        %rename,
                        "marker watcher: undecodable queue entry can't be set aside; \
                         will retry next tick"
                    ),
                }
                return;
            }
        };
        match self.applier.apply(&entry).await {
            ApplyOutcome::Applied => {
                if let Err(e) = fs::remove_file(path) {
                    tracing::warn!(
                        file = %path.display(),
                        %e,
                        "marker watcher: post-apply delete failed; \
                         next tick will re-apply (idempotent)"
                    );
                }
            }
            ApplyOutcome::Retry => {
                // Leave the file; next tick picks it up.
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_coordinator::{MarkerQueue, TxnTopic};
    use parking_lot::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Capturing {
        calls: Mutex<Vec<MarkerEntry>>,
        outcome: Mutex<ApplyOutcome>,
    }

    impl Capturing {
        fn new(outcome: ApplyOutcome) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                outcome: Mutex::new(outcome),
            })
        }
    }

    #[async_trait]
    impl MarkerApplier for Capturing {
        async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
            self.calls.lock().push(entry.clone());
            *self.outcome.lock()
        }
    }

    fn entry(pid: i64, epoch: i16) -> MarkerEntry {
        MarkerEntry {
            transactional_id: format!("tx-{pid}"),
            producer_id: pid,
            producer_epoch: epoch,
            commit: true,
            coordinator_epoch: 0,
            partitions: vec![TxnTopic {
                topic: "t".to_owned(),
                partitions: vec![0],
            }],
        }
    }

    #[tokio::test]
    async fn happy_path_applies_and_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        let capturing = Capturing::new(ApplyOutcome::Applied);
        let w = MarkerWatcher::new(q.inbox("kaas-0"), capturing.clone());
        w.tick().await;

        let calls = capturing.calls.lock().clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].producer_id, 42);
        assert!(q.list("kaas-0").unwrap().is_empty(), "file must be deleted");
    }

    #[tokio::test]
    async fn retry_outcome_leaves_file_in_place() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        let capturing = Capturing::new(ApplyOutcome::Retry);
        let w = MarkerWatcher::new(q.inbox("kaas-0"), capturing.clone());
        w.tick().await;

        assert_eq!(capturing.calls.lock().len(), 1);
        // File should remain — applier asked for retry.
        assert_eq!(q.list("kaas-0").unwrap().len(), 1);

        // Second tick re-applies.
        w.tick().await;
        assert_eq!(capturing.calls.lock().len(), 2);
    }

    #[tokio::test]
    async fn missing_inbox_is_silent_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let inbox = tmp.path().join("does-not-exist");
        let capturing = Capturing::new(ApplyOutcome::Applied);
        let w = MarkerWatcher::new(inbox, capturing.clone());
        w.tick().await; // must not panic / error
        assert!(capturing.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn multiple_entries_one_tick() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-0", &entry(1, 0)).unwrap();
        q.enqueue("kaas-0", &entry(2, 0)).unwrap();
        q.enqueue("kaas-0", &entry(3, 0)).unwrap();
        let capturing = Capturing::new(ApplyOutcome::Applied);
        let w = MarkerWatcher::new(q.inbox("kaas-0"), capturing.clone());
        w.tick().await;
        assert_eq!(capturing.calls.lock().len(), 3);
        assert!(q.list("kaas-0").unwrap().is_empty());
    }

    /// gh #320: an undecodable entry is set aside rather than retried for
    /// ever, an empty one too, and it's kept. A good entry next to it still
    /// applies, and the next tick has nothing to retry.
    #[tokio::test]
    async fn an_undecodable_entry_is_set_aside_and_the_rest_still_apply() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let inbox = q.inbox("kaas-0");
        fs::create_dir_all(&inbox).unwrap();
        fs::write(inbox.join("garbage.json"), b"\xffnot-json").unwrap();
        fs::write(inbox.join("empty.json"), b"").unwrap();
        let capturing = Capturing::new(ApplyOutcome::Applied);
        let w = MarkerWatcher::new(inbox.clone(), capturing.clone());
        q.enqueue("kaas-0", &entry(7, 1)).unwrap();
        w.tick().await;
        assert_eq!(capturing.calls.lock().len(), 1, "the good entry applies");

        for name in ["garbage.json", "empty.json"] {
            assert!(!inbox.join(name).exists(), "{name}");
            assert!(inbox.join(format!("{name}.bad")).exists(), "{name}");
        }
        assert!(q.list("kaas-0").unwrap().is_empty(), "list skips them too");
        w.tick().await;
        assert_eq!(capturing.calls.lock().len(), 1);
    }

    /// Idempotency across ticks: enqueue + tick + re-enqueue with
    /// same (pid, epoch) + tick — applier sees the marker twice
    /// (both deletes happen). Wasteful but correct.
    #[tokio::test]
    async fn duplicate_enqueue_after_apply_reapplies() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let ticks = Arc::new(AtomicUsize::new(0));
        struct Counter(Arc<AtomicUsize>);
        #[async_trait]
        impl MarkerApplier for Counter {
            async fn apply(&self, _e: &MarkerEntry) -> ApplyOutcome {
                self.0.fetch_add(1, Ordering::SeqCst);
                ApplyOutcome::Applied
            }
        }
        let applier: Arc<dyn MarkerApplier> = Arc::new(Counter(ticks.clone()));
        let w = MarkerWatcher::new(q.inbox("kaas-0"), applier);

        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        w.tick().await;
        assert_eq!(ticks.load(Ordering::SeqCst), 1);

        // Producer retried EndTxn; coord enqueued again.
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        w.tick().await;
        assert_eq!(ticks.load(Ordering::SeqCst), 2);
    }
}
