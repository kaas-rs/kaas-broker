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
//! An entry is owed to its partitions, not to this broker (gh #316).
//! `EndTxn` has already answered by the time it's read, so nothing
//! upstream will send it again. A partition that moved since the
//! coordinator wrote it is forwarded to the inbox of the broker that
//! leads it now, and one that nobody leads yet stays in the file for the
//! next tick. Only a partition whose topic is gone is dropped. Apache's
//! marker channel does the same: it looks the leader up again and resends.
//!
//! The watcher claims each file before it reads it, by renaming it to a
//! name only it writes (see `kaas_coordinator::marker_queue`). The
//! coordinator writes `<pid>-<epoch>.json` again for the producer's next
//! transaction, so without the claim a delete after the apply could take
//! that next entry with it, and rewriting a partly applied file could
//! overwrite it.
//!
//! Idempotency: the file is deleted after successful application.
//! A duplicate application (file replayed after a crash) just
//! appends an extra control batch — wasteful, not incorrect
//! (control batches don't carry sequence numbers and consumers
//! don't react to duplicate markers).
//!
//! The [`MarkerApplier`] seam exists for tests; production wires
//! [`crate::txn_markers::BrokerMarkerApplier`].

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kaas_coordinator::{
    claimed_name, forwarded_name, is_claimed, MarkerEntry, MarkerQueue, TxnTopic,
};
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

/// Default poll cadence — matches `FenceWatcher::DEFAULT_POLL`.
pub const DEFAULT_POLL: Duration = Duration::from_secs(2);

/// Applies a [`MarkerEntry`] to local storage, and says what's left of it.
#[async_trait]
pub trait MarkerApplier: Send + Sync + 'static {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome;
}

/// What became of an entry's partitions. A partition in neither list is
/// done: its marker is durable here, or its topic is gone. The watcher
/// deletes the file once both lists are empty.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ApplyOutcome {
    /// Partitions another broker leads now, by its id. The watcher writes
    /// them to that broker's inbox.
    pub forward: Vec<(String, Vec<TxnTopic>)>,
    /// Partitions to try again next tick, kept in the file.
    pub keep: Vec<TxnTopic>,
}

impl ApplyOutcome {
    /// Every partition is done.
    pub fn done() -> Self {
        Self::default()
    }

    /// Nothing is done: try every partition again next tick.
    pub fn retry(entry: &MarkerEntry) -> Self {
        Self {
            forward: Vec::new(),
            keep: entry.partitions.clone(),
        }
    }
}

/// Polls the per-broker marker inbox and dispatches each entry into
/// the wired [`MarkerApplier`].
pub struct MarkerWatcher {
    queue: MarkerQueue,
    self_id: String,
    inbox: PathBuf,
    applier: Arc<dyn MarkerApplier>,
    poll: Duration,
    /// Makes claimed names unique within one process.
    claims: AtomicU64,
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
    /// Watch `self_id`'s inbox in `queue`.
    pub fn new(
        queue: MarkerQueue,
        self_id: impl Into<String>,
        applier: Arc<dyn MarkerApplier>,
    ) -> Self {
        let self_id = self_id.into();
        Self {
            inbox: queue.inbox(&self_id),
            queue,
            self_id,
            applier,
            poll: DEFAULT_POLL,
            claims: AtomicU64::new(0),
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
    /// can drive the watcher deterministically. Its file work is `tokio::fs`
    /// or the blocking pool, so it stays off the scheduler workers (gh #332).
    pub async fn tick(&self) {
        let mut entries = match tokio::fs::read_dir(&self.inbox).await {
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
        let mut names: Vec<String> = Vec::new();
        while let Ok(Some(e)) = entries.next_entry().await {
            let Ok(name) = e.file_name().into_string() else {
                continue;
            };
            if name.ends_with(".json") {
                names.push(name);
            }
        }

        for name in names {
            let claimed = if is_claimed(&name) {
                Some(name)
            } else {
                self.claim(&name).await
            };
            if let Some(claimed) = claimed {
                self.process_one(&claimed).await;
            }
        }
    }

    /// Rename `name` to a claimed name nothing else in the inbox has. `None`
    /// when it can't be claimed this tick.
    async fn claim(&self, name: &str) -> Option<String> {
        let from = self.inbox.join(name);
        loop {
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or_default();
            let seq = self.claims.fetch_add(1, Ordering::Relaxed) & 0xf_ffff;
            let claimed = claimed_name(name, (nanos << 20) | u128::from(seq));
            let to = self.inbox.join(&claimed);
            // Only this watcher writes claimed names, so checking first
            // can't race: a rename would replace a file already there.
            match tokio::fs::try_exists(&to).await {
                Ok(false) => {}
                Ok(true) => continue,
                Err(e) => {
                    tracing::warn!(file = %to.display(), %e, "marker watcher: can't check a claim; will retry next tick");
                    return None;
                }
            }
            return match tokio::fs::rename(&from, &to).await {
                Ok(()) => Some(claimed),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(e) => {
                    tracing::warn!(
                        file = %from.display(),
                        %e,
                        "marker watcher: claiming a queue entry failed; will retry next tick"
                    );
                    None
                }
            };
        }
    }

    async fn process_one(&self, name: &str) {
        let path = self.inbox.join(name);
        let data = match tokio::fs::read(&path).await {
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
                match tokio::fs::rename(&path, &aside).await {
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
        let ApplyOutcome { forward, mut keep } = self.applier.apply(&entry).await;

        // Forward before the file shrinks, so a crash in between sends a
        // partition twice rather than not at all.
        for (target, partitions) in forward {
            if partitions.is_empty() {
                continue;
            }
            if target == self.self_id {
                keep.extend(partitions);
                continue;
            }
            let fwd = MarkerEntry {
                partitions: partitions.clone(),
                ..entry.clone()
            };
            let fwd_name = forwarded_name(&self.self_id, name, &partitions);
            let (queue, to) = (self.queue.clone(), target.clone());
            let written =
                tokio::task::spawn_blocking(move || queue.write_entry(&to, &fwd_name, &fwd))
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::other(e)));
            match written {
                Ok(()) => tracing::info!(
                    target = %target,
                    pid = entry.producer_id,
                    ?partitions,
                    "marker watcher: forwarded markers for partitions that moved to their \
                     leader"
                ),
                Err(e) => {
                    tracing::warn!(
                        target = %target,
                        %e,
                        "marker watcher: forwarding markers failed; keeping them for the \
                         next tick"
                    );
                    keep.extend(partitions);
                }
            }
        }

        if keep.is_empty() {
            if let Err(e) = tokio::fs::remove_file(&path).await {
                tracing::warn!(
                    file = %path.display(),
                    %e,
                    "marker watcher: post-apply delete failed; \
                     next tick will re-apply (idempotent)"
                );
            }
        } else if keep != entry.partitions {
            // Only what's still owed, so the next tick doesn't append a second
            // marker to the partitions that are done.
            let rest = MarkerEntry {
                partitions: keep,
                ..entry
            };
            let (queue, me, file) = (self.queue.clone(), self.self_id.clone(), name.to_owned());
            let rewritten =
                tokio::task::spawn_blocking(move || queue.write_entry(&me, &file, &rest))
                    .await
                    .unwrap_or_else(|e| Err(std::io::Error::other(e)));
            if let Err(e) = rewritten {
                tracing::warn!(
                    file = %path.display(),
                    %e,
                    "marker watcher: can't drop the applied partitions from a queue entry; \
                     the next tick appends their markers again"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex;
    use std::fs;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Default)]
    struct Capturing {
        calls: Mutex<Vec<MarkerEntry>>,
        retry: AtomicBool,
    }

    impl Capturing {
        fn new(retry: bool) -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
                retry: AtomicBool::new(retry),
            })
        }
    }

    #[async_trait]
    impl MarkerApplier for Capturing {
        async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
            self.calls.lock().push(entry.clone());
            if self.retry.load(Ordering::SeqCst) {
                ApplyOutcome::retry(entry)
            } else {
                ApplyOutcome::done()
            }
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

    fn parts(partitions: Vec<i32>) -> Vec<TxnTopic> {
        vec![TxnTopic {
            topic: "t".to_owned(),
            partitions,
        }]
    }

    fn watcher(q: &MarkerQueue, applier: Arc<dyn MarkerApplier>) -> MarkerWatcher {
        MarkerWatcher::new(q.clone(), "kaas-0", applier)
    }

    #[tokio::test]
    async fn happy_path_applies_and_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        let capturing = Capturing::new(false);
        let w = watcher(&q, capturing.clone());
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
        let capturing = Capturing::new(true);
        let w = watcher(&q, capturing.clone());
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
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let capturing = Capturing::new(false);
        let w = MarkerWatcher::new(q, "nobody", capturing.clone());
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
        let capturing = Capturing::new(false);
        let w = watcher(&q, capturing.clone());
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
        let capturing = Capturing::new(false);
        let w = watcher(&q, capturing.clone());
        q.enqueue("kaas-0", &entry(7, 1)).unwrap();
        w.tick().await;
        assert_eq!(capturing.calls.lock().len(), 1, "the good entry applies");

        let names: Vec<String> = fs::read_dir(&inbox)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        for stem in ["garbage", "empty"] {
            assert!(
                names
                    .iter()
                    .any(|n| n.ends_with(&format!("-{stem}.json.bad"))),
                "{stem} set aside: {names:?}"
            );
        }
        assert_eq!(names.len(), 2, "nothing else is left: {names:?}");
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
                ApplyOutcome::done()
            }
        }
        let applier: Arc<dyn MarkerApplier> = Arc::new(Counter(ticks.clone()));
        let w = watcher(&q, applier);

        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        w.tick().await;
        assert_eq!(ticks.load(Ordering::SeqCst), 1);

        // Producer retried EndTxn; coord enqueued again.
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        w.tick().await;
        assert_eq!(ticks.load(Ordering::SeqCst), 2);
    }

    /// gh #316: the coordinator writes `<pid>-<epoch>.json` again for the
    /// producer's next transaction, and here it lands while the first one is
    /// being applied. The delete after that apply must not take it along.
    #[tokio::test]
    async fn a_coordinator_write_during_an_apply_is_not_deleted_with_it() {
        struct WritesNextTxn {
            queue: MarkerQueue,
            calls: Mutex<Vec<MarkerEntry>>,
        }
        #[async_trait]
        impl MarkerApplier for WritesNextTxn {
            async fn apply(&self, e: &MarkerEntry) -> ApplyOutcome {
                let first = {
                    let mut calls = self.calls.lock();
                    calls.push(e.clone());
                    calls.len() == 1
                };
                if first {
                    let next = MarkerEntry {
                        commit: false,
                        ..e.clone()
                    };
                    self.queue.enqueue("kaas-0", &next).unwrap();
                }
                ApplyOutcome::done()
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let applier = Arc::new(WritesNextTxn {
            queue: q.clone(),
            calls: Mutex::new(Vec::new()),
        });
        let w = watcher(&q, applier.clone());
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();

        w.tick().await;
        assert_eq!(q.list("kaas-0").unwrap().len(), 1, "the next txn's entry");
        w.tick().await;
        let commits: Vec<bool> = applier.calls.lock().iter().map(|e| e.commit).collect();
        assert_eq!(commits, [true, false], "both transactions' markers apply");
        assert!(q.list("kaas-0").unwrap().is_empty());
    }

    /// gh #316: a partition that moved goes to its new leader's inbox, one
    /// nobody leads stays in the file on its own, and the partition that was
    /// applied isn't applied again.
    #[tokio::test]
    async fn moved_partitions_are_forwarded_and_unled_ones_kept() {
        struct Splits(Mutex<Vec<MarkerEntry>>);
        #[async_trait]
        impl MarkerApplier for Splits {
            async fn apply(&self, e: &MarkerEntry) -> ApplyOutcome {
                self.0.lock().push(e.clone());
                if e.partitions == parts(vec![0, 1, 2]) {
                    ApplyOutcome {
                        forward: vec![("kaas-1".to_owned(), parts(vec![1]))],
                        keep: parts(vec![2]),
                    }
                } else {
                    ApplyOutcome::done()
                }
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let applier = Arc::new(Splits(Mutex::new(Vec::new())));
        let w = watcher(&q, applier.clone());
        let mut e = entry(42, 3);
        e.partitions = parts(vec![0, 1, 2]);
        q.enqueue("kaas-0", &e).unwrap();

        w.tick().await;
        let forwarded = q.list("kaas-1").unwrap();
        assert_eq!(forwarded.len(), 1);
        assert!(
            forwarded[0].0.starts_with("f-kaas-0-"),
            "{}",
            forwarded[0].0
        );
        assert_eq!(forwarded[0].1.partitions, parts(vec![1]));
        assert_eq!(forwarded[0].1.producer_id, 42);
        let kept = q.list("kaas-0").unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1.partitions, parts(vec![2]));

        w.tick().await;
        let seen: Vec<Vec<TxnTopic>> = applier
            .0
            .lock()
            .iter()
            .map(|e| e.partitions.clone())
            .collect();
        assert_eq!(seen, [parts(vec![0, 1, 2]), parts(vec![2])]);
        assert!(q.list("kaas-0").unwrap().is_empty());
        assert_eq!(q.list("kaas-1").unwrap().len(), 1, "kaas-1 hasn't ticked");
    }

    /// A forward that can't be written stays owed here, rather than being
    /// counted as sent.
    #[tokio::test]
    async fn a_failed_forward_keeps_its_partitions() {
        struct ToNowhere;
        #[async_trait]
        impl MarkerApplier for ToNowhere {
            async fn apply(&self, e: &MarkerEntry) -> ApplyOutcome {
                ApplyOutcome {
                    forward: vec![("no/such/broker".to_owned(), e.partitions.clone())],
                    keep: Vec::new(),
                }
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let w = watcher(&q, Arc::new(ToNowhere));
        q.enqueue("kaas-0", &entry(42, 3)).unwrap();
        w.tick().await;
        let kept = q.list("kaas-0").unwrap();
        assert_eq!(kept.len(), 1);
        assert_eq!(kept[0].1, entry(42, 3));
    }

    /// gh #332: a FIFO stands in for a slow volume: reading it blocks until
    /// something opens the other end. On this one-thread runtime the sleep can't
    /// end if the read is waiting on the worker.
    #[tokio::test]
    async fn a_slow_read_stays_off_the_workers() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let inbox = q.inbox("kaas-0");
        fs::create_dir_all(&inbox).unwrap();
        let made = std::process::Command::new("mkfifo")
            .arg(inbox.join("1-0.json"))
            .status()
            .unwrap();
        assert!(made.success());
        let body = serde_json::to_vec(&entry(1, 0)).unwrap();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let fifo_dir = inbox.clone();
        let writer = std::thread::spawn(move || {
            let parked = released.recv_timeout(Duration::from_secs(5)).is_err();
            // The watcher claimed it first, so it has its claimed name now.
            let fifo = fs::read_dir(&fifo_dir)
                .unwrap()
                .map(|e| e.unwrap().path())
                .find(|p| is_claimed(&p.file_name().unwrap().to_string_lossy()))
                .unwrap();
            fs::write(&fifo, body).unwrap();
            parked
        });
        let cap = Capturing::new(false);
        let w = watcher(&q, cap.clone());
        let task = tokio::spawn(async move { w.tick().await });

        tokio::time::sleep(Duration::from_millis(100)).await;
        let stalled = !task.is_finished();
        let _ = release.send(());
        task.await.unwrap();

        assert!(
            !writer.join().unwrap(),
            "the read parked the scheduler worker"
        );
        assert!(stalled, "the pass never reached the read: proves nothing");
        assert_eq!(cap.calls.lock().len(), 1, "the entry must still be applied");
    }
}
