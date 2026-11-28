//! Phase 6 gh #175 cross-broker COMMIT / ABORT marker broadcast.
//!
//! End-to-end check that an `EndTxn` on broker A's txn coordinator
//! results in a control batch landing in broker B's partition log
//! via the shared `marker_queue/` directory on the data dir.
//! Unit tests cover each side independently — `MarkerQueue::enqueue`
//! (kaas-coordinator) and `MarkerWatcher::tick` (kaas-broker). This
//! file wires them together against one shared tempdir to confirm
//! the on-disk shape, file naming, and idempotency match across
//! the crate boundary.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;

use async_trait::async_trait;
use kaas_broker::{ApplyOutcome, MarkerApplier, MarkerWatcher};
use kaas_coordinator::{MarkerEntry, MarkerQueue, TxnTopic};

#[derive(Default)]
struct Capturing {
    calls: parking_lot::Mutex<Vec<MarkerEntry>>,
}

impl Capturing {
    fn drain(&self) -> Vec<MarkerEntry> {
        std::mem::take(&mut *self.calls.lock())
    }
}

#[async_trait]
impl MarkerApplier for Capturing {
    async fn apply(&self, entry: &MarkerEntry) -> ApplyOutcome {
        self.calls.lock().push(entry.clone());
        ApplyOutcome::done()
    }
}

fn build_watcher(queue: &MarkerQueue, self_id: &str) -> (Arc<MarkerWatcher>, Arc<Capturing>) {
    let capturing = Arc::new(Capturing::default());
    let watcher = Arc::new(MarkerWatcher::new(
        queue.clone(),
        self_id,
        capturing.clone(),
    ));
    (watcher, capturing)
}

fn entry(pid: i64, commit: bool, topic: &str, partitions: Vec<i32>) -> MarkerEntry {
    MarkerEntry {
        transactional_id: format!("tx-{pid}"),
        producer_id: pid,
        producer_epoch: 0,
        commit,
        coordinator_epoch: 0,
        partitions: vec![TxnTopic {
            topic: topic.to_owned(),
            partitions,
        }],
    }
}

/// A enqueues, B applies, the file disappears.
#[tokio::test]
async fn end_to_end_cross_broker_dispatch() {
    let tmp = tempfile::tempdir().unwrap();
    let queue = MarkerQueue::open(tmp.path()).unwrap();
    let (watcher_b, applied_b) = build_watcher(&queue, "kaas-b");

    // Txn coordinator on A enqueues a commit marker targeted at B
    // because B leads the participating partition.
    queue
        .enqueue("kaas-b", &entry(42, true, "t", vec![0, 1, 2]))
        .unwrap();
    assert_eq!(queue.list("kaas-b").unwrap().len(), 1);

    watcher_b.tick().await;
    let got = applied_b.drain();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].producer_id, 42);
    assert!(got[0].commit);
    assert!(
        queue.list("kaas-b").unwrap().is_empty(),
        "applied file must be deleted"
    );
}

/// Watcher reads only its own inbox — files targeted at the other
/// broker stay put.
#[tokio::test]
async fn inbox_routing_keeps_per_target_isolation() {
    let tmp = tempfile::tempdir().unwrap();
    let queue = MarkerQueue::open(tmp.path()).unwrap();
    let (watcher_b, applied_b) = build_watcher(&queue, "kaas-b");
    let (watcher_c, applied_c) = build_watcher(&queue, "kaas-c");

    queue
        .enqueue("kaas-b", &entry(1, true, "t", vec![0]))
        .unwrap();
    queue
        .enqueue("kaas-c", &entry(2, false, "t", vec![1]))
        .unwrap();

    watcher_b.tick().await;
    assert_eq!(applied_b.drain().len(), 1);
    assert!(
        applied_c.drain().is_empty(),
        "B's tick must not touch C's inbox"
    );

    watcher_c.tick().await;
    let got = applied_c.drain();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].producer_id, 2);
    assert!(!got[0].commit);
}

/// Producer retried `EndTxn`, txn coord re-enqueued under the same
/// `<pid>-<epoch>` file name. The receiver applies it twice (once
/// per visible file).
#[tokio::test]
async fn retry_after_apply_reapplies_idempotent_at_consumer_level() {
    let tmp = tempfile::tempdir().unwrap();
    let queue = MarkerQueue::open(tmp.path()).unwrap();
    let (watcher_b, applied_b) = build_watcher(&queue, "kaas-b");

    queue
        .enqueue("kaas-b", &entry(42, true, "t", vec![0]))
        .unwrap();
    watcher_b.tick().await;
    assert_eq!(applied_b.drain().len(), 1);

    // Producer's EndTxn retry: same (pid, epoch) → overwrites with
    // identical bytes (since the file was deleted, this is a fresh
    // create). Watcher applies it again.
    queue
        .enqueue("kaas-b", &entry(42, true, "t", vec![0]))
        .unwrap();
    watcher_b.tick().await;
    assert_eq!(applied_b.drain().len(), 1);
}

/// Multi-partition marker — one entry with a list of partitions,
/// applier sees it as one call.
#[tokio::test]
async fn multi_partition_in_single_marker() {
    let tmp = tempfile::tempdir().unwrap();
    let queue = MarkerQueue::open(tmp.path()).unwrap();
    let (watcher_b, applied_b) = build_watcher(&queue, "kaas-b");

    queue
        .enqueue(
            "kaas-b",
            &MarkerEntry {
                transactional_id: "tx-1".to_owned(),
                producer_id: 7,
                producer_epoch: 2,
                commit: true,
                coordinator_epoch: 0,
                partitions: vec![
                    TxnTopic {
                        topic: "t".to_owned(),
                        partitions: vec![0, 1, 2],
                    },
                    TxnTopic {
                        topic: "u".to_owned(),
                        partitions: vec![5],
                    },
                ],
            },
        )
        .unwrap();

    watcher_b.tick().await;
    let got = applied_b.drain();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].partitions.len(), 2);
    assert_eq!(got[0].partitions[0].partitions, vec![0, 1, 2]);
}

/// MarkerQueue.list and what MarkerWatcher reads must agree on
/// shape — sanity check across the crate boundary without going
/// through tick().
#[tokio::test]
async fn list_matches_watcher_view() {
    let tmp = tempfile::tempdir().unwrap();
    let queue = MarkerQueue::open(tmp.path()).unwrap();
    queue
        .enqueue("kaas-b", &entry(42, true, "t", vec![0]))
        .unwrap();
    queue
        .enqueue("kaas-b", &entry(99, false, "t", vec![1]))
        .unwrap();

    let listed = queue.list("kaas-b").unwrap();
    assert_eq!(listed.len(), 2);
    let listed_pids: std::collections::HashSet<i64> =
        listed.iter().map(|(_, e)| e.producer_id).collect();

    let (watcher_b, applied_b) = build_watcher(&queue, "kaas-b");
    watcher_b.tick().await;
    let applied_pids: std::collections::HashSet<i64> = applied_b
        .drain()
        .into_iter()
        .map(|e| e.producer_id)
        .collect();

    assert_eq!(listed_pids, applied_pids);
}
