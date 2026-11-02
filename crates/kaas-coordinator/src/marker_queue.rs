//! Cross-broker COMMIT / ABORT marker dispatch via the shared PVC.
//!
//! Kaas's answer to Apache's `WriteTxnMarkers` RPC (gh #175). Where
//! Apache uses a Kafka-wire RPC from the txn coordinator broker to
//! each partition's leader, kaas writes one JSON file per
//! `(pid, epoch, target_broker)` under
//! `<data_dir>/__cluster/marker_queue/to-<target>/`. Every broker
//! polls its own `to-<self>/` directory via `MarkerWatcher` and
//! applies the markers as control-batch appends — same pattern as
//! `FenceLog` / `FenceWatcher`.
//!
//! Idempotency is layered:
//!
//! 1. File name is `<pid>-<epoch>.json` so a producer's `EndTxn`
//!    retry overwrites the prior entry with identical content.
//! 2. The receiver deletes the file after successful application.
//! 3. Duplicate application (file replayed after restart) just
//!    appends an extra control batch to the partition — wasteful,
//!    not incorrect (consumers don't act on duplicate markers).
//!
//! Cross-broker latency is bounded by the watcher's poll interval
//! (2 s default). `commitTransaction()` on a Java producer waits
//! seconds anyway; kaas returns success from `EndTxn` as soon as
//! the queue entry is written, not when the marker lands.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::TxnTopic;

/// Conventional directory inside `<data_dir>/__cluster/` where all
/// brokers publish marker queue entries.
pub const MARKER_QUEUE_DIR_NAME: &str = "marker_queue";

/// One enqueued marker dispatch — the unit a receiver applies.
/// JSON shape is broker-version-portable; new fields must default
/// to a sensible no-op so an older receiver doesn't choke on a
/// newer writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarkerEntry {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// `true` for COMMIT, `false` for ABORT.
    pub commit: bool,
    pub coordinator_epoch: i32,
    pub partitions: Vec<TxnTopic>,
}

/// Per-cluster marker queue. Cheap to construct — opens no files
/// until `enqueue` is called.
#[derive(Debug, Clone)]
pub struct MarkerQueue {
    root: PathBuf,
}

impl MarkerQueue {
    /// Open (creating-on-demand) the marker-queue root under
    /// `cluster_dir/marker_queue/`. The per-target subdirectories
    /// are created lazily on `enqueue`.
    pub fn open(cluster_dir: &Path) -> io::Result<Self> {
        let root = cluster_dir.join(MARKER_QUEUE_DIR_NAME);
        crate::atomic_write::create_dir_all_durable(&root)?;
        Ok(Self { root })
    }

    /// Conventional directory the receiver polls.
    pub fn inbox(&self, broker_id: &str) -> PathBuf {
        self.root.join(format!("to-{broker_id}"))
    }

    /// Atomically write an entry into `to-<target_broker>/`. File
    /// name is `<pid>-<epoch>.json` so a retry from the same
    /// producer overwrites the prior content rather than piling up.
    pub fn enqueue(&self, target_broker: &str, entry: &MarkerEntry) -> io::Result<()> {
        if target_broker.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "marker queue: empty target broker",
            ));
        }
        let inbox = self.inbox(target_broker);
        crate::atomic_write::create_dir_all_durable(&inbox)?;
        let name = format!("{}-{}.json", entry.producer_id, entry.producer_epoch);
        atomic_write_json(&inbox, &name, entry)?;
        Ok(())
    }

    /// Snapshot the pending entries in a target broker's inbox.
    /// Tests use this to inspect the on-disk shape without a
    /// `MarkerWatcher`. Returns `Vec<(file_name, entry)>`.
    pub fn list(&self, target_broker: &str) -> io::Result<Vec<(String, MarkerEntry)>> {
        let inbox = self.inbox(target_broker);
        let entries = match fs::read_dir(&inbox) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut out = Vec::new();
        for e in entries.flatten() {
            let name = match e.file_name().into_string() {
                Ok(n) => n,
                Err(_) => continue,
            };
            if !name.ends_with(".json") {
                continue;
            }
            let data = fs::read(e.path())?;
            if data.is_empty() {
                continue;
            }
            let entry: MarkerEntry = serde_json::from_slice(&data).map_err(io::Error::other)?;
            out.push((name, entry));
        }
        Ok(out)
    }
}

/// Conventional helper for the dispatcher side.
pub fn marker_queue_dir(cluster_dir: &Path) -> PathBuf {
    cluster_dir.join(MARKER_QUEUE_DIR_NAME)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_entry() -> MarkerEntry {
        MarkerEntry {
            transactional_id: "tx-1".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            commit: true,
            coordinator_epoch: 7,
            partitions: vec![TxnTopic {
                topic: "t".to_owned(),
                partitions: vec![0, 1, 2],
            }],
        }
    }

    #[test]
    fn enqueue_then_list_roundtrips() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-1", &sample_entry()).unwrap();
        let pending = q.list("kaas-1").unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, "42-3.json");
        assert_eq!(pending[0].1, sample_entry());
    }

    #[test]
    fn enqueue_is_idempotent_on_same_pid_epoch() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-1", &sample_entry()).unwrap();
        // Second enqueue with the same (pid, epoch) overwrites —
        // not a second file.
        q.enqueue("kaas-1", &sample_entry()).unwrap();
        assert_eq!(q.list("kaas-1").unwrap().len(), 1);
    }

    #[test]
    fn different_targets_get_separate_inboxes() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        q.enqueue("kaas-1", &sample_entry()).unwrap();
        q.enqueue("kaas-2", &sample_entry()).unwrap();
        assert_eq!(q.list("kaas-1").unwrap().len(), 1);
        assert_eq!(q.list("kaas-2").unwrap().len(), 1);
    }

    #[test]
    fn empty_target_broker_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let err = q.enqueue("", &sample_entry()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn list_on_missing_inbox_is_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        assert!(q.list("kaas-nobody").unwrap().is_empty());
    }

    #[test]
    fn inbox_path_uses_to_prefix() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        let inbox = q.inbox("kaas-3");
        assert_eq!(inbox.file_name().unwrap(), "to-kaas-3");
    }

    #[test]
    fn marker_queue_dir_under_cluster() {
        let cluster = Path::new("/data/__cluster");
        assert_eq!(
            marker_queue_dir(cluster),
            Path::new("/data/__cluster/marker_queue")
        );
    }
}
