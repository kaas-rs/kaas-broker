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
//!
//! # File names (gh #316)
//!
//! An inbox has more than one writer, so names say who wrote what:
//!
//! - `<pid>-<epoch>.json`: written by the transaction's coordinator.
//! - `c-<unique>-<stem>.json`: a file the inbox's own broker has claimed,
//!   by renaming it, before it reads it. Only that broker writes these
//!   names, so it can rewrite one with the partitions still owed, and a
//!   coordinator writing `<pid>-<epoch>.json` again can't be overwritten
//!   or deleted along with it.
//! - `f-<from>-<hash>.json`: partitions `<from>` found in its inbox and
//!   forwarded here because this broker leads them now. The hash covers the
//!   claimed file it came from and the partitions, so forwarding the same
//!   thing again after a crash rewrites the same file.

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
        let name = format!("{}-{}.json", entry.producer_id, entry.producer_epoch);
        self.write_entry(target_broker, &name, entry)
    }

    /// Atomically write `entry` as `name` in `to-<target_broker>/`, replacing
    /// a file of that name. See the module docs for who may write which name.
    pub fn write_entry(
        &self,
        target_broker: &str,
        name: &str,
        entry: &MarkerEntry,
    ) -> io::Result<()> {
        if target_broker.is_empty() || target_broker.contains('/') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("marker queue: bad target broker {target_broker:?}"),
            ));
        }
        if name.contains('/') || name.starts_with('.') || !name.ends_with(".json") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("marker queue: bad entry name {name:?}"),
            ));
        }
        let inbox = self.inbox(target_broker);
        crate::atomic_write::create_dir_all_durable(&inbox)?;
        atomic_write_json(&inbox, name, entry)
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

const CLAIMED_PREFIX: &str = "c-";

/// Whether `name` is a file an inbox's broker has already claimed.
pub fn is_claimed(name: &str) -> bool {
    name.starts_with(CLAIMED_PREFIX)
}

/// The name a broker renames `name` to when it claims it. `unique` must not
/// repeat in that inbox.
pub fn claimed_name(name: &str, unique: u128) -> String {
    let stem = name.strip_suffix(".json").unwrap_or(name);
    format!("{CLAIMED_PREFIX}{unique:x}-{stem}.json")
}

/// The name `from` gives the `partitions` of its claimed file `claimed` that
/// it forwards to another broker's inbox.
pub fn forwarded_name(from: &str, claimed: &str, partitions: &[TxnTopic]) -> String {
    let mut h = Fnv64::default();
    h.write(from.as_bytes());
    h.write(&[0]);
    h.write(claimed.as_bytes());
    for t in partitions {
        h.write(&[0]);
        h.write(t.topic.as_bytes());
        for p in &t.partitions {
            h.write(&p.to_be_bytes());
        }
    }
    format!("f-{from}-{:016x}.json", h.0)
}

/// FNV-1a, 64-bit: stable across builds, unlike `DefaultHasher`.
struct Fnv64(u64);

impl Default for Fnv64 {
    fn default() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }
}

impl Fnv64 {
    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }
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
    fn an_entry_name_must_stay_a_json_file_in_its_inbox() {
        let tmp = tempfile::tempdir().unwrap();
        let q = MarkerQueue::open(tmp.path()).unwrap();
        for (target, name) in [
            ("kaas-1", "../x.json"),
            ("kaas-1", ".hidden.json"),
            ("kaas-1", "x.json.tmp"),
            ("../kaas-1", "x.json"),
        ] {
            let err = q.write_entry(target, name, &sample_entry()).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput, "{target} {name}");
        }
        q.write_entry("kaas-1", "f-kaas-0-1.json", &sample_entry())
            .unwrap();
        assert_eq!(q.list("kaas-1").unwrap()[0].0, "f-kaas-0-1.json");
    }

    /// gh #316: claimed and forwarded names never collide with what a
    /// coordinator writes, and a forward of the same partitions from the same
    /// claimed file is the same file.
    #[test]
    fn claimed_and_forwarded_names() {
        let claimed = claimed_name("42-3.json", 0xabc);
        assert_eq!(claimed, "c-abc-42-3.json");
        assert!(is_claimed(&claimed));
        assert!(!is_claimed("42-3.json"));
        assert!(!is_claimed(&forwarded_name("kaas-0", &claimed, &[])));

        let parts = |p: Vec<i32>| {
            vec![TxnTopic {
                topic: "t".to_owned(),
                partitions: p,
            }]
        };
        let a = forwarded_name("kaas-0", &claimed, &parts(vec![0]));
        assert_eq!(a, forwarded_name("kaas-0", &claimed, &parts(vec![0])));
        assert!(a.starts_with("f-kaas-0-") && a.ends_with(".json"));
        for other in [
            forwarded_name("kaas-0", &claimed, &parts(vec![1])),
            forwarded_name("kaas-0", "c-abd-42-3.json", &parts(vec![0])),
            forwarded_name("kaas-2", &claimed, &parts(vec![0])),
        ] {
            assert_ne!(a, other);
        }
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
