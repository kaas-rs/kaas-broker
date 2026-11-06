//! gh #335: a commit in flight during a topic delete can't leave an offset behind.
//!
//! A commit checks the topic is known, then waits for the group's write lock, and a
//! delete can land between the two: the purge visits the group and moves on, and the
//! commit then writes an offset for a topic that's gone. If the topic is recreated,
//! the group inherits it and can resume past the end of the new log, with no error on
//! either side (the gh #240 symptom).

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use kaas_broker::{Broker, TopicMeta, TopicRegistry};
use kaas_coordinator::offset_store::FenceStamp;
use kaas_coordinator::{
    offset_key, BrokerEndpoint, BrokerLookup, FetchSpec, FnLookup, LocalGroupSource,
    LocalTxnSource, Manager, OffsetStore,
};
use kaas_storage::{MemoryStorage, StorageEngine};
use parking_lot::{Condvar, Mutex};

const GROUP: &str = "g";
const TOPIC: &str = "t";
const OTHER: &str = "u";
const PID: i64 = 42;

struct Cluster {
    _tmp: tempfile::TempDir,
    broker: Arc<Broker>,
    mgr: Arc<Manager>,
}

fn cluster() -> Cluster {
    let tmp = tempfile::tempdir().unwrap();
    let topics = Arc::new(TopicRegistry::new());
    for name in [TOPIC, OTHER] {
        topics.insert(TopicMeta {
            name: name.to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
    }
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    let broker = Arc::new(Broker::new(engine, topics, "test", 0));
    let lookup: Arc<dyn BrokerLookup> = Arc::new(FnLookup::new(|id: &str| {
        (id == "kaas-0").then(|| BrokerEndpoint {
            node_id: 0,
            host: "kaas-0.local".to_owned(),
            port: 9092,
        })
    }));
    let mgr = Manager::new(
        "kaas-0",
        Arc::new(OffsetStore::new(tmp.path())),
        lookup,
        LocalGroupSource::new("kaas-0"),
    );
    mgr.set_txn_assignment_source(LocalTxnSource::new("kaas-0"));
    broker.install_coord_manager(mgr.clone());
    Cluster {
        _tmp: tmp,
        broker,
        mgr,
    }
}

fn one(topic: &str, offset: i64) -> HashMap<String, i64> {
    HashMap::from([(offset_key(topic, 0), offset)])
}

fn committed(c: &Cluster, topic: &str) -> i64 {
    let spec = FetchSpec {
        topic: topic.to_owned(),
        partitions: vec![0],
    };
    let (offsets, _) = c
        .mgr
        .offset_fetch(GROUP, std::slice::from_ref(&spec))
        .expect("this broker coordinates the group");
    offsets[&offset_key(topic, 0)]
}

/// Stops writes to the group inside its write lock until released, as a slow fsync
/// would, with a first commit in that state. Returns the release and that commit.
async fn hold_the_group_lock(c: &Cluster) -> (impl FnOnce(), std::thread::JoinHandle<i16>) {
    let (reached, reached_rx) = mpsc::channel();
    let reached = Mutex::new(reached);
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let stop = gate.clone();
    c.mgr.offsets.set_fence_source(move || {
        let _ = reached.lock().send(());
        let mut open = stop.0.lock();
        while !*open {
            let timed_out = stop
                .1
                .wait_for(&mut open, Duration::from_secs(10))
                .timed_out();
            if timed_out {
                break;
            }
        }
        FenceStamp::default()
    });
    let mgr = c.mgr.clone();
    let holder =
        std::thread::spawn(move || mgr.offset_commit(GROUP, one(TOPIC, 2), HashMap::new()));
    reached_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    let release = move || {
        *gate.0.lock() = true;
        gate.1.notify_all();
    };
    (release, holder)
}

/// Queues a purge of `TOPIC` for the group's write lock, and waits long enough for
/// it to be first in line.
async fn queue_the_purge(c: &Cluster) -> (Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let mgr = c.mgr.clone();
    let done = Arc::new(AtomicBool::new(false));
    let finished = done.clone();
    let purge = std::thread::spawn(move || {
        mgr.purge_topic_offsets(TOPIC);
        finished.store(true, Ordering::SeqCst);
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    (done, purge)
}

/// The order that matters, forced: a commit that has already passed the known-topic
/// check is queued for the group's write lock behind the purge. Before the fix it
/// wrote the offset after the purge had moved on.
#[tokio::test(flavor = "multi_thread")]
async fn a_commit_queued_behind_the_purge_leaves_nothing_behind() {
    let c = cluster();
    assert_eq!(c.mgr.offset_commit(GROUP, one(TOPIC, 1), HashMap::new()), 0);
    assert_eq!(c.mgr.offset_commit(GROUP, one(OTHER, 5), HashMap::new()), 0);
    let (release, holder) = hold_the_group_lock(&c).await;

    // The delete: gone from the registry, then the purge.
    c.broker.topics.remove(TOPIC);
    let (purged, purge) = queue_the_purge(&c).await;

    // The commit in flight: it passed the check before the delete, which is why this
    // goes to the Manager, past the handler's check. It queues behind the purge.
    let mgr = c.mgr.clone();
    let late = std::thread::spawn(move || {
        let metadata = HashMap::from([(offset_key(TOPIC, 0), "late".to_owned())]);
        mgr.offset_commit(GROUP, one(TOPIC, 9), metadata)
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!purged.load(Ordering::SeqCst), "the purge never queued");

    release();
    holder.join().unwrap();
    purge.join().unwrap();
    late.join().unwrap();

    assert_eq!(
        committed(&c, TOPIC),
        -1,
        "an offset outlived the topic's delete"
    );
    assert_eq!(
        committed(&c, OTHER),
        5,
        "the group's other topic must survive"
    );
    let spec = FetchSpec {
        topic: TOPIC.to_owned(),
        partitions: vec![0],
    };
    let (_, metadata) = c.mgr.offset_fetch(GROUP, &[spec]).unwrap();
    assert!(
        metadata.is_empty(),
        "its metadata outlived it: {metadata:?}"
    );
}

/// The transactional twin. A commit of staged offsets has to read them under the
/// lock: read before it, they'd be written after the purge dropped them. The topic
/// stays in the registry here, as it would once a recreate is applied.
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_commit_queued_behind_the_purge_writes_nothing() {
    let c = cluster();
    c.mgr.offsets.store_pending(GROUP, PID, one(TOPIC, 7));
    let (release, holder) = hold_the_group_lock(&c).await;
    let (purged, purge) = queue_the_purge(&c).await;

    let mgr = c.mgr.clone();
    let late = std::thread::spawn(move || mgr.offsets.commit_pending(GROUP, PID));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!purged.load(Ordering::SeqCst), "the purge never queued");

    release();
    holder.join().unwrap();
    purge.join().unwrap();
    late.join().unwrap().unwrap();

    assert_eq!(
        committed(&c, TOPIC),
        -1,
        "a staged offset outlived the purge"
    );
}

/// Staged as the delete lands, after the handler's check: refused, or a recreate
/// would let a later commit write it.
#[tokio::test(flavor = "multi_thread")]
async fn an_offset_staged_after_the_delete_is_refused() {
    let c = cluster();
    c.broker.topics.remove(TOPIC);
    c.mgr.offsets.store_pending(GROUP, PID, one(TOPIC, 7));
    c.mgr.offsets.store_pending(GROUP, PID, one(OTHER, 8));
    c.broker.topics.insert(TopicMeta {
        name: TOPIC.to_owned(),
        partition_count: 1,
        topic_id: [0; 16],
    });
    c.mgr.offsets.commit_pending(GROUP, PID).unwrap();

    assert_eq!(
        committed(&c, TOPIC),
        -1,
        "a staged offset outlived the delete"
    );
    assert_eq!(committed(&c, OTHER), 8, "the rest must still commit");
}

/// A transaction's staged offsets on a deleted topic go with it, as in Apache's
/// `GroupMetadata.removeOffsets`. Otherwise a commit after the delete writes them out.
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_offset_on_a_deleted_topic_is_never_committed() {
    let c = cluster();
    c.mgr.offsets.store_pending(GROUP, PID, one(TOPIC, 7));
    c.mgr.offsets.store_pending(GROUP, PID, one(OTHER, 8));

    c.broker.topics.remove(TOPIC);
    c.broker.forget_deleted_topic(TOPIC).await;
    c.mgr.offsets.commit_pending(GROUP, PID).unwrap();

    assert_eq!(
        committed(&c, TOPIC),
        -1,
        "a staged offset outlived the delete"
    );
    assert_eq!(
        committed(&c, OTHER),
        8,
        "the rest of the transaction must commit"
    );
}

/// Staged before a delete and recreate, and committed after: the recreate's topic
/// exists, so only dropping it at the delete keeps it out.
#[tokio::test(flavor = "multi_thread")]
async fn a_staged_offset_does_not_survive_a_recreate() {
    let c = cluster();
    c.mgr.offsets.store_pending(GROUP, PID, one(TOPIC, 7));

    c.broker.topics.remove(TOPIC);
    c.broker.forget_deleted_topic(TOPIC).await;
    c.broker.topics.insert(TopicMeta {
        name: TOPIC.to_owned(),
        partition_count: 1,
        topic_id: [0; 16],
    });
    c.mgr.offsets.commit_pending(GROUP, PID).unwrap();

    assert_eq!(
        committed(&c, TOPIC),
        -1,
        "the dead incarnation's offset came back"
    );
}

/// With nothing racing, a commit and a staged commit still land.
#[tokio::test(flavor = "multi_thread")]
async fn an_ordinary_commit_still_lands() {
    let c = cluster();
    assert_eq!(c.mgr.offset_commit(GROUP, one(TOPIC, 3), HashMap::new()), 0);
    assert_eq!(committed(&c, TOPIC), 3);
    c.mgr.offsets.store_pending(GROUP, PID, one(TOPIC, 4));
    c.mgr.offsets.commit_pending(GROUP, PID).unwrap();
    assert_eq!(committed(&c, TOPIC), 4);
}
