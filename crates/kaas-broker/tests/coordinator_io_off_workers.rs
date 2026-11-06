//! gh #328: coordinator file work stays off the scheduler workers.
//!
//! The offset store and the txn store do their I/O synchronously, behind locks
//! that the next caller waits on across someone else's fsync. Waiting on a
//! worker parks every connection that worker drives: one busy group took
//! ApiVersions from about 0.5 ms to over 300 ms.
//!
//! Each test runs on tokio's default test runtime, which has one thread. It stops
//! a route partway through with a [`Gate`], then sleeps. If the route is waiting
//! on the worker the sleep can't end, the gate gives up after [`WATCHDOG`], and
//! [`parks_nothing`] reports that. Nothing here depends on how long anything
//! takes, short of a 5 s stall of the machine.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use kaas_broker::txn_markers::{reconcile_pending_markers, sweep_transactions};
use kaas_broker::{
    AddOffsetsToTxnHandler, AddPartitionsToTxnHandler, Broker, DeleteGroupsHandler,
    DescribeGroupsHandler, EndTxnHandler, InitProducerIdHandler, JoinGroupHandler,
    OffsetCommitHandler, OffsetDeleteHandler, OffsetFetchHandler, ProducerIdAllocator, TopicMeta,
    TopicRegistry, PID_BLOCK, PRODUCER_ID_DIR,
};
use kaas_codec::api::common::{write_array_len, write_str};
use kaas_codec::api::{
    delete_groups, describe_groups, end_txn, init_producer_id, join_group, offset_commit,
    offset_delete, offset_fetch,
};
use kaas_codec::primitives::{write_i16, write_i32, write_i64, write_i8};
use kaas_coordinator::offset_store::FenceStamp;
use kaas_coordinator::{
    BrokerEndpoint, BrokerLookup, FenceLog, FnLookup, LocalGroupSource, LocalTxnSource, Manager,
    OffsetStore, TxnOffsetHook, TxnState, TxnStateStore,
};
use kaas_protocol::{ConnState, Handler};
use kaas_storage::{MemoryStorage, StorageEngine};
use parking_lot::{Condvar, Mutex};
use tokio::task::JoinHandle;

/// How long a [`Gate`] waits for the test before concluding the worker is parked.
const WATCHDOG: Duration = Duration::from_secs(5);
const GROUP: &str = "g";
const TOPIC: &str = "t";
const TXN: &str = "tx-1";

/// Stops whatever waits on it until the test opens it, or the watchdog fires.
#[derive(Default)]
struct Gate {
    open: Mutex<bool>,
    opened: Condvar,
    gave_up: AtomicBool,
}

impl Gate {
    fn wait(&self) {
        let deadline = Instant::now() + WATCHDOG;
        let mut open = self.open.lock();
        while !*open {
            if self.opened.wait_until(&mut open, deadline).timed_out() {
                self.gave_up.store(true, Ordering::SeqCst);
                return;
            }
        }
    }

    fn open(&self) {
        *self.open.lock() = true;
        self.opened.notify_all();
    }
}

/// The check: `task` is stopped at the gate without stopping this test.
async fn parks_nothing<T>(gate: &Gate, task: &JoinHandle<T>) {
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stalled = !task.is_finished();
    gate.open();
    assert!(
        !gate.gave_up.load(Ordering::SeqCst),
        "the route parked the scheduler worker until the watchdog fired"
    );
    assert!(stalled, "the route never reached the gate: proves nothing");
}

struct Cluster {
    _tmp: tempfile::TempDir,
    root: std::path::PathBuf,
    broker: Arc<Broker>,
    mgr: Arc<Manager>,
    store: Arc<TxnStateStore>,
}

fn cluster() -> Cluster {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().to_owned();
    let topics = Arc::new(TopicRegistry::new());
    topics.insert(TopicMeta {
        name: TOPIC.to_owned(),
        partition_count: 1,
        topic_id: [0; 16],
    });
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
        Arc::new(OffsetStore::new(&root)),
        lookup,
        LocalGroupSource::new("kaas-0"),
    );
    mgr.set_txn_assignment_source(LocalTxnSource::new("kaas-0"));
    broker.install_coord_manager(mgr.clone());
    let store = Arc::new(TxnStateStore::open(&root, 0).unwrap());
    broker.install_txn_state(store.clone());
    Cluster {
        _tmp: tmp,
        root,
        broker,
        mgr,
        store,
    }
}

fn one_offset(offset: i64) -> HashMap<String, i64> {
    HashMap::from([(kaas_coordinator::offset_key(TOPIC, 0), offset)])
}

/// Another thread stopped partway through a commit to `GROUP`, holding its write
/// lock, as a slow fsync would.
struct Held {
    gate: Arc<Gate>,
    thread: std::thread::JoinHandle<()>,
}

fn hold_group_lock(c: &Cluster) -> Held {
    let gate = Arc::new(Gate::default());
    let (reached, reached_rx) = mpsc::channel();
    let reached = Mutex::new(reached);
    let stop = gate.clone();
    // Every write reads the fence with the lock held.
    c.mgr.offsets.set_fence_source(move || {
        let _ = reached.lock().send(());
        stop.wait();
        FenceStamp::default()
    });
    let mgr = c.mgr.clone();
    let thread = std::thread::spawn(move || {
        mgr.offset_commit(GROUP, one_offset(7), HashMap::new());
    });
    reached_rx.recv_timeout(WATCHDOG).unwrap();
    Held { gate, thread }
}

/// A [`TxnOffsetHook`] that passes `skip` calls, then stops at the gate. The
/// store holds its global mutex across the hook.
struct GatedHook {
    gate: Arc<Gate>,
    skip: AtomicUsize,
    reached: Mutex<mpsc::Sender<()>>,
}

impl TxnOffsetHook for GatedHook {
    fn on_end_txn(&self, _: &str, _: i64, _: bool) -> std::io::Result<()> {
        let passed = self
            .skip
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if !passed {
            let _ = self.reached.lock().send(());
            self.gate.wait();
        }
        Ok(())
    }
}

fn gated_hook(c: &Cluster, skip: usize) -> (Arc<Gate>, mpsc::Receiver<()>) {
    let gate = Arc::new(Gate::default());
    let (reached, reached_rx) = mpsc::channel();
    c.store.set_offset_hook(Arc::new(GatedHook {
        gate: gate.clone(),
        skip: AtomicUsize::new(skip),
        reached: Mutex::new(reached),
    }));
    (gate, reached_rx)
}

/// An `Ongoing` transaction on `TXN` that owes offsets to `GROUP`.
fn ongoing_txn(c: &Cluster, txn_id: &str) -> (i64, i16) {
    let (pid, epoch) = c.store.get_or_allocate(txn_id, || 1000).unwrap();
    c.store
        .add_offsets_to_txn(txn_id, pid, epoch, GROUP, 100)
        .unwrap();
    (pid, epoch)
}

/// Another thread inside the txn store's global mutex, stopped in an offset hook.
fn hold_txn_store(c: &Cluster) -> Held {
    let (gate, reached) = gated_hook(c, 0);
    let (pid, epoch) = ongoing_txn(c, "holder");
    c.store.prepare_end_txn("holder", pid, epoch, true).unwrap();
    let store = c.store.clone();
    let thread = std::thread::spawn(move || {
        store.settle_offsets("holder", pid, epoch, true).unwrap();
    });
    reached.recv_timeout(WATCHDOG).unwrap();
    Held { gate, thread }
}

fn mkfifo(path: &Path) {
    let made = std::process::Command::new("mkfifo")
        .arg(path)
        .status()
        .unwrap();
    assert!(made.success(), "mkfifo {}", path.display());
}

fn request(h: impl Handler, version: i16, body: BytesMut) -> JoinHandle<Bytes> {
    tokio::spawn(async move {
        let conn = Mutex::new(ConnState::new(
            "plain",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ));
        h.handle(&conn, version, body.freeze())
            .await
            .unwrap()
            .freeze()
    })
}

// --- request bodies -------------------------------------------------------

fn offset_commit_body() -> BytesMut {
    let req = offset_commit::Request {
        group_id: GROUP.to_owned(),
        generation_id: -1,
        member_id: String::new(),
        group_instance_id: None,
        retention_time_ms: offset_commit::DEFAULT_RETENTION_TIME_MS,
        topics: vec![offset_commit::OffsetCommitTopic {
            name: TOPIC.to_owned(),
            partitions: vec![offset_commit::OffsetCommitPartition {
                partition_index: 0,
                committed_offset: 9,
                committed_leader_epoch: -1,
                committed_metadata: None,
            }],
        }],
    };
    let mut body = BytesMut::new();
    offset_commit::encode_request(&mut body, &req, 8).unwrap();
    body
}

fn offset_fetch_body(
    version: i16,
    topics: Option<Vec<offset_fetch::OffsetFetchTopic>>,
) -> BytesMut {
    let req = if version >= 8 {
        offset_fetch::Request {
            group_id: String::new(),
            topics: None,
            groups: vec![offset_fetch::OffsetFetchGroup {
                group_id: GROUP.to_owned(),
                topics,
            }],
            require_stable: false,
        }
    } else {
        offset_fetch::Request {
            group_id: GROUP.to_owned(),
            topics,
            groups: Vec::new(),
            require_stable: false,
        }
    };
    let mut body = BytesMut::new();
    offset_fetch::encode_request(&mut body, &req, version).unwrap();
    body
}

fn named_topic() -> Option<Vec<offset_fetch::OffsetFetchTopic>> {
    Some(vec![offset_fetch::OffsetFetchTopic {
        name: TOPIC.to_owned(),
        partition_indexes: vec![0],
    }])
}

fn end_txn_body(pid: i64, epoch: i16) -> BytesMut {
    let mut w = BytesMut::new();
    write_str(&mut w, TXN, false).unwrap();
    write_i64(&mut w, pid);
    write_i16(&mut w, epoch);
    write_i8(&mut w, 1);
    w
}

fn init_producer_id_body(txn_id: &str) -> BytesMut {
    let mut w = BytesMut::new();
    write_str(&mut w, txn_id, false).unwrap();
    write_i32(&mut w, 60_000);
    w
}

// --- the offset store's write lock ----------------------------------------

#[tokio::test]
async fn offset_commit_stays_off_the_workers() {
    let c = cluster();
    let held = hold_group_lock(&c);
    let task = request(
        OffsetCommitHandler::new(c.broker.clone()),
        8,
        offset_commit_body(),
    );
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn cold_offset_fetch_stays_off_the_workers() {
    for (version, topics) in [(5, named_topic()), (5, None), (8, named_topic()), (8, None)] {
        let c = cluster();
        let held = hold_group_lock(&c);
        let task = request(
            OffsetFetchHandler::new(c.broker.clone()),
            version,
            offset_fetch_body(version, topics),
        );
        parks_nothing(&held.gate, &task).await;
        task.await.unwrap();
        held.thread.join().unwrap();
    }
}

#[tokio::test]
async fn describe_groups_stays_off_the_workers() {
    let c = cluster();
    let held = hold_group_lock(&c);
    let req = describe_groups::Request {
        groups: vec![GROUP.to_owned()],
        include_authorized_operations: false,
    };
    let mut body = BytesMut::new();
    describe_groups::encode_request(&mut body, &req, 5).unwrap();
    let task = request(DescribeGroupsHandler::new(c.broker.clone()), 5, body);
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn delete_groups_stays_off_the_workers() {
    let c = cluster();
    let held = hold_group_lock(&c);
    let req = delete_groups::Request {
        group_names: vec![GROUP.to_owned()],
    };
    let mut body = BytesMut::new();
    delete_groups::encode_request(&mut body, &req, 2).unwrap();
    let task = request(DeleteGroupsHandler::new(c.broker.clone()), 2, body);
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn offset_delete_stays_off_the_workers() {
    let c = cluster();
    let held = hold_group_lock(&c);
    let req = offset_delete::Request {
        group_id: GROUP.to_owned(),
        topics: vec![offset_delete::OffsetDeleteTopic {
            name: TOPIC.to_owned(),
            partitions: vec![0],
        }],
    };
    let mut body = BytesMut::new();
    offset_delete::encode_request(&mut body, &req, 0).unwrap();
    let task = request(OffsetDeleteHandler::new(c.broker.clone()), 0, body);
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

/// The first join since a restart reads the group's file.
#[tokio::test]
async fn a_cold_join_stays_off_the_workers() {
    let c = cluster();
    let held = hold_group_lock(&c);
    let req = join_group::Request {
        group_id: GROUP.to_owned(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 30_000,
        member_id: String::new(),
        group_instance_id: None,
        protocol_type: "consumer".to_owned(),
        protocols: vec![join_group::JoinGroupProtocol {
            name: "range".to_owned(),
            metadata: Bytes::from_static(b"meta"),
        }],
        reason: None,
    };
    let mut body = BytesMut::new();
    join_group::encode_request(&mut body, &req, 9).unwrap();
    let task = request(JoinGroupHandler::new(c.broker.clone()), 9, body);
    parks_nothing(&held.gate, &task).await;
    // It goes on to wait out the initial rebalance delay, which isn't the point.
    task.abort();
    held.thread.join().unwrap();
}

/// A topic delete walks every group's file.
#[tokio::test]
async fn the_purge_on_a_topic_delete_stays_off_the_workers() {
    let c = cluster();
    assert_eq!(c.mgr.offset_commit(GROUP, one_offset(1), HashMap::new()), 0);
    let held = hold_group_lock(&c);
    let broker = c.broker.clone();
    let task = tokio::spawn(async move { broker.forget_deleted_topic(TOPIC).await });
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

// --- the txn store's global mutex -----------------------------------------

#[tokio::test]
async fn init_producer_id_stays_off_the_workers() {
    let c = cluster();
    let held = hold_txn_store(&c);
    let task = request(
        InitProducerIdHandler::new(c.broker.clone()),
        0,
        init_producer_id_body("tx-new"),
    );
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn add_partitions_to_txn_stays_off_the_workers() {
    let c = cluster();
    let (pid, epoch) = c.store.get_or_allocate(TXN, || 1).unwrap();
    let held = hold_txn_store(&c);
    let mut body = BytesMut::new();
    write_str(&mut body, TXN, false).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_array_len(&mut body, 1, false).unwrap();
    write_str(&mut body, TOPIC, false).unwrap();
    write_array_len(&mut body, 1, false).unwrap();
    write_i32(&mut body, 0);
    let task = request(AddPartitionsToTxnHandler::new(c.broker.clone()), 0, body);
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn add_offsets_to_txn_stays_off_the_workers() {
    let c = cluster();
    let (pid, epoch) = c.store.get_or_allocate(TXN, || 1).unwrap();
    let held = hold_txn_store(&c);
    let mut body = BytesMut::new();
    write_str(&mut body, TXN, false).unwrap();
    write_i64(&mut body, pid);
    write_i16(&mut body, epoch);
    write_str(&mut body, GROUP, false).unwrap();
    let task = request(AddOffsetsToTxnHandler::new(c.broker.clone()), 0, body);
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

/// EndTxn fires the hook twice, from the settle and from the complete. Both must
/// be off the workers, and the transaction must still finish.
#[tokio::test]
async fn end_txns_offset_hook_stays_off_the_workers() {
    for skip in [0, 1] {
        let c = cluster();
        let (gate, _reached) = gated_hook(&c, skip);
        let (pid, epoch) = ongoing_txn(&c, TXN);
        let task = request(
            EndTxnHandler::new(c.broker.clone()),
            0,
            end_txn_body(pid, epoch),
        );
        parks_nothing(&gate, &task).await;
        let mut out = task.await.unwrap();
        assert_eq!(end_txn::decode_response(&mut out, 0).unwrap().error_code, 0);
        assert_eq!(c.store.snapshot()[TXN].state, TxnState::CompleteCommit);
    }
}

/// Behind another caller's hook, its first store call waits on the mutex.
#[tokio::test]
async fn end_txn_waiting_on_the_store_stays_off_the_workers() {
    let c = cluster();
    c.store.get_or_allocate(TXN, || 1).unwrap();
    let held = hold_txn_store(&c);
    let task = request(EndTxnHandler::new(c.broker.clone()), 0, end_txn_body(1, 0));
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

/// A rejoin over an `Ongoing` transaction finishes the abort itself.
#[tokio::test]
async fn the_abort_on_a_rejoin_stays_off_the_workers() {
    let c = cluster();
    let (gate, _reached) = gated_hook(&c, 0);
    ongoing_txn(&c, TXN);
    let task = request(
        InitProducerIdHandler::new(c.broker.clone()),
        0,
        init_producer_id_body(TXN),
    );
    parks_nothing(&gate, &task).await;
    let mut out = task.await.unwrap();
    let resp = init_producer_id::decode_response(&mut out, 0).unwrap();
    assert_eq!(resp.error_code, 51, "CONCURRENT_TRANSACTIONS");
    assert_eq!(c.store.snapshot()[TXN].state, TxnState::CompleteAbort);
}

/// A rejoin broadcasts its fence. A FIFO stands in for the log: reading it blocks
/// until something opens the other end.
#[tokio::test]
async fn the_fence_on_a_rejoin_stays_off_the_workers() {
    let c = cluster();
    c.store.get_or_allocate(TXN, || 1).unwrap();
    let dir = c.root.join("fences");
    c.broker
        .install_fence_log(Arc::new(FenceLog::open(&dir, "kaas-0").unwrap()));
    let path = dir.join("from-kaas-0.json");
    mkfifo(&path);
    let gate = Arc::new(Gate::default());
    let stop = gate.clone();
    let writer = std::thread::spawn(move || {
        stop.wait();
        std::fs::write(&path, b"{}").unwrap();
    });
    let task = request(
        InitProducerIdHandler::new(c.broker.clone()),
        0,
        init_producer_id_body(TXN),
    );
    parks_nothing(&gate, &task).await;
    task.await.unwrap();
    writer.join().unwrap();
}

/// An idempotent producer takes no lock in the txn store, but every 1000th PID
/// persists the next block (gh #332). A FIFO stands in for the temp file: opening
/// it to write blocks until something reads the other end.
#[tokio::test]
async fn a_new_pid_block_stays_off_the_workers() {
    let c = cluster();
    c.broker
        .install_producer_id_allocator(Arc::new(ProducerIdAllocator::open(&c.root, 0).unwrap()));
    for _ in 0..PID_BLOCK {
        c.broker.next_producer_id();
    }
    let fifo = c.root.join(PRODUCER_ID_DIR).join("kaas-0.json.tmp");
    mkfifo(&fifo);
    let gate = Arc::new(Gate::default());
    let stop = gate.clone();
    let reader = std::thread::spawn(move || {
        stop.wait();
        let _ = std::fs::read(&fifo);
    });
    let mut body = BytesMut::new();
    write_i16(&mut body, -1); // a null transactional id
    write_i32(&mut body, 60_000);

    let task = request(InitProducerIdHandler::new(c.broker.clone()), 0, body);
    parks_nothing(&gate, &task).await;
    let mut out = task.await.unwrap();
    let resp = init_producer_id::decode_response(&mut out, 0).unwrap();
    assert_eq!(resp.error_code, 0);
    assert!(resp.producer_id > 0);
    reader.join().unwrap();
}

// --- the reaper's tick ----------------------------------------------------

#[tokio::test]
async fn the_txn_sweep_stays_off_the_workers() {
    let c = cluster();
    let held = hold_txn_store(&c);
    let (broker, store) = (c.broker.clone(), c.store.clone());
    let task = tokio::spawn(async move { sweep_transactions(&broker, &store, 0).await });
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();
}

#[tokio::test]
async fn the_marker_reconcile_stays_off_the_workers() {
    // Waiting on the mutex to list what's pending.
    let c = cluster();
    let held = hold_txn_store(&c);
    let (broker, store) = (c.broker.clone(), c.store.clone());
    let task = tokio::spawn(async move { reconcile_pending_markers(&broker, &store, None).await });
    parks_nothing(&held.gate, &task).await;
    task.await.unwrap();
    held.thread.join().unwrap();

    // Then its settle and its complete, in the hook.
    for skip in [0, 1] {
        let c = cluster();
        let (gate, _reached) = gated_hook(&c, skip);
        let (pid, epoch) = ongoing_txn(&c, TXN);
        c.store.prepare_end_txn(TXN, pid, epoch, true).unwrap();
        let (broker, store) = (c.broker.clone(), c.store.clone());
        let task =
            tokio::spawn(async move { reconcile_pending_markers(&broker, &store, None).await });
        parks_nothing(&gate, &task).await;
        assert_eq!(
            task.await.unwrap(),
            1,
            "it must still finish the transaction"
        );
        assert_eq!(c.store.snapshot()[TXN].state, TxnState::CompleteCommit);
    }
}
