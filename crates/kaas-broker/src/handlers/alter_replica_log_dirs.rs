//! AlterReplicaLogDirs handler (key 34, KIP-113) — the gh #221
//! phase-3 volume-pool migration verb.
//!
//! Authorization (gh #293): `Alter` on the cluster, as in Apache's
//! `handleAlterReplicaLogDirsRequest`. Without it every requested
//! partition answers `CLUSTER_AUTHORIZATION_FAILED` (31) and nothing is
//! resolved or moved. The gh #224 annotation driver calls
//! [`migrate_partition`] directly, with no client principal, so it is
//! not gated here.
//!
//! Per requested `(destination path, topic, partition)`:
//!
//! 1. Resolve the destination path against the engine's log dirs —
//!    unknown paths answer `LOG_DIR_NOT_FOUND` (57), cordoned members
//!    `INVALID_REQUEST` (42) (KIP-1066: no new placements).
//! 2. Leadership gate: only the partition's current leader holds its
//!    files; everyone else answers `REPLICA_NOT_AVAILABLE` (9).
//! 3. `StorageEngine::move_partition_to_log_dir` — close, fresh-copy
//!    to the target root (appends fail retriable-`Migrating` during
//!    the window).
//! 4. Flip the placement record: `KafkaTopic.status.volumeAssignments`
//!    via the CR writer (durable truth), then the local registry (the
//!    engine's resolver — so the next open lands on the new root
//!    without waiting for the watch echo).
//! 5. Reclaim the source directory, best-effort — a failure leaves
//!    orphan bytes, never a correctness problem, and is logged.
//!
//! If the CR flip fails after the copy, the copy is rolled back and
//! the partition stays where it was — placement truth and data
//! location never diverge.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::alter_replica_log_dirs as arld;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;
use tracing::{info, warn};

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_REPLICA_NOT_AVAILABLE: i16 = 9;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_INVALID_REQUEST: i16 = 42;
const ERR_KAFKA_STORAGE_ERROR: i16 = 56;
const ERR_LOG_DIR_NOT_FOUND: i16 = 57;

#[derive(Debug)]
pub struct AlterReplicaLogDirsHandler {
    broker: Arc<Broker>,
}

impl AlterReplicaLogDirsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

/// Move one partition to `log_dir_name`, returning the wire error
/// code (`0` = moved). Shared by the AlterReplicaLogDirs handler and
/// the gh #224 `kaas.rs/migrate-to-volume` annotation driver — same
/// close → fresh-copy → record-flip-with-rollback → reclaim sequence
/// either way.
pub async fn migrate_partition(
    broker: &Broker,
    topic: &str,
    partition: i32,
    log_dir_name: &str,
) -> i16 {
    // A missing topic, or a partition past its count: Apache's
    // UNKNOWN_TOPIC_OR_PARTITION (gh #309).
    if !broker
        .topics
        .get(topic)
        .is_some_and(|m| (0..m.partition_count).contains(&partition))
    {
        return ERR_UNKNOWN_TOPIC_OR_PARTITION;
    }
    // Only the current leader holds the partition's files. Dev /
    // single-broker mode (no coordinator) trivially leads.
    if let Some(c) = broker.coordinator() {
        if !c.owns(topic, partition) {
            return ERR_REPLICA_NOT_AVAILABLE;
        }
    }
    let src_dir = match broker
        .engine
        .move_partition_to_log_dir(topic, partition, log_dir_name)
        .await
    {
        Ok(Some(src)) => src,
        // gh #297: the data is already in the target dir. Apache answers
        // NONE and does nothing. Nothing was copied, so there is nothing
        // to roll back or reclaim: the directory is the live partition.
        // The placement record is still brought in line, because a record
        // that lags the data is how the gh #224 driver reaches this case.
        Ok(None) => return confirm_placement(broker, topic, partition, log_dir_name).await,
        Err(kaas_storage::StorageError::UnknownTopicOrPartition) => {
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        }
        Err(kaas_storage::StorageError::Unsupported(_)) => return ERR_LOG_DIR_NOT_FOUND,
        Err(err) => {
            warn!(topic, partition, %err, "AlterReplicaLogDirs: move failed");
            return ERR_KAFKA_STORAGE_ERROR;
        }
    };

    // Durable truth first: flip the CR status record. On failure,
    // roll the copy back so data location and placement record
    // never diverge (the copy is fresh-per-attempt, so a retry
    // redoes it from scratch).
    if let Some(writer) = broker.cr_writer() {
        if let Err(err) = writer
            .set_partition_log_dir(topic, partition, log_dir_name)
            .await
        {
            warn!(topic, partition, %err,
                "AlterReplicaLogDirs: placement-record flip failed; rolling back copy");
            let target = broker
                .engine
                .log_dirs()
                .into_iter()
                .find(|d| d.name == log_dir_name)
                .map(|d| d.path.join(topic).join(partition.to_string()));
            if let Some(dst) = target {
                let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(dst)).await;
            }
            return ERR_KAFKA_STORAGE_ERROR;
        }
    } else {
        // No CR writer (dev mode): the local registry is the only
        // placement record there is.
        info!(
            topic,
            partition, log_dir_name, "AlterReplicaLogDirs: dev-mode local flip"
        );
    }
    broker
        .topics
        .set_volume_assignment(topic, partition, log_dir_name);

    // Source reclaim is best-effort: a failure (gh #76-style busy
    // handles on NFS) leaves orphan bytes on the old volume, never
    // an inconsistency. Logged so an operator can chase it.
    let src = src_dir.clone();
    if let Err(err) = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&src)).await {
        warn!(topic, partition, %err, "AlterReplicaLogDirs: source reclaim join error");
    }
    info!(topic, partition, log_dir_name, src = %src_dir.display(),
        "AlterReplicaLogDirs: partition moved");
    ERR_NONE
}

/// Does the `migrate-to-volume` driver have work to do for a partition
/// whose data is in `actual` and whose placement record says `record`?
/// Done only when both are at `target`. A record at the target with the data
/// elsewhere still needs the move: going by the record alone read as a
/// finished drain with the data left behind. Data at the target with a
/// lagging record needs its record corrected, which the same-dir path does
/// (gh #309).
#[must_use]
pub fn migration_needed(actual: &str, record: Option<&str>, target: &str) -> bool {
    actual != target || record.unwrap_or(kaas_storage::DEFAULT_LOG_DIR_NAME) != target
}

/// One pass of the `kaas.rs/migrate-to-volume` driver (gh #224): move each
/// of `topic`'s partitions that still needs it onto `target`, one at a time.
///
/// Level-triggered and idempotent. A partition already at the target is
/// skipped, one led elsewhere is another broker's job, and a transient
/// failure is retried by the next pass. `pace` follows each move, so a drain
/// never saturates the substrate.
///
/// Returns how many partitions moved, or `Err(57)` if `target` isn't a log
/// dir, which stops the pass: it can't work for any other partition either.
pub async fn drive_migration(
    broker: &Broker,
    topic: &str,
    partitions: i32,
    target: &str,
    pace: std::time::Duration,
) -> Result<usize, i16> {
    let mut moved = 0;
    for p in 0..partitions {
        let actual = broker.engine.partition_log_dir(topic, p);
        let record = broker.topics.volume_assignment(topic, p);
        if !migration_needed(&actual, record.as_deref(), target) {
            continue;
        }
        match migrate_partition(broker, topic, p, target).await {
            ERR_NONE => {
                moved += 1;
                tokio::time::sleep(pace).await;
            }
            ERR_LOG_DIR_NOT_FOUND => return Err(ERR_LOG_DIR_NOT_FOUND),
            _ => {}
        }
    }
    Ok(moved)
}

/// The partition already lives in `log_dir_name`: make the placement
/// record say so, touching no files. Answers 0 either way, as Apache does
/// for this case (gh #309). The move has nothing left to do, and
/// `partition_dir` serves from where the data is, so a record still lagging
/// after a failed patch costs nothing; the next attempt corrects it.
async fn confirm_placement(
    broker: &Broker,
    topic: &str,
    partition: i32,
    log_dir_name: &str,
) -> i16 {
    if broker.topics.volume_assignment(topic, partition).as_deref() == Some(log_dir_name) {
        return ERR_NONE;
    }
    if let Some(writer) = broker.cr_writer() {
        if let Err(err) = writer
            .set_partition_log_dir(topic, partition, log_dir_name)
            .await
        {
            warn!(topic, partition, %err,
                "AlterReplicaLogDirs: placement-record update failed; data is already in place");
            return ERR_NONE;
        }
    }
    broker
        .topics
        .set_volume_assignment(topic, partition, log_dir_name);
    ERR_NONE
}

#[async_trait]
impl Handler for AlterReplicaLogDirsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = arld::decode_request(&mut body, version)?;

        // Moving a partition's data between volumes is a cluster-level
        // operation in Apache, not a per-topic one.
        let authorized = self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::cluster(),
            Operation::Alter,
        );

        // path → log dir, from the same list DescribeLogDirs reports.
        let dirs = self.broker.engine.log_dirs();

        // Aggregate per topic across the request's dir entries.
        let mut results: BTreeMap<String, Vec<arld::ResponsePartition>> = BTreeMap::new();
        for dir_req in &req.dirs {
            let target = dirs
                .iter()
                .find(|d| d.path.as_os_str() == std::ffi::OsStr::new(&dir_req.path));
            for t in &dir_req.topics {
                for p in &t.partitions {
                    let error_code = match target {
                        _ if !authorized => ERR_CLUSTER_AUTHZ_FAILED,
                        None => ERR_LOG_DIR_NOT_FOUND,
                        // KIP-1066: cordoned log dirs accept no new
                        // placements — moves included.
                        Some(d) if d.cordoned => ERR_INVALID_REQUEST,
                        Some(d) => migrate_partition(&self.broker, &t.name, *p, &d.name).await,
                    };
                    results
                        .entry(t.name.clone())
                        .or_default()
                        .push(arld::ResponsePartition {
                            partition_index: *p,
                            error_code,
                        });
                }
            }
        }

        let resp = arld::Response {
            throttle_time_ms: 0,
            results: results
                .into_iter()
                .map(|(topic_name, partitions)| arld::ResponseTopic {
                    topic_name,
                    partitions,
                })
                .collect(),
        };
        let mut out = BytesMut::new();
        arld::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_authz::{DenyAllAuthorizer, DenyOpAuthorizer};
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_auth::Authorizer;
    use kaas_storage::engine::LogDirInfo;
    use kaas_storage::{DiskStorageEngine, Fs, PartitionConfig, RealFs, StorageEngine};
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    /// A two-root pool (`default` + `fast`) holding partition `t-0` on
    /// the default root, with a sentinel file to follow across a move.
    struct Pool {
        _tmp: tempfile::TempDir,
        src: PathBuf,
        fast_root: PathBuf,
        broker: Arc<Broker>,
    }

    async fn pool(authorizer: Arc<dyn Authorizer>) -> Pool {
        let tmp = tempfile::tempdir().unwrap();
        let default_root = tmp.path().join("data");
        let fast_root = tmp.path().join("fast");
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let engine = DiskStorageEngine::new(fs, default_root.clone(), PartitionConfig::default())
            .with_extra_log_dirs(vec![LogDirInfo {
                name: "fast".to_owned(),
                path: fast_root.clone(),
                default_eligible: true,
                cordoned: false,
                labels: Default::default(),
            }]);
        engine.create_partition("t", 0).await.unwrap();
        let src = default_root.join("t/0");
        std::fs::write(src.join("sentinel"), b"x").unwrap();
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let engine: Arc<dyn StorageEngine> = Arc::new(engine);
        let broker = Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            authorizer,
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        Pool {
            _tmp: tmp,
            src,
            fast_root,
            broker,
        }
    }

    async fn alter(p: &Pool, path: &std::path::Path) -> i16 {
        let req = arld::Request {
            dirs: vec![arld::RequestDir {
                path: path.display().to_string(),
                topics: vec![arld::RequestTopic {
                    name: "t".to_owned(),
                    partitions: vec![0],
                }],
            }],
        };
        let mut body = BytesMut::new();
        arld::encode_request(&mut body, &req, 2).unwrap();
        let h = AlterReplicaLogDirsHandler::new(p.broker.clone());
        let out = h.handle(&conn(), 2, body.freeze()).await.unwrap();
        let resp = arld::decode_response(&mut out.freeze(), 2).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].topic_name, "t");
        resp.results[0].partitions[0].error_code
    }

    /// gh #293: moving a partition between volumes needs `Alter` on the
    /// cluster (Apache's literal 31). Denied, nothing is copied and the
    /// source stays put — and the gate runs before path resolution, so
    /// an unknown path is 31 too, not `LOG_DIR_NOT_FOUND`.
    #[tokio::test]
    async fn denied_alter_is_31_and_moves_nothing() {
        let authorizers: [Arc<dyn Authorizer>; 2] = [
            Arc::new(DenyAllAuthorizer),
            Arc::new(DenyOpAuthorizer(Operation::Alter)),
        ];
        for authorizer in authorizers {
            let p = pool(authorizer).await;
            let fast = p.fast_root.clone();
            assert_eq!(alter(&p, &fast).await, 31);
            assert_eq!(alter(&p, std::path::Path::new("/no/such/dir")).await, 31);
            assert!(!p.fast_root.join("t/0").exists(), "partition copied");
            assert!(p.src.join("sentinel").is_file(), "source touched");
        }
    }

    /// Control: the gate asks for `Alter`, so a principal lacking only
    /// `Describe` still moves the partition.
    #[tokio::test]
    async fn alter_on_the_cluster_moves_the_partition() {
        let p = pool(Arc::new(DenyOpAuthorizer(Operation::Describe))).await;
        let fast = p.fast_root.clone();
        assert_eq!(alter(&p, &fast).await, 0);
        assert!(p.fast_root.join("t/0/sentinel").is_file(), "not moved");
    }

    /// gh #297: a move to the dir the partition is already in is Apache's
    /// no-op, answered 0. It used to hand the live partition dir to the
    /// reclaim, which deleted it and still answered 0. Covers the wire
    /// path (the default root, before any move) and the gh #224 driver's
    /// entry point (`migrate_partition`, after a real move to `fast`).
    #[tokio::test]
    async fn a_move_to_the_current_dir_is_a_no_op_that_keeps_the_data() {
        let p = pool(Arc::new(DenyOpAuthorizer(Operation::Describe))).await;
        let default_root = p.src.parent().unwrap().parent().unwrap().to_path_buf();

        assert_eq!(alter(&p, &default_root).await, 0);
        assert!(
            p.src.join("sentinel").is_file(),
            "the live partition was deleted"
        );

        let fast = p.fast_root.clone();
        assert_eq!(alter(&p, &fast).await, 0);
        let moved = p.fast_root.join("t/0/sentinel");
        assert!(moved.is_file(), "not moved");
        assert_eq!(migrate_partition(&p.broker, "t", 0, "fast").await, 0);
        assert!(
            moved.is_file(),
            "the live partition was deleted by a repeat move"
        );
        assert_eq!(
            p.broker.topics.volume_assignment("t", 0).as_deref(),
            Some("fast")
        );
    }

    /// gh #309: a record lagging the data, with the registry wired as the
    /// placement resolver as in production. The same-dir path answers 0,
    /// keeps the files and corrects the record, and a CR patch that fails
    /// still answers 0 and touches no files (Apache's no-op answers NONE).
    #[tokio::test]
    async fn a_lagging_record_is_corrected_without_touching_files() {
        use crate::topic_cr_writer::{ConfigOpWithValue, TopicCRWriter, TopicWriteError};

        #[derive(Debug)]
        struct Writer {
            fail: bool,
        }
        #[async_trait]
        impl TopicCRWriter for Writer {
            async fn create_topic(
                &self,
                _: &str,
                _: i32,
                _: &[(String, String)],
            ) -> Result<(), TopicWriteError> {
                unreachable!()
            }
            async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
                unreachable!()
            }
            async fn update_topic_config(
                &self,
                _: &str,
                _: &[ConfigOpWithValue],
            ) -> Result<(), TopicWriteError> {
                unreachable!()
            }
            async fn delete_topic(&self, _: &str) -> Result<(), TopicWriteError> {
                unreachable!()
            }
            async fn set_partition_log_dir(
                &self,
                _: &str,
                _: i32,
                _: &str,
            ) -> Result<(), TopicWriteError> {
                if self.fail {
                    Err(TopicWriteError::Forbidden("no".to_owned()))
                } else {
                    Ok(())
                }
            }
        }

        for fail in [false, true] {
            let tmp = tempfile::tempdir().unwrap();
            let default_root = tmp.path().join("data");
            let fast_root = tmp.path().join("fast");
            let topics = Arc::new(TopicRegistry::new());
            topics.insert(TopicMeta {
                name: "t".to_owned(),
                partition_count: 1,
                topic_id: [0; 16],
            });
            let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
            let engine = DiskStorageEngine::new(fs, default_root, PartitionConfig::default())
                .with_extra_log_dirs(vec![LogDirInfo {
                    name: "fast".to_owned(),
                    path: fast_root.clone(),
                    default_eligible: true,
                    cordoned: false,
                    labels: Default::default(),
                }]);
            engine.set_placement_resolver(topics.clone());
            // The data is on `fast`, the record says `default`.
            topics.set_volume_assignment("t", 0, "fast");
            engine.create_partition("t", 0).await.unwrap();
            let moved = fast_root.join("t/0/sentinel");
            std::fs::write(&moved, b"x").unwrap();
            topics.set_volume_assignment("t", 0, "default");
            let engine: Arc<dyn StorageEngine> = Arc::new(engine);
            let broker = Arc::new(Broker::new(engine, topics, "test", 0));
            broker.install_cr_writer(Arc::new(Writer { fail }));

            assert_eq!(
                migrate_partition(&broker, "t", 0, "fast").await,
                0,
                "fail={fail}"
            );
            assert!(moved.is_file(), "fail={fail}: the partition was touched");
            let record = broker.topics.volume_assignment("t", 0);
            let want = if fail { "default" } else { "fast" };
            assert_eq!(record.as_deref(), Some(want), "fail={fail}");
        }
    }

    /// Counts the record patches a driver pass makes.
    #[derive(Debug, Default)]
    struct RecordingWriter {
        patched: Mutex<Vec<i32>>,
    }

    #[async_trait]
    impl crate::topic_cr_writer::TopicCRWriter for RecordingWriter {
        async fn create_topic(
            &self,
            _: &str,
            _: i32,
            _: &[(String, String)],
        ) -> Result<(), crate::topic_cr_writer::TopicWriteError> {
            unreachable!()
        }
        async fn expand_topic(
            &self,
            _: &str,
            _: i32,
        ) -> Result<(), crate::topic_cr_writer::TopicWriteError> {
            unreachable!()
        }
        async fn update_topic_config(
            &self,
            _: &str,
            _: &[crate::topic_cr_writer::ConfigOpWithValue],
        ) -> Result<(), crate::topic_cr_writer::TopicWriteError> {
            unreachable!()
        }
        async fn delete_topic(
            &self,
            _: &str,
        ) -> Result<(), crate::topic_cr_writer::TopicWriteError> {
            unreachable!()
        }
        async fn set_partition_log_dir(
            &self,
            _: &str,
            partition: i32,
            _: &str,
        ) -> Result<(), crate::topic_cr_writer::TopicWriteError> {
            self.patched.lock().push(partition);
            Ok(())
        }
    }

    /// A topic `t` with partitions on the default root, each with a sentinel
    /// file, and the registry wired as the placement resolver as in
    /// production. `on_fast` partitions start on `fast` with a matching
    /// record instead.
    struct DrivePool {
        _tmp: tempfile::TempDir,
        default_root: PathBuf,
        fast_root: PathBuf,
        broker: Arc<Broker>,
        writer: Arc<RecordingWriter>,
    }

    async fn drive_pool(partitions: i32, on_fast: &[i32]) -> DrivePool {
        let tmp = tempfile::tempdir().unwrap();
        let default_root = tmp.path().join("data");
        let fast_root = tmp.path().join("fast");
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: partitions,
            topic_id: [0; 16],
        });
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let engine = DiskStorageEngine::new(fs, default_root.clone(), PartitionConfig::default())
            .with_extra_log_dirs(vec![LogDirInfo {
                name: "fast".to_owned(),
                path: fast_root.clone(),
                default_eligible: true,
                cordoned: false,
                labels: Default::default(),
            }]);
        engine.set_placement_resolver(topics.clone());
        for p in 0..partitions {
            let root = if on_fast.contains(&p) {
                topics.set_volume_assignment("t", p, "fast");
                &fast_root
            } else {
                &default_root
            };
            engine.create_partition("t", p).await.unwrap();
            std::fs::write(root.join(format!("t/{p}/sentinel")), b"x").unwrap();
        }
        let engine: Arc<dyn StorageEngine> = Arc::new(engine);
        let broker = Arc::new(Broker::new(engine, topics, "test", 0));
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        DrivePool {
            _tmp: tmp,
            default_root,
            fast_root,
            broker,
            writer,
        }
    }

    const NO_PACE: std::time::Duration = std::time::Duration::ZERO;

    /// gh #309: the driver moves what needs moving, leaves a finished
    /// partition alone, and paces itself after each move.
    #[tokio::test]
    async fn the_driver_moves_what_needs_it_and_skips_the_rest() {
        let p = drive_pool(3, &[1]).await;
        // Longer than the two moves take on their own, or this wouldn't
        // notice the pacing going away.
        let pace = std::time::Duration::from_millis(250);
        let started = std::time::Instant::now();
        assert_eq!(
            drive_migration(&p.broker, "t", 3, "fast", pace).await,
            Ok(2)
        );
        assert!(started.elapsed() >= pace * 2, "{:?}", started.elapsed());
        for n in 0..3 {
            let sentinel = p.fast_root.join(format!("t/{n}/sentinel"));
            assert!(sentinel.is_file(), "partition {n} isn't on fast");
            assert_eq!(p.broker.engine.partition_log_dir("t", n), "fast");
        }
        assert!(!p.default_root.join("t/0/sentinel").exists());
        assert_eq!(*p.writer.patched.lock(), [0, 2], "partition 1 was done");

        // A second pass has nothing left to do.
        assert_eq!(
            drive_migration(&p.broker, "t", 3, "fast", NO_PACE).await,
            Ok(0)
        );
        assert_eq!(*p.writer.patched.lock(), [0, 2]);
    }

    /// gh #309: a record already at the target with the data elsewhere is
    /// not a finished drain. Going by the record alone left the data behind
    /// and read as done.
    #[tokio::test]
    async fn the_driver_moves_data_that_a_matching_record_left_behind() {
        let p = drive_pool(1, &[]).await;
        p.broker.topics.set_volume_assignment("t", 0, "fast");
        assert_eq!(p.broker.engine.partition_log_dir("t", 0), "default");
        assert_eq!(
            drive_migration(&p.broker, "t", 1, "fast", NO_PACE).await,
            Ok(1)
        );
        assert!(p.fast_root.join("t/0/sentinel").is_file());
        assert_eq!(p.broker.engine.partition_log_dir("t", 0), "fast");
    }

    /// gh #309: an unknown target stops the pass with Apache's 57, and
    /// nothing moves.
    #[tokio::test]
    async fn an_unknown_target_stops_the_driver() {
        let p = drive_pool(2, &[]).await;
        assert_eq!(
            drive_migration(&p.broker, "t", 2, "nope", NO_PACE).await,
            Err(57)
        );
        assert!(p.default_root.join("t/0/sentinel").is_file());
        assert!(p.default_root.join("t/1/sentinel").is_file());
    }

    /// gh #309: a partition led elsewhere is another broker's job. It stays
    /// put, and the pass carries on past it.
    #[tokio::test]
    async fn the_driver_leaves_partitions_it_doesnt_lead() {
        use crate::handlers::test_cluster::{self, PEER, SELF};
        let p = drive_pool(3, &[]).await;
        let cluster = tempfile::tempdir().unwrap();
        test_cluster::install(
            &p.broker,
            cluster.path(),
            &[("t", 0, PEER), ("t", 1, SELF), ("t", 2, PEER)],
        );
        assert_eq!(
            drive_migration(&p.broker, "t", 3, "fast", NO_PACE).await,
            Ok(1)
        );
        assert!(p.default_root.join("t/0/sentinel").is_file());
        assert!(p.fast_root.join("t/1/sentinel").is_file());
        assert!(p.default_root.join("t/2/sentinel").is_file());
    }

    /// gh #309: a partition past the topic's count is Apache's 3, not 9.
    #[tokio::test]
    async fn an_out_of_range_partition_is_3() {
        let p = pool(Arc::new(DenyOpAuthorizer(Operation::Describe))).await;
        assert_eq!(migrate_partition(&p.broker, "t", 99, "fast").await, 3);
        assert_eq!(migrate_partition(&p.broker, "missing", 0, "fast").await, 3);
    }

    /// gh #309: the drain is done only when the data and the record are
    /// both at the target.
    #[test]
    fn a_drain_is_finished_only_when_data_and_record_agree() {
        assert!(!migration_needed("fast", Some("fast"), "fast"));
        assert!(
            migration_needed("default", Some("fast"), "fast"),
            "data left behind"
        );
        assert!(
            migration_needed("fast", Some("default"), "fast"),
            "record lags"
        );
        assert!(migration_needed("fast", None, "fast"), "no record yet");
        assert!(!migration_needed("default", None, "default"));
    }
}
