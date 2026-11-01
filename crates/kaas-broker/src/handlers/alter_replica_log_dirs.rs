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
    if broker.topics.get(topic).is_none() {
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
        Ok(src) => src,
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
        for authorizer in [
            Arc::new(DenyAllAuthorizer) as Arc<dyn Authorizer>,
            Arc::new(DenyOpAuthorizer(Operation::Alter)),
        ] {
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
}
