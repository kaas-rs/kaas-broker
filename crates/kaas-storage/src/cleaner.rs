//! `RetentionCleaner` — size- and time-based log retention.
//!
//! The `delete`-policy cleaner. Walks every partition this broker
//! leads, asks it for a cleanup target under `retention.bytes` and
//! `retention.ms`, and calls `delete_records` when either has been
//! exceeded. The compactor (`cleanup.policy=compact`) and its gh #116
//! knobs are still open on gh #158; this is the `delete` half only.
//!
//! Both knobs are ceilings, so a pass takes whichever target reaps
//! more. `-1` on either means "retain forever", as in Apache.
//!
//! # Threading
//!
//! The cleaner does NOT own a background task — it exposes a
//! [`RetentionCleaner::run_once`] entry point, which `bins/kaas`
//! drives on a `tokio::time::interval` (`KAAS_RETENTION_CHECK_INTERVAL`,
//! Apache's `log.retention.check.interval.ms`) with a cancellation
//! token so a SIGTERM drain doesn't race a segment delete.
//!
//! # Why it must be leader-gated
//!
//! A pass unlinks files on the shared volume. Two brokers reaping the
//! same partition is the classic NFS substrate rule 3 violation, and
//! unlinking a file a peer holds open silly-renames it to `.nfsXXXX`
//! instead of freeing the space (gh #76). [`OwnershipSource`] is the
//! gate; it degrades to "own everything" so dev mode still cleans.

use std::sync::Arc;

use crate::disk::DiskStorageEngine;
use crate::errors::StorageError;
use crate::topicconfig::sentinel_to_option;

/// Per-topic retention policy under `cleanup.policy=delete`. The
/// compactor's knobs (`min.compaction.lag.ms`, `delete.retention.ms`)
/// land with the compactor itself — see gh #158.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// `retention.bytes`. `None` = no size cap (the `-1` sentinel).
    pub retention_bytes: Option<u64>,
    /// `retention.ms`. `None` = retain forever (the `-1` sentinel).
    pub retention_ms: Option<u64>,
}

impl RetentionPolicy {
    /// Nothing to enforce — skip the partition without touching it.
    pub fn is_unlimited(&self) -> bool {
        self.retention_bytes.is_none() && self.retention_ms.is_none()
    }
}

/// Per-topic policy resolver.
///
/// `partition` is passed because it's what locates the file, not
/// because policy varies per partition: a topic's partitions can sit
/// on different volume-pool roots, and `.config.json` is written per
/// involved root, so resolving the topic dir needs to know which
/// partition is being asked about.
pub trait PolicySource: Send + Sync + 'static {
    fn policy_for(&self, topic: &str, partition: i32) -> RetentionPolicy;
}

/// "Do I lead this partition?" Retention deletes files, so only the
/// broker that owns the partition may run it — otherwise two brokers
/// unlink the same segments on the shared volume (NFS substrate rule
/// 3, `docs/src/architecture/nfs-substrate.md`).
///
/// Same degrade-safe shape as the gh #91 txn gate: the default impl
/// answers `true`, so dev mode and single-broker deployments (which
/// install no source) keep cleaning rather than silently stopping.
pub trait OwnershipSource: Send + Sync + 'static {
    fn owns_partition(&self, topic: &str, partition: i32) -> bool;
}

/// Ownership source that owns everything. Dev mode and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct OwnsEverything;

impl OwnershipSource for OwnsEverything {
    fn owns_partition(&self, _topic: &str, _partition: i32) -> bool {
        true
    }
}

/// Single-policy source used by the basic `RetentionCleaner::new`
/// constructor.
#[derive(Debug, Clone, Copy)]
pub struct FixedPolicySource {
    policy: RetentionPolicy,
}

impl FixedPolicySource {
    pub fn new(policy: RetentionPolicy) -> Self {
        Self { policy }
    }
}

impl PolicySource for FixedPolicySource {
    fn policy_for(&self, _topic: &str, _partition: i32) -> RetentionPolicy {
        self.policy
    }
}

/// The production policy source: reads each topic's `.config.json`
/// off the shared volume on every lookup.
///
/// Deliberately uncached. The file is small, a pass runs every few
/// minutes, and re-reading it *is* the hot-reload path — an operator
/// editing `retentionMs` on a `KafkaTopic` CR has the change take
/// effect at the next pass with no broker restart. `defaults` fills in
/// any key the file omits.
pub struct TopicConfigPolicySource {
    engine: Arc<DiskStorageEngine>,
    defaults: RetentionPolicy,
}

impl std::fmt::Debug for TopicConfigPolicySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicConfigPolicySource")
            .field("defaults", &self.defaults)
            .finish_non_exhaustive()
    }
}

impl TopicConfigPolicySource {
    pub fn new(engine: Arc<DiskStorageEngine>, defaults: RetentionPolicy) -> Self {
        Self { engine, defaults }
    }
}

impl TopicConfigPolicySource {
    /// Read `(topic, partition)`'s `.config.json`, or `None` when it is
    /// absent or unreadable. An unreadable file is treated as absent on
    /// purpose: the fallback is the engine default, and defaulting is
    /// the safe direction for every knob here.
    fn read(&self, topic: &str, partition: i32) -> Option<crate::topicconfig::TopicConfigFile> {
        let dir = self.engine.topic_dir(topic, partition);
        match crate::topicconfig::read_topic_config(self.engine.fs(), &dir) {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    topic,
                    partition,
                    error = %e,
                    "unreadable .config.json; using default topic config"
                );
                None
            }
        }
    }

    /// The topic's partition tuning — `segment.bytes` and
    /// `segment.ms` layered over `base`.
    ///
    /// This is the other half of honouring a topic's config, and the
    /// half whose absence made `retention.ms` look broken: without a
    /// per-topic `segment.bytes` every partition rolled at the global
    /// 1 GiB, so a low-volume topic had exactly one segment — the
    /// active one, which retention may never delete.
    pub fn partition_config_for(
        &self,
        topic: &str,
        partition: i32,
        base: &crate::partition::PartitionConfig,
    ) -> crate::partition::PartitionConfig {
        match self.read(topic, partition) {
            Some(cfg) => crate::topicconfig::apply_to_partition_config(&cfg, base),
            None => base.clone(),
        }
    }
}

impl PolicySource for TopicConfigPolicySource {
    fn policy_for(&self, topic: &str, partition: i32) -> RetentionPolicy {
        let Some(cfg) = self.read(topic, partition) else {
            return self.defaults;
        };
        RetentionPolicy {
            retention_bytes: cfg
                .retention_bytes
                .map_or(self.defaults.retention_bytes, sentinel_to_option),
            retention_ms: cfg
                .retention_ms
                .map_or(self.defaults.retention_ms, sentinel_to_option),
        }
    }
}

/// Wall clock as epoch milliseconds. Saturates at 0 for a clock set
/// before 1970, which would otherwise wrap into a huge cutoff and reap
/// everything.
fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|d| i64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

pub struct RetentionCleaner {
    engine: Arc<DiskStorageEngine>,
    policy_source: Arc<dyn PolicySource>,
    ownership: Arc<dyn OwnershipSource>,
    /// Injectable clock (epoch ms) so tests can advance time past a
    /// retention window without sleeping.
    now_ms: Arc<dyn Fn() -> i64 + Send + Sync>,
    /// Optional per-topic tuning resolver. When set, each sweep
    /// re-resolves every owned partition's `segment.bytes` /
    /// `segment.ms` and applies any change to the live partition.
    #[allow(clippy::type_complexity)]
    partition_config: Option<
        Arc<
            dyn Fn(
                    &str,
                    i32,
                    &crate::partition::PartitionConfig,
                ) -> crate::partition::PartitionConfig
                + Send
                + Sync,
        >,
    >,
}

impl std::fmt::Debug for RetentionCleaner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetentionCleaner")
            .field("engine", &self.engine)
            .finish()
    }
}

impl RetentionCleaner {
    /// Build a cleaner with a single engine-wide retention policy.
    /// Use [`Self::with_policy_source`] for per-topic policies.
    pub fn new(engine: Arc<DiskStorageEngine>, policy: RetentionPolicy) -> Self {
        Self::with_policy_source(engine, Arc::new(FixedPolicySource::new(policy)))
    }

    pub fn with_policy_source(
        engine: Arc<DiskStorageEngine>,
        policy_source: Arc<dyn PolicySource>,
    ) -> Self {
        Self {
            engine,
            policy_source,
            ownership: Arc::new(OwnsEverything),
            now_ms: Arc::new(now_epoch_ms),
            partition_config: None,
        }
    }

    /// Also carry `.config.json` tuning changes onto live partitions
    /// each pass (see the call site for why the sweep owns this).
    #[allow(clippy::type_complexity)]
    pub fn with_partition_config(
        mut self,
        f: Arc<
            dyn Fn(
                    &str,
                    i32,
                    &crate::partition::PartitionConfig,
                ) -> crate::partition::PartitionConfig
                + Send
                + Sync,
        >,
    ) -> Self {
        self.partition_config = Some(f);
        self
    }

    /// Install the leadership gate. Without one the cleaner assumes it
    /// owns every open partition — right for dev and single-broker,
    /// wrong for a cluster.
    pub fn with_ownership(mut self, ownership: Arc<dyn OwnershipSource>) -> Self {
        self.ownership = ownership;
        self
    }

    /// Override the clock (epoch ms). Test hook.
    pub fn with_clock(mut self, now_ms: Arc<dyn Fn() -> i64 + Send + Sync>) -> Self {
        self.now_ms = now_ms;
        self
    }

    /// One cleanup pass over every open partition. Returns the count
    /// of partitions where retention actually triggered a
    /// `delete_records` call (useful for tests + OTel meter wire-up).
    pub async fn run_once(&self) -> Result<u32, StorageError> {
        let started = std::time::Instant::now();
        let out = self.run_once_inner().await;
        let m = kaas_observability::metrics::global();
        m.cleaner_duration
            .record(started.elapsed().as_secs_f64(), &[]);
        m.cleaner_runs.add(
            1,
            &[kaas_observability::KeyValue::new(
                "result",
                if out.is_ok() { "ok" } else { "error" },
            )],
        );
        out
    }

    async fn run_once_inner(&self) -> Result<u32, StorageError> {
        let mut cleaned = 0u32;
        let now = (self.now_ms)();
        for (topic, partition) in self.engine.iter_partition_keys() {
            if !self.ownership.owns_partition(&topic, partition) {
                continue;
            }
            let Some(p) = self.engine.partition(&topic, partition) else {
                continue;
            };
            // Re-resolve the topic's tuning while we're here. In Apache
            // a `kafka-configs.sh --alter` on segment.bytes / segment.ms
            // applies to the live log; here the sweep is what carries a
            // `.config.json` edit onto an already-open partition, so the
            // lag is one interval instead of a broker restart.
            if let Some(cfg) = self.partition_config.as_ref() {
                let next = cfg(&topic, partition, &p.config());
                if next != *p.config() {
                    tracing::info!(
                        topic,
                        partition,
                        segment_bytes = next.segment_bytes,
                        segment_ms = ?next.segment_ms,
                        "topic config changed; applying to open partition"
                    );
                    p.set_config(next);
                }
            }

            let policy = self.policy_source.policy_for(&topic, partition);
            if policy.is_unlimited() {
                continue;
            }

            // Both knobs are ceilings on what may be retained, so the
            // effective target is whichever reaps more. Kafka applies
            // them independently and the union is what survives.
            let by_size = policy
                .retention_bytes
                .and_then(|b| p.cleanup_target_for_size_bytes(b));
            let by_time = policy.retention_ms.and_then(|ms| {
                let cutoff = now.saturating_sub(i64::try_from(ms).unwrap_or(i64::MAX));
                p.cleanup_target_for_time(self.engine.fs(), cutoff)
            });
            let Some(target) = by_size.max(by_time) else {
                continue;
            };
            // delete_records is idempotent under "target <= log_start".
            // Skip the call when the target would be a no-op.
            if target <= p.log_start_offset() {
                continue;
            }
            // Attribute the reclaim to whichever knob actually drove
            // the chosen target; a tie is reported as size, matching
            // the order Kafka evaluates them in.
            let reason = if Some(target) == by_size {
                "size"
            } else {
                "time"
            };
            let before = p.partition_size();
            p.delete_records(target).await?;
            let after = p.partition_size();
            let reclaimed = before.saturating_sub(after);
            cleaned += 1;
            tracing::info!(
                topic,
                partition,
                target,
                reason,
                reclaimed_bytes = reclaimed,
                "retention advanced log start offset"
            );
            let m = kaas_observability::metrics::global();
            m.cleaner_segments_deleted
                .add(1, &[kaas_observability::KeyValue::new("reason", reason)]);
            if reclaimed > 0 {
                m.cleaner_bytes_reclaimed.add(
                    u64::try_from(reclaimed).unwrap_or(0),
                    &[kaas_observability::KeyValue::new("reason", reason)],
                );
            }
        }
        Ok(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::StorageEngine;
    use crate::fs::{Fs, RealFs};
    use crate::partition::PartitionConfig;
    use bytes::Bytes;
    use std::path::PathBuf;

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    fn build_batch(num_records: i32, max_timestamp: i64) -> Bytes {
        let body_size = 49 + 16;
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&0i64.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        buf[16] = 2;
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        buf[35..43].copy_from_slice(&max_timestamp.to_be_bytes());
        buf[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        crate::segment::stamp_crc(&mut buf);
        Bytes::from(buf)
    }

    fn engine_at(dir: PathBuf, segment_bytes: u64) -> Arc<DiskStorageEngine> {
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        Arc::new(DiskStorageEngine::new(
            fs,
            dir,
            PartitionConfig {
                segment_bytes,
                ..Default::default()
            },
        ))
    }

    #[test]
    fn no_op_when_total_under_cap() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            for _ in 0..3 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(1 << 30),
                    retention_ms: None,
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn drops_oldest_closed_segments_when_over_cap() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            // Roll after each batch — each closed segment is exactly
            // one batch worth.
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            // 5 appends → at least 4 closed segments + 1 active.
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            // Keep only the last batch's worth of data.
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(one_len),
                    retention_ms: None,
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 1, "exactly one partition was cleaned");
            // log_start should have advanced past at least 3 records.
            assert!(
                e.log_start_offset("t", 0).unwrap() >= 3,
                "expected log_start to advance past dropped batches"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn no_policy_means_no_cleanup() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: None,
                    retention_ms: None,
                },
            );
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn cleaner_is_idempotent_under_repeat_passes() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..5 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let cleaner = RetentionCleaner::new(
                e.clone(),
                RetentionPolicy {
                    retention_bytes: Some(one_len * 2),
                    retention_ms: None,
                },
            );
            // First pass cleans; subsequent passes are no-ops.
            let a = cleaner.run_once().await.unwrap();
            let b = cleaner.run_once().await.unwrap();
            let c = cleaner.run_once().await.unwrap();
            assert_eq!(a, 1);
            assert_eq!(b, 0);
            assert_eq!(c, 0);
            e.relinquish_all().await.unwrap();
        });
    }

    /// Roll a segment per batch so every batch is its own closed
    /// segment, with `stamp_ms` as its record timestamp.
    async fn seed_segments(e: &Arc<DiskStorageEngine>, n: usize, stamp_ms: i64) {
        for _ in 0..n {
            e.append("t", 0, 0, -1, build_batch(1, stamp_ms))
                .await
                .unwrap();
        }
    }

    fn time_only(retention_ms: u64) -> RetentionPolicy {
        RetentionPolicy {
            retention_bytes: None,
            retention_ms: Some(retention_ms),
        }
    }

    #[test]
    fn time_retention_drops_segments_past_the_cutoff() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            // Records stamped at t=1_000_000; 5 appends → 4 closed
            // segments carrying a known max_timestamp + 1 active.
            seed_segments(&e, 5, 1_000_000).await;

            // "Now" is one hour later, retention is one minute — every
            // closed segment has aged out.
            let cleaner = RetentionCleaner::new(e.clone(), time_only(60_000))
                .with_clock(Arc::new(|| 1_000_000 + 3_600_000));
            let cleaned = cleaner.run_once().await.unwrap();
            assert_eq!(cleaned, 1, "the partition was cleaned");
            assert!(
                e.log_start_offset("t", 0).unwrap() >= 4,
                "expected log_start past every closed segment, got {}",
                e.log_start_offset("t", 0).unwrap()
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn time_retention_keeps_records_inside_the_window() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            seed_segments(&e, 5, 1_000_000).await;

            // Now is 30 s later against a 1 h retention: nothing is old
            // enough, and the pass must not touch the log.
            let cleaner = RetentionCleaner::new(e.clone(), time_only(3_600_000))
                .with_clock(Arc::new(|| 1_000_000 + 30_000));
            assert_eq!(cleaner.run_once().await.unwrap(), 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn time_retention_stops_at_the_first_segment_still_in_window() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            // Two old segments, then two recent ones. Retention must
            // stop at the first in-window segment rather than reaping
            // every old segment it can find — log start has to stay
            // contiguous.
            seed_segments(&e, 2, 1_000_000).await;
            seed_segments(&e, 3, 9_000_000).await;

            let cleaner = RetentionCleaner::new(e.clone(), time_only(60_000))
                .with_clock(Arc::new(|| 9_030_000));
            assert_eq!(cleaner.run_once().await.unwrap(), 1);
            assert_eq!(
                e.log_start_offset("t", 0).unwrap(),
                2,
                "exactly the two aged-out segments were dropped"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn segments_inherited_across_a_restart_age_out_by_mtime() {
        // The production path after ANY restart. Nothing persists a
        // segment's largest timestamp — the manifest has no segment
        // list — so a segment this process didn't roll comes back from
        // `list_segments` with `max_timestamp: -1`, and retention has
        // to fall back to the log file's mtime (as Apache does). Every
        // other test here rolls its own segments, so this is the only
        // one exercising the arm a restarted broker actually uses.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            {
                let e = engine_at(tmp.path().to_path_buf(), one_len);
                seed_segments(&e, 5, 1_000_000).await;
                e.relinquish_all().await.unwrap();
            }
            // Fresh engine over the same directory — the restart.
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            let p = e.partition("t", 0).unwrap();
            let stamps = p.closed_segment_max_timestamps();
            assert_eq!(
                stamps.first().copied(),
                Some(crate::segment::UNKNOWN_MAX_TIMESTAMP),
                "the oldest segment is inherited, so its timestamp must be \
                 unrecorded — otherwise this test isn't exercising the mtime \
                 arm (stamps: {stamps:?})"
            );

            // The files were written moments ago, so date them against
            // a clock far in the future rather than sleeping.
            let ten_years = 10 * 365 * 24 * 3_600_000i64;
            let cleaner = RetentionCleaner::new(e.clone(), time_only(60_000))
                .with_clock(Arc::new(move || now_epoch_ms() + ten_years));
            assert_eq!(cleaner.run_once().await.unwrap(), 1);
            assert!(
                e.log_start_offset("t", 0).unwrap() > 0,
                "mtime fallback did not reap anything"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn a_restart_takeover_does_not_reap_fresh_data() {
        // gh #267, end to end: produce, graceful shutdown, fresh engine,
        // take-over, one sweep at the 7-day default. The take-over rolls
        // the adopted active segment closed before any append lands, and
        // that roll used to stamp a largest timestamp of 0 — epoch 1970
        // — so this very sweep reaped minutes-old data on every restart.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            {
                let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
                for _ in 0..3 {
                    e.append("t", 0, 0, -1, build_batch(1, now_epoch_ms()))
                        .await
                        .unwrap();
                }
                e.relinquish_all().await.unwrap();
            }
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            e.take_over("t", 0, 2).await.unwrap();
            let p = e.partition("t", 0).unwrap();
            assert_eq!(
                p.closed_segment_max_timestamps().len(),
                1,
                "the take-over must roll the adopted segment closed, or this \
                 test isn't exercising the path that reaped"
            );

            // Apache's (and kaas's advertised) retention.ms default.
            let cleaner = RetentionCleaner::new(e.clone(), time_only(604_800_000));
            assert_eq!(cleaner.run_once().await.unwrap(), 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            assert_eq!(e.high_watermark("t", 0).unwrap(), 3);
            assert!(
                !e.read("t", 0, 0, 1 << 20).await.unwrap().is_empty(),
                "records written before the restart must still be readable"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn a_topics_segment_bytes_reaches_the_partition() {
        // Apache applies a topic's segment.bytes to its log. kaas wrote
        // the value to .config.json, echoed it back through
        // DescribeConfigs, and handed every partition the engine-wide
        // default instead — so a topic asking for small segments never
        // rolled, never had a closed segment, and could not age out
        // however its retention was set.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            // Engine default is huge; the topic asks for one batch.
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            let dir = e.topic_dir("t", 0);
            std::fs::create_dir_all(&dir).unwrap();
            crate::topicconfig::write_topic_config(
                e.fs(),
                &dir,
                &crate::topicconfig::TopicConfigFile {
                    segment_bytes: Some(i64::try_from(one_len).unwrap()),
                    ..Default::default()
                },
            )
            .unwrap();

            seed_segments(&e, 5, 1_000_000).await;
            let p = e.partition("t", 0).unwrap();
            assert_eq!(p.config().segment_bytes, one_len, "topic override ignored");
            assert!(
                !p.closed_segment_max_timestamps().is_empty(),
                "the topic's segment.bytes should have rolled segments"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn segment_ms_rolls_a_segment_that_never_fills() {
        // The other half of making retention work on a low-volume
        // topic: without a time-driven roll the only segment is the
        // active one, which retention may never delete.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            let dir = e.topic_dir("t", 0);
            std::fs::create_dir_all(&dir).unwrap();
            crate::topicconfig::write_topic_config(
                e.fs(),
                &dir,
                &crate::topicconfig::TopicConfigFile {
                    // Roll on any append at least 1 ms after creation.
                    segment_ms: Some(1),
                    ..Default::default()
                },
            )
            .unwrap();

            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            let p = e.partition("t", 0).unwrap();
            assert_eq!(p.config().segment_ms, Some(1));
            assert!(
                p.closed_segment_max_timestamps().is_empty(),
                "nothing closed yet — one append, one active segment"
            );

            // Far below segment_bytes, so only the age arm can roll it.
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            assert_eq!(
                p.closed_segment_max_timestamps().len(),
                1,
                "segment.ms did not roll a segment far below segment.bytes"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn the_age_check_stats_a_segment_at_most_once() {
        // `age_ms` runs on the append path, once per produced batch,
        // and every partition comes back from a restart or takeover
        // with an adopted segment (no in-process creation time). A
        // non-memoised lookup is therefore a `stat` per batch against
        // the shared volume — measured as a ~5% produce regression
        // before this test existed.
        use crate::fs::{Fs, RealFs};
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Debug)]
        struct CountingFs {
            inner: RealFs,
            stats: AtomicUsize,
        }
        impl Fs for CountingFs {
            fn open_read(
                &self,
                p: &std::path::Path,
            ) -> std::io::Result<Box<dyn crate::fs::FileRead>> {
                self.inner.open_read(p)
            }
            fn open_write(
                &self,
                p: &std::path::Path,
                append: bool,
            ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
                self.inner.open_write(p, append)
            }
            fn create(
                &self,
                p: &std::path::Path,
            ) -> std::io::Result<Box<dyn crate::fs::FileWrite>> {
                self.inner.create(p)
            }
            fn fsync(&self, f: &mut dyn crate::fs::FileWrite) -> std::io::Result<()> {
                self.inner.fsync(f)
            }
            fn rename(&self, a: &std::path::Path, b: &std::path::Path) -> std::io::Result<()> {
                self.inner.rename(a, b)
            }
            fn remove(&self, p: &std::path::Path) -> std::io::Result<()> {
                self.inner.remove(p)
            }
            fn fsync_dir(&self, p: &std::path::Path) -> std::io::Result<()> {
                self.inner.fsync_dir(p)
            }
            fn mkdir_all(&self, p: &std::path::Path) -> std::io::Result<()> {
                self.inner.mkdir_all(p)
            }
            fn readdir(&self, p: &std::path::Path) -> std::io::Result<Vec<std::path::PathBuf>> {
                self.inner.readdir(p)
            }
            fn stat(&self, p: &std::path::Path) -> std::io::Result<std::fs::Metadata> {
                if p.extension().is_some_and(|e| e == "log") {
                    self.stats.fetch_add(1, Ordering::Relaxed);
                }
                self.inner.stat(p)
            }
        }

        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            // Lay down a segment, then reopen so it is *adopted* —
            // the arm with no in-process creation time.
            {
                let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
                e.relinquish_all().await.unwrap();
            }
            let fs = Arc::new(CountingFs {
                inner: RealFs::new(),
                stats: AtomicUsize::new(0),
            });
            let e = Arc::new(DiskStorageEngine::new(
                fs.clone(),
                tmp.path().to_path_buf(),
                crate::partition::PartitionConfig {
                    segment_bytes: 1 << 30,
                    ..Default::default()
                },
            ));
            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            let after_open = fs.stats.load(Ordering::Relaxed);

            for _ in 0..50 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let growth = fs.stats.load(Ordering::Relaxed) - after_open;
            assert_eq!(
                growth, 0,
                "50 appends issued {growth} log stats — the age check is \
                 re-statting per batch instead of memoising"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn segment_ms_minus_one_never_rolls_on_time() {
        let base = crate::partition::PartitionConfig::default();
        assert_eq!(
            base.segment_ms,
            Some(7 * 24 * 60 * 60 * 1000),
            "default must match Apache's 7-day segment.ms"
        );
        let disabled = crate::topicconfig::apply_to_partition_config(
            &crate::topicconfig::TopicConfigFile {
                segment_ms: Some(-1),
                ..Default::default()
            },
            &base,
        );
        assert_eq!(disabled.segment_ms, None, "-1 must disable the time roll");
        // An absent key leaves the engine default alone.
        let untouched = crate::topicconfig::apply_to_partition_config(
            &crate::topicconfig::TopicConfigFile::default(),
            &base,
        );
        assert_eq!(untouched.segment_ms, base.segment_ms);
    }

    #[test]
    fn a_config_change_reaches_an_already_open_partition() {
        // `kafka-configs.sh --alter segment.bytes` applies to the live
        // log in Apache. Here the sweep carries it.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let e = engine_at(tmp.path().to_path_buf(), 1 << 30);
            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            let p = e.partition("t", 0).unwrap();
            assert_eq!(p.config().segment_bytes, 1 << 30);

            let dir = e.topic_dir("t", 0);
            crate::topicconfig::write_topic_config(
                e.fs(),
                &dir,
                &crate::topicconfig::TopicConfigFile {
                    segment_bytes: Some(4096),
                    ..Default::default()
                },
            )
            .unwrap();

            let src = Arc::new(TopicConfigPolicySource::new(
                e.clone(),
                RetentionPolicy::default(),
            ));
            let src2 = src.clone();
            let cleaner = RetentionCleaner::with_policy_source(e.clone(), src)
                .with_partition_config(Arc::new(move |t, part, base| {
                    src2.partition_config_for(t, part, base)
                }));
            cleaner.run_once().await.unwrap();

            assert_eq!(
                p.config().segment_bytes,
                4096,
                "sweep did not carry the config change onto the open partition"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn retain_forever_sentinels_disable_both_knobs() {
        // `-1` on either knob is Kafka's retain-forever. The CRD allows
        // it (gh #33), and `0` reaches us as an unset field.
        assert_eq!(sentinel_to_option(-1), None);
        assert_eq!(sentinel_to_option(0), None);
        assert_eq!(sentinel_to_option(5), Some(5));

        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            seed_segments(&e, 5, 1_000_000).await;
            let cleaner = RetentionCleaner::new(e.clone(), RetentionPolicy::default())
                .with_clock(Arc::new(|| i64::MAX / 2));
            assert_eq!(cleaner.run_once().await.unwrap(), 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn a_partition_this_broker_does_not_lead_is_left_alone() {
        // Retention unlinks files on a shared volume; a non-leader
        // reaping is NFS substrate rule 3 (and silly-renames whatever
        // the real leader holds open, gh #76).
        struct OwnsNothing;
        impl OwnershipSource for OwnsNothing {
            fn owns_partition(&self, _t: &str, _p: i32) -> bool {
                false
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            seed_segments(&e, 5, 1_000_000).await;
            let cleaner = RetentionCleaner::new(e.clone(), time_only(1))
                .with_clock(Arc::new(|| 9_000_000))
                .with_ownership(Arc::new(OwnsNothing));
            assert_eq!(cleaner.run_once().await.unwrap(), 0);
            assert_eq!(e.log_start_offset("t", 0).unwrap(), 0);
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn topic_config_source_reads_the_file_and_honours_sentinels() {
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let e = engine_at(tmp.path().to_path_buf(), 1 << 20);
            e.append("t", 0, 0, -1, build_batch(1, 1_000))
                .await
                .unwrap();
            let defaults = RetentionPolicy {
                retention_bytes: Some(999),
                retention_ms: Some(888),
            };
            let src = TopicConfigPolicySource::new(e.clone(), defaults);

            // No file yet → defaults.
            assert_eq!(src.policy_for("t", 0), defaults);

            // A file overrides per key, and `-1` means forever.
            let dir = e.topic_dir("t", 0);
            crate::topicconfig::write_topic_config(
                e.fs(),
                &dir,
                &crate::topicconfig::TopicConfigFile {
                    retention_ms: Some(60_000),
                    retention_bytes: Some(-1),
                    ..Default::default()
                },
            )
            .unwrap();
            assert_eq!(
                src.policy_for("t", 0),
                RetentionPolicy {
                    retention_bytes: None,
                    retention_ms: Some(60_000),
                },
                "explicit -1 must win over the default, not fall back to it"
            );
            e.relinquish_all().await.unwrap();
        });
    }

    #[test]
    fn cleanup_target_lock_free_calculation() {
        // Direct test of Partition::cleanup_target_for_size_bytes
        // outside the cleaner orchestration.
        let tmp = tempfile::tempdir().unwrap();
        rt().block_on(async {
            let one_len = u64::try_from(build_batch(1, 1_000).len()).unwrap();
            let e = engine_at(tmp.path().to_path_buf(), one_len);
            for _ in 0..4 {
                e.append("t", 0, 0, -1, build_batch(1, 1_000))
                    .await
                    .unwrap();
            }
            let p = e.partition("t", 0).unwrap();
            // Three closed segments; active has the last record.
            // retention_bytes = one_len * 2 → keep ~2 segments worth.
            let target = p.cleanup_target_for_size_bytes(one_len * 2);
            assert!(target.is_some(), "expected a non-None cleanup target");
            // No cleanup when retention is huge.
            assert!(p.cleanup_target_for_size_bytes(1 << 30).is_none());
            e.relinquish_all().await.unwrap();
        });
    }
}
