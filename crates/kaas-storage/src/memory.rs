//! In-memory [`StorageEngine`] backed by per-partition `Vec<Bytes>`.
//!
//! In-memory storage engine. Used by:
//!
//! - Dev mode (no `MY_POD_NAME` env): the broker boots with a
//!   `MemoryStorage` and serves Produce/Fetch in-process without
//!   touching disk.
//! - Unit tests that need a `StorageEngine` without disk setup.
//! - Phase 2 workstream H's cross-engine equivalence test: drive the
//!   same byte stream into `MemoryStorage` and `DiskStorageEngine`,
//!   assert `read` returns the same bytes from both.
//!
//! Not safe for production — data is lost on restart.
//!
//! # Byte-opacity
//!
//! [`MemoryStorage::append`] rewrites the first 8 bytes (baseOffset)
//! of every incoming batch to the partition's current HWM. The v2
//! RecordBatch CRC covers byte 21 onward, so this overwrite is wire-
//! correct. Records bytes (everything past the 61-byte header) are
//! never inspected or modified.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use dashmap::DashMap;

use crate::engine::StorageEngine;
use crate::errors::StorageError;

/// The `data_dir` sentinel returned for an in-memory engine.
pub const MEMORY_DATA_DIR: &str = "memory://";

#[derive(Debug, Clone)]
struct MemoryBatch {
    base_offset: i64,
    last_offset_delta: i32,
    raw: Bytes,
}

#[derive(Debug, Default)]
struct MemoryPartition {
    batches: Vec<MemoryBatch>,
    high_water: i64,
    log_start: i64,
}

#[derive(Debug)]
pub struct MemoryStorage {
    partitions: DashMap<(String, i32), parking_lot::Mutex<MemoryPartition>>,
    data_dir: PathBuf,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            partitions: DashMap::new(),
            data_dir: PathBuf::from(MEMORY_DATA_DIR),
        }
    }

    /// Get-or-create a partition slot. Returns the key entry so the
    /// caller can lock the inner state.
    fn ensure(&self, topic: &str, partition: i32) {
        self.partitions
            .entry((topic.to_owned(), partition))
            .or_default();
    }
}

#[async_trait]
impl StorageEngine for MemoryStorage {
    async fn append(
        &self,
        topic: &str,
        partition: i32,
        _epoch: u32,
        _acks: i16,
        batch: Bytes,
    ) -> Result<i64, StorageError> {
        if batch.is_empty() {
            return Ok(self.high_watermark(topic, partition).unwrap_or(0));
        }
        // Minimum batch wire length needed to read lastOffsetDelta at
        // [23..27]. Anything shorter isn't a valid v2 RecordBatch.
        if batch.len() < 27 {
            return Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("memory storage: batch too short: {} bytes", batch.len()),
            )));
        }

        self.ensure(topic, partition);
        let entry = self
            .partitions
            .get(&(topic.to_owned(), partition))
            .ok_or(StorageError::UnknownTopicOrPartition)?;
        let mut guard = entry.lock();

        // Brokers own offsets: rewrite baseOffset to current HWM (v2
        // CRC covers byte 21 onward, so this overwrite is wire-correct).
        let assigned = guard.high_water;
        let mut owned = BytesMut::with_capacity(batch.len());
        owned.extend_from_slice(&batch);
        owned[0..8].copy_from_slice(&assigned.to_be_bytes());

        let mut delta_bytes = [0u8; 4];
        delta_bytes.copy_from_slice(&owned[23..27]);
        let last_offset_delta = i32::from_be_bytes(delta_bytes);

        let frozen = owned.freeze();
        guard.batches.push(MemoryBatch {
            base_offset: assigned,
            last_offset_delta,
            raw: frozen,
        });
        guard.high_water = assigned + i64::from(last_offset_delta) + 1;
        Ok(assigned)
    }

    async fn read(
        &self,
        topic: &str,
        partition: i32,
        start_offset: i64,
        max_bytes: usize,
    ) -> Result<Bytes, StorageError> {
        let Some(entry) = self.partitions.get(&(topic.to_owned(), partition)) else {
            return Ok(Bytes::new());
        };
        let guard = entry.lock();
        let mut out = BytesMut::new();
        for b in &guard.batches {
            // Skip batches fully before start_offset.
            if b.base_offset + i64::from(b.last_offset_delta) < start_offset {
                continue;
            }
            // Honour log_start_offset: batches strictly below it are
            // invisible to Fetch even if start_offset asked for them.
            if b.base_offset + i64::from(b.last_offset_delta) < guard.log_start {
                continue;
            }
            // Whole batches only, at least the first (KIP-74, gh #287).
            if !out.is_empty() && out.len() + b.raw.len() > max_bytes {
                break;
            }
            out.extend_from_slice(&b.raw);
            if out.len() >= max_bytes {
                break;
            }
        }
        Ok(out.freeze())
    }

    fn high_watermark(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(entry) => Ok(entry.lock().high_water),
            None => Ok(0),
        }
    }

    fn log_start_offset(&self, topic: &str, partition: i32) -> Result<i64, StorageError> {
        match self.partitions.get(&(topic.to_owned(), partition)) {
            Some(entry) => Ok(entry.lock().log_start),
            None => Ok(0),
        }
    }

    fn offset_for_timestamp(
        &self,
        _topic: &str,
        _partition: i32,
        _timestamp_ms: i64,
    ) -> Result<(i64, i64), StorageError> {
        // MemoryStorage doesn't retain batch maxTimestamps. Sentinel
        // is the wire-correct "no matching record"
        // answer.
        Ok((-1, -1))
    }

    fn offset_for_leader_epoch(
        &self,
        _topic: &str,
        _partition: i32,
        _leader_epoch: i32,
    ) -> Result<(i32, i64), StorageError> {
        // No epoch tracking in memory storage; "nothing to truncate to".
        Ok((-1, -1))
    }

    async fn delete_records(
        &self,
        topic: &str,
        partition: i32,
        target_offset: i64,
    ) -> Result<i64, StorageError> {
        self.ensure(topic, partition);
        let entry = self
            .partitions
            .get(&(topic.to_owned(), partition))
            .ok_or(StorageError::UnknownTopicOrPartition)?;
        let mut guard = entry.lock();
        // KIP-107: target == -1 means "purge to HWM".
        let new_start = if target_offset < 0 {
            guard.high_water
        } else if target_offset > guard.high_water {
            return Err(StorageError::OffsetOutOfRange);
        } else {
            target_offset
        };
        if new_start > guard.log_start {
            guard.log_start = new_start;
        }
        Ok(guard.log_start)
    }

    async fn create_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        self.ensure(topic, partition);
        Ok(())
    }

    async fn delete_partition(&self, topic: &str, partition: i32) -> Result<(), StorageError> {
        self.partitions.remove(&(topic.to_owned(), partition));
        Ok(())
    }

    /// gh #219: nothing is persisted here, so dropping the entries is
    /// the whole job — but it still has to happen, or a recreated topic
    /// keeps serving the deleted one's records in dev mode.
    async fn abandon_topic(&self, topic: &str) -> usize {
        let keys: Vec<(String, i32)> = self
            .partitions
            .iter()
            .map(|kv| kv.key().clone())
            .filter(|(t, _)| t == topic)
            .collect();
        let dropped = keys.len();
        for key in keys {
            self.partitions.remove(&key);
        }
        dropped
    }

    fn partition_size(&self, topic: &str, partition: i32) -> i64 {
        // No segment files; partition size is 0 by convention.
        let _ = (topic, partition);
        0
    }

    fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    async fn take_over(
        &self,
        topic: &str,
        partition: i32,
        _epoch: u32,
    ) -> Result<i64, StorageError> {
        self.ensure(topic, partition);
        self.high_watermark(topic, partition)
    }

    async fn relinquish(&self, _topic: &str, _partition: i32) -> Result<(), StorageError> {
        Ok(())
    }

    fn open_partition_keys(&self) -> Vec<(String, i32)> {
        self.partitions.iter().map(|kv| kv.key().clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal v2 RecordBatch carrying `n` records and a
    /// caller-supplied baseOffset (which the engine will rewrite).
    /// Bytes outside the offset / delta fields are zeros — records
    /// payload is never inspected.
    fn build_batch(base_offset: i64, num_records: i32) -> Bytes {
        let body_size = 49 + 16; // header tail + 16-byte filler payload
        let total = 12 + body_size;
        let mut buf = vec![0u8; total];
        buf[0..8].copy_from_slice(&base_offset.to_be_bytes());
        let body_len_i32 = i32::try_from(body_size).unwrap();
        buf[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        // magic = 2 at offset 16
        buf[16] = 2;
        // lastOffsetDelta at [23..27] = num_records - 1
        let last_offset_delta = num_records - 1;
        buf[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
        Bytes::from(buf)
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    // --- Append / Read --------------------------------------------------

    #[test]
    fn append_returns_assigned_base_offset() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            let raw = build_batch(999, 1);
            // Producer's claim of base_offset=999 is ignored — engine
            // rewrites to current HWM (0 for a fresh partition).
            let base = s.append("t", 0, 0, -1, raw).await.unwrap();
            assert_eq!(base, 0);
            assert_eq!(s.high_watermark("t", 0).unwrap(), 1);
        });
    }

    #[test]
    fn sequential_appends_advance_hwm_by_record_count() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            let assigned = [
                s.append("t", 0, 0, -1, build_batch(0, 5)).await.unwrap(),
                s.append("t", 0, 0, -1, build_batch(0, 3)).await.unwrap(),
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap(),
            ];
            assert_eq!(assigned, [0, 5, 8]);
            assert_eq!(s.high_watermark("t", 0).unwrap(), 9);
        });
    }

    #[test]
    fn read_returns_bytes_from_start_offset() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            for _ in 0..3 {
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            }
            let got = s.read("t", 0, 1, 4096).await.unwrap();
            // Should skip the first batch (last offset = 0) and return
            // the second + third.
            assert_eq!(got.len(), build_batch(0, 1).len() * 2);
        });
    }

    #[test]
    fn read_returns_empty_for_unknown_partition() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            let got = s.read("unknown", 0, 0, 4096).await.unwrap();
            assert!(got.is_empty());
        });
    }

    #[test]
    fn read_honours_max_bytes_cap() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            let raw = build_batch(0, 1);
            let one_len = raw.len();
            for _ in 0..5 {
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            }
            // Cap below one batch's worth → returns one batch and stops.
            let got = s.read("t", 0, 0, one_len - 1).await.unwrap();
            assert_eq!(got.len(), one_len);
        });
    }

    #[test]
    fn append_rewrites_base_offset_in_stored_bytes() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(999, 1)).await.unwrap();
            let bytes = s.read("t", 0, 0, 4096).await.unwrap();
            // First 8 bytes of the stored batch should be the assigned
            // base_offset (0), not the producer's 999.
            let mut tmp = [0u8; 8];
            tmp.copy_from_slice(&bytes[0..8]);
            assert_eq!(i64::from_be_bytes(tmp), 0);
        });
    }

    // --- Partition isolation -------------------------------------------

    #[test]
    fn appends_on_different_partitions_are_isolated() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(0, 2)).await.unwrap();
            s.append("t", 1, 0, -1, build_batch(0, 5)).await.unwrap();
            assert_eq!(s.high_watermark("t", 0).unwrap(), 2);
            assert_eq!(s.high_watermark("t", 1).unwrap(), 5);
        });
    }

    #[test]
    fn high_watermark_unknown_partition_is_zero() {
        let s = MemoryStorage::new();
        assert_eq!(s.high_watermark("nope", 0).unwrap(), 0);
        assert_eq!(s.log_start_offset("nope", 0).unwrap(), 0);
    }

    // --- create / delete partition --------------------------------------

    #[test]
    fn create_then_delete_partition() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.create_partition("t", 0).await.unwrap();
            assert_eq!(s.high_watermark("t", 0).unwrap(), 0);
            s.delete_partition("t", 0).await.unwrap();
            // Reads on a deleted partition return empty.
            let got = s.read("t", 0, 0, 4096).await.unwrap();
            assert!(got.is_empty());
        });
    }

    // --- DeleteRecords --------------------------------------------------

    #[test]
    fn delete_records_advances_log_start() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            for _ in 0..5 {
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            }
            let new_start = s.delete_records("t", 0, 3).await.unwrap();
            assert_eq!(new_start, 3);
            assert_eq!(s.log_start_offset("t", 0).unwrap(), 3);
            // Read from offset 0 should now skip batches with last
            // offset < log_start (so batches 0..3 are hidden).
            let got = s.read("t", 0, 0, 4096).await.unwrap();
            assert_eq!(got.len(), build_batch(0, 1).len() * 2);
        });
    }

    #[test]
    fn delete_records_purge_to_hwm_with_negative_one() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            for _ in 0..4 {
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            }
            let new_start = s.delete_records("t", 0, -1).await.unwrap();
            assert_eq!(new_start, 4);
            assert_eq!(s.log_start_offset("t", 0).unwrap(), 4);
        });
    }

    #[test]
    fn delete_records_past_hwm_is_offset_out_of_range() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            let err = s.delete_records("t", 0, 999).await.unwrap_err();
            assert!(matches!(err, StorageError::OffsetOutOfRange));
        });
    }

    #[test]
    fn delete_records_never_rewinds_log_start() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            for _ in 0..10 {
                s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            }
            s.delete_records("t", 0, 5).await.unwrap();
            // A lower target must not rewind.
            let r = s.delete_records("t", 0, 3).await.unwrap();
            assert_eq!(r, 5);
            assert_eq!(s.log_start_offset("t", 0).unwrap(), 5);
        });
    }

    // --- Sentinels ------------------------------------------------------

    #[test]
    fn offset_for_timestamp_sentinel() {
        let s = MemoryStorage::new();
        assert_eq!(s.offset_for_timestamp("t", 0, 1_000_000).unwrap(), (-1, -1));
    }

    #[test]
    fn offset_for_leader_epoch_sentinel() {
        let s = MemoryStorage::new();
        assert_eq!(s.offset_for_leader_epoch("t", 0, 5).unwrap(), (-1, -1));
    }

    // --- Misc -----------------------------------------------------------

    #[test]
    fn partition_size_is_zero() {
        let s = MemoryStorage::new();
        assert_eq!(s.partition_size("t", 0), 0);
    }

    #[test]
    fn data_dir_sentinel() {
        let s = MemoryStorage::new();
        assert_eq!(s.data_dir().to_str().unwrap(), "memory://");
    }

    #[test]
    fn take_over_returns_current_hwm() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(0, 3)).await.unwrap();
            let hwm = s.take_over("t", 0, 1).await.unwrap();
            assert_eq!(hwm, 3);
        });
    }

    #[test]
    fn relinquish_is_noop() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(0, 1)).await.unwrap();
            s.relinquish("t", 0).await.unwrap();
            // Data still accessible afterwards.
            assert_eq!(s.high_watermark("t", 0).unwrap(), 1);
        });
    }

    #[test]
    fn empty_batch_returns_current_hwm() {
        rt().block_on(async {
            let s = MemoryStorage::new();
            s.append("t", 0, 0, -1, build_batch(0, 2)).await.unwrap();
            let hwm = s.append("t", 0, 0, -1, Bytes::new()).await.unwrap();
            assert_eq!(hwm, 2);
        });
    }

    // --- StorageEngine trait object usability --------------------------

    #[test]
    fn arc_dyn_storage_engine_compiles() {
        let _s: std::sync::Arc<dyn StorageEngine> = std::sync::Arc::new(MemoryStorage::new());
    }
}
