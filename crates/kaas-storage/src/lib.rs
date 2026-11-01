//! kaas-storage — the DiskStorageEngine.
//!
//! # Byte-opacity contract
//!
//! Record-batch bytes flow through `kaas-storage` as `bytes::Bytes` —
//! never decoded into a `Record` struct. The only module that will need
//! to inspect record contents is the log compactor (gh #158, not yet
//! built), and its granularity is per-batch.
//!
//! # Initial slice (Phase 2 commit 1)
//!
//! - [`fs`] / [`atomic_write`] / [`manifest`] / [`topicconfig`] — workstream A
//! - [`segment`] — workstream B-minimal (filename helpers + [`segment::SegmentMeta`])
//! - [`idempotence`] / [`producer_snapshot`] — workstream C
//!
//! The partition core ([`partition`]), `DiskStorageEngine` ([`engine`]),
//! recovery, cleaner, and compactor land in follow-up commits per the
//! Phase 2 plan.

pub mod atomic_write;
pub mod cleaner;
pub mod disk;
pub mod engine;
pub mod errors;
pub mod fs;
pub mod idempotence;
pub mod manifest;
pub mod memory;
pub mod partition;
pub mod producer_snapshot;
pub mod recovery_checkpoint;
pub mod segment;
pub mod topic_identity;
pub mod topicconfig;
pub mod txn_index;
pub mod txn_index_snapshot;

pub use cleaner::{
    FixedPolicySource, OwnershipSource, OwnsEverything, PolicySource, RetentionCleaner,
    RetentionPolicy, TopicConfigPolicySource,
};
pub use disk::DiskStorageEngine;
pub use engine::{
    fs_capacity, parse_log_dirs_json, LogDirInfo, PlacementResolver, StorageEngine,
    TopicIdentityResolver, TopicIncarnation, DEFAULT_LOG_DIR_NAME,
};
pub use errors::StorageError;
pub use fs::{Fs, RealFs};
pub use idempotence::{
    parse_batch_producer_info, parse_batch_txn_info, BatchProducerInfo, BatchTxnInfo, Outcome,
    ProducerEntry, ProducerStates, RecentBatch, RING_SIZE,
};
pub use manifest::Manifest;
pub use memory::{MemoryStorage, MEMORY_DATA_DIR};
pub use partition::{Partition, PartitionConfig, ReadSnapshot};
pub use producer_snapshot::{
    read_producer_snapshot, write_producer_snapshot, ProducerSnapshot, ProducerSnapshotEntry,
    PRODUCER_SNAPSHOT_FILENAME, PRODUCER_SNAPSHOT_VERSION,
};
pub use segment::{
    batch_attributes, list_segments, parse_batch_offsets, parse_segment_stem, read_batches,
    scan_high_watermark, scan_high_watermark_from, search_index, search_index_bytes,
    segment_index_path, segment_log_path, ActiveSegment, RolledTail, ScanResult, ScanStop,
    SegmentMeta, INDEX_ENTRY_SIZE,
};
pub use topic_identity::{
    classify as classify_topic_identity, read_topic_identity, write_topic_identity,
    IdentityVerdict, TopicIdentity, TOPIC_IDENTITY_FILENAME,
};
pub use topicconfig::{read_topic_config, write_topic_config, TopicConfigFile};
pub use txn_index::{AbortedTxn, AbortedTxnIndex, OpenTxnIndex};
