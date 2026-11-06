//! Top-level storage error sentinels.
//!
//! Storage error sentinels —
//! Phase 2 commits map them onto wire error codes in `kaas-protocol`'s
//! Produce/Fetch handlers.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    /// The producer's epoch is older than the partition's current epoch.
    #[error("epoch mismatch")]
    EpochMismatch,

    /// Idempotent-producer guard: the batch's sequence has a gap after the
    /// producer's last one, or a new epoch doesn't start at 0. Wire error 45.
    #[error("out of order sequence number")]
    OutOfOrderSequence,

    /// Idempotent-producer guard: the batch's PID/epoch is older than
    /// what's tracked. Wire error 47.
    #[error("invalid producer epoch")]
    InvalidProducerEpoch,

    /// Idempotent-producer guard: the batch's (firstSeq, lastSeq) tuple
    /// is already in the dedupe window. Wire error 46.
    #[error("duplicate sequence number")]
    DuplicateSequence,

    /// A client's transactional batch with no open transaction on this
    /// partition and no live verification from the coordinator: the transaction
    /// ended while it was on its way (gh #299). Appending it would open a
    /// transaction nothing ever closes. Wire error 48.
    #[error("transactional batch outside an ongoing transaction")]
    InvalidTxnState,

    /// The fsync watchdog (gh #95) deadline elapsed. Subsequent Append
    /// calls fail fast until the engine drops the partition.
    #[error("storage stalled")]
    Stalled,

    /// Topic / partition does not exist in the engine.
    #[error("unknown topic or partition")]
    UnknownTopicOrPartition,
    /// The batch is larger than the topic's `segment.bytes`, so no segment
    /// can hold it. Apache's `RecordBatchTooLargeException`; wire error
    /// 18, `RECORD_LIST_TOO_LARGE`.
    #[error("record batch larger than segment.bytes")]
    RecordBatchTooLarge,

    /// Read offset is outside `[log_start_offset, high_watermark)`.
    #[error("offset out of range")]
    OffsetOutOfRange,

    /// Partition was relinquished (closed) while the request was in
    /// flight.
    #[error("partition closed")]
    Closed,

    /// gh #221 phase 3: the partition is mid-move between log dirs
    /// (AlterReplicaLogDirs). Brief window; clients see a retriable
    /// error and come back after the cutover.
    #[error("partition migrating between log dirs")]
    Migrating,

    /// gh #241: the topic directory on disk still carries a previous
    /// incarnation's identity stamp, so the operator has not reclaimed
    /// it yet. Opening it would mean serving the dead incarnation's
    /// segments *and* holding FDs the reclaim's rename-aside would move
    /// out from under us — every subsequent append landing in a
    /// `.deleting-*` copy nobody can read. Retriable: the gh #215
    /// reconcile re-drives take-over, and clients see the same
    /// retriable wire error as a log-dir move.
    #[error("topic dir belongs to a previous incarnation; not reclaimed yet")]
    StaleTopicDir,

    /// The engine doesn't implement this operation (e.g. log-dir
    /// moves on the in-memory dev engine).
    #[error("unsupported: {0}")]
    Unsupported(&'static str),

    /// Producer snapshot or manifest decoded as a future schema version
    /// that this binary doesn't understand. Recoverable — the caller
    /// starts fresh.
    #[error("unknown on-disk schema version: {0}")]
    UnknownSchemaVersion(i64),

    /// Underlying filesystem I/O error.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// JSON parse error from manifest, producer snapshot, or topic
    /// config.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
}
