# kaas-storage

The disk storage engine: segments, manifest, cleaner, idempotent-producer state, the aborted-txn index, and the in-memory dev-mode engine.

> Read [Storage engine hot path](../architecture/storage-hot-path.md) and
> [File-handle ownership](../architecture/file-handles.md) **before** the
> engine code — the group-commit, manifest-lag, and single-FD semantics are
> deliberate and easy to misread as bugs from inside a single file.

**Module map**: `engine.rs` (the `StorageEngine` trait + `DiskStorageEngine`),
`disk.rs` (engine-level orchestration), `partition.rs` (the partition core:
mutex-guarded write path, ArcSwap read snapshot, per-partition committer
task), `segment.rs` (epoch-prefixed segment files + sparse index),
`manifest.rs` (tmp+fsync+rename state file), `cleaner.rs` (delete-policy
retention only — compaction is still unimplemented, gh #158) +
`topicconfig.rs` (retention/segment/compaction knobs from `.config.json`),
`recovery_checkpoint.rs` (the bounded-recovery-scan checkpoint, gh #230),
`topic_identity.rs` (the `.topic-id.json` incarnation stamp, gh #219),
`idempotence.rs` + `producer_snapshot.rs` (per-PID dedupe rings and their
persistence), `txn_index.rs` + `txn_index_snapshot.rs` (open-transaction and
aborted ranges for read-committed Fetch, and their persistence, gh #177),
`memory.rs` (dev-mode in-memory engine), `atomic_write.rs`,
`errors.rs`, `fs.rs` (the filesystem seam that lets tests fault-inject).
The crate also carries the multi-log-dir/volume-pool plumbing
(`parse_log_dirs_json`, the `PlacementResolver` seam — gh #221/#224).

**Invariants callers must hold**:

- **Single writer per partition** is enforced by coordinator ownership +
  epoch-prefixed filenames, not by the filesystem — calling `append` on a
  partition you don't own is a protocol violation upstream, not something
  the engine can fully defend against.
- **Batch bytes are opaque.** The engine peeks fixed-size headers
  (idempotence info, offsets) and rewrites the base offset in place;
  nothing may decode records.
- **The manifest lags by design** — recovery reconciles from the log on
  open; treating `manifest.json` as current truth mid-flight is a bug.
- **FDs belong to the leader**: `take_over` opens handles, `relinquish`
  closes them; holding handles elsewhere reintroduces the NFS silly-rename
  problem (gh #76).

**Start reading at** `partition.rs::append`, following one batch from
classification to the committer's `sync_all`.
