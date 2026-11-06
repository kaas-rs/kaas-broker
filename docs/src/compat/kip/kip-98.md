# KIP-98 — exactly-once foundation

**Status: implemented** — see the [KIP index](../kip-index.md).

## What the KIP changes in Apache Kafka

KIP-98 (Kafka 0.11) is the exactly-once foundation, in two layers. The
**idempotent producer** gets a producer ID and epoch from
`InitProducerId` and stamps every batch with per-partition sequence
numbers, letting the broker deduplicate retries. **Transactions** add a
transaction coordinator (state in the `__transaction_state` internal
topic), atomic multi-partition writes terminated by COMMIT/ABORT control
batches, and a `read_committed` isolation level in which consumers only
see records below the last stable offset (LSO) and filter aborted
transactions. Every txn API — AddPartitionsToTxn, AddOffsetsToTxn,
TxnOffsetCommit, EndTxn, WriteTxnMarkers — originates here.

## How kaas implements it

The full state machine is documented in
[Transactions & idempotence](../../architecture/transactions.md); this
page maps the KIP's pieces to source and names the substitutions.

**Idempotent producer.** `InitProducerId` (key 22, v0–v4,
`crates/kaas-broker/src/handlers/init_producer_id.rs`) hands
non-transactional producers a fresh PID with epoch 0 from a per-broker
partitioned block allocator (`crates/kaas-broker/src/producer_id.rs`):
`pid = (broker_id + 1) · 2^40 + local`, with `local` advancing in
1000-PID blocks whose end is persisted before any PID in the block is
handed out — so a restart skips forward instead of reissuing, and
brokers can never collide (`transactional_id` of `""` counts as
non-transactional, the KIP-98 client convention). Per-partition dedupe lives in
`crates/kaas-storage/src/idempotence.rs`: a five-batch ring per PID —
sized to Java's `max.in.flight.requests.per.connection=5` — classified
under the partition mutex as duplicate (echo the cached `baseOffset`, no
log write), out-of-order (wire error 45), invalid epoch (wire 47), or
accept. Sequences wrap to 0, and a PID with no state accepts any sequence,
both as in Apache. Only the 57-byte v2 batch header is parsed; record
payloads stay opaque. The window survives restart via `producer-state.snapshot`
(`crates/kaas-storage/src/producer_snapshot.rs`), written on take-over
and close/relinquish beside `manifest.json`.

**Transactions — with three honest substitutions:**

- **No `__transaction_state` topic.** Coordinator state is slot-sharded
  JSON — `/data/__cluster/txn_state/slot-N.json`, 50 slots matching
  Apache's `transaction.state.log.num.partitions`
  (`crates/kaas-coordinator/src/txn_state.rs`). Failover is "open the
  file", no log replay. Rationale in [Non-goals](../non-goals.md).
- **Markers via the shared volume, not an RPC.** `EndTxn`
  (`crates/kaas-broker/src/handlers/end_txn.rs`) writes control batches
  directly to partitions this broker leads; for peers it enqueues one
  JSON file per `(pid, epoch, target)` under
  `/data/__cluster/marker_queue/to-<broker>/`
  (`crates/kaas-coordinator/src/marker_queue.rs`), which each broker's
  `marker_watcher` polls and applies
  (`crates/kaas-broker/src/marker_watcher.rs`). `EndTxn` returns success
  once the queue entry is written
  ([EndTxn](../api/transactions.md#endtxn)).
- **`read_committed` via LSO clamp.** The Fetch handler
  (`crates/kaas-broker/src/handlers/fetch.rs`) caps reads at the
  partition's last stable offset and returns `AbortedTransactions[]`
  from the aborted-txn index (`crates/kaas-storage/src/txn_index.rs`).

The remaining txn handlers (`add_partitions_to_txn.rs`,
`add_offsets_to_txn.rs`, `txn_offset_commit.rs`,
`write_txn_markers.rs` under `crates/kaas-broker/src/handlers/`) drive
the `Empty → Ongoing → PrepareCommit/PrepareAbort →
CompleteCommit/CompleteAbort` transitions. `EndTxn` is two-phase: the
prepare transition retains the partition and group lists as the durable
dispatch set, and the complete transition runs only once every marker is
durable — a dispatch failure leaves the entry prepared and retriable. A
10 s timeout reaper lands overdue transactions in `PrepareAbort` with an
epoch bump, and a marker-reconcile pass on the same tick dispatches
their ABORT markers and completes them. Epoch
fencing on producer rejoin is [KIP-360](kip-360.md); transactional
consume-process-produce offsets are [KIP-447](kip-447.md).

## How it's verified

`bins/kaas/tests/eos_v2.rs` runs the whole loop over real TCP with
hand-rolled wire bytes: `eos_commit_path_records_visible_to_read_committed`
and `eos_abort_path_populates_aborted_transactions`. Unit coverage:
`duplicate_in_window_returns_cached_offset`,
`a_pid_with_no_state_accepts_any_sequence`, `older_epoch_is_invalid`, and
`ring_caps_at_five_entries` in `idempotence.rs`;
`roundtrip_through_atomic_write` and
`future_version_is_dropped_not_misinterpreted` in `producer_snapshot.rs`;
`first_call_allocates_epoch_zero_rejoin_bumps`,
`end_txn_happy_commit_clears_partitions_and_fires_hook`,
`persistence_round_trip_across_open`, and
`reaper_aborts_overdue_bumps_epoch_fires_hook` in `txn_state.rs`.
Shell suite: `scripts/kafka-verifiable-producer.sh` (explicit
`enable.idempotence=true` run asserting no OutOfOrderSequence /
InvalidProducerEpoch), `scripts/kafka-txn-coordinator.sh`,
`scripts/kafka-txn-timeout.sh`, and `scripts/kafka-transactions.sh`.
