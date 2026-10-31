# Transaction APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

These six keys are the [KIP-98](../kip/kip-98.md) transactional-producer
surface plus the [KIP-447](../kip/kip-447.md) (EOS v2) offset-commit path.
The machinery behind them — the slot-file state store that replaces
Apache's `__transaction_state` topic, the shared-volume marker queue that
replaces coordinator-to-leader RPCs, and the timeout reaper — is described
in [Transactions & idempotence](../../architecture/transactions.md)
(including the state diagram); this page sticks to the wire contracts.

Four facts are shared by everything below:

- **Routing.** The transaction coordinator for a `transactional.id` is a
  pure function: `hash(transactional.id)` into the sorted full broker
  set, with a deterministic fallback into the alive subset when the
  preferred broker is down (`pick_txn_coordinator`,
  `crates/kaas-broker/src/group_hash.rs`). Clients resolve it via
  `FindCoordinator` with `key_type = 1`
  (`crates/kaas-coordinator/src/manager.rs`); the coordinator-side
  handlers (keys 22, 24, 25, 26) re-check ownership and answer
  `NOT_COORDINATOR` (16) from any other broker. Dev mode owns every id.
- **State.** Per-`transactional.id` state lives in
  `/data/__cluster/txn_state/slot-N.json`, sharded
  `fnv1a32(transactional.id) % 50` — Apache's
  `transaction.state.log.num.partitions=50` default
  (`crates/kaas-coordinator/src/txn_state.rs`). Every mutation re-reads
  the slot file and writes back atomically (tmp + fsync + rename), so
  coordinator failover is just the new owner reading the same file — no
  log replay. `EndTxn` is two-phase, as in Apache: `prepare_end_txn`
  moves `Ongoing → Prepare{Commit,Abort}` **retaining** the partition
  and group lists (the durable record of which markers still owe a
  write), and `complete_end_txn` clears them only once every marker is
  durable.
- **Errors.** The store's failures map identically in every
  coordinator-side handler: unknown id or PID mismatch →
  `INVALID_PRODUCER_ID_MAPPING` (49); epoch mismatch → `PRODUCER_FENCED`
  (90, the txn-coordinator convention — the Produce path keeps
  `INVALID_PRODUCER_EPOCH` 47); transition already in flight →
  `CONCURRENT_TRANSACTIONS` (51); store not yet wired at boot →
  `COORDINATOR_NOT_AVAILABLE` (15, retryable); empty `transactional.id`
  → `INVALID_REQUEST` (42). One honest wart: invalid transitions are
  answered with wire code **50**, but Apache's `INVALID_TXN_STATE` is
  **48** (50 is `INVALID_TRANSACTION_TIMEOUT`), so a Java client raises
  `InvalidTxnTimeoutException` where Apache raises
  `InvalidTxnStateException`. Off by label, not by behaviour.
- **Reaper.** A per-broker task fires every 10 s (Apache's
  `transaction.abort.timed.out.transaction.cleanup.interval.ms` default)
  and transitions `Ongoing` entries past
  `ongoingSinceMs + transactionTimeoutMs` to **`PrepareAbort`** with an
  epoch bump, keeping the dispatch set — a timed-out transaction owes
  ABORT markers like any other. A marker-reconcile pass on the same
  tick places the outstanding markers and completes the transaction,
  which is when staged offsets are discarded
  (`bins/kaas/src/cluster.rs`,
  `crates/kaas-broker/src/txn_markers.rs`). Both sweeps are
  **ownership-gated** on the txn-coordinator hash — each broker walks
  only its own slots; dev mode (no coordinator) owns everything.

One cross-cutting note up front: this surface is ACL-gated the way
Apache 3.7 gates it. Every transactional handler checks `WRITE` on the
`TransactionalId` resource (denial → `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`,
53) before coordinator routing is revealed; the offset-adjacent handlers
additionally check `READ` on the group (30) and — for `TxnOffsetCommit` —
`READ` per topic (29); `WriteTxnMarkers` requires `CLUSTER_ACTION` on the
Cluster resource (31). The one deliberate exception is the *idempotent*
`InitProducerId` path (empty `transactional.id`), which is not gated —
the Java client enables idempotence by default and Apache relaxed the
same gate in KIP-679; Produce's per-topic `WRITE` check is the
enforcement point.

## InitProducerId

Allocates the `(producer id, producer epoch)` pair — the entry point for
both idempotent and transactional producers. The Java client enables
idempotence by default since Kafka 3.0, so every producer sends this at
startup.

**Versions**: v0–v4 (flexible from v2).

**Handling** — a null or empty `transactional.id` is the idempotent
path: any broker answers locally with a fresh PID from its persisted
block allocator (below) and epoch 0, no coordinator gate. A non-empty `transactional.id`
hits the coordinator gate, then the state store: the first call for an
id allocates a fresh PID at epoch 0; **every reconnect returns the same
PID with `epoch + 1`** — fencing is the monotonic epoch, the KIP-98
contract as amended by [KIP-360](../kip/kip-360.md). Epoch exhaustion at
`i16::MAX - 1` rotates to a fresh PID at epoch 0. The request's
`transaction_timeout_ms` is recorded on the entry as the reaper's
deadline input.

The v3+ `producer_id` / `producer_epoch` fields are validated as the
caller's claimed identity, as in Apache: a half-set pair is
`INVALID_REQUEST`; a claim that is neither the current epoch (bump) nor
the one it was just bumped from (retry, same answer) is
`PRODUCER_FENCED`. A rejoin that finds an `Ongoing` transaction aborts it
first — ABORT markers at a bumped epoch — and answers
`CONCURRENT_TRANSACTIONS`; one that finds `Prepare*` answers
`CONCURRENT_TRANSACTIONS` until the markers land. Details on the
[KIP-360](../kip/kip-360.md) page.

After every `epoch > 0` bump the handler fences the old session twice
over: an in-process walk advances the PID's epoch and clears its dedupe
window on every partition this broker leads, and the bump is appended to
this broker's outbound fence file
(`/data/__cluster/producer_fences/from-<broker>.json`) so peer brokers'
`FenceWatcher` applies it within its 2 s poll. During the boot window
before the store is wired, the handler degrades gracefully: fresh PID,
epoch 0, and a one-shot warning that the rejoin fence is disabled.

**Deviations from Apache 3.7**:

- `transaction_timeout_ms` is not validated against a
  `transaction.max.timeout.ms` ceiling (Apache rejects oversized values
  with `INVALID_TRANSACTION_TIMEOUT`); kaas records whatever is sent.
- PID allocation is cluster-unique but by a different mechanism than
  Apache's. Apache allocates PID blocks through the quorum; kaas has no
  quorum, so it **partitions the PID space per broker**
  (`pid = (broker_id + 1) × 2⁴⁰ + local`) and persists each broker's
  block high-water to the shared volume **before** any PID in the
  block is handed out — a crash skips forward to a fresh block, never
  rewinds. No two brokers, and no two incarnations of one broker, can
  issue the same PID.

**Source**: `crates/kaas-broker/src/handlers/init_producer_id.rs`
(handler), `crates/kaas-codec/src/api/init_producer_id.rs` (codec),
`crates/kaas-coordinator/src/txn_state.rs` (store),
`crates/kaas-coordinator/src/fence_log.rs` +
`crates/kaas-broker/src/fence_watcher.rs` (cross-broker fence),
`crates/kaas-storage/src/idempotence.rs` (dedupe window it seeds).

**Verified by**: handler unit tests (same-PID/epoch-bump rejoin,
fence-log broadcast, empty-string id treated as non-transactional, and the
gh #201 set: `stale_claim_is_producer_fenced`,
`current_claim_bumps_and_its_retry_is_stable`,
`half_set_claim_is_invalid_request`,
`rejoin_mid_ongoing_aborts_it_and_answers_concurrent`);
`first_call_allocates_epoch_zero_rejoin_bumps`,
`epoch_overflow_rotates_to_fresh_pid`,
`epoch_exhaustion_rotates_one_short_of_max` and
`a_rejoin_mid_ongoing_prepares_an_abort_at_a_bumped_epoch` in
`crates/kaas-coordinator/src/txn_state.rs`; `bins/kaas/tests/eos_v2.rs`;
`scripts/kafka-txn-coordinator.sh` and `scripts/kafka-txn-timeout.sh`
(wire-surface probes — the Kafka 4.x CLI dropped
`--transactional-id` from the verifiable producer, so shell coverage is
ApiVersions plus on-PVC state checks).

## AddPartitionsToTxn

Declares the partitions a transaction will produce to, before the first
transactional batch lands there. The first successful `Add*` call is
what actually starts the transaction.

**Versions**: v0–v3 (flexible from v3).

**Handling** — after the shared gates, the store unions the requested
`(topic, partition)` tuples into the entry's partition list. Validation
order matches Apache: missing entry → 49, PID mismatch → 49, epoch
mismatch → 90, `Prepare*` in flight → 51. From `Empty` or a `Complete*`
state the entry transitions to `Ongoing` and **stamps `ongoingSinceMs`**
— the timeout reaper's deadline clock. Re-adding already-recorded
partitions with no state change is an idempotent no-op (no slot-file
rewrite). The v0–v3 response has no top-level error field, so a
top-level rejection (wrong coordinator, empty id, boot window) is
repeated on every requested partition; the Java client picks any one.

**Deviations from Apache 3.7**:

- Apache 3.7 additionally serves v4 — the KIP-890 phase-1 batched shape
  its brokers use for server-side verification. kaas stops at v3, which
  is the version client producers negotiate; nothing client-visible is
  missing.
- None known otherwise, beyond the shared warts in the preamble (wire
  code 50 for invalid transitions is unreachable here — bad states map
  to 49/90/51).

**Source**: `crates/kaas-broker/src/handlers/add_partitions_to_txn.rs`
(handler), `crates/kaas-codec/src/api/add_partitions_to_txn.rs` (codec),
`crates/kaas-coordinator/src/txn_state.rs` (`add_partitions`).

**Verified by**: handler unit tests (per-partition error fan-out, happy
path); `add_partitions_happy_path_then_idempotent`,
`add_partitions_unions_across_calls`, `epoch_mismatch_fences`, and
`add_partitions_concurrent_transition_rejected` in
`crates/kaas-coordinator/src/txn_state.rs`; `bins/kaas/tests/eos_v2.rs`;
`scripts/kafka-txn-coordinator.sh` (ApiVersions advertisement).

## AddOffsetsToTxn

Declares the consumer group whose offsets the transaction will commit —
what the Java client sends when `sendOffsetsToTransaction()` is called,
before the `TxnOffsetCommit` itself.

**Versions**: v0–v3 (flexible from v3).

**Handling** — after the shared gates, the store appends `group_id` to
the entry's group list (deduplicated; re-adding is a no-op). Exactly
like `AddPartitionsToTxn`, a call from `Empty` or `Complete*`
transitions the entry to `Ongoing` and stamps `ongoingSinceMs` — either
`Add*` API can open the transaction. The recorded group list is what
`EndTxn`'s offset hook later walks to commit or discard the pending
offsets staged by `TxnOffsetCommit`. The response is a single top-level
error code. An empty `group_id` is rejected through the
invalid-transition mapping (wire 50 — see the preamble wart).

**Deviations from Apache 3.7**:

- None known beyond the shared warts in the preamble (wire 50 where
  Apache uses 48).

**Source**: `crates/kaas-broker/src/handlers/add_offsets_to_txn.rs`
(handler), `crates/kaas-codec/src/api/add_offsets_to_txn.rs` (codec),
`crates/kaas-coordinator/src/txn_state.rs` (`add_offsets_to_txn`).

**Verified by**: handler unit tests (group recorded on happy path,
unknown producer → 49); `end_txn_happy_commit_clears_partitions_and_fires_hook`
in `crates/kaas-coordinator/src/txn_state.rs` (group list consumed by
the hook); `bins/kaas/tests/eos_v2.rs`;
`scripts/kafka-txn-coordinator.sh` (ApiVersions advertisement).

## EndTxn

Commits or aborts the transaction (`committed` boolean in the request).
This is where kaas diverges most visibly from Apache's internals while
keeping the client-visible contract.

**Versions**: v0–v3 (flexible from v3).

**Handling** — after the shared gates, the flow is **prepare →
dispatch → complete**, Apache's own shape. `prepare_end_txn` moves
`Ongoing → Prepare{Commit,Abort}`, deliberately retaining the
partition and group lists — the durable record of which markers still
owe a write. Marker dispatch then splits by partition leader (from
`assignment.json` via the broker `Coordinator`): **self-led
partitions** get the COMMIT/ABORT control batch built and appended
directly with `acks = -1`, *before* any queue writes, so a coordinator
crash mid-dispatch never loses the local marker; **peer-led
partitions** get one queue file per target broker under
`/data/__cluster/marker_queue/to-<broker>/<pid>-<epoch>.json` — a
durably written queue entry is the durability boundary for a peer
partition, since the peer's `MarkerWatcher` (2 s poll) retries until
the marker applies. Only once every marker is durable does
`complete_end_txn` run: `Prepare* → Complete*`, lists cleared,
`ongoingSinceMs` zeroed, and the offset hook fired per recorded group
(commit materialises the offsets `TxnOffsetCommit` staged, abort
discards them). Any dispatch failure answers the retriable
`COORDINATOR_NOT_AVAILABLE` (15) and leaves the entry prepared — a
producer retry or the 10 s marker reconcile re-derives the identical
dispatch set and finishes the job.

A retried `EndTxn` in the matching `Complete*` state is answered
idempotently (error 0, no second marker); a direction mismatch or
`EndTxn` against `Empty` returns wire 50 (intended
`INVALID_TXN_STATE`); an epoch mismatch returns `PRODUCER_FENCED`.
The queue file name makes producer retries overwrite rather than pile
up.

**Deviations from Apache 3.7**:

- **Peer markers are applied asynchronously.** Apache's coordinator
  drives `WriteTxnMarkers` RPCs and completes the transaction when every
  marker is *written to the partition log*; kaas completes once the
  queue entries land, so `read_committed` visibility (LSO advance) on
  peer-led partitions trails the `commitTransaction()` return by up to
  the 2 s poll.
- `CoordinatorEpoch` in emitted markers is always 0 (kaas tracks no txn
  coordinator epoch distinct from the assignment epoch); consumers do
  not act on the field.
- Wire 50 where Apache answers `INVALID_TXN_STATE` (48) — see preamble.

**Source**: `crates/kaas-broker/src/handlers/end_txn.rs` (handler),
`crates/kaas-codec/src/api/end_txn.rs` (codec),
`crates/kaas-broker/src/control_batch.rs` (marker batch),
`crates/kaas-broker/src/txn_markers.rs` (shared dispatch + reconcile),
`crates/kaas-coordinator/src/txn_state.rs` (`prepare_end_txn` /
`complete_end_txn`),
`crates/kaas-coordinator/src/marker_queue.rs` +
`crates/kaas-broker/src/marker_watcher.rs` (cross-broker dispatch).

**Verified by**: handler unit tests (commit appends a marker and
advances the HWM, idempotent retry writes no second marker, epoch
mismatch → 90, `Empty` → invalid); `end_txn_idempotent_retry_returns_ok`
and `end_txn_against_empty_is_invalid` in
`crates/kaas-coordinator/src/txn_state.rs`; queue round-trip and
overwrite-on-retry tests in `crates/kaas-coordinator/src/marker_queue.rs`;
`bins/kaas/tests/eos_v2.rs` (commit path visible to `read_committed`,
abort path populates `AbortedTransactions[]`).

## WriteTxnMarkers

Apache's inter-broker API: the transaction coordinator tells each
partition leader to write COMMIT/ABORT control batches. kaas serves the
**receiver side** for wire compatibility — but no kaas broker ever sends
it; cross-broker markers travel the shared-volume queue instead (see
`EndTxn` above).

**Versions**: v0–v1 (flexible from v1).

**Handling** — for each marker in the request the handler builds a
control batch from `(producer_id, producer_epoch, transaction_result,
coordinator_epoch)` and, per partition: checks leadership against the
assignment (`NOT_LEADER_OR_FOLLOWER`, 6, if this broker doesn't lead
it), runs an idempotent `create_partition` safety net, and appends with
`acks = -1`. Append failures map to `UNKNOWN_SERVER_ERROR` (-1)
per partition. Dev mode (no `Coordinator`) treats every partition as
self-led. An external coordinator or test harness driving this API gets
exactly Apache's receiver behaviour.

**Deviations from Apache 3.7**:

- The sender side does not exist: kaas coordinators dispatch markers via
  `/data/__cluster/marker_queue/`, never via this RPC. Invisible to
  clients (the API is broker-internal in Apache), but relevant when
  tracing a cluster on the wire.
- `CLUSTER_ACTION` on the Cluster resource is required, as in Apache
  (denial → `CLUSTER_AUTHORIZATION_FAILED` (31) on every partition). No
  kaas component holds that ACL — coordinators dispatch via the marker
  queue — so under `simple` authorization this API is effectively
  broker-only unless you grant `ClusterAction` to a harness principal
  deliberately.

**Source**: `crates/kaas-broker/src/handlers/write_txn_markers.rs`
(handler), `crates/kaas-codec/src/api/write_txn_markers.rs` (codec),
`crates/kaas-broker/src/control_batch.rs` (marker batch).

**Verified by**: handler unit tests (per-partition append advances the
HWM, empty marker list → empty response). No shell script drives it —
the Apache CLI tools never send this API, and kaas brokers don't either;
the queue path it replaces is exercised end to end by
`bins/kaas/tests/eos_v2.rs` and the marker-queue tests.

## TxnOffsetCommit

The transactional counterpart of `OffsetCommit`: stages the consume-side
offsets of a consume-process-produce cycle so they become visible
atomically with the transaction — the [KIP-447](../kip/kip-447.md)
(EOS v2) contract.

**Versions**: v0–v3 (flexible from v3).

**Handling** — this handler runs on the **group** coordinator
(`hash(group.id)`), not the transaction coordinator; a broker that
doesn't coordinate the group answers `NOT_COORDINATOR` on every
partition, as does the boot window before the manager is wired. Offsets
are flattened to the same key shape `OffsetCommit` uses and staged in
the offset store's **pending layer keyed by `(group_id, producer_id)`**
— invisible to `OffsetFetch` until `EndTxn(commit)` fires
`commit_pending`; abort (or a reaper sweep) fires `discard_pending`.
The pending layer is memory-only by design: staged offsets of an
unfinished transaction dying with the broker is abort-equivalent, which
is the correct outcome.

**Deviations from Apache 3.7**:

- `producer_epoch`, `generation_id`, and `member_id` are decoded but not
  validated. Apache fences zombies at this API with
  `INVALID_PRODUCER_EPOCH` / `ILLEGAL_GENERATION` /
  `UNKNOWN_MEMBER_ID`; kaas keys staging purely on
  `(group_id, producer_id)`.
- **Cross-broker gap**: the `EndTxn` offset hook fires on the *txn*
  coordinator's local offset store. When `hash(transactional.id)` and
  `hash(group.id)` resolve to different brokers, the pending entry
  staged here is never materialised — the group replays from its last
  committed offset, which breaks exactly-once (duplicates, not loss)
  for that group. Single-broker deployments and hash-coinciding cases
  are complete; cross-broker completion is an open follow-up (tracked
  as gh #114).

**Source**: `crates/kaas-broker/src/handlers/txn_offset_commit.rs`
(handler), `crates/kaas-codec/src/api/txn_offset_commit.rs` (codec),
`crates/kaas-coordinator/src/offset_store.rs` (pending layer),
`crates/kaas-coordinator/src/txn_state.rs` (`TxnOffsetHook` seam).

**Verified by**: handler unit tests (pending staged and invisible until
commit, `NOT_COORDINATOR` without a manager);
`pending_invisible_to_fetch_until_commit_pending` and
`discard_pending_drops_unmaterialised_offsets` in
`crates/kaas-coordinator/src/offset_store.rs`;
`bins/kaas/tests/eos_v2.rs` (staged offsets across commit and abort);
`scripts/kafka-txn-coordinator.sh` (ApiVersions advertisement).
