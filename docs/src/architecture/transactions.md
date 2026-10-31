# Transactions & idempotence

Idempotent-producer dedupe, the transaction coordinator state machine on slot-sharded JSON files, and EOS v2 end to end.

In Apache Kafka, exactly-once rests on two pieces of broker machinery:
per-partition producer state (the idempotence dedupe window) and a
transaction coordinator whose state lives in the internal
`__transaction_state` topic. kaas keeps the first nearly verbatim and
replaces the second: there is no `__transaction_state` topic. Like
`__consumer_offsets`
([Consumer-group coordination](./consumer-groups.md)), it is an
internal topic replaced by plain JSON files on the shared volume — the
third substitution from the [introduction](../introduction.md).

None of this is exotic. The Java producer has enabled idempotence by
default since Kafka 3.0, so *every* `kafka-console-producer` invocation
exercises this machinery — it's hot-path, not an opt-in feature. Four
layers of state, all on the shared volume:

| Layer | Where it lives |
|---|---|
| PID allocation (`InitProducerId`) | a persisted block allocator, one file per broker under `/data/__cluster/producer_ids/`; transactional IDs get the same PID + `epoch+1` on rejoin |
| Per-partition dedupe | a 5-batch ring per PID, held in memory under the partition mutex |
| Snapshot persistence | `producer-state.snapshot` next to the partition manifest |
| Per-`transactional.id` state | slot-sharded `/data/__cluster/txn_state/slot-N.json` |

## Idempotent producer

`InitProducerId` (key 22) hands a non-transactional producer a fresh PID
at epoch 0. On every Produce, classification runs **under the partition
mutex, before append** against a per-PID ring of the last 5 batches —
mirroring the Java client's `max.in.flight.requests.per.connection=5`:

- **duplicate** → echo the cached `baseOffset`, no log write;
- **out-of-order sequence** → error 45 (`OUT_OF_ORDER_SEQUENCE_NUMBER`);
- **stale epoch** → error 47 (`INVALID_PRODUCER_EPOCH`);
- otherwise accept and advance the ring.

The ring survives leadership moves via `producer-state.snapshot`
(written on segment roll + relinquish, restored on take-over — see
[File-handle ownership](./file-handles.md)).

### PIDs are never reused

The dedupe ring is keyed by `(PID, epoch)`, so handing the same PID to
two different producers is not a cosmetic collision — the second one
inherits the first one's sequence history. Its batches are then either
**silently dropped** (sequence range matches a cached batch → classified
duplicate, stale base offset echoed, produce "succeeds", consumers read
nothing) or **rejected** with `OUT_OF_ORDER_SEQUENCE_NUMBER`. Both
failure modes have been observed in practice.

Apache Kafka draws PIDs from a global counter whose next block is
persisted (ZooKeeper's `/latest_producer_id_block`, KRaft's
`ProducerIdsRecord`). kaas has no metadata quorum (a stated
[non-goal](../compat/non-goals.md)), so it partitions the PID space by
broker ordinal instead:

```text
pid = (broker_id + 1) * 2^40 + local
```

Each broker is the single writer of its own slice *and* of its own
block file `/data/__cluster/producer_ids/kaas-<id>.json`, so there is
no cross-broker read-modify-write on the shared volume. `local`
advances in blocks of 1000, and the block end is persisted (tmp +
fsync + rename) **before** any PID in it is handed out — a crash can
only skip PIDs forward, never rewind. The `+ 1` keeps broker 0 clear of
the low PIDs an earlier in-memory allocator handed out, so an upgrade
can't collide with producer state already on the volume.

### Fencing across partitions and brokers

A transactional producer that reconnects gets the **same PID with
`epoch+1`** — fencing is the monotonic epoch, exactly Apache's KIP-360
contract. A v3+ request that carries the producer's claimed identity is
checked first: a stale claim gets `PRODUCER_FENCED` instead of a newer
epoch, and a retry of a claimed bump gets the same answer back (details
on the [KIP-360 page](../compat/kip/kip-360.md)). Two mechanisms make the
bump stick everywhere:

- **Cross-partition fence**: after every `epoch > 0` rejoin, the
  InitProducerId handler walks every local partition, advances the
  PID's epoch and clears its dedupe window — so a zombie batch from the
  old session is fenced even on partitions the new session hasn't
  touched yet.
- **Cross-broker fence broadcast**: the bump is appended to a
  per-broker fence log under `/data/__cluster/producer_fences/`; every
  peer polls the logs and applies the bumps it hasn't seen. Same
  shared-volume pattern as the marker queue below — no new RPC surface.

## Transaction state machine

Per-`transactional.id` state is slot-sharded across
`/data/__cluster/txn_state/slot-N.json` (50 slots,
`fnv1a(transactional.id) % 50` — the same 50 Apache Kafka defaults to
for `transaction.state.log.num.partitions`). The states a transaction
actually visits:

```mermaid
stateDiagram-v2
    [*] --> Empty : InitProducerId first allocation<br/>PID assigned, epoch 0
    Empty --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn<br/>stamps ongoingSinceMs
    Ongoing --> PrepareCommit : EndTxn(commit)<br/>partitions + groups retained —<br/>the durable dispatch set
    Ongoing --> PrepareAbort : EndTxn(abort)
    Ongoing --> PrepareAbort : timeout reaper, 10 s sweep<br/>ongoingSinceMs + transactionTimeoutMs elapsed<br/>epoch bump, dispatch set retained
    PrepareCommit --> CompleteCommit : every marker durable<br/>clears partitions + groups,<br/>staged offsets committed
    PrepareAbort --> CompleteAbort : every marker durable<br/>staged offsets discarded
    Ongoing --> PrepareAbort : InitProducerId rejoin<br/>old session fenced at epoch + 1,<br/>answers CONCURRENT_TRANSACTIONS
    CompleteCommit --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn<br/>next transaction begins
    CompleteAbort --> Ongoing : AddPartitionsToTxn /<br/>AddOffsetsToTxn
    CompleteCommit --> Empty : InitProducerId rejoin<br/>epoch + 1
    CompleteAbort --> Empty : InitProducerId rejoin<br/>epoch + 1
```

Facts the diagram compresses:

- The `Prepare*` states are the durability pivot, exactly as in Apache:
  a prepared entry **keeps its partition and group lists**, and that
  retained list *is* the durable record of "these markers still owe a
  write". A marker-dispatch failure, a coordinator crash, or a producer
  retry all re-derive the identical dispatch set from it; only the
  transition to `Complete*` — taken once every marker is durable —
  clears the lists and releases the staged offsets.
- `InitProducerId` on a **rejoin** keeps the same PID, bumps
  `epoch += 1` and lands the entry in `Empty`. If the entry is `Ongoing`,
  the rejoin aborts that transaction first: `PrepareAbort` at `epoch + 1`
  with its lists kept, ABORT markers written, and `CONCURRENT_TRANSACTIONS`
  back to the client, whose retry then bumps onto the clean entry (gh #201).
  Before that fix the new session inherited the open transaction, and its
  first commit committed the zombie's records too. A rejoin during
  `Prepare*` answers `CONCURRENT_TRANSACTIONS` until the markers land.
  Once the epoch reaches `i16::MAX - 1` the rejoin rotates to a fresh PID
  at epoch 0 instead; the last epoch is kept back for that fence bump.
- A retried `EndTxn` in the matching `Complete*` state is answered
  idempotently (no second transition); a direction mismatch returns
  `INVALID_TXN_STATE`, and `EndTxn` on `Empty` is `INVALID_TXN_STATE`
  too. Epoch mismatches return `PRODUCER_FENCED` everywhere.

## EndTxn: commit flow

Cross-broker marker dispatch goes through a queue on the shared volume —
there is **no** WriteTxnMarkers RPC between brokers. `EndTxn` is
**two-phase**: prepare, dispatch every marker, then complete. For a
peer-led partition, a durably written queue entry counts as a
dispatched marker (the peer's watcher retries until it applies); a
dispatch failure returns the retriable `COORDINATOR_NOT_AVAILABLE` and
leaves the transaction prepared, so a producer retry — or the
background reconcile below — re-derives the same dispatch set and
finishes the job.

```mermaid
flowchart TD
    producer["Producer: EndTxn(commit)"] --> handler["EndTxn handler on the txn coordinator broker<br/>ownership gate — otherwise NOT_COORDINATOR"]
    handler --> prepare["state store: prepare_end_txn<br/>Ongoing → PrepareCommit<br/>partitions + groups retained — the dispatch set<br/>persist slot-N.json (tmp + fsync + rename)"]
    prepare --> split{"leader of each<br/>txn partition?"}
    split -- "self-led" --> local["write COMMIT control batch directly<br/>append to the log, acks=-1"]
    split -- "peer-led" --> enqueue["marker queue enqueue<br/>marker_queue/to-&lt;broker&gt;/&lt;pid&gt;-&lt;epoch&gt;.json"]
    local --> complete["state store: complete_end_txn<br/>PrepareCommit → CompleteCommit<br/>clears partitions + groups, ongoingSinceMs = 0"]
    enqueue --> complete
    complete --> hook["offset hook, per recorded group<br/>commit → commit pending offsets<br/>abort → discard pending offsets"]
    hook --> respond["EndTxn response error_code=0"]
    split -- "any dispatch fails" --> retriable["respond COORDINATOR_NOT_AVAILABLE (retriable)<br/>entry stays PrepareCommit — reconcile finishes it"]
    enqueue -.-> watcher["peer broker's marker watcher<br/>polls its own to-&lt;self&gt;/ every 2 s"]
    watcher -.-> apply["applies marker as control-batch append<br/>to partitions it leads, then deletes the file"]
```

The offset hook fires on the complete transition — not the prepare —
so staged offsets only become visible to `OffsetFetch` once the
markers backing them are durable.

Self-led markers are written *before* the queue entries, so a
coordinator crash mid-dispatch never loses the local marker. A retried
`EndTxn` overwrites the same `{pid}-{epoch}.json` file — the queue is
idempotent by naming. Consumers in `read_committed` only see the
transaction's records once these markers land (the fetch path clamps to
the last stable offset).

## Coordinator routing and staged offsets

Which broker coordinates a transaction is the same deterministic hash
story as consumer groups: `hash(transactional.id)` picks the slot
owner, and non-coordinators answer the txn APIs with `NOT_COORDINATOR`
— see [Consumer-group coordination](./consumer-groups.md). On
coordinator failover the new owner simply reads the same slot file off
the shared volume: close-to-open consistency means the file *is* the
materialized state, with no log replay — this is the architectural
replacement for Apache's `__transaction_state` topic.

`TxnOffsetCommit` (key 28) stages consumer offsets in a **pending**
layer keyed by `(group ID, PID)` in the offset store — invisible to
`OffsetFetch` until `EndTxn` commits. `AddOffsetsToTxn` (key 25)
records which groups the transaction will touch, so the EndTxn offset
hook knows exactly which pending sets to commit or discard. That hook
firing atomically with the state transition is the KIP-447 (EOS v2)
contract.

## The timeout reaper and the marker reconcile

The transaction timeout reaper fires every 10 s — Apache's
`transaction.abort.timed.out.transaction.cleanup.interval.ms` default.
Any `Ongoing` entry past `ongoingSinceMs + transactionTimeoutMs`
transitions to **`PrepareAbort`** with an epoch bump — and, crucially,
keeps its partition and group lists: a timed-out transaction owes
ABORT markers exactly like a client-driven abort does.

A **marker reconcile** pass shares the same 10 s tick: it walks every
prepared transaction this broker coordinates, places the outstanding
markers, and runs the complete transition — which is when the staged
offsets are discarded (or committed) via the offset hook. The two
halves are complementary, not redundant: the EndTxn handler's inline
dispatch keeps commit latency off the sweep interval, but only the
reconcile can finish a transaction whose producer crashed, was fenced,
or (for a reaper abort) never existed to retry at all. A dispatch that
still fails is left prepared and retried next pass, deliberately
without bound.

Both sweeps are **ownership-gated**: a transaction slot file has
exactly one legal writer — its coordinator — so each broker reaps and
reconciles only the transactions it owns (an ungated sweep would have
every broker read-modify-writing the same slot files on the shared
volume, violating [the substrate rules](./nfs-substrate.md)). The gate
degrades safely at both edges: with no coordinator installed
(dev/single-broker) everything is owned, and in cluster mode nothing
is owned until the first assignment load — which delays a sweep by one
poll rather than skipping it.

## Implementation notes (for contributors)

- Dedupe ring: `crates/kaas-storage/src/idempotence.rs`
  (`ProducerStates`); snapshot persistence:
  `crates/kaas-storage/src/producer_snapshot.rs`.
- PID block allocator: `crates/kaas-broker/src/producer_id.rs`
  (gh #219 — both the silent-drop and the `OUT_OF_ORDER` symptoms of
  PID reuse were seen there; the pre-fix allocator was an in-memory
  `AtomicI64`).
- Fence-on-rejoin contract: gh #22; claimed-identity validation and
  abort-on-rejoin: gh #201 (`TxnStateStore::init_producer_id`, a port of
  Apache's `prepareInitProducerIdTransit`). Cross-broker fence broadcast
  (gh #108 phase 2): fence log in
  `crates/kaas-coordinator/src/fence_log.rs`, applied by each peer's
  `FenceWatcher` (`crates/kaas-broker/src/fence_watcher.rs`).
- Txn state store + slot sharding:
  `crates/kaas-coordinator/src/txn_state.rs` — the architectural answer
  to gh #29 (no literal `__transaction_state` topic).
- Marker queue: gh #175. Txn-slot hash ownership: gh #91. Two-phase
  EndTxn + the marker reconcile: gh #225, shared dispatch in
  `crates/kaas-broker/src/txn_markers.rs` (`reconcile_pending_markers`
  over `TxnStateStore::pending_marker_dispatches`).
- The reaper and reconcile are spawned by the broker's cluster runtime
  (`bins/kaas/src/cluster.rs`), both gated on `Broker::owns_txn`
  (`abort_overdue_owned` on the store side; the ungated
  `abort_overdue` is tests/dev-mode only).
- The full KIP-447 consume-process-produce-commit round trip runs
  against an in-process broker in `bins/kaas/tests/eos_v2.rs`.
