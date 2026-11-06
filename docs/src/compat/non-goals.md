# Non-goals

No KRaft, no replication/ISR, no internal topics, and no tiered storage (yet) — every omission with its rationale, not silence.

kaas keeps Kafka's wire contract but replaces Kafka's
distributed-systems machinery with Kubernetes primitives and a shared
filesystem. That trade shows up as **three deliberate substitutions**,
plus a short list of deferred features. This page is the ledger. Every
entry follows the same shape: *what Apache does* → *what kaas does
instead* → *why* → *what would change our mind* (where there's an
honest answer). If a parity task ever implicitly requires one of these,
the right move is to flag it, not to quietly grow the machinery.

## KRaft / metadata quorum

**Apache**: a Raft-based controller quorum (KRaft) replaced ZooKeeper as
the metadata store and controller-election mechanism.

**kaas**: a Kubernetes `Lease` (`kaas-controller`) elects the
controller; `leaseTransitions` is the monotonic epoch; the Kubernetes
API server is the metadata store
([details](../architecture/controller.md)).

**Why**: (a) the API server already *is* a replicated, consistent
metadata store — reimplementing one in-process duplicates that role for
no operational gain; (b) `holderIdentity` + `leaseTransitions` encode
"current controller + monotonic epoch" exactly as needed; (c) Raft
brings a peer gossip protocol and a large code surface the rest of the
broker has no use for.

**What would change our mind**: running kaas outside Kubernetes. That's
not on the roadmap — Kubernetes-native is the premise of the project.

## Replication / ISR

**Apache**: each partition is replicated across N brokers with an
in-sync replica set, leader election, and fencing RPCs.

**kaas**: single-writer-per-partition on shared `ReadWriteMany` storage;
the substrate provides durability and the epoch-prefixed segment
filenames provide split-brain safety by construction
([details](../architecture/storage-hot-path.md)).

**Why**: (a) ISR replication is most of what makes multi-broker Kafka
operationally hard — preferred-leader election, under-replicated alerts,
controlled-shutdown choreography; kaas trades that for the NFS server's
(already-solved) redundancy; (b) modern NFS/SAN substrates replicate at
the storage layer — replicating again in-broker doubles the write cost
for nothing; (c) a stale ex-leader physically *cannot* corrupt a new
leader's log, because it writes to segment files named with a dead
epoch.

**Consequence to be honest about**: broker loss makes its partitions
unavailable until the controller reassigns them (seconds), and storage
loss is data loss — durability is exactly as good as the substrate.
That's the contract; see
[Storage substrate requirements](../operations/storage.md).

## Internal topics

**Apache**: coordinator state lives in compacted internal topics —
consumer offsets in `__consumer_offsets`, transaction-coordinator state
in `__transaction_state` — replayed on coordinator failover, with the
partition leadership of those topics deciding which broker coordinates
which group or transaction.

**kaas**: plain JSON files on the shared volume. Transaction state is
slot-sharded (`txn_state/slot-N.json`, 50 slots — matching Apache's
default of 50 `__transaction_state` partitions;
[details](../architecture/transactions.md)); consumer offsets are one
file per group ([details](../architecture/consumer-groups.md)). The
coordinator routing Apache derives from internal-topic partition
leadership, kaas derives from a hash over the broker set.

**Why**: (a) without replication, an internal-topic-as-log buys nothing
over a file; (b) NFS close-to-open consistency means the file *is* the
materialized state — failover is "open the file", no replay; (c)
debuggability: a stuck transaction is `cat slot-N.json`.

## Tiered storage / S3 (KIP-405) — deferred, not refused

**Apache 3.6+**: remote log storage with a local hot tier.

**kaas**: no remote tier. The tiered-storage-only API surfaces
(`EARLIEST_LOCAL_TIMESTAMP`, `EARLIEST_PENDING_UPLOAD_OFFSET` in
ListOffsets) are deliberately skipped — clients only send them when
configured for remote tiers.

**Why**: the NFS substrate is already bulk-priced storage, and KIP-405
roughly doubles the cleanup/retention state machine.

**What would change our mind**: this is the one entry that's genuinely
*deferred* — an S3 backend is intended later, and the storage engine's
byte-opaque segments are designed not to preclude it.

## Fetch sessions (KIP-227) — stateless by contract

kaas answers every Fetch with `SessionID=0` — Apache's documented signal
for "broker doesn't support sessions" — so clients send full fetch state
per request. Echoing the client's session ID without maintaining session
state was an actual bug (clients sent incremental deltas against state
kaas didn't have and silently dropped partitions); `SessionID=0` is the
*correct* unsupported-marker, not a shortcut. The extra per-request CPU
is fine at kaas's scale; session caching is a future optimisation, not a
correctness gap.

## The rest of the tracked non-goal KIPs

- **KIP-48 (delegation tokens)** — token auth targets large multi-tenant
  clusters brokering their own trust; kaas deployments authenticate via
  SCRAM or mTLS backed by Kubernetes-managed secrets, or — for
  token-shaped needs — OAuth bearer tokens on an
  [OAUTHBEARER listener](../architecture/listeners-auth.md).
- **KIP-664 (Describe/ListTransactions)** — admin tooling over
  coordinator state; a follow-up. Until then the slot files on the
  volume are directly inspectable, which covers the debugging use case
  the KIP exists for.
- **KIP-714 (client metrics push)** — out of scope for the preview line;
  kaas's own observability is OTLP-push
  ([Observability](../architecture/observability.md)).
- **KIP-848 / KIP-1071 (next-gen rebalance)** — post-3.7 protocols; out
  of the 3.7 parity target by definition.
- **KIP-932 (share groups)** — Kafka 4.0+; the shell-tool suite marks
  the share-group tools as skipped with an explicit reason
  ([Verification story](verification.md)).

## Inter-broker surface

The Apache inter-broker/controller keys (LeaderAndIsr, StopReplica,
UpdateMetadata, ControlledShutdown, the KRaft quorum and Envelope
family) don't exist in kaas at all — there is no replication protocol to
drive and no quorum to speak. Apart from the Kubernetes API (the controller
Lease and the peer-endpoint watch), kaas brokers coordinate through exactly two
channels: the heartbeat gRPC stream and files on the shared volume
([Controller](../architecture/controller.md)).
