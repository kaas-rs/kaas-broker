# Consumer-group coordination

Deterministic hash routing of group coordination, two-tier ownership via `assignment.json`, and group takeover on assignment change.

In Apache Kafka, "which broker coordinates group G?" is a storage
question: `partitionFor(groupId)` hashes the group ID into the internal
`__consumer_offsets` topic, and whoever leads the resulting partition is
the group coordinator. Group metadata and committed offsets are records
in that topic; coordinator failover is partition-leadership failover.

kaas has no `__consumer_offsets` topic. It is one of the two internal
topics kaas replaces with plain JSON files on the shared volume — the
third substitution from the [introduction](../introduction.md); the
other is `__transaction_state`, covered in
[Transactions & idempotence](./transactions.md). Committed offsets live
in one file per group, and coordinator routing hashes the group ID
directly into the broker set instead of into a topic's partitions.

## The coordinator is a pure function

Routing is a stateless computation over the group ID, the broker set,
and the alive set — no lookup table, no election, no I/O:

- **Hash**: FNV-1a (32-bit) over the group ID, modulo the number of
  brokers. Clients never compute the coordinator themselves — they ask
  via `FindCoordinator`, exactly as against Apache Kafka — so any
  deterministic hash works as long as every broker computes the same
  one.
- **Stable divisor**: the modulus is pinned to the **full broker set**
  the controller knows about — including draining and dead brokers —
  *not* the alive count. Holding the divisor constant keeps existing
  assignments stable across rolling restarts; modding by the alive
  count would reshuffle roughly (N−1)/N of all groups on every pod
  up/down event.
- **Preferred-slot-down fallback**: when the hashed broker isn't alive,
  a deterministic alternate is picked from the alive subset, so
  coordination keeps working through a rolling restart.

The same machinery routes transaction coordination:
`hash(transactional.id)` picks the transaction coordinator, which is
what gates cross-broker transaction handling (see
[Transactions & idempotence](./transactions.md)).

## Two-tier ownership

A broker answers "do I coordinate group G?" in two tiers:

1. **Explicit entries win**: if the controller's `assignment.json`
   carries a `consumerGroups[]` entry for G, that broker is the
   coordinator. This is the controller's group-balancing output — and
   the forward-compat lever for sticky rebalancing.
2. **Hash fallback** otherwise: the pure function above, over the full
   broker set.

For stable broker sets the two tiers converge, so the hash is the
load-bearing path in steady state.

They can still disagree, though, and the disagreement is the reason
both tiers need care. An explicit entry sticks to its broker across
alive-set changes, so over time it can name a broker the hash would
not. That makes *losing* an entry a real event: the group silently
falls through to the hash, and if the hash says someone else, the
group's clients get `NOT_COORDINATOR` and rebuild from scratch — on a
perfectly healthy group, with no broker having restarted.

So an entry is retired only when its broker leaves the cluster, never
because the group went quiet. The controller learns which groups are
active from what brokers report, and a group drops out of that report
for reasons that have nothing to do with coordination: its members all
left for a moment, a heartbeat window went stale, an idle group was
swept from memory. None of those mean the coordinator should move.

The same rule binds every component that asks the question. Anything
deciding "do I still coordinate this group?" — including the takeover
pass below — has to consult *both* tiers, because a group living on
the hash tier alone (any brand-new group, for one) otherwise looks
unowned to whoever reads only the explicit list.

And it binds the controller hardest of all, in a way that is easy to
miss: **the entry it writes has to agree with the hash it is
replacing.** A brand-new group has no entry, so brokers serve it from
the hash. The moment the controller writes the group's first entry,
that entry becomes the answer. If the controller picked the
coordinator with a different function than the hash tier uses — even a
perfectly good one — then writing it *moves the group*, and the group's
clients see `NOT_COORDINATOR` and rebuild, once, shortly after they
start, for no reason a user could ever diagnose. Both sides resolve
through the same function over the same broker list, so the first entry
confirms rather than relocates.

## Group takeover and the orphan sweep

When the assignment changes — a broker joins, leaves, or dies — every
broker sweeps the groups it holds in memory and drops the ones the
*current* assignment no longer routes to it. Groups it gains are not
eagerly migrated: the new coordinator's first `JoinGroup` creates the
group lazily and loads its persisted offsets from the group's file on
the shared volume.

Note which set the sweep walks: **every** group resident in memory, not
the groups this broker believes it coordinates. Those are different
sets, and the second one is precisely the wrong one — filtering by
ownership before sweeping hides the disowned groups the sweep exists to
evict.

The sweep is what keeps memory bounded across alive-set churn, and it
is what keeps `kafka-consumer-groups.sh --list` honest: the AdminClient
unions `ListGroups` across all brokers, so a single broker holding one
forgotten in-memory group would make a deleted group reappear
cluster-wide.

Coordinator changes are logged on both brokers involved — the one
losing the group and the one gaining it — with the assignment version
that caused the move, so a client-visible rebalance can be traced back
to the recompute that triggered it.

Ownership also filters the read side: `ListGroups` on a broker only
returns groups it currently coordinates, and `DescribeGroups` for a
group owned elsewhere answers `NOT_COORDINATOR` — a stale entry on a
non-coordinator broker is never visible to clients.

## Where group state lives

- **Membership, generation, protocol state**: in-memory on the
  coordinator broker, exactly as in Apache Kafka. It is lost on
  coordinator failover — consumers rejoin and rebalance, which is
  what a coordinator change looks like to clients of Apache Kafka too.
- **Committed offsets**: one JSON file per group,
  `/data/__cluster/__consumer_offsets/<groupID>.json`, on the shared
  volume (a group ID can be any string, so `/`, NUL and `%` are
  percent-encoded in the file name). Durable across failover: where an
  Apache Kafka coordinator
  replays its `__consumer_offsets` partition to materialize offsets,
  the new kaas coordinator simply reads the same file — the file *is*
  the materialized state.
- **Transactional offsets** are staged in a pending layer keyed by
  `(group ID, producer ID)` and only become visible to `OffsetFetch`
  when the transaction commits — see
  [Transactions & idempotence](./transactions.md).

## Writing offsets

Apache appends a commit to `__consumer_offsets`, and the log gives each
writer its own place. kaas rewrites a group's whole file, which makes three
things its own problem:

- **A stale coordinator must not overwrite a newer one.** Every write
  carries a stamp of the controller epoch and assignment version, and
  refuses to replace a file stamped by a newer assignment. Without it, a
  coordinator that hadn't yet noticed a handoff would erase partitions
  committed only at the new one.
- **Concurrent commits must not drop each other's offsets.** One lock per
  group covers reading the cache, writing the file and updating the cache,
  and the cache moves only after the write lands. So a failed commit leaves
  fetches answering what's on disk, and the retry finds the group as it was.
- **An idle consumer must not rewrite the file.** A commit that moves
  nothing skips the write.

A file that can't be *read* isn't "no offsets": OffsetFetch, DescribeGroups
and DeleteGroups answer `COORDINATOR_LOAD_IN_PROGRESS` (14). Answering `-1`
would reset the consumer per `auto.offset.reset`.

Deleting a topic removes its committed offsets, as Apache's tombstones do.
Without that, a delete and recreate under the same name (Kafka Streams'
`application-reset` does this on every run) hands the new topic the old
one's offsets, and if the new log is shorter the group resumes past its end
and idles with no error on either side. Each broker purges only the groups
it coordinates, since a group's file has one legal writer. A commit racing
the delete checks the topic again under the group's lock and answers
`UNKNOWN_TOPIC_OR_PARTITION` (3) if it's gone, which clients retry.

This work runs on a bounded set of blocking threads, never on the runtime's
workers. The stores do synchronous I/O behind locks, and waiting on someone
else's fsync on a worker would stall every connection that worker drives.

## Implementation notes (for contributors)

- Offset writes: `OffsetStore::write_group`, `write_and_cache` and
  `FenceStamp` in `crates/kaas-coordinator/src/offset_store.rs` (gh #167,
  gh #318). Unreadable files: `Manager::group_load_error` (gh #326). The
  purge: `Manager::purge_topic_offsets` (gh #240, gh #331), and the re-check
  under the lock: `OffsetStore::set_partition_source` (gh #335). A commit
  that waits out a delete *and* a recreate can still write the dead
  incarnation's offset; closing that needs offsets that know their topic's
  incarnation (gh #105).
- Off the workers: `Broker::coordinator_io` and `txn_store_io` in
  `crates/kaas-broker/src/broker.rs`, bounded by `COORDINATOR_IO_THREADS`
  (gh #328, gh #332).
- Routing: `crates/kaas-broker/src/group_hash.rs` — pure functions over
  `(key, brokers, alive)`, no state, no I/O (gh #92). The same hash
  gates txn-slot ownership (gh #91).
- The coordinator manager's group-assignment source is **hot-swapped**
  from `bins/kaas/src/cluster.rs` after the broker `Coordinator` boots;
  the bootstrap source is an always-true local stub for the brief
  window before the cluster runtime is up (tests substitute their own).
  Don't unwire the swap: an earlier attempt (v0.1.52) hit the
  chicken-and-egg where strict coordinator checks blocked fresh-group
  bootstrap, and was reverted in v0.1.53 (gh #92).
- Takeover: `GroupTakeoverDriver`
  (`crates/kaas-broker/src/group_takeover.rs`) runs a single sweep over
  `Manager::resident_groups()` on every assignment change — the old
  prev→next diff is subsumed (only gain-side logging still compares
  `prev`); the sweep fixed the gh #89 stale-`--list` symptom. Lazy
  group creation is `Manager::get_or_create`.
- Read-side ownership filtering: `Manager::list_groups()` /
  `describe_groups()` in `crates/kaas-coordinator/src/manager.rs`.
- State: membership/generation in `crates/kaas-coordinator/src/group.rs`;
  offset persistence in `crates/kaas-coordinator/src/offset_store.rs`.
