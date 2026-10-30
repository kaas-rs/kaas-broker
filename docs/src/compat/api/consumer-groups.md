# Consumer-group APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

Every API on this page except [FindCoordinator](#findcoordinator) is answered
only by the group's coordinator broker; any other broker returns
`NOT_COORDINATOR` (16) and the client re-resolves. Coordinator-of-G is
two-tier: an explicit `assignment.json.consumerGroups[]` entry wins, otherwise
a deterministic FNV-1a 32-bit hash of the group ID mod the **full** broker set
(not the alive count), with a deterministic fallback into the alive subset
when the preferred slot is down.
Apache answers the same question with `__consumer_offsets` partition
leadership; kaas has no `__consumer_offsets` topic, so it hashes directly into
the broker set — see [Consumer-group
coordination](../../architecture/consumer-groups.md) for the full routing
story.

Where the state lives: membership, generation, and rebalance state are
**in-memory** on the coordinator — lost on coordinator failover, after
which consumers rejoin and rebalance, which is exactly what an Apache
coordinator move looks like to clients.
Committed offsets are **durable** per-group JSON files at
`/data/__cluster/__consumer_offsets/<groupID>.json` on the shared volume;
the next coordinator reads the same file. The offsets file is lazily loaded into memory when a group
first joins on a broker (`Manager::get_or_create`) — a caveat for
manual-assignment consumers is noted under [OffsetFetch](#offsetfetch).

During the boot window before the coordinator `Manager` is installed,
handlers return `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE` so clients
retry — no request is silently dropped.

**Authorization** (gh #268) follows Apache 3.7 under `authorization: simple`.
The group check runs before coordinator routing, so a denied principal never
learns where a group lives or whether it exists:

| API | Group | Topic | On deny |
|---|---|---|---|
| FindCoordinator | `Describe` (`Describe` on the transactional id for key type 1) | — | 30 / 53 |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup | `Read` | — | 30 |
| OffsetCommit | `Read` | `Read` | 30, or 29 per denied topic |
| OffsetFetch | `Describe` | `Describe` | 30, or 29 per denied topic |
| DescribeGroups | `Describe` | — | 30 per group |
| ListGroups | `Describe` per group, or `Describe` on the cluster for all | — | filtered, no error |
| DeleteGroups | `Delete` | — | 30 per group |
| OffsetDelete | `Delete` | `Read` | 30, or 29 per denied topic |

As in Apache, an Allow for `Read`, `Write`, `Delete` or `Alter` also grants
`Describe`, so a consumer holding only `Read` on its group passes the
`Describe` checks. Verified end to end by
`crates/kaas-broker/tests/discovery_and_group_authz.rs`.

## OffsetCommit

Persists a group's consumed positions. Key 8.

**Versions**: v2–v8 (flexible from v8)

**Handling**: the handler flattens the nested `(topic, partition)` request
into a `topic/partition → offset` map plus a parallel metadata map and hands
both to `Manager::offset_commit`, which merges them into the group's in-memory
cache and atomically rewrites `__consumer_offsets/<groupID>.json`
(tmp + fsync + rename). The resulting group-level error code is stamped
uniformly across every partition in the response. Per-partition
`committed_metadata` strings round-trip; empty strings clear the
entry and come back as the wire null sentinel.

**Deviations from Apache 3.7**:

- The advertised floor is v2, not v0 — the v0/v1 shapes were never decoded
  correctly and are not offered. Additionally, v2–v4's `retention_time_ms`
  field is **not decoded**, so v2–v4 requests that carry it mis-parse; this is
  an acknowledged divergence tracked as follow-up in the codec module. In
  practice every modern client negotiates v8 via ApiVersions and never hits
  it.
- `generation_id`, `member_id`, and `group.instance.id` are decoded but not
  validated against the group state machine — no `ILLEGAL_GENERATION`,
  `UNKNOWN_MEMBER_ID`, or `FENCED_INSTANCE_ID` fencing on the commit path. A
  zombie commit from an evicted member is accepted.
- Commits are best-effort durable: a disk-write failure is logged but still
  reported as success (`Manager::offset_commit`).
- `committed_leader_epoch` (v6+) is decoded but not persisted; fetches always
  return `-1` for it.

**Source**: `crates/kaas-broker/src/handlers/offset_commit.rs`,
`crates/kaas-codec/src/api/offset_commit.rs`,
`crates/kaas-coordinator/src/offset_store.rs`.

**Verified by**: `crates/kaas-broker/tests/group_dispatch.rs` (full lifecycle
round trip), `commit_then_fetch_roundtrips` / `commit_with_metadata_persists`
in `offset_store.rs`, `scripts/kafka-consumer-groups.sh` (`--reset-offsets
--execute` drives OffsetCommit), `scripts/kafka-console-consumer.sh`.

## OffsetFetch

Reads a group's committed positions back. Key 9.

**Versions**: v1–v8 (flexible from v6)

**Handling**: v1–v7 carry a single group; v8+ batches multiple groups in one
request (`groups[]`), and the handler resolves each group independently —
per-group `NOT_COORDINATOR` for groups owned elsewhere. Lookups read the
coordinator's in-memory cache; partitions without a committed offset return
the `-1` sentinel. Offsets staged by transactional producers via
[TxnOffsetCommit](transactions.md#txnoffsetcommit) live in a separate pending
layer and are **invisible here until the transaction commits** — an aborted
transaction's offsets are never observable.

**Deviations from Apache 3.7**:

- The "fetch everything" sentinel (null topics list) returns an **empty**
  result instead of enumerating all committed offsets. Symptom:
  `kafka-consumer-groups --describe` on a group with no active members shows
  nothing even though offsets are on disk (noted in
  `scripts/kafka-consumer-groups.sh`, which dropped its `--shift-by` scenario
  because of it). Follow-up tracked in the handler.
- `require_stable` (v7+, [KIP-447](../kip/kip-447.md)) is accepted and
  ignored. kaas never returns `UNSTABLE_OFFSET_COMMIT`; it returns the last
  durably-committed offset. Reads can never see dirty in-flight offsets
  (the pending layer guarantees that), but a caller asking to *wait out* an
  in-flight commit doesn't get the retriable error Apache would send.
- Committed offsets are only loaded from disk when the group first **joins**
  on this broker. A standalone manual-assignment consumer (`assign()` +
  `commitSync`, no JoinGroup) that fetches after a coordinator change hits a
  cold cache and gets `-1` until its next commit.
- `committed_leader_epoch` is always `-1`.
- Advertised floor is v1, not v0 (v0 was the ZooKeeper-era offsets path).

**Source**: `crates/kaas-broker/src/handlers/offset_fetch.rs`,
`crates/kaas-codec/src/api/offset_fetch.rs`,
`crates/kaas-coordinator/src/offset_store.rs`.

**Verified by**: `crates/kaas-broker/tests/group_dispatch.rs`,
`pending_invisible_to_fetch_until_commit_pending` /
`discard_pending_drops_unmaterialised_offsets` in `offset_store.rs`,
`bins/kaas/tests/eos_v2.rs` (transactional offsets become visible only after
EndTxn), `scripts/kafka-consumer-groups.sh`.

## FindCoordinator

Resolves which broker coordinates a group (or a transactional ID). Key 10.
The one group API any broker answers.

**Versions**: v0–v4 (flexible from v3)

**Handling**: `key_type` 0 routes through the group-assignment source
(explicit `assignment.json` entry, else the FNV-1a hash — see the preamble),
`key_type` 1 through the transaction-assignment source (same hash
machinery); any other value gets `INVALID_REQUEST` (42). The resolved broker
ID is mapped to `(node_id, host, port)` via the EndpointSlice-backed broker
registry; in dev mode the lookup resolves self only. No manager installed, no txn source
yet, or no alive broker for the slot → `COORDINATOR_NOT_AVAILABLE` (15) and
the client retries. v4+ batches multiple `coordinator_keys[]` into one
request; v0–v3 use the legacy single-key shape (at v3 the legacy shape merely
gains flexible encoding — the array form is strictly v4+).

**Deviations from Apache 3.7**:

- The response is **not listener-aware**: the advertised port is the broker's
  primary client port (the first configured listener / the headless Service's
  Kafka port), regardless of which listener the request arrived on. Metadata
  got per-listener advertisement; FindCoordinator has not, so on a
  multi-listener cluster a client that bootstrapped on a secondary listener is
  handed the first listener's port here.

**Source**: `crates/kaas-broker/src/handlers/find_coordinator.rs`,
`crates/kaas-codec/src/api/find_coordinator.rs`,
`crates/kaas-coordinator/src/manager.rs`, `crates/kaas-broker/src/group_hash.rs`;
the broker registry in `bins/kaas/src/cluster.rs` and
`crates/kaas-k8s/src/endpoints.rs`.

**Verified by**: `find_coordinator_resolves_self` /
`find_coordinator_txn_with_no_source_is_unavailable` /
`find_coordinator_unknown_key_type_is_invalid_request` in `manager.rs`,
hash-routing tests in `group_hash.rs`,
`crates/kaas-broker/tests/group_dispatch.rs`,
`scripts/kafka-consumer-groups.sh`.

## JoinGroup

Enters a member into the group and drives the rebalance state machine
(`Empty → PreparingRebalance → CompletingRebalance → Stable`). Key 11.

**Versions**: v2–v9 (flexible from v6)

**Handling**: the handler translates the codec request into the coordinator's
`JoinRequest` and parks the connection on a oneshot until the rebalance round
completes. Joining an `Empty` group starts the initial rebalance with Apache's
3 s `group.initial.rebalance.delay.ms` (extended per new arrival, capped at
the max member `rebalance_timeout_ms`); joining a `Stable` group
bounces it back to `PreparingRebalance` and cancels any in-flight sync round.
The leader is the first joiner of the round; protocol selection is
leader-first-mutual (first protocol the leader declares that every member also
lists). Dynamic members that fail to rejoin within the rebalance timeout are
evicted at round completion; static members
([KIP-345](../kip/kip-345.md) `group.instance.id`) survive a missed rejoin.

**Deviations from Apache 3.7**:

- [KIP-394](../kip/kip-394.md)'s v4+ two-step handshake is **not
  implemented**: kaas never returns `MEMBER_ID_REQUIRED` (79). An empty
  `member_id` is assigned inline and the join proceeds in one round trip — the
  legacy pre-v4 path, explicitly marked as a follow-up in the group
  coordinator's join path. Clients work fine (they simply skip
  a retry), but Apache's ghost-member protection is absent.
- [KIP-345](../kip/kip-345.md) is partial: `group.instance.id` is plumbed
  through join/sync/heartbeat/leave and static members survive the non-rejoin
  eviction, but there is **no `FENCED_INSTANCE_ID` fencing** — two members
  presenting the same instance ID are treated as two members — and a static
  rejoin triggers a full rebalance rather than Apache's
  return-cached-assignment fast path (`skip_assignment` is always `false`).
- [KIP-800](../kip/kip-800.md) `reason` strings (v8+) are decoded and
  discarded, not logged.
- The advertised floor is v2, not v0 — v0/v1 lack `rebalance_timeout_ms` and
  were never decoded correctly.
- Member IDs are generated as `<principal>-<counter-hex>` from the
  connection's authenticated principal (empty for anonymous listeners), not
  Apache's `<client.id>-<UUID>` — cosmetic, but visible in `--describe`.

**Source**: `crates/kaas-broker/src/handlers/join_group.rs`,
`crates/kaas-codec/src/api/join_group.rs`,
`crates/kaas-coordinator/src/group.rs`.

**Verified by**: `single_member_rebalance_completes_via_initial_delay` /
`shutdown_fires_pending_joiners_with_unknown_member` in `group.rs`,
`crates/kaas-broker/tests/group_dispatch.rs`,
`scripts/kafka-consumer-groups.sh`, `scripts/kafka-verifiable-consumer.sh`.

## Heartbeat

Keeps a member's session alive and signals rebalances in progress. Key 12.

**Versions**: v0–v4 (flexible from v4)

**Handling**: each heartbeat re-arms the member's session-timeout task (a
tokio timer against the member's `session_timeout_ms`); expiry evicts the
member and bounces a `Stable` group into `PreparingRebalance`. Checks run in
Apache's order: unknown member (or `Empty`/`Dead` group) →
`UNKNOWN_MEMBER_ID` (25); generation mismatch → `ILLEGAL_GENERATION` (22);
group mid-rebalance → the timer is still reset, then
`REBALANCE_IN_PROGRESS` (27) tells the member to rejoin; otherwise success.

**Deviations from Apache 3.7**:

- `group.instance.id` (v3+, [KIP-345](../kip/kip-345.md)) is decoded but not
  used for fencing — no `FENCED_INSTANCE_ID` here either.

**Source**: `crates/kaas-broker/src/handlers/heartbeat.rs`,
`crates/kaas-codec/src/api/heartbeat.rs`,
`crates/kaas-coordinator/src/group.rs`.

**Verified by**: `heartbeat_unknown_member_for_empty_group` in `group.rs`,
`not_coordinator_on_offset_commit_when_source_says_no` in `manager.rs` (also
covers heartbeat's NOT_COORDINATOR path),
`crates/kaas-broker/tests/group_dispatch.rs`, any live consumer in
`scripts/kafka-console-consumer.sh` / `scripts/kafka-verifiable-consumer.sh`.

## LeaveGroup

Removes one or more members from a group. Key 13.

**Versions**: v0–v5 (flexible from v4)

**Handling**: the handler collects member IDs from both the legacy single
`member_id` field (v0–v2) and the v3+ `members[]` batch
([KIP-345](../kip/kip-345.md) admin removal shape), then removes them in one
pass: each known member is dropped (its session timer aborted, any parked
JoinGroup waiter woken), unknown members get per-member
`UNKNOWN_MEMBER_ID` (25). The last member leaving returns the group to
`Empty`; a leave from `Stable` triggers `PreparingRebalance`.

**Deviations from Apache 3.7**:

- On the legacy v0–v2 single-member shape the per-member result is
  **dropped**: the top-level `error_code` is 0 whenever this broker
  coordinates the group, so an unknown-member leave at v0–v2 reports success.
  The v3+ batch shape carries per-member codes correctly.
- Leaving a group the broker coordinates but holds no state for returns
  top-level success with an empty member list rather than per-member
  `UNKNOWN_MEMBER_ID`.
- [KIP-800](../kip/kip-800.md) per-member `reason` strings (v5) are decoded
  and discarded.

**Source**: `crates/kaas-broker/src/handlers/leave_group.rs`,
`crates/kaas-codec/src/api/leave_group.rs`,
`crates/kaas-coordinator/src/group.rs`.

**Verified by**: `leave_drops_state_back_to_empty` in `group.rs`,
`v5_java_client_fixture_with_reason` in the codec module (byte-level fixture
captured from a real Java client),
`crates/kaas-broker/tests/group_dispatch.rs`,
`scripts/kafka-consumer-groups.sh`.

## SyncGroup

Distributes the leader-computed assignment to every member of a rebalance
round. Key 14.

**Versions**: v0–v5 (flexible from v4)

**Handling**: the leader's SyncGroup stores per-member assignments on the
current sync round, flips the group to `Stable`, and wakes every parked
follower; followers park until the leader delivers (or a fresh JoinGroup
cancels the round, in which case they wake with
`REBALANCE_IN_PROGRESS` (27) instead of a bogus empty assignment).
Checks: unknown member → `UNKNOWN_MEMBER_ID` (25); generation mismatch →
`ILLEGAL_GENERATION` (22). A follower's sync is valid in both
`CompletingRebalance` *and* `Stable` (the leader may finish first).
Members the leader omitted receive a zero-byte assignment.

**Deviations from Apache 3.7**:

- v5's `protocol_type` / `protocol_name` request fields are accepted but not
  cross-checked against the group — kaas never returns
  `INCONSISTENT_GROUP_PROTOCOL` from SyncGroup.
- Omitted members get raw empty bytes; encoding a *valid* empty
  `ConsumerProtocolAssignment` struct instead is a follow-up (tracked
  as gh #111).
- No `FENCED_INSTANCE_ID` for static members ([KIP-345](../kip/kip-345.md)),
  same as the rest of the group surface.

**Source**: `crates/kaas-broker/src/handlers/sync_group.rs`,
`crates/kaas-codec/src/api/sync_group.rs`,
`crates/kaas-coordinator/src/group.rs`.

**Verified by**: `sync_returns_leader_supplied_assignment` in `group.rs`,
`join_then_sync_then_offset_commit_then_fetch_roundtrip` in `manager.rs`,
`crates/kaas-broker/tests/group_dispatch.rs`,
`scripts/kafka-verifiable-consumer.sh`.

## DescribeGroups

Snapshots named groups: state, protocol, members, assignments. Key 15.

**Versions**: v0–v5 (flexible from v5)

**Handling**: `Manager::describe_groups` answers per group and filters by
ownership — a group coordinated elsewhere gets a per-group
`NOT_COORDINATOR` (16) entry, which matters because the Java AdminClient
unions results across brokers (without the filter, one broker's stale
in-memory entry reappeared cluster-wide). Owned groups return their live
snapshot; group-state strings match Apache's exactly (`Empty`,
`PreparingRebalance`, `CompletingRebalance`, `Stable`, `Dead`).

**Deviations from Apache 3.7**:

- A group this broker coordinates but has no state for is described as
  `Empty` with no members; Apache describes an unknown group as `Dead`.
- Per-member `client_host` and `member_metadata` are returned empty (the
  coordinator tracks the host but the snapshot doesn't carry it yet);
  `member_assignment` is populated.
- `authorized_operations` (v3+) is always 0 — kaas neither computes the
  operations bitmap nor returns Apache's `INT32_MIN` "not requested"
  sentinel.

**Source**: `crates/kaas-broker/src/handlers/describe_groups.rs`,
`crates/kaas-codec/src/api/describe_groups.rs`,
`crates/kaas-coordinator/src/manager.rs`.

**Verified by**: `scripts/kafka-consumer-groups.sh` (`--describe` scenario).
There is no dedicated unit test for the DescribeGroups handler; the
underlying `Group::describe` snapshot is exercised by the lifecycle tests in
`crates/kaas-coordinator/src/group.rs`.

## ListGroups

Enumerates the groups a broker coordinates. Key 16.

**Versions**: v0–v4 (flexible from v3)

**Handling**: snapshots every in-memory group and filters by coordinator
ownership — the same ownership filter as DescribeGroups, so the AdminClient's
cross-broker union never shows a group twice or shows stale orphans. The v4+
`states_filter` is applied broker-side by exact state-string match. With no
coordinator manager installed (boot window) the response is an empty list
with `error_code = 0`, mirroring an idle Apache broker.

**Deviations from Apache 3.7**:

- Only **in-memory** groups are listed. A group that exists solely as a
  committed-offsets file — no live members since the coordinator restarted or
  the group moved — doesn't appear in `--list` until a member joins again.
  Apache materialises such groups from `__consumer_offsets` and lists them as
  `Empty`.

**Source**: `crates/kaas-broker/src/handlers/list_groups.rs`,
`crates/kaas-codec/src/api/list_groups.rs`,
`crates/kaas-coordinator/src/manager.rs`,
`crates/kaas-broker/src/group_takeover.rs` (the orphan sweep that keeps the
list honest).

**Verified by**: `crates/kaas-broker/tests/group_dispatch.rs`,
`scripts/kafka-consumer-groups.sh` (`--list` before and after `--delete` —
the group must appear and then actually vanish).

## DeleteGroups

Drops a group's coordinator state and committed offsets. Key 42.

**Versions**: v0–v2 (flexible from v2)

**Handling**: per group, in order: not this broker's group →
`NOT_COORDINATOR` (16); no in-memory state *and* no offsets file →
`GROUP_ID_NOT_FOUND` (69); live state that isn't `Empty`/`Dead` →
`NON_EMPTY_GROUP` (67); otherwise the in-memory group is shut down and the
`__consumer_offsets/<groupID>.json` file deleted. A disk-delete failure after
the in-memory wipe is swallowed — the stale file is harmless and the
operator's startup sweep re-cleans it. A group whose only trace is the
offsets file (all members long gone) counts as existing and is deletable,
matching Apache.

**Deviations from Apache 3.7**: None known.

**Source**: `crates/kaas-broker/src/handlers/delete_groups.rs`,
`crates/kaas-codec/src/api/delete_groups.rs`,
`crates/kaas-coordinator/src/manager.rs`.

**Verified by**: `delete_group_non_empty_when_state_is_stable` /
`delete_group_unknown_returns_group_id_not_found` in `manager.rs`,
`scripts/kafka-consumer-groups.sh` scenario 5 (`--delete` must succeed *and*
the group must vanish from a subsequent `--list`).

## OffsetDelete

Drops specific `(topic, partition)` committed offsets without deleting the
group. Key 47. Drives
`kafka-consumer-groups.sh --delete-offsets` and
`AdminClient.deleteConsumerGroupOffsets()`.

**Versions**: v0 only (not flexible)

**Handling**: the handler builds the canonical `topic/partition` key list and
calls `Manager::delete_offsets`, which removes the entries from the group's
cache and rewrites the offsets file. Per-partition results: removed → 0;
no committed entry under that key → `UNKNOWN_TOPIC_OR_PARTITION` (3).
Per-partition errors are suppressed (0) whenever the group-level error is
non-zero. Wire-shape quirk faithfully reproduced: the group-level
`error_code` precedes `throttle_time_ms` — the opposite field order from
DeleteGroups.

**Deviations from Apache 3.7**:

- The only group-level errors kaas produces are `NOT_COORDINATOR` (16) and
  success. An unknown group returns group-level 0 with every partition marked
  `UNKNOWN_TOPIC_OR_PARTITION`, where Apache returns
  `GROUP_ID_NOT_FOUND` (69).
- No subscription guard: Apache refuses to delete offsets for topics a
  `Stable` consumer-protocol group is actively subscribed to
  (`GROUP_SUBSCRIBED_TO_TOPIC`, 86). kaas deletes them regardless of group
  state.

**Source**: `crates/kaas-broker/src/handlers/offset_delete.rs`,
`crates/kaas-codec/src/api/offset_delete.rs`,
`crates/kaas-coordinator/src/offset_store.rs`.

**Verified by**: `delete_partitions_removes_only_requested_keys` in
`offset_store.rs`; the shell-tool path rides
`scripts/kafka-consumer-groups.sh`.
