# Cluster & log-dir APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

## ApiVersions

The bootstrap call: tells the client which API keys and version ranges this
broker serves, so everything else on these pages is discoverable rather than
guessed.

**Versions**: v0–v4 (flexible from v3, [KIP-482](../kip/kip-482.md)).

**Handling**: the response is built directly from the codec's `ApiSpec`
registry — the same table that generates the
[API support matrix](../api-matrix.md) — so the advertised surface is the wire
truth by construction: 40 keys, sorted, deduplicated (unit tests
assert the count and both invariants). The API is on the pre-auth allowlist, so it works before SASL
completes. Two protocol subtleties are implemented faithfully:

- **The v0-response-header quirk**: the ApiVersions *response* header is
  always encoded as header v0 — no tagged-field block — even on flexible
  request versions. This is Apache's documented exception, kept so a client
  that misjudged the broker's capabilities can still parse the error code.
- **Unknown-version fallback**: when a client requests a version outside the
  supported range, the dispatcher does not return `UNSUPPORTED_VERSION` (as it
  does for every other key) — ApiVersions is special-cased so version
  negotiation can always complete.

**Deviations from Apache 3.7**:

- The unknown-version fallback **clamps to the broker's max version and
  answers success**, where Apache answers error 35 in a v0-encoded body
  listing its supported range. Only clients newer than v4 can observe the
  difference.
- The v3+ request fields `client_software_name` / `client_software_version`
  are ignored — the handler doesn't decode the request body at all, so
  they never reach logs or metrics.
- No `SupportedFeatures` / `FinalizedFeatures` tagged fields (KIP-584) in the
  response. They're optional tagged fields, so clients treat their absence as
  "no feature versioning" — consistent with kaas having no KRaft feature
  levels (see [Non-goals](../non-goals.md)).

**Source**: `crates/kaas-broker/src/handlers/api_versions.rs`,
`crates/kaas-codec/src/api/api_versions.rs` (`response_from_registry`),
`crates/kaas-codec/src/api/registry.rs` (the always-v0 exception in
`ApiSpec::response_header`),
`crates/kaas-protocol/src/dispatch.rs` (clamp + pre-auth allowlist).

**Verified by**: `scripts/kafka-broker-api-versions.sh` (asserts the required
API set is advertised and that every advertised range overlaps the Java
client's — any `[usable: -1]` line fails the run); handler and codec
round-trip tests in the source files above; dispatcher clamp tests in
`crates/kaas-protocol/src/dispatch.rs`.

## DescribeCluster

What `AdminClient.describeCluster()` calls: cluster id, controller, and the
live broker set — the same three facts Metadata carries, without the per-topic
payload. `kafka-cluster.sh cluster-id` and most UIs' cluster panes land here.

**Versions**: v0–v2 (flexible from v0 — this API postdates
[KIP-482](../kip/kip-482.md), so there is no legacy encoding). **v2 is a
deliberate exception to the Apache 3.7 parity target**: `IncludeFencedBrokers` /
`IsFenced` are KIP-1073, which is Kafka 4.0 surface. kaas serves it because it
has a real fenced state to report — see
[broker fencing](../../architecture/broker-fencing.md) — and because clients
that ask for fenced brokers unconditionally cannot otherwise negotiate a
version where the field is legal.

**Handling**: broker rows come from the same catalog Metadata advertises, so
both APIs answer with the port of the listener the request arrived on, peers at
their stable per-broker DNS name, and self at its own advertised host. A client
that reached one API on the authed listener is never handed the anonymous port
by the other. The controller is the broker holding the `kaas-controller` Lease,
read from the applied `assignment.json`.

**Fenced brokers**: a fenced broker is registered but not serving — its pod
exists and its EndpointSlice entry is still there, but it is not Ready. Those
rows are omitted unless a v2 request sets `IncludeFencedBrokers`, matching
Apache and matching Metadata, which never advertises them. Asking for them is
the only way to tell a degraded cluster from a smaller one. The broker
answering the request never reports *itself* as fenced.

The API itself is not authorization-gated — Apache answers the broker list to
any authenticated principal. The optional `ClusterAuthorizedOperations`
bitfield is: it needs `Describe` on the cluster resource, and then each
supported operation is evaluated in turn. Three distinct answers, which clients
read differently: `-2147483648` when the client didn't ask, `0` when it asked
but lacks `Describe`, and the computed bitfield otherwise.

kaas serves broker endpoints only — controller election runs on a Kubernetes
Lease rather than a KRaft quorum (see [Non-goals](../non-goals.md)) — so a v1
request for the controller endpoint type gets `MISMATCHED_ENDPOINT_TYPE` (114)
and any other value `UNSUPPORTED_ENDPOINT_TYPE` (115), mirroring Apache. v0
carries no endpoint-type field, so neither error can reach a v0 client.

**Deviations from Apache 3.7**:

- The `ClusterAuthorizedOperations` bitfield never sets the `ClusterAction` or
  `IdempotentWrite` bits. Apache lists both as cluster-scoped operations; kaas
  models neither — there is no Kafka-RPC inter-broker surface for
  `ClusterAction` to guard (peers talk gRPC heartbeats), and idempotent produce
  is gated by `Write` on the topic rather than by a cluster-level grant.
  Reporting a bit the authorizer cannot evaluate would be a guess.
- `Rack` is always null — kaas has no rack awareness (it has no replicas to
  place).
- `IsFenced` is derived from EndpointSlice readiness, not from a KRaft-style
  broker registration + session timeout. The practical difference: a broker
  that is booting (registered, not yet finished takeover) reads as fenced,
  which is the same answer Apache gives for a broker that has registered but
  not yet caught up.
- `ControllerId` falls back to *this* broker when no assignment has been
  applied yet, where Apache would answer `-1`. Every kaas broker serves the same
  admin surface, so a client that dials the answer always reaches something
  that can serve it.

**Source**: `crates/kaas-broker/src/handlers/describe_cluster.rs`,
`crates/kaas-broker/src/listener_advert.rs` (the catalog shared with Metadata),
`crates/kaas-codec/src/api/describe_cluster.rs`.

**Verified by**: `scripts/kafka-cluster.sh` (cluster-id round trip plus an
`AdminClient.describeCluster()` call through `kafka-broker-api-versions`);
handler tests in the source file above, covering fenced-row filtering at v1 and
v2 in both directions; the DescribeCluster leg of `bins/kaas/tests/smoke.rs`,
which drives the flexible-from-v0 header path through the real dispatcher.

## DescribeLogDirs

Reports log directories and per-partition sizes — `kafka-log-dirs.sh
--describe` and Kafbat-UI's storage pane.

**Versions**: v0–v4 (flexible from v2; matches Apache 3.7's range).

**Handling**: kaas reports **one log directory per pool member** — the
default data dir plus every `storage.pool[]` volume (the
[volume pool](../../architecture/volume-pool.md); single-volume
deployments report exactly one dir, as before).
Partitions group under the dir the placement record assigns them to. A
null topics filter expands to every topic in the broker's registry with
all partitions; a named topic with an empty partition list expands to
all its partitions; unknown topics are silently dropped (matching
Apache, which omits partitions it doesn't host). Each partition row
carries `partition_size` from the storage engine, `offset_lag = 0`, and
`is_future_key = false`; the dir-level `error_code` is always 0. v4
responses carry per-dir `TotalBytes` / `UsableBytes` (KIP-827) from a
`statvfs` of the dir's filesystem — `-1` when the probe fails (the
dev-mode `memory://` sentinel).

**Deviations from Apache 3.7**:

- `partition_size` is only real for partitions **this broker currently
  leads** — the engine sums segment sizes of open partitions and reports 0
  for everything else. Apache reports sizes for every replica a broker hosts;
  kaas has no replicas, so per-partition sizes on a multi-broker cluster are
  scattered across the brokers that lead them (`kafka-log-dirs.sh` queries all
  brokers by default, so the union is complete — with zero-rows for the
  non-leaders).
- `offset_lag` is hardwired to 0 and `is_future_key` to false — coherent for
  a broker with no followers and no intra-broker reassignment, but a client
  should not read fetch-lag meaning into it.
**Source**: `crates/kaas-broker/src/handlers/describe_log_dirs.rs`,
`crates/kaas-storage/src/disk.rs` (`partition_size`, `log_dirs`),
`crates/kaas-codec/src/api/describe_log_dirs.rs`.

**Verified by**: `scripts/kafka-log-dirs.sh` (all-dirs describe plus
topic-filtered describe); `partition_size_sums_segment_sizes` and
`placement_resolver_routes_partition_dirs` in
`crates/kaas-storage/src/disk.rs`; version roundtrips in the codec
module.

## AlterReplicaLogDirs

Moves a partition between log dirs on the same broker (KIP-113) — in
kaas, the volume-pool migration verb: the drain path for cordoned pool
members.

**Versions**: v0–v2 (flexible from v2; matches Apache 3.7's range).

**Handling**: the destination is a log dir *path* as reported by
DescribeLogDirs. Per partition, the current leader closes the
partition, fresh-copies its directory to the destination volume, flips
the placement record (`KafkaTopic.status.volumeAssignments`), updates
its local registry, and reclaims the source directory. Produce/fetch
during the copy window fail with the retriable `LEADER_NOT_AVAILABLE`.
Error codes: unknown path → `LOG_DIR_NOT_FOUND` (57); cordoned member
(KIP-1066: no new placements) → `INVALID_REQUEST` (42); not this
broker's partition → `REPLICA_NOT_AVAILABLE` (9); failed copy or
record flip → `KAFKA_STORAGE_ERROR` (56), with the copy rolled back so
data location and placement record never diverge.

**Deviations from Apache 3.7**:

- Apache moves a replica live (future replica + catch-up, then swap);
  kaas pauses the partition for the copy — a brief unavailability
  window instead of a background reassignment, coherent with
  single-writer-per-partition and no followers.
- Only the partition's current **leader** accepts the move (it is the
  only broker holding the files); Apache accepts on any broker hosting
  a replica.

**Source**: `crates/kaas-broker/src/handlers/alter_replica_log_dirs.rs`,
`crates/kaas-storage/src/disk.rs` (`move_partition_to_log_dir`),
`crates/kaas-codec/src/api/alter_replica_log_dirs.rs`.

**Verified by**: `move_partition_between_log_dirs` in
`crates/kaas-storage/src/disk.rs`; `all_versions_roundtrip` in the
codec module; `scripts/kafka-log-dirs.sh`.
