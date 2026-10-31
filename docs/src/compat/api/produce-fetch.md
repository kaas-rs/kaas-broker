# Produce, Fetch, ListOffsets & Metadata

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

## Produce

Append RecordBatches to partition logs. This is the write hot path: the
[storage engine chapter](../../architecture/storage-hot-path.md) covers the
mechanics end to end; this section covers the wire contract.

**Versions**: v3–v9 (flexible from v9)

Four gates run before any byte reaches the engine, per partition: the topic
must exist in the registry (else `UNKNOWN_TOPIC_OR_PARTITION`, 3), the broker
`Coordinator` must own the partition per the applied `assignment.json` (else
`NOT_LEADER_FOR_PARTITION`, 6), the controller-connectivity self-fence must
see a fresh controller heartbeat — a broker cut off from the controller
stops acking within 3 s (also 6) — and the cluster-wide authorizer must
grant topic Write (else `TOPIC_AUTHORIZATION_FAILED`, 29). A batch larger
than Apache's `message.max.bytes` default (1 MiB + 12 bytes batch overhead)
is rejected with `MESSAGE_TOO_LARGE` (18) before it hits storage.

Inside the engine, under the partition mutex: idempotent-producer
classification runs first — a duplicate (PID/epoch/sequence already in the
5-batch ring window) echoes the cached base offset with `error_code = 0` and
never touches the log; out-of-order sequences get `OUT_OF_ORDER_SEQUENCE_NUMBER`
(45); a stale producer epoch gets `INVALID_PRODUCER_EPOCH` (47). Accepted
batches have their base offset rewritten in place to the current high
watermark (the v2 CRC covers byte 21 onward, so the 8-byte overwrite is
wire-correct) and the client's bytes land on disk verbatim — the broker never
parses records, only fixed-size header peeks. For `acks = -1`, the append that
crosses the flush threshold parks on the per-partition committer's group-commit
fsync — one `sync_all()` cycle serves every concurrent appender —
with `KAAS_FLUSH_INTERVAL_MESSAGES` (default 1) as the durability dial,
overridable per topic via the `flush.messages` topic config.

The produce quota is checked once per request over the summed record bytes,
after the appends, and feeds `throttle_time_ms` in the response.

**Deviations from Apache 3.7**:

- `acks = 0` still gets a wire response. Apache sends nothing for acks=0
  produce requests; kaas's dispatcher has no response-suppression path, so the
  handler's response is always written back.
- The `message.max.bytes` cap is the fixed Apache default — the per-topic
  `max.message.bytes` config override is not consulted.
- `log_append_time_ms` is always `-1`: the broker never stamps append time,
  so `message.timestamp.type=LogAppendTime` semantics are not implemented.
- The v8+ `record_errors` / `error_message` fields are never populated —
  errors are reported per partition, not per record.

**Source**: `crates/kaas-broker/src/handlers/produce.rs`,
`crates/kaas-codec/src/api/produce.rs`; engine side in
`crates/kaas-storage/src/partition.rs` and
`crates/kaas-storage/src/idempotence.rs`.

**Verified by**: `unknown_topic_returns_error_code_3` (handler unit test),
`produce_fetch_metadata_roundtrip` in `bins/kaas/tests/smoke.rs` (which
also asserts the fetched batch is byte-identical to the produced one),
`append_then_read_byte_identical` in `crates/kaas-storage/src/disk.rs`,
and the full transactional round trip in `bins/kaas/tests/eos_v2.rs`. Shell
suite: `scripts/kafka-console-producer.sh`,
`scripts/kafka-producer-perf-test.sh`, `scripts/kafka-verifiable-producer.sh`.

## Fetch

Read RecordBatches from partition logs. The symmetric half of the byte-opacity
contract: batch bytes come back off disk undecoded (see the
[storage hot path](../../architecture/storage-hot-path.md)).

**Versions**: v4–v12 (flexible from v12)

The front door mirrors Produce: topic-exists, `Coordinator::owns`, and a
cluster-wide topic Read ACL check, per partition (there is no self-fence gate
on reads). The handler then picks the read cap: the high watermark for
`read_uncommitted`, the last stable offset for `read_committed` (the LSO is
the highest offset with no in-flight transaction extending past it). The
engine copies batch bytes from the segment files into memory; for
`read_committed` the result is trimmed to whole batches strictly below the
LSO — a batch that straddles the cap is dropped along with everything after
it, since batches are atomic — and `aborted_transactions` is populated from
the partition's aborted-txn index over the returned range, so the client can
filter aborted records. A reader already at or past the cap gets an
empty response. Out-of-range offsets map to `OFFSET_OUT_OF_RANGE` (1). The
fetch quota is checked once over the summed response bytes and feeds
`throttle_time_ms`.

The response is materialized bytes — copied from the segments, not
`sendfile`/spliced. The codec keeps records byte-opaque precisely so a splice
path stays possible; today it is a future optimisation, not what ships.

**Deviations from Apache 3.7**:

- **No KIP-227 fetch sessions**: `session_id = 0` on every response,
  regardless of what the client sent. That is Apache's documented marker for
  "broker doesn't support sessions", so clients fall back to full fetch
  requests per poll — a deliberate contract, not a gap
  ([non-goals](../non-goals.md)). `session_epoch` and `forgotten_topics` are
  decoded and ignored.
- **No long-poll**: `max_wait_ms` and `min_bytes` are decoded but ignored.
  An empty fetch returns immediately instead of parking until data arrives or
  the wait expires; clients simply re-poll.
- The leader-epoch fields (`current_leader_epoch`, `last_fetched_epoch`) are
  not validated — kaas never returns `FENCED_LEADER_EPOCH` or
  `UNKNOWN_LEADER_EPOCH`, and KIP-320 truncation detection does not apply
  (there are no followers to diverge from).
- `preferred_read_replica` is always `-1` and `rack_id` is ignored — no
  follower fetching, because there is no replication
  ([non-goals](../non-goals.md)).

**Source**: `crates/kaas-broker/src/handlers/fetch.rs`,
`crates/kaas-codec/src/api/fetch.rs`; aborted-txn index in
`crates/kaas-storage/src/txn_index.rs`.

**Verified by**: the `trim_to_offset_*` unit tests and
`unknown_topic_returns_error_3` (asserts `session_id == 0`) in the handler,
`produce_fetch_metadata_roundtrip` in `bins/kaas/tests/smoke.rs`, and the
read-committed assertions `eos_commit_path_records_visible_to_read_committed`
/ `eos_abort_path_populates_aborted_transactions` in
`bins/kaas/tests/eos_v2.rs`. Shell suite:
`scripts/kafka-console-consumer.sh`, `scripts/kafka-consumer-perf-test.sh`,
`scripts/kafka-e2e-latency.sh`.

## ListOffsets

Resolve a timestamp (or a sentinel) to an offset per partition —
what `kafka-get-offsets.sh` and every consumer's `auto.offset.reset` ride on
([KIP-32](../kip/kip-32.md) semantics).

**Versions**: v1–v7 (flexible from v6)

The handler checks the topic exists (else `UNKNOWN_TOPIC_OR_PARTITION`, 3),
then translates the request timestamp: `-2` (EARLIEST) returns the partition's
`log_start_offset`, `-1` (LATEST) returns the high watermark — both echoed
with `timestamp = -1`, matching Apache's sentinel-response shape. Any other
timestamp is passed to the engine's `offset_for_timestamp`.

**Deviations from Apache 3.7**:

- **Timestamp→offset lookup is not implemented.** Segments track their
  `max_timestamp`, but the engine-level index that maps a timestamp to
  an offset is a follow-up: `offset_for_timestamp` returns the `(-1, -1)`
  "no matching offset" sentinel unconditionally, in both the disk and
  in-memory engines. Every non-sentinel timestamp query — including
  `MAX_TIMESTAMP` (`-3`, nominally in the registered v7 range) — gets
  `offset = -1, timestamp = -1` with `error_code = 0`. Only the `-1`/`-2`
  sentinels resolve.
- `isolation_level` is decoded but ignored: LATEST returns the high
  watermark even for `read_committed` clients, where Apache returns the last
  stable offset.
- The tiered-storage-only sentinels (`EARLIEST_LOCAL_TIMESTAMP`,
  `EARLIEST_PENDING_UPLOAD_OFFSET`) are deliberately unsupported — clients
  only send them when configured for a remote tier
  ([non-goals](../non-goals.md)); on the wire they fall into the same
  `(-1, -1)` path as any other timestamp.
- `leader_epoch` is always `0` in success responses.
- No leadership gate: there is no `Coordinator::owns` check — a non-leader
  broker answers from its local engine view (typically offset `0`) instead of
  `NOT_LEADER_OR_FOLLOWER`. Clients that route by Metadata leadership never
  hit this in steady state. Authorization does match Apache (gh #268):
  `Describe` on the topic, checked before existence, so every partition of a
  denied topic answers `TOPIC_AUTHORIZATION_FAILED` (29) whether or not the
  topic exists.

**Source**: `crates/kaas-broker/src/handlers/list_offsets.rs`,
`crates/kaas-codec/src/api/list_offsets.rs`; the sentinel-returning engine
lookup in `crates/kaas-storage/src/disk.rs`.

**Verified by**: handler unit tests `latest_returns_high_watermark`,
`earliest_returns_log_start_offset`, `unknown_topic_returns_3`,
`topic_without_describe_returns_29_existing_or_not`. Shell suite:
`scripts/kafka-get-offsets.sh` (exercises the `-2`/`-1` sentinels before and
after producing — the paths clients actually use).

## Metadata

The cluster-discovery API: which brokers exist, who leads each partition, and
where to connect — the response every client bootstraps from.

**Versions**: v1–v10 (flexible from v9)

**Per-listener advertisement**: the handler precomputes one
advertised `(host, port)` per configured listener from the `KAAS_LISTENERS`
env, and each connection carries its listener name, so a client that
bootstrapped on `:9095` gets `:9095` back — not the anonymous listener's port.
Without this, authed-listener clients were routed back to the plain listener
and looped on SCRAM retry ([Listeners & auth](../../architecture/listeners-auth.md)).
Peer brokers are advertised at their stable in-cluster FQDN with the port of
the listener the client connected on; per-broker external hostname templates
for peers are a follow-up.

**Leadership comes from `assignment.json`** via the broker `Coordinator`: each
partition's `leader_id` is the assignment entry's broker ordinal, and
`controller_id` is parsed from the assignment's controller identity
([Controller](../../architecture/controller.md)). Dev mode — and any partition
missing from the assignment (fresh topic, recompute pending) — falls back to
self. A null request topic list returns every known topic and an empty one
returns none, only brokers and cluster info: Apache's contract from v1,
where only v0 read an empty list as "all topics" (gh #274). Unknown
requested topics get a per-topic `UNKNOWN_TOPIC_OR_PARTITION` (3). A v10
entry with a null name or a non-zero topic id gets `INVALID_REQUEST` (42) for
the whole request, as in Apache below v12, where lookup by id starts.
`replica_nodes` and `isr_nodes` are
`[leader]` — the truthful shape for a single-writer design with no
replication ([non-goals](../non-goals.md)).

**Authorization** (gh #268) is Apache's: `Describe` on each topic, checked
before anything else about it. An all-topics request silently omits the
topics the principal can't describe — `kafka-topics.sh --list` shows only
what it may see — while a named topic answers `TOPIC_AUTHORIZATION_FAILED`
(29) whether or not it exists, so the error code never confirms a name.
Auto-creation's `Create` check runs only for a topic that passed `Describe`.

**Deviations from Apache 3.7**:

- **Topic IDs serve the all-zero sentinel on the wire.** The v10 `topic_id`
  field is encoded, but the value is always the null UUID: the operator mints
  a real v4 UUID into each `KafkaTopic`'s `Status.TopicID`, and the
  broker-side observation plumbing exists, yet the watcher callback that
  feeds the topic registry inserts `[0; 16]`.
  [KIP-516](../kip/kip-516.md) is partial: minted operator-side, not yet
  propagated to the Metadata response.
- `leader_epoch` is always `0` (v7+ field) — clients that cache epochs for
  truncation detection get a constant.
- Auto-creation (`auto.create.topics.enable`, chart
  `autoCreateTopics.enabled`, gh #242) mints a `KafkaTopic` CR and answers
  `LEADER_NOT_AVAILABLE` (5) until the operator materializes it. It needs
  `Create` on the topic; Apache also accepts `Create` on the cluster, which
  kaas does not consult.
- `topic_authorized_operations` / `cluster_authorized_operations` are always
  `0`, even when the v8+ request flags ask for them — the ACL bitsets are
  not computed.
- `rack` is always null and `is_internal` always false — kaas advertises no
  internal topics (there is no on-wire `__consumer_offsets`).
- v11+ is not registered; clients negotiate down to v10 via ApiVersions.

**Source**: `crates/kaas-broker/src/handlers/metadata.rs`,
`crates/kaas-codec/src/api/metadata.rs`; the registry-feeding watcher
callback in `bins/kaas/src/main.rs`.

**Verified by**: handler unit tests `per_listener_port_echoed_back`,
`returns_self_as_only_broker_and_leader`, `empty_topic_list_returns_all_known`,
`unknown_topic_returns_per_topic_error_3`,
`all_topics_omits_what_the_principal_cannot_describe`,
`named_topic_without_describe_is_29_existing_or_not`,
`auto_create_checks_describe_before_create`;
`crates/kaas-broker/tests/discovery_and_group_authz.rs` (the gh #268
live-cluster ACL, end to end);
`produce_fetch_metadata_roundtrip` in `bins/kaas/tests/smoke.rs`. Shell
suite: `scripts/kafka-topics.sh` (`--list` / `--describe` ride Metadata).
