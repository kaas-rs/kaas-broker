# Topic & config admin APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

The whole admin surface on this page is **CR-mediated**: kaas never mutates
topic state directly off a wire request. Writes become creates/patches/deletes
of `KafkaTopic` custom resources,
the operator reconciles the CR into on-disk state, and the broker observes the
result through its topic watcher — see
[Kubernetes integration](../../architecture/kubernetes.md). In dev mode
(`MY_POD_NAME` unset, no kube client) the CR writer is a stub that refuses
every write, so the mutating APIs answer `CLUSTER_AUTHORIZATION_FAILED` (31)
with the message `broker is not running in cluster mode`.

## CreateTopics

Creates topics — the broker side of `kafka-topics.sh --create` and
`AdminClient.createTopics()`.

**Versions**: v0–v7 (flexible from v5).

**Handling**: per requested topic, the handler authorizes `Create` on the
cluster, else on the topic resource (Apache's rule, shared with Metadata
auto-create, gh #280), then POSTs a fresh `KafkaTopic` CR. The operator reconciles it
into partition directories on the shared volume; the broker picks the topic up
via its topic watcher and serves it on subsequent requests — creation is
therefore **asynchronous** (a success response means the CR was accepted, not
that partition dirs exist yet). `num_partitions = -1` (the AdminClient's
"server default", KIP-464) takes the broker's `num.partitions`
(`broker.autoCreateTopics.numPartitions`); `replication_factor = -1` means
1, kaas's only replica count. `0` or anything below `-1` is refused with
`INVALID_PARTITIONS` (37) / `INVALID_REPLICATION_FACTOR` (38), as in Apache
(gh #280; until then every non-positive value silently meant 1).
Kafka topic names that aren't valid RFC 1123 subdomains (Kafka Streams
internals, dotted names) get a deterministic synthetic CR name
`kaas-topic-<16 hex>` with the literal name stashed in `spec.topicName`.
Config overrides on the request (`--config retention.ms=600000`) are
validated against the supported key set and land in the minted CR's
`spec.config`, so the operator materialises them on first reconcile exactly
as if they had been authored on the CR; an unknown key or an unparseable
value fails that topic's creation with `INVALID_CONFIG` (40), as in Apache.
`validate_only` (v1+) runs the authorization, writer, and config-validation
checks, then returns the would-be response without minting the CR. Error
mapping: authorization denial → `TOPIC_AUTHORIZATION_FAILED` (29), bad
config → `INVALID_CONFIG` (40), existing CR → `TOPIC_ALREADY_EXISTS` (36),
missing writer or Kubernetes RBAC denial → `CLUSTER_AUTHORIZATION_FAILED`
(31), other kube errors → `UNKNOWN_SERVER_ERROR` (-1). On ArgoCD-managed
clusters, `admin.argocd.enabled` on the Helm chart makes the minted CR
carry ArgoCD tracking/coexistence annotations so runtime-created topics
appear in the Application tree without being prune targets — see
[Kubernetes integration](../../architecture/kubernetes.md).

**Deviations from Apache 3.7**:

- The supported config-key set is the eight tunable keys DescribeConfigs
  reports — an override outside it is rejected with `INVALID_CONFIG` where
  Apache would accept any of its several dozen topic keys. One accept-only
  exception: `message.timestamp.type=CreateTime` validates and is dropped
  (it names the only behaviour kaas has; Kafka Streams stamps it on every
  internal topic it creates), while `LogAppendTime` is rejected.
- The v7+ response `topic_id` ([KIP-516](../kip/kip-516.md)) is always the
  all-zero UUID: the real TopicID is minted by the operator on first
  reconcile, after the response has gone out.
- `replication_factor` is accepted and echoed but has no effect — kaas is
  single-writer-per-partition by design (see [Non-goals](../non-goals.md)).
- `validate_only` does not check for an existing topic; it reports success
  even when a real create would return `TOPIC_ALREADY_EXISTS`.

**Source**: `crates/kaas-broker/src/handlers/create_topics.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs`,
`crates/kaas-codec/src/api/create_topics.rs`.

**Verified by**: `scripts/kafka-topics.sh` (create/list/describe scenarios);
codec round-trip tests in `crates/kaas-codec/src/api/create_topics.rs`
(including `v7_carries_topic_id`); CR-name mapping and config-conversion
tests in `crates/kaas-broker/src/topic_cr_writer.rs`; config-threading and
rejection handler tests in `crates/kaas-broker/src/handlers/create_topics.rs`.

## DeleteTopics

Deletes topics by name — `kafka-topics.sh --delete`,
`AdminClient.deleteTopics()`.

**Versions**: v0–v5 (flexible from v4).

**Handling**: per topic, the handler deletes the `KafkaTopic` CR, then drops
the topic from the in-memory registry. The operator's reconcile tears down the
partition directories; before that lands, every broker's topic watch sees the
Kubernetes delete event, drops the topic from its registry, abandons the open
partitions (closing log/index file handles without persisting state — the
topic is gone, not handed over), and purges the topic's committed
consumer-group offsets, as Apache tombstones them out of
`__consumer_offsets`. Closing the handles first matters because NFS
silly-renames open files and the operator's directory delete wedges (see
[File-handle ownership](../../architecture/file-handles.md)). A missing CR
answers `UNKNOWN_TOPIC_OR_PARTITION` (3); other writer errors are reported as
`INVALID_REQUEST` (42) with a message. In dev mode only the registry removal
runs — on-disk (in-memory-engine) data is left alone.

**Deviations from Apache 3.7**:

- Authorization: `Delete` on the topic per entry, as in Apache — denial
  answers `TOPIC_AUTHORIZATION_FAILED` (29) and skips the CR delete.
- Deletion is asynchronous: the wire response confirms the CR delete, while
  directory teardown follows on the operator's reconcile.

**Source**: `crates/kaas-broker/src/handlers/delete_topics.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs`.

**Verified by**: `scripts/kafka-topics.sh` (scenario 5, delete-and-confirm).

## DeleteRecords

Advances a partition's log start offset ([KIP-107](../kip/kip-107.md)) —
`kafka-delete-records.sh`, Kafbat-UI's "purge messages".

**Versions**: v0–v2 (flexible from v2).

**Handling**: this is a storage-path API, not a CR write. Per partition the
handler applies the same ownership gate Produce uses — with a cluster
coordinator wired, partitions this broker doesn't lead answer
`NOT_LEADER_OR_FOLLOWER` (6). The storage engine then advances `logStart` to
the target offset (`-1` = purge to the high watermark; a target past the HWM
is `OFFSET_OUT_OF_RANGE` (1)) and returns the new low watermark. Records below
`logStart` become invisible to Fetch immediately, and closed segments that
fall entirely below it are unlinked from disk on the spot — safe on NFS
because only the leader holds open handles.

**Deviations from Apache 3.7**:

- The **active segment is not rolled or reclaimed** by DeleteRecords, and a
  closed segment only partially covered by the purge is kept whole. Visibility
  moves immediately; the covering bytes are reclaimed later by segment roll
  and retention. Apache behaves similarly for partial segments but kaas holds
  the active segment even when the purge covers the entire log.
- Authorization: `Delete` on the topic, as in Apache — denial answers
  `TOPIC_AUTHORIZATION_FAILED` (29) per partition and nothing is purged.

**Source**: `crates/kaas-broker/src/handlers/delete_records.rs`,
`crates/kaas-storage/src/partition.rs` (`delete_records`),
`crates/kaas-storage/src/disk.rs`.

**Verified by**: `scripts/kafka-delete-records.sh` (produce 10, purge to 7,
assert earliest = 7); `delete_records_*` unit tests in
`crates/kaas-storage/src/partition.rs` and `crates/kaas-storage/src/memory.rs`.

## DescribeConfigs

Reads topic and broker configuration — `kafka-configs.sh --describe` and every
admin UI's config pane.

**Versions**: v0–v4 (flexible from v4).

**Handling**: two resource types are served. **TOPIC**: authorize
`DescribeConfigs` on the topic (denial → 29), require the topic in the
registry (miss → `UNKNOWN_TOPIC_OR_PARTITION` (3)), then answer an
Apache-3.7-compatible defaults table of the nine config keys kaas actually
honours — `retention.ms`, `retention.bytes`, `segment.bytes`, `segment.ms`,
`cleanup.policy`, `min.compaction.lag.ms`, `delete.retention.ms`,
`flush.messages`, and the fixed-value `message.timestamp.type` (always
`CreateTime`) — with the topic's stored overrides layered on top.
(`flush.messages` advertises a null default: its effective default is the
broker-wide flush interval, which the static table can't know — a fixed
number here would be the advertised-vs-enforced drift this page keeps
warning about.) An overridden key reports the
override as its value with source `DYNAMIC_TOPIC_CONFIG`, so
`kafka-configs.sh --describe` (which shows only non-default entries) and
admin UIs distinguish "someone set this" from "this is the default", as in
Apache. Overrides are re-read from the operator-materialised per-topic
config file on every request, so a change is visible as soon as the
operator has reconciled it — no broker restart. v1+ attaches the synonym
chain per entry (the dynamic override first when present, then the
`DEFAULT_CONFIG` it shadows), v3+ adds one-line documentation strings, and
the request's `configuration_keys` filter is honoured. **BROKER**: answers
a small fixed read-only table (`broker.id` plus static defaults) so
`kafka-configs.sh --entity-type brokers` and Kafbat-UI's broker page work.
Everything else (`BROKER_LOGGER` included) gets a per-resource
`UNSUPPORTED_VERSION` (35).

**Deviations from Apache 3.7**:

- Only nine topic keys are reported, versus Apache's several dozen; tools
  that iterate the full key set see a short list.
- In dev mode (in-memory storage engine) there is no per-topic config file,
  so every key reports its default.
- The broker table reports static `kafka.version = 3.6.0` /
  `inter.broker.protocol.version = 3.6` strings (predating the 3.7 parity
  target).
- `BROKER_LOGGER` is unsupported and answers `UNSUPPORTED_VERSION` (35),
  where Apache serves log4j levels.

**Source**: `crates/kaas-broker/src/handlers/describe_configs.rs`,
`crates/kaas-broker/src/topic_config_defaults.rs`.

**Verified by**: `scripts/kafka-configs.sh` (broker describe, topic describe,
`--describe --all`, per-broker-id describe); override-layering handler tests
in `crates/kaas-broker/src/handlers/describe_configs.rs`.

## CreatePartitions

Grows a topic's partition count ([KIP-195](../kip/kip-195.md)) —
`kafka-topics.sh --alter --partitions N`.

**Versions**: v0–v3 (flexible from v2).

**Handling**: authorize `Alter` on the topic (denial → 29), then merge-patch
`KafkaTopic.spec.partitions` to the new count. The writer reads the CR first
and refuses a decrease client-side with `INVALID_PARTITIONS` (37) — the
operator's reconciler enforces the same guard as backstop. A missing CR is
`UNKNOWN_TOPIC_OR_PARTITION` (3); dev mode / RBAC denial is
`CLUSTER_AUTHORIZATION_FAILED` (31). The operator creates the new partition
directories on reconcile and the broker serves them after its watcher fires —
expansion is asynchronous, same as topic creation. `validate_only` (v1+)
short-circuits before the patch.

**Deviations from Apache 3.7**:

- A request for the **same** partition count succeeds as a no-op; Apache
  returns `INVALID_PARTITIONS` when the requested count doesn't exceed the
  current one. Only a strict decrease is refused.
- The request's manual `assignments` (replica placement per new partition) are
  ignored — there are no replicas to place (see
  [Non-goals](../non-goals.md)); partition-to-broker placement is the
  controller's job.

**Source**: `crates/kaas-broker/src/handlers/create_partitions.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs` (`expand_topic`).

**Verified by**: `scripts/kafka-topics.sh` (scenario 4, alter-and-describe);
writer unit tests in `crates/kaas-broker/src/topic_cr_writer.rs`.

## IncrementalAlterConfigs

Per-key topic config mutation ([KIP-339](../kip/kip-339.md)) —
`kafka-configs.sh --alter --add-config` / `--delete-config`.

**Versions**: v0–v1 (flexible from v1).

**Handling**: TOPIC resources only. The handler authorizes `AlterConfigs` on
the topic, translates the op list, and issues a single JSON-merge patch on
`KafkaTopic.spec.config`: `SET` writes the parsed value (integer keys become
JSON numbers), `DELETE` — and `SET` with a null value — write JSON null. The
patchable key set is the eight tunable keys DescribeConfigs reports, accepted in
dotted or camelCase form; a key outside it, or a value that doesn't parse
for its key, is rejected with `INVALID_CONFIG` (40) before anything reaches
the Kubernetes API server. The operator materialises the change on
reconcile, the storage engine's cleaner picks it up, and a subsequent
DescribeConfigs reports the override as `DYNAMIC_TOPIC_CONFIG`.
`validate_only` runs the same validation and skips the patch. `BROKER` and
`BROKER_LOGGER` resource types answer a per-resource `UNSUPPORTED_VERSION`
(35) — there is no dynamic broker-config surface.

**Deviations from Apache 3.7**:

- **`APPEND` and `SUBTRACT` are unsupported** and answer
  `UNSUPPORTED_VERSION` (35): every kaas topic-config key is scalar, so the
  list-valued ops have nothing to apply to.
- Config keys outside the allow-list answer `INVALID_CONFIG` (40)
  as Apache does for unknown names — but the allow-list itself is far
  smaller than Apache's key set, so keys Apache would accept
  (`max.message.bytes`, ...) are rejected here.
- `BROKER` / `BROKER_LOGGER` alteration is unsupported (Apache 3.7 supports
  dynamic broker configs, KIP-226).
- One bad op fails the whole resource — the ops for a resource are applied as
  a single all-or-nothing merge patch.
- The change is asynchronous: it is visible to DescribeConfigs once the
  operator has reconciled the CR (typically well under a second), not
  atomically with the alter response.

**Source**: `crates/kaas-broker/src/handlers/incremental_alter_configs.rs`,
`crates/kaas-broker/src/topic_cr_writer.rs` (`update_topic_config`,
`config_key_to_json_field`, `config_value_to_json`).

**Verified by**: `scripts/kafka-configs.sh` (scenario 3); key/value-mapping
unit tests in `crates/kaas-broker/src/topic_cr_writer.rs`; rejection handler
tests in `crates/kaas-broker/src/handlers/incremental_alter_configs.rs`.
