# KafkaTopic

`KafkaTopic` declares a topic: its partition count, its per-topic
configuration, and (optionally) which storage volumes hold its data.
It is the kaas equivalent of Strimzi's `KafkaTopic`, and like
Strimzi's it is **bidirectional**: you can author topics as CRs in
git, or create them over the Kafka protocol (`kafka-topics.sh
--create`, `AdminClient`, auto-creation) — a wire-created topic
appears as a CR minted by the broker, and wire-level config changes
(`kafka-configs.sh --alter`) are patches to the CR. Either way,
`kubectl get kafkatopics` always shows the truth.

## Spec

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
  namespace: kafka
spec:
  partitions: 12
  config:
    retentionMs: 604800000      # 7 days — also the enforced default
    segmentBytes: 268435456     # 256 MiB
  storage:
    volumes: [premium]          # optional; requires a volume pool
```

| Field | Kafka equivalent | Meaning |
|---|---|---|
| `partitions` | `--partitions` | Partition count, min 1. Can grow, **never shrink** — a decrease is rejected with `Ready=False` and no filesystem change, matching Kafka semantics. |
| `topicName` | the on-wire topic name | Only needed when the Kafka name is not a valid Kubernetes resource name (uppercase, double underscores, >253 chars). Empty means `metadata.name` is the topic name. Mirrors Strimzi's `spec.topicName`. |
| `config` | per-topic configs | See below. |
| `storage` | *(no Apache equivalent)* | Volume placement — see [the volume pool](../../architecture/volume-pool.md). |

There is no `replicas` field and never will be: kaas has [no
replication](../../compat/non-goals.md) — durability comes from the
storage substrate. Wire-level creates asking for `replicationFactor >
1` are accepted and clamped, since clients routinely hardcode 3.

### `spec.config`

Each field maps 1:1 to a Kafka topic config; unset means the broker
default. Values land in the topic's `.config.json` on the shared
volume, which brokers re-read on use — **config changes hot-reload, no
restart**, taking effect at the next retention sweep / segment roll /
append.

| CR field | Kafka config | Notes |
|---|---|---|
| `retentionMs` | `retention.ms` | `-1` = keep forever. Unset = the broker default of 7 days (like Apache) — retention **is enforced**; a topic with no retention config ages out. |
| `retentionBytes` | `retention.bytes` | Per-partition cap; oldest closed segments are deleted first. `-1`/`0` = unlimited. |
| `segmentBytes` | `segment.bytes` | Roll size for log segments. |
| `segmentMs` | `segment.ms` | Time-based roll, 7-day default. This is what makes `retentionMs` effective on low-volume topics — retention only ever deletes *closed* segments. |
| `cleanupPolicy` | `cleanup.policy` | `delete`, `compact`, or `compact,delete`. **Honesty note:** the compactor is not implemented yet (gh #158). Retention skips a `compact` topic, as in Apache, and nothing compacts it, so it keeps every record and its disk use only grows. Size the volume for that, or use `compact,delete`, which runs retention as usual. |
| `minCompactionLagMs` | `min.compaction.lag.ms` | Stored/advertised only, pending the compactor. |
| `deleteRetentionMs` | `delete.retention.ms` | Stored/advertised only, pending the compactor. |
| `flushMessages` | `flush.messages` | Per-topic fsync interval, overriding the broker-wide setting. `1` = fsync every batch (honest `acks=all`), `0` = flush only at segment roll. The durability/throughput dial discussed in [Performance](../performance.md). |

Configs set over the wire (`kafka-configs.sh --alter --topic orders
--add-config retention.ms=86400000`) are patched into `spec.config` on
this CR, and `DescribeConfigs` reports them as dynamic topic configs —
so the CR and the admin API never disagree.

### `spec.storage`

Optional; only meaningful when the chart declares a volume pool.
Either `volumes` (explicit log-dir names) or `volumeSelector` (label
match over pool members) — mutually exclusive. Placement is
**creation-sticky**: editing the list affects new partitions only;
existing partitions never move implicitly, and drift is surfaced in
status rather than auto-migrated. The [volume pool
page](../../architecture/volume-pool.md) covers this in full,
including explicit migration.

## Status

| Field | Meaning |
|---|---|
| `partitionCount` | Partitions actually materialized on disk. |
| `topicId` | Stable v4 UUID minted on first reconcile, never rotated; a deleted-and-recreated topic gets a fresh one (Apache's KIP-516 contract). This UUID is also the topic's on-disk identity stamp, which is what makes delete→recreate safe on shared storage. *(Not yet served on the wire — Metadata still reports nil topic IDs; see the [KIP index](../../compat/kip-index.md).)* |
| `volumeAssignments` | Partition → log-dir name map (creation-sticky record of placement). |
| `partitionsOutsideSpec` | Count of partitions placed on volumes no longer in `spec.storage` — drift from a spec edit, awaiting explicit migration. |
| `conditions` | `Ready`, with rejection reasons (e.g. partition decrease). |

## Deleting a topic

Deleting the CR is the delete path — there is no separate
`kafka-topics.sh --delete` state to reconcile against. Three things
follow, all matching Apache semantics:

- The data is reclaimed (staged aside atomically, then removed — safe
  even while brokers hold the files open).
- The topic's **committed consumer-group offsets are purged**, exactly
  as Apache tombstones them out of `__consumer_offsets`. A recreated
  topic starts with no committed offsets — this is what
  `kafka-streams-application-reset.sh` depends on.
- A recreated topic of the same name is a **new incarnation**: fresh
  `topicId`, fresh producer state, fresh log. Nothing leaks across.

## Implementation notes (for contributors)

- Type: `crates/kaas-operator-api/src/kafkatopic.rs` (use
  `effective_topic_name()`, never read `spec.topic_name` directly —
  gh #86); generated schema `deploy/crds/kaas.rs_kafkatopics.yaml`.
- Reconciler: `crates/kaas-operator-controllers/src/kafkatopic_controller.rs`
  — partition dirs, `.config.json`, TopicID mint (gh #105), identity
  stamping (gh #219), volume placement (gh #221/#224).
- Broker-side admin writes route through
  `crates/kaas-broker/src/topic_cr_writer.rs` (gh #52, gh #9);
  delete-side data/offset hygiene is gh #219/#240/#241.
