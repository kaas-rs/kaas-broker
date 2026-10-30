//! `KafkaTopic` — partition-dir + topic-config CR.
//!
//! The
//! operator's `KafkaTopicReconciler` materialises one partition
//! directory per partition under `/data/<topic>/<partition>/`,
//! writes a `.config.json` next to it, and mints `Status.TopicID`
//! (gh #105, KIP-516) as a v4 UUID on first reconcile.
//!
//! `metadata.name` is the K8s resource name; the on-wire Kafka topic
//! name comes from [`KafkaTopicSpec::topic_name`] (gh #86 — admin-
//! protocol creates synthesise a hash-derived metadata.name when the
//! Kafka name fails RFC 1123). [`KafkaTopic::effective_topic_name`]
//! is the canonical accessor; reach for it instead of either field
//! directly.

use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::condition::Condition;

#[derive(CustomResource, Debug, Clone, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[kube(
    group = "kaas.rs",
    version = "v1alpha1",
    kind = "KafkaTopic",
    plural = "kafkatopics",
    singular = "kafkatopic",
    namespaced,
    status = "KafkaTopicStatus",
    printcolumn = r#"{"name":"Partitions","type":"integer","jsonPath":".spec.partitions"}"#,
    printcolumn = r#"{"name":"Ready","type":"string","jsonPath":".status.conditions[?(@.type=='Ready')].status"}"#
)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicSpec {
    /// On-wire Kafka topic name. Empty → defaults to `metadata.name`.
    /// Use [`KafkaTopic::effective_topic_name`] in callers; never read
    /// this field directly.
    ///
    /// gh #86 admin-protocol path: when a literal Kafka topic name
    /// (uppercase, double underscores, > 253 chars) isn't a valid RFC
    /// 1123 K8s resource name, the broker synthesises a hash-derived
    /// `metadata.name` and stores the literal name here. Mirrors
    /// Strimzi's `spec.topicName`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(length(max = 249))]
    pub topic_name: String,

    /// Partition count. Cannot be decreased once set (the reconciler
    /// rejects with `InvalidPartitionCount`).
    #[schemars(range(min = 1))]
    pub partitions: i32,

    #[serde(default)]
    pub config: KafkaTopicConfig,

    /// gh #221 phase 2: which log dirs (pool volumes) this topic's
    /// partitions may be placed on.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub storage: Option<KafkaTopicStorage>,
}

/// Per-topic volume binding (gh #221 phase 2). One field, three
/// cases: a single name pins the topic to that log dir; multiple
/// names stripe partitions round-robin across the set; unset uses
/// the default set (every pool member with `defaultEligible: true`).
///
/// Placement is **creation-sticky**: editing this list never moves
/// existing partitions — new placements follow the new set, and
/// partitions sitting outside it are surfaced via
/// `status.partitionsOutsideSpec` until an explicit migration.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStorage {
    /// Pool log-dir names (chart `storage.pool[].name`; `default` is
    /// the data volume). Unknown names fail the reconcile loudly.
    /// Mutually exclusive with `volumeSelector`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub volumes: Vec<String>,

    /// gh #224: label selector over pool members (nodeSelector
    /// vocabulary — every key/value must match a member's
    /// `storage.pool[].labels`). Resolved to an eligible set at
    /// reconcile time, so topic CRs stay decoupled from
    /// infrastructure names. Mutually exclusive with `volumes`;
    /// matching zero members fails the reconcile loudly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_selector: Option<std::collections::BTreeMap<String, String>>,
}

/// Per-topic configuration knobs. All optional — unset = broker default.
/// Field shapes mirror the operator-side JSON exactly so the broker's
/// `kaas_storage::TopicConfigFile` deserialises the same `.config.json`
/// the operator writes.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicConfig {
    /// `retention.ms` in Kafka semantics: `-1` = infinite (never delete
    /// by time). Streams sets this on changelog topics.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = -1))]
    pub retention_ms: Option<i64>,

    /// Caps per-partition log size. When the cleaner runs and a
    /// partition's total segment bytes exceed this, oldest closed
    /// segments are deleted until the partition is back under the
    /// limit. Active segment is never touched. `-1` = unlimited
    /// (Kafka convention); `0` = treat as unlimited too.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = -1))]
    pub retention_bytes: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1))]
    pub segment_bytes: Option<i64>,

    /// Rolls the active segment once it has been open this long, even
    /// if it never reaches `segmentBytes`. Apache's `segment.ms`, same
    /// 7-day default. `-1` = never roll on time.
    ///
    /// This is what makes `retentionMs` work on a low-volume topic:
    /// retention only ever deletes *closed* segments, so a topic that
    /// never fills a segment keeps everything regardless of its
    /// retention setting.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = -1))]
    pub segment_ms: Option<i64>,

    /// `delete`, `compact`, or `compact,delete`. Empty → broker
    /// default (`delete`). Validation is operator-side via the regex.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    #[schemars(regex(pattern = r"^(delete|compact|compact,delete)?$"))]
    pub cleanup_policy: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub min_compaction_lag_ms: Option<i64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub delete_retention_ms: Option<i64>,

    /// Apache's per-topic `flush.messages` (gh #213): fsync the log
    /// after this many records, overriding the broker-wide
    /// `KAAS_FLUSH_INTERVAL_MESSAGES`. `1` = fsync every batch
    /// (honest acks=all); `0` = flush only at segment roll. Unset =
    /// broker default.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 0))]
    pub flush_messages: Option<i64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct KafkaTopicStatus {
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub partition_count: i32,

    /// Stable v4 UUID minted by the operator on first reconcile
    /// (gh #105, KIP-516). Never rotated; deleting + re-creating a
    /// topic mints a fresh UUID per Apache's contract. The broker
    /// surfaces this on Metadata v10+ responses.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topic_id: String,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub conditions: Vec<Condition>,

    /// gh #221 phase 2: partition → log-dir name, stamped by the
    /// reconciler when a partition is first placed and never
    /// recomputed for existing partitions (creation-sticky). String
    /// keys because JSON object keys are strings.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub volume_assignments: std::collections::BTreeMap<String, String>,

    /// Number of partitions currently placed on log dirs outside
    /// `spec.storage.volumes` — drift from a spec edit, surfaced
    /// instead of auto-migrated (data moves are explicit, gh #221).
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub partitions_outside_spec: i32,
}

impl KafkaTopic {
    /// On-wire Kafka topic name. Returns `spec.topic_name` when set,
    /// otherwise `metadata.name`. Callers in the broker
    /// (`run_topic_watch`) and operator (KafkaTopicReconciler) MUST use
    /// this accessor — see the gh #86 note in the module-level
    /// docs.
    pub fn effective_topic_name(&self) -> &str {
        if !self.spec.topic_name.is_empty() {
            return &self.spec.topic_name;
        }
        self.metadata.name.as_deref().unwrap_or("")
    }
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
