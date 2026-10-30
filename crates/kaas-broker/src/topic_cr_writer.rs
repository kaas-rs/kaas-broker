//! `TopicCRWriter` — broker → `KafkaTopic` CR patch path.
//!
//! The admin handlers `CreatePartitions` (key 37) and
//! `IncrementalAlterConfigs` (key 44) translate wire-level config
//! changes into PATCH operations on the corresponding `KafkaTopic`
//! CR. The operator then reconciles the change normally — no
//! direct broker → operator coupling.
//!
//! ## Trait + impls
//!
//! The trait lives at the top level so handlers can take an
//! `Arc<dyn TopicCRWriter>` without depending on `kube`. The real
//! impl is [`KubeTopicCRWriter`] (feature `cr-writer`): kube-backed
//! `Patch::Merge` against `Api<KafkaTopic>`.
//!
//! ## Op surface
//!
//! [`ConfigOp`] mirrors Apache's IncrementalAlterConfigs op enum:
//! `Set` and `Delete` map onto JSON-merge patches; `Append` and
//! `Subtract` are list-valued ops that kaas's topic configs
//! don't support — the writer returns [`TopicWriteError::UnsupportedOp`]
//! and the handler surfaces it as `UNSUPPORTED_VERSION` (35).

use async_trait::async_trait;
use serde_json::Value;
use thiserror::Error;

/// One incremental config-key mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOp {
    /// Topic-config key, e.g. `retention.ms`. Mapped onto the
    /// corresponding `KafkaTopic.spec.config.*` JSON field by
    /// [`TopicCRWriter::update_topic_config`].
    pub key: String,
    pub kind: ConfigOpKind,
}

/// `IncrementalAlterConfigs.AlterConfigOp.OpType` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOpKind {
    /// Set the key to `value`. `value` is None when the wire payload
    /// is null (Apache uses null for "remove"; clients shouldn't
    /// send a Set op with null, but the codec allows it).
    Set,
    /// Remove the key — patch as JSON null.
    Delete,
    /// Append to a list-valued config. kaas's keys are all
    /// scalar — returns `UnsupportedOp` at the writer.
    Append,
    /// Subtract from a list-valued config. Same as Append.
    Subtract,
}

impl ConfigOp {
    /// Convenience: pair a value with a `Set` op.
    pub fn set(key: impl Into<String>, _value: impl Into<String>) -> Self {
        // Note: value is consumed by [`TopicCRWriter::update_topic_config`]'s
        // patch construction at the impl layer; the public op carries the
        // discriminant only. Callers thread the actual value through
        // a parallel slice. (The Apache wire shape has value alongside
        // op; the handler reads both off the codec request.)
        Self {
            key: key.into(),
            kind: ConfigOpKind::Set,
        }
    }
    pub fn delete(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            kind: ConfigOpKind::Delete,
        }
    }
}

/// Errors a writer can surface. Mapped to wire error codes at the
/// handler boundary — see the per-handler tables.
#[derive(Debug, Error)]
pub enum TopicWriteError {
    /// `KafkaTopic` CR with this name doesn't exist in the operator's
    /// namespace. Wire: `UNKNOWN_TOPIC_OR_PARTITION` (3).
    #[error("topic not found: {0}")]
    NotFound(String),

    /// Patch was refused (RBAC, admission webhook). Wire:
    /// `CLUSTER_AUTHORIZATION_FAILED` (31).
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// Caller tried to use `Append` / `Subtract` on a scalar config
    /// key. Wire: `UNSUPPORTED_VERSION` (35).
    #[error("unsupported config op: {0:?}")]
    UnsupportedOp(ConfigOpKind),

    /// gh #236: unknown config key or unparseable value. Wire:
    /// `INVALID_CONFIG` (40) — a real rejection instead of the
    /// silent success that motivated the issue (the API server
    /// *prunes* unknown `spec.config` fields from a merge patch, so
    /// anything not caught here would report success and change
    /// nothing).
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// Caller tried to shrink partition count. Wire:
    /// `INVALID_PARTITIONS` (37).
    #[error("invalid partitions: {0}")]
    InvalidPartitions(String),

    /// CreateTopics: a CR with the requested name already exists.
    /// Wire: `TOPIC_ALREADY_EXISTS` (36).
    #[error("topic already exists: {0}")]
    AlreadyExists(String),

    /// Anything else; bubble up for logging. Wire:
    /// `UNKNOWN_SERVER_ERROR` (-1).
    #[error("other: {0}")]
    Other(String),
}

/// Patch operations the handler issues against the CR.
#[async_trait]
pub trait TopicCRWriter: Send + Sync + 'static {
    /// Create a fresh `KafkaTopic` CR. Called by
    /// `CreateTopicsHandler` (API key 19). `Ok(())` on success or
    /// when the CR already exists (idempotent creates map to
    /// `TOPIC_ALREADY_EXISTS` upstream — the caller decides which
    /// error code to surface).
    ///
    /// `configs` are the wire request's topic-level overrides
    /// (`--config retention.ms=600000`), landed in the minted CR's
    /// `spec.config` so the operator materialises them on first
    /// reconcile (gh #236 — they were previously parsed and
    /// dropped). Unknown keys / bad values fail the whole creation
    /// with [`TopicWriteError::InvalidConfig`].
    async fn create_topic(
        &self,
        name: &str,
        num_partitions: i32,
        configs: &[(String, String)],
    ) -> Result<(), TopicWriteError>;

    /// Patch `KafkaTopic.spec.partitions` to `new_count`. The
    /// operator's reconciler validates the decrease guard; this
    /// helper also catches it client-side so the wire response is
    /// precise.
    async fn expand_topic(&self, name: &str, new_count: i32) -> Result<(), TopicWriteError>;

    /// Apply a set of `(name, op, value)` mutations to
    /// `KafkaTopic.spec.config`. The writer maps each op to a JSON
    /// patch: `Set` → field = parsed-value, `Delete` → field = null.
    /// `Append` / `Subtract` return [`TopicWriteError::UnsupportedOp`].
    async fn update_topic_config(
        &self,
        name: &str,
        ops: &[ConfigOpWithValue],
    ) -> Result<(), TopicWriteError>;

    /// Delete the `KafkaTopic` CR. Called by `DeleteTopicsHandler`
    /// (API key 20). The operator's reconciler tears down the
    /// partition dirs and the topic-watcher fires Deleted on every
    /// broker so open handles close before `remove_dir_all` swings.
    async fn delete_topic(&self, name: &str) -> Result<(), TopicWriteError>;

    /// gh #221 phase 3: flip one partition's placement record
    /// (`status.volumeAssignments[partition] = log_dir`) after an
    /// `AlterReplicaLogDirs` copy completes. The reconciler's
    /// or_insert-only stamping never fights the flipped value.
    async fn set_partition_log_dir(
        &self,
        name: &str,
        partition: i32,
        log_dir: &str,
    ) -> Result<(), TopicWriteError>;
}

/// Op + value pair the handler passes through to the writer. Kept
/// separate from [`ConfigOp`] so the value's lifetime stays scoped
/// to the patch operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOpWithValue {
    pub key: String,
    pub kind: ConfigOpKind,
    /// `None` ↔ wire null. Always `None` for `Delete`.
    pub value: Option<String>,
}

/// Convert a topic-config key + value string into a JSON value for
/// the spec.config patch. The shape mirrors what the operator's
/// `KafkaTopicConfig` deserialiser expects — integer fields as
/// JSON numbers, `cleanupPolicy` as a string.
///
/// gh #236: fallible. The old shape passed bad integers and unknown
/// keys through as strings "for the operator schema to reject" —
/// but a merge patch's unknown fields are *pruned* by the API
/// server, and a string where the CRD wants an integer 422s into an
/// opaque `UNKNOWN_SERVER_ERROR`. Rejecting here yields the
/// `INVALID_CONFIG` (40) a Kafka client actually understands.
pub fn config_value_to_json(key: &str, value: &str) -> Result<Value, TopicWriteError> {
    match key {
        "segment.ms"
        | "segmentMs"
        | "retention.ms"
        | "retentionMs"
        | "retention.bytes"
        | "retentionBytes"
        | "segment.bytes"
        | "segmentBytes"
        | "min.compaction.lag.ms"
        | "minCompactionLagMs"
        | "delete.retention.ms"
        | "deleteRetentionMs"
        | "flush.messages"
        | "flushMessages" => match value.parse::<i64>() {
            Ok(n) => Ok(Value::Number(n.into())),
            Err(_) => Err(TopicWriteError::InvalidConfig(format!(
                "{key}: not an integer: {value:?}"
            ))),
        },
        "cleanup.policy" | "cleanupPolicy" => match value {
            // Same set the CRD's regex admits; rejecting early spares
            // the client an admission-webhook error dressed as -1.
            "delete" | "compact" | "compact,delete" => Ok(Value::String(value.to_string())),
            _ => Err(TopicWriteError::InvalidConfig(format!(
                "{key}: must be delete, compact, or compact,delete; got {value:?}"
            ))),
        },
        _ => Err(TopicWriteError::InvalidConfig(format!(
            "unknown config key: {key}"
        ))),
    }
}

/// Accept-only keys: valid Apache configs whose **only** legal value
/// on kaas is the one kaas already enforces by construction, so they
/// are validated and then deliberately *not* persisted — the default
/// IS the requested behaviour, and a CR field for a knob with one
/// legal value would be noise. The canonical case (found by Kafka
/// Streams the day gh #236 shipped): `InternalTopicManager` stamps
/// `message.timestamp.type=CreateTime` on every repartition/changelog
/// topic it creates, and kaas is CreateTime-only — byte-opacity means
/// the broker never rewrites record timestamps, which is exactly
/// CreateTime semantics (LogAppendTime is the missing half of
/// KIP-32).
fn is_accept_only_key(key: &str) -> bool {
    matches!(key, "message.timestamp.type" | "messageTimestampType")
}

/// Validate an accept-only key's value: `Ok` when it names the
/// behaviour kaas already has, `Err` when it demands one kaas lacks.
fn validate_accept_only(key: &str, value: &str) -> Result<(), TopicWriteError> {
    match (key, value) {
        ("message.timestamp.type" | "messageTimestampType", "CreateTime") => Ok(()),
        ("message.timestamp.type" | "messageTimestampType", other) => {
            Err(TopicWriteError::InvalidConfig(format!(
                "{key}: kaas is CreateTime-only (the broker never rewrites record \
                 timestamps); {other:?} is not supported"
            )))
        }
        _ => Ok(()),
    }
}

/// gh #236: build the `spec.config` JSON object for a fresh CR from
/// the wire request's `(key, value)` pairs. Pure — the CreateTopics
/// handler calls it up front so validation (and `validate_only`)
/// behaves identically whether or not a kube writer is installed.
pub fn create_configs_to_spec(
    configs: &[(String, String)],
) -> Result<serde_json::Map<String, Value>, TopicWriteError> {
    let mut out = serde_json::Map::new();
    for (key, value) in configs {
        if is_accept_only_key(key) {
            validate_accept_only(key, value)?;
            continue;
        }
        let Some(field) = config_key_to_json_field(key) else {
            return Err(TopicWriteError::InvalidConfig(format!(
                "unknown config key: {key}"
            )));
        };
        out.insert(field.into(), config_value_to_json(key, value)?);
    }
    Ok(out)
}

/// gh #236: translate IncrementalAlterConfigs ops into the
/// `spec.config` merge-patch object (`Set` → parsed value, `Delete`
/// / `Set` with null → JSON null). Pure for the same reason as
/// [`create_configs_to_spec`] — the handler validates through it
/// before touching the writer.
pub fn ops_to_config_patch(
    ops: &[ConfigOpWithValue],
) -> Result<serde_json::Map<String, Value>, TopicWriteError> {
    let mut out = serde_json::Map::new();
    for op in ops {
        match op.kind {
            ConfigOpKind::Append | ConfigOpKind::Subtract => {
                return Err(TopicWriteError::UnsupportedOp(op.kind));
            }
            ConfigOpKind::Set => {
                if is_accept_only_key(&op.key) {
                    if let Some(value) = op.value.as_deref() {
                        validate_accept_only(&op.key, value)?;
                    }
                    // Valid and already the enforced behaviour —
                    // nothing to persist.
                    continue;
                }
                let Some(field) = config_key_to_json_field(&op.key) else {
                    return Err(TopicWriteError::InvalidConfig(format!(
                        "unknown config key: {}",
                        op.key
                    )));
                };
                match op.value.as_deref() {
                    // Set with null → treat as Delete.
                    None => {
                        out.insert(field.into(), Value::Null);
                    }
                    Some(value) => {
                        out.insert(field.into(), config_value_to_json(&op.key, value)?);
                    }
                }
            }
            ConfigOpKind::Delete => {
                // Deleting an accept-only key resets it to a default
                // it already has — a no-op, not an error.
                if is_accept_only_key(&op.key) {
                    continue;
                }
                let Some(field) = config_key_to_json_field(&op.key) else {
                    return Err(TopicWriteError::InvalidConfig(format!(
                        "unknown config key: {}",
                        op.key
                    )));
                };
                out.insert(field.into(), Value::Null);
            }
        }
    }
    Ok(out)
}

/// Map an Apache wire `key` to the JSON field on
/// `KafkaTopicConfig`. The CR carries camelCase fields; the wire
/// uses dotted names. Returns `None` for unknown keys, which the
/// handler reports as `UNSUPPORTED_VERSION`.
pub fn config_key_to_json_field(key: &str) -> Option<&'static str> {
    match key {
        "retention.ms" | "retentionMs" => Some("retentionMs"),
        "segment.ms" | "segmentMs" => Some("segmentMs"),
        "retention.bytes" | "retentionBytes" => Some("retentionBytes"),
        "segment.bytes" | "segmentBytes" => Some("segmentBytes"),
        "cleanup.policy" | "cleanupPolicy" => Some("cleanupPolicy"),
        "min.compaction.lag.ms" | "minCompactionLagMs" => Some("minCompactionLagMs"),
        "delete.retention.ms" | "deleteRetentionMs" => Some("deleteRetentionMs"),
        // gh #213: per-topic flush.messages. Rejecting it broke every
        // client that creates topics with `--config flush.messages=1`
        // (the bench suite does) the moment gh #236 started validating.
        "flush.messages" | "flushMessages" => Some("flushMessages"),
        _ => None,
    }
}

/// Returns the `(metadata.name, spec.topicName)` pair for a Kafka
/// topic name (gh #86). A valid RFC 1123 subdomain name is used as
/// `metadata.name` directly with `spec.topicName` left empty
/// (Strimzi's recommendation). Otherwise — uppercase Streams
/// internals like `app-KSTREAM-AGGREGATE-...-repartition`, dotted
/// names, >253 chars — synthesise a deterministic
/// `kaas-topic-<16 hex of sha1[:8]>` and stash the literal Kafka
/// name in `spec.topicName`.
///
/// The synthetic shape MUST stay byte-identical to the v0.1 broker's
/// output: during a mixed-version rollout both sides must resolve the
/// same Kafka name to the same CR, or a flavor flip would duplicate
/// CRs for the same topic directory.
pub fn name_for_cr(kafka_name: &str) -> (String, String) {
    if kafka_name.len() <= 253 && is_rfc1123_subdomain(kafka_name) {
        return (kafka_name.to_string(), String::new());
    }
    use sha1::{Digest, Sha1};
    let digest = Sha1::digest(kafka_name.as_bytes());
    use std::fmt::Write;
    let hex = digest[..8].iter().fold(String::new(), |mut acc, b| {
        let _ = write!(acc, "{b:02x}");
        acc
    });
    (format!("kaas-topic-{hex}"), kafka_name.to_string())
}

/// K8s resource-name validation: lowercase alphanumeric labels with
/// interior hyphens, dot-separated. Implements the rfc1123
/// check without pulling the regex crate in.
fn is_rfc1123_subdomain(s: &str) -> bool {
    !s.is_empty()
        && s.split('.').all(|label| {
            !label.is_empty()
                && label.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                && label.ends_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
                && label
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        })
}

// --- kube-backed impl ------------------------------------------------

#[cfg(feature = "cr-writer")]
pub use kube_impl::KubeTopicCRWriter;

#[cfg(feature = "cr-writer")]
mod kube_impl {
    use super::*;
    use kaas_operator_api::KafkaTopic;
    use kube::api::{Patch, PatchParams};

    /// gh #245: stamp broker writes with a recognisable fieldManager
    /// so managedFields attributes them (default is `unknown`).
    fn broker_patch_params() -> PatchParams {
        PatchParams {
            field_manager: Some("kaas-broker".to_owned()),
            ..Default::default()
        }
    }
    use kube::Api;
    use serde_json::json;

    /// Real kube-backed writer.
    #[derive(Clone)]
    pub struct KubeTopicCRWriter {
        client: kube::Client,
        namespace: String,
        argocd: crate::argocd::ArgoCdConfig,
    }

    impl std::fmt::Debug for KubeTopicCRWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KubeTopicCRWriter")
                .field("namespace", &self.namespace)
                .field("argocd", &self.argocd)
                .finish_non_exhaustive()
        }
    }

    impl KubeTopicCRWriter {
        pub fn new(client: kube::Client, namespace: impl Into<String>) -> Self {
            Self {
                client,
                namespace: namespace.into(),
                argocd: crate::argocd::ArgoCdConfig::default(),
            }
        }

        /// Opt broker-minted CRs into the ArgoCD Application tree
        /// (gh #84 + gh #106, re-ported from the Go broker). The
        /// default config stamps nothing.
        pub fn with_argocd(mut self, argocd: crate::argocd::ArgoCdConfig) -> Self {
            self.argocd = argocd;
            self
        }

        fn api(&self) -> Api<KafkaTopic> {
            Api::namespaced(self.client.clone(), &self.namespace)
        }
    }

    /// Build the CR `create_topic` will POST. A free function (no
    /// client) so the annotation/config stamping is testable without
    /// a cluster.
    fn build_cr(
        namespace: &str,
        argocd: &crate::argocd::ArgoCdConfig,
        name: &str,
        num_partitions: i32,
        configs: &[(String, String)],
    ) -> Result<KafkaTopic, TopicWriteError> {
        use kaas_operator_api::{KafkaTopicConfig, KafkaTopicSpec};

        // gh #236: land the wire request's config overrides in the
        // minted CR so the operator materialises them on first
        // reconcile. The handler has already validated the pairs;
        // this re-derivation keeps the writer safe for other
        // callers.
        let spec_config = super::create_configs_to_spec(configs)?;
        let config: KafkaTopicConfig = serde_json::from_value(Value::Object(spec_config))
            .map_err(|e| TopicWriteError::InvalidConfig(e.to_string()))?;

        // gh #86: non-RFC-1123 Kafka names (Streams internals)
        // get a deterministic synthetic CR name with the literal
        // name carried in spec.topicName.
        let (meta_name, topic_name) = super::name_for_cr(name);
        // ArgoCD coexistence (see `crate::argocd`): the tracking-id
        // must carry the CR's metadata.name — the synthesised one
        // on the gh #86 path — because that is what keys ArgoCD's
        // resource tree. None (the default) leaves the CR plain.
        let annotations = argocd.annotations(
            &<KafkaTopic as kube::Resource>::group(&()),
            &<KafkaTopic as kube::Resource>::kind(&()),
            namespace,
            &meta_name,
        );
        Ok(KafkaTopic {
            metadata: kube::api::ObjectMeta {
                name: Some(meta_name),
                namespace: Some(namespace.to_owned()),
                annotations,
                ..Default::default()
            },
            spec: KafkaTopicSpec {
                topic_name,
                partitions: num_partitions,
                config,
                storage: None,
            },
            status: None,
        })
    }

    #[async_trait::async_trait]
    impl TopicCRWriter for KubeTopicCRWriter {
        async fn create_topic(
            &self,
            name: &str,
            num_partitions: i32,
            configs: &[(String, String)],
        ) -> Result<(), TopicWriteError> {
            use kube::api::PostParams;

            let cr = build_cr(&self.namespace, &self.argocd, name, num_partitions, configs)?;
            // gh #245: name the writer. Without a fieldManager every
            // broker-minted CR shows `manager: unknown` in
            // managedFields, which is exactly what made the 2026-08-02
            // CR-deletion incident unattributable after the fact.
            let pp = PostParams {
                field_manager: Some("kaas-broker".to_owned()),
                ..Default::default()
            };
            match self.api().create(&pp, &cr).await {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 409 => {
                    Err(TopicWriteError::AlreadyExists(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn expand_topic(&self, name: &str, new_count: i32) -> Result<(), TopicWriteError> {
            // Client-side decrease guard: read current, refuse if
            // shrinking. The operator-side reconciler enforces the
            // same rule (with the status-condition message); doing
            // it here too returns a precise wire code without
            // round-tripping the operator.
            let (meta_name, _) = super::name_for_cr(name);
            let api = self.api();
            match api.get(&meta_name).await {
                Ok(t) => {
                    if t.spec.partitions > new_count {
                        return Err(TopicWriteError::InvalidPartitions(format!(
                            "current {} → requested {}",
                            t.spec.partitions, new_count
                        )));
                    }
                }
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(TopicWriteError::NotFound(name.into()));
                }
                Err(e) => return Err(map_kube_err(e)),
            }
            let patch = json!({ "spec": { "partitions": new_count } });
            api.patch(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
                .map(|_| ())
                .map_err(map_kube_err)
        }

        async fn update_topic_config(
            &self,
            name: &str,
            ops: &[ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            let config = super::ops_to_config_patch(ops)?;
            let patch = json!({ "spec": { "config": config } });
            let (meta_name, _) = super::name_for_cr(name);
            let api = self.api();
            match api
                .patch(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn delete_topic(&self, name: &str) -> Result<(), TopicWriteError> {
            use kube::api::DeleteParams;
            let (meta_name, _) = super::name_for_cr(name);
            match self
                .api()
                .delete(&meta_name, &DeleteParams::default())
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }

        async fn set_partition_log_dir(
            &self,
            name: &str,
            partition: i32,
            log_dir: &str,
        ) -> Result<(), TopicWriteError> {
            // JSON merge patch on the status map: object fields merge,
            // so only this partition's entry changes. Needs the
            // kafkatopics/status RBAC verb (broker-rbac.yaml).
            let mut assignments = serde_json::Map::new();
            assignments.insert(partition.to_string(), Value::String(log_dir.to_owned()));
            let patch = json!({ "status": { "volumeAssignments": assignments } });
            let (meta_name, _) = super::name_for_cr(name);
            match self
                .api()
                .patch_status(&meta_name, &broker_patch_params(), &Patch::Merge(&patch))
                .await
            {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(TopicWriteError::NotFound(name.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }
    }

    fn map_kube_err(e: kube::Error) -> TopicWriteError {
        match &e {
            kube::Error::Api(api) if api.code == 403 => {
                TopicWriteError::Forbidden(api.message.clone())
            }
            kube::Error::Api(api) if api.code == 404 => {
                TopicWriteError::NotFound(api.message.clone())
            }
            _ => TopicWriteError::Other(e.to_string()),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::argocd::ArgoCdConfig;

        fn argo() -> ArgoCdConfig {
            ArgoCdConfig {
                enabled: true,
                application_name: "kaas".into(),
                compare_options: "IgnoreExtraneous".into(),
                sync_options: "Delete=false".into(),
            }
        }

        #[test]
        fn default_config_mints_plain_crs() {
            let cr = build_cr("kaas", &ArgoCdConfig::default(), "orders", 1, &[]).expect("build");
            assert_eq!(
                cr.metadata.annotations, None,
                "non-ArgoCD installs must see no argocd.argoproj.io/* metadata"
            );
        }

        #[test]
        fn argocd_config_stamps_tracking_and_coexistence_annotations() {
            let cr = build_cr("kaas", &argo(), "orders", 1, &[]).expect("build");
            let ann = cr.metadata.annotations.expect("annotations present");
            assert_eq!(
                ann.get("argocd.argoproj.io/tracking-id")
                    .map(String::as_str),
                Some("kaas:kaas.rs/KafkaTopic:kaas/orders")
            );
            assert_eq!(
                ann.get("argocd.argoproj.io/compare-options")
                    .map(String::as_str),
                Some("IgnoreExtraneous")
            );
            assert_eq!(
                ann.get("argocd.argoproj.io/sync-options")
                    .map(String::as_str),
                Some("Delete=false")
            );
        }

        #[test]
        fn tracking_id_uses_the_synthesised_meta_name() {
            // gh #86 names (Streams internals) get a synthetic CR
            // name, and ArgoCD's tree is keyed by metadata.name — a
            // tracking-id carrying the Kafka name would point at a
            // resource that doesn't exist.
            let kafka_name = "app-KSTREAM-AGGREGATE-STATE-STORE-repartition";
            let cr = build_cr("kaas", &argo(), kafka_name, 1, &[]).expect("build");
            let (meta_name, _) = super::super::name_for_cr(kafka_name);
            assert!(meta_name.starts_with("kaas-topic-"), "precondition");
            let ann = cr.metadata.annotations.expect("annotations present");
            assert_eq!(
                ann.get("argocd.argoproj.io/tracking-id")
                    .map(String::as_str),
                Some(format!("kaas:kaas.rs/KafkaTopic:kaas/{meta_name}").as_str())
            );
        }

        #[test]
        fn every_accepted_config_key_survives_the_crd_schema() {
            // gh #236's dangerous half: the API server prunes merge-patch
            // fields the CRD doesn't declare, so a key mapped to a field
            // the CRD lacks "succeeds" and stores nothing. A field serde
            // drops on the way through `KafkaTopicConfig` is one the API
            // server would prune.
            use super::super::{ops_to_config_patch, ConfigOpKind, ConfigOpWithValue};
            use kaas_operator_api::KafkaTopicConfig;
            let pairs: Vec<(String, String)> = [
                ("retention.ms", "600000"),
                ("retention.bytes", "1048576"),
                ("segment.bytes", "16777216"),
                ("segment.ms", "3600000"),
                ("cleanup.policy", "compact"),
                ("min.compaction.lag.ms", "5000"),
                ("delete.retention.ms", "7000"),
                ("flush.messages", "1"),
            ]
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();

            let spec = super::super::create_configs_to_spec(&pairs).expect("all accepted");
            assert_eq!(spec.len(), pairs.len());
            let cfg: KafkaTopicConfig =
                serde_json::from_value(Value::Object(spec.clone())).expect("CRD shape");
            assert_eq!(
                serde_json::to_value(&cfg).unwrap(),
                Value::Object(spec.clone()),
                "a field the CRD doesn't declare was dropped"
            );

            // IncrementalAlterConfigs SETs build the same merge patch.
            let ops: Vec<ConfigOpWithValue> = pairs
                .iter()
                .map(|(k, v)| ConfigOpWithValue {
                    key: k.clone(),
                    kind: ConfigOpKind::Set,
                    value: Some(v.clone()),
                })
                .collect();
            assert_eq!(ops_to_config_patch(&ops).unwrap(), spec);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_key_to_json_field_matches_known_keys() {
        assert_eq!(
            config_key_to_json_field("retention.ms"),
            Some("retentionMs")
        );
        assert_eq!(config_key_to_json_field("retentionMs"), Some("retentionMs"));
        assert_eq!(
            config_key_to_json_field("cleanup.policy"),
            Some("cleanupPolicy")
        );
        assert_eq!(
            config_key_to_json_field("min.compaction.lag.ms"),
            Some("minCompactionLagMs")
        );
        assert_eq!(config_key_to_json_field("unknown.key"), None);
    }

    #[test]
    fn config_value_parses_integer_fields() {
        assert_eq!(
            config_value_to_json("retention.ms", "60000").unwrap(),
            Value::Number(60_000_i64.into())
        );
        assert_eq!(
            config_value_to_json("cleanup.policy", "compact").unwrap(),
            Value::String("compact".into())
        );
        // gh #236: an unparseable integer is a rejection, not a
        // string the API server would 422 on (or worse, prune).
        assert!(matches!(
            config_value_to_json("retention.ms", "huh"),
            Err(TopicWriteError::InvalidConfig(_))
        ));
        assert!(matches!(
            config_value_to_json("cleanup.policy", "vacuum"),
            Err(TopicWriteError::InvalidConfig(_))
        ));
    }

    #[test]
    fn create_configs_land_in_spec_config_fields() {
        let spec = create_configs_to_spec(&[
            ("retention.ms".into(), "600000".into()),
            ("segment.bytes".into(), "16777216".into()),
            ("cleanup.policy".into(), "compact".into()),
        ])
        .unwrap();
        assert_eq!(spec["retentionMs"], Value::Number(600_000_i64.into()));
        assert_eq!(spec["segmentBytes"], Value::Number(16_777_216_i64.into()));
        assert_eq!(spec["cleanupPolicy"], Value::String("compact".into()));
    }

    #[test]
    fn flush_messages_is_a_supported_create_config() {
        // gh #213 regression shape: `kafka-topics --create --config
        // flush.messages=1` — the bench suite's init container. The
        // gh #236 validation initially rejected it, failing every
        // topic create that carried the flag.
        let spec = create_configs_to_spec(&[("flush.messages".into(), "1".into())]).unwrap();
        assert_eq!(spec["flushMessages"], Value::Number(1_i64.into()));
    }

    #[test]
    fn message_timestamp_type_createtime_is_accepted_and_dropped() {
        // The Kafka Streams shape: InternalTopicManager creates every
        // repartition/changelog topic with this key. CreateTime is
        // kaas's only behaviour, so it validates and stays out of the
        // CR; LogAppendTime demands the missing KIP-32 half → 40.
        let spec = create_configs_to_spec(&[
            ("cleanup.policy".into(), "compact".into()),
            ("message.timestamp.type".into(), "CreateTime".into()),
        ])
        .unwrap();
        assert_eq!(spec.len(), 1, "accept-only key must not be persisted");
        assert!(spec.contains_key("cleanupPolicy"));

        let err =
            create_configs_to_spec(&[("message.timestamp.type".into(), "LogAppendTime".into())])
                .unwrap_err();
        assert!(matches!(err, TopicWriteError::InvalidConfig(_)));
    }

    #[test]
    fn create_config_with_unknown_key_is_invalid_config() {
        // The dangerous half of gh #236 was the silent success: an
        // unsupported key must fail the creation, not vanish.
        let err =
            create_configs_to_spec(&[("max.message.bytes".into(), "1000".into())]).unwrap_err();
        assert!(matches!(err, TopicWriteError::InvalidConfig(_)));
    }

    #[test]
    fn alter_ops_map_set_and_delete() {
        let patch = ops_to_config_patch(&[
            ConfigOpWithValue {
                key: "retention.ms".into(),
                kind: ConfigOpKind::Set,
                value: Some("1200000".into()),
            },
            ConfigOpWithValue {
                key: "segment.ms".into(),
                kind: ConfigOpKind::Delete,
                value: None,
            },
        ])
        .unwrap();
        assert_eq!(patch["retentionMs"], Value::Number(1_200_000_i64.into()));
        assert_eq!(patch["segmentMs"], Value::Null);
    }

    #[test]
    fn alter_op_on_unknown_key_is_invalid_config() {
        // Was UnsupportedOp → UNSUPPORTED_VERSION (35), which told the
        // client its *client* was too new. INVALID_CONFIG (40) names
        // the actual problem.
        let err = ops_to_config_patch(&[ConfigOpWithValue {
            key: "max.message.bytes".into(),
            kind: ConfigOpKind::Set,
            value: Some("1000".into()),
        }])
        .unwrap_err();
        assert!(matches!(err, TopicWriteError::InvalidConfig(_)));
    }
}
