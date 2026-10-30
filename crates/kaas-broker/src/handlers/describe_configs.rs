//! DescribeConfigs handler — API key 32.
//!
//! Topic-only surface (BROKER + BROKER_LOGGER return
//! `UNSUPPORTED_VERSION` (35) on the per-resource result — same
//! Strimzi-compat shape `IncrementalAlterConfigs` uses).
//!
//! For each topic resource, answer with the Apache-3.7-compatible
//! default table plus the topic's stored overrides (gh #236). The
//! handler doesn't reach into the operator's `KafkaTopic.spec.config`
//! directly — it re-reads the operator-materialised `.config.json`
//! through `StorageEngine::topic_config` on every request (the same
//! hot-reload contract the retention sweep uses), so an override
//! reports `DYNAMIC_TOPIC_CONFIG` and a CR edit is visible with no
//! restart.
//!
//! v1+ adds `include_synonyms` (every entry carries a default-source
//! synonym, preceded by a dynamic-source one when overridden —
//! Apache's synonym-chain shape). v3+ adds `include_documentation`
//! (looked up via [`topic_config_defaults::description`]).
//!
//! Authorization: `Operation::DescribeConfigs` on the topic.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::describe_configs::{
    self, config_type, resource_type, source, DescribeConfigsResult, DescribeConfigsResultConfig,
    DescribeConfigsSynonym, Response,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_config_defaults;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNSUPPORTED_VERSION: i16 = 35;

#[derive(Debug)]
pub struct DescribeConfigsHandler {
    broker: Arc<Broker>,
}

impl DescribeConfigsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeConfigsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_configs::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let mut results = Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            // gh #109 parity: BROKER resources answer the live broker
            // config (read-only DEFAULT_CONFIG entries) so
            // kafka-configs.sh --entity-type brokers and kafbat-ui's
            // broker page work. Only BROKER_LOGGER (and anything
            // else) stays unsupported.
            if resource.resource_type == resource_type::BROKER {
                results.push(DescribeConfigsResult {
                    error_code: ERR_NONE,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: broker_configs(&self.broker, &resource, version),
                });
                continue;
            }
            if resource.resource_type != resource_type::TOPIC {
                results.push(DescribeConfigsResult {
                    error_code: ERR_UNSUPPORTED_VERSION,
                    error_message: Some("only Topic resource type is supported".into()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            // Authorize.
            let res = Resource::topic(&resource.resource_name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &res, Operation::DescribeConfigs)
            {
                results.push(DescribeConfigsResult {
                    error_code: ERR_TOPIC_AUTHZ_FAILED,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            // Topic must exist on this broker.
            if self.broker.topics.get(&resource.resource_name).is_none() {
                results.push(DescribeConfigsResult {
                    error_code: ERR_UNKNOWN_TOPIC,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name.clone(),
                    configs: vec![],
                });
                continue;
            }

            // gh #236: layer the topic's stored overrides (the
            // operator-materialised `.config.json`) over the default
            // table, so an override reports `DYNAMIC_TOPIC_CONFIG`
            // and `--describe` (non-`--all`) shows it. Re-read per
            // request — same hot-reload contract the retention sweep
            // uses.
            let overrides = self
                .broker
                .engine
                .topic_config(&resource.resource_name)
                .unwrap_or_default();

            let configs = topic_config_defaults::ALL_KEYS
                .iter()
                .filter(|entry| match resource.configuration_keys.as_ref() {
                    None => true,
                    Some(keys) => keys.iter().any(|k| k == entry.dotted_name),
                })
                .map(|entry| {
                    make_config(
                        entry,
                        override_value(&overrides, entry.dotted_name),
                        version,
                    )
                })
                .collect();

            results.push(DescribeConfigsResult {
                error_code: ERR_NONE,
                error_message: None,
                resource_type: resource.resource_type,
                resource_name: resource.resource_name.clone(),
                configs,
            });
        }

        let resp = Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        describe_configs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

/// gh #109 broker-config table. Values match Apache 3.7's defaults
/// where kaas has no live knob, and kaas's architectural
/// invariants where it does (replication factor is always 1 — the
/// CSI layer owns durability, not Kafka-level replication). Same
/// entry set as v0.1's `brokerConfigs` minus `listeners`
/// (the broker doesn't thread the advertised host into the
/// handler; kafbat-ui renders the rest fine without it).
fn broker_configs(
    broker: &Broker,
    resource: &kaas_codec::api::describe_configs::DescribeConfigsResource,
    version: i16,
) -> Vec<DescribeConfigsResultConfig> {
    let entries: &[(&str, String)] = &[
        ("broker.id", broker.broker_id.to_string()),
        ("auto.create.topics.enable", "true".into()),
        ("num.partitions", "1".into()),
        ("default.replication.factor", "1".into()),
        ("inter.broker.protocol.version", "3.6".into()),
        ("kafka.version", "3.6.0".into()),
    ];
    entries
        .iter()
        .filter(|(name, _)| match resource.configuration_keys.as_ref() {
            None => true,
            Some(keys) => keys.iter().any(|k| k == name),
        })
        .map(|(name, value)| DescribeConfigsResultConfig {
            name: (*name).into(),
            value: Some(value.clone()),
            read_only: true,
            is_default: true,
            is_sensitive: false,
            synonyms: vec![],
            config_type: if version >= 2 {
                config_type::STRING
            } else {
                config_type::UNKNOWN
            },
            config_source: if version >= 1 {
                source::DEFAULT_CONFIG
            } else {
                source::UNKNOWN
            },
            documentation: None,
        })
        .collect()
}

/// The topic's stored override for `key` as a wire string, or `None`
/// when the topic falls through to the default. `Option` fields use
/// presence; `cleanup.policy`'s unset shape is the empty string
/// (v0.1 `omitempty` compatibility, see `TopicConfigFile`).
fn override_value(cfg: &kaas_storage::TopicConfigFile, key: &str) -> Option<String> {
    match key {
        "retention.ms" => cfg.retention_ms.map(|v| v.to_string()),
        "retention.bytes" => cfg.retention_bytes.map(|v| v.to_string()),
        "segment.bytes" => cfg.segment_bytes.map(|v| v.to_string()),
        "segment.ms" => cfg.segment_ms.map(|v| v.to_string()),
        "cleanup.policy" => (!cfg.cleanup_policy.is_empty()).then(|| cfg.cleanup_policy.clone()),
        "min.compaction.lag.ms" => cfg.min_compaction_lag_ms.map(|v| v.to_string()),
        "delete.retention.ms" => cfg.delete_retention_ms.map(|v| v.to_string()),
        "flush.messages" => cfg.flush_messages.map(|v| v.to_string()),
        _ => None,
    }
}

fn make_config(
    entry: &topic_config_defaults::Entry,
    override_value: Option<String>,
    version: i16,
) -> DescribeConfigsResultConfig {
    let default = entry.default_value.map(str::to_owned);
    let is_default = override_value.is_none();
    let value = override_value.or_else(|| default.clone());
    let synonyms = if version >= 1 {
        // Apache's synonym chain: the dynamic topic override first
        // (when set), then the cluster default it shadows.
        let mut s = Vec::with_capacity(2);
        if !is_default {
            s.push(DescribeConfigsSynonym {
                name: entry.dotted_name.into(),
                value: value.clone(),
                source: source::DYNAMIC_TOPIC_CONFIG,
            });
        }
        s.push(DescribeConfigsSynonym {
            name: entry.dotted_name.into(),
            value: default,
            source: source::DEFAULT_CONFIG,
        });
        s
    } else {
        vec![]
    };
    DescribeConfigsResultConfig {
        name: entry.dotted_name.into(),
        value,
        read_only: false,
        is_default,
        is_sensitive: false,
        synonyms,
        config_type: if version >= 2 {
            entry.config_type
        } else {
            config_type::UNKNOWN
        },
        config_source: if version >= 1 {
            if is_default {
                source::DEFAULT_CONFIG
            } else {
                source::DYNAMIC_TOPIC_CONFIG
            }
        } else {
            source::UNKNOWN
        },
        documentation: if version >= 3 {
            Some(entry.documentation.into())
        } else {
            None
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::describe_configs::{DescribeConfigsResource, Request};
    use kaas_storage::{
        DiskStorageEngine, MemoryStorage, PartitionConfig, RealFs, StorageEngine, TopicConfigFile,
    };
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with(engine: Arc<dyn StorageEngine>, topic: &str) -> Arc<Broker> {
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: topic.to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    async fn describe(broker: Arc<Broker>, topic: &str) -> Vec<DescribeConfigsResultConfig> {
        let h = DescribeConfigsHandler::new(broker);
        let req = Request {
            resources: vec![DescribeConfigsResource {
                resource_type: resource_type::TOPIC,
                resource_name: topic.to_owned(),
                configuration_keys: None,
            }],
            include_synonyms: true,
            include_documentation: false,
        };
        let mut buf = BytesMut::new();
        describe_configs::encode_request(&mut buf, &req, 4).unwrap();
        let out = h.handle(&conn(), 4, buf.freeze()).await.unwrap();
        let mut r = out.freeze();
        let resp = describe_configs::decode_response(&mut r, 4).unwrap();
        assert_eq!(resp.results[0].error_code, ERR_NONE);
        resp.results[0].configs.clone()
    }

    fn entry<'a>(
        configs: &'a [DescribeConfigsResultConfig],
        key: &str,
    ) -> &'a DescribeConfigsResultConfig {
        configs
            .iter()
            .find(|c| c.name == key)
            .expect("key missing from response")
    }

    #[tokio::test]
    async fn a_topic_without_overrides_reports_every_key_as_default() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let configs = describe(broker_with(engine, "t"), "t").await;
        assert_eq!(configs.len(), topic_config_defaults::ALL_KEYS.len());
        // `is_default` is a v0-only wire field; v1+ clients read
        // `config_source`, so that is what these tests pin.
        for c in &configs {
            assert_eq!(c.config_source, source::DEFAULT_CONFIG, "{}", c.name);
        }
    }

    #[tokio::test]
    async fn a_stored_override_reports_dynamic_topic_config() {
        // gh #236: the operator-materialised `.config.json` must come
        // back as DYNAMIC_TOPIC_CONFIG, or every UI and CLI on earth
        // reports "someone set this" as "this is the default" — and
        // `kafka-configs.sh --describe` (non---all) shows nothing.
        let tmp = tempfile::tempdir().unwrap();
        let engine = Arc::new(DiskStorageEngine::new(
            Arc::new(RealFs),
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
        let dir = engine.topic_dir("orders", 0);
        std::fs::create_dir_all(&dir).unwrap();
        kaas_storage::write_topic_config(
            engine.fs(),
            &dir,
            &TopicConfigFile {
                retention_ms: Some(600_000),
                cleanup_policy: "compact".into(),
                ..Default::default()
            },
        )
        .unwrap();

        let broker = broker_with(engine.clone(), "orders");
        let configs = describe(broker.clone(), "orders").await;

        let retention = entry(&configs, "retention.ms");
        assert_eq!(retention.value.as_deref(), Some("600000"));
        assert_eq!(retention.config_source, source::DYNAMIC_TOPIC_CONFIG);
        // Synonym chain: the override first, the shadowed default after.
        assert_eq!(retention.synonyms[0].source, source::DYNAMIC_TOPIC_CONFIG);
        assert_eq!(retention.synonyms[0].value.as_deref(), Some("600000"));
        assert_eq!(
            retention.synonyms.last().unwrap().source,
            source::DEFAULT_CONFIG
        );
        assert_eq!(
            retention.synonyms.last().unwrap().value.as_deref(),
            Some("604800000")
        );

        let policy = entry(&configs, "cleanup.policy");
        assert_eq!(policy.value.as_deref(), Some("compact"));
        assert_eq!(policy.config_source, source::DYNAMIC_TOPIC_CONFIG);

        // Keys the file doesn't carry stay default.
        let seg = entry(&configs, "segment.bytes");
        assert_eq!(seg.config_source, source::DEFAULT_CONFIG);

        // Hot reload: a config edit (operator rewrite of the file) is
        // visible on the next request, no restart, no cache.
        kaas_storage::write_topic_config(
            engine.fs(),
            &dir,
            &TopicConfigFile {
                retention_ms: Some(1_200_000),
                ..Default::default()
            },
        )
        .unwrap();
        let configs = describe(broker, "orders").await;
        assert_eq!(
            entry(&configs, "retention.ms").value.as_deref(),
            Some("1200000")
        );
        assert_eq!(
            entry(&configs, "cleanup.policy").config_source,
            source::DEFAULT_CONFIG
        );
    }

    #[tokio::test]
    async fn every_persisted_override_reports_dynamic_topic_config() {
        // gh #236, per key: `override_value` maps `.config.json` fields
        // to dotted names one by one, so a field missing there reports
        // DEFAULT_CONFIG while the broker enforces the override. Every
        // field is spelled out (no `..Default`) so a new one fails to
        // compile here until it is covered.
        let tmp = tempfile::tempdir().unwrap();
        let engine = Arc::new(DiskStorageEngine::new(
            Arc::new(RealFs),
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
        let dir = engine.topic_dir("orders", 0);
        std::fs::create_dir_all(&dir).unwrap();
        kaas_storage::write_topic_config(
            engine.fs(),
            &dir,
            &TopicConfigFile {
                retention_ms: Some(600_000),
                retention_bytes: Some(1_048_576),
                segment_bytes: Some(16_777_216),
                segment_ms: Some(3_600_000),
                cleanup_policy: "compact".into(),
                min_compaction_lag_ms: Some(5_000),
                delete_retention_ms: Some(7_000),
                flush_messages: Some(1),
            },
        )
        .unwrap();

        let configs = describe(broker_with(engine, "orders"), "orders").await;
        for (key, want) in [
            ("retention.ms", "600000"),
            ("retention.bytes", "1048576"),
            ("segment.bytes", "16777216"),
            ("segment.ms", "3600000"),
            ("cleanup.policy", "compact"),
            ("min.compaction.lag.ms", "5000"),
            ("delete.retention.ms", "7000"),
            ("flush.messages", "1"),
        ] {
            let c = entry(&configs, key);
            assert_eq!(c.value.as_deref(), Some(want), "{key}");
            assert_eq!(c.config_source, source::DYNAMIC_TOPIC_CONFIG, "{key}");
        }
    }
}
