//! CreateTopics handler — API key 19 (gh #51).
//!
//! Translates `AdminClient.createTopics()` / `kafka-topics.sh
//! --create` into a POST that mints a fresh `KafkaTopic` CR via the
//! installed [`TopicCRWriter`]. The operator then reconciles the
//! CR, creating the topic's on-disk partition directories on the
//! shared PVC; the broker observes the new KafkaTopic via its
//! existing topic watch (`run_topic_watch`) and serves Produce/Fetch
//! against it on subsequent requests.
//!
//! Authorization: `Operation::Create` on the cluster, else on the topic
//! resource — Apache's rule (gh #280), shared with Metadata auto-create.
//!
//! Wire error mapping (see also [`TopicWriteError`]):
//!
//! - authorization denied → `TOPIC_AUTHORIZATION_FAILED` (29)
//! - partitions 0 / < -1  → `INVALID_PARTITIONS` (37)
//! - replication 0 / < -1 → `INVALID_REPLICATION_FACTOR` (38)
//! - missing CR writer    → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - bad config key/value → `INVALID_CONFIG` (40)
//! - CR already exists    → `TOPIC_ALREADY_EXISTS` (36)
//! - RBAC denial          → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - other kube error     → `UNKNOWN_SERVER_ERROR` (-1)
//!
//! `-1` partitions means the broker default, `KAAS_NUM_PARTITIONS`
//! (Apache's `num.partitions`, KIP-464 — what `kafka-topics.sh --create`
//! sends without `--partitions`); `-1` replicas means 1, the only replica
//! count kaas has (gh #280).
//!
//! `validate_only: true` (v1+) short-circuits BEFORE minting the CR —
//! the authorization + writer + config-validation checks still run
//! and the would-be response is returned without mutating state.
//!
//! Config overrides on the wire request (retention.ms etc.) land in
//! the minted CR's `spec.config` (gh #236) — the operator
//! materialises them to `.config.json` on first reconcile, exactly
//! as if they had been authored on the CR. Unknown keys or
//! unparseable values fail the whole creation with
//! `INVALID_CONFIG` (40); the pre-gh #236 behaviour was to parse
//! and silently drop them.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::create_topics;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::{create_configs_to_spec, TopicWriteError};

const ERR_NONE: i16 = 0;
const ERR_TOPIC_ALREADY_EXISTS: i16 = 36;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_INVALID_PARTITIONS: i16 = 37;
const ERR_INVALID_REPLICATION_FACTOR: i16 = 38;
const ERR_INVALID_CONFIG: i16 = 40;
const ERR_UNKNOWN_SERVER: i16 = -1;

#[derive(Debug)]
pub struct CreateTopicsHandler {
    broker: Arc<Broker>,
    /// Partition count for a request that sends `-1` (gh #280).
    default_partitions: i32,
}

impl CreateTopicsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self {
            broker,
            default_partitions: 1,
        }
    }

    /// The broker's `num.partitions` (`KAAS_NUM_PARTITIONS`), used when a
    /// request asks for the default with `-1`.
    #[must_use]
    pub fn with_default_partitions(mut self, n: i32) -> Self {
        self.default_partitions = n;
        self
    }
}

#[async_trait]
impl Handler for CreateTopicsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = create_topics::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let writer = self.broker.cr_writer();
        let mut topics = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            if !super::may_create_topic(&self.broker, &principal, &topic.name) {
                topics.push(create_topics::CreatableTopicResult::new(
                    &topic.name,
                    ERR_TOPIC_AUTHZ_FAILED,
                ));
                continue;
            }

            // gh #280, Apache's KIP-464 rules: -1 asks for the broker
            // default, anything else must be positive.
            let num_partitions = match topic.num_partitions {
                -1 => self.default_partitions,
                n if n > 0 => n,
                _ => {
                    topics.push(
                        create_topics::CreatableTopicResult::new(
                            &topic.name,
                            ERR_INVALID_PARTITIONS,
                        )
                        .with_error_message(
                            "Number of partitions must be larger than 0, or -1 to use the default value.",
                        ),
                    );
                    continue;
                }
            };
            let replication_factor = match topic.replication_factor {
                -1 => 1,
                n if n > 0 => n,
                _ => {
                    topics.push(
                        create_topics::CreatableTopicResult::new(
                            &topic.name,
                            ERR_INVALID_REPLICATION_FACTOR,
                        )
                        .with_error_message(
                            "Replication factor must be larger than 0, or -1 to use the default value.",
                        ),
                    );
                    continue;
                }
            };

            let Some(w) = writer.as_ref() else {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_CLUSTER_AUTHZ_FAILED)
                        .with_error_message("broker is not running in cluster mode"),
                );
                continue;
            };

            // gh #236: validate the config overrides before minting
            // anything (and on the validate_only path too). A null
            // value means "use the default" — skip it rather than
            // materialise an override.
            let configs: Vec<(String, String)> = topic
                .configs
                .iter()
                .filter_map(|c| c.value.as_ref().map(|v| (c.name.clone(), v.clone())))
                .collect();
            if let Err(e) = create_configs_to_spec(&configs) {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_INVALID_CONFIG)
                        .with_error_message(e.to_string()),
                );
                continue;
            }

            if req.validate_only {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_NONE)
                        .with_created(num_partitions, replication_factor),
                );
                continue;
            }

            match w.create_topic(&topic.name, num_partitions, &configs).await {
                Ok(()) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_NONE)
                        .with_created(num_partitions, replication_factor),
                ),
                Err(TopicWriteError::AlreadyExists(_)) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_TOPIC_ALREADY_EXISTS)
                        .with_error_message("topic already exists"),
                ),
                Err(TopicWriteError::Forbidden(msg)) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_CLUSTER_AUTHZ_FAILED)
                        .with_error_message(msg),
                ),
                Err(TopicWriteError::InvalidConfig(msg)) => topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_INVALID_CONFIG)
                        .with_error_message(msg),
                ),
                Err(other) => {
                    tracing::warn!(error = %other, topic = %topic.name, "CreateTopics failed");
                    topics.push(create_topics::CreatableTopicResult::new(
                        &topic.name,
                        ERR_UNKNOWN_SERVER,
                    ));
                }
            }
        }

        let resp = create_topics::Response {
            throttle_time_ms: 0,
            topics,
        };
        let mut out = BytesMut::new();
        create_topics::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_cr_writer::{ConfigOpWithValue, TopicCRWriter};
    use crate::topic_registry::TopicRegistry;
    use kaas_codec::api::create_topics::{CreatableTopic, CreatableTopicConfig, Request};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    /// One recorded `create_topic` call.
    type CreateCall = (String, i32, Vec<(String, String)>);

    /// Records `create_topic` calls, configs included, so the tests
    /// can assert the gh #236 threading actually happens.
    #[derive(Debug, Default)]
    struct RecordingWriter {
        calls: Mutex<Vec<CreateCall>>,
    }

    #[async_trait]
    impl TopicCRWriter for RecordingWriter {
        async fn create_topic(
            &self,
            name: &str,
            n: i32,
            configs: &[(String, String)],
        ) -> Result<(), TopicWriteError> {
            self.calls
                .lock()
                .push((name.to_owned(), n, configs.to_vec()));
            Ok(())
        }
        async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
            unreachable!()
        }
        async fn update_topic_config(
            &self,
            _: &str,
            _: &[ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            unreachable!()
        }
        async fn delete_topic(&self, _: &str) -> Result<(), TopicWriteError> {
            unreachable!()
        }
        async fn set_partition_log_dir(
            &self,
            _: &str,
            _: i32,
            _: &str,
        ) -> Result<(), TopicWriteError> {
            unreachable!()
        }
    }

    fn broker_with_writer(writer: Arc<RecordingWriter>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        b.install_cr_writer(writer);
        b
    }

    async fn create(
        broker: Arc<Broker>,
        configs: Vec<CreatableTopicConfig>,
    ) -> create_topics::Response {
        create_with(CreateTopicsHandler::new(broker), 3, 1, configs).await
    }

    async fn create_with(
        h: CreateTopicsHandler,
        num_partitions: i32,
        replication_factor: i16,
        configs: Vec<CreatableTopicConfig>,
    ) -> create_topics::Response {
        let req = Request {
            topics: vec![CreatableTopic {
                name: "t".into(),
                num_partitions,
                replication_factor,
                assignments: vec![],
                configs,
            }],
            timeout_ms: 1000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        create_topics::encode_request(&mut buf, &req, 5).unwrap();
        let out = h.handle(&conn(), 5, buf.freeze()).await.unwrap();
        let mut r = out.freeze();
        create_topics::decode_response(&mut r, 5).unwrap()
    }

    #[tokio::test]
    async fn config_overrides_reach_the_writer() {
        let w = Arc::new(RecordingWriter::default());
        let resp = create(
            broker_with_writer(w.clone()),
            vec![CreatableTopicConfig {
                name: "retention.ms".into(),
                value: Some("600000".into()),
            }],
        )
        .await;
        assert_eq!(resp.topics[0].error_code, ERR_NONE);
        let calls = w.calls.lock().clone();
        assert_eq!(
            calls,
            vec![(
                "t".into(),
                3,
                vec![("retention.ms".into(), "600000".into())]
            )],
            "the wire config override must be threaded into the CR mint"
        );
    }

    #[tokio::test]
    async fn an_unknown_config_key_fails_the_creation_with_invalid_config() {
        // The gh #236 contract: reject, never silently drop. Apache
        // answers INVALID_CONFIG (40) here.
        let w = Arc::new(RecordingWriter::default());
        let resp = create(
            broker_with_writer(w.clone()),
            vec![CreatableTopicConfig {
                name: "max.message.bytes".into(),
                value: Some("1000".into()),
            }],
        )
        .await;
        assert_eq!(resp.topics[0].error_code, ERR_INVALID_CONFIG);
        assert!(
            w.calls.lock().is_empty(),
            "a rejected creation must not mint a CR"
        );
    }

    #[tokio::test]
    async fn an_unparseable_value_fails_the_creation_with_invalid_config() {
        let w = Arc::new(RecordingWriter::default());
        let resp = create(
            broker_with_writer(w.clone()),
            vec![CreatableTopicConfig {
                name: "retention.ms".into(),
                value: Some("ten minutes".into()),
            }],
        )
        .await;
        assert_eq!(resp.topics[0].error_code, ERR_INVALID_CONFIG);
        assert!(w.calls.lock().is_empty());
    }

    /// gh #280: -1 asks for the broker default (Apache's
    /// `num.partitions`), which is what `kafka-topics.sh --create` sends
    /// without `--partitions`.
    #[tokio::test]
    async fn minus_one_partitions_uses_the_broker_default() {
        let w = Arc::new(RecordingWriter::default());
        let h = CreateTopicsHandler::new(broker_with_writer(w.clone())).with_default_partitions(6);
        let resp = create_with(h, -1, -1, vec![]).await;
        assert_eq!(resp.topics[0].error_code, ERR_NONE);
        assert_eq!(resp.topics[0].num_partitions, 6);
        assert_eq!(resp.topics[0].replication_factor, 1);
        assert_eq!(w.calls.lock()[0].1, 6);
    }

    /// gh #280: Apache's codes, written out — 0 and anything below -1 are
    /// invalid, and nothing is minted.
    #[tokio::test]
    async fn zero_or_below_minus_one_is_rejected() {
        for (partitions, replicas, code) in [(0, 1, 37), (-2, 1, 37), (3, 0, 38), (3, -2, 38)] {
            let w = Arc::new(RecordingWriter::default());
            let h = CreateTopicsHandler::new(broker_with_writer(w.clone()));
            let resp = create_with(h, partitions, replicas, vec![]).await;
            assert_eq!(
                resp.topics[0].error_code, code,
                "partitions={partitions} replicas={replicas}"
            );
            assert!(w.calls.lock().is_empty());
        }
    }

    /// gh #280: Apache accepts `Create` on the cluster in place of
    /// `Create` on each topic; without either the topic is refused.
    #[tokio::test]
    async fn cluster_create_suffices_and_neither_grant_is_refused() {
        use crate::handlers::test_authz::{ClusterCreateOnlyAuthorizer, DenyOpAuthorizer};
        use kaas_auth::Operation;
        let with_auth = |auth: Arc<dyn kaas_auth::Authorizer>, w: Arc<RecordingWriter>| {
            let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
            let b = Arc::new(Broker::with_auth(
                engine,
                Arc::new(TopicRegistry::new()),
                "test",
                0,
                auth,
                Arc::new(kaas_auth::NoQuotaChecker),
            ));
            b.install_cr_writer(w);
            b
        };
        let w = Arc::new(RecordingWriter::default());
        let resp = create(
            with_auth(Arc::new(ClusterCreateOnlyAuthorizer), w.clone()),
            vec![],
        )
        .await;
        assert_eq!(resp.topics[0].error_code, ERR_NONE);
        assert_eq!(w.calls.lock().len(), 1);

        let w = Arc::new(RecordingWriter::default());
        let resp = create(
            with_auth(Arc::new(DenyOpAuthorizer(Operation::Create)), w.clone()),
            vec![],
        )
        .await;
        assert_eq!(resp.topics[0].error_code, ERR_TOPIC_AUTHZ_FAILED);
        assert!(w.calls.lock().is_empty());
    }
}
