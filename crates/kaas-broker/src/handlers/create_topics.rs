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
//! - name twice in request → `INVALID_REQUEST` (42), every entry,
//!   before authorization (gh #292)
//! - authorization denied, or `__cluster_metadata` → `TOPIC_AUTHORIZATION_FAILED` (29)
//! - invalid topic name   → `INVALID_TOPIC_EXCEPTION` (17)
//! - topic already known  → `TOPIC_ALREADY_EXISTS` (36), `validate_only` too
//! - null config value    → `INVALID_CONFIG` (40)
//! - assignments + counts → `INVALID_REQUEST` (42)
//! - bad assignments      → `INVALID_REPLICA_ASSIGNMENT` (39)
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
//! A manual replica assignment (`--replica-assignment`, `new NewTopic(name,
//! replicasAssignments)`) sends `-1` for both counts plus one entry per
//! partition. kaas has no replicas, so the entries' only effect is the
//! partition count: one partition per entry, validated the way Apache's
//! `ReplicationControlManager` does it (gh #292). Broker ids aren't
//! checked against the registered brokers; kaas places partitions
//! itself.
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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::create_topics;
use kaas_codec::api::describe_configs::source;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_config_defaults;
use crate::topic_cr_writer::{config_key_to_json_field, create_configs_to_spec, TopicWriteError};

const ERR_NONE: i16 = 0;
const ERR_INVALID_TOPIC_EXCEPTION: i16 = 17;
const ERR_TOPIC_ALREADY_EXISTS: i16 = 36;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_INVALID_PARTITIONS: i16 = 37;
const ERR_INVALID_REPLICATION_FACTOR: i16 = 38;
const ERR_INVALID_REPLICA_ASSIGNMENT: i16 = 39;
const ERR_INVALID_CONFIG: i16 = 40;
const ERR_INVALID_REQUEST: i16 = 42;
const ERR_UNKNOWN_SERVER: i16 = -1;

/// `Topic.CLUSTER_METADATA_TOPIC_NAME`.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

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

        // Apache's KafkaApis checks this before authorization: every
        // entry of a name that appears more than once is refused.
        let mut occurrences: HashMap<String, usize> = HashMap::new();
        for t in &req.topics {
            *occurrences.entry(t.name.clone()).or_default() += 1;
        }

        for topic in req.topics {
            if occurrences.get(&topic.name).copied().unwrap_or(0) > 1 {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_INVALID_REQUEST)
                        .with_error_message("Found multiple entries for this topic."),
                );
                continue;
            }
            // `__cluster_metadata` is KRaft's own log: Apache leaves it out of
            // the allowed names, so it reads as unauthorized (gh #314).
            if topic.name == CLUSTER_METADATA_TOPIC
                || !super::may_create_topic(&self.broker, &principal, &topic.name)
            {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_TOPIC_AUTHZ_FAILED)
                        .with_error_message("Authorization failed."),
                );
                continue;
            }
            // gh #314: the name becomes a directory. After the ACL, so a
            // principal without rights learns nothing about it.
            if !kaas_storage::is_valid_topic_name(&topic.name) {
                topics.push(
                    create_topics::CreatableTopicResult::new(
                        &topic.name,
                        ERR_INVALID_TOPIC_EXCEPTION,
                    )
                    .with_error_message(format!(
                        "Topic name \"{}\" is invalid: it must be 1 to 249 characters of \
                         [a-zA-Z0-9._-], and not \".\" or \"..\"",
                        topic.name
                    )),
                );
                continue;
            }
            // Apache's order from here: exists, null configs, then the counts
            // (gh #314). `validate_only` must report an existing topic too; a
            // topic the watch hasn't delivered yet is caught by the CR create.
            if self.broker.topics.get(&topic.name).is_some() {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_TOPIC_ALREADY_EXISTS)
                        .with_error_message(format!("Topic '{}' already exists.", topic.name)),
                );
                continue;
            }
            // gh #292: Apache refuses a null config value rather than
            // reading it as "use the default".
            let null_configs: Vec<&str> = topic
                .configs
                .iter()
                .filter(|c| c.value.is_none())
                .map(|c| c.name.as_str())
                .collect();
            if !null_configs.is_empty() {
                topics.push(
                    create_topics::CreatableTopicResult::new(&topic.name, ERR_INVALID_CONFIG)
                        .with_error_message(format!(
                            "Null value not supported for topic configs: {}",
                            null_configs.join(",")
                        )),
                );
                continue;
            }

            let counts = if topic.assignments.is_empty() {
                self.resolve_counts(&topic)
            } else {
                manual_assignment_counts(&topic)
            };
            let (num_partitions, replication_factor) = match counts {
                Ok(c) => c,
                Err((code, msg)) => {
                    topics.push(
                        create_topics::CreatableTopicResult::new(&topic.name, code)
                            .with_error_message(msg),
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
            // anything (and on the validate_only path too).
            let configs: Vec<(String, String)> = topic
                .configs
                .iter()
                .filter_map(|c| c.value.as_ref().map(|v| (c.name.clone(), v.clone())))
                .collect();
            let spec = match create_configs_to_spec(&configs) {
                Ok(spec) => spec,
                Err(e) => {
                    topics.push(
                        create_topics::CreatableTopicResult::new(&topic.name, ERR_INVALID_CONFIG)
                            .with_error_message(e.to_string()),
                    );
                    continue;
                }
            };
            let created = |principal: &Principal| {
                self.created_row(
                    principal,
                    &topic.name,
                    num_partitions,
                    replication_factor,
                    &spec,
                )
            };

            if req.validate_only {
                topics.push(created(&principal));
                continue;
            }

            match w.create_topic(&topic.name, num_partitions, &configs).await {
                Ok(()) => topics.push(created(&principal)),
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

/// What DescribeConfigs will report for the new topic (gh #315), from the same
/// table. The operator hasn't materialised `.config.json` yet, so overrides
/// come from the request's validated `spec`. An accept-only key isn't stored,
/// so it reads as the default here, as it will there.
fn created_topic_configs(
    spec: &serde_json::Map<String, serde_json::Value>,
) -> Vec<create_topics::CreatableTopicConfigResult> {
    topic_config_defaults::ALL_KEYS
        .iter()
        .map(|entry| {
            let stored = config_key_to_json_field(entry.dotted_name)
                .and_then(|field| spec.get(field))
                .map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_owned));
            let config_source = if stored.is_some() {
                source::DYNAMIC_TOPIC_CONFIG
            } else {
                source::DEFAULT_CONFIG
            };
            create_topics::CreatableTopicConfigResult {
                name: entry.dotted_name.to_owned(),
                value: stored.or_else(|| entry.default_value.map(str::to_owned)),
                read_only: false,
                config_source,
                is_sensitive: false,
            }
        })
        .collect()
}

/// A failed check: the wire code and Apache's message.
type Refusal = (i16, &'static str);

impl CreateTopicsHandler {
    /// A successful create's row (KIP-525). The counts and configs need
    /// DescribeConfigs on the topic, as in Apache: without it they stay at
    /// their defaults and `TopicConfigErrorCode` says why, while the create
    /// itself still succeeded. It's a filter, so a denial isn't audited
    /// (`logIfDenied = false`).
    fn created_row(
        &self,
        principal: &Principal,
        name: &str,
        num_partitions: i32,
        replication_factor: i16,
        spec: &serde_json::Map<String, serde_json::Value>,
    ) -> create_topics::CreatableTopicResult {
        let row = create_topics::CreatableTopicResult::new(name, ERR_NONE);
        if self.broker.authorizer.authorize_quietly(
            principal,
            &Resource::topic(name),
            Operation::DescribeConfigs,
        ) {
            row.with_created(num_partitions, replication_factor)
                .with_configs(created_topic_configs(spec))
        } else {
            row.with_config_error(ERR_TOPIC_AUTHZ_FAILED)
        }
    }

    /// gh #280, Apache's KIP-464 rules for a request without manual
    /// assignments: -1 asks for the broker default, anything else must
    /// be positive.
    fn resolve_counts(
        &self,
        topic: &create_topics::CreatableTopic,
    ) -> Result<(i32, i16), (i16, String)> {
        let refuse = |(code, msg): Refusal| (code, msg.to_owned());
        let num_partitions = match topic.num_partitions {
            -1 => self.default_partitions,
            n if n > 0 => n,
            _ => {
                return Err(refuse((
                    ERR_INVALID_PARTITIONS,
                    "Number of partitions was set to an invalid non-positive value.",
                )))
            }
        };
        let replication_factor = match topic.replication_factor {
            -1 => 1,
            n if n > 0 => n,
            _ => {
                return Err(refuse((
                    ERR_INVALID_REPLICATION_FACTOR,
                    "Replication factor must be larger than 0, or -1 to use the default value.",
                )))
            }
        };
        Ok((num_partitions, replication_factor))
    }
}

/// The partition count a manual replica assignment implies, checked as
/// Apache's `ReplicationControlManager.createTopic` and
/// `validateManualPartitionAssignment` check it (gh #292):
/// - both counts must be -1;
/// - partition indices must be unique and form 0..n;
/// - every replica list must be non-empty, name no broker twice, and
///   have the same length.
///
/// The replication factor reported back is that length. Broker ids
/// aren't checked against the registered brokers: kaas places
/// partitions itself and has no replicas.
fn manual_assignment_counts(
    topic: &create_topics::CreatableTopic,
) -> Result<(i32, i16), (i16, String)> {
    if topic.replication_factor != -1 {
        return Err((
            ERR_INVALID_REQUEST,
            "A manual partition assignment was specified, but replication factor was not set to -1."
                .to_owned(),
        ));
    }
    if topic.num_partitions != -1 {
        return Err((
            ERR_INVALID_REQUEST,
            "A manual partition assignment was specified, but numPartitions was not set to -1."
                .to_owned(),
        ));
    }
    let bad = |msg: String| (ERR_INVALID_REPLICA_ASSIGNMENT, msg);
    let mut indices = BTreeSet::new();
    let mut replicas: Option<usize> = None;
    for a in &topic.assignments {
        if !indices.insert(a.partition_index) {
            return Err(bad(format!(
                "Found multiple manual partition assignments for partition {}",
                a.partition_index
            )));
        }
        if a.broker_ids.is_empty() {
            return Err(bad(
                "The manual partition assignment includes an empty replica list.".to_owned(),
            ));
        }
        let mut ids = a.broker_ids.clone();
        ids.sort_unstable();
        if let Some(w) = ids.windows(2).find(|w| w[0] == w[1]) {
            return Err(bad(format!(
                "The manual partition assignment includes the broker {} more than once.",
                w[0]
            )));
        }
        match replicas {
            Some(r) if r != ids.len() => {
                return Err(bad(format!(
                    "The manual partition assignment includes a partition with {} replica(s), \
                     but this is not consistent with previous partitions, which have {r} replica(s).",
                    ids.len()
                )))
            }
            _ => replicas = Some(ids.len()),
        }
    }
    // Unique indices forming 0..n: the largest must be n - 1, and none
    // may be negative.
    let n = indices.len();
    let consecutive = indices.first() == Some(&0)
        && indices
            .last()
            .and_then(|&last| usize::try_from(last).ok())
            .is_some_and(|last| last + 1 == n);
    if !consecutive {
        return Err(bad(
            "partitions should be a consecutive 0-based integer sequence".to_owned(),
        ));
    }
    let num_partitions = i32::try_from(n).unwrap_or(i32::MAX);
    let replication_factor = replicas
        .and_then(|r| i16::try_from(r).ok())
        .unwrap_or(i16::MAX);
    Ok((num_partitions, replication_factor))
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

    /// gh #315 (KIP-525): a successful create returns the configs
    /// DescribeConfigs will report: an override as DYNAMIC_TOPIC_CONFIG (1),
    /// the rest as DEFAULT_CONFIG (5), both on a real create and on
    /// validate-only.
    #[tokio::test]
    async fn a_created_topic_returns_what_describe_configs_will_report() {
        for validate_only in [false, true] {
            let writer = Arc::new(RecordingWriter::default());
            let h = CreateTopicsHandler::new(broker_with_writer(writer));
            let req = Request {
                topics: vec![CreatableTopic {
                    name: "t".into(),
                    num_partitions: 3,
                    replication_factor: 1,
                    assignments: vec![],
                    configs: vec![
                        CreatableTopicConfig {
                            name: "retention.ms".into(),
                            value: Some("3600000".into()),
                        },
                        CreatableTopicConfig {
                            name: "cleanup.policy".into(),
                            value: Some("compact".into()),
                        },
                    ],
                }],
                timeout_ms: 1000,
                validate_only,
            };
            let mut buf = BytesMut::new();
            create_topics::encode_request(&mut buf, &req, 5).unwrap();
            let out = h.handle(&conn(), 5, buf.freeze()).await.unwrap();
            let resp = create_topics::decode_response(&mut out.freeze(), 5).unwrap();
            let row = &resp.topics[0];
            assert_eq!(row.error_code, 0);
            assert_eq!(row.topic_config_error_code, 0);
            assert_eq!((row.num_partitions, row.replication_factor), (3, 1));

            let names: Vec<_> = row.configs.iter().map(|c| c.name.as_str()).collect();
            let want: Vec<_> = topic_config_defaults::ALL_KEYS
                .iter()
                .map(|e| e.dotted_name)
                .collect();
            assert_eq!(names, want);
            let get = |name: &str| row.configs.iter().find(|c| c.name == name).unwrap();
            assert_eq!(get("retention.ms").value.as_deref(), Some("3600000"));
            assert_eq!(get("retention.ms").config_source, 1);
            assert_eq!(get("cleanup.policy").value.as_deref(), Some("compact"));
            assert_eq!(get("cleanup.policy").config_source, 1);
            let segment = get("segment.bytes");
            assert_eq!(segment.config_source, 5);
            let default = topic_config_defaults::ALL_KEYS
                .iter()
                .find(|e| e.dotted_name == "segment.bytes")
                .unwrap()
                .default_value;
            assert_eq!(segment.value.as_deref(), default);
            assert!(row.configs.iter().all(|c| !c.read_only && !c.is_sensitive));
        }
    }

    /// gh #315: an accept-only key isn't stored, so DescribeConfigs reports it
    /// as the default, and so does the create.
    #[tokio::test]
    async fn an_accept_only_override_reads_as_the_default() {
        let writer = Arc::new(RecordingWriter::default());
        let resp = create(
            broker_with_writer(writer),
            vec![CreatableTopicConfig {
                name: "message.timestamp.type".into(),
                value: Some("CreateTime".into()),
            }],
        )
        .await;
        let row = &resp.topics[0];
        assert_eq!(row.error_code, 0);
        assert!(row.configs.iter().all(|c| c.config_source == 5), "{row:?}");
    }

    /// gh #315: Create without DescribeConfigs still creates the topic. The
    /// row says 29 in `TopicConfigErrorCode`, and the counts and configs stay
    /// at their defaults, as in Apache.
    #[tokio::test]
    async fn without_describe_configs_the_create_succeeds_and_the_row_says_29() {
        use crate::handlers::test_authz::DenyOpAuthorizer;
        let writer = Arc::new(RecordingWriter::default());
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let broker = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyOpAuthorizer(Operation::DescribeConfigs)),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        broker.install_cr_writer(writer.clone());
        let resp = create(broker, vec![]).await;
        let row = &resp.topics[0];
        assert_eq!(row.error_code, 0);
        assert_eq!(row.topic_config_error_code, 29);
        assert_eq!((row.num_partitions, row.replication_factor), (-1, -1));
        assert!(row.configs.is_empty());
        assert_eq!(writer.calls.lock().len(), 1, "the topic was still created");
    }

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

    /// Send `topics` in one request (v5, so the counts are on the wire).
    async fn create_many(
        w: &Arc<RecordingWriter>,
        topics: Vec<CreatableTopic>,
        validate_only: bool,
    ) -> create_topics::Response {
        let req = Request {
            topics,
            timeout_ms: 1000,
            validate_only,
        };
        let mut buf = BytesMut::new();
        create_topics::encode_request(&mut buf, &req, 5).unwrap();
        let h = CreateTopicsHandler::new(broker_with_writer(w.clone())).with_default_partitions(1);
        let out = h.handle(&conn(), 5, buf.freeze()).await.unwrap();
        create_topics::decode_response(&mut out.freeze(), 5).unwrap()
    }

    fn topic(
        name: &str,
        partitions: i32,
        replicas: i16,
        assignments: &[(i32, &[i32])],
    ) -> CreatableTopic {
        CreatableTopic {
            name: name.into(),
            num_partitions: partitions,
            replication_factor: replicas,
            assignments: assignments
                .iter()
                .map(
                    |(partition_index, ids)| create_topics::CreatableReplicaAssignment {
                        partition_index: *partition_index,
                        broker_ids: ids.to_vec(),
                    },
                )
                .collect(),
            configs: vec![],
        }
    }

    /// gh #292: a manual assignment (`--replica-assignment`) creates one
    /// partition per entry. It was ignored, so three assignments made
    /// `KAAS_NUM_PARTITIONS` partitions. The reported replication factor
    /// is the assignment's.
    #[tokio::test]
    async fn a_manual_assignment_creates_one_partition_per_entry() {
        let w = Arc::new(RecordingWriter::default());
        let resp = create_many(
            &w,
            vec![topic(
                "t",
                -1,
                -1,
                &[(1, &[1, 2]), (0, &[0, 1]), (2, &[2, 0])],
            )],
            false,
        )
        .await;
        let r = &resp.topics[0];
        assert_eq!(
            (r.error_code, r.num_partitions, r.replication_factor),
            (0, 3, 2)
        );
        assert_eq!(w.calls.lock()[0].1, 3);
    }

    /// gh #292: Apache's codes for a bad manual assignment, written out:
    /// explicit counts alongside one are INVALID_REQUEST (42); a broken
    /// assignment is INVALID_REPLICA_ASSIGNMENT (39). Nothing is minted.
    #[tokio::test]
    async fn bad_manual_assignments_are_refused_with_apaches_codes() {
        let cases: [(CreatableTopic, i16); 7] = [
            (topic("t", 3, -1, &[(0, &[0])]), 42),
            (topic("t", -1, 1, &[(0, &[0])]), 42),
            (topic("t", -1, -1, &[(0, &[0]), (0, &[1])]), 39),
            (topic("t", -1, -1, &[(0, &[0]), (2, &[1])]), 39),
            (topic("t", -1, -1, &[(0, &[])]), 39),
            (topic("t", -1, -1, &[(0, &[1, 1])]), 39),
            (topic("t", -1, -1, &[(0, &[0, 1]), (1, &[0])]), 39),
        ];
        for (t, code) in cases {
            let w = Arc::new(RecordingWriter::default());
            let desc = format!("{:?}", t.assignments);
            let resp = create_many(&w, vec![t], false).await;
            assert_eq!(resp.topics[0].error_code, code, "{desc}");
            assert!(w.calls.lock().is_empty(), "{desc}");
        }
    }

    /// gh #292: a null config value is Apache's INVALID_CONFIG (40), not a
    /// silently skipped entry, on the validate-only path too.
    #[tokio::test]
    async fn a_null_config_value_is_invalid_config_40() {
        for validate_only in [true, false] {
            let w = Arc::new(RecordingWriter::default());
            let mut t = topic("t", 1, 1, &[]);
            t.configs = vec![CreatableTopicConfig {
                name: "retention.ms".into(),
                value: None,
            }];
            let resp = create_many(&w, vec![t], validate_only).await;
            assert_eq!(resp.topics[0].error_code, 40);
            assert_eq!(
                resp.topics[0].error_message.as_deref(),
                Some("Null value not supported for topic configs: retention.ms")
            );
            assert!(w.calls.lock().is_empty());
        }
    }

    /// gh #292: Apache answers INVALID_REQUEST (42) for every entry of a
    /// name that appears more than once; other topics are unaffected.
    /// Error rows carry the schema default -1 for both counts.
    #[tokio::test]
    async fn a_duplicated_name_is_invalid_request_42_for_every_entry() {
        for validate_only in [true, false] {
            let w = Arc::new(RecordingWriter::default());
            let resp = create_many(
                &w,
                vec![
                    topic("a", 1, 1, &[]),
                    topic("b", 1, 1, &[]),
                    topic("a", 2, 1, &[]),
                ],
                validate_only,
            )
            .await;
            let got: Vec<(&str, i16, i32, i16)> = resp
                .topics
                .iter()
                .map(|t| {
                    (
                        t.name.as_str(),
                        t.error_code,
                        t.num_partitions,
                        t.replication_factor,
                    )
                })
                .collect();
            assert_eq!(got, [("a", 42, -1, -1), ("b", 0, 1, 1), ("a", 42, -1, -1)]);
            let minted: Vec<String> = w.calls.lock().iter().map(|c| c.0.clone()).collect();
            let want: &[&str] = if validate_only { &[] } else { &["b"] };
            assert_eq!(minted, want);
        }
    }

    /// gh #314: Apache's `Topic.validate`, as INVALID_TOPIC_EXCEPTION (17),
    /// and nothing is minted, on the validate-only path too. A name reaches
    /// the filesystem, so these used to escape the data dir.
    #[tokio::test]
    async fn an_invalid_topic_name_is_17_and_never_minted() {
        for validate_only in [true, false] {
            for name in ["a/b", "..", ".", "", "../../x", "kperf-/../victim", "a b"] {
                let w = Arc::new(RecordingWriter::default());
                let resp = create_many(&w, vec![topic(name, 1, 1, &[])], validate_only).await;
                assert_eq!(resp.topics[0].error_code, 17, "{name:?}");
                assert!(w.calls.lock().is_empty(), "{name:?} was minted");
            }
        }
    }

    /// gh #314: `__cluster_metadata` reads as unauthorized, as in Apache.
    #[tokio::test]
    async fn the_cluster_metadata_topic_is_29() {
        let w = Arc::new(RecordingWriter::default());
        let resp = create_many(&w, vec![topic("__cluster_metadata", 1, 1, &[])], true).await;
        assert_eq!(resp.topics[0].error_code, 29);
    }

    /// gh #314: Apache's order. A denied principal learns nothing about the
    /// name, and a null config wins over a bad count.
    #[tokio::test]
    async fn checks_run_in_apaches_order() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let denied = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        let req = Request {
            topics: vec![topic("../x", 1, 1, &[])],
            timeout_ms: 1000,
            validate_only: true,
        };
        let mut buf = BytesMut::new();
        create_topics::encode_request(&mut buf, &req, 5).unwrap();
        let out = CreateTopicsHandler::new(denied)
            .handle(&conn(), 5, buf.freeze())
            .await
            .unwrap();
        let resp = create_topics::decode_response(&mut out.freeze(), 5).unwrap();
        assert_eq!(resp.topics[0].error_code, 29);

        let w = Arc::new(RecordingWriter::default());
        let mut t = topic("t", 0, 1, &[]);
        t.configs = vec![CreatableTopicConfig {
            name: "retention.ms".into(),
            value: None,
        }];
        let resp = create_many(&w, vec![t], true).await;
        assert_eq!(
            resp.topics[0].error_code, 40,
            "null config before the count"
        );
    }

    /// gh #314: validate-only reports a topic that exists, as a real create
    /// would.
    #[tokio::test]
    async fn validate_only_reports_an_existing_topic_36() {
        use crate::topic_registry::TopicMeta;
        let w = Arc::new(RecordingWriter::default());
        let b = broker_with_writer(w.clone());
        b.topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let req = Request {
            topics: vec![topic("t", 1, 1, &[])],
            timeout_ms: 1000,
            validate_only: true,
        };
        let mut buf = BytesMut::new();
        create_topics::encode_request(&mut buf, &req, 5).unwrap();
        let out = CreateTopicsHandler::new(b)
            .handle(&conn(), 5, buf.freeze())
            .await
            .unwrap();
        let resp = create_topics::decode_response(&mut out.freeze(), 5).unwrap();
        assert_eq!(resp.topics[0].error_code, 36);
    }
}
