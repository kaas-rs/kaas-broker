//! Metadata handler (key 3).
//!
//! Single-broker shape: every partition leads on `broker_id = self`,
//! every topic carries the all-zero `topic_id` sentinel until Phase 7
//! mints real UUIDs.
//!
//! Per-listener port advertisement (gh #125): the handler picks the
//! port matching `ConnState::listener_name` from the listener table
//! it was constructed with, so a client that bootstrapped on
//! the authed listener gets back the authed port, not the anonymous
//! one. Phase 3 single-listener clusters resolve to the single entry.
//!
//! ## Topic auto-creation (gh #109, gh #242)
//!
//! Apache `auto.create.topics.enable`. An unknown topic named by a
//! request that set `allowAutoTopicCreation` mints a `KafkaTopic` CR
//! through the installed [`TopicCRWriter`] and answers
//! `LEADER_NOT_AVAILABLE` (5) — a retriable code — because the CR
//! exists but the operator hasn't materialised the partition dirs and
//! the controller hasn't assigned a leader, so there is no truthful
//! leader to advertise yet. The client re-requests metadata and gets
//! the real thing.
//!
//! This matters beyond convenience: Kafka Streams' DSL `.to(sink)`
//! never calls `AdminClient.createTopics` for its sink topic (only for
//! changelog/repartition topics), so without this path a Streams app
//! whose output topic doesn't already exist can never make progress.
//!
//! Four guards, each with a test: the broker-side switch must be on,
//! the client must not have opted out (the flag exists in v4+; v0-v3
//! carry no field and decode to Apache's `true` default, so the broker
//! switch is the only gate for those), the `__` prefix is reserved, and
//! the principal needs `Create` on the topic — otherwise Metadata would
//! be a way around the authorizer that `CreateTopics` enforces.
//!
//! An all-topics request (a null list) can't trip any of this: every
//! name comes from the registry, so the unknown-topic branch is
//! unreachable. Apache carves the same case out explicitly via
//! `!metadataRequest.isAllTopics`. An empty list asks for no topics
//! (gh #274), so it can't trip it either.
//!
//! ## Authorization (gh #268)
//!
//! `Describe` on each topic, checked before anything else about it —
//! Apache's `handleTopicMetadataRequest` shape. An all-topics request
//! silently drops the topics the principal can't describe; a named one
//! answers `TOPIC_AUTHORIZATION_FAILED` (29) whether or not the topic
//! exists, so the error code doesn't reveal existence. The auto-create
//! `Create` check above only runs for a topic that passed `Describe`.
//!
//! [`TopicCRWriter`]: crate::topic_cr_writer::TopicCRWriter

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::acl_types::operation as acl_op;
use kaas_codec::api::describe_cluster::NOT_REQUESTED;
use kaas_codec::api::metadata;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;
use crate::cli::ListenerEntry;
use crate::handlers::describe_cluster::cluster_authorized_operations;
use crate::listener_advert::{
    advertised_brokers, controller_id, trailing_ordinal, ListenerAdverts,
};
use crate::topic_cr_writer::TopicWriteError;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_INVALID_REQUEST: i16 = 42;

/// Apache's supported operations for the TOPIC resource
/// (`AclEntry.supportedOperations`), with the `AclOperation` bit each sets
/// in `topic_authorized_operations` (gh #278).
const TOPIC_OPS: &[(Operation, i8)] = &[
    (Operation::Read, acl_op::READ),
    (Operation::Write, acl_op::WRITE),
    (Operation::Create, acl_op::CREATE),
    (Operation::Delete, acl_op::DELETE),
    (Operation::Alter, acl_op::ALTER),
    (Operation::Describe, acl_op::DESCRIBE),
    (Operation::DescribeConfigs, acl_op::DESCRIBE_CONFIGS),
    (Operation::AlterConfigs, acl_op::ALTER_CONFIGS),
];

/// Apache reserves the `__` prefix for internal topics
/// (`__consumer_offsets`, `__transaction_state`). kaas has no such
/// topics on disk — the equivalent state lives under `__cluster/`
/// (see the non-goals in CLAUDE.md) — but a client asking for one
/// must never mint a CR for it.
fn is_reserved_internal(name: &str) -> bool {
    name.starts_with("__")
}

#[derive(Debug)]
pub struct MetadataHandler {
    broker: Arc<Broker>,
    listeners: ListenerAdverts,
    /// Apache `auto.create.topics.enable` (gh #242). Off unless
    /// [`with_auto_create`](Self::with_auto_create) says otherwise, so
    /// tests and dev-mode wiring keep the plain
    /// `UNKNOWN_TOPIC_OR_PARTITION` behaviour; `bins/kaas` turns it on
    /// from `Cli`.
    auto_create: bool,
    /// Apache `num.partitions` — partition count for an auto-created
    /// topic. Only read when `auto_create` is set.
    num_partitions: i32,
}

impl MetadataHandler {
    pub fn new(broker: Arc<Broker>, listeners: &[ListenerEntry]) -> Self {
        Self {
            broker,
            listeners: ListenerAdverts::new(listeners),
            auto_create: false,
            num_partitions: 1,
        }
    }

    /// Enable broker-side topic auto-creation. `num_partitions` is
    /// clamped to at least 1 — a zero-partition topic is unservable
    /// and the CR would be rejected downstream anyway.
    #[must_use]
    pub fn with_auto_create(mut self, enabled: bool, num_partitions: i32) -> Self {
        self.auto_create = enabled;
        self.num_partitions = num_partitions.max(1);
        self
    }

    /// Mint a `KafkaTopic` CR for an unknown topic named by a Metadata
    /// request that opted into auto-creation, and return the error code
    /// to report for it.
    ///
    /// `LEADER_NOT_AVAILABLE` is the success answer, not an error: the
    /// CR exists but the operator hasn't created the partition dirs and
    /// the controller hasn't assigned a leader yet, so there is nothing
    /// truthful to advertise. Java clients treat code 5 as retriable and
    /// re-request metadata, which is exactly the handshake Apache uses.
    /// `AlreadyExists` gets the same answer — a concurrent request (or
    /// an operator-authored CR the watch hasn't delivered yet) won the
    /// race, which is success for this caller.
    async fn auto_create_topic(&self, principal: &Principal, name: &str) -> i16 {
        if is_reserved_internal(name) {
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        }
        // Auto-creation is a create, so it takes the same ACL as
        // CreateTopics — otherwise Metadata would be a way around the
        // authorizer.
        if !super::may_create_topic(&self.broker, principal, name) {
            return ERR_TOPIC_AUTHZ_FAILED;
        }
        let Some(writer) = self.broker.cr_writer() else {
            // Dev mode / no kube client: nothing can create the topic,
            // so the honest answer is that it doesn't exist.
            return ERR_UNKNOWN_TOPIC_OR_PARTITION;
        };
        // Auto-create carries no config overrides — broker defaults,
        // same as Apache's `auto.create.topics.enable` path.
        match writer.create_topic(name, self.num_partitions, &[]).await {
            Ok(()) | Err(TopicWriteError::AlreadyExists(_)) => {
                tracing::info!(topic = %name, partitions = self.num_partitions, "auto-created topic");
                ERR_LEADER_NOT_AVAILABLE
            }
            Err(e) => {
                tracing::warn!(topic = %name, error = %e, "topic auto-create failed");
                ERR_UNKNOWN_TOPIC_OR_PARTITION
            }
        }
    }
}

#[async_trait]
impl Handler for MetadataHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = metadata::decode_request(&mut body, version)?;

        // gh #274: null asks for every topic, an empty list for none
        // (brokers and cluster info only). That is Apache from v1; only
        // v0, which kaas doesn't serve, read an empty list as "all".
        let requested: Option<Vec<String>> = match req.topics {
            None => None,
            Some(topics) => {
                // Lookup by topic id starts at v12. Below that Apache
                // rejects a null name or a non-zero id before it looks at
                // anything else, and kaas serves at most v10.
                if topics
                    .iter()
                    .any(|t| t.name.is_none() || t.topic_id != metadata::NULL_TOPIC_ID)
                {
                    return invalid_request_response(topics, version);
                }
                // Apache de-duplicates the requested names (gh #278).
                let mut seen = std::collections::HashSet::new();
                Some(
                    topics
                        .into_iter()
                        .filter_map(|t| t.name)
                        .filter(|n| seen.insert(n.clone()))
                        .collect(),
                )
            }
        };

        let (listener_name, principal) = {
            let c = conn.lock();
            (
                c.listener_name.clone(),
                c.principal.clone().unwrap_or_else(Principal::anonymous),
            )
        };
        let advert = self.listeners.for_listener(&listener_name);

        // Cluster mode: advertise the live broker set, each peer at
        // its stable FQDN with the port of the listener this client
        // connected on (gh #125 symmetry). DescribeCluster (key 60)
        // shares this catalog — see `crate::listener_advert`.
        // (External per-broker hostname templates for peers are a
        // follow-up — the external listener ships disabled.)
        let brokers: Vec<metadata::Broker> = advertised_brokers(&self.broker, &advert)
            .into_iter()
            // gh #249: fenced brokers are registered but not serving.
            // Apache omits them from Metadata for the same reason —
            // advertising one sends clients to a broker that cannot
            // answer. DescribeCluster v2 is where they surface.
            .filter(|b| !b.fenced)
            .map(|b| metadata::Broker {
                node_id: b.node_id,
                host: b.host,
                port: b.port,
                rack: None,
            })
            .collect();

        // Per-partition leader from the applied assignment; self
        // when no coordinator is wired (dev) or the partition is
        // missing from the assignment (fresh topic, next recompute
        // pending).
        let coord = self.broker.coordinator();
        let controller_id = controller_id(&self.broker);

        // All topics, or exactly the requested ones with
        // UNKNOWN_TOPIC_OR_PARTITION for any not in the registry.
        let all_topics = requested.is_none();
        let topic_names: Vec<String> = match requested {
            None => self
                .broker
                .topics
                .all()
                .into_iter()
                .map(|t| t.name)
                .collect(),
            Some(names) => names,
        };

        // gh #278: Apache fills this per topic only when asked, and only
        // for topics that passed `Describe` (an authorization-failure row
        // keeps the schema default, `i32::MIN` = "not computed").
        let topic_ops = |name: &str| -> i32 {
            if !req.include_topic_authorized_operations {
                return NOT_REQUESTED;
            }
            let resource = Resource::topic(name);
            TOPIC_OPS
                .iter()
                .filter(|(op, _)| {
                    self.broker
                        .authorizer
                        .authorize_quietly(&principal, &resource, *op)
                })
                .fold(0, |bits, (_, code)| bits | (1 << code))
        };

        let mut topics = Vec::with_capacity(topic_names.len());
        for name in topic_names {
            // gh #268: before the registry lookup, so an unauthorized
            // principal learns nothing about existence. The all-topics
            // form only filters, so it checks quietly: leaving a topic
            // out of the list is not a denial worth a log line.
            let resource = Resource::topic(&name);
            let authz = &self.broker.authorizer;
            let allowed = if all_topics {
                authz.authorize_quietly(&principal, &resource, Operation::Describe)
            } else {
                authz.authorize(&principal, &resource, Operation::Describe)
            };
            if !allowed {
                if !all_topics {
                    topics.push(error_topic(name, ERR_TOPIC_AUTHZ_FAILED));
                }
                continue;
            }
            match self.broker.topics.get(&name) {
                Some(meta) => {
                    let mut partitions =
                        Vec::with_capacity(usize::try_from(meta.partition_count).unwrap_or(0));
                    for i in 0..meta.partition_count {
                        let leader_id = coord
                            .as_ref()
                            .and_then(|c| c.leader_for(&name, i))
                            .and_then(|owner| trailing_ordinal(&owner))
                            .unwrap_or(self.broker.broker_id);
                        partitions.push(metadata::Partition {
                            error_code: 0,
                            partition_index: i,
                            leader_id,
                            leader_epoch: 0,
                            replica_nodes: vec![leader_id],
                            isr_nodes: vec![leader_id],
                            offline_replicas: Vec::new(),
                        });
                    }
                    topics.push(metadata::Topic {
                        error_code: 0,
                        topic_authorized_operations: topic_ops(&meta.name),
                        name: meta.name,
                        topic_id: meta.topic_id,
                        is_internal: false,
                        partitions,
                    });
                }
                None => {
                    // gh #242: only when the broker has the feature on
                    // AND the client asked for it. A v4+ client that
                    // cleared the flag keeps the plain
                    // UNKNOWN_TOPIC_OR_PARTITION answer; a v1-v3 request
                    // has no flag and decodes to Apache's `true` default.
                    let error_code = if self.auto_create && req.allow_auto_topic_creation {
                        self.auto_create_topic(&principal, &name).await
                    } else {
                        ERR_UNKNOWN_TOPIC_OR_PARTITION
                    };
                    let ops = if error_code == ERR_TOPIC_AUTHZ_FAILED {
                        NOT_REQUESTED
                    } else {
                        topic_ops(&name)
                    };
                    topics.push(metadata::Topic {
                        topic_authorized_operations: ops,
                        ..error_topic(name, error_code)
                    });
                }
            }
        }

        let resp = metadata::Response {
            throttle_time_ms: 0,
            brokers,
            cluster_id: Some(self.broker.cluster_id.clone()),
            controller_id,
            topics,
            cluster_authorized_operations: cluster_authorized_operations(
                &self.broker,
                &principal,
                req.include_cluster_authorized_operations,
            ),
        };

        let mut out = BytesMut::new();
        metadata::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

/// Apache's answer to a request it rejects (`MetadataRequest::getErrorResponse`):
/// every requested topic echoed with `INVALID_REQUEST`, a null name as
/// `""`, and no brokers, cluster id or controller.
fn invalid_request_response(
    topics: Vec<metadata::RequestTopic>,
    version: i16,
) -> Result<BytesMut, HandlerError> {
    let resp = metadata::Response {
        throttle_time_ms: 0,
        brokers: Vec::new(),
        cluster_id: None,
        controller_id: -1,
        topics: topics
            .into_iter()
            .map(|t| metadata::Topic {
                topic_id: t.topic_id,
                ..error_topic(t.name.unwrap_or_default(), ERR_INVALID_REQUEST)
            })
            .collect(),
        cluster_authorized_operations: NOT_REQUESTED,
    };
    let mut out = BytesMut::new();
    metadata::encode_response(&mut out, &resp, version)?;
    Ok(out)
}

/// A topic entry that carries only an error: no partitions, nil id, and
/// authorized operations left at the schema default.
fn error_topic(name: String, error_code: i16) -> metadata::Topic {
    metadata::Topic {
        error_code,
        name,
        topic_id: [0; 16],
        is_internal: false,
        partitions: Vec::new(),
        topic_authorized_operations: NOT_REQUESTED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::ListenerEntry;
    use crate::topic_cr_writer::TopicCRWriter;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::common::{write_nullable_array_len, write_str};
    use kaas_codec::primitives::write_i8;
    use kaas_codec::tagged;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn(listener: &str) -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            listener,
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with(topics: Vec<(&str, i32)>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let r = Arc::new(TopicRegistry::new());
        for (n, p) in topics {
            r.insert(TopicMeta {
                name: n.to_owned(),
                partition_count: p,
                topic_id: [0; 16],
            });
        }
        Arc::new(Broker::new(engine, r, "kaas-dev", 0))
    }

    fn listeners() -> Vec<ListenerEntry> {
        vec![
            ListenerEntry {
                name: "internal".to_owned(),
                addr: "0.0.0.0:9092".to_owned(),
                advertised_host: None,
                tls: None,
                authentication_type: None,
                oauth: None,
            },
            ListenerEntry {
                name: "external".to_owned(),
                addr: "0.0.0.0:9094".to_owned(),
                advertised_host: Some("broker-0.cluster.local".to_owned()),
                tls: None,
                authentication_type: None,
                oauth: None,
            },
        ]
    }

    fn encode_request_v9(topics: &[&str]) -> Bytes {
        encode_request_v9_auto(topics, false)
    }

    fn encode_request_v9_auto(topics: &[&str], allow_auto_create: bool) -> Bytes {
        encode_request_v9_opt(Some(topics), allow_auto_create)
    }

    /// The all-topics request: a null list (gh #274).
    fn encode_request_v9_all() -> Bytes {
        encode_request_v9_opt(None, false)
    }

    fn encode_request_v9_opt(topics: Option<&[&str]>, allow_auto_create: bool) -> Bytes {
        let flexible = true; // v9 flexible
        let mut w = BytesMut::new();
        write_nullable_array_len(&mut w, topics.map(<[_]>::len), flexible).unwrap();
        for n in topics.unwrap_or_default() {
            write_str(&mut w, n, flexible).unwrap();
            tagged::write_empty(&mut w);
        }
        write_i8(&mut w, i8::from(allow_auto_create)); // allow_auto_topic_creation (v4+)
        write_i8(&mut w, 0); // include_cluster_authorized_operations (v8-10)
        write_i8(&mut w, 0); // include_topic_authorized_operations (v8+)
        tagged::write_empty(&mut w);
        w.freeze()
    }

    /// Records every `create_topic` call so a test can assert both
    /// that the CR write happened and what partition count it carried.
    #[derive(Debug, Default)]
    struct RecordingWriter {
        calls: Mutex<Vec<(String, i32)>>,
        already_exists: bool,
    }

    impl RecordingWriter {
        fn calls(&self) -> Vec<(String, i32)> {
            self.calls.lock().clone()
        }
    }

    #[async_trait]
    impl TopicCRWriter for RecordingWriter {
        async fn create_topic(
            &self,
            name: &str,
            n: i32,
            _configs: &[(String, String)],
        ) -> Result<(), TopicWriteError> {
            self.calls.lock().push((name.to_owned(), n));
            if self.already_exists {
                return Err(TopicWriteError::AlreadyExists(name.into()));
            }
            Ok(())
        }
        async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never expands a topic")
        }
        async fn update_topic_config(
            &self,
            _: &str,
            _: &[crate::topic_cr_writer::ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never edits topic config")
        }
        async fn delete_topic(&self, _: &str) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never deletes a topic")
        }
        async fn set_partition_log_dir(
            &self,
            _: &str,
            _: i32,
            _: &str,
        ) -> Result<(), TopicWriteError> {
            unreachable!("Metadata never moves a partition")
        }
    }

    /// Drive one v9 Metadata request and return the per-topic error
    /// code for the single requested topic.
    async fn error_code_for(h: &MetadataHandler, topic: &str, allow_auto: bool) -> i16 {
        let body = encode_request_v9_auto(&[topic], allow_auto);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        metadata::decode_response(&mut r, 9).unwrap().topics[0].error_code
    }

    #[tokio::test]
    async fn returns_self_as_only_broker_and_leader() {
        let h = MetadataHandler::new(broker_with(vec![("events", 3)]), &listeners());
        let body = encode_request_v9(&["events"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].node_id, 0);
        assert_eq!(resp.cluster_id.as_deref(), Some("kaas-dev"));
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "events");
        assert_eq!(resp.topics[0].partitions.len(), 3);
        for p in &resp.topics[0].partitions {
            assert_eq!(p.leader_id, 0);
            assert_eq!(p.replica_nodes, vec![0]);
            assert_eq!(p.isr_nodes, vec![0]);
        }
    }

    #[tokio::test]
    async fn per_listener_port_echoed_back() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["events"]);

        let internal = h.handle(&conn("internal"), 9, body.clone()).await.unwrap();
        let mut r = internal.freeze();
        let resp_int = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_int.brokers[0].port, 9092);
        assert_eq!(resp_int.brokers[0].host, "127.0.0.1");

        let external = h.handle(&conn("external"), 9, body).await.unwrap();
        let mut r = external.freeze();
        let resp_ext = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp_ext.brokers[0].port, 9094);
        assert_eq!(resp_ext.brokers[0].host, "broker-0.cluster.local");
    }

    #[tokio::test]
    async fn unknown_topic_returns_per_topic_error_3() {
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners());
        let body = encode_request_v9(&["nope"]);
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics[0].error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        assert!(resp.topics[0].partitions.is_empty());
    }

    // --- gh #242: auto.create.topics.enable -----------------------

    #[tokio::test]
    async fn auto_create_mints_cr_and_answers_leader_not_available() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 3);

        assert_eq!(
            error_code_for(&h, "brand-new", true).await,
            ERR_LEADER_NOT_AVAILABLE
        );
        // The CR carries the configured num.partitions, not the
        // requesting client's guess (Metadata has no such field).
        assert_eq!(writer.calls(), vec![("brand-new".to_owned(), 3)]);
    }

    #[tokio::test]
    async fn auto_create_requires_the_client_to_ask() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        // Feature on, but the v4+ client explicitly opted out. An
        // opt-out must be honoured even with the broker switch on,
        // or `allow.auto.create.topics=false` on the consumer means
        // nothing.
        assert_eq!(
            error_code_for(&h, "nope", false).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_disabled_never_writes() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners());

        assert_eq!(
            error_code_for(&h, "nope", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_refuses_reserved_internal_names() {
        let broker = broker_with(vec![("events", 1)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        assert_eq!(
            error_code_for(&h, "__consumer_offsets", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_without_a_writer_reports_unknown() {
        // Dev mode: no kube client, so nothing can mint the CR and
        // claiming LEADER_NOT_AVAILABLE would loop the client forever.
        let h = MetadataHandler::new(broker_with(vec![("events", 1)]), &listeners())
            .with_auto_create(true, 1);
        assert_eq!(
            error_code_for(&h, "nope", true).await,
            ERR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    #[tokio::test]
    async fn auto_create_treats_already_exists_as_success() {
        let broker = broker_with(vec![("events", 1)]);
        broker.install_cr_writer(Arc::new(RecordingWriter {
            calls: Mutex::new(Vec::new()),
            already_exists: true,
        }));
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);
        // A concurrent creator won the race; the topic exists, so the
        // caller should retry rather than be told it's unknown.
        assert_eq!(
            error_code_for(&h, "raced", true).await,
            ERR_LEADER_NOT_AVAILABLE
        );
    }

    fn broker_with_auth(
        topics: Vec<(&str, i32)>,
        auth: Arc<dyn kaas_auth::Authorizer>,
    ) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let r = Arc::new(TopicRegistry::new());
        for (n, p) in topics {
            r.insert(TopicMeta {
                name: n.to_owned(),
                partition_count: p,
                topic_id: [0; 16],
            });
        }
        Arc::new(Broker::with_auth(
            engine,
            r,
            "kaas-dev",
            0,
            auth,
            Arc::new(kaas_auth::NoQuotaChecker),
        ))
    }

    #[tokio::test]
    async fn auto_create_honours_the_authorizer() {
        // Auto-creation is a create. If Metadata skipped the ACL check,
        // an unauthorized client could mint topics through it even
        // though CreateTopics would refuse. `Describe` is granted so
        // the request reaches the `Create` check.
        use crate::handlers::test_authz::DenyOpAuthorizer;
        let broker = broker_with_auth(Vec::new(), Arc::new(DenyOpAuthorizer(Operation::Create)));
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        assert_eq!(
            error_code_for(&h, "forbidden-topic", true).await,
            ERR_TOPIC_AUTHZ_FAILED
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn auto_create_checks_describe_before_create() {
        // gh #268: Apache answers 29 for an undescribable topic before
        // the auto-create path is considered, even when `Create` would
        // pass, so neither existence nor a CR write leaks.
        use crate::handlers::test_authz::DenyOpAuthorizer;
        let broker = broker_with_auth(Vec::new(), Arc::new(DenyOpAuthorizer(Operation::Describe)));
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 1);

        assert_eq!(
            error_code_for(&h, "brand-new", true).await,
            ERR_TOPIC_AUTHZ_FAILED
        );
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn all_topics_omits_what_the_principal_cannot_describe() {
        // gh #268: a list-all filters silently; it must not error.
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let broker = broker_with_auth(
            vec![("a", 1), ("secret", 2)],
            Arc::new(DenyNamedAuthorizer("secret")),
        );
        let h = MetadataHandler::new(broker, &listeners());
        let out = h
            .handle(&conn("internal"), 9, encode_request_v9_all())
            .await
            .unwrap();
        let resp = metadata::decode_response(&mut out.freeze(), 9).unwrap();
        let names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["a"]);
    }

    #[tokio::test]
    async fn named_topic_without_describe_is_29_existing_or_not() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let broker = broker_with_auth(vec![("a", 1)], Arc::new(DenyAllAuthorizer));
        let h = MetadataHandler::new(broker, &listeners());
        assert_eq!(error_code_for(&h, "a", false).await, ERR_TOPIC_AUTHZ_FAILED);
        assert_eq!(
            error_code_for(&h, "nope", false).await,
            ERR_TOPIC_AUTHZ_FAILED
        );
    }

    #[tokio::test]
    async fn auto_create_leaves_known_topics_alone() {
        let broker = broker_with(vec![("events", 2)]);
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 9);

        assert_eq!(error_code_for(&h, "events", true).await, 0);
        assert!(writer.calls().is_empty());
    }

    #[tokio::test]
    async fn null_topic_list_returns_all_known() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1), ("b", 2)]), &listeners());
        let out = h
            .handle(&conn("internal"), 9, encode_request_v9_all())
            .await
            .unwrap();
        let mut r = out.freeze();
        let resp = metadata::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.topics.len(), 2);
        let names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
    }

    #[tokio::test]
    async fn empty_topic_list_returns_brokers_and_no_topics() {
        // gh #274: Apache from v1. Empty meant "all" only at v0.
        let h = MetadataHandler::new(broker_with(vec![("a", 1), ("b", 2)]), &listeners());
        let out = h
            .handle(&conn("internal"), 9, encode_request_v9(&[]))
            .await
            .unwrap();
        let resp = metadata::decode_response(&mut out.freeze(), 9).unwrap();
        assert!(resp.topics.is_empty(), "got {:?}", resp.topics);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.cluster_id.as_deref(), Some("kaas-dev"));
    }

    /// One v10 topic entry: `(topic_id, nullable name)`.
    fn encode_request_v10(topic_id: [u8; 16], name: Option<&str>) -> Bytes {
        use kaas_codec::api::common::write_nullable_str;
        use kaas_codec::primitives::write_uuid;
        let mut w = BytesMut::new();
        write_nullable_array_len(&mut w, Some(1), true).unwrap();
        write_uuid(&mut w, &topic_id);
        write_nullable_str(&mut w, name, true).unwrap();
        tagged::write_empty(&mut w);
        write_i8(&mut w, 0); // allow_auto_topic_creation
        write_i8(&mut w, 0); // include_cluster_authorized_operations
        write_i8(&mut w, 0); // include_topic_authorized_operations
        tagged::write_empty(&mut w);
        w.freeze()
    }

    #[tokio::test]
    async fn v10_rejects_a_topic_named_only_by_id() {
        // gh #274: this used to drop the nameless entry and so answer
        // with every topic. Apache rejects a null name or a non-zero id
        // below v12 (lookup by id is v12+), echoing each entry.
        let h = MetadataHandler::new(broker_with(vec![("a", 1)]), &listeners());
        for (id, name) in [([0xab; 16], None), ([0xab; 16], Some("a")), ([0; 16], None)] {
            let out = h
                .handle(&conn("internal"), 10, encode_request_v10(id, name))
                .await
                .unwrap();
            let resp = metadata::decode_response(&mut out.freeze(), 10).unwrap();
            assert_eq!(resp.topics.len(), 1, "{name:?}");
            assert_eq!(resp.topics[0].error_code, ERR_INVALID_REQUEST);
            assert_eq!(resp.topics[0].name, name.unwrap_or_default());
            assert_eq!(resp.topics[0].topic_id, id);
            assert!(resp.brokers.is_empty());
        }
    }

    #[tokio::test]
    async fn v10_serves_a_named_topic_with_a_nil_id() {
        let h = MetadataHandler::new(broker_with(vec![("a", 2)]), &listeners());
        let out = h
            .handle(
                &conn("internal"),
                10,
                encode_request_v10([0; 16], Some("a")),
            )
            .await
            .unwrap();
        let resp = metadata::decode_response(&mut out.freeze(), 10).unwrap();
        assert_eq!(resp.topics[0].error_code, 0);
        assert_eq!(resp.topics[0].partitions.len(), 2);
    }

    /// Metadata v9 with both authorized-operations flags.
    fn encode_request_v9_ops(topics: &[&str], include_cluster: bool, include_topic: bool) -> Bytes {
        let mut w = BytesMut::new();
        write_nullable_array_len(&mut w, Some(topics.len()), true).unwrap();
        for n in topics {
            write_str(&mut w, n, true).unwrap();
            tagged::write_empty(&mut w);
        }
        write_i8(&mut w, 0); // allow_auto_topic_creation
        write_i8(&mut w, i8::from(include_cluster));
        write_i8(&mut w, i8::from(include_topic));
        tagged::write_empty(&mut w);
        w.freeze()
    }

    async fn metadata_v9(h: &MetadataHandler, body: Bytes) -> metadata::Response {
        let out = h.handle(&conn("internal"), 9, body).await.unwrap();
        metadata::decode_response(&mut out.freeze(), 9).unwrap()
    }

    /// gh #278: `i32::MIN` ("not computed") unless the client asks; 0
    /// would tell it the principal may do nothing.
    #[tokio::test]
    async fn authorized_operations_are_omitted_unless_asked() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1)]), &listeners());
        let resp = metadata_v9(&h, encode_request_v9_ops(&["a", "nope"], false, false)).await;
        assert_eq!(resp.cluster_authorized_operations, i32::MIN);
        for t in &resp.topics {
            assert_eq!(t.topic_authorized_operations, i32::MIN, "{}", t.name);
        }
    }

    /// gh #278: asked for, the fields carry Apache's bitfields — every
    /// supported topic op, and the cluster set DescribeCluster reports.
    #[tokio::test]
    async fn authorized_operations_are_computed_when_asked() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1)]), &listeners());
        let resp = metadata_v9(&h, encode_request_v9_ops(&["a", "nope"], true, true)).await;
        let topic_bits = TOPIC_OPS.iter().fold(0, |b, (_, c)| b | (1 << c));
        assert_eq!(resp.topics[0].topic_authorized_operations, topic_bits);
        assert_eq!(
            resp.topics[1].topic_authorized_operations, topic_bits,
            "an unknown topic that passed Describe gets the field too, as in Apache"
        );
        assert_eq!(resp.topics[1].error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        let cluster_bits = [
            acl_op::CREATE,
            acl_op::ALTER,
            acl_op::DESCRIBE,
            acl_op::DESCRIBE_CONFIGS,
            acl_op::ALTER_CONFIGS,
        ]
        .iter()
        .fold(0, |b, c| b | (1 << c));
        assert_eq!(resp.cluster_authorized_operations, cluster_bits);
    }

    /// gh #278: an authorization-failure row keeps the schema default.
    #[tokio::test]
    async fn a_denied_topic_row_leaves_authorized_operations_unset() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let broker = broker_with_auth(vec![("secret", 1)], Arc::new(DenyNamedAuthorizer("secret")));
        let h = MetadataHandler::new(broker, &listeners());
        let resp = metadata_v9(&h, encode_request_v9_ops(&["secret"], false, true)).await;
        assert_eq!(resp.topics[0].error_code, ERR_TOPIC_AUTHZ_FAILED);
        assert_eq!(resp.topics[0].topic_authorized_operations, i32::MIN);
    }

    /// gh #278: Apache de-duplicates the requested names.
    #[tokio::test]
    async fn duplicate_topic_names_are_answered_once() {
        let h = MetadataHandler::new(broker_with(vec![("a", 1)]), &listeners());
        let resp = metadata_v9(&h, encode_request_v9(&["a", "a", "nope", "nope"])).await;
        let names: Vec<&str> = resp.topics.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, ["a", "nope"]);
    }

    /// gh #280: auto-create accepts Apache's cluster-wide `Create` grant,
    /// the same rule as CreateTopics.
    #[tokio::test]
    async fn auto_create_accepts_cluster_create() {
        use crate::handlers::test_authz::ClusterCreateOnlyAuthorizer;
        let broker = broker_with_auth(Vec::new(), Arc::new(ClusterCreateOnlyAuthorizer));
        let writer = Arc::new(RecordingWriter::default());
        broker.install_cr_writer(writer.clone());
        let h = MetadataHandler::new(broker, &listeners()).with_auto_create(true, 2);
        assert_eq!(
            error_code_for(&h, "brand-new", true).await,
            ERR_LEADER_NOT_AVAILABLE
        );
        assert_eq!(writer.calls(), [("brand-new".to_owned(), 2)]);
    }
}
