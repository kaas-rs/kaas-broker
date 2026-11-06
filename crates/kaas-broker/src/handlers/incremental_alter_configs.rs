//! IncrementalAlterConfigs handler — API key 44 (gh #9).
//!
//! Topic-only surface (BROKER + BROKER_LOGGER return
//! `UNSUPPORTED_VERSION` (35)). Per-resource error codes ride in
//! the response; the top-level `error_code` stays 0.
//!
//! Each op gets translated by the installed [`TopicCRWriter`] into
//! a JSON-merge PATCH on `KafkaTopic.spec.config`. `Set` writes
//! the parsed value, `Delete` writes null, `Append` / `Subtract`
//! aren't implemented and surface as `UNSUPPORTED_VERSION`, even on
//! `cleanup.policy`, which is list-typed. Unknown keys and
//! unparseable values are rejected with `INVALID_CONFIG` (40)
//! before anything touches the API server (gh #236).
//!
//! Authorization: `Operation::AlterConfigs` on the topic resource.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::incremental_alter_configs;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::{
    ops_to_config_patch, ConfigOpKind, ConfigOpWithValue, TopicWriteError,
};

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNSUPPORTED_VERSION: i16 = 35;
const ERR_INVALID_CONFIG: i16 = 40;
const ERR_UNKNOWN_SERVER: i16 = -1;

#[derive(Debug)]
pub struct IncrementalAlterConfigsHandler {
    broker: Arc<Broker>,
}

impl IncrementalAlterConfigsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for IncrementalAlterConfigsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = incremental_alter_configs::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let writer = self.broker.cr_writer();
        let mut responses = Vec::with_capacity(req.resources.len());

        for resource in req.resources {
            if resource.resource_type != incremental_alter_configs::resource_type::TOPIC {
                // BROKER + BROKER_LOGGER aren't implemented yet: the
                // broker has no dynamic-config surface (gh #338 for
                // log levels). A parity gap, not a non-goal.
                responses.push(response_for(
                    &resource,
                    ERR_UNSUPPORTED_VERSION,
                    Some("only Topic resource type is supported"),
                ));
                continue;
            }

            let res = Resource::topic(&resource.resource_name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &res, Operation::AlterConfigs)
            {
                responses.push(response_for(&resource, ERR_TOPIC_AUTHZ_FAILED, None));
                continue;
            }

            let Some(w) = writer.as_ref() else {
                responses.push(response_for(
                    &resource,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some("broker is not running in cluster mode"),
                ));
                continue;
            };

            let ops: Vec<ConfigOpWithValue> = resource
                .configs
                .iter()
                .map(|c| ConfigOpWithValue {
                    key: c.name.clone(),
                    kind: wire_op_to_kind(c.op),
                    value: c.value.clone(),
                })
                .collect();

            // gh #236: validate up front so `validate_only` answers
            // honestly and a bad key/value never reaches the API
            // server (whose merge-patch semantics would *prune* an
            // unknown field — the silent success this issue is about).
            if let Err(e) = ops_to_config_patch(&ops) {
                let (code, msg) = match &e {
                    TopicWriteError::UnsupportedOp(kind) => {
                        (ERR_UNSUPPORTED_VERSION, format!("unsupported op: {kind:?}"))
                    }
                    other => (ERR_INVALID_CONFIG, other.to_string()),
                };
                responses.push(response_for(&resource, code, Some(&msg)));
                continue;
            }

            if req.validate_only {
                responses.push(response_for(&resource, ERR_NONE, None));
                continue;
            }

            match w.update_topic_config(&resource.resource_name, &ops).await {
                Ok(()) => responses.push(response_for(&resource, ERR_NONE, None)),
                Err(TopicWriteError::NotFound(_)) => {
                    responses.push(response_for(&resource, ERR_UNKNOWN_TOPIC, None))
                }
                Err(TopicWriteError::UnsupportedOp(kind)) => responses.push(response_for(
                    &resource,
                    ERR_UNSUPPORTED_VERSION,
                    Some(&format!("unsupported op: {kind:?}")),
                )),
                Err(TopicWriteError::InvalidConfig(msg)) => {
                    responses.push(response_for(&resource, ERR_INVALID_CONFIG, Some(&msg)))
                }
                Err(TopicWriteError::Forbidden(msg)) => responses.push(response_for(
                    &resource,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some(&msg),
                )),
                Err(other) => {
                    tracing::warn!(error = %other, topic = %resource.resource_name, "IncrementalAlterConfigs failed");
                    responses.push(response_for(&resource, ERR_UNKNOWN_SERVER, None));
                }
            }
        }

        let resp = incremental_alter_configs::Response {
            throttle_time_ms: 0,
            responses,
        };
        let mut out = BytesMut::new();
        incremental_alter_configs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn wire_op_to_kind(op: i8) -> ConfigOpKind {
    match op {
        incremental_alter_configs::op::SET => ConfigOpKind::Set,
        incremental_alter_configs::op::DELETE => ConfigOpKind::Delete,
        incremental_alter_configs::op::APPEND => ConfigOpKind::Append,
        incremental_alter_configs::op::SUBTRACT => ConfigOpKind::Subtract,
        // Unknown op discriminant — bias toward Set so the writer
        // can decide; in practice this never happens because the
        // codec validates the i8 against the schema.
        _ => ConfigOpKind::Set,
    }
}

fn response_for(
    resource: &incremental_alter_configs::AlterConfigsResource,
    code: i16,
    message: Option<&str>,
) -> incremental_alter_configs::AlterConfigsResourceResponse {
    incremental_alter_configs::AlterConfigsResourceResponse {
        error_code: code,
        error_message: message.map(str::to_owned),
        resource_type: resource.resource_type,
        resource_name: resource.resource_name.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_cr_writer::TopicCRWriter;
    use crate::topic_registry::TopicRegistry;
    use kaas_codec::api::incremental_alter_configs::{
        op, resource_type, AlterConfigOp, AlterConfigsResource, Request,
    };
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    #[derive(Debug, Default)]
    struct RecordingWriter {
        calls: Mutex<Vec<(String, Vec<ConfigOpWithValue>)>>,
    }

    #[async_trait]
    impl TopicCRWriter for RecordingWriter {
        async fn create_topic(
            &self,
            _: &str,
            _: i32,
            _: &[(String, String)],
        ) -> Result<(), TopicWriteError> {
            unreachable!()
        }
        async fn expand_topic(&self, _: &str, _: i32) -> Result<(), TopicWriteError> {
            unreachable!()
        }
        async fn update_topic_config(
            &self,
            name: &str,
            ops: &[ConfigOpWithValue],
        ) -> Result<(), TopicWriteError> {
            self.calls.lock().push((name.to_owned(), ops.to_vec()));
            Ok(())
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

    async fn alter(
        writer: Arc<RecordingWriter>,
        key: &str,
        value: Option<&str>,
        wire_op: i8,
    ) -> incremental_alter_configs::Response {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let broker = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        broker.install_cr_writer(writer);
        let h = IncrementalAlterConfigsHandler::new(broker);
        let req = Request {
            resources: vec![AlterConfigsResource {
                resource_type: resource_type::TOPIC,
                resource_name: "t".into(),
                configs: vec![AlterConfigOp {
                    name: key.into(),
                    op: wire_op,
                    value: value.map(str::to_owned),
                }],
            }],
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        incremental_alter_configs::encode_request(&mut buf, &req, 1).unwrap();
        let out = h.handle(&conn(), 1, buf.freeze()).await.unwrap();
        let mut r = out.freeze();
        incremental_alter_configs::decode_response(&mut r, 1).unwrap()
    }

    #[tokio::test]
    async fn a_valid_set_reaches_the_writer() {
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(w.clone(), "retention.ms", Some("1200000"), op::SET).await;
        assert_eq!(resp.responses[0].error_code, ERR_NONE);
        assert_eq!(w.calls.lock().len(), 1);
    }

    #[tokio::test]
    async fn an_unknown_key_answers_invalid_config_not_success() {
        // gh #236: `kafka-configs.sh --alter --add-config
        // max.message.bytes=…` used to answer UNSUPPORTED_VERSION —
        // and before that, plain success. INVALID_CONFIG (40) is the
        // honest, actionable answer.
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(w.clone(), "max.message.bytes", Some("1000"), op::SET).await;
        assert_eq!(resp.responses[0].error_code, ERR_INVALID_CONFIG);
        assert!(
            resp.responses[0]
                .error_message
                .as_deref()
                .unwrap_or_default()
                .contains("max.message.bytes"),
            "the message should name the offending key"
        );
        assert!(w.calls.lock().is_empty(), "nothing may reach the CR");
    }

    #[tokio::test]
    async fn an_unparseable_value_answers_invalid_config() {
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(w.clone(), "retention.ms", Some("huh"), op::SET).await;
        assert_eq!(resp.responses[0].error_code, ERR_INVALID_CONFIG);
        assert!(w.calls.lock().is_empty());
    }
}
