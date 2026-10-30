//! CreatePartitions handler — API key 37 (gh #52).
//!
//! Translates `AdminClient.createPartitions()` / `kafka-topics.sh
//! --alter --partitions N` into a PATCH on `KafkaTopic.spec.partitions`
//! via the installed [`TopicCRWriter`]. The operator then reconciles
//! the new partition count, creating the new directory entries on the
//! shared PVC; the broker observes them via its existing topic
//! watch (`run_topic_watch`) and serves the additional partitions on
//! the next Metadata request.
//!
//! Authorization: `Operation::Alter` on the topic resource.
//!
//! Wire error mapping (see also [`TopicWriteError`]):
//!
//! - missing CR writer → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - CR absent           → `UNKNOWN_TOPIC_OR_PARTITION` (3)
//! - shrink attempt      → `INVALID_PARTITIONS` (37)
//! - RBAC denial         → `CLUSTER_AUTHORIZATION_FAILED` (31)
//! - other kube error    → `UNKNOWN_SERVER_ERROR` (-1)
//!
//! `validate_only: true` (v1+) short-circuits BEFORE issuing the
//! patch — same client-side guard runs, then the would-be response
//! is returned without mutating state.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::create_partitions;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::topic_cr_writer::TopicWriteError;

const ERR_NONE: i16 = 0;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_INVALID_PARTITIONS: i16 = 37;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_UNKNOWN_SERVER: i16 = -1;

#[derive(Debug)]
pub struct CreatePartitionsHandler {
    broker: Arc<Broker>,
}

impl CreatePartitionsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for CreatePartitionsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = create_partitions::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let writer = self.broker.cr_writer();
        let mut results = Vec::with_capacity(req.topics.len());

        for topic in req.topics {
            // Authorize first so the response carries TOPIC_AUTHORIZATION_FAILED
            // (29) when relevant, not the generic CLUSTER variant.
            let resource = Resource::topic(&topic.name);
            if !self
                .broker
                .authorizer
                .authorize(&principal, &resource, Operation::Alter)
            {
                results.push(error_result(&topic.name, ERR_TOPIC_AUTHZ_FAILED, None));
                continue;
            }

            // No writer → broker is running without K8s admin
            // surface: refuse cleanly.
            let Some(w) = writer.as_ref() else {
                results.push(error_result(
                    &topic.name,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some("broker is not running in cluster mode"),
                ));
                continue;
            };

            // validate_only — skip the actual patch, return the
            // outcome we'd have produced.
            if req.validate_only {
                results.push(success_result(&topic.name));
                continue;
            }

            match w.expand_topic(&topic.name, topic.count).await {
                Ok(()) => results.push(success_result(&topic.name)),
                Err(TopicWriteError::NotFound(_)) => {
                    results.push(error_result(&topic.name, ERR_UNKNOWN_TOPIC, None))
                }
                Err(TopicWriteError::InvalidPartitions(msg)) => results.push(error_result(
                    &topic.name,
                    ERR_INVALID_PARTITIONS,
                    Some(&msg),
                )),
                Err(TopicWriteError::Forbidden(msg)) => results.push(error_result(
                    &topic.name,
                    ERR_CLUSTER_AUTHZ_FAILED,
                    Some(&msg),
                )),
                Err(other) => {
                    tracing::warn!(error = %other, topic = %topic.name, "CreatePartitions failed");
                    results.push(error_result(&topic.name, ERR_UNKNOWN_SERVER, None));
                }
            }
        }

        let resp = create_partitions::Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        create_partitions::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn success_result(name: &str) -> create_partitions::CreatePartitionsTopicResult {
    create_partitions::CreatePartitionsTopicResult {
        name: name.into(),
        error_code: ERR_NONE,
        error_message: None,
    }
}

fn error_result(
    name: &str,
    code: i16,
    message: Option<&str>,
) -> create_partitions::CreatePartitionsTopicResult {
    create_partitions::CreatePartitionsTopicResult {
        name: name.into(),
        error_code: code,
        error_message: message.map(str::to_owned),
    }
}
