//! OffsetDelete handler (key 47, gh #100).
//!
//! v0 only. Drops specific `(topic, partition)` committed offset
//! entries without dropping the whole group. Group-level errors:
//! `NOT_COORDINATOR` (16), `GROUP_ID_NOT_FOUND` (69),
//! `NON_EMPTY_GROUP` (67). Per-partition errors —
//! `UNKNOWN_TOPIC_OR_PARTITION` (3) — only fire when the group-level
//! error is 0.
//!
//! Authorization (gh #268), Apache's order: `Delete` on the group
//! before coordinator routing — the top-level
//! `GROUP_AUTHORIZATION_FAILED` (30) with no topics otherwise — then
//! `Read` on each topic. A denied topic's partitions answer
//! `TOPIC_AUTHORIZATION_FAILED` (29) and keep their offsets.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::offset_delete;
use kaas_coordinator::build_offset_key;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct OffsetDeleteHandler {
    broker: Arc<Broker>,
}

impl OffsetDeleteHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for OffsetDeleteHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_delete::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        if !self.broker.authorizer.authorize(
            &principal,
            &Resource::group(&req.group_id),
            Operation::Delete,
        ) {
            return group_error_response(version, ERR_GROUP_AUTHZ_FAILED);
        }
        let mgr = match self.broker.coord_manager() {
            None => return group_error_response(version, ERR_NOT_COORDINATOR),
            Some(m) => m,
        };
        let denied: HashSet<&str> = req
            .topics
            .iter()
            .filter(|t| {
                !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::topic(&t.name),
                    Operation::Read,
                )
            })
            .map(|t| t.name.as_str())
            .collect();

        // Build the `topic/partition` key list expected by
        // OffsetStore::delete_partitions.
        let keys: Vec<String> = req
            .topics
            .iter()
            .filter(|t| !denied.contains(t.name.as_str()))
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |&p| build_offset_key(&t.name, p))
            })
            .collect();
        let (group_error, removed) = mgr.delete_offsets(&req.group_id, &keys);

        let topics_resp: Vec<offset_delete::OffsetDeleteTopicResponse> = req
            .topics
            .iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .iter()
                    .map(|&p| {
                        let key = build_offset_key(&t.name, p);
                        let error_code = if group_error != 0 {
                            // Per Apache's contract per-partition
                            // errors are suppressed when the group-
                            // level error is non-zero.
                            0
                        } else if denied.contains(t.name.as_str()) {
                            ERR_TOPIC_AUTHZ_FAILED
                        } else if *removed.get(&key).unwrap_or(&false) {
                            0
                        } else {
                            ERR_UNKNOWN_TOPIC_OR_PARTITION
                        };
                        offset_delete::OffsetDeletePartitionResponse {
                            partition_index: p,
                            error_code,
                        }
                    })
                    .collect();
                offset_delete::OffsetDeleteTopicResponse {
                    name: t.name.clone(),
                    partitions,
                }
            })
            .collect();

        let resp = offset_delete::Response {
            error_code: group_error,
            throttle_time_ms: 0,
            topics: topics_resp,
        };
        let mut out = BytesMut::new();
        offset_delete::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

/// A group-level error with no topics, Apache's shape for a request
/// that never reached the per-partition stage.
fn group_error_response(version: i16, error_code: i16) -> Result<BytesMut, HandlerError> {
    let resp = offset_delete::Response {
        error_code,
        throttle_time_ms: 0,
        topics: Vec::new(),
    };
    let mut out = BytesMut::new();
    offset_delete::encode_response(&mut out, &resp, version)?;
    Ok(out)
}
