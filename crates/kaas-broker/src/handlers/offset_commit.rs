//! OffsetCommit handler (key 8).
//!
//! Flattens the codec's nested `(topic, partition)` shape into a
//! single `offset_key → offset` map, delegates to
//! [`Manager::offset_commit`], and echoes the per-partition layout
//! back with the resulting error code stamped uniformly across all
//! partitions.
//!
//! Authorization (gh #268), Apache's order: `Read` on the group
//! first — before coordinator routing, so a denied principal learns
//! nothing about where the group lives — then `Read` on each topic.
//! Denied topics answer `TOPIC_AUTHORIZATION_FAILED` (29) and are
//! left out of the commit; the rest commit normally.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::offset_commit;
use kaas_coordinator::build_offset_key;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct OffsetCommitHandler {
    broker: Arc<Broker>,
}

impl OffsetCommitHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for OffsetCommitHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_commit::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        if !self.broker.authorizer.authorize(
            &principal,
            &Resource::group(&req.group_id),
            Operation::Read,
        ) {
            return write_response(version, req.topics, |_| ERR_GROUP_AUTHZ_FAILED);
        }
        let denied: HashSet<String> = req
            .topics
            .iter()
            .filter(|t| {
                !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::topic(&t.name),
                    Operation::Read,
                )
            })
            .map(|t| t.name.clone())
            .collect();

        // Build the offsets + metadata maps the Manager expects.
        let mut offsets: HashMap<String, i64> = HashMap::new();
        let mut metadata: HashMap<String, String> = HashMap::new();
        for t in req.topics.iter().filter(|t| !denied.contains(&t.name)) {
            for p in &t.partitions {
                let key = build_offset_key(&t.name, p.partition_index);
                offsets.insert(key.clone(), p.committed_offset);
                if let Some(m) = &p.committed_metadata {
                    if !m.is_empty() {
                        metadata.insert(key, m.clone());
                    }
                }
            }
        }

        // Nothing authorized to commit: answer the per-topic denials
        // without touching (or revealing) the coordinator.
        let group_error = if offsets.is_empty() && !denied.is_empty() {
            0
        } else {
            match self.broker.coord_manager() {
                None => ERR_NOT_COORDINATOR,
                Some(mgr) => mgr.offset_commit(&req.group_id, offsets, metadata),
            }
        };

        write_response(version, req.topics, |topic| {
            if denied.contains(topic) {
                ERR_TOPIC_AUTHZ_FAILED
            } else {
                group_error
            }
        })
    }
}

/// Echo the request's `(topic, partition)` layout with a per-topic
/// error code.
fn write_response(
    version: i16,
    topics: Vec<offset_commit::OffsetCommitTopic>,
    code_for_topic: impl Fn(&str) -> i16,
) -> Result<BytesMut, HandlerError> {
    let topics_resp: Vec<offset_commit::OffsetCommitTopicResponse> = topics
        .into_iter()
        .map(|t| {
            let error_code = code_for_topic(&t.name);
            let partitions = t
                .partitions
                .iter()
                .map(|p| offset_commit::OffsetCommitPartitionResponse {
                    partition_index: p.partition_index,
                    error_code,
                })
                .collect();
            offset_commit::OffsetCommitTopicResponse {
                name: t.name,
                partitions,
            }
        })
        .collect();

    let resp = offset_commit::Response {
        throttle_time_ms: 0,
        topics: topics_resp,
    };
    let mut out = BytesMut::new();
    offset_commit::encode_response(&mut out, &resp, version)?;
    Ok(out)
}
