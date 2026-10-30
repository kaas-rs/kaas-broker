//! OffsetFetch handler (key 9).
//!
//! Supports both the v1..=v7 single-group form and the v8+ batch
//! `groups[]` shape. `Manager::offset_fetch` returns the
//! `(committed → i64, metadata → String)` pair per group; the
//! handler joins them per the codec response layout.
//!
//! Apache's "fetch every committed offset" sentinel is a `None`
//! topic list. The Phase-5 OffsetStore doesn't carry "every key"
//! enumeration — the handler maps null-topics to an empty result
//! rather than enumerating, matching the dev-mode `Broker::new`
//! behaviour. The cluster path lands when the Manager grows a
//! `local_offsets(group_id) -> Vec<FetchSpec>` accessor.
//!
//! Authorization (gh #268), Apache's order: `Describe` on the group
//! before coordinator routing — `GROUP_AUTHORIZATION_FAILED` (30) as
//! the top-level error on v2–v7, per group on v8+, per partition on
//! v1, which has no top-level field — then `Describe` on each topic,
//! whose denied partitions answer `TOPIC_AUTHORIZATION_FAILED` (29)
//! with no offset.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::offset_fetch;
use kaas_coordinator::{build_offset_key, FetchSpec};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct OffsetFetchHandler {
    broker: Arc<Broker>,
}

impl OffsetFetchHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for OffsetFetchHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_fetch::decode_request(&mut body, version)?;
        let mgr = self.broker.coord_manager();
        let principal = principal_from(conn);
        let may_describe_topic = |topic: &str| self.allowed(&principal, Resource::topic(topic));

        let resp = if version >= 8 {
            let mut groups_resp =
                Vec::<offset_fetch::OffsetFetchGroupResponse>::with_capacity(req.groups.len());
            for g in &req.groups {
                if !self.allowed(&principal, Resource::group(&g.group_id)) {
                    groups_resp.push(offset_fetch::OffsetFetchGroupResponse {
                        group_id: g.group_id.clone(),
                        topics: Vec::new(),
                        error_code: ERR_GROUP_AUTHZ_FAILED,
                    });
                    continue;
                }
                let (topics_resp, group_err) =
                    fetch_group(mgr.as_ref(), &g.group_id, &g.topics, &may_describe_topic);
                groups_resp.push(offset_fetch::OffsetFetchGroupResponse {
                    group_id: g.group_id.clone(),
                    topics: topics_resp,
                    error_code: group_err,
                });
            }
            offset_fetch::Response {
                throttle_time_ms: 0,
                topics: Vec::new(),
                error_code: 0,
                groups: groups_resp,
            }
        } else if !self.allowed(&principal, Resource::group(&req.group_id)) {
            // v1 has no top-level error field; Apache stamps the code
            // on every requested partition instead.
            let topics = if version < 2 {
                req.topics
                    .iter()
                    .flatten()
                    .map(|t| error_topic(t, ERR_GROUP_AUTHZ_FAILED))
                    .collect()
            } else {
                Vec::new()
            };
            offset_fetch::Response {
                throttle_time_ms: 0,
                topics,
                error_code: ERR_GROUP_AUTHZ_FAILED,
                groups: Vec::new(),
            }
        } else {
            let topics_in = req.topics.clone().unwrap_or_default();
            let (topics_resp, group_err) =
                fetch_group(mgr.as_ref(), &req.group_id, &topics_in, &may_describe_topic);
            offset_fetch::Response {
                throttle_time_ms: 0,
                topics: topics_resp,
                error_code: group_err,
                groups: Vec::new(),
            }
        };
        let mut out = BytesMut::new();
        offset_fetch::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl OffsetFetchHandler {
    fn allowed(&self, principal: &Principal, resource: Resource) -> bool {
        self.broker
            .authorizer
            .authorize(principal, &resource, Operation::Describe)
    }
}

fn fetch_group(
    mgr: Option<&Arc<kaas_coordinator::Manager>>,
    group_id: &str,
    topics: &[offset_fetch::OffsetFetchTopic],
    may_describe_topic: &dyn Fn(&str) -> bool,
) -> (Vec<offset_fetch::OffsetFetchTopicResponse>, i16) {
    let mgr = match mgr {
        None => {
            return (
                topics.iter().map(|t| error_topic(t, 0)).collect(),
                ERR_NOT_COORDINATOR,
            )
        }
        Some(m) => m,
    };
    let specs: Vec<FetchSpec> = topics
        .iter()
        .filter(|t| may_describe_topic(&t.name))
        .map(|t| FetchSpec {
            topic: t.name.clone(),
            partitions: t.partition_indexes.clone(),
        })
        .collect();
    let (committed, metadata) = match mgr.offset_fetch(group_id, &specs) {
        None => {
            return (
                topics.iter().map(|t| error_topic(t, 0)).collect(),
                ERR_NOT_COORDINATOR,
            )
        }
        Some(pair) => pair,
    };

    let topics_resp = topics
        .iter()
        .map(|t| {
            if may_describe_topic(&t.name) {
                committed_topic(t, &committed, &metadata)
            } else {
                error_topic(t, ERR_TOPIC_AUTHZ_FAILED)
            }
        })
        .collect();
    (topics_resp, 0)
}

fn committed_topic(
    t: &offset_fetch::OffsetFetchTopic,
    committed: &HashMap<String, i64>,
    metadata: &HashMap<String, String>,
) -> offset_fetch::OffsetFetchTopicResponse {
    offset_fetch::OffsetFetchTopicResponse {
        name: t.name.clone(),
        partitions: t
            .partition_indexes
            .iter()
            .map(|&p| {
                let key = build_offset_key(&t.name, p);
                let committed_offset = *committed.get(&key).unwrap_or(&-1);
                let meta = metadata.get(&key).cloned();
                offset_fetch::OffsetFetchPartitionResponse {
                    partition_index: p,
                    committed_offset,
                    committed_leader_epoch: -1,
                    metadata: meta,
                    error_code: 0,
                }
            })
            .collect(),
    }
}

/// The requested partitions with no offset and `error_code` on each.
fn error_topic(
    t: &offset_fetch::OffsetFetchTopic,
    error_code: i16,
) -> offset_fetch::OffsetFetchTopicResponse {
    offset_fetch::OffsetFetchTopicResponse {
        name: t.name.clone(),
        partitions: t
            .partition_indexes
            .iter()
            .map(|&p| offset_fetch::OffsetFetchPartitionResponse {
                partition_index: p,
                committed_offset: -1,
                committed_leader_epoch: -1,
                metadata: None,
                error_code,
            })
            .collect(),
    }
}
