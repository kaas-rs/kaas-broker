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

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_codec::api::offset_fetch;
use kaas_coordinator::{build_offset_key, FetchSpec};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;

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
        _conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = offset_fetch::decode_request(&mut body, version)?;
        let mgr = self.broker.coord_manager();

        let resp = if version >= 8 {
            let mut groups_resp =
                Vec::<offset_fetch::OffsetFetchGroupResponse>::with_capacity(req.groups.len());
            for g in &req.groups {
                let (topics_resp, group_err) = fetch_group(mgr.as_ref(), &g.group_id, &g.topics);
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
        } else {
            let topics_in = req.topics.clone().unwrap_or_default();
            let (topics_resp, group_err) = fetch_group(mgr.as_ref(), &req.group_id, &topics_in);
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

fn fetch_group(
    mgr: Option<&Arc<kaas_coordinator::Manager>>,
    group_id: &str,
    topics: &[offset_fetch::OffsetFetchTopic],
) -> (Vec<offset_fetch::OffsetFetchTopicResponse>, i16) {
    let mgr = match mgr {
        None => return (empty_topics(topics), ERR_NOT_COORDINATOR),
        Some(m) => m,
    };
    let specs: Vec<FetchSpec> = topics
        .iter()
        .map(|t| FetchSpec {
            topic: t.name.clone(),
            partitions: t.partition_indexes.clone(),
        })
        .collect();
    let (committed, metadata) = match mgr.offset_fetch(group_id, &specs) {
        None => return (empty_topics(topics), ERR_NOT_COORDINATOR),
        Some(pair) => pair,
    };

    let topics_resp = topics
        .iter()
        .map(|t| offset_fetch::OffsetFetchTopicResponse {
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
        })
        .collect();
    (topics_resp, 0)
}

fn empty_topics(
    topics: &[offset_fetch::OffsetFetchTopic],
) -> Vec<offset_fetch::OffsetFetchTopicResponse> {
    topics
        .iter()
        .map(|t| offset_fetch::OffsetFetchTopicResponse {
            name: t.name.clone(),
            partitions: t
                .partition_indexes
                .iter()
                .map(|&p| offset_fetch::OffsetFetchPartitionResponse {
                    partition_index: p,
                    committed_offset: -1,
                    committed_leader_epoch: -1,
                    metadata: None,
                    error_code: 0,
                })
                .collect(),
        })
        .collect()
}
