//! ListOffsets handler (key 2).
//!
//! Translates special timestamps to engine queries:
//! - `-2` (EARLIEST) → `log_start_offset`
//! - `-1` (LATEST)   → `high_watermark`
//! - otherwise       → `engine.offset_for_timestamp(ts)`

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::list_offsets;
use kaas_protocol::{ConnState, Handler, HandlerError};
use kaas_storage::StorageError;
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;

const TS_EARLIEST: i64 = -2;
const TS_LATEST: i64 = -1;

#[derive(Debug)]
pub struct ListOffsetsHandler {
    broker: Arc<Broker>,
}

impl ListOffsetsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for ListOffsetsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = list_offsets::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        let mut topics = Vec::with_capacity(req.topics.len());
        for t in &req.topics {
            // gh #268: `Describe` on the topic, ahead of the existence
            // check so a denied principal can't probe for topic names.
            let authorized = self.broker.authorizer.authorize(
                &principal,
                &Resource::topic(&t.name),
                Operation::Describe,
            );
            let mut parts = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                parts.push(if authorized {
                    self.resolve_one(&t.name, p)
                } else {
                    error_partition(p.partition_index, ERR_TOPIC_AUTHZ_FAILED)
                });
            }
            topics.push(list_offsets::TopicResponse {
                name: t.name.clone(),
                partitions: parts,
            });
        }

        let resp = list_offsets::Response {
            throttle_time_ms: 0,
            topics,
        };
        let mut out = BytesMut::new();
        list_offsets::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl ListOffsetsHandler {
    fn resolve_one(
        &self,
        topic: &str,
        p: &list_offsets::Partition,
    ) -> list_offsets::PartitionResponse {
        if self.broker.topics.get(topic).is_none() {
            return error_partition(p.partition_index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        }
        let (timestamp, offset) = match p.timestamp {
            TS_EARLIEST => {
                let off = self
                    .broker
                    .engine
                    .log_start_offset(topic, p.partition_index)
                    .unwrap_or(0);
                (-1, off)
            }
            TS_LATEST => {
                let off = self
                    .broker
                    .engine
                    .high_watermark(topic, p.partition_index)
                    .unwrap_or(0);
                (-1, off)
            }
            ts => match self
                .broker
                .engine
                .offset_for_timestamp(topic, p.partition_index, ts)
            {
                Ok((off, matched_ts)) => (matched_ts, off),
                Err(StorageError::UnknownTopicOrPartition) => {
                    return error_partition(p.partition_index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
                }
                Err(_) => (-1, -1),
            },
        };
        list_offsets::PartitionResponse {
            partition_index: p.partition_index,
            error_code: 0,
            timestamp,
            offset,
            leader_epoch: 0,
        }
    }
}

fn error_partition(partition_index: i32, error_code: i16) -> list_offsets::PartitionResponse {
    list_offsets::PartitionResponse {
        partition_index,
        error_code,
        timestamp: -1,
        offset: -1,
        leader_epoch: -1,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::common::{write_array_len, write_str};
    use kaas_codec::primitives::{write_i32, write_i64, write_i8};
    use kaas_codec::tagged;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker_with_topic(name: &str) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: name.to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    fn encode_request_v6(topic: &str, timestamp: i64) -> Bytes {
        let flexible = true; // v6 is flexible
        let mut w = BytesMut::new();
        write_i32(&mut w, -1); // replica_id
        write_i8(&mut w, 0); // isolation_level
        write_array_len(&mut w, 1, flexible).unwrap();
        write_str(&mut w, topic, flexible).unwrap();
        write_array_len(&mut w, 1, flexible).unwrap();
        write_i32(&mut w, 0); // partition_index
        write_i32(&mut w, -1); // current_leader_epoch
        write_i64(&mut w, timestamp);
        tagged::write_empty(&mut w);
        tagged::write_empty(&mut w);
        tagged::write_empty(&mut w);
        w.freeze()
    }

    #[tokio::test]
    async fn latest_returns_high_watermark() {
        let h = ListOffsetsHandler::new(broker_with_topic("t"));
        let body = encode_request_v6("t", TS_LATEST);
        let out = h.handle(&conn(), 6, body).await.unwrap();
        let mut r = out.freeze();
        let resp = list_offsets::decode_response(&mut r, 6).unwrap();
        let p = &resp.topics[0].partitions[0];
        assert_eq!(p.error_code, 0);
        assert_eq!(p.timestamp, -1);
        assert_eq!(p.offset, 0); // empty partition
    }

    #[tokio::test]
    async fn earliest_returns_log_start_offset() {
        let h = ListOffsetsHandler::new(broker_with_topic("t"));
        let body = encode_request_v6("t", TS_EARLIEST);
        let out = h.handle(&conn(), 6, body).await.unwrap();
        let mut r = out.freeze();
        let resp = list_offsets::decode_response(&mut r, 6).unwrap();
        assert_eq!(resp.topics[0].partitions[0].offset, 0);
    }

    #[tokio::test]
    async fn topic_without_describe_returns_29_existing_or_not() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let h = ListOffsetsHandler::new(Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        )));
        for topic in ["t", "other"] {
            let out = h
                .handle(&conn(), 6, encode_request_v6(topic, TS_LATEST))
                .await
                .unwrap();
            let resp = list_offsets::decode_response(&mut out.freeze(), 6).unwrap();
            let p = &resp.topics[0].partitions[0];
            assert_eq!(p.error_code, ERR_TOPIC_AUTHZ_FAILED, "topic {topic}");
            assert_eq!(p.offset, -1, "topic {topic}");
        }
    }

    #[tokio::test]
    async fn unknown_topic_returns_3() {
        let h = ListOffsetsHandler::new(broker_with_topic("t"));
        let body = encode_request_v6("other", TS_LATEST);
        let out = h.handle(&conn(), 6, body).await.unwrap();
        let mut r = out.freeze();
        let resp = list_offsets::decode_response(&mut r, 6).unwrap();
        assert_eq!(resp.topics[0].partitions[0].error_code, 3);
    }
}
