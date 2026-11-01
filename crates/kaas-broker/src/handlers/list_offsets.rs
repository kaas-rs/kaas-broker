//! ListOffsets handler (key 2).
//!
//! Translates special timestamps to engine queries:
//! - `-2` (EARLIEST) → `log_start_offset`
//! - `-1` (LATEST)   → the last fetchable offset: `last_stable_offset`
//!   for a `read_committed` client, `high_watermark` otherwise (gh #296)
//! - otherwise       → `engine.offset_for_timestamp(ts)`
//!
//! Gates run per partition, as in Apache's `handleListOffsetRequest` and
//! `fetchOffsetForTimestamp` (gh #295):
//! 1. `Describe` on the topic (29).
//! 2. The topic exists and the partition is inside its count (3).
//! 3. With a cluster `Coordinator` wired, this broker leads the
//!    partition (6). The exception is `replica_id = -2`, Apache's
//!    debugging replica, which any broker may answer. A non-leader's
//!    engine has no open partition, so before this it answered 0 for
//!    both offsets instead of redirecting the client.

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
const ERR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;

const TS_EARLIEST: i64 = -2;
const TS_LATEST: i64 = -1;
/// `ListOffsetsRequest.DEBUGGING_REPLICA_ID`: answered from any broker.
const DEBUGGING_REPLICA_ID: i32 = -2;
/// `IsolationLevel.READ_COMMITTED` (request v2+).
const READ_COMMITTED: i8 = 1;

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
                    self.resolve_one(&t.name, p, &req)
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
        req: &list_offsets::Request,
    ) -> list_offsets::PartitionResponse {
        if !self
            .broker
            .topics
            .get(topic)
            .is_some_and(|m| (0..m.partition_count).contains(&p.partition_index))
        {
            return error_partition(p.partition_index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
        }
        if req.replica_id != DEBUGGING_REPLICA_ID {
            if let Some(c) = self.broker.coordinator() {
                if !c.owns(topic, p.partition_index) {
                    return error_partition(p.partition_index, ERR_NOT_LEADER_OR_FOLLOWER);
                }
            }
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
            // Apache's `fetchOffsetForTimestamp`: LATEST is the last
            // offset this client may fetch. A read_committed client must
            // not be pointed past an open transaction's first record.
            TS_LATEST => {
                let engine = &self.broker.engine;
                let off = if req.isolation_level == READ_COMMITTED {
                    engine.last_stable_offset(topic, p.partition_index)
                } else {
                    engine.high_watermark(topic, p.partition_index)
                };
                (-1, off.unwrap_or(0))
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

    /// A whole v6 request (flexible), for multi-partition shapes.
    fn encode_v6(req: &list_offsets::Request) -> Bytes {
        let mut w = BytesMut::new();
        write_i32(&mut w, req.replica_id);
        write_i8(&mut w, req.isolation_level);
        write_array_len(&mut w, req.topics.len(), true).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, true).unwrap();
            write_array_len(&mut w, t.partitions.len(), true).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, p.partition_index);
                write_i32(&mut w, p.current_leader_epoch);
                write_i64(&mut w, p.timestamp);
                tagged::write_empty(&mut w);
            }
            tagged::write_empty(&mut w);
        }
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

    /// gh #295: after the Describe ACL, Apache's order is existence and
    /// partition range (3), then leadership (6) unless `replica_id` is
    /// the debugging replica (-2). A coordinator is installed so the
    /// leadership gate runs: `t-0` is a peer's, `t-1` is ours.
    #[tokio::test]
    async fn existence_and_range_before_leadership() {
        use crate::handlers::test_cluster::{self, PEER, SELF};
        let tmp = tempfile::tempdir().unwrap();
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 2,
            topic_id: [0; 16],
        });
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(engine, topics, "test", 0));
        test_cluster::install(&b, tmp.path(), &[("t", 0, PEER), ("t", 1, SELF)]);
        let h = ListOffsetsHandler::new(b);

        let codes = |replica_id: i32| {
            let req = list_offsets::Request {
                replica_id,
                isolation_level: 0,
                topics: [("t", vec![0, 1, 2, 999]), ("missing", vec![0])]
                    .into_iter()
                    .map(|(name, parts)| list_offsets::Topic {
                        name: name.to_owned(),
                        partitions: parts
                            .into_iter()
                            .map(|partition_index| list_offsets::Partition {
                                partition_index,
                                current_leader_epoch: -1,
                                timestamp: TS_LATEST,
                            })
                            .collect(),
                    })
                    .collect(),
            };
            let body = encode_v6(&req);
            let h = &h;
            async move {
                let out = h.handle(&conn(), 6, body).await.unwrap();
                let resp = list_offsets::decode_response(&mut out.freeze(), 6).unwrap();
                resp.topics
                    .iter()
                    .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
                    .collect::<Vec<_>>()
            }
        };
        // A consumer (replica_id -1): t-0 [peer], t-1 [ours], t-2 and
        // t-999 out of range, a missing topic.
        assert_eq!(codes(-1).await, [6, 0, 3, 3, 3]);
        // The debugging replica may ask any broker, but never for a
        // partition that doesn't exist.
        assert_eq!(codes(DEBUGGING_REPLICA_ID).await, [0, 0, 3, 3, 3]);
    }

    /// gh #296: LATEST is the last fetchable offset. An open transaction
    /// holds the LSO at its first record (offset 1 here, after one plain
    /// batch), so read_committed gets 1 while read_uncommitted gets the
    /// high watermark, 2.
    #[tokio::test]
    async fn latest_is_the_lso_for_read_committed() {
        use kaas_storage::{DiskStorageEngine, Fs, PartitionConfig, RealFs};
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
            fs,
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
        engine.create_partition("t", 0).await.unwrap();
        // One plain batch, then a transactional data batch left open.
        for (attrs, pid, seq) in [(0i16, -1i64, -1i32), (0x10, 7, 0)] {
            let mut b = crate::control_batch::build_control_batch(pid, 0, true, 0);
            b[21..23].copy_from_slice(&attrs.to_be_bytes());
            b[53..57].copy_from_slice(&seq.to_be_bytes());
            let crc = kaas_codec::crc::compute(&b[21..]);
            b[17..21].copy_from_slice(&crc.to_be_bytes());
            engine.append("t", 0, 0, -1, Bytes::from(b)).await.unwrap();
        }
        assert_eq!(engine.high_watermark("t", 0).unwrap(), 2);
        assert_eq!(engine.last_stable_offset("t", 0).unwrap(), 1);

        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let h = ListOffsetsHandler::new(Arc::new(Broker::new(engine, topics, "test", 0)));
        for (isolation_level, want) in [(0, 2), (READ_COMMITTED, 1)] {
            let req = list_offsets::Request {
                replica_id: -1,
                isolation_level,
                topics: vec![list_offsets::Topic {
                    name: "t".to_owned(),
                    partitions: vec![list_offsets::Partition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp: TS_LATEST,
                    }],
                }],
            };
            let out = h.handle(&conn(), 6, encode_v6(&req)).await.unwrap();
            let resp = list_offsets::decode_response(&mut out.freeze(), 6).unwrap();
            let p = &resp.topics[0].partitions[0];
            assert_eq!(
                (p.error_code, p.offset),
                (0, want),
                "isolation {isolation_level}"
            );
        }
    }
}
