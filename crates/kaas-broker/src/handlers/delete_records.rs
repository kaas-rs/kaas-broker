//! DeleteRecords handler (key 21).
//!
//! The broker side of `kafka-delete-records.sh` / Kafbat's "Purge
//! messages": per partition, advance the log start offset to the
//! caller's target (KIP-107; `-1` = purge to HWM) so earlier records
//! become invisible to Fetch and eligible for retention cleanup.
//!
//! Gates run per partition in the same order as Produce's and Apache's
//! `handleDeleteRecordsRequest` (gh #295):
//! 1. `Delete` on the topic (29).
//! 2. The topic exists and the partition is inside its count (3).
//! 3. With a cluster [`Coordinator`] wired, this broker leads the
//!    partition (6). Without one (dev mode) it leads everything.
//!
//! [`Coordinator`]: crate::coordinator::Coordinator

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::delete_records;
use kaas_protocol::{ConnState, Handler, HandlerError};
use kaas_storage::StorageError;
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERR_UNKNOWN_TOPIC: i16 = 3;
const ERR_NOT_LEADER: i16 = 6;
const ERR_TOPIC_AUTHZ_FAILED: i16 = 29;

#[derive(Debug)]
pub struct DeleteRecordsHandler {
    broker: Arc<Broker>,
}

impl DeleteRecordsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteRecordsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_records::decode_request(&mut body, version)?;
        let principal = principal_from(conn);

        let coordinator = self.broker.coordinator();
        let mut topics = Vec::with_capacity(req.topics.len());
        for topic in &req.topics {
            // gh #199: Apache requires `Delete` on the topic — this
            // API destroys data (advances logStart past records).
            let authorized = self.broker.authorizer.authorize(
                &principal,
                &Resource::topic(&topic.name),
                Operation::Delete,
            );
            let mut partitions = Vec::with_capacity(topic.partitions.len());
            for p in &topic.partitions {
                let mut pr = delete_records::DeleteRecordsPartitionResult {
                    partition_index: p.partition_index,
                    low_watermark: -1,
                    error_code: ERR_NONE,
                };
                if !authorized {
                    pr.error_code = ERR_TOPIC_AUTHZ_FAILED;
                    partitions.push(pr);
                    continue;
                }
                // gh #295: Apache's `metadataCache.contains(tp)` — a
                // missing topic or out-of-range partition is 3, never a
                // leadership error.
                if !self
                    .broker
                    .topics
                    .get(&topic.name)
                    .is_some_and(|m| (0..m.partition_count).contains(&p.partition_index))
                {
                    pr.error_code = ERR_UNKNOWN_TOPIC;
                    partitions.push(pr);
                    continue;
                }
                if let Some(c) = coordinator.as_ref() {
                    if !c.owns(&topic.name, p.partition_index) {
                        pr.error_code = ERR_NOT_LEADER;
                        partitions.push(pr);
                        continue;
                    }
                }
                match self
                    .broker
                    .engine
                    .delete_records(&topic.name, p.partition_index, p.offset)
                    .await
                {
                    Ok(low_watermark) => pr.low_watermark = low_watermark,
                    Err(StorageError::OffsetOutOfRange) => {
                        pr.error_code = ERR_OFFSET_OUT_OF_RANGE;
                    }
                    Err(_) => pr.error_code = ERR_UNKNOWN_TOPIC,
                }
                partitions.push(pr);
            }
            topics.push(delete_records::DeleteRecordsTopicResult {
                name: topic.name.clone(),
                partitions,
            });
        }

        let resp = delete_records::Response {
            throttle_time_ms: 0,
            topics,
        };
        let mut out = BytesMut::new();
        delete_records::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_authz::DenyAllAuthorizer;
    use crate::topic_registry::TopicRegistry;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    /// gh #199: no `Delete` on the topic -> 29 per partition, and the
    /// log-start offset stays where it was.
    #[tokio::test]
    async fn denied_delete_returns_authz_failed_and_purges_nothing() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        b.engine.create_partition("t", 0).await.unwrap();
        let h = DeleteRecordsHandler::new(b.clone());

        let req = delete_records::Request {
            topics: vec![delete_records::DeleteRecordsTopic {
                name: "t".into(),
                partitions: vec![delete_records::DeleteRecordsPartition {
                    partition_index: 0,
                    offset: -1,
                }],
            }],
            timeout_ms: 1000,
        };
        let mut body = BytesMut::new();
        delete_records::encode_request(&mut body, &req, 2).unwrap();
        let out = h.handle(&conn(), 2, body.freeze()).await.unwrap();
        let mut r = out.freeze();
        let resp = delete_records::decode_response(&mut r, 2).unwrap();

        let pr = &resp.topics[0].partitions[0];
        assert_eq!(pr.error_code, ERR_TOPIC_AUTHZ_FAILED);
        assert_eq!(pr.low_watermark, -1, "no purge may have happened");
    }

    /// gh #295: with the Delete ACL granted, Apache's order is existence
    /// and partition range (3), then leadership (6). A coordinator is
    /// installed so the leadership gate runs rather than dev mode's
    /// "lead everything": `t-0` is a peer's, `t-1` is ours.
    #[tokio::test]
    async fn existence_and_range_before_leadership() {
        use crate::handlers::test_cluster::{self, PEER, SELF};
        use crate::topic_registry::TopicMeta;
        let tmp = tempfile::tempdir().unwrap();
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 2,
            topic_id: [0; 16],
        });
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(engine, topics, "test", 0));
        b.engine.create_partition("t", 1).await.unwrap();
        test_cluster::install(&b, tmp.path(), &[("t", 0, PEER), ("t", 1, SELF)]);

        let req = delete_records::Request {
            topics: [("t", vec![0, 1, 2, 999]), ("missing", vec![0])]
                .into_iter()
                .map(|(name, parts)| delete_records::DeleteRecordsTopic {
                    name: name.into(),
                    partitions: parts
                        .into_iter()
                        .map(|partition_index| delete_records::DeleteRecordsPartition {
                            partition_index,
                            offset: -1,
                        })
                        .collect(),
                })
                .collect(),
            timeout_ms: 1000,
        };
        let mut body = BytesMut::new();
        delete_records::encode_request(&mut body, &req, 2).unwrap();
        let h = DeleteRecordsHandler::new(b);
        let out = h.handle(&conn(), 2, body.freeze()).await.unwrap();
        let resp = delete_records::decode_response(&mut out.freeze(), 2).unwrap();
        let codes: Vec<(&str, i32, i16)> = resp
            .topics
            .iter()
            .flat_map(|t| {
                t.partitions
                    .iter()
                    .map(move |p| (t.name.as_str(), p.partition_index, p.error_code))
            })
            .collect();
        assert_eq!(
            codes,
            [
                ("t", 0, 6),
                ("t", 1, 0),
                ("t", 2, 3),
                ("t", 999, 3),
                ("missing", 0, 3)
            ]
        );
    }
}
