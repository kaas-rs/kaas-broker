//! DescribeLogDirs handler (key 35).
//!
//! One log dir per kaas broker (the engine's data dir); one topic
//! entry per requested topic — or every known topic when the request
//! carries a null filter. Partition sizes come straight from
//! [`StorageEngine::partition_size`].
//!
//! Authorization (gh #293): `Describe` on the cluster, as in Apache's
//! `handleDescribeLogDirsRequest`. Without it the response carries no
//! results and, from v3, the top-level `CLUSTER_AUTHORIZATION_FAILED`
//! (31); v0–v2 have no top-level error field, so a denied client sees an
//! empty list, as on Apache.
//!
//! [`StorageEngine::partition_size`]: kaas_storage::StorageEngine::partition_size

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::describe_log_dirs;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;

#[derive(Debug)]
pub struct DescribeLogDirsHandler {
    broker: Arc<Broker>,
}

impl DescribeLogDirsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    /// Resolve the request's topic/partition filter against the
    /// registry: null topics → every known topic, every partition; a
    /// named topic → exactly its listed partitions, so an empty list
    /// contributes nothing (Apache builds the set from the listed
    /// indexes, gh #298); unknown topics are dropped.
    fn wanted(&self, req: &describe_log_dirs::Request) -> Vec<(String, Vec<i32>)> {
        match req.topics.as_ref() {
            None => self
                .broker
                .topics
                .all()
                .into_iter()
                .map(|t| (t.name.clone(), (0..t.partition_count).collect()))
                .collect(),
            Some(topics) => topics
                .iter()
                .filter(|t| self.broker.topics.get(&t.name).is_some())
                .map(|t| (t.name.clone(), t.partitions.clone()))
                .collect(),
        }
    }
}

#[async_trait]
impl Handler for DescribeLogDirsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_log_dirs::decode_request(&mut body, version)?;

        if !self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::cluster(),
            Operation::Describe,
        ) {
            let resp = describe_log_dirs::Response {
                throttle_time_ms: 0,
                error_code: ERR_CLUSTER_AUTHZ_FAILED,
                results: Vec::new(),
            };
            let mut out = BytesMut::new();
            describe_log_dirs::encode_response(&mut out, &resp, version)?;
            return Ok(out);
        }

        // gh #221 phase 2: one LogDirResult per log dir (pool
        // member), every member reported even when empty — Kafka
        // reports all configured log.dirs. Partitions group under
        // the dir the placement resolver assigns them to.
        let wanted = self.wanted(&req);
        // Resolved once per partition: finding where the data is stats the
        // volumes, and every dir below asks.
        let located: HashMap<(&str, i32), String> = wanted
            .iter()
            .flat_map(|(name, parts)| parts.iter().map(move |p| (name.as_str(), *p)))
            .map(|(name, p)| ((name, p), self.broker.engine.partition_log_dir(name, p)))
            .collect();
        let results = self
            .broker
            .engine
            .log_dirs()
            .into_iter()
            .map(|dir| {
                // KIP-827 (v4+): per-dir filesystem capacity. The
                // codec drops the fields below v4; (-1, -1) is the
                // "unknown" sentinel (e.g. dev-mode memory://).
                let (total_bytes, usable_bytes) = kaas_storage::fs_capacity(&dir.path);
                let topics = wanted
                    .iter()
                    .filter_map(|(name, parts)| {
                        let partitions: Vec<_> = parts
                            .iter()
                            .filter(|p| located.get(&(name.as_str(), **p)) == Some(&dir.name))
                            .map(|p| describe_log_dirs::ResponsePartition {
                                partition_index: *p,
                                partition_size: self.broker.engine.partition_size(name, *p),
                                offset_lag: 0,
                                is_future_key: false,
                            })
                            .collect();
                        if partitions.is_empty() {
                            return None;
                        }
                        Some(describe_log_dirs::ResponseTopic {
                            name: name.clone(),
                            partitions,
                        })
                    })
                    .collect();
                describe_log_dirs::LogDirResult {
                    error_code: 0,
                    log_dir: dir.path.display().to_string(),
                    topics,
                    total_bytes,
                    usable_bytes,
                }
            })
            .collect();

        let resp = describe_log_dirs::Response {
            throttle_time_ms: 0,
            error_code: 0,
            results,
        };
        let mut out = BytesMut::new();
        describe_log_dirs::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::test_authz::{DenyAllAuthorizer, DenyOpAuthorizer};
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_auth::Authorizer;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    async fn describe(
        authorizer: Arc<dyn Authorizer>,
        version: i16,
    ) -> describe_log_dirs::Response {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let broker = Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            authorizer,
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        let mut body = BytesMut::new();
        describe_log_dirs::encode_request(
            &mut body,
            &describe_log_dirs::Request::default(),
            version,
        )
        .unwrap();
        let conn = Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ));
        let out = DescribeLogDirsHandler::new(broker)
            .handle(&conn, version, body.freeze())
            .await
            .unwrap();
        describe_log_dirs::decode_response(&mut out.freeze(), version).unwrap()
    }

    /// gh #293: listing log dirs, placements and sizes needs `Describe`
    /// on the cluster. Denied: no results and, from v3, Apache's literal
    /// 31 at the top level; v0–v2 have no field to carry it.
    #[tokio::test]
    async fn denied_describe_lists_nothing() {
        let authorizers: [Arc<dyn Authorizer>; 2] = [
            Arc::new(DenyAllAuthorizer),
            Arc::new(DenyOpAuthorizer(Operation::Describe)),
        ];
        for authorizer in authorizers {
            let resp = describe(authorizer.clone(), 4).await;
            assert_eq!(resp.error_code, 31);
            assert!(resp.results.is_empty(), "log dirs leaked");
            let resp = describe(authorizer, 1).await;
            assert!(resp.results.is_empty(), "log dirs leaked at v1");
        }
    }

    /// Control: `Describe` alone is enough.
    #[tokio::test]
    async fn describe_on_the_cluster_lists_the_dirs() {
        let resp = describe(Arc::new(DenyOpAuthorizer(Operation::Alter)), 4).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].topics[0].name, "t");
    }

    /// gh #298: only a null topics array means "everything". A named topic
    /// contributes exactly its listed partitions, so an empty list
    /// contributes nothing, as in Apache.
    #[tokio::test]
    async fn a_topic_with_an_empty_partition_list_contributes_nothing() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 3,
            topic_id: [0; 16],
        });
        let h = DescribeLogDirsHandler::new(Arc::new(Broker::new(engine, topics, "test", 0)));
        let listed = |req: describe_log_dirs::Request| {
            let h = &h;
            async move {
                let mut body = BytesMut::new();
                describe_log_dirs::encode_request(&mut body, &req, 4).unwrap();
                let conn = Mutex::new(ConnState::new(
                    "internal",
                    SocketAddr::from_str("127.0.0.1:9092").unwrap(),
                ));
                let out = h.handle(&conn, 4, body.freeze()).await.unwrap();
                let resp = describe_log_dirs::decode_response(&mut out.freeze(), 4).unwrap();
                resp.results[0]
                    .topics
                    .iter()
                    .flat_map(|t| t.partitions.iter().map(|p| p.partition_index))
                    .collect::<Vec<i32>>()
            }
        };
        let named = |partitions: Vec<i32>| describe_log_dirs::Request {
            topics: Some(vec![describe_log_dirs::RequestTopic {
                name: "t".to_owned(),
                partitions,
            }]),
        };
        assert_eq!(listed(named(vec![])).await, Vec::<i32>::new());
        assert_eq!(listed(named(vec![1])).await, [1]);
        assert_eq!(
            listed(describe_log_dirs::Request { topics: None }).await,
            [0, 1, 2]
        );
    }
}
