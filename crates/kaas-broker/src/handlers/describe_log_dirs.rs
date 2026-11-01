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
    /// registry: null topics → every known topic, every partition;
    /// a named topic with an empty partition list → every partition
    /// of that topic; unknown topics are dropped.
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
                .filter_map(|t| {
                    let known = self.broker.topics.get(&t.name)?;
                    let parts = if t.partitions.is_empty() {
                        (0..known.partition_count).collect()
                    } else {
                        t.partitions.clone()
                    };
                    Some((t.name.clone(), parts))
                })
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
                            .filter(|p| self.broker.engine.partition_log_dir(name, **p) == dir.name)
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
}
