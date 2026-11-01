//! Produce handler (key 0).
//!
//! Iterates `req.topic_data`, calls `engine.append` per partition,
//! maps `StorageError` variants to wire error codes. Records bytes
//! flow through as opaque `Option<Bytes>` — no decode, no re-encode.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::produce;
use kaas_protocol::{ConnState, Handler, HandlerError, Reply};
use kaas_storage::StorageError;
use parking_lot::Mutex;

use crate::broker::Broker;

// Wire error codes (subset). Same numeric values Apache uses.
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_NOT_LEADER_FOR_PARTITION: i16 = 6;
/// Apache's `MESSAGE_TOO_LARGE` (`RecordTooLargeException`): a batch over
/// `max.message.bytes`. Until gh #285 this was 18, `RECORD_LIST_TOO_LARGE`,
/// which the Java producer doesn't answer by splitting the batch.
const ERR_MESSAGE_TOO_LARGE: i16 = 10;
/// Mirrors Apache Kafka's `message.max.bytes` default: 1 MiB of
/// records plus 12 bytes of batch overhead (gh #14).
/// Apache's `message.max.bytes` default (1 MiB + batch overhead).
/// The production wiring overrides via [`ProduceHandler::with_max_message_bytes`]
/// from `KAAS_MAX_MESSAGE_BYTES` (gh #253).
const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_048_588;
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERR_OUT_OF_ORDER_SEQUENCE_NUMBER: i16 = 45;
const ERR_DUPLICATE_SEQUENCE_NUMBER: i16 = 46;
const ERR_INVALID_PRODUCER_EPOCH: i16 = 47;
const ERR_GENERIC: i16 = -1; // UNKNOWN_SERVER_ERROR
/// gh #168: what Apache answers when the log dir behind a partition
/// fails (ENOSPC, EIO). Retriable on the client (triggers a metadata
/// refresh) but named — a full disk no longer reads as "unknown
/// server error".
const ERR_KAFKA_STORAGE_ERROR: i16 = 56;

#[derive(Debug)]
pub struct ProduceHandler {
    broker: Arc<Broker>,
    max_message_bytes: usize,
}

impl ProduceHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self {
            broker,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
    }

    /// gh #253: broker-wide `message.max.bytes`. Was a hard-coded
    /// constant from the Rust cutover (the Go broker read
    /// `SKAFKA_MAX_MESSAGE_BYTES`, gh #14) — >1 MiB batches had no
    /// knob at all.
    pub fn with_max_message_bytes(mut self, max: usize) -> Self {
        self.max_message_bytes = max;
        self
    }
}

#[async_trait]
impl Handler for ProduceHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = produce::decode_request(&mut body, version)?;

        let acks = req.acks;
        let principal = conn
            .lock()
            .principal
            .clone()
            .unwrap_or_else(Principal::anonymous);

        // Cumulative byte count for the per-principal produce quota.
        let total_bytes: usize = req
            .topic_data
            .iter()
            .flat_map(|t| t.partition_data.iter())
            .filter_map(|p| p.records.as_ref().map(|b| b.len()))
            .sum();

        let mut responses = Vec::with_capacity(req.topic_data.len());
        for t in &req.topic_data {
            let mut partition_responses = Vec::with_capacity(t.partition_data.len());
            for p in &t.partition_data {
                let pr = self.append_one(&principal, &t.name, p, acks).await;
                partition_responses.push(pr);
            }
            responses.push(produce::TopicResponse {
                name: t.name.clone(),
                partition_responses,
            });
        }

        let throttle_time_ms = self
            .broker
            .quotas
            .check_produce_quota(&principal, total_bytes);

        // gh #200: an acks=0 producer reads no response, so Apache sends
        // none — and when a partition failed, closes the connection
        // instead, the only signal such a producer gets to refresh its
        // metadata (KafkaApis.handleProduceRequest). The body is still
        // built: the quota above is recorded either way, as in Apache.
        if acks == 0 {
            let failed = responses
                .iter()
                .flat_map(|t| &t.partition_responses)
                .any(|p| p.error_code != 0);
            conn.lock().reply = if failed { Reply::Close } else { Reply::None };
        }

        let resp = produce::Response {
            responses,
            throttle_time_ms,
        };
        let mut out = BytesMut::new();
        produce::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl ProduceHandler {
    async fn append_one(
        &self,
        principal: &Principal,
        topic: &str,
        p: &produce::PartitionData,
        acks: i16,
    ) -> produce::PartitionResponse {
        // Cluster-wide ACL check (gh #126). Topic-level Write is the
        // canonical Apache mapping for Produce. First, so a principal
        // without rights learns nothing about the topic — not whether it
        // exists, not who leads it (gh #282, as gh #268 did for the
        // discovery APIs).
        let resource = Resource::topic(topic);
        if !self
            .broker
            .authorizer
            .authorize(principal, &resource, Operation::Write)
        {
            return error_partition_bumped(topic, p.index, ERR_TOPIC_AUTHORIZATION_FAILED);
        }

        // Unknown topic, or a partition past the topic's count: Apache's
        // UNKNOWN_TOPIC_OR_PARTITION, not a leadership error.
        match self.broker.topics.get(topic) {
            Some(meta) if (0..meta.partition_count).contains(&p.index) => {}
            _ => {
                return error_partition_bumped(topic, p.index, ERR_UNKNOWN_TOPIC_OR_PARTITION);
            }
        }

        // Phase 5 cluster check: a real `Coordinator` answers "do I
        // lead this partition?" against the most recently applied
        // assignment.json. Dev mode (no coordinator wired) keeps the
        // `LocalLeaseManager` "always lead" path.
        if let Some(c) = self.broker.coordinator() {
            if !c.owns(topic, p.index) {
                return error_partition_bumped(topic, p.index, ERR_NOT_LEADER_FOR_PARTITION);
            }
            // gh #62 self-fence: a broker cut off from the controller
            // stops acking within 3 s even when its assignment.json
            // view is stale — the bound on how long a partitioned
            // ex-leader keeps accepting writes. No-op unless the
            // cluster runtime armed it (enable_self_fence).
            if !c.heartbeat_fresh_for_writes() {
                let m = kaas_observability::metrics::global();
                m.heartbeat_misses.add(1, &[]);
                m.self_fence_events.add(1, &[]);
                return error_partition_bumped(topic, p.index, ERR_NOT_LEADER_FOR_PARTITION);
            }
        }

        let Some(records) = p.records.clone() else {
            // Null records is a no-op produce per Apache. Echo the
            // current HWM so the client's offset accounting stays
            // sane.
            let base = self
                .broker
                .engine
                .high_watermark(topic, p.index)
                .unwrap_or(0);
            return ok_partition(p.index, base, 0);
        };

        // gh #14: enforce broker-side max.message.bytes. Java
        // producers cap their own batches at max.request.size (1 MB
        // default) but misconfigured clients can still send bigger;
        // without this check the storage engine accepts arbitrarily
        // large batches and trips MaxFetchBytes loops at consume
        // time. Cap matches Apache's `message.max.bytes` default
        // (1 MiB + batch overhead).
        if records.len() > self.max_message_bytes {
            return error_partition_bumped(topic, p.index, ERR_MESSAGE_TOO_LARGE);
        }

        // Ensure the partition is open (create_partition is idempotent
        // on the engine side).
        if let Err(err) = self.broker.engine.create_partition(topic, p.index).await {
            tracing::warn!(%err, topic, partition = p.index, "create_partition failed");
            return map_error(topic, p.index, &err);
        }

        // Real cluster epoch from `Coordinator::current_epoch` when
        // wired; LocalLeaseManager (epoch 0) otherwise.
        let epoch = self
            .broker
            .coordinator()
            .and_then(|c| c.current_epoch(topic, p.index))
            .unwrap_or_else(|| self.broker.local_lease.current_epoch());
        match self
            .broker
            .engine
            .append(topic, p.index, epoch, acks, records)
            .await
        {
            Ok(base_offset) => {
                let log_start = self
                    .broker
                    .engine
                    .log_start_offset(topic, p.index)
                    .unwrap_or(0);
                ok_partition(p.index, base_offset, log_start)
            }
            Err(err) => {
                tracing::warn!(%err, topic, partition = p.index, "append failed");
                map_error(topic, p.index, &err)
            }
        }
    }
}

fn ok_partition(index: i32, base_offset: i64, log_start_offset: i64) -> produce::PartitionResponse {
    produce::PartitionResponse {
        index,
        error_code: 0,
        base_offset,
        log_append_time_ms: -1,
        log_start_offset,
        record_errors: Vec::new(),
        error_message: None,
    }
}

fn error_partition(index: i32, error_code: i16) -> produce::PartitionResponse {
    produce::PartitionResponse {
        index,
        error_code,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: Vec::new(),
        error_message: None,
    }
}

/// Same shape as `error_partition` but also bumps
/// `kaas.produce.errors` labelled by topic + error_code. Callers
/// that don't know the topic (unlikely) can still reach for
/// `error_partition` directly.
fn error_partition_bumped(topic: &str, index: i32, error_code: i16) -> produce::PartitionResponse {
    kaas_observability::metrics::global().produce_errors.add(
        1,
        &[
            kaas_observability::KeyValue::new("topic", topic.to_string()),
            kaas_observability::KeyValue::new("error_code", i64::from(error_code)),
        ],
    );
    error_partition(index, error_code)
}

fn map_error(topic: &str, index: i32, err: &StorageError) -> produce::PartitionResponse {
    let code = match err {
        StorageError::EpochMismatch => ERR_NOT_LEADER_FOR_PARTITION,
        StorageError::OutOfOrderSequence => ERR_OUT_OF_ORDER_SEQUENCE_NUMBER,
        StorageError::DuplicateSequence => ERR_DUPLICATE_SEQUENCE_NUMBER,
        StorageError::InvalidProducerEpoch => ERR_INVALID_PRODUCER_EPOCH,
        StorageError::UnknownTopicOrPartition => ERR_UNKNOWN_TOPIC_OR_PARTITION,
        StorageError::Stalled => ERR_LEADER_NOT_AVAILABLE,
        // Mid-migration (gh #221 phase 3): retriable; the client
        // refreshes metadata and retries after the cutover.
        StorageError::Migrating => ERR_LEADER_NOT_AVAILABLE,
        // gh #241: the topic dir is still the previous incarnation's
        // and the operator has not reclaimed it. Same retriable shape —
        // far better than accepting the write into a directory that is
        // about to be renamed out from under us.
        StorageError::StaleTopicDir => ERR_LEADER_NOT_AVAILABLE,
        // gh #168: a raw filesystem failure on the append path (ENOSPC,
        // EIO) is Apache's KAFKA_STORAGE_ERROR, not an anonymous -1.
        StorageError::Io(_) => ERR_KAFKA_STORAGE_ERROR,
        _ => ERR_GENERIC,
    };
    error_partition_bumped(topic, index, code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_codec::api::common::{
        write_array_len, write_nullable_bytes, write_nullable_str, write_str,
    };
    use kaas_codec::primitives::{write_i16, write_i32};
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

    fn broker_with_topic(name: &str, partitions: i32) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: name.to_owned(),
            partition_count: partitions,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    fn encode_request(req: &produce::Request, version: i16) -> Bytes {
        let flexible = version >= produce::MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        if version >= 3 {
            write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        }
        write_i16(&mut w, req.acks);
        write_i32(&mut w, req.timeout_ms);
        write_array_len(&mut w, req.topic_data.len(), flexible).unwrap();
        for t in &req.topic_data {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partition_data.len(), flexible).unwrap();
            for p in &t.partition_data {
                write_i32(&mut w, p.index);
                write_nullable_bytes(&mut w, p.records.as_deref(), flexible).unwrap();
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w.freeze()
    }

    #[tokio::test]
    async fn unknown_topic_returns_error_code_3() {
        let h = ProduceHandler::new(broker_with_topic("known", 1));
        let req = produce::Request {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1000,
            topic_data: vec![produce::TopicData {
                name: "unknown".to_owned(),
                partition_data: vec![produce::PartitionData {
                    index: 0,
                    records: Some(Bytes::from_static(&[0; 64])),
                }],
            }],
        };
        let body = encode_request(&req, 9);
        let out = h.handle(&conn(), 9, body).await.unwrap();
        let mut r = out.freeze();
        let resp = produce::decode_response(&mut r, 9).unwrap();
        assert_eq!(resp.responses[0].partition_responses[0].error_code, 3);
    }

    /// gh #200: the reply directive the request loop acts on.
    #[tokio::test]
    async fn only_acks_zero_withholds_the_response() {
        let h = ProduceHandler::new(broker_with_topic("known", 1));
        let req = |acks| produce::Request {
            transactional_id: None,
            acks,
            timeout_ms: 1000,
            topic_data: vec![produce::TopicData {
                name: "unknown".to_owned(),
                partition_data: vec![produce::PartitionData {
                    index: 0,
                    records: Some(Bytes::from_static(&[0; 64])),
                }],
            }],
        };
        for (acks, want) in [(0, Reply::Close), (1, Reply::Send), (-1, Reply::Send)] {
            let c = conn();
            h.handle(&c, 9, encode_request(&req(acks), 9))
                .await
                .unwrap();
            assert_eq!(c.lock().reply, want, "acks={acks}");
        }
    }

    /// gh #282: the Write ACL comes first, so a principal without rights
    /// gets 29 whether or not the topic exists; with rights, a partition
    /// past the topic's count is Apache's 3, not a leadership error.
    #[tokio::test]
    async fn acl_before_existence_and_range_is_unknown_partition() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let part = |index| produce::PartitionData {
            index,
            records: Some(Bytes::from_static(&[0; 64])),
        };
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "known".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let denied = ProduceHandler::new(Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        )));
        let anon = Principal::anonymous();
        for topic in ["known", "missing"] {
            let r = denied.append_one(&anon, topic, &part(0), -1).await;
            assert_eq!(r.error_code, ERR_TOPIC_AUTHORIZATION_FAILED, "{topic}");
        }
        let allowed = ProduceHandler::new(broker_with_topic("known", 1));
        let r = allowed.append_one(&anon, "known", &part(999), -1).await;
        assert_eq!(r.error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    /// gh #285: a batch over max.message.bytes is Apache's
    /// MESSAGE_TOO_LARGE, 10 — not 18 (RECORD_LIST_TOO_LARGE), which the
    /// Java producer doesn't answer by splitting the batch.
    #[tokio::test]
    async fn an_oversized_batch_is_message_too_large_10() {
        let h = ProduceHandler::new(broker_with_topic("known", 1)).with_max_message_bytes(32);
        let part = produce::PartitionData {
            index: 0,
            records: Some(Bytes::from_static(&[0; 64])),
        };
        let r = h
            .append_one(&Principal::anonymous(), "known", &part, -1)
            .await;
        assert_eq!(r.error_code, 10);
    }
}
