//! Fetch handler (key 1).
//!
//! Stateless full-fetch per gh #4: `session_id = 0` regardless of
//! what the client sent. Read-committed isolation per gh #31: the
//! read cap clamps to the last stable offset and the
//! `aborted_transactions` list is populated from the txn index.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::fetch;
use kaas_protocol::{ConnState, Handler, HandlerError};
use kaas_storage::StorageError;
use parking_lot::Mutex;

use crate::broker::Broker;

const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_OFFSET_OUT_OF_RANGE: i16 = 1;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_NOT_LEADER_FOR_PARTITION: i16 = 6;
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;

#[derive(Debug)]
pub struct FetchHandler {
    broker: Arc<Broker>,
}

impl FetchHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for FetchHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = fetch::decode_request(&mut body, version)?;

        let principal = conn
            .lock()
            .principal
            .clone()
            .unwrap_or_else(Principal::anonymous);

        // gh #176: KIP-98 isolation_level. 0 = read_uncommitted (the
        // pre-Phase-6 behaviour); 1 = read_committed — cap reads at
        // LSO and emit AbortedTransactions[].
        let read_committed = req.isolation_level == 1;

        let mut responses = Vec::with_capacity(req.topics.len());
        let mut total_bytes: usize = 0;
        for t in &req.topics {
            let mut parts = Vec::with_capacity(t.partitions.len());
            for p in &t.partitions {
                let resp = self.read_one(&principal, &t.name, p, read_committed).await;
                total_bytes += resp.records.as_ref().map(|b| b.len()).unwrap_or(0);
                parts.push(resp);
            }
            responses.push(fetch::TopicResponse {
                name: t.name.clone(),
                partitions: parts,
            });
        }

        let throttle_time_ms = self
            .broker
            .quotas
            .check_fetch_quota(&principal, total_bytes);

        let resp = fetch::Response {
            throttle_time_ms,
            error_code: 0,
            session_id: 0, // gh #4 — stateless contract
            responses,
        };
        let mut out = BytesMut::new();
        fetch::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl FetchHandler {
    async fn read_one(
        &self,
        principal: &Principal,
        topic: &str,
        p: &fetch::Partition,
        read_committed: bool,
    ) -> fetch::PartitionResponse {
        // Read ACL first, so a principal without rights learns nothing
        // about the topic — not whether it exists, not who leads it
        // (gh #282, as gh #268 did for the discovery APIs).
        let resource = Resource::topic(topic);
        if !self
            .broker
            .authorizer
            .authorize(principal, &resource, Operation::Read)
        {
            return error_partition_bumped(
                topic,
                p.partition_index,
                ERR_TOPIC_AUTHORIZATION_FAILED,
            );
        }

        // Unknown topic, or a partition past the topic's count: Apache's
        // UNKNOWN_TOPIC_OR_PARTITION, not a leadership error.
        match self.broker.topics.get(topic) {
            Some(meta) if (0..meta.partition_count).contains(&p.partition_index) => {}
            _ => {
                return error_partition_bumped(
                    topic,
                    p.partition_index,
                    ERR_UNKNOWN_TOPIC_OR_PARTITION,
                );
            }
        }

        // Phase 5 cluster check (mirrors Produce). Dev mode (no
        // coordinator wired) falls through to the always-lead path.
        if let Some(c) = self.broker.coordinator() {
            if !c.owns(topic, p.partition_index) {
                return error_partition_bumped(
                    topic,
                    p.partition_index,
                    ERR_NOT_LEADER_FOR_PARTITION,
                );
            }
            // Owned per the assignment but not opened yet: its takeover is
            // still running. The offset reads below would answer 0, as if
            // the log were empty (gh #301).
            if !self
                .broker
                .engine
                .is_partition_open(topic, p.partition_index)
            {
                return error_partition_bumped(topic, p.partition_index, ERR_LEADER_NOT_AVAILABLE);
            }
        }

        // Best-effort metadata; HWM = 0 if the partition has never
        // been written to.
        let hwm = self
            .broker
            .engine
            .high_watermark(topic, p.partition_index)
            .unwrap_or(0);
        let log_start = self
            .broker
            .engine
            .log_start_offset(topic, p.partition_index)
            .unwrap_or(0);
        // gh #176: LSO = highest offset at which every transaction
        // is committed/aborted (no in-flight txn extends past it).
        // For read_uncommitted, the consumer doesn't act on this —
        // we still report it correctly.
        let lso = self
            .broker
            .engine
            .last_stable_offset(topic, p.partition_index)
            .unwrap_or(hwm);
        let read_cap = if read_committed { lso } else { hwm };

        // Apache's `LocalLog.read` / `checkLogStartOffset`: an offset below
        // log start or past the log end (the HWM here, with no replicas) is
        // OFFSET_OUT_OF_RANGE, which is what fires a consumer's
        // `auto.offset.reset`. Answering 0 left a consumer past the end
        // idling forever, and served one below the start from log start
        // (gh #310).
        if p.fetch_offset < log_start || p.fetch_offset > hwm {
            return error_partition_bumped(topic, p.partition_index, ERR_OFFSET_OUT_OF_RANGE);
        }

        // Reading from the high-watermark forward is the "I'm caught
        // up, give me nothing" case. Engine returns empty Bytes.
        let max_bytes = usize::try_from(p.partition_max_bytes.max(0)).unwrap_or(0);
        let bytes = if read_committed && p.fetch_offset >= lso {
            // Read-committed and the consumer is already at or past
            // LSO — nothing stable to serve yet.
            Bytes::new()
        } else {
            match self
                .broker
                .engine
                .read(topic, p.partition_index, p.fetch_offset, max_bytes)
                .await
            {
                Ok(b) => b,
                Err(StorageError::OffsetOutOfRange) => {
                    return error_partition_bumped(
                        topic,
                        p.partition_index,
                        ERR_OFFSET_OUT_OF_RANGE,
                    );
                }
                Err(StorageError::UnknownTopicOrPartition) => {
                    return error_partition_bumped(
                        topic,
                        p.partition_index,
                        ERR_UNKNOWN_TOPIC_OR_PARTITION,
                    );
                }
                Err(StorageError::EpochMismatch) => {
                    return error_partition_bumped(
                        topic,
                        p.partition_index,
                        ERR_NOT_LEADER_FOR_PARTITION,
                    );
                }
                Err(err) => {
                    tracing::warn!(%err, topic, partition = p.partition_index, "fetch read failed");
                    return error_partition_bumped(topic, p.partition_index, -1);
                }
            }
        };

        // For read_committed, trim any batch whose base_offset >=
        // LSO so the consumer doesn't see records from in-flight
        // txns. Then populate AbortedTransactions[] over the
        // returned offset range so the client filters aborted-txn
        // records.
        let (bytes, aborted) = if read_committed {
            let trimmed = trim_to_offset(bytes, read_cap);
            let aborts = self.broker.engine.aborted_transactions_in_range(
                topic,
                p.partition_index,
                p.fetch_offset,
                read_cap,
            );
            let wire: Vec<fetch::AbortedTransaction> = aborts
                .into_iter()
                .map(|a| fetch::AbortedTransaction {
                    producer_id: a.producer_id,
                    first_offset: a.first_offset,
                })
                .collect();
            (trimmed, wire)
        } else {
            (bytes, Vec::new())
        };

        fetch::PartitionResponse {
            partition_index: p.partition_index,
            error_code: 0,
            high_watermark: hwm,
            last_stable_offset: lso,
            log_start_offset: log_start,
            aborted_transactions: aborted,
            preferred_read_replica: -1,
            // Empty rather than null, as Apache sends it: librdkafka refuses
            // a null `records` as a parse failure and spins (gh #310).
            records: Some(bytes),
        }
    }
}

/// Walk v2 RecordBatch bytes and return only the prefix containing
/// batches whose **entire** offset range is strictly below
/// `max_offset` — i.e., `base_offset + last_offset_delta < max_offset`.
/// A batch that straddles `max_offset` is dropped along with
/// everything after it, matching Apache's `read_committed` Fetch
/// behaviour (batches are atomic — the broker never returns a
/// partial batch). Reads only the 27 bytes of v2 header needed
/// (`base_offset`, `batch_length`, `last_offset_delta`); records
/// payload stays byte-opaque.
fn trim_to_offset(bytes: Bytes, max_offset: i64) -> Bytes {
    let mut pos = 0usize;
    while pos + 27 <= bytes.len() {
        let mut base_buf = [0u8; 8];
        base_buf.copy_from_slice(&bytes[pos..pos + 8]);
        let base = i64::from_be_bytes(base_buf);
        let mut delta_buf = [0u8; 4];
        delta_buf.copy_from_slice(&bytes[pos + 23..pos + 27]);
        let last_offset_delta = i32::from_be_bytes(delta_buf);
        let last_offset = base.saturating_add(i64::from(last_offset_delta));
        if last_offset >= max_offset {
            return bytes.slice(..pos);
        }
        let mut len_buf = [0u8; 4];
        len_buf.copy_from_slice(&bytes[pos + 8..pos + 12]);
        let batch_len = i32::from_be_bytes(len_buf);
        // batch_len counts bytes after the length field; full batch
        // is 12 + batch_len. Defensive on negative / overflow.
        let total = 12usize.saturating_add(usize::try_from(batch_len.max(0)).unwrap_or(0));
        let next = pos.saturating_add(total);
        if next <= pos || next > bytes.len() {
            // Truncated tail — drop it.
            return bytes.slice(..pos);
        }
        pos = next;
    }
    bytes
}

fn error_partition(partition_index: i32, error_code: i16) -> fetch::PartitionResponse {
    fetch::PartitionResponse {
        partition_index,
        error_code,
        high_watermark: -1,
        last_stable_offset: -1,
        log_start_offset: -1,
        aborted_transactions: Vec::new(),
        preferred_read_replica: -1,
        records: Some(Bytes::new()),
    }
}

/// `error_partition` + a `kaas.fetch.errors` bump labelled by
/// topic + error_code. Every partition-level Fetch failure routes
/// through here so on-call sees the failure rate even when the
/// success counter has gone flat.
fn error_partition_bumped(
    topic: &str,
    partition_index: i32,
    error_code: i16,
) -> fetch::PartitionResponse {
    kaas_observability::metrics::global().fetch_errors.add(
        1,
        &[
            kaas_observability::KeyValue::new("topic", topic.to_string()),
            kaas_observability::KeyValue::new("error_code", i64::from(error_code)),
        ],
    );
    error_partition(partition_index, error_code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::{TopicMeta, TopicRegistry};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn broker() -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        Arc::new(Broker::new(engine, topics, "test", 0))
    }

    /// One v2 batch covering offsets [base, base + records - 1].
    /// Total batch size = 12 + body_len; body_len chosen so the
    /// minimum 27-byte header read works.
    fn mk_v2_batch(base: i64, records: i32) -> Vec<u8> {
        let body_len = 15usize; // smallest legal value: header through byte 26
        let mut b = vec![0u8; 12 + body_len];
        b[0..8].copy_from_slice(&base.to_be_bytes());
        let body_len_i32 = i32::try_from(body_len).unwrap();
        b[8..12].copy_from_slice(&body_len_i32.to_be_bytes());
        // lastOffsetDelta = records - 1
        b[23..27].copy_from_slice(&(records - 1).to_be_bytes());
        b
    }

    #[test]
    fn trim_to_offset_drops_batches_that_straddle_cap() {
        // batch A: offsets [0, 9]; batch B: [10, 19]; batch C: [20, 29].
        let mut stream = Vec::new();
        stream.extend_from_slice(&mk_v2_batch(0, 10));
        stream.extend_from_slice(&mk_v2_batch(10, 10));
        stream.extend_from_slice(&mk_v2_batch(20, 10));
        let raw = Bytes::from(stream);
        // Cap at 15 — batch B straddles LSO (last = 19 >= 15), drop.
        let got = trim_to_offset(raw, 15);
        assert_eq!(got.len(), 27, "only batch A (12 + 15 bytes) survives");
        let mut base_buf = [0u8; 8];
        base_buf.copy_from_slice(&got[0..8]);
        assert_eq!(i64::from_be_bytes(base_buf), 0);
    }

    #[test]
    fn trim_to_offset_keeps_full_batches_whose_last_offset_is_below_cap() {
        // batch A: [0, 9]; batch B: [10, 14]; cap at 15 → both kept
        // (B's last_offset = 14 < 15).
        let mut stream = Vec::new();
        stream.extend_from_slice(&mk_v2_batch(0, 10));
        stream.extend_from_slice(&mk_v2_batch(10, 5));
        let raw = Bytes::from(stream);
        let got = trim_to_offset(raw, 15);
        assert_eq!(got.len(), 27 * 2);
    }

    #[test]
    fn trim_to_offset_keeps_everything_if_cap_past_last_batch() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&mk_v2_batch(0, 10));
        stream.extend_from_slice(&mk_v2_batch(10, 10));
        let raw = Bytes::from(stream);
        let got = trim_to_offset(raw.clone(), 1_000);
        assert_eq!(got.len(), raw.len());
    }

    #[test]
    fn trim_to_offset_empty_when_first_batch_straddles_cap() {
        let b = mk_v2_batch(50, 10); // [50, 59]
        let raw = Bytes::from(b);
        // cap at 55 — batch's last (59) >= 55 → drop.
        assert!(trim_to_offset(raw, 55).is_empty());
    }

    #[tokio::test]
    async fn unknown_topic_returns_error_3() {
        // Build a minimal request: replica_id, max_wait, min_bytes,
        // max_bytes, isolation, session_id, session_epoch, one topic
        // with one partition.
        use kaas_codec::api::common::{write_array_len, write_str};
        use kaas_codec::primitives::{write_i32, write_i64, write_i8};
        use kaas_codec::tagged;
        let mut w = BytesMut::new();
        write_i32(&mut w, -1); // replica_id
        write_i32(&mut w, 500); // max_wait_ms
        write_i32(&mut w, 1); // min_bytes
        write_i32(&mut w, 1024 * 1024); // max_bytes
        write_i8(&mut w, 0); // isolation_level
        write_i32(&mut w, 0); // session_id
        write_i32(&mut w, -1); // session_epoch
                               // topics (1)
        write_array_len(&mut w, 1, true).unwrap();
        write_str(&mut w, "unknown", true).unwrap();
        write_array_len(&mut w, 1, true).unwrap();
        write_i32(&mut w, 0); // partition_index
        write_i32(&mut w, -1); // current_leader_epoch (v9+)
        write_i64(&mut w, 0); // fetch_offset
        write_i32(&mut w, -1); // last_fetched_epoch (v12+)
        write_i64(&mut w, 0); // log_start_offset (v5+)
        write_i32(&mut w, 64 * 1024); // partition_max_bytes
        tagged::write_empty(&mut w); // partition tag
        tagged::write_empty(&mut w); // topic tag
                                     // forgotten_topics (0)
        write_array_len(&mut w, 0, true).unwrap();
        // rack_id (v11+)
        write_str(&mut w, "", true).unwrap();
        tagged::write_empty(&mut w); // request tag

        let body = w.freeze();
        let h = FetchHandler::new(broker());
        let out = h.handle(&conn(), 12, body).await.unwrap();
        let mut r = out.freeze();
        let resp = fetch::decode_response(&mut r, 12).unwrap();
        assert_eq!(resp.session_id, 0, "session_id must be 0 (gh #4)");
        assert_eq!(resp.responses[0].partitions[0].error_code, 3);
    }

    /// gh #282: the Read ACL comes first, so a principal without rights
    /// gets 29 whether or not the topic exists; with rights, a partition
    /// past the topic's count is Apache's 3, not a leadership error.
    #[tokio::test]
    async fn acl_before_existence_and_range_is_unknown_partition() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let part = |i| fetch::Partition {
            partition_index: i,
            current_leader_epoch: -1,
            fetch_offset: 0,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let denied = FetchHandler::new(Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        )));
        let anon = Principal::anonymous();
        for topic in ["t", "missing"] {
            let r = denied.read_one(&anon, topic, &part(0), false).await;
            assert_eq!(r.error_code, ERR_TOPIC_AUTHORIZATION_FAILED, "{topic}");
        }
        let allowed = FetchHandler::new(broker());
        let r = allowed.read_one(&anon, "t", &part(999), false).await;
        assert_eq!(r.error_code, ERR_UNKNOWN_TOPIC_OR_PARTITION);
    }

    /// gh #301: an owned partition its takeover hasn't opened yet is
    /// LEADER_NOT_AVAILABLE (5). It used to answer HWM 0 with no records,
    /// as if the log were empty. After the takeover it serves.
    #[tokio::test]
    async fn an_owned_partition_not_yet_open_is_leader_not_available() {
        use crate::handlers::test_cluster::{self, SELF};
        use kaas_storage::{DiskStorageEngine, Fs, PartitionConfig, RealFs};
        let tmp = tempfile::tempdir().unwrap();
        let cluster = tempfile::tempdir().unwrap();
        let fs: Arc<dyn Fs> = Arc::new(RealFs::new());
        let engine: Arc<dyn StorageEngine> = Arc::new(DiskStorageEngine::new(
            fs,
            tmp.path().to_path_buf(),
            PartitionConfig::default(),
        ));
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        let b = Arc::new(Broker::new(engine.clone(), topics, "test", 0));
        test_cluster::install(&b, cluster.path(), &[("t", 0, SELF)]);
        let h = FetchHandler::new(b);
        let part = fetch::Partition {
            partition_index: 0,
            current_leader_epoch: -1,
            fetch_offset: 0,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 1024,
        };
        let anon = Principal::anonymous();
        let r = h.read_one(&anon, "t", &part, false).await;
        assert_eq!(r.error_code, 5);
        engine.take_over("t", 0, 1).await.unwrap();
        let r = h.read_one(&anon, "t", &part, false).await;
        assert_eq!(r.error_code, 0);
    }

    /// gh #310: an offset below log start or past the HWM is Apache's
    /// OFFSET_OUT_OF_RANGE (1), at both isolation levels. An offset at the
    /// HWM is the caught-up case: no error, and empty records, not null.
    #[tokio::test]
    async fn offsets_outside_the_log_are_out_of_range() {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        for i in 0..5 {
            let raw = Bytes::from(mk_v2_batch(i, 1));
            engine.append("t", 0, 0, -1, raw).await.unwrap();
        }
        engine.delete_records("t", 0, 2).await.unwrap();
        let h = FetchHandler::new(Arc::new(Broker::new(engine, topics, "test", 0)));
        let anon = Principal::anonymous();
        for read_committed in [false, true] {
            for (fetch_offset, code, has_data) in [
                (-5, 1, false),
                (-1, 1, false),
                (0, 1, false),
                (1, 1, false),
                (2, 0, true),
                (4, 0, true),
                (5, 0, false),
                (6, 1, false),
                (999_999, 1, false),
            ] {
                let part = fetch::Partition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    fetch_offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1 << 20,
                };
                let r = h.read_one(&anon, "t", &part, read_committed).await;
                let ctx = format!("offset {fetch_offset}, read_committed {read_committed}");
                assert_eq!(r.error_code, code, "{ctx}");
                let records = r.records.as_ref().expect("never null");
                assert_eq!(!records.is_empty(), has_data, "{ctx}");
            }
        }
    }

    /// One record of a v2 batch with the given attributes and producer.
    fn batch_of(attributes: i16, producer_id: i64) -> Bytes {
        let body = 49 + 16;
        let mut b = vec![0u8; 12 + body];
        b[8..12].copy_from_slice(&i32::try_from(body).unwrap().to_be_bytes());
        b[16] = 2;
        b[21..23].copy_from_slice(&attributes.to_be_bytes());
        b[43..51].copy_from_slice(&producer_id.to_be_bytes());
        if producer_id < 0 {
            b[53..57].copy_from_slice(&(-1i32).to_be_bytes());
        }
        Bytes::from(b)
    }

    /// gh #310, on the disk engine with a real open transaction. A
    /// `read_committed` consumer between the LSO and the HWM is caught up,
    /// not out of range: the bound is the log end, not what it may read.
    /// Bounding it by the LSO would answer 1 during every open transaction,
    /// and each such consumer would reset its position.
    #[tokio::test]
    async fn read_committed_past_the_lso_is_empty_not_out_of_range() {
        let tmp = tempfile::tempdir().unwrap();
        let fs: Arc<dyn kaas_storage::Fs> = Arc::new(kaas_storage::RealFs::new());
        let engine: Arc<dyn StorageEngine> = Arc::new(kaas_storage::DiskStorageEngine::new(
            fs,
            tmp.path().to_path_buf(),
            kaas_storage::PartitionConfig::default(),
        ));
        let topics = Arc::new(TopicRegistry::new());
        topics.insert(TopicMeta {
            name: "t".to_owned(),
            partition_count: 1,
            topic_id: [0; 16],
        });
        engine.take_over("t", 0, 1).await.unwrap();
        // Offsets: 0 stable, 1 the open transaction, 2 written after it.
        const TRANSACTIONAL: i16 = 0x0010;
        engine.append("t", 0, 1, -1, batch_of(0, -1)).await.unwrap();
        engine
            .append("t", 0, 1, -1, batch_of(TRANSACTIONAL, 42))
            .await
            .unwrap();
        engine.append("t", 0, 1, -1, batch_of(0, -1)).await.unwrap();
        assert_eq!(engine.high_watermark("t", 0).unwrap(), 3);
        assert_eq!(engine.last_stable_offset("t", 0).unwrap(), 1);

        let h = FetchHandler::new(Arc::new(Broker::new(engine, topics, "test", 0)));
        let anon = Principal::anonymous();
        for (read_committed, fetch_offset, code, has_data) in [
            (true, 0, 0, true),
            (true, 1, 0, false), // at the LSO
            (true, 2, 0, false), // between the LSO and the HWM
            (true, 3, 0, false), // at the HWM
            (true, 4, 1, false),
            (true, -1, 1, false),
            (false, 2, 0, true),
            (false, 3, 0, false),
            (false, 4, 1, false),
        ] {
            let part = fetch::Partition {
                partition_index: 0,
                current_leader_epoch: -1,
                fetch_offset,
                last_fetched_epoch: -1,
                log_start_offset: -1,
                partition_max_bytes: 1 << 20,
            };
            let r = h.read_one(&anon, "t", &part, read_committed).await;
            let ctx = format!("offset {fetch_offset}, read_committed {read_committed}");
            assert_eq!(r.error_code, code, "{ctx}");
            let records = r.records.as_ref().expect("never null");
            assert_eq!(!records.is_empty(), has_data, "{ctx}");
            if code == 0 {
                assert_eq!(r.high_watermark, 3, "{ctx}");
                assert_eq!(r.last_stable_offset, 1, "{ctx}");
            }
        }
    }
}
