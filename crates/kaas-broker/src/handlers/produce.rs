//! Produce handler (key 0).
//!
//! Iterates `req.topic_data`, calls `engine.append` per partition,
//! maps `StorageError` variants to wire error codes. Records bytes
//! flow through as opaque `Option<Bytes>` — no decode, no re-encode.
//!
//! Two checks read batch headers (attributes only, gh #294), as Apache
//! does:
//! - A request carrying any transactional batch needs a
//!   `transactional_id` the principal may `Write`
//!   (`handleProduceRequest`). Otherwise every partition answers
//!   `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53) and nothing is
//!   appended.
//! - A client may not write control batches (`LogValidator`). Only the
//!   transaction coordinator writes COMMIT/ABORT markers, and it appends
//!   them straight to the engine, not through Produce. A client-sent
//!   control batch answers `INVALID_RECORD` (87).
//!
//! Both checks rely on the records being exactly what the header says
//! they are, which is the structural check's job (gh #294 follow-up).
//! Each partition must carry exactly one magic-v2 batch whose
//! `batchLength` spans the whole records field (Apache's
//! `ProduceRequest.validateRecords`: `INVALID_RECORD`, 87). A length too
//! small to be a batch, an unknown magic, or a CRC that doesn't match is
//! `CORRUPT_MESSAGE` (2). Without this, a batch whose `batchLength` lied
//! slipped past both attribute checks while the append path still read
//! its header. A bad CRC was acked and later truncated every acked record
//! behind it on recovery, and a second batch reused offsets.
//!
//! The header's other fields are checked too, as Apache's
//! `analyzeAndValidateRecords` and `LogValidator.validateBatch` do
//! (gh #305), because the append path advances the high watermark by
//! `lastOffsetDelta`. Without these, a CRC-valid batch with
//! `lastOffsetDelta = -1000` or `i32::MAX` wrecked the partition's offsets
//! for good.

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
const ERR_CORRUPT_MESSAGE: i16 = 2;
const ERR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
const ERR_LEADER_NOT_AVAILABLE: i16 = 5;
const ERR_NOT_LEADER_FOR_PARTITION: i16 = 6;
/// Apache's `MESSAGE_TOO_LARGE` (`RecordTooLargeException`): a batch over
/// `max.message.bytes`. Until gh #285 this was 18, `RECORD_LIST_TOO_LARGE`,
/// which the Java producer doesn't answer by splitting the batch.
const ERR_MESSAGE_TOO_LARGE: i16 = 10;
/// Apache's `RECORD_LIST_TOO_LARGE` (`RecordBatchTooLargeException`): a
/// batch larger than the topic's `segment.bytes`.
const ERR_RECORD_LIST_TOO_LARGE: i16 = 18;
/// Mirrors Apache Kafka's `message.max.bytes` default: 1 MiB of
/// records plus 12 bytes of batch overhead (gh #14).
/// Apache's `message.max.bytes` default (1 MiB + batch overhead).
/// The production wiring overrides via [`ProduceHandler::with_max_message_bytes`]
/// from `KAAS_MAX_MESSAGE_BYTES` (gh #253).
const DEFAULT_MAX_MESSAGE_BYTES: usize = 1_048_588;
const ERR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
const ERR_INVALID_REQUIRED_ACKS: i16 = 21;
const ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED: i16 = 53;
const ERR_UNSUPPORTED_COMPRESSION_TYPE: i16 = 76;
const ERR_INVALID_RECORD: i16 = 87;
/// v2 RecordBatch attribute bits (`RecordBatch.isTransactional` /
/// `isControlBatch`).
const ATTR_TRANSACTIONAL: i16 = 0x10;
const ATTR_CONTROL: i16 = 0x20;
/// The low three attribute bits name the codec (`CompressionType`).
const ATTR_COMPRESSION_MASK: i16 = 0x07;
const COMPRESSION_ZSTD: i16 = 4;
/// The smallest possible v2 record: one byte each for its length,
/// attributes, timestamp delta, offset delta, key length, value length and
/// header count.
const MIN_RECORD_BYTES: i64 = 7;
/// How far a batch's size can bound its records when compressed: no codec
/// expands a byte past this. zstd's run-length blocks reach about 32 KiB per
/// byte, gzip about 1 KiB.
const MAX_COMPRESSION_RATIO: i64 = 65_536;
/// Everything before the records in a v2 batch.
const V2_HEADER_BYTES: usize = 61;
/// Produce's first version allowed to carry ZSTD (KIP-110).
const ZSTD_MIN_PRODUCE_VERSION: i16 = 7;
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

        // gh #294: checked before anything per-topic. Without it, a
        // principal with only topic Write could open a transaction under
        // a PID no coordinator knows. Nothing would ever abort it, and it
        // would pin the partition's LSO for every read_committed consumer.
        let txn_denied = self.transactional_id_denied(&principal, &req);

        let mut responses = Vec::with_capacity(req.topic_data.len());
        for t in &req.topic_data {
            let mut partition_responses = Vec::with_capacity(t.partition_data.len());
            for p in &t.partition_data {
                let pr = if txn_denied {
                    error_partition_bumped(
                        &t.name,
                        p.index,
                        ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                    )
                } else {
                    self.append_one(&principal, &t.name, p, acks, version).await
                };
                partition_responses.push(pr);
            }
            responses.push(produce::TopicResponse {
                name: t.name.clone(),
                partition_responses,
            });
        }

        // A refused request appended nothing, so it isn't charged
        // against the produce byte quota (Apache answers it before
        // recording the quota).
        let throttle_time_ms = if txn_denied {
            0
        } else {
            self.broker
                .quotas
                .check_produce_quota(&principal, total_bytes)
        };

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
    /// Apache's `handleProduceRequest` gate: a request with any
    /// transactional batch must name a `transactional_id` the principal
    /// may `Write`. A request with no transactional batches passes,
    /// whatever its `transactional_id`.
    fn transactional_id_denied(&self, principal: &Principal, req: &produce::Request) -> bool {
        let transactional = req
            .topic_data
            .iter()
            .flat_map(|t| &t.partition_data)
            .filter_map(|p| p.records.as_deref())
            .any(|r| has_batch_with(r, ATTR_TRANSACTIONAL));
        if !transactional {
            return false;
        }
        !req.transactional_id.as_deref().is_some_and(|id| {
            self.broker.authorizer.authorize(
                principal,
                &Resource::transactional_id(id),
                Operation::Write,
            )
        })
    }

    async fn append_one(
        &self,
        principal: &Principal,
        topic: &str,
        p: &produce::PartitionData,
        acks: i16,
        version: i16,
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

        // Apache's `ProduceRequest.validateRecords`, which runs before the
        // leadership check: exactly one magic-v2 batch spanning the field,
        // and no ZSTD below v7.
        let batch_size = match p.records.as_deref().map(|r| validate_structure(r, version)) {
            Some(Ok(size)) => Some(size),
            Some(Err(code)) => return error_partition_bumped(topic, p.index, code),
            None => None,
        };

        // Apache's `ReplicaManager.appendRecords` refuses these acks before
        // it looks at leadership.
        if !matches!(acks, -1..=1) {
            return error_partition_bumped(topic, p.index, ERR_INVALID_REQUIRED_ACKS);
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

        // Null records: Apache's append path has no records to read and
        // answers UNKNOWN_SERVER_ERROR (gh #307).
        let (Some(records), Some(batch_size)) = (p.records.clone(), batch_size) else {
            return error_partition_bumped(topic, p.index, ERR_GENERIC);
        };
        // An incomplete tail isn't part of the batch (`trimInvalidBytes`).
        let records = records.slice(..batch_size);

        // UnifiedLog's `analyzeAndValidateRecords`: a client's batch starts
        // at 0. The base offset is rewritten on append, so this one only
        // matters for parity.
        if batch_base_offset(&records) != 0 {
            return error_partition_bumped(topic, p.index, ERR_INVALID_RECORD);
        }

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

        // UnifiedLog's `batch.ensureValid()`, after the size check. A bad
        // CRC must never be acked: the recovery scan stops at it, and would
        // truncate every acked record written after it (gh #228).
        if kaas_codec::crc::verify_batch_crc(&records).is_err() {
            return error_partition_bumped(topic, p.index, ERR_CORRUPT_MESSAGE);
        }

        // `analyzeAndValidateRecords` reads the codec after the CRC, and
        // `CompressionType.forId` throws on an id it doesn't know, which
        // reaches the client as UNKNOWN_SERVER_ERROR.
        if batch_attributes(&records) & ATTR_COMPRESSION_MASK > COMPRESSION_ZSTD {
            return error_partition_bumped(topic, p.index, ERR_GENERIC);
        }

        // `LogValidator.validateBatch`, gh #305 and gh #294.
        if !batch_fields_are_valid(&records) {
            return error_partition_bumped(topic, p.index, ERR_INVALID_RECORD);
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

/// One step of Apache's batch iterator (`ByteBufferLogInputStream.nextBatch`)
/// over `rest`: the size of the whole batch at its front, `None` if there
/// isn't a complete one, or `CORRUPT_MESSAGE` for a length too small to be
/// any record format or an impossible magic.
fn next_batch_size(rest: &[u8]) -> Result<Option<usize>, i16> {
    /// `LegacyRecord.RECORD_OVERHEAD_V0`, the iterator's lower bound.
    const MIN_BATCH_LENGTH: i32 = 14;
    /// `HEADER_SIZE_UP_TO_MAGIC`.
    const BYTES_THROUGH_MAGIC: usize = 17;
    let Some(len4) = rest.get(8..12) else {
        return Ok(None);
    };
    let declared = i32::from_be_bytes([len4[0], len4[1], len4[2], len4[3]]);
    if declared < MIN_BATCH_LENGTH {
        return Err(ERR_CORRUPT_MESSAGE);
    }
    if rest.len() < BYTES_THROUGH_MAGIC {
        return Ok(None);
    }
    if rest[16] > 2 {
        return Err(ERR_CORRUPT_MESSAGE);
    }
    let size = 12 + usize::try_from(declared).unwrap_or(usize::MAX - 12);
    Ok((size <= rest.len()).then_some(size))
}

/// The structural half of Apache's record validation, returning the size
/// of the one batch to append. Reads the batch header only, so byte opacity
/// holds. Follows the iterator's rules (gh #307):
/// - No complete batch is `INVALID_RECORD`.
/// - Magic 0/1 is `INVALID_RECORD` ("must use magic v2").
/// - ZSTD below Produce v7 is `UNSUPPORTED_COMPRESSION_TYPE` (76).
/// - A complete second batch is `INVALID_RECORD` ("exactly one record batch
///   per partition"). An incomplete tail isn't a batch at all: Apache's
///   `trimInvalidBytes` drops it, and so does the caller here.
/// - A magic-2 batch too short for its header is `CORRUPT_MESSAGE`, where
///   Apache's `isValid` fails it.
fn validate_structure(records: &[u8], version: i16) -> Result<usize, i16> {
    let Some(size) = next_batch_size(records)? else {
        return Err(ERR_INVALID_RECORD);
    };
    if records[16] != 2 {
        return Err(ERR_INVALID_RECORD);
    }
    if version < ZSTD_MIN_PRODUCE_VERSION
        && batch_attributes(records) & ATTR_COMPRESSION_MASK == COMPRESSION_ZSTD
    {
        return Err(ERR_UNSUPPORTED_COMPRESSION_TYPE);
    }
    if next_batch_size(&records[size..])?.is_some() {
        return Err(ERR_INVALID_RECORD);
    }
    if size < V2_HEADER_BYTES {
        return Err(ERR_CORRUPT_MESSAGE);
    }
    Ok(size)
}

// Header readers for a batch `validate_structure` has accepted, so all 61
// header bytes are there. Headers only: byte opacity holds.
fn batch_base_offset(b: &[u8]) -> i64 {
    i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}
fn batch_attributes(b: &[u8]) -> i16 {
    i16::from_be_bytes([b[21], b[22]])
}

/// Apache's `LogValidator.validateBatch` for a client's v2 batch:
/// - `lastOffsetDelta + 1` must be positive and equal a positive
///   `recordCount`. The high watermark advances by that delta, so a wrong
///   one moves it backwards or past what a segment can index.
/// - No control batches: only the coordinator writes COMMIT/ABORT markers,
///   and the append path takes any control batch as one (gh #294).
/// - A batch with a producer id needs a non-negative base sequence, or it
///   skips the dedupe window.
///
/// Apache also walks the records and refuses a count that disagrees with
/// them. Byte opacity rules that out here, but an uncompressed batch's
/// count can still be bounded from its size alone: every record takes at
/// least [`MIN_RECORD_BYTES`]. That stops a header whose two fields agree
/// with each other while claiming far more records than the batch holds
/// (gh #305 verification). A compressed batch can only be bounded loosely, by
/// the most any codec can expand: enough that a tiny batch can't claim billions
/// of records and force a segment roll for almost no bytes (gh #312). The rest
/// is a limit of the byte-opacity contract; see the wire-protocol docs.
fn batch_fields_are_valid(b: &[u8]) -> bool {
    let last_offset_delta = i32::from_be_bytes([b[23], b[24], b[25], b[26]]);
    let producer_id = i64::from_be_bytes([b[43], b[44], b[45], b[46], b[47], b[48], b[49], b[50]]);
    let base_sequence = i32::from_be_bytes([b[53], b[54], b[55], b[56]]);
    let record_count = i32::from_be_bytes([b[57], b[58], b[59], b[60]]);
    let count_from_offsets = i64::from(last_offset_delta) + 1;
    let uncompressed = batch_attributes(b) & ATTR_COMPRESSION_MASK == 0;
    let payload = i64::try_from(b.len().saturating_sub(V2_HEADER_BYTES)).unwrap_or(i64::MAX);
    let max_expanded = if uncompressed {
        payload
    } else {
        payload.saturating_mul(MAX_COMPRESSION_RATIO)
    };
    let fits_payload = i64::from(record_count) * MIN_RECORD_BYTES <= max_expanded;
    count_from_offsets > 0
        && record_count > 0
        && count_from_offsets == i64::from(record_count)
        && fits_payload
        && batch_attributes(b) & ATTR_CONTROL == 0
        && !(producer_id >= 0 && base_sequence < 0)
}

/// Whether any whole batch in `records` has the attribute bit `flag` set.
fn has_batch_with(records: &[u8], flag: i16) -> bool {
    kaas_storage::batch_attributes(records).any(|a| a & flag != 0)
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
        StorageError::RecordBatchTooLarge => ERR_RECORD_LIST_TOO_LARGE,
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
            let r = denied.append_one(&anon, topic, &part(0), -1, 9).await;
            assert_eq!(r.error_code, ERR_TOPIC_AUTHORIZATION_FAILED, "{topic}");
        }
        let allowed = ProduceHandler::new(broker_with_topic("known", 1));
        let r = allowed.append_one(&anon, "known", &part(999), -1, 9).await;
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
            records: Some(batch(0, -1, -1)),
        };
        let r = h
            .append_one(&Principal::anonymous(), "known", &part, -1, 9)
            .await;
        assert_eq!(r.error_code, 10);
    }

    /// A v2 batch with the given attributes, producer id and base
    /// sequence. The body is a COMMIT marker's, which is opaque to every
    /// check here; the CRC is recomputed so the batch stays valid.
    fn batch(attrs: i16, producer_id: i64, base_sequence: i32) -> Bytes {
        let mut b = crate::control_batch::build_control_batch(producer_id, 0, true, 0);
        b[21..23].copy_from_slice(&attrs.to_be_bytes());
        b[53..57].copy_from_slice(&base_sequence.to_be_bytes());
        let crc = kaas_codec::crc::compute(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    fn txn_broker(authorizer: Arc<dyn kaas_auth::Authorizer>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let topics = Arc::new(TopicRegistry::new());
        for name in ["orders", "audit"] {
            topics.insert(TopicMeta {
                name: name.to_owned(),
                partition_count: 1,
                topic_id: [0; 16],
            });
        }
        Arc::new(Broker::with_auth(
            engine,
            topics,
            "test",
            0,
            authorizer,
            Arc::new(kaas_auth::NoQuotaChecker),
        ))
    }

    /// Produce `orders`'s batch and `audit`'s batch in one request; the
    /// per-partition error codes, in that order.
    async fn produce_two(
        broker: &Arc<Broker>,
        transactional_id: Option<&str>,
        orders: Bytes,
        audit: Bytes,
    ) -> Vec<i16> {
        let req = produce::Request {
            transactional_id: transactional_id.map(str::to_owned),
            acks: -1,
            timeout_ms: 1000,
            topic_data: [("orders", orders), ("audit", audit)]
                .into_iter()
                .map(|(name, records)| produce::TopicData {
                    name: name.to_owned(),
                    partition_data: vec![produce::PartitionData {
                        index: 0,
                        records: Some(records),
                    }],
                })
                .collect(),
        };
        let h = ProduceHandler::new(broker.clone());
        let out = h.handle(&conn(), 9, encode_request(&req, 9)).await.unwrap();
        let resp = produce::decode_response(&mut out.freeze(), 9).unwrap();
        resp.responses
            .iter()
            .map(|t| t.partition_responses[0].error_code)
            .collect()
    }

    fn appended(broker: &Broker, topic: &str) -> i64 {
        broker.engine.high_watermark(topic, 0).unwrap_or(0)
    }

    /// gh #294: one transactional batch makes the whole request need
    /// Write on its transactional id (Apache's literal 53, for every
    /// partition, nothing appended). The plain batch next to it is
    /// refused too, as in Apache.
    #[tokio::test]
    async fn a_transactional_batch_needs_write_on_the_transactional_id() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let plain = || batch(0, -1, -1);
        let txn = || batch(ATTR_TRANSACTIONAL, 7, 0);

        // No transactional id at all.
        let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
        assert_eq!(produce_two(&b, None, plain(), txn()).await, [53, 53]);
        // A transactional id the principal may not Write.
        assert_eq!(
            produce_two(&b, Some("nobody"), txn(), plain()).await,
            [53, 53]
        );
        assert_eq!((appended(&b, "orders"), appended(&b, "audit")), (0, 0));

        // Allowed: both land.
        assert_eq!(
            produce_two(&b, Some("payments-tx"), txn(), plain()).await,
            [0, 0]
        );
        assert_eq!((appended(&b, "orders"), appended(&b, "audit")), (1, 1));
    }

    /// Control: no transactional batch, no transactional-id check, even
    /// when the request names one the principal can't use.
    #[tokio::test]
    async fn a_non_transactional_request_skips_the_transactional_id_check() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
        let plain = || batch(0, -1, -1);
        assert_eq!(produce_two(&b, None, plain(), plain()).await, [0, 0]);
        assert_eq!(
            produce_two(&b, Some("nobody"), plain(), plain()).await,
            [0, 0]
        );
    }

    /// gh #294: a client may not write a control batch. Apache's
    /// `LogValidator` answers INVALID_RECORD (87). It is refused per
    /// partition, nothing is appended, and the other partition is
    /// unaffected.
    #[tokio::test]
    async fn a_client_control_batch_is_invalid_record_87() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
        // A forged COMMIT marker, transactional id and all.
        let marker = batch(ATTR_TRANSACTIONAL | ATTR_CONTROL, 7, -1);
        let plain = batch(0, -1, -1);
        assert_eq!(
            produce_two(&b, Some("payments-tx"), marker, plain).await,
            [87, 0]
        );
        assert_eq!((appended(&b, "orders"), appended(&b, "audit")), (0, 1));
        // A control batch behind a data batch in the same records blob.
        let mut run = BytesMut::from(&batch(0, -1, -1)[..]);
        run.extend_from_slice(&batch(ATTR_CONTROL, -1, -1));
        assert_eq!(
            produce_two(&b, None, run.freeze(), batch(0, -1, -1)).await,
            [87, 0]
        );
    }

    /// `b` with its `batchLength` field overwritten.
    fn with_length(b: &Bytes, len: i32) -> Bytes {
        let mut v = b.to_vec();
        v[8..12].copy_from_slice(&len.to_be_bytes());
        Bytes::from(v)
    }

    /// gh #294 follow-up: Apache's structural checks, with its literal
    /// codes. Each malformed `orders` batch is refused and appends
    /// nothing; the well-formed `audit` batch next to it lands.
    #[tokio::test]
    async fn malformed_records_are_refused_with_apaches_codes() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let good = batch(0, -1, -1);
        let real = i32::try_from(good.len() - 12).unwrap();
        let mut bad_crc = good.to_vec();
        bad_crc[60] ^= 0xff;
        let mut magic1 = good.to_vec();
        magic1[16] = 1;
        let mut magic9 = good.to_vec();
        magic9[16] = 9;
        let mut two = good.to_vec();
        two.extend_from_slice(&good);
        let cases: Vec<(&str, Bytes, i16)> = vec![
            ("short", Bytes::from_static(&[0; 8]), 87),
            ("length 0", with_length(&good, 0), 2),
            ("length -1", with_length(&good, -1), 2),
            ("length past the field", with_length(&good, real + 1), 87),
            ("length i32::MAX", with_length(&good, i32::MAX), 87),
            // One short: a batch whose CRC can't match, plus a one-byte tail.
            ("length one short", with_length(&good, real - 1), 2),
            ("two batches", Bytes::from(two), 87),
            ("magic 1", Bytes::from(magic1), 87),
            ("magic 9", Bytes::from(magic9), 2),
            ("bad CRC", Bytes::from(bad_crc), 2),
        ];
        for (what, records, code) in cases {
            let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
            assert_eq!(
                produce_two(&b, None, records, batch(0, -1, -1)).await,
                [code, 0],
                "{what}"
            );
            assert_eq!(appended(&b, "orders"), 0, "{what} was appended");
        }
    }

    /// gh #294 follow-up, the verifier's bypass: a transactional batch or
    /// a forged marker whose `batchLength` lies used to slip past both
    /// attribute checks (the batch walker skipped it), while the append
    /// path still read its header. It opened a phantom transaction or
    /// recorded an abort, and answered 0. Now nothing is appended and the
    /// LSO doesn't move.
    #[tokio::test]
    async fn a_lying_batch_length_cannot_bypass_the_transactional_checks() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let forged = [
            batch(ATTR_TRANSACTIONAL, 424_242, 0),
            batch(ATTR_TRANSACTIONAL | ATTR_CONTROL, 424_242, -1),
            batch(ATTR_CONTROL, 424_242, -1),
        ];
        for b0 in &forged {
            let real = i32::try_from(b0.len() - 12).unwrap();
            for len in [0, -1, 1000, i32::MAX, real + 1] {
                let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
                let codes = produce_two(&b, None, with_length(b0, len), batch(0, -1, -1)).await;
                assert_ne!(codes[0], 0, "length {len} accepted");
                assert_eq!(appended(&b, "orders"), 0, "length {len} appended");
                assert_eq!(
                    b.engine.last_stable_offset("orders", 0).unwrap_or(0),
                    0,
                    "length {len} moved the LSO"
                );
            }
        }
    }

    /// A batch larger than the topic's `segment.bytes` is Apache's
    /// RECORD_LIST_TOO_LARGE, 18 (`RecordBatchTooLargeException`), as
    /// opposed to MESSAGE_TOO_LARGE (10) for one over `max.message.bytes`.
    #[test]
    fn a_batch_over_segment_bytes_is_record_list_too_large_18() {
        let r = map_error("t", 0, &StorageError::RecordBatchTooLarge);
        assert_eq!(r.error_code, 18);
    }

    /// `batch(0, -1, -1)` with `edit` applied and the CRC recomputed, so
    /// only the field under test is wrong.
    fn edited(edit: impl FnOnce(&mut Vec<u8>)) -> Bytes {
        let mut b = batch(0, -1, -1).to_vec();
        edit(&mut b);
        let crc = kaas_codec::crc::compute(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        Bytes::from(b)
    }

    /// gh #305: a CRC-valid batch whose header lies about its offsets or
    /// count is Apache's INVALID_RECORD (87, `LogValidator.validateBatch`).
    /// Each of these used to be acked; a bad `lastOffsetDelta` moved the
    /// high watermark backwards or to 2^31. Nothing is appended, and the
    /// well-formed batch next to it lands.
    #[tokio::test]
    async fn a_lying_batch_header_is_refused_with_apaches_codes() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let delta = |d: i32| move |b: &mut Vec<u8>| b[23..27].copy_from_slice(&d.to_be_bytes());
        let count = |c: i32| move |b: &mut Vec<u8>| b[57..61].copy_from_slice(&c.to_be_bytes());
        let cases: Vec<(&str, Bytes, i16)> = vec![
            ("delta -1000", edited(delta(-1000)), 87),
            ("delta -1", edited(delta(-1)), 87),
            ("delta i32::MAX", edited(delta(i32::MAX)), 87),
            ("delta past the count", edited(delta(1)), 87),
            ("count -1", edited(count(-1)), 87),
            ("count 0", edited(count(0)), 87),
            ("count 5", edited(count(5)), 87),
            (
                "base offset 7",
                edited(|b| b[0..8].copy_from_slice(&7i64.to_be_bytes())),
                87,
            ),
            ("a PID with a negative sequence", batch(0, 7, -1), 87),
            (
                "unknown codec 5",
                edited(|b| b[21..23].copy_from_slice(&5i16.to_be_bytes())),
                -1,
            ),
            (
                "unknown codec 7",
                edited(|b| b[21..23].copy_from_slice(&7i16.to_be_bytes())),
                -1,
            ),
        ];
        for (what, records, code) in cases {
            let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
            assert_eq!(
                produce_two(&b, None, records, batch(0, -1, -1)).await,
                [code, 0],
                "{what}"
            );
            assert_eq!(appended(&b, "orders"), 0, "{what} was appended");
        }
    }

    /// gh #305 verification: an uncompressed batch can't hold more records
    /// than its size allows, even when `lastOffsetDelta` and `recordCount`
    /// agree. Each record takes at least 7 bytes. Apache also answers 87.
    #[tokio::test]
    async fn an_uncompressed_batch_claiming_more_records_than_fit_is_87() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let claiming = |delta: i32, count: i32| {
            edited(move |h| {
                h[23..27].copy_from_slice(&delta.to_be_bytes());
                h[57..61].copy_from_slice(&count.to_be_bytes());
            })
        };
        for (delta, count) in [(i32::MAX - 1, i32::MAX), (9, 10)] {
            let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
            assert_eq!(
                produce_two(&b, None, claiming(delta, count), batch(0, -1, -1)).await,
                [87, 0],
                "count {count}"
            );
            assert_eq!(appended(&b, "orders"), 0, "count {count}");
        }
    }

    /// gh #305: every field of a well-formed multi-record batch is
    /// accepted: the control for the checks above.
    #[tokio::test]
    async fn an_honest_multi_record_batch_is_accepted() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
        let two = edited(|h| {
            h[23..27].copy_from_slice(&1i32.to_be_bytes());
            h[57..61].copy_from_slice(&2i32.to_be_bytes());
        });
        assert_eq!(produce_two(&b, None, two, batch(0, -1, -1)).await, [0, 0]);
        assert_eq!(appended(&b, "orders"), 2);
    }

    /// gh #305: ZSTD needs Produce v7 (KIP-110). Below it, Apache's
    /// `validateRecords` answers UNSUPPORTED_COMPRESSION_TYPE (76).
    #[tokio::test]
    async fn zstd_below_produce_v7_is_76() {
        for (version, want) in [(3, 76), (6, 76), (7, 0), (9, 0)] {
            let h = ProduceHandler::new(broker_with_topic("known", 1));
            let part = produce::PartitionData {
                index: 0,
                records: Some(edited(|b| {
                    b[21..23].copy_from_slice(&COMPRESSION_ZSTD.to_be_bytes());
                })),
            };
            let r = h
                .append_one(&Principal::anonymous(), "known", &part, -1, version)
                .await;
            assert_eq!(r.error_code, want, "v{version}");
        }
    }

    /// gh #307: Apache's iterator ends at an incomplete tail rather than
    /// failing, and `trimInvalidBytes` drops it, so the batch in front lands
    /// and only its own bytes are stored. A *complete* second batch is 87.
    #[tokio::test]
    async fn an_incomplete_tail_is_trimmed_not_refused() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let good = batch(0, -1, -1);
        for (what, tail) in [
            ("5 stray bytes", good[..5].to_vec()),
            ("a partial batch", good[..30].to_vec()),
        ] {
            let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
            let mut records = good.to_vec();
            records.extend_from_slice(&tail);
            assert_eq!(
                produce_two(&b, None, Bytes::from(records), batch(0, -1, -1)).await,
                [0, 0],
                "{what}"
            );
            let stored = b.engine.read("orders", 0, 0, 1 << 20).await.unwrap();
            assert_eq!(stored.len(), good.len(), "{what}: the tail was stored");
        }
    }

    /// gh #307: the short-field and magic orderings, Apache's literal codes
    /// from sending the same bytes to a 3.7.2 broker.
    #[tokio::test]
    async fn short_fields_answer_apaches_codes() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let with = |len: usize, declared: i32, magic: u8| {
            let mut v = vec![0u8; len];
            v[8..12].copy_from_slice(&declared.to_be_bytes());
            if len > 16 {
                v[16] = magic;
            }
            Bytes::from(v)
        };
        for (what, records, code) in [
            ("15 bytes, length 20", with(15, 20, 2), 87),
            ("length 20, magic 1", with(32, 20, 1), 87),
            ("length 20, magic 2", with(32, 20, 2), 2),
            ("length 13", with(32, 13, 2), 2),
        ] {
            let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
            assert_eq!(
                produce_two(&b, None, records, batch(0, -1, -1)).await,
                [code, 0],
                "{what}"
            );
        }
    }

    /// gh #307: invalid acks are Apache's INVALID_REQUIRED_ACKS (21), and
    /// null records its UNKNOWN_SERVER_ERROR (-1). Neither writes anything.
    #[tokio::test]
    async fn invalid_acks_and_null_records() {
        let h = ProduceHandler::new(broker_with_topic("known", 1));
        let anon = Principal::anonymous();
        let good = produce::PartitionData {
            index: 0,
            records: Some(batch(0, -1, -1)),
        };
        for acks in [2, -2] {
            let r = h.append_one(&anon, "known", &good, acks, 9).await;
            assert_eq!(r.error_code, 21, "acks {acks}");
        }
        let null = produce::PartitionData {
            index: 0,
            records: None,
        };
        assert_eq!(
            h.append_one(&anon, "known", &null, -1, 9).await.error_code,
            -1
        );
        assert_eq!(h.broker.engine.high_watermark("known", 0).unwrap_or(0), 0);
    }

    /// gh #312: a compressed batch can't claim more records than any codec
    /// could expand its bytes into. A tiny batch claiming billions forced a
    /// segment roll for almost no bytes. A count inside the bound is accepted:
    /// that's the byte-opacity limit, and a real batch can compress that well.
    #[tokio::test]
    async fn a_compressed_batch_claiming_impossibly_many_records_is_87() {
        use crate::handlers::test_authz::DenyNamedAuthorizer;
        let gzip = |delta: i32, count: i32| {
            edited(move |h| {
                h[21..23].copy_from_slice(&1i16.to_be_bytes());
                h[23..27].copy_from_slice(&delta.to_be_bytes());
                h[57..61].copy_from_slice(&count.to_be_bytes());
            })
        };
        let b = txn_broker(Arc::new(DenyNamedAuthorizer("nobody")));
        assert_eq!(
            produce_two(&b, None, gzip(i32::MAX - 1, i32::MAX), batch(0, -1, -1)).await,
            [87, 0]
        );
        assert_eq!(appended(&b, "orders"), 0);
        assert_eq!(
            produce_two(&b, None, gzip(999, 1000), batch(0, -1, -1)).await,
            [0, 0],
            "plausibly compressed"
        );
        assert_eq!(appended(&b, "orders"), 1000);
    }
}
