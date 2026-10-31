//! Fetch — API key 1. v4–v12, flexible from v12 (KIP-482).
//!
//! v13+
//! (UUID topic ids in request) is not in scope for Phase 3 — clients
//! negotiating to v13 fall back to v12 via ApiVersions.
//!
//! # Stateless session contract (gh #4)
//!
//! Kaas does not implement KIP-227 incremental fetch sessions. The
//! Fetch handler always returns `session_id = 0` regardless of what
//! the client sent; clients then issue full Fetch requests every
//! poll. The codec preserves the wire bytes faithfully — the
//! stateless policy is enforced in the handler, not the codec.
//!
//! # Byte opacity
//!
//! [`PartitionResponse::records`] flows as `Option<Bytes>` — the
//! response payload comes straight out of the storage engine and is
//! never inspected here.
//!
//! # Version map
//!
//! | v | Adds                                                          |
//! |--:|---------------------------------------------------------------|
//! | 4 | `isolation_level` req · `last_stable_offset` + `aborted_transactions` resp |
//! | 5 | `log_start_offset` (req partition + resp partition)           |
//! | 7 | `session_id` + `session_epoch` req · `forgotten_topics` req · `error_code` + `session_id` resp |
//! | 9 | `current_leader_epoch` req partition                          |
//! | 11| `rack_id` req · `preferred_read_replica` resp partition       |
//! | 12| KIP-482 flexible (compact + tagged fields) · `last_fetched_epoch` req partition |

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_bytes, read_str, write_array_len, write_nullable_bytes, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, read_i8, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (4, 12);
pub const MIN_FLEXIBLE: i16 = 12;

pub const SPEC: ApiSpec = ApiSpec {
    key: 1,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `-1` for clients (any non-negative id is a broker replica
    /// fetcher — kaas has no replication so this is always `-1`).
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    /// v3+. Total per-response cap across all partitions.
    pub max_bytes: i32,
    /// v4+. `0` = read_uncommitted, `1` = read_committed.
    pub isolation_level: i8,
    /// v7+. Set to `0` by Phase 3 clients on first Fetch; the handler
    /// always responds with `session_id = 0` regardless.
    pub session_id: i32,
    /// v7+.
    pub session_epoch: i32,
    pub topics: Vec<Topic>,
    /// v7+. KIP-227 forgotten-topics-data. Kaas decodes but ignores
    /// — the stateless contract makes this a no-op.
    pub forgotten_topics: Vec<ForgottenTopic>,
    /// v11+. Rack hint for follower fetching; kaas ignores it.
    pub rack_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub partition_index: i32,
    /// v9+.
    pub current_leader_epoch: i32,
    pub fetch_offset: i64,
    /// v12+.
    pub last_fetched_epoch: i32,
    /// v5+.
    pub log_start_offset: i64,
    pub partition_max_bytes: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForgottenTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// v1+.
    pub throttle_time_ms: i32,
    /// v7+. Top-level error (e.g. INVALID_FETCH_SESSION_EPOCH).
    pub error_code: i16,
    /// v7+. Kaas always returns `0` per gh #4.
    pub session_id: i32,
    pub responses: Vec<TopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicResponse {
    pub name: String,
    pub partitions: Vec<PartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    /// v4+. `-1` when not available.
    pub last_stable_offset: i64,
    /// v5+.
    pub log_start_offset: i64,
    /// v4+. Empty in Phase 3 (read-uncommitted path always; no txn
    /// markers in the engine until Phase 6).
    pub aborted_transactions: Vec<AbortedTransaction>,
    /// v11+. `-1` = no preferred follower.
    pub preferred_read_replica: i32,
    /// Opaque v2 RecordBatch bytes from the storage engine.
    pub records: Option<Bytes>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let replica_id = read_i32(buf)?;
    let max_wait_ms = read_i32(buf)?;
    let min_bytes = read_i32(buf)?;
    let max_bytes = if version >= 3 {
        read_i32(buf)?
    } else {
        i32::MAX
    };
    let isolation_level = if version >= 4 { read_i8(buf)? } else { 0 };
    let (session_id, session_epoch) = if version >= 7 {
        (read_i32(buf)?, read_i32(buf)?)
    } else {
        (0, -1)
    };

    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let current_leader_epoch = if version >= 9 { read_i32(buf)? } else { -1 };
            let fetch_offset = read_i64(buf)?;
            let last_fetched_epoch = if version >= 12 { read_i32(buf)? } else { -1 };
            let log_start_offset = if version >= 5 { read_i64(buf)? } else { -1 };
            let partition_max_bytes = read_i32(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(Partition {
                partition_index,
                current_leader_epoch,
                fetch_offset,
                last_fetched_epoch,
                log_start_offset,
                partition_max_bytes,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(Topic { name, partitions });
    }

    let mut forgotten_topics = Vec::new();
    if version >= 7 {
        let fc = read_array_len(buf, flexible)?;
        forgotten_topics.reserve(fc);
        for _ in 0..fc {
            let name = read_str(buf, flexible)?;
            let pc = read_array_len(buf, flexible)?;
            let mut parts = Vec::with_capacity(pc);
            for _ in 0..pc {
                parts.push(read_i32(buf)?);
            }
            if flexible {
                tagged::read(buf)?;
            }
            forgotten_topics.push(ForgottenTopic {
                name,
                partitions: parts,
            });
        }
    }

    let rack_id = if version >= 11 {
        read_str(buf, flexible)?
    } else {
        String::new()
    };

    if flexible {
        tagged::read(buf)?;
    }

    Ok(Request {
        replica_id,
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        session_id,
        session_epoch,
        topics,
        forgotten_topics,
        rack_id,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    if version >= 1 {
        write_i32(buf, resp.throttle_time_ms);
    }
    if version >= 7 {
        write_i16(buf, resp.error_code);
        write_i32(buf, resp.session_id);
    }

    write_array_len(buf, resp.responses.len(), flexible)?;
    for t in &resp.responses {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i16(buf, p.error_code);
            write_i64(buf, p.high_watermark);
            if version >= 4 {
                write_i64(buf, p.last_stable_offset);
            }
            if version >= 5 {
                write_i64(buf, p.log_start_offset);
            }
            if version >= 4 {
                write_array_len(buf, p.aborted_transactions.len(), flexible)?;
                for a in &p.aborted_transactions {
                    write_i64(buf, a.producer_id);
                    write_i64(buf, a.first_offset);
                    if flexible {
                        tagged::write_empty(buf);
                    }
                }
            }
            if version >= 11 {
                write_i32(buf, p.preferred_read_replica);
            }
            write_nullable_bytes(buf, p.records.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }

    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let throttle_time_ms = if version >= 1 { read_i32(buf)? } else { 0 };
    let (error_code, session_id) = if version >= 7 {
        (read_i16(buf)?, read_i32(buf)?)
    } else {
        (0, 0)
    };

    let topic_count = read_array_len(buf, flexible)?;
    let mut responses = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            let high_watermark = read_i64(buf)?;
            let last_stable_offset = if version >= 4 { read_i64(buf)? } else { -1 };
            let log_start_offset = if version >= 5 { read_i64(buf)? } else { -1 };
            let aborted_transactions = if version >= 4 {
                let ac = read_array_len(buf, flexible)?;
                let mut at = Vec::with_capacity(ac);
                for _ in 0..ac {
                    let producer_id = read_i64(buf)?;
                    let first_offset = read_i64(buf)?;
                    if flexible {
                        tagged::read(buf)?;
                    }
                    at.push(AbortedTransaction {
                        producer_id,
                        first_offset,
                    });
                }
                at
            } else {
                Vec::new()
            };
            let preferred_read_replica = if version >= 11 { read_i32(buf)? } else { -1 };
            let records = read_nullable_bytes(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(PartitionResponse {
                partition_index,
                error_code,
                high_watermark,
                last_stable_offset,
                log_start_offset,
                aborted_transactions,
                preferred_read_replica,
                records,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        responses.push(TopicResponse { name, partitions });
    }

    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
        session_id,
        responses,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::write_i8;

    fn sample_request(version: i16) -> Request {
        Request {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: if version >= 3 { 1024 * 1024 } else { i32::MAX },
            isolation_level: if version >= 4 { 1 } else { 0 },
            session_id: 0,
            session_epoch: -1,
            topics: vec![Topic {
                name: "events".to_owned(),
                partitions: vec![
                    Partition {
                        partition_index: 0,
                        current_leader_epoch: if version >= 9 { 7 } else { -1 },
                        fetch_offset: 100,
                        last_fetched_epoch: if version >= 12 { 6 } else { -1 },
                        log_start_offset: if version >= 5 { 0 } else { -1 },
                        partition_max_bytes: 64 * 1024,
                    },
                    Partition {
                        partition_index: 1,
                        current_leader_epoch: if version >= 9 { 7 } else { -1 },
                        fetch_offset: 200,
                        last_fetched_epoch: if version >= 12 { 6 } else { -1 },
                        log_start_offset: if version >= 5 { 50 } else { -1 },
                        partition_max_bytes: 64 * 1024,
                    },
                ],
            }],
            forgotten_topics: if version >= 7 {
                vec![ForgottenTopic {
                    name: "old".to_owned(),
                    partitions: vec![2, 3],
                }]
            } else {
                Vec::new()
            },
            rack_id: if version >= 11 {
                "rack-a".to_owned()
            } else {
                String::new()
            },
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            session_id: 0,
            responses: vec![TopicResponse {
                name: "events".to_owned(),
                partitions: vec![PartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    high_watermark: 1000,
                    last_stable_offset: if version >= 4 { 999 } else { -1 },
                    log_start_offset: if version >= 5 { 0 } else { -1 },
                    aborted_transactions: Vec::new(),
                    preferred_read_replica: -1,
                    records: Some(Bytes::from_static(&[0x01, 0x02, 0x03, 0x04])),
                }],
            }],
        }
    }

    fn encode_request(req: &Request, version: i16) -> BytesMut {
        let flexible = version >= MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_i32(&mut w, req.replica_id);
        write_i32(&mut w, req.max_wait_ms);
        write_i32(&mut w, req.min_bytes);
        if version >= 3 {
            write_i32(&mut w, req.max_bytes);
        }
        if version >= 4 {
            write_i8(&mut w, req.isolation_level);
        }
        if version >= 7 {
            write_i32(&mut w, req.session_id);
            write_i32(&mut w, req.session_epoch);
        }
        write_array_len(&mut w, req.topics.len(), flexible).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partitions.len(), flexible).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, p.partition_index);
                if version >= 9 {
                    write_i32(&mut w, p.current_leader_epoch);
                }
                write_i64(&mut w, p.fetch_offset);
                if version >= 12 {
                    write_i32(&mut w, p.last_fetched_epoch);
                }
                if version >= 5 {
                    write_i64(&mut w, p.log_start_offset);
                }
                write_i32(&mut w, p.partition_max_bytes);
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if version >= 7 {
            write_array_len(&mut w, req.forgotten_topics.len(), flexible).unwrap();
            for ft in &req.forgotten_topics {
                write_str(&mut w, &ft.name, flexible).unwrap();
                write_array_len(&mut w, ft.partitions.len(), flexible).unwrap();
                for p in &ft.partitions {
                    write_i32(&mut w, *p);
                }
                if flexible {
                    tagged::write_empty(&mut w);
                }
            }
        }
        if version >= 11 {
            write_str(&mut w, &req.rack_id, flexible).unwrap();
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w
    }

    fn roundtrip_request(version: i16) {
        let req = sample_request(version);
        let w = encode_request(&req, version);
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request roundtrip v{version}");
        assert!(r.is_empty(), "stray bytes after request v{version}");
    }

    fn roundtrip_response(version: i16) {
        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response roundtrip v{version}");
        assert!(r.is_empty(), "stray bytes after response v{version}");
    }

    #[test]
    fn request_v4_roundtrip() {
        roundtrip_request(4);
    }
    #[test]
    fn request_v7_session_fields_present() {
        roundtrip_request(7);
    }
    #[test]
    fn request_v9_leader_epoch_present() {
        roundtrip_request(9);
    }
    #[test]
    fn request_v11_rack_id_present() {
        roundtrip_request(11);
    }
    #[test]
    fn request_v12_flexible_roundtrip() {
        roundtrip_request(12);
    }

    #[test]
    fn response_v4_roundtrip() {
        roundtrip_response(4);
    }
    #[test]
    fn response_v7_session_id_present() {
        roundtrip_response(7);
    }
    #[test]
    fn response_v11_preferred_replica_present() {
        roundtrip_response(11);
    }
    #[test]
    fn response_v12_flexible_roundtrip() {
        roundtrip_response(12);
    }

    #[test]
    fn records_are_byte_opaque() {
        let resp = sample_response(12);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 12).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 12).unwrap();
        assert_eq!(
            got.responses[0].partitions[0].records,
            resp.responses[0].partitions[0].records
        );
    }

    #[test]
    fn aborted_transactions_present_on_v4_plus() {
        let mut resp = sample_response(4);
        resp.responses[0].partitions[0]
            .aborted_transactions
            .push(AbortedTransaction {
                producer_id: 9,
                first_offset: 42,
            });
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 4).unwrap();
        assert_eq!(got, resp);
    }
}
