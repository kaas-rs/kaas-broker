//! ListOffsets — API key 2. v1–v7, flexible from v6 (KIP-482).
//!
//!
//! # Version map
//!
//! | v | Adds                                                          |
//! |--:|---------------------------------------------------------------|
//! | 1 | Drops `max_num_offsets`; partition response carries `timestamp` + `offset` |
//! | 2 | `isolation_level` req · `throttle_time_ms` resp               |
//! | 4 | `current_leader_epoch` req partition · `leader_epoch` resp partition |
//! | 6 | KIP-482 flexible encoding                                     |
//! | 7 | Adds `EARLIEST_LOCAL_TIMESTAMP` (-4) / `MAX_TIMESTAMP` (-3) sentinel timestamps (no wire shape change) |

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, read_i8, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (1, 7);
pub const MIN_FLEXIBLE: i16 = 6;

pub const SPEC: ApiSpec = ApiSpec {
    key: 2,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `-1` for clients.
    pub replica_id: i32,
    /// v2+. `0` = read_uncommitted, `1` = read_committed.
    pub isolation_level: i8,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub partition_index: i32,
    /// v4+.
    pub current_leader_epoch: i32,
    /// `-1` = LATEST, `-2` = EARLIEST. Other negative sentinels
    /// (-3 MAX_TIMESTAMP, -4 EARLIEST_LOCAL_TIMESTAMP) are accepted on
    /// the wire but flow through to the storage engine for handling.
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// v2+.
    pub throttle_time_ms: i32,
    pub topics: Vec<TopicResponse>,
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
    /// v1+. Echoed back for timestamp lookups; `-1` for
    /// EARLIEST/LATEST sentinels.
    pub timestamp: i64,
    /// v1+.
    pub offset: i64,
    /// v4+.
    pub leader_epoch: i32,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let replica_id = read_i32(buf)?;
    let isolation_level = if version >= 2 { read_i8(buf)? } else { 0 };

    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let current_leader_epoch = if version >= 4 { read_i32(buf)? } else { -1 };
            let timestamp = read_i64(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(Partition {
                partition_index,
                current_leader_epoch,
                timestamp,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(Topic { name, partitions });
    }

    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        replica_id,
        isolation_level,
        topics,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    if version >= 2 {
        write_i32(buf, resp.throttle_time_ms);
    }

    write_array_len(buf, resp.topics.len(), flexible)?;
    for t in &resp.topics {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i16(buf, p.error_code);
            if version >= 1 {
                write_i64(buf, p.timestamp);
                write_i64(buf, p.offset);
            }
            if version >= 4 {
                write_i32(buf, p.leader_epoch);
            }
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

    let throttle_time_ms = if version >= 2 { read_i32(buf)? } else { 0 };
    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            let (timestamp, offset) = if version >= 1 {
                (read_i64(buf)?, read_i64(buf)?)
            } else {
                (-1, -1)
            };
            let leader_epoch = if version >= 4 { read_i32(buf)? } else { -1 };
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(PartitionResponse {
                partition_index,
                error_code,
                timestamp,
                offset,
                leader_epoch,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(TopicResponse { name, partitions });
    }

    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        topics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::write_i8;

    fn sample_request(version: i16) -> Request {
        Request {
            replica_id: -1,
            isolation_level: if version >= 2 { 1 } else { 0 },
            topics: vec![Topic {
                name: "events".to_owned(),
                partitions: vec![
                    Partition {
                        partition_index: 0,
                        current_leader_epoch: if version >= 4 { 5 } else { -1 },
                        timestamp: -2,
                    },
                    Partition {
                        partition_index: 1,
                        current_leader_epoch: if version >= 4 { 5 } else { -1 },
                        timestamp: 1_700_000_000_000,
                    },
                ],
            }],
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            topics: vec![TopicResponse {
                name: "events".to_owned(),
                partitions: vec![PartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                    timestamp: -1,
                    offset: 42,
                    leader_epoch: if version >= 4 { 5 } else { -1 },
                }],
            }],
        }
    }

    fn encode_request(req: &Request, version: i16) -> BytesMut {
        let flexible = version >= MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_i32(&mut w, req.replica_id);
        if version >= 2 {
            write_i8(&mut w, req.isolation_level);
        }
        write_array_len(&mut w, req.topics.len(), flexible).unwrap();
        for t in &req.topics {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partitions.len(), flexible).unwrap();
            for p in &t.partitions {
                write_i32(&mut w, p.partition_index);
                if version >= 4 {
                    write_i32(&mut w, p.current_leader_epoch);
                }
                write_i64(&mut w, p.timestamp);
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
        w
    }

    fn roundtrip_request(version: i16) {
        let req = sample_request(version);
        let w = encode_request(&req, version);
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());
    }

    fn roundtrip_response(version: i16) {
        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn request_v1_roundtrip() {
        roundtrip_request(1);
    }
    #[test]
    fn request_v2_isolation_present() {
        roundtrip_request(2);
    }
    #[test]
    fn request_v4_leader_epoch_present() {
        roundtrip_request(4);
    }
    #[test]
    fn request_v6_flexible_roundtrip() {
        roundtrip_request(6);
    }
    #[test]
    fn request_v7_roundtrip() {
        roundtrip_request(7);
    }

    #[test]
    fn response_v1_roundtrip() {
        roundtrip_response(1);
    }
    #[test]
    fn response_v2_throttle_present() {
        roundtrip_response(2);
    }
    #[test]
    fn response_v4_leader_epoch_present() {
        roundtrip_response(4);
    }
    #[test]
    fn response_v6_flexible_roundtrip() {
        roundtrip_response(6);
    }

    #[test]
    fn earliest_sentinel_passes_through() {
        let mut req = sample_request(7);
        req.topics[0].partitions[0].timestamp = -2; // EARLIEST
        let w = encode_request(&req, 7);
        let mut r = w.freeze();
        let got = decode_request(&mut r, 7).unwrap();
        assert_eq!(got.topics[0].partitions[0].timestamp, -2);
    }
}
