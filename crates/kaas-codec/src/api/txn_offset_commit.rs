//! TxnOffsetCommit — API key 28. v0–v3, flexible from v3.
//!
//!
//! Sent by a transactional producer to stage consumer-group offset
//! commits as part of an open transaction. The group coordinator
//! holds the offsets in a pending layer (`OffsetStore::store_pending`)
//! until EndTxn fires the offset hook with commit or discard.
//!
//! KIP-447 added `GenerationId`, `MemberId`, `GroupInstanceId` at v3
//! so a fencing rebalance can reject stale txn commits.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_nullable_str, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 28,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub transactional_id: String,
    pub group_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// v3+. `-1` when not present.
    pub generation_id: i32,
    /// v3+. Empty when not present.
    pub member_id: String,
    /// v3+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<Partition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub partition_index: i32,
    pub committed_offset: i64,
    /// v2+. `-1` when not present.
    pub committed_leader_epoch: i32,
    /// `None` ↔ wire null.
    pub committed_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub topics: Vec<ResponseTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseTopic {
    pub name: String,
    pub partitions: Vec<ResponsePartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let transactional_id = read_str(buf, flexible)?;
    let group_id = read_str(buf, flexible)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;

    let (generation_id, member_id, group_instance_id) = if version >= 3 {
        let g = read_i32(buf)?;
        let m = read_str(buf, flexible)?;
        let gi = read_nullable_str(buf, flexible)?;
        (g, m, gi)
    } else {
        (-1, String::new(), None)
    };

    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let committed_offset = read_i64(buf)?;
            let committed_leader_epoch = if version >= 2 { read_i32(buf)? } else { -1 };
            let committed_metadata = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(Partition {
                partition_index,
                committed_offset,
                committed_leader_epoch,
                committed_metadata,
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
        transactional_id,
        group_id,
        producer_id,
        producer_epoch,
        generation_id,
        member_id,
        group_instance_id,
        topics,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_i32(buf, resp.throttle_time_ms);
    write_array_len(buf, resp.topics.len(), flexible)?;
    for t in &resp.topics {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i16(buf, p.error_code);
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
    let throttle_time_ms = read_i32(buf)?;
    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(ResponsePartition {
                partition_index,
                error_code,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(ResponseTopic { name, partitions });
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
    use crate::primitives::write_i64;

    fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
        use crate::api::common::write_nullable_str;
        let flexible = version >= MIN_FLEXIBLE;
        write_str(buf, &req.transactional_id, flexible)?;
        write_str(buf, &req.group_id, flexible)?;
        write_i64(buf, req.producer_id);
        write_i16(buf, req.producer_epoch);
        if version >= 3 {
            write_i32(buf, req.generation_id);
            write_str(buf, &req.member_id, flexible)?;
            write_nullable_str(buf, req.group_instance_id.as_deref(), flexible)?;
        }
        write_array_len(buf, req.topics.len(), flexible)?;
        for t in &req.topics {
            write_str(buf, &t.name, flexible)?;
            write_array_len(buf, t.partitions.len(), flexible)?;
            for p in &t.partitions {
                write_i32(buf, p.partition_index);
                write_i64(buf, p.committed_offset);
                if version >= 2 {
                    write_i32(buf, p.committed_leader_epoch);
                }
                write_nullable_str(buf, p.committed_metadata.as_deref(), flexible)?;
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

    fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
        super::decode_response(buf, version)
    }

    fn sample_request(version: i16) -> Request {
        Request {
            transactional_id: "tx-1".to_owned(),
            group_id: "g1".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            generation_id: if version >= 3 { 5 } else { -1 },
            member_id: if version >= 3 {
                "consumer-0".to_owned()
            } else {
                String::new()
            },
            group_instance_id: if version >= 3 {
                Some("instance-0".to_owned())
            } else {
                None
            },
            topics: vec![Topic {
                name: "t1".to_owned(),
                partitions: vec![
                    Partition {
                        partition_index: 0,
                        committed_offset: 100,
                        committed_leader_epoch: if version >= 2 { 7 } else { -1 },
                        committed_metadata: Some("meta".to_owned()),
                    },
                    Partition {
                        partition_index: 1,
                        committed_offset: 200,
                        committed_leader_epoch: if version >= 2 { 7 } else { -1 },
                        committed_metadata: None,
                    },
                ],
            }],
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            topics: vec![ResponseTopic {
                name: "t1".to_owned(),
                partitions: vec![ResponsePartition {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        }
    }

    fn roundtrip(version: i16) {
        let req = sample_request(version);
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = sample_response();
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn v0_roundtrip() {
        roundtrip(0);
    }

    #[test]
    fn v2_leader_epoch_present() {
        roundtrip(2);
    }

    #[test]
    fn v3_flexible_kip447_fields() {
        roundtrip(3);
    }
}
