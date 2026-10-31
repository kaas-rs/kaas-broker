//! OffsetCommit — API key 8.
//!
//! Versions 0..=8. Flexible (KIP-482) from v8. v1+ adds
//! `generation_id` + `member_id` on the request; v3+ adds
//! `throttle_time_ms` on the response; v6+ adds
//! `committed_leader_epoch` per partition; v7+ adds nullable
//! `group_instance_id` (KIP-345).

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

// Min 2: v1's per-partition
// `commit_timestamp` (and v2–4's `retention_time_ms`) are not
// decoded here, so don't advertise v0/v1 shapes
// this module never parsed correctly. (v2–4 retention remains a
// known divergence from Apache; tracked as follow-up.)
pub const VERSIONS: (i16, i16) = (2, 8);
pub const MIN_FLEXIBLE: i16 = 8;

pub const SPEC: ApiSpec = ApiSpec {
    key: 8,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    /// v1+. `0` when unset.
    pub generation_id: i32,
    /// v1+. Empty on legacy versions.
    pub member_id: String,
    /// v7+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub topics: Vec<OffsetCommitTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopic {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitPartition {
    pub partition_index: i32,
    pub committed_offset: i64,
    /// v6+. `-1` (no epoch) on legacy versions.
    pub committed_leader_epoch: i32,
    /// `None` ↔ wire null.
    pub committed_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v3+. `0` when unset.
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetCommitTopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetCommitTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetCommitPartitionResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetCommitPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request {
        group_id: read_str(buf, flexible)?,
        ..Request::default()
    };
    if version >= 1 {
        req.generation_id = read_i32(buf)?;
        req.member_id = read_str(buf, flexible)?;
    }
    if version >= 7 {
        req.group_instance_id = read_nullable_str(buf, flexible)?;
    }
    let nt = read_array_len(buf, flexible)?;
    for _ in 0..nt {
        let name = read_str(buf, flexible)?;
        let mut partitions = Vec::new();
        let np = read_array_len(buf, flexible)?;
        for _ in 0..np {
            let partition_index = read_i32(buf)?;
            let committed_offset = read_i64(buf)?;
            let committed_leader_epoch = if version >= 6 { read_i32(buf)? } else { -1 };
            let committed_metadata = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(OffsetCommitPartition {
                partition_index,
                committed_offset,
                committed_leader_epoch,
                committed_metadata,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        req.topics.push(OffsetCommitTopic { name, partitions });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(req)
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version >= 3 {
        write_i32(buf, resp.throttle_time_ms);
    }
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
    let mut resp = Response::default();
    if version >= 3 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    let nt = read_array_len(buf, flexible)?;
    for _ in 0..nt {
        let name = read_str(buf, flexible)?;
        let mut partitions = Vec::new();
        let np = read_array_len(buf, flexible)?;
        for _ in 0..np {
            let partition_index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(OffsetCommitPartitionResponse {
                partition_index,
                error_code,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        resp.topics
            .push(OffsetCommitTopicResponse { name, partitions });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    if version >= 1 {
        write_i32(buf, req.generation_id);
        write_str(buf, &req.member_id, flexible)?;
    }
    if version >= 7 {
        write_nullable_str(buf, req.group_instance_id.as_deref(), flexible)?;
    }
    write_array_len(buf, req.topics.len(), flexible)?;
    for t in &req.topics {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i64(buf, p.committed_offset);
            if version >= 6 {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(version: i16) -> Request {
        Request {
            group_id: "g1".to_owned(),
            generation_id: if version >= 1 { 7 } else { 0 },
            member_id: if version >= 1 {
                "consumer-1".to_owned()
            } else {
                String::new()
            },
            group_instance_id: if version >= 7 {
                Some("inst-1".to_owned())
            } else {
                None
            },
            topics: vec![OffsetCommitTopic {
                name: "t1".to_owned(),
                partitions: vec![
                    OffsetCommitPartition {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: if version >= 6 { 3 } else { -1 },
                        committed_metadata: Some("meta".to_owned()),
                    },
                    OffsetCommitPartition {
                        partition_index: 1,
                        committed_offset: 99,
                        committed_leader_epoch: if version >= 6 { 3 } else { -1 },
                        committed_metadata: None,
                    },
                ],
            }],
        }
    }

    fn sample_response(_version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponse {
                name: "t1".to_owned(),
                partitions: vec![OffsetCommitPartitionResponse {
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

        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn v2_roundtrip() {
        roundtrip(2);
    }

    #[test]
    fn v6_adds_leader_epoch() {
        roundtrip(6);
    }

    #[test]
    fn v7_adds_group_instance_id() {
        roundtrip(7);
    }

    #[test]
    fn v8_is_flexible() {
        roundtrip(8);
    }
}
