//! OffsetCommit — API key 8.
//!
//! Versions 2..=8 (v0/v1 are not advertised: v1's per-partition
//! `commit_timestamp` is not decoded). Flexible (KIP-482) from v8. v1+ adds
//! `generation_id` + `member_id` on the request; v3+ adds
//! `throttle_time_ms` on the response; v2–v4 carry a top-level
//! `retention_time_ms` (decoded and ignored, gh #189); v6+ adds
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

// Min 2: v1's per-partition `commit_timestamp` is not decoded here, so
// don't advertise the v0/v1 shapes.
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
    /// v2–v4 only; `-1` (Apache's "broker default") otherwise. Decoded
    /// so the topics after it parse (gh #189), then ignored: kaas
    /// expires no committed offsets, so there is no retention to set.
    pub retention_time_ms: i64,
    pub topics: Vec<OffsetCommitTopic>,
}

/// Apache's `OffsetCommitRequest.DEFAULT_RETENTION_TIME`.
pub const DEFAULT_RETENTION_TIME_MS: i64 = -1;

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
        retention_time_ms: DEFAULT_RETENTION_TIME_MS,
        ..Request::default()
    };
    if version >= 1 {
        req.generation_id = read_i32(buf)?;
        req.member_id = read_str(buf, flexible)?;
    }
    if version >= 7 {
        req.group_instance_id = read_nullable_str(buf, flexible)?;
    }
    if (2..=4).contains(&version) {
        req.retention_time_ms = read_i64(buf)?;
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
    if (2..=4).contains(&version) {
        write_i64(buf, req.retention_time_ms);
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
            retention_time_ms: if (2..=4).contains(&version) {
                86_400_000
            } else {
                DEFAULT_RETENTION_TIME_MS
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

    /// gh #189: v2–v4 carry RetentionTimeMs between MemberId and Topics.
    /// Hand-written bytes in the layout of Apache's
    /// OffsetCommitRequest.json, not encoder output (gh #272): before the
    /// fix the decoder read the retention as the topic array.
    #[test]
    fn v2_to_v4_skip_retention_time_before_the_topics() {
        let mut raw = Vec::new();
        raw.extend_from_slice(&[0, 1, b'g']); // group_id
        raw.extend_from_slice(&5i32.to_be_bytes()); // generation_id
        raw.extend_from_slice(&[0, 1, b'm']); // member_id
        raw.extend_from_slice(&86_400_000i64.to_be_bytes()); // retention_time_ms
        raw.extend_from_slice(&1i32.to_be_bytes()); // topics: 1
        raw.extend_from_slice(&[0, 1, b't']); // name
        raw.extend_from_slice(&1i32.to_be_bytes()); // partitions: 1
        raw.extend_from_slice(&0i32.to_be_bytes()); // partition_index
        raw.extend_from_slice(&42i64.to_be_bytes()); // committed_offset
        raw.extend_from_slice(&(-1i16).to_be_bytes()); // committed_metadata: null
        for v in 2..=4 {
            let mut b = Bytes::from(raw.clone());
            let got = decode_request(&mut b, v).unwrap();
            assert_eq!(got.retention_time_ms, 86_400_000, "v{v}");
            assert_eq!(got.topics[0].name, "t", "v{v}");
            assert_eq!(got.topics[0].partitions[0].committed_offset, 42, "v{v}");
            assert!(b.is_empty(), "v{v}");
        }
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
