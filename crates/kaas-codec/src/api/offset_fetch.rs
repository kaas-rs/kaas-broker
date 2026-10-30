//! OffsetFetch — API key 9.
//!
//! Versions 0..=8. Flexible (KIP-482) from v6. v2+ adds top-level
//! `error_code` on the response; v3+ adds `throttle_time_ms`; v5+
//! adds `committed_leader_epoch` per partition; v7+ adds
//! `require_stable` boolean on the request (KIP-447 read-committed
//! offset fetch); v8+ replaces the single-group shape with a batch
//! `groups[]` form.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::headers::HeaderVersion;
use crate::primitives::{
    read_bool, read_i16, read_i32, read_i64, write_bool, write_i16, write_i32, write_i64,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (1, 8);
pub const MIN_FLEXIBLE: i16 = 6;

const fn header_for(version: i16) -> HeaderVersion {
    if version >= MIN_FLEXIBLE {
        HeaderVersion::V2
    } else {
        HeaderVersion::V1
    }
}

fn request_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

fn response_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

pub const SPEC: ApiSpec = ApiSpec {
    key: 9,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
    request_hdr,
    response_hdr,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// v1..=v7 single-group form. Empty at v8.
    pub group_id: String,
    /// v1..=v7. `None` means "fetch every committed topic".
    pub topics: Option<Vec<OffsetFetchTopic>>,
    /// v8+ batch form.
    pub groups: Vec<OffsetFetchGroup>,
    /// v7+. False on legacy versions.
    pub require_stable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroup {
    pub group_id: String,
    /// `None` means "fetch every committed topic" — the form Kafka 4's
    /// AdminClient sends for `listConsumerGroupOffsets` (and so
    /// `kafka-consumer-groups --describe`).
    pub topics: Option<Vec<OffsetFetchTopic>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v3+. `0` when unset.
    pub throttle_time_ms: i32,
    /// v1..=v7 flat topic list.
    pub topics: Vec<OffsetFetchTopicResponse>,
    /// v2..=v7 top-level error code.
    pub error_code: i16,
    /// v8+ per-group response form.
    pub groups: Vec<OffsetFetchGroupResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetFetchPartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchPartitionResponse {
    pub partition_index: i32,
    pub committed_offset: i64,
    /// v5+. `-1` on legacy versions.
    pub committed_leader_epoch: i32,
    /// `None` ↔ wire null.
    pub metadata: Option<String>,
    pub error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetFetchGroupResponse {
    pub group_id: String,
    pub topics: Vec<OffsetFetchTopicResponse>,
    pub error_code: i16,
}

fn decode_partition(
    buf: &mut Bytes,
    version: i16,
    flexible: bool,
) -> Result<OffsetFetchPartitionResponse, CodecError> {
    let partition_index = read_i32(buf)?;
    let committed_offset = read_i64(buf)?;
    let committed_leader_epoch = if version >= 5 { read_i32(buf)? } else { -1 };
    let metadata = read_nullable_str(buf, flexible)?;
    let error_code = read_i16(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(OffsetFetchPartitionResponse {
        partition_index,
        committed_offset,
        committed_leader_epoch,
        metadata,
        error_code,
    })
}

fn encode_partition(
    buf: &mut BytesMut,
    p: &OffsetFetchPartitionResponse,
    version: i16,
    flexible: bool,
) -> Result<(), CodecError> {
    write_i32(buf, p.partition_index);
    write_i64(buf, p.committed_offset);
    if version >= 5 {
        write_i32(buf, p.committed_leader_epoch);
    }
    write_nullable_str(buf, p.metadata.as_deref(), flexible)?;
    write_i16(buf, p.error_code);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    if version >= 8 {
        let ng = read_array_len(buf, flexible)?;
        for _ in 0..ng {
            let group_id = read_str(buf, true)?;
            // Nullable, like the v1..=v7 list below: keep the null.
            let topics = match crate::primitives::read_uvarint(buf)? {
                0 => None,
                n => {
                    let nt = usize::try_from(n - 1).map_err(|_| CodecError::LengthOverflow)?;
                    let mut topics = Vec::with_capacity(nt);
                    for _ in 0..nt {
                        let name = read_str(buf, true)?;
                        let mut partition_indexes = Vec::new();
                        let np = read_array_len(buf, true)?;
                        for _ in 0..np {
                            partition_indexes.push(read_i32(buf)?);
                        }
                        tagged::read(buf)?;
                        topics.push(OffsetFetchTopic {
                            name,
                            partition_indexes,
                        });
                    }
                    Some(topics)
                }
            };
            tagged::read(buf)?;
            req.groups.push(OffsetFetchGroup { group_id, topics });
        }
        req.require_stable = read_bool(buf)?;
        tagged::read(buf)?;
        return Ok(req);
    }
    req.group_id = read_str(buf, flexible)?;
    // Topics array: v1..=v7. The wire encodes `null` as "fetch all
    // committed offsets" — surface that as `None`. The generic
    // `read_array_len` collapses null to zero, so peek the length
    // prefix manually to preserve the null sentinel.
    let topics_len = if flexible {
        let u = crate::primitives::read_uvarint(buf)?;
        match u {
            0 => None,
            n => Some(usize::try_from(n - 1).map_err(|_| CodecError::LengthOverflow)?),
        }
    } else {
        let n = read_i32(buf)?;
        if n < 0 {
            None
        } else {
            Some(usize::try_from(n).map_err(|_| CodecError::LengthOverflow)?)
        }
    };
    match topics_len {
        None => req.topics = None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n);
            for _ in 0..n {
                let name = read_str(buf, flexible)?;
                let np = read_array_len(buf, flexible)?;
                let mut partition_indexes = Vec::with_capacity(np);
                for _ in 0..np {
                    partition_indexes.push(read_i32(buf)?);
                }
                if flexible {
                    tagged::read(buf)?;
                }
                topics.push(OffsetFetchTopic {
                    name,
                    partition_indexes,
                });
            }
            req.topics = Some(topics);
        }
    }
    if version >= 7 {
        req.require_stable = read_bool(buf)?;
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
    if version >= 8 {
        write_array_len(buf, resp.groups.len(), true)?;
        for g in &resp.groups {
            write_str(buf, &g.group_id, true)?;
            write_array_len(buf, g.topics.len(), true)?;
            for t in &g.topics {
                write_str(buf, &t.name, true)?;
                write_array_len(buf, t.partitions.len(), true)?;
                for p in &t.partitions {
                    encode_partition(buf, p, version, true)?;
                }
                tagged::write_empty(buf);
            }
            write_i16(buf, g.error_code);
            tagged::write_empty(buf);
        }
        tagged::write_empty(buf);
        return Ok(());
    }
    write_array_len(buf, resp.topics.len(), flexible)?;
    for t in &resp.topics {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            encode_partition(buf, p, version, flexible)?;
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    if version >= 2 {
        write_i16(buf, resp.error_code);
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
    if version >= 8 {
        let ng = read_array_len(buf, true)?;
        for _ in 0..ng {
            let group_id = read_str(buf, true)?;
            let mut topics = Vec::new();
            let nt = read_array_len(buf, true)?;
            for _ in 0..nt {
                let name = read_str(buf, true)?;
                let mut partitions = Vec::new();
                let np = read_array_len(buf, true)?;
                for _ in 0..np {
                    partitions.push(decode_partition(buf, version, true)?);
                }
                tagged::read(buf)?;
                topics.push(OffsetFetchTopicResponse { name, partitions });
            }
            let error_code = read_i16(buf)?;
            tagged::read(buf)?;
            resp.groups.push(OffsetFetchGroupResponse {
                group_id,
                topics,
                error_code,
            });
        }
        tagged::read(buf)?;
        return Ok(resp);
    }
    let nt = read_array_len(buf, flexible)?;
    for _ in 0..nt {
        let name = read_str(buf, flexible)?;
        let mut partitions = Vec::new();
        let np = read_array_len(buf, flexible)?;
        for _ in 0..np {
            partitions.push(decode_partition(buf, version, flexible)?);
        }
        if flexible {
            tagged::read(buf)?;
        }
        resp.topics
            .push(OffsetFetchTopicResponse { name, partitions });
    }
    if version >= 2 {
        resp.error_code = read_i16(buf)?;
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version >= 8 {
        write_array_len(buf, req.groups.len(), true)?;
        for g in &req.groups {
            write_str(buf, &g.group_id, true)?;
            match &g.topics {
                None => crate::primitives::write_uvarint(buf, 0),
                Some(topics) => {
                    write_array_len(buf, topics.len(), true)?;
                    for t in topics {
                        write_str(buf, &t.name, true)?;
                        write_array_len(buf, t.partition_indexes.len(), true)?;
                        for &i in &t.partition_indexes {
                            write_i32(buf, i);
                        }
                        tagged::write_empty(buf);
                    }
                }
            }
            tagged::write_empty(buf);
        }
        write_bool(buf, req.require_stable);
        tagged::write_empty(buf);
        return Ok(());
    }
    write_str(buf, &req.group_id, flexible)?;
    match &req.topics {
        None => {
            if flexible {
                crate::primitives::write_uvarint(buf, 0);
            } else {
                write_i32(buf, -1);
            }
        }
        Some(topics) => {
            write_array_len(buf, topics.len(), flexible)?;
            for t in topics {
                write_str(buf, &t.name, flexible)?;
                write_array_len(buf, t.partition_indexes.len(), flexible)?;
                for &i in &t.partition_indexes {
                    write_i32(buf, i);
                }
                if flexible {
                    tagged::write_empty(buf);
                }
            }
        }
    }
    if version >= 7 {
        write_bool(buf, req.require_stable);
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
        if version >= 8 {
            Request {
                groups: vec![OffsetFetchGroup {
                    group_id: "g1".to_owned(),
                    topics: Some(vec![OffsetFetchTopic {
                        name: "t1".to_owned(),
                        partition_indexes: vec![0, 1],
                    }]),
                }],
                require_stable: true,
                ..Request::default()
            }
        } else {
            Request {
                group_id: "g1".to_owned(),
                topics: Some(vec![OffsetFetchTopic {
                    name: "t1".to_owned(),
                    partition_indexes: vec![0, 1, 2],
                }]),
                require_stable: version >= 7,
                ..Request::default()
            }
        }
    }

    fn sample_response(version: i16) -> Response {
        if version >= 8 {
            Response {
                throttle_time_ms: 0,
                groups: vec![OffsetFetchGroupResponse {
                    group_id: "g1".to_owned(),
                    topics: vec![OffsetFetchTopicResponse {
                        name: "t1".to_owned(),
                        partitions: vec![OffsetFetchPartitionResponse {
                            partition_index: 0,
                            committed_offset: 42,
                            committed_leader_epoch: 3,
                            metadata: Some("m".to_owned()),
                            error_code: 0,
                        }],
                    }],
                    error_code: 0,
                }],
                ..Response::default()
            }
        } else {
            Response {
                throttle_time_ms: 0,
                topics: vec![OffsetFetchTopicResponse {
                    name: "t1".to_owned(),
                    partitions: vec![OffsetFetchPartitionResponse {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: if version >= 5 { 3 } else { -1 },
                        metadata: Some("m".to_owned()),
                        error_code: 0,
                    }],
                }],
                error_code: 0,
                ..Response::default()
            }
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
    fn v1_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn v5_adds_leader_epoch() {
        roundtrip(5);
    }

    #[test]
    fn v6_is_flexible() {
        roundtrip(6);
    }

    #[test]
    fn v7_adds_require_stable() {
        roundtrip(7);
    }

    #[test]
    fn v8_is_batch_form() {
        roundtrip(8);
    }

    #[test]
    fn null_topics_is_fetch_all() {
        let req = Request {
            group_id: "g".to_owned(),
            topics: None,
            ..Request::default()
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 5).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 5).unwrap();
        assert_eq!(got.topics, None);
    }

    #[test]
    fn v8_null_topics_is_fetch_all() {
        // Kafka 4's `listConsumerGroupOffsets` sends this; collapsing
        // the null to an empty list answered it with nothing.
        let req = Request {
            groups: vec![
                OffsetFetchGroup {
                    group_id: "all".to_owned(),
                    topics: None,
                },
                OffsetFetchGroup {
                    group_id: "none".to_owned(),
                    topics: Some(Vec::new()),
                },
            ],
            ..Request::default()
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 8).unwrap();
        let got = decode_request(&mut w.freeze(), 8).unwrap();
        assert_eq!(got.groups, req.groups);
    }
}
