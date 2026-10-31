//! OffsetDelete — API key 47.
//!
//! Versions 0..=0. Non-flexible. Drives
//! `AdminClient.deleteConsumerGroupOffsets()` and
//! `kafka-consumer-groups.sh --delete-offsets` — drops specific
//! `(topic, partition)` committed offsets without dropping the whole
//! group.
//!
//! Wire shape note: the group-level `error_code` precedes
//! `throttle_time_ms` (the opposite of DeleteGroups). Per-partition
//! errors live on each `PartitionResponse` and are only set when the
//! group-level `error_code` is 0.

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_array_len, read_i16, read_i32, read_string, write_array_len, write_i16, write_i32,
    write_string,
};
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 0);

pub const SPEC: ApiSpec = ApiSpec {
    key: 47,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: None,
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    pub topics: Vec<OffsetDeleteTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub error_code: i16,
    pub throttle_time_ms: i32,
    pub topics: Vec<OffsetDeleteTopicResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OffsetDeleteTopicResponse {
    pub name: String,
    pub partitions: Vec<OffsetDeletePartitionResponse>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OffsetDeletePartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, _version: i16) -> Result<Request, CodecError> {
    let group_id = read_string(buf)?;
    let mut topics = Vec::new();
    let nt = read_array_len(buf)?;
    for _ in 0..nt {
        let name = read_string(buf)?;
        let mut partitions = Vec::new();
        let np = read_array_len(buf)?;
        for _ in 0..np {
            partitions.push(read_i32(buf)?);
        }
        topics.push(OffsetDeleteTopic { name, partitions });
    }
    Ok(Request { group_id, topics })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    _version: i16,
) -> Result<(), CodecError> {
    // Group-level ErrorCode precedes ThrottleTime — note this differs
    // from the more common (throttle, error_code, topics) order.
    write_i16(buf, resp.error_code);
    write_i32(buf, resp.throttle_time_ms);
    write_array_len(buf, resp.topics.len())?;
    for t in &resp.topics {
        write_string(buf, &t.name)?;
        write_array_len(buf, t.partitions.len())?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i16(buf, p.error_code);
        }
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, _version: i16) -> Result<Response, CodecError> {
    let error_code = read_i16(buf)?;
    let throttle_time_ms = read_i32(buf)?;
    let mut topics = Vec::new();
    let nt = read_array_len(buf)?;
    for _ in 0..nt {
        let name = read_string(buf)?;
        let mut partitions = Vec::new();
        let np = read_array_len(buf)?;
        for _ in 0..np {
            let partition_index = read_i32(buf)?;
            let p_err = read_i16(buf)?;
            partitions.push(OffsetDeletePartitionResponse {
                partition_index,
                error_code: p_err,
            });
        }
        topics.push(OffsetDeleteTopicResponse { name, partitions });
    }
    Ok(Response {
        error_code,
        throttle_time_ms,
        topics,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, _version: i16) -> Result<(), CodecError> {
    write_string(buf, &req.group_id)?;
    write_array_len(buf, req.topics.len())?;
    for t in &req.topics {
        write_string(buf, &t.name)?;
        write_array_len(buf, t.partitions.len())?;
        for &p in &t.partitions {
            write_i32(buf, p);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn v0_roundtrip() {
        let req = Request {
            group_id: "g1".to_owned(),
            topics: vec![OffsetDeleteTopic {
                name: "t1".to_owned(),
                partitions: vec![0, 1, 2],
            }],
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 0).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 0).unwrap();
        assert_eq!(got, req);
        assert!(r.is_empty());

        let resp = Response {
            error_code: 0,
            throttle_time_ms: 0,
            topics: vec![OffsetDeleteTopicResponse {
                name: "t1".to_owned(),
                partitions: vec![OffsetDeletePartitionResponse {
                    partition_index: 0,
                    error_code: 0,
                }],
            }],
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 0).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 0).unwrap();
        assert_eq!(got, resp);
        assert!(r.is_empty());
    }

    #[test]
    fn group_level_error_precedes_throttle() {
        // Apache OffsetDelete v0 puts ErrorCode (2 bytes) BEFORE
        // ThrottleTimeMs (4 bytes). Verify by hand-encoding and
        // checking the first 6 bytes of the wire payload.
        let resp = Response {
            error_code: 16, // NOT_COORDINATOR
            throttle_time_ms: 0x01020304,
            topics: Vec::new(),
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 0).unwrap();
        let bytes = w.freeze();
        assert_eq!(&bytes[..6], &[0x00, 0x10, 0x01, 0x02, 0x03, 0x04]);
    }
}
