//! DeleteRecords — API key 21.
//!
//! Versions 0..=2. Flexible (KIP-482) from v2. Used by
//! `kafka-delete-records.sh` / Kafbat's "Purge messages" to advance a
//! partition's log start offset (KIP-107); earlier records become
//! invisible to Fetch and eligible for retention cleanup.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 2);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 21,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub topics: Vec<DeleteRecordsTopic>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteRecordsTopic {
    pub name: String,
    pub partitions: Vec<DeleteRecordsPartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeleteRecordsPartition {
    pub partition_index: i32,
    /// `-1` = "all current records" (purge to HWM, KIP-107 sentinel).
    pub offset: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub topics: Vec<DeleteRecordsTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteRecordsTopicResult {
    pub name: String,
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeleteRecordsPartitionResult {
    pub partition_index: i32,
    /// New log start offset after the truncation.
    pub low_watermark: i64,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let nt = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(nt);
    for _ in 0..nt {
        let name = read_str(buf, flexible)?;
        let np = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(np);
        for _ in 0..np {
            let partition_index = read_i32(buf)?;
            let offset = read_i64(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(DeleteRecordsPartition {
                partition_index,
                offset,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(DeleteRecordsTopic { name, partitions });
    }
    let timeout_ms = read_i32(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request { topics, timeout_ms })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.topics.len(), flexible)?;
    for t in &req.topics {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partitions.len(), flexible)?;
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i64(buf, p.offset);
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_i32(buf, req.timeout_ms);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
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
            write_i64(buf, p.low_watermark);
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
    let nt = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(nt);
    for _ in 0..nt {
        let name = read_str(buf, flexible)?;
        let np = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(np);
        for _ in 0..np {
            let partition_index = read_i32(buf)?;
            let low_watermark = read_i64(buf)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(DeleteRecordsPartitionResult {
                partition_index,
                low_watermark,
                error_code,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(DeleteRecordsTopicResult { name, partitions });
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

    fn roundtrip(version: i16) {
        let req = Request {
            topics: vec![DeleteRecordsTopic {
                name: "orders".into(),
                partitions: vec![
                    DeleteRecordsPartition {
                        partition_index: 0,
                        offset: 42,
                    },
                    DeleteRecordsPartition {
                        partition_index: 1,
                        offset: -1,
                    },
                ],
            }],
            timeout_ms: 30_000,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            topics: vec![DeleteRecordsTopicResult {
                name: "orders".into(),
                partitions: vec![DeleteRecordsPartitionResult {
                    partition_index: 0,
                    low_watermark: 42,
                    error_code: 0,
                }],
            }],
        };
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
    fn v1_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn v2_is_flexible() {
        roundtrip(2);
    }
}
