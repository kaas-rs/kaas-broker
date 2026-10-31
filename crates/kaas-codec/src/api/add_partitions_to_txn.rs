//! AddPartitionsToTxn — API key 24. v0–v3, flexible from v3.
//!
//!
//! Sent by a transactional producer before its first Produce to a
//! partition under an open transaction. The txn coordinator records
//! the (topic, partition) tuples so EndTxn can dispatch markers to
//! every leader that received writes.
//!
//! v4 introduces multi-transaction batching (`Transactions[]`); kaas
//! ships v0–v3 only — the single-txn shape is what every Java/Go/Rust
//! client actually sends.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 24,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Topic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<TopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TopicResult {
    pub name: String,
    pub partition_results: Vec<PartitionResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct PartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let transactional_id = read_str(buf, flexible)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;

    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            partitions.push(read_i32(buf)?);
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
        producer_id,
        producer_epoch,
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
    write_array_len(buf, resp.results.len(), flexible)?;
    for t in &resp.results {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partition_results.len(), flexible)?;
        for pr in &t.partition_results {
            write_i32(buf, pr.partition_index);
            write_i16(buf, pr.error_code);
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
    let mut results = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partition_results = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let partition_index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            partition_results.push(PartitionResult {
                partition_index,
                error_code,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        results.push(TopicResult {
            name,
            partition_results,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::write_i64;

    fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
        let flexible = version >= MIN_FLEXIBLE;
        write_str(buf, &req.transactional_id, flexible)?;
        write_i64(buf, req.producer_id);
        write_i16(buf, req.producer_epoch);
        write_array_len(buf, req.topics.len(), flexible)?;
        for t in &req.topics {
            write_str(buf, &t.name, flexible)?;
            write_array_len(buf, t.partitions.len(), flexible)?;
            for p in &t.partitions {
                write_i32(buf, *p);
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

    fn sample_request() -> Request {
        Request {
            transactional_id: "tx-1".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            topics: vec![
                Topic {
                    name: "t1".to_owned(),
                    partitions: vec![0, 1, 2],
                },
                Topic {
                    name: "t2".to_owned(),
                    partitions: vec![7],
                },
            ],
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            results: vec![TopicResult {
                name: "t1".to_owned(),
                partition_results: vec![
                    PartitionResult {
                        partition_index: 0,
                        error_code: 0,
                    },
                    PartitionResult {
                        partition_index: 1,
                        error_code: 47, // INVALID_PRODUCER_EPOCH
                    },
                ],
            }],
        }
    }

    fn roundtrip(version: i16) {
        let req = sample_request();
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
    fn v2_legacy_roundtrip() {
        roundtrip(2);
    }

    #[test]
    fn v3_flexible_roundtrip() {
        roundtrip(3);
    }

    #[test]
    fn empty_topics() {
        let req = Request {
            transactional_id: "tx".to_owned(),
            producer_id: 1,
            producer_epoch: 0,
            topics: vec![],
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 3).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 3).unwrap();
        assert_eq!(got, req);
    }
}
