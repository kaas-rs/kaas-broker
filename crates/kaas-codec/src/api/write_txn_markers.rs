//! WriteTxnMarkers — API key 27. v0–v1, flexible from v1.
//!
//!
//! Sent by the txn coordinator to each partition's leader to write
//! the COMMIT/ABORT control batch. The receiving broker validates
//! that it leads the partition, builds a control batch, and appends
//! it via the storage engine.
//!
//! Phase 6 implements the receiver side; the cross-broker dispatch
//! from the txn coordinator (gh #114) is deferred — the same-broker
//! fast path in `EndTxn` writes control batches directly without
//! routing through this RPC.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, read_i8, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);
pub const MIN_FLEXIBLE: i16 = 1;

pub const SPEC: ApiSpec = ApiSpec {
    key: 27,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub markers: Vec<WritableTxnMarker>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WritableTxnMarker {
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// `false` = ABORT, `true` = COMMIT.
    pub transaction_result: bool,
    pub topics: Vec<WritableTxnMarkerTopic>,
    pub coordinator_epoch: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WritableTxnMarkerTopic {
    pub name: String,
    pub partition_indexes: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub markers: Vec<WritableTxnMarkerResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WritableTxnMarkerResult {
    pub producer_id: i64,
    pub topics: Vec<WritableTxnMarkerTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WritableTxnMarkerTopicResult {
    pub name: String,
    pub partitions: Vec<WritableTxnMarkerPartitionResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WritableTxnMarkerPartitionResult {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let marker_count = read_array_len(buf, flexible)?;
    let mut markers = Vec::with_capacity(marker_count);
    for _ in 0..marker_count {
        let producer_id = read_i64(buf)?;
        let producer_epoch = read_i16(buf)?;
        let transaction_result = read_i8(buf)? != 0;

        let topic_count = read_array_len(buf, flexible)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = read_str(buf, flexible)?;
            let part_count = read_array_len(buf, flexible)?;
            let mut partition_indexes = Vec::with_capacity(part_count);
            for _ in 0..part_count {
                partition_indexes.push(read_i32(buf)?);
            }
            if flexible {
                tagged::read(buf)?;
            }
            topics.push(WritableTxnMarkerTopic {
                name,
                partition_indexes,
            });
        }
        let coordinator_epoch = read_i32(buf)?;
        if flexible {
            tagged::read(buf)?;
        }
        markers.push(WritableTxnMarker {
            producer_id,
            producer_epoch,
            transaction_result,
            topics,
            coordinator_epoch,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request { markers })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, resp.markers.len(), flexible)?;
    for m in &resp.markers {
        write_i64(buf, m.producer_id);
        write_array_len(buf, m.topics.len(), flexible)?;
        for t in &m.topics {
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
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let marker_count = read_array_len(buf, flexible)?;
    let mut markers = Vec::with_capacity(marker_count);
    for _ in 0..marker_count {
        let producer_id = read_i64(buf)?;
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
                partitions.push(WritableTxnMarkerPartitionResult {
                    partition_index,
                    error_code,
                });
            }
            if flexible {
                tagged::read(buf)?;
            }
            topics.push(WritableTxnMarkerTopicResult { name, partitions });
        }
        if flexible {
            tagged::read(buf)?;
        }
        markers.push(WritableTxnMarkerResult {
            producer_id,
            topics,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response { markers })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::write_i8;

    fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
        let flexible = version >= MIN_FLEXIBLE;
        write_array_len(buf, req.markers.len(), flexible)?;
        for m in &req.markers {
            write_i64(buf, m.producer_id);
            write_i16(buf, m.producer_epoch);
            write_i8(buf, if m.transaction_result { 1 } else { 0 });
            write_array_len(buf, m.topics.len(), flexible)?;
            for t in &m.topics {
                write_str(buf, &t.name, flexible)?;
                write_array_len(buf, t.partition_indexes.len(), flexible)?;
                for p in &t.partition_indexes {
                    write_i32(buf, *p);
                }
                if flexible {
                    tagged::write_empty(buf);
                }
            }
            write_i32(buf, m.coordinator_epoch);
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

    fn sample_request() -> Request {
        Request {
            markers: vec![WritableTxnMarker {
                producer_id: 42,
                producer_epoch: 3,
                transaction_result: true,
                topics: vec![WritableTxnMarkerTopic {
                    name: "t1".to_owned(),
                    partition_indexes: vec![0, 1, 2],
                }],
                coordinator_epoch: 7,
            }],
        }
    }

    fn sample_response() -> Response {
        Response {
            markers: vec![WritableTxnMarkerResult {
                producer_id: 42,
                topics: vec![WritableTxnMarkerTopicResult {
                    name: "t1".to_owned(),
                    partitions: vec![WritableTxnMarkerPartitionResult {
                        partition_index: 0,
                        error_code: 0,
                    }],
                }],
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
    fn v1_flexible_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn empty_markers() {
        let req = Request { markers: vec![] };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 1).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 1).unwrap();
        assert_eq!(got, req);
    }
}
