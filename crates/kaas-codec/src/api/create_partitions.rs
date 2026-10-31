//! CreatePartitions — API key 37.
//!
//! Versions 0..=3. Flexible (KIP-482) from v2. Used by
//! `AdminClient.createPartitions()` and `kafka-topics.sh --alter
//! --partitions N`. Bumps the partition count on an existing topic
//! (decreases are rejected by the operator-side reconciler).
//!
//! Per-topic optional `assignments` field carries explicit replica
//! assignments for the new partitions. kaas uses single-writer-per-
//! partition (no replication), so the field is parsed for protocol
//! fidelity but unused at the handler.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_nullable_str, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_bool, read_i16, read_i32, write_bool, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 37,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub topics: Vec<CreatePartitionsTopic>,
    /// `timeout.ms` — how long the broker waits for the new
    /// partition dirs to materialise before returning.
    pub timeout_ms: i32,
    /// v1+. When `true`, run validation and return the would-be
    /// error codes without mutating state.
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopic {
    pub name: String,
    /// New total partition count (NOT a delta).
    pub count: i32,
    /// `None` ↔ wire null — let the controller pick replica assignments.
    /// `Some(rows)` with one inner Vec per new partition, listing
    /// broker IDs.
    pub assignments: Option<Vec<Vec<i32>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<CreatePartitionsTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatePartitionsTopicResult {
    pub name: String,
    pub error_code: i16,
    /// v1+. Operator-side reason on failure, e.g.
    /// `"reducing partition count is not supported"`. `None` ↔ wire null.
    pub error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let name = read_str(buf, flexible)?;
        let count = read_i32(buf)?;
        // assignments — `Option<Vec<Vec<i32>>>`.
        let outer = read_array_len_nullable(buf, flexible)?;
        let assignments = match outer {
            None => None,
            Some(m) => {
                let mut rows = Vec::with_capacity(m);
                for _ in 0..m {
                    let inner = read_array_len(buf, flexible)?;
                    let mut row = Vec::with_capacity(inner);
                    for _ in 0..inner {
                        row.push(read_i32(buf)?);
                    }
                    if flexible {
                        tagged::read(buf)?;
                    }
                    rows.push(row);
                }
                Some(rows)
            }
        };
        if flexible {
            tagged::read(buf)?;
        }
        req.topics.push(CreatePartitionsTopic {
            name,
            count,
            assignments,
        });
    }
    req.timeout_ms = read_i32(buf)?;
    req.validate_only = read_bool(buf)?;
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
    write_i32(buf, resp.throttle_time_ms);
    write_array_len(buf, resp.results.len(), flexible)?;
    for r in &resp.results {
        write_str(buf, &r.name, flexible)?;
        write_i16(buf, r.error_code);
        if version >= 1 {
            crate::api::common::write_nullable_str(buf, r.error_message.as_deref(), flexible)?;
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
    let mut resp = Response {
        throttle_time_ms: read_i32(buf)?,
        ..Response::default()
    };
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let name = read_str(buf, flexible)?;
        let error_code = read_i16(buf)?;
        let error_message = if version >= 1 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.results.push(CreatePartitionsTopicResult {
            name,
            error_code,
            error_message,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.topics.len(), flexible)?;
    for t in &req.topics {
        write_str(buf, &t.name, flexible)?;
        write_i32(buf, t.count);
        match &t.assignments {
            None => write_array_len_nullable(buf, None, flexible)?,
            Some(rows) => {
                write_array_len_nullable(buf, Some(rows.len()), flexible)?;
                for row in rows {
                    write_array_len(buf, row.len(), flexible)?;
                    for id in row {
                        write_i32(buf, *id);
                    }
                    if flexible {
                        tagged::write_empty(buf);
                    }
                }
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_i32(buf, req.timeout_ms);
    write_bool(buf, req.validate_only);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

// Nullable-array length: Apache uses `-1` for legacy null, `0` for
// compact null. The non-nullable variants in `common.rs` reject
// `-1`, so we open-code the nullable forms here. (Used only by
// CreatePartitions' `assignments` field today; promote to
// `common.rs` if a second caller appears.)
fn read_array_len_nullable(buf: &mut Bytes, flexible: bool) -> Result<Option<usize>, CodecError> {
    use crate::primitives::{read_i32, read_uvarint};
    if flexible {
        let raw = read_uvarint(buf)?;
        if raw == 0 {
            Ok(None)
        } else {
            let dec = raw - 1;
            usize::try_from(dec)
                .map(Some)
                .map_err(|_| CodecError::LengthOverflow)
        }
    } else {
        let raw = read_i32(buf)?;
        if raw < 0 {
            Ok(None)
        } else {
            usize::try_from(raw)
                .map(Some)
                .map_err(|_| CodecError::LengthOverflow)
        }
    }
}

fn write_array_len_nullable(
    buf: &mut BytesMut,
    count: Option<usize>,
    flexible: bool,
) -> Result<(), CodecError> {
    use crate::primitives::{write_i32, write_uvarint};
    match count {
        None => {
            if flexible {
                write_uvarint(buf, 0);
            } else {
                write_i32(buf, -1);
            }
        }
        Some(n) => {
            if flexible {
                let v = u64::try_from(n)
                    .map_err(|_| CodecError::LengthOverflow)?
                    .checked_add(1)
                    .ok_or(CodecError::LengthOverflow)?;
                write_uvarint(buf, v);
            } else {
                let v = i32::try_from(n).map_err(|_| CodecError::LengthOverflow)?;
                write_i32(buf, v);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> Request {
        Request {
            topics: vec![
                CreatePartitionsTopic {
                    name: "events".into(),
                    count: 12,
                    assignments: None,
                },
                CreatePartitionsTopic {
                    name: "with-assign".into(),
                    count: 4,
                    assignments: Some(vec![vec![0, 1, 2], vec![1, 2, 0]]),
                },
            ],
            timeout_ms: 30_000,
            validate_only: false,
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            results: vec![
                CreatePartitionsTopicResult {
                    name: "events".into(),
                    error_code: 0,
                    error_message: None,
                },
                CreatePartitionsTopicResult {
                    name: "with-assign".into(),
                    error_code: 37, // INVALID_PARTITIONS
                    error_message: if version >= 1 {
                        Some("reducing partition count is not supported".into())
                    } else {
                        None
                    },
                },
            ],
        }
    }

    fn roundtrip(version: i16) {
        let req = sample_request();
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty(), "trailing bytes on request v{version}");

        let resp = sample_response(version);
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
    fn v1_carries_error_message() {
        roundtrip(1);
    }

    #[test]
    fn v2_is_flexible() {
        roundtrip(2);
    }

    #[test]
    fn v3_roundtrip() {
        roundtrip(3);
    }

    #[test]
    fn validate_only_round_trips() {
        let req = Request {
            topics: vec![CreatePartitionsTopic {
                name: "t".into(),
                count: 3,
                assignments: None,
            }],
            timeout_ms: 10_000,
            validate_only: true,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 3).unwrap();
        let got = decode_request(&mut w.freeze(), 3).unwrap();
        assert!(got.validate_only);
    }
}
