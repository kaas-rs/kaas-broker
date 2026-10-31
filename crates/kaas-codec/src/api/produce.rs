//! Produce — API key 0. v3–v9, flexible from v9 (KIP-482).
//!
//!
//! # Byte opacity
//!
//! [`PartitionData::records`] carries the raw v2 RecordBatch payload as
//! a zero-copy [`Bytes`] sub-slice into the request frame. The codec
//! never decodes records here; the storage engine writes them straight
//! to a segment file. A reviewer who proposes changing this to a
//! `Vec<Record>` is breaking the byte-opacity invariant called out in
//! the crate-root doc.
//!
//! # Version map
//!
//! | v | Adds                                                |
//! |--:|-----------------------------------------------------|
//! | 3 | `transactional_id` request field                    |
//! | 5 | `log_start_offset` response field                   |
//! | 8 | `record_errors` + `error_message` response fields   |
//! | 9 | KIP-482 flexible encoding (compact arrays/strings + tagged fields) |

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_bytes, read_nullable_str, read_str, write_array_len,
    write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (3, 9);
pub const MIN_FLEXIBLE: i16 = 9;

pub const SPEC: ApiSpec = ApiSpec {
    key: 0,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// v3+. `None` on the wire null; `Some("")` for an explicit empty
    /// transactional id (rare — the Java client sends null for
    /// non-transactional producers).
    pub transactional_id: Option<String>,
    /// `-1` = all, `0` = none, `1` = leader.
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<TopicData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicData {
    pub name: String,
    pub partition_data: Vec<PartitionData>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionData {
    pub index: i32,
    /// Opaque v2 RecordBatch bytes. `None` if the client sent a null
    /// records block (legal but unusual — produces a no-op).
    pub records: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub responses: Vec<TopicResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicResponse {
    pub name: String,
    pub partition_responses: Vec<PartitionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionResponse {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    /// v2+. `-1` when not available (kaas never sets log-append
    /// time, so this is always `-1` on the wire).
    pub log_append_time_ms: i64,
    /// v5+. The partition's current `log_start_offset` so the client
    /// can detect retention catch-up.
    pub log_start_offset: i64,
    /// v8+. Per-record errors when `error_code != 0` carries record
    /// granularity. Kaas does not emit these (always empty).
    pub record_errors: Vec<RecordError>,
    /// v8+. Human-readable detail for `error_code`. `None` ↔ wire null.
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordError {
    pub batch_index: i32,
    pub batch_index_error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let transactional_id = if version >= 3 {
        read_nullable_str(buf, flexible)?
    } else {
        None
    };
    let acks = read_i16(buf)?;
    let timeout_ms = read_i32(buf)?;

    let topic_count = read_array_len(buf, flexible)?;
    let mut topic_data = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partition_data = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let index = read_i32(buf)?;
            let records = read_nullable_bytes(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            partition_data.push(PartitionData { index, records });
        }
        if flexible {
            tagged::read(buf)?;
        }
        topic_data.push(TopicData {
            name,
            partition_data,
        });
    }

    if flexible {
        tagged::read(buf)?;
    }

    Ok(Request {
        transactional_id,
        acks,
        timeout_ms,
        topic_data,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    write_array_len(buf, resp.responses.len(), flexible)?;
    for t in &resp.responses {
        write_str(buf, &t.name, flexible)?;
        write_array_len(buf, t.partition_responses.len(), flexible)?;
        for p in &t.partition_responses {
            write_i32(buf, p.index);
            write_i16(buf, p.error_code);
            write_i64(buf, p.base_offset);
            if version >= 2 {
                write_i64(buf, p.log_append_time_ms);
            }
            if version >= 5 {
                write_i64(buf, p.log_start_offset);
            }
            if version >= 8 {
                write_array_len(buf, p.record_errors.len(), flexible)?;
                for re in &p.record_errors {
                    write_i32(buf, re.batch_index);
                    write_nullable_str(buf, re.batch_index_error_message.as_deref(), flexible)?;
                    if flexible {
                        tagged::write_empty(buf);
                    }
                }
                write_nullable_str(buf, p.error_message.as_deref(), flexible)?;
            }
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }

    if version >= 1 {
        write_i32(buf, resp.throttle_time_ms);
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let topic_count = read_array_len(buf, flexible)?;
    let mut responses = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let name = read_str(buf, flexible)?;
        let part_count = read_array_len(buf, flexible)?;
        let mut partition_responses = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let index = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            let base_offset = read_i64(buf)?;
            let log_append_time_ms = if version >= 2 { read_i64(buf)? } else { -1 };
            let log_start_offset = if version >= 5 { read_i64(buf)? } else { -1 };
            let (record_errors, error_message) = if version >= 8 {
                let re_count = read_array_len(buf, flexible)?;
                let mut record_errors = Vec::with_capacity(re_count);
                for _ in 0..re_count {
                    let batch_index = read_i32(buf)?;
                    let batch_index_error_message = read_nullable_str(buf, flexible)?;
                    if flexible {
                        tagged::read(buf)?;
                    }
                    record_errors.push(RecordError {
                        batch_index,
                        batch_index_error_message,
                    });
                }
                let error_message = read_nullable_str(buf, flexible)?;
                (record_errors, error_message)
            } else {
                (Vec::new(), None)
            };
            if flexible {
                tagged::read(buf)?;
            }
            partition_responses.push(PartitionResponse {
                index,
                error_code,
                base_offset,
                log_append_time_ms,
                log_start_offset,
                record_errors,
                error_message,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        responses.push(TopicResponse {
            name,
            partition_responses,
        });
    }

    let throttle_time_ms = if version >= 1 { read_i32(buf)? } else { 0 };
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        responses,
        throttle_time_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::common::{write_nullable_bytes, write_nullable_str};
    use crate::primitives::{write_i16 as wi16, write_i32 as wi32};

    fn sample_request(version: i16) -> Request {
        Request {
            transactional_id: if version >= 3 {
                Some("tx-7".to_owned())
            } else {
                None
            },
            acks: -1,
            timeout_ms: 30_000,
            topic_data: vec![
                TopicData {
                    name: "events".to_owned(),
                    partition_data: vec![
                        PartitionData {
                            index: 0,
                            records: Some(Bytes::from_static(&[0xaa, 0xbb, 0xcc])),
                        },
                        PartitionData {
                            index: 1,
                            records: None,
                        },
                    ],
                },
                TopicData {
                    name: "audit".to_owned(),
                    partition_data: vec![PartitionData {
                        index: 0,
                        records: Some(Bytes::from_static(&[0xde, 0xad, 0xbe, 0xef])),
                    }],
                },
            ],
        }
    }

    fn sample_response(version: i16) -> Response {
        let mut p = PartitionResponse {
            index: 0,
            error_code: 0,
            base_offset: 42,
            log_append_time_ms: -1,
            // The decoder reports `-1` for fields not present on the
            // wire; round-trip tests at this version must match.
            log_start_offset: if version >= 5 { 0 } else { -1 },
            record_errors: Vec::new(),
            error_message: None,
        };
        if version >= 8 {
            p.record_errors.push(RecordError {
                batch_index: 1,
                batch_index_error_message: Some("bad record".to_owned()),
            });
            p.error_message = Some("partial".to_owned());
            p.error_code = 1;
        }
        Response {
            responses: vec![TopicResponse {
                name: "events".to_owned(),
                partition_responses: vec![p],
            }],
            throttle_time_ms: 0,
        }
    }

    fn encode_request(req: &Request, version: i16) -> BytesMut {
        // Re-encoder used only by the tests in this file — the broker
        // never serialises a Produce request, so this is intentionally
        // kept out of the public surface (matches api_versions.rs).
        let flexible = version >= MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        if version >= 3 {
            write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        }
        wi16(&mut w, req.acks);
        wi32(&mut w, req.timeout_ms);
        write_array_len(&mut w, req.topic_data.len(), flexible).unwrap();
        for t in &req.topic_data {
            write_str(&mut w, &t.name, flexible).unwrap();
            write_array_len(&mut w, t.partition_data.len(), flexible).unwrap();
            for p in &t.partition_data {
                wi32(&mut w, p.index);
                write_nullable_bytes(&mut w, p.records.as_deref(), flexible).unwrap();
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
        assert_eq!(got, req, "request roundtrip v{version}");
        assert!(r.is_empty(), "stray bytes after request v{version}");
    }

    fn roundtrip_response(version: i16) {
        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response roundtrip v{version}");
        assert!(r.is_empty(), "stray bytes after response v{version}");
    }

    #[test]
    fn request_v3_roundtrip() {
        roundtrip_request(3);
    }

    #[test]
    fn request_v5_roundtrip() {
        roundtrip_request(5);
    }

    #[test]
    fn request_v8_roundtrip() {
        roundtrip_request(8);
    }

    #[test]
    fn request_v9_flexible_roundtrip() {
        roundtrip_request(9);
    }

    #[test]
    fn response_v3_roundtrip() {
        roundtrip_response(3);
    }

    #[test]
    fn response_v5_log_start_offset_present() {
        roundtrip_response(5);
    }

    #[test]
    fn response_v8_record_errors_present() {
        roundtrip_response(8);
    }

    #[test]
    fn response_v9_flexible_roundtrip() {
        roundtrip_response(9);
    }

    #[test]
    fn records_are_byte_opaque() {
        let req = sample_request(9);
        let w = encode_request(&req, 9);
        let mut r = w.freeze();
        let got = decode_request(&mut r, 9).unwrap();
        // The records Bytes round-trip unchanged — the decoder did not
        // peek inside the batch payload at any point.
        let original = req.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .unwrap();
        let echoed = got.topic_data[0].partition_data[0]
            .records
            .as_ref()
            .unwrap();
        assert_eq!(original, echoed);
    }

    /// gh #272: v3–v8 responses have no tagged-fields byte in the
    /// header; this test used to assert they did.
    #[test]
    fn header_versions() {
        use crate::headers::HeaderVersion;
        assert_eq!(SPEC.request_header(3), HeaderVersion::V1);
        assert_eq!(SPEC.request_header(8), HeaderVersion::V1);
        assert_eq!(SPEC.request_header(9), HeaderVersion::V2);
        assert_eq!(SPEC.response_header(3), HeaderVersion::V0);
        assert_eq!(SPEC.response_header(8), HeaderVersion::V0);
        assert_eq!(SPEC.response_header(9), HeaderVersion::V1);
    }
}
