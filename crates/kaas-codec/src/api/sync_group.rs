//! SyncGroup — API key 14.
//!
//! Versions 0..=5. Flexible (KIP-482) from v4. v3+ adds nullable
//! `group_instance_id` on the request (KIP-345); v5+ adds nullable
//! `protocol_type` + `protocol_name` on both directions.
//!
//! Per-member `assignment` bytes are **non-nullable** per Apache's
//! schema (gh #96): empty bytes are normal, but the null sentinel
//! kills the Java consumer's generated decoder. The Rust types model
//! the assignment as `Bytes` (not `Option<Bytes>`); callers pass
//! `Bytes::new()` for empty.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bytes, read_compact_bytes, read_i16, read_i32, write_bytes, write_compact_bytes,
    write_i16, write_i32,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 5);
pub const MIN_FLEXIBLE: i16 = 4;

pub const SPEC: ApiSpec = ApiSpec {
    key: 14,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    /// v3+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    /// v5+. `None` ↔ wire null.
    pub protocol_type: Option<String>,
    /// v5+. `None` ↔ wire null.
    pub protocol_name: Option<String>,
    pub assignments: Vec<SyncAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncAssignment {
    pub member_id: String,
    /// Byte-opaque client-side partition-assignor blob. May be empty.
    /// `None` ↔ wire null (request-side nullable per Apache's schema).
    pub assignment: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// v5+. Apache marks this nullable but kaas encodes
    /// non-nullable (gh #96) — empty string is the "absent" form.
    pub protocol_type: String,
    /// v5+. Same non-nullable convention.
    pub protocol_name: String,
    /// **Non-nullable** per Apache's schema — empty bytes for error
    /// responses, not null.
    pub assignment: Bytes,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request {
        group_id: read_str(buf, flexible)?,
        generation_id: read_i32(buf)?,
        member_id: read_str(buf, flexible)?,
        ..Request::default()
    };
    if version >= 3 {
        req.group_instance_id = read_nullable_str(buf, flexible)?;
    }
    if version >= 5 {
        req.protocol_type = read_nullable_str(buf, flexible)?;
        req.protocol_name = read_nullable_str(buf, flexible)?;
    }
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let member_id = read_str(buf, flexible)?;
        let assignment = crate::api::common::read_nullable_bytes(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        req.assignments.push(SyncAssignment {
            member_id,
            assignment,
        });
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
    if version >= 1 {
        write_i32(buf, resp.throttle_time_ms);
    }
    write_i16(buf, resp.error_code);
    if version >= 5 {
        write_str(buf, &resp.protocol_type, flexible)?;
        write_str(buf, &resp.protocol_name, flexible)?;
    }
    if flexible {
        write_compact_bytes(buf, &resp.assignment)?;
        tagged::write_empty(buf);
    } else {
        write_bytes(buf, &resp.assignment)?;
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut resp = Response::default();
    if version >= 1 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    resp.error_code = read_i16(buf)?;
    if version >= 5 {
        resp.protocol_type = read_str(buf, flexible)?;
        resp.protocol_name = read_str(buf, flexible)?;
    }
    resp.assignment = if flexible {
        read_compact_bytes(buf)?
    } else {
        read_bytes(buf)?
    };
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    write_i32(buf, req.generation_id);
    write_str(buf, &req.member_id, flexible)?;
    if version >= 3 {
        write_nullable_str(buf, req.group_instance_id.as_deref(), flexible)?;
    }
    if version >= 5 {
        write_nullable_str(buf, req.protocol_type.as_deref(), flexible)?;
        write_nullable_str(buf, req.protocol_name.as_deref(), flexible)?;
    }
    write_array_len(buf, req.assignments.len(), flexible)?;
    for a in &req.assignments {
        write_str(buf, &a.member_id, flexible)?;
        crate::api::common::write_nullable_bytes(buf, a.assignment.as_deref(), flexible)?;
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
            generation_id: 7,
            member_id: "consumer-1".to_owned(),
            group_instance_id: if version >= 3 {
                Some("inst-1".to_owned())
            } else {
                None
            },
            protocol_type: if version >= 5 {
                Some("consumer".to_owned())
            } else {
                None
            },
            protocol_name: if version >= 5 {
                Some("range".to_owned())
            } else {
                None
            },
            assignments: vec![SyncAssignment {
                member_id: "consumer-1".to_owned(),
                assignment: Some(Bytes::from_static(b"\x00\x01\x02")),
            }],
        }
    }

    fn sample_response(version: i16) -> Response {
        let (protocol_type, protocol_name) = if version >= 5 {
            ("consumer".to_owned(), "range".to_owned())
        } else {
            (String::new(), String::new())
        };
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            protocol_type,
            protocol_name,
            assignment: Bytes::from_static(b"\xde\xad\xbe\xef"),
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
    fn v0_roundtrip() {
        roundtrip(0);
    }

    #[test]
    fn v3_adds_group_instance_id() {
        roundtrip(3);
    }

    #[test]
    fn v4_is_flexible() {
        roundtrip(4);
    }

    #[test]
    fn v5_adds_protocol_fields() {
        roundtrip(5);
    }

    #[test]
    fn empty_assignment_is_non_null() {
        // gh #96: the assignment field is non-nullable per Apache's
        // schema. Empty bytes must round trip as Bytes::new(), never
        // as None / -1 length.
        let resp = Response {
            throttle_time_ms: 0,
            error_code: 25, // UNKNOWN_MEMBER_ID
            protocol_type: String::new(),
            protocol_name: String::new(),
            assignment: Bytes::new(),
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 4).unwrap();
        assert_eq!(got, resp);
        assert!(got.assignment.is_empty());
    }
}
