//! JoinGroup — API key 11.
//!
//! Versions 0..=9. Flexible (KIP-482) from v6. v5+ adds nullable
//! `group_instance_id` (KIP-345); v7+ adds nullable `protocol_type`
//! on the response; v8+ adds nullable `reason` on the request
//! (KIP-800); v9+ adds `skip_assignment` boolean on the response.
//!
//! Per-protocol `metadata` and per-member `metadata` are
//! **non-nullable** per Apache's schema (gh #96): empty bytes are
//! normal, null kills the Java client during rebalance.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_bytes, read_compact_bytes, read_i16, read_i32, write_bool, write_bytes,
    write_compact_bytes, write_i16, write_i32,
};
use crate::tagged;
use crate::Bytes;

// Min 2: v0 lacks `rebalance_timeout_ms`
// (added v1) and this module decodes it unconditionally, so
// advertising v0 offered a shape we never parsed correctly.
pub const VERSIONS: (i16, i16) = (2, 9);
pub const MIN_FLEXIBLE: i16 = 6;

pub const SPEC: ApiSpec = ApiSpec {
    key: 11,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    /// v5+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupProtocol>,
    /// v8+. `None` ↔ wire null.
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupProtocol {
    pub name: String,
    /// Non-nullable per Apache's schema. Empty bytes are normal.
    pub metadata: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v2+. `0` when unset.
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub generation_id: i32,
    /// v7+. Nullable per Apache; kaas encodes non-nullable (empty
    /// = absent).
    pub protocol_type: String,
    pub protocol_name: String,
    pub leader: String,
    /// v9+.
    pub skip_assignment: bool,
    pub member_id: String,
    pub members: Vec<JoinGroupMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinGroupMember {
    pub member_id: String,
    /// v5+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    /// Non-nullable per Apache's schema.
    pub metadata: Bytes,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request {
        group_id: read_str(buf, flexible)?,
        session_timeout_ms: read_i32(buf)?,
        rebalance_timeout_ms: read_i32(buf)?,
        member_id: read_str(buf, flexible)?,
        ..Request::default()
    };
    if version >= 5 {
        req.group_instance_id = read_nullable_str(buf, flexible)?;
    }
    req.protocol_type = read_str(buf, flexible)?;
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let name = read_str(buf, flexible)?;
        let metadata = if flexible {
            read_compact_bytes(buf)?
        } else {
            read_bytes(buf)?
        };
        if flexible {
            tagged::read(buf)?;
        }
        req.protocols.push(JoinGroupProtocol { name, metadata });
    }
    if version >= 8 {
        req.reason = read_nullable_str(buf, flexible)?;
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
    if version >= 2 {
        write_i32(buf, resp.throttle_time_ms);
    }
    write_i16(buf, resp.error_code);
    write_i32(buf, resp.generation_id);
    if version >= 7 {
        write_str(buf, &resp.protocol_type, flexible)?;
    }
    write_str(buf, &resp.protocol_name, flexible)?;
    write_str(buf, &resp.leader, flexible)?;
    if version >= 9 {
        write_bool(buf, resp.skip_assignment);
    }
    write_str(buf, &resp.member_id, flexible)?;
    write_array_len(buf, resp.members.len(), flexible)?;
    for m in &resp.members {
        write_str(buf, &m.member_id, flexible)?;
        if version >= 5 {
            write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
        }
        if flexible {
            write_compact_bytes(buf, &m.metadata)?;
        } else {
            write_bytes(buf, &m.metadata)?;
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
    if version >= 2 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    resp.error_code = read_i16(buf)?;
    resp.generation_id = read_i32(buf)?;
    if version >= 7 {
        resp.protocol_type = read_str(buf, flexible)?;
    }
    resp.protocol_name = read_str(buf, flexible)?;
    resp.leader = read_str(buf, flexible)?;
    if version >= 9 {
        resp.skip_assignment = read_bool(buf)?;
    }
    resp.member_id = read_str(buf, flexible)?;
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let member_id = read_str(buf, flexible)?;
        let group_instance_id = if version >= 5 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        let metadata = if flexible {
            read_compact_bytes(buf)?
        } else {
            read_bytes(buf)?
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.members.push(JoinGroupMember {
            member_id,
            group_instance_id,
            metadata,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    write_i32(buf, req.session_timeout_ms);
    write_i32(buf, req.rebalance_timeout_ms);
    write_str(buf, &req.member_id, flexible)?;
    if version >= 5 {
        write_nullable_str(buf, req.group_instance_id.as_deref(), flexible)?;
    }
    write_str(buf, &req.protocol_type, flexible)?;
    write_array_len(buf, req.protocols.len(), flexible)?;
    for p in &req.protocols {
        write_str(buf, &p.name, flexible)?;
        if flexible {
            write_compact_bytes(buf, &p.metadata)?;
        } else {
            write_bytes(buf, &p.metadata)?;
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    if version >= 8 {
        write_nullable_str(buf, req.reason.as_deref(), flexible)?;
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
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: "consumer-1".to_owned(),
            group_instance_id: if version >= 5 {
                Some("inst-1".to_owned())
            } else {
                None
            },
            protocol_type: "consumer".to_owned(),
            protocols: vec![JoinGroupProtocol {
                name: "range".to_owned(),
                metadata: Bytes::from_static(b"meta"),
            }],
            reason: if version >= 8 {
                Some("rebalance".to_owned())
            } else {
                None
            },
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            generation_id: 1,
            protocol_type: if version >= 7 {
                "consumer".to_owned()
            } else {
                String::new()
            },
            protocol_name: "range".to_owned(),
            leader: "consumer-1".to_owned(),
            skip_assignment: version >= 9,
            member_id: "consumer-1".to_owned(),
            members: vec![JoinGroupMember {
                member_id: "consumer-1".to_owned(),
                group_instance_id: if version >= 5 {
                    Some("inst-1".to_owned())
                } else {
                    None
                },
                metadata: Bytes::from_static(b"subscription"),
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
    fn v5_adds_group_instance_id() {
        roundtrip(5);
    }

    #[test]
    fn v6_is_flexible() {
        roundtrip(6);
    }

    #[test]
    fn v7_adds_protocol_type() {
        roundtrip(7);
    }

    #[test]
    fn v8_adds_reason() {
        roundtrip(8);
    }

    #[test]
    fn v9_adds_skip_assignment() {
        roundtrip(9);
    }
}
