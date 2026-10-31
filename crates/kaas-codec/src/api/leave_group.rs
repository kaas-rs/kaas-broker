//! LeaveGroup — API key 13.
//!
//! Versions 0..=5. Flexible (KIP-482) from v4. v3+ replaces the
//! single `member_id` request field with a `members[]` batch
//! (KIP-345); v1+ adds `throttle_time_ms` on the response; v5 adds
//! a per-member `reason` on the request (KIP-800 — the response is
//! unchanged from v4).
//!
//! The v5 `reason` field is implemented here per Apache Kafka 3.7
//! (v0.1 advertised 0..=4 and never carried it).

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 5);
pub const MIN_FLEXIBLE: i16 = 4;

pub const SPEC: ApiSpec = ApiSpec {
    key: 13,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_id: String,
    /// v0..=v2 single-member form. Empty at v3+.
    pub member_id: String,
    /// v3+ batch form.
    pub members: Vec<LeaveMember>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveMember {
    pub member_id: String,
    /// `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    /// v5+ (KIP-800). `None` ↔ wire null; absent below v5.
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// v3+.
    pub members: Vec<LeaveMemberResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaveMemberResponse {
    pub member_id: String,
    /// `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let group_id = read_str(buf, flexible)?;
    if version <= 2 {
        let member_id = read_str(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        return Ok(Request {
            group_id,
            member_id,
            members: Vec::new(),
        });
    }
    let n = read_array_len(buf, flexible)?;
    let mut members = Vec::with_capacity(n);
    for _ in 0..n {
        let member_id = read_str(buf, flexible)?;
        let group_instance_id = read_nullable_str(buf, flexible)?;
        let reason = if version >= 5 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        if flexible {
            tagged::read(buf)?;
        }
        members.push(LeaveMember {
            member_id,
            group_instance_id,
            reason,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        group_id,
        member_id: String::new(),
        members,
    })
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
    if version >= 3 {
        write_array_len(buf, resp.members.len(), flexible)?;
        for m in &resp.members {
            write_str(buf, &m.member_id, flexible)?;
            write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
            write_i16(buf, m.error_code);
            if flexible {
                tagged::write_empty(buf);
            }
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
    if version >= 1 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    resp.error_code = read_i16(buf)?;
    if version >= 3 {
        let n = read_array_len(buf, flexible)?;
        for _ in 0..n {
            let member_id = read_str(buf, flexible)?;
            let group_instance_id = read_nullable_str(buf, flexible)?;
            let error_code = read_i16(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            resp.members.push(LeaveMemberResponse {
                member_id,
                group_instance_id,
                error_code,
            });
        }
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    if version <= 2 {
        write_str(buf, &req.member_id, flexible)?;
        if flexible {
            tagged::write_empty(buf);
        }
        return Ok(());
    }
    write_array_len(buf, req.members.len(), flexible)?;
    for m in &req.members {
        write_str(buf, &m.member_id, flexible)?;
        write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
        if version >= 5 {
            write_nullable_str(buf, m.reason.as_deref(), flexible)?;
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
        let mut req = Request {
            group_id: "g1".to_owned(),
            ..Request::default()
        };
        if version <= 2 {
            req.member_id = "consumer-1".to_owned();
        } else {
            req.members = vec![
                LeaveMember {
                    member_id: "consumer-1".to_owned(),
                    group_instance_id: Some("inst-1".to_owned()),
                    reason: (version >= 5).then(|| "the consumer is being closed".to_owned()),
                },
                LeaveMember {
                    member_id: "consumer-2".to_owned(),
                    group_instance_id: None,
                    reason: None,
                },
            ];
        }
        req
    }

    fn sample_response(version: i16) -> Response {
        let mut resp = Response::default();
        if version >= 3 {
            resp.members = vec![LeaveMemberResponse {
                member_id: "consumer-1".to_owned(),
                group_instance_id: Some("inst-1".to_owned()),
                error_code: 0,
            }];
        }
        resp
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
    fn v0_single_member() {
        roundtrip(0);
    }

    #[test]
    fn v3_batch_members() {
        roundtrip(3);
    }

    #[test]
    fn v4_is_flexible() {
        roundtrip(4);
    }

    #[test]
    fn v5_roundtrip() {
        roundtrip(5);
    }

    /// Wire fixture: LeaveGroup v5 request as the Java client sends it
    /// (KIP-800 `reason` present, `group_instance_id` null). The
    /// pre-fix decoder read the reason's length byte as a tagged-field
    /// count and errored — every `kafka-console-consumer` close then
    /// got a bare error body back and threw "Buffer underflow while
    /// parsing response" + hung 30 s in LEAVE_GROUP (found live on
    /// v0.1.190, Phase 9 A.0).
    #[test]
    fn v5_java_client_fixture_with_reason() {
        let wire: &[u8] = &[
            0x03, b'g', b'1', // group_id: compact_string "g1"
            0x02, // members: compact array, 1 entry
            0x04, b'm', b'-', b'1', // member_id: "m-1"
            0x00, // group_instance_id: null
            0x04, b'b', b'y', b'e', // reason: "bye"
            0x00, // member tagged fields
            0x00, // top-level tagged fields
        ];
        let mut buf = Bytes::copy_from_slice(wire);
        let req = decode_request(&mut buf, 5).unwrap();
        assert!(buf.is_empty(), "entire fixture must be consumed");
        assert_eq!(req.group_id, "g1");
        assert_eq!(req.members.len(), 1);
        assert_eq!(req.members[0].member_id, "m-1");
        assert_eq!(req.members[0].group_instance_id, None);
        assert_eq!(req.members[0].reason.as_deref(), Some("bye"));

        // And the same bytes must round-trip out of encode_request.
        let mut out = BytesMut::new();
        encode_request(&mut out, &req, 5).unwrap();
        assert_eq!(&out[..], wire);
    }

    /// v4 must NOT carry the reason field (it's v5+ only).
    #[test]
    fn v4_omits_reason_on_the_wire() {
        let req = Request {
            group_id: "g1".to_owned(),
            member_id: String::new(),
            members: vec![LeaveMember {
                member_id: "m-1".to_owned(),
                group_instance_id: None,
                reason: Some("ignored at v4".to_owned()),
            }],
        };
        let mut out = BytesMut::new();
        encode_request(&mut out, &req, 4).unwrap();
        let mut r = out.freeze();
        let got = decode_request(&mut r, 4).unwrap();
        assert_eq!(got.members[0].reason, None);
    }
}
