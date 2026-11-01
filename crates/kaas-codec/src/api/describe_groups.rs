//! DescribeGroups — API key 15.
//!
//! Versions 0..=5. Flexible (KIP-482) from v5. v1+ adds
//! `throttle_time_ms`; v3+ adds `include_authorized_operations` on the
//! request + `authorized_operations` per group; v4+ adds nullable
//! `group_instance_id` per member (KIP-345).
//!
//! Per-member `metadata` and `assignment` are **non-nullable** per
//! Apache's schema (gh #96): the Java AdminClient throws on null
//! markers during describe.

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

pub const VERSIONS: (i16, i16) = (0, 5);
pub const MIN_FLEXIBLE: i16 = 5;

pub const SPEC: ApiSpec = ApiSpec {
    key: 15,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub groups: Vec<String>,
    /// v3+. False on legacy versions.
    pub include_authorized_operations: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroup {
    pub error_code: i16,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribedGroupMember>,
    /// v3+. `0` when unset.
    pub authorized_operations: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribedGroupMember {
    pub member_id: String,
    /// v4+. `None` ↔ wire null.
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    /// Non-nullable per Apache's schema.
    pub member_metadata: Bytes,
    /// Non-nullable per Apache's schema.
    pub member_assignment: Bytes,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        req.groups.push(read_str(buf, flexible)?);
    }
    if version >= 3 {
        req.include_authorized_operations = read_bool(buf)?;
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
    write_array_len(buf, resp.groups.len(), flexible)?;
    for g in &resp.groups {
        write_i16(buf, g.error_code);
        write_str(buf, &g.group_id, flexible)?;
        write_str(buf, &g.group_state, flexible)?;
        write_str(buf, &g.protocol_type, flexible)?;
        write_str(buf, &g.protocol_data, flexible)?;
        write_array_len(buf, g.members.len(), flexible)?;
        for m in &g.members {
            write_str(buf, &m.member_id, flexible)?;
            if version >= 4 {
                write_nullable_str(buf, m.group_instance_id.as_deref(), flexible)?;
            }
            write_str(buf, &m.client_id, flexible)?;
            write_str(buf, &m.client_host, flexible)?;
            if flexible {
                write_compact_bytes(buf, &m.member_metadata)?;
                write_compact_bytes(buf, &m.member_assignment)?;
                tagged::write_empty(buf);
            } else {
                write_bytes(buf, &m.member_metadata)?;
                write_bytes(buf, &m.member_assignment)?;
            }
        }
        if version >= 3 {
            write_i32(buf, g.authorized_operations);
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
    if version >= 1 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let error_code = read_i16(buf)?;
        let group_id = read_str(buf, flexible)?;
        let group_state = read_str(buf, flexible)?;
        let protocol_type = read_str(buf, flexible)?;
        let protocol_data = read_str(buf, flexible)?;
        let mut members = Vec::new();
        let nm = read_array_len(buf, flexible)?;
        for _ in 0..nm {
            let member_id = read_str(buf, flexible)?;
            let group_instance_id = if version >= 4 {
                read_nullable_str(buf, flexible)?
            } else {
                None
            };
            let client_id = read_str(buf, flexible)?;
            let client_host = read_str(buf, flexible)?;
            let (member_metadata, member_assignment) = if flexible {
                let md = read_compact_bytes(buf)?;
                let asg = read_compact_bytes(buf)?;
                tagged::read(buf)?;
                (md, asg)
            } else {
                (read_bytes(buf)?, read_bytes(buf)?)
            };
            members.push(DescribedGroupMember {
                member_id,
                group_instance_id,
                client_id,
                client_host,
                member_metadata,
                member_assignment,
            });
        }
        // Apache's schema default below v3 is INT_MIN ("not computed").
        let authorized_operations = if version >= 3 {
            read_i32(buf)?
        } else {
            i32::MIN
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.groups.push(DescribedGroup {
            error_code,
            group_id,
            group_state,
            protocol_type,
            protocol_data,
            members,
            authorized_operations,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.groups.len(), flexible)?;
    for g in &req.groups {
        write_str(buf, g, flexible)?;
    }
    if version >= 3 {
        write_bool(buf, req.include_authorized_operations);
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
            groups: vec!["g1".to_owned(), "g2".to_owned()],
            include_authorized_operations: version >= 3,
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: 0,
                group_id: "g1".to_owned(),
                group_state: "Stable".to_owned(),
                protocol_type: "consumer".to_owned(),
                protocol_data: "range".to_owned(),
                members: vec![DescribedGroupMember {
                    member_id: "consumer-1".to_owned(),
                    group_instance_id: if version >= 4 {
                        Some("inst-1".to_owned())
                    } else {
                        None
                    },
                    client_id: "rdkafka".to_owned(),
                    client_host: "/127.0.0.1".to_owned(),
                    member_metadata: Bytes::from_static(b"meta"),
                    member_assignment: Bytes::from_static(b"asg"),
                }],
                authorized_operations: if version >= 3 { 328 } else { i32::MIN },
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
    fn v0_roundtrip() {
        roundtrip(0);
    }

    #[test]
    fn v3_adds_authorized_ops() {
        roundtrip(3);
    }

    #[test]
    fn v4_adds_group_instance_id() {
        roundtrip(4);
    }

    #[test]
    fn v5_is_flexible() {
        roundtrip(5);
    }
}
