//! DescribeAcls — API key 29.
//!
//! Versions 0..=3. Flexible (KIP-482) from v2. v1+ adds the
//! `pattern_type` axis (KIP-290).
//!
//! Key-29 slice of the ACL admin surface (gh #107).

use bytes::BytesMut;

use crate::api::acl_types::{
    read_entry_tags, read_filter, write_entry_tags, write_filter, AclFilter,
};
use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i8, write_i16, write_i32, write_i8};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 29,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub filter: AclFilter,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// `None` ↔ wire null.
    pub error_message: Option<String>,
    pub resources: Vec<DescribeAclsResource>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DescribeAclsResource {
    pub resource_type: i8,
    pub resource_name: String,
    /// v1+.
    pub pattern_type: i8,
    pub acls: Vec<MatchingAcl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MatchingAcl {
    pub principal: String,
    pub host: String,
    pub operation: i8,
    pub permission: i8,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let filter = read_filter(buf, version, flexible)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request { filter })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_filter(buf, &req.filter, version, flexible)?;
    write_entry_tags(buf, flexible);
    Ok(())
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_i32(buf, resp.throttle_time_ms);
    write_i16(buf, resp.error_code);
    write_nullable_str(buf, resp.error_message.as_deref(), flexible)?;
    write_array_len(buf, resp.resources.len(), flexible)?;
    for res in &resp.resources {
        write_i8(buf, res.resource_type);
        write_str(buf, &res.resource_name, flexible)?;
        if version >= 1 {
            write_i8(buf, res.pattern_type);
        }
        write_array_len(buf, res.acls.len(), flexible)?;
        for a in &res.acls {
            write_str(buf, &a.principal, flexible)?;
            write_str(buf, &a.host, flexible)?;
            write_i8(buf, a.operation);
            write_i8(buf, a.permission);
            write_entry_tags(buf, flexible);
        }
        write_entry_tags(buf, flexible);
    }
    write_entry_tags(buf, flexible);
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = read_i32(buf)?;
    let error_code = read_i16(buf)?;
    let error_message = read_nullable_str(buf, flexible)?;
    let n = read_array_len(buf, flexible)?;
    let mut resources = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        let pattern_type = if version >= 1 { read_i8(buf)? } else { 0 };
        let na = read_array_len(buf, flexible)?;
        let mut acls = Vec::with_capacity(na);
        for _ in 0..na {
            let principal = read_str(buf, flexible)?;
            let host = read_str(buf, flexible)?;
            let operation = read_i8(buf)?;
            let permission = read_i8(buf)?;
            read_entry_tags(buf, flexible)?;
            acls.push(MatchingAcl {
                principal,
                host,
                operation,
                permission,
            });
        }
        read_entry_tags(buf, flexible)?;
        resources.push(DescribeAclsResource {
            resource_type,
            resource_name,
            pattern_type,
            acls,
        });
    }
    read_entry_tags(buf, flexible)?;
    Ok(Response {
        throttle_time_ms,
        error_code,
        error_message,
        resources,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::acl_types::{operation, pattern_type, permission, resource_type};

    fn sample_request(version: i16) -> Request {
        Request {
            filter: AclFilter {
                resource_type_filter: resource_type::TOPIC,
                resource_name_filter: Some("orders".into()),
                pattern_type_filter: if version >= 1 { pattern_type::ANY } else { 0 },
                principal_filter: None,
                host_filter: None,
                operation: operation::ANY,
                permission_type: permission::ANY,
            },
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            resources: vec![DescribeAclsResource {
                resource_type: resource_type::TOPIC,
                resource_name: "orders".into(),
                pattern_type: if version >= 1 {
                    pattern_type::LITERAL
                } else {
                    0
                },
                acls: vec![MatchingAcl {
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: operation::READ,
                    permission: permission::ALLOW,
                }],
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
    fn v1_adds_pattern_type() {
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
}
