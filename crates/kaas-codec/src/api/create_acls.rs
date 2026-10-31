//! CreateAcls — API key 30.
//!
//! Versions 0..=3. Flexible (KIP-482) from v2. v1+ adds
//! `pattern_type` on each creation (KIP-290).
//!
//! Key-30 slice of the ACL admin surface (gh #107).

use bytes::BytesMut;

use crate::api::acl_types::{read_entry_tags, write_entry_tags, AclBinding};
use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i8, write_i16, write_i32, write_i8};
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 30,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub creations: Vec<AclBinding>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<CreateAclsResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CreateAclsResult {
    pub error_code: i16,
    /// `None` ↔ wire null.
    pub error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let n = read_array_len(buf, flexible)?;
    let mut creations = Vec::with_capacity(n);
    for _ in 0..n {
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        let pattern_type = if version >= 1 { read_i8(buf)? } else { 0 };
        let principal = read_str(buf, flexible)?;
        let host = read_str(buf, flexible)?;
        let operation = read_i8(buf)?;
        let permission = read_i8(buf)?;
        read_entry_tags(buf, flexible)?;
        creations.push(AclBinding {
            resource_type,
            resource_name,
            pattern_type,
            principal,
            host,
            operation,
            permission,
        });
    }
    read_entry_tags(buf, flexible)?;
    Ok(Request { creations })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.creations.len(), flexible)?;
    for b in &req.creations {
        write_i8(buf, b.resource_type);
        write_str(buf, &b.resource_name, flexible)?;
        if version >= 1 {
            write_i8(buf, b.pattern_type);
        }
        write_str(buf, &b.principal, flexible)?;
        write_str(buf, &b.host, flexible)?;
        write_i8(buf, b.operation);
        write_i8(buf, b.permission);
        write_entry_tags(buf, flexible);
    }
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
    write_array_len(buf, resp.results.len(), flexible)?;
    for r in &resp.results {
        write_i16(buf, r.error_code);
        write_nullable_str(buf, r.error_message.as_deref(), flexible)?;
        write_entry_tags(buf, flexible);
    }
    write_entry_tags(buf, flexible);
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = read_i32(buf)?;
    let n = read_array_len(buf, flexible)?;
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, flexible)?;
        read_entry_tags(buf, flexible)?;
        results.push(CreateAclsResult {
            error_code,
            error_message,
        });
    }
    read_entry_tags(buf, flexible)?;
    Ok(Response {
        throttle_time_ms,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::acl_types::{operation, pattern_type, permission, resource_type};

    fn sample_request(version: i16) -> Request {
        Request {
            creations: vec![AclBinding {
                resource_type: resource_type::TOPIC,
                resource_name: "orders".into(),
                pattern_type: if version >= 1 {
                    pattern_type::LITERAL
                } else {
                    0
                },
                principal: "User:alice".into(),
                host: "*".into(),
                operation: operation::WRITE,
                permission: permission::ALLOW,
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

        let resp = Response {
            throttle_time_ms: 0,
            results: vec![
                CreateAclsResult::default(),
                CreateAclsResult {
                    error_code: 42,
                    error_message: Some("boom".into()),
                },
            ],
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
