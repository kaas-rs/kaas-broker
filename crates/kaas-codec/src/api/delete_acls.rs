//! DeleteAcls — API key 31.
//!
//! Versions 0..=3. Flexible (KIP-482) from v2. v1+ adds
//! `pattern_type` on filters and matching-ACL rows (KIP-290).
//!
//! Key-31 slice of the ACL admin surface (gh #107).

use bytes::BytesMut;

use crate::api::acl_types::{
    read_entry_tags, read_filter, write_entry_tags, write_filter, AclBinding, AclFilter,
};
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
    key: 31,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub filters: Vec<AclFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub filter_results: Vec<DeleteAclsFilterResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteAclsFilterResult {
    pub error_code: i16,
    /// `None` ↔ wire null.
    pub error_message: Option<String>,
    pub matching_acls: Vec<DeleteAclsMatchingAcl>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeleteAclsMatchingAcl {
    pub error_code: i16,
    /// `None` ↔ wire null.
    pub error_message: Option<String>,
    pub binding: AclBinding,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let n = read_array_len(buf, flexible)?;
    let mut filters = Vec::with_capacity(n);
    for _ in 0..n {
        let f = read_filter(buf, version, flexible)?;
        read_entry_tags(buf, flexible)?;
        filters.push(f);
    }
    read_entry_tags(buf, flexible)?;
    Ok(Request { filters })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.filters.len(), flexible)?;
    for f in &req.filters {
        write_filter(buf, f, version, flexible)?;
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
    write_array_len(buf, resp.filter_results.len(), flexible)?;
    for fr in &resp.filter_results {
        write_i16(buf, fr.error_code);
        write_nullable_str(buf, fr.error_message.as_deref(), flexible)?;
        write_array_len(buf, fr.matching_acls.len(), flexible)?;
        for m in &fr.matching_acls {
            write_i16(buf, m.error_code);
            write_nullable_str(buf, m.error_message.as_deref(), flexible)?;
            write_i8(buf, m.binding.resource_type);
            write_str(buf, &m.binding.resource_name, flexible)?;
            if version >= 1 {
                write_i8(buf, m.binding.pattern_type);
            }
            write_str(buf, &m.binding.principal, flexible)?;
            write_str(buf, &m.binding.host, flexible)?;
            write_i8(buf, m.binding.operation);
            write_i8(buf, m.binding.permission);
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
    let n = read_array_len(buf, flexible)?;
    let mut filter_results = Vec::with_capacity(n);
    for _ in 0..n {
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, flexible)?;
        let nm = read_array_len(buf, flexible)?;
        let mut matching_acls = Vec::with_capacity(nm);
        for _ in 0..nm {
            let m_error_code = read_i16(buf)?;
            let m_error_message = read_nullable_str(buf, flexible)?;
            let resource_type = read_i8(buf)?;
            let resource_name = read_str(buf, flexible)?;
            let pattern_type = if version >= 1 { read_i8(buf)? } else { 0 };
            let principal = read_str(buf, flexible)?;
            let host = read_str(buf, flexible)?;
            let operation = read_i8(buf)?;
            let permission = read_i8(buf)?;
            read_entry_tags(buf, flexible)?;
            matching_acls.push(DeleteAclsMatchingAcl {
                error_code: m_error_code,
                error_message: m_error_message,
                binding: AclBinding {
                    resource_type,
                    resource_name,
                    pattern_type,
                    principal,
                    host,
                    operation,
                    permission,
                },
            });
        }
        read_entry_tags(buf, flexible)?;
        filter_results.push(DeleteAclsFilterResult {
            error_code,
            error_message,
            matching_acls,
        });
    }
    read_entry_tags(buf, flexible)?;
    Ok(Response {
        throttle_time_ms,
        filter_results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::acl_types::{operation, pattern_type, permission, resource_type};

    fn roundtrip(version: i16) {
        let req = Request {
            filters: vec![AclFilter {
                resource_type_filter: resource_type::TOPIC,
                resource_name_filter: Some("orders".into()),
                pattern_type_filter: if version >= 1 { pattern_type::ANY } else { 0 },
                principal_filter: Some("User:alice".into()),
                host_filter: None,
                operation: operation::ANY,
                permission_type: permission::ANY,
            }],
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            filter_results: vec![DeleteAclsFilterResult {
                error_code: 0,
                error_message: None,
                matching_acls: vec![DeleteAclsMatchingAcl {
                    error_code: 0,
                    error_message: None,
                    binding: AclBinding {
                        resource_type: resource_type::TOPIC,
                        resource_name: "orders".into(),
                        pattern_type: if version >= 1 {
                            pattern_type::LITERAL
                        } else {
                            0
                        },
                        principal: "User:alice".into(),
                        host: "*".into(),
                        operation: operation::READ,
                        permission: permission::ALLOW,
                    },
                }],
            }],
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
