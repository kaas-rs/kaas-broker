//! IncrementalAlterConfigs — API key 44.
//!
//! Versions 0..=1. Flexible (KIP-482) from v1. Per-resource list of
//! `(name, op, value)` triples that mutate one config key at a time.
//!
//! `op` values (Apache `AlterConfigOp.OpType`):
//!
//! - `0` = `SET`
//! - `1` = `DELETE`
//! - `2` = `APPEND` (list-valued configs)
//! - `3` = `SUBTRACT` (list-valued configs)
//!
//! kaas's topic configs are scalar — `APPEND` / `SUBTRACT` return
//! `UNSUPPORTED_VERSION` at the handler. Carry the value through here
//! at codec level for fidelity.
//!
//! `resource_type` follows Apache's ConfigResource scheme: `2 = Topic`,
//! `4 = Broker`, `8 = BrokerLogger`. The handler restricts to Topic.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_i16, read_i32, read_i8, write_bool, write_i16, write_i32, write_i8,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);
pub const MIN_FLEXIBLE: i16 = 1;

pub const SPEC: ApiSpec = ApiSpec {
    key: 44,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub resources: Vec<AlterConfigsResource>,
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResource {
    /// `2 = Topic`, `4 = Broker`, `8 = BrokerLogger`.
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<AlterConfigOp>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigOp {
    pub name: String,
    /// `0 = SET`, `1 = DELETE`, `2 = APPEND`, `3 = SUBTRACT`.
    pub op: i8,
    /// `None` ↔ wire null. Always `None` for `DELETE`.
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub responses: Vec<AlterConfigsResourceResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterConfigsResourceResponse {
    pub error_code: i16,
    /// `None` ↔ wire null. Apache emits a string on failure.
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        let m = read_array_len(buf, flexible)?;
        let mut configs = Vec::with_capacity(m);
        for _ in 0..m {
            let name = read_str(buf, flexible)?;
            let op = read_i8(buf)?;
            let value = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            configs.push(AlterConfigOp { name, op, value });
        }
        if flexible {
            tagged::read(buf)?;
        }
        req.resources.push(AlterConfigsResource {
            resource_type,
            resource_name,
            configs,
        });
    }
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
    write_array_len(buf, resp.responses.len(), flexible)?;
    for r in &resp.responses {
        write_i16(buf, r.error_code);
        write_nullable_str(buf, r.error_message.as_deref(), flexible)?;
        write_i8(buf, r.resource_type);
        write_str(buf, &r.resource_name, flexible)?;
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
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, flexible)?;
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        resp.responses.push(AlterConfigsResourceResponse {
            error_code,
            error_message,
            resource_type,
            resource_name,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.resources.len(), flexible)?;
    for r in &req.resources {
        write_i8(buf, r.resource_type);
        write_str(buf, &r.resource_name, flexible)?;
        write_array_len(buf, r.configs.len(), flexible)?;
        for c in &r.configs {
            write_str(buf, &c.name, flexible)?;
            write_i8(buf, c.op);
            write_nullable_str(buf, c.value.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_bool(buf, req.validate_only);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

/// Convenience constants for the `op` discriminant.
pub mod op {
    pub const SET: i8 = 0;
    pub const DELETE: i8 = 1;
    pub const APPEND: i8 = 2;
    pub const SUBTRACT: i8 = 3;
}

/// Convenience constants for `resource_type`.
pub mod resource_type {
    pub const TOPIC: i8 = 2;
    pub const BROKER: i8 = 4;
    pub const BROKER_LOGGER: i8 = 8;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> Request {
        Request {
            resources: vec![AlterConfigsResource {
                resource_type: resource_type::TOPIC,
                resource_name: "events".into(),
                configs: vec![
                    AlterConfigOp {
                        name: "retention.ms".into(),
                        op: op::SET,
                        value: Some("60000".into()),
                    },
                    AlterConfigOp {
                        name: "segment.bytes".into(),
                        op: op::DELETE,
                        value: None,
                    },
                ],
            }],
            validate_only: false,
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            responses: vec![AlterConfigsResourceResponse {
                error_code: 0,
                error_message: None,
                resource_type: resource_type::TOPIC,
                resource_name: "events".into(),
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
    fn v1_is_flexible() {
        roundtrip(1);
    }
}
