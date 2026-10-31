//! FindCoordinator — API key 10.
//!
//! Versions 0..=4. Flexible (KIP-482) from v3. v1+ adds the
//! `key_type` byte (0=group, 1=transaction). v4+ supersedes the
//! single-key request with a `coordinator_keys` array (batch
//! lookup) and the single-coordinator response shape with a
//! `coordinators` array.
//!
//! See gh #91 PR 3 — at v3 the wire shape is the legacy
//! single-coordinator form wrapped in flexible tagged fields, NOT an
//! array. Clients only switch to the v4 array once the broker
//! advertises v4 max.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i8, write_i16, write_i32, write_i8};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 10,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// v0..=v3 single-key form. Empty string at v4.
    pub key: String,
    /// v1+. 0 = group, 1 = transaction.
    pub key_type: i8,
    /// v4+ batch form. Empty before v4.
    pub coordinator_keys: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    /// v0..=v3 single-coordinator form. Empty at v4 (use
    /// `coordinators[]`).
    pub error_code: i16,
    /// v1..=v3 nullable. `None` ↔ wire null.
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// v4+ batch form. Empty before v4.
    pub coordinators: Vec<CoordinatorResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoordinatorResult {
    pub key: String,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub error_code: i16,
    /// `None` ↔ wire null.
    pub error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    if version <= 3 {
        req.key = read_str(buf, flexible)?;
    }
    if version >= 1 {
        req.key_type = read_i8(buf)?;
    }
    if version >= 4 {
        let n = read_array_len(buf, flexible)?;
        for _ in 0..n {
            req.coordinator_keys.push(read_str(buf, flexible)?);
        }
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
    if version >= 4 {
        write_array_len(buf, resp.coordinators.len(), flexible)?;
        for c in &resp.coordinators {
            write_str(buf, &c.key, flexible)?;
            write_i32(buf, c.node_id);
            write_str(buf, &c.host, flexible)?;
            write_i32(buf, c.port);
            write_i16(buf, c.error_code);
            write_nullable_str(buf, c.error_message.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
        return Ok(());
    }
    // v0..=v3 single-coordinator form. v3 wraps it in flexible
    // tagged fields; v0..=v2 stay legacy.
    write_i16(buf, resp.error_code);
    if version >= 1 {
        write_nullable_str(buf, resp.error_message.as_deref(), flexible)?;
    }
    write_i32(buf, resp.node_id);
    write_str(buf, &resp.host, flexible)?;
    write_i32(buf, resp.port);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = if version >= 1 { read_i32(buf)? } else { 0 };
    let mut resp = Response {
        throttle_time_ms,
        ..Response::default()
    };
    if version >= 4 {
        let n = read_array_len(buf, flexible)?;
        for _ in 0..n {
            let key = read_str(buf, flexible)?;
            let node_id = read_i32(buf)?;
            let host = read_str(buf, flexible)?;
            let port = read_i32(buf)?;
            let error_code = read_i16(buf)?;
            let error_message = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            resp.coordinators.push(CoordinatorResult {
                key,
                node_id,
                host,
                port,
                error_code,
                error_message,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        return Ok(resp);
    }
    resp.error_code = read_i16(buf)?;
    if version >= 1 {
        resp.error_message = read_nullable_str(buf, flexible)?;
    }
    resp.node_id = read_i32(buf)?;
    resp.host = read_str(buf, flexible)?;
    resp.port = read_i32(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version <= 3 {
        write_str(buf, &req.key, flexible)?;
    }
    if version >= 1 {
        write_i8(buf, req.key_type);
    }
    if version >= 4 {
        write_array_len(buf, req.coordinator_keys.len(), flexible)?;
        for k in &req.coordinator_keys {
            write_str(buf, k, flexible)?;
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
        let mut req = Request::default();
        if version <= 3 {
            req.key = "g1".to_owned();
        }
        if version >= 1 {
            req.key_type = 0; // group
        }
        if version >= 4 {
            req.coordinator_keys = vec!["g1".to_owned(), "g2".to_owned()];
        }
        req
    }

    fn sample_response(version: i16) -> Response {
        let mut resp = Response::default();
        if version >= 1 {
            resp.throttle_time_ms = 0;
        }
        if version >= 4 {
            resp.coordinators = vec![CoordinatorResult {
                key: "g1".to_owned(),
                node_id: 0,
                host: "broker-0".to_owned(),
                port: 9092,
                error_code: 0,
                error_message: None,
            }];
        } else {
            resp.node_id = 0;
            resp.host = "broker-0".to_owned();
            resp.port = 9092;
            if version >= 1 {
                resp.error_message = None;
            }
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
    fn v0_single_key() {
        roundtrip(0);
    }

    #[test]
    fn v1_adds_key_type() {
        roundtrip(1);
    }

    #[test]
    fn v3_single_key_flexible() {
        roundtrip(3);
    }

    #[test]
    fn v4_batch_form() {
        roundtrip(4);
    }

    #[test]
    fn v4_coordinator_error_carries_message() {
        let resp = Response {
            throttle_time_ms: 0,
            coordinators: vec![CoordinatorResult {
                key: "g".to_owned(),
                node_id: -1,
                host: String::new(),
                port: -1,
                error_code: 15, // COORDINATOR_NOT_AVAILABLE
                error_message: Some("not available".to_owned()),
            }],
            ..Response::default()
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 4).unwrap();
        assert_eq!(got, resp);
    }
}
