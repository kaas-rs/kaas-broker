//! Heartbeat — API key 12.
//!
//! Versions 0..=4. Flexible (KIP-482) since v4. v1+ adds
//! `throttle_time_ms` on the response; v3+ adds a nullable
//! `group_instance_id` on the request (KIP-345 static membership).

use bytes::BytesMut;

use crate::api::common::{read_nullable_str, read_str, write_nullable_str, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 4;

pub const SPEC: ApiSpec = ApiSpec {
    key: 12,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    /// v3+ (KIP-345 static membership). `None` ↔ wire null.
    pub group_instance_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response {
    /// v1+. `0` when unset on legacy versions.
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let group_id = read_str(buf, flexible)?;
    let generation_id = read_i32(buf)?;
    let member_id = read_str(buf, flexible)?;
    let group_instance_id = if version >= 3 {
        read_nullable_str(buf, flexible)?
    } else {
        None
    };
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        group_id,
        generation_id,
        member_id,
        group_instance_id,
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
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = if version >= 1 { read_i32(buf)? } else { 0 };
    let error_code = read_i16(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_str(buf, &req.group_id, flexible)?;
    write_i32(buf, req.generation_id);
    write_str(buf, &req.member_id, flexible)?;
    if version >= 3 {
        write_nullable_str(buf, req.group_instance_id.as_deref(), flexible)?;
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
                Some("instance-1".to_owned())
            } else {
                None
            },
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: if version >= 1 { 42 } else { 0 },
            error_code: 0,
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
    fn v1_adds_throttle_time() {
        roundtrip(1);
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
    fn null_group_instance_id_roundtrip() {
        let req = Request {
            group_id: "g".to_owned(),
            generation_id: 0,
            member_id: "m".to_owned(),
            group_instance_id: None,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 4).unwrap();
        assert_eq!(got.group_instance_id, None);
    }

    #[test]
    fn error_code_round_trips() {
        // Apache 25 = UNKNOWN_MEMBER_ID
        let resp = Response {
            throttle_time_ms: 0,
            error_code: 25,
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 4).unwrap();
        assert_eq!(got, resp);
    }
}
