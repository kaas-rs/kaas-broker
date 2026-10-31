//! DeleteGroups — API key 42.
//!
//! Versions 0..=2. Flexible (KIP-482) from v2. Used by
//! `AdminClient.deleteConsumerGroups()` and
//! `kafka-consumer-groups.sh --delete` to drop a consumer group's
//! coordinator-side state plus its committed offsets (gh #89).

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 2);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 42,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub group_names: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<DeleteGroupsResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteGroupsResult {
    pub group_id: String,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        req.group_names.push(read_str(buf, flexible)?);
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
    write_i32(buf, resp.throttle_time_ms);
    write_array_len(buf, resp.results.len(), flexible)?;
    for r in &resp.results {
        write_str(buf, &r.group_id, flexible)?;
        write_i16(buf, r.error_code);
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
        let group_id = read_str(buf, flexible)?;
        let error_code = read_i16(buf)?;
        if flexible {
            tagged::read(buf)?;
        }
        resp.results.push(DeleteGroupsResult {
            group_id,
            error_code,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.group_names.len(), flexible)?;
    for n in &req.group_names {
        write_str(buf, n, flexible)?;
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> Request {
        Request {
            group_names: vec!["g1".to_owned(), "g2".to_owned()],
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            results: vec![
                DeleteGroupsResult {
                    group_id: "g1".to_owned(),
                    error_code: 0,
                },
                DeleteGroupsResult {
                    group_id: "g2".to_owned(),
                    error_code: 69, // GROUP_ID_NOT_FOUND
                },
            ],
        }
    }

    fn roundtrip(version: i16) {
        let req = sample_request();
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req);
        assert!(r.is_empty());

        let resp = sample_response();
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp);
        assert!(r.is_empty());
    }

    #[test]
    fn v0_roundtrip() {
        roundtrip(0);
    }

    #[test]
    fn v1_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn v2_is_flexible() {
        roundtrip(2);
    }
}
