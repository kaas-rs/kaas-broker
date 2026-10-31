//! SaslHandshake — API key 17.
//!
//! Versions 0..=1. Never flexible (max version is 1). Request carries
//! the chosen SASL mechanism name; response advertises the broker's
//! enabled mechanisms plus an error code that is non-zero when the
//! requested mechanism is not enabled.

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_array_len, read_i16, read_string, write_array_len, write_i16, write_string,
};
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);

pub const SPEC: ApiSpec = ApiSpec {
    key: 17,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: None,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub mechanism: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub error_code: i16,
    pub mechanisms: Vec<String>,
}

pub fn decode_request(buf: &mut Bytes, _version: i16) -> Result<Request, CodecError> {
    let mechanism = read_string(buf)?;
    Ok(Request { mechanism })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    _version: i16,
) -> Result<(), CodecError> {
    write_i16(buf, resp.error_code);
    write_array_len(buf, resp.mechanisms.len())?;
    for m in &resp.mechanisms {
        write_string(buf, m)?;
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, _version: i16) -> Result<Response, CodecError> {
    let error_code = read_i16(buf)?;
    let n = read_array_len(buf)?;
    let mut mechanisms = Vec::with_capacity(n);
    for _ in 0..n {
        mechanisms.push(read_string(buf)?);
    }
    Ok(Response {
        error_code,
        mechanisms,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, _version: i16) -> Result<(), CodecError> {
    write_string(buf, &req.mechanism)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(version: i16) {
        let req = Request {
            mechanism: "SCRAM-SHA-512".to_owned(),
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            error_code: 0,
            mechanisms: vec!["SCRAM-SHA-512".to_owned(), "PLAIN".to_owned()],
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
    fn v1_roundtrip() {
        roundtrip(1);
    }

    #[test]
    fn unsupported_mechanism_error_code_round_trips() {
        // Apache 33 = UNSUPPORTED_SASL_MECHANISM
        let resp = Response {
            error_code: 33,
            mechanisms: vec!["SCRAM-SHA-512".to_owned()],
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 1).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 1).unwrap();
        assert_eq!(got, resp);
    }
}
