//! DeleteTopics — API key 20.
//!
//! Versions 0..=5. Flexible (KIP-482) from v4. v1+ adds
//! `throttle_time_ms` on the response; v5+ adds a nullable
//! `error_message` per result. v6 added topic-id addressing and made
//! the name nullable — not supported.

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
    key: 20,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+.
    pub throttle_time_ms: i32,
    pub responses: Vec<DeletableTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DeletableTopicResult {
    pub name: String,
    pub error_code: i16,
    /// v5+. `None` ↔ wire null (the spec's "no error" shape).
    pub error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let n = read_array_len(buf, flexible)?;
    let mut topic_names = Vec::with_capacity(n);
    for _ in 0..n {
        topic_names.push(read_str(buf, flexible)?);
    }
    let timeout_ms = read_i32(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        topic_names,
        timeout_ms,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.topic_names.len(), flexible)?;
    for name in &req.topic_names {
        write_str(buf, name, flexible)?;
    }
    write_i32(buf, req.timeout_ms);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
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
    write_array_len(buf, resp.responses.len(), flexible)?;
    for r in &resp.responses {
        write_str(buf, &r.name, flexible)?;
        write_i16(buf, r.error_code);
        if version >= 5 {
            write_nullable_str(buf, r.error_message.as_deref(), flexible)?;
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
        let name = read_str(buf, flexible)?;
        let error_code = read_i16(buf)?;
        let error_message = if version >= 5 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.responses.push(DeletableTopicResult {
            name,
            error_code,
            error_message,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(version: i16) {
        let req = Request {
            topic_names: vec!["orders".into(), "audit-log".into()],
            timeout_ms: 30_000,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            responses: vec![
                DeletableTopicResult {
                    name: "orders".into(),
                    error_code: 0,
                    error_message: None,
                },
                DeletableTopicResult {
                    name: "missing".into(),
                    error_code: 3,
                    error_message: if version >= 5 {
                        Some("unknown topic".into())
                    } else {
                        None
                    },
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
    fn v1_adds_throttle() {
        roundtrip(1);
    }

    #[test]
    fn v4_is_flexible() {
        roundtrip(4);
    }

    #[test]
    fn v5_adds_error_message() {
        roundtrip(5);
    }
}
