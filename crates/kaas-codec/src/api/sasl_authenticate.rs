//! SaslAuthenticate — API key 36.
//!
//! Versions 0..=2. Flexible (KIP-482) since v2. v1+ adds
//! `session_lifetime_ms` on the response.
//!
//! Carries one SASL exchange round trip: the client emits a mechanism-
//! specific payload (e.g. SCRAM `client-first`, `client-final`); the
//! broker drives its state machine one step and returns either the
//! next server payload or the final outcome. Apache's `error_code` is
//! `NETWORK_EXCEPTION` (13) for protocol violations and
//! `SASL_AUTHENTICATION_FAILED` (58) for failed credentials.

use bytes::BytesMut;

use crate::api::common::{read_nullable_bytes, read_nullable_str, write_nullable_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_i16, read_i64, write_bytes, write_compact_bytes, write_i16, write_i64,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 2);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 36,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// Mechanism-specific bytes. `None` is wire-null. Apache always
    /// sends `Some(...)`; the option models the schema honestly.
    pub auth_bytes: Option<Bytes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub error_code: i16,
    /// Empty string ↔ wire null per Apache's schema. Carried as
    /// `Option<String>` here so callers can be explicit.
    pub error_message: Option<String>,
    /// Server-side payload to forward to the client. Apache's schema
    /// is non-nullable bytes; empty bytes on auth completion is
    /// normal (no further challenge).
    pub auth_bytes: Bytes,
    /// v1+. `0` when unset.
    pub session_lifetime_ms: i64,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let auth_bytes = read_nullable_bytes(buf, flexible)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request { auth_bytes })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_i16(buf, resp.error_code);
    write_nullable_str(buf, resp.error_message.as_deref(), flexible)?;
    if flexible {
        write_compact_bytes(buf, &resp.auth_bytes)?;
    } else {
        write_bytes(buf, &resp.auth_bytes)?;
    }
    if version >= 1 {
        write_i64(buf, resp.session_lifetime_ms);
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let error_code = read_i16(buf)?;
    let error_message = read_nullable_str(buf, flexible)?;
    let auth_bytes = if flexible {
        crate::primitives::read_compact_bytes(buf)?
    } else {
        crate::primitives::read_bytes(buf)?
    };
    let session_lifetime_ms = if version >= 1 { read_i64(buf)? } else { 0 };
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        error_code,
        error_message,
        auth_bytes,
        session_lifetime_ms,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    crate::api::common::write_nullable_bytes(buf, req.auth_bytes.as_deref(), flexible)?;
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
            auth_bytes: Some(Bytes::from_static(b"n,,n=alice,r=fyko+d2lbbFgONRv9qkxdawL")),
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            error_code: 0,
            error_message: None,
            auth_bytes: Bytes::from_static(
                b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096",
            ),
            session_lifetime_ms: if version >= 1 { 60_000 } else { 0 },
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
    fn v1_adds_session_lifetime() {
        roundtrip(1);
    }

    #[test]
    fn v2_is_flexible() {
        roundtrip(2);
    }

    #[test]
    fn null_auth_bytes_roundtrip() {
        let req = Request { auth_bytes: None };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 2).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 2).unwrap();
        assert_eq!(got.auth_bytes, None);
    }

    #[test]
    fn error_response_with_message() {
        let resp = Response {
            error_code: 58, // SASL_AUTHENTICATION_FAILED
            error_message: Some("invalid credentials".to_owned()),
            auth_bytes: Bytes::new(),
            session_lifetime_ms: 0,
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 2).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 2).unwrap();
        assert_eq!(got, resp);
    }
}
