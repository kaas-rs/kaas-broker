//! Kafka request/response headers (v0/v1/v2).
//!
//! - **Request v0**: `int16 api_key, int16 api_version, int32 correlation_id`
//! - **Request v1**: v0 + `nullable_string client_id`
//! - **Request v2**: v1 + `tagged_fields` (KIP-482 flexible)
//!
//! - **Response v0**: `int32 correlation_id`
//! - **Response v1**: v0 + `tagged_fields`
//!
//! The (api_key, api_version) → header version mapping is derived from
//! each registry entry's flexible range by
//! [`crate::api::registry::ApiSpec::request_header`] and
//! [`crate::api::registry::ApiSpec::response_header`], as Apache's
//! generated `ApiMessageType.{request,response}HeaderVersion` does.

use bytes::BytesMut;

use crate::errors::CodecError;
use crate::primitives::{
    read_i16, read_i32, read_nullable_string, write_i16, write_i32, write_nullable_string,
};
use crate::tagged;
use crate::Bytes;

/// Wire encoding version for a (request|response) header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderVersion {
    /// Legacy header without `client_id` (request) or without tagged fields
    /// (response). The response header of every non-flexible version, and
    /// of ApiVersions at every version.
    V0,
    /// Request: v0 + `client_id`. Response: v0 + tagged fields.
    V1,
    /// Request: v1 + tagged fields (KIP-482 flexible). Response headers do
    /// not have a V2 shape — flexible responses use V1.
    V2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHeader {
    pub api_key: i16,
    pub api_version: i16,
    pub correlation_id: i32,
    /// `None` if the client sent the null sentinel (legacy clients) or if
    /// the header version is V0 (no `client_id` field at all).
    pub client_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

pub fn decode_request_header(
    buf: &mut Bytes,
    version: HeaderVersion,
) -> Result<RequestHeader, CodecError> {
    let api_key = read_i16(buf)?;
    let api_version = read_i16(buf)?;
    let correlation_id = read_i32(buf)?;
    let client_id = match version {
        HeaderVersion::V0 => None,
        HeaderVersion::V1 | HeaderVersion::V2 => read_nullable_string(buf)?,
    };
    if matches!(version, HeaderVersion::V2) {
        tagged::read(buf)?;
    }
    Ok(RequestHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

pub fn encode_request_header(
    buf: &mut BytesMut,
    hdr: &RequestHeader,
    version: HeaderVersion,
) -> Result<(), CodecError> {
    write_i16(buf, hdr.api_key);
    write_i16(buf, hdr.api_version);
    write_i32(buf, hdr.correlation_id);
    match version {
        HeaderVersion::V0 => {}
        HeaderVersion::V1 | HeaderVersion::V2 => {
            write_nullable_string(buf, hdr.client_id.as_deref())?;
        }
    }
    if matches!(version, HeaderVersion::V2) {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn encode_response_header(buf: &mut BytesMut, hdr: &ResponseHeader, version: HeaderVersion) {
    write_i32(buf, hdr.correlation_id);
    if !matches!(version, HeaderVersion::V0) {
        tagged::write_empty(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(hdr: RequestHeader, version: HeaderVersion) {
        let mut w = BytesMut::new();
        encode_request_header(&mut w, &hdr, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request_header(&mut r, version).unwrap();
        assert_eq!(got, hdr);
        assert!(r.is_empty(), "stray bytes after header");
    }

    #[test]
    fn request_v0_no_client_id() {
        roundtrip_request(
            RequestHeader {
                api_key: 0,
                api_version: 0,
                correlation_id: 42,
                client_id: None,
            },
            HeaderVersion::V0,
        );
    }

    #[test]
    fn request_v1_with_client_id() {
        roundtrip_request(
            RequestHeader {
                api_key: 3,
                api_version: 9,
                correlation_id: -7,
                client_id: Some("rdkafka".to_owned()),
            },
            HeaderVersion::V1,
        );
    }

    #[test]
    fn request_v1_null_client_id() {
        roundtrip_request(
            RequestHeader {
                api_key: 18,
                api_version: 1,
                correlation_id: 1,
                client_id: None,
            },
            HeaderVersion::V1,
        );
    }

    #[test]
    fn request_v2_flexible_tagged_block() {
        roundtrip_request(
            RequestHeader {
                api_key: 18,
                api_version: 3,
                correlation_id: 99,
                client_id: Some("franz".to_owned()),
            },
            HeaderVersion::V2,
        );
    }

    #[test]
    fn response_v0_no_tag_block() {
        let hdr = ResponseHeader {
            correlation_id: 1234,
        };
        let mut w = BytesMut::new();
        encode_response_header(&mut w, &hdr, HeaderVersion::V0);
        assert_eq!(
            &w[..],
            &1234i32.to_be_bytes(),
            "v0 response header is just the correlation id"
        );
    }

    #[test]
    fn response_v1_flexible_with_tag_block() {
        let hdr = ResponseHeader { correlation_id: -1 };
        let mut w = BytesMut::new();
        encode_response_header(&mut w, &hdr, HeaderVersion::V1);
        assert_eq!(
            &w[..],
            &[0xff, 0xff, 0xff, 0xff, 0x00],
            "v1 response header is corr_id + uvarint(0)"
        );
    }
}
