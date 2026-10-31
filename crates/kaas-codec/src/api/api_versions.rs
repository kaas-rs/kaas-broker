//! ApiVersions — API key 18.
//!
//! Versions 0..=4 supported. Flexible (KIP-482) since v3. v3 added the
//! `client_software_name` / `client_software_version` request fields.

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_array_len, read_compact_array_len, read_compact_string, read_i16, read_i32,
    write_array_len, write_compact_array_len, write_i16, write_i32,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 18,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// v3+ only.
    pub client_software_name: Option<String>,
    /// v3+ only.
    pub client_software_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub error_code: i16,
    pub api_versions: Vec<ApiVersionEntry>,
    /// v1+ only. Set to 0 unless the broker is currently throttling this
    /// client.
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiVersionEntry {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let mut req = Request {
        client_software_name: None,
        client_software_version: None,
    };
    if version >= MIN_FLEXIBLE {
        req.client_software_name = Some(read_compact_string(buf)?);
        req.client_software_version = Some(read_compact_string(buf)?);
        tagged::read(buf)?;
    }
    Ok(req)
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    write_i16(buf, resp.error_code);

    if version >= MIN_FLEXIBLE {
        write_compact_array_len(buf, resp.api_versions.len())?;
        for v in &resp.api_versions {
            write_i16(buf, v.api_key);
            write_i16(buf, v.min_version);
            write_i16(buf, v.max_version);
            tagged::write_empty(buf);
        }
    } else {
        write_array_len(buf, resp.api_versions.len())?;
        for v in &resp.api_versions {
            write_i16(buf, v.api_key);
            write_i16(buf, v.min_version);
            write_i16(buf, v.max_version);
        }
    }

    if version >= 1 {
        write_i32(buf, resp.throttle_time_ms);
    }

    if version >= MIN_FLEXIBLE {
        tagged::write_empty(buf);
    }

    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let error_code = read_i16(buf)?;
    let count = if version >= MIN_FLEXIBLE {
        read_compact_array_len(buf)?
    } else {
        read_array_len(buf)?
    };
    let mut api_versions = Vec::with_capacity(count);
    for _ in 0..count {
        let api_key = read_i16(buf)?;
        let min_version = read_i16(buf)?;
        let max_version = read_i16(buf)?;
        if version >= MIN_FLEXIBLE {
            tagged::read(buf)?;
        }
        api_versions.push(ApiVersionEntry {
            api_key,
            min_version,
            max_version,
        });
    }
    let throttle_time_ms = if version >= 1 { read_i32(buf)? } else { 0 };
    if version >= MIN_FLEXIBLE {
        tagged::read(buf)?;
    }
    Ok(Response {
        error_code,
        api_versions,
        throttle_time_ms,
    })
}

/// Build an ApiVersions response carrying every key registered in
/// [`crate::api::registry::ALL`]. Sorted by key for deterministic output —
/// tests rely on it, and clients can be picky about order.
pub fn response_from_registry(throttle_time_ms: i32) -> Response {
    let mut api_versions: Vec<ApiVersionEntry> = crate::api::registry::ALL
        .iter()
        .map(|s| ApiVersionEntry {
            api_key: s.key,
            min_version: s.min_version,
            max_version: s.max_version,
        })
        .collect();
    api_versions.sort_by_key(|e| e.api_key);
    Response {
        error_code: 0,
        api_versions,
        throttle_time_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::write_compact_string;

    fn roundtrip_response(version: i16, resp: Response) {
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp);
        assert!(r.is_empty(), "stray bytes after response v{}", version);
    }

    fn sample_response() -> Response {
        Response {
            error_code: 0,
            api_versions: vec![
                ApiVersionEntry {
                    api_key: 0,
                    min_version: 3,
                    max_version: 9,
                },
                ApiVersionEntry {
                    api_key: 1,
                    min_version: 4,
                    max_version: 12,
                },
                ApiVersionEntry {
                    api_key: 18,
                    min_version: 0,
                    max_version: 4,
                },
            ],
            throttle_time_ms: 0,
        }
    }

    #[test]
    fn response_v0_roundtrip() {
        roundtrip_response(0, sample_response());
    }

    #[test]
    fn response_v2_roundtrip() {
        roundtrip_response(2, sample_response());
    }

    #[test]
    fn response_v3_flexible_roundtrip() {
        roundtrip_response(3, sample_response());
    }

    #[test]
    fn response_v4_flexible_roundtrip() {
        roundtrip_response(4, sample_response());
    }

    #[test]
    fn request_v0_has_empty_body() {
        let mut buf = Bytes::new();
        let req = decode_request(&mut buf, 0).unwrap();
        assert_eq!(req.client_software_name, None);
        assert_eq!(req.client_software_version, None);
    }

    #[test]
    fn request_v3_roundtrips_client_info() {
        let mut w = BytesMut::new();
        write_compact_string(&mut w, "franz-go").unwrap();
        write_compact_string(&mut w, "1.18.0").unwrap();
        tagged::write_empty(&mut w);
        let mut r = w.freeze();
        let req = decode_request(&mut r, 3).unwrap();
        assert_eq!(req.client_software_name.as_deref(), Some("franz-go"));
        assert_eq!(req.client_software_version.as_deref(), Some("1.18.0"));
        assert!(r.is_empty());
    }

    #[test]
    fn registry_response_is_sorted() {
        let resp = response_from_registry(0);
        // Phase 1 seed has only one key in the registry, but the helper
        // must remain stable as keys are added.
        let mut keys: Vec<i16> = resp.api_versions.iter().map(|e| e.api_key).collect();
        let sorted = {
            let mut s = keys.clone();
            s.sort_unstable();
            s
        };
        assert_eq!(keys, sorted);
        keys.dedup();
        assert_eq!(
            keys.len(),
            resp.api_versions.len(),
            "duplicate api keys in registry"
        );
    }

    #[test]
    fn registry_response_includes_spec() {
        let resp = response_from_registry(0);
        let entry = resp
            .api_versions
            .iter()
            .find(|e| e.api_key == 18)
            .expect("ApiVersions key in registry response");
        assert_eq!(entry.min_version, 0);
        assert_eq!(entry.max_version, 4);
    }
}
