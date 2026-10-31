//! ListGroups — API key 16.
//!
//! Versions 0..=4. Flexible (KIP-482) from v3. v1+ adds
//! `throttle_time_ms` on the response; v4+ adds a `states_filter`
//! array on the request and a `group_state` field per result.

use bytes::BytesMut;

use crate::api::common::{read_array_len, read_str, write_array_len, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 16,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// v4+. Empty on legacy versions.
    pub states_filter: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v1+. `0` when unset.
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub groups: Vec<ListedGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    /// v4+. Empty on legacy versions.
    pub group_state: String,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    if version >= 4 {
        let n = read_array_len(buf, flexible)?;
        for _ in 0..n {
            req.states_filter.push(read_str(buf, flexible)?);
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
    write_i16(buf, resp.error_code);
    write_array_len(buf, resp.groups.len(), flexible)?;
    for g in &resp.groups {
        write_str(buf, &g.group_id, flexible)?;
        write_str(buf, &g.protocol_type, flexible)?;
        if version >= 4 {
            write_str(buf, &g.group_state, flexible)?;
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
    resp.error_code = read_i16(buf)?;
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let group_id = read_str(buf, flexible)?;
        let protocol_type = read_str(buf, flexible)?;
        let group_state = if version >= 4 {
            read_str(buf, flexible)?
        } else {
            String::new()
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.groups.push(ListedGroup {
            group_id,
            protocol_type,
            group_state,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version >= 4 {
        write_array_len(buf, req.states_filter.len(), flexible)?;
        for s in &req.states_filter {
            write_str(buf, s, flexible)?;
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
        Request {
            states_filter: if version >= 4 {
                vec!["Stable".to_owned(), "Empty".to_owned()]
            } else {
                Vec::new()
            },
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            groups: vec![ListedGroup {
                group_id: "g1".to_owned(),
                protocol_type: "consumer".to_owned(),
                group_state: if version >= 4 {
                    "Stable".to_owned()
                } else {
                    String::new()
                },
            }],
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
    fn v3_is_flexible() {
        roundtrip(3);
    }

    #[test]
    fn v4_adds_states_filter_and_group_state() {
        roundtrip(4);
    }
}
