//! DescribeClientQuotas — API key 48.
//!
//! Versions 0..=1. Flexible (KIP-482) from v1. KIP-546 admin surface
//! for inspecting client quotas (user, client-id, ip — kaas only
//! supports `user`).
//!
//! The request carries a filter `components[]` — each component
//! constrains the matched entities:
//!
//! - `entity_type`: `"user"` / `"client-id"` / `"ip"`
//! - `match_type`: `0 = EXACT`, `1 = DEFAULT` (entity_type=null in
//!   Apache terms; the literal "<default>" entity), `2 = ANY`
//! - `match`: when `match_type=0`, the exact entity name
//!
//! Response is one entry per matched entity, each carrying a map of
//! `(quota_key → f64)`.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_f64, read_i16, read_i32, read_i8, write_bool, write_f64, write_i16, write_i32,
    write_i8,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);
pub const MIN_FLEXIBLE: i16 = 1;

pub const SPEC: ApiSpec = ApiSpec {
    key: 48,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Request {
    pub components: Vec<ComponentData>,
    /// `true` → component conditions are OR-ed (any-match);
    /// `false` → AND-ed.
    pub strict: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ComponentData {
    /// `"user"`, `"client-id"`, `"ip"`.
    pub entity_type: String,
    /// `0=EXACT`, `1=DEFAULT`, `2=ANY`.
    pub match_type: i8,
    /// `None` ↔ wire null. Required for `EXACT`, ignored otherwise.
    pub match_: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    /// `None` ↔ wire null. Apache returns null on success.
    pub error_message: Option<String>,
    pub entries: Vec<EntryData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryData {
    /// One row per `entity_type` axis carried on this entry — at
    /// most one of each `user` / `client-id` / `ip`.
    pub entity: Vec<EntityData>,
    pub values: Vec<ValueData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntityData {
    pub entity_type: String,
    /// `None` ↔ "<default>" entity (Apache's null is the per-axis
    /// fallback row).
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ValueData {
    pub key: String,
    pub value: f64,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let entity_type = read_str(buf, flexible)?;
        let match_type = read_i8(buf)?;
        let match_ = read_nullable_str(buf, flexible)?;
        if flexible {
            tagged::read(buf)?;
        }
        req.components.push(ComponentData {
            entity_type,
            match_type,
            match_,
        });
    }
    req.strict = read_bool(buf)?;
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
    write_i16(buf, resp.error_code);
    write_nullable_str(buf, resp.error_message.as_deref(), flexible)?;
    write_array_len(buf, resp.entries.len(), flexible)?;
    for entry in &resp.entries {
        write_array_len(buf, entry.entity.len(), flexible)?;
        for axis in &entry.entity {
            write_str(buf, &axis.entity_type, flexible)?;
            write_nullable_str(buf, axis.entity_name.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        write_array_len(buf, entry.values.len(), flexible)?;
        for v in &entry.values {
            write_str(buf, &v.key, flexible)?;
            write_f64(buf, v.value);
            if flexible {
                tagged::write_empty(buf);
            }
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
    let throttle_time_ms = read_i32(buf)?;
    let error_code = read_i16(buf)?;
    let error_message = read_nullable_str(buf, flexible)?;
    let n = read_array_len(buf, flexible)?;
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let an = read_array_len(buf, flexible)?;
        let mut entity = Vec::with_capacity(an);
        for _ in 0..an {
            let entity_type = read_str(buf, flexible)?;
            let entity_name = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            entity.push(EntityData {
                entity_type,
                entity_name,
            });
        }
        let vn = read_array_len(buf, flexible)?;
        let mut values = Vec::with_capacity(vn);
        for _ in 0..vn {
            let key = read_str(buf, flexible)?;
            let value = read_f64(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            values.push(ValueData { key, value });
        }
        if flexible {
            tagged::read(buf)?;
        }
        entries.push(EntryData { entity, values });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
        error_message,
        entries,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.components.len(), flexible)?;
    for c in &req.components {
        write_str(buf, &c.entity_type, flexible)?;
        write_i8(buf, c.match_type);
        write_nullable_str(buf, c.match_.as_deref(), flexible)?;
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_bool(buf, req.strict);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

/// `match_type` discriminants from Apache's
/// `ClientQuotaFilterComponent.MatchType`.
pub mod match_type {
    pub const EXACT: i8 = 0;
    pub const DEFAULT: i8 = 1;
    pub const ANY: i8 = 2;
}

/// `entity_type` axis names. kaas only supports `USER`; other
/// values surface `INVALID_REQUEST` at the handler.
pub mod entity_type {
    pub const USER: &str = "user";
    pub const CLIENT_ID: &str = "client-id";
    pub const IP: &str = "ip";
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request() -> Request {
        Request {
            components: vec![ComponentData {
                entity_type: entity_type::USER.into(),
                match_type: match_type::ANY,
                match_: None,
            }],
            strict: false,
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            entries: vec![EntryData {
                entity: vec![EntityData {
                    entity_type: entity_type::USER.into(),
                    entity_name: Some("alice".into()),
                }],
                values: vec![
                    ValueData {
                        key: "producer_byte_rate".into(),
                        value: 1_048_576.0,
                    },
                    ValueData {
                        key: "consumer_byte_rate".into(),
                        value: 2_097_152.0,
                    },
                ],
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
