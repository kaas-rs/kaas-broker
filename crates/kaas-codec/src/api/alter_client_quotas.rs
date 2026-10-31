//! AlterClientQuotas — API key 49.
//!
//! Versions 0..=1. Flexible (KIP-482) from v1. KIP-546 admin
//! mutation API. Each entry targets one entity (one row per axis)
//! and carries a list of `(quota_key, value, remove)` ops:
//!
//! - `remove = false`, `value = Some(v)` → set the quota to `v`.
//! - `remove = true` (or `value = None` on the wire) → clear the
//!   override; the entity falls back to the cluster default.
//!
//! Apache's wire shape carries `value: f64` always — `None`/`remove`
//! both clear it. We model `value: Option<f64>` so the handler can
//! distinguish the two cases honestly; the encoder emits `0.0` plus
//! `remove=true` for `None`.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_f64, read_i16, read_i32, write_bool, write_f64, write_i16, write_i32,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 1);
pub const MIN_FLEXIBLE: i16 = 1;

pub const SPEC: ApiSpec = ApiSpec {
    key: 49,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Request {
    pub entries: Vec<EntryData>,
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryData {
    pub entity: Vec<EntityData>,
    pub ops: Vec<OpData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntityData {
    pub entity_type: String,
    /// `None` ↔ "<default>" entity (Apache's null is the per-axis
    /// fallback row).
    pub entity_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OpData {
    pub key: String,
    /// `None` → remove the quota (wire shape: `value=0.0, remove=true`).
    /// `Some(v)` → set the quota to `v`.
    pub value: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub entries: Vec<EntryResponseData>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryResponseData {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub entity: Vec<EntityData>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
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
        let on = read_array_len(buf, flexible)?;
        let mut ops = Vec::with_capacity(on);
        for _ in 0..on {
            let key = read_str(buf, flexible)?;
            let raw = read_f64(buf)?;
            let remove = read_bool(buf)?;
            if flexible {
                tagged::read(buf)?;
            }
            ops.push(OpData {
                key,
                value: if remove { None } else { Some(raw) },
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        req.entries.push(EntryData { entity, ops });
    }
    req.validate_only = read_bool(buf)?;
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
    write_array_len(buf, resp.entries.len(), flexible)?;
    for e in &resp.entries {
        write_i16(buf, e.error_code);
        write_nullable_str(buf, e.error_message.as_deref(), flexible)?;
        write_array_len(buf, e.entity.len(), flexible)?;
        for axis in &e.entity {
            write_str(buf, &axis.entity_type, flexible)?;
            write_nullable_str(buf, axis.entity_name.as_deref(), flexible)?;
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
    let mut resp = Response {
        throttle_time_ms: read_i32(buf)?,
        ..Response::default()
    };
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, flexible)?;
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
        if flexible {
            tagged::read(buf)?;
        }
        resp.entries.push(EntryResponseData {
            error_code,
            error_message,
            entity,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.entries.len(), flexible)?;
    for e in &req.entries {
        write_array_len(buf, e.entity.len(), flexible)?;
        for axis in &e.entity {
            write_str(buf, &axis.entity_type, flexible)?;
            write_nullable_str(buf, axis.entity_name.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        write_array_len(buf, e.ops.len(), flexible)?;
        for op in &e.ops {
            write_str(buf, &op.key, flexible)?;
            match op.value {
                Some(v) => {
                    write_f64(buf, v);
                    write_bool(buf, false);
                }
                None => {
                    write_f64(buf, 0.0);
                    write_bool(buf, true);
                }
            }
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_bool(buf, req.validate_only);
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
            entries: vec![EntryData {
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                }],
                ops: vec![
                    OpData {
                        key: "producer_byte_rate".into(),
                        value: Some(1_048_576.0),
                    },
                    OpData {
                        key: "consumer_byte_rate".into(),
                        value: None, // remove
                    },
                ],
            }],
            validate_only: false,
        }
    }

    fn sample_response() -> Response {
        Response {
            throttle_time_ms: 0,
            entries: vec![EntryResponseData {
                error_code: 0,
                error_message: None,
                entity: vec![EntityData {
                    entity_type: "user".into(),
                    entity_name: Some("alice".into()),
                }],
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

    #[test]
    fn remove_value_round_trips_as_none() {
        let req = sample_request();
        // The second op is `value: None` — verify it round-trips that way.
        assert_eq!(req.entries[0].ops[1].value, None);
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 1).unwrap();
        let got = decode_request(&mut w.freeze(), 1).unwrap();
        assert_eq!(got.entries[0].ops[1].value, None);
    }
}
