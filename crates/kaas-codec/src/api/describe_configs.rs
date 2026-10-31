//! DescribeConfigs — API key 32.
//!
//! Versions 0..=4. Flexible (KIP-482) from v4. Read-side companion
//! of [`super::incremental_alter_configs`]. Filter by
//! `resource_type` (2=Topic, 4=Broker, 8=BrokerLogger) + optional
//! `configuration_keys` list to narrow the response.
//!
//! v1+ adds `include_synonyms` (defaults / dynamic-broker / dynamic-
//! topic chain). v3+ adds `include_documentation`. v2+ enriches each
//! returned entry with `config_type` and `config_source`.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_i16, read_i32, read_i8, write_bool, write_i16, write_i32, write_i8,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 4;

pub const SPEC: ApiSpec = ApiSpec {
    key: 32,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub resources: Vec<DescribeConfigsResource>,
    /// v1+. When `true`, include the synonym chain on each entry.
    pub include_synonyms: bool,
    /// v3+. When `true`, include per-key `documentation`.
    pub include_documentation: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResource {
    pub resource_type: i8,
    pub resource_name: String,
    /// `None` ↔ wire null → all keys. `Some(empty)` → filter for none.
    pub configuration_keys: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<DescribeConfigsResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResult {
    pub error_code: i16,
    pub error_message: Option<String>,
    pub resource_type: i8,
    pub resource_name: String,
    pub configs: Vec<DescribeConfigsResultConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsResultConfig {
    pub name: String,
    pub value: Option<String>,
    pub read_only: bool,
    /// v0 carries `is_default: bool`; v1+ deprecated in favour of
    /// `config_source`. We track both and the encoder picks the
    /// right one for the wire version.
    pub is_default: bool,
    /// v1+ `is_sensitive`.
    pub is_sensitive: bool,
    /// v1+ synonyms. Empty unless `include_synonyms = true`.
    pub synonyms: Vec<DescribeConfigsSynonym>,
    /// v2+ `config_type` discriminant (1=BOOLEAN, 2=STRING, 3=INT,
    /// 4=SHORT, 5=LONG, 6=DOUBLE, 7=LIST, 8=CLASS, 9=PASSWORD).
    pub config_type: i8,
    /// v1+ `config_source` (1=DYNAMIC_TOPIC_CONFIG, …, 5=DEFAULT_CONFIG).
    pub config_source: i8,
    /// v3+ documentation. Empty unless `include_documentation = true`.
    pub documentation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeConfigsSynonym {
    pub name: String,
    pub value: Option<String>,
    pub source: i8,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        let configuration_keys = read_nullable_array(buf, flexible, |b| read_str(b, flexible))?;
        if flexible {
            tagged::read(buf)?;
        }
        req.resources.push(DescribeConfigsResource {
            resource_type,
            resource_name,
            configuration_keys,
        });
    }
    if version >= 1 {
        req.include_synonyms = read_bool(buf)?;
    }
    if version >= 3 {
        req.include_documentation = read_bool(buf)?;
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
        write_i16(buf, r.error_code);
        write_nullable_str(buf, r.error_message.as_deref(), flexible)?;
        write_i8(buf, r.resource_type);
        write_str(buf, &r.resource_name, flexible)?;
        write_array_len(buf, r.configs.len(), flexible)?;
        for c in &r.configs {
            write_str(buf, &c.name, flexible)?;
            write_nullable_str(buf, c.value.as_deref(), flexible)?;
            write_bool(buf, c.read_only);
            if version == 0 {
                write_bool(buf, c.is_default);
            }
            if version >= 1 {
                write_i8(buf, c.config_source);
                write_bool(buf, c.is_sensitive);
                write_array_len(buf, c.synonyms.len(), flexible)?;
                for s in &c.synonyms {
                    write_str(buf, &s.name, flexible)?;
                    write_nullable_str(buf, s.value.as_deref(), flexible)?;
                    write_i8(buf, s.source);
                    if flexible {
                        tagged::write_empty(buf);
                    }
                }
            }
            if version >= 2 {
                write_i8(buf, c.config_type);
            }
            if version >= 3 {
                write_nullable_str(buf, c.documentation.as_deref(), flexible)?;
            }
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
        let resource_type = read_i8(buf)?;
        let resource_name = read_str(buf, flexible)?;
        let cn = read_array_len(buf, flexible)?;
        let mut configs = Vec::with_capacity(cn);
        for _ in 0..cn {
            let name = read_str(buf, flexible)?;
            let value = read_nullable_str(buf, flexible)?;
            let read_only = read_bool(buf)?;
            let is_default = if version == 0 { read_bool(buf)? } else { false };
            let (config_source, is_sensitive, synonyms) = if version >= 1 {
                let cs = read_i8(buf)?;
                let sens = read_bool(buf)?;
                let sn = read_array_len(buf, flexible)?;
                let mut syn = Vec::with_capacity(sn);
                for _ in 0..sn {
                    let n = read_str(buf, flexible)?;
                    let v = read_nullable_str(buf, flexible)?;
                    let s = read_i8(buf)?;
                    if flexible {
                        tagged::read(buf)?;
                    }
                    syn.push(DescribeConfigsSynonym {
                        name: n,
                        value: v,
                        source: s,
                    });
                }
                (cs, sens, syn)
            } else {
                (0, false, Vec::new())
            };
            let config_type = if version >= 2 { read_i8(buf)? } else { 0 };
            let documentation = if version >= 3 {
                read_nullable_str(buf, flexible)?
            } else {
                None
            };
            if flexible {
                tagged::read(buf)?;
            }
            configs.push(DescribeConfigsResultConfig {
                name,
                value,
                read_only,
                is_default,
                is_sensitive,
                synonyms,
                config_type,
                config_source,
                documentation,
            });
        }
        if flexible {
            tagged::read(buf)?;
        }
        resp.results.push(DescribeConfigsResult {
            error_code,
            error_message,
            resource_type,
            resource_name,
            configs,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.resources.len(), flexible)?;
    for r in &req.resources {
        write_i8(buf, r.resource_type);
        write_str(buf, &r.resource_name, flexible)?;
        write_nullable_array(buf, r.configuration_keys.as_deref(), flexible, |b, k| {
            write_str(b, k, flexible)
        })?;
        if flexible {
            tagged::write_empty(buf);
        }
    }
    if version >= 1 {
        write_bool(buf, req.include_synonyms);
    }
    if version >= 3 {
        write_bool(buf, req.include_documentation);
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

/// Convenience constants for `resource_type`.
pub mod resource_type {
    pub const TOPIC: i8 = 2;
    pub const BROKER: i8 = 4;
    pub const BROKER_LOGGER: i8 = 8;
}

/// `config_source` discriminants (Apache's ConfigEntry.ConfigSource).
pub mod source {
    pub const UNKNOWN: i8 = 0;
    pub const DYNAMIC_TOPIC_CONFIG: i8 = 1;
    pub const DYNAMIC_BROKER_CONFIG: i8 = 2;
    pub const DYNAMIC_DEFAULT_BROKER_CONFIG: i8 = 3;
    pub const STATIC_BROKER_CONFIG: i8 = 4;
    pub const DEFAULT_CONFIG: i8 = 5;
    pub const DYNAMIC_BROKER_LOGGER_CONFIG: i8 = 6;
}

/// `config_type` discriminants (Apache's ConfigEntry.ConfigType).
pub mod config_type {
    pub const UNKNOWN: i8 = 0;
    pub const BOOLEAN: i8 = 1;
    pub const STRING: i8 = 2;
    pub const INT: i8 = 3;
    pub const SHORT: i8 = 4;
    pub const LONG: i8 = 5;
    pub const DOUBLE: i8 = 6;
    pub const LIST: i8 = 7;
    pub const CLASS: i8 = 8;
    pub const PASSWORD: i8 = 9;
}

// ---- nullable array helpers (local; same shape as create_partitions') ----

fn read_nullable_array<T, F>(
    buf: &mut Bytes,
    flexible: bool,
    mut read_one: F,
) -> Result<Option<Vec<T>>, CodecError>
where
    F: FnMut(&mut Bytes) -> Result<T, CodecError>,
{
    use crate::primitives::{read_i32, read_uvarint};
    let len = if flexible {
        let raw = read_uvarint(buf)?;
        if raw == 0 {
            return Ok(None);
        }
        usize::try_from(raw - 1).map_err(|_| CodecError::LengthOverflow)?
    } else {
        let raw = read_i32(buf)?;
        if raw < 0 {
            return Ok(None);
        }
        usize::try_from(raw).map_err(|_| CodecError::LengthOverflow)?
    };
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(read_one(buf)?);
    }
    Ok(Some(out))
}

fn write_nullable_array<T, F>(
    buf: &mut BytesMut,
    xs: Option<&[T]>,
    flexible: bool,
    mut write_one: F,
) -> Result<(), CodecError>
where
    F: FnMut(&mut BytesMut, &T) -> Result<(), CodecError>,
{
    use crate::primitives::{write_i32, write_uvarint};
    match xs {
        None => {
            if flexible {
                write_uvarint(buf, 0);
            } else {
                write_i32(buf, -1);
            }
        }
        Some(items) => {
            if flexible {
                let v = u64::try_from(items.len())
                    .map_err(|_| CodecError::LengthOverflow)?
                    .checked_add(1)
                    .ok_or(CodecError::LengthOverflow)?;
                write_uvarint(buf, v);
            } else {
                let v = i32::try_from(items.len()).map_err(|_| CodecError::LengthOverflow)?;
                write_i32(buf, v);
            }
            for x in items {
                write_one(buf, x)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(version: i16) -> Request {
        Request {
            resources: vec![DescribeConfigsResource {
                resource_type: resource_type::TOPIC,
                resource_name: "events".into(),
                configuration_keys: Some(vec!["retention.ms".into(), "segment.bytes".into()]),
            }],
            include_synonyms: version >= 1,
            include_documentation: version >= 3,
        }
    }

    fn sample_response(version: i16) -> Response {
        let mut configs = vec![DescribeConfigsResultConfig {
            name: "retention.ms".into(),
            value: Some("604800000".into()),
            read_only: false,
            is_default: version == 0,
            is_sensitive: false,
            synonyms: if version >= 1 {
                vec![DescribeConfigsSynonym {
                    name: "log.retention.ms".into(),
                    value: Some("604800000".into()),
                    source: source::DEFAULT_CONFIG,
                }]
            } else {
                vec![]
            },
            config_type: if version >= 2 { config_type::LONG } else { 0 },
            config_source: if version >= 1 {
                source::DEFAULT_CONFIG
            } else {
                0
            },
            documentation: if version >= 3 {
                Some("How long messages are kept on disk.".into())
            } else {
                None
            },
        }];
        // Verify v0 doesn't echo deprecated `is_default` outside v0.
        if version > 0 {
            configs[0].is_default = false;
        }
        Response {
            throttle_time_ms: 0,
            results: vec![DescribeConfigsResult {
                error_code: 0,
                error_message: None,
                resource_type: resource_type::TOPIC,
                resource_name: "events".into(),
                configs,
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
    fn v1_carries_synonyms() {
        roundtrip(1);
    }

    #[test]
    fn v2_carries_config_type() {
        roundtrip(2);
    }

    #[test]
    fn v3_carries_documentation() {
        roundtrip(3);
    }

    #[test]
    fn v4_is_flexible() {
        roundtrip(4);
    }

    #[test]
    fn null_configuration_keys_roundtrip() {
        let req = Request {
            resources: vec![DescribeConfigsResource {
                resource_type: resource_type::TOPIC,
                resource_name: "events".into(),
                configuration_keys: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 4).unwrap();
        let got = decode_request(&mut w.freeze(), 4).unwrap();
        assert!(got.resources[0].configuration_keys.is_none());
    }
}
