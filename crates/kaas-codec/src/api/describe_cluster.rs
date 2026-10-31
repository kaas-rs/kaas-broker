//! DescribeCluster — API key 60 ([KIP-700]).
//!
//! Versions 0..=2:
//!
//! - v0 — flexible from the first version (this API postdates
//!   KIP-482 entirely, so there is no legacy encoding to support).
//! - v1 — adds `EndpointType` on both request and response (KIP-919:
//!   `1` = brokers, `2` = controllers), and makes
//!   `MISMATCHED_ENDPOINT_TYPE` / `UNSUPPORTED_ENDPOINT_TYPE` valid
//!   top-level error codes.
//! - v2 — adds `IncludeFencedBrokers` to the request and `IsFenced`
//!   per broker row (KIP-1073).
//!
//! **v2 is a deliberate exception to the Apache 3.7 parity target**
//! (it is 4.0 surface), taken with gh #249 because kaas grew a real
//! fenced state and had no way to report it: a broker that exists but
//! isn't serving is simply absent from Metadata, so a degraded
//! cluster is indistinguishable from a smaller one. Serving v2 also
//! means clients that ask for fenced brokers — which several do
//! unconditionally — negotiate a version where the field is legal
//! instead of failing to encode their own request.
//!
//! The response's `ClusterAuthorizedOperations` is a 32-bit field of
//! `1 << AclOperation.code` bits. `-2147483648` (`i32::MIN`) is the
//! documented "not requested" sentinel, distinct from `0` = "requested,
//! nothing authorized" — see [`NOT_REQUESTED`].
//!
//! [KIP-700]: https://cwiki.apache.org/confluence/x/2xRRCQ

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

pub const VERSIONS: (i16, i16) = (0, 2);
/// Flexible from the very first version — no legacy branch anywhere in
/// this module.
pub const MIN_FLEXIBLE: i16 = 0;

/// Endpoint types a DescribeCluster request may ask about (KIP-919).
pub mod endpoint_type {
    pub const UNKNOWN: i8 = 0;
    pub const BROKER: i8 = 1;
    pub const CONTROLLER: i8 = 2;
}

/// `ClusterAuthorizedOperations` value meaning "the client didn't ask".
/// Apache's schema default; clients map it back to `None`. A client that
/// *did* ask but is authorized for nothing gets `0` instead.
pub const NOT_REQUESTED: i32 = i32::MIN;

pub const SPEC: ApiSpec = ApiSpec {
    key: 60,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub include_cluster_authorized_operations: bool,
    /// v1+; v0 requests carry no field and decode to the schema default
    /// [`endpoint_type::BROKER`], so a v0 client can never mismatch.
    pub endpoint_type: i8,
    /// v2+ (KIP-1073). Below v2 there is no way to ask, so fenced
    /// brokers are always omitted — which is also Apache's answer
    /// when the flag is false.
    pub include_fenced_brokers: bool,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: endpoint_type::BROKER,
            include_fenced_brokers: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub error_message: Option<String>,
    /// v1+ only; dropped from the encoding below v1.
    pub endpoint_type: i8,
    pub cluster_id: String,
    pub controller_id: i32,
    pub brokers: Vec<Broker>,
    pub cluster_authorized_operations: i32,
}

impl Default for Response {
    fn default() -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            endpoint_type: endpoint_type::BROKER,
            cluster_id: String::new(),
            controller_id: -1,
            brokers: Vec::new(),
            cluster_authorized_operations: NOT_REQUESTED,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Broker {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
    /// v2+ (KIP-1073): registered but not serving. Dropped from the
    /// encoding below v2, where a fenced broker is simply not listed.
    pub is_fenced: bool,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let include_cluster_authorized_operations = read_bool(buf)?;
    let endpoint_type = if version >= 1 {
        read_i8(buf)?
    } else {
        endpoint_type::BROKER
    };
    let include_fenced_brokers = if version >= 2 { read_bool(buf)? } else { false };
    tagged::read(buf)?;
    Ok(Request {
        include_cluster_authorized_operations,
        endpoint_type,
        include_fenced_brokers,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    write_bool(buf, req.include_cluster_authorized_operations);
    if version >= 1 {
        write_i8(buf, req.endpoint_type);
    }
    if version >= 2 {
        write_bool(buf, req.include_fenced_brokers);
    }
    tagged::write_empty(buf);
    Ok(())
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    const FLEX: bool = true;
    write_i32(buf, resp.throttle_time_ms);
    write_i16(buf, resp.error_code);
    write_nullable_str(buf, resp.error_message.as_deref(), FLEX)?;
    if version >= 1 {
        write_i8(buf, resp.endpoint_type);
    }
    write_str(buf, &resp.cluster_id, FLEX)?;
    write_i32(buf, resp.controller_id);
    write_array_len(buf, resp.brokers.len(), FLEX)?;
    for b in &resp.brokers {
        write_i32(buf, b.broker_id);
        write_str(buf, &b.host, FLEX)?;
        write_i32(buf, b.port);
        write_nullable_str(buf, b.rack.as_deref(), FLEX)?;
        if version >= 2 {
            write_bool(buf, b.is_fenced);
        }
        tagged::write_empty(buf);
    }
    write_i32(buf, resp.cluster_authorized_operations);
    tagged::write_empty(buf);
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    const FLEX: bool = true;
    let throttle_time_ms = read_i32(buf)?;
    let error_code = read_i16(buf)?;
    let error_message = read_nullable_str(buf, FLEX)?;
    let endpoint_type = if version >= 1 {
        read_i8(buf)?
    } else {
        endpoint_type::BROKER
    };
    let cluster_id = read_str(buf, FLEX)?;
    let controller_id = read_i32(buf)?;
    let n = read_array_len(buf, FLEX)?;
    let mut brokers = Vec::with_capacity(n);
    for _ in 0..n {
        let broker_id = read_i32(buf)?;
        let host = read_str(buf, FLEX)?;
        let port = read_i32(buf)?;
        let rack = read_nullable_str(buf, FLEX)?;
        let is_fenced = if version >= 2 { read_bool(buf)? } else { false };
        tagged::read(buf)?;
        brokers.push(Broker {
            broker_id,
            host,
            port,
            rack,
            is_fenced,
        });
    }
    let cluster_authorized_operations = read_i32(buf)?;
    tagged::read(buf)?;
    Ok(Response {
        throttle_time_ms,
        error_code,
        error_message,
        endpoint_type,
        cluster_id,
        controller_id,
        brokers,
        cluster_authorized_operations,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(version: i16) {
        let req = Request {
            include_cluster_authorized_operations: true,
            endpoint_type: endpoint_type::BROKER,
            include_fenced_brokers: version >= 2,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            endpoint_type: endpoint_type::BROKER,
            cluster_id: "kaas-dev".into(),
            controller_id: 1,
            brokers: vec![
                Broker {
                    broker_id: 0,
                    host: "kaas-0.kaas-brokers".into(),
                    port: 9092,
                    rack: None,
                    is_fenced: false,
                },
                Broker {
                    broker_id: 1,
                    host: "kaas-1.kaas-brokers".into(),
                    port: 9092,
                    rack: Some("zone-a".into()),
                    is_fenced: version >= 2,
                },
            ],
            cluster_authorized_operations: 0b1010,
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn all_versions_roundtrip() {
        for v in VERSIONS.0..=VERSIONS.1 {
            roundtrip(v);
        }
    }

    /// The endpoint-type field is v1+ on both sides. A v0 request has
    /// no room for it, so it must decode to the BROKER default rather
    /// than eating the tagged-field block.
    #[test]
    fn endpoint_type_is_v1_gated() {
        let mut w = BytesMut::new();
        encode_request(
            &mut w,
            &Request {
                include_cluster_authorized_operations: false,
                endpoint_type: endpoint_type::CONTROLLER,
                include_fenced_brokers: false,
            },
            0,
        )
        .unwrap();
        // bool + empty tagged section, nothing else.
        assert_eq!(&w[..], &[0u8, 0u8]);
        let mut r = w.freeze();
        assert_eq!(
            decode_request(&mut r, 0).unwrap().endpoint_type,
            endpoint_type::BROKER
        );

        let mut w = BytesMut::new();
        encode_request(
            &mut w,
            &Request {
                include_cluster_authorized_operations: false,
                endpoint_type: endpoint_type::CONTROLLER,
                include_fenced_brokers: false,
            },
            1,
        )
        .unwrap();
        let mut r = w.freeze();
        assert_eq!(
            decode_request(&mut r, 1).unwrap().endpoint_type,
            endpoint_type::CONTROLLER
        );
    }

    /// The KIP-1073 fields are v2-only on both sides: below v2 they
    /// must vanish from the encoding entirely rather than shift the
    /// following bytes. A fenced broker is simply not listed there.
    #[test]
    fn fenced_fields_are_v2_gated() {
        let resp = Response {
            cluster_id: "kaas-dev".into(),
            brokers: vec![Broker {
                broker_id: 1,
                host: "kaas-1".into(),
                port: 9092,
                rack: None,
                is_fenced: true,
            }],
            ..Default::default()
        };
        for v in [0, 1] {
            let mut w = BytesMut::new();
            encode_response(&mut w, &resp, v).unwrap();
            let got = decode_response(&mut w.freeze(), v).unwrap();
            assert!(!got.brokers[0].is_fenced, "v{v} carries no IsFenced");
        }
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 2).unwrap();
        let got = decode_response(&mut w.freeze(), 2).unwrap();
        assert!(got.brokers[0].is_fenced);

        // Request side: the flag only exists at v2.
        let req = Request {
            include_fenced_brokers: true,
            ..Default::default()
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 1).unwrap();
        let got = decode_request(&mut w.freeze(), 1).unwrap();
        assert!(!got.include_fenced_brokers);
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, 2).unwrap();
        let got = decode_request(&mut w.freeze(), 2).unwrap();
        assert!(got.include_fenced_brokers);
    }

    /// `i32::MIN` is the "client didn't ask" sentinel and must survive
    /// the round trip — a client reads it back as `None`, where `0`
    /// means "asked, authorized for nothing".
    #[test]
    fn not_requested_sentinel_survives() {
        let resp = Response {
            cluster_id: "kaas-dev".into(),
            ..Default::default()
        };
        assert_eq!(resp.cluster_authorized_operations, NOT_REQUESTED);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 1).unwrap();
        let got = decode_response(&mut w.freeze(), 1).unwrap();
        assert_eq!(got.cluster_authorized_operations, i32::MIN);
    }

    /// Every version is flexible, so both headers are the tagged-field
    /// shape at v0 — unlike every other admin key, which has a legacy
    /// baseline.
    #[test]
    fn headers_are_flexible_from_v0() {
        use crate::headers::HeaderVersion;
        assert!(SPEC.is_flexible(0));
        assert_eq!(SPEC.request_header(0), HeaderVersion::V2);
        assert_eq!(SPEC.response_header(0), HeaderVersion::V1);
    }
}
