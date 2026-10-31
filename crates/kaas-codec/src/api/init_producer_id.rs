//! InitProducerId — API key 22. v0–v4, flexible from v2 (KIP-482).
//!
//!
//! The Java producer issues this on startup when
//! `enable.idempotence=true` (the default since Kafka 3.0) to obtain a
//! `(producer_id, epoch)` pair used to tag every Produce batch for
//! the producer's lifetime.
//!
//! v3+ adds `(producer_id, producer_epoch)` request fields per KIP-360
//! so a producer can ask for renewal of an existing PID after a fatal
//! error. The codec only carries them; the broker handler validates
//! them against the txn state store (gh #201).

use bytes::BytesMut;

use crate::api::common::read_nullable_str;
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::headers::HeaderVersion;
use crate::primitives::{read_i16, read_i32, read_i64, write_i16, write_i32, write_i64};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 2;

const fn header_for(version: i16) -> HeaderVersion {
    if version >= MIN_FLEXIBLE {
        HeaderVersion::V2
    } else {
        HeaderVersion::V1
    }
}

fn request_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

fn response_hdr(version: i16) -> HeaderVersion {
    header_for(version)
}

pub const SPEC: ApiSpec = ApiSpec {
    key: 22,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
    request_hdr,
    response_hdr,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `None` ↔ wire null = non-transactional producer.
    /// `Some(id)` = transactional producer (Phase 6 honours the id).
    pub transactional_id: Option<String>,
    pub transaction_timeout_ms: i32,
    /// v3+. `-1` when not present (legacy versions).
    pub producer_id: i64,
    /// v3+. `-1` when not present.
    pub producer_epoch: i16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let transactional_id = read_nullable_str(buf, flexible)?;
    let transaction_timeout_ms = read_i32(buf)?;
    let (producer_id, producer_epoch) = if version >= 3 {
        (read_i64(buf)?, read_i16(buf)?)
    } else {
        (-1, -1)
    };
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        transactional_id,
        transaction_timeout_ms,
        producer_id,
        producer_epoch,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_i32(buf, resp.throttle_time_ms);
    write_i16(buf, resp.error_code);
    write_i64(buf, resp.producer_id);
    write_i16(buf, resp.producer_epoch);
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = read_i32(buf)?;
    let error_code = read_i16(buf)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
        producer_id,
        producer_epoch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::common::write_nullable_str;

    fn encode_request(req: &Request, version: i16) -> BytesMut {
        let flexible = version >= MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        write_nullable_str(&mut w, req.transactional_id.as_deref(), flexible).unwrap();
        write_i32(&mut w, req.transaction_timeout_ms);
        if version >= 3 {
            write_i64(&mut w, req.producer_id);
            write_i16(&mut w, req.producer_epoch);
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w
    }

    fn sample_request(version: i16) -> Request {
        Request {
            transactional_id: Some("tx-1".to_owned()),
            transaction_timeout_ms: 60_000,
            producer_id: if version >= 3 { 12345 } else { -1 },
            producer_epoch: if version >= 3 { 7 } else { -1 },
        }
    }

    fn roundtrip_request(version: i16) {
        let req = sample_request(version);
        let w = encode_request(&req, version);
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());
    }

    fn roundtrip_response(version: i16) {
        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
            producer_id: 999,
            producer_epoch: 0,
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn request_v0_roundtrip() {
        roundtrip_request(0);
    }
    #[test]
    fn request_v2_flexible_roundtrip() {
        roundtrip_request(2);
    }
    #[test]
    fn request_v3_pid_fields_present() {
        roundtrip_request(3);
    }
    #[test]
    fn request_v4_roundtrip() {
        roundtrip_request(4);
    }

    #[test]
    fn response_v0_roundtrip() {
        roundtrip_response(0);
    }
    #[test]
    fn response_v2_flexible_roundtrip() {
        roundtrip_response(2);
    }
    #[test]
    fn response_v4_roundtrip() {
        roundtrip_response(4);
    }

    #[test]
    fn null_transactional_id_is_non_transactional() {
        let req = Request {
            transactional_id: None,
            transaction_timeout_ms: 0,
            producer_id: -1,
            producer_epoch: -1,
        };
        let w = encode_request(&req, 4);
        let mut r = w.freeze();
        let got = decode_request(&mut r, 4).unwrap();
        assert_eq!(got.transactional_id, None);
    }
}
