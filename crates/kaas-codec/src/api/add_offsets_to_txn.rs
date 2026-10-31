//! AddOffsetsToTxn — API key 25. v0–v3, flexible from v3.
//!
//!
//! Sent by a transactional producer before TxnOffsetCommit to tell
//! the txn coordinator "I'm going to commit offsets for consumer
//! group G as part of this transaction". The coordinator records the
//! group association so EndTxn can sweep pending offsets on
//! commit/abort.

use bytes::BytesMut;

use crate::api::common::read_str;
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i64, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 25,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub group_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let transactional_id = read_str(buf, flexible)?;
    let producer_id = read_i64(buf)?;
    let producer_epoch = read_i16(buf)?;
    let group_id = read_str(buf, flexible)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        transactional_id,
        producer_id,
        producer_epoch,
        group_id,
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
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let throttle_time_ms = crate::primitives::read_i32(buf)?;
    let error_code = read_i16(buf)?;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::common::write_str;
    use crate::primitives::write_i64;

    fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
        let flexible = version >= MIN_FLEXIBLE;
        write_str(buf, &req.transactional_id, flexible)?;
        write_i64(buf, req.producer_id);
        write_i16(buf, req.producer_epoch);
        write_str(buf, &req.group_id, flexible)?;
        if flexible {
            tagged::write_empty(buf);
        }
        Ok(())
    }

    fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
        super::decode_response(buf, version)
    }

    fn sample_request() -> Request {
        Request {
            transactional_id: "tx-1".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            group_id: "g1".to_owned(),
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

        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
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
    fn v2_legacy_roundtrip() {
        roundtrip(2);
    }

    #[test]
    fn v3_flexible_roundtrip() {
        roundtrip(3);
    }
}
