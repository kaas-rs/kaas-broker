//! EndTxn — API key 26. v0–v3, flexible from v3.
//!
//!
//! Sent by a transactional producer to commit or abort the current
//! transaction. The txn coordinator transitions Ongoing → CompleteCommit
//! or CompleteAbort, clears pending offsets via the offset hook, and
//! dispatches WriteTxnMarkers to each partition leader.

use bytes::BytesMut;

use crate::api::common::read_str;
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i64, read_i8, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 3);
pub const MIN_FLEXIBLE: i16 = 3;

pub const SPEC: ApiSpec = ApiSpec {
    key: 26,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    /// `false` = abort, `true` = commit.
    pub committed: bool,
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
    let committed = read_i8(buf)? != 0;
    if flexible {
        tagged::read(buf)?;
    }
    Ok(Request {
        transactional_id,
        producer_id,
        producer_epoch,
        committed,
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
    use crate::primitives::{write_i64, write_i8};

    fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
        let flexible = version >= MIN_FLEXIBLE;
        write_str(buf, &req.transactional_id, flexible)?;
        write_i64(buf, req.producer_id);
        write_i16(buf, req.producer_epoch);
        write_i8(buf, if req.committed { 1 } else { 0 });
        if flexible {
            tagged::write_empty(buf);
        }
        Ok(())
    }

    fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
        super::decode_response(buf, version)
    }

    fn roundtrip(version: i16, committed: bool) {
        let req = Request {
            transactional_id: "tx-1".to_owned(),
            producer_id: 42,
            producer_epoch: 3,
            committed,
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version} committed={committed}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp);
        assert!(r.is_empty());
    }

    #[test]
    fn v0_commit() {
        roundtrip(0, true);
    }

    #[test]
    fn v0_abort() {
        roundtrip(0, false);
    }

    #[test]
    fn v3_flexible_commit() {
        roundtrip(3, true);
    }

    #[test]
    fn v3_flexible_abort() {
        roundtrip(3, false);
    }
}
