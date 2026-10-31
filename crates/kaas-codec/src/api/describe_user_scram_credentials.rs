//! DescribeUserScramCredentials — API key 50 (gh #252, KIP-554).
//!
//! Version 0 only; flexible from v0 (the API was born post-KIP-482).
//! Reports which SCRAM mechanisms a user has credentials for and at
//! how many iterations — never the salt or keys (leaking those lets
//! a privileged observer harvest material for offline attack; see
//! `kaas_auth::ScramInfo`).
//!
//! The request's `users` array is **nullable, and null ≠ empty**:
//! null means "describe every user with SCRAM credentials"
//! (`kafka-configs.sh --describe --entity-type users` with no
//! `--entity-name`), empty means "describe none". The generic
//! `read_array_len` helper folds null into 0, so this module reads
//! the compact-array varint itself.

use bytes::BytesMut;

use crate::api::common::{read_nullable_str, write_array_len, write_nullable_str};
use crate::api::common::{read_str, write_str};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_i16, read_i32, read_i8, read_uvarint, write_i16, write_i32, write_i8, write_uvarint,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 0);
pub const MIN_FLEXIBLE: i16 = 0;

pub const SPEC: ApiSpec = ApiSpec {
    key: 50,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

/// `ScramMechanism` discriminants (Apache's enum).
pub mod mechanism {
    pub const UNKNOWN: i8 = 0;
    pub const SCRAM_SHA_256: i8 = 1;
    pub const SCRAM_SHA_512: i8 = 2;
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// `None` ↔ wire null → every user with SCRAM credentials.
    /// `Some(vec![])` → none.
    pub users: Option<Vec<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    /// Top-level error — request-shape problems only; per-user
    /// problems ride in `results[].error_code`.
    pub error_code: i16,
    pub error_message: Option<String>,
    pub results: Vec<DescribeUserResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeUserResult {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
    pub credential_infos: Vec<CredentialInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CredentialInfo {
    /// See [`mechanism`].
    pub mechanism: i8,
    pub iterations: i32,
}

pub fn decode_request(buf: &mut Bytes, _version: i16) -> Result<Request, CodecError> {
    // Compact nullable array: 0 = null, n = n-1 elements.
    let raw = read_uvarint(buf)?;
    let users = if raw == 0 {
        None
    } else {
        let n = usize::try_from(raw - 1).map_err(|_| CodecError::InvalidUvarint)?;
        let mut users = Vec::with_capacity(n.min(1024));
        for _ in 0..n {
            // Each entry is a struct { name: compact string } + tags.
            let name = read_str(buf, true)?;
            tagged::read(buf)?;
            users.push(name);
        }
        Some(users)
    };
    tagged::read(buf)?;
    Ok(Request { users })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, _version: i16) -> Result<(), CodecError> {
    match &req.users {
        None => write_uvarint(buf, 0),
        Some(users) => {
            write_uvarint(buf, u64::try_from(users.len()).unwrap_or(u64::MAX - 1) + 1);
            for u in users {
                write_str(buf, u, true)?;
                tagged::write_empty(buf);
            }
        }
    }
    tagged::write_empty(buf);
    Ok(())
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    _version: i16,
) -> Result<(), CodecError> {
    write_i32(buf, resp.throttle_time_ms);
    write_i16(buf, resp.error_code);
    write_nullable_str(buf, resp.error_message.as_deref(), true)?;
    write_array_len(buf, resp.results.len(), true)?;
    for r in &resp.results {
        write_str(buf, &r.user, true)?;
        write_i16(buf, r.error_code);
        write_nullable_str(buf, r.error_message.as_deref(), true)?;
        write_array_len(buf, r.credential_infos.len(), true)?;
        for c in &r.credential_infos {
            write_i8(buf, c.mechanism);
            write_i32(buf, c.iterations);
            tagged::write_empty(buf);
        }
        tagged::write_empty(buf);
    }
    tagged::write_empty(buf);
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, _version: i16) -> Result<Response, CodecError> {
    let throttle_time_ms = read_i32(buf)?;
    let error_code = read_i16(buf)?;
    let error_message = read_nullable_str(buf, true)?;
    let n = crate::api::common::read_array_len(buf, true)?;
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = read_str(buf, true)?;
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, true)?;
        let cn = crate::api::common::read_array_len(buf, true)?;
        let mut credential_infos = Vec::with_capacity(cn);
        for _ in 0..cn {
            let mech = read_i8(buf)?;
            let iterations = read_i32(buf)?;
            tagged::read(buf)?;
            credential_infos.push(CredentialInfo {
                mechanism: mech,
                iterations,
            });
        }
        tagged::read(buf)?;
        results.push(DescribeUserResult {
            user,
            error_code,
            error_message,
            credential_infos,
        });
    }
    tagged::read(buf)?;
    Ok(Response {
        throttle_time_ms,
        error_code,
        error_message,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_users_round_trips_distinct_from_empty() {
        for req in [
            Request { users: None },
            Request {
                users: Some(vec![]),
            },
            Request {
                users: Some(vec!["alice".into(), "bob".into()]),
            },
        ] {
            let mut buf = BytesMut::new();
            encode_request(&mut buf, &req, 0).unwrap();
            let mut b = buf.freeze();
            let got = decode_request(&mut b, 0).unwrap();
            assert_eq!(got, req);
            assert!(b.is_empty());
        }
    }

    #[test]
    fn response_round_trips() {
        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
            error_message: None,
            results: vec![DescribeUserResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
                credential_infos: vec![CredentialInfo {
                    mechanism: mechanism::SCRAM_SHA_512,
                    iterations: 4096,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        encode_response(&mut buf, &resp, 0).unwrap();
        let mut b = buf.freeze();
        assert_eq!(decode_response(&mut b, 0).unwrap(), resp);
        assert!(b.is_empty());
    }
}
