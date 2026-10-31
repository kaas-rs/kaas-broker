//! AlterUserScramCredentials — API key 51 (gh #252, KIP-554).
//!
//! Version 0 only; flexible from v0. The client sends *pre-salted*
//! material: the broker never sees the password, only
//! `(salt, salted_password, iterations)` per upsertion — the stored
//! and server keys are derived server-side (RFC 5802). Deletions
//! name a `(user, mechanism)` pair.
//!
//! Response is one row per affected user.

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_bytes, read_nullable_str, read_str, write_array_len,
    write_nullable_bytes, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_i16, read_i32, read_i8, write_i16, write_i32, write_i8};
use crate::tagged;
use crate::Bytes;

pub use crate::api::describe_user_scram_credentials::mechanism;

pub const VERSIONS: (i16, i16) = (0, 0);
pub const MIN_FLEXIBLE: i16 = 0;

pub const SPEC: ApiSpec = ApiSpec {
    key: 51,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub deletions: Vec<ScramCredentialDeletion>,
    pub upsertions: Vec<ScramCredentialUpsertion>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialDeletion {
    pub name: String,
    /// See [`mechanism`].
    pub mechanism: i8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialUpsertion {
    pub name: String,
    /// See [`mechanism`].
    pub mechanism: i8,
    pub iterations: i32,
    pub salt: Vec<u8>,
    /// PBKDF2(HMAC, password, salt, iterations) — the client keeps
    /// the password, the broker derives stored/server keys from this.
    pub salted_password: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<AlterUserResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AlterUserResult {
    pub user: String,
    pub error_code: i16,
    pub error_message: Option<String>,
}

pub fn decode_request(buf: &mut Bytes, _version: i16) -> Result<Request, CodecError> {
    let dn = read_array_len(buf, true)?;
    let mut deletions = Vec::with_capacity(dn);
    for _ in 0..dn {
        let name = read_str(buf, true)?;
        let mech = read_i8(buf)?;
        tagged::read(buf)?;
        deletions.push(ScramCredentialDeletion {
            name,
            mechanism: mech,
        });
    }
    let un = read_array_len(buf, true)?;
    let mut upsertions = Vec::with_capacity(un);
    for _ in 0..un {
        let name = read_str(buf, true)?;
        let mech = read_i8(buf)?;
        let iterations = read_i32(buf)?;
        let salt = read_nullable_bytes(buf, true)?.ok_or(CodecError::UnexpectedEof)?;
        let salted_password = read_nullable_bytes(buf, true)?.ok_or(CodecError::UnexpectedEof)?;
        tagged::read(buf)?;
        upsertions.push(ScramCredentialUpsertion {
            name,
            mechanism: mech,
            iterations,
            salt: salt.to_vec(),
            salted_password: salted_password.to_vec(),
        });
    }
    tagged::read(buf)?;
    Ok(Request {
        deletions,
        upsertions,
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, _version: i16) -> Result<(), CodecError> {
    write_array_len(buf, req.deletions.len(), true)?;
    for d in &req.deletions {
        write_str(buf, &d.name, true)?;
        write_i8(buf, d.mechanism);
        tagged::write_empty(buf);
    }
    write_array_len(buf, req.upsertions.len(), true)?;
    for u in &req.upsertions {
        write_str(buf, &u.name, true)?;
        write_i8(buf, u.mechanism);
        write_i32(buf, u.iterations);
        write_nullable_bytes(buf, Some(&u.salt), true)?;
        write_nullable_bytes(buf, Some(&u.salted_password), true)?;
        tagged::write_empty(buf);
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
    write_array_len(buf, resp.results.len(), true)?;
    for r in &resp.results {
        write_str(buf, &r.user, true)?;
        write_i16(buf, r.error_code);
        write_nullable_str(buf, r.error_message.as_deref(), true)?;
        tagged::write_empty(buf);
    }
    tagged::write_empty(buf);
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, _version: i16) -> Result<Response, CodecError> {
    let throttle_time_ms = read_i32(buf)?;
    let n = read_array_len(buf, true)?;
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let user = read_str(buf, true)?;
        let error_code = read_i16(buf)?;
        let error_message = read_nullable_str(buf, true)?;
        tagged::read(buf)?;
        results.push(AlterUserResult {
            user,
            error_code,
            error_message,
        });
    }
    tagged::read(buf)?;
    Ok(Response {
        throttle_time_ms,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_round_trips() {
        let req = Request {
            deletions: vec![ScramCredentialDeletion {
                name: "old".into(),
                mechanism: mechanism::SCRAM_SHA_512,
            }],
            upsertions: vec![ScramCredentialUpsertion {
                name: "alice".into(),
                mechanism: mechanism::SCRAM_SHA_512,
                iterations: 4096,
                salt: vec![1, 2, 3, 4],
                salted_password: vec![9; 64],
            }],
        };
        let mut buf = BytesMut::new();
        encode_request(&mut buf, &req, 0).unwrap();
        let mut b = buf.freeze();
        assert_eq!(decode_request(&mut b, 0).unwrap(), req);
        assert!(b.is_empty());
    }

    #[test]
    fn response_round_trips() {
        let resp = Response {
            throttle_time_ms: 0,
            results: vec![AlterUserResult {
                user: "alice".into(),
                error_code: 0,
                error_message: None,
            }],
        };
        let mut buf = BytesMut::new();
        encode_response(&mut buf, &resp, 0).unwrap();
        let mut b = buf.freeze();
        assert_eq!(decode_response(&mut b, 0).unwrap(), resp);
        assert!(b.is_empty());
    }
}
