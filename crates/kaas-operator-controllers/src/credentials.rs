//! Materialises `/data/__cluster/credentials.json` from `KafkaUser`
//! CRs. The broker reads the same file (`kaas_auth::CredentialLoader`).
//! Same JSON shape: capitalised keys via `serde(rename_all = ...)`,
//! base64-encoded raw bytes for SCRAM salt / storedKey / serverKey.
//!
//! ## SCRAM-SHA-512 derivation
//!
//! SCRAM computation, line-for-line stable with v0.1:
//!
//! 1. 16-byte random salt from the OS RNG.
//! 2. `SaltedPassword = PBKDF2-HMAC-SHA512(password, salt, 8192, 64)`.
//! 3. `ClientKey = HMAC-SHA512(SaltedPassword, "Client Key")`.
//! 4. `StoredKey = SHA512(ClientKey)`.
//! 5. `ServerKey = HMAC-SHA512(SaltedPassword, "Server Key")`.
//!
//! The plaintext password is **never** stored — only the derived keys
//! land in `credentials.json`. Iteration count is pinned at 8192,
//! same as Strimzi's `User Operator` default and Apache Kafka 3.7's
//! recommended floor for SCRAM-SHA-512.

use std::path::{Path, PathBuf};

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac_array;
use rand::{rngs::OsRng, RngCore};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};

use crate::errors::ControllerError;

/// Iteration count used for PBKDF2-HMAC-SHA512.
pub const SCRAM_ITERATIONS: u32 = 8192;

/// On-disk shape of `__cluster/credentials.json`.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CredentialsFile {
    pub version: u32,
    #[serde(default)]
    pub users: Vec<UserCredential>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct UserCredential {
    pub username: String,
    pub auth_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram: Option<ScramCredential>,
    /// CN used for mTLS principal lookup. Renamed `tlsCN` on disk
    /// (v0.1 field name).
    #[serde(default, rename = "tlsCN", skip_serializing_if = "String::is_empty")]
    pub tls_cn: String,
    /// ServiceAccount reference (Phase 7 K8s-SA path).
    #[serde(
        default,
        rename = "serviceAccount",
        skip_serializing_if = "Option::is_none"
    )]
    pub sa: Option<SaCredential>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quotas: Option<CredQuotas>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct ScramCredential {
    /// Base64-encoded random 16-byte salt.
    pub salt: String,
    /// Base64-encoded `SHA512(HMAC(SaltedPw, "Client Key"))`.
    pub stored_key: String,
    /// Base64-encoded `HMAC(SaltedPw, "Server Key")`.
    pub server_key: String,
    pub iterations: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SaCredential {
    pub name: String,
    pub namespace: String,
}

/// Per-user quota knobs. Field names mirror the CRD's `spec.quotas`
/// so both halves of `credentials.json` (operator writer + broker
/// reader) use the same JSON tags.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct CredQuotas {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_max_byte_rate_per_broker: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consumer_max_byte_rate_per_broker: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_percentage: Option<i32>,
}

impl CredentialsFile {
    /// Replace or append `cred` keyed by `username`.
    pub fn upsert_user(&mut self, cred: UserCredential) {
        for slot in self.users.iter_mut() {
            if slot.username == cred.username {
                *slot = cred;
                return;
            }
        }
        self.users.push(cred);
    }

    /// Drop the named user. Returns `true` if an entry was actually
    /// removed, so callers can skip an unchanged rewrite.
    pub fn remove_user(&mut self, username: &str) -> bool {
        let before = self.users.len();
        self.users.retain(|u| u.username != username);
        self.users.len() != before
    }

    /// `true` if any entry has the named user.
    pub fn has_user(&self, username: &str) -> bool {
        self.users.iter().any(|u| u.username == username)
    }
}

/// `<cluster_dir>/credentials.json`. Takes the cluster-state dir
/// itself — the operator resolves `KAAS_CLUSTER_DIR` (default
/// `<data_dir>/__cluster`, gh #221 phase 1) once at startup.
pub fn credentials_path(cluster_dir: &Path) -> PathBuf {
    cluster_dir.join("credentials.json")
}

/// Read `credentials.json`. Returns an empty `{version: 1, users: []}`
/// when the file is absent (pre-operator-write boot returns
/// no error, just an empty struct so the next write upserts cleanly).
pub fn read_credentials(cluster_dir: &Path) -> Result<CredentialsFile, ControllerError> {
    let path = credentials_path(cluster_dir);
    match std::fs::read(&path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(CredentialsFile {
            version: 1,
            users: Vec::new(),
        }),
        Err(e) => Err(ControllerError::Io(e)),
    }
}

/// Atomic write of `credentials.json` via `<kaas_storage>::atomic_write`
/// (tempfile + rename + fsync). Stamps `version = 1` so downstream
/// readers can gate on it later if we ever bump the format.
pub fn write_credentials(cluster_dir: &Path, cf: &CredentialsFile) -> Result<(), ControllerError> {
    let cluster_dir = cluster_dir.to_path_buf();
    kaas_storage::fs::create_dir_all_durable(&cluster_dir)?;
    let mut stamped = cf.clone();
    stamped.version = 1;
    let fs = kaas_storage::fs::RealFs::new();
    kaas_storage::atomic_write::atomic_write_json_pretty(
        &fs,
        &cluster_dir,
        "credentials.json",
        &stamped,
    )?;
    Ok(())
}

/// Derive a SCRAM-SHA-512 credential from a plaintext password.
///
/// Random 16-byte salt drawn from `OsRng`; the password itself is
/// never stored. Byte-for-byte stable with the v0.1 output for a
/// given (password, salt) pair.
pub fn compute_scram(password: &str) -> Result<ScramCredential, ControllerError> {
    let mut salt = [0u8; 16];
    OsRng.try_fill_bytes(&mut salt)?;

    let salted_pw: [u8; 64] =
        pbkdf2_hmac_array::<Sha512, 64>(password.as_bytes(), &salt, SCRAM_ITERATIONS);

    let client_key = hmac_sha512(&salted_pw, b"Client Key");
    let stored_key = sha512(&client_key);
    let server_key = hmac_sha512(&salted_pw, b"Server Key");

    Ok(ScramCredential {
        salt: BASE64.encode(salt),
        stored_key: BASE64.encode(stored_key),
        server_key: BASE64.encode(server_key),
        iterations: SCRAM_ITERATIONS,
    })
}

/// 32-char alphanumeric password generator. Used by the operator's
/// gh #136 path: when `KafkaUser.spec.authentication.password` is
/// unset, the operator generates a stable password on first reconcile
/// and writes it to the output Secret `<user>-kafka-credentials`.
/// Subsequent reconciles read the password back from the same Secret
/// (so the SCRAM hash stays stable across operator restarts —
/// matches Strimzi's User Operator behaviour).
pub fn generate_alphanum_password(len: usize) -> Result<String, ControllerError> {
    // Draw ASCII letters + digits uniformly via
    // rejection sampling against an OsRng byte stream rather than
    // pulling in `rand::distributions::Alphanumeric` (avoids the
    // distribution-trait surface on the SDK boundary).
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    // Use the largest multiple of ALPHABET.len() that fits in u8 so
    // modular reduction is unbiased. ALPHABET.len() == 62 → cutoff = 62*4 = 248.
    let cutoff = usize::from(u8::MAX) + 1 - ((usize::from(u8::MAX) + 1) % ALPHABET.len());
    let mut out = Vec::with_capacity(len);
    let mut buf = [0u8; 64];
    while out.len() < len {
        OsRng.try_fill_bytes(&mut buf)?;
        for b in buf {
            let idx = usize::from(b);
            if idx >= cutoff {
                continue;
            }
            out.push(ALPHABET[idx % ALPHABET.len()]);
            if out.len() == len {
                break;
            }
        }
    }
    String::from_utf8(out).map_err(|e| ControllerError::Other(format!("password utf8: {e}")))
}

fn hmac_sha512(key: &[u8], msg: &[u8]) -> [u8; 64] {
    let Ok(mut mac) = Hmac::<Sha512>::new_from_slice(key) else {
        // HMAC<Sha512> accepts any key length; the only failure path is
        // an internal invariant that has never tripped. Surface defensively.
        return [0u8; 64];
    };
    mac.update(msg);
    let out = mac.finalize().into_bytes();
    let mut arr = [0u8; 64];
    arr.copy_from_slice(&out);
    arr
}

fn sha512(data: &[u8]) -> [u8; 64] {
    let digest = Sha512::digest(data);
    let mut arr = [0u8; 64];
    arr.copy_from_slice(&digest);
    arr
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_file_roundtrip() {
        let mut cf = CredentialsFile {
            version: 1,
            users: vec![],
        };
        cf.upsert_user(UserCredential {
            username: "alice".into(),
            auth_type: "scram-sha-512".into(),
            scram: Some(ScramCredential {
                salt: "QUFBQUFBQUFBQUFBQUFBQQ==".into(),
                stored_key: "c3RvcmVk".into(),
                server_key: "c2VydmVy".into(),
                iterations: SCRAM_ITERATIONS,
            }),
            tls_cn: String::new(),
            sa: None,
            quotas: None,
        });
        let json = serde_json::to_string(&cf).expect("serialise");
        // verify the on-disk field names match the v0.1 output verbatim
        assert!(json.contains("\"authType\":\"scram-sha-512\""));
        assert!(json.contains("\"storedKey\":\"c3RvcmVk\""));
        assert!(json.contains("\"serverKey\":\"c2VydmVy\""));
        let back: CredentialsFile = serde_json::from_str(&json).expect("roundtrip");
        assert_eq!(cf, back);
    }

    #[test]
    fn upsert_replaces_existing() {
        let mut cf = CredentialsFile {
            version: 1,
            users: vec![],
        };
        cf.upsert_user(UserCredential {
            username: "alice".into(),
            auth_type: "tls".into(),
            scram: None,
            tls_cn: "first".into(),
            sa: None,
            quotas: None,
        });
        cf.upsert_user(UserCredential {
            username: "alice".into(),
            auth_type: "tls".into(),
            scram: None,
            tls_cn: "second".into(),
            sa: None,
            quotas: None,
        });
        assert_eq!(cf.users.len(), 1);
        assert_eq!(cf.users[0].tls_cn, "second");
    }

    #[test]
    fn remove_user_is_noop_when_absent() {
        let mut cf = CredentialsFile::default();
        assert!(!cf.remove_user("nobody"), "nothing to remove → false");
        assert!(cf.users.is_empty());
    }

    #[test]
    fn remove_user_reports_removal() {
        let mut cf = CredentialsFile::default();
        cf.upsert_user(UserCredential {
            username: "gone".into(),
            auth_type: "scram-sha-512".into(),
            scram: None,
            tls_cn: String::new(),
            sa: None,
            quotas: None,
        });
        assert!(cf.remove_user("gone"), "removed an entry → true");
        assert!(!cf.remove_user("gone"), "already gone → false");
    }

    #[test]
    fn compute_scram_produces_well_shaped_keys() {
        let s = compute_scram("hunter2").expect("derive");
        assert_eq!(s.iterations, SCRAM_ITERATIONS);
        // salt: base64-encoded 16 bytes → 24 chars including "=" padding
        let salt_bytes = BASE64.decode(&s.salt).expect("salt decodes");
        assert_eq!(salt_bytes.len(), 16);
        // storedKey / serverKey: base64-encoded SHA-512 / HMAC-SHA512 → 64 bytes
        let stored = BASE64.decode(&s.stored_key).expect("stored decodes");
        assert_eq!(stored.len(), 64);
        let server = BASE64.decode(&s.server_key).expect("server decodes");
        assert_eq!(server.len(), 64);
    }

    #[test]
    fn compute_scram_is_distinct_per_salt() {
        // Two derivations of the same password use independent salts;
        // every field except `iterations` differs.
        let a = compute_scram("samepw").unwrap();
        let b = compute_scram("samepw").unwrap();
        assert_ne!(a.salt, b.salt);
        assert_ne!(a.stored_key, b.stored_key);
        assert_ne!(a.server_key, b.server_key);
        assert_eq!(a.iterations, b.iterations);
    }

    #[test]
    fn generate_alphanum_password_length_and_charset() {
        let pw = generate_alphanum_password(32).unwrap();
        assert_eq!(pw.len(), 32);
        assert!(pw.chars().all(|c| c.is_ascii_alphanumeric()));
    }

    #[test]
    fn read_credentials_absent_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let cf = read_credentials(tmp.path()).expect("ok");
        assert_eq!(cf.version, 1);
        assert!(cf.users.is_empty());
    }

    #[test]
    fn write_then_read_roundtrip_via_atomic_write() {
        let tmp = tempfile::tempdir().unwrap();
        let mut cf = CredentialsFile::default();
        cf.upsert_user(UserCredential {
            username: "bob".into(),
            auth_type: "kubernetes-serviceaccount".into(),
            scram: None,
            tls_cn: String::new(),
            sa: Some(SaCredential {
                name: "bob-sa".into(),
                namespace: "default".into(),
            }),
            quotas: Some(CredQuotas {
                producer_max_byte_rate_per_broker: Some(1_000_000),
                consumer_max_byte_rate_per_broker: None,
                request_percentage: Some(25),
            }),
        });
        write_credentials(tmp.path(), &cf).expect("write");
        let back = read_credentials(tmp.path()).expect("read");
        assert_eq!(back.version, 1);
        assert_eq!(back.users.len(), 1);
        assert_eq!(back.users[0].username, "bob");
        assert_eq!(
            back.users[0]
                .quotas
                .as_ref()
                .unwrap()
                .producer_max_byte_rate_per_broker,
            Some(1_000_000)
        );
    }
}
