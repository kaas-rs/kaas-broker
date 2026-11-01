//! AlterUserScramCredentials handler — API key 51 (gh #252,
//! KIP-554).
//!
//! Upsertions derive the stored/server keys from the wire's
//! `(salt, salted_password, iterations)` (the broker never sees the
//! password) and persist them by patching
//! `KafkaUser.spec.authentication.scram` via the installed
//! [`UserCRWriter`] — the operator materialises `credentials.json`
//! on reconcile and every broker hot-reloads it, so the rotation is
//! **asynchronous** like every CR-mediated admin write (typically
//! well under the ~5 s reload tick).
//!
//! Deviations, all explicit errors rather than silent drops:
//! - kaas serves SCRAM-SHA-512 only; SHA-256 upsertions answer
//!   `UNSUPPORTED_SASL_MECHANISM` (33).
//! - Deletions answer `UNSUPPORTED_VERSION` (35): a kaas user's
//!   credential lifecycle belongs to its `KafkaUser` CR (delete the
//!   CR or change its `authentication.type`), and the operator would
//!   re-materialise anything the broker removed — a delete that
//!   silently comes back is worse than a refusal.
//!
//! - An upsertion for a user with no `KafkaUser` CR answers
//!   `RESOURCE_NOT_FOUND` (91). Apache's upsert creates the user; here
//!   the CR owns the user's lifecycle.
//!
//! Classification follows Apache's `ScramControlManager` (gh #286): one
//! result per distinct user, `DUPLICATE_RESOURCE` (92) for a user named
//! twice (a deletion plus an upsertion included, and nothing is
//! written), and iterations outside 4096..=16384 are
//! `UNACCEPTABLE_CREDENTIAL` (93).
//!
//! Authorization: `Operation::Alter` on the cluster resource. A denial
//! answers one sorted row per distinct user carrying
//! `CLUSTER_AUTHORIZATION_FAILED` (31), as Apache's `getErrorResponse`
//! does.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::alter_user_scram_credentials::{
    self, mechanism, AlterUserResult, Response, ScramCredentialUpsertion,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;
use crate::user_cr_writer::{ScramCredentialSpec, UserWriteError};

const ERR_NONE: i16 = 0;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_UNSUPPORTED_SASL_MECHANISM: i16 = 33;
const ERR_UNSUPPORTED_VERSION: i16 = 35;
const ERR_INVALID_REQUEST: i16 = 42;
const ERR_RESOURCE_NOT_FOUND: i16 = 91; // gh #276: was 83, ELIGIBLE_LEADERS_NOT_AVAILABLE
const ERR_DUPLICATE_RESOURCE: i16 = 92;
const ERR_UNACCEPTABLE_CREDENTIAL: i16 = 93;
const ERR_UNKNOWN_SERVER: i16 = -1;

/// Apache's minimum PBKDF2 iteration count for SCRAM-SHA-512.
const MIN_ITERATIONS: i32 = 4096;
/// Apache's maximum (`ScramMechanism.maxIterations`, both mechanisms).
const MAX_ITERATIONS: i32 = 16384;

#[derive(Debug)]
pub struct AlterUserScramCredentialsHandler {
    broker: Arc<Broker>,
}

impl AlterUserScramCredentialsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for AlterUserScramCredentialsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = alter_user_scram_credentials::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let results =
            if self
                .broker
                .authorizer
                .authorize(&principal, &Resource::cluster(), Operation::Alter)
            {
                self.alter(req).await
            } else {
                // Apache's `getErrorResponse`: one row per distinct user,
                // sorted, each carrying the error.
                let users: BTreeSet<String> = req
                    .deletions
                    .into_iter()
                    .map(|d| d.name)
                    .chain(req.upsertions.into_iter().map(|u| u.name))
                    .collect();
                users
                    .into_iter()
                    .map(|user| AlterUserResult {
                        user,
                        error_code: ERR_CLUSTER_AUTHZ_FAILED,
                        error_message: Some("Cluster authorization failed.".into()),
                    })
                    .collect()
            };

        let resp = Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        alter_user_scram_credentials::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

impl AlterUserScramCredentialsHandler {
    /// One result per distinct user, sorted by name. Entries are
    /// classified the way Apache's `ScramControlManager.alterCredentials`
    /// does it (gh #286): deletions first, then upsertions.
    /// - A user already marked with an error ignores further entries.
    /// - A second entry for a user already queued (deletion or
    ///   upsertion) drops the queued one and marks the user
    ///   `DUPLICATE_RESOURCE` (92). Nothing is written for that user, so
    ///   a delete-plus-upsert can't report a failure while the
    ///   credential was in fact rotated.
    /// - Otherwise the entry is validated and queued.
    async fn alter(&self, req: alter_user_scram_credentials::Request) -> Vec<AlterUserResult> {
        let mut errors: BTreeMap<String, (i16, String)> = BTreeMap::new();
        let mut deletions: BTreeSet<String> = BTreeSet::new();
        let mut upsertions: BTreeMap<String, ScramCredentialUpsertion> = BTreeMap::new();

        for d in req.deletions {
            if errors.contains_key(&d.name) {
                continue;
            }
            if deletions.remove(&d.name) {
                errors.insert(d.name, duplicate());
            } else if let Err(e) = validate_name_and_mechanism(&d.name, d.mechanism) {
                errors.insert(d.name, e);
            } else {
                deletions.insert(d.name);
            }
        }
        for u in req.upsertions {
            if errors.contains_key(&u.name) {
                continue;
            }
            if deletions.remove(&u.name) || upsertions.remove(&u.name).is_some() {
                errors.insert(u.name, duplicate());
            } else if let Err(e) = validate_upsertion(&u) {
                errors.insert(u.name, e);
            } else {
                upsertions.insert(u.name.clone(), u);
            }
        }

        let mut results: BTreeMap<String, (i16, Option<String>)> = errors
            .into_iter()
            .map(|(user, (code, msg))| (user, (code, Some(msg))))
            .collect();
        for user in deletions {
            results.insert(
                user,
                (
                    ERR_UNSUPPORTED_VERSION,
                    Some(
                        "kaas does not support SCRAM credential deletion over the wire: \
                         the credential lifecycle belongs to the KafkaUser CR (delete the \
                         CR or change spec.authentication.type)"
                            .into(),
                    ),
                ),
            );
        }
        for (user, u) in upsertions {
            let outcome = self.write(&user, u).await;
            results.insert(user, outcome);
        }
        results
            .into_iter()
            .map(|(user, (error_code, error_message))| AlterUserResult {
                user,
                error_code,
                error_message,
            })
            .collect()
    }

    async fn write(&self, user: &str, u: ScramCredentialUpsertion) -> (i16, Option<String>) {
        let Some(writer) = self.broker.user_cr_writer() else {
            return (
                ERR_CLUSTER_AUTHZ_FAILED,
                Some("broker is not running in cluster mode".into()),
            );
        };
        let (stored_key, server_key) =
            kaas_auth::scram::keys_from_salted_password(&u.salted_password);
        let spec = ScramCredentialSpec {
            salt: u.salt,
            stored_key,
            server_key,
            iterations: u.iterations,
        };
        match writer.set_scram_credential(user, spec).await {
            Ok(()) => (ERR_NONE, None),
            // kaas deviation: the KafkaUser CR owns the user's lifecycle,
            // so an upsert can't create one. Apache's upsert never fails
            // this way.
            Err(UserWriteError::NotFound(_)) => (
                ERR_RESOURCE_NOT_FOUND,
                Some("no KafkaUser with this name".to_owned()),
            ),
            Err(UserWriteError::InvalidTarget(m)) => (ERR_INVALID_REQUEST, Some(m)),
            Err(UserWriteError::Forbidden(m)) => (ERR_CLUSTER_AUTHZ_FAILED, Some(m)),
            Err(other) => {
                tracing::warn!(error = %other, user, "AlterUserScramCredentials failed");
                (ERR_UNKNOWN_SERVER, None)
            }
        }
    }
}

fn duplicate() -> (i16, String) {
    (
        ERR_DUPLICATE_RESOURCE,
        "A user credential cannot be altered twice in the same request".into(),
    )
}

/// Apache's `validateScramUsernameAndMechanism`.
fn validate_name_and_mechanism(name: &str, mech: i8) -> Result<(), (i16, String)> {
    if name.is_empty() {
        return Err((
            ERR_UNACCEPTABLE_CREDENTIAL,
            "Username must not be empty".into(),
        ));
    }
    if mech != mechanism::SCRAM_SHA_256 && mech != mechanism::SCRAM_SHA_512 {
        return Err((
            ERR_UNSUPPORTED_SASL_MECHANISM,
            "Unknown SCRAM mechanism".into(),
        ));
    }
    Ok(())
}

/// Apache's `validateUpsertion` (iterations within 4096..=16384), then
/// kaas's two additions: SCRAM-SHA-512 only, and non-empty salt and
/// salted password. Running them last means an upsertion Apache rejects
/// gets Apache's answer.
fn validate_upsertion(u: &ScramCredentialUpsertion) -> Result<(), (i16, String)> {
    validate_name_and_mechanism(&u.name, u.mechanism)?;
    if u.iterations < MIN_ITERATIONS {
        return Err((ERR_UNACCEPTABLE_CREDENTIAL, "Too few iterations".into()));
    }
    if u.iterations > MAX_ITERATIONS {
        return Err((ERR_UNACCEPTABLE_CREDENTIAL, "Too many iterations".into()));
    }
    if u.mechanism != mechanism::SCRAM_SHA_512 {
        return Err((
            ERR_UNSUPPORTED_SASL_MECHANISM,
            "kaas supports SCRAM-SHA-512 only".into(),
        ));
    }
    if u.salt.is_empty() || u.salted_password.is_empty() {
        return Err((
            ERR_UNACCEPTABLE_CREDENTIAL,
            "salt and saltedPassword must be non-empty".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use crate::user_cr_writer::UserCRWriter;
    use kaas_codec::api::alter_user_scram_credentials::{
        Request, ScramCredentialDeletion, ScramCredentialUpsertion,
    };
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn() -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            "internal",
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    #[derive(Debug, Default)]
    struct RecordingWriter {
        calls: Mutex<Vec<(String, ScramCredentialSpec)>>,
        /// Answer every write as "no such KafkaUser".
        missing: bool,
    }

    #[async_trait]
    impl UserCRWriter for RecordingWriter {
        async fn set_scram_credential(
            &self,
            username: &str,
            cred: ScramCredentialSpec,
        ) -> Result<(), UserWriteError> {
            if self.missing {
                return Err(UserWriteError::NotFound(username.to_owned()));
            }
            self.calls.lock().push((username.to_owned(), cred));
            Ok(())
        }
    }

    fn broker_with_writer(w: Arc<RecordingWriter>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        b.install_user_cr_writer(w);
        b
    }

    async fn alter(broker: Arc<Broker>, req: Request) -> Response {
        let h = AlterUserScramCredentialsHandler::new(broker);
        let mut buf = BytesMut::new();
        alter_user_scram_credentials::encode_request(&mut buf, &req, 0).unwrap();
        let out = h.handle(&conn(), 0, buf.freeze()).await.unwrap();
        let mut r = out.freeze();
        alter_user_scram_credentials::decode_response(&mut r, 0).unwrap()
    }

    fn upsertion(name: &str) -> ScramCredentialUpsertion {
        ScramCredentialUpsertion {
            name: name.into(),
            mechanism: mechanism::SCRAM_SHA_512,
            iterations: 4096,
            salt: vec![1, 2, 3, 4],
            salted_password: vec![7; 64],
        }
    }

    #[tokio::test]
    async fn an_upsertion_derives_keys_and_reaches_the_writer() {
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(
            broker_with_writer(w.clone()),
            Request {
                deletions: vec![],
                upsertions: vec![upsertion("alice")],
            },
        )
        .await;
        assert_eq!(resp.results[0].error_code, ERR_NONE);
        let calls = w.calls.lock();
        assert_eq!(calls.len(), 1);
        let (user, cred) = &calls[0];
        assert_eq!(user, "alice");
        assert_eq!(cred.iterations, 4096);
        // The derived keys must match the RFC 5802 derivation the
        // SCRAM verifier uses — otherwise the rotated credential
        // can never authenticate anyone.
        let (stored, server) = kaas_auth::scram::keys_from_salted_password(&[7; 64]);
        assert_eq!(cred.stored_key, stored);
        assert_eq!(cred.server_key, server);
    }

    #[tokio::test]
    async fn sha256_answers_unsupported_sasl_mechanism() {
        let w = Arc::new(RecordingWriter::default());
        let mut up = upsertion("alice");
        up.mechanism = mechanism::SCRAM_SHA_256;
        let resp = alter(
            broker_with_writer(w.clone()),
            Request {
                deletions: vec![],
                upsertions: vec![up],
            },
        )
        .await;
        assert_eq!(resp.results[0].error_code, 33, "UNSUPPORTED_SASL_MECHANISM");
        assert!(w.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn low_iterations_answer_unacceptable_credential() {
        let w = Arc::new(RecordingWriter::default());
        let mut up = upsertion("alice");
        up.iterations = 1;
        let resp = alter(
            broker_with_writer(w.clone()),
            Request {
                deletions: vec![],
                upsertions: vec![up],
            },
        )
        .await;
        assert_eq!(resp.results[0].error_code, 93, "UNACCEPTABLE_CREDENTIAL");
        assert!(w.calls.lock().is_empty());
    }

    #[tokio::test]
    async fn deletions_are_refused_not_dropped() {
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(
            broker_with_writer(w),
            Request {
                deletions: vec![ScramCredentialDeletion {
                    name: "alice".into(),
                    mechanism: mechanism::SCRAM_SHA_512,
                }],
                upsertions: vec![],
            },
        )
        .await;
        assert_eq!(resp.results[0].error_code, 35, "UNSUPPORTED_VERSION");
        assert!(resp.results[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("KafkaUser"));
    }

    /// gh #276: a user with no KafkaUser CR answers Apache's
    /// RESOURCE_NOT_FOUND, 91 — it went out as 83
    /// (ELIGIBLE_LEADERS_NOT_AVAILABLE), and no test pinned it. The
    /// other tests here assert Apache's literals for the same reason.
    #[tokio::test]
    async fn a_user_without_a_cr_is_resource_not_found_91() {
        let w = Arc::new(RecordingWriter {
            missing: true,
            ..RecordingWriter::default()
        });
        let resp = alter(
            broker_with_writer(w),
            Request {
                deletions: vec![],
                upsertions: vec![upsertion("ghost")],
            },
        )
        .await;
        assert_eq!(resp.results[0].error_code, 91);
    }

    fn deletion(name: &str) -> ScramCredentialDeletion {
        ScramCredentialDeletion {
            name: name.into(),
            mechanism: mechanism::SCRAM_SHA_512,
        }
    }

    fn codes(resp: &Response) -> Vec<(&str, i16)> {
        resp.results
            .iter()
            .map(|r| (r.user.as_str(), r.error_code))
            .collect()
    }

    /// gh #286: a user named twice in one request is Apache's
    /// DUPLICATE_RESOURCE (92), one row, and nothing is written. That
    /// covers a deletion plus an upsertion (`kafka-configs.sh` with both
    /// --add-config and --delete-config) and two upsertions; a third
    /// entry for the user changes nothing. Other users are unaffected.
    #[tokio::test]
    async fn a_user_named_twice_is_duplicate_resource_92_and_nothing_is_written() {
        let w = Arc::new(RecordingWriter::default());
        let resp = alter(
            broker_with_writer(w.clone()),
            Request {
                deletions: vec![deletion("alice")],
                upsertions: vec![
                    upsertion("alice"),
                    upsertion("bob"),
                    upsertion("bob"),
                    upsertion("bob"),
                    upsertion("carol"),
                ],
            },
        )
        .await;
        assert_eq!(codes(&resp), [("alice", 92), ("bob", 92), ("carol", 0)]);
        let calls = w.calls.lock();
        let written: Vec<&str> = calls.iter().map(|(u, _)| u.as_str()).collect();
        assert_eq!(written, ["carol"], "a duplicated user must not be rotated");
    }

    /// gh #286: Apache caps iterations at 16384 (UNACCEPTABLE_CREDENTIAL,
    /// 93); the cap itself is accepted.
    #[tokio::test]
    async fn iterations_above_16384_are_unacceptable_credential_93() {
        let w = Arc::new(RecordingWriter::default());
        let mut over = upsertion("alice");
        over.iterations = 16_385;
        let mut cap = upsertion("bob");
        cap.iterations = 16_384;
        let resp = alter(
            broker_with_writer(w.clone()),
            Request {
                deletions: vec![],
                upsertions: vec![over, cap],
            },
        )
        .await;
        assert_eq!(codes(&resp), [("alice", 93), ("bob", 0)]);
        assert_eq!(
            resp.results[0].error_message.as_deref(),
            Some("Too many iterations")
        );
        assert_eq!(w.calls.lock().len(), 1);
    }

    /// gh #286: Apache's `getErrorResponse` on a cluster-authorization
    /// denial is one sorted row per distinct user carrying 31.
    #[tokio::test]
    async fn a_denial_is_one_sorted_row_per_user() {
        use crate::handlers::test_authz::DenyAllAuthorizer;
        let w = Arc::new(RecordingWriter::default());
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let b = Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
            Arc::new(DenyAllAuthorizer),
            Arc::new(kaas_auth::NoQuotaChecker),
        ));
        b.install_user_cr_writer(w.clone());
        let resp = alter(
            b,
            Request {
                deletions: vec![deletion("bob")],
                upsertions: vec![upsertion("carol"), upsertion("alice"), upsertion("carol")],
            },
        )
        .await;
        assert_eq!(codes(&resp), [("alice", 31), ("bob", 31), ("carol", 31)]);
        assert!(w.calls.lock().is_empty());
    }
}
