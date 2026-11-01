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
//! Deviations, both explicit errors rather than silent drops:
//! - kaas serves SCRAM-SHA-512 only; SHA-256 upsertions answer
//!   `UNSUPPORTED_SASL_MECHANISM` (33).
//! - Deletions answer `UNSUPPORTED_VERSION` (35): a kaas user's
//!   credential lifecycle belongs to its `KafkaUser` CR (delete the
//!   CR or change its `authentication.type`), and the operator would
//!   re-materialise anything the broker removed — a delete that
//!   silently comes back is worse than a refusal.
//!
//! Authorization: `Operation::Alter` on the cluster resource,
//! answered per-user (Apache's shape for this API).

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::alter_user_scram_credentials::{self, mechanism, AlterUserResult, Response};
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
const ERR_UNACCEPTABLE_CREDENTIAL: i16 = 93;
const ERR_UNKNOWN_SERVER: i16 = -1;

/// Apache's minimum PBKDF2 iteration count for SCRAM-SHA-512.
const MIN_ITERATIONS: i32 = 4096;

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
        let authorized =
            self.broker
                .authorizer
                .authorize(&principal, &Resource::cluster(), Operation::Alter);

        let mut results = Vec::with_capacity(req.deletions.len() + req.upsertions.len());

        for d in &req.deletions {
            results.push(if authorized {
                AlterUserResult {
                    user: d.name.clone(),
                    error_code: ERR_UNSUPPORTED_VERSION,
                    error_message: Some(
                        "kaas does not support SCRAM credential deletion over the wire: \
                         the credential lifecycle belongs to the KafkaUser CR (delete the \
                         CR or change spec.authentication.type)"
                            .into(),
                    ),
                }
            } else {
                denied(&d.name)
            });
        }

        for u in req.upsertions {
            if !authorized {
                results.push(denied(&u.name));
                continue;
            }
            if u.mechanism != mechanism::SCRAM_SHA_512 {
                results.push(AlterUserResult {
                    user: u.name,
                    error_code: ERR_UNSUPPORTED_SASL_MECHANISM,
                    error_message: Some("kaas supports SCRAM-SHA-512 only".into()),
                });
                continue;
            }
            if u.iterations < MIN_ITERATIONS || u.salt.is_empty() || u.salted_password.is_empty() {
                results.push(AlterUserResult {
                    user: u.name,
                    error_code: ERR_UNACCEPTABLE_CREDENTIAL,
                    error_message: Some(format!(
                        "salt and saltedPassword must be non-empty and iterations >= {MIN_ITERATIONS}"
                    )),
                });
                continue;
            }
            let Some(writer) = self.broker.user_cr_writer() else {
                results.push(AlterUserResult {
                    user: u.name,
                    error_code: ERR_CLUSTER_AUTHZ_FAILED,
                    error_message: Some("broker is not running in cluster mode".into()),
                });
                continue;
            };

            let (stored_key, server_key) =
                kaas_auth::scram::keys_from_salted_password(&u.salted_password);
            let spec = ScramCredentialSpec {
                salt: u.salt,
                stored_key,
                server_key,
                iterations: u.iterations,
            };
            let (code, msg) = match writer.set_scram_credential(&u.name, spec).await {
                Ok(()) => (ERR_NONE, None),
                Err(UserWriteError::NotFound(_)) => (
                    ERR_RESOURCE_NOT_FOUND,
                    Some("no KafkaUser with this name".to_owned()),
                ),
                Err(UserWriteError::InvalidTarget(m)) => (ERR_INVALID_REQUEST, Some(m)),
                Err(UserWriteError::Forbidden(m)) => (ERR_CLUSTER_AUTHZ_FAILED, Some(m)),
                Err(other) => {
                    tracing::warn!(error = %other, user = %u.name, "AlterUserScramCredentials failed");
                    (ERR_UNKNOWN_SERVER, None)
                }
            };
            results.push(AlterUserResult {
                user: u.name,
                error_code: code,
                error_message: msg,
            });
        }

        let resp = Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        alter_user_scram_credentials::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn denied(user: &str) -> AlterUserResult {
    AlterUserResult {
        user: user.to_owned(),
        error_code: ERR_CLUSTER_AUTHZ_FAILED,
        error_message: None,
    }
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
    }

    #[async_trait]
    impl UserCRWriter for RecordingWriter {
        async fn set_scram_credential(
            &self,
            username: &str,
            cred: ScramCredentialSpec,
        ) -> Result<(), UserWriteError> {
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
        assert_eq!(resp.results[0].error_code, ERR_UNSUPPORTED_SASL_MECHANISM);
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
        assert_eq!(resp.results[0].error_code, ERR_UNACCEPTABLE_CREDENTIAL);
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
        assert_eq!(resp.results[0].error_code, ERR_UNSUPPORTED_VERSION);
        assert!(resp.results[0]
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("KafkaUser"));
    }
}
