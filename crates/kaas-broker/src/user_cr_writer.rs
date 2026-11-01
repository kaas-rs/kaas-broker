//! `UserCRWriter` — broker → `KafkaUser.spec.authentication.scram`
//! path (gh #252, KIP-554).
//!
//! `AlterUserScramCredentials` (key 51) persists a rotated credential
//! by patching the pre-derived SCRAM material into the user's CR —
//! the exact flow the CRD anticipated ("the wire-level admin path
//! uses this to rotate runtime credentials without an intermediate
//! Secret"). The operator's reconcile then passes it through to
//! `credentials.json` verbatim and every broker hot-reloads it. Same
//! CR-mediated shape as the topic and ACL writers: no broker →
//! operator coupling, no second writer of `credentials.json`.
//!
//! The writer never *creates* a `KafkaUser` (404 → [`UserWriteError::NotFound`]),
//! matching the ACL writer's edit-only stance: users are declarative
//! resources; the wire rotates credentials for users that exist.

use async_trait::async_trait;
use thiserror::Error;

/// Pre-derived SCRAM-SHA-512 material to land on the CR. Raw bytes —
/// the kube impl base64-encodes at the boundary, mirroring
/// `KafkaUserScramCredential`'s wire shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScramCredentialSpec {
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
    pub iterations: i32,
}

/// Errors mapped to wire codes at the handler boundary:
#[derive(Debug, Error)]
pub enum UserWriteError {
    /// No `KafkaUser` CR with this name. Wire: `RESOURCE_NOT_FOUND` (91).
    #[error("user not found: {0}")]
    NotFound(String),

    /// The CR exists but its `authentication.type` is something other
    /// than scram-sha-512 (tls, kubernetes-serviceaccount) — stamping
    /// SCRAM material would silently flip the user's auth mechanism.
    /// Wire: `INVALID_REQUEST` (42) with the message.
    #[error("invalid target: {0}")]
    InvalidTarget(String),

    /// RBAC / admission refusal. Wire: `CLUSTER_AUTHORIZATION_FAILED` (31).
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// Anything else. Wire: `UNKNOWN_SERVER_ERROR` (-1).
    #[error("other: {0}")]
    Other(String),
}

/// Credential mutations the AlterUserScramCredentials handler issues.
#[async_trait]
pub trait UserCRWriter: Send + Sync + 'static {
    /// Upsert the user's SCRAM-SHA-512 credential: patch
    /// `spec.authentication.{type: scram-sha-512, scram: <spec>}`.
    /// Only legal when the CR's current `authentication.type` is
    /// `scram-sha-512` or unset — see [`UserWriteError::InvalidTarget`].
    async fn set_scram_credential(
        &self,
        username: &str,
        cred: ScramCredentialSpec,
    ) -> Result<(), UserWriteError>;
}

// --- kube-backed impl ------------------------------------------------

#[cfg(feature = "cr-writer")]
pub use kube_impl::KubeUserCRWriter;

#[cfg(feature = "cr-writer")]
mod kube_impl {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use kaas_operator_api::KafkaUser;
    use kube::api::{Patch, PatchParams};
    use kube::Api;
    use serde_json::json;

    /// Real kube-backed writer over the KafkaUser CRs in one namespace.
    #[derive(Clone)]
    pub struct KubeUserCRWriter {
        client: kube::Client,
        namespace: String,
    }

    impl std::fmt::Debug for KubeUserCRWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KubeUserCRWriter")
                .field("namespace", &self.namespace)
                .finish_non_exhaustive()
        }
    }

    impl KubeUserCRWriter {
        pub fn new(client: kube::Client, namespace: impl Into<String>) -> Self {
            Self {
                client,
                namespace: namespace.into(),
            }
        }

        fn api(&self) -> Api<KafkaUser> {
            Api::namespaced(self.client.clone(), &self.namespace)
        }
    }

    #[async_trait]
    impl UserCRWriter for KubeUserCRWriter {
        async fn set_scram_credential(
            &self,
            username: &str,
            cred: ScramCredentialSpec,
        ) -> Result<(), UserWriteError> {
            // Read-first (same shape as expand_topic's decrease
            // guard): a 404 is the honest RESOURCE_NOT_FOUND, and a
            // non-SCRAM user must not have its auth mechanism flipped
            // by a credential rotation.
            let user = match self.api().get(username).await {
                Ok(u) => u,
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(UserWriteError::NotFound(username.into()));
                }
                Err(e) => return Err(map_kube_err(e)),
            };
            // An authorization-only user (no authentication block, gh #42)
            // has no SCRAM mechanism to rotate — reject rather than
            // stamp one on and silently convert it.
            match user.spec.authentication.as_ref().map(|a| a.kind.as_str()) {
                Some("scram-sha-512") | Some("") => {}
                Some(other) => {
                    return Err(UserWriteError::InvalidTarget(format!(
                        "KafkaUser {username} has authentication.type {other:?}; \
                         SCRAM credential rotation applies to scram-sha-512 users only"
                    )));
                }
                None => {
                    return Err(UserWriteError::InvalidTarget(format!(
                        "KafkaUser {username} is authorization-only (no authentication); \
                         SCRAM credential rotation applies to scram-sha-512 users only"
                    )));
                }
            }

            let patch = json!({
                "spec": {
                    "authentication": {
                        "type": "scram-sha-512",
                        "scram": {
                            "salt": BASE64.encode(&cred.salt),
                            "storedKey": BASE64.encode(&cred.stored_key),
                            "serverKey": BASE64.encode(&cred.server_key),
                            "iterations": cred.iterations,
                        }
                    }
                }
            });
            let pp = PatchParams {
                // gh #245: attributable managedFields.
                field_manager: Some("kaas-broker".to_owned()),
                ..Default::default()
            };
            match self.api().patch(username, &pp, &Patch::Merge(&patch)).await {
                Ok(_) => Ok(()),
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    Err(UserWriteError::NotFound(username.into()))
                }
                Err(e) => Err(map_kube_err(e)),
            }
        }
    }

    fn map_kube_err(e: kube::Error) -> UserWriteError {
        match &e {
            kube::Error::Api(api) if api.code == 403 => {
                UserWriteError::Forbidden(api.message.clone())
            }
            _ => UserWriteError::Other(e.to_string()),
        }
    }
}
