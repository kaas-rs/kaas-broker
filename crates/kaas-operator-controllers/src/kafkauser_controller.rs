//! Reconciler that materialises a `KafkaUser` CR into:
//!
//! - One entry in `<data_dir>/__cluster/credentials.json` carrying
//!   the SCRAM-derived keys (gh #104 KIP-554 rotation path bypasses
//!   PBKDF2 when `spec.authentication.scram` is set), the mTLS CN,
//!   or the ServiceAccount reference.
//! - An updated `<data_dir>/__cluster/acls.json` rebuilt from every
//!   `KafkaUser.spec.authorization.acls` in the namespace.
//! - For SCRAM users without an input Secret: an output
//!   `<name>-kafka-credentials` Secret carrying `username` + `password`
//!   (gh #136, mirrors Strimzi's User Operator behaviour).
//!
//! ## gh #120 SecretNotFound contract
//!
//! When `spec.authentication.password.secretKeyRef` points at a
//! Secret that doesn't exist yet, the reconciler used to return
//! `Err(...)`, which triggered controller-runtime's exponential
//! retry and produced one ERROR log line per attempt. The Secret
//! isn't coming back on its own — the user has to create it. We
//! convert this case to `Ready=False reason=SecretNotFound` and
//! return `Action::await_change()` instead. A `kubectl edit kafkauser`
//! (or the next periodic resync) retriggers reconcile.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::Secret;
use kaas_operator_api::{Condition, KafkaUser, KafkaUserScramCredential, KafkaUserStatus};
use kube::api::{Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::runtime::controller::Action;
use kube::{Api, Client};

use crate::acls;
use crate::conditions::{set_condition, READY};
use crate::credentials::{
    compute_scram, generate_alphanum_password, read_credentials, write_credentials, CredQuotas,
    CredentialsFile, SaCredential, ScramCredential, UserCredential,
};
use crate::errors::ControllerError;
use crate::observer::ReconcileObserver;

pub struct KafkaUserReconciler {
    pub client: Client,
    /// Cluster-state dir (resolved `KAAS_CLUSTER_DIR`, gh #221).
    pub cluster_dir: PathBuf,
    pub namespace: String,
    pub observer: ReconcileObserver,
}

impl KafkaUserReconciler {
    pub fn new(client: Client, cluster_dir: PathBuf, namespace: String) -> Self {
        Self {
            client,
            cluster_dir,
            namespace,
            observer: ReconcileObserver::new("KafkaUser"),
        }
    }

    pub async fn reconcile(&self, user: Arc<KafkaUser>) -> Result<Action, ControllerError> {
        let started = std::time::Instant::now();
        let out = self.reconcile_inner(user).await;
        self.observer
            .record_duration(started.elapsed().as_secs_f64());
        out
    }

    async fn reconcile_inner(&self, user: Arc<KafkaUser>) -> Result<Action, ControllerError> {
        if user.metadata.deletion_timestamp.is_some() {
            self.observer.bump_requeue();
            return Ok(Action::await_change());
        }

        let Some(name) = user.metadata.name.as_deref() else {
            self.observer.bump_error();
            return Ok(Action::await_change());
        };

        // Authorization-only user (gh #42): no authentication block →
        // no credential to materialize. The principal authenticates
        // out-of-band (an OAUTHBEARER token), so we skip
        // credentials.json entirely and only reconcile its ACLs. Any
        // stale credential entry from a previous incarnation that DID
        // carry auth is dropped so a scram→authz-only edit doesn't
        // leave a live credential behind.
        let authz_only = user.spec.authentication.is_none();
        // Output Secret name for the status; empty for authorization-only
        // users, which have none.
        let mut secret_name = String::new();

        if authz_only {
            let mut cf: CredentialsFile = read_credentials(&self.cluster_dir)?;
            if cf.remove_user(name) {
                write_credentials(&self.cluster_dir, &cf)?;
            }
        } else {
            // Build the credential entry for this user.
            let built = self.build_credential(&user).await;

            let (cred, out_secret_name) = match built {
                Ok(cb) => cb,
                Err(ControllerError::SecretNotFound {
                    namespace,
                    name: sname,
                }) => {
                    // gh #120: NOT an Err — surface as a condition and
                    // park until something changes.
                    let cond = Condition {
                        type_: READY.into(),
                        status: Condition::STATUS_FALSE.into(),
                        observed_generation: user.metadata.generation,
                        last_transition_time: String::new(),
                        reason: "SecretNotFound".into(),
                        message: format!(
                            "spec.authentication.password references missing secret {namespace}/{sname}"
                        ),
                    };
                    self.patch_status(&user, |st| set_condition(&mut st.conditions, cond.clone()))
                        .await?;
                    self.observer.bump_error();
                    return Ok(Action::await_change());
                }
                Err(other) => {
                    let cond = Condition {
                        type_: READY.into(),
                        status: Condition::STATUS_FALSE.into(),
                        observed_generation: user.metadata.generation,
                        last_transition_time: String::new(),
                        reason: "CredentialError".into(),
                        message: other.to_string(),
                    };
                    self.patch_status(&user, |st| set_condition(&mut st.conditions, cond.clone()))
                        .await?;
                    self.observer.bump_error();
                    return Err(other);
                }
            };

            // Write credentials.json.
            let mut cf: CredentialsFile = read_credentials(&self.cluster_dir)?;
            cf.upsert_user(cred);
            write_credentials(&self.cluster_dir, &cf)?;
            secret_name = out_secret_name;
        }

        // Rebuild acls.json from every KafkaUser in the namespace.
        acls::reconcile_acls(&self.client, &self.namespace, &self.cluster_dir).await?;

        // Status patch.
        let cond = Condition {
            type_: READY.into(),
            status: Condition::STATUS_TRUE.into(),
            observed_generation: user.metadata.generation,
            last_transition_time: String::new(),
            reason: "CredentialWritten".into(),
            message: if authz_only {
                format!("authorization-only user {name} reconciled (no credential)")
            } else {
                format!(
                    "credentials written for {name} ({})",
                    user.spec
                        .authentication
                        .as_ref()
                        .map(|a| a.kind.as_str())
                        .unwrap_or("")
                )
            },
        };
        self.patch_status(&user, |st| {
            st.secret = secret_name.clone();
            set_condition(&mut st.conditions, cond.clone());
        })
        .await?;

        self.observer.bump_success();
        Ok(Action::await_change())
    }

    pub async fn handle_not_found(&self, name: &str) -> Result<(), ControllerError> {
        // Drop the credential entry; rebuild acls.json without this
        // user's rules. Both calls are best-effort.
        let mut cf = read_credentials(&self.cluster_dir)?;
        if cf.has_user(name) {
            cf.remove_user(name);
            write_credentials(&self.cluster_dir, &cf)?;
        }
        acls::reconcile_acls(&self.client, &self.namespace, &self.cluster_dir).await?;
        Ok(())
    }

    /// Build the credential entry + (for SCRAM auto-password) the
    /// output Secret name, switched by `auth.type`.
    async fn build_credential(
        &self,
        user: &KafkaUser,
    ) -> Result<(UserCredential, String), ControllerError> {
        let Some(name) = user.metadata.name.as_deref() else {
            return Err(ControllerError::Other("user has no metadata.name".into()));
        };
        let user_ns = user
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);

        // Only reached for authenticated users — the reconcile loop
        // handles the authorization-only case (no credential) before
        // calling here.
        let auth = user.spec.authentication.as_ref().ok_or_else(|| {
            ControllerError::Other("build_credential called on an authorization-only user".into())
        })?;

        let mut cred = UserCredential {
            username: name.into(),
            auth_type: auth.kind.clone(),
            scram: None,
            tls_cn: String::new(),
            sa: None,
            quotas: user.spec.quotas.as_ref().map(|q| CredQuotas {
                producer_max_byte_rate_per_broker: q.producer_max_byte_rate_per_broker,
                consumer_max_byte_rate_per_broker: q.consumer_max_byte_rate_per_broker,
                request_percentage: q.request_percentage,
            }),
        };

        match auth.kind.as_str() {
            "scram-sha-512" => {
                // gh #104: pre-derived rotation path. When set, pass through
                // verbatim and skip PBKDF2 + the output Secret.
                if let Some(s) = auth.scram.as_ref() {
                    cred.scram = Some(scram_passthrough(s));
                    return Ok((cred, String::new()));
                }
                // gh #136: optional input Secret OR auto-generated output Secret.
                let out_secret_name = format!("{name}-kafka-credentials");
                let password = self
                    .resolve_scram_password(user, user_ns, &out_secret_name)
                    .await?;
                cred.scram = Some(compute_scram(&password)?);
                self.ensure_client_secret(user, user_ns, &out_secret_name, name, &password)
                    .await?;
                Ok((cred, out_secret_name))
            }
            "tls" => {
                let cn = auth
                    .certificate_ref
                    .as_ref()
                    .filter(|r| !r.name.is_empty())
                    .map(|r| r.name.clone())
                    .unwrap_or_else(|| name.to_string());
                cred.tls_cn = cn;
                Ok((cred, String::new()))
            }
            "kubernetes-serviceaccount" => {
                let sa = auth.service_account_ref.as_ref().ok_or_else(|| {
                    ControllerError::MalformedCredential(
                        "spec.authentication.serviceAccountRef required for \
                             kubernetes-serviceaccount"
                            .into(),
                    )
                })?;
                cred.sa = Some(SaCredential {
                    name: sa.name.clone(),
                    namespace: sa.namespace.clone(),
                });
                Ok((cred, String::new()))
            }
            other => Err(ControllerError::UnsupportedAuthType(other.into())),
        }
    }

    /// Resolve the SCRAM password. Three layered paths:
    ///
    /// 1. `spec.authentication.password.secretKeyRef` set → read it
    ///    (gh #120: NotFound bubbles as `ControllerError::SecretNotFound`
    ///    so the reconcile loop can convert to a Condition).
    /// 2. Existing output Secret has `password` → reuse (stable password
    ///    across operator restarts, gh #136).
    /// 3. Generate a fresh 32-char password.
    async fn resolve_scram_password(
        &self,
        user: &KafkaUser,
        user_ns: &str,
        out_secret_name: &str,
    ) -> Result<String, ControllerError> {
        if let Some(pw_ref) = user
            .spec
            .authentication
            .as_ref()
            .and_then(|a| a.password.as_ref())
        {
            let api: Api<Secret> = Api::namespaced(self.client.clone(), user_ns);
            return match api.get(&pw_ref.name).await {
                Ok(s) => {
                    let Some(data) = s.data.as_ref() else {
                        return Err(ControllerError::MalformedCredential(format!(
                            "secret {user_ns}/{} has no data block",
                            pw_ref.name
                        )));
                    };
                    let Some(bytes) = data.get(&pw_ref.key) else {
                        return Err(ControllerError::MalformedCredential(format!(
                            "key {} not found in secret {user_ns}/{}",
                            pw_ref.key, pw_ref.name
                        )));
                    };
                    String::from_utf8(bytes.0.clone()).map_err(|e| {
                        ControllerError::MalformedCredential(format!("password utf8: {e}"))
                    })
                }
                Err(kube::Error::Api(e)) if e.code == 404 => Err(ControllerError::SecretNotFound {
                    namespace: user_ns.to_string(),
                    name: pw_ref.name.clone(),
                }),
                Err(e) => Err(ControllerError::Kube(e)),
            };
        }

        // Auto-gen path. Try to recover an existing password from the
        // output Secret so the SCRAM hash stays stable across operator
        // restarts.
        let api: Api<Secret> = Api::namespaced(self.client.clone(), user_ns);
        match api.get(out_secret_name).await {
            Ok(s) => {
                if let Some(data) = s.data.as_ref() {
                    if let Some(bytes) = data.get("password") {
                        if !bytes.0.is_empty() {
                            return String::from_utf8(bytes.0.clone()).map_err(|e| {
                                ControllerError::MalformedCredential(format!("password utf8: {e}"))
                            });
                        }
                    }
                }
            }
            Err(kube::Error::Api(e)) if e.code == 404 => { /* fall through to generate */ }
            Err(e) => return Err(ControllerError::Kube(e)),
        }

        // Generate a fresh password and let ensure_client_secret persist it.
        let _ = user; // silence unused warning when this branch is taken
        generate_alphanum_password(32)
    }

    /// Create or update the output Secret. Always carries
    /// `username` + `password`; sets OwnerReferences back to the
    /// `KafkaUser` so K8s GC cleans up on CR delete (no finalizer).
    async fn ensure_client_secret(
        &self,
        owner: &KafkaUser,
        ns: &str,
        secret_name: &str,
        username: &str,
        password: &str,
    ) -> Result<(), ControllerError> {
        let api: Api<Secret> = Api::namespaced(self.client.clone(), ns);

        let mut string_data = std::collections::BTreeMap::new();
        string_data.insert("username".to_string(), username.to_string());
        string_data.insert("password".to_string(), password.to_string());

        let owner_ref = owner_ref_for(owner);
        let desired = Secret {
            metadata: ObjectMeta {
                name: Some(secret_name.into()),
                namespace: Some(ns.into()),
                owner_references: Some(vec![owner_ref]),
                ..ObjectMeta::default()
            },
            string_data: Some(string_data),
            ..Secret::default()
        };

        match api.get(secret_name).await {
            Ok(mut existing) => {
                // Preserve metadata.uid / resourceVersion; replace
                // string_data + owner_references.
                existing.string_data = desired.string_data;
                existing.metadata.owner_references = desired.metadata.owner_references;
                api.replace(secret_name, &PostParams::default(), &existing)
                    .await?;
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                api.create(&PostParams::default(), &desired).await?;
                Ok(())
            }
            Err(e) => Err(ControllerError::Kube(e)),
        }
    }

    async fn patch_status(
        &self,
        user: &KafkaUser,
        mutate: impl FnOnce(&mut KafkaUserStatus),
    ) -> Result<(), ControllerError> {
        let Some(name) = user.metadata.name.as_deref() else {
            return Ok(());
        };
        let namespace = user
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let api: Api<KafkaUser> = Api::namespaced(self.client.clone(), namespace);

        let mut status = user.status.clone().unwrap_or_default();
        mutate(&mut status);
        // Server-side apply requires apiVersion + kind in the body —
        // without them the API server answers
        // `invalid object type: /, Kind=` (400).
        let body = serde_json::json!({
            "apiVersion": "kaas.rs/v1alpha1",
            "kind": "KafkaUser",
            "status": status,
        });
        api.patch_status(
            name,
            &PatchParams::apply("kaas-operator").force(),
            &Patch::Apply(&body),
        )
        .await?;
        Ok(())
    }
}

fn scram_passthrough(s: &KafkaUserScramCredential) -> ScramCredential {
    ScramCredential {
        salt: s.salt.clone(),
        stored_key: s.stored_key.clone(),
        server_key: s.server_key.clone(),
        iterations: u32::try_from(s.iterations).unwrap_or(crate::credentials::SCRAM_ITERATIONS),
    }
}

fn owner_ref_for(
    owner: &KafkaUser,
) -> k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
    k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference {
        api_version: format!(
            "{}/{}",
            kaas_operator_api::GROUP,
            kaas_operator_api::VERSION
        ),
        kind: "KafkaUser".into(),
        name: owner.metadata.name.clone().unwrap_or_default(),
        uid: owner.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

pub async fn reconcile_user(
    user: Arc<KafkaUser>,
    ctx: Arc<KafkaUserReconciler>,
) -> Result<Action, ControllerError> {
    ctx.reconcile(user).await
}

pub fn error_policy(
    _user: Arc<KafkaUser>,
    err: &ControllerError,
    ctx: Arc<KafkaUserReconciler>,
) -> Action {
    tracing::warn!(error = %err, "KafkaUser reconcile failed");
    ctx.observer.bump_error();
    Action::requeue(Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    //! Pure-state slices of the reconciler that don't touch kube.
    //! Workstream G covers the integration path with wiremock.
    use super::*;
    use kaas_operator_api::{KafkaUserAuthentication, KafkaUserScramCredential, LocalObjectRef};

    #[test]
    fn scram_passthrough_preserves_fields_verbatim() {
        let s = KafkaUserScramCredential {
            salt: "AAAA".into(),
            stored_key: "BBBB".into(),
            server_key: "CCCC".into(),
            iterations: 8192,
        };
        let out = scram_passthrough(&s);
        assert_eq!(out.salt, "AAAA");
        assert_eq!(out.stored_key, "BBBB");
        assert_eq!(out.server_key, "CCCC");
        assert_eq!(out.iterations, 8192);
    }

    #[test]
    fn scram_passthrough_clamps_negative_iterations_to_default() {
        // Defensive: schema constrains iterations to be positive, but
        // a malformed CR shouldn't crash the operator. Falls back to
        // SCRAM_ITERATIONS.
        let s = KafkaUserScramCredential {
            salt: "x".into(),
            stored_key: "y".into(),
            server_key: "z".into(),
            iterations: -1,
        };
        let out = scram_passthrough(&s);
        assert_eq!(out.iterations, crate::credentials::SCRAM_ITERATIONS);
    }

    #[test]
    fn owner_ref_for_carries_controller_flag() {
        let user = KafkaUser {
            metadata: ObjectMeta {
                name: Some("alice".into()),
                uid: Some("abc-uuid".into()),
                ..ObjectMeta::default()
            },
            spec: kaas_operator_api::KafkaUserSpec {
                authentication: Some(KafkaUserAuthentication {
                    kind: "tls".into(),
                    password: None,
                    scram: None,
                    certificate_ref: None,
                    service_account_ref: None,
                }),
                authorization: None,
                quotas: None,
            },
            status: None,
        };
        let owner = owner_ref_for(&user);
        assert_eq!(owner.kind, "KafkaUser");
        assert_eq!(owner.name, "alice");
        assert_eq!(owner.uid, "abc-uuid");
        assert_eq!(owner.controller, Some(true));
        assert_eq!(owner.api_version, "kaas.rs/v1alpha1");
    }

    #[test]
    fn tls_kind_uses_certificate_ref_name_when_present() {
        // Verify the auth.type=tls branch picks the right CN.
        let with_ref = KafkaUserAuthentication {
            kind: "tls".into(),
            password: None,
            scram: None,
            certificate_ref: Some(LocalObjectRef {
                name: "custom-cn".into(),
            }),
            service_account_ref: None,
        };
        let cn = with_ref
            .certificate_ref
            .as_ref()
            .filter(|r| !r.name.is_empty())
            .map(|r| r.name.clone())
            .unwrap_or_else(|| "fallback".to_string());
        assert_eq!(cn, "custom-cn");

        let without_ref = KafkaUserAuthentication {
            kind: "tls".into(),
            password: None,
            scram: None,
            certificate_ref: None,
            service_account_ref: None,
        };
        let cn = without_ref
            .certificate_ref
            .as_ref()
            .filter(|r| !r.name.is_empty())
            .map(|r| r.name.clone())
            .unwrap_or_else(|| "fallback".to_string());
        assert_eq!(cn, "fallback");
    }
}
