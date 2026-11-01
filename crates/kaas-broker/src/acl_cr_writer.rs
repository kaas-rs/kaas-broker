//! `AclCRWriter` — broker → `KafkaUser.spec.authorization.acls` path.
//!
//! gh #107 + gh #135: ACLs are authored inline on each principal's KafkaUser
//! CR; the wire-level CreateAcls / DeleteAcls / DescribeAcls handlers
//! translate the AdminClient int8-enum shape into this string shape
//! and delegate here. The operator's ACL reconcile rebuilds
//! `/data/__cluster/acls.json` from the merged set on the next
//! reconcile and every broker's ACL engine hot-reloads.
//!
//! Last-write-wins under concurrent edits: one shot `Update` with the
//! read resourceVersion — apiserver conflict → error → wire-level
//! UNKNOWN_SERVER_ERROR → AdminClient retry. Mutating a git-managed
//! KafkaUser will surface as ArgoCD drift until the next sync; that's
//! the intentional trade for letting the admin protocol reach the
//! canonical store.

use async_trait::async_trait;
use thiserror::Error;

/// Broker-side representation of one ACL row. Strings rather than
/// int8s so the writer patches directly into the KafkaUser ACL
/// fields without re-mapping.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclBinding {
    /// `"User:alice"`.
    pub principal: String,
    /// `topic` | `group` | `cluster` | `transactionalId`.
    pub resource_type: String,
    pub resource_name: String,
    /// `literal` | `prefix`.
    pub pattern_type: String,
    /// Capitalised: `Read`, `Write`, `All`, ...
    pub operation: String,
    /// `Allow` | `Deny`.
    pub permission: String,
    /// Round-tripped verbatim; the broker ignores it.
    pub host: String,
}

/// Filter shape used by Describe and Delete. Empty strings mean "any"
/// along that axis (wire-level ANY codes collapse to empty). For
/// `pattern_type`, "" matches every entry; "literal"/"prefix" match
/// exactly; "match" expands to literal+prefix (KIP-290).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AclFilter {
    pub principal: String,
    pub resource_type: String,
    pub resource_name: String,
    pub pattern_type: String,
    pub operation: String,
    pub permission: String,
    pub host: String,
}

/// Errors the writer can surface. Mapped to wire error codes at the
/// handler boundary.
#[derive(Debug, Error)]
pub enum AclWriteError {
    /// The binding's principal doesn't correspond to an existing
    /// KafkaUser CR — kaas does not auto-create CRs on a runtime
    /// ACL write. Wire: `INVALID_REQUEST` (42).
    #[error("no KafkaUser CR for principal {0}")]
    UnknownPrincipal(String),

    /// Principal isn't of the form `User:<name>`. Apache admits
    /// `Group:` and service-account principals too; kaas maps only
    /// to KafkaUser today. Wire: `INVALID_REQUEST` (42).
    #[error("principal must be of the form User:<name>, got {0:?}")]
    InvalidPrincipal(String),

    /// Anything else (apiserver error, conflict). Wire:
    /// `UNKNOWN_SERVER_ERROR` (-1).
    #[error("{0}")]
    Other(String),
}

/// Persistence interface for the three ACL admin APIs.
#[async_trait]
pub trait AclCRWriter: Send + Sync + 'static {
    /// Append one ACL row to the principal's KafkaUser CR.
    /// Idempotent: an existing entry with the same resource +
    /// pattern + permission + host folds the operation into its
    /// `operations` list (or no-ops when already present).
    async fn create_acl(&self, binding: AclBinding) -> Result<(), AclWriteError>;

    /// Remove entries (or single operations within an entry) matching
    /// the filter across every KafkaUser CR. Returns the flat list of
    /// removed bindings — one per (entry, operation) pair touched.
    async fn delete_acls(&self, filter: AclFilter) -> Result<Vec<AclBinding>, AclWriteError>;

    /// Expand every inline entry into one binding per operation and
    /// return those matching the filter. Read-only.
    async fn list_acls(&self, filter: AclFilter) -> Result<Vec<AclBinding>, AclWriteError>;
}

/// True when `filter` is empty (wildcard) or matches exactly.
fn match_string(filter: &str, value: &str) -> bool {
    filter.is_empty() || filter == value
}

/// Wire-level MATCH expands to literal+prefix per KIP-290; empty = ANY.
fn match_pattern(filter: &str, value: &str) -> bool {
    if filter.is_empty() || filter == "match" {
        value == "literal" || value == "prefix"
    } else {
        filter == value
    }
}

/// Filter test across every axis of a flat binding.
pub fn match_binding(f: &AclFilter, b: &AclBinding) -> bool {
    match_string(&f.principal, &b.principal)
        && match_string(&f.resource_type, &b.resource_type)
        && match_string(&f.resource_name, &b.resource_name)
        && match_pattern(&f.pattern_type, &b.pattern_type)
        && match_string(&f.operation, &b.operation)
        && match_string(&f.permission, &b.permission)
        && match_string(&f.host, &b.host)
}

/// `"User:alice"` → `"alice"`.
#[cfg(any(feature = "cr-writer", test))]
fn principal_to_user_name(principal: &str) -> Result<&str, AclWriteError> {
    principal
        .strip_prefix("User:")
        .filter(|rest| !rest.is_empty())
        .ok_or_else(|| AclWriteError::InvalidPrincipal(principal.to_string()))
}

/// CR's lowercase `allow`/`deny` → the on-disk `Allow`/`Deny` casing
/// the wire response and the broker's ACL engine both expect. Only the
/// kube writer calls it, so it carries that gate alone: with `test` added
/// it was dead code in a featureless `cargo test -p kaas-broker`, which
/// `-D warnings` turns into a build failure.
#[cfg(feature = "cr-writer")]
fn normalise_permission(t: &str) -> &'static str {
    if t.eq_ignore_ascii_case("deny") {
        "Deny"
    } else {
        "Allow"
    }
}

#[cfg(feature = "cr-writer")]
pub use kube_impl::KubeAclCRWriter;

#[cfg(feature = "cr-writer")]
mod kube_impl {
    use super::*;
    use kaas_operator_api::{
        KafkaUser, KafkaUserAcl, KafkaUserAclResource, KafkaUserAuthorization,
    };
    use kube::api::{ListParams, PostParams};
    use kube::Api;

    /// Real kube-backed writer over the KafkaUser CRs in one
    /// namespace (typically the broker's own).
    #[derive(Clone)]
    pub struct KubeAclCRWriter {
        client: kube::Client,
        namespace: String,
    }

    impl std::fmt::Debug for KubeAclCRWriter {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("KubeAclCRWriter")
                .field("namespace", &self.namespace)
                .finish_non_exhaustive()
        }
    }

    impl KubeAclCRWriter {
        pub fn new(client: kube::Client, namespace: impl Into<String>) -> Self {
            Self {
                client,
                namespace: namespace.into(),
            }
        }

        fn api(&self) -> Api<KafkaUser> {
            Api::namespaced(self.client.clone(), &self.namespace)
        }

        async fn update(&self, user: &KafkaUser) -> Result<(), AclWriteError> {
            let name = user.metadata.name.as_deref().unwrap_or_default();
            self.api()
                .replace(
                    name,
                    &PostParams {
                        // gh #245: attributable managedFields.
                        field_manager: Some("kaas-broker".to_owned()),
                        ..Default::default()
                    },
                    user,
                )
                .await
                .map(|_| ())
                .map_err(|e| AclWriteError::Other(format!("update KafkaUser {name}: {e}")))
        }
    }

    /// Entry matches binding on (resource.{type,name,patternType},
    /// permission, host) — operations intentionally NOT in the key so
    /// reads/writes coalesce onto one entry.
    fn same_acl_shape(e: &KafkaUserAcl, b: &AclBinding, pattern: &str, acl_type: &str) -> bool {
        if e.resource.kind != b.resource_type || e.resource.name != b.resource_name {
            return false;
        }
        let entry_pattern = if e.resource.pattern_type.is_empty() {
            "literal"
        } else {
            e.resource.pattern_type.as_str()
        };
        if entry_pattern != pattern {
            return false;
        }
        let entry_type = if e.kind.is_empty() {
            "allow".to_string()
        } else {
            e.kind.to_ascii_lowercase()
        };
        entry_type == acl_type && e.host == b.host
    }

    /// One (entry, operation) pair → flat binding, defaults applied.
    fn acl_entry_to_binding(principal: &str, e: &KafkaUserAcl, op: &str) -> AclBinding {
        let pattern = if e.resource.pattern_type.is_empty() {
            "literal"
        } else {
            e.resource.pattern_type.as_str()
        };
        AclBinding {
            principal: principal.to_string(),
            resource_type: e.resource.kind.clone(),
            resource_name: e.resource.name.clone(),
            pattern_type: pattern.to_string(),
            operation: op.to_string(),
            permission: normalise_permission(&e.kind).to_string(),
            host: e.host.clone(),
        }
    }

    /// Partition the entry's operations into (matched, kept) under the
    /// filter; a non-matching entry returns ([], all).
    fn split_ops_by_filter(
        e: &KafkaUserAcl,
        f: &AclFilter,
        principal: &str,
    ) -> (Vec<String>, Vec<String>) {
        let keep_all = || (Vec::new(), e.operations.clone());
        if !match_string(&f.principal, principal)
            || !match_string(&f.resource_type, &e.resource.kind)
            || !match_string(&f.resource_name, &e.resource.name)
        {
            return keep_all();
        }
        let entry_pattern = if e.resource.pattern_type.is_empty() {
            "literal"
        } else {
            e.resource.pattern_type.as_str()
        };
        if !match_pattern(&f.pattern_type, entry_pattern)
            || !match_string(&f.permission, normalise_permission(&e.kind))
            || !match_string(&f.host, &e.host)
        {
            return keep_all();
        }
        e.operations
            .iter()
            .cloned()
            .partition(|op| f.operation.is_empty() || *op == f.operation)
    }

    #[async_trait]
    impl AclCRWriter for KubeAclCRWriter {
        async fn create_acl(&self, b: AclBinding) -> Result<(), AclWriteError> {
            let username = principal_to_user_name(&b.principal)?.to_string();

            let mut user = match self.api().get(&username).await {
                Ok(u) => u,
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(AclWriteError::UnknownPrincipal(b.principal.clone()));
                }
                Err(e) => {
                    return Err(AclWriteError::Other(format!(
                        "get KafkaUser {username}: {e}"
                    )));
                }
            };

            let authz = user
                .spec
                .authorization
                .get_or_insert_with(KafkaUserAuthorization::default);
            if authz.kind.is_empty() {
                authz.kind = "simple".into();
            }

            let pattern = if b.pattern_type.is_empty() {
                "literal".to_string()
            } else {
                b.pattern_type.clone()
            };
            let acl_type = if b.permission.is_empty() {
                "allow".to_string()
            } else {
                b.permission.to_ascii_lowercase()
            };

            for e in &mut authz.acls {
                if !same_acl_shape(e, &b, &pattern, &acl_type) {
                    continue;
                }
                if e.operations.contains(&b.operation) {
                    return Ok(());
                }
                e.operations.push(b.operation.clone());
                return self.update(&user).await;
            }

            authz.acls.push(KafkaUserAcl {
                resource: KafkaUserAclResource {
                    kind: b.resource_type.clone(),
                    name: b.resource_name.clone(),
                    pattern_type: pattern,
                },
                operations: vec![b.operation.clone()],
                kind: acl_type,
                host: b.host.clone(),
            });
            self.update(&user).await
        }

        async fn delete_acls(&self, f: AclFilter) -> Result<Vec<AclBinding>, AclWriteError> {
            let users = self
                .api()
                .list(&ListParams::default())
                .await
                .map_err(|e| AclWriteError::Other(format!("list KafkaUser: {e}")))?;

            let mut removed = Vec::new();
            for mut user in users {
                if user.metadata.deletion_timestamp.is_some() {
                    continue;
                }
                let principal =
                    format!("User:{}", user.metadata.name.as_deref().unwrap_or_default());
                if !match_string(&f.principal, &principal) {
                    continue;
                }
                let Some(authz) = user.spec.authorization.as_mut() else {
                    continue;
                };
                if authz.acls.is_empty() {
                    continue;
                }

                let mut new_acls = Vec::with_capacity(authz.acls.len());
                let mut mutated = false;
                for e in &authz.acls {
                    let (matched, kept) = split_ops_by_filter(e, &f, &principal);
                    for op in &matched {
                        removed.push(acl_entry_to_binding(&principal, e, op));
                        mutated = true;
                    }
                    if matched.is_empty() {
                        new_acls.push(e.clone());
                        continue;
                    }
                    if kept.is_empty() {
                        continue;
                    }
                    let mut updated = e.clone();
                    updated.operations = kept;
                    new_acls.push(updated);
                }
                if !mutated {
                    continue;
                }
                authz.acls = new_acls;
                self.update(&user).await?;
            }
            Ok(removed)
        }

        async fn list_acls(&self, f: AclFilter) -> Result<Vec<AclBinding>, AclWriteError> {
            let users = self
                .api()
                .list(&ListParams::default())
                .await
                .map_err(|e| AclWriteError::Other(format!("list KafkaUser: {e}")))?;

            let mut out = Vec::new();
            for user in &users {
                if user.metadata.deletion_timestamp.is_some() {
                    continue;
                }
                let principal =
                    format!("User:{}", user.metadata.name.as_deref().unwrap_or_default());
                if !match_string(&f.principal, &principal) {
                    continue;
                }
                let Some(authz) = user.spec.authorization.as_ref() else {
                    continue;
                };
                for e in &authz.acls {
                    for op in &e.operations {
                        let b = acl_entry_to_binding(&principal, e, op);
                        if match_binding(&f, &b) {
                            out.push(b);
                        }
                    }
                }
            }
            Ok(out)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(op: &str) -> AclBinding {
        AclBinding {
            principal: "User:alice".into(),
            resource_type: "topic".into(),
            resource_name: "orders".into(),
            pattern_type: "literal".into(),
            operation: op.into(),
            permission: "Allow".into(),
            host: "*".into(),
        }
    }

    #[test]
    fn empty_filter_matches_everything() {
        assert!(match_binding(&AclFilter::default(), &binding("Read")));
    }

    #[test]
    fn filter_axes_match_exactly() {
        let f = AclFilter {
            principal: "User:alice".into(),
            resource_type: "topic".into(),
            operation: "Read".into(),
            ..AclFilter::default()
        };
        assert!(match_binding(&f, &binding("Read")));
        assert!(!match_binding(&f, &binding("Write")));
    }

    #[test]
    fn match_pattern_expands_to_literal_and_prefix() {
        assert!(match_pattern("match", "literal"));
        assert!(match_pattern("match", "prefix"));
        assert!(match_pattern("", "literal"));
        assert!(!match_pattern("prefix", "literal"));
    }

    #[test]
    fn principal_parse_rejects_non_user() {
        assert!(principal_to_user_name("User:alice").is_ok());
        assert!(principal_to_user_name("Group:dev").is_err());
        assert!(principal_to_user_name("User:").is_err());
    }
}
