//! Walks every `KafkaUser` CR in the operator's namespace, projects
//! their inline `spec.authorization.acls` into the on-disk
//! `acls.json` shape, and atomic-writes the file under
//! `/data/__cluster/`. Same JSON shape the broker's `AclEngine` (in
//! `kaas-auth`) already consumes — no broker-side code change.
//!
//! `acl_to_entry` is the per-rule translation step; it's `pub`
//! because workstream G's tests assert the defaulting behaviour
//! (gh #137: pattern_type=literal, type=allow when CR field empty).
//!
//! Note the historical capitalisation of `permission`: the on-disk
//! field is `Allow` / `Deny` (capital initial) while the CR field is
//! `allow` / `deny` (lowercase, Strimzi-faithful). We translate at
//! the boundary so the public surface stays Strimzi-shape while the
//! broker side keeps the existing case-sensitive matcher.

use std::path::Path;

use kaas_operator_api::{KafkaUser, KafkaUserAcl};
use kube::{api::ListParams, Api, Client};
use serde::{Deserialize, Serialize};

use crate::errors::ControllerError;

/// On-disk shape of `__cluster/acls.json`.
#[derive(Debug, Default, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AclFile {
    pub version: u32,
    #[serde(default)]
    pub acls: Vec<AclEntry>,
}

/// One ACL rule as stored on disk. Field shapes are pinned
/// byte-for-byte (the broker's existing `kaas-auth` loader
/// gates on these names).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AclEntry {
    /// `User:<name>` — note the literal `User:` prefix the broker
    /// matches against.
    pub principal: String,

    pub resource: AclResource,

    pub operations: Vec<String>,

    /// `Allow` or `Deny` (capitalised). Case-sensitive on the broker
    /// side — see `crates/kaas-auth/src/acls.rs`.
    pub permission: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct AclResource {
    #[serde(rename = "type")]
    pub kind: String,
    pub name: String,
    pub pattern_type: String,
}

/// List every `KafkaUser` CR in `namespace` and rebuild `acls.json`
/// from the union of their inline `spec.authorization.acls`.
///
/// Called from `KafkaUserReconciler` on every CR change so a single
/// user-level edit propagates within one reconcile cycle. Safe to
/// call from concurrent reconciles — the underlying `atomic_write`
/// guarantees the broker only ever sees a complete file.
///
/// Users with `metadata.deletionTimestamp` set are excluded from the
/// rebuild (their rules go away as soon as the delete is observed,
/// without waiting for the final `Deleted` event).
pub async fn reconcile_acls(
    client: &Client,
    namespace: &str,
    cluster_dir: &Path,
) -> Result<(), ControllerError> {
    let api: Api<KafkaUser> = Api::namespaced(client.clone(), namespace);
    let users = api.list(&ListParams::default()).await?;

    let mut entries: Vec<AclEntry> = Vec::new();
    for u in &users.items {
        if u.metadata.deletion_timestamp.is_some() {
            continue;
        }
        let Some(authz) = u.spec.authorization.as_ref() else {
            continue;
        };
        if authz.acls.is_empty() {
            continue;
        }
        let Some(name) = u.metadata.name.as_deref() else {
            continue;
        };
        let principal = format!("User:{name}");
        for acl in &authz.acls {
            entries.push(acl_to_entry(&principal, acl));
        }
    }

    write_acl_file(
        cluster_dir,
        &AclFile {
            version: 1,
            acls: entries,
        },
    )
}

/// Project one `KafkaUserACL` (Strimzi-style, lowercased
/// `type: allow|deny`) onto the on-disk shape (capitalised
/// `permission: Allow|Deny`). Defaults the pattern_type to `literal`
/// when empty and the type to `allow` when empty — both mirror
/// Strimzi defaults per gh #137 (no apiserver defaulting).
pub fn acl_to_entry(principal: &str, acl: &KafkaUserAcl) -> AclEntry {
    let pattern_type = if acl.resource.pattern_type.is_empty() {
        "literal"
    } else {
        acl.resource.pattern_type.as_str()
    };
    let permission = if acl.kind.eq_ignore_ascii_case("deny") {
        "Deny"
    } else {
        "Allow"
    };
    AclEntry {
        principal: principal.to_string(),
        resource: AclResource {
            kind: acl.resource.kind.clone(),
            name: acl.resource.name.clone(),
            pattern_type: pattern_type.to_string(),
        },
        operations: acl.operations.clone(),
        permission: permission.to_string(),
    }
}

/// Atomic write of `acls.json`. Stamps `version = 1`.
pub fn write_acl_file(cluster_dir: &Path, file: &AclFile) -> Result<(), ControllerError> {
    let cluster_dir = cluster_dir.to_path_buf();
    std::fs::create_dir_all(&cluster_dir)?;
    let mut stamped = file.clone();
    stamped.version = 1;
    let fs = kaas_storage::fs::RealFs::new();
    kaas_storage::atomic_write::atomic_write_json_pretty(&fs, &cluster_dir, "acls.json", &stamped)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_operator_api::KafkaUserAclResource;

    fn rule(kind: &str, res_kind: &str, name: &str, pattern: &str, ops: &[&str]) -> KafkaUserAcl {
        KafkaUserAcl {
            resource: KafkaUserAclResource {
                kind: res_kind.into(),
                name: name.into(),
                pattern_type: pattern.into(),
            },
            operations: ops.iter().map(|s| (*s).to_string()).collect(),
            kind: kind.into(),
            host: String::new(),
        }
    }

    #[test]
    fn acl_to_entry_defaults_pattern_to_literal_when_empty() {
        let e = acl_to_entry(
            "User:alice",
            &rule("allow", "topic", "events", "", &["Read"]),
        );
        assert_eq!(e.resource.pattern_type, "literal");
    }

    #[test]
    fn acl_to_entry_capitalises_permission() {
        let allow = acl_to_entry("User:a", &rule("allow", "topic", "x", "literal", &["Read"]));
        let deny = acl_to_entry("User:a", &rule("deny", "topic", "x", "literal", &["Read"]));
        let empty = acl_to_entry("User:a", &rule("", "topic", "x", "literal", &["Read"]));
        assert_eq!(allow.permission, "Allow");
        assert_eq!(deny.permission, "Deny");
        // Empty kind defaults to Allow (gh #137 — operator-side default).
        assert_eq!(empty.permission, "Allow");
    }

    #[test]
    fn acl_to_entry_passes_through_pattern_when_set() {
        let e = acl_to_entry("User:a", &rule("allow", "topic", "x", "prefix", &["Read"]));
        assert_eq!(e.resource.pattern_type, "prefix");
    }

    #[test]
    fn acl_file_roundtrip_via_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let f = AclFile {
            version: 1,
            acls: vec![AclEntry {
                principal: "User:alice".into(),
                resource: AclResource {
                    kind: "topic".into(),
                    name: "events".into(),
                    pattern_type: "literal".into(),
                },
                operations: vec!["Read".into(), "Describe".into()],
                permission: "Allow".into(),
            }],
        };
        write_acl_file(tmp.path(), &f).unwrap();
        let bytes = std::fs::read(tmp.path().join("acls.json")).unwrap();
        let back: AclFile = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(back, f);
    }
}
