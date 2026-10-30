//! `acls.json` loader + ACL evaluation.
//!
//! Deny takes precedence over
//! Allow; default is deny. An Allow rule also grants the operations
//! Apache implies from it (see [`implies`]); a Deny rule matches only
//! its own operation. Decisions are cached for 5 s keyed on
//! `(principal_name, resource_type, resource_name, op)` to keep the
//! Produce/Fetch hot path off the rule walk.
//!
//! Rule list sits behind `arc_swap::ArcSwap<Vec<AclRule>>` so
//! `reload()` swaps atomically; the cache is wiped on every reload
//! so a freshly-loaded deny rule takes effect on the next request.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use dashmap::DashMap;
use serde::Deserialize;

use crate::authorizer::Authorizer;
use crate::errors::AuthError;
use crate::types::{Operation, Principal, Resource};

const CACHE_TTL: Duration = Duration::from_secs(5);

#[derive(Debug, Deserialize)]
struct AclFile {
    #[serde(default)]
    version: u32,
    #[serde(default)]
    acls: Vec<AclEntry>,
}

#[derive(Debug, Deserialize)]
struct AclEntry {
    principal: String,
    resource: AclResource,
    #[serde(default)]
    operations: Vec<String>,
    permission: String,
}

#[derive(Debug, Deserialize)]
struct AclResource {
    #[serde(rename = "type")]
    res_type: String,
    name: String,
    #[serde(rename = "patternType", default)]
    pattern_type: String,
}

#[derive(Debug, Clone)]
struct AclRule {
    principal: String, // "User:alice" / "User:*" / "*"
    res_type: String,
    res_name: String,
    pattern_type: String, // "literal" | "prefix" | "any" | "match"
    operations: HashSet<Operation>,
    operation_all: bool,
    deny: bool,
}

#[derive(Debug, Eq, PartialEq, Hash)]
struct CacheKey {
    principal: String,
    res_type: String,
    res_name: String,
    op: Operation,
}

#[derive(Debug)]
struct CachedDecision {
    allowed: bool,
    expires_at: Instant,
}

#[derive(Debug)]
pub struct AclEngine {
    path: PathBuf,
    rules: ArcSwap<Vec<AclRule>>,
    cache: DashMap<CacheKey, CachedDecision>,
}

impl AclEngine {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            rules: ArcSwap::from_pointee(Vec::new()),
            cache: DashMap::new(),
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read `acls.json` and replace the rule set atomically. Missing
    /// file is `Ok(())`.
    pub fn reload(&self) -> Result<(), AuthError> {
        let data = match std::fs::read(&self.path) {
            Ok(d) => d,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                self.rules.store(Arc::new(Vec::new()));
                self.cache.clear();
                return Ok(());
            }
            Err(err) => return Err(AuthError::Io(err)),
        };
        let parsed: AclFile = serde_json::from_slice(&data)?;
        let _ = parsed.version;
        let mut rules = Vec::with_capacity(parsed.acls.len());
        for a in parsed.acls {
            let mut ops = HashSet::new();
            let mut operation_all = false;
            for op_str in a.operations {
                if op_str == "All" {
                    operation_all = true;
                    continue;
                }
                match Operation::parse(&op_str) {
                    Some(op) => {
                        ops.insert(op);
                    }
                    None => {
                        tracing::warn!(
                            op = op_str.as_str(),
                            principal = a.principal.as_str(),
                            "acls: unknown Operation — entry dropped",
                        );
                    }
                }
            }
            rules.push(AclRule {
                principal: a.principal,
                res_type: a.resource.res_type,
                res_name: a.resource.name,
                pattern_type: if a.resource.pattern_type.is_empty() {
                    "literal".to_owned()
                } else {
                    a.resource.pattern_type
                },
                operations: ops,
                operation_all,
                deny: a.permission == "Deny",
            });
        }
        self.rules.store(Arc::new(rules));
        self.cache.clear();
        Ok(())
    }

    /// Install rules directly (test helper, no disk).
    #[cfg(any(test, feature = "test-helpers"))]
    #[allow(clippy::expect_used)]
    pub fn install_for_test(&self, json: &str) {
        let parsed: AclFile = serde_json::from_str(json).expect("test acl json");
        let mut rules = Vec::new();
        for a in parsed.acls {
            let mut ops = HashSet::new();
            let mut operation_all = false;
            for op_str in a.operations {
                if op_str == "All" {
                    operation_all = true;
                    continue;
                }
                if let Some(op) = Operation::parse(&op_str) {
                    ops.insert(op);
                }
            }
            rules.push(AclRule {
                principal: a.principal,
                res_type: a.resource.res_type,
                res_name: a.resource.name,
                pattern_type: if a.resource.pattern_type.is_empty() {
                    "literal".to_owned()
                } else {
                    a.resource.pattern_type
                },
                operations: ops,
                operation_all,
                deny: a.permission == "Deny",
            });
        }
        self.rules.store(Arc::new(rules));
        self.cache.clear();
    }

    fn evaluate(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool {
        let rules = self.rules.load();
        let principal_str = principal.as_principal_string();
        let mut allow = false;
        for r in rules.iter() {
            if !matches_principal(&r.principal, &principal_str) {
                continue;
            }
            if !matches_resource(r, resource) {
                continue;
            }
            if r.deny {
                // Deny short-circuits immediately.
                if r.operation_all || r.operations.contains(&op) {
                    return false;
                }
                continue;
            }
            if r.operation_all || r.operations.iter().any(|held| implies(*held, op)) {
                allow = true;
            }
        }
        allow
    }
}

/// Whether an Allow rule for `held` grants `requested`. Apache's ACL
/// inheritance (`AclOperation`, `AclAuthorizer.allowAclExists`):
/// `Read`, `Write`, `Delete` and `Alter` each imply `Describe`, and
/// `AlterConfigs` implies `DescribeConfigs`. Only Allow rules widen
/// this way; a Deny rule matches its own operation.
///
/// gh #268: the `Describe` gates on Metadata, FindCoordinator and
/// OffsetFetch lean on this. A Strimzi consumer holding only `Read`
/// on its group is the common shape, and without the implication it
/// could not find its coordinator.
fn implies(held: Operation, requested: Operation) -> bool {
    held == requested
        || match requested {
            Operation::Describe => matches!(
                held,
                Operation::Read | Operation::Write | Operation::Delete | Operation::Alter
            ),
            Operation::DescribeConfigs => held == Operation::AlterConfigs,
            _ => false,
        }
}

impl Authorizer for AclEngine {
    fn authorize(&self, principal: &Principal, resource: &Resource, op: Operation) -> bool {
        let key = CacheKey {
            principal: principal.as_principal_string(),
            res_type: resource.kind.as_str().to_owned(),
            res_name: resource.name.clone(),
            op,
        };
        if let Some(entry) = self.cache.get(&key) {
            if entry.expires_at > Instant::now() {
                return entry.allowed;
            }
        }
        let allowed = self.evaluate(principal, resource, op);
        if !allowed {
            tracing::warn!(
                principal = %principal,
                resource = %format!("{}/{}", resource.kind.as_str(), resource.name),
                operation = op.as_str(),
                "acl: denied",
            );
            kaas_observability::metrics::global().acl_deny.add(
                1,
                &[
                    kaas_observability::KeyValue::new(
                        "resource_type",
                        resource.kind.as_str().to_owned(),
                    ),
                    kaas_observability::KeyValue::new("operation", op.as_str().to_owned()),
                ],
            );
        }
        self.cache.insert(
            key,
            CachedDecision {
                allowed,
                expires_at: Instant::now() + CACHE_TTL,
            },
        );
        allowed
    }
}

fn matches_principal(rule_principal: &str, req_principal: &str) -> bool {
    rule_principal == "User:*" || rule_principal == "*" || rule_principal == req_principal
}

fn matches_resource(rule: &AclRule, res: &Resource) -> bool {
    if rule.res_type != "*" && rule.res_type != res.kind.as_str() {
        return false;
    }
    match rule.pattern_type.as_str() {
        "literal" => rule.res_name == res.name || rule.res_name == "*",
        "prefix" => res.name.starts_with(&rule.res_name),
        "any" => true,
        "match" => res.name.starts_with(&rule.res_name),
        _ => rule.res_name == res.name,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::PrincipalKind;

    fn alice() -> Principal {
        Principal {
            name: "alice".to_owned(),
            kind: PrincipalKind::User,
        }
    }

    fn bob() -> Principal {
        Principal {
            name: "bob".to_owned(),
            kind: PrincipalKind::User,
        }
    }

    fn engine_with(json: &str) -> AclEngine {
        let e = AclEngine::new("/tmp/kaas-auth-acl-test");
        e.install_for_test(json);
        e
    }

    #[test]
    fn default_deny() {
        let e = engine_with(r#"{"acls":[]}"#);
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));
    }

    #[test]
    fn allow_specific_user_topic_op() {
        let e = engine_with(
            r#"{"acls":[{"principal":"User:alice",
                "resource":{"type":"topic","name":"foo","patternType":"literal"},
                "operations":["Write"],"permission":"Allow"}]}"#,
        );
        assert!(e.authorize(&alice(), &Resource::topic("foo"), Operation::Write));
        assert!(!e.authorize(&alice(), &Resource::topic("bar"), Operation::Write));
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));
        assert!(!e.authorize(&bob(), &Resource::topic("foo"), Operation::Write));
    }

    #[test]
    fn deny_overrides_allow() {
        let e = engine_with(
            r#"{"acls":[
                {"principal":"User:*","resource":{"type":"topic","name":"*","patternType":"literal"},
                 "operations":["Write"],"permission":"Allow"},
                {"principal":"User:alice","resource":{"type":"topic","name":"locked","patternType":"literal"},
                 "operations":["Write"],"permission":"Deny"}
            ]}"#,
        );
        assert!(e.authorize(&alice(), &Resource::topic("other"), Operation::Write));
        assert!(!e.authorize(&alice(), &Resource::topic("locked"), Operation::Write));
        assert!(e.authorize(&bob(), &Resource::topic("locked"), Operation::Write));
    }

    #[test]
    fn prefix_pattern() {
        let e = engine_with(
            r#"{"acls":[{"principal":"User:alice",
                "resource":{"type":"topic","name":"app-","patternType":"prefix"},
                "operations":["Read"],"permission":"Allow"}]}"#,
        );
        assert!(e.authorize(&alice(), &Resource::topic("app-events"), Operation::Read));
        assert!(!e.authorize(&alice(), &Resource::topic("other"), Operation::Read));
    }

    #[test]
    fn operation_all_grants_every_op() {
        let e = engine_with(
            r#"{"acls":[{"principal":"User:alice",
                "resource":{"type":"topic","name":"foo","patternType":"literal"},
                "operations":["All"],"permission":"Allow"}]}"#,
        );
        for op in [
            Operation::Read,
            Operation::Write,
            Operation::Create,
            Operation::Describe,
        ] {
            assert!(e.authorize(&alice(), &Resource::topic("foo"), op), "{op:?}");
        }
    }

    #[test]
    fn allow_implies_describe_like_apache() {
        // gh #268: Apache's ACL inheritance. A consumer group granted
        // only `Read` must still pass FindCoordinator's `Describe`.
        for held in ["Read", "Write", "Delete", "Alter"] {
            let e = engine_with(&format!(
                r#"{{"acls":[{{"principal":"User:alice",
                    "resource":{{"type":"group","name":"g","patternType":"literal"}},
                    "operations":["{held}"],"permission":"Allow"}}]}}"#
            ));
            assert!(
                e.authorize(&alice(), &Resource::group("g"), Operation::Describe),
                "{held} must imply Describe"
            );
        }
        let e = engine_with(
            r#"{"acls":[{"principal":"User:alice",
                "resource":{"type":"topic","name":"foo","patternType":"literal"},
                "operations":["AlterConfigs","Create"],"permission":"Allow"}]}"#,
        );
        assert!(e.authorize(
            &alice(),
            &Resource::topic("foo"),
            Operation::DescribeConfigs
        ));
        // Neither of these is in Apache's implied set.
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Describe));
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));
    }

    #[test]
    fn deny_is_not_widened_by_implication() {
        // Apache matches a Deny rule on its exact operation: the
        // Describe deny wins over the Read allow's implied Describe,
        // and leaves Read itself alone.
        let e = engine_with(
            r#"{"acls":[
                {"principal":"User:alice","resource":{"type":"topic","name":"foo","patternType":"literal"},
                 "operations":["Read"],"permission":"Allow"},
                {"principal":"User:alice","resource":{"type":"topic","name":"foo","patternType":"literal"},
                 "operations":["Describe"],"permission":"Deny"}
            ]}"#,
        );
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Describe));
        assert!(e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));
    }

    #[test]
    fn reload_swaps_atomically() {
        let path = tempfile::NamedTempFile::new().unwrap().into_temp_path();
        std::fs::write(
            &path,
            r#"{"acls":[{"principal":"User:alice",
                "resource":{"type":"topic","name":"foo","patternType":"literal"},
                "operations":["Read"],"permission":"Allow"}]}"#,
        )
        .unwrap();
        let e = AclEngine::new(&path);
        e.reload().unwrap();
        assert!(e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));

        std::fs::write(&path, r#"{"acls":[]}"#).unwrap();
        e.reload().unwrap();
        assert!(!e.authorize(&alice(), &Resource::topic("foo"), Operation::Read));
    }

    #[test]
    fn anonymous_default_denies() {
        let e = engine_with(r#"{"acls":[]}"#);
        assert!(!e.authorize(
            &Principal::anonymous(),
            &Resource::topic("foo"),
            Operation::Read
        ));
    }

    #[test]
    fn anonymous_explicit_allow_grants() {
        let e = engine_with(
            r#"{"acls":[{"principal":"User:ANONYMOUS",
                "resource":{"type":"topic","name":"*","patternType":"literal"},
                "operations":["Read"],"permission":"Allow"}]}"#,
        );
        assert!(e.authorize(
            &Principal::anonymous(),
            &Resource::topic("foo"),
            Operation::Read
        ));
    }
}
