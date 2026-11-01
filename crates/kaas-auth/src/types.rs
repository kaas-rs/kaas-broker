//! Shared types — `Principal`, `Resource`, `Operation`, `Quotas`.
//!
//! These cross every workstream in `kaas-auth` and re-export through the
//! `kaas-protocol` connstate so handlers in `kaas-broker` carry one
//! canonical principal type.

use std::fmt;

/// Authenticated identity. Anonymous sessions carry
/// `PrincipalKind::Anonymous` with a fixed name (matches the Apache
/// `User:ANONYMOUS` convention).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Principal {
    pub name: String,
    pub kind: PrincipalKind,
}

impl Principal {
    /// The pre-SASL placeholder. Authorizers see this when an
    /// anonymous listener forwards a request to authorize.
    pub fn anonymous() -> Self {
        Self {
            name: "ANONYMOUS".to_owned(),
            kind: PrincipalKind::Anonymous,
        }
    }

    /// `"User:alice"`-style render — matches the principal-string
    /// format ACL rules use on the wire.
    pub fn as_principal_string(&self) -> String {
        format!("{}:{}", self.kind.as_str(), self.name)
    }
}

impl fmt::Display for Principal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.as_principal_string())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PrincipalKind {
    User,
    ServiceAccount,
    Anonymous,
}

impl PrincipalKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "User",
            Self::ServiceAccount => "ServiceAccount",
            // ACL rules in credentials.json target "User:ANONYMOUS"
            // (Strimzi-compat), not a separate kind — preserve that
            // render so an operator's allow rule for the anonymous
            // principal lands the same way as a User rule.
            Self::Anonymous => "User",
        }
    }
}

/// What's being accessed in an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resource {
    pub kind: ResourceKind,
    pub name: String,
    pub pattern: PatternType,
}

impl Resource {
    pub fn topic(name: impl Into<String>) -> Self {
        Self {
            kind: ResourceKind::Topic,
            name: name.into(),
            pattern: PatternType::Literal,
        }
    }

    pub fn group(name: impl Into<String>) -> Self {
        Self {
            kind: ResourceKind::Group,
            name: name.into(),
            pattern: PatternType::Literal,
        }
    }

    pub fn cluster() -> Self {
        Self {
            kind: ResourceKind::Cluster,
            name: "kafka-cluster".to_owned(),
            pattern: PatternType::Literal,
        }
    }

    pub fn transactional_id(name: impl Into<String>) -> Self {
        Self {
            kind: ResourceKind::TransactionalId,
            name: name.into(),
            pattern: PatternType::Literal,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResourceKind {
    Topic,
    Group,
    Cluster,
    TransactionalId,
}

impl ResourceKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Topic => "topic",
            Self::Group => "group",
            Self::Cluster => "cluster",
            Self::TransactionalId => "transactionalId",
        }
    }
}

/// On-wire pattern used by an ACL rule (not by a request resource —
/// requests always carry `Literal`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternType {
    Literal,
    Prefix,
    Any,
    Match,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Operation {
    Read,
    Write,
    Create,
    Delete,
    Alter,
    Describe,
    DescribeConfigs,
    AlterConfigs,
    /// Broker-internal APIs (WriteTxnMarkers et al). Apache grants
    /// this to broker principals only; a client holding it can inject
    /// control batches, so treat it like a super-user bit (gh #199).
    ClusterAction,
    /// Cluster-level idempotent-produce grant. Nothing in kaas gates on it
    /// (since KIP-679 Apache's InitProducerId accepts `Write` on any topic
    /// in its place), but it is a CLUSTER operation Apache reports in the
    /// authorized-operations bitfields, so an `All` or explicit grant must
    /// answer it (gh #284).
    IdempotentWrite,
}

impl Operation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Read => "Read",
            Self::Write => "Write",
            Self::Create => "Create",
            Self::Delete => "Delete",
            Self::Alter => "Alter",
            Self::Describe => "Describe",
            Self::DescribeConfigs => "DescribeConfigs",
            Self::AlterConfigs => "AlterConfigs",
            Self::ClusterAction => "ClusterAction",
            Self::IdempotentWrite => "IdempotentWrite",
        }
    }

    /// Parse an Apache-shape operation name. Unknown strings map to
    /// `None`. ACL rules that target an unknown op are dropped on
    /// load with a warning.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Read" => Some(Self::Read),
            "Write" => Some(Self::Write),
            "Create" => Some(Self::Create),
            "Delete" => Some(Self::Delete),
            "Alter" => Some(Self::Alter),
            "Describe" => Some(Self::Describe),
            "DescribeConfigs" => Some(Self::DescribeConfigs),
            "AlterConfigs" => Some(Self::AlterConfigs),
            "ClusterAction" => Some(Self::ClusterAction),
            "IdempotentWrite" => Some(Self::IdempotentWrite),
            _ => None,
        }
    }
}

/// Per-user throughput limits loaded from credentials.json.
///
/// Byte-rate fields are PER BROKER (KIP-13 semantics): with N brokers
/// the effective cluster-wide ceiling is N × the configured value.
/// Named honestly here (vs Strimzi's `producerByteRate` etc.) so the
/// per-broker meaning is legible at the CR level.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Quotas {
    pub producer_max_byte_rate_per_broker: Option<i64>,
    pub consumer_max_byte_rate_per_broker: Option<i64>,
    pub request_percentage: Option<i32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_principal_renders_as_user() {
        let p = Principal::anonymous();
        assert_eq!(p.as_principal_string(), "User:ANONYMOUS");
    }

    #[test]
    fn user_principal_renders() {
        let p = Principal {
            name: "alice".to_owned(),
            kind: PrincipalKind::User,
        };
        assert_eq!(p.as_principal_string(), "User:alice");
    }

    #[test]
    fn operation_parse_round_trips() {
        for op in [
            Operation::Read,
            Operation::Write,
            Operation::Create,
            Operation::Delete,
            Operation::Alter,
            Operation::Describe,
            Operation::DescribeConfigs,
            Operation::AlterConfigs,
            Operation::ClusterAction,
            Operation::IdempotentWrite,
        ] {
            assert_eq!(Operation::parse(op.as_str()), Some(op));
        }
        assert_eq!(Operation::parse("Unknown"), None);
    }

    #[test]
    fn resource_constructors() {
        let t = Resource::topic("foo");
        assert_eq!(t.kind, ResourceKind::Topic);
        assert_eq!(t.pattern, PatternType::Literal);
        assert_eq!(t.name, "foo");

        let c = Resource::cluster();
        assert_eq!(c.kind, ResourceKind::Cluster);
    }
}
