//! Broker-side API handler implementations.
//!
//! One file per API; each impl satisfies the
//! [`kaas_protocol::Handler`] trait. The host (`bins/kaas/main.rs`)
//! registers them on a [`kaas_protocol::Dispatcher`].

pub mod acls;
pub mod add_offsets_to_txn;
pub mod add_partitions_to_txn;
pub mod alter_client_quotas;
pub mod alter_replica_log_dirs;
pub mod alter_user_scram_credentials;
pub mod api_versions;
pub mod create_partitions;
pub mod create_topics;
pub mod delete_groups;
pub mod delete_records;
pub mod delete_topics;
pub mod describe_client_quotas;
pub mod describe_cluster;
pub mod describe_configs;
pub mod describe_groups;
pub mod describe_log_dirs;
pub mod describe_user_scram_credentials;
pub mod end_txn;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod sasl;
pub mod sync_group;
pub mod txn_offset_commit;
pub mod write_txn_markers;

pub use acls::{CreateAclsHandler, DeleteAclsHandler, DescribeAclsHandler};
pub use add_offsets_to_txn::AddOffsetsToTxnHandler;
pub use add_partitions_to_txn::AddPartitionsToTxnHandler;
pub use alter_client_quotas::AlterClientQuotasHandler;
pub use alter_replica_log_dirs::{migrate_partition, AlterReplicaLogDirsHandler};
pub use alter_user_scram_credentials::AlterUserScramCredentialsHandler;
pub use api_versions::ApiVersionsHandler;
pub use create_partitions::CreatePartitionsHandler;
pub use create_topics::CreateTopicsHandler;
pub use delete_groups::DeleteGroupsHandler;
pub use delete_records::DeleteRecordsHandler;
pub use delete_topics::DeleteTopicsHandler;
pub use describe_client_quotas::DescribeClientQuotasHandler;
pub use describe_cluster::DescribeClusterHandler;
pub use describe_configs::DescribeConfigsHandler;
pub use describe_groups::DescribeGroupsHandler;
pub use describe_log_dirs::DescribeLogDirsHandler;
pub use describe_user_scram_credentials::DescribeUserScramCredentialsHandler;
pub use end_txn::EndTxnHandler;
pub use fetch::FetchHandler;
pub use find_coordinator::FindCoordinatorHandler;
pub use heartbeat::HeartbeatHandler;
pub use incremental_alter_configs::IncrementalAlterConfigsHandler;
pub use init_producer_id::InitProducerIdHandler;
pub use join_group::JoinGroupHandler;
pub use leave_group::LeaveGroupHandler;
pub use list_groups::ListGroupsHandler;
pub use list_offsets::ListOffsetsHandler;
pub use metadata::MetadataHandler;
pub use offset_commit::OffsetCommitHandler;
pub use offset_delete::OffsetDeleteHandler;
pub use offset_fetch::OffsetFetchHandler;
pub use produce::ProduceHandler;
pub use sasl::{SaslAuthenticateHandler, SaslHandshakeHandler};
pub use sync_group::SyncGroupHandler;
pub use txn_offset_commit::TxnOffsetCommitHandler;
pub use write_txn_markers::WriteTxnMarkersHandler;

/// The connection's authenticated principal, or `User:ANONYMOUS` on a
/// listener that never ran SASL. Shared by every handler that
/// consults the authorizer (gh #199 hoisted the per-file copies).
pub(crate) fn principal_from(
    conn: &parking_lot::Mutex<kaas_protocol::ConnState>,
) -> kaas_auth::Principal {
    conn.lock()
        .principal
        .clone()
        .unwrap_or_else(kaas_auth::Principal::anonymous)
}

/// KIP-588 (gh #285): `PRODUCER_FENCED` (90) is new in InitProducerId v4
/// and AddPartitionsToTxn / AddOffsetsToTxn / EndTxn v2. An older client
/// doesn't know it, so Apache answers those versions with
/// `INVALID_PRODUCER_EPOCH` (47), which the client already handles the
/// same way. `first_version` is where the API learned 90.
pub(crate) fn producer_fenced_for(code: i16, version: i16, first_version: i16) -> i16 {
    const PRODUCER_FENCED: i16 = 90;
    const INVALID_PRODUCER_EPOCH: i16 = 47;
    if code == PRODUCER_FENCED && version < first_version {
        INVALID_PRODUCER_EPOCH
    } else {
        code
    }
}

/// Whether `principal` may create topic `name`: `Create` on the cluster,
/// else `Create` on the topic — Apache's rule for both CreateTopics and
/// Metadata auto-creation, so neither is a way around the other
/// (gh #280). The cluster check is quiet (Apache's `logIfDenied = false`):
/// lacking the cluster-wide grant is the normal case, not a denial.
pub(crate) fn may_create_topic(
    broker: &crate::broker::Broker,
    principal: &kaas_auth::Principal,
    name: &str,
) -> bool {
    use kaas_auth::{Operation, Resource};
    broker
        .authorizer
        .authorize_quietly(principal, &Resource::cluster(), Operation::Create)
        || broker
            .authorizer
            .authorize(principal, &Resource::topic(name), Operation::Create)
}

/// gh #199 deny-path test doubles. Handler tests build a broker with
/// one of these to prove a gate exists and fires the right code.
#[cfg(test)]
mod tests {
    /// gh #285: Apache's literal codes and KIP-588's version thresholds.
    #[test]
    fn producer_fenced_is_down_converted_only_below_the_first_version() {
        assert_eq!(super::producer_fenced_for(90, 3, 4), 47);
        assert_eq!(super::producer_fenced_for(90, 4, 4), 90);
        assert_eq!(
            super::producer_fenced_for(51, 0, 4),
            51,
            "other codes untouched"
        );
        assert_eq!(super::producer_fenced_for(0, 0, 2), 0);
    }
}

#[cfg(test)]
pub(crate) mod test_authz {
    use kaas_auth::{Authorizer, Operation, Principal, Resource};

    /// Denies every check.
    #[derive(Debug)]
    pub struct DenyAllAuthorizer;

    impl Authorizer for DenyAllAuthorizer {
        fn authorize(&self, _p: &Principal, _r: &Resource, _op: Operation) -> bool {
            false
        }
    }

    /// Denies any resource with the given name, allows the rest —
    /// for mixed-outcome shapes (one topic denied, the other not).
    #[derive(Debug)]
    pub struct DenyNamedAuthorizer(pub &'static str);

    impl Authorizer for DenyNamedAuthorizer {
        fn authorize(&self, _p: &Principal, r: &Resource, _op: Operation) -> bool {
            r.name != self.0
        }
    }

    /// Denies one operation everywhere, allows the rest — for gates
    /// that check two operations in order (Metadata's `Describe`, then
    /// `Create` on auto-create).
    #[derive(Debug)]
    pub struct DenyOpAuthorizer(pub Operation);

    impl Authorizer for DenyOpAuthorizer {
        fn authorize(&self, _p: &Principal, _r: &Resource, op: Operation) -> bool {
            op != self.0
        }
    }

    /// Grants `Create` only on the cluster resource, everything else
    /// everywhere — Apache's cluster-wide create grant (gh #280).
    #[derive(Debug)]
    pub struct ClusterCreateOnlyAuthorizer;

    impl Authorizer for ClusterCreateOnlyAuthorizer {
        fn authorize(&self, _p: &Principal, r: &Resource, op: Operation) -> bool {
            op != Operation::Create || r.kind == kaas_auth::ResourceKind::Cluster
        }
    }
}
