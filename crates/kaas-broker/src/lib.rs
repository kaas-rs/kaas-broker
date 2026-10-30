//! kaas-broker — broker glue: [`Broker`], [`TopicRegistry`],
//! [`LocalLeaseManager`], env-var parser ([`cli`]).
//!
//! Phase 3 ships the narrow shape every handler reads from. Phase 5
//! grows this with the real `Coordinator` (assignment.json watcher),
//! takeover driver, heartbeat client. The `heartbeatpb` module is
//! generated at build time by `tonic-build` from
//! `proto/heartbeat.proto` and is Phase-5-consumer-only for now.

pub mod acl_cr_writer;
pub mod argocd;
pub mod assignment;
pub mod broker;
pub mod cli;
pub mod control_batch;
pub mod coordinator;
pub mod fence_watcher;
pub mod group_hash;
pub mod group_takeover;
pub mod handlers;
pub mod heartbeat_client;
pub mod listener_advert;
pub mod local_lease;
pub mod marker_watcher;
pub mod producer_id;
pub mod self_fence;
pub mod takeover;
pub mod topic_config_defaults;
pub mod topic_cr_writer;
pub mod topic_registry;
pub mod txn_markers;
pub mod user_cr_writer;

#[cfg(feature = "cr-writer")]
pub use acl_cr_writer::KubeAclCRWriter;
pub use acl_cr_writer::{AclBinding, AclCRWriter, AclFilter, AclWriteError};
pub use assignment::{
    Assignment, AssignmentChangeHandler, BrokerAssignment, BrokerHealth, ConsumerGroupAssignment,
    PartitionAssignment, PartitionRole,
};
pub use broker::{Broker, BrokerNode, ClusterBrokerView};
pub use cli::{Cli, ListenerEntry, TlsConfig as CliTlsConfig};
pub use control_batch::build_control_batch;
pub use coordinator::{
    is_serving, partition_key, Coordinator, HeartbeatSource, LeaseEpochSource, LocalHeartbeat,
    LocalLeaseEpoch,
};
pub use fence_watcher::{FenceWatcher, ProducerEpochFencer};
pub use group_hash::{
    coordinator_slot, pick_coordinator, pick_group_coordinator, pick_txn_coordinator,
};
pub use group_takeover::GroupTakeoverDriver;
pub use handlers::{
    migrate_partition, AddOffsetsToTxnHandler, AddPartitionsToTxnHandler, AlterClientQuotasHandler,
    AlterReplicaLogDirsHandler, AlterUserScramCredentialsHandler, ApiVersionsHandler,
    CreateAclsHandler, CreatePartitionsHandler, CreateTopicsHandler, DeleteAclsHandler,
    DeleteGroupsHandler, DeleteRecordsHandler, DeleteTopicsHandler, DescribeAclsHandler,
    DescribeClientQuotasHandler, DescribeClusterHandler, DescribeConfigsHandler,
    DescribeGroupsHandler, DescribeLogDirsHandler, DescribeUserScramCredentialsHandler,
    EndTxnHandler, FetchHandler, FindCoordinatorHandler, HeartbeatHandler,
    IncrementalAlterConfigsHandler, InitProducerIdHandler, JoinGroupHandler, LeaveGroupHandler,
    ListGroupsHandler, ListOffsetsHandler, MetadataHandler, OffsetCommitHandler,
    OffsetDeleteHandler, OffsetFetchHandler, ProduceHandler, SaslAuthenticateHandler,
    SaslHandshakeHandler, SyncGroupHandler, TxnOffsetCommitHandler, WriteTxnMarkersHandler,
};
pub use heartbeat_client::{CommandHandler, HeartbeatClient, TargetResolver};
pub use local_lease::LocalLeaseManager;
pub use marker_watcher::{ApplyOutcome, MarkerApplier, MarkerWatcher};
pub use producer_id::{ProducerIdAllocator, PID_BLOCK, PID_STRIDE, PRODUCER_ID_DIR};
pub use self_fence::{is_heartbeat_fresh, DEFAULT_HEARTBEAT_TIMEOUT};
pub use takeover::TakeoverDriver;
#[cfg(feature = "cr-writer")]
pub use topic_cr_writer::KubeTopicCRWriter;
pub use topic_cr_writer::{
    config_key_to_json_field, config_value_to_json, ConfigOp, ConfigOpKind, ConfigOpWithValue,
    TopicCRWriter, TopicWriteError,
};
pub use topic_registry::{ConfigError as TopicConfigError, TopicMeta, TopicRegistry};
#[cfg(feature = "cr-writer")]
pub use user_cr_writer::KubeUserCRWriter;
pub use user_cr_writer::{ScramCredentialSpec, UserCRWriter, UserWriteError};

pub mod heartbeatpb {
    // tonic-build emits `as i32`, large match arms, and similar patterns that
    // trip the workspace clippy gate. Generated code is not subject to the
    // hand-written style rules.
    #![allow(
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        missing_debug_implementations
    )]
    tonic::include_proto!("kaas.heartbeat.v1");
}
