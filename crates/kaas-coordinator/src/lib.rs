//! kaas-coordinator — consumer-group + transaction coordinator,
//! offset store.
//!
//! Populated in Phase 5 (group coordinator) + Phase 6 (transactions).

pub mod atomic_write;
pub mod errors;
pub mod fence_log;
pub mod group;
pub mod manager;
pub mod marker_queue;
pub mod offset_store;
pub mod txn_state;

pub use errors::CoordError;
pub use fence_log::{fence_log_dir, FenceLog, FENCE_DIR_NAME};
pub use group::{
    error_codes, Group, GroupSnapshot, GroupState, HeartbeatRequest, JoinOutcome, JoinRequest,
    JoinedMember, MemberSnapshot, ProtocolMetadata, SyncAssignment, SyncOutcome, SyncRequest,
};
pub use manager::{
    build_offset_key, BrokerEndpoint, BrokerId, BrokerLookup, CoordinatorResolution, FnLookup,
    GroupAssignmentSource, LeaveOutcome, LocalGroupSource, LocalTxnSource, Manager,
    TxnAssignmentSource,
};
pub use marker_queue::{marker_queue_dir, MarkerEntry, MarkerQueue, MARKER_QUEUE_DIR_NAME};
pub use offset_store::{offset_key, FetchSpec, OffsetStore};
pub use txn_state::{
    EndTxnOutcome, PendingMarkerDispatch, TxnAbortRecord, TxnEntry, TxnOffsetHook, TxnState,
    TxnStateError, TxnStateStore, TxnTopic, DEFAULT_NUM_SLOTS,
};
