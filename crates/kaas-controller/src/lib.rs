//! kaas-controller — controller-side bring-up.
//!
//! Phase 5 ships:
//!
//! - [`balancer`] — rendezvous-hash partition + consumer-group
//!   placement with deterministic smoothing.
//! - [`assignment_writer`] — atomic `assignment.json` writer with
//!   the trait seams (`TopicSource`, `BrokerSource`,
//!   `GroupSource`).
//! - [`heartbeat_server`] — controller-side bidi gRPC server.
//! - [`election`] + [`LocalElection`] — dev-mode "always elected"
//!   stub. The kube-backed Lease implementation lands in workstream
//!   E follow-up alongside `ControllerWatch`.

pub mod assignment_writer;
pub mod balancer;
pub mod election;
pub mod heartbeat_server;

#[cfg(feature = "kube-election")]
pub mod kube_election;

#[cfg(feature = "kube-election")]
pub use kube_election::{
    KubeLeaseElection, DEFAULT_LEASE_DURATION, DEFAULT_RENEW_DEADLINE, DEFAULT_RETRY_PERIOD,
};

pub use assignment_writer::{
    AssignmentLoop, AssignmentReason, BrokerSource, GroupSource, StaticSources, TopicSource,
};
pub use balancer::{balance, balance_groups, rendezvous_hash, GroupSpec, TopicSpec};
pub use election::{LeaseElection, LocalElection};
pub use heartbeat_server::{BrokerLiveness, HeartbeatServer, HeartbeatService};
