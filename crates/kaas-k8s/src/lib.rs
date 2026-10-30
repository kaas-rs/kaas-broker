//! kaas-k8s — broker-side Kubernetes helpers.
//!
//! Phase 5 ships the pure-state cores + the trait seams that
//! `bins/kaas/main.rs` wires together:
//!
//! - [`identity`] — `BrokerIdentity` + `DnsConfig`. Pure code, no
//!   kube dep.
//! - [`endpoints`] — `BrokerRegistry` over the StatefulSet's
//!   headless `Service`. Pure state; the kube-bound pump consumes
//!   `EndpointSlice` events and calls into `apply_slice` /
//!   `delete_slice`.
//! - [`readiness`] — the `kaas.rs/PartitionsReady` condition name.
//!
//! Kube-bound watchers live behind the `kube-watchers` feature
//! (default). The real pump implementations are the workstream E
//! follow-up tracked as task #10 — Phase 5 ships the function
//! signatures so `main.rs` can call them today and the bodies
//! drop in without a wire change.

pub mod endpoints;
pub mod identity;
pub mod readiness;

#[cfg(feature = "kube-watchers")]
pub mod kube_watchers;

pub use endpoints::{BrokerEndpoint, BrokerRegistry, EndpointSliceData, EndpointSliceEntry};
pub use identity::{parse_ordinal, BrokerIdentity, DnsConfig, IdentityError};
pub use readiness::READINESS_CONDITION;
