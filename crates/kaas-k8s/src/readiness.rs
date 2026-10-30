//! Pod-readiness gate plumbing.
//!
//! The Helm chart
//! declares a custom `kaas.rs/PartitionsReady` `readinessGate` on
//! each broker pod; the broker is not added to the headless
//! `Service`'s endpoints until that gate flips to `True`. The
//! kube-backed patch is `kube_watchers::patch_readiness`.

/// The `Condition.Type` value the chart's `readinessGate` waits
/// for. Defined here so the kube-backed impl + the chart agree on
/// the exact string.
pub const READINESS_CONDITION: &str = "kaas.rs/PartitionsReady";
