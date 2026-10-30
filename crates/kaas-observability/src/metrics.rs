//! Central OTel instrument registry.
//!
//! Every metric
//! name, description, unit, and label set here is pinned — dashboards
//! and alert rules were written against the v0.1 OTLP output.
//!
//! The 14 `register_*` helpers keep that layout; adding a
//! new subsystem means adding a new helper and a new entry on
//! [`Metrics`]. Do NOT sneak instruments into an existing helper — the
//! grouping is load-bearing for reviewer legibility.

use std::sync::Arc;

use arc_swap::ArcSwap;
use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};

use crate::topic_traffic::{register_topic_traffic_instruments, TopicTrafficMeter};

/// Explicit histogram bucket boundaries for every seconds-unit
/// latency histogram. Ports `latencySecondsBoundaries` from
/// `metrics.go:51`.
///
/// Without these, OTel falls back to its default HTTP-latency-oriented
/// boundaries (5..10000) and every kaas observation (sub-second to
/// mid-second) collapses into the [0, 5] bucket; histogram_quantile
/// then interpolates every percentile to fixed values regardless of
/// load (gh #79). Range: 100 µs (in-process hot path) → 30 s
/// (failover / drain-scale events).
pub const LATENCY_SECONDS_BOUNDARIES: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// Central OTel instrument registry.
///
/// Passed around as [`Arc<Metrics>`] — [`Counter`], [`Histogram`], etc.
/// are all cheap to clone (Arc-backed internally). Call sites reach for
/// it via [`global()`].
#[derive(Debug, Clone)]
pub struct Metrics {
    /// Per-topic Produce/Fetch counters (gh #115 + gh #121 PR1). See
    /// [`TopicTrafficMeter`] for the always-emit design.
    pub topic_traffic: Arc<TopicTrafficMeter>,

    /// Per-request handler latency histogram. Topic label intentionally
    /// omitted to cap cardinality; per-topic volume is in
    /// [`Self::topic_traffic`].
    pub request_latency: Histogram<f64>,

    // Storage.
    pub write_latency: Histogram<f64>,
    pub read_latency: Histogram<f64>,
    pub fsync_latency: Histogram<f64>,

    // Cluster controller leadership.
    pub controller_failovers: Counter<u64>,
    pub controller_failover_duration: Histogram<f64>,
    pub controller_lease_conflicts: Counter<u64>,

    // Assignment loop (controller-side).
    pub assignment_changes: Counter<u64>,
    pub assignment_file_writes: Counter<u64>,
    pub assignment_file_write_latency: Histogram<f64>,
    pub assignment_pushes: Counter<u64>,
    pub assignment_polls: Counter<u64>,
    pub stale_assignments_rejected: Counter<u64>,

    // Broker-side heartbeat.
    pub heartbeat_rtt: Histogram<f64>,
    pub heartbeat_misses: Counter<u64>,
    pub self_fence_events: Counter<u64>,

    // Byte-opacity tripwires — MUST stay at zero. See [`crate::byteopacity`].
    pub codec_record_decode: Counter<u64>,
    pub codec_batch_reencode: Counter<u64>,

    // Consumer groups.
    pub group_rebalances: Counter<u64>,

    // Auth.
    pub auth_success: Counter<u64>,
    pub auth_failure: Counter<u64>,
    pub acl_deny: Counter<u64>,
    pub quota_throttle: Counter<u64>,

    // TLS / external access.
    pub tls_handshakes: Counter<u64>,
    pub cert_reloads: Counter<u64>,

    // Connection counters.
    pub connections: Counter<u64>,
    pub connections_open: UpDownCounter<i64>,

    // Per-partition Produce/Fetch errors (gh #132).
    pub produce_errors: Counter<u64>,
    pub fetch_errors: Counter<u64>,

    // Cleaner (retention).
    pub cleaner_runs: Counter<u64>,
    pub cleaner_duration: Histogram<f64>,
    pub cleaner_segments_deleted: Counter<u64>,
    pub cleaner_bytes_reclaimed: Counter<u64>,

    // Compactor (log-compaction).
    pub compaction_runs: Counter<u64>,
    pub compaction_duration: Histogram<f64>,
    pub compaction_records_kept: Counter<u64>,
    pub compaction_records_dropped: Counter<u64>,
    pub compaction_bytes_in: Counter<u64>,
    pub compaction_bytes_out: Counter<u64>,

    // OTLP push self-observability (gh #121 PR4).
    pub otlp_push_success: Counter<u64>,
    pub otlp_push_failure: Counter<u64>,
    pub otlp_push_duration: Histogram<f64>,

    // Operator reconciler (gh #121 PR5).
    pub operator_reconciles: Counter<u64>,
    pub operator_reconcile_duration: Histogram<f64>,

    // K8s API call observability (gh #121 PR4.5).
    pub k8s_api_calls: Counter<u64>,
    pub k8s_api_latency: Histogram<f64>,
}

/// Build every instrument on the given meter and register the
/// always-emit callbacks for [`TopicTrafficMeter`].
///
/// OTel Rust 0.27's `.build()` is infallible — invalid instrument
/// config yields a no-op instrument with an internal log line, not
/// an error. That means [`new_metrics`] can never fail, and the
/// global-noop fallback in [`global`] doesn't need panic paths.
#[must_use]
pub fn new_metrics(m: &Meter) -> Metrics {
    let topic_traffic = Arc::new(TopicTrafficMeter::new());
    register_topic_traffic_instruments(m, Arc::clone(&topic_traffic));

    Metrics {
        topic_traffic,

        request_latency: latency_hist(m, "kaas.request.latency", "Kafka request handler latency"),
        write_latency: latency_hist(m, "kaas.storage.write.latency", "Partition append latency"),
        read_latency: latency_hist(m, "kaas.storage.read.latency", "Partition read latency"),
        fsync_latency: latency_hist(m, "kaas.storage.fsync.latency", "Segment fsync latency"),

        controller_failovers: counter(
            m,
            "kaas.controller.failovers",
            "Times this broker won the cluster controller lease",
        ),
        controller_failover_duration: latency_hist(
            m,
            "kaas.controller.failover.duration",
            "Seconds from winning the lease to the first AssignmentLoop write",
        ),
        controller_lease_conflicts: counter(
            m,
            "kaas.controller.lease.conflicts",
            "Controller Lease writes answered 409 because another writer got there first, by \
             write (takeover, renew, release)",
        ),

        assignment_changes: counter(
            m,
            "kaas.assignment.changes",
            "AssignmentLoop recompute+write iterations",
        ),
        assignment_file_writes: counter(
            m,
            "kaas.assignment.file.writes",
            "AssignmentStore.Write attempts (result=ok|error)",
        ),
        assignment_file_write_latency: latency_hist(
            m,
            "kaas.assignment.file.write.latency",
            "AssignmentStore.Write tmp+rename duration",
        ),
        assignment_pushes: counter(
            m,
            "kaas.assignment.pushes",
            "ASSIGNMENT_CHANGED broadcasts via heartbeat server",
        ),
        assignment_polls: counter(
            m,
            "kaas.assignment.polls",
            "assignment.json mtime poll iterations (change_detected=true|false)",
        ),
        stale_assignments_rejected: counter(
            m,
            "kaas.stale.assignments.rejected",
            "assignment.json reads dropped because controllerEpoch was behind",
        ),

        heartbeat_rtt: latency_hist(
            m,
            "kaas.heartbeat.rtt",
            "Broker→controller→broker heartbeat round-trip",
        ),
        heartbeat_misses: counter(
            m,
            "kaas.heartbeat.misses",
            "Heartbeats not received within heartbeatTimeout",
        ),
        self_fence_events: counter(
            m,
            "kaas.self.fence.events",
            "Times this broker self-fenced due to stale heartbeat",
        ),

        codec_record_decode: counter(
            m,
            "kaas.codec.record.decode",
            "Tripwire: code path decoded an individual record. MUST stay at zero — alert if non-zero",
        ),
        codec_batch_reencode: counter(
            m,
            "kaas.codec.batch.reencode",
            "Tripwire: code path re-encoded a RecordBatch. MUST stay at zero — alert if non-zero",
        ),

        group_rebalances: counter(
            m,
            "kaas.group.rebalances",
            "Consumer group rebalance completions",
        ),

        auth_success: counter(
            m,
            "kaas.auth.success",
            "Successful SASL / mTLS authentications",
        ),
        auth_failure: counter(
            m,
            "kaas.auth.failure",
            "Failed authentication attempts",
        ),
        acl_deny: counter(m, "kaas.acl.deny", "Authorization denials"),
        quota_throttle: counter(
            m,
            "kaas.quota.throttle",
            "Requests that hit a quota and were throttled",
        ),
        tls_handshakes: counter(m, "kaas.tls.handshakes", "TLS handshakes completed"),
        cert_reloads: counter(
            m,
            "kaas.cert.reloads",
            "TLS certificate hot-reloads (result=ok|error). Failures stay visible — cert-manager mid-rotation or stale Secret mounts surface as result=error and don't go silent.",
        ),

        connections: counter(m, "kaas.connections", "New client connections accepted"),
        connections_open: m
            .i64_up_down_counter("kaas.connections.open")
            .with_description("Currently open client connections")
            .build(),

        produce_errors: counter_with_unit(
            m,
            "kaas.produce.errors",
            "Per-partition Produce failures (labels: topic, error_code). Bumped on every error path — storage stalled, not leader, corrupt batch, auth denied, out-of-order sequence, fenced producer epoch.",
            "{error}",
        ),
        fetch_errors: counter_with_unit(
            m,
            "kaas.fetch.errors",
            "Per-partition Fetch failures (labels: topic, error_code). Bumped on every error path — not leader, read failure, auth denied.",
            "{error}",
        ),

        cleaner_runs: counter(
            m,
            "kaas.cleaner.runs",
            "Retention cleaner partition-pass completions (result=ok|error)",
        ),
        cleaner_duration: latency_hist(
            m,
            "kaas.cleaner.duration",
            "Wall-clock per retention cleaner partition pass",
        ),
        cleaner_segments_deleted: counter_with_unit(
            m,
            "kaas.cleaner.segments.deleted",
            "Segments deleted by the retention cleaner (reason=time|size)",
            "{segment}",
        ),
        cleaner_bytes_reclaimed: counter_with_unit(
            m,
            "kaas.cleaner.bytes.reclaimed",
            "Bytes freed by retention deletes (reason=time|size). Approximates disk-pressure relief; on NFS the actual unlink may lag if another broker held the fd.",
            "By",
        ),

        compaction_runs: counter(
            m,
            "kaas.compaction.runs",
            "Log compactor partition-pass completions (result=ok|error|aborted)",
        ),
        compaction_duration: latency_hist(
            m,
            "kaas.compaction.duration",
            "Wall-clock per log compactor partition pass",
        ),
        compaction_records_kept: counter_with_unit(
            m,
            "kaas.compaction.records.kept",
            "Records surviving the compactor's keep-latest-per-key pass",
            "{record}",
        ),
        compaction_records_dropped: counter_with_unit(
            m,
            "kaas.compaction.records.dropped",
            "Records superseded by a later write for the same key — the dedup win",
            "{record}",
        ),
        compaction_bytes_in: counter_with_unit(
            m,
            "kaas.compaction.bytes.in",
            "Source-segment bytes scanned by the compactor (before dedup)",
            "By",
        ),
        compaction_bytes_out: counter_with_unit(
            m,
            "kaas.compaction.bytes.out",
            "Replacement-segment bytes written by the compactor (after dedup). bytes.in - bytes.out is the size savings.",
            "By",
        ),

        otlp_push_success: counter(
            m,
            "kaas.otlp.push.success",
            "OTLP metric exports that succeeded",
        ),
        otlp_push_failure: counter(
            m,
            "kaas.otlp.push.failure",
            "OTLP metric exports that failed (err_class=timeout|refused|other)",
        ),
        otlp_push_duration: latency_hist(
            m,
            "kaas.otlp.push.duration",
            "Time spent in Exporter.Export — high values suggest backend pressure",
        ),

        operator_reconciles: counter(
            m,
            "kaas.operator.reconciles",
            "Operator reconcile completions (kind=CR kind, result=ok|requeue|error)",
        ),
        operator_reconcile_duration: latency_hist(
            m,
            "kaas.operator.reconcile.duration",
            "Operator Reconcile() wall-clock per call (kind=...)",
        ),

        k8s_api_calls: counter(
            m,
            "kaas.k8s.api.calls",
            "Apiserver calls from the broker (operation=Get|List|Watch|Patch|Update|Create, resource=KafkaTopic|EndpointSlice|Lease|Pod, result=ok|error)",
        ),
        k8s_api_latency: latency_hist(
            m,
            "kaas.k8s.api.latency",
            "Apiserver call wall-clock per (operation, resource)",
        ),
    }
}

fn counter(m: &Meter, name: &'static str, description: &'static str) -> Counter<u64> {
    m.u64_counter(name).with_description(description).build()
}

fn counter_with_unit(
    m: &Meter,
    name: &'static str,
    description: &'static str,
    unit: &'static str,
) -> Counter<u64> {
    m.u64_counter(name)
        .with_description(description)
        .with_unit(unit)
        .build()
}

fn latency_hist(m: &Meter, name: &'static str, description: &'static str) -> Histogram<f64> {
    m.f64_histogram(name)
        .with_description(description)
        .with_unit("s")
        .with_boundaries(LATENCY_SECONDS_BOUNDARIES.to_vec())
        .build()
}

// ---------- global registry ----------

fn global_slot() -> &'static ArcSwap<Metrics> {
    static SLOT: std::sync::OnceLock<ArcSwap<Metrics>> = std::sync::OnceLock::new();
    SLOT.get_or_init(|| {
        // Before bootstrap runs, opentelemetry::global::meter() returns
        // a Noop meter — every instrument built on it silently drops
        // observations. That's the right behaviour for tests + pre-boot
        // call sites, so we don't need a special "noop" path.
        let noop = opentelemetry::global::meter("kaas-noop");
        ArcSwap::from_pointee(new_metrics(&noop))
    })
}

/// Returns the currently-installed metrics registry. Before
/// [`crate::bootstrap`] runs, this is a no-op registry built off OTel's
/// global Noop meter — safe to dereference from tests without nil
/// checks.
#[must_use]
pub fn global() -> Arc<Metrics> {
    global_slot().load_full()
}

/// Install a fresh metrics registry. Called by [`crate::bootstrap`]
/// after the real meter provider is set on the OTel global. Tests may
/// call this to install a reader-backed registry for assertion.
pub fn set_global(m: Arc<Metrics>) {
    global_slot().store(m);
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use opentelemetry::KeyValue;

    #[test]
    fn global_returns_noop_before_bootstrap() {
        // Doesn't panic; observations are silently dropped.
        let m = global();
        m.request_latency.record(0.001, &[]);
        m.codec_record_decode
            .add(1, &[KeyValue::new("site", "test")]);
    }

    #[test]
    fn set_global_swaps_registry() {
        let meter = opentelemetry::global::meter("test-meter");
        let fresh = Arc::new(new_metrics(&meter));
        set_global(fresh);
        // no panic
        let _ = global();
    }
}
