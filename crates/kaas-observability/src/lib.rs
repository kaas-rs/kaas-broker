//! kaas-observability — OTLP metrics + tracing, /healthz, tripwires.
//!
//! Central registry for every
//! OTel instrument kaas emits; `bootstrap` wires OTLP push (metrics)
//! and OTLP gRPC (traces) against the endpoints exposed by the chart via
//! the standard `OTEL_EXPORTER_OTLP_*` env vars.
//!
//! The `Metrics` struct is Arc-shared; call sites reach for it via
//! [`metrics::global()`]. Before [`bootstrap`] runs, `global()` returns
//! a no-op registry backed by OTel's `NoopMeterProvider` — safe to
//! dereference from tests and pre-boot code without nil checks.

pub mod bootstrap;
pub mod byteopacity;
pub mod gauges;
pub mod health;
pub mod k8s_api;
pub mod metrics;
pub mod otlp_push_observer;
pub mod topic_traffic;
pub mod tracing;

// Re-export the OTel types call sites need so downstream crates don't
// have to depend on `opentelemetry` themselves.
pub use opentelemetry::{self, KeyValue};

pub use bootstrap::{bootstrap, Providers};
pub use byteopacity::{bump_codec_batch_reencode, bump_codec_record_decode};
pub use gauges::{set_gauge_source, GaugeSource, LogDirCapacityGauge, PartitionGauge};
pub use health::{
    health_router, is_draining, main_alive, mark_draining, ready, record_main_tick, set_ready,
    RuntimeState,
};
pub use k8s_api::record_k8s_call;
pub use metrics::{global, new_metrics, set_global, Metrics};
pub use topic_traffic::TopicTrafficMeter;
pub use tracing::install_tracing;

/// Errors returned by [`bootstrap`]. Anything downstream (metric
/// exporter build failure, tracer build failure) folds into this
/// enum — call sites in `bins/*` propagate via `anyhow`.
#[derive(Debug, thiserror::Error)]
pub enum ObservabilityError {
    #[error("otlp metric exporter: {0}")]
    MetricExporter(String),
    #[error("otlp trace exporter: {0}")]
    TraceExporter(String),
    #[error("meter provider shutdown: {0}")]
    MeterShutdown(String),
    #[error("tracer provider shutdown: {0}")]
    TracerShutdown(String),
}
