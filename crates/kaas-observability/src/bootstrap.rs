//! OTel SDK bring-up.
//!
//! Observability bootstrap. Reads:
//!
//! * `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` — if set, push metrics via
//!   OTLP/HTTP (http/protobuf). Prometheus's native OTLP receiver
//!   (`/api/v1/otlp/v1/metrics`) speaks only http/protobuf; exporting gRPC
//!   here just gets an h2 GoAway from Prometheus.
//! * `KAAS_METRIC_EXPORT_INTERVAL` — duration string (`30s` etc.).
//!   Default `30s` per gh #133 (SDK default is 60s → `rate([1m])`
//!   panels see one sample and gap).
//! * `OTEL_RESOURCE_ATTRIBUTES`, `OTEL_SERVICE_NAME` — the standard
//!   resource env vars (gh #269); see [`compose_resource`] for how they
//!   rank against kaas's own attributes.
//! * `OTEL_SERVICE_VERSION`, `MY_POD_NAME`, `KAAS_NAMESPACE` — folded
//!   into resource attributes so Grafana can filter by broker /
//!   namespace.
//!
//! kaas exports no traces (gh #269). It used to wire a span exporter
//! and a sampler, but no code ever opened a span, so they shipped
//! nothing.
//!
//! With no metrics endpoint set the SDK still runs but every export
//! goes nowhere — safe for tests + local runs.

use std::sync::Arc;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::{Key, KeyValue};
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider};
use opentelemetry_sdk::resource::{EnvResourceDetector, TelemetryResourceDetector};
use opentelemetry_sdk::runtime;
use opentelemetry_sdk::Resource;
use tokio_util::sync::CancellationToken;

use crate::gauges::install_runtime_gauges;
use crate::metrics::{new_metrics, set_global, Metrics};
use crate::otlp_push_observer::ObservedExporter;
use crate::ObservabilityError;

/// Handle to the initialised OTel SDK.
#[derive(Debug)]
pub struct Providers {
    pub meter_provider: SdkMeterProvider,
    pub metrics: Arc<Metrics>,
    service: &'static str,
    _cancel: CancellationToken,
}

impl Providers {
    /// Register the broker's runtime observable gauges
    /// (`kaas.is.controller`, `kaas.assignment.version`, the
    /// per-partition and per-log-dir gauges). Only the broker calls
    /// this (gh #290). They describe a broker, and an observable gauge
    /// exports on every push even before a source is installed. When
    /// [`bootstrap`] registered them for every process, the operator
    /// pushed a zero-valued `kaas_is_controller` and
    /// `kaas_assignment_version` with no `instance` label into the
    /// per-broker panels.
    pub fn install_runtime_gauges(&self) {
        install_runtime_gauges(&global::meter(self.service));
    }

    /// Flush the last metric export and shut the meter provider down.
    pub fn shutdown(self) -> Result<(), ObservabilityError> {
        self.meter_provider
            .shutdown()
            .map_err(|e| ObservabilityError::MeterShutdown(e.to_string()))
    }
}

/// Set up the meter provider and install the global `Metrics`
/// registry. The broker then registers its runtime gauges with
/// [`Providers::install_runtime_gauges`]. Idempotent w.r.t. the
/// OTel globals — safe to call once per process; subsequent calls
/// stomp the previous provider.
///
/// `service` becomes `service.name` in exported resource attributes;
/// pass `"kaas"` or `"kaas-operator"`.
pub async fn bootstrap(
    service: &'static str,
    cancel: CancellationToken,
) -> Result<Providers, ObservabilityError> {
    let resource = build_resource(service);

    // --- Meter provider ---
    let mut mp_builder = SdkMeterProvider::builder().with_resource(resource);
    if let Ok(endpoint) = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
        if !endpoint.is_empty() {
            let raw = MetricExporter::builder()
                .with_http()
                .with_endpoint(ensure_scheme(&endpoint))
                .with_timeout(Duration::from_secs(10))
                .build()
                .map_err(|e| ObservabilityError::MetricExporter(e.to_string()))?;

            let wrapped = ObservedExporter::new(raw);
            let interval = parse_duration_env("KAAS_METRIC_EXPORT_INTERVAL")
                .unwrap_or_else(|| Duration::from_secs(30));
            let reader = PeriodicReader::builder(wrapped, runtime::Tokio)
                .with_interval(interval)
                .build();
            mp_builder = mp_builder.with_reader(reader);
        }
    }
    let meter_provider = mp_builder.build();
    global::set_meter_provider(meter_provider.clone());

    // Build central metric registry from the freshly-installed
    // provider so every instrument records against the real reader.
    let meter = global::meter(service);
    let metrics = Arc::new(new_metrics(&meter));
    set_global(Arc::clone(&metrics));

    ::tracing::info!(
        service,
        otlp_metrics = std::env::var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
            .map(|s| !s.is_empty())
            .unwrap_or(false),
        "observability: initialised"
    );

    Ok(Providers {
        meter_provider,
        metrics,
        service,
        _cancel: cancel,
    })
}

fn build_resource(service: &str) -> Resource {
    // `Resource::new` alone runs no detectors, which is how
    // `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` went unread
    // (gh #269). Not `Resource::default()` either: its service-name
    // detector falls back to `unknown_service`, which would outrank
    // kaas's own name once merged.
    let detected = Resource::from_detectors(
        Duration::ZERO,
        vec![
            Box::new(TelemetryResourceDetector),
            Box::new(EnvResourceDetector::new()),
        ],
    );
    compose_resource(service, &detected, |name| {
        std::env::var(name).ok().filter(|v| !v.is_empty())
    })
}

/// Layer kaas's attributes over what the environment declared
/// (`detected`: `telemetry.sdk.*` plus `OTEL_RESOURCE_ATTRIBUTES`).
///
/// - `service.name` follows the SDK's own order — `OTEL_SERVICE_NAME`,
///   then a `service.name` in `OTEL_RESOURCE_ATTRIBUTES` — with
///   `service` standing in for `unknown_service` as the last resort.
/// - Everything else kaas sets outranks `OTEL_RESOURCE_ATTRIBUTES`, as
///   the resource SDK spec has it for application-provided attributes:
///   the pod identity comes from the downward API and is what
///   per-broker dashboards key on.
fn compose_resource(
    service: &str,
    detected: &Resource,
    var: impl Fn(&str) -> Option<String>,
) -> Resource {
    let service_name = var("OTEL_SERVICE_NAME")
        .or_else(|| {
            detected
                .get(Key::new("service.name"))
                .map(|v| v.to_string())
        })
        .unwrap_or_else(|| service.to_owned());
    let mut attrs = vec![KeyValue::new("service.name", service_name)];
    if let Some(v) = var("OTEL_SERVICE_VERSION") {
        attrs.push(KeyValue::new("service.version", v));
    }
    if let Some(v) = var("MY_POD_NAME") {
        attrs.push(KeyValue::new("k8s.pod.name", v.clone()));
        // Prometheus's OTLP receiver promotes service.instance.id
        // to the `instance` label on every series; without it, all
        // brokers flatten under the same `job=kaas` and
        // per-broker drill-down in Grafana isn't possible.
        attrs.push(KeyValue::new("service.instance.id", v));
    }
    if let Some(v) = var("KAAS_NAMESPACE") {
        attrs.push(KeyValue::new("k8s.namespace.name", v));
    }
    detected.merge(&Resource::new(attrs))
}

/// Parse a short-form duration string (`"30s"`, `"500ms"`) into a
/// [`Duration`]. Returns `None` on parse failure so the caller falls
/// back to the default rather than dying on a chart-config typo.
fn parse_duration_env(name: &str) -> Option<Duration> {
    let raw = std::env::var(name).ok()?;
    parse_duration_str(&raw)
}

fn parse_duration_str(s: &str) -> Option<Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num, unit) = split_num_unit(s)?;
    let value: f64 = num.parse().ok()?;
    let nanos = match unit {
        "ns" => value,
        "us" | "µs" => value * 1_000.0,
        "ms" => value * 1_000_000.0,
        "s" | "" => value * 1_000_000_000.0,
        "m" => value * 60.0 * 1_000_000_000.0,
        "h" => value * 3600.0 * 1_000_000_000.0,
        _ => return None,
    };
    if nanos < 0.0 || !nanos.is_finite() {
        return None;
    }
    // Go back through seconds to sidestep the f64→u64 cast lint.
    // `nanos` is finite non-negative here so `from_secs_f64` is safe.
    Some(Duration::from_secs_f64(nanos / 1_000_000_000.0))
}

fn split_num_unit(s: &str) -> Option<(&str, &str)> {
    let idx = s
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+')
        .unwrap_or(s.len());
    if idx == 0 {
        None
    } else {
        Some((&s[..idx], &s[idx..]))
    }
}

/// Ensure the endpoint carries a URI scheme. Unlike the v0.1
/// exporter (which wanted bare `host:port`), the
/// Rust exporters parse the endpoint as a full URI — a scheme-less
/// `host:port` makes hyper read `host` as the scheme and the export
/// dies with `InvalidUri`. Chart values written for earlier releases
/// omit the scheme, so default them to `http://` (in-cluster,
/// plaintext). The scheme IS the plaintext/TLS selector — the old
/// `OTEL_EXPORTER_OTLP_INSECURE` env was never read here and its
/// chart emission was dropped (gh #257).
fn ensure_scheme(s: &str) -> String {
    if s.contains("://") {
        s.replacen("grpc://", "http://", 1)
    } else {
        format!("http://{s}")
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn ensure_scheme_variants() {
        assert_eq!(ensure_scheme("foo:4317"), "http://foo:4317");
        assert_eq!(ensure_scheme("http://foo:4318"), "http://foo:4318");
        assert_eq!(ensure_scheme("https://foo:4318"), "https://foo:4318");
        assert_eq!(ensure_scheme("grpc://foo:4317"), "http://foo:4317");
        assert_eq!(
            ensure_scheme("http://prom:9090/api/v1/otlp/v1/metrics"),
            "http://prom:9090/api/v1/otlp/v1/metrics"
        );
    }

    #[test]
    fn parse_duration_accepts_short_forms() {
        assert_eq!(parse_duration_str("30s"), Some(Duration::from_secs(30)));
        assert_eq!(
            parse_duration_str("500ms"),
            Some(Duration::from_millis(500))
        );
        assert_eq!(parse_duration_str("1m"), Some(Duration::from_secs(60)));
        assert_eq!(parse_duration_str("2h"), Some(Duration::from_secs(7200)));
        assert_eq!(
            parse_duration_str("1000us"),
            Some(Duration::from_micros(1000))
        );
        assert_eq!(parse_duration_str("100"), Some(Duration::from_secs(100)));
    }

    #[test]
    fn parse_duration_rejects_garbage() {
        assert_eq!(parse_duration_str(""), None);
        assert_eq!(parse_duration_str("abc"), None);
        assert_eq!(parse_duration_str("-1s"), None);
        assert_eq!(parse_duration_str("1years"), None);
    }

    fn vars<'a>(set: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |name| {
            set.iter()
                .find(|(k, _)| *k == name)
                .map(|(_, v)| (*v).to_owned())
        }
    }

    fn attr(resource: &Resource, key: &'static str) -> Option<String> {
        resource.get(Key::new(key)).map(|v| v.to_string())
    }

    fn declared(attrs: &[(&'static str, &'static str)]) -> Resource {
        Resource::new(attrs.iter().map(|(k, v)| KeyValue::new(*k, *v)))
    }

    #[test]
    fn with_nothing_declared_the_service_is_named_by_kaas() {
        let r = compose_resource("kaas", &declared(&[]), vars(&[]));
        assert_eq!(attr(&r, "service.name").as_deref(), Some("kaas"));
        assert_eq!(attr(&r, "service.instance.id"), None);
    }

    #[test]
    fn otel_resource_attributes_reach_the_resource() {
        let env = declared(&[("deployment.environment.name", "prod")]);
        let r = compose_resource("kaas", &env, vars(&[]));
        assert_eq!(
            attr(&r, "deployment.environment.name").as_deref(),
            Some("prod")
        );
    }

    #[test]
    fn service_name_prefers_otel_service_name_then_resource_attributes() {
        let env = declared(&[("service.name", "from-attrs")]);
        let r = compose_resource("kaas", &env, vars(&[]));
        assert_eq!(attr(&r, "service.name").as_deref(), Some("from-attrs"));

        let r = compose_resource("kaas", &env, vars(&[("OTEL_SERVICE_NAME", "from-env")]));
        assert_eq!(attr(&r, "service.name").as_deref(), Some("from-env"));
    }

    #[test]
    fn the_pod_identity_outranks_otel_resource_attributes() {
        let env = declared(&[
            ("k8s.pod.name", "someone-else"),
            ("service.instance.id", "x"),
        ]);
        let r = compose_resource("kaas", &env, vars(&[("MY_POD_NAME", "kaas-1")]));
        assert_eq!(attr(&r, "k8s.pod.name").as_deref(), Some("kaas-1"));
        assert_eq!(attr(&r, "service.instance.id").as_deref(), Some("kaas-1"));
    }

    #[test]
    fn the_built_resource_carries_the_sdk_attributes() {
        let r = build_resource("kaas");
        assert_eq!(attr(&r, "telemetry.sdk.language").as_deref(), Some("rust"));
    }

    #[tokio::test]
    async fn bootstrap_without_an_endpoint_succeeds() {
        std::env::remove_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        let providers = bootstrap("kaas-test", CancellationToken::new())
            .await
            .expect("bootstrap should succeed with no OTLP endpoint");
        providers.shutdown().unwrap();
    }
}
