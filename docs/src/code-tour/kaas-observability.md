# kaas-observability

The OTLP metrics bootstrap, the log subscriber, and the `/healthz` HTTP handler.

The content of this crate — what gets exported where, the `/healthz`
runtime view, why gauges read through lock-free snapshots — is covered in
the [Observability architecture chapter](../architecture/observability.md);
this page is the code map.

**Module map**: `bootstrap.rs` (OTel SDK bring-up from
`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` and the resource env; metrics push
OTLP/HTTP http/protobuf — the only dialect Prometheus's native OTLP
receiver speaks; no traces, gh #269),
`metrics.rs` (the Arc-shared `Metrics` registry — `global()` returns a
no-op registry before bootstrap, so pre-boot code and tests never
nil-check), `gauges.rs` (the `GaugeSource` seam the broker feeds partition
gauges through), `health.rs` (axum `/healthz` + `/readyz`; the
`RuntimeState` trait), `otlp_push_observer.rs` (a `PushMetricExporter` wrapper that makes OTLP
push failures alertable, gh #121), `k8s_api.rs` (K8s API call metrics),
`topic_traffic.rs`, `tracing.rs`
(`tracing-subscriber` log filter and formatter).

**Invariant callers must hold**: gauge callbacks and `RuntimeState`
implementations must never take hot-path locks — the gh #134 outage
(a stuck NFS fsync starving the metrics pipeline) is why the storage
engine exposes lock-free snapshots for exactly these readers.

**Start reading at** `health.rs` (the `RuntimeState` trait is a compact
inventory of what the broker considers its own vital signs).
