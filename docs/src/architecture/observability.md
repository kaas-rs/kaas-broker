# Observability

OTLP metrics pushed to Prometheus, structured logs, and the `/healthz` endpoint's rich runtime state.

Apache Kafka exposes its metrics as JMX MBeans, and most deployments
bolt a Prometheus JMX exporter onto every broker to scrape them. kaas
pushes its metrics over OTLP instead: brokers send them straight to
Prometheus's native OTLP receiver, with no sidecar and no exporter
agent. kaas exports no traces; see [Logs and traces](#logs-and-traces).

## Bootstrap: push-mode OTLP

The OTel SDK is wired from environment variables (all emitted by the
Helm chart):

- `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` — metrics are **pushed** via
  OTLP/HTTP (http/protobuf) to Prometheus's native OTLP receiver
  (`/api/v1/otlp/v1/metrics`). Prometheus speaks only http/protobuf on
  that path — exporting gRPC just gets an h2 GoAway.
- `KAAS_METRIC_EXPORT_INTERVAL` — a duration string (`30s`, `1m`);
  default `30s`. The SDK default of 60 s made `rate([1m])` panels see a
  single sample and gap.
- `OTEL_SERVICE_VERSION`, `MY_POD_NAME`, `KAAS_NAMESPACE` — folded into
  resource attributes so dashboards can filter by broker and namespace.
- `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` — the standard
  resource variables, honoured alongside the SDK's `telemetry.sdk.*`
  attributes. `service.name` is `OTEL_SERVICE_NAME`, else a
  `service.name` in `OTEL_RESOURCE_ATTRIBUTES`, else `kaas` (or
  `kaas-operator`). For everything else kaas sets, kaas wins: the pod
  identity above comes from the downward API and is what per-broker
  dashboards key on, so a conflicting `k8s.pod.name` or
  `service.instance.id` in `OTEL_RESOURCE_ATTRIBUTES` is overridden.

With no endpoint set the SDK still runs and exports go nowhere —
safe for tests and local dev. Before bootstrap runs, the global metrics
handle is a no-op registry, so pre-boot code and tests can record
without nil checks.

One metrics-pipeline invariant is load-bearing enough to repeat here:
gauge callbacks (high watermark, log start offset) read the storage
engine through a lock-free snapshot, never the partition mutex. Before
that split, a stuck NFS fsync holding the mutex stalled the OTel gauge
callback and *all* broker metrics vanished from Prometheus until the
stall cleared — exactly when you needed them most.

## `/healthz` and `/readyz`

Both are served on port 8080:

- **`/readyz`** is the kubelet readiness probe, and it is honest:
  listeners bound, the main runtime alive, and — in cluster mode —
  every partition the assignment gives this broker open in the storage
  engine, i.e. takeover complete. It is served from a dedicated thread
  and runtime, never the main runtime it reports on, so a wedged broker
  answers *unready* instead of hanging the probe. The full story — the
  two signals, and how readiness paces a rolling update — is
  [Honest readiness & rollout pacing](./readiness-rollout.md).
- **`/healthz`** returns a JSON runtime view: controller identity
  (`is_controller`, `controller_id`, `controller_epoch`), heartbeat
  health (`heartbeat_rtt_ms`, `heartbeat_age_ms`), assignment state
  (`assignment_version`, `assignment_age_ms`), partition counts
  (`partitions_led`, `partitions_assigned`, `partitions_recovering`),
  and `storage_stalled` — true when any partition's most recent
  committer fsync tripped the stall watchdog, surfacing "storage
  backend wedged" before the broker accumulates enough queued appenders
  to look outwardly idle.

Fields with no measurement yet return `-1` internally and render as
JSON `null`, so dashboards show "not measured" rather than a misleading
zero. In local-dev mode (no cluster runtime) the whole runtime view is
absent and the handler serves zero-valued fields — the right answer
when no controller, coordinator, or heartbeat client is running.

`partitions_led` sources from the same `assignment.json`-backed view of
leadership as the Produce/Fetch ownership check — `/healthz` never
invents its own answer to "who leads what".

## Byte-opacity tripwires

The broker's load-bearing invariant is *"kaas is a byte mover, not a
byte interpreter"*: no code path decodes individual records or
re-encodes a `RecordBatch` (see
[Storage engine hot path](./storage-hot-path.md)). Two counters give
that invariant teeth — `codec_record_decode` and
`codec_batch_reencode` — which **must stay at zero in steady state**.
Every increment names the offending call site in a `site` attribute,
and the `KaasByteOpacityViolated` alert fires on non-zero.

As of today no kaas code path calls these functions; the counters exist
so the *first* violation is an alert, not an archaeology project.

## Logs and traces

One log subscriber is installed at boot: a filter honouring
`KAAS_LOG_LEVEL` (the broker also honours `RUST_LOG`, which wins) and a
formatter honouring `KAAS_LOG_FORMAT` (`json` or text). Logs go to
stdout, for whatever node log collector the cluster runs.

kaas exports no traces. Earlier releases wired an OTLP/gRPC span
exporter and a sampler, but no code path ever opened a span, so they
sent nothing; the pipeline and the chart's `observability.otlp.traces.*`
values were removed (gh #269). Log lines carry no `trace_id` or
`span_id` for the same reason. If request spans are added later, the
OTLP exporter is backend-neutral, so they would not tie kaas to any one
tracing backend.

## Implementation notes (for contributors)

- Everything lives in `crates/kaas-observability`: `bootstrap.rs`
  (OTel SDK bring-up), `health.rs` (the axum `/healthz`/`/readyz`
  surface; the JSON view is the `RuntimeState` trait), `byteopacity.rs`
  (tripwire counters), `tracing.rs` (`install_tracing`, the
  `tracing-subscriber` log stack).
- Codec-side tripwire counterparts:
  `crates/kaas-codec/src/tripwires.rs`; CI assertion:
  `bins/kaas/tests/byte_opacity.rs`.
- Two standing rules: pick `KAAS_METRIC_EXPORT_INTERVAL` with the
  Prometheus `rate()` window in mind, and gauge callbacks must read
  the engine's `ArcSwap` snapshot, never the partition mutex.
- gh #95 — the fsync stall watchdog behind `storage_stalled`;
  gh #208/#211 — the honest, serving-gated `/readyz` (see
  [readiness & rollout](./readiness-rollout.md)).
