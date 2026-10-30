# kaas-operator (operator binary)

The operator entrypoint driving the reconcilers in `kaas-operator-controllers`.

A small binary by design: it boots the three reconcilers (KafkaTopic,
KafkaUser, KafkaCluster) against a namespace-scoped `kube::Client`, runs
the leader-elected orphan sweep (an initial pass at election, then every
5 minutes), serves `/healthz` + `/readyz` over axum, and shuts down
cleanly on SIGTERM.

Configuration is all environment (chart-templated by
`deploy/helm/kaas/templates/operator-deployment.yaml`): `KAAS_DATA_DIR`
(shared PVC mount, default `/data`), `KAAS_CLUSTER_DIR` and `KAAS_LOG_DIRS`
(gh #221), `KAAS_NAMESPACE`, `KAAS_LOG_LEVEL` / `KAAS_LOG_FORMAT`, the
metrics bind address and `HEALTH_PROBE_BIND_ADDRESS`, and the OTel
variables (`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, the resource env)
consumed by [kaas-observability](kaas-observability.md)'s bootstrap.

Remember the architectural stance whenever tempted to grow this binary:
the operator is a startup/admission component. Brokers serve traffic while
it's down ([runtime independence](../architecture/runtime-independence.md));
anything that would put it on a request path belongs elsewhere.
