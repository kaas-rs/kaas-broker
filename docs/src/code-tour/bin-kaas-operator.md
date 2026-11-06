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
(gh #221), `KAAS_NAMESPACE`, `KAAS_LOG_LEVEL` / `KAAS_LOG_FORMAT`, and the
OTel variables (`OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`,
`OTEL_SERVICE_VERSION`, the resource env) consumed by
[kaas-observability](kaas-observability.md)'s bootstrap. Metrics are
pushed over OTLP; the operator has no scrape endpoint (gh #265).
`HEALTH_PROBE_BIND_ADDRESS` is read but not templated: the default
`:8081` is what the chart's probes target.

Only the holder of the `kaas-operator-leader` Lease reconciles or sweeps,
and it renews the Lease for as long as it leads, with the same
`KubeLeaseElection::campaign` the broker's controller uses (gh #329). Three
things about that are deliberate:

- **A standby reads as Ready.** A rollout surges the new pod and stops the
  old one only once the new one is Ready, and the old pod holds the Lease
  until it's stopped. A standby that waited for the Lease would deadlock the
  rollout.
- **Losing the Lease is `exit(1)`, at once.** Another candidate may already be
  reconciling, so nothing drains. The restart comes back as a candidate.
- **SIGTERM keeps the Lease until the reconcilers stop.** New reconciles stop
  straight away and running ones get 20 s, so no successor starts next to a
  running reconcile. Then it releases, so the successor doesn't wait out the
  15 s.

`bins/kaas-operator/tests/leader_lease.rs` covers these on the real binary,
against a fake API server on loopback. The child's environment is cleared on
purpose, because `Client::try_default` would otherwise use whatever kubeconfig
the machine has.

Remember the architectural stance whenever tempted to grow this binary:
the operator is a startup/admission component. Brokers serve traffic while
it's down ([runtime independence](../architecture/runtime-independence.md));
anything that would put it on a request path belongs elsewhere.
