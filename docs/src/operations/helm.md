# Helm chart & listener configuration

Deploying with the chart: the Strimzi-shape listeners array, cluster-wide authorization values, and how the bundled CRDs are handled.

The chart at `deploy/helm/kaas/` is the source of truth for production
configuration — replicas, controller-Lease tuning, storage class, image
repositories. It deploys the broker `StatefulSet`, the operator
`Deployment`, and up to three classes of shared RWX PVC: the data
volume, an optional dedicated control-plane volume
(`storage.controlPlane.enabled`), and one per `storage.pool[]` entry.
Installation, image derivation, and
the smoke test live in the chart's own `deploy/helm/kaas/README.md`; this
chapter covers the concepts that need more than a values table, and the
[chart values reference](./helm-values.md) documents every key
exhaustively.

```bash
helm install my-kaas oci://ghcr.io/kaas-rs/charts/kaas-broker \
  --version 0.3.1-preview \
  --namespace kafka --create-namespace \
  --set storage.className=<your-rwx-class> \
  --set broker.replicaCount=3
```

## The listeners array

`.Values.listeners` is a Strimzi-shape array: each entry declares
`name` (free-form), `port`, `type` (`internal` / `external`), `tls`, an
`authentication.type` (`none` / `scram-sha-512` / `mtls` / `plain` /
`oauth`), and an
optional `enabled` flag (absence = enabled). The default values ship four
entries — `plain` (9092, anonymous), `external` (9093, TLS, disabled by
default), `authed` (9095, SCRAM, disabled by default), and `oauth`
(9096, internal, TLS, SASL/OAUTHBEARER, disabled by default).

The templates iterate the array to emit the StatefulSet container ports,
the `KAAS_LISTENERS` JSON env the broker parses, the Service ports, and
the NOTES.txt bootstrap output. The three axes are orthogonal — see
[Listeners, authentication, authorization](../architecture/listeners-auth.md)
for how the broker treats them. Combination constraints: `mtls`
authentication requires `tls: true`, and the broker refuses `plain` and
`oauth` (SASL PLAIN / OAUTHBEARER) over non-TLS connections at runtime —
both send reusable credentials on the wire.

Two behaviours to know before enabling an external listener:

- **Only the first `type: external` listener** drives the `KafkaCluster`
  CR plumbing (Certificates, per-broker Services, TLSRoutes) — the
  operator currently understands a single external listener.
- External listeners use **per-broker hostnames on one SAN-per-broker
  certificate** (works with HTTP-01 ACME; one DNS record per broker).
  cert-manager rotates the single Secret in place and brokers hot-reload it
  without a restart. Scaling `replicaCount` re-reconciles the Certificate's
  SAN list.

## Cluster-wide authorization

Authorization is deliberately **not** per-listener:
`.Values.authorization.type` (`""` = off, `simple` = ACL enforcement) and
`.Values.authorization.superUsers` (list of `User:foo` strings, emitted as
`KAAS_SUPER_USERS`) apply to every listener. Authentication stays
per-listener. Users, ACLs, and quotas are authored as `KafkaUser` CRs — see
[Kubernetes integration](../architecture/kubernetes.md).

## CRDs on install and upgrade

The chart bundles its CRDs in `crds/`. Helm installs them on first
install but
**deliberately never upgrades CRDs** from a chart's `crds/` directory — on
any release that changes them, apply the new CRDs explicitly *before*
`helm upgrade` (exact commands in the chart README's "CRDs on upgrade"
section).

## Other levers

- `broker.controllerLease.durationSeconds` / `renewDeadlineSeconds` /
  `retryPeriodSeconds` (defaults 15 / 10 / 2) — controller failover
  latency vs API-server write rate. They must satisfy lease > renew >
  retry; a set that doesn't is logged at boot and replaced by the
  defaults, as is any value that isn't a positive number of seconds.
- `broker.minReadySeconds` (default 60) — how long a freshly Ready broker
  must stay Ready before the rollout proceeds to the next pod.
- `broker.retentionCheckIntervalSeconds` (default 300) — the retention
  sweep interval; `0` disables the sweep.
- `broker.maxMessageBytes` (default 1048588) — Apache's
  `message.max.bytes`, the cap on one Produce batch.
- `broker.fsyncMaxLatencyMs` (default 30000) — fsync watchdog deadline;
  `0` disables.
- `auth.requireSasl` (default false) — arms the SASL gate on anonymous
  listeners too, so every connection must authenticate.
- `auth.sslPrincipalMappingRules` — Apache's
  `ssl.principal.mapping.rules` for mapping mTLS subject DNs to
  principals.
- `podDisruptionBudget.maxUnavailable` (default 1) — keeps voluntary
  disruptions from taking multiple single-writer brokers down at once.
- `storage.controlPlane.enabled` (default false) — moves cluster-wide
  coordination state onto its own PVC, so a full data volume cannot take
  the control plane down.
- `storage.pool[]` (default empty) — additional named volumes for
  per-topic placement; see the
  [volume pool](../architecture/volume-pool.md) page.
- `storage.*` — see [Storage substrate requirements](./storage.md); the
  PVCs carry `helm.sh/resource-policy: keep`, so uninstall never deletes
  data.

## Implementation notes (for contributors)

- The listeners array is the gh #126 shape, rendered by the helpers in
  the chart's `_helpers.tpl`. The `KafkaCluster` CR template still
  synthesizes the legacy single-listener shape from the first external
  entry (via `kaas.firstByType`); refactoring the operator to consume
  the array natively is open follow-up.
- The bundled CRDs are generated by `cargo xtask gen-crds`; the CI
  `rust` job fails on drift between the source types and the committed
  YAML.
