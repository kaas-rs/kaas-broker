# KafkaCluster

`KafkaCluster` is the top-level cluster CR — but it owns much less than
a Strimzi user would expect, and that is the first thing to understand
about it. Under Strimzi, the `Kafka` CR is the whole cluster: the
operator builds the broker pods, storage, listeners, everything, from
it. In kaas the **Helm chart owns the broker workload** — StatefulSet,
volumes, listener array, environment — and the `KafkaCluster` CR
carries only the parts the operator reconciles at runtime: **external
listener plumbing** (cert-manager certificates, per-broker Services,
Gateway-API TLSRoutes) and the cluster-scoped status.

You normally never author this CR by hand. The chart templates one per
release (`deploy/helm/kaas/templates/kafkacluster.yaml`) from your
Helm values; day-2 changes flow through `helm upgrade`, not `kubectl
edit`. The reference below is for reading the object and for
understanding what the operator does with it.

## Spec

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaCluster
metadata:
  name: kaas
  namespace: kafka
spec:
  replicas: 3
  storage:
    className: nfs
    size: 50Gi
  listeners:
    internal:
      port: 9092
    external:
      enabled: true
      port: 9093
      hostnamePattern: "broker-%d.kafka.example.com"
      bootstrapHostname: "kafka.example.com"
      tls:
        certManager:
          enabled: true
          issuerRef:
            name: letsencrypt
            kind: ClusterIssuer
      gateway:
        enabled: true
        gatewayRef:
          name: public-gateway
          namespace: gateway-system
```

| Field | Meaning |
|---|---|
| `replicas` | Broker count. **Templated from `.Values.broker.replicaCount` — never hand-edit**; the reconciler reads it (to know how many per-broker Services/routes to build) but the StatefulSet's replica count is the chart's. |
| `storage.className` / `storage.size` | Informational mirror of the chart's storage values. |
| `listeners.internal.port` | In-cluster client port. Defaults to `9092` (apiserver-defaulted). |
| `listeners.external.enabled` | Master switch for the external plumbing below. `false` (the default) means no Certificates, Services, or TLSRoutes are created. |
| `listeners.external.port` | Advertised external port, default `9093`. |
| `listeners.external.hostnamePattern` | printf-style pattern with `%d` for the broker ordinal, e.g. `broker-%d.kafka.example.com`. Every broker needs its own routable hostname because Kafka clients bootstrap once, then connect to each broker directly at its advertised address. |
| `listeners.external.bootstrapHostname` | Optional convenience hostname added to the certificate SANs (so a single bootstrap address presents a valid certificate). Not required for operation. |
| `listeners.external.tls.certManager` | When `enabled`, the operator creates one cert-manager `Certificate` covering the per-broker hostnames, issued by `issuerRef` (`kind`: `ClusterIssuer` or `Issuer`; empty defaults to `ClusterIssuer` at reconcile time). |
| `listeners.external.gateway` | When `enabled`, the operator creates one Gateway-API `TLSRoute` per broker attached to `gatewayRef`, SNI-routing each hostname to that broker's Service. The route is created at whichever version the API server serves, newest first (`v1`, `v1alpha3`, `v1alpha2`); if it serves none, the `Ready` condition says so. |
| `listeners.external.service.annotations` | Declared for extra annotations on the per-broker Services (cloud load-balancer knobs) — **not yet applied**: the reconciler currently ignores this field. |

Everything the operator creates from this CR carries an
`OwnerReference` back to it, so deleting the `KafkaCluster` lets
Kubernetes garbage-collect the Certificates, Services, and TLSRoutes
with no operator involvement.

Note the asymmetry with the chart's listener model: the chart supports
an arbitrary **array** of listeners (see [Helm chart & listener
configuration](../helm.md)), while this CR still models the legacy
single internal/external pair. The chart bridges the two by
synthesizing this shape from the first listener of each type;
refactoring the operator to consume the array natively is planned.

## Status

| Field | Meaning |
|---|---|
| `bootstrapServers` | The resolved bootstrap addresses for the cluster, one list entry per reachable path. |
| `conditions` | `Ready` reflects the last reconcile of the external plumbing. |

`kubectl get kafkaclusters` prints `Replicas`, `External` (whether the
external listener is enabled), and `Ready`.

## What it does *not* do

- It does not create or scale the broker pods — Helm does.
- It does not define which listeners exist or their authentication —
  that is the chart's listener array, delivered to brokers by
  environment variable.
- Deleting it does not delete topic data; it tears down the external
  access path only.

## Implementation notes (for contributors)

- Type: `crates/kaas-operator-api/src/kafkacluster.rs`; generated
  schema `deploy/crds/kaas.rs_kafkaclusters.yaml`.
- Reconciler: `crates/kaas-operator-controllers/` (KafkaCluster
  reconciler, 300 s requeue).
- The chart→CR template is
  `deploy/helm/kaas/templates/kafkacluster.yaml`, using the
  `kaas.firstByType` helper to collapse the listener array into the
  legacy single-listener shape. Consuming the array natively is open
  follow-up.
