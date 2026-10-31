# Chart values reference

Every key in `deploy/helm/kaas/values.yaml`, what it does, and where it
lands. The [Helm chart chapter](./helm.md) tells the narrative story
(listeners, upgrades, CRDs); this page is the exhaustive reference.
Defaults shown are the chart's.

Two Helm behaviors worth knowing before overriding anything:

- Helm **deep-merges** values, so restating a default changes nothing —
  and *removing* a chart default requires an explicit `null`, not
  omission.

## `image` and `operator.image`

| Key | Default | Meaning |
|---|---|---|
| `image.repository` | `""` | Broker image. Empty derives `ghcr.io/kaas-rs/kaas-broker`, with a `-preview` suffix appended automatically when the resolved tag is a pre-release (contains `-`) — the same naming rule the release workflow uses. An explicit value overrides the derivation. |
| `image.tag` | `""` | Defaults to the chart's `appVersion`. |
| `image.pullPolicy` | `IfNotPresent` | |
| `operator.image.*` | `""` / `""` / `IfNotPresent` | Same derivation for `ghcr.io/kaas-rs/kaas-broker-operator[-preview]`. |

## `operator`

| Key | Default | Meaning |
|---|---|---|
| `operator.enabled` | `true` | Deploy the operator (single replica, leader-elected). Without it, CRs are never reconciled — see [runtime independence](../architecture/runtime-independence.md) for what keeps working. |
| `operator.resources` | 100m/128Mi – 500m/256Mi | Requests/limits. |
| `operator.podSecurityContext` | non-root 65532, group 0, `fsGroup: 0`, `OnRootMismatch` | Mirrors the broker's shared-volume permission scheme (below) — the operator writes the same volume at reconcile time. There is deliberately no init container here: the reconcile loop retries until the broker's permission floor has run. |

## `broker`

| Key | Default | Meaning |
|---|---|---|
| `replicaCount` | `3` | Brokers in the StatefulSet. Multi-broker requires RWX storage — see [storage requirements](./storage.md). |
| `clusterID` | `kaas-local` | The cluster id reported to clients. |
| `minReadySeconds` | `60` | Rolling-update pacing: a pod must hold Ready this long before the next one is replaced. Belt-and-braces on top of [honest readiness](../architecture/readiness-rollout.md); raise it on slow storage where takeover recovery scans run long. |
| `ports.health` | `8080` | Health/readiness HTTP (fixed). |
| `ports.heartbeat` | `9094` | Inter-broker heartbeat gRPC (fixed, in-cluster only). |
| `resources` | 500m/1Gi – 2/4Gi | Requests/limits. For benchmarking, note the memory limit also caps page cache under cgroup v2 — a tight limit costs cold-read throughput. |
| `readinessGate.enabled` | `true` | Adds the `kaas.rs/PartitionsReady` pod readiness gate: a pod joins Service endpoints only once its partition directories exist. Disable only for bare-bones smoke tests. |

### `broker.podSecurityContext`

Default: non-root UID 65532, group 0, `fsGroup: 0`,
`fsGroupChangePolicy: OnRootMismatch`. Two independent mechanisms both
make the shared volume writable: on CSI drivers that honour `fsGroup`,
the kubelet chowns the volume to group 0 (the broker runs 65532:0, the
Strimzi "primary GID 0" convention); on shared NFS — where most CSI
drivers skip that chown — the broker's `partition-init` init container
is the floor, chowning the volume itself before the broker starts.

### `broker.controllerLease`

| Key | Default |
|---|---|
| `durationSeconds` | `15` |
| `renewDeadlineSeconds` | `10` |
| `retryPeriodSeconds` | `2` |

The Kubernetes Lease behind [controller election](../architecture/controller.md).
Tighter = faster failover, more API-server writes. On an API server with
high tail latency (small shared nodes), widen the ratios — a lease that
flaps during pod churn moves the controller for no reason.

### Durability, retention, limits

| Key | Default | Kafka equivalent | Meaning |
|---|---|---|---|
| `flushIntervalMessages` | `1` | `log.flush.interval.messages` | The durability dial. `1` = fsync every record (honest `acks=all`; kaas has no replication, so fsync is the *only* durability mechanism). `N` = up to N−1 records lost per partition on crash. `0` = fsync only at segment roll. Overridable per topic via `flush.messages`. See [performance](./performance.md) for what this costs. |
| `retentionCheckIntervalSeconds` | `300` | `log.retention.check.interval.ms` | Retention sweep cadence — retention **is enforced** (7-day default for unconfigured topics). `0` disables the sweep entirely. Leader-gated; the active segment is never reclaimed. |
| `maxMessageBytes` | `1048588` | `message.max.bytes` | Cap on one Produce batch (Apache's default: 1 MiB + header overhead). Oversized batches get `MESSAGE_TOO_LARGE`. Raise together with consumer `fetch.max.bytes`. |
| `fsyncMaxLatencyMs` | `30000` | — | Fsync watchdog — the stuck-storage tripwire. Every acked produce fsyncs (at the default flush interval), and a hung NFS server makes that fsync block *forever*, not fail: appenders queue behind the partition lock and producers time out with no error naming the cause. Past this deadline the append instead fails with a **retriable** error and the broker's health state reports storage as stalled, so clients retry and `/healthz` points at the substrate. Apache has no equivalent because ISR failover covers a broker with a hung disk; kaas has no replicas, so failing fast is the fallback. On healthy storage it never fires (fsyncs run in the tens of milliseconds). `0` disables it, for substrates whose legitimate worst-case fsync latency exceeds any sane deadline. |
| `txnState.numSlots` | `50` | `transaction.state.log.num.partitions` | Transaction-state slot-file count, cluster-wide. Changing it on a live cluster re-shards slot ownership; drain transactional producers first. |
| `autoCreateTopics.enabled` | `true` | `auto.create.topics.enable` | Metadata requests for unknown topics mint a `KafkaTopic` CR and answer `LEADER_NOT_AVAILABLE` until the operator materializes it — what Kafka Streams' `.to(sink)` relies on. |
| `autoCreateTopics.numPartitions` | `1` | `num.partitions` | Partition count for auto-created topics only. |

## `storage`

| Key | Default | Meaning |
|---|---|---|
| `className` | `ceph-filesystem` | StorageClass for the data volume. Multi-broker needs RWX with NFSv4-class semantics — see [storage requirements](./storage.md) for the provider matrix. |
| `size` | `500Gi` | |
| `accessMode` | `ReadWriteMany` | `ReadWriteOnce` + a local-path class is fine for single-broker. |
| `mountPath` | `/data` | |

All PVCs the chart renders carry `helm.sh/resource-policy: keep` — they
survive `helm uninstall` and must be deleted explicitly.

### `storage.controlPlane`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Move cluster-wide coordination state (assignment, transaction slots, offsets, credentials, queues) onto its own small volume, so a runaway topic filling the data volume degrades into per-topic produce errors instead of taking the control plane down. |
| `className` | `""` (= `storage.className`) | |
| `size` / `accessMode` / `mountPath` | `1Gi` / `ReadWriteMany` / `/cluster` | |

**Enabling this on an existing cluster is a breaking change** (pre-v1
policy): redeploy fresh, or copy `__cluster/*` onto the new volume with
brokers and operator scaled down.

### `storage.pool[]`

Default `[]`. Named additional RWX volumes — "log dirs" in Kafka's
KIP-113 vocabulary — mounted on every broker at `/vols/<name>` and
selectable per topic. Each entry:

| Field | Meaning |
|---|---|
| `name` | Log-dir name topics bind to via `KafkaTopic.spec.storage.volumes`. |
| `size`, `className`, `accessMode` | PVC shape; empty `className` inherits `storage.className`. |
| `defaultEligible` | `true` = receives topics that don't name volumes; `false` = reserved for explicit binding. |
| `cordoned` | `true` = no *new* placements (the decommission drain primitive). |
| `labels` | Matched by `KafkaTopic.spec.storage.volumeSelector`. |

The full model — placement stickiness, selectors, explicit migration —
is [the volume pool chapter](../architecture/volume-pool.md).

## `auth`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `true` | `false` swaps in an allow-all engine: no authentication anywhere, every connection is `User:ANONYMOUS`. |
| `requireSasl` | `false` | `true` arms the SASL pre-auth gate on *anonymous* listeners too — closes the "connect to the plain listener and skip auth" hole cluster-wide. `auth.enabled: false` outranks it. |
| `sslPrincipalMappingRules` | `""` | Apache's `ssl.principal.mapping.rules`, verbatim — regex rules mapping an mTLS client cert's subject DN to a principal. Empty = use the CN. Parse errors fail startup deliberately, so a typo crash-loops instead of silently mapping every cert to its CN. |

Authentication/authorization architecture, including what each listener
`authentication.type` means, is
[Listeners, authentication, authorization](../architecture/listeners-auth.md).

## `admin.argocd`

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Stamp ArgoCD coexistence annotations onto CRs the *broker* creates at runtime (admin-protocol topic creation), so they render in the Application tree instead of being pruned as drift. |
| `applicationName` | `""` (= release name) | The Application the tracking-id claims. |
| `compareOptions` | `IgnoreExtraneous` | Passed through verbatim; `""` skips the annotation so runtime topics surface as deliberate drift. |
| `syncOptions` | `Delete=false` | Default means runtime-created topics survive an Application delete. `""` restores cascade-delete; `Prune=false,Delete=false` surfaces drift *and* survives deletes. |

Details and the reasoning: [Kubernetes integration](../architecture/kubernetes.md).

## Scheduling and identity

| Key | Default | Meaning |
|---|---|---|
| `podDisruptionBudget.enabled` / `maxUnavailable` | `true` / `1` | Applies to node drains and other eviction-API disruptions. Note it is inert for StatefulSet rollouts (those delete pods directly); rollout pacing is `broker.minReadySeconds`. |
| `serviceAccount.broker.create` / `.name` | `true` / `""` | |
| `serviceAccount.operator.create` / `.name` | `true` / `""` | |
| `clusterDomain` | `cluster.local` | DNS suffix for the per-broker FQDNs advertised on internal listeners. Override only on clusters with a non-default CoreDNS domain. |

## `observability`

| Key | Default | Meaning |
|---|---|---|
| `otlp.metrics.enabled` | `false` | Push metrics (OTLP/HTTP) to Prometheus's native OTLP receiver (`--web.enable-otlp-receiver`). |
| `otlp.metrics.endpoint` | `http://prometheus.observability...:9090/api/v1/otlp/v1/metrics` | Path must end in `/v1/metrics`. |
| `otlp.metrics.exportInterval` | `"30s"` | Push cadence. The SDK's 60 s default leaves Grafana `rate([1m])` panels with one sample per window; 30 s guarantees two. Duration strings accepted. |
| `logs.level` / `logs.format` | `info` / `json` | `debug…error`; `json` or `text`. |
| `alerts.enabled` | `false` | Render a PrometheusRule with the load-bearing kaas alerts (self-fence, stale assignments…). Needs Prometheus Operator. |
| `alerts.additionalLabels` | `{}` | Merged into every rule (Alertmanager routing). |
| `alerts.thresholds.*` | see values.yaml | Per-alert overrides. |

The wider observability story: [Observability](../architecture/observability.md).

## `listeners[]`

The Strimzi-shape listener array — each entry is one TCP listener on
every broker, described by three orthogonal axes (`type`, `tls`,
`authentication.type`). The [Helm chapter](./helm.md) and
[listeners architecture page](../architecture/listeners-auth.md) cover
the model; this is the field reference. Defaults ship four entries:
`plain` (9092, anonymous), `external` (9093, TLS, disabled), `authed`
(9095, SCRAM, disabled), `oauth` (9096, TLS + OAUTHBEARER, disabled).

| Field | Meaning |
|---|---|
| `name` | Free-form, unique; keys the per-listener auth engine and appears in Metadata advertisement. Duplicate names or ports fail at boot. |
| `enabled` | Absent = enabled. |
| `port` | Unique per entry. |
| `type` | `internal` (headless-Service DNS) or `external` (per-broker plumbing below). |
| `tls` | Independent of authentication; `tls: true` + `type: none` is opportunistic TLS. |
| `authentication.type` | `none` / `scram-sha-512` / `plain` / `mtls` / `oauth`. `mtls` requires `tls: true`; SASL PLAIN and OAUTHBEARER are *refused* over non-TLS connections at runtime (they carry reusable credentials). Anonymous listeners skip ACL evaluation entirely unless `auth.requireSasl` arms them. |
| `clientCA.existingSecret` / `key` | Internal TLS listeners only (default key `ca.crt`): the Secret holding the CA that signs client certificates. Only *this* listener then requires a client certificate — SCRAM or OAuth listeners on the same broker keep accepting certificate-less clients, the way Apache Kafka's per-listener `listener.name.<name>.ssl.client.auth` works. Required for an internal `mtls` listener (the template fails without it); the external listener uses its own `clientCA.enabled` block below. |

### `external`-type extras

| Field | Default | Meaning |
|---|---|---|
| `hostnamePattern` | `broker-%d.kafka.example.com` | Per-broker FQDN pattern (`%d` = ordinal), used for certificate SANs and routes. **Honesty note:** the pattern does not currently reach Metadata advertisement — external listeners still advertise the in-cluster FQDN, so external access effectively requires SNI routing that terminates on those internal names. Tracked as a known gap. |
| `bootstrapHostname` | `""` | Optional single bootstrap CNAME, added to certificate SANs. |
| `certManager.enabled` / `issuerRef` | `true` / `letsencrypt-prod` (`ClusterIssuer`) | One cert-manager Certificate covering all per-broker hostnames. |
| `clientCA.enabled` / `existingSecret` / `key` | `false` / `""` / `ca.crt` | Require client certs signed by this CA; pair with `authentication.type: mtls`. |
| `gateway.enabled` / `gatewayRef` | `true` / `kaas-gateway` in `kafka` | One Gateway-API TLSRoute per broker (TLS passthrough). With `false`, the per-broker Services remain and can be fronted by LoadBalancers instead. |
| `service.annotations` | `{}` | Extra annotations for the per-broker Services — **not yet applied** by the operator (declared, ignored; tracked as a known gap). |

Only the **first** `type: external` entry drives the operator's
Certificate/Service/TLSRoute reconciliation today.

### `oauth` authentication fields

Strimzi's `KafkaListenerAuthenticationOAuth` field names, verbatim:

| Field | Meaning |
|---|---|
| `validIssuerUri` | Exact-match `iss` claim. |
| `jwksEndpointUri` | Where signing keys are fetched — every `jwksRefreshSeconds` (default 300) and early on an unknown key id. Fail-closed before the first fetch. |
| `userNameClaim` | Claim that becomes the principal (`User:<value>`); default `sub`. |
| `fallbackUserNameClaim` | Tried when `userNameClaim` is absent. |
| `checkAudience` / `clientId` | When `true`, the token's `aud` must contain `clientId`. Off by default. |
| `maxSecondsWithoutReauthentication` | KIP-368: advertise `session_lifetime_ms = min(this, token remaining lifetime)` and refuse requests past the deadline until re-authentication. Unset = sessions outlive their token. |

## `authorization`

| Key | Default | Meaning |
|---|---|---|
| `type` | `""` | `""` = no authorization (Strimzi's "missing = no restrictions"). `simple` = ACL enforcement from the operator-managed ACL file. Cluster-wide — authentication stays per-listener, and quotas fire regardless of this setting (orthogonal axes). |
| `superUsers` | `[]` | Principals that bypass ACL evaluation (early-allow). Matched verbatim: bare names for SCRAM/OAuth principals, `CN=…` for mTLS subjects. |

## Implementation notes (for contributors)

- Templates consuming these values live in `deploy/helm/kaas/templates/`;
  listener JSON assembly is the `kaas.listenersJSON` helper in
  `deploy/helm/kaas/templates/_helpers.tpl`, landing as the
  `KAAS_LISTENERS` env (gh #126). Env-var names for the broker knobs are
  parsed in `crates/kaas-broker/src/cli.rs`.
- The external advertised-hostname gap is gh #263; the unapplied
  `service.annotations` is part of gh #266.
- This page documents chart defaults, not any specific deployment.
  When a key moves or dies, update this page in the same commit — the
  drift gates don't cover values keys.
