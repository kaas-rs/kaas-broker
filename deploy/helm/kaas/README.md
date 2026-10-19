# kaas Helm chart

Deploys the kaas broker StatefulSet and operator Deployment backed by shared
ReadWriteMany PersistentVolumeClaims — a data volume, plus optionally a
dedicated control-plane volume (`storage.controlPlane.enabled`) and one PVC
per `storage.pool[]` entry.

## Images

The chart derives the broker and operator image repositories from the resolved
tag: `ghcr.io/kaas-rs/kaas` + `kaas-operator`, with a `-preview`
suffix appended automatically when the tag is a pre-release (contains a `-`),
matching the release workflow's image-naming rule — so the chart default
points at images that actually exist for preview tags, with no override
needed.

**Explicit repositories win.** Setting `image.repository` and/or
`operator.image.repository` bypasses the derivation entirely for that image
(the empty-string defaults mean "derive from the tag"). Use this for airgapped
mirrors:

```bash
helm install my-kaas oci://ghcr.io/kaas-rs/charts/kaas \
  --set image.repository=registry.example.com/mirrors/kaas-preview \
  --set operator.image.repository=registry.example.com/mirrors/kaas-operator-preview \
  ...
```

## Prerequisites

- Kubernetes >= 1.27
- A `ReadWriteMany` StorageClass (see **StorageClass guidance** below)
- Helm >= 3.8 (for OCI chart support)

## Installation

The chart is published as an OCI artifact to GHCR. No `helm repo add` needed.

The chart bundles its CRDs under `crds/`, so Helm installs them automatically on
first install. The chart is always pushed under the name `kaas` (from
`Chart.yaml`); pre-release tags (`vX.Y.Z-*`) only rename the *images* to their
`*-preview` variants — the image helpers derive that suffix from the tag, so no
repository override is needed:

```bash
helm install my-kaas oci://ghcr.io/kaas-rs/charts/kaas \
  --version 0.3.1-preview \
  --namespace kafka --create-namespace \
  --set storage.className=ceph-filesystem \
  --set broker.replicaCount=3
```

See available versions:

```bash
helm show all oci://ghcr.io/kaas-rs/charts/kaas --version 0.3.1-preview
```

### CRDs on upgrade

Helm [deliberately does not upgrade CRDs](https://helm.sh/docs/chart_best_practices/custom_resource_definitions/)
that it installed from the chart's `crds/` directory. When a release upgrades
CRDs, apply them explicitly before `helm upgrade`:

```bash
# Pull the new chart version locally, then apply the CRDs it ships:
helm pull oci://ghcr.io/kaas-rs/charts/kaas --version 0.3.1-preview --untar
kubectl apply -f kaas/crds/

# Or apply them straight from the repo at a specific ref:
REF=v0.3.1-preview
BASE=https://raw.githubusercontent.com/kaas-rs/kaas-broker/${REF}/deploy/crds
for f in kaas.rs_kafkaclusters.yaml \
         kaas.rs_kafkatopics.yaml \
         kaas.rs_kafkausers.yaml; do
  kubectl apply -f "${BASE}/${f}"
done
```

## StorageClass guidance

kaas stores partition data on shared PVCs — the default data volume, plus any
`storage.pool[]` volumes (mounted at `/vols/<name>`) that topics are placed
on. The StorageClass must
support `ReadWriteMany` and provide NFSv4-class semantics: atomic same-directory
rename, fsync durability, and close-to-open consistency.

Single-writer enforcement comes from epoch-prefixed segment filenames + the
broker coordinator's ownership decision (see `crates/kaas-controller`), so the
StorageClass does not need to support `flock()`. Any RWX volume that meets
NFSv4-class semantics works.

| StorageClass | Status | Notes |
|---|---|---|
| **CephFS (Rook / ceph-csi)** | ✅ Production | Strong same-directory rename atomicity; recommended. |
| **csi-driver-nfs / NFSv4.1 server** | ✅ Production | Use `nconnect=4-8` and `acregmax=1` for sub-second mtime freshness on assignment.json polling. |
| **AWS EFS / Azure Files Premium NFS / GCP Filestore** | ✅ Production | All offer NFSv4-class semantics. |
| **Longhorn / OpenEBS RWX** | ✅ Production | Block-backed RWX. |
| **Local / hostPath** | ✅ Single-pod dev | Not RWX; only works with `broker.replicaCount: 1`. |

### NFS mount options

For any NFS-backed StorageClass (csi-driver-nfs, EFS, Filestore, etc.) set
`mountOptions` on the StorageClass — not on the PVC, since `mountOptions` is a
StorageClass field that the CSI driver translates into NFS mount flags at
attach time. Example:

```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: kaas-nfs
provisioner: nfs.csi.k8s.io
mountOptions:
  - nfsvers=4.1
  - acregmax=1        # sub-second mtime freshness on assignment.json polling
  - acdirmax=1        # same, for directory listings the sweeps depend on
  - hard              # block on server unavailability instead of returning EIO
parameters:
  server: nfs.example.com
  share: /export/kaas
```

The `acregmax=1` setting matters most: the broker polls assignment.json's
mtime as the fast-failover signal, inotify never fires for a write made by
another NFS client, and the default attribute cache (`acregmin=3` /
`acregmax=60`) can serve a stale mtime for up to a minute — delaying every
controller failover by that much.

**`mountOptions` are frozen into each PV at provision time.** Editing the
StorageClass changes only *newly provisioned* volumes; existing PVs keep the
options they were created with (`kubectl get pv <name> -o jsonpath='{.spec.mountOptions}'`
to check). Retrofitting a live deployment means recreating the volumes, which
on a `subDir`-templating provisioner with `reclaimPolicy: Delete` is a
data-destroying operation — plan it, don't fold it into a config edit.

#### On `nconnect`

`nconnect=N` opens N TCP connections to the server instead of one, so more
RPCs can be in flight concurrently. It does **not** reduce the latency of any
individual fsync — a common misreading, and the reason it is not in the
example above.

It only helps when the transport is actually the constraint, which is rarer
than it looks: kaas's produce path is usually bound by how long the *filer*
takes to make a write durable, not by how many requests the client can carry.
Check before reaching for it:

```bash
# on a broker pod — the xprt: line for the data mount
awk '/^device .* mounted on \/data /{f=1} f&&/^\txprt:/{print; exit}' /proc/self/mountstats
# xprt: tcp <port> <bind> <connects> <connect_time> <idle> <sends> <recvs>
#            <bad_xids> <req_u> <bklog_u> <max_slots> <sending_u> <pending_u>
```

`bklog_u` — the 10th field after `tcp` — is the cumulative count of RPCs that
waited for a free transport slot. **If it is 0, `nconnect` will buy nothing**:
the connection was never full. (`max_slots`, the next field, is the peak slot
count actually reached.) Then compare the average RTT of `WRITE` against a
metadata op like `GETATTR` in the per-op section of the same output — divide
the `rtt` column by the `ops` column. The gap between them is what the server
spends making writes durable, and no mount option shortens it.

Note that mounts to the same server share one transport by default, so every
`device` stanza for that server reports identical `xprt:` counters — reading
one is enough.

A worked example, measured on a spinning-disk NAS: `GETATTR` averaged 2.3 ms
against a 2.7 ms ICMP round trip, while a 44 KB `WRITE` averaged 19.3 ms — so
~17 ms, roughly 88% of the cost, was the filer committing to stable storage,
with `bklog_u` at 0 throughout. On that substrate `nconnect` is not the lever;
faster storage is.

## External access

The external listener uses **explicit per-broker hostnames** with a
**SAN-per-broker certificate** — the chart materialises a single
cert-manager `Certificate` whose `dnsNames` list includes
`broker-0.kafka.example.com`, `broker-1.kafka.example.com`, …, plus
the optional `bootstrapHostname`. Both choices are deliberate:

- **Per-broker hostnames, not wildcard.** Wildcard hostnames
  (`*.kafka.example.com`) would simplify DNS but require a DNS-01 ACME
  challenge — which adds an external dependency on a DNS provider that
  cert-manager can program. Explicit per-broker hostnames work with
  HTTP-01 (Gateway-fronted) or any pre-existing DNS-managed by
  whoever runs the cluster. The cost is one DNS record per broker
  pod, only changing when `broker.replicaCount` changes.
- **SAN-per-broker, not separate cert-per-broker.** Issuing one
  certificate per broker would multiply ACME issuance cost and
  rotation churn for no gain — every broker pod mounts the same
  Secret, and the in-process TLS listener picks the right SNI from
  the cert's SAN list. cert-manager rotates this single Secret
  in-place; the broker fsnotify-watches the mount and hot-reloads
  without a pod restart.

If you scale `broker.replicaCount` up at runtime, the operator
re-reconciles the `KafkaCluster` CR and updates the Certificate's
`dnsNames` to add the new SAN; cert-manager then re-issues the cert.
This is a one-time cost per scale event, not per request.

## Configuration

See `values.yaml` for the full set of tunables. Common overrides:

| Key | Default | Purpose |
|---|---|---|
| `image.repository` | `""` | Explicit broker image repo; overrides the derived default |
| `operator.image.repository` | `""` | Explicit operator image repo; overrides the derived default |
| `broker.replicaCount` | 3 | Number of broker pods |
| `storage.className` | ceph-filesystem | RWX StorageClass |
| `storage.size` | 500Gi | PVC capacity |
| `storage.controlPlane.enabled` | false | Dedicated control-plane volume (gh #221 phase 1): cluster-wide coordination state (`assignment.json`, txn slots, consumer offsets, credentials/ACLs) moves to its own PVC, mounted at `storage.controlPlane.mountPath` and exported as `KAAS_CLUSTER_DIR`. Enabling on an existing cluster is a breaking change (pre-v1 policy: fresh deploy, or manually copy `/data/__cluster/*` while scaled down). |
| `storage.controlPlane.size` | 1Gi | Control-plane PVC capacity |
| `storage.controlPlane.className` | "" | "" → same as `storage.className` |
| `storage.pool` | [] | Named pool log dirs (gh #221 phase 2): each entry `{name, size, className, accessMode, defaultEligible, cordoned, labels}` becomes its own RWX PVC mounted at `/vols/<name>` on brokers + operator and advertised via `KAAS_LOG_DIRS`. Topics bind with `KafkaTopic.spec.storage.volumes` or by label match with `spec.storage.volumeSelector` (against the entry's `labels`, gh #224); `defaultEligible: false` members only receive topics that name them explicitly; `cordoned: true` refuses new placements (drain primitive). Placement is creation-sticky — the `kaas.rs/migrate-to-volume` annotation drives an explicit data move. |
| `auth.enabled` | true | Enable credentials.json/acls.json loading |
| `auth.requireSasl` | false | Anonymous listeners require SASL too — every connection must authenticate before any non-handshake API (gh #251) |
| `auth.sslPrincipalMappingRules` | `""` | Apache `ssl.principal.mapping.rules` (KIP-371): regex rules mapping mTLS subject DNs to principals |
| `listeners[]` | plain/external/authed/oauth | Strimzi-shape listener array (gh #126): per-entry `name`, `port`, `type`, `tls`, `authentication.type`, `enabled` — see values.yaml comments. `authentication.type: oauth` (gh #42) adds `validIssuerUri`, `jwksEndpointUri`, `userNameClaim`, `fallbackUserNameClaim`, `checkAudience`+`clientId`, `jwksRefreshSeconds`, `maxSecondsWithoutReauthentication` (Strimzi field names, passed to the broker verbatim) |
| `authorization.type` | `""` | Cluster-wide authorization: `""` (off) or `simple` (ACL-based) |
| `authorization.superUsers` | `[]` | Principals that bypass ACL evaluation |
| `broker.flushIntervalMessages` | 1 | `KAAS_FLUSH_INTERVAL_MESSAGES` durability dial |
| `broker.minReadySeconds` | 60 | StatefulSet `minReadySeconds`: how long a freshly Ready broker must stay Ready before the rollout proceeds |
| `broker.retentionCheckIntervalSeconds` | 300 | Retention sweep interval (Apache `log.retention.check.interval.ms`); `0` disables the sweep |
| `broker.maxMessageBytes` | 1048588 | Apache `message.max.bytes` — broker-wide cap on one Produce batch (gh #253) |
| `broker.fsyncMaxLatencyMs` | 30000 | fsync watchdog deadline; `0` disables (gh #256) |
| `broker.controllerLease.durationSeconds` | 15 | Cluster-controller Lease lifetime; lower = faster failover, more etcd writes |
| `podDisruptionBudget.maxUnavailable` | 1 | Equivalent to Kafka min-ISR guarantee |

## Smoke test

```bash
# Port-forward to the client Service (release name + "-kaas"):
kubectl -n kafka port-forward svc/my-kaas-kaas 9092:9092 &

# Create a topic:
cat <<EOF | kubectl apply -f -
apiVersion: kaas.rs/v1alpha1
kind: KafkaTopic
metadata:
  name: test
  namespace: kafka
spec:
  partitions: 3
EOF

# Produce and consume with kcat:
echo "hello" | kcat -b localhost:9092 -t test -P
kcat -b localhost:9092 -t test -C -o beginning -e
```

## Uninstall

```bash
helm uninstall my-kaas -n kafka
```

**Note:** the PVCs are NOT deleted on uninstall (`helm.sh/resource-policy: keep`
annotation). Delete them manually if you want to reclaim the storage — the data
PVC, plus the control-plane PVC and any pool PVCs if those were enabled:

```bash
kubectl -n kafka delete pvc my-kaas-kaas-data
kubectl -n kafka get pvc   # any remaining control-plane / pool PVCs
```

**Note:** the operator uses **no cleanup finalizers** — deleting CRs never
hangs on the operator being alive. Owned resources (Certificates, Services,
TLSRoutes, credential Secrets) carry `OwnerReferences`, so Kubernetes GC
removes them with their CR; on-disk leftovers are reclaimed by the
operator's leader-elected startup sweep on its next start.
