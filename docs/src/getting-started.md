# Getting Started

Deploy kaas onto a Kubernetes cluster with the Helm chart, or run a
single broker locally in dev mode with in-memory storage.

## Deploy on Kubernetes

Prerequisites: Kubernetes ≥ 1.27, Helm ≥ 3.8, and — for more than one
broker — a `ReadWriteMany` StorageClass with NFSv4-class semantics (what
"NFSv4-class" means precisely, and which providers qualify, is
[Storage substrate requirements](operations/storage.md)). A single-broker
cluster can run on a plain `ReadWriteOnce` local-path class.

```bash
helm install my-kaas oci://ghcr.io/kaas-rs/charts/kaas-broker \
  --version 0.3.1-preview \
  --namespace kafka --create-namespace \
  --set storage.className=<your-rwx-class> \
  --set broker.replicaCount=3
```

That deploys a broker `StatefulSet`, the operator, and the shared
volume(s). The chart's defaults give you an anonymous in-cluster
listener on 9092; TLS, SCRAM, OAuth (SASL/OAUTHBEARER against an OIDC
issuer, disabled by default on 9096), and external access are chart
values — see
[Helm chart & listener configuration](operations/helm.md).

### Create a topic

Topics are Kubernetes resources:

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaTopic
metadata:
  name: test
  namespace: kafka
spec:
  partitions: 3
```

If you'd rather stay in Kafka's own tooling, that works too —
`kafka-topics.sh --create` (or any Admin-API client) creates the same
`KafkaTopic` resource for you, so both routes end in one place and
`kubectl get kafkatopics` always shows the truth.

Retention is enforced: a topic without an explicit retention config
ages out after 7 days (Kafka's own default) — set `retentionMs: -1` to
keep data forever.

### Talk to it

Any Kafka client, unchanged:

```bash
kubectl -n kafka port-forward svc/my-kaas-kaas 9092:9092 &
echo "hello" | kcat -b localhost:9092 -t test -P
kcat -b localhost:9092 -t test -C -o beginning -e
```

From here, the [system overview](architecture/overview.md) explains
what you just deployed — which pod does what, and where your bytes
actually went.

## Run locally in dev mode

The broker binary detects dev mode by the absence of the `MY_POD_NAME`
env var (which the StatefulSet always sets): storage flips to
in-memory, no Kubernetes API is needed, and the broker treats itself as
leader of every partition.

```bash
cargo run -p kaas
```

Point any Kafka client at `localhost:9092`. Nothing is persisted and
nothing is cluster-aware — it's a protocol-correct scratchpad for
client development and codec work, not a single-node production mode.

## Build the book and the code

```bash
cargo build --workspace          # toolchain is pinned; rustup auto-installs
cargo test  --workspace --all-features
cargo xtask docs --serve         # this book, live-reloading
```
