# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Layout

System-level architecture lives in the documentation book's Part I (`docs/src/architecture/`; build with `cargo xtask docs`, preview with `cargo xtask docs --serve`). The release line is `v0.3.x-preview` (see [`docs/RELEASING.md`](./docs/RELEASING.md); `v0.3.0-preview` added SASL/OAUTHBEARER, minor-bumped from the `v0.2.x` patch line); how the book is built, gated, and published is [`docs/README.md`](./docs/README.md). `docs-publish.yml` deploys the book to this repo's own Pages site at `kaas-rs.github.io/kaas-broker/`. The landing page at `kaas.rs` is an independent site in `kaas-rs/kaas-landing-page` that links here by absolute URL — the two share no build, and re-coupling them (one domain serving both) would require a credentialed cross-repo handoff, which was tried and removed. The only cross-repo contract is that the landing page deep-links `index.html` + `getting-started.html`; the `docs` CI job guards those.

## Where the rest lives

Invariants that belong to one area live next to its code. **Before reading or changing code in an area below, `Read` its file.** A nested `CLAUDE.md` loads automatically only on Read, Edit and Write, never when a file is read through Bash. Add a new gotcha to the file for its area, not here.

The notes record what's easy to get wrong; the book explains the design to a reader who knows Kafka. Book pages are linked by path rather than `@` imported, so a page costs context only when opened (the exception is `code-tour/topic-lifecycle.md`, which the two crates at the centre of that path import).

| Area | Read first | Book (under `docs/src/`) |
|---|---|---|
| Segments, recovery, fsync, takeover epochs, retention, on-disk layout, volume pool | `crates/kaas-storage/CLAUDE.md` | `architecture/storage-hot-path.md`, `architecture/file-handles.md`, `architecture/volume-pool.md`, `architecture/nfs-substrate.md` |
| Idempotent producer, transactions, group routing, including their handlers in `crates/kaas-broker/src/handlers/` | `crates/kaas-coordinator/CLAUDE.md` | `architecture/transactions.md`, `architecture/consumer-groups.md`, `compat/api/transactions.md`, `compat/api/consumer-groups.md` |
| Controller lease, balancer, broker fencing, `crates/kaas-k8s/src/endpoints.rs` | `crates/kaas-controller/CLAUDE.md` | `architecture/controller.md`, `architecture/broker-fencing.md` |
| Topic delete and recreate, directory reclaim, the topic watch, the offset purge | `docs/src/code-tour/topic-lifecycle.md` | `architecture/file-handles.md`, `architecture/nfs-substrate.md`, `architecture/consumer-groups.md` |
| Listeners, SASL and mTLS, ACLs, quotas, including `crates/kaas-protocol` | `crates/kaas-auth/CLAUDE.md` | `architecture/listeners-auth.md`, `compat/api/auth.md`, `compat/api/acls-quotas.md` |
| Admin handlers and broker RBAC, TopicID, fetch sessions | `crates/kaas-broker/CLAUDE.md` | `architecture/runtime-independence.md`, `architecture/kubernetes.md`, `compat/api-reference.md` |
| CRD types, operator reconcilers, the operator's leader lease, `bins/kaas-operator` | `crates/kaas-operator-controllers/CLAUDE.md` | `architecture/kubernetes.md`, `operations/crds.md`, `code-tour/bin-kaas-operator.md` |
| SIGTERM drain, `/readyz` | `bins/kaas/CLAUDE.md` | `architecture/readiness-rollout.md`, `architecture/file-handles.md` |
| Helm chart | `deploy/CLAUDE.md` | `operations/helm.md`, `operations/helm-values.md`, `operations/storage.md` |
| `scripts/kafka-*.sh` | `scripts/CLAUDE.md` | `compat/verification.md` |
| Wire codec | `crates/kaas-codec/CLAUDE.md` | `compat/wire-protocol.md`, `compat/api-matrix.md` |
| Metrics and the wire-up audit | `crates/kaas-observability/CLAUDE.md` | `architecture/observability.md` |

## Keeping the docs up to date

The book in `docs/src/` is the documentation; the `CLAUDE.md` files point to it and hold the warnings for whoever changes the code. Nothing keeps them in step automatically: the drift gate catches moved files and changed API keys, never prose that has gone wrong. So treat the docs as part of the change:

- **Before changing an area**, read its book page from the table above. If the page and the code disagree, the code wins and the page gets fixed.
- **When a change alters behaviour**, update the page in the same commit: wire behaviour and error codes, defaults, env vars and chart values, on-disk files, ordering rules, anything a user or a later contributor would rely on. A change that only moves code needs nothing.
- **When a change opens or closes a known gap**, update the KIP page and `compat/kip-index.md`, and fix any "tracked as gh #N" that no longer holds.
- **Before committing a doc change**, run `cargo xtask check-docs-drift` and `cargo xtask docs`. The writing conventions are in `docs/README.md`.

## Parity target & non-goals

kaas targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams parity. Behaviour is verified against the matrix tracked in the [kaas-migration-parity](https://github.com/users/Woestebanaan/projects/2) GitHub project. When a feature is ambiguous, default to "match Apache Kafka 3.7" rather than inventing kaas-specific semantics.

- **Deliberate post-3.7 exception:** DescribeCluster **v2** (KIP-1073,
  `IncludeFencedBrokers` / `IsFenced`) is served, because kaas has a real
  fenced state (gh #249) and no other way to report it. This is the only
  such exception; adding another needs the same explicit justification.
- **Deferred (intended later, not now):** tiered storage / S3 backend. Skip the tiered-storage-only API surfaces (e.g. `EARLIEST_LOCAL_TIMESTAMP`, `EARLIEST_PENDING_UPLOAD_OFFSET`) — clients only request them when configured for remote tiers.
- **Non-goals (off the table by design):** KRaft / metadata quorum (kaas uses K8s Leases instead), replication / ISR (single-writer-per-partition), and a literal `__transaction_state` internal topic (kaas uses per-broker JSON slot files on the shared NFS PVC — see `crates/kaas-coordinator/CLAUDE.md`; gh #29). Don't add any of these; flag if a "parity" task implicitly requires them.

## Common commands

```bash
cargo xtask ci                                           # fmt + clippy + test + release build, all in one
cargo xtask gen-proto                                    # force-rebuild kaas-broker so tonic-build re-runs
cargo xtask gen-crds                                     # regenerate deploy/crds/ + deploy/helm/kaas/crds/
cargo xtask check-crd-drift                              # CI gate: gen-crds then git diff --exit-code
cargo xtask gen-api-matrix                               # regenerate docs/src/compat/api-matrix.md from the codec registry
cargo xtask check-docs-drift                             # CI gate: gen-api-matrix + git diff + book source-path scan
cargo xtask check-chart [--values FILE]                  # CI gate: render the chart and parse every document; helm template and helm lint don't
cargo xtask docs                                         # mdbook build docs (--serve for live preview)
```

`rust-toolchain.toml` pins Rust 1.97 (the floor is 1.85 — transitive `getrandom` needs edition 2024 — but the pin tracks current stable); `rustup` auto-installs it on first invocation. The toolchain file lists `rust-analyzer` as a component so editors use a version matching the pin rather than their own bundled build. Note `.cargo/config.toml` sets `rustflags = ["-D", "warnings"]` repo-wide, so a toolchain bump turns any new rustc lint into a hard build failure, not just a CI-gate failure. `protoc` is vendored via `protoc-bin-vendored` inside `kaas-broker/build.rs` — no `apt install protobuf-compiler` needed. Generated proto code is silenced from the workspace clippy gate via a module-scope `#![allow(...)]`.

### CI, releases, and the live cluster

`.github/workflows/docker-publish.yml` is tag-triggered: it builds the broker + operator images from `bins/*/Dockerfile`, pushes them to `ghcr.io/kaas-rs/kaas-broker[-preview]` / `kaas-broker-operator[-preview]`, pushes the Helm chart to `oci://ghcr.io/kaas-rs/charts/kaas-broker`, then creates the GitHub Release — titled and described from the **annotated tag message** (a lightweight tag falls back to the commit message), so write the tag message as release notes.

**CRD drift** — if you edit anything under `crates/kaas-operator-api/`, run `cargo xtask gen-crds` and commit both `deploy/crds/` and `deploy/helm/kaas/crds/`. The `rust` CI job fails on drift.

Releases are tag-driven; see [`docs/RELEASING.md`](./docs/RELEASING.md). **Always bump the patch (`v0.2.N-preview` → `v0.2.N+1-preview`), never re-cut a tag.** **No backwards compatibility before v1, with one carve-out** (owner decision, 2026-07-23): a release that leaves CRD schemas **unchanged** must support an in-place rolling update from the immediately preceding preview (adjacent-version heartbeat/state/wire contracts keep working). A release that **changes CRDs** may break anything — delete-and-redeploy is its supported upgrade path; note the break in the tag message. Don't spend effort on migration shims, legacy-layout adoption, or deep mixed-version guards beyond the adjacent-release roll.

**The live cluster (`k3s-cluster`, `apps/kaas` + the `apps/strimzi` reference) has no anonymous listener and enforces ACLs.** Both clusters serve the same three TLS listeners under the same names and ports — `mtls` 9093 (client certificate), `scram` 9095 (SASL/SCRAM-SHA-512), `oauth` 9096 (SASL/OAUTHBEARER, EntraID) — so a client moves between them by changing only the host. kaas's `mtls` trusts the shared Kafka clients CA (cert-manager `kafka-clients-ca-issuer`) through the chart's per-listener `clientCA` (v0.3.8+), which arms certificate verification on that listener alone; Strimzi's uses its own clients CA. Both run `authorization: simple` with no super users. Every client is a named KafkaUser (`apps/*/kafka-users/`): `kafka-bench` (bench Jobs, `kperf-*`), `kafka-streams` (streams tests), `throttled-user` (quota benches), `kafbat-ui` / kaas-ui (all-access UIs), `kafka-connect` (Strimzi only), `kaas-scripts` (this repo's `scripts/`, admin). Jobs don't carry credentials inline: an ExternalSecret per namespace (`kafka-bench-client`, `kafka-streams-client`) renders ready-to-mount `client.properties` / `consumer.properties` / StreamsConfig from the generated password + broker CA. So a new in-cluster client needs a KafkaUser with ACLs *and* a mounted client config — a bare `--bootstrap-server` call will hang on the TLS/SASL handshake rather than fail fast.

**Resetting a git-managed topic (e.g. `kperf-bench`) is delete-CR → wait → re-sync, not delete-and-recreate by name.** Delete the `KafkaTopic` CR on each cluster (kaas's topic watch abandons the partitions and the operator reclaims the directory; Strimzi's topic operator deletes the topic), confirm the topic is gone from the brokers, wait **2 minutes**, then re-sync the CR from ArgoCD. The wait lets kaas's reclaim finish — rename aside, recreate the partition dirs, `remove_dir_all` the staged copy — before the new incarnation's CR arrives; recreating inside that window is the gh #219 / gh #241 stale-directory territory. Check afterwards that `status.topicId` changed and every partition's latest offset is 0.

## Architecture

kaas is a from-scratch Kafka-protocol-compatible broker that runs on Kubernetes. Two binaries ship in this repo:

- **`bins/kaas`** — the broker. Listeners are declared via the `KAAS_LISTENERS` JSON env (gh #126); the chart emits one entry per `.Values.listeners[]` item. Fixed ports: 8080 health, 9094 inter-broker heartbeat gRPC.
- **`bins/kaas-operator`** — an operator that reconciles 3 CRDs into on-disk config files (auth/topics) and Kubernetes plumbing (TLS routes, etc.).

### Brokers are runtime-independent of the operator

This is the most important architectural fact and is easy to misread from the directory layout. The operator is a **startup/admission** component, not a hot-path dependency:

- Brokers read `KafkaTopic` CRs at startup (and watch them for new topics / partition expansion), but the read is non-fatal — a missing/unreachable API server only blocks new topic creation, never serving of existing topics.
- The Produce/Fetch hot path makes **zero K8s API calls**. Ownership lookups are in-memory.
- Admin handlers do patch `KafkaTopic` CRs, through `crates/kaas-broker/src/topic_cr_writer.rs`: see `crates/kaas-broker/CLAUDE.md`.

If you find yourself adding a runtime dependency from broker → operator (e.g. a watch on a CR that blocks request handling), stop — that's an architectural change.

### Controller broker, leases, and the authoritative assignment file

The "controller" is **a broker that holds the `kaas-controller` Lease**, not a separate process. Its responsibilities:

- Observes peer brokers via gRPC heartbeats (`proto/heartbeat.proto`, tonic-build output in `kaas-broker`, `crates/kaas-controller/src/heartbeat_server.rs`).
- Computes partition + consumer-group assignments (`crates/kaas-controller/src/balancer.rs`, `assignment_writer.rs`).
- Writes `/data/__cluster/assignment.json` on the shared RWX PVC. The file is epoch-prefixed by `leaseTransitions`; brokers reject writes with stale epochs (verified by `crates/kaas-controller/tests/stale_controller_race.rs`).

`assignment.json` is the **single source of truth for partition leadership** (gh #75 cleanup). The Metadata response, the produce/fetch ownership check, and `/healthz`'s `partitions_led` all source from it via the broker `Coordinator` (`crates/kaas-broker/src/coordinator.rs`). There is no per-partition Lease — only the singleton `kaas-controller` Lease used for cluster-wide controller election. The local-lease shim (`crates/kaas-broker/src/local_lease.rs`, always "yes, I lead") exists for dev mode only.

Lease writes, the balancer and fencing are in `crates/kaas-controller/CLAUDE.md`.

### The NFS substrate contract (governing principle)

Multi-broker mode runs on a shared RWX (NFSv4-class) volume that IS the coordination substrate. NFS gives you exactly three guarantees — same-directory `rename()` atomic, `open(O_CREAT|O_EXCL)` atomic, close-to-open consistency — and nothing else; `remove_dir_all`, read-modify-write, and check-then-act (which is `Partition::open`'s shape) are all non-atomic. So the rule for any code touching the shared volume is: **(1)** persist via `tmp + fsync + rename + fsync(dir)` (`crates/kaas-storage/src/atomic_write.rs`), never in-place — the directory fsync is free on NFS but is what makes the rename durable on the chart's local-path mode (gh #231); **(2)** any compound op that can't be a single atomic primitive must be idempotent and driven to completion by retry/reconcile, never one-shot; **(3)** mutate through single-writer + epoch fencing so there's no concurrent writer to race. Most historical bugs are violations: gh #203 (operator `remove_dir_all` races broker open → rule 3), gh #215 (`take_over` not retried → rule 2), gh #205 (sweep not resumable → rule 2), gh #76 (unlink-while-open silly-rename → rule 3). Full write-up + the reviewer checklist: `docs/src/architecture/nfs-substrate.md`. When adding code that writes the shared volume, run it past the three questions before shipping.

### Client-supplied names

**Client-supplied names become paths, so they must not be able to leave their directory (gh #314).** A topic name goes through `kaas_storage::is_valid_topic_name` (Apache's `Topic.validate`, plus the reserved `__cluster` and `.deleting-*`) in CreateTopics, Metadata auto-create, the operator's reconcile and the broker's topic watch, and `DiskStorageEngine::ensure_open` refuses anything else as the last line of defence, because opening creates the directory. A prefix ACL matches the raw string, so `kperf-/../victim` passes a grant on `kperf-`: an ACL check is no substitute. Group ids can't be refused (Apache allows any string), so `OffsetStore` percent-encodes `/`, NUL and `%` in the file name. If you add a path built from a client string, run it past this.
