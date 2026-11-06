# kaas-broker

**Book:** `docs/src/architecture/runtime-independence.md`, `docs/src/architecture/kubernetes.md`, `docs/src/compat/api-reference.md`, `docs/src/code-tour/kaas-broker.md`. Read them for the design, and update them in the same commit when behaviour here changes.

Idempotent producer, transaction and group handlers are covered by `crates/kaas-coordinator/CLAUDE.md`. Listener and ACL rules are in `crates/kaas-auth/CLAUDE.md`. For topic delete, recreate and reclaim, read `docs/src/code-tour/topic-lifecycle.md`.

## Admin handlers and broker RBAC

**Admin handlers DO patch `KafkaTopic` CRs** (gh #52, gh #9, gh #236). `CreatePartitions` (key 37, KIP-195) and `IncrementalAlterConfigs` (key 44, KIP-339) route through `crates/kaas-broker/src/topic_cr_writer.rs`: partition expansion patches `spec.partitions`; config updates patch `spec.config` per-key (SET/DELETE only — APPEND/SUBTRACT return `UNSUPPORTED_VERSION`; unknown keys / unparseable values return `INVALID_CONFIG` (40) — **the API server silently prunes unknown merge-patch fields, so anything not rejected client-side becomes a success that does nothing**, which was gh #236's dangerous half). `CreateTopics` (key 19) lands its wire config overrides in the minted CR's `spec.config`, same validation. The operator then materialises the change as usual, and `DescribeConfigs` (key 32) reports stored overrides as `DYNAMIC_TOPIC_CONFIG` by re-reading `.config.json` through `StorageEngine::topic_config` per request (hot-reload, like the retention sweep). **Broker RBAC carries only the verbs the code calls** (gh #265) — `patch` but not `update` on `kafkatopics` (every spec write is a merge patch), `list` + `update` on `kafkausers` (the ACL writer lists users to match DescribeAcls/DeleteAcls filters, then get → replace — `list` was wrongly cut in v0.3.20 and silently broke `kafka-acls.sh --list/--remove` until v0.3.34) — so if you add another admin write path, check `deploy/helm/kaas/templates/broker-rbac.yaml` doesn't need a new verb. A `replace` is `update`, a `Patch::*` is `patch`, an `Api::list` is `list`, a watch is `list,watch` — and grep with calls joined across lines (`self\n.api()\n.list(` is how the `kafkausers` cut slipped through). `IncrementalAlterConfigs` is TOPIC-only; BROKER / BROKER_LOGGER resource types return `UNSUPPORTED_VERSION` (no dynamic broker-config surface yet). Broker-**created** CRs optionally carry ArgoCD tracking/coexistence annotations (`crates/kaas-broker/src/argocd.rs`, chart `admin.argocd.*` → `KAAS_ARGOCD_*` env; gh #84/#106 re-ported from Go) — creation only: the ACL writer edits *existing* `KafkaUser` CRs and must never stamp ArgoCD metadata on a git-managed resource.

## Discovery

`listener_advert.rs` holds the advertised-endpoint rule, the broker catalog and the controller-id derivation, shared by Metadata and DescribeCluster: two discovery APIs that disagreed on the port would loop an authed client on SASL retry.

`list_offsets.rs` answers the earliest and latest sentinels. Timestamp-to-offset resolution is still stubbed to `(-1,-1)` in `kaas-storage/src/disk.rs`.

## TopicID propagation (gh #105, KIP-516)

Every `KafkaTopic`'s `Status.TopicID` carries a v4-shape UUID, generated cryptographically in `crates/kaas-operator-controllers/src/kafkatopic_controller.rs` on first reconcile and **never rotated** (Apache's contract: re-created topics get distinct IDs). The *intended* state flow — topic watcher stashes `Status.TopicID` → topic registry surfaces it → Metadata v10+ writes the 16 raw bytes — is only partially wired: the production topic watch (`run_topic_watch` in `kube_watchers.rs`, wired from `bins/kaas/src/main.rs`) inserts registry entries with the all-zero sentinel, so **Metadata currently serves nil topic IDs for every topic** and clients fall back to name-based lookups. Wiring the ID through (and `CreateTopics` v7+ carrying the UUID in its response) is open follow-up.

## Fetch sessions (gh #4)

KIP-227 incremental fetch sessions are stateless: kaas returns **`SessionID=0` on every Fetch response** regardless of what the client sent. Echoing the client's SessionID was the pre-fix bug — clients then sent incremental deltas against state kaas didn't have and silently 'forgot' partitions from their subscription. Apache's documented contract for "broker doesn't support sessions" is `SessionID=0`, which makes clients fall back to full Fetch data per request. CPU cost is fine at kaas's scale; KIP-227 caching is a future optimisation, not a correctness gap.
