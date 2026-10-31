# Broker/operator runtime independence

Why the operator is a startup/admission component, not a hot-path
dependency — the Produce/Fetch path makes zero Kubernetes API calls.

This is the most important architectural fact in kaas, and the easiest
one to misread from the deployment layout: kaas ships a broker *and* an
operator, so it looks like a classic operator-managed system where the
operator sits in the middle of everything. It doesn't. The relationship
mirrors one you already know from Apache Kafka, where a broker that
loses sight of the controller keeps serving from the metadata it has —
it just stops learning anything new. In kaas, the operator and the
Kubernetes API server behind it play that role. The operator is a
**startup and admission** component:

- Brokers read `KafkaTopic` CRs at startup and watch them for new
  topics and partition expansion — but the read is **non-fatal**. A
  missing or unreachable API server only blocks *new* topic creation;
  existing topics keep serving.
- The Produce/Fetch hot path makes **zero Kubernetes API calls**.
  Ownership lookups are in-memory, against the broker's view of
  [`assignment.json`](./controller.md).
- Authentication and authorization state (`credentials.json`,
  `acls.json`) is read from the shared volume with hot-reload — the
  operator *writes* those files when `KafkaUser` CRs change, but a
  broker never asks Kubernetes a question to authenticate a client.
- Brokers serve traffic while the operator is crash-looping, upgrading,
  or deleted. What degrades without the operator: new
  `KafkaTopic`/`KafkaUser` CRs stop being materialized, and
  external-listener plumbing (Certificates, TLSRoutes) stops
  reconciling. What does not degrade: every already-created topic,
  credential, and ACL.

One honest caveat, and it is not a Kubernetes dependency: an
OAUTHBEARER listener validates tokens against signing keys fetched from
the OAuth *issuer* — an external service — and is fail-closed before
the first successful fetch. An unreachable issuer blocks new
authentications on that listener; every other listener, and every
already-authenticated connection, is unaffected.

## The one Kubernetes dependency on the control path

Brokers do keep a few long-lived Kubernetes watches: the
`kaas-controller` Lease (controller election), the `KafkaTopic` CR
watch (topic catalog), and the headless-Service endpoint watch (peer
discovery). All of them feed the *control* plane — assignment
recomputation — not the data plane. If the API server goes away, the
current controller keeps its Lease view, `assignment.json` stays where
it is, and partition leadership simply stops *changing* until the API
server returns. Clients notice nothing.

"Until the API server returns" is load-bearing, and it costs the topic
watch real machinery to honour. Kubernetes ends watch streams for
entirely routine reasons — a relist, an API-server rollout, a network
blip — so the watch rebuilds its stream with exponential backoff
instead of treating stream end as completion; a watch that exits on the
first routine disconnect stops tracking topics *permanently*, with no
error to log. The watch also treats every relist as a full reconcile:
the topic set the fresh stream reports is diffed against what the watch
last knew, and anything missing is retracted. Without that diff, a
topic deleted while the watch was disconnected would never produce a
delete event and would linger forever — the broker serving Metadata for
a topic that no longer exists, and the controller assigning its
partitions to brokers that then fail to open them.

## Admin writes go through CRs — deliberately

The broker isn't strictly read-only against Kubernetes: the Kafka
admin APIs are served by **writing** CRs, so that the operator remains
the single materializer of topic and user state:

- **Topic admin APIs** work the `KafkaTopic` CR: `CreateTopics` (and
  Metadata-driven auto-creation) mints one, `CreatePartitions` patches
  `spec.partitions`, `IncrementalAlterConfigs` patches `spec.config`
  per key, `DeleteTopics` deletes it, and `AlterReplicaLogDirs` records
  a partition move in the CR's status.
- **User admin APIs** edit an *existing* `KafkaUser` CR:
  `CreateAcls`/`DeleteAcls` mutate `spec.authorization.acls`, and the
  SCRAM admin API (KIP-554) patches `spec.authentication.scram`.

The operator then materializes the change — partition directories,
`.config.json`, `credentials.json`, `acls.json` — exactly as if you had
edited the CR yourself. This keeps one writer for on-disk state while
still serving the Kafka admin surface. (It's also why broker RBAC
carries write verbs on `kafkatopics` — `create`, `patch` and `delete` —
plus `kafkatopics/status` and `get,update,patch` on `kafkausers`; see
[Kubernetes integration](./kubernetes.md).)

## The line not to cross

If a change adds a broker→operator *runtime* dependency — a CR watch
that blocks request handling, a reconcile the hot path waits on —
that's an architectural change, not an implementation detail. The
invariant to preserve: **a broker that has already started serves
Produce/Fetch with the Kubernetes API server unreachable.**

## Implementation notes (for contributors)

- Admin CR writes route through
  `crates/kaas-broker/src/topic_cr_writer.rs` (topics),
  `crates/kaas-broker/src/acl_cr_writer.rs` (ACLs), and
  `crates/kaas-broker/src/user_cr_writer.rs` (KIP-554 SCRAM rotation,
  gh #252).
- The self-restarting, relist-reconciling topic watch is
  `run_topic_watch` (gh #202): backoff 1 s → 30 s, reset on any event;
  the relist diff keys off the `Event::InitApply` topic set and only
  returns `Ok(())` on cancellation.
