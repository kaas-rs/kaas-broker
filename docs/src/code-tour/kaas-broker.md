# kaas-broker

Broker glue: the on-broker Coordinator, takeover drivers, topic registry, CR write paths, and one handler module per Kafka API.

The largest crate, but structurally simple: everything either *answers a
request* (`handlers/`) or *maintains the state requests read from*
(everything else).

**State side**: `broker.rs` (the `Broker` — the narrow shape every handler
reads — plus `coordinator_io` / `txn_store_io`, which keep coordinator file
work off the runtime workers, gh #328), `coordinator.rs` (the on-broker `Coordinator`: watches
`assignment.json`, answers every ownership question with hash-fallthrough
group/txn routing), `takeover.rs` + `group_takeover.rs` (drivers that
diff assignments into storage-engine take-over/relinquish and group
load/evict — including the gh #89 orphan sweep), `group_hash.rs`
(deterministic coordinator routing), `topic_registry.rs`, `self_fence.rs`
(stops acking writes when heartbeats stall), `heartbeat_client.rs`,
`fence_watcher.rs` + `marker_watcher.rs` (shared-volume pollers applying
peer fences and txn markers), `producer_id.rs` (persisted per-broker PID
blocks, gh #219), `txn_markers.rs` (shared COMMIT/ABORT marker dispatch +
the prepared-txn reconcile, gh #225), `assignment.rs` (the
`assignment.json` schema), `control_batch.rs` (COMMIT/ABORT control-batch
encoder), `listener_advert.rs` (the advertised-endpoint rule + broker
catalog shared by Metadata and DescribeCluster), `argocd.rs` (ArgoCD
coexistence annotations on broker-minted CRs), `topic_config_defaults.rs`
(the DescribeConfigs defaults table), `cli.rs` (env/listener parsing),
`local_lease.rs` (dev mode).

**Write-back side**: `topic_cr_writer.rs`, `acl_cr_writer.rs`, and
`user_cr_writer.rs` — the only paths where serving a Kafka request writes
to Kubernetes (CreateTopics/CreatePartitions/IncrementalAlterConfigs and
Metadata auto-topic-creation → `KafkaTopic`, Create/DeleteAcls →
`KafkaUser`, AlterUserScramCredentials → `KafkaUser.spec.authentication.scram`
— KIP-554, gh #252). None of them is installed without a kube client, where
most topic admin APIs answer `CLUSTER_AUTHORIZATION_FAILED` (31) and DeleteTopics
only removes the topic from the registry. The quota
admin API writes no CR: it changes the serving broker's enforcer in memory.

**Handlers**: one module per API key under `handlers/`. The per-key
behaviour — versions, semantics, deviations — is documented exhaustively in
[Part II's per-API reference](../compat/api-reference.md); don't duplicate
it here or there.

**Invariant callers must hold**: handlers never talk to Kubernetes or the
per-listener auth engine directly — ownership comes from the `Coordinator`,
authorization from the cluster-wide authorizer, and K8s writes go through
the CR writers. That's what keeps the hot path
[runtime-independent](../architecture/runtime-independence.md).

**Start reading at** `broker.rs`, then `coordinator.rs`, then one thin
handler (`handlers/list_groups.rs`) before the big ones
(`handlers/produce.rs`, `handlers/fetch.rs`).
