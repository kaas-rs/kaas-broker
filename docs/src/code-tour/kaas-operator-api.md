# kaas-operator-api

The kube-derive CRD types — the source `cargo xtask gen-crds` renders into `deploy/crds/` and the Helm chart.

Three CRD types, one module each: `kafkacluster.rs` (external-listener
plumbing, plus the storage / internal-listener / gateway / service config
shapes), `kafkatopic.rs` (partitions + `spec.config` + `Status.TopicID`,
plus `status.volumeAssignments` and the `kaas.rs/migrate-to-volume`
annotation contract), `kafkauser.rs` (Strimzi-shape authentication, inline
`spec.authorization.acls`, and the honestly-named
`producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker` quota
fields), plus
the `condition.rs` / `scheme.rs` helper modules. The
semantics — who reconciles what, why the quota names diverge from Strimzi,
the no-finalizers model — live in
[Kubernetes integration](../architecture/kubernetes.md).

**The generation contract is the thing to internalize**: every type derives
`kube::CustomResource` + `schemars::JsonSchema`, and `cargo xtask gen-crds`
walks them into `deploy/crds/*.yaml` mirrored into the chart. CI fails on
drift — so editing *anything* in this crate means regenerating and
committing both YAML trees in the same change. Field-level validation
attributes are part of the wire contract with the apiserver, not
decoration.

**Both binaries depend on this crate** — the operator to reconcile, the
broker to read `KafkaTopic`s and write admin changes back. It must stay
free of behaviour: types, validation, defaults, nothing else.

**Start reading at** `kafkauser.rs` — it's the richest schema and shows
every pattern the other three use.
