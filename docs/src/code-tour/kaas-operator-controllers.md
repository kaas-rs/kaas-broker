# kaas-operator-controllers

One reconciler per CRD, materializing state to files on the shared PVC, plus leader-elected orphan sweeps (at election, then every 5 minutes).

Two layers:

- **Reconcilers** — `kafkatopic_controller.rs` (partition directories +
  `.config.json`; mints `Status.TopicID` on first reconcile and never
  rotates it; refuses partition decrease), `kafkauser_controller.rs`
  (derives SCRAM entries into `credentials.json`, rebuilds `acls.json`,
  owns the `<user>-kafka-credentials` Secret; the gh #104 pre-derived
  passthrough enables zero-downtime rotation), `kafkacluster_controller.rs`
  (cert-manager Certificates, per-broker Services, Gateway TLSRoutes — all
  with OwnerReferences).

- **Helpers** — `credentials.rs` + `acls.rs` (the file materializers),
  `sweep.rs` (the leader-elected orphan sweep — an initial pass at leader
  election plus a periodic re-run every 5 minutes, resumable across
  interrupted passes, gh #205 — dropping topic dirs and credential entries
  with no matching CR), `conditions.rs` (status conditions), `errors.rs`,
  `observer.rs` (reconcile counters).

**The cleanup model is the crate's defining decision — no finalizers.**
Deleting CRs never blocks on the operator being alive; Kubernetes GC
handles owned resources, and the sweep reclaims on-disk leftovers on the
next pass. The ArgoCD cascade-delete deadlock that forced
this design is told in
[Kubernetes integration](../architecture/kubernetes.md).

**Invariant callers must hold**: reconcilers must stay idempotent and
convergent — every materialized artifact is rebuilt from the full CR set,
never incrementally patched into an unknown state, which is what makes the
sweep safe to run blindly at startup.

**Start reading at** `kafkauser_controller.rs` (the richest reconcile),
then `sweep.rs`.
