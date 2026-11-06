# kaas-controller

**Book:** `docs/src/architecture/controller.md`, `docs/src/architecture/broker-fencing.md`, `docs/src/code-tour/kaas-controller.md`. Read them for the design, and update them in the same commit when behaviour here changes.

`assignment.json` as the source of truth, and when the controller is elected, are in the root `CLAUDE.md`. The NFS substrate contract in the root `CLAUDE.md` governs anything here that writes the shared volume.

## Lease writes

**Every `kaas-controller` Lease write is GET → modify → `replace` carrying the read's `resourceVersion`** (`crates/kaas-controller/src/kube_election.rs`, the same as client-go's `LeaseLock.Update`) — takeover, renew, and release alike; the operator's leader election shares the code. A 409 means another writer got there first: takeover → lost the round, renew → transient (the next tick re-reads), release → leave it. **A renew failure counts from the start of the last attempt that landed (gh #334)**, so a holder abdicates before its lease can expire. It counted from the end of the first failed attempt, so with every call hanging a leader ran for 24 s against a 15 s lease. There's one abdication point, and nothing outlives the window: a retry's sleep and a failing attempt both stop at its end. The one exception is an attempt with no failure behind it, after a stall of the task itself, which gets up to the gap between the window and the lease, because the lease may still be ours. A win whose answer took longer than the window starts nothing and tries again, so a persistently slow API server never elects, and the candidate keeps the lease in its name meanwhile. **This is stricter than client-go**, whose `RenewDeadline` window starts a retry period after the last success, and the cost is blip tolerance: the longest outage survived is 5.9-7.5 s on 15/10/2 and 15.9-17.5 s on 30/20/2, depending on where in the cycle it starts. On the broker a longer one costs a controller restart, with no epoch bump if nobody took the lease. Widen `KAAS_CONTROLLER_*` rather than loosening the bound. Don't go back to a forced server-side apply: it has no precondition, so two candidates racing an expired Lease both won at the same `leaseTransitions`, and the broker fence (`controller_epoch < lease_epoch`) can't separate equal epochs. The compare-and-swap gives one winner per transition, not fencing — the epoch fences still do that. The tests' fake API server reads *before* its race barrier on purpose: waiting first let the first racer's write land before the second's read, so the race test passed against a last-writer-wins server.

## Assignment and the balancer

The controller recomputes the assignment when:
- It first wins the controller Lease (initial recompute).
- A `KafkaTopic` CR is added / modified / deleted — wired via the topic-watcher's change notification (gh #74, `bins/kaas/src/cluster.rs`).
- A broker joins or leaves the alive set — wired via the broker-set watcher polling the alive set every 2 s (gh #77, `bins/kaas/src/cluster.rs`).

Non-controller brokers watch `assignment.json` via file notification + 1s poll (`crates/kaas-broker/src/coordinator.rs`); the `TakeoverDriver` / `GroupTakeoverDriver` registered on assignment change (`takeover.rs`, `group_takeover.rs`) opens or relinquishes partitions in the storage engine to match.

**The balancer is incremental, and the phase order is what makes it so (gh #206, gh #247).** `balance()` runs: pin every partition whose previous leader is still alive → place only the rest, levelling each topic across the alive set → smooth cluster-wide to `max − min ≤ 1` → smooth per-topic without disturbing that → reconcile epochs. **Preserve must stay first.** Until gh #206 it ran last: placement was pure rendezvous and `smooth_partitions` was a pure function of `(topics, alive)`, so creating or deleting *any* topic re-derived the whole layout and migrated partitions of unrelated topics — every move a real open/recover/close cycle on NFS plus a window where the outgoing leader is still acking (the gh #227 dual-writer window, whose "not just broker death" trigger was exactly this). Kafka Streams' `application-reset` deletes and recreates topics on every run, so the churn was routine. Two properties in the smoothers are deliberate: victims are chosen non-sticky-first (a freshly placed slot costs nothing to move, an inherited one costs a takeover), and the per-topic pass repairs skew by **swapping** with a partition of another topic — cluster counts are untouched by construction, so the two passes can't fight. Per-topic evenness is its own goal, not a side effect of the cluster-wide count: at 24/24/25 the cluster smoother's predicate is already false, which is how a 3-partition topic pinned all three partitions to one broker forever (gh #247). Both smoothers strictly decrease `Σ count²`, which is what bounds them.

Three modes, picked at boot (top of `bins/kaas/src/cluster.rs`): **in-memory** with neither `MY_POD_NAME` nor `KAAS_DATA_DIR` set, where no `Coordinator` runs and produce and Metadata fall back to local-lease paths that always treat self as leader; **single-broker disk** with only `KAAS_DATA_DIR`, which writes a self-only `assignment.json`; and **cluster** with `MY_POD_NAME`.

## Broker fencing (gh #249)

Three states, not two: **serving** (EndpointSlice-listed and Ready),
**fenced** (listed, not Ready — exists but isn't serving), **gone** (not
listed). K8s supplies the registry, so a scale-down *is* deregistration —
no `unregister` verb, no tombstones. `BrokerRegistry` keeps not-ready
entries rather than filtering them; an ordinal absent from its **owning
slice** (sharded Services exist) is removed; self is never inserted,
downgraded, or removed by slice data (the gh #208 self-eviction death
spiral). The controller writes the tri-state into `assignment.json`'s
long-dormant `BrokerHealth { Alive, Draining, Dead }`: every *registered*
broker gets a row, and a fenced one keeps the `last_seen` from when it was
last alive.

**The reporting was the motive; the divisor was the bug.** `group_hash`
documents that `hash(groupID) % num_brokers` MUST divide by the full broker
set "including draining / dead", but the list came from `assignment.json`,
which held only the alive set — so losing 1 of 3 brokers silently moved the
divisor 3 → 2 and rehashed ~2/3 of all group and txn coordinators, exactly
when the cluster was already degraded. Don't "clean up" `Dead` rows: they
are the divisor. Test: `a_dead_broker_moves_only_its_own_groups`.

**Draining is fencing from the other side.** SIGTERM calls
`mark_draining()` *before* teardown; the next heartbeat (~1 s) carries it,
the controller drops the broker from the alive set while it is still
healthy enough to hand over, and marks it `Draining` (still registered, so
the divisor holds). The self-pin in `decide_alive` outranks draining — an
empty alive set would unassign the cluster. This is the proactive drain
hint the gh #61/#139 shutdown notes left open.

Fenced is derived from readiness, so a **booting** broker reads as fenced
until takeover completes — deliberate, and what Apache reports for a
registered-but-not-caught-up broker. Readiness still does NOT drive the
alive set (gh #208): that stays on the heartbeat's `healthy` bit, so a
booting broker remains assignable while reading as fenced. Metadata omits
fenced rows; DescribeCluster v2 reports them. Book page:
`docs/src/architecture/broker-fencing.md`.
