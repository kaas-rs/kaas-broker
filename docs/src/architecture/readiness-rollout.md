# Honest readiness & rollout pacing

`/readyz` means "serving every assigned partition"; a separate liveness
signal feeds the controller's alive set — together they pace rolling
updates and evict wedged brokers.

In Apache Kafka, a rolling restart is paced by replication: you (or
Strimzi) roll one broker, wait for under-replicated partitions to drain
back to zero, then roll the next — the ISR is the signal that it is
safe to continue. kaas has no replicas, so that signal doesn't exist.
What paces a kaas rollout instead is the Kubernetes readiness probe,
which means `/readyz` has to answer a precise question: **is this
broker serving the partitions it was assigned?** Getting that answer
right is what lets a StatefulSet rolling update pace itself — and
getting it *wrong* is what once let two brokers go out of service at
the same time, and let a wedged broker sit undetected for 25 minutes.

## Two signals, not one

A booting broker and a wedged broker look identical from the outside:
both are `NotReady`, both are still heartbeating. They demand opposite
treatment — the booting one must be *kept* (so it can take over), the
wedged one *evicted* (so its partitions move). No single readiness bit
separates them, so the broker publishes two:

| signal | means | computed from | consumed by |
|---|---|---|---|
| **`serving`** | takeover of every assigned partition is complete | the assigned set in `assignment.json` vs the partitions open in the storage engine | `/readyz` |
| **`healthy`** | the main (request) runtime is still scheduling tasks | a liveness tick refreshed by a 1 s idle loop and by every completed request, and stale after 5 s of neither | the controller's alive set, via the heartbeat |

The crucial subtlety: **`serving` cannot detect a wedge.** When the
main runtime seizes up — the observed failure was both worker threads
pinned on a synchronous NFS scan under a 2-CPU limit — the partitions
stay *open* in the engine, so `serving` still reads `true`. The thing
that actually dies is the runtime's ability to run tasks, which is
exactly what the `healthy` tick measures: no worker free to bump the
tick → it goes stale → the broker stops reporting `healthy`.

```
/readyz = listeners-bound
        AND external TLS has a hostname  (it can't advertise itself otherwise)
        AND main runtime alive           (not wedged)
        AND (cluster ? serving : true)   (takeover complete)
```

`/readyz` (and `/healthz`) are served from a **dedicated thread and
runtime**, never the main runtime they report on. That is what makes
the wedge observable: the handler can still answer while the main
runtime is pinned, and it answers *unready*, because the liveness tick
it reads has gone stale.

## The circular dependency, and how `healthy` breaks it

Gating `/readyz` on `serving` looks like it should deadlock, and with
the old alive set it did:

```mermaid
flowchart TD
    boot["broker boots, listeners bind"] --> ready0["/readyz honest:<br/>NotReady until takeover done"]
    ready0 --> es["EndpointSlice: NotReady<br/>→ dropped from readiness"]
    es --> alive0["alive set filters on readiness<br/>→ broker excluded"]
    alive0 --> noassign["controller assigns it<br/>zero partitions"]
    noassign --> notake["nothing to take over"]
    notake --> ready0
    style ready0 fill:#fee,stroke:#c33
    style alive0 fill:#fee,stroke:#c33
```

The fix is to make the alive set depend on `healthy`, not readiness. A
booting broker's main runtime *is* running (it is busy taking over), so
it reports `healthy = true` throughout boot and stays assignable — even
while its `/readyz` is deliberately `NotReady`. A wedged broker reports
`healthy = false` and drops out. Readiness is freed to be honest.

`healthy` travels on the heartbeat, which runs on the broker's
**control-plane runtime** — a separate runtime from the main one. That
is deliberate: the heartbeat survives a main-runtime wedge (so the
broker can still be *told* things), which is precisely why `healthy`
has to be an explicit bit rather than "is the heartbeat connected". A
connected heartbeat proves the control runtime is alive; only the tick
proves the *main* runtime is.

## A rolling update, end to end

```mermaid
sequenceDiagram
    participant SS as StatefulSet<br/>controller
    participant N as kaas-2<br/>(restarting)
    participant HB as kaas-2 heartbeat<br/>(control runtime)
    participant CTL as kaas-0<br/>(cluster controller)
    participant HZ as kaas-2 /readyz<br/>(dedicated runtime)

    SS->>N: delete + recreate (image bump)
    N->>N: listeners bind
    N->>N: main-runtime liveness tick starts
    HB->>CTL: BrokerStatus{ healthy = true }
    Note over CTL: alive set = connected ∧ healthy<br/>→ kaas-2 is assignable
    CTL->>N: assignment.json: kaas-2 leads P0..Pk
    N->>N: takeover: open + recover P0..Pk<br/>(off the main runtime)
    SS->>HZ: GET /readyz
    HZ-->>SS: 503 — alive but NOT serving yet
    Note over SS: minReadySeconds timer cannot start<br/>→ kaas-1 is NOT killed
    N->>N: takeover completes → serving
    SS->>HZ: GET /readyz
    HZ-->>SS: 200 — serving
    Note over SS: continuously Ready ≥ minReadySeconds<br/>→ now safe to roll kaas-1
    SS->>SS: proceed to kaas-1
```

Contrast the wedge case: after the image bump kaas-2's main runtime
seizes. The tick stops, `healthy` flips to `false`, and the controller
drops kaas-2 from the alive set and reassigns its partitions to a live
peer — in seconds, off the heartbeat, rather than waiting ~25 minutes
for a `tcpSocket` liveness probe to notice. Meanwhile kaas-2's
`/readyz`, answered from its still-responsive dedicated runtime,
reports `503`.

## Rolling-upgrade note

`healthy` is trusted unconditionally: a connected broker reporting
`false` is evicted from the alive set. Fast failover comes from the
heartbeat *connection* itself — a genuinely dead broker drops its
stream and vanishes from the alive set within a heartbeat — while a
wedged one is evicted on its next `healthy = false` report. One
consequence of the pre-v1 no-backwards-compatibility policy: broker
images that predate the `healthy` signal always report `false`, so a
rolling upgrade from one is unsupported — deploy fresh instead.

## Belt and braces: `minReadySeconds`

`broker.minReadySeconds` (default 60) makes the StatefulSet wait for
that many seconds of *continuous* readiness before rolling the next
pod. With honest readiness it is a safety margin rather than the
mechanism — a readiness flap during a late-breaking takeover resets the
timer and re-paces the rollout for free.

## Implementation notes (for contributors)

The incidents behind this design: gh #208 (two brokers out of service
at once), gh #211 (25-minute undetected wedge), gh #209/#210 (the
main-runtime seize on a synchronous NFS scan; takeover moved off the
main runtime).

- `crates/kaas-observability/src/health.rs` — `compute_ready`, the
  `record_main_tick`/`main_alive` liveness tick,
  `RuntimeState::serving`.
- `crates/kaas-broker/src/coordinator.rs` — `is_serving` (assigned ⊆
  open).
- `crates/kaas-storage/src/engine.rs` — `open_partition_keys` on the
  trait.
- `bins/kaas/src/main.rs` — the dedicated health runtime + the
  main-runtime tick task.
- `bins/kaas/src/cluster.rs` — `decide_alive`, the `healthy`-gated
  alive-set policy.
- `crates/kaas-controller/src/heartbeat_server.rs` — per-broker
  `healthy` / `broker_liveness()`.
- `proto/heartbeat.proto` — `BrokerStatus.healthy` (field 6). The
  earlier sticky `ever_healthy` guard tolerated images predating field
  6 (proto3 default `false`); it was dropped under the pre-v1
  no-backcompat policy — see `docs/RELEASING.md`.
