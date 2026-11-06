# Performance vs Strimzi

Where kaas stands against Strimzi-managed Apache Kafka on the same
substrate, and why: group-commit fsync versus page-cache acks.

Benchmark reports are recorded under `docs/perf-results/`, which is
gitignored, so they aren't in the repository; this chapter summarizes the current head-to-head series and
the methodology behind it. Treat every number as bound to its
configuration — both systems ran on the *same* single-node k3s host and
the same NFS export, which is a valid relative comparison and an unusual
absolute environment.

## The current head-to-head series

The `bench-compare-v2` harness runs both systems through the same client
matrix: plain and idempotent produce (5 producer pods, 1 KB records,
`acks=all`), group and no-group consume, a consumer-group scale-up, and
a Kafka Streams wordcount. The 2026-08-01 → 2026-08-11 series (kaas
`v0.2.27-preview` → `v0.3.1-preview`, 3 brokers, **default honest
fsync** — the equivalent of `log.flush.interval.messages=1`;
Strimzi/Kafka 4.2.0, 3 brokers) gives, as ranges across runs:

| Scenario | kaas / Strimzi throughput | Read |
|---|---|---|
| produce, `acks=all` | **1.73× – 2.02×** (typically ~1.85–1.9×) | kaas leads, with p99 latency 3–4× lower |
| produce, idempotent | **1.88× – 2.11×** (typically ~1.95×) | kaas leads; idempotence costs kaas nothing measurable |
| consume (group) | **0.90× – 1.06×** | reproducibly at parity |
| consume (no group) | 0.23× – 5.51× | noise-dominated in both directions; no verdict |

One 4.94× produce run and one 0.54× group-consume run in the series
are excluded as outliers per the methodology below; the ranges quote
the reproducible band. The tightening versus the July series (which
ranged up to 2.70×/4.48×) is better measurement, not regression — the
July highs were single-run artifacts.

From the most recent report
(`docs/perf-results/bench-compare-v2-20260811-164056Z.md`, kaas
`v0.3.1-preview`): produce 23.5 MB/s vs 11.6 MB/s summed across
producers, with p50 6.5 s vs 12.4 s and p99 8.2 s vs 30.8 s under
saturation; idempotent produce 23.6 vs 11.2 MB/s; group consume 86.7
vs 95.9 MB/s; the Kafka Streams wordcount passes on both systems with
identical settle times.

These are the latest recorded numbers, from `v0.3.1-preview`. Later releases
added per-batch validation and a CRC check to the produce path, and that
hasn't been re-measured.

Two scenarios are deliberately not summarized into a verdict: the
**no-group consume** spread is too wide to call anything but noisy on
this rig (0.23× and 5.51× appear in the same week), and the
**rebalance scale-up** comparison remains polluted by harness
artifacts — runs where one side's pod logs are missed, or where the
consumers drain the input inside a single reporting interval, produce
nonsense ratios. A green number you can't trust is worse than no
number, so both stay unquoted until the harness reports cleanly.

Earlier results that showed kaas *behind* on produce predate two fixes
that invalidated them: a broker bug where the flush-interval setting
was parsed but dropped, and a NAS cabling fault that capped the storage
link at ~10 MB/s until 2026-07-12.

## Why the shapes differ

The architectural difference drives both columns. Apache acknowledges
`acks=all` once the write reaches the ISR's page caches — fsync happens
later, asynchronously. kaas has [no
replication](../compat/non-goals.md), so its `acks=all` at default
settings means a real NFS COMMIT round-trip before the ack; the
[group-commit design](../architecture/storage-hot-path.md) exists to
share one COMMIT across every concurrently-parked producer, which is
how an honest-fsync broker ends up ahead of a page-cache-ack broker on
a substrate where COMMIT latency dominates. The flush-interval dial
([storage](./storage.md)) trades durability back toward Apache's
posture where page-cache-equivalent semantics are acceptable.

## What sets the produce ceiling

On a slow-fsync substrate the produce ceiling obeys Little's law:

> throughput = concurrent durable writes × bytes per write ÷ write round-trip

and the model is exact here, not approximate. Measured on this NAS at
a single broker: 4.8 concurrent writes × ~49.6 KB per write ÷ 16.6 ms
COMMIT round-trip predicts 14.4 MB/s; the bench observed 14.42 MB/s.
The practical consequence is that almost nothing you would
instinctively tune moves the number, because none of it moves any of
the three terms. Tested and flat: broker CPU 2 → 6 cores (+0.5%),
partition count 16 → 64 (flat throughput, worse tail latency), client
`max.in.flight.requests.per.connection` 5 → 20 (nothing), and internal
lock restructuring around the fsync (nothing — see the dead-ends
below). The NFS transport itself idles at ~14% link utilisation, so
the bottleneck is latency, not bandwidth.

What does move it:

- **The flush interval** (skipping the durability wait entirely):
  3.2× — but that is a durability trade, not an optimisation.
- **Faster storage** (lower COMMIT latency): directly proportional.
- **Broker count**: 3 brokers reproducibly deliver ~1.58× one broker
  (~23 vs ~14.4–15.1 MB/s) — which is why the head-to-head series
  above runs 3 brokers as the representative configuration.
  Honesty note: the *mechanism* is an open question. All three broker
  pods share one node — one kernel NFS client, one transport — so
  none of the per-broker explanations tested so far accounts for it.

Before optimising this path, compute the Little's-law budget from the
NFS mount's own counters (write ops, bytes sent, round-trip time in
`mountstats`) and check which term the change would actually move.

## Methodology

Perf conclusions on this project follow rules learned the hard way:

- **Ranges over single runs** — single runs on a shared home-lab node
  are noise-dominated; one recorded pattern is a 3-fast-2-slow cycle
  driven by page-cache eviction. The table above quotes the spread
  across the series, not a best run.
- **Substrate liveness checks** — each bench snapshots NFS RPC counters
  and node network rates, so a degraded NAS link (see above) shows up
  in the report instead of silently poisoning the numbers.
- **Cooldowns between runs** (120 s in the compare harness) so one
  system's tail I/O doesn't bleed into the other's warm-up.
- **Distrust surprising verdicts** — a PASS can come from a stale
  topic and a FAIL from a harness bug; identical failures on both
  systems indict the harness, not the brokers.

## Dead-ends already tried

Recorded from earlier tuning rounds so they aren't re-litigated without
new evidence: PGO builds, `FADV_SEQUENTIAL` on segment reads, and
flush-interval `0` (pure throughput mode) all failed to move
steady-state numbers meaningfully on this substrate — the NFS COMMIT
round-trip, not CPU or readahead, is the dominant cost.

One dead-end deserves its own warning label: **moving the fsync off
the partition lock** (syncing a cloned file descriptor so appends
proceed during the COMMIT). It was built and measured — writes grew
2.9% larger, throughput did not move (Little's law again: it changes
no term) — and then deliberately reverted, because it made an
easy-to-break invariant load-bearing for durability: the flush
sequence published to waiting producers had to be the one sampled
*before* the sync, and nothing structural prevented a tidy-minded
refactor from silently acking unsynced data. Don't rebuild it without
new evidence that the budget above has changed.
