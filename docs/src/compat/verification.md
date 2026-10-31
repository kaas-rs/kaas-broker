# Verification story

How parity claims are backed: the Apache shell-tool suite, the integration tests, the parity project board, and the bench methodology.

A compatibility claim is only as good as the thing that would catch it
being wrong — which is also why this book can afford to be blunt about
gaps: the machinery that backs every "implemented" would surface a
regression just as loudly. kaas layers four:

## 1. The Apache shell-tool suite

The strongest evidence that unmodified Kafka tooling works against kaas
is running unmodified Kafka tooling against kaas. 41 per-tool scripts
(`scripts/kafka-*.sh`, at the repo root) run the *actual Apache Kafka
distribution's* shell tools (`kafka-topics`, `kafka-console-producer`,
`kafka-consumer-groups`, `kafka-acls`,
`kafka-{producer,consumer}-perf-test`,
`kafka-verifiable-{producer,consumer}`, …) against a live kaas cluster —
the same binaries an operator would point at Apache, unchanged. Every
script honours the same `BOOTSTRAP` / `KAFKA_BIN` overrides; defaults
target the in-cluster Service DNS and `/opt/kafka/bin`.

Two honesty conventions:

- **Skips are explicit, not silent.** Scripts covering features that are
  [non-goals](non-goals.md) or post-3.7 (KRaft tools, share groups, …)
  print a one-line reason and `exit 77` — discoverable, and never
  pretending to test something that can't work.
- **The baseline is recorded and diffable.**
  `scripts/.parity-baseline.txt` pins the expected per-script result.
  Current baseline (captured 2026-07-19 against `v0.2.4-preview`,
  production 3-broker shape on NFS-RWX): **21 PASS / 20 SKIP / 0
  FAIL**. Every SKIP maps to a documented non-goal or a post-3.7
  feature. Reruns assert against the baseline, so a PASS→FAIL downgrade
  is a regression, not an anecdote.

## 2. In-process integration tests

Run on every CI push (`cargo test --workspace --all-features`), against
a broker started inside the test process:

- wire-level round trips (produce → fetch → metadata);
- the SASL/SCRAM handshake plus ACL enforcement;
- [byte opacity](wire-protocol.md#the-byte-opacity-contract): a produced
  batch comes back byte-identical through Fetch;
- multi-broker bring-up: assignment, takeover, coordinator routing;
- the full KIP-447 consume-process-produce-commit round trip (EOS v2);
- controller failover and the stale-epoch fence.

Plus per-crate unit tests — including the codec's fixture tests, which
pin encode/decode byte-identity against captures from Apache Kafka 3.7.

## 3. The parity project board

The
[kaas-migration-parity](https://github.com/users/Woestebanaan/projects/2)
GitHub project tracks the feature matrix item by item — the working
surface where "what does 3.7 do here?" questions get resolved before
they become code. When a feature is ambiguous, the default is *match
Apache Kafka 3.7*, never invent kaas-specific semantics.

## 4. Docs that can't rot

Two CI gates keep this book honest (`cargo xtask check-docs-drift`):

- The [API support matrix](api-matrix.md) is generated from the same
  codec registry that builds the ApiVersions response; CI fails if the
  committed page drifts from the wire surface.
- Every `crates/…` / `bins/…` source path cited anywhere in the book is
  checked against the tree; a refactor that moves a file fails CI until
  the citation is fixed.

## Performance verification

Benchmarks are treated with the same suspicion as compatibility claims:
multi-run (5×) averaging with outlier exclusion, NFS RPC + network-rate
snapshots as a NAS-liveness probe, and recorded reports under
`docs/perf-results/`. Current standing and methodology live in
[Performance vs Strimzi](../operations/performance.md).

## Implementation notes (for contributors)

- Shared shell-suite helpers (`BOOTSTRAP`, `KAFKA_BIN`, `skip`) live in
  `scripts/_common.sh`, sourced by every `kafka-*.sh` script.
- The integration tests above, in list order:
  `bins/kaas/tests/smoke.rs`, `bins/kaas/tests/auth_smoke.rs`,
  `bins/kaas/tests/oauth_smoke.rs`,
  `bins/kaas/tests/cluster_bringup.rs` + `cluster_smoke.rs`,
  `bins/kaas/tests/eos_v2.rs`, `bins/kaas/tests/retention.rs`, and
  `crates/kaas-controller/tests/controller_failover.rs` +
  `stale_controller_race.rs`.
