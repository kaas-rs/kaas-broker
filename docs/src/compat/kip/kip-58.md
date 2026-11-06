# KIP-58 — min compaction lag

**Status: partial — config surface only; the gate itself is not yet
enforced.** See the [KIP index](../kip-index.md).

## What the KIP changes in Apache Kafka

KIP-58 (Kafka 0.10.1) added `min.compaction.lag.ms`: a per-topic
guarantee that a record stays uncompacted for at least the configured
period. The log cleaner treats the head of the log inside the lag window
as off-limits, so consumers of a compacted topic get a bounded window in
which they are guaranteed to see *every* update, not just the latest per
key.

## How kaas implements it

What exists today is the **configuration plumbing**, end to end:

- `min.compaction.lag.ms` is a field on the per-topic config file the
  operator materialises from `KafkaTopic.spec.config`
  (`min_compaction_lag_ms` in `crates/kaas-storage/src/topicconfig.rs`,
  written to `/data/<topic>/.config.json`; `None` is kept distinct from
  `0` so "unset" falls through to the engine default).
- `IncrementalAlterConfigs` accepts the key and patches it onto the CR
  (`crates/kaas-broker/src/topic_cr_writer.rs`).
- `DescribeConfigs` advertises it with default `0` — "compact
  immediately", matching Apache — from the defaults table in
  `crates/kaas-broker/src/topic_config_defaults.rs`.

What does **not** exist yet is the compactor that would honour the gate.
`crates/kaas-storage/src/cleaner.rs` implements size- and time-based
delete-policy retention (whichever target reaps more), and its module doc
says so plainly: the compactor honouring these knobs
(`min.compaction.lag.ms`, `delete.retention.ms`) is follow-up work
(tracked as gh #158). The compaction
metrics in `crates/kaas-observability/src/metrics.rs`
(`kaas.compaction.*`) are declared ahead of that work and nothing records
them today. So the knob round-trips through every admin surface but gates
nothing — and "never compacted" is no longer the safe reading of an
infinite lag: the retention cleaner does not consult `cleanup.policy`,
and the enforced `retention.ms` default of 7 days means a
`cleanup.policy=compact` topic's closed segments age out like any
other's unless `retention.ms` is set to `-1`. Until the compactor lands,
data on compacted topics is subject to delete-style retention (tracked as
gh #339).

The intended enforcement semantics (per-segment `maxTimestamp` inside the
lag window ⇒ segment skipped) are described with the storage engine in
[Storage hot path](../../architecture/storage-hot-path.md); treat that
section as design intent until the compactor lands.

## How it's verified

Verification currently covers the config plumbing, not the gate:
`roundtrip_preserves_unset_vs_zero` and `only_set_fields_are_emitted` in
`crates/kaas-storage/src/topicconfig.rs` pin the unset-vs-zero
distinction, and `scripts/kafka-configs.sh` exercises the
`--describe` / `--alter` round trip against a live broker. There are no
compaction-behaviour tests because there is no compaction behaviour to
test — their absence is the honest signal of this page's status.
