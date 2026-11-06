# KIP-13 — per-broker client quotas

**Status: implemented** — see the [KIP index](../kip-index.md).

## What the KIP changes in Apache Kafka

KIP-13 (Kafka 0.9) introduced byte-rate quotas for clients: per-principal
producer and consumer caps, enforced **per broker** rather than
cluster-wide. A broker that sees a client exceed its rate computes a
throttle delay and returns it as `throttle_time_ms` in the response, so a
well-behaved client backs off. With N brokers, the effective cluster-wide
ceiling is N × the configured rate — that per-broker semantic is part of
the KIP, not an accident.

## How kaas implements it

The enforcement engine is a per-principal token bucket in
`crates/kaas-auth/src/quota.rs` (`QuotaEnforcer`). Rates come from the
`KafkaUser` CR, whose fields are deliberately named
`producerMaxByteRatePerBroker` / `consumerMaxByteRatePerBroker`
(`crates/kaas-operator-api/src/kafkauser.rs`) to make the KIP-13
per-broker semantics legible at the CR level — same behaviour as
Strimzi/Apache, named honestly.

Mechanics, all in `quota.rs`:

- Refill is continuous at the configured rate, capped at one second's
  worth of tokens; a rate of 0 means unlimited; a brand-new principal's
  bucket is seeded full so first contact isn't throttled.
- **Debt-carry**: the deduction is unconditional and the bucket
  is allowed to go negative; `throttle_time_ms` is the time to refill back
  to zero. The earlier clamp-at-zero version let N concurrent clients
  sharing a principal each see a "full" bucket and burst at N×rate — the
  16-vs-10 MiB/s gap observed under bench-perf.
- Runtime overrides land via `AlterClientQuotas`
  ([quota admin APIs](../api/acls-quotas.md#alterclientquotas), KIP-546):
  `set_user_quota` live-updates an existing bucket; resolution order is
  override > credentials store.

The Produce and Fetch handlers call the checker on every request
(`crates/kaas-broker/src/handlers/produce.rs`, `fetch.rs`) and put the
result straight into the response's `throttle_time_ms`. Quotas fire
regardless of whether authorization is enabled — they are orthogonal axes
([Listeners, auth, quotas](../../architecture/listeners-auth.md#quotas)).
A `NoQuotaChecker` stands in only under `KAAS_AUTH_DISABLED=true`.

**Where kaas does less than Apache**: the broker computes and returns
`throttle_time_ms` but never mutes the connection after responding. That
response-then-mute ordering belongs to [KIP-219](kip-219.md), which is
partial — enforcement currently relies on the client honouring the
throttle hint, which official clients do.

## How it's verified

Unit tests in `crates/kaas-auth/src/quota.rs`:
`multi_client_contention_carries_debt` (pins the debt-carry —
back-to-back drains must yield strictly increasing throttle),
`over_limit_throttles`, `zero_rate_means_unlimited`,
`per_principal_isolation`, `refill_clears_debt_over_time`, and
`set_user_quota_live_updates_existing_bucket`.

Against a live cluster, `scripts/kafka-configs.sh` drives the quota CRUD
surface: `--alter` / `--describe` user quotas, the three supported keys, and
the refusal of `controller_mutation_rate`. Throttling itself is covered by
`produce_exceeds_quota_returns_throttle` in `bins/kaas/tests/auth_smoke.rs`.
