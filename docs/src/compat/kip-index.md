# KIP index

All KIPs the codebase references, split honestly: implemented, partial, or deliberate non-goal.

KIP numbers are how Kafka names its own evolution, which makes them the
natural vocabulary for the question every evaluator asks: *which Kafka
do I actually get?* This index answers in three buckets, and the split
is source-verified, not aspirational: **implemented** means the
behaviour ships and is exercised by tests or the
[shell-tool suite](verification.md); **partial** pages lead with what's
*missing*; non-goals get a rationale in [Non-goals](non-goals.md), not
silence. Wire-facing KIPs also appear per key in the
[generated API matrix](api-matrix.md)'s KIPs column. The scope is
Apache Kafka 3.7, with one deliberate post-3.7 exception: DescribeCluster
v2 (KIP-1073) is served, because kaas has a real fenced broker state and
no other way to report it — see [KIP-700](kip/kip-700.md).

> Four corrections relative to earlier planning documents, all found by
> source-verifying during the book build: **KIP-516, KIP-32, KIP-58, and
> KIP-354 are partial, not implemented.** Topic IDs are minted but not
> propagated to the wire (KIP-516); LogAppendTime and timestamp lookup
> don't exist (KIP-32); and the two compaction knobs (KIP-58, KIP-354)
> remain config plumbing without an enforcing compactor — though the
> delete-policy retention cleaner now runs (gh #250).

## Implemented (14)

| KIP | What it is | kaas page |
|---|---|---|
| KIP-13 | Per-broker client quotas (byte-rate throttling) | [KIP-13](kip/kip-13.md) |
| KIP-98 | Exactly-once: idempotent producer + transactions | [KIP-98](kip/kip-98.md) |
| KIP-107 | `DeleteRecords` admin API (key 21) | [KIP-107](kip/kip-107.md) |
| KIP-195 | `CreatePartitions` admin API (key 37) | [KIP-195](kip/kip-195.md) |
| KIP-255 | SASL/OAUTHBEARER — OIDC JWTs validated against the issuer's JWKS | [KIP-255](kip/kip-255.md) |
| KIP-290 | Prefixed ACL resource patterns | [KIP-290](kip/kip-290.md) |
| KIP-339 | `IncrementalAlterConfigs` (key 44) | [KIP-339](kip/kip-339.md) |
| KIP-360 | Producer epoch bump on re-initialization | [KIP-360](kip/kip-360.md) |
| KIP-371 | mTLS principal mapping (`ssl.principal.mapping.rules`) | [KIP-371](kip/kip-371.md) |
| KIP-482 | Flexible versions + tagged fields | [KIP-482](kip/kip-482.md) |
| KIP-546 | Client-quota admin APIs (keys 48/49) | [KIP-546](kip/kip-546.md) |
| KIP-700 | `DescribeCluster` admin API (key 60) | [KIP-700](kip/kip-700.md) |
| KIP-800 | Join/leave reason strings | [KIP-800](kip/kip-800.md) |
| KIP-890 | Part 1: Produce verifies a transaction's first batch on a partition with its coordinator | [KIP-890](kip/kip-890.md) |

## Partial (11)

Each page leads with the "what's missing" block — these are the book's
credibility test.

| KIP | Landed | Missing | kaas page |
|---|---|---|---|
| KIP-32 | CreateTime timestamps round-trip byte-identically; batch `MaxTimestamp` tracked per segment | LogAppendTime entirely; timestamp→offset ListOffsets lookup (`(-1,-1)` sentinel) | [KIP-32](kip/kip-32.md) |
| KIP-58 | `min.compaction.lag.ms` config plumbing (CR → `.config.json` → DescribeConfigs) | the compactor that would enforce it; the delete-policy retention cleaner does run | [KIP-58](kip/kip-58.md) |
| KIP-101 | segment filenames carry the leader epoch | leader-epoch cache + lookup (`offset_for_leader_epoch` returns the `(-1,-1)` sentinel); wire key 23 unregistered | [KIP-101](kip/kip-101.md) |
| KIP-219 | `throttle_time_ms` computed (debt-carry) and returned | the broker never mutes the channel after responding — throttling relies on client cooperation | [KIP-219](kip/kip-219.md) |
| KIP-345 | `group.instance.id` plumbed through join/sync; static members survive the eviction sweep | `FENCED_INSTANCE_ID` fencing of duplicate static members | [KIP-345](kip/kip-345.md) |
| KIP-354 | `delete.retention.ms` config plumbing | tombstone-expiry enforcement (same missing compactor); upstream's `max.compaction.lag.ms` doesn't exist anywhere | [KIP-354](kip/kip-354.md) |
| KIP-368 | re-auth on a live connection; `session_lifetime_ms` advertised + deadline enforced on `oauth` listeners; same-principal guard | a broker-wide `connections.max.reauth.ms` for SCRAM/PLAIN sessions; disconnect (vs in-band 58) past the deadline | [KIP-368](kip/kip-368.md) |
| KIP-394 | `MEMBER_ID_REQUIRED` error code defined | the v4+ two-step join handshake — `join()` still takes the legacy assign-inline path | [KIP-394](kip/kip-394.md) |
| KIP-447 | `TxnOffsetCommit` staging, offset settlement in `EndTxn`, `TxnOffsetCommit` v3 on the wire | cross-broker completion, so offsets only apply when the group and transaction coordinators are the same broker; generation, member and producer-epoch checks on `TxnOffsetCommit` | [KIP-447](kip/kip-447.md) |
| KIP-516 | operator mints `Status.TopicID` (v4 UUID, never rotated) | broker-side wire propagation — the production topic watch inserts the all-zero sentinel, so Metadata serves nil topic IDs (tracked as gh #346) | [KIP-516](kip/kip-516.md) |
| KIP-554 | wire keys 50/51 served — describe answers from the live credential store, alter rotates via the `KafkaUser` CR | SCRAM-SHA-256 (kaas is SHA-512-only); wire-side credential *deletion* (refused — the lifecycle stays on the CR) | [KIP-554](kip/kip-554.md) |

## Deliberate non-goals (8)

Rationale for each in [Non-goals](non-goals.md).

| KIP | What it is | Why not |
|---|---|---|
| KIP-48 | Delegation tokens | no token-based auth surface; SCRAM/mTLS cover the deployment model |
| KIP-227 | Incremental fetch sessions | stateless by contract: `SessionID=0` on every response |
| KIP-405 | Tiered storage | deferred, not refused — the NFS substrate is already a near-tier |
| KIP-664 | Describe/ListTransactions tooling | follow-up; slot files are directly inspectable meanwhile |
| KIP-714 | Client metrics push | out of scope for the preview line |
| KIP-848 | Next-gen consumer rebalance protocol | post-3.7 |
| KIP-932 | Share groups (queues) | Kafka 4.0+ |
| KIP-1071 | Streams rebalance protocol | post-3.7 |

## Implementation notes (for contributors)

The missing compactor behind the KIP-58 and KIP-354 rows is tracked as
gh #158 — which now covers only the compactor: size- and time-based
retention are wired and enforced by the `RetentionCleaner`
(gh #250). The corrections in the note above come
from the 2026-07-19 source sweep, recorded on the affected KIP pages.
