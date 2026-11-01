# ACL & quota admin APIs

Per-API reference — see the [API support matrix](../api-matrix.md) for the generated version table.

kaas has no ACL store of its own: ACLs live **inline on each principal's
`KafkaUser` CR** (`spec.authorization.acls` — see
[Kubernetes integration](../../architecture/kubernetes.md)). The three ACL
admin APIs translate the AdminClient's int8-enum wire shape into that CR shape
and delegate to the ACL CR writer; the operator's reconcile then
rebuilds `/data/__cluster/acls.json` and every broker's ACL engine hot-reloads
it. Runtime edits to git-managed `KafkaUser` CRs will show up as ArgoCD drift
until the next sync — the intentional trade for letting the admin protocol
reach the canonical store. Without a writer wired (dev mode), DescribeAcls
returns an empty set and CreateAcls/DeleteAcls report per-entry success
**without persisting anything**.

One cross-cutting note: the trio is gated the way Apache gates it —
`DescribeAcls` requires `Describe` on the `Cluster` resource, and
`CreateAcls`/`DeleteAcls` require `Alter` on it; denial answers
`CLUSTER_AUTHORIZATION_FAILED` (31) before the CR writer is consulted, so
the answer is the same with or without an apiserver wired. The `host`
field of a binding is stored and round-tripped verbatim but ignored by
ACL evaluation.

## DescribeAcls

Lists ACL bindings matching a filter — `kafka-acls.sh --list`.

**Versions**: v0–v3 (flexible from v2).

**Handling**: the wire filter's `ANY`/`UNKNOWN` codes and null strings
collapse to wildcards; a `MATCH` pattern filter expands to literal + prefixed
per [KIP-290](../kip/kip-290.md); v0 (pre-KIP-290) pins the pattern filter to
`literal` so prefixed entries are never returned to a v0 client. The writer
lists every `KafkaUser` CR (skipping ones mid-deletion), expands each inline
ACL entry into one binding per operation, applies the filter, and the handler
folds the flat list back into Apache's per-resource shape — one resource row
per `(type, name, pattern)` with the matching ACLs inside. Filter errors
(resource types kaas can't express) answer `INVALID_REQUEST` (42); apiserver
failures answer `UNKNOWN_SERVER_ERROR` (-1).

**Deviations from Apache 3.7**:

- Resource types are limited to topic, group, cluster, and transactional-ID —
  `DELEGATION_TOKEN` and `USER` filters answer `INVALID_REQUEST` (42)
  (delegation tokens are a [non-goal](../non-goals.md)).
- Dev mode answers an empty list rather than an error.

**Source**: `crates/kaas-broker/src/handlers/acls.rs`,
`crates/kaas-broker/src/acl_cr_writer.rs`,
`crates/kaas-codec/src/api/acl_types.rs`,
`crates/kaas-codec/src/api/describe_acls.rs`.

**Verified by**: `scripts/kafka-acls.sh` (list/add/list/remove round trip
against a temporary KafkaUser); enum-translation and grouping unit tests in
`crates/kaas-broker/src/handlers/acls.rs`; filter-matching unit tests in
`crates/kaas-broker/src/acl_cr_writer.rs`.

## CreateAcls

Adds ACL bindings — `kafka-acls.sh --add`.

**Versions**: v0–v3 (flexible from v2).

**Handling**: per binding, the wire enums are validated (`ANY`/`UNKNOWN`
codes, and resource types kaas can't express, answer `INVALID_REQUEST` (42));
v0 bindings get `literal` pattern semantics. The principal must be of the form
`User:<name>` **and a `KafkaUser` CR with that name must already exist** —
kaas never auto-creates CRs from a runtime ACL write; both failures answer
`INVALID_REQUEST` (42). Creation is idempotent and coalescing: an existing
entry with the same resource, pattern, permission, and host absorbs the new
operation into its `operations` list (or no-ops when already present). The
write is a single `Update` with the read `resourceVersion`; a concurrent-edit
conflict surfaces as `UNKNOWN_SERVER_ERROR` (-1) and the AdminClient retries.

**Deviations from Apache 3.7**:

- Principals other than `User:` (e.g. `Group:`) are rejected — kaas maps
  principals 1:1 onto `KafkaUser` CRs.
- An ACL for a principal with **no KafkaUser CR is refused** (`INVALID_REQUEST`
  with `no KafkaUser CR for principal ...`); Apache accepts ACLs for arbitrary
  principal strings. Create the KafkaUser first.
- Dev mode reports success without persisting.

**Source**: `crates/kaas-broker/src/handlers/acls.rs`,
`crates/kaas-broker/src/acl_cr_writer.rs` (`create_acl`),
`crates/kaas-codec/src/api/create_acls.rs`.

**Verified by**: `scripts/kafka-acls.sh`; unit tests in
`crates/kaas-broker/src/handlers/acls.rs` and
`crates/kaas-broker/src/acl_cr_writer.rs` (principal parsing, enum mapping);
end-to-end ACL enforcement in `bins/kaas/tests/auth_smoke.rs`
(`acl_denies_unconfigured_topic`).

## DeleteAcls

Removes ACL bindings matching filters — `kafka-acls.sh --remove`.

**Versions**: v0–v3 (flexible from v2).

**Handling**: same filter translation as DescribeAcls (KIP-290 `MATCH`
expansion, v0 literal pinning). The writer walks every `KafkaUser` CR,
partitions each inline entry's operations into matched vs kept, rewrites the
CR when anything matched, and returns the flat list of removed bindings — one
per `(entry, operation)` pair — which the handler echoes as the per-filter
`matching_acls`. Entries whose operations are only partially matched are kept
with the remaining operations; entries emptied out are dropped. CRs
mid-deletion are skipped.

**Deviations from Apache 3.7**:

- Dev mode reports success with zero matches, without touching anything.

**Source**: `crates/kaas-broker/src/handlers/acls.rs`,
`crates/kaas-broker/src/acl_cr_writer.rs` (`delete_acls`),
`crates/kaas-codec/src/api/delete_acls.rs`.

**Verified by**: `scripts/kafka-acls.sh` (remove-and-verify scenario);
filter-partition unit tests in `crates/kaas-broker/src/acl_cr_writer.rs`.

## DescribeClientQuotas

Reads client quota entries ([KIP-546](../kip/kip-546.md)) —
`kafka-configs.sh --entity-type users --describe`.

**Versions**: v0–v1 (flexible from v1).

**Handling**: authorizes `DescribeConfigs` on the Cluster resource (Apache's
mapping for quota describe; denial → `CLUSTER_AUTHORIZATION_FAILED` (31)).
kaas supports a single entity axis: `user`. An exact-match component describes
that user; `ANY` (or an empty component list) lists every user with a quota.
Values resolve **runtime override first, CR-backed store second**: overrides
installed by [AlterClientQuotas](#alterclientquotas) shadow the quotas the
operator materialised into `/data/__cluster/credentials.json` from
`KafkaUser.spec.quotas`. Reported keys: `producer_byte_rate`,
`consumer_byte_rate`, `request_percentage`. With no quota enforcer wired
(auth disabled), the response is an empty success — indistinguishable on the
wire from "no quotas configured", mirroring Apache.

**Deviations from Apache 3.7**:

- Only the `user` entity axis exists. `client-id` / `ip` components, and the
  `DEFAULT` match type (`<default>` user entity), return an **empty result**
  rather than an error — kaas users are CR-instantiated, so there is no
  default entity.
- Quota values are **per-broker** ([KIP-13](../kip/kip-13.md)) — same
  semantics as Apache, but worth restating: with N brokers the cluster-wide
  ceiling is N × the reported value. The CR field names
  (`producerMaxByteRatePerBroker`) say so explicitly; the wire keys keep
  Apache's names.

**Source**: `crates/kaas-broker/src/handlers/describe_client_quotas.rs`,
`crates/kaas-auth/src/quota.rs` (`describe_user_quota`, `list_user_quotas`).

**Verified by**: `scripts/kafka-configs.sh` (quota scenarios 6–9); resolution-
order unit tests in `crates/kaas-auth/src/quota.rs`
(`describe_user_quota_resolution_order`); and
`crates/kaas-broker/tests/client_quotas.rs`, which builds the broker the way
`bins/kaas` does, so it catches the admin surface losing the enforcer — it did,
from the Rust cutover until gh #260.

## AlterClientQuotas

Sets or removes client quota values ([KIP-546](../kip/kip-546.md)) —
`kafka-configs.sh --entity-type users --alter`.

**Versions**: v0–v1 (flexible from v1).

**Handling**: authorizes `AlterConfigs` on the Cluster resource once for the
whole request (denial → per-entry `CLUSTER_AUTHORIZATION_FAILED` (31)). Each
entry must name exactly one `user` entity with an explicit name — anything
else answers `INVALID_REQUEST` (42). Ops merge onto the user's current
effective quotas with Apache semantics: a set replaces just the named key, a
remove drops just that key, unspecified keys are preserved. Supported keys are
`producer_byte_rate`, `consumer_byte_rate`, and `request_percentage`; an
unknown key answers `INVALID_CONFIG` (40). The merged result is installed as a
**runtime override** on the quota enforcer, live-updating any active token
bucket; a merge that empties every field clears the override, reverting the
user to the CR-backed value. With no enforcer wired (auth disabled) each entry
answers `UNSUPPORTED_VERSION` (35). `validate_only` skips the install.

**Deviations from Apache 3.7**:

- **Alterations are not persisted.** The override lives in the enforcer's
  memory: it does not write back to the `KafkaUser` CR, it is lost on broker
  restart, and it applies **only on the broker that served the request** —
  peers keep the store-backed value. Durable, cluster-wide quotas belong on
  `KafkaUser.spec.quotas` (see
  [Kubernetes integration](../../architecture/kubernetes.md)). Treat this API
  as a live-tuning knob, not a store.
- `request_percentage` is accepted, stored, and reported, but nothing
  enforces it — kaas throttles produce/fetch byte rates only, with no
  request-time CPU quota.
- Entity axes other than a single named `user` are rejected (`INVALID_REQUEST`),
  including the `<default>` entity.

**Source**: `crates/kaas-broker/src/handlers/alter_client_quotas.rs`,
`crates/kaas-auth/src/quota.rs` (`set_user_quota`).

**Verified by**: `scripts/kafka-configs.sh` (alter/describe/clear round trip);
`alter_changes_the_limit_that_throttles` in
`crates/kaas-broker/tests/client_quotas.rs` (the override reaches the instance
on the throttling path); `set_user_quota_live_updates_existing_bucket` and the debt-carry contention
test in `crates/kaas-auth/src/quota.rs`; enforcement end-to-end in
`bins/kaas/tests/auth_smoke.rs` (`produce_exceeds_quota_returns_throttle`).

## DescribeUserScramCredentials

Lists which SCRAM mechanisms a user has credentials for —
`kafka-configs.sh --describe --entity-type users` and the AdminClient's
`describeUserScramCredentials()` ([KIP-554](../kip/kip-554.md)).

**Versions**: v0 (flexible from v0).

**Handling**: authorize `Describe` on the cluster (denial → top-level
`CLUSTER_AUTHORIZATION_FAILED` (31)), then answer from the live credential
store — the operator-materialised `credentials.json`, hot-reloaded, so the
response reflects the store the SCRAM authenticator actually verifies
against. A null or empty `users` array describes every user with SCRAM
credentials (an empty one described nobody until gh #276);
a named user without any answers a per-user `RESOURCE_NOT_FOUND` (91); a
user named twice answers `DUPLICATE_RESOURCE` (92) — all Apache's shapes
(both codes went out as 83 / 81 until gh #276).
Only mechanism + iteration count are reported, never salts or keys.

**Deviations from Apache 3.7**:

- Every credential reports mechanism `SCRAM-SHA-512` — kaas serves no
  SCRAM-SHA-256, so a user never has more than one credential entry.

**Source**: `crates/kaas-broker/src/handlers/describe_user_scram_credentials.rs`,
`crates/kaas-codec/src/api/describe_user_scram_credentials.rs`,
`crates/kaas-auth/src/credentials.rs` (`list_all_scram_users`).

**Verified by**: codec round-trip tests (null-vs-empty users pinned) in
`crates/kaas-codec/src/api/describe_user_scram_credentials.rs`; handler tests
in `describe_user_scram_credentials.rs` and `alter_user_scram_credentials.rs`
that assert Apache's literal codes (91, 92, 93, 33, 35 — gh #276). The
user-describe lines in `scripts/kafka-configs.sh` only grep quota output, so
they don't check these codes.

## AlterUserScramCredentials

Rotates a user's SCRAM credential over the wire —
`kafka-configs.sh --alter --entity-type users --add-config 'SCRAM-SHA-512=...'`
([KIP-554](../kip/kip-554.md)).

**Versions**: v0 (flexible from v0).

**Handling**: authorize `Alter` on the cluster (denial → per-user
`CLUSTER_AUTHORIZATION_FAILED` (31)). The wire carries pre-salted material —
`(salt, saltedPassword, iterations)`, never the password — and the broker
derives the RFC 5802 stored/server keys and patches them into
`KafkaUser.spec.authentication.scram`. The operator materialises the change
into `credentials.json` on reconcile and every broker hot-reloads it, so
the rotation is **asynchronous** (typically a few seconds) and cluster-wide.
The CR must already exist (`RESOURCE_NOT_FOUND` (91) otherwise) and its
`authentication.type` must be `scram-sha-512` or unset — rotating a `tls`
user's SCRAM credential would silently flip its auth mechanism, so that
answers `INVALID_REQUEST` (42). Iterations below 4096 or empty
salt/saltedPassword answer `UNACCEPTABLE_CREDENTIAL` (93).

**Deviations from Apache 3.7**:

- **SCRAM-SHA-512 only** — SHA-256 upsertions answer
  `UNSUPPORTED_SASL_MECHANISM` (33).
- **Deletions are refused** with `UNSUPPORTED_VERSION` (35): the credential
  lifecycle belongs to the `KafkaUser` CR (delete the CR or change its
  `authentication.type`). The operator would re-materialise anything the
  broker removed, and a deletion that silently comes back is worse than a
  refusal.
- The rotation is visible to SCRAM handshakes only after the operator
  reconcile plus the brokers' credential reload (~10 s worst case), not
  atomically with the response.

**Source**: `crates/kaas-broker/src/handlers/alter_user_scram_credentials.rs`,
`crates/kaas-broker/src/user_cr_writer.rs`,
`crates/kaas-codec/src/api/alter_user_scram_credentials.rs`,
`crates/kaas-auth/src/scram.rs` (`keys_from_salted_password`).

**Verified by**: handler tests (key derivation, mechanism/iteration
rejection, deletion refusal) in
`crates/kaas-broker/src/handlers/alter_user_scram_credentials.rs`; codec
round-trips in `crates/kaas-codec/src/api/alter_user_scram_credentials.rs`.
