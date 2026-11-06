# KafkaUser

`KafkaUser` declares a principal: how it authenticates, what it may do
(inline ACLs), and how much throughput it gets (quotas). The
`spec.authentication` / `spec.authorization` shape mirrors Strimzi's
`KafkaUser` 1:1, so Strimzi manifests port over nearly unchanged — the
deliberate divergences are called out below.

In Apache Kafka the same ground is covered by three separate surfaces
(SCRAM credentials in cluster metadata, `kafka-acls.sh`,
`kafka-configs.sh --entity-type users` for quotas); here one CR per
principal carries all three, and the operator materializes them into
the credential and ACL files brokers hot-reload.

## A SCRAM user with ACLs and quotas

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaUser
metadata:
  name: orders-service
  namespace: kafka
spec:
  authentication:
    type: scram-sha-512
  authorization:
    type: simple
    acls:
      - resource:
          type: topic
          name: orders
          patternType: literal
        operations: [Read, Write, Describe]
      - resource:
          type: group
          name: orders-
          patternType: prefix
        operations: [Read]
  quotas:
    producerMaxByteRatePerBroker: 1048576
    consumerMaxByteRatePerBroker: 2097152
```

The principal name is `User:<metadata.name>` — here `User:orders-service`.

## `spec.authentication`

| `type` | Meaning |
|---|---|
| `scram-sha-512` | SASL/SCRAM credential. With no `password` ref, the operator generates a stable 32-char password and publishes it in the output Secret `<user>-kafka-credentials` (named in `status.secret`). Point `password.name`/`password.key` at your own Secret to bring a password instead; the output Secret is still written, holding that password. A pre-derived `scram` block skips it. |
| `tls` | mTLS principal. The operator stamps the certificate CN from `certificateRef` as the principal. |
| `kubernetes-serviceaccount` | ServiceAccount-JWT principal via `serviceAccountRef`. |

Only SCRAM-SHA-**512** exists — there is no SCRAM-SHA-256 anywhere in
kaas, so clients must say `sasl.mechanism=SCRAM-SHA-512`.

The nested `scram` block (salt, storedKey, serverKey, iterations) is a
pre-derived credential in RFC 5802 terms. You normally never write it:
it is the storage form used by the wire-level SCRAM admin API
(`kafka-configs.sh --alter --entity-type users`, KIP-554) when a
credential is rotated at runtime — the broker patches it into this CR,
keeping the CR the single source of truth for the credential
lifecycle.

### Authorization-only users (no `authentication`)

`spec.authentication` is optional. A principal that authenticates
out-of-band — an OAuth client whose JWT is validated against the
issuer's JWKS on an `oauth` listener — has no credential for the
operator to materialize. Its CR names the principal via
`metadata.name` (the token's `sub` claim) and carries only
`authorization` and/or `quotas`:

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaUser
metadata:
  name: analytics-pipeline        # = the JWT `sub`
  namespace: kafka
spec:
  authorization:
    type: simple
    acls:
      - resource: {type: topic, name: metrics, patternType: prefix}
        operations: [Read, Describe]
```

This mirrors Strimzi, whose oauth users are authorization-only too.

## `spec.authorization`

`type: simple` (the only authorizer today; the field exists for
forward compatibility). ACLs are **inline on the user** — there are no
separate ACL or user-group CRs. To grant the same rule to N
principals, repeat it on N CRs; that is the standard Strimzi-pattern
trade of greppability over indirection.

Each ACL entry:

| Field | Values | Default |
|---|---|---|
| `resource.type` | `topic`, `group`, `cluster`, `transactionalId` | required |
| `resource.name` | resource name, or prefix when `patternType: prefix`; `*` for all | required |
| `resource.patternType` | `literal`, `prefix` | `literal` |
| `operations` | Apache operation names: `Read`, `Write`, `Create`, `Delete`, `Describe`, `Alter`, `All`, … | required, min 1 |
| `type` | `allow`, `deny` | `allow` |
| `host` | source-IP filter | any. **Stored but not enforced** — only "any host" is evaluated today. |

Evaluation follows Apache semantics: deny beats allow, no matching ACL
means deny (unless the principal is a super-user, or authorization is
disabled cluster-wide). Wire-level `kafka-acls.sh --add/--remove`
works too, and edits the `acls` list of the — necessarily existing —
matching `KafkaUser` CR.

## `spec.quotas`

The **one deliberate naming divergence from Strimzi**:

| kaas field | Strimzi field | Why renamed |
|---|---|---|
| `producerMaxByteRatePerBroker` | `producerByteRate` | Kafka quotas are enforced **per broker** (KIP-13): with N brokers the effective cluster-wide ceiling is N × the value. Strimzi's name reads cluster-wide; the kaas name says what actually happens. |
| `consumerMaxByteRatePerBroker` | `consumerByteRate` | Same. |
| `requestPercentage` | `requestPercentage` | Unchanged (0–100). Stored and reported, but not enforced. |

The byte-rate semantics are identical to Strimzi/Apache — only the names
differ. They are enforced whether or not authorization is enabled; the two are
orthogonal axes.

## Status

| Field | Meaning |
|---|---|
| `secret` | Name of the output credentials Secret, for SCRAM users without a pre-derived `scram` block. |
| `conditions` | `Ready`; a user referencing a missing password Secret parks unready until the Secret appears. |

`kubectl get kafkausers` prints `Auth type` and `Ready`.

## Implementation notes (for contributors)

- Type: `crates/kaas-operator-api/src/kafkauser.rs`; generated schema
  `deploy/crds/kaas.rs_kafkausers.yaml`. ACL-shape defaults are
  operator-side, not apiserver-side (gh #137).
- Reconciler materializes to `/data/__cluster/credentials.json`
  (upsert) + `acls.json` (rebuilt from all users); broker hot-reload
  lives in `crates/kaas-auth/src/`.
- The Strimzi-shape surface landed in gh #135 (which removed the old
  `KafkaACL`/`KafkaUserGroup` CRs); optional authentication is gh #42;
  KIP-554 rotation writes through
  `crates/kaas-broker/src/user_cr_writer.rs` (gh #252); ACL admin
  writes through `crates/kaas-broker/src/acl_cr_writer.rs` — which
  deliberately never stamps ArgoCD metadata (see [Kubernetes
  integration](../../architecture/kubernetes.md)).
