# Introduction

kaas is a from-scratch, Apache Kafka 3.7 wire-compatible broker built to
run on Kubernetes — clients and tools connect unchanged, while Kubernetes
primitives and a shared filesystem replace Kafka's own distributed-systems
machinery.

If you know Apache Kafka, you already know how to use kaas. Java clients,
librdkafka, franz-go, Kafka Streams applications, and the stock shell
tools (`kafka-topics.sh`, `kafka-console-producer.sh`, …) all talk to it
as if it were a Kafka 3.7 cluster. What changes is everything behind the
socket — and this book is organised around exactly that: what stays the
same (the wire contract, [proven in Part II](compat/wire-protocol.md))
and what is replaced (the machinery, [explained in Part
I](architecture/overview.md)).

## One idea, three substitutions

Kafka's hardest operational problems — quorums, replication, partition
movement — follow from one assumption: brokers own local disks and must
protect the data on them. kaas drops that assumption. Brokers are
(nearly) stateless pods; partition data lives on shared `ReadWriteMany`
volumes that every broker mounts; durability is the storage layer's job
and coordination is Kubernetes' job. That single move replaces three
pieces of Kafka machinery, and the whole book keeps referring back to
them:

1. **KRaft (or ZooKeeper) → a Kubernetes Lease.** The controller is
   whichever broker currently holds the `kaas-controller` Lease. There
   is no quorum and no replicated state machine; the Kubernetes API
   server is the metadata store.
2. **Replication & ISR → a single writer per partition.** Exactly one
   broker leads — and writes — each partition at a time, fenced by
   epochs so a deposed leader cannot corrupt the log. There are no
   followers: `acks=all` is an fsync to the shared volume, not N
   replicas, and a broker "takes over" a partition by opening its files,
   not by copying data.
3. **Internal topics → files on the shared volume.** Consumer offsets
   (`__consumer_offsets`) and transaction state (`__transaction_state`)
   are plain JSON files in a cluster-state directory instead of
   replicated internal topics.

Each substitution is a real trade. [Non-goals](compat/non-goals.md)
states plainly what you give up; Part I walks through how each
replacement works and why it is safe on the storage kaas targets.

## A translation table

Where the things you manage in Kafka live in kaas:

| In Apache Kafka | In kaas |
|---|---|
| controller quorum (KRaft) | the `kaas-controller` Kubernetes Lease |
| replication factor & ISR | single writer per partition; durability from the RWX substrate |
| `__consumer_offsets` | per-group JSON files on the shared volume |
| `__transaction_state` | slot-sharded JSON files on the shared volume |
| topic management (Admin API, `kafka-topics.sh`) | works unchanged — materialised as `KafkaTopic` custom resources |
| SCRAM users, ACLs, quotas | `KafkaUser` custom resources (the Strimzi pattern) |
| `log.dirs` / JBOD | the [volume pool](architecture/volume-pool.md): named RWX volumes, mounted by every broker |
| replacing a broker = re-replicating its data | rescheduling a pod; takeover is a file open |

## What ships

Two binaries and a Helm chart: the broker, an operator that reconciles
the custom resources into on-disk state and Kubernetes plumbing (and
stays entirely off the produce/fetch hot path), and the chart that
deploys both. The release line is `v0.3.x-preview` — pre-v1, kaas makes
no backwards-compatibility promises between releases; see
[Releasing](operations/releasing.md) for the exact upgrade contract.

## The parity target

kaas targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams
parity — and the book is structured to *prove* that claim rather than
assert it. [Part II](compat/wire-protocol.md) carries a
[generated API matrix](compat/api-matrix.md) that CI pins to the actual
wire surface, a [KIP index](compat/kip-index.md) that says implemented /
partial / non-goal honestly, and a
[verification story](compat/verification.md) built on Apache's own shell
tools, run against a live cluster.

## How to read this book

- **Evaluating kaas?** Read this page, then
  [Non-goals](compat/non-goals.md) — the fastest honest answer to "can
  it replace my cluster" — then [Getting
  Started](getting-started.md) to try it.
- **Operating it?** [Getting Started](getting-started.md), then Part I
  in order starting at the [system overview](architecture/overview.md),
  then Part IV for the [chart](operations/helm.md) and the
  [storage substrate requirements](operations/storage.md) your filer
  must meet.
- **Contributing?** Parts I and II are the semantics; Part III is the
  [crate-by-crate tour](code-tour/workspace.md) of where they live in
  the source.
