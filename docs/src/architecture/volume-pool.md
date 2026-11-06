# The volume pool: log dirs & placement

If you know Apache Kafka, you know its storage model: every broker owns
local disks, listed in `log.dirs`, and a partition's replicas live on
the specific brokers that host them. kaas keeps the Kafka protocol but
inverts that model. kaas brokers are (nearly) stateless processes on
Kubernetes: partition data lives on **shared `ReadWriteMany` volumes**
that every broker mounts, any broker can serve any partition, and
durability comes from the storage layer instead of replication (there
are no followers — see [the RWX substrate contract](./nfs-substrate.md)
for the ground rules that makes this safe).

Out of the box, all of that shared storage is **one** volume. The
volume pool lets you mount **several named volumes** and control, per
topic, which one holds its data. kaas deliberately describes this in
Kafka's own vocabulary: **one pool volume = one log dir** (the KIP-113
concept). `kafka-log-dirs.sh --describe` against a kaas cluster lists
every pool member, exactly as it would list a JBOD broker's disks.

## Why you would want more than one volume

- **Throughput, on cloud filers.** On the storage kaas targets in the
  cloud (FSx for NetApp ONTAP, Azure NetApp Files, Azure Files
  provisioned tiers), I/O budget is provisioned **per volume**. Brokers
  add CPU and network, never disk bandwidth — the pool is how the
  substrate itself scales.
- **Tiering.** Put source-of-truth topics on a premium volume and
  recreatable ones (Kafka Streams changelog/repartition topics) on a
  cheap one — the same reason Kafka users mix disk classes, expressed
  per topic instead of per broker.
- **Blast radius.** A volume that fills up or fails takes down the
  topics placed on it, not the cluster.

One property has no Apache Kafka equivalent: because every volume is
mounted by every broker, **placement and leadership are independent**.
Pinning a topic to one volume constrains where its *bytes* live, never
which broker leads it. JBOD on local disks can't do that.

## Declaring a pool

The pool is Helm chart configuration (`storage.pool[]`). Each member
becomes its own PVC, mounted on every broker and on the operator at
`/vols/<name>`:

```yaml
storage:
  className: nfs          # the default data volume (log dir "default")
  size: 100Gi
  pool:
    - name: bulk
      size: 500Gi
      className: standard-files   # per-member class = per-member substrate/QoS
      defaultEligible: true       # may receive topics with no explicit binding
      labels:
        class: standard           # matched by volumeSelector (below)
    - name: premium
      size: 100Gi
      className: premium-files
      defaultEligible: false      # reserved: only topics that ask for it
      labels:
        class: premium
      # cordoned: true            # drain mode: accepts no NEW placements
```

Things to know:

- The data volume is always a member, under the reserved name
  `default`. An empty `pool: []` is exactly the classic single-volume
  layout.
- Members are addressed by **name**, never by position — removing one
  never renumbers the rest.
- `defaultEligible: false` makes a member **reserved**: topics land on
  it only by naming or selecting it. This is what keeps auto-created
  topics (Streams creates repartition topics without asking you) off
  premium storage.
- Adding or changing members is a chart upgrade and therefore a rolling
  restart — a capacity operation, the same cadence as adding brokers.
  Creating a *topic* never waits on volume provisioning.
- Every member must satisfy the same
  [substrate contract](./nfs-substrate.md) (NFSv4-class semantics).
  Mixing, say, NFS and CephFS members is fine; a member that lies about
  fsync is not a slower tier, it is a correctness bug.

## Binding topics to volumes

kaas manages topics as Kubernetes custom resources (`KafkaTopic`) — the
Strimzi-style pattern; topics created over the Kafka Admin API get a CR
created for them. The binding is one optional field on the topic:

```yaml
apiVersion: kaas.rs/v1alpha1
kind: KafkaTopic
metadata:
  name: orders
spec:
  partitions: 12
  storage:
    volumes: [premium]        # pin: every partition on `premium`
```

Three shapes, one field:

- **Pin** — `volumes: [premium]`: hard isolation, one volume's budget.
- **Stripe** — `volumes: [bulk-1, bulk-2]`: partitions are spread
  round-robin across the set.
- **Unset** — the topic uses the *default set*: `default` plus every
  member with `defaultEligible: true`.

If you'd rather not hard-code infrastructure names into topic
definitions, select members by label instead (the `nodeSelector` idea,
applied to storage):

```yaml
spec:
  storage:
    volumeSelector:
      class: premium          # every key/value must match the member's labels
```

`volumes` and `volumeSelector` are mutually exclusive. A binding that
names an unknown member, or a selector that matches nothing, fails the
topic's reconcile loudly — `Ready=False` with reason
`InvalidVolumeBinding` — rather than silently placing data somewhere
you didn't intend.

## How placement behaves

Placement is decided **once, when a partition is created**, and
recorded in the topic's status:

```yaml
status:
  volumeAssignments:
    "0": premium
    "1": premium
    "2": premium
  partitionsOutsideSpec: 0
```

Editing the binding later **never moves data**. New partitions (from
expansion) follow the new set; existing partitions keep serving where
they are and are counted in `status.partitionsOutsideSpec` — visible
drift instead of surprise I/O. This is deliberate: kaas has no
replication layer to move data behind your back, so an inter-volume
move is a raw copy, and raw copies only happen when you ask for one
(next section).

Where the data actually is outranks the record. Placement is eventually
consistent: a broker routinely opens a partition before it has seen
`status.volumeAssignments`, and at boot it takes over partitions before
its topic watch has listed anything. So a broker opens the volume the
record names if that volume holds the partition's manifest; otherwise any
pool member that holds one; and only if none does, the volume the record
names, as a fresh partition. A record naming a member that no longer
exists falls back to `default`, so resolution can never make a partition
unopenable. Only an explicit move relocates bytes.

## Moving data: cordon & drain

Draining a member (to decommission it, or to move a hot topic) is a
three-step, explicitly operator-driven flow:

1. **Cordon** it (`cordoned: true` on the member, chart upgrade).
   From Kafka 4.3's vocabulary (KIP-1066): a cordoned log dir accepts
   no *new* partition placements — even from topics that name it —
   while existing partitions keep serving in place.
2. **Move the partitions off.** The convenient path is an annotation
   on each affected topic:

   ```bash
   kubectl annotate kafkatopic orders kaas.rs/migrate-to-volume=bulk
   ```

   Each broker then walks the partitions it leads through the move —
   close, copy to the target volume and fsync the copy, flip the placement
   record, reclaim the source — one partition every few seconds. The
   annotation is
   level-triggered and idempotent: partitions already on the target are
   skipped, and you remove the annotation once
   `status.volumeAssignments` shows the move complete. (The underlying
   Kafka API is `AlterReplicaLogDirs`, which you can also drive
   directly; the destination is the log-dir *path* as shown by
   `kafka-log-dirs.sh --describe`.)

   What clients see: producers and consumers of a partition get a brief
   retriable `LEADER_NOT_AVAILABLE` window while its files are copied —
   standard client retries absorb it. If the record flip fails, the
   copy is rolled back; data location and placement record never
   disagree.
3. **Remove the member** from `storage.pool[]` once it hosts nothing,
   and delete its PVC.

## Observing the pool

- `kafka-log-dirs.sh --describe` — every member with its partitions,
  and (v4 of the API, KIP-827) per-dir `TotalBytes` / `UsableBytes`.
- Metrics: `kaas.log.dir.total.bytes` and `kaas.log.dir.usable.bytes`,
  labelled per log dir — the pool's headroom on a dashboard.
- `kubectl get kafkatopics.kaas.rs -o wide` plus the status fields
  above for placement and drift.

## Choosing backends

- **FSx ONTAP / ANF** (incl. manual-QoS capacity pools): each member is
  an independently provisioned throughput budget — the pool multiplies
  substrate bandwidth. The target case.
- **EFS Elastic**: the filesystem scales its own throughput and bills
  per byte; a pool adds mounts and cost with no spreading gain — use
  the pool for *tiering* only.
- **CephFS (Rook)**: qualifies with no filer required; measure MDS and
  OSD-journal latency before promoting a member to a hot tier.
- **Single-filer homelab NFS**: members share one filer's budget; the
  pool is for layout/tier testing only.

## Implementation notes (for contributors)

The flow of placement truth, and where it lives in the source:

```text
operator reconciler: round-robin over the eligible set
  (crates/kaas-operator-controllers/src/kafkatopic_controller.rs)
  → KafkaTopic.status.volumeAssignments
    → broker topic watch (crates/kaas-k8s/src/kube_watchers.rs)
      → TopicRegistry, the engine's PlacementResolver
        (crates/kaas-broker/src/topic_registry.rs)
        → partition path resolution in the storage engine
          (crates/kaas-storage/src/disk.rs)
```

Segment files, the manifest, the producer-state snapshot, and the
recovery checkpoint live inside the partition directory and follow it
between volumes. Topic-level files (`.config.json`, the topic-identity
stamp) are written to every root hosting the topic's partitions, and
the topic-incarnation check and orphan sweep run per root — a deleted
topic is reclaimed wherever its partitions were placed.

The data-outranks-record precedence in `partition_dir` is gh #234: a
restart used to re-derive the assigned volume, find it empty, and bring
half a topic back as empty logs. gh #231 made the move's copy durable
before the source is deleted.
