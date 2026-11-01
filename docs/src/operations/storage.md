# Storage substrate requirements

What kaas demands of the shared volume: same-directory rename atomicity, fsync durability, and close-to-open consistency.

kaas has [no replication](../compat/non-goals.md) — durability is exactly
as good as the volume underneath it. That makes the storage substrate the
most important operational decision in a deployment.

## The three-property contract

Multi-broker kaas requires a `ReadWriteMany` volume with NFSv4-class
semantics. Each property is load-bearing in a specific place:

1. **Same-directory rename atomicity** — every metadata file
   (`manifest.json`, `assignment.json`, txn slot files, credentials) is
   written tmp + fsync + rename, then the directory is fsynced; a crash
   mid-write must leave either the old or the new file, never a torn one.
2. **Fsync durability** — the group-commit `sync_all()` is the `acks=all`
   promise ([storage hot path](../architecture/storage-hot-path.md)).
3. **Close-to-open consistency** — a file written and closed on one broker
   must read back complete on the next broker that opens it; transaction
   coordinator failover is literally "open the slot file"
   ([transactions](../architecture/transactions.md)).

Single-writer enforcement does **not** come from the filesystem — no
`flock()` needed. It comes from coordinator ownership plus epoch-prefixed
segment filenames.

## Provider matrix

| StorageClass | Status | Notes |
|---|---|---|
| CephFS (Rook / ceph-csi) | production | strong same-directory rename atomicity |
| csi-driver-nfs / NFSv4.1 server | production | see mount options below |
| AWS EFS / Azure Files Premium NFS / GCP Filestore | production | NFSv4-class semantics |
| Longhorn / OpenEBS RWX | production | block-backed RWX |
| local-path / hostPath | single-broker dev only | not RWX; requires `broker.replicaCount: 1` and `storage.accessMode: ReadWriteOnce` |

The single-broker RWO shape is a real configuration, not a hack — the chart
accepts it, and it sidesteps NFS entirely for edge/dev deployments.

## NFS mount options that matter

Set on the StorageClass (`mountOptions`), not the PVC:

```yaml
mountOptions:
  - nfsvers=4.1
  - nconnect=8    # parallel TCP connections; faster concurrent fsyncs
  - acregmax=1    # sub-second attribute-cache expiry
  - hard          # block on server unavailability instead of EIO
```

`acregmax=1` matters most: brokers poll `assignment.json`'s mtime as the
failover signal, and NFS's default 60 s attribute cache would delay every
controller failover by up to a minute. `nconnect` raises throughput when
multiple brokers fsync concurrently.

One reclaim-policy caution: keep the data PV on `reclaimPolicy: Retain`
(or the chart's kept PVCs — all three PVC classes carry
`helm.sh/resource-policy: keep`) — with `Delete` and a templated NFS
subdirectory, a PVC recreate can race the old PV's deletion into removing
the *new* volume's directory.

## More than one volume

The chart can split storage beyond the single data volume, and both
options follow the same substrate contract above:

- **A dedicated control-plane volume** (`storage.controlPlane.enabled`)
  moves the cluster-state directory — assignment, transaction slots,
  consumer offsets, credentials — onto its own PVC, so a full data
  volume cannot take the control plane down with it.
- **The volume pool** (`storage.pool[]`) declares additional named RWX
  volumes that topics can be placed on per-topic — Kafka's "log dirs",
  spread across volumes instead of local disks. See the
  [volume pool](../architecture/volume-pool.md) page for placement,
  selectors, and migration.

## The durability dial

`KAAS_FLUSH_INTERVAL_MESSAGES` (chart value `broker.flushIntervalMessages`)
defaults to **1**: every batch waits for its group-commit fsync — honest
`acks=all` against the substrate. Raising it (e.g. 10000) approximates
Apache's default posture, where `acks=all` acknowledges replicated
page-cache writes and `log.flush.interval.messages` is effectively
unbounded — comparable durability semantics to a single Apache broker.
The recorded benchmarks ([performance](./performance.md)) run the
default honest fsync (interval 1). NFS COMMIT latency dominates either
way; this dial decides how often you pay it.
