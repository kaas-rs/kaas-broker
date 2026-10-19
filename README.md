# kaas

A from-scratch **Apache Kafka wire-compatible broker that runs on Kubernetes** — no KRaft,
no replication, no ZooKeeper. Kubernetes primitives (Leases, CRDs, a shared RWX volume) do
the heavy lifting; Apache Kafka clients (Java, librdkafka, franz-go) connect unchanged.

kaas targets **Apache Kafka 3.7** for wire-protocol and Kafka Streams parity. Behaviour is
verified against the matrix tracked in the
[kaas-migration-parity](https://github.com/users/Woestebanaan/projects/2) project board.
Current release line: `v0.2.x-preview`.

## Documentation

- **[The kaas book](https://kaas-rs.github.io/kaas-broker/)** — architecture (Part I is
  the authoritative architecture doc), Kafka compatibility (the generated API
  matrix + per-KIP status), code tour, operations. Source in [`docs/`](./docs/),
  rebuilt from `main` on every push; build locally with `cargo xtask docs` (or
  `--serve` for a live preview).
- **[kaas.rs](https://kaas.rs/)** — the project landing page, a separate site in
  [kaas-rs/kaas-landing-page](https://github.com/kaas-rs/kaas-landing-page).
- [`docs/RELEASING.md`](./docs/RELEASING.md) — tag-driven release procedure.

## Quickstart

Deploy with the Helm chart in [`deploy/helm/kaas/`](./deploy/helm/kaas/) — see its
[README](./deploy/helm/kaas/README.md) for values (listeners, storage substrate, CRD
handling):

```bash
helm install kaas oci://ghcr.io/kaas-rs/charts/kaas
```

Two images ship from this repo: the broker (`bins/kaas`) and the operator
(`bins/kaas-operator`), published to GHCR on every release tag.
