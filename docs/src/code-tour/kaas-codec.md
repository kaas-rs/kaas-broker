# kaas-codec

The Kafka wire-protocol codec: frames, primitives, CRC32C, KIP-482 tagged fields, and per-API request/response types with the ApiSpec registry.

The wire boundary of the whole system. Every API module round-trips its
supported versions through its own encoder and decoder, and one module
(`leave_group.rs`) also decodes a captured Java-client request. Round
trips can't catch a bug the encoder and decoder share, which is how every
legacy-version response carried a stray header byte until gh #272. Header
versions are now pinned against Apache's values instead
(`registry.rs`'s `header_versions_match_apache`), and the
`bins/kaas/tests/*` smoke suites exercise a real socket.

**Module map**: `frame.rs` (length-prefixed framing, streaming
`FrameReader`), `primitives.rs` + `tagged.rs` (Kafka primitive types and
KIP-482 compact/tagged encoding), `headers.rs` (request/response
header shapes; which one applies is `ApiSpec::request_header` /
`response_header` in the registry, derived from the flexible range),
`crc.rs` (CRC32C batch verification), `errors.rs`, one module
per API under `src/api/`, and `src/api/registry.rs` — the `ApiSpec` table.

**The invariant callers must hold**: RecordBatch payloads are byte-opaque.
There is no `Record` struct in this crate, and none may be added — batch
bytes travel as `Option<bytes::Bytes>`, and the only code reading past the
fixed v2 header is CRC verification (`crc.rs`). `tripwires.rs` has
counters a violating path is meant to bump, but nothing would make it, so
the byte-identity tests are what catch a change to the bytes (gh #275).
The full rationale is in
[Wire protocol & framing](../compat/wire-protocol.md).

**The registry is load-bearing**: `src/api/registry.rs::ALL` (40 entries,
count-asserted by a unit test) drives the ApiVersions response, the
header-version lookup, *and* the book's
[generated API matrix](../compat/api-matrix.md) — adding an API without a
registry row is structurally impossible to ship quietly.

**Where the boundary sits**: kaas-codec knows nothing about storage,
Kubernetes, or request handling — its only workspace consumers are
`kaas-protocol` (framing/dispatch) and the handler layer above.

**Start reading at** `src/api/registry.rs`, then one small API module
(`src/api/api_versions.rs`) end to end.
