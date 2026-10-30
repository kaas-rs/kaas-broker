# kaas-codec

The Kafka wire-protocol codec: frames, primitives, CRC32C, KIP-482 tagged fields, and per-API request/response types with the ApiSpec registry.

The wire boundary of the whole system. Everything here is pinned by fixture
tests: every registered API key/version encodes and decodes byte-identically
against captures from Apache Kafka 3.7.

**Module map**: `frame.rs` (length-prefixed framing, streaming
`FrameReader`), `primitives.rs` + `tagged.rs` (Kafka primitive types and
KIP-482 compact/tagged encoding), `headers.rs` (per-API header-version
resolution), `crc.rs` (CRC32C batch verification), `errors.rs`, one module
per API under `src/api/`, and `src/api/registry.rs` — the `ApiSpec` table.

**The invariant callers must hold**: RecordBatch payloads are byte-opaque.
There is no `Record` struct in this crate, and none may be added — batch
bytes travel as `Option<bytes::Bytes>`, and the only code reading past the
fixed v2 header is CRC verification (`crc.rs`). Any future violation must
bump the counters in `tripwires.rs`, which tests assert are zero. The full rationale is in
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
