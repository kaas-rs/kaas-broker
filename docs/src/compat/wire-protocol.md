# Wire protocol & framing

Length-prefixed frames, KIP-482 flexible versions with tagged fields, and byte-opaque RecordBatch handling — the codec layer under every parity claim in this part of the book.

Point an unmodified Kafka client at kaas — Java, librdkafka, franz-go,
the stock shell tools — and everything it sends and receives passes
through the codec described here. The codec's standing claim: every
registered API key and version encodes and decodes **byte-identically**
against fixtures captured from Apache Kafka 3.7. The rest of Part II
substantiates compatibility surface by surface; this page covers the
layer all of those surfaces share.

## Framing and headers

Kafka's transport is length-prefixed frames: a 4-byte big-endian size,
then the message. Inside a frame:

- **Request header** — API key, API version, correlation ID, client ID,
  and (for flexible versions) tagged fields.
- **Response header** — correlation ID, plus tagged fields when
  flexible.

One subtlety Kafka clients normally hide from you: the header's *own*
version depends on `(api key, api version)`. kaas derives it from each
registry entry's flexible-from version, the same rule Apache's generated
code applies: tagged fields on a flexible version and none on a legacy
one. ApiVersions is the one exception: its response header is always v0.
Until gh #272 each API module carried its own header functions, and every
legacy-version response gained a tagged-fields byte that a client
negotiating that version would misread.

## KIP-482: flexible versions and tagged fields

From a per-API cutover version, Kafka switches to "flexible" encoding:
compact strings and arrays (varint lengths) and **tagged fields** — an
extensible `(tag, size, bytes)` section that lets brokers and clients
attach optional data without a version bump.

Which version each API flips to flexible is data, not code: the
"Flexible" column in the [generated API matrix](api-matrix.md) comes
straight from the codec registry, and the same registry table drives the
ApiVersions response — the matrix cannot disagree with what the broker
advertises.

## The byte-opacity contract

There is **no `Record` struct in kaas** — deliberately. RecordBatch
payloads flow through the codec as opaque bytes: a zero-copy slice of
the request frame on the way in, the same bytes off disk on the way out.
The only code that reads past the fixed v2 batch header is CRC
verification (CRC32C over opaque input).

Consequences worth internalizing:

- The log file *is* the wire format — Produce appends the client's bytes
  verbatim (base offset rewritten in place), Fetch returns them verbatim
  (see [Storage engine hot path](../architecture/storage-hot-path.md)).
- Per-record features (timestamps, headers, per-record validation) are
  honoured **byte-opaquely**: kaas preserves what producers wrote
  without interpreting it. Where Apache applies per-record semantics —
  e.g. tombstone expiry during compaction — kaas will apply the
  batch-level equivalent (the compactor itself is not yet implemented).
- The contract is enforced, not aspirational: tripwire counters
  (surfaced as metrics — see
  [Observability](../architecture/observability.md)) must read zero
  after every test run, and any future record-decoding code path is
  required to bump them, making the first violation loud.

## Where to dig deeper

- [API support matrix](api-matrix.md) — every served key, generated from
  the registry.
- [Per-API reference](api-reference.md) — semantics and deviations per
  key.
- [KIP index](kip-index.md) — protocol-evolution KIPs and where kaas
  stands on each.
- [Non-goals](non-goals.md) — the keys and features that are absent on
  purpose.
- [Verification story](verification.md) — the suites that would catch
  any of this being wrong.

## Implementation notes (for contributors)

Everything wire-level lives in `crates/kaas-codec`:

- `crates/kaas-codec/src/frame.rs` — frame reader/writer, plus the
  streaming `FrameReader` used by the server accept loop.
- `crates/kaas-codec/src/headers.rs` — per-API `HeaderVersion`
  resolution.
- `crates/kaas-codec/src/tagged.rs` — the KIP-482 tagged-field envelope;
  `crates/kaas-codec/src/primitives.rs` — compact strings, arrays, and
  the other wire primitives.
- `crates/kaas-codec/src/crc.rs` — CRC32C over opaque batch bytes.
- `crates/kaas-codec/src/api/` — one request/response module per API;
  `crates/kaas-codec/src/api/registry.rs` — the `ApiSpec` table that
  drives both the ApiVersions response and the generated matrix.
- `crates/kaas-codec/src/tripwires.rs` — the byte-opacity counters,
  asserted zero after real traffic by `bins/kaas/tests/byte_opacity.rs`.
