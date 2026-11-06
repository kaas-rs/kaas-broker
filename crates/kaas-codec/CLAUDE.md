# kaas-codec

**Book:** `docs/src/compat/wire-protocol.md`, `docs/src/compat/api-matrix.md`, `docs/src/code-tour/kaas-codec.md`. Read them for the design, and update them in the same commit when behaviour here changes.

**Header versions are derived, not written per API** (`ApiSpec::request_header` / `response_header`, from `min_flexible`; ApiVersions' response header is always v0): the per-module copies once gave every legacy-version response a stray tagged-fields byte (gh #272). Per-module tests are round trips through kaas's own encoder and decoder, which can't see a bug the two share — pin a wire claim against Apache's values or a real socket. Byte opacity has no counter or alert (gh #275) — no record type plus the byte-identity tests hold it.
