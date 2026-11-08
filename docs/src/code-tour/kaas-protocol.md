# kaas-protocol

Multi-listener TCP/TLS bring-up, the per-listener pre-auth dispatch gate, framing, and per-connection state.

The layer between the codec and the handlers: it owns sockets, connection
lifecycle, and the decision of *whether a request may be dispatched at all*.

**Module map**: `server.rs` (multi-listener TCP/TLS accept loops —
TCP_NODELAY on accept, mTLS principal-mapper wiring), `tls.rs` (a
listener's TLS config loaded from its files and reloaded when they change,
gh #343), `frame.rs`
(`Connection<S>`: async stream + frame reader, request headers parsed via
the codec registry, responses written with the right header version),
`dispatch.rs` (the API-key router), `connstate.rs` (per-connection mutable
state: listener name, SASL progress, principal).

**The pre-auth gate** is the crate's most consequential logic: on an
authenticated listener, `dispatch.rs` refuses every API except
SaslHandshake (17), ApiVersions (18), and SaslAuthenticate (36) until the
connection's SASL exchange completes. The same gate enforces the KIP-368
re-authentication deadline — past the session lifetime it answers every
request with error 58 until the client re-authenticates. Listener identity
travels on
`ConnState` as a free-form name matching the chart's `listeners[]` entries;
the auth engine is selected per listener
([architecture chapter](../architecture/listeners-auth.md)).

**Error contract**: a request that fails dispatch-level checks gets a
proper error-code response with the client's correlation ID — connections
are not dropped for policy failures.

**Where the boundary sits**: kaas-protocol depends on `kaas-codec`,
`kaas-auth`, `kaas-observability` and, only for `StorageError`, `kaas-storage`;
it knows nothing about Kubernetes. Handlers are
registered into the dispatcher by `bins/kaas` — the crate defines the
`Handler` seam, not the handlers.

**Start reading at** `dispatch.rs`, then `server.rs` for the listener
bring-up.
