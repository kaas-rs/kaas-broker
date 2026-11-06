# kaas-observability

**Book:** `docs/src/architecture/observability.md`, `docs/src/code-tour/kaas-observability.md`. Read them for the design, and update them in the same commit when behaviour here changes.

There is no trace pipeline: it never had a span to export and was removed (gh #269).

The retention cleaner was dead code for a long time, and its metrics still passed the wire-up audit (gh #250).

**The metrics audit did not catch this, and won't catch the next one.** `cleaner_runs` / `cleaner_bytes_reclaimed` sat in `EXPECTED_WIRED` (`crates/kaas-observability/tests/wire_up_audit.rs`) the whole time it was dead code — the audit greps for *call sites*, and `cleaner.rs` had them; nothing checked that anything ever called `cleaner.rs`. A metric can be "wired" and still be structurally unreachable.
