//! Byte-opacity tripwire counters.
//!
//! The broker is a byte mover, not a byte interpreter. Two invariants
//! protect that:
//!
//! - **Record decode** — no code path may parse an individual record out of
//!   a RecordBatch. The closest the codec gets is CRC verification over the
//!   opaque batch bytes in [`crate::crc`].
//! - **Batch re-encode** — no code path may rebuild a RecordBatch from
//!   decoded fields. Storage stores the wire bytes as-is; the response path
//!   emits them as-is.
//!
//! Either invariant being broken should fail loudly. These counters are the
//! pre-production trip mechanism: every offender calls
//! [`bump_codec_record_decode`] or [`bump_codec_batch_reencode`] with a
//! `site` string, and the integration tests in `crates/kaas-codec/tests/`
//! assert both counters read zero at the end of every run.
//!
//! In Phase 1 these are atomic counters. Phase 8 swaps them for OTLP
//! metric instruments behind the same function signature so production
//! alerts can fire on `kaas_codec_record_decode_total{site=...}` and
//! `kaas_codec_batch_reencode_total{site=...}`.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

static RECORD_DECODE: AtomicU64 = AtomicU64::new(0);
static BATCH_REENCODE: AtomicU64 = AtomicU64::new(0);

/// Forwarder installed once at boot by `kaas-observability::bootstrap`.
/// When `Some`, every [`bump_codec_record_decode`] /
/// [`bump_codec_batch_reencode`] call fires the OTel counters too
/// (via `kaas_observability::byteopacity::bump_codec_*`) so the
/// `KaasByteOpacityViolated` alert can trip on tripwires firing
/// in production — not just in the local process-atomic counter.
///
/// Kept as `Fn` pointers rather than a full trait object so the
/// upstream dependency (kaas-codec → kaas-observability) doesn't invert
/// the workspace dep graph. kaas-observability owns the concrete
/// implementation; kaas-codec only knows the signature.
pub type TripwireHook = fn(&'static str);

static RECORD_DECODE_HOOK: OnceLock<TripwireHook> = OnceLock::new();
static BATCH_REENCODE_HOOK: OnceLock<TripwireHook> = OnceLock::new();

/// Install the forwarders. Called once at boot from
/// `kaas_observability::bootstrap`. Subsequent calls are silently
/// ignored (the `OnceLock` semantics).
pub fn install_tripwire_hooks(record_decode: TripwireHook, batch_reencode: TripwireHook) {
    let _ = RECORD_DECODE_HOOK.set(record_decode);
    let _ = BATCH_REENCODE_HOOK.set(batch_reencode);
}

/// Bump the record-decode tripwire. **No production code path should ever
/// call this** — every increment is a bug. Fires the process-atomic
/// counter and, if `kaas-observability::bootstrap` ran, the OTel counter
/// via [`install_tripwire_hooks`].
pub fn bump_codec_record_decode(site: &'static str) {
    RECORD_DECODE.fetch_add(1, Ordering::Relaxed);
    if let Some(hook) = RECORD_DECODE_HOOK.get() {
        hook(site);
    }
}

/// Bump the batch-reencode tripwire. Same contract as
/// [`bump_codec_record_decode`].
pub fn bump_codec_batch_reencode(site: &'static str) {
    BATCH_REENCODE.fetch_add(1, Ordering::Relaxed);
    if let Some(hook) = BATCH_REENCODE_HOOK.get() {
        hook(site);
    }
}

/// Test-only readout of the record-decode counter. Production code has no
/// reason to inspect tripwires — the alerts fire off the OTLP exporter,
/// not off this getter.
pub fn record_decode_count() -> u64 {
    RECORD_DECODE.load(Ordering::Relaxed)
}

/// Test-only readout of the batch-reencode counter.
pub fn batch_reencode_count() -> u64 {
    BATCH_REENCODE.load(Ordering::Relaxed)
}

/// Test-only reset. Lets the integration tests start from a known baseline
/// even when run in random order alongside the meta-test.
pub fn reset_for_test() {
    RECORD_DECODE.store(0, Ordering::Relaxed);
    BATCH_REENCODE.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Meta-test: prove the bump function does increment the counter, so
    /// alerts wired against `kaas_codec_record_decode_total` will fire.
    /// This is the only test that calls `bump_*` by design — production
    /// code must never call it.
    #[test]
    fn bumps_are_observable() {
        reset_for_test();
        bump_codec_record_decode("meta_test");
        assert_eq!(record_decode_count(), 1);
        bump_codec_batch_reencode("meta_test");
        assert_eq!(batch_reencode_count(), 1);
    }
}
