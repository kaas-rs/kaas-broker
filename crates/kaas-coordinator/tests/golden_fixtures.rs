//! gh #172 — round-trip v0.1-written slot + fence-log files through
//! the Rust serde types and assert byte equality.
//!
//! The fixtures under `tests/fixtures/` were captured from a live
//! v0.1 broker. They exercise the
//! shapes Phase 6 cares about:
//!
//! - `txn_state/slot-9.json`  — single entry, `CompleteCommit`,
//!   rejoin (epoch=1).
//! - `txn_state/slot-25.json` — single entry, `Ongoing`, with
//!   `partitions`, `ongoingSinceMs`, `transactionTimeoutMs`, and
//!   `groups` all populated. The "everything present" case.
//! - `txn_state/slot-40.json` — single entry, `Empty` (state field
//!   omitted), only `transactionTimeoutMs` recorded. The
//!   `omitempty`-heavy case.
//! - `txn_state/slot-48.json` — single entry, `CompleteAbort`.
//! - `fence_log/from-kaas-0.json` — stringified `pid → epoch` map.
//!
//! Each test loads the captured bytes, deserializes via
//! [`serde_json`], re-serializes, and asserts the output is
//! byte-identical to the input. Any drift in field order or
//! `skip_serializing_if` behaviour fails the test — that's the
//! catch the [gh #169] hedge wants before Phase 9 cutover.
//!
//! [gh #169]: https://github.com/kaas-rs/kaas-broker/issues/169
//!
//! Regenerating: `cd archive && go run ./cmd/capture-txn-fixtures
//! ../crates/kaas-coordinator/tests/fixtures/` overwrites every
//! committed fixture.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use kaas_coordinator::TxnEntry;

fn roundtrip_slot(name: &str, bytes: &[u8]) {
    let state: HashMap<String, TxnEntry> =
        serde_json::from_slice(bytes).unwrap_or_else(|e| panic!("{name}: deserialize: {e}"));
    let reencoded =
        serde_json::to_vec(&state).unwrap_or_else(|e| panic!("{name}: re-serialize: {e}"));
    if bytes != reencoded.as_slice() {
        let got = std::str::from_utf8(&reencoded).unwrap_or("<non-utf8>");
        let want = std::str::from_utf8(bytes).unwrap_or("<non-utf8>");
        panic!("{name}: serde round-trip drift\n  go-side: {want}\n  rust-side: {got}");
    }
}

#[test]
fn slot_9_complete_commit_round_trips() {
    let bytes = include_bytes!("fixtures/txn_state/slot-9.json");
    roundtrip_slot("slot-9.json", bytes);
}

#[test]
fn slot_25_ongoing_with_groups_round_trips() {
    let bytes = include_bytes!("fixtures/txn_state/slot-25.json");
    roundtrip_slot("slot-25.json", bytes);
}

#[test]
fn slot_40_bare_timeout_only_round_trips() {
    let bytes = include_bytes!("fixtures/txn_state/slot-40.json");
    roundtrip_slot("slot-40.json", bytes);
}

#[test]
fn slot_48_complete_abort_round_trips() {
    let bytes = include_bytes!("fixtures/txn_state/slot-48.json");
    roundtrip_slot("slot-48.json", bytes);
}

#[test]
fn fence_log_round_trips() {
    let bytes = include_bytes!("fixtures/fence_log/from-kaas-0.json");
    // Fence log shape: `{ stringified-pid → epoch }`. No struct
    // ordering issue possible; serde_json preserves insertion
    // order for HashMap-backed values via... actually, no — Rust's
    // HashMap is unordered. Use BTreeMap for stable serialization.
    let state: std::collections::BTreeMap<String, i16> = serde_json::from_slice(bytes).unwrap();
    let reencoded = serde_json::to_vec(&state).unwrap();
    // The v0.1 writer emitted keys in sorted order for this
    // map (its JSON encoder sorts map keys lexicographically). The
    // BTreeMap re-serialization does the same, so the round trip
    // is exact.
    assert_eq!(bytes, reencoded.as_slice(), "fence log round trip drift");
}

/// Sanity check: the captured fixtures exercise every variant of
/// the on-disk shape the Rust port needs to read. Bumps as a
/// reminder if someone trims the captured set without thinking
/// about coverage.
#[test]
fn fixture_set_covers_every_state_shape() {
    let slots = [
        include_bytes!("fixtures/txn_state/slot-9.json").as_slice(),
        include_bytes!("fixtures/txn_state/slot-25.json").as_slice(),
        include_bytes!("fixtures/txn_state/slot-40.json").as_slice(),
        include_bytes!("fixtures/txn_state/slot-48.json").as_slice(),
    ];
    let mut saw_commit = false;
    let mut saw_abort = false;
    let mut saw_ongoing = false;
    let mut saw_empty_state_field_omitted = false;
    let mut saw_groups = false;
    let mut saw_partitions = false;
    for bytes in slots {
        let state: HashMap<String, TxnEntry> = serde_json::from_slice(bytes).unwrap();
        for entry in state.values() {
            use kaas_coordinator::TxnState;
            match entry.state {
                TxnState::CompleteCommit => saw_commit = true,
                TxnState::CompleteAbort => saw_abort = true,
                TxnState::Ongoing => saw_ongoing = true,
                TxnState::Empty => saw_empty_state_field_omitted = true,
                _ => {}
            }
            if !entry.groups.is_empty() {
                saw_groups = true;
            }
            if !entry.partitions.is_empty() {
                saw_partitions = true;
            }
        }
    }
    assert!(saw_commit, "no CompleteCommit case in fixtures");
    assert!(saw_abort, "no CompleteAbort case in fixtures");
    assert!(saw_ongoing, "no Ongoing case in fixtures");
    assert!(
        saw_empty_state_field_omitted,
        "no Empty (omitted state field) case in fixtures"
    );
    assert!(saw_groups, "no populated `groups` case in fixtures");
    assert!(saw_partitions, "no populated `partitions` case in fixtures");
}
