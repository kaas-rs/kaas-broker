//! Deterministic hash routing for group + txn coordinator selection.
//!
//! Pure functions
//! over `(key, brokers, alive)`; no state, no I/O. The Coordinator
//! consults these for `OwnsGroup` / `GroupCoordinator` /
//! `OwnsTxn` / `TxnCoordinator` whenever the explicit-override tier
//! (`assignment.json.consumerGroups[]`) has no entry for the key.
//!
//! ## Hash
//!
//! FNV-1a 32-bit, mod `num_brokers`. Kaas clients never compute
//! the coordinator locally — they always go through
//! `FindCoordinator` — so any deterministic 32-bit hash works as
//! long as every broker uses the same one. FNV-1a is trivial to
//! implement inline (no extra dep), deterministic across
//! architectures, and matches the v0.1 output
//! byte-for-byte.
//!
//! ## Stable divisor
//!
//! `num_brokers` MUST be the full broker set size (every broker the
//! controller knows about — including draining / dead). Holding the
//! divisor constant is what keeps existing assignments stable
//! across rolling restarts. Modding by `len(alive)` would reshuffle
//! ~(N-1)/N of all keys on every pod up/down event.

use std::collections::HashMap;

/// FNV-1a 32-bit over `bytes`. Inlined to avoid a tiny external
/// dep; output matches the standard FNV-1a vectors for
/// equal inputs.
fn fnv1a_32(bytes: &[u8]) -> u32 {
    const OFFSET_BASIS: u32 = 0x811c_9dc5;
    const PRIME: u32 = 0x0100_0193;
    let mut h = OFFSET_BASIS;
    for &b in bytes {
        h ^= u32::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// `hash(key) % num_brokers`. Returns 0 when `num_brokers == 0`
/// (caller treats that as "no broker available").
pub fn coordinator_slot(key: &str, num_brokers: usize) -> usize {
    if num_brokers == 0 {
        return 0;
    }
    // u32 → u64 → usize is safe on every target (usize ≥ 32 bits).
    let h = u64::from(fnv1a_32(key.as_bytes()));
    let n = u64::try_from(num_brokers).unwrap_or(u64::MAX);
    usize::try_from(h % n).unwrap_or(0)
}

/// "Preferred broker; fall back through the alive subset
/// deterministically" — the shared decision used by
/// `pick_group_coordinator` and `pick_txn_coordinator`.
///
/// 1. Sort `brokers` to get a stable lookup index.
/// 2. Compute `slot = hash(key) % len(brokers)`.
/// 3. If `brokers_sorted[slot]` is alive, return it (stable
///    preferred case).
/// 4. Otherwise pick from the alive subset at
///    `slot % len(alive_sorted)`. Stable across multiple invocations
///    of the same alive snapshot so a client rejoining during a
///    transient outage doesn't ping-pong between brokers.
///
/// Returns `None` when no broker is alive (caller surfaces
/// `COORDINATOR_NOT_AVAILABLE`; client retries `FindCoordinator`).
pub fn pick_coordinator(
    key: &str,
    brokers: &[String],
    alive: &HashMap<String, bool>,
) -> Option<String> {
    if brokers.is_empty() {
        return None;
    }
    let mut sorted: Vec<String> = brokers.to_vec();
    sorted.sort();
    let slot = coordinator_slot(key, sorted.len());
    if *alive.get(&sorted[slot]).unwrap_or(&false) {
        return Some(sorted[slot].clone());
    }
    let alive_sorted: Vec<&String> = sorted
        .iter()
        .filter(|b| *alive.get(*b).unwrap_or(&false))
        .collect();
    if alive_sorted.is_empty() {
        return None;
    }
    Some(alive_sorted[slot % alive_sorted.len()].clone())
}

/// Resolve `group_id` to its coordinator broker. See
/// [`pick_coordinator`] for the algorithm.
pub fn pick_group_coordinator(
    group_id: &str,
    brokers: &[String],
    alive: &HashMap<String, bool>,
) -> Option<String> {
    pick_coordinator(group_id, brokers, alive)
}

/// Resolve `transactional_id` to its coordinator broker (gh #91).
/// Sibling of [`pick_group_coordinator`] — same hash, same fallback.
pub fn pick_txn_coordinator(
    transactional_id: &str,
    brokers: &[String],
    alive: &HashMap<String, bool>,
) -> Option<String> {
    pick_coordinator(transactional_id, brokers, alive)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brokers(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("kaas-{i}")).collect()
    }

    fn alive_all(brokers: &[String]) -> HashMap<String, bool> {
        brokers.iter().map(|b| (b.clone(), true)).collect()
    }

    #[test]
    fn empty_broker_set_returns_none() {
        let alive = HashMap::new();
        assert!(pick_group_coordinator("g", &[], &alive).is_none());
    }

    #[test]
    fn pick_returns_preferred_when_alive() {
        let b = brokers(3);
        let alive = alive_all(&b);
        let pick = pick_group_coordinator("g1", &b, &alive).unwrap();
        assert!(b.contains(&pick));
    }

    #[test]
    fn slot_is_stable_across_input_orderings() {
        // The function sorts internally so swapping the broker order
        // in the input slice yields the same answer.
        let b1 = vec![
            "kaas-2".to_owned(),
            "kaas-0".to_owned(),
            "kaas-1".to_owned(),
        ];
        let b2 = vec![
            "kaas-0".to_owned(),
            "kaas-1".to_owned(),
            "kaas-2".to_owned(),
        ];
        let alive = alive_all(&b1);
        let a = pick_group_coordinator("group-key-1", &b1, &alive).unwrap();
        let b = pick_group_coordinator("group-key-1", &b2, &alive).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn preferred_down_falls_through_to_alternate_alive() {
        let b = brokers(3);
        let mut alive = alive_all(&b);
        // Find which broker is preferred for a known key and mark
        // it down; the function must return one of the other two.
        let key = "group-x";
        let preferred = pick_group_coordinator(key, &b, &alive).unwrap();
        alive.insert(preferred.clone(), false);
        let alt = pick_group_coordinator(key, &b, &alive).unwrap();
        assert_ne!(alt, preferred);
        assert!(*alive.get(&alt).unwrap());
    }

    #[test]
    fn no_alive_brokers_returns_none() {
        let b = brokers(3);
        let alive: HashMap<String, bool> = b.iter().map(|b| (b.clone(), false)).collect();
        assert!(pick_group_coordinator("g", &b, &alive).is_none());
    }

    #[test]
    fn divisor_is_full_broker_set_not_alive_size() {
        // Two distinct alive sets but the same full broker list:
        // the preferred slot must come out identical even though the
        // alive count differs (gh #92 invariant).
        let b = brokers(4);
        let alive_a = alive_all(&b);
        let mut alive_b = alive_a.clone();
        alive_b.insert("kaas-3".to_owned(), false);
        let a = pick_group_coordinator("g-stable", &b, &alive_a).unwrap();
        let b_pick = pick_group_coordinator("g-stable", &b, &alive_b).unwrap();
        // Either the preferred is still alive in both → same answer;
        // or the preferred was kaas-3 (down in alive_b) and the
        // fallback can differ. Skip the case where the preferred
        // was kaas-3 — we want the stable-preferred check.
        if a != "kaas-3" {
            assert_eq!(a, b_pick);
        }
    }

    #[test]
    fn txn_and_group_use_same_algorithm() {
        let b = brokers(3);
        let alive = alive_all(&b);
        // Same input key resolves to the same broker through both
        // wrappers — same FNV hash, same fallback.
        let g = pick_group_coordinator("key-1", &b, &alive).unwrap();
        let t = pick_txn_coordinator("key-1", &b, &alive).unwrap();
        assert_eq!(g, t);
    }

    /// Byte-for-byte check against FNV-1a reference vectors
    /// captured from the v0.1 implementation for the
    /// same inputs.
    #[test]
    fn fnv1a_known_values() {
        assert_eq!(fnv1a_32(b""), 0x811c_9dc5);
        assert_eq!(fnv1a_32(b"a"), 0xe40c_292c);
        assert_eq!(fnv1a_32(b"foobar"), 0xbf9c_f968);
    }
}
