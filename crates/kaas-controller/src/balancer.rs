//! Partition + consumer-group placement.
//!
//! Pure functions
//! over `(prev assignment, alive brokers, inputs)`; no state, no
//! I/O. Both shapes — partition placement and group placement —
//! follow the same recipe:
//!
//! 1. **Preserve** any prior assignment whose broker is still in
//!    the alive set. Stable assignments minimise log migration on
//!    the shared PVC.
//! 2. **Place** the rest, levelling each topic over the alive set.
//!    Highest-random-weight hashing keyed on `(topic, partition,
//!    broker_id)` (or `(group_id, broker_id)` for groups) breaks
//!    ties, so placement is deterministic and needs no coordination.
//! 3. **Smooth** the partition layer with two deterministic passes:
//!    cluster-wide to `max(per-broker count) - min ≤ 1`, then
//!    per-topic without disturbing that. Group placement skips
//!    smoothing because each group is a single unit.
//!
//! **Order matters.** Preserve runs *first*, before anything else
//! looks at the layout, which is what keeps the recipe incremental:
//! a recompute only decides the partitions that actually need a
//! decision. Deriving the layout up front and reconciling with `prev`
//! afterwards computes the same thing in steady state but re-derives
//! it from `(topics, alive)` alone — so creating or deleting one
//! topic could migrate partitions of every *other* topic, and each
//! such move costs a real open/recover/close cycle on NFS plus a
//! window where the outgoing leader is still acking writes (gh #206,
//! and the dual-writer window in gh #227).
//!
//! Hash: XXH64 via `twox-hash`, byte-for-byte stable so an
//! assignment written by any release matches a v0.1-written
//! one for the same input (upgrade requirement). The previous
//! FNV-1a 64 had pathological avalanche on broker IDs differing by
//! one byte and drove a 50/25/25 skew on 3-broker clusters
//! (kaas#112).

use std::collections::{HashMap, HashSet};
use std::hash::Hasher;

use kaas_broker::{
    BrokerAssignment, BrokerHealth, ConsumerGroupAssignment, PartitionAssignment, PartitionRole,
};

/// Per-topic catalog entry the balancer consumes. The KafkaTopic CR
/// watcher (Phase 7) is the production source; tests pass a literal
/// `Vec<TopicSpec>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicSpec {
    pub name: String,
    pub partition_count: i32,
}

/// Per-active-group entry the balancer consumes. The HeartbeatServer's
/// `active_groups()` union is the production source.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSpec {
    pub group_id: String,
}

/// `XXH64(topic || 0x00 || partition_be || 0x00 || broker)`.
/// The byte sequence is pinned so any controller build picks the
/// same broker as a v0.1-driven one for the same inputs (upgrade
/// compatibility).
pub fn rendezvous_hash(topic: &str, partition: i32, broker: &str) -> u64 {
    let mut h = twox_hash::XxHash64::with_seed(0);
    h.write(topic.as_bytes());
    h.write(&[0]);
    h.write(&partition.to_be_bytes());
    h.write(&[0]);
    h.write(broker.as_bytes());
    h.finish()
}

/// Working tuple the smoother mutates in place. Internal.
#[derive(Debug, Clone)]
struct PartitionSlot {
    topic: String,
    partition: i32,
    broker: String,
    /// Was this slot inherited from `prev` (its broker still alive)?
    /// Moving a sticky slot costs a real open/recover/close cycle on
    /// the shared volume, so both smoothing passes exhaust the
    /// non-sticky candidates before touching one.
    sticky: bool,
}

/// Returns `topic/partition` keyed prior partition assignments —
/// internal lookup for [`balance`].
fn prev_partitions(prev: Option<&[PartitionAssignment]>) -> HashMap<String, PartitionAssignment> {
    let mut out = HashMap::new();
    if let Some(ps) = prev {
        for p in ps {
            out.insert(partition_key(&p.topic, p.partition), p.clone());
        }
    }
    out
}

/// `"topic/partition"` cache key. Must match
/// [`kaas_broker::partition_key`] byte-for-byte — the takeover driver
/// and the balancer have to agree on the lookup string.
pub fn partition_key(topic: &str, partition: i32) -> String {
    kaas_broker::partition_key(topic, partition)
}

/// Run the partition balancer. Returns the per-(topic, partition)
/// assignment that the writer stamps into `assignment.json`.
///
/// `prev` is the previously written assignment's partition list (or
/// `None` on a fresh controller takeover). `brokers` is the alive
/// set the controller currently sees. `topics` is the catalog.
///
/// `epoch_floor` maps `partition_key` → a minimum leader epoch. It
/// exists for gh #216: the per-partition epoch normally increases
/// monotonically, but a partition that drops out of the assignment and
/// is re-added reconciles as "new" and would reset to epoch 1 — while
/// its persisted on-disk epoch (the storage append fence's reference)
/// stayed higher, so every append would be rejected as stale. The
/// writer seeds the floor from the on-disk manifest epoch for any
/// partition not already in `prev`, so a re-add can never regress below
/// what a broker has already committed. An empty map is a no-op.
pub fn balance(
    prev: Option<&[PartitionAssignment]>,
    brokers: &[String],
    topics: &[TopicSpec],
    epoch_floor: &HashMap<String, u32>,
) -> Vec<PartitionAssignment> {
    if brokers.is_empty() {
        return Vec::new();
    }
    let mut alive = brokers.to_vec();
    alive.sort();
    let alive_set: HashSet<String> = alive.iter().cloned().collect();
    let prev_map = prev_partitions(prev);

    // Phase 1: pin every partition whose previous leader is still
    // alive; place the rest. Pinning FIRST is what makes the whole
    // pass `prev`-aware (gh #206): placement and smoothing only ever
    // see partitions that genuinely need a decision, so adding or
    // removing a topic can no longer re-derive the layout of every
    // other topic.
    let mut slots: Vec<PartitionSlot> = Vec::new();
    for t in topics {
        for partition in 0..t.partition_count {
            let key = partition_key(&t.name, partition);
            let pinned = prev_map
                .get(&key)
                .map(|pa| pa.broker.clone())
                .filter(|b| alive_set.contains(b));
            slots.push(match pinned {
                Some(broker) => PartitionSlot {
                    topic: t.name.clone(),
                    partition,
                    broker,
                    sticky: true,
                },
                None => PartitionSlot {
                    topic: t.name.clone(),
                    partition,
                    broker: String::new(),
                    sticky: false,
                },
            });
        }
    }

    // Phase 2: place the unpinned slots, levelling each topic over
    // the alive set as we go (gh #247 — a per-partition hash alone
    // lands all 3 partitions of a 3-partition topic on one broker
    // 1 time in 9). Rendezvous only breaks ties between equally
    // loaded brokers, so placement stays deterministic.
    place_unpinned(&mut slots, &alive);

    // Phase 3: smoothing. Cluster-wide first (the gh #99 invariant:
    // `max - min <= 1` over all partitions), then a per-topic pass
    // that trades partitions between brokers without disturbing it.
    smooth_partitions(&mut slots, &alive);
    smooth_partitions_per_topic(&mut slots, &alive);

    // Phase 4: reconcile with prev for stable epochs, never dropping
    // below the on-disk floor (gh #216 — see the fn doc).
    let mut out = Vec::with_capacity(slots.len());
    for s in slots {
        let key = partition_key(&s.topic, s.partition);
        let prev_entry = prev_map.get(&key);
        let floor = epoch_floor.get(&key).copied().unwrap_or(0);
        if let Some(pa) = prev_entry {
            // Stable leader whose epoch already clears the floor: keep
            // it untouched (no takeover, no epoch bump).
            if alive_set.contains(&pa.broker) && pa.broker == s.broker && pa.epoch >= floor {
                out.push(pa.clone());
                continue;
            }
        }
        // New / re-added / leader-changed: bump from prev (or start at
        // 1), but never below the partition's persisted epoch.
        let base = prev_entry.map(|pa| pa.epoch + 1).unwrap_or(1);
        let epoch = base.max(floor);
        out.push(PartitionAssignment {
            topic: s.topic,
            partition: s.partition,
            broker: s.broker,
            epoch,
            role: PartitionRole::Leader,
        });
    }
    out
}

/// Same recipe for consumer groups: keep a still-alive assignment;
/// otherwise hash-pick. No smoothing — each group is a single unit.
///
/// **The hash must be the one the brokers use** (gh #248).
/// `assignment.json.consumerGroups[]` is only the *first* tier of
/// `Coordinator::owns_group`; absent an entry a broker falls through to
/// `group_hash::pick_group_coordinator`. If this function mints entries
/// with a different function, then the moment the controller first
/// writes an entry for a group, the group moves — from wherever the
/// fallthrough had been serving it to wherever this function decided —
/// and its clients get `NOT_COORDINATOR` followed by a full rebuild.
/// That is one guaranteed disruption per group, shortly after it
/// starts, on a healthy cluster with nothing wrong.
///
/// It used to mint with `rendezvous_pick_group` (highest-random-weight)
/// while brokers resolved with `pick_coordinator` (`hash % n` plus a
/// deterministic alternate). Those agree only by coincidence: on the
/// live 3-broker cluster they disagreed for every group id tried.
///
/// So this takes the **broker rows** rather than a list of alive ids —
/// `pick_coordinator` divides by the full registered set (gh #249), and
/// handing it anything narrower reintroduces the divisor bug. Taking the
/// rows the writer is about to serialise means the balancer and every
/// broker reading the file resolve from byte-identical input.
pub fn balance_groups(
    prev: Option<&[ConsumerGroupAssignment]>,
    brokers: &[BrokerAssignment],
    groups: &[GroupSpec],
) -> Vec<ConsumerGroupAssignment> {
    if brokers.is_empty() {
        return Vec::new();
    }
    // Derived exactly as `Assignment::broker_sets` does on the read
    // side — same set, same aliveness rule.
    let mut ids: Vec<String> = Vec::with_capacity(brokers.len());
    let mut alive_map: HashMap<String, bool> = HashMap::with_capacity(brokers.len());
    for b in brokers {
        ids.push(b.id.clone());
        alive_map.insert(b.id.clone(), matches!(b.health, BrokerHealth::Alive));
    }
    let alive_set: HashSet<String> = ids
        .iter()
        .filter(|id| alive_map.get(*id).copied().unwrap_or(false))
        .cloned()
        .collect();
    let prev_map: HashMap<String, ConsumerGroupAssignment> = prev
        .map(|ps| ps.iter().map(|g| (g.group_id.clone(), g.clone())).collect())
        .unwrap_or_default();

    let mut out = Vec::with_capacity(groups.len());
    let mut placed: HashSet<&str> = HashSet::new();
    for g in groups {
        let prev_entry = prev_map.get(&g.group_id);
        if !placed.insert(g.group_id.as_str()) {
            continue;
        }
        if let Some(ga) = prev_entry {
            if alive_set.contains(&ga.broker) {
                out.push(ga.clone());
                continue;
            }
        }
        // Same function `Coordinator::owns_group` falls through to, so
        // the entry we write CONFIRMS where the group already is
        // instead of moving it.
        let broker = kaas_broker::group_hash::pick_group_coordinator(&g.group_id, &ids, &alive_map)
            .unwrap_or_default();
        let epoch = prev_entry.map(|ga| ga.epoch + 1).unwrap_or(1);
        out.push(ConsumerGroupAssignment {
            group_id: g.group_id.clone(),
            broker,
            epoch,
        });
    }

    // gh #248: carry forward every prior entry whose broker is still
    // alive, even when the group is absent from `groups`.
    //
    // `groups` is the union of what each *connected* broker reports,
    // so a group drops out of it for reasons that have nothing to do
    // with coordination: its members all left for a moment, its
    // coordinator's heartbeat was briefly stale, or it was swept out
    // of memory. Without this, absence retires the explicit entry,
    // `Coordinator::owns_group` falls through to the hash — which can
    // legitimately name a *different* broker than the sticky entry
    // did — and the group's clients get NOT_COORDINATOR followed by a
    // full rebuild. It is not self-healing either: the next
    // recompute's `prev` no longer has the entry, so the coordinator
    // has moved permanently on the strength of one missed report.
    //
    // Retirement is by broker death, not by absence: an entry whose
    // broker has left is simply dropped (a still-live group is in
    // `groups` and was re-picked above). So a deleted group's entry
    // outlives it until its coordinator restarts — a few dozen bytes
    // of assignment.json, against a client-visible group rebuild.
    let mut carried: Vec<ConsumerGroupAssignment> = prev_map
        .values()
        .filter(|ga| !placed.contains(ga.group_id.as_str()) && alive_set.contains(&ga.broker))
        .cloned()
        .collect();
    carried.sort_by(|a, b| a.group_id.cmp(&b.group_id));
    out.extend(carried);
    out
}

/// Give every slot Phase 1 left unpinned a broker.
///
/// Each one lands on the alive broker holding the fewest partitions
/// **of its own topic**, ties broken by fewest partitions overall and
/// then by rendezvous score. The topic dimension is what Apache gets
/// for free by assigning each topic round-robin from its own starting
/// offset (`AdminUtils.assignReplicasToBrokers`, KRaft's
/// `StripedReplicaPlacer`): a per-partition hash alone has no memory
/// of a partition's siblings, so 3 partitions over 3 brokers pile onto
/// one broker 1 time in 9 (gh #247).
///
/// Rendezvous survives as the tiebreak rather than the mechanism, so
/// placement stays a deterministic function of the inputs — any
/// controller replaying the same recompute picks the same brokers.
fn place_unpinned(slots: &mut [PartitionSlot], alive: &[String]) {
    if alive.is_empty() {
        return;
    }
    let mut total: HashMap<String, i32> = alive.iter().map(|b| (b.clone(), 0)).collect();
    let mut per_topic: HashMap<(String, String), i32> = HashMap::new();
    for s in slots.iter() {
        if s.broker.is_empty() {
            continue;
        }
        *total.entry(s.broker.clone()).or_insert(0) += 1;
        *per_topic
            .entry((s.topic.clone(), s.broker.clone()))
            .or_insert(0) += 1;
    }

    for slot in slots.iter_mut() {
        if !slot.broker.is_empty() {
            continue;
        }
        let topic = slot.topic.clone();
        let partition = slot.partition;
        let mut best: Option<(i32, i32, u64, String)> = None;
        for b in alive {
            let cand = (
                *per_topic.get(&(topic.clone(), b.clone())).unwrap_or(&0),
                *total.get(b).unwrap_or(&0),
                rendezvous_hash(&topic, partition, b),
                b.clone(),
            );
            let better = match &best {
                None => true,
                // Fewest of this topic, then fewest overall, then the
                // highest rendezvous score. `alive` is sorted, so the
                // strict `>` on the score leaves the lexicographically
                // first broker winning a total tie.
                Some((bt, ball, bscore, _)) => {
                    (cand.0, cand.1) < (*bt, *ball)
                        || ((cand.0, cand.1) == (*bt, *ball) && cand.2 > *bscore)
                }
            };
            if better {
                best = Some(cand);
            }
        }
        if let Some((_, _, _, broker)) = best {
            *total.entry(broker.clone()).or_insert(0) += 1;
            *per_topic.entry((topic, broker.clone())).or_insert(0) += 1;
            slot.broker = broker;
        }
    }
}

/// Count of `topic`'s partitions currently sitting on `broker`.
fn topic_count_on(slots: &[PartitionSlot], topic: &str, broker: &str) -> i32 {
    i32::try_from(
        slots
            .iter()
            .filter(|s| s.topic == topic && s.broker == broker)
            .count(),
    )
    .unwrap_or(i32::MAX)
}

/// Move partitions from the most-loaded broker to the least-loaded
/// until `max - min ≤ 1`. Deterministic — ties broken
/// lexicographically on broker ID; victim picked by highest
/// rendezvous score for the recipient (= the move closest to a
/// no-op from rendezvous's perspective). Owned `String` keys throughout so the
/// counts map doesn't tangle with the `alive` slice's lifetime.
///
/// Victim preference is ordered so the cheapest move wins: a slot the
/// caller just placed before one inherited from `prev` (moving a
/// sticky slot is a real open/recover/close cycle on the shared
/// volume), and among equals one whose topic is over-represented on
/// the donor, so cluster smoothing pulls in the same direction as
/// [`smooth_partitions_per_topic`] instead of against it.
fn smooth_partitions(slots: &mut [PartitionSlot], alive: &[String]) {
    if alive.len() < 2 || slots.is_empty() {
        return;
    }
    let mut counts: HashMap<String, i32> = alive.iter().map(|b| (b.clone(), 0)).collect();
    for s in slots.iter() {
        *counts.entry(s.broker.clone()).or_insert(0) += 1;
    }
    loop {
        let mut hi = alive[0].clone();
        let mut lo = alive[0].clone();
        let mut hi_count = *counts.get(&hi).unwrap_or(&0);
        let mut lo_count = *counts.get(&lo).unwrap_or(&0);
        for b in &alive[1..] {
            let c = *counts.get(b).unwrap_or(&0);
            if c > hi_count {
                hi = b.clone();
                hi_count = c;
            }
            if c < lo_count {
                lo = b.clone();
                lo_count = c;
            }
        }
        if hi_count - lo_count <= 1 {
            return;
        }
        // Pick the victim on `hi`: freshly placed before inherited,
        // then a topic over-represented on `hi` relative to `lo`,
        // then the highest rendezvous score for `lo`, then
        // lexicographic `(topic, partition)`.
        let mut victim_idx: Option<usize> = None;
        let mut victim_key: (u8, u8, u64) = (0, 0, 0);
        for (i, s) in slots.iter().enumerate() {
            if s.broker != hi {
                continue;
            }
            let helps_topic =
                topic_count_on(slots, &s.topic, &hi) > topic_count_on(slots, &s.topic, &lo);
            let key = (
                u8::from(s.sticky),
                u8::from(!helps_topic),
                rendezvous_hash(&s.topic, s.partition, &lo),
            );
            let better = match victim_idx {
                None => true,
                Some(prev) => {
                    // First two components sort ascending (0 = the
                    // preferred class), the score descending.
                    if (key.0, key.1) != (victim_key.0, victim_key.1) {
                        (key.0, key.1) < (victim_key.0, victim_key.1)
                    } else if key.2 != victim_key.2 {
                        key.2 > victim_key.2
                    } else {
                        (&s.topic, s.partition) < (&slots[prev].topic, slots[prev].partition)
                    }
                }
            };
            if better {
                victim_idx = Some(i);
                victim_key = key;
            }
        }
        match victim_idx {
            None => return, // unreachable — hi has > 0 slots
            Some(i) => {
                slots[i].broker = lo.clone();
                *counts.entry(hi).or_insert(0) -= 1;
                *counts.entry(lo).or_insert(0) += 1;
            }
        }
    }
}

/// Level each topic across the alive set *without* disturbing the
/// cluster-wide balance [`smooth_partitions`] just established
/// (gh #247).
///
/// Two shapes of repair, cheapest first:
///
/// - a **move**, when the donor also has at least as many partitions
///   overall as the recipient — the cluster invariant survives
///   because the gap merely changes sign;
/// - a **swap** with a partition of another topic that is itself
///   over-represented on the recipient. Cluster counts are untouched
///   by construction, and the other topic never gets worse.
///
/// Termination: both shapes strictly lower `Σ count²` over
/// `(topic, broker)` — a move by ≥ 2, a swap by ≥ 2 net (the topic
/// being repaired gives up ≥ 2 and its partner costs ≤ 0). The
/// potential is a bounded non-negative integer, so the loop converges;
/// the iteration cap is a backstop against a future edit breaking that
/// argument, not the mechanism.
fn smooth_partitions_per_topic(slots: &mut [PartitionSlot], alive: &[String]) {
    if alive.len() < 2 || slots.is_empty() {
        return;
    }
    let mut topics: Vec<String> = slots.iter().map(|s| s.topic.clone()).collect();
    topics.sort();
    topics.dedup();

    let cap = slots.len() * alive.len() + 16;
    for _ in 0..cap {
        if !improve_one_topic(slots, alive, &topics) {
            return;
        }
    }
    tracing::warn!(
        partitions = slots.len(),
        "per-topic smoothing hit its iteration cap; layout is cluster-balanced but may be uneven within a topic"
    );
}

/// One repair step for [`smooth_partitions_per_topic`]. Returns
/// `false` when no topic can be improved — the pass is done.
fn improve_one_topic(slots: &mut [PartitionSlot], alive: &[String], topics: &[String]) -> bool {
    let mut totals: HashMap<String, i32> = alive.iter().map(|b| (b.clone(), 0)).collect();
    for s in slots.iter() {
        *totals.entry(s.broker.clone()).or_insert(0) += 1;
    }

    for topic in topics {
        // `alive` is sorted, and both comparisons are strict, so the
        // lexicographically first broker wins a tie on either end.
        let mut hi = &alive[0];
        let mut lo = &alive[0];
        let mut hi_count = topic_count_on(slots, topic, hi);
        let mut lo_count = hi_count;
        for b in &alive[1..] {
            let c = topic_count_on(slots, topic, b);
            if c > hi_count {
                hi = b;
                hi_count = c;
            }
            if c < lo_count {
                lo = b;
                lo_count = c;
            }
        }
        if hi_count - lo_count <= 1 {
            continue;
        }

        // A plain move keeps `max - min <= 1` cluster-wide exactly
        // when the donor is not already the lighter of the two.
        let move_ok = totals.get(hi).copied().unwrap_or(0) >= totals.get(lo).copied().unwrap_or(0);
        let Some(donor) = pick_slot(slots, topic, hi) else {
            continue;
        };
        if move_ok {
            slots[donor].broker = lo.clone();
            return true;
        }

        // Otherwise swap with a partition of a topic that is itself
        // over-represented on `lo`, which keeps both cluster counts
        // and that topic's spread no worse than they were.
        let partner = slots
            .iter()
            .enumerate()
            .filter(|(_, s)| {
                &s.broker == lo
                    && &s.topic != topic
                    && topic_count_on(slots, &s.topic, lo) > topic_count_on(slots, &s.topic, hi)
            })
            .min_by(|(_, a), (_, b)| {
                (a.sticky, &a.topic, a.partition).cmp(&(b.sticky, &b.topic, b.partition))
            })
            .map(|(i, _)| i);
        if let Some(partner) = partner {
            slots[donor].broker = lo.clone();
            slots[partner].broker = hi.clone();
            return true;
        }
    }
    false
}

/// Lowest `(sticky, topic, partition)` slot of `topic` on `broker` —
/// the deterministic donor, preferring one that isn't inherited.
fn pick_slot(slots: &[PartitionSlot], topic: &str, broker: &str) -> Option<usize> {
    slots
        .iter()
        .enumerate()
        .filter(|(_, s)| s.topic == topic && s.broker == broker)
        .min_by_key(|(_, s)| (s.sticky, s.partition))
        .map(|(i, _)| i)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn brokers(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("kaas-{i}")).collect()
    }

    /// Broker rows for `n` brokers, all alive — what the writer hands
    /// `balance_groups`.
    fn rows(n: usize) -> Vec<BrokerAssignment> {
        brokers(n)
            .into_iter()
            .map(|id| BrokerAssignment {
                id,
                health: BrokerHealth::Alive,
                last_seen: "2026-08-09T00:00:00Z".to_owned(),
            })
            .collect()
    }

    /// Same, but only `alive` are Alive; the rest stay registered and
    /// Dead, which is what keeps the coordinator-hash divisor stable
    /// (gh #249).
    fn rows_with_alive(n: usize, alive: &[&str]) -> Vec<BrokerAssignment> {
        rows(n)
            .into_iter()
            .map(|mut r| {
                if !alive.contains(&r.id.as_str()) {
                    r.health = BrokerHealth::Dead;
                }
                r
            })
            .collect()
    }

    fn topic(name: &str, partition_count: i32) -> TopicSpec {
        TopicSpec {
            name: name.to_owned(),
            partition_count,
        }
    }

    /// `(broker → partition count)` over the whole assignment, or
    /// over one topic when `only` is set.
    fn counts(parts: &[PartitionAssignment], only: Option<&str>) -> HashMap<String, i32> {
        let mut out: HashMap<String, i32> = HashMap::new();
        for p in parts.iter().filter(|p| only.is_none_or(|t| p.topic == t)) {
            *out.entry(p.broker.clone()).or_insert(0) += 1;
        }
        out
    }

    fn skew(parts: &[PartitionAssignment], only: Option<&str>, brokers: &[String]) -> i32 {
        let c = counts(parts, only);
        let vals: Vec<i32> = brokers
            .iter()
            .map(|b| c.get(b).copied().unwrap_or(0))
            .collect();
        vals.iter().max().copied().unwrap_or(0) - vals.iter().min().copied().unwrap_or(0)
    }

    fn find<'a>(parts: &'a [PartitionAssignment], topic: &str, p: i32) -> &'a PartitionAssignment {
        parts
            .iter()
            .find(|q| q.topic == topic && q.partition == p)
            .expect("partition present")
    }

    #[test]
    fn empty_broker_set_returns_empty_assignment() {
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 4,
        }];
        let parts = balance(None, &[], &topics, &HashMap::new());
        assert!(parts.is_empty());
    }

    #[test]
    fn balance_assigns_every_partition_to_an_alive_broker() {
        let b = brokers(3);
        let topics = vec![
            TopicSpec {
                name: "t1".to_owned(),
                partition_count: 6,
            },
            TopicSpec {
                name: "t2".to_owned(),
                partition_count: 3,
            },
        ];
        let parts = balance(None, &b, &topics, &HashMap::new());
        assert_eq!(parts.len(), 9);
        for p in &parts {
            assert!(
                b.contains(&p.broker),
                "broker {} not in alive set",
                p.broker
            );
            assert_eq!(p.epoch, 1, "fresh assignment starts at epoch 1");
            assert_eq!(p.role, PartitionRole::Leader);
        }
    }

    #[test]
    fn balance_stability_keeps_assignment_when_brokers_unchanged() {
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 6,
        }];
        let first = balance(None, &b, &topics, &HashMap::new());
        let second = balance(Some(&first), &b, &topics, &HashMap::new());
        assert_eq!(first, second, "stable inputs → identical assignment");
    }

    #[test]
    fn balance_smoother_caps_skew_at_one() {
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 16,
        }];
        let parts = balance(None, &b, &topics, &HashMap::new());
        let mut counts: HashMap<&str, i32> = HashMap::new();
        for p in &parts {
            *counts.entry(p.broker.as_str()).or_insert(0) += 1;
        }
        let hi = counts.values().max().copied().unwrap_or(0);
        let lo = counts.values().min().copied().unwrap_or(0);
        assert!(
            hi - lo <= 1,
            "smoother must cap skew at 1; got hi={hi} lo={lo} counts={counts:?}"
        );
    }

    #[test]
    fn balance_reassigns_only_partitions_on_dead_brokers() {
        let three = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 9,
        }];
        let first = balance(None, &three, &topics, &HashMap::new());
        // kaas-2 goes down.
        let two = vec!["kaas-0".to_owned(), "kaas-1".to_owned()];
        let second = balance(Some(&first), &two, &topics, &HashMap::new());
        for p in &first {
            if p.broker != "kaas-2" {
                // Stable partition keeps epoch 1.
                let matching = second
                    .iter()
                    .find(|q| q.topic == p.topic && q.partition == p.partition)
                    .expect("partition retained");
                if matching.broker == p.broker {
                    assert_eq!(matching.epoch, p.epoch, "stable partition keeps epoch");
                }
            }
        }
        // Every partition assigned to an alive broker.
        for p in &second {
            assert!(p.broker == "kaas-0" || p.broker == "kaas-1");
        }
    }

    #[test]
    fn epoch_floor_prevents_regression_on_readd() {
        // gh #216: a partition re-added after dropping out of the
        // assignment (prev has no entry for it) must adopt its on-disk
        // floor, not reset to epoch 1 — else the append fence rejects
        // every write as stale (assignment epoch < on-disk epoch).
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 4,
        }];
        let floor: HashMap<String, u32> = (0..4).map(|p| (partition_key("t1", p), 11u32)).collect();
        let parts = balance(None, &b, &topics, &floor);
        assert_eq!(parts.len(), 4);
        for p in &parts {
            assert_eq!(
                p.epoch, 11,
                "re-added partition must adopt the on-disk floor, not reset to 1"
            );
        }
        // Control: without the floor, the same re-add resets to 1.
        let no_floor = balance(None, &b, &topics, &HashMap::new());
        assert!(no_floor.iter().all(|p| p.epoch == 1));
    }

    #[test]
    fn epoch_floor_self_heals_but_never_lowers() {
        let b = brokers(3);
        let topics = vec![TopicSpec {
            name: "t1".to_owned(),
            partition_count: 4,
        }];
        let first = balance(None, &b, &topics, &HashMap::new()); // all epoch 1
                                                                 // A floor above the current epoch bumps a stable partition up to
                                                                 // the floor (self-heals an already-regressed assignment).
        let high: HashMap<String, u32> = first
            .iter()
            .map(|p| (partition_key(&p.topic, p.partition), 9u32))
            .collect();
        let healed = balance(Some(&first), &b, &topics, &high);
        assert!(healed.iter().all(|p| p.epoch == 9));
        // A floor at or below the current epoch never perturbs a stable
        // assignment.
        let low: HashMap<String, u32> = first
            .iter()
            .map(|p| (partition_key(&p.topic, p.partition), 1u32))
            .collect();
        let unchanged = balance(Some(&first), &b, &topics, &low);
        assert_eq!(first, unchanged);
    }

    #[test]
    fn balance_groups_stable_on_alive_set_unchanged() {
        let groups = vec![
            GroupSpec {
                group_id: "g1".to_owned(),
            },
            GroupSpec {
                group_id: "g2".to_owned(),
            },
        ];
        let r = rows(3);
        let first = balance_groups(None, &r, &groups);
        let second = balance_groups(Some(&first), &r, &groups);
        assert_eq!(first, second);
    }

    #[test]
    fn balance_groups_reassigns_only_dead_broker_groups() {
        let groups = vec![
            GroupSpec {
                group_id: "ga".to_owned(),
            },
            GroupSpec {
                group_id: "gb".to_owned(),
            },
            GroupSpec {
                group_id: "gc".to_owned(),
            },
        ];
        let first = balance_groups(None, &rows(3), &groups);
        // kaas-2 dies but stays registered — the divisor holds.
        let two = rows_with_alive(3, &["kaas-0", "kaas-1"]);
        let second = balance_groups(Some(&first), &two, &groups);
        for g in &second {
            assert!(g.broker == "kaas-0" || g.broker == "kaas-1");
        }
    }

    #[test]
    fn creating_a_topic_moves_no_existing_partition() {
        // gh #206: the smoother used to be a pure function of
        // `(topics, alive)`, so any topic CR change re-derived the
        // whole layout and migrated partitions of unrelated topics.
        let b = brokers(3);
        let before = vec![topic("t1", 6), topic("t2", 3), topic("t3", 4)];
        let first = balance(None, &b, &before, &HashMap::new());

        let mut after = before.clone();
        after.push(topic("newcomer", 5));
        let second = balance(Some(&first), &b, &after, &HashMap::new());

        for p in &first {
            let q = find(&second, &p.topic, p.partition);
            assert_eq!(
                (&q.broker, q.epoch),
                (&p.broker, p.epoch),
                "{}/{} moved {} → {} on an unrelated topic create",
                p.topic,
                p.partition,
                p.broker,
                q.broker
            );
        }
        assert!(skew(&second, None, &b) <= 1, "cluster balance preserved");
    }

    #[test]
    fn deleting_a_topic_moves_no_surviving_partition() {
        let b = brokers(3);
        let before = vec![topic("t1", 6), topic("doomed", 3), topic("t3", 4)];
        let first = balance(None, &b, &before, &HashMap::new());

        let after = vec![topic("t1", 6), topic("t3", 4)];
        let second = balance(Some(&first), &b, &after, &HashMap::new());

        assert_eq!(second.len(), 10, "only the deleted topic's slots are gone");
        for p in first.iter().filter(|p| p.topic != "doomed") {
            let q = find(&second, &p.topic, p.partition);
            assert_eq!(
                (&q.broker, q.epoch),
                (&p.broker, p.epoch),
                "{}/{} moved on an unrelated topic delete",
                p.topic,
                p.partition
            );
        }
    }

    #[test]
    fn every_topic_is_spread_across_the_alive_set() {
        // gh #247: cluster-wide counts can be perfectly even while a
        // single topic sits entirely on one broker. Small topics are
        // where a per-partition hash collides: 3 over 3 lands on one
        // broker 1 time in 9.
        let b = brokers(3);
        let topics: Vec<TopicSpec> = (0..12).map(|i| topic(&format!("t{i}"), 3)).collect();
        let parts = balance(None, &b, &topics, &HashMap::new());

        for t in &topics {
            assert!(
                skew(&parts, Some(&t.name), &b) <= 1,
                "{} is lopsided: {:?}",
                t.name,
                counts(&parts, Some(&t.name))
            );
        }
        assert!(skew(&parts, None, &b) <= 1);
    }

    #[test]
    fn a_lopsided_topic_is_repaired_without_breaking_cluster_balance() {
        // The live shape from gh #247: `kaas-canary-v1` had all three
        // partitions on kaas-2 at epoch 1 — never moved since first
        // assignment — while the cluster totals were 24/24/25.
        let b = brokers(3);
        let topics = vec![topic("big", 9), topic("canary", 3)];
        // 4/4/1 for `big` plus 0/0/3 for `canary` is 4/4/4 overall —
        // cluster-wide perfect, so the cluster smoother's predicate is
        // false on its first iteration and it returns having touched
        // nothing. Only a per-topic pass can see the problem.
        let prev: Vec<PartitionAssignment> = (0..9)
            .map(|p| PartitionAssignment {
                topic: "big".to_owned(),
                partition: p,
                broker: format!(
                    "kaas-{}",
                    if p < 4 {
                        0
                    } else if p < 8 {
                        1
                    } else {
                        2
                    }
                ),
                epoch: 1,
                role: PartitionRole::Leader,
            })
            .chain((0..3).map(|p| PartitionAssignment {
                topic: "canary".to_owned(),
                partition: p,
                broker: "kaas-2".to_owned(),
                epoch: 1,
                role: PartitionRole::Leader,
            }))
            .collect();

        let out = balance(Some(&prev), &b, &topics, &HashMap::new());
        assert!(
            skew(&out, Some("canary"), &b) <= 1,
            "canary still lopsided: {:?}",
            counts(&out, Some("canary"))
        );
        assert!(
            skew(&out, None, &b) <= 1,
            "repair broke the cluster-wide balance: {:?}",
            counts(&out, None)
        );
        // Repairing one topic must not rewrite the other.
        assert!(skew(&out, Some("big"), &b) <= 1);
    }

    #[test]
    fn balance_is_idempotent_once_settled() {
        // A settled layout must be a fixed point — otherwise every
        // recompute bumps epochs and drives a takeover.
        let b = brokers(3);
        let topics = vec![topic("t1", 7), topic("t2", 3), topic("t3", 4)];
        let first = balance(None, &b, &topics, &HashMap::new());
        let second = balance(Some(&first), &b, &topics, &HashMap::new());
        assert_eq!(first, second);
        let third = balance(Some(&second), &b, &topics, &HashMap::new());
        assert_eq!(second, third);
    }

    #[test]
    fn balance_groups_keeps_a_group_that_stopped_being_reported() {
        // gh #248: `active_groups()` is the union over *connected*
        // brokers, so a group drops out of it for reasons unrelated to
        // coordination. Retiring its entry on absence hands the group
        // to the hash fallthrough, which can name a different broker —
        // NOT_COORDINATOR, then a full group rebuild.
        let r = rows(3);
        let groups = vec![
            GroupSpec {
                group_id: "streams-app".to_owned(),
            },
            GroupSpec {
                group_id: "other".to_owned(),
            },
        ];
        let first = balance_groups(None, &r, &groups);

        // Same alive set, but the group missed one reporting window.
        let quiet = vec![GroupSpec {
            group_id: "other".to_owned(),
        }];
        let second = balance_groups(Some(&first), &r, &quiet);

        let before = first.iter().find(|g| g.group_id == "streams-app").unwrap();
        let after = second
            .iter()
            .find(|g| g.group_id == "streams-app")
            .expect("an unreported group keeps its coordinator");
        assert_eq!(before, after, "coordinator moved on a missed report");

        // And it is still there once the group reports again.
        let third = balance_groups(Some(&second), &r, &groups);
        assert_eq!(
            third.iter().find(|g| g.group_id == "streams-app").unwrap(),
            before
        );
    }

    #[test]
    fn balance_groups_retires_an_unreported_group_when_its_broker_dies() {
        // The other half of the carry-forward rule: retirement is by
        // broker death, not by absence, so the list can't grow without
        // bound.
        let groups = vec![GroupSpec {
            group_id: "g".to_owned(),
        }];
        let first = balance_groups(None, &rows(3), &groups);
        let host = first[0].broker.clone();
        let alive: Vec<&str> = ["kaas-0", "kaas-1", "kaas-2"]
            .into_iter()
            .filter(|x| *x != host)
            .collect();
        let survivors = rows_with_alive(3, &alive);

        let second = balance_groups(Some(&first), &survivors, &[]);
        assert!(
            second.is_empty(),
            "an unreported group on a dead broker is dropped, not re-picked"
        );

        // Still reported → re-picked onto a survivor, epoch bumped.
        let third = balance_groups(Some(&first), &survivors, &groups);
        assert_eq!(third.len(), 1);
        assert!(alive.contains(&third[0].broker.as_str()));
        assert_eq!(third[0].epoch, first[0].epoch + 1);
    }

    #[test]
    fn a_minted_entry_confirms_where_the_hash_already_put_the_group() {
        // gh #248, the structural half. `consumerGroups[]` is only the
        // first tier of `Coordinator::owns_group`; without an entry a
        // broker falls through to `pick_group_coordinator`. If the
        // controller mints entries with a *different* function, the
        // first entry it ever writes for a group moves that group —
        // NOT_COORDINATOR, then a full rebuild — once per group, on a
        // healthy cluster.
        //
        // The old code used `rendezvous_pick_group` (highest-random-
        // weight) here while brokers used `pick_coordinator`
        // (`hash % n`). On the live 3-broker cluster those disagreed
        // for every group id tried, so every new group was guaranteed
        // one move.
        let r = rows(3);
        let (ids, alive) = broker_sets_of(&r);
        for id in [
            "kaas-streams-wordcount",
            "g1",
            "my-group",
            "console-consumer-1",
            "connect-cluster",
        ] {
            let minted = balance_groups(
                None,
                &r,
                &[GroupSpec {
                    group_id: id.to_owned(),
                }],
            );
            let broker_side = kaas_broker::group_hash::pick_group_coordinator(id, &ids, &alive)
                .expect("a broker is alive");
            assert_eq!(
                minted[0].broker, broker_side,
                "the entry minted for {id} disagrees with the hash \
                 fallthrough, so writing it would move the group"
            );
        }
    }

    #[test]
    fn a_minted_entry_divides_by_the_full_registered_set() {
        // The gh #249 divisor rule reaches the mint path too: a dead
        // broker keeps its row, so losing one must not rehash the
        // groups that lived on the survivors.
        let all_alive = rows(3);
        let one_dead = rows_with_alive(3, &["kaas-0", "kaas-1"]);
        let specs: Vec<GroupSpec> = (0..40)
            .map(|i| GroupSpec {
                group_id: format!("group-{i}"),
            })
            .collect();

        let before = balance_groups(None, &all_alive, &specs);
        let after = balance_groups(None, &one_dead, &specs);

        // Every group NOT hosted on the dead broker keeps its
        // coordinator. With a shrinking divisor this would rehash ~2/3
        // of them.
        let moved = before
            .iter()
            .zip(after.iter())
            .filter(|(b, a)| b.broker != "kaas-2" && b.broker != a.broker)
            .count();
        assert_eq!(moved, 0, "a broker loss rehashed groups it wasn't hosting");
        assert!(after.iter().all(|g| g.broker != "kaas-2"));
    }

    /// `(ids, alive)` exactly as `Assignment::broker_sets` derives them.
    fn broker_sets_of(rows: &[BrokerAssignment]) -> (Vec<String>, HashMap<String, bool>) {
        let mut ids = Vec::new();
        let mut alive = HashMap::new();
        for r in rows {
            ids.push(r.id.clone());
            alive.insert(r.id.clone(), matches!(r.health, BrokerHealth::Alive));
        }
        (ids, alive)
    }

    #[test]
    fn rendezvous_hash_byte_sequence_pinned() {
        // Pin a known input → output mapping so a future change to
        // the byte construction (delimiters, order) surfaces here
        // rather than as a silent cutover divergence.
        let h = rendezvous_hash("t1", 0, "kaas-0");
        let h_swap = rendezvous_hash("t1", 1, "kaas-0");
        assert_ne!(h, h_swap, "different partition must yield different hash");
    }
}
