//! Per-topic Produce / Fetch counters that always emit at scrape.
//!
//! Per-topic traffic counters (gh #115, gh #121 PR1). Apache Kafka's `BytesInPerSec` / `BytesOutPerSec`
//! MBeans emit a current cumulative value at every scrape (idle topics
//! read zero, not "no data"). Pre-#121 kaas used `Int64Counter`
//! instruments that only emit on `Add()`, so idle topics disappeared
//! from the timeseries and `rate()` panels gapped.
//!
//! Design: per-topic atomic accumulators, updated by one atomic-add on
//! the hot path. The 4 [`ObservableCounter`] instruments each carry
//! their own callback that walks a snapshot of the map and observes
//! every topic at every scrape — even topics that haven't seen traffic
//! since boot.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

/// Always-emit per-topic accumulators + the OTel instrument bindings.
#[derive(Debug, Default)]
pub struct TopicTrafficMeter {
    counters: DashMap<String, Arc<TopicCounters>>,
}

#[derive(Debug, Default)]
struct TopicCounters {
    produce_records: AtomicI64,
    produce_bytes: AtomicI64,
    fetch_records: AtomicI64,
    fetch_bytes: AtomicI64,
}

impl TopicTrafficMeter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            counters: DashMap::new(),
        }
    }

    /// Ensure an accumulator exists for `topic`. Idempotent. After this,
    /// the topic emits zero on every scrape until traffic arrives —
    /// eliminating the gh #115 "no data" gap on idle topics.
    pub fn touch(&self, topic: &str) {
        if topic.is_empty() {
            return;
        }
        self.counters
            .entry(topic.to_string())
            .or_insert_with(|| Arc::new(TopicCounters::default()));
    }

    /// Drop the accumulator. Called on `KafkaTopic` CR delete
    /// (`Broker::forget_deleted_topic`, gh #261) so orphan timeseries
    /// don't linger on the dashboard.
    pub fn forget(&self, topic: &str) {
        self.counters.remove(topic);
    }

    /// Whether `topic` has an accumulator, i.e. emits a series at every
    /// scrape. For assertions, like [`Self::produce_records`].
    #[must_use]
    pub fn tracks(&self, topic: &str) -> bool {
        self.counters.contains_key(topic)
    }

    /// Hot-path from the Produce handler. Auto-touches the topic.
    pub fn record_produce(&self, topic: &str, records: i64, bytes: i64) {
        let tc = self.ensure(topic);
        if records > 0 {
            tc.produce_records.fetch_add(records, Ordering::Relaxed);
        }
        if bytes > 0 {
            tc.produce_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    /// Current cumulative produce record count for `topic` (0 if the
    /// topic has no counter yet). Production reads go through the
    /// observable-counter callback; this accessor is for assertions.
    #[must_use]
    pub fn produce_records(&self, topic: &str) -> i64 {
        self.counters
            .get(topic)
            .map(|tc| tc.produce_records.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Hot-path from the Fetch handler. Auto-touches the topic.
    /// `bytes` may be 0 (empty Fetch response) — the accumulator still
    /// gets touched so idle topics keep emitting.
    pub fn record_fetch(&self, topic: &str, records: i64, bytes: i64) {
        let tc = self.ensure(topic);
        if records > 0 {
            tc.fetch_records.fetch_add(records, Ordering::Relaxed);
        }
        if bytes > 0 {
            tc.fetch_bytes.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn ensure(&self, topic: &str) -> Arc<TopicCounters> {
        if topic.is_empty() {
            // Return a throwaway accumulator so hot-path callers don't
            // need to nil-check. Cheap: 4 AtomicI64.
            return Arc::new(TopicCounters::default());
        }
        // Fast path: read-only entry access via DashMap.
        if let Some(existing) = self.counters.get(topic) {
            return Arc::clone(&existing);
        }
        Arc::clone(
            &self
                .counters
                .entry(topic.to_string())
                .or_insert_with(|| Arc::new(TopicCounters::default())),
        )
    }

    fn snapshot(&self) -> Vec<TopicSnapshot> {
        self.counters
            .iter()
            .map(|entry| TopicSnapshot {
                topic: entry.key().clone(),
                produce_records: entry.value().produce_records.load(Ordering::Relaxed),
                produce_bytes: entry.value().produce_bytes.load(Ordering::Relaxed),
                fetch_records: entry.value().fetch_records.load(Ordering::Relaxed),
                fetch_bytes: entry.value().fetch_bytes.load(Ordering::Relaxed),
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
struct TopicSnapshot {
    topic: String,
    produce_records: i64,
    produce_bytes: i64,
    fetch_records: i64,
    fetch_bytes: i64,
}

/// Register the four always-emit observable counters against `meter`,
/// each with a callback that walks `traffic`'s snapshot.
///
/// Kept as a free function (rather than an `impl` method) so the
/// callbacks capture an `Arc<TopicTrafficMeter>` cleanly — `&self`
/// methods would fight with the callback's `'static` lifetime bound.
pub fn register_topic_traffic_instruments(meter: &Meter, traffic: Arc<TopicTrafficMeter>) {
    let t = Arc::clone(&traffic);
    meter
        .u64_observable_counter("kaas.produce.records")
        .with_description(
            "Cumulative records produced per topic. Emits at every scrape interval (including 0 for idle topics) so dashboard rate() panels never gap.",
        )
        .with_unit("{record}")
        .with_callback(move |observer| {
            for snap in t.snapshot() {
                observer.observe(
                    to_u64(snap.produce_records),
                    &[KeyValue::new("topic", snap.topic)],
                );
            }
        })
        .build();

    let t = Arc::clone(&traffic);
    meter
        .u64_observable_counter("kaas.produce.bytes")
        .with_description("Cumulative bytes produced per topic. Idle-emit invariant.")
        .with_unit("By")
        .with_callback(move |observer| {
            for snap in t.snapshot() {
                observer.observe(
                    to_u64(snap.produce_bytes),
                    &[KeyValue::new("topic", snap.topic)],
                );
            }
        })
        .build();

    let t = Arc::clone(&traffic);
    meter
        .u64_observable_counter("kaas.fetch.records")
        .with_description("Cumulative records fetched per topic. Idle-emit invariant.")
        .with_unit("{record}")
        .with_callback(move |observer| {
            for snap in t.snapshot() {
                observer.observe(
                    to_u64(snap.fetch_records),
                    &[KeyValue::new("topic", snap.topic)],
                );
            }
        })
        .build();

    let t = Arc::clone(&traffic);
    meter
        .u64_observable_counter("kaas.fetch.bytes")
        .with_description("Cumulative bytes fetched per topic. Idle-emit invariant.")
        .with_unit("By")
        .with_callback(move |observer| {
            for snap in t.snapshot() {
                observer.observe(
                    to_u64(snap.fetch_bytes),
                    &[KeyValue::new("topic", snap.topic)],
                );
            }
        })
        .build();
}

fn to_u64(v: i64) -> u64 {
    // Cumulative counters are non-negative; negative values would be a
    // logic bug upstream (underflow). Saturate at 0 so a corrupt
    // accumulator doesn't produce a bogus 9 exabyte value.
    u64::try_from(v).unwrap_or(0)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn touch_forget_are_idempotent() {
        let m = TopicTrafficMeter::new();
        m.touch("foo");
        m.touch("foo");
        assert!(m.counters.contains_key("foo"));
        m.forget("foo");
        m.forget("foo");
        assert!(!m.counters.contains_key("foo"));
    }

    #[test]
    fn empty_topic_is_ignored() {
        let m = TopicTrafficMeter::new();
        m.record_produce("", 1, 100);
        m.touch("");
        assert!(m.counters.is_empty());
    }

    #[test]
    fn record_produce_and_fetch_accumulate() {
        let m = TopicTrafficMeter::new();
        m.record_produce("foo", 5, 1000);
        m.record_produce("foo", 3, 500);
        m.record_fetch("foo", 4, 800);

        let snaps = m.snapshot();
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].produce_records, 8);
        assert_eq!(snaps[0].produce_bytes, 1500);
        assert_eq!(snaps[0].fetch_records, 4);
        assert_eq!(snaps[0].fetch_bytes, 800);
    }

    #[test]
    fn snapshot_returns_all_topics() {
        let m = TopicTrafficMeter::new();
        m.record_produce("foo", 1, 100);
        m.record_produce("bar", 2, 200);
        m.touch("baz"); // idle-emit case

        let snaps = m.snapshot();
        assert_eq!(snaps.len(), 3);
    }

    #[test]
    fn to_u64_saturates_negatives() {
        assert_eq!(to_u64(-1), 0);
        assert_eq!(to_u64(0), 0);
        assert_eq!(to_u64(42), 42);
    }
}
