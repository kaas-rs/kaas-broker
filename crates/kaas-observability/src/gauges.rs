//! Runtime observable gauges + [`GaugeSource`] trait.
//!
//! The
//! runtime gauges (`kaas.is.controller`, `kaas.assignment.version`,
//! per-partition leader/epoch/HWM) are registered once on the broker's
//! meter by [`crate::Providers::install_runtime_gauges`]. The operator
//! never registers them (gh #290). A single global [`GaugeSource`]
//! provides the snapshot at every scrape.
//!
//! A `None` source (the default before [`set_gauge_source`] is called)
//! makes every gauge report zero — dashboards prefer present-but-zero
//! over missing series.

use std::sync::Arc;

use arc_swap::ArcSwapOption;
use opentelemetry::metrics::Meter;
use opentelemetry::KeyValue;

/// One row in the per-partition gauge sample.
#[derive(Debug, Clone)]
pub struct PartitionGauge {
    pub topic: String,
    pub partition: i32,
    pub leader_id: i64,
    pub epoch: i64,
    pub high_watermark: i64,
}

/// Snapshot the v3 runtime state that the Phase 10 gauges sample.
///
/// Implementations must be safe to call from a metrics callback
/// (once per push interval) and complete quickly — the SDK serialises
/// callbacks per scrape, so a slow source stalls every other gauge.
pub trait GaugeSource: Send + Sync + 'static {
    fn is_controller(&self) -> i64;
    fn assignment_version(&self) -> i64;
    fn broker_count_alive(&self) -> i64;
    fn broker_count_assigned(&self) -> i64;
    fn assignment_file_size_bytes(&self) -> i64;
    fn partitions(&self) -> Vec<PartitionGauge>;
    /// gh #221 phase 3: per-log-dir filesystem capacity (KIP-827
    /// numbers, exported as gauges so the pool's headroom is
    /// dashboardable). Default empty for sources predating the
    /// volume pool.
    fn log_dirs(&self) -> Vec<LogDirCapacityGauge> {
        Vec::new()
    }
}

/// One log dir's capacity sample (KIP-827 shape; `-1` = unknown).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirCapacityGauge {
    pub name: String,
    pub total_bytes: i64,
    pub usable_bytes: i64,
}

fn source_slot() -> &'static ArcSwapOption<Box<dyn GaugeSource>> {
    static SLOT: std::sync::OnceLock<ArcSwapOption<Box<dyn GaugeSource>>> =
        std::sync::OnceLock::new();
    SLOT.get_or_init(|| ArcSwapOption::from(None))
}

/// Install the snapshot source. Called by `bins/kaas::main` after
/// the v3 runtime is up. Pass `None` to reset to the no-op default
/// (every gauge reports zero).
pub fn set_gauge_source(s: Option<Box<dyn GaugeSource>>) {
    source_slot().store(s.map(Arc::new));
}

fn load_source() -> Option<Arc<Box<dyn GaugeSource>>> {
    source_slot().load_full()
}

/// Register the Phase 10 observable gauges on `meter`. Called once by
/// the broker, via [`crate::Providers::install_runtime_gauges`]. Safe to call before
/// [`set_gauge_source`] — until a source is installed, every gauge
/// reports 0.
pub fn install_runtime_gauges(meter: &Meter) {
    meter
        .i64_observable_gauge("kaas.is.controller")
        .with_description("1 if this broker holds the cluster controller lease, 0 otherwise")
        .with_callback(|observer| match load_source() {
            Some(src) => observer.observe(src.is_controller(), &[]),
            None => observer.observe(0, &[]),
        })
        .build();

    meter
        .i64_observable_gauge("kaas.assignment.version")
        .with_description("Most recent assignmentVersion applied by this broker")
        .with_callback(|observer| match load_source() {
            Some(src) => observer.observe(src.assignment_version(), &[]),
            None => observer.observe(0, &[]),
        })
        .build();

    meter
        .i64_observable_gauge("kaas.broker.count.alive")
        .with_description("Live brokers as observed by this broker")
        .with_callback(|observer| match load_source() {
            Some(src) => observer.observe(src.broker_count_alive(), &[]),
            None => observer.observe(0, &[]),
        })
        .build();

    meter
        .i64_observable_gauge("kaas.broker.count.assigned")
        .with_description("Distinct brokers in the current assignment.json")
        .with_callback(|observer| match load_source() {
            Some(src) => observer.observe(src.broker_count_assigned(), &[]),
            None => observer.observe(0, &[]),
        })
        .build();

    meter
        .i64_observable_gauge("kaas.assignment.file.size")
        .with_description("Size of assignment.json in the cluster-state dir")
        .with_unit("By")
        .with_callback(|observer| match load_source() {
            Some(src) => observer.observe(src.assignment_file_size_bytes(), &[]),
            None => observer.observe(0, &[]),
        })
        .build();

    meter
        .i64_observable_gauge("kaas.log.dir.total.bytes")
        .with_description("Filesystem capacity of each log dir (KIP-827 TotalBytes; -1 unknown)")
        .with_unit("By")
        .with_callback(|observer| {
            if let Some(src) = load_source() {
                for d in src.log_dirs() {
                    observer.observe(
                        d.total_bytes,
                        &[opentelemetry::KeyValue::new("log_dir", d.name.clone())],
                    );
                }
            }
        })
        .build();

    meter
        .i64_observable_gauge("kaas.log.dir.usable.bytes")
        .with_description(
            "Usable bytes remaining on each log dir (KIP-827 UsableBytes; -1 unknown)",
        )
        .with_unit("By")
        .with_callback(|observer| {
            if let Some(src) = load_source() {
                for d in src.log_dirs() {
                    observer.observe(
                        d.usable_bytes,
                        &[opentelemetry::KeyValue::new("log_dir", d.name.clone())],
                    );
                }
            }
        })
        .build();

    meter
        .i64_observable_gauge("kaas.partition.leader")
        .with_description("Per-partition leader broker ordinal")
        .with_callback(|observer| {
            if let Some(src) = load_source() {
                for p in src.partitions() {
                    observer.observe(
                        p.leader_id,
                        &[
                            KeyValue::new("topic", p.topic),
                            KeyValue::new("partition", i64::from(p.partition)),
                        ],
                    );
                }
            }
        })
        .build();

    meter
        .i64_observable_gauge("kaas.partition.epoch")
        .with_description("Per-partition leader epoch")
        .with_callback(|observer| {
            if let Some(src) = load_source() {
                for p in src.partitions() {
                    observer.observe(
                        p.epoch,
                        &[
                            KeyValue::new("topic", p.topic),
                            KeyValue::new("partition", i64::from(p.partition)),
                        ],
                    );
                }
            }
        })
        .build();

    meter
        .i64_observable_gauge("kaas.partition.high.watermark")
        .with_description("Per-partition high watermark offset")
        .with_callback(|observer| {
            if let Some(src) = load_source() {
                for p in src.partitions() {
                    observer.observe(
                        p.high_watermark,
                        &[
                            KeyValue::new("topic", p.topic),
                            KeyValue::new("partition", i64::from(p.partition)),
                        ],
                    );
                }
            }
        })
        .build();
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    struct FakeSource(i64);
    impl GaugeSource for FakeSource {
        fn is_controller(&self) -> i64 {
            self.0
        }
        fn assignment_version(&self) -> i64 {
            self.0 + 1
        }
        fn broker_count_alive(&self) -> i64 {
            3
        }
        fn broker_count_assigned(&self) -> i64 {
            3
        }
        fn assignment_file_size_bytes(&self) -> i64 {
            1024
        }
        fn partitions(&self) -> Vec<PartitionGauge> {
            vec![]
        }
    }

    #[test]
    fn set_and_clear_source() {
        set_gauge_source(Some(Box::new(FakeSource(1))));
        let s = load_source().unwrap();
        assert_eq!(s.is_controller(), 1);
        assert_eq!(s.assignment_version(), 2);

        set_gauge_source(None);
        assert!(load_source().is_none());
    }
}
