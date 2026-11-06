//! Per-topic configuration file: `/data/<topic>/.config.json`.
//!
//! The operator
//! writes this file when reconciling a `KafkaTopic` CR; the broker
//! reads it on partition open and via the `notify`-driven hot-reload
//! task (Phase 2 workstream D).
//!
//! Every field is `Option<...>` so "unset" survives the round trip
//! distinct from "set to 0" — the broker's per-partition override
//! fields treat `None` as "fall through to the engine default".

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

/// Filename written by the operator under `/data/<topic>/`.
pub const TOPIC_CONFIG_FILENAME: &str = ".config.json";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicConfigFile {
    /// `retention.ms` — segments older than this are eligible for the
    /// cleaner. Zero is interpreted by the cleaner as "engine default".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_ms: Option<i64>,

    /// `retention.bytes` — partition total above this triggers oldest-
    /// segment cleanup. Zero = unlimited.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retention_bytes: Option<i64>,

    /// `segment.bytes` — roll the active segment at this size.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_bytes: Option<i64>,

    /// `segment.ms` — roll the active segment once it has been open
    /// this long, whatever its size. Apache's default is 7 days, and
    /// it is what makes `retention.ms` work on a low-volume topic:
    /// retention only ever deletes closed segments.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub segment_ms: Option<i64>,

    /// `cleanup.policy` — `"delete"`, `"compact"`, or `"compact,delete"`.
    /// Empty string omitted from the JSON (v0.1 `omitempty` compatibility).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub cleanup_policy: String,

    /// `min.compaction.lag.ms` (KIP-58, gh #116).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_compaction_lag_ms: Option<i64>,

    /// `delete.retention.ms` (KIP-354, gh #116).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_retention_ms: Option<i64>,

    /// `flush.messages` (gh #213) — per-topic override of the
    /// broker-wide `KAAS_FLUSH_INTERVAL_MESSAGES`. `0` = flush only
    /// at segment roll; unset = broker default.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flush_messages: Option<i64>,
}

impl TopicConfigFile {
    /// Whether `cleanup.policy` includes `delete`, which is what permits
    /// time and size retention and `DeleteRecords`. Apache's
    /// `LogConfig.delete`. An unset policy is the default, `delete`.
    pub fn allows_delete(&self) -> bool {
        self.cleanup_policy.is_empty()
            || self.cleanup_policy.split(',').any(|p| p.trim() == "delete")
    }
}

/// Kafka's "unlimited / never" sentinel is `-1`; `0` reaches us as an
/// unset field far more often than as a deliberate "immediately", so
/// both map to `None`.
pub fn sentinel_to_option(v: i64) -> Option<u64> {
    if v <= 0 {
        None
    } else {
        u64::try_from(v).ok()
    }
}

/// Layer a topic's `segment.bytes` / `segment.ms` over `base`.
///
/// Kept here rather than in the engine so the partition-open path and
/// the retention sweep resolve a topic's tuning the same way — they
/// disagreed for the life of the project, in the sense that only one of
/// them existed: every partition rolled at the global default and a
/// topic's `segmentBytes` was written to disk and read by nobody.
pub fn apply_to_partition_config(
    cfg: &TopicConfigFile,
    base: &crate::partition::PartitionConfig,
) -> crate::partition::PartitionConfig {
    let mut out = base.clone();
    if let Some(b) = cfg.segment_bytes.and_then(sentinel_to_option) {
        out.segment_bytes = b;
    }
    // `-1` is a real Apache setting here ("never roll on time"), so an
    // explicit value always wins — including when it disables the roll.
    if let Some(raw) = cfg.segment_ms {
        out.segment_ms = sentinel_to_option(raw);
    }
    // gh #213: per-topic `flush.messages`. An explicit value always
    // wins — including `0`, which means "flush only at segment roll"
    // (the committer's trigger predicate treats <= 0 as never).
    if let Some(v) = cfg.flush_messages {
        out.flush_interval_messages = v;
    }
    out
}

/// Read the per-topic config file. Returns `Ok(None)` when the file
/// is absent — the broker falls back to engine defaults.
pub fn read_topic_config(fs: &dyn Fs, topic_dir: &Path) -> io::Result<Option<TopicConfigFile>> {
    let path = topic_dir.join(TOPIC_CONFIG_FILENAME);
    match fs.open_read(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf)?;
            let c: TopicConfigFile = serde_json::from_slice(&buf).map_err(io::Error::other)?;
            Ok(Some(c))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Atomically write the per-topic config file. Called by the operator
/// (Phase 7 wires the reconciler).
pub fn write_topic_config(fs: &dyn Fs, topic_dir: &Path, c: &TopicConfigFile) -> io::Result<()> {
    atomic_write_json(fs, topic_dir, TOPIC_CONFIG_FILENAME, c)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn empty_config_is_a_bare_object() {
        let c = TopicConfigFile::default();
        let json = serde_json::to_string(&c).unwrap();
        assert_eq!(
            json, "{}",
            "default config should serialize to empty object"
        );
    }

    #[test]
    fn only_set_fields_are_emitted() {
        let c = TopicConfigFile {
            retention_ms: Some(86_400_000),
            cleanup_policy: "compact,delete".into(),
            ..Default::default()
        };
        let json = serde_json::to_string(&c).unwrap();
        // Field order = struct declaration order via serde.
        assert_eq!(
            json,
            r#"{"retentionMs":86400000,"cleanupPolicy":"compact,delete"}"#
        );
    }

    #[test]
    fn flush_messages_overrides_the_engine_default() {
        let base = crate::partition::PartitionConfig {
            flush_interval_messages: 500,
            ..Default::default()
        };
        // Explicit override wins — including 0 (flush only at roll).
        for (set, want) in [(Some(1), 1), (Some(0), 0), (None, 500)] {
            let cfg = TopicConfigFile {
                flush_messages: set,
                ..Default::default()
            };
            assert_eq!(
                apply_to_partition_config(&cfg, &base).flush_interval_messages,
                want,
                "flush_messages={set:?}"
            );
        }
    }

    #[test]
    fn missing_file_returns_none() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        assert!(read_topic_config(&fs, tmp.path()).unwrap().is_none());
    }

    #[test]
    fn roundtrip_preserves_unset_vs_zero() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        let c = TopicConfigFile {
            retention_ms: Some(0),
            retention_bytes: None,
            segment_bytes: Some(1 << 30),
            segment_ms: None,
            cleanup_policy: "compact".into(),
            min_compaction_lag_ms: None,
            delete_retention_ms: Some(86_400_000),
            flush_messages: Some(1),
        };
        write_topic_config(&fs, tmp.path(), &c).unwrap();
        let got = read_topic_config(&fs, tmp.path()).unwrap().unwrap();
        assert_eq!(got, c, "unset vs zero must round-trip distinctly");
    }
}
