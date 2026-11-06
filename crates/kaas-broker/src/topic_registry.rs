//! In-memory topic registry seeded from `KAAS_TOPICS` env JSON.
//!
//! Phase 3 stand-in for the `KafkaTopic` CR watcher that lands in
//! Phase 5/7. The shape is intentionally narrow — just what the
//! Metadata handler reads.

use std::collections::HashMap;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("topics seed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("topics seed: partitions must be > 0 for topic {0}")]
    InvalidPartitions(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopicMeta {
    pub name: String,
    pub partition_count: i32,
    /// 16-byte UUID. All-zero is the gh #105 fallback for legacy CRs
    /// without `Status.TopicID`. Phase 3 always emits all-zero (the
    /// operator will mint real ids in Phase 7).
    #[serde(default = "TopicMeta::null_topic_id")]
    pub topic_id: [u8; 16],
}

impl TopicMeta {
    fn null_topic_id() -> [u8; 16] {
        [0; 16]
    }
}

/// JSON shape accepted in `KAAS_TOPICS`. Mirrors the simplest
/// possible KafkaTopic CR projection — name + partitions. Extra
/// fields are ignored so the env-var can grow without breaking
/// downgrade.
#[derive(Debug, Deserialize)]
struct TopicSeedEntry {
    name: String,
    partitions: i32,
}

#[derive(Debug)]
pub struct TopicRegistry {
    inner: RwLock<HashMap<String, TopicMeta>>,
    /// gh #221 phase 2: topic → (partition-as-string → log-dir name),
    /// stashed from `KafkaTopic.status.volumeAssignments` by the
    /// topic watch. Side map (not a `TopicMeta` field) so the meta
    /// struct keeps its stable literal shape across the many
    /// construction sites. Keys are strings because the CR status
    /// map is JSON.
    volume_assignments: RwLock<HashMap<String, std::collections::BTreeMap<String, String>>>,
    /// gh #241: topic → `KafkaTopic.status.topicId`, stashed by the
    /// topic watch. A **present key with an empty value** means "CR
    /// seen, not stamped yet" and is load-bearing — the engine's
    /// identity gate treats that differently from an absent key (no CR
    /// knowledge at all). Side map for the same reason as
    /// `volume_assignments`, plus one of its own: `TopicMeta.topic_id`
    /// is what the Metadata handler serves on the wire, and filling it
    /// in would flip kaas from advertising nil topic IDs to real ones
    /// — that is gh #346's call to make, not this gate's.
    topic_ids: RwLock<HashMap<String, String>>,
    /// gh #241: has the `KafkaTopic` watch completed at least one full
    /// list? Until it has, this registry knows nothing and must not be
    /// used to withhold a partition — that is the difference between
    /// "the API server is unreachable, serve what's on disk" and "the
    /// watch is live and simply hasn't reached this topic yet".
    synced: std::sync::atomic::AtomicBool,
}

impl Default for TopicRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TopicRegistry {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            volume_assignments: RwLock::new(HashMap::new()),
            topic_ids: RwLock::new(HashMap::new()),
            synced: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn from_env_json(json: &str) -> Result<Self, ConfigError> {
        let entries: Vec<TopicSeedEntry> = if json.trim().is_empty() {
            Vec::new()
        } else {
            serde_json::from_str(json)?
        };
        let mut map = HashMap::with_capacity(entries.len());
        for e in entries {
            if e.partitions <= 0 {
                return Err(ConfigError::InvalidPartitions(e.name));
            }
            map.insert(
                e.name.clone(),
                TopicMeta {
                    name: e.name,
                    partition_count: e.partitions,
                    topic_id: [0; 16],
                },
            );
        }
        Ok(Self {
            inner: RwLock::new(map),
            volume_assignments: RwLock::new(HashMap::new()),
            topic_ids: RwLock::new(HashMap::new()),
            synced: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub fn get(&self, name: &str) -> Option<TopicMeta> {
        self.inner.read().get(name).cloned()
    }

    pub fn all(&self) -> Vec<TopicMeta> {
        let g = self.inner.read();
        let mut out: Vec<TopicMeta> = g.values().cloned().collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }

    pub fn insert(&self, m: TopicMeta) {
        self.inner.write().insert(m.name.clone(), m);
    }

    pub fn remove(&self, name: &str) {
        self.inner.write().remove(name);
        self.volume_assignments.write().remove(name);
        self.topic_ids.write().remove(name);
    }

    /// Replace the stashed partition→log-dir placement for `topic`
    /// (gh #221 phase 2). The topic watch calls this on every CR
    /// apply; an empty map clears the entry.
    pub fn set_volume_assignments(
        &self,
        topic: &str,
        map: std::collections::BTreeMap<String, String>,
    ) {
        if map.is_empty() {
            self.volume_assignments.write().remove(topic);
        } else {
            self.volume_assignments
                .write()
                .insert(topic.to_owned(), map);
        }
    }

    /// Point one partition at a log dir (gh #221 phase 3 — the
    /// AlterReplicaLogDirs cutover updates the local view immediately
    /// instead of waiting for the CR-status watch echo).
    pub fn set_volume_assignment(&self, topic: &str, partition: i32, log_dir: &str) {
        self.volume_assignments
            .write()
            .entry(topic.to_owned())
            .or_default()
            .insert(partition.to_string(), log_dir.to_owned());
    }

    /// Log-dir name hosting `(topic, partition)`, if explicitly
    /// placed. `None` → the default log dir.
    pub fn volume_assignment(&self, topic: &str, partition: i32) -> Option<String> {
        self.volume_assignments
            .read()
            .get(topic)?
            .get(&partition.to_string())
            .cloned()
    }

    /// Stash `KafkaTopic.status.topicId` for `topic` (gh #241). The
    /// topic watch calls this on every CR apply, passing `None` while
    /// the operator has yet to stamp the status — which is recorded as
    /// an empty string, NOT as an absent key: "CR seen, unstamped" and
    /// "never heard of it" mean opposite things to the identity gate.
    pub fn set_topic_id(&self, topic: &str, id: Option<&str>) {
        self.topic_ids
            .write()
            .insert(topic.to_owned(), id.unwrap_or_default().to_owned());
    }

    /// Record that the `KafkaTopic` watch has completed a full list, so
    /// the registry's *absences* start carrying meaning (gh #241).
    /// Latching, never cleared: a later disconnect doesn't un-know the
    /// topics we already hold.
    pub fn mark_synced(&self) {
        self.synced
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Has the `KafkaTopic` watch completed a full list? Until it has, a
    /// topic's absence here means nothing.
    pub fn is_synced(&self) -> bool {
        self.synced.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// The incarnation this broker believes `topic` is at (gh #241).
    ///
    /// The absent-key case is the subtle one. Before the watch has ever
    /// listed, absence means "we know nothing" → `Unknown` → the engine
    /// adopts whatever is on disk, which is what lets a broker serve
    /// existing topics with the API server unreachable. *After* a
    /// successful list, absence means "the watch is live and this topic
    /// hasn't reached us yet" → `Pending` → the engine waits rather than
    /// opening a directory the operator may be about to reclaim.
    ///
    /// That distinction is the whole gate in practice: a broker learns
    /// it leads a partition from `assignment.json` (1 s poll), which
    /// routinely beats its own CR watch, so a freshly (re)created topic
    /// is *always* momentarily absent here.
    pub fn incarnation(&self, topic: &str) -> kaas_storage::TopicIncarnation {
        let synced = self.synced.load(std::sync::atomic::Ordering::Relaxed);
        match self.topic_ids.read().get(topic) {
            None if !synced => kaas_storage::TopicIncarnation::Unknown,
            None => kaas_storage::TopicIncarnation::Pending,
            Some(id) if id.is_empty() => kaas_storage::TopicIncarnation::Pending,
            Some(id) => kaas_storage::TopicIncarnation::Known(id.clone()),
        }
    }

    pub fn len(&self) -> usize {
        self.inner.read().len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.read().is_empty()
    }
}

/// gh #221 phase 2: the registry IS the broker's placement source —
/// the topic watch stashes `KafkaTopic.status.volumeAssignments`
/// here, and the storage engine resolves partition roots through it.
impl kaas_storage::PlacementResolver for TopicRegistry {
    fn log_dir_of(&self, topic: &str, partition: i32) -> Option<String> {
        self.volume_assignment(topic, partition)
    }
}

/// gh #241: and the registry is also the incarnation source — the same
/// watch that feeds placement feeds `status.topicId`, which is what
/// lets the engine tell a directory the operator has reclaimed for
/// *this* incarnation from one it has not touched yet.
impl kaas_storage::TopicIdentityResolver for TopicRegistry {
    fn incarnation_of(&self, topic: &str) -> kaas_storage::TopicIncarnation {
        self.incarnation(topic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_json_is_empty_registry() {
        let r = TopicRegistry::from_env_json("").unwrap();
        assert!(r.is_empty());
    }

    #[test]
    fn seed_parses_two_topics() {
        let r = TopicRegistry::from_env_json(
            r#"[{"name":"t1","partitions":3},{"name":"t2","partitions":1}]"#,
        )
        .unwrap();
        assert_eq!(r.len(), 2);
        let t1 = r.get("t1").unwrap();
        assert_eq!(t1.partition_count, 3);
        assert_eq!(t1.topic_id, [0; 16]);
    }

    #[test]
    fn zero_partitions_rejected() {
        let err = TopicRegistry::from_env_json(r#"[{"name":"x","partitions":0}]"#).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidPartitions(_)));
    }

    /// gh #241: the three-valued incarnation. `Unknown` and `Pending`
    /// look alike from the outside and mean opposite things — the first
    /// adopts whatever is on disk, the second waits for the operator.
    #[test]
    fn incarnation_distinguishes_unknown_from_unstamped() {
        let r = TopicRegistry::new();
        assert_eq!(
            r.incarnation("never-seen"),
            kaas_storage::TopicIncarnation::Unknown
        );

        r.set_topic_id("t", None);
        assert_eq!(r.incarnation("t"), kaas_storage::TopicIncarnation::Pending);

        // Once the watch has listed, an *absent* topic flips from
        // Unknown to Pending: the watch is live, so absence means "not
        // delivered yet", and a broker learns it leads a partition from
        // assignment.json before its own watch catches up.
        r.mark_synced();
        assert_eq!(
            r.incarnation("never-seen"),
            kaas_storage::TopicIncarnation::Pending
        );

        r.set_topic_id("t", Some("uuid-1"));
        assert_eq!(
            r.incarnation("t"),
            kaas_storage::TopicIncarnation::Known("uuid-1".into())
        );

        // A delete drops the id. Post-sync that reads as Pending, not
        // Unknown — and refusing to open a deleted topic's directory is
        // exactly right; the assignment drops it in the same beat.
        r.remove("t");
        assert_eq!(r.incarnation("t"), kaas_storage::TopicIncarnation::Pending);
    }

    /// Before the first list, absence must stay Unknown — this is the
    /// arm that keeps a broker serving on-disk topics when the API
    /// server is unreachable.
    #[test]
    fn unsynced_registry_reports_everything_unknown() {
        let r = TopicRegistry::new();
        assert_eq!(
            r.incarnation("anything"),
            kaas_storage::TopicIncarnation::Unknown
        );
    }

    #[test]
    fn all_returns_sorted_by_name() {
        let r = TopicRegistry::from_env_json(
            r#"[{"name":"z","partitions":1},{"name":"a","partitions":1}]"#,
        )
        .unwrap();
        let all = r.all();
        assert_eq!(all[0].name, "a");
        assert_eq!(all[1].name, "z");
    }
}
