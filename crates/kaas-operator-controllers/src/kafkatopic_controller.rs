//! Reconciler that materialises a `KafkaTopic` CR into:
//!
//! - Partition directories at `<data_dir>/<effective_topic_name>/<0..partitions>/`.
//! - A per-topic `.config.json` next to the topic dir, consumed by the
//!   broker's cleaner / compactor via `kaas_storage::TopicConfigFile`.
//! - `Status.TopicID` — a v4 UUID minted on first reconcile, never
//!   rotated (gh #105, KIP-516), and stamped onto the topic dir as
//!   `.topic-id.json` so a recreated topic can be told apart from its
//!   predecessor.
//!
//! Cleanup model is reconcile-time + sweep — no finalizers, and no
//! delete event is load-bearing:
//!
//! - **Deleted and recreated** under the same name (gh #219): the
//!   reconciler sees a stamp that isn't this incarnation's TopicID and
//!   stages the old directory aside *before* the new one uses it. See
//!   `KafkaTopicReconciler::reclaim_stale_incarnation`.
//! - **Deleted for good**: the leader-elected periodic
//!   [`crate::sweep::sweep_topics`] pass reclaims the orphan. Latency
//!   there is fine — nothing is waiting on the directory.
//!
//! See the gh #76 / gh #86 / gh #203 notes inline.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use kaas_operator_api::{Condition, KafkaTopic, KafkaTopicConfig};
use kaas_storage::TopicConfigFile;
use kube::api::{Patch, PatchParams};
use kube::runtime::controller::Action;
use kube::{Api, Client};

use crate::conditions::{set_condition, READY};
use crate::errors::ControllerError;
use crate::observer::ReconcileObserver;

/// Reconciler context. One per controller; cloned per-reconcile via
/// `Arc<Ctx>`.
pub struct KafkaTopicReconciler {
    pub client: Client,
    pub data_dir: PathBuf,
    /// gh #221 phase 2: pool log dirs beyond the default data dir
    /// (parsed from `KAAS_LOG_DIRS`; empty = single-volume layout).
    pub log_dirs: Vec<kaas_storage::LogDirInfo>,
    pub observer: ReconcileObserver,
}

impl KafkaTopicReconciler {
    pub fn new(client: Client, data_dir: PathBuf, log_dirs: Vec<kaas_storage::LogDirInfo>) -> Self {
        Self {
            client,
            data_dir,
            log_dirs,
            observer: ReconcileObserver::new("KafkaTopic"),
        }
    }

    /// Root path for a log-dir name; unknown names fall back to the
    /// data dir, mirroring the broker's resolver so both sides agree
    /// on where a mis-referenced partition actually lives.
    fn root_of(&self, name: &str) -> PathBuf {
        if name == kaas_storage::DEFAULT_LOG_DIR_NAME {
            return self.data_dir.clone();
        }
        self.log_dirs
            .iter()
            .find(|d| d.name == name)
            .map(|d| d.path.clone())
            .unwrap_or_else(|| self.data_dir.clone())
    }

    /// Reconcile entry point. The top-level [`reconcile_topic`]
    /// wrapper adapts this to kube-rs's `Controller::run` signature.
    pub async fn reconcile(&self, topic: Arc<KafkaTopic>) -> Result<Action, ControllerError> {
        let started = std::time::Instant::now();
        let out = self.reconcile_inner(topic).await;
        self.observer
            .record_duration(started.elapsed().as_secs_f64());
        out
    }

    async fn reconcile_inner(&self, topic: Arc<KafkaTopic>) -> Result<Action, ControllerError> {
        // A CR mid-delete (deletionTimestamp non-nil) has nothing to
        // materialise. Its directory is reclaimed later — by the sweep
        // if the topic stays gone, or by `reclaim_stale_incarnation`
        // when a topic of the same name is recreated. Neither path
        // depends on this reconcile, so bail out cleanly.
        if topic.metadata.deletion_timestamp.is_some() {
            self.observer.bump_requeue();
            return Ok(Action::await_change());
        }

        let topic_name = topic.effective_topic_name().to_string();
        if topic_name.is_empty() {
            // Defensive: a CR without metadata.name AND without spec.topic_name
            // wouldn't have made it past the apiserver, but if it does we
            // can't dispatch to any directory. Skip without erroring.
            self.observer.bump_error();
            return Ok(Action::await_change());
        }

        // Partition-decrease guard:
        // ConditionFalse with reason `InvalidPartitionCount` and DO NOT
        // mutate the filesystem. Caller is expected to fix the CR.
        let existing_count = topic
            .status
            .as_ref()
            .map(|s| s.partition_count)
            .unwrap_or(0);
        if existing_count > 0 && topic.spec.partitions < existing_count {
            let cond = Condition {
                type_: READY.into(),
                status: Condition::STATUS_FALSE.into(),
                observed_generation: topic.metadata.generation,
                last_transition_time: String::new(),
                reason: "InvalidPartitionCount".into(),
                message: "reducing partition count is not supported".into(),
            };
            self.patch_status(&topic, |st| set_condition(&mut st.conditions, cond.clone()))
                .await?;
            self.observer.bump_error();
            // No requeue — user has to fix the CR; await_change wakes us on the next edit.
            return Ok(Action::await_change());
        }

        // TopicID first: it is this incarnation's identity, and step 1
        // needs it to tell "our directory" from "the previous
        // incarnation's directory sitting at the same path".
        let next_topic_id = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);

        // gh #221 phase 2: resolve the topic's eligible log-dir set
        // and extend the creation-sticky placement for partitions
        // that don't have one yet. An unknown volume name is a loud
        // reconcile failure — a typo must not silently place data.
        let eligible = match eligible_log_dirs(&self.log_dirs, topic.spec.storage.as_ref()) {
            Ok(set) => set,
            Err(why) => {
                let cond = Condition {
                    type_: READY.into(),
                    status: Condition::STATUS_FALSE.into(),
                    observed_generation: topic.metadata.generation,
                    last_transition_time: String::new(),
                    reason: "InvalidVolumeBinding".into(),
                    message: why,
                };
                self.patch_status(&topic, |st| set_condition(&mut st.conditions, cond.clone()))
                    .await?;
                self.observer.bump_error();
                return Ok(Action::await_change());
            }
        };
        let mut assignments = topic
            .status
            .as_ref()
            .map(|s| s.volume_assignments.clone())
            .unwrap_or_default();
        for p in 0..topic.spec.partitions {
            assignments.entry(p.to_string()).or_insert_with(|| {
                eligible[usize::try_from(p).unwrap_or(0) % eligible.len()].clone()
            });
        }
        // Drift (never auto-migrated, gh #221): partitions placed on
        // dirs outside the *current* spec set keep serving where they
        // are and are only counted.
        let outside_spec = i32::try_from(
            (0..topic.spec.partitions)
                .filter(|p| {
                    assignments
                        .get(&p.to_string())
                        .is_some_and(|name| !eligible.contains(name))
                })
                .count(),
        )
        .unwrap_or(i32::MAX);

        // 1. Reclaim stale incarnations on every involved root, then
        // mkdir partition dirs (both idempotent). The reclaim must
        // run per root: a delete→recreate leaves the old
        // incarnation's dirs on whichever volumes hosted it.
        let fs = kaas_storage::fs::RealFs::new();
        let mut involved_roots: Vec<PathBuf> = vec![self.data_dir.clone()];
        for name in assignments.values() {
            let root = self.root_of(name);
            if !involved_roots.contains(&root) {
                involved_roots.push(root);
            }
        }
        let mut identity = kaas_storage::IdentityVerdict::Match;
        let mut staged_paths: Vec<PathBuf> = Vec::new();
        for root in &involved_roots {
            let topic_dir = root.join(&topic_name);
            let reclaimed =
                reclaim_stale_incarnation(root, &topic_name, &topic_dir, &next_topic_id, &fs)?;
            if reclaimed.verdict != kaas_storage::IdentityVerdict::Match {
                identity = reclaimed.verdict;
            }
            staged_paths.extend(reclaimed.staged);
        }
        for p in 0..topic.spec.partitions {
            let name = assignments
                .get(&p.to_string())
                .map(String::as_str)
                .unwrap_or(kaas_storage::DEFAULT_LOG_DIR_NAME);
            let part_dir = self.root_of(name).join(&topic_name).join(p.to_string());
            std::fs::create_dir_all(&part_dir)?;
        }
        // gh #220: only now delete the staged copies. The rename in
        // step 1 frees each live path instantly, but a recursive
        // delete of a real topic is not instant — 554 ms was observed
        // on NFS — and until these mkdirs land, a broker opening a
        // partition on this path fails with ENOENT. Deleting after
        // them keeps the live path absent for microseconds instead of
        // for the whole unlink walk. Failure is fine: the sweep
        // retries every `.deleting-*`.
        for staged in &staged_paths {
            let _ = std::fs::remove_dir_all(staged);
        }

        // 2. Write per-topic .config.json. The broker watches this
        // file and applies retention/segment/compaction knobs on
        // partition open.
        let cfg = topic_config_file(&topic.spec.config);
        for root in &involved_roots {
            kaas_storage::write_topic_config(&fs, &root.join(&topic_name), &cfg)?;
        }

        // 3. Status update: partition count + TopicID (v4 UUID, minted
        // on first reconcile, NEVER rotated per gh #105).
        let next_count = topic.spec.partitions;

        let ready = Condition {
            type_: READY.into(),
            status: Condition::STATUS_TRUE.into(),
            observed_generation: topic.metadata.generation,
            last_transition_time: String::new(),
            reason: "PartitionsCreated".into(),
            message: format!("{} partition directories created", next_count),
        };

        self.patch_status(&topic, |st| {
            st.partition_count = next_count;
            st.topic_id = next_topic_id.clone();
            st.volume_assignments = assignments.clone();
            st.partitions_outside_spec = outside_spec;
            set_condition(&mut st.conditions, ready.clone());
        })
        .await?;

        // Stamp the directory only once the TopicID is durable in the
        // CR status (gh #219). Stamping a *locally minted* ID before
        // that would be a data-loss trap: if the status patch keeps
        // failing, every reconcile mints a different ID, and each one
        // would see the previous one's stamp as a stale incarnation and
        // reclaim a live topic. Post-patch, a failed patch simply
        // leaves the dir unstamped — which is always adopted, never
        // reclaimed — and the next reconcile stamps it for real.
        //
        // Skipped when the stamp already matches: the steady-state
        // 5-minute requeue shouldn't rewrite a file per topic on the
        // shared volume for no reason.
        if identity != kaas_storage::IdentityVerdict::Match {
            for root in &involved_roots {
                kaas_storage::write_topic_identity(&fs, &root.join(&topic_name), &next_topic_id)?;
            }
        }

        self.observer.bump_success();
        // 5 min default requeue (controller-runtime-style
        // SyncPeriod fallback); watch events are
        // the primary driver, this is the safety net.
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    /// gh #219: reclaim a directory left behind by a **previous
    /// incarnation** of this topic name before the current one uses it.
    ///
    /// Partition dirs are addressed by name, so a delete→recreate under
    /// the same name (Kafka Streams' `application-reset` does this on
    /// every run) inherits the old incarnation's segments, manifest
    /// (high watermark / log start offset / epoch) and
    /// `producer-state.snapshot` — the last of which rejects the new
    /// producer's first batch with `OUT_OF_ORDER_SEQUENCE_NUMBER` or
    /// silently swallows it as a duplicate. `Status.TopicID` is minted
    /// fresh for a recreated CR (gh #105), so a stamp mismatch is an
    /// exact, race-free "this is not my directory" signal.
    ///
    /// Reconcile-driven on purpose: it needs no delete event, so there
    /// is no ordering hazard between a delete watch and the reconciler
    /// re-creating the dir. Re-running it is a no-op (NFS rule 2).
    ///
    /// **Unstamped is never stale.** A directory with no stamp predates
    /// this check, or was created by a broker's `Partition::open`;
    /// adopting it is the only safe reading — deleting on "unknown"
    /// would eat live data on upgrade.
    ///
    /// Returns the stamp verdict (so the caller can skip re-stamping a
    /// directory that already agrees) and the staged path still awaiting
    /// deletion, which the caller removes **after** re-creating the live
    /// partition dirs (gh #220).
    async fn patch_status(
        &self,
        topic: &KafkaTopic,
        mutate: impl FnOnce(&mut kaas_operator_api::KafkaTopicStatus),
    ) -> Result<(), ControllerError> {
        let Some(name) = topic.metadata.name.as_deref() else {
            return Ok(());
        };
        let namespace = topic.metadata.namespace.as_deref().unwrap_or("default");
        let api: Api<KafkaTopic> = Api::namespaced(self.client.clone(), namespace);

        let mut status = topic.status.clone().unwrap_or_default();
        mutate(&mut status);

        // Server-side apply requires apiVersion + kind in the body —
        // without them the API server answers
        // `invalid object type: /, Kind=` (400) and every reconcile
        // hot-loops through the error policy.
        let body = serde_json::json!({
            "apiVersion": "kaas.rs/v1alpha1",
            "kind": "KafkaTopic",
            "status": status,
        });
        api.patch_status(
            name,
            &PatchParams::apply("kaas-operator").force(),
            &Patch::Apply(&body),
        )
        .await?;
        Ok(())
    }
}

/// gh #221 phase 2: the log-dir names a topic's partitions may be
/// placed on. `spec.storage.volumes` set → validated names (the
/// reserved `default` refers to the data volume); unset → the
/// default set: `default` plus every pool member with
/// `defaultEligible: true` (reserved members opt out of receiving
/// unbound topics — Streams auto-creates arrive casually).
pub(crate) fn eligible_log_dirs(
    pool: &[kaas_storage::LogDirInfo],
    spec: Option<&kaas_operator_api::KafkaTopicStorage>,
) -> Result<Vec<String>, String> {
    let requested: Vec<String> = spec.map(|s| s.volumes.clone()).unwrap_or_default();
    let selector = spec
        .and_then(|s| s.volume_selector.as_ref())
        .filter(|m| !m.is_empty());

    if !requested.is_empty() && selector.is_some() {
        return Err(
            "spec.storage.volumes and spec.storage.volumeSelector are mutually exclusive".into(),
        );
    }

    // gh #224: selector → the members whose labels contain every
    // selector pair (nodeSelector semantics). The default data dir
    // carries no labels, so a selector never matches it — selecting
    // by class implies the pool.
    if let Some(sel) = selector {
        let matched: Vec<String> = pool
            .iter()
            .filter(|d| !d.cordoned && sel.iter().all(|(k, v)| d.labels.get(k) == Some(v)))
            .map(|d| d.name.clone())
            .collect();
        if matched.is_empty() {
            return Err(format!(
                "spec.storage.volumeSelector matches no uncordoned pool member ({})",
                sel.iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        return Ok(matched);
    }

    if requested.is_empty() {
        let mut set = vec![kaas_storage::DEFAULT_LOG_DIR_NAME.to_owned()];
        set.extend(
            pool.iter()
                .filter(|d| d.default_eligible && !d.cordoned)
                .map(|d| d.name.clone()),
        );
        return Ok(set);
    }
    for name in &requested {
        let known =
            name == kaas_storage::DEFAULT_LOG_DIR_NAME || pool.iter().any(|d| &d.name == name);
        if !known {
            return Err(format!(
                "spec.storage.volumes references unknown log dir '{name}'"
            ));
        }
    }
    // KIP-1066 cordon semantics: a cordoned member accepts no new
    // placements even when named explicitly — existing partitions
    // stay put (placement is sticky), new ones go to the remaining
    // eligible members. All members cordoned → loud failure.
    let uncordoned: Vec<String> = requested
        .iter()
        .filter(|name| {
            pool.iter()
                .find(|d| &&d.name == name)
                .is_none_or(|d| !d.cordoned)
        })
        .cloned()
        .collect();
    if uncordoned.is_empty() {
        return Err(format!(
            "every volume in spec.storage.volumes is cordoned ({})",
            requested.join(", ")
        ));
    }
    Ok(uncordoned)
}

/// Generate a canonical hyphenated v4-shape UUID. Mirrors
/// `kafkatopic_controller.go:generateTopicUUID` — kube-rs has
/// `uuid::Uuid::new_v4()` which produces the same RFC 4122 layout,
/// but spell it out byte-for-byte so the regex match against
/// `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`
/// is unambiguous.
pub fn generate_topic_uuid() -> String {
    uuid::Uuid::new_v4().hyphenated().to_string()
}

/// Free function passed to `kube::runtime::Controller::run`. The
/// `Ctx` is the reconciler itself behind `Arc`.
pub async fn reconcile_topic(
    topic: Arc<KafkaTopic>,
    ctx: Arc<KafkaTopicReconciler>,
) -> Result<Action, ControllerError> {
    ctx.reconcile(topic).await
}

/// Error policy for `kube::runtime::Controller::run`. Errors
/// requeue after 10 s; matches controller-runtime's default backoff
/// floor.
pub fn error_policy(
    _topic: Arc<KafkaTopic>,
    err: &ControllerError,
    ctx: Arc<KafkaTopicReconciler>,
) -> Action {
    tracing::warn!(error = %err, "KafkaTopic reconcile failed");
    ctx.observer.bump_error();
    Action::requeue(Duration::from_secs(10))
}

/// The `.config.json` the broker reads for a topic's `spec.config`.
/// Destructured without `..` so a new CRD config field fails to
/// compile until it is mapped — a field dropped here is an override
/// the API server stored and the broker never sees (gh #236).
pub fn topic_config_file(cfg: &KafkaTopicConfig) -> TopicConfigFile {
    let KafkaTopicConfig {
        retention_ms,
        retention_bytes,
        segment_bytes,
        segment_ms,
        cleanup_policy,
        min_compaction_lag_ms,
        delete_retention_ms,
        flush_messages,
    } = cfg;
    TopicConfigFile {
        retention_ms: *retention_ms,
        retention_bytes: *retention_bytes,
        segment_bytes: *segment_bytes,
        segment_ms: *segment_ms,
        cleanup_policy: cleanup_policy.clone(),
        min_compaction_lag_ms: *min_compaction_lag_ms,
        delete_retention_ms: *delete_retention_ms,
        flush_messages: *flush_messages,
    }
}

/// Bonus helper for the `bins/kaas-operator` main: derive the
/// CR's filesystem path so other tools (e.g. the sweep) can share
/// the resolution rule.
pub fn topic_dir_for(data_dir: &Path, topic: &KafkaTopic) -> PathBuf {
    data_dir.join(topic.effective_topic_name())
}

/// Outcome of [`reclaim_stale_incarnation`].
#[derive(Debug)]
pub(crate) struct Reclaimed {
    pub(crate) verdict: kaas_storage::IdentityVerdict,
    /// Path the old incarnation was renamed to, still to be deleted.
    /// `None` when nothing was staged. gh #220: the caller deletes it
    /// *after* re-creating the live partition dirs.
    pub(crate) staged: Option<PathBuf>,
}

/// Body of [`KafkaTopicReconciler::reclaim_stale_incarnation`] — free
/// so it is testable without a `kube::Client`. See that method for the
/// contract.
pub(crate) fn reclaim_stale_incarnation(
    data_dir: &Path,
    name: &str,
    topic_dir: &Path,
    topic_id: &str,
    fs: &dyn kaas_storage::fs::Fs,
) -> Result<Reclaimed, ControllerError> {
    use kaas_storage::IdentityVerdict;

    let unstamped = |()| Reclaimed {
        verdict: IdentityVerdict::Unstamped,
        staged: None,
    };
    if topic_id.is_empty() {
        return Ok(unstamped(()));
    }
    let verdict = match kaas_storage::classify_topic_identity(fs, topic_dir, topic_id) {
        Ok(v) => v,
        // An unreadable stamp is "unknown", not "stale".
        Err(err) => {
            tracing::warn!(topic = name, %err, "topic identity unreadable; adopting the dir");
            return Ok(unstamped(()));
        }
    };
    if verdict != IdentityVerdict::Stale {
        return Ok(Reclaimed {
            verdict,
            staged: None,
        });
    }
    tracing::info!(
        topic = name,
        topic_id,
        "topic dir belongs to a previous incarnation; reclaiming before reuse"
    );
    Ok(Reclaimed {
        verdict: IdentityVerdict::Stale,
        staged: stage_topic_dir(data_dir, name)?,
    })
}

/// gh #203: free a topic's directory without racing a broker's
/// `Partition::open`.
///
/// Atomically renames `<data_dir>/<name>` aside to a
/// `.deleting-<name>.<nanos>` sibling — a same-parent rename is the one
/// atomic NFS primitive — so the destructive `remove_dir_all` never runs
/// on the live path a broker may be mid-open on. See
/// `docs/src/architecture/nfs-substrate.md`.
///
/// gh #220: this **only stages**. It returns the staged path and leaves
/// the deletion to the caller, which first re-creates the live partition
/// dirs. The rename frees the live path in one atomic step, but the
/// recursive delete that follows is a long unlink walk (554 ms measured
/// on NFS for a modest topic), and every millisecond of it is a
/// millisecond in which a broker opening a partition on this path gets
/// ENOENT. A staged dir whose deletion can't finish (FDs still held,
/// gh #76) is retried by the resumable sweep.
pub(crate) fn stage_topic_dir(
    data_dir: &Path,
    name: &str,
) -> Result<Option<PathBuf>, ControllerError> {
    let path = data_dir.join(name);
    if !path.exists() {
        return Ok(None);
    }
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let staged = data_dir.join(format!(
        "{}{name}.{nanos}",
        crate::sweep::STAGED_DELETE_PREFIX
    ));
    match std::fs::rename(&path, &staged) {
        Ok(()) => Ok(Some(staged)),
        // Already gone (a concurrent actor moved/removed it) → done.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(ControllerError::Io(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_operator_api::{KafkaTopic, KafkaTopicConfig, KafkaTopicSpec, KafkaTopicStatus};
    use kube::api::ObjectMeta;

    /// gh #203: deleting a topic dir frees the LIVE path immediately
    /// (atomic rename) so a concurrent broker open can't race the
    /// recursive delete. With no open FDs the staged copy is fully
    /// gone afterwards.
    #[test]
    fn stage_frees_live_path_and_moves_data_aside() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let topic = root.join("doomed");
        std::fs::create_dir_all(topic.join("0")).unwrap();
        std::fs::write(topic.join("0").join("manifest.json"), b"{}").unwrap();

        let staged = stage_topic_dir(root, "doomed").unwrap().expect("staged");

        // Live path is free — a broker mkdir on it now creates a fresh
        // dir instead of racing a delete-in-progress.
        assert!(!topic.exists(), "live topic path freed by the rename");
        std::fs::create_dir_all(&topic).unwrap();
        assert!(topic.exists());

        // The old contents moved wholesale to the staged path, which no
        // broker will ever open; deleting it is the caller's job.
        assert!(staged.join("0").join("manifest.json").exists());
        std::fs::remove_dir_all(&staged).unwrap();
    }

    /// Seed `<root>/<name>` with a partition dir carrying leftover
    /// state, stamped with `stamp` (empty = unstamped).
    fn seed_topic_dir(root: &Path, name: &str, stamp: &str) -> PathBuf {
        let dir = root.join(name);
        std::fs::create_dir_all(dir.join("0")).unwrap();
        std::fs::write(
            dir.join("0").join("manifest.json"),
            br#"{"highWatermark":500}"#,
        )
        .unwrap();
        std::fs::write(dir.join("0").join("producer-state.snapshot"), b"{}").unwrap();
        if !stamp.is_empty() {
            kaas_storage::write_topic_identity(&kaas_storage::fs::RealFs::new(), &dir, stamp)
                .unwrap();
        }
        dir
    }

    /// gh #219: a topic deleted and recreated under the same name gets
    /// a fresh `Status.TopicID`, so the directory left by the previous
    /// incarnation must be staged aside — otherwise the new topic
    /// inherits its high watermark and, worse, its idempotence dedupe
    /// window (`OUT_OF_ORDER_SEQUENCE_NUMBER` on the first produce).
    #[test]
    fn stale_incarnation_dir_is_reclaimed() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "reset-me", "old-incarnation");

        let out =
            reclaim_stale_incarnation(root, "reset-me", &dir, "new-incarnation", &fs).unwrap();

        assert_eq!(out.verdict, kaas_storage::IdentityVerdict::Stale);
        assert!(
            !dir.exists(),
            "the previous incarnation's dir must not survive into the new one"
        );
        // gh #220: the delete is deferred to the caller, so the staged
        // copy is still on disk (and still carries the old data).
        let staged = out.staged.expect("stale reclaim stages the old dir");
        assert!(staged.join("0").join("manifest.json").exists());
    }

    /// Steady state: the same incarnation reconciling again must not
    /// touch its own data.
    #[test]
    fn matching_incarnation_dir_is_kept() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "steady", "same-id");

        let out = reclaim_stale_incarnation(root, "steady", &dir, "same-id", &fs).unwrap();

        // `Match` is also what tells the reconciler not to re-stamp the
        // directory on every 5-minute requeue.
        assert_eq!(out.verdict, kaas_storage::IdentityVerdict::Match);
        assert!(out.staged.is_none(), "nothing staged when the stamp agrees");
        assert!(dir.join("0").join("manifest.json").exists(), "data kept");
    }

    /// Upgrade safety: directories written before the stamp existed
    /// (and directories a broker created on `Partition::open`) carry no
    /// identity. "Unknown" must adopt, never delete — anything else
    /// would wipe every live topic on the first reconcile after
    /// upgrade.
    #[test]
    fn unstamped_dir_is_adopted_not_deleted() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "legacy", "");

        reclaim_stale_incarnation(root, "legacy", &dir, "fresh-id", &fs).unwrap();

        assert!(
            dir.join("0").join("manifest.json").exists(),
            "an unstamped dir is adopted, never reclaimed"
        );
    }

    /// A CR whose status hasn't been written yet has no identity to
    /// compare against; that must be a no-op, not a reclaim.
    #[test]
    fn empty_topic_id_never_reclaims() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "no-id-yet", "some-id");

        reclaim_stale_incarnation(root, "no-id-yet", &dir, "", &fs).unwrap();

        assert!(dir.join("0").join("manifest.json").exists());
    }

    #[test]
    fn reclaim_on_a_missing_dir_is_ok() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = tmp.path().join("never-existed");
        reclaim_stale_incarnation(tmp.path(), "never-existed", &dir, "id", &fs).unwrap();
    }

    /// gh #220: the live path must be usable again the instant the
    /// rename returns — the caller re-creates the partition dirs and
    /// only *then* deletes the staged copy. Pinned by making the staged
    /// tree undeletable: if re-creating the live path were sequenced
    /// behind a successful delete, this would leave the topic without
    /// its partition dirs.
    #[cfg(unix)]
    #[test]
    fn live_path_is_usable_before_the_staged_delete_finishes() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let fs = kaas_storage::fs::RealFs::new();
        let dir = seed_topic_dir(root, "reset-me", "old-incarnation");

        let out = reclaim_stale_incarnation(root, "reset-me", &dir, "new-id", &fs).unwrap();
        let staged = out.staged.expect("staged");
        // Un-removable, standing in for a broker still holding an FD
        // inside it (gh #76 silly-rename).
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o555)).unwrap();

        // What the reconciler does next, in order.
        std::fs::create_dir_all(dir.join("0")).unwrap();
        let delete_failed = std::fs::remove_dir_all(&staged).is_err();

        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755)).unwrap();

        assert!(delete_failed, "test needs the staged delete to fail");
        assert!(
            dir.join("0").is_dir(),
            "partition dir exists even though the staged delete could not finish"
        );
    }

    #[test]
    fn stage_missing_dir_is_none() {
        let tmp = tempfile::tempdir().unwrap();
        assert!(stage_topic_dir(tmp.path(), "never-existed")
            .unwrap()
            .is_none());
    }

    #[test]
    fn generate_topic_uuid_matches_v4_pattern() {
        let pat = regex_lite_v4();
        for _ in 0..100 {
            let id = generate_topic_uuid();
            assert!(pat.is_match(&id), "uuid {id} does not match v4 pattern");
        }
    }

    fn regex_lite_v4() -> regex::Regex {
        regex::Regex::new("^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
            .unwrap()
    }

    #[test]
    fn every_spec_config_field_reaches_config_json() {
        // gh #236: an override the API server stored but the operator
        // never writes to `.config.json` is never enforced, and
        // DescribeConfigs reports it as the default.
        let spec = KafkaTopicConfig {
            retention_ms: Some(600_000),
            retention_bytes: Some(1_048_576),
            segment_bytes: Some(16_777_216),
            segment_ms: Some(3_600_000),
            cleanup_policy: "compact".into(),
            min_compaction_lag_ms: Some(5_000),
            delete_retention_ms: Some(7_000),
            flush_messages: Some(1),
        };
        let tmp = tempfile::tempdir().unwrap();
        let fs = kaas_storage::fs::RealFs::new();
        kaas_storage::write_topic_config(&fs, tmp.path(), &topic_config_file(&spec)).unwrap();
        let back = kaas_storage::read_topic_config(&fs, tmp.path())
            .unwrap()
            .expect(".config.json written");
        assert_eq!(back.retention_ms, Some(600_000));
        assert_eq!(back.retention_bytes, Some(1_048_576));
        assert_eq!(back.segment_bytes, Some(16_777_216));
        assert_eq!(back.segment_ms, Some(3_600_000));
        assert_eq!(back.cleanup_policy, "compact");
        assert_eq!(back.min_compaction_lag_ms, Some(5_000));
        assert_eq!(back.delete_retention_ms, Some(7_000));
        assert_eq!(back.flush_messages, Some(1));
    }

    #[test]
    fn topic_dir_for_uses_effective_name() {
        // metadata.name only — falls back to it.
        let t = KafkaTopic {
            metadata: ObjectMeta {
                name: Some("meta-name".into()),
                ..ObjectMeta::default()
            },
            spec: KafkaTopicSpec {
                topic_name: String::new(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
                storage: None,
            },
            status: None,
        };
        assert_eq!(
            topic_dir_for(Path::new("/data"), &t),
            Path::new("/data/meta-name")
        );

        // spec.topic_name overrides metadata.name (gh #86 path).
        let synth = KafkaTopic {
            metadata: ObjectMeta {
                name: Some("hash-of-thing".into()),
                ..ObjectMeta::default()
            },
            spec: KafkaTopicSpec {
                topic_name: "My.Real.Topic".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
                storage: None,
            },
            status: None,
        };
        assert_eq!(
            topic_dir_for(Path::new("/data"), &synth),
            Path::new("/data/My.Real.Topic")
        );
    }

    #[test]
    fn partition_decrease_predicate() {
        // Verify the guard condition: existing > 0 AND spec < existing.
        let cases: &[(i32, i32, bool)] = &[
            (0, 1, false), // never reconciled → first run
            (1, 1, false), // same
            (1, 3, false), // grow
            (3, 1, true),  // shrink
            (5, 0, true),  // shrink to 0 (theoretically prevented by min=1 in the schema)
        ];
        for (existing, spec, expected) in cases {
            let got = *existing > 0 && *spec < *existing;
            assert_eq!(got, *expected, "existing={existing} spec={spec}");
        }
    }

    #[test]
    fn topic_id_preserved_when_already_set() {
        let original = "11111111-2222-4333-8444-555555555555".to_string();
        let topic = KafkaTopic {
            metadata: ObjectMeta::default(),
            spec: KafkaTopicSpec {
                topic_name: "t".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
                storage: None,
            },
            status: Some(KafkaTopicStatus {
                volume_assignments: Default::default(),
                partitions_outside_spec: 0,
                partition_count: 1,
                topic_id: original.clone(),
                conditions: vec![],
            }),
        };
        let preserved = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);
        assert_eq!(preserved, original);
    }

    #[test]
    fn topic_id_minted_when_status_missing_or_blank() {
        let topic = KafkaTopic {
            metadata: ObjectMeta::default(),
            spec: KafkaTopicSpec {
                topic_name: "t".into(),
                partitions: 1,
                config: KafkaTopicConfig::default(),
                storage: None,
            },
            status: None,
        };
        let id = topic
            .status
            .as_ref()
            .filter(|s| !s.topic_id.is_empty())
            .map(|s| s.topic_id.clone())
            .unwrap_or_else(generate_topic_uuid);
        assert!(regex_lite_v4().is_match(&id));
    }

    // --- gh #221 phase 2: eligible-set + placement -----------------

    fn pool() -> Vec<kaas_storage::LogDirInfo> {
        vec![
            kaas_storage::LogDirInfo {
                name: "fast".into(),
                path: "/vols/fast".into(),
                default_eligible: true,
                cordoned: false,
                labels: [("class".to_string(), "standard".to_string())].into(),
            },
            kaas_storage::LogDirInfo {
                name: "premium".into(),
                path: "/vols/premium".into(),
                default_eligible: false, // reserved: explicit binding only
                cordoned: false,
                labels: [("class".to_string(), "premium".to_string())].into(),
            },
        ]
    }

    fn storage_spec(volumes: &[&str]) -> kaas_operator_api::KafkaTopicStorage {
        kaas_operator_api::KafkaTopicStorage {
            volumes: volumes.iter().map(|s| s.to_string()).collect(),
            volume_selector: None,
        }
    }

    #[test]
    fn unset_binding_uses_default_eligible_set() {
        let set = eligible_log_dirs(&pool(), None).unwrap();
        // default + fast; premium is reserved (defaultEligible=false).
        assert_eq!(set, vec!["default".to_string(), "fast".to_string()]);
    }

    #[test]
    fn explicit_binding_may_name_reserved_members() {
        let set = eligible_log_dirs(&pool(), Some(&storage_spec(&["premium"]))).unwrap();
        assert_eq!(set, vec!["premium".to_string()]);
    }

    #[test]
    fn unknown_name_fails_loudly() {
        let err = eligible_log_dirs(&pool(), Some(&storage_spec(&["fast", "typo"]))).unwrap_err();
        assert!(err.contains("'typo'"), "{err}");
    }

    #[test]
    fn round_robin_is_creation_sticky() {
        // First placement: 4 partitions over [default, fast].
        let eligible = eligible_log_dirs(&pool(), None).unwrap();
        let mut assignments = std::collections::BTreeMap::new();
        for p in 0..4 {
            assignments.entry(p.to_string()).or_insert_with(|| {
                eligible[usize::try_from(p).unwrap_or(0) % eligible.len()].clone()
            });
        }
        assert_eq!(assignments["0"], "default");
        assert_eq!(assignments["1"], "fast");
        assert_eq!(assignments["2"], "default");
        assert_eq!(assignments["3"], "fast");

        // Spec edit pins the topic to premium; existing entries must
        // NOT move — only new partitions (expansion) follow the new
        // set — and the old ones count as drift.
        let eligible = eligible_log_dirs(&pool(), Some(&storage_spec(&["premium"]))).unwrap();
        for p in 0..6 {
            assignments.entry(p.to_string()).or_insert_with(|| {
                eligible[usize::try_from(p).unwrap_or(0) % eligible.len()].clone()
            });
        }
        assert_eq!(assignments["0"], "default", "sticky");
        assert_eq!(assignments["4"], "premium", "expansion follows new set");
        assert_eq!(assignments["5"], "premium");
        let outside = (0..6)
            .filter(|p| {
                assignments
                    .get(&p.to_string())
                    .is_some_and(|n| !eligible.contains(n))
            })
            .count();
        assert_eq!(outside, 4, "the four original placements are drift");
    }

    #[test]
    fn cordoned_member_excluded_from_default_set_and_explicit_sets() {
        let mut p = pool();
        p[0].cordoned = true; // fast

        // Default set: cordoned member drops out.
        let set = eligible_log_dirs(&p, None).unwrap();
        assert_eq!(set, vec!["default".to_string()]);

        // Explicit set: cordoned member is skipped, remainder used.
        let set = eligible_log_dirs(&p, Some(&storage_spec(&["fast", "premium"]))).unwrap();
        assert_eq!(set, vec!["premium".to_string()]);

        // Everything cordoned → loud failure.
        let err = eligible_log_dirs(&p, Some(&storage_spec(&["fast"]))).unwrap_err();
        assert!(err.contains("cordoned"), "{err}");
    }

    // --- gh #224: volumeSelector ------------------------------------

    fn selector_spec(pairs: &[(&str, &str)]) -> kaas_operator_api::KafkaTopicStorage {
        kaas_operator_api::KafkaTopicStorage {
            volumes: Vec::new(),
            volume_selector: Some(
                pairs
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
        }
    }

    #[test]
    fn selector_resolves_to_matching_members() {
        let set =
            eligible_log_dirs(&pool(), Some(&selector_spec(&[("class", "premium")]))).unwrap();
        assert_eq!(set, vec!["premium".to_string()]);
    }

    #[test]
    fn selector_matching_nothing_fails_loudly() {
        let err =
            eligible_log_dirs(&pool(), Some(&selector_spec(&[("class", "gold")]))).unwrap_err();
        assert!(err.contains("matches no uncordoned pool member"), "{err}");
    }

    #[test]
    fn selector_skips_cordoned_members() {
        let mut p = pool();
        p[1].cordoned = true; // premium
        let err = eligible_log_dirs(&p, Some(&selector_spec(&[("class", "premium")]))).unwrap_err();
        assert!(err.contains("cordoned"), "{err}");
    }

    #[test]
    fn selector_and_volumes_are_mutually_exclusive() {
        let mut spec = selector_spec(&[("class", "premium")]);
        spec.volumes = vec!["fast".into()];
        let err = eligible_log_dirs(&pool(), Some(&spec)).unwrap_err();
        assert!(err.contains("mutually exclusive"), "{err}");
    }
}
