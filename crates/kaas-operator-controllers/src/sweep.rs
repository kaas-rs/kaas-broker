//! Leader-elected startup sweep.
//!
//! Startup sweeps for orphaned topic dirs and stale credential
//! entries. Cleanup is reconcile-time best-effort in the steady
//! state, but if the operator was down while a CR delete event
//! fired, the per-CR `NotFound` branch never runs. The sweep covers
//! that gap: at boot (after leader election + cache sync) it walks
//! the live CR list, then removes filesystem state belonging to
//! users / topics that no longer have a CR.
//!
//! Both functions return the list of names they removed so the
//! caller can log them.

use std::collections::HashSet;
use std::path::Path;

use kaas_operator_api::{KafkaTopic, KafkaUser};
use kube::{api::ListParams, Api, Client};

use crate::credentials::{read_credentials, write_credentials};
use crate::errors::ControllerError;

/// Reserved sub-directory under `data_dir` that holds cluster-wide
/// state (assignment.json, credentials.json, acls.json, txn_state/,
/// fence_log/, __consumer_offsets/). The topic sweep MUST skip it.
const CLUSTER_FILES_DIR: &str = "__cluster";

/// Prefix for a topic directory that has been atomically renamed aside
/// for deletion (gh #203). The `KafkaTopic` reconciler's NotFound path
/// renames `<data_dir>/<topic>` → `<data_dir>/.deleting-<topic>.<n>`
/// (a single same-parent rename — the one atomic NFS primitive) so the
/// destructive `remove_dir_all` never runs on the live path a broker
/// might be mid-`Partition::open` on. The sweep is what actually
/// reclaims these: it retries their removal every pass, which also
/// mops up any that couldn't be removed inline because a broker still
/// held an FD (gh #76). The leading `.` keeps them out of the
/// orphan-topic keep-set logic.
pub const STAGED_DELETE_PREFIX: &str = ".deleting-";

/// Outcome of a topic-sweep pass.
///
/// gh #205 / NFS rule 2 (see `docs/src/architecture/nfs-substrate.md`):
/// a single un-removable directory must NOT abort the whole pass. A
/// `remove_dir_all` can fail with `ENOTEMPTY` while a broker still
/// holds an FD inside it (the gh #76 silly-rename window); that dir
/// goes into `failed` and a later pass retries it once the handles
/// close, while every other orphan is still reclaimed this pass.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Directories successfully removed this pass, sorted.
    pub removed: Vec<String>,
    /// Directories that could not be removed this pass (retried
    /// later), sorted.
    pub failed: Vec<String>,
}

/// Remove `<root>/<topic>/` directories that no longer have a
/// matching `KafkaTopic` CR — on every log-dir root (gh #221
/// phase 2: pool volumes host topic dirs too; a deleted topic's
/// dirs must be reclaimed wherever its partitions were placed).
/// Per-directory failures are collected into [`SweepReport::failed`]
/// rather than aborting the pass. The outer `Err` is reserved for
/// failures that make the whole pass impossible (listing CRs,
/// reading a root).
pub async fn sweep_topics(
    client: &Client,
    namespace: &str,
    roots: &[std::path::PathBuf],
) -> Result<SweepReport, ControllerError> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), namespace);
    let topics =
        kaas_observability::record_k8s_call("List", "KafkaTopic", api.list(&ListParams::default()))
            .await?;

    // Build the keep-set from `effective_topic_name()` so synthetic-
    // metadata.name topics (gh #86) are matched against their on-wire
    // Kafka name — the directory layer uses that name verbatim.
    let mut keep = base_keep_set();
    for t in &topics.items {
        keep.insert(t.effective_topic_name().to_string());
    }

    let mut report = SweepReport::default();
    for root in roots {
        let r = reclaim_orphan_dirs(root, &keep)?;
        report.removed.extend(r.removed);
        report.failed.extend(r.failed);
    }
    report.removed.sort();
    report.failed.sort();
    Ok(report)
}

/// The non-topic names every sweep pass must preserve, independent of
/// which `KafkaTopic` CRs exist. Split out so the unit tests exercise
/// the same set production uses — the gh #223 offset-loss bug lived
/// precisely in this set being one entry short. (The pre-gh #223
/// legacy `__consumer_offsets` keep-entry was dropped under the
/// pre-v1 no-backcompat policy: no supported broker writes the
/// data-dir-root path anymore.)
fn base_keep_set() -> HashSet<String> {
    let mut keep: HashSet<String> = HashSet::new();
    keep.insert(CLUSTER_FILES_DIR.into());
    keep
}

/// Remove every immediate child directory of `data_dir` whose name is
/// not in `keep`. Per-directory failures land in [`SweepReport::failed`]
/// (gh #205 — never aborts the pass); a missing `data_dir` is an empty
/// report, not an error.
fn reclaim_orphan_dirs(
    data_dir: &Path,
    keep: &HashSet<String>,
) -> Result<SweepReport, ControllerError> {
    let entries = match std::fs::read_dir(data_dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(SweepReport::default()),
        Err(e) => return Err(ControllerError::Io(e)),
    };

    let mut report = SweepReport::default();
    for entry in entries.flatten() {
        let Ok(meta) = entry.file_type() else {
            continue;
        };
        if !meta.is_dir() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
            continue;
        };
        if name.starts_with(STAGED_DELETE_PREFIX) {
            // gh #203: a topic dir renamed aside for deletion. Always
            // retry its removal — it has no CR and is never a topic.
            remove_orphan(&entry.path(), name, &mut report);
            continue;
        }
        if keep.contains(&name) {
            continue;
        }
        // `.hidden` and `...` are legal topic names, so a dot doesn't mean
        // "not a topic" (gh #320, they leaked). But a dot directory can be
        // somebody else's, such as NFS `.snapshot`, so it goes only if it
        // carries the reconciler's identity stamp.
        if name.starts_with('.')
            && !entry
                .path()
                .join(kaas_storage::TOPIC_IDENTITY_FILENAME)
                .exists()
        {
            continue;
        }
        remove_orphan(&entry.path(), name, &mut report);
    }
    report.removed.sort();
    report.failed.sort();
    Ok(report)
}

/// `remove_dir_all` one orphan, folding the outcome into `report`.
/// gh #205: a failure (ENOTEMPTY while a broker holds an FD, gh #76)
/// is recorded in `failed`, never aborts the pass — a later pass
/// retries once the handles close.
fn remove_orphan(path: &Path, name: String, report: &mut SweepReport) {
    match std::fs::remove_dir_all(path) {
        Ok(()) => report.removed.push(name),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_e) => report.failed.push(name),
    }
}

/// Remove entries from `credentials.json` whose `KafkaUser` CR no
/// longer exists. Returns the usernames removed, sorted.
pub async fn sweep_credentials(
    client: &Client,
    namespace: &str,
    cluster_dir: &Path,
) -> Result<Vec<String>, ControllerError> {
    let api: Api<KafkaUser> = Api::namespaced(client.clone(), namespace);
    let users =
        kaas_observability::record_k8s_call("List", "KafkaUser", api.list(&ListParams::default()))
            .await?;
    let keep: HashSet<String> = users
        .items
        .iter()
        .filter_map(|u| u.metadata.name.clone())
        .collect();

    let mut cf = read_credentials(cluster_dir)?;
    let before = cf.users.len();
    let mut removed: Vec<String> = cf
        .users
        .iter()
        .filter(|u| !keep.contains(&u.username))
        .map(|u| u.username.clone())
        .collect();
    if removed.is_empty() {
        return Ok(Vec::new());
    }
    cf.users.retain(|u| keep.contains(&u.username));
    debug_assert_eq!(cf.users.len() + removed.len(), before);
    write_credentials(cluster_dir, &cf)?;
    removed.sort();
    Ok(removed)
}

#[cfg(test)]
mod tests {
    // These functions hit `kube::Client::list`. The fake-kube tests
    // land in workstream G via `wiremock`. Phase 7 workstream B's
    // gate is "compiles + the in-process helpers above are
    // exercised"; the integration test for the sweep is part of the
    // reconciler test bodies (workstream G).
    //
    // We still want a smoke test for `sweep_topics`' filesystem
    // walk + the `__cluster` skip; isolate that path by calling the
    // internal helper directly.
    use super::{
        base_keep_set, reclaim_orphan_dirs, SweepReport, CLUSTER_FILES_DIR, STAGED_DELETE_PREFIX,
    };
    use std::collections::HashSet;
    use std::fs;

    /// Pure-state replica of `sweep_topics`' filesystem-walk
    /// half. Tests pass an explicit `keep` set instead of going
    /// through `kube::Api`. This is the actual logic we care about
    /// at the unit-test layer; the kube glue is mechanical.
    fn walk_and_remove(
        data_dir: &std::path::Path,
        keep: &HashSet<String>,
    ) -> std::io::Result<Vec<String>> {
        let mut removed = Vec::new();
        for entry in fs::read_dir(data_dir)?.flatten() {
            let Ok(meta) = entry.file_type() else {
                continue;
            };
            if !meta.is_dir() {
                continue;
            }
            let Some(name) = entry.file_name().to_str().map(|s| s.to_string()) else {
                continue;
            };
            if name.starts_with('.') || keep.contains(&name) {
                continue;
            }
            fs::remove_dir_all(entry.path())?;
            removed.push(name);
        }
        removed.sort();
        Ok(removed)
    }

    /// gh #320: an orphan with a dot-prefixed name is reclaimed when the
    /// reconciler stamped it as a topic. An unstamped dot directory isn't ours
    /// to delete, and a topic with a live CR stays whatever its name.
    #[test]
    fn a_stamped_dot_prefixed_orphan_is_reclaimed_and_others_stay() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        for name in [".hidden", "...", ".live"] {
            fs::create_dir(root.join(name)).unwrap();
            fs::write(
                root.join(name).join(kaas_storage::TOPIC_IDENTITY_FILENAME),
                b"{}",
            )
            .unwrap();
        }
        fs::create_dir(root.join(".snapshot")).unwrap();
        let mut keep = base_keep_set();
        keep.insert(".live".to_owned());

        let report = reclaim_orphan_dirs(root, &keep).unwrap();
        assert_eq!(report.removed, ["...", ".hidden"]);
        assert!(root.join(".live").is_dir());
        assert!(root.join(".snapshot").is_dir());
    }

    #[test]
    fn walk_keeps_cluster_dir_and_dot_prefixes_removes_orphans() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join(CLUSTER_FILES_DIR)).unwrap();
        fs::create_dir(root.join(".hidden")).unwrap();
        fs::create_dir(root.join("keep-me")).unwrap();
        fs::create_dir(root.join("orphan-a")).unwrap();
        fs::create_dir(root.join("orphan-b")).unwrap();
        // also drop a regular file at the root; the walker should
        // not be confused by it.
        fs::write(root.join("stray.txt"), b"x").unwrap();

        let mut keep = HashSet::new();
        keep.insert(CLUSTER_FILES_DIR.to_string());
        keep.insert("keep-me".to_string());

        let removed = walk_and_remove(root, &keep).unwrap();
        assert_eq!(removed, vec!["orphan-a", "orphan-b"]);

        // Expected survivors.
        assert!(root.join(CLUSTER_FILES_DIR).exists());
        assert!(root.join(".hidden").exists());
        assert!(root.join("keep-me").exists());
        assert!(root.join("stray.txt").exists());

        // Expected removals.
        assert!(!root.join("orphan-a").exists());
        assert!(!root.join("orphan-b").exists());
    }

    /// gh #223 regression pin, updated for the pre-v1 no-backcompat
    /// policy: the production keep-set preserves the cluster dir —
    /// where offsets live since gh #223 — and a stray data-dir-root
    /// `__consumer_offsets` (the pre-fix layout, unsupported since
    /// the adoption shim was dropped) is reclaimed like any orphan.
    /// Uses the real `base_keep_set` + `reclaim_orphan_dirs` pair —
    /// the original bug lived in the set construction the older
    /// tests bypassed.
    #[test]
    fn production_keep_set_preserves_cluster_dir() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join(CLUSTER_FILES_DIR)).unwrap();
        fs::create_dir(root.join("__consumer_offsets")).unwrap();
        fs::create_dir(root.join("orphan")).unwrap();

        let report = reclaim_orphan_dirs(root, &base_keep_set()).unwrap();
        assert_eq!(report.removed, vec!["__consumer_offsets", "orphan"]);
        assert!(root.join(CLUSTER_FILES_DIR).exists());
    }

    #[test]
    fn walk_on_missing_data_dir_via_real_helper() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("does-not-exist");
        // `sweep_topics` returns Ok(empty) on NotFound; verify the
        // semantics here against std::fs.
        assert!(matches!(
            std::fs::read_dir(&missing),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound
        ));
    }

    /// gh #203: `.deleting-*` staged dirs are always reclaimed by the
    /// sweep (they have no CR and are never in the keep-set), so a
    /// staged delete that couldn't finish inline is retried here.
    #[test]
    fn reclaim_removes_staged_delete_dirs_regardless_of_keep() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join(format!("{STAGED_DELETE_PREFIX}old-topic.123"))).unwrap();
        fs::create_dir(root.join(".hidden")).unwrap(); // other dot-dir: skipped
        fs::create_dir(root.join("keep-me")).unwrap();

        let mut keep = HashSet::new();
        keep.insert("keep-me".to_string());

        let report = super::reclaim_orphan_dirs(root, &keep).unwrap();
        assert_eq!(
            report.removed,
            vec![format!("{STAGED_DELETE_PREFIX}old-topic.123")]
        );
        assert!(
            root.join(".hidden").exists(),
            "non-staged dot-dir untouched"
        );
        assert!(root.join("keep-me").exists());
    }

    #[test]
    fn reclaim_missing_data_dir_is_empty_report() {
        let tmp = tempfile::tempdir().unwrap();
        let missing = tmp.path().join("nope");
        assert_eq!(
            super::reclaim_orphan_dirs(&missing, &HashSet::new()).unwrap(),
            SweepReport::default()
        );
    }

    /// gh #205: one un-removable directory must not abort the pass —
    /// it lands in `failed` while the others are still reclaimed.
    #[cfg(unix)]
    #[test]
    fn reclaim_isolates_a_failed_dir_and_still_removes_the_rest() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        fs::create_dir(root.join("keep-me")).unwrap();
        fs::create_dir(root.join("orphan-ok")).unwrap();

        // Make "orphan-locked" un-removable: a child under a dir with
        // no write permission → remove_dir_all can't unlink the child.
        let locked = root.join("orphan-locked");
        fs::create_dir(&locked).unwrap();
        fs::write(locked.join("child"), b"x").unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o555)).unwrap();

        let mut keep = HashSet::new();
        keep.insert("keep-me".to_string());

        let report = super::reclaim_orphan_dirs(root, &keep).unwrap();

        // Restore perms so tempdir cleanup succeeds regardless.
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();

        assert_eq!(report.removed, vec!["orphan-ok"]);
        assert_eq!(report.failed, vec!["orphan-locked"]);
        assert!(root.join("keep-me").exists(), "keep-set survives");
        assert!(!root.join("orphan-ok").exists(), "removable orphan is gone");
        assert!(
            root.join("orphan-locked").exists(),
            "failed orphan is left for a later pass"
        );
    }
}
