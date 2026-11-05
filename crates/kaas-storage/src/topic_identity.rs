//! Per-topic identity stamp: `/data/<topic>/.topic-id.json`.
//!
//! A partition directory is addressed by **name**
//! (`/data/<topic>/<partition>/`), so a topic that is deleted and
//! recreated under the same name silently inherits the previous
//! incarnation's log segments, manifest (high watermark, log start
//! offset, epoch) and `producer-state.snapshot` (the idempotence dedupe
//! window). Apache Kafka doesn't have that problem: `topic ID`s
//! (KIP-516) make a recreated topic a *different* topic, and its log
//! dir is renamed aside at delete time.
//!
//! kaas mints the same per-topic UUID on the `KafkaTopic` CR
//! (`Status.TopicID`, never rotated — a recreated CR gets a fresh one),
//! so the operator writes it next to the topic's `.config.json` and
//! compares on every reconcile. A mismatch means the directory belongs
//! to a previous incarnation and must be reclaimed before the new one
//! uses it.
//!
//! This is deliberately **reconcile-driven, not event-driven**: it
//! needs no delete event, no ordering between watch streams, and it is
//! idempotent — NFS substrate rule 2 (see
//! `docs/src/architecture/nfs-substrate.md`).

use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic_write::atomic_write_json;
use crate::fs::Fs;

/// Filename written by the operator under `/data/<topic>/`. Dot-
/// prefixed like `.config.json` so directory walks that look for
/// partition dirs skip it.
pub const TOPIC_IDENTITY_FILENAME: &str = ".topic-id.json";

/// The identity stamp. `topic_id` is the `KafkaTopic`'s
/// `Status.TopicID` — a v4 UUID in canonical hyphenated form.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TopicIdentity {
    pub topic_id: String,
}

/// Read the identity stamp. `Ok(None)` when the file is absent — a
/// directory created before this stamp existed, or one a broker
/// re-created on partition open. Callers must treat that as "unknown
/// identity, adopt it", never as "stale".
pub fn read_topic_identity(fs: &dyn Fs, topic_dir: &Path) -> io::Result<Option<TopicIdentity>> {
    let path = topic_dir.join(TOPIC_IDENTITY_FILENAME);
    match fs.open_read(&path) {
        Ok(mut f) => {
            let mut buf = Vec::new();
            io::Read::read_to_end(&mut f, &mut buf)?;
            let id: TopicIdentity = serde_json::from_slice(&buf).map_err(io::Error::other)?;
            if id.topic_id.is_empty() {
                return Ok(None);
            }
            Ok(Some(id))
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Atomically stamp `topic_dir` with `topic_id`.
pub fn write_topic_identity(fs: &dyn Fs, topic_dir: &Path, topic_id: &str) -> io::Result<()> {
    atomic_write_json(
        fs,
        topic_dir,
        TOPIC_IDENTITY_FILENAME,
        &TopicIdentity {
            topic_id: topic_id.to_owned(),
        },
    )
}

/// What the directory's stamp says about the incarnation being
/// reconciled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityVerdict {
    /// No stamp on disk: either a fresh directory or one predating the
    /// stamp. Adopt — never destructive on an unknown.
    Unstamped,
    /// Stamp matches the incarnation being reconciled. Steady state.
    Match,
    /// Stamp belongs to a previous incarnation of this topic name. The
    /// directory must be reclaimed before it is reused.
    Stale,
}

/// Classify `topic_dir` against `topic_id`. A read error is reported as
/// [`IdentityVerdict::Unstamped`] by the caller's choice — this
/// function propagates it so the caller can decide.
pub fn classify(fs: &dyn Fs, topic_dir: &Path, topic_id: &str) -> io::Result<IdentityVerdict> {
    match read_topic_identity(fs, topic_dir)? {
        None => Ok(IdentityVerdict::Unstamped),
        Some(id) if id.topic_id == topic_id => Ok(IdentityVerdict::Match),
        Some(_) => Ok(IdentityVerdict::Stale),
    }
}

/// Apache's `Topic.validate`: non-empty, not `.` or `..`, at most 249
/// characters, and only `[a-zA-Z0-9._-]`. Plus two names Apache allows but
/// kaas can't, because a topic's directory sits in the data dir beside them:
/// `__cluster` (the default cluster-state dir; a topic there would be reclaimed
/// along with `acls.json` and the rest) and `.deleting-*` (the reclaim's staging
/// prefix, which the sweep removes).
///
/// A topic name becomes a directory, so this is what keeps a name inside the
/// data dir (gh #314). A prefix ACL matches the raw string, so `kperf-/../x`
/// passes a grant on `kperf-`. CreateTopics and Metadata auto-create answer
/// `INVALID_TOPIC_EXCEPTION` (17) with it; the operator and the broker's topic
/// watch also check, because a CR can be written directly and the CRD limits
/// only the length.
#[must_use]
pub fn is_valid_topic_name(name: &str) -> bool {
    const MAX_NAME_LENGTH: usize = 249;
    !name.is_empty()
        && name != "."
        && name != ".."
        && name != "__cluster"
        && !name.starts_with(".deleting-")
        && name.len() <= MAX_NAME_LENGTH
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fs::RealFs;

    #[test]
    fn missing_stamp_is_unstamped() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        assert!(read_topic_identity(&fs, tmp.path()).unwrap().is_none());
        assert_eq!(
            classify(&fs, tmp.path(), "abc").unwrap(),
            IdentityVerdict::Unstamped
        );
    }

    #[test]
    fn roundtrip_and_match() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_topic_identity(&fs, tmp.path(), "11111111-2222-4333-8444-555555555555").unwrap();
        assert_eq!(
            read_topic_identity(&fs, tmp.path()).unwrap().unwrap(),
            TopicIdentity {
                topic_id: "11111111-2222-4333-8444-555555555555".into()
            }
        );
        assert_eq!(
            classify(&fs, tmp.path(), "11111111-2222-4333-8444-555555555555").unwrap(),
            IdentityVerdict::Match
        );
    }

    /// The delete→recreate case: same name, different UUID.
    #[test]
    fn different_uuid_is_stale() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_topic_identity(&fs, tmp.path(), "old-id").unwrap();
        assert_eq!(
            classify(&fs, tmp.path(), "new-id").unwrap(),
            IdentityVerdict::Stale
        );
    }

    /// An empty stamp reads as "unknown", not as a mismatch — a topic
    /// whose CR has no `Status.TopicID` yet must never be reclaimed.
    #[test]
    fn empty_stamp_is_unstamped() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_topic_identity(&fs, tmp.path(), "").unwrap();
        assert_eq!(
            classify(&fs, tmp.path(), "some-id").unwrap(),
            IdentityVerdict::Unstamped
        );
    }

    #[test]
    fn stamp_is_written_atomically_without_leaving_a_tmp() {
        let tmp = tempfile::tempdir().unwrap();
        let fs = RealFs::new();
        write_topic_identity(&fs, tmp.path(), "id").unwrap();
        let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(leftovers.is_empty(), "tmp file leaked: {leftovers:?}");
    }

    /// gh #314: Apache's `Topic.validate`, including every traversal shape.
    #[test]
    fn topic_names_follow_apaches_rule() {
        for ok in [
            "events",
            "a.b_c-1",
            "__consumer_offsets",
            "...",
            &"a".repeat(249),
        ] {
            assert!(is_valid_topic_name(ok), "{ok}");
        }
        for bad in [
            "",
            ".",
            "..",
            "a/b",
            "../x",
            "../../x",
            "kperf-/../victim",
            "a b",
            "a\\b",
            "a%2Fb",
            "caf\u{e9}",
            &"a".repeat(250),
            "__cluster",
            ".deleting-foo.1",
        ] {
            assert!(!is_valid_topic_name(bad), "{bad:?}");
        }
    }
}
