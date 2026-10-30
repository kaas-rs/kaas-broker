//! Workspace call-site audit.
//!
//! Scans every `crates/*/src/**` and `bins/*/src/**` file for
//! `metrics::global().<field>.` and `record_k8s_call(` invocations
//! and asserts every metric we've committed to wiring has at least
//! one hit.
//!
//! Serves two purposes:
//!
//! 1. **Regression guard** — a rename or accidental removal of a
//!    call site fails the test rather than silently dropping the
//!    metric from Grafana.
//! 2. **Coverage checklist** — the `EXPECTED_ZERO` list documents
//!    which metrics are intentionally unwired (tripwires, forward-
//!    compat, or subsystem-not-yet-ported), so a reviewer can spot
//!    when the assumption breaks.
//!
//! If you wire a new call site for a field currently in
//! `EXPECTED_ZERO`, move it to `EXPECTED_WIRED`. If you add a new
//! `Metrics` field, add it to whichever list matches its status.
//!
//! The test walks the workspace, not the Metrics struct — so a new
//! field added without an audit-list update will still ship (the
//! test doesn't gate on struct-vs-list drift); this is a deliberate
//! design choice so field additions don't force a two-file diff.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

/// Metrics that MUST have at least one call site somewhere in the
/// workspace. Kept in the order they appear on the `Metrics` struct
/// (see `crates/kaas-observability/src/metrics.rs`) for eyeball diff.
const EXPECTED_WIRED: &[&str] = &[
    "request_latency",
    "write_latency",
    "read_latency",
    "fsync_latency",
    "controller_failovers",
    "controller_failover_duration",
    "controller_lease_conflicts",
    "assignment_changes",
    "assignment_file_writes",
    "assignment_file_write_latency",
    "assignment_pushes",
    "assignment_polls",
    "stale_assignments_rejected",
    "heartbeat_misses",
    "group_rebalances",
    "auth_success",
    "auth_failure",
    "acl_deny",
    "quota_throttle",
    "tls_handshakes",
    "connections",
    "connections_open",
    "cleaner_runs",
    "cleaner_duration",
    "cleaner_segments_deleted",
    "cleaner_bytes_reclaimed",
    "produce_errors",
    "fetch_errors",
    "operator_reconciles",
    "operator_reconcile_duration",
    // k8s_api_* land via `record_k8s_call`; the string "k8s_api_calls"
    // never appears as a `metrics::global()` postfix.
];

/// Metrics that are intentionally unwired today. Categories:
///
/// * **Tripwires** (should never fire): `codec_record_decode`,
///   `codec_batch_reencode`. Alertable-on-nonzero. The
///   `kaas_codec::tripwires::install_tripwire_hooks` seam forwards any
///   future bump to the OTel counters, but no production emitter
///   exists today.
/// * **Forward-compat / not-yet-ported subsystems**: compactor
///   metrics (`compaction_*`) — no compactor exists yet;
///   `cert_reloads` — TLS certs are bound at startup; hot reload is
///   a Phase 8/9 gap; `heartbeat_rtt` — requires a controller-side
///   echo-timestamp response that Phase 5 didn't wire;
///   `self_fence_events` — self-fence is invoked from
///   `bins/kaas/main::cluster` and hasn't been threaded through
///   yet.
/// * **Self-observed via `ObservedExporter`**: `otlp_push_success`,
///   `otlp_push_failure`, `otlp_push_duration` — see
///   `otlp_push_observer.rs`.
const EXPECTED_ZERO: &[&str] = &[
    "codec_record_decode",
    "codec_batch_reencode",
    "self_fence_events",
    "heartbeat_rtt",
    "cert_reloads",
    "compaction_runs",
    "compaction_duration",
    "compaction_records_kept",
    "compaction_records_dropped",
    "compaction_bytes_in",
    "compaction_bytes_out",
    "otlp_push_success",
    "otlp_push_failure",
    "otlp_push_duration",
];

fn workspace_root() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // kaas-observability -> crates -> workspace root.
    p.pop();
    p.pop();
    p
}

fn walk_rs_files(root: &Path, out: &mut Vec<PathBuf>) {
    let entries = match fs::read_dir(root) {
        Ok(e) => e,
        Err(_) => return,
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let ty = match entry.file_type() {
            Ok(t) => t,
            Err(_) => continue,
        };
        if ty.is_dir() {
            // Skip the kaas-observability tree so the audit doesn't
            // catch the field-declaration lines in `metrics.rs`.
            if path.ends_with("kaas-observability") || path.ends_with("target") {
                continue;
            }
            walk_rs_files(&path, out);
        } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn scan_call_sites() -> HashSet<String> {
    let root = workspace_root();
    let mut files = Vec::new();
    for sub in ["crates", "bins"] {
        walk_rs_files(&root.join(sub), &mut files);
    }

    // Candidate emitter suffixes on OTel instruments used across
    // the workspace. `record_produce` / `record_fetch` are the
    // TopicTrafficMeter methods; the rest come from
    // `Counter<u64>` / `Histogram<f64>` / `UpDownCounter<i64>`.
    const SUFFIXES: &[&str] = &[".add(", ".record(", ".record_produce(", ".record_fetch("];

    let mut hits = HashSet::new();
    for f in files {
        let src = match fs::read_to_string(&f) {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Sliding-window scan: for every occurrence of `.<ident><ws><suffix>`
        // where suffix is one of the emitter methods, record `ident`.
        // Rustfmt often breaks the chain across lines
        // (`.field\n    .record(`), so skip whitespace between the
        // ident and the suffix.
        for suffix in SUFFIXES {
            for (i, _) in src.match_indices(suffix) {
                let head = &src[..i];
                let head_trimmed_end = head.trim_end();
                let ident_end = head_trimmed_end.len();
                let ident_start = head_trimmed_end
                    .rfind(|c: char| !c.is_ascii_alphanumeric() && c != '_')
                    .map(|p| p + 1)
                    .unwrap_or(0);
                if ident_start >= ident_end {
                    continue;
                }
                let ident = &head_trimmed_end[ident_start..ident_end];
                if ident.len() < 3 {
                    continue;
                }
                hits.insert(ident.to_string());
            }
        }
    }
    hits
}

#[test]
fn every_expected_wired_field_has_call_site() {
    let hits = scan_call_sites();
    let mut missing = Vec::new();
    for field in EXPECTED_WIRED {
        if !hits.contains(*field) {
            missing.push(*field);
        }
    }
    assert!(
        missing.is_empty(),
        "expected-wired Metrics fields without any workspace call site: {missing:?}. \
         If the wire-up was intentionally removed, move the entry from EXPECTED_WIRED \
         to EXPECTED_ZERO."
    );
}

#[test]
fn expected_zero_and_wired_are_disjoint() {
    let wired: HashSet<&str> = EXPECTED_WIRED.iter().copied().collect();
    let zero: HashSet<&str> = EXPECTED_ZERO.iter().copied().collect();
    let overlap: Vec<_> = wired.intersection(&zero).collect();
    assert!(
        overlap.is_empty(),
        "field in both EXPECTED_WIRED and EXPECTED_ZERO: {overlap:?}"
    );
}
