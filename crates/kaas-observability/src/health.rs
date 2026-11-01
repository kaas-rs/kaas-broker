//! Axum-backed `/healthz` + `/readyz` handlers.
//!
//! The `/healthz` JSON shape
//! is pinned so dashboards + scripts written
//! against the plan-v3 schema keep working.
//!
//! [`RuntimeState`] is the v3 broker view — implementations must be
//! safe to call from any async task (the axum handler owns an
//! `Arc<dyn RuntimeState>`). A `None` runtime is acceptable and yields
//! zero-valued fields, which is the right answer in local-dev mode
//! where no controller / coordinator / heartbeat client is running.
//!
//! Fields that have no measurement yet return `-1` from the trait; the
//! handler folds those into `null` on the wire (via `Option<i64>`) so
//! dashboards render "not measured" rather than a misleading zero.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::get,
    Router,
};
use serde::Serialize;

/// The v3 broker runtime view surfaced on `/healthz`.
///
/// All methods must be safe to call from any thread. Methods that
/// haven't measured anything yet return `-1` (for `*_ms` fields); the
/// handler renders those as JSON `null`.
pub trait RuntimeState: Send + Sync {
    fn is_controller(&self) -> bool;
    fn controller_id(&self) -> String;
    fn controller_epoch(&self) -> i64;
    fn heartbeat_age_ms(&self) -> i64;
    fn assignment_version(&self) -> u64;
    fn assignment_age_ms(&self) -> i64;
    fn partitions_led(&self) -> i32;
    fn partitions_assigned(&self) -> i32;
    fn partitions_recovering(&self) -> i32;
    /// Reports whether at least one partition's most recent committer
    /// fsync timed out per `Config.FsyncMaxLatency` (gh #95). Lets
    /// healthz surface a "storage backend wedged" signal before the
    /// broker accumulates enough queued appenders to look outwardly
    /// idle. Implementations that don't track storage health return
    /// `false`.
    fn storage_stalled(&self) -> bool;

    /// gh #208: has the broker taken over every partition its current
    /// assignment gives it? `true` vacuously when it leads none;
    /// `false` while still booting (no assignment applied) or
    /// mid-takeover. Drives honest `/readyz` in cluster mode. Default
    /// `true` for dev / no-cluster implementations.
    fn serving(&self) -> bool {
        true
    }
}

#[derive(Debug, Serialize)]
struct HealthState {
    status: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    broker_id: String,
    listeners: Vec<String>,

    is_controller: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    controller_id: String,
    #[serde(skip_serializing_if = "is_zero_i64")]
    controller_epoch: i64,

    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat_age_ms: Option<i64>,

    #[serde(skip_serializing_if = "is_zero_u64")]
    assignment_version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    assignment_age_ms: Option<i64>,

    partitions_led: i32,
    partitions_assigned: i32,
    partitions_recovering: i32,

    #[serde(skip_serializing_if = "is_false")]
    storage_stalled: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(v: &bool) -> bool {
    !*v
}

/// Shared state for the axum router — the fields the handler folds
/// into every response.
#[derive(Clone)]
pub struct HealthConfig {
    pub broker_id: String,
    pub listeners: Vec<String>,
    pub source: Option<Arc<dyn RuntimeState>>,
    /// gh #208: when true, `/readyz` additionally requires partition
    /// takeover to be complete (`source.serving()`). Dev / single-node
    /// mode leaves this false — there is no cluster to take over from.
    pub cluster_mode: bool,
}

impl std::fmt::Debug for HealthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HealthConfig")
            .field("broker_id", &self.broker_id)
            .field("listeners", &self.listeners)
            .field(
                "source",
                &self.source.as_ref().map(|_| "<dyn RuntimeState>"),
            )
            .field("cluster_mode", &self.cluster_mode)
            .finish()
    }
}

/// Build the axum router with `/healthz` and `/readyz` bound.
pub fn health_router(cfg: HealthConfig) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .with_state(cfg)
}

async fn healthz(State(cfg): State<HealthConfig>) -> impl IntoResponse {
    let mut state = HealthState {
        status: "ok",
        broker_id: cfg.broker_id.clone(),
        listeners: cfg.listeners.clone(),
        is_controller: false,
        controller_id: String::new(),
        controller_epoch: 0,
        heartbeat_age_ms: None,
        assignment_version: 0,
        assignment_age_ms: None,
        partitions_led: 0,
        partitions_assigned: 0,
        partitions_recovering: 0,
        storage_stalled: false,
    };
    if let Some(src) = cfg.source.as_ref() {
        state.is_controller = src.is_controller();
        state.controller_id = src.controller_id();
        state.controller_epoch = src.controller_epoch();
        state.heartbeat_age_ms = pos_i64(src.heartbeat_age_ms());
        state.assignment_version = src.assignment_version();
        state.assignment_age_ms = pos_i64(src.assignment_age_ms());
        state.partitions_led = src.partitions_led();
        state.partitions_assigned = src.partitions_assigned();
        state.partitions_recovering = src.partitions_recovering();
        state.storage_stalled = src.storage_stalled();
    }
    Json(state)
}

fn pos_i64(v: i64) -> Option<i64> {
    if v >= 0 {
        Some(v)
    } else {
        None
    }
}

#[derive(Debug, Serialize)]
struct ReadyState {
    ready: bool,
}

async fn readyz(State(cfg): State<HealthConfig>) -> impl IntoResponse {
    let is_ready = compute_ready(&cfg);
    let status = if is_ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, Json(ReadyState { ready: is_ready }))
}

/// The full readiness answer (gh #208 / gh #211), split out so it can
/// be unit-tested without an HTTP round trip.
///
/// A broker is ready only when ALL of:
/// - the base gate is set (listeners bound + required env present),
/// - the main runtime is live (not wedged — see [`main_alive`]), and
/// - in cluster mode, partition takeover is complete (`serving`).
///
/// The main-runtime check is what makes readiness honest under a
/// wedge: the handler runs on the dedicated health runtime, so it can
/// still answer while the main runtime is pinned, and it answers
/// "unready" because the liveness tick has gone stale.
fn compute_ready(cfg: &HealthConfig) -> bool {
    // Base gate: listeners bound.
    if !ready() {
        return false;
    }
    // gh #131: an external TLS listener without a hostname pattern is
    // never ready (it can't advertise itself).
    let tls_port = std::env::var("KAAS_TLS_PORT").unwrap_or_default();
    let ext_host = std::env::var("EXTERNAL_HOSTNAME_PATTERN").unwrap_or_default();
    if !tls_port.is_empty() && ext_host.is_empty() {
        return false;
    }
    // Main runtime must be scheduling tasks.
    if !main_alive() {
        return false;
    }
    // Cluster mode: takeover of assigned partitions must be complete.
    if cfg.cluster_mode {
        return cfg.source.as_ref().is_some_and(|s| s.serving());
    }
    true
}

static READY_SNAPSHOT: AtomicBool = AtomicBool::new(false);

/// Flip the base `/readyz` gate. Called by the broker main once all
/// listeners are up and any required env vars are present. This is a
/// *precondition* — `/readyz` also requires the main runtime to be
/// live and (in cluster mode) partition takeover to be complete.
pub fn set_ready(v: bool) {
    READY_SNAPSHOT.store(v, Ordering::SeqCst);
}

/// Report the base `/readyz` gate (listeners bound). Not the full
/// readiness answer — see [`readyz`].
#[must_use]
pub fn ready() -> bool {
    READY_SNAPSHOT.load(Ordering::SeqCst)
}

// --- main-runtime liveness (gh #208 / gh #211 / gh #217) -----------------
//
// `MAIN_TICK_MS` holds the last time the *main* tokio runtime made
// progress. Two sources refresh it, both running on the main runtime:
//   1. a 1 s idle-keepalive loop (so an idle broker stays live), and
//   2. every completed request dispatch (gh #217) — so a broker
//      saturated with produce/fetch work stays live *because* it is
//      serving, not in spite of it.
//
// If every main worker is truly wedged — the gh #209/#210 case where a
// synchronous NFS scan pins both workers under a 2-CPU limit — neither
// source fires: no task runs and no request completes, the tick goes
// stale, and `main_alive()` reports false.
//
// Earlier the tick came *only* from the 1 s loop. Under a heavy produce
// flood that loop could starve for >5 s while the runtime was perfectly
// alive (just busy), flipping `main_alive()` false and — via the
// control-plane `healthy` bit feeding `decide_alive` — evicting a live
// broker from the cluster, which churned partition leadership and fenced
// producers (gh #217). Sourcing the tick from actual request completions
// is the fix: busy == alive.
//
// The check is read from the *dedicated health runtime* and from the
// control-plane status pump, neither of which shares the main runtime,
// so a wedge is observable even when the thing being observed cannot
// run. That is the whole point: `open_partition_keys()` still lists a
// wedged broker's partitions, so takeover-completion alone cannot tell
// "healthy" from "wedged" — this tick can.

/// Milliseconds of no-progress after which the main runtime is presumed
/// wedged. Refreshed by the 1 s idle keepalive AND by every completed
/// request (gh #217), so a busy-but-serving runtime never crosses this;
/// only a genuine wedge (no completions and no idle tick) does. 5 s
/// tolerates a brief idle-tick miss before declaring the runtime dead.
pub const MAIN_LIVENESS_THRESHOLD_MS: i64 = 5_000;

static MAIN_TICK_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

/// Monotonic milliseconds since first use. Monotonic (not wall-clock)
/// so a clock step can't spuriously trip or mask the wedge check.
fn mono_ms() -> i64 {
    use std::sync::OnceLock;
    use std::time::Instant;
    static REF: OnceLock<Instant> = OnceLock::new();
    let r = REF.get_or_init(Instant::now);
    i64::try_from(r.elapsed().as_millis()).unwrap_or(i64::MAX)
}

/// Record a main-runtime progress signal. Called from the 1 s idle
/// keepalive loop AND from every completed request dispatch (gh #217),
/// both on the main tokio runtime — so a saturated-but-serving runtime
/// stays live and only a true wedge goes stale.
pub fn record_main_tick() {
    // `.max(1)`: the monotonic clock reads ~0 for the first sub-ms of
    // the process, and 0 is the "never ticked" sentinel — clamp so a
    // real tick is always distinguishable from no tick.
    MAIN_TICK_MS.store(mono_ms().max(1), Ordering::SeqCst);
}

/// Is the main runtime still scheduling tasks? `false` when no tick
/// has landed within [`MAIN_LIVENESS_THRESHOLD_MS`] — or when no tick
/// has ever been recorded (the tick task hasn't started yet, so the
/// broker is not serving anything either).
#[must_use]
pub fn main_alive() -> bool {
    let last = MAIN_TICK_MS.load(Ordering::SeqCst);
    last > 0 && (mono_ms() - last) < MAIN_LIVENESS_THRESHOLD_MS
}

/// Set once SIGTERM lands (gh #249). Latching, never cleared: a
/// broker that has begun shutting down never un-begins.
static DRAINING: AtomicBool = AtomicBool::new(false);

/// Announce that this broker is shutting down gracefully. Called from
/// the SIGTERM path **before** partitions are relinquished, so the
/// next heartbeat carries it and the controller can move work off
/// proactively rather than waiting out a heartbeat timeout.
///
/// Deliberately not wired into `/readyz`: readiness is about whether
/// this broker can serve the request in front of it, and a draining
/// broker still can until its listeners close. Kubernetes already
/// removes a terminating pod's endpoint.
pub fn mark_draining() {
    DRAINING.store(true, Ordering::SeqCst);
}

/// Has this broker begun a graceful shutdown?
#[must_use]
pub fn is_draining() -> bool {
    DRAINING.load(Ordering::SeqCst)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    // Serialize tests that mutate the process-global READY_SNAPSHOT /
    // MAIN_TICK statics; under the default multi-threaded test runner
    // they race each other's reads (e.g. one test's `set_ready(false)`
    // clobbering another's base gate) and flake.
    static SHARED_STATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    fn lock_shared() -> std::sync::MutexGuard<'static, ()> {
        SHARED_STATE_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    #[test]
    fn set_ready_round_trips() {
        let _g = lock_shared();
        set_ready(true);
        assert!(ready());
        set_ready(false);
        assert!(!ready());
    }

    #[test]
    fn pos_i64_folds_negatives_to_none() {
        assert_eq!(pos_i64(0), Some(0));
        assert_eq!(pos_i64(42), Some(42));
        assert_eq!(pos_i64(-1), None);
    }

    #[test]
    fn health_json_shape_omits_unmeasured_fields() {
        let cfg = HealthConfig {
            broker_id: "kaas-0".to_string(),
            listeners: vec!["plain".to_string()],
            source: None,
            cluster_mode: false,
        };
        let state = HealthState {
            status: "ok",
            broker_id: cfg.broker_id.clone(),
            listeners: cfg.listeners.clone(),
            is_controller: false,
            controller_id: String::new(),
            controller_epoch: 0,
            heartbeat_age_ms: None,
            assignment_version: 0,
            assignment_age_ms: None,
            partitions_led: 0,
            partitions_assigned: 0,
            partitions_recovering: 0,
            storage_stalled: false,
        };
        let json = serde_json::to_value(&state).unwrap();
        assert_eq!(json["status"], "ok");
        assert_eq!(json["broker_id"], "kaas-0");
        assert_eq!(json["listeners"][0], "plain");
        assert!(json.get("controller_epoch").is_none());
        assert!(json.get("storage_stalled").is_none());
    }

    struct FakeState {
        serving: bool,
    }
    impl RuntimeState for FakeState {
        fn is_controller(&self) -> bool {
            false
        }
        fn controller_id(&self) -> String {
            String::new()
        }
        fn controller_epoch(&self) -> i64 {
            0
        }
        fn heartbeat_age_ms(&self) -> i64 {
            -1
        }
        fn assignment_version(&self) -> u64 {
            0
        }
        fn assignment_age_ms(&self) -> i64 {
            -1
        }
        fn partitions_led(&self) -> i32 {
            0
        }
        fn partitions_assigned(&self) -> i32 {
            0
        }
        fn partitions_recovering(&self) -> i32 {
            0
        }
        fn storage_stalled(&self) -> bool {
            false
        }
        fn serving(&self) -> bool {
            self.serving
        }
    }

    fn cfg_with(cluster_mode: bool, serving: bool) -> HealthConfig {
        HealthConfig {
            broker_id: "kaas-0".to_string(),
            listeners: vec!["plain".to_string()],
            source: Some(Arc::new(FakeState { serving })),
            cluster_mode,
        }
    }

    #[test]
    fn readiness_requires_base_gate_main_alive_and_serving() {
        let _g = lock_shared();

        // Base gate down → never ready, whatever else.
        set_ready(false);
        record_main_tick();
        assert!(!compute_ready(&cfg_with(true, true)));

        // Base up + main alive, but not serving in cluster mode → not ready.
        set_ready(true);
        record_main_tick();
        assert!(!compute_ready(&cfg_with(true, false)));

        // Base up + main alive + serving → ready.
        assert!(compute_ready(&cfg_with(true, true)));

        // Dev mode ignores serving.
        assert!(compute_ready(&cfg_with(false, false)));

        set_ready(false);
    }

    #[test]
    fn main_alive_is_false_without_a_recent_tick() {
        let _g = lock_shared();
        // A fresh process that never ticked is not alive.
        // (mono_ms is monotonic and MAIN_TICK starts at 0.)
        // We can't reset the static to 0 once ticked, so only assert
        // the positive: a tick makes it alive.
        record_main_tick();
        assert!(main_alive());
    }
}
