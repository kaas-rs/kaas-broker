//! Kube-backed `LeaseElection` implementation.
//!
//! Wraps the `coordination.k8s.io/v1` `Lease` API in a hand-rolled
//! acquire + renew loop. We don't lean on `kube::runtime::lease`
//! because it doesn't expose `lease_transitions` cleanly, and we
//! need that exact value for the controller epoch fence that
//! `kaas-broker::Coordinator` consults.
//!
//! Algorithm:
//!
//! 1. `GET` the Lease. If missing, `CREATE` it with the current
//!    identity. Read its current `holder_identity` +
//!    `lease_transitions` + `renew_time`.
//! 2. If `holder_identity == self` and we're within
//!    `lease_duration`, refresh `renew_time` and call ourselves
//!    elected with the current `lease_transitions` value.
//! 3. If `holder_identity` is unset OR the existing renew is
//!    stale (`renew_time + lease_duration < now`), take over: bump
//!    `lease_transitions += 1` if the holder changed, set
//!    `holder_identity = self_id`, stamp `renew_time = now`.
//! 4. Otherwise sleep `retry_period` and retry.
//!
//! # Every write is a compare-and-swap
//!
//! Each write — takeover, renew, release — modifies the object the
//! preceding `GET` returned and `replace`s (HTTP `PUT`) it. That object
//! still carries the `resourceVersion` of the read, so the API server
//! applies the write only if nobody has written the Lease since, and
//! answers 409 Conflict otherwise; a racing `CREATE` gets the same 409
//! as `AlreadyExists`. This is client-go's `LeaseLock.Update`, and it
//! is what makes the election elect one candidate.
//!
//! These writes used to be forced server-side applies with no
//! precondition, i.e. last writer wins. Two candidates that both read
//! an expired Lease both wrote `leaseTransitions = N + 1` and both
//! believed they were controller *at the same epoch*, which the
//! broker-side fence (`controller_epoch < lease_epoch`) cannot tell
//! apart — so both controllers' `assignment.json` writes were accepted
//! and the last rename won. A renew could likewise re-assert
//! `holderIdentity = self` over a takeover that had already landed,
//! and a release blanked whoever held the Lease.
//!
//! A 409 means someone else wrote first, and each path reads it that
//! way: a takeover lost the race (`Ok(None)`, retry after
//! `retry_period`); a renew re-reads on its next tick and either finds
//! itself still holding or sees the new holder and steps down; a
//! release leaves the Lease to whoever wrote it.
//!
//! `replace` sends the whole object, so fields this module doesn't
//! touch are carried over from the read rather than pruned. That
//! retires two server-side-apply hazards: a release whose body omitted
//! `leaseTransitions` reset the epoch 119 → 1 live, and the
//! [`LAST_HOLDER_ANNOTATION`] relied on apply pruning to disappear (it
//! is now removed explicitly on takeover).
//!
//! The compare-and-swap is not a fence. It guarantees one winner per
//! transition, but a deposed holder keeps acting until its next renew
//! fails, and the epoch fences on `assignment.json` and on each
//! partition remain what makes a stale controller harmless (client-go's
//! leader election makes the same disclaimer).

// Module-gating done at the `pub mod kube_election;` declaration
// in `lib.rs`; the duplicate `#![cfg(...)]` would trip clippy's
// duplicated-attribute lint.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime;
use kube::api::{Api, PostParams};
use kube::Client;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::election::LeaseElection;

/// Default lease timings: 15 s lease, 10 s renew deadline, 2 s retry.
/// These are client-go's leader-election defaults — Apache Kafka has no
/// controller Lease to match (gh #61, gh #281).
pub const DEFAULT_LEASE_DURATION: Duration = Duration::from_secs(15);
pub const DEFAULT_RENEW_DEADLINE: Duration = Duration::from_secs(10);
pub const DEFAULT_RETRY_PERIOD: Duration = Duration::from_secs(2);

/// client-go's leader-election jitter factor: the renew deadline must
/// leave room for more than one jittered retry.
const JITTER_FACTOR: f64 = 1.2;

/// client-go's `NewLeaderElector` checks, so a timing set kaas accepts is
/// one Kubernetes' own elector would (gh #281): the lease must outlast
/// the renew deadline, and the renew deadline must exceed
/// `1.2 × retry period`, or the holder loses the Lease while still
/// renewing it.
///
/// # Errors
///
/// The rule the set breaks, for the caller's log line.
pub fn validate_timings(
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
) -> Result<(), &'static str> {
    if retry_period.is_zero() {
        return Err("retry period must be positive");
    }
    if lease_duration <= renew_deadline {
        return Err("lease duration must be greater than the renew deadline");
    }
    if renew_deadline.as_secs_f64() <= JITTER_FACTOR * retry_period.as_secs_f64() {
        return Err("renew deadline must be greater than 1.2 × the retry period");
    }
    Ok(())
}

/// Annotation stamped on the Lease by [`KubeLeaseElection::release`]
/// recording who held it before `holderIdentity` was blanked. Lets a
/// subsequent acquire distinguish "same broker re-acquiring after its
/// own release" (no real handover — don't bump `leaseTransitions`)
/// from a genuine change of holder (gh #204). Self-cleaning: the
/// takeover removes it once a real holder is set.
const LAST_HOLDER_ANNOTATION: &str = "kaas.rs/last-holder";

/// Field manager recorded on every Lease write.
const FIELD_MANAGER: &str = "kaas";

fn post_params() -> PostParams {
    PostParams {
        field_manager: Some(FIELD_MANAGER.to_owned()),
        ..PostParams::default()
    }
}

/// A Lease write answered 409: the `replace` carried a
/// `resourceVersion` someone else has since moved past, or the
/// `create` lost to another candidate's. Either way another writer got
/// there first — see the module docs.
fn is_conflict(err: &kube::Error) -> bool {
    matches!(err, kube::Error::Api(ae) if ae.code == 409)
}

/// Count a 409 on a Lease write, labelled by which write lost.
fn record_conflict(write: &'static str) {
    kaas_observability::metrics::global()
        .controller_lease_conflicts
        .add(1, &[kaas_observability::KeyValue::new("write", write)]);
}

/// Split a kube error into log fields. kube's own `Display` for an API
/// error prints the server's message twice — as text, then again inside
/// an escaped `ErrorResponse { .. }` dump. `code` and `reason` are
/// present only for errors the API server answered.
fn error_fields(err: &kube::Error) -> (Option<u16>, Option<&str>, String) {
    match err {
        kube::Error::Api(ae) => (Some(ae.code), Some(ae.reason.as_str()), ae.message.clone()),
        other => (None, None, other.to_string()),
    }
}

/// A Lease's `holderIdentity` for a log line; empty means released.
fn holder_label(holder: &str) -> &str {
    if holder.is_empty() {
        "<released>"
    } else {
        holder
    }
}

/// What a renew or release found when it went to write the Lease.
#[derive(Debug, PartialEq, Eq)]
enum LeaseWrite {
    /// We held the Lease and the write landed.
    Written,
    /// As of our read the Lease names someone else; nothing was written.
    NotHolder { holder: String, epoch: Option<i32> },
    /// The Lease object no longer exists; nothing was written.
    Gone,
    /// We held it, but someone else wrote it before our write landed.
    Conflict,
}

/// `leaseTransitions` is the controller epoch that fences
/// `assignment.json`; it must count *handovers*, not acquisitions.
/// Bump only when the previous holder — the live `holderIdentity` if
/// non-empty, else the [`LAST_HOLDER_ANNOTATION`] a release left
/// behind — is someone else or unknown (gh #204).
fn next_transitions(prev_holder: Option<&str>, identity: &str, current: i32) -> i32 {
    match prev_holder {
        Some(prev) if prev == identity => current,
        _ => current + 1,
    }
}

pub struct KubeLeaseElection {
    client: Client,
    namespace: String,
    lease_name: String,
    identity: String,
    lease_duration: Duration,
    renew_deadline: Duration,
    retry_period: Duration,
}

impl std::fmt::Debug for KubeLeaseElection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KubeLeaseElection")
            .field("namespace", &self.namespace)
            .field("lease_name", &self.lease_name)
            .field("identity", &self.identity)
            .field("lease_duration", &self.lease_duration)
            .finish_non_exhaustive()
    }
}

impl KubeLeaseElection {
    pub fn new(
        client: Client,
        namespace: impl Into<String>,
        lease_name: impl Into<String>,
        identity: impl Into<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            client,
            namespace: namespace.into(),
            lease_name: lease_name.into(),
            identity: identity.into(),
            lease_duration: DEFAULT_LEASE_DURATION,
            renew_deadline: DEFAULT_RENEW_DEADLINE,
            retry_period: DEFAULT_RETRY_PERIOD,
        })
    }

    /// Override the lease cadence (test hook).
    pub fn with_timings(
        mut self: Arc<Self>,
        lease_duration: Duration,
        renew_deadline: Duration,
        retry_period: Duration,
    ) -> Arc<Self> {
        if let Some(inner) = Arc::get_mut(&mut self) {
            inner.lease_duration = lease_duration;
            inner.renew_deadline = renew_deadline;
            inner.retry_period = retry_period;
        }
        self
    }

    fn api(&self) -> Api<Lease> {
        Api::namespaced(self.client.clone(), &self.namespace)
    }

    /// Compute the canonical RFC3339 microtime stamp we write into
    /// `spec.renewTime`. Same shape client-go's leader election
    /// writes.
    fn now_microtime() -> MicroTime {
        MicroTime(chrono::Utc::now())
    }

    /// Single acquire attempt. Returns `Ok(Some(epoch))` when the
    /// caller is elected, `Ok(None)` when another holder is still
    /// renewing, and `Err(...)` on transport / API errors.
    async fn try_acquire(&self) -> kube::Result<Option<i64>> {
        let api = self.api();
        let existing = api.get_opt(&self.lease_name).await?;
        let now = chrono::Utc::now();

        let (current_holder, current_transitions, renew_time, lease_seconds) = match &existing {
            None => (None, 0i32, None, None),
            Some(l) => {
                let spec = l.spec.clone().unwrap_or_default();
                (
                    spec.holder_identity,
                    spec.lease_transitions.unwrap_or(0),
                    spec.renew_time,
                    spec.lease_duration_seconds,
                )
            }
        };
        let last_holder_annotation = existing.as_ref().and_then(|l| {
            l.metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(LAST_HOLDER_ANNOTATION))
                .cloned()
        });

        let we_already_hold = current_holder.as_deref() == Some(self.identity.as_str());
        let lease_window = lease_seconds
            .map(|s| Duration::from_secs(u64::try_from(s.max(0)).unwrap_or(0)))
            .unwrap_or(self.lease_duration);
        let last_renew_age = renew_time
            .as_ref()
            .map(|MicroTime(t)| now.signed_duration_since(*t).to_std().unwrap_or_default());
        let lease_is_stale = last_renew_age.map(|age| age > lease_window).unwrap_or(true);
        // An empty holder means the previous controller released on
        // shutdown (client-go ReleaseOnCancel semantics) — free to
        // take without waiting out the lease window.
        let released = current_holder.as_deref().is_none_or(str::is_empty);

        if !we_already_hold && !lease_is_stale && !released {
            debug!(
                holder = current_holder.as_deref().unwrap_or("<none>"),
                "lease still held by another controller; retrying"
            );
            return Ok(None);
        }

        // The previous holder is the live one if set; a blank holder
        // means a release happened, and the release stamped who it
        // was in the annotation. Bumping on every re-acquire (the
        // pre-gh #204 behaviour) inflated the epoch by ~1000 with a
        // single broker and zero real handovers.
        let prev_holder = match current_holder.as_deref() {
            Some(h) if !h.is_empty() => Some(h),
            _ => last_holder_annotation.as_deref(),
        };
        let new_transitions = next_transitions(prev_holder, &self.identity, current_transitions);

        let duration_secs = i32::try_from(self.lease_duration.as_secs()).unwrap_or(i32::MAX);
        let stamp = Self::now_microtime();
        let written = match existing {
            Some(mut lease) => {
                // Put back the object just read: it still carries that
                // read's `resourceVersion`, so this lands only if nobody
                // has written the Lease since (module docs).
                let spec = lease.spec.get_or_insert_with(Default::default);
                spec.holder_identity = Some(self.identity.clone());
                spec.lease_duration_seconds = Some(duration_secs);
                spec.acquire_time = Some(stamp.clone());
                spec.renew_time = Some(stamp);
                spec.lease_transitions = Some(new_transitions);
                if let Some(annotations) = lease.metadata.annotations.as_mut() {
                    annotations.remove(LAST_HOLDER_ANNOTATION);
                }
                api.replace(&self.lease_name, &post_params(), &lease).await
            }
            None => {
                let spec = k8s_openapi::api::coordination::v1::LeaseSpec {
                    holder_identity: Some(self.identity.clone()),
                    lease_duration_seconds: Some(duration_secs),
                    acquire_time: Some(stamp.clone()),
                    renew_time: Some(stamp),
                    lease_transitions: Some(new_transitions),
                    ..Default::default()
                };
                let lease = Lease {
                    metadata: kube::api::ObjectMeta {
                        name: Some(self.lease_name.clone()),
                        namespace: Some(self.namespace.clone()),
                        ..Default::default()
                    },
                    spec: Some(spec),
                };
                api.create(&post_params(), &lease).await
            }
        };
        match written {
            Ok(_) => Ok(Some(i64::from(new_transitions))),
            Err(err) if is_conflict(&err) => {
                record_conflict("takeover");
                info!(
                    identity = %self.identity,
                    "controller lease taken by another candidate first (409); standing by"
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    /// Refresh `renewTime` if we still hold the Lease. An `Err` is a
    /// transient API failure (the caller decides when to give up via
    /// `renew_deadline`), and so is [`LeaseWrite::Conflict`]: the Lease
    /// changed since our read, and the next tick's read says whether we
    /// still hold it. Replacing over it is how a renew used to undo a
    /// takeover that had already landed.
    async fn try_renew(&self) -> kube::Result<LeaseWrite> {
        let duration_secs = i32::try_from(self.lease_duration.as_secs()).unwrap_or(i32::MAX);
        self.write_if_held(|lease| {
            let spec = lease.spec.get_or_insert_with(Default::default);
            spec.lease_duration_seconds = Some(duration_secs);
            spec.renew_time = Some(Self::now_microtime());
        })
        .await
    }

    /// Best-effort release on shutdown: blank `holderIdentity` so
    /// the next candidate doesn't wait out the full lease window
    /// (client-go's ReleaseOnCancel). Errors are logged and
    /// swallowed — the lease going stale is the fallback.
    async fn release(&self) {
        match self.try_release().await {
            Ok(LeaseWrite::Written | LeaseWrite::Gone) => {}
            Ok(LeaseWrite::NotHolder { holder, epoch }) => info!(
                identity = %self.identity,
                holder = holder_label(&holder),
                epoch,
                "controller lease no longer ours at release; leaving it"
            ),
            Ok(LeaseWrite::Conflict) => {
                record_conflict("release");
                info!(
                    identity = %self.identity,
                    "controller lease written by someone else before our release landed (409); \
                     leaving it"
                );
            }
            Err(err) => {
                let (code, reason, message) = error_fields(&err);
                warn!(
                    code,
                    reason,
                    %message,
                    "lease election: release failed (lease will expire naturally)"
                );
            }
        }
    }

    /// Blank `holderIdentity`, leaving a note of who released it. Only
    /// the holder may: blanking someone else's would hand their term to
    /// the next candidate.
    async fn try_release(&self) -> kube::Result<LeaseWrite> {
        self.write_if_held(|lease| {
            // `leaseTransitions` rides along from the read untouched, so
            // a release can't rewind the epoch fence.
            let spec = lease.spec.get_or_insert_with(Default::default);
            spec.holder_identity = Some(String::new());
            spec.renew_time = Some(Self::now_microtime());
            // Record who is releasing so the next acquire can tell a
            // self-re-acquire from a real handover and leave
            // `leaseTransitions` alone for the former (gh #204).
            lease
                .metadata
                .annotations
                .get_or_insert_with(Default::default)
                .insert(LAST_HOLDER_ANNOTATION.to_owned(), self.identity.clone());
        })
        .await
    }

    /// Read the Lease and, if we hold it, `edit` it and put it back
    /// conditionally on the read's `resourceVersion` (module docs).
    async fn write_if_held(&self, edit: impl FnOnce(&mut Lease)) -> kube::Result<LeaseWrite> {
        let api = self.api();
        let Some(mut lease) = api.get_opt(&self.lease_name).await? else {
            return Ok(LeaseWrite::Gone);
        };
        let spec = lease.spec.get_or_insert_with(Default::default);
        if spec.holder_identity.as_deref() != Some(self.identity.as_str()) {
            return Ok(LeaseWrite::NotHolder {
                holder: spec.holder_identity.take().unwrap_or_default(),
                epoch: spec.lease_transitions,
            });
        }
        edit(&mut lease);
        match api.replace(&self.lease_name, &post_params(), &lease).await {
            Ok(_) => Ok(LeaseWrite::Written),
            Err(err) if is_conflict(&err) => Ok(LeaseWrite::Conflict),
            Err(err) => Err(err),
        }
    }

    /// Long-running election driver: acquire → `on_acquired(epoch,
    /// leader_token)` → renew every `retry_period` → on loss run
    /// `on_lost`, cancel `leader_token`, and re-enter candidacy.
    /// Returns only when `cancel` fires (best-effort releasing the
    /// Lease if held).
    ///
    /// Leader-election callbacks: `on_acquired` ≙
    /// start-of-leadership (spawn controller tasks bound to the
    /// token), `on_lost` ≙ `OnStoppedLeading` — invoked
    /// *synchronously before* any re-acquire, so state it flips
    /// (e.g. an is_controller gauge) can never trail a newer
    /// acquisition.
    pub async fn campaign<F, G>(
        self: Arc<Self>,
        cancel: CancellationToken,
        on_acquired: F,
        on_lost: G,
    ) where
        F: Fn(i64, CancellationToken) + Send + Sync,
        G: Fn() + Send + Sync,
    {
        loop {
            let epoch = tokio::select! {
                () = cancel.cancelled() => return,
                epoch = self.acquire() => epoch,
            };
            info!(epoch, identity = %self.identity, "controller lease acquired");
            let leader_token = cancel.child_token();
            on_acquired(epoch, leader_token.clone());

            // Leadership: renew until loss or shutdown. Transient
            // API errors are tolerated until `renew_deadline` of
            // continuous failure — mirrors client-go semantics.
            let mut first_err_at: Option<std::time::Instant> = None;
            loop {
                tokio::select! {
                    () = cancel.cancelled() => {
                        on_lost();
                        leader_token.cancel();
                        self.release().await;
                        return;
                    }
                    // The controller stack cancels its own token when
                    // it can't run (e.g. the initial assignment write
                    // failed). Release so another candidate takes over
                    // immediately instead of us renewing a leaderless
                    // lease.
                    () = leader_token.cancelled() => {
                        warn!(identity = %self.identity,
                              "controller stack ended; releasing lease and re-entering candidacy");
                        self.release().await;
                        break;
                    }
                    () = tokio::time::sleep(self.retry_period) => {}
                }
                // A single kube call must never block the loop past
                // the renew window — the API server on a loaded node
                // can stall for tens of seconds (observed p99 1.7 s,
                // long tail ≫ lease_duration during pod churn) and an
                // unbounded call here is how the lease gets stolen
                // out from under a healthy controller. Per-attempt
                // budget = renew_deadline (client-go semantics: one
                // slow-but-live call may use the whole window; a hung
                // one burns it once and we abdicate cleanly).
                let attempt_budget = self.renew_deadline.max(Duration::from_secs(3));
                match tokio::time::timeout(attempt_budget, self.try_renew()).await {
                    Ok(Ok(LeaseWrite::Written)) => first_err_at = None,
                    Ok(Ok(LeaseWrite::NotHolder { holder, epoch })) => {
                        warn!(
                            identity = %self.identity,
                            new_holder = holder_label(&holder),
                            epoch,
                            "controller lease lost to another holder"
                        );
                        break;
                    }
                    Ok(Ok(LeaseWrite::Gone)) => {
                        warn!(identity = %self.identity,
                              "controller lease lost: the Lease object was deleted");
                        break;
                    }
                    Ok(Ok(LeaseWrite::Conflict)) => {
                        record_conflict("renew");
                        let since = *first_err_at.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() > self.renew_deadline {
                            warn!(identity = %self.identity,
                                  "controller lease kept changing under our renew past \
                                   renew_deadline; abdicating");
                            break;
                        }
                        warn!(identity = %self.identity,
                              "controller lease written by someone else since our read (409); \
                               re-reading next tick to check we still hold it");
                    }
                    Ok(Err(err)) => {
                        let (code, reason, message) = error_fields(&err);
                        let since = *first_err_at.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() > self.renew_deadline {
                            warn!(
                                code,
                                reason,
                                %message,
                                "controller lease renew failed past renew_deadline; abdicating"
                            );
                            break;
                        }
                        warn!(code, reason, %message, "controller lease renew failed; retrying");
                    }
                    Err(_elapsed) => {
                        let since = *first_err_at.get_or_insert_with(std::time::Instant::now);
                        if since.elapsed() > self.renew_deadline {
                            warn!(
                                "controller lease renew timed out past renew_deadline; abdicating"
                            );
                            break;
                        }
                        warn!(
                            budget_s = attempt_budget.as_secs(),
                            "controller lease renew attempt timed out; retrying"
                        );
                    }
                }
            }
            on_lost();
            leader_token.cancel();
        }
    }
}

#[async_trait]
impl LeaseElection for KubeLeaseElection {
    async fn acquire(&self) -> i64 {
        let started = std::time::Instant::now();
        loop {
            match self.try_acquire().await {
                Ok(Some(epoch)) => {
                    let m = kaas_observability::metrics::global();
                    m.controller_failovers.add(1, &[]);
                    m.controller_failover_duration
                        .record(started.elapsed().as_secs_f64(), &[]);
                    return epoch;
                }
                Ok(None) => {}
                Err(err) => {
                    let (code, reason, message) = error_fields(&err);
                    warn!(code, reason, %message, "lease election: try_acquire failed; retrying");
                }
            }
            tokio::time::sleep(self.retry_period).await;
        }
    }

    fn identity(&self) -> String {
        self.identity.clone()
    }
}

#[cfg(test)]
mod tests {

    /// gh #281: client-go's rules, including the 1.2 jitter factor.
    #[test]
    fn validate_timings_matches_client_go() {
        let s = std::time::Duration::from_secs;
        assert!(validate_timings(s(15), s(10), s(2)).is_ok(), "the defaults");
        assert!(validate_timings(s(30), s(20), s(2)).is_ok());
        assert!(
            validate_timings(s(15), s(15), s(2)).is_err(),
            "lease == renew"
        );
        assert!(
            validate_timings(s(15), s(20), s(2)).is_err(),
            "lease < renew"
        );
        assert!(
            validate_timings(s(12), s(10), s(9)).is_err(),
            "10 <= 1.2 × 9"
        );
        assert!(validate_timings(s(60), s(6), s(5)).is_err(), "6 <= 1.2 × 5");
        assert!(validate_timings(s(15), s(10), s(0)).is_err());
    }
    use super::*;

    use std::convert::Infallible;

    use http_body_util::BodyExt;
    use k8s_openapi::api::coordination::v1::LeaseSpec;
    use kube::client::Body;
    use parking_lot::Mutex;
    use tokio::sync::Barrier;

    const NS: &str = "kaas";
    const LEASE: &str = "kaas-controller";

    /// One Lease behind the API server's optimistic concurrency — the
    /// part under test: every write stamps a new `resourceVersion`, and
    /// a `PUT` carrying any other version, or a `POST` for a Lease that
    /// already exists, is answered 409.
    #[derive(Default)]
    struct FakeApiServer {
        stored: Mutex<Stored>,
        /// Holds every `GET` until this many have arrived, so racing
        /// candidates are guaranteed to read the same version.
        read_barrier: Option<Barrier>,
        /// Applied once, just after the next `GET` is answered: another
        /// writer landing between a caller's read and its write.
        pending_write: Mutex<Option<ForeignWrite>>,
    }

    type ForeignWrite = Box<dyn FnOnce(&mut Lease) + Send>;

    #[derive(Default)]
    struct Stored {
        lease: Option<Lease>,
        version: u64,
    }

    impl Stored {
        fn put(&mut self, mut lease: Lease) -> Lease {
            self.version += 1;
            lease.metadata.resource_version = Some(self.version.to_string());
            self.lease = Some(lease.clone());
            lease
        }
    }

    impl FakeApiServer {
        fn holding(lease: Lease) -> Self {
            let server = Self::default();
            server.stored.lock().put(lease);
            server
        }

        fn with_racers(mut self, n: usize) -> Self {
            self.read_barrier = Some(Barrier::new(n));
            self
        }

        fn write_after_next_read(&self, write: impl FnOnce(&mut Lease) + Send + 'static) {
            *self.pending_write.lock() = Some(Box::new(write));
        }

        fn lease(&self) -> Lease {
            self.stored.lock().lease.clone().unwrap_or_default()
        }

        fn election(self: &Arc<Self>, identity: &str) -> Arc<KubeLeaseElection> {
            let server = Arc::clone(self);
            let service = tower::service_fn(move |req: http::Request<Body>| {
                let server = Arc::clone(&server);
                async move { Ok::<_, Infallible>(server.handle(req).await) }
            });
            KubeLeaseElection::new(Client::new(service, NS), NS, LEASE, identity)
        }

        async fn handle(&self, req: http::Request<Body>) -> http::Response<Body> {
            let (parts, body) = req.into_parts();
            let body = body.collect().await.expect("request body").to_bytes();
            let item = parts.uri.path().ends_with(&format!("/leases/{LEASE}"));
            match (parts.method.as_str(), item) {
                ("GET", true) => {
                    let read = {
                        let mut stored = self.stored.lock();
                        let read = stored.lease.clone();
                        if let (Some(lease), Some(write)) =
                            (&read, self.pending_write.lock().take())
                        {
                            let mut theirs = lease.clone();
                            write(&mut theirs);
                            stored.put(theirs);
                        }
                        read
                    };
                    // Read, *then* wait: waiting first only proves both
                    // reads arrived, and the first racer's write can land
                    // before the second's read — no race left to test.
                    if let Some(barrier) = &self.read_barrier {
                        barrier.wait().await;
                    }
                    match read {
                        Some(lease) => reply(200, &lease),
                        None => status(404, "NotFound"),
                    }
                }
                ("PUT", true) => {
                    let incoming: Lease = serde_json::from_slice(&body).expect("lease body");
                    let mut stored = self.stored.lock();
                    match stored.lease.as_ref().map(|l| &l.metadata.resource_version) {
                        None => status(404, "NotFound"),
                        Some(current) if *current != incoming.metadata.resource_version => {
                            status(409, "Conflict")
                        }
                        Some(_) => reply(200, &stored.put(incoming)),
                    }
                }
                ("POST", false) => {
                    let incoming: Lease = serde_json::from_slice(&body).expect("lease body");
                    let mut stored = self.stored.lock();
                    if stored.lease.is_some() {
                        return status(409, "AlreadyExists");
                    }
                    reply(201, &stored.put(incoming))
                }
                _ => status(405, "MethodNotAllowed"),
            }
        }
    }

    fn reply(code: u16, lease: &Lease) -> http::Response<Body> {
        json(code, serde_json::to_vec(lease).expect("lease json"))
    }

    fn status(code: u16, reason: &str) -> http::Response<Body> {
        let status = serde_json::json!({
            "kind": "Status", "apiVersion": "v1", "metadata": {},
            "status": "Failure", "message": reason, "reason": reason, "code": code,
        });
        json(code, serde_json::to_vec(&status).expect("status json"))
    }

    fn json(code: u16, body: Vec<u8>) -> http::Response<Body> {
        http::Response::builder()
            .status(code)
            .header("content-type", "application/json")
            .body(Body::from(body))
            .expect("response")
    }

    /// `renewed` is when the holder last renewed; the epoch makes a
    /// lease long expired.
    fn lease(holder: &str, transitions: i32, renewed: chrono::DateTime<chrono::Utc>) -> Lease {
        Lease {
            metadata: kube::api::ObjectMeta {
                name: Some(LEASE.to_owned()),
                namespace: Some(NS.to_owned()),
                ..Default::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.to_owned()),
                lease_duration_seconds: Some(15),
                renew_time: Some(MicroTime(renewed)),
                lease_transitions: Some(transitions),
                ..Default::default()
            }),
        }
    }

    fn holder(lease: &Lease) -> Option<&str> {
        lease.spec.as_ref()?.holder_identity.as_deref()
    }

    fn transitions(lease: &Lease) -> Option<i32> {
        lease.spec.as_ref()?.lease_transitions
    }

    fn last_holder(lease: &Lease) -> Option<&str> {
        lease
            .metadata
            .annotations
            .as_ref()?
            .get(LAST_HOLDER_ANNOTATION)
            .map(String::as_str)
    }

    #[tokio::test]
    async fn two_candidates_taking_over_an_expired_lease_elect_exactly_one() {
        // Both read the same expired Lease. Under the old forced apply
        // both writes landed and both came back elected at epoch 5.
        let expired = lease("kaas-0", 4, chrono::DateTime::UNIX_EPOCH);
        let server = Arc::new(FakeApiServer::holding(expired).with_racers(2));
        let (a, b) = (server.election("kaas-1"), server.election("kaas-2"));
        let (a, b) = tokio::join!(a.try_acquire(), b.try_acquire());
        let (a, b) = (a.expect("kaas-1 attempt"), b.expect("kaas-2 attempt"));

        let winner = match (a, b) {
            (Some(5), None) => "kaas-1",
            (None, Some(5)) => "kaas-2",
            other => unreachable!("exactly one candidate must win epoch 5, got {other:?}"),
        };
        let stored = server.lease();
        assert_eq!(holder(&stored), Some(winner));
        assert_eq!(transitions(&stored), Some(5));
    }

    #[tokio::test]
    async fn two_candidates_creating_the_lease_elect_exactly_one() {
        let server = Arc::new(FakeApiServer::default().with_racers(2));
        let (a, b) = (server.election("kaas-1"), server.election("kaas-2"));
        let (a, b) = tokio::join!(a.try_acquire(), b.try_acquire());
        let outcomes = [a.expect("kaas-1 attempt"), b.expect("kaas-2 attempt")];
        assert_eq!(
            outcomes.iter().flatten().count(),
            1,
            "exactly one create may win: {outcomes:?}"
        );
    }

    #[tokio::test]
    async fn a_renew_does_not_undo_a_takeover_that_landed_after_its_read() {
        let server = Arc::new(FakeApiServer::holding(lease(
            "kaas-1",
            7,
            chrono::Utc::now(),
        )));
        server.write_after_next_read(|theirs| {
            let spec = theirs.spec.get_or_insert_with(Default::default);
            spec.holder_identity = Some("kaas-2".to_owned());
            spec.lease_transitions = Some(8);
        });
        let me = server.election("kaas-1");

        assert_eq!(
            me.try_renew().await.expect("renew"),
            LeaseWrite::Conflict,
            "a stale renew must not land"
        );
        let stored = server.lease();
        assert_eq!(holder(&stored), Some("kaas-2"), "the takeover must survive");
        assert_eq!(transitions(&stored), Some(8));
        // The next tick re-reads and can say who took over, and at what
        // epoch, for the step-down log line.
        assert_eq!(
            me.try_renew().await.expect("second renew"),
            LeaseWrite::NotHolder {
                holder: "kaas-2".to_owned(),
                epoch: Some(8)
            }
        );
    }

    #[tokio::test]
    async fn a_renew_of_a_deleted_lease_reports_it_gone() {
        let server = Arc::new(FakeApiServer::default());
        let renewed = server.election("kaas-1").try_renew().await;
        assert_eq!(renewed.expect("renew"), LeaseWrite::Gone);
    }

    #[tokio::test]
    async fn a_release_leaves_another_holders_lease_alone() {
        let server = Arc::new(FakeApiServer::holding(lease(
            "kaas-2",
            3,
            chrono::Utc::now(),
        )));
        let released = server.election("kaas-1").try_release().await;
        assert_eq!(
            released.expect("release"),
            LeaseWrite::NotHolder {
                holder: "kaas-2".to_owned(),
                epoch: Some(3)
            }
        );
        assert_eq!(holder(&server.lease()), Some("kaas-2"));
    }

    #[tokio::test]
    async fn release_then_self_reacquire_keeps_the_epoch_and_clears_the_breadcrumb() {
        let server = Arc::new(FakeApiServer::holding(lease(
            "kaas-1",
            9,
            chrono::Utc::now(),
        )));
        let me = server.election("kaas-1");

        assert_eq!(
            me.try_release().await.expect("release"),
            LeaseWrite::Written
        );
        let released = server.lease();
        assert_eq!(holder(&released), Some(""));
        assert_eq!(
            transitions(&released),
            Some(9),
            "a release must not rewind the epoch"
        );
        assert_eq!(last_holder(&released), Some("kaas-1"));

        // gh #204: taking our own released lease back is not a handover.
        assert_eq!(me.try_acquire().await.expect("acquire"), Some(9));
        let held = server.lease();
        assert_eq!(holder(&held), Some("kaas-1"));
        assert_eq!(
            last_holder(&held),
            None,
            "the takeover must drop the breadcrumb"
        );
    }

    #[test]
    fn an_api_error_logs_its_message_once() {
        let message = "Operation cannot be fulfilled on leases.coordination.k8s.io \
                       \"kaas-controller\": the object has been modified";
        let err = kube::Error::Api(kube::error::ErrorResponse {
            status: "Failure".to_owned(),
            message: message.to_owned(),
            reason: "Conflict".to_owned(),
            code: 409,
        });
        assert_eq!(
            error_fields(&err),
            (Some(409), Some("Conflict"), message.to_owned())
        );
    }

    #[test]
    fn a_self_reacquire_after_release_keeps_the_epoch() {
        // gh #204: release blanks holderIdentity but the annotation
        // remembers us; taking the lease back is not a transition.
        assert_eq!(next_transitions(Some("kaas-1"), "kaas-1", 42), 42);
    }

    #[test]
    fn a_real_handover_bumps_the_epoch() {
        assert_eq!(next_transitions(Some("kaas-0"), "kaas-1", 42), 43);
    }

    #[test]
    fn an_unknown_previous_holder_bumps_conservatively() {
        // Fresh lease, or a pre-annotation release: no way to prove
        // it was us, so the fence must move forward.
        assert_eq!(next_transitions(None, "kaas-1", 0), 1);
    }

    #[test]
    fn a_renew_while_holding_keeps_the_epoch() {
        // try_acquire while we are still the live holder.
        assert_eq!(next_transitions(Some("kaas-2"), "kaas-2", 7), 7);
    }
}
