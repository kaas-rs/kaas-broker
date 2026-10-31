//! Per-user token-bucket quotas with debt-carry (gh #125).
//!
//! The bucket is allowed
//! to go negative; throttle is proportional to the negative balance.
//! No clamp at zero — that's the bug gh #125 fixed (concurrent
//! clients sharing a principal would each see an independent
//! throttle delay and burst at N×rate).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;

use crate::credentials::CredentialStore;
use crate::types::{Principal, Quotas};

/// Pluggable clock seam so multi-client contention tests are
/// deterministic. Production uses [`RealClock`]; tests use a manual
/// clock that advances when the test asks.
pub trait Clock: Send + Sync + std::fmt::Debug + 'static {
    fn now(&self) -> Instant;
}

#[derive(Debug, Default)]
pub struct RealClock;

impl Clock for RealClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

pub trait QuotaChecker: Send + Sync + std::fmt::Debug + 'static {
    /// Deduct `bytes` from the principal's producer bucket. Returns
    /// `throttle_time_ms` (0 = no throttle needed).
    fn check_produce_quota(&self, principal: &Principal, bytes: usize) -> i32;
    /// Deduct `bytes` from the principal's consumer bucket. Returns
    /// `throttle_time_ms` (0 = no throttle needed).
    fn check_fetch_quota(&self, principal: &Principal, bytes: usize) -> i32;

    /// The [`QuotaEnforcer`] behind this checker, if any. The quota
    /// admin APIs (keys 48/49) read and write limits through it, so
    /// taking it from the checker means they act on the instance that
    /// throttles, with no second wiring step to forget (gh #260).
    fn as_enforcer(self: Arc<Self>) -> Option<Arc<QuotaEnforcer>> {
        None
    }
}

/// No-throttle mode — every check returns 0. Used when no listener
/// has SCRAM/mTLS auth wired (anonymous-only brokers; ANONYMOUS has
/// no quota config to enforce against).
#[derive(Debug, Default)]
pub struct NoQuotaChecker;

impl QuotaChecker for NoQuotaChecker {
    fn check_produce_quota(&self, _p: &Principal, _bytes: usize) -> i32 {
        0
    }

    fn check_fetch_quota(&self, _p: &Principal, _bytes: usize) -> i32 {
        0
    }
}

#[derive(Debug)]
struct TokenBucket {
    producer_tokens: f64,
    consumer_tokens: f64,
    last_refill: Instant,
    producer_rate: f64, // bytes/sec; 0 = unlimited
    consumer_rate: f64,
}

#[derive(Debug)]
pub struct QuotaEnforcer {
    store: Arc<dyn CredentialStore>,
    clock: Arc<dyn Clock>,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    buckets: HashMap<String, TokenBucket>,
    overrides: HashMap<String, Quotas>,
}

impl QuotaEnforcer {
    pub fn new(store: Arc<dyn CredentialStore>) -> Self {
        Self::with_clock(store, Arc::new(RealClock))
    }

    pub fn with_clock(store: Arc<dyn CredentialStore>, clock: Arc<dyn Clock>) -> Self {
        Self {
            store,
            clock,
            inner: Mutex::new(Inner {
                buckets: HashMap::new(),
                overrides: HashMap::new(),
            }),
        }
    }

    /// Install a runtime override (AlterClientQuotas, gh #103). Pass
    /// `None` to clear and revert to the store-backed value. Live-
    /// updates the existing bucket if any.
    pub fn set_user_quota(&self, username: &str, quotas: Option<Quotas>) {
        let mut inner = self.inner.lock();
        match &quotas {
            None => {
                inner.overrides.remove(username);
            }
            Some(q) => {
                inner.overrides.insert(username.to_owned(), q.clone());
            }
        }
        if let Some(b) = inner.buckets.get_mut(username) {
            let effective = match quotas {
                Some(q) => Some(q),
                None => self.store.lookup_quotas(username),
            };
            apply_quotas(b, effective.as_ref());
        }
    }

    /// Override > store > nil — same resolution order as
    /// `set_user_quota`. Used by DescribeClientQuotas "exact-match".
    pub fn describe_user_quota(&self, username: &str) -> Option<Quotas> {
        let inner = self.inner.lock();
        if let Some(q) = inner.overrides.get(username) {
            return Some(q.clone());
        }
        self.store.lookup_quotas(username)
    }

    /// Union of overrides + store-backed entries; overrides win on
    /// collision. Used by DescribeClientQuotas "list all users".
    pub fn list_user_quotas(&self) -> HashMap<String, Quotas> {
        let inner = self.inner.lock();
        let mut out: HashMap<String, Quotas> = self.store.list_all_quotas();
        for (u, q) in &inner.overrides {
            out.insert(u.clone(), q.clone());
        }
        out
    }

    fn check(&self, principal: &Principal, bytes: usize, producer: bool) -> i32 {
        let mut inner = self.inner.lock();
        let now = self.clock.now();
        let username = principal.name.clone();
        // Borrow-checker dance: resolve quotas BEFORE we take the
        // mutable borrow on `buckets`, so we don't double-borrow
        // `inner` via `overrides` and `buckets` at once.
        let resolved_quotas = if inner.buckets.contains_key(&username) {
            None
        } else {
            Some(
                inner
                    .overrides
                    .get(&username)
                    .cloned()
                    .or_else(|| self.store.lookup_quotas(&username)),
            )
        };
        let bucket = inner
            .buckets
            .entry(username)
            .or_insert_with(|| new_bucket(now, resolved_quotas.flatten().as_ref()));

        let elapsed = duration_secs(now.saturating_duration_since(bucket.last_refill));
        bucket.last_refill = now;

        let (rate, tokens) = if producer {
            (bucket.producer_rate, &mut bucket.producer_tokens)
        } else {
            (bucket.consumer_rate, &mut bucket.consumer_tokens)
        };

        if rate == 0.0 {
            return 0;
        }

        // Refill, capped at one second's worth.
        *tokens += rate * elapsed;
        if *tokens > rate {
            *tokens = rate;
        }

        // gh #125: deduct unconditionally; let the bucket carry
        // negative balance forward as debt. Throttle is proportional
        // to the negative balance.
        let bytes_f = bytes_to_f64(bytes);
        *tokens -= bytes_f;
        if *tokens >= 0.0 {
            return 0;
        }
        // Compute the throttle as the time to refill back to zero,
        // saturating at i32::MAX to keep wire encoding well-defined
        // under extreme over-allocation.
        let throttle_secs = (-*tokens) / rate;
        let throttle_ms = (throttle_secs * 1000.0).ceil();
        if throttle_ms <= 0.0 {
            0
        } else if throttle_ms >= i32_max_as_f64() {
            i32::MAX
        } else {
            throttle_ms_to_i32(throttle_ms)
        }
    }
}

fn i32_max_as_f64() -> f64 {
    #[allow(clippy::as_conversions, clippy::cast_precision_loss)]
    {
        i32::MAX as f64
    }
}

impl QuotaChecker for QuotaEnforcer {
    fn check_produce_quota(&self, principal: &Principal, bytes: usize) -> i32 {
        let throttle_ms = self.check(principal, bytes, true);
        if throttle_ms > 0 {
            kaas_observability::metrics::global().quota_throttle.add(
                1,
                &[kaas_observability::KeyValue::new("direction", "produce")],
            );
        }
        throttle_ms
    }

    fn check_fetch_quota(&self, principal: &Principal, bytes: usize) -> i32 {
        let throttle_ms = self.check(principal, bytes, false);
        if throttle_ms > 0 {
            kaas_observability::metrics::global().quota_throttle.add(
                1,
                &[kaas_observability::KeyValue::new("direction", "fetch")],
            );
        }
        throttle_ms
    }

    fn as_enforcer(self: Arc<Self>) -> Option<Arc<QuotaEnforcer>> {
        Some(self)
    }
}

fn new_bucket(now: Instant, quotas: Option<&Quotas>) -> TokenBucket {
    let mut b = TokenBucket {
        producer_tokens: 0.0,
        consumer_tokens: 0.0,
        last_refill: now,
        producer_rate: 0.0,
        consumer_rate: 0.0,
    };
    apply_quotas(&mut b, quotas);
    // Seed the buckets at full so a brand-new principal isn't
    // immediately throttled.
    b.producer_tokens = b.producer_rate;
    b.consumer_tokens = b.consumer_rate;
    b
}

fn apply_quotas(b: &mut TokenBucket, q: Option<&Quotas>) {
    b.producer_rate = q
        .and_then(|q| q.producer_max_byte_rate_per_broker)
        .map(i64_to_f64)
        .unwrap_or(0.0);
    b.consumer_rate = q
        .and_then(|q| q.consumer_max_byte_rate_per_broker)
        .map(i64_to_f64)
        .unwrap_or(0.0);
}

fn duration_secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

fn i64_to_f64(v: i64) -> f64 {
    // i64 fits in f64 with the usual rounding for the magnitudes we
    // care about (byte rates well under 2^53). Use the `as` form
    // inside a wrapper so it lives in one place rather than spread
    // across the hot path.
    #[allow(clippy::as_conversions, clippy::cast_precision_loss)]
    {
        v as f64
    }
}

fn bytes_to_f64(v: usize) -> f64 {
    #[allow(clippy::as_conversions, clippy::cast_precision_loss)]
    {
        v as f64
    }
}

fn throttle_ms_to_i32(v: f64) -> i32 {
    #[allow(
        clippy::as_conversions,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    {
        v as i32
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::TestCred;
    use crate::types::PrincipalKind;
    use crate::CredentialLoader;
    use std::sync::Mutex as StdMutex;

    fn p(name: &str) -> Principal {
        Principal {
            name: name.to_owned(),
            kind: PrincipalKind::User,
        }
    }

    fn loader_with_user(username: &str, producer: i64, consumer: i64) -> Arc<CredentialLoader> {
        let loader = CredentialLoader::new("/tmp/kaas-auth-quota-test");
        loader.install_for_test(vec![TestCred {
            username: username.to_owned(),
            auth_type: "scram-sha-512".to_owned(),
            quotas: Some(Quotas {
                producer_max_byte_rate_per_broker: Some(producer),
                consumer_max_byte_rate_per_broker: Some(consumer),
                request_percentage: None,
            }),
            ..TestCred::default()
        }]);
        Arc::new(loader)
    }

    /// Test clock that only advances when explicitly bumped. Critical
    /// for the gh #125 contention test — back-to-back calls must see
    /// the same instant.
    #[derive(Debug)]
    struct ManualClock {
        now: StdMutex<Instant>,
    }

    impl ManualClock {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                now: StdMutex::new(Instant::now()),
            })
        }

        fn advance(&self, by: Duration) {
            *self.now.lock().unwrap() += by;
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    #[test]
    fn under_limit_does_not_throttle() {
        let q =
            QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), ManualClock::new());
        assert_eq!(q.check_produce_quota(&p("alice"), 500), 0);
    }

    #[test]
    fn over_limit_throttles() {
        let q =
            QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), ManualClock::new());
        let throttle = q.check_produce_quota(&p("alice"), 5000);
        assert!(throttle > 0, "5x over quota must throttle");
    }

    #[test]
    fn zero_rate_means_unlimited() {
        let q = QuotaEnforcer::with_clock(loader_with_user("bob", 0, 0), ManualClock::new());
        assert_eq!(q.check_produce_quota(&p("bob"), 10_000_000), 0);
    }

    #[test]
    fn per_principal_isolation() {
        let q =
            QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), ManualClock::new());
        q.check_produce_quota(&p("alice"), 1000);
        assert_eq!(q.check_produce_quota(&p("bob"), 500), 0);
    }

    /// Pins gh #125 — three back-to-back drain calls on a single
    /// bucket must yield strictly increasing throttle. The clock
    /// stays fixed so no token refill happens between calls.
    #[test]
    fn multi_client_contention_carries_debt() {
        let clock = ManualClock::new();
        let q =
            QuotaEnforcer::with_clock(loader_with_user("bench-perf", 1000, 1000), clock.clone());
        let pr = p("bench-perf");

        // First request: drain bucket to exactly 0 → throttle 0.
        assert_eq!(q.check_produce_quota(&pr, 1000), 0);

        // Second request: bucket at 0, needs 1000 bytes → goes to
        // -1000. Throttle > 0.
        let first = q.check_produce_quota(&pr, 1000);
        assert!(first > 0, "second drain must throttle, got {first}");

        // Third request: bucket at -1000, needs another 1000 → -2000.
        // Throttle MUST be larger than the second. With the
        // pre-gh-#125 clamp at zero, this would equal `first`.
        let second = q.check_produce_quota(&pr, 1000);
        assert!(
            second > first,
            "third drain throttle {second} ≤ second {first} — gh #125 regression"
        );
    }

    #[test]
    fn refill_clears_debt_over_time() {
        let clock = ManualClock::new();
        let q = QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), clock.clone());

        // Drain past zero.
        q.check_produce_quota(&p("alice"), 1000);
        q.check_produce_quota(&p("alice"), 1000); // now at -1000

        clock.advance(Duration::from_secs(2));
        // After 2 seconds at 1000 B/s rate, bucket refills past 0
        // and a 100-byte request fits.
        assert_eq!(q.check_produce_quota(&p("alice"), 100), 0);
    }

    #[test]
    fn set_user_quota_live_updates_existing_bucket() {
        let clock = ManualClock::new();
        let q = QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), clock.clone());
        q.check_produce_quota(&p("alice"), 100);
        // Drop the rate to 100 B/s; next big request must throttle
        // proportionally to the new rate.
        q.set_user_quota(
            "alice",
            Some(Quotas {
                producer_max_byte_rate_per_broker: Some(100),
                consumer_max_byte_rate_per_broker: Some(100),
                request_percentage: None,
            }),
        );
        // Drain.
        q.check_produce_quota(&p("alice"), 200);
        let throttle = q.check_produce_quota(&p("alice"), 200);
        assert!(throttle > 0);
    }

    #[test]
    fn describe_user_quota_resolution_order() {
        let q =
            QuotaEnforcer::with_clock(loader_with_user("alice", 1000, 1000), ManualClock::new());
        // Store-backed by default.
        assert_eq!(
            q.describe_user_quota("alice")
                .unwrap()
                .producer_max_byte_rate_per_broker,
            Some(1000)
        );
        // Override wins.
        q.set_user_quota(
            "alice",
            Some(Quotas {
                producer_max_byte_rate_per_broker: Some(7),
                ..Quotas::default()
            }),
        );
        assert_eq!(
            q.describe_user_quota("alice")
                .unwrap()
                .producer_max_byte_rate_per_broker,
            Some(7)
        );
        // Clear override; falls back.
        q.set_user_quota("alice", None);
        assert_eq!(
            q.describe_user_quota("alice")
                .unwrap()
                .producer_max_byte_rate_per_broker,
            Some(1000)
        );
    }
}
