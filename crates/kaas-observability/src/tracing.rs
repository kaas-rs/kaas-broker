//! `tracing-subscriber` bring-up: the log filter and formatter.
//!
//! The level itself is the *caller's* input: the operator reads
//! `KAAS_LOG_LEVEL` directly, and the broker resolves it in
//! `kaas_broker::cli` (`RUST_LOG` wins, `KAAS_LOG_LEVEL` next,
//! `info` last — gh #254; the broker ignored the chart's level
//! entirely from the Rust cutover until then). `KAAS_LOG_FORMAT`
//! (json|text) is read here. Replaces the ad-hoc `init_tracing`
//! stubs in `bins/kaas/main.rs` and `bins/kaas-operator/main.rs`.
//!
//! Logs go to stdout only. There is no OTel layer and no trace
//! correlation: kaas exports no traces (gh #269).

use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

/// Install the global `tracing` subscriber. Safe to call once at
/// startup; subsequent calls are a no-op (the harness's global
/// dispatcher only accepts one install).
pub fn install_tracing(log_level: &str, log_format: &str) {
    let filter = EnvFilter::try_new(log_level).unwrap_or_else(|_| EnvFilter::new("info"));

    if log_format.eq_ignore_ascii_case("json") {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer().json())
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(filter)
            .with(tracing_subscriber::fmt::layer())
            .try_init();
    }
}
