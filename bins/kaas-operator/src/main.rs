//! kaas-operator — Phase 7 binary.
//!
//! Boots three
//! reconcilers (KafkaTopic, KafkaUser, KafkaCluster) against a
//! namespace-scoped `kube::Client`, runs the leader-elected startup
//! sweep, exposes `/healthz` + `/readyz` over axum, and shuts down
//! cleanly on SIGTERM. Only the holder of the leader lease reconciles
//! or sweeps; it renews the lease while it leads, and exits if it loses
//! it (gh #329).
//!
//! Environment variables (chart-templated by
//! `templates/operator-deployment.yaml` unless noted):
//!
//! - `KAAS_DATA_DIR`            — shared PVC mount (default `/data`)
//! - `KAAS_CLUSTER_DIR`         — cluster-state dir (default
//!   `<KAAS_DATA_DIR>/__cluster`); templated only when
//!   `storage.controlPlane.enabled` mounts it on its own PVC (gh #221)
//! - `KAAS_LOG_DIRS`            — volume-pool log dirs as JSON; templated
//!   only when `storage.pool` is non-empty (gh #221 phase 2)
//! - `KAAS_NAMESPACE`           — operator's own namespace
//! - `KAAS_LOG_LEVEL`           — `debug`/`info`/`warn`/`error`
//! - `KAAS_LOG_FORMAT`          — `json` or `text`
//! - `HEALTH_PROBE_BIND_ADDRESS`  — healthz / readyz; not templated, the
//!   default `:8081` is what the chart's probes target
//! - `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`, `OTEL_SERVICE_VERSION`,
//!   `KAAS_METRIC_EXPORT_INTERVAL` (templated when
//!   `observability.otlp.metrics.enabled`), plus `OTEL_RESOURCE_ATTRIBUTES`
//!   and `OTEL_SERVICE_NAME` (not templated) — read by
//!   `kaas_observability::bootstrap`. Metrics are pushed over OTLP;
//!   there is no scrape endpoint.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{routing::get, Router};
use futures::StreamExt;
use kaas_controller::KubeLeaseElection;
use kaas_operator_api::{KafkaCluster, KafkaTopic, KafkaUser};
use kaas_operator_controllers::{
    kafkacluster_controller, kafkatopic_controller, kafkauser_controller, sweep,
    KafkaClusterReconciler, KafkaTopicReconciler, KafkaUserReconciler,
};
use kube::api::ListParams;
use kube::runtime::watcher::Config as WatcherConfig;
use kube::runtime::Controller;
use kube::{Api, Client};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

const DEFAULT_DATA_DIR: &str = "/data";
const DEFAULT_NAMESPACE: &str = "default";
const DEFAULT_LOG_LEVEL: &str = "info";
const DEFAULT_LOG_FORMAT: &str = "json";
const DEFAULT_PROBE_ADDR: &str = "0.0.0.0:8081";
const LEADER_LEASE: &str = "kaas-operator-leader";
/// gh #205: cadence of the resumable orphan sweep re-run. Slow — the
/// steady-state reclaim is reconcile-time; this only mops up orphans a
/// prior pass couldn't remove (FDs held) or that the operator missed
/// while down.
const SWEEP_INTERVAL: Duration = Duration::from_secs(300);
/// How long a SIGTERM waits for running reconciles before exiting anyway. Inside
/// the pod's default 30 s `terminationGracePeriodSeconds`, with room for the rest
/// of the shutdown.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
/// The API calls on the way out (the lease release, the last log line) get this
/// each. The API server may be why we're stopping.
const SHUTDOWN_API_BUDGET: Duration = Duration::from_secs(3);

#[tokio::main]
async fn main() -> Result<()> {
    // rustls 0.23 requires a CryptoProvider to be picked before
    // kube-rs's first TLS handshake (transitive hyper-rustls). We
    // enable `ring` at workspace level; the provider isn't auto-
    // installed. Without this call the operator panics on the first
    // `Client::try_default()` invocation.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let log_level = env_or("KAAS_LOG_LEVEL", DEFAULT_LOG_LEVEL);
    let log_format = env_or("KAAS_LOG_FORMAT", DEFAULT_LOG_FORMAT);

    kaas_observability::install_tracing(&log_level, &log_format);
    let obs_cancel = CancellationToken::new();
    let providers = kaas_observability::bootstrap("kaas-operator", obs_cancel.clone())
        .await
        .context("initialising observability")?;

    let data_dir = PathBuf::from(env_or("KAAS_DATA_DIR", DEFAULT_DATA_DIR));
    // gh #221 phase 1: cluster-wide state (credentials.json,
    // acls.json, …) may live on its own control-plane volume. Unset →
    // the legacy `<data_dir>/__cluster` layout.
    let cluster_dir = std::env::var("KAAS_CLUSTER_DIR")
        .ok()
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| data_dir.join("__cluster"));
    // gh #221 phase 2: pool log dirs (same env JSON the broker gets).
    let log_dirs =
        kaas_storage::parse_log_dirs_json(&std::env::var("KAAS_LOG_DIRS").unwrap_or_default())
            .map_err(|e: String| anyhow::anyhow!(e))?;
    kaas_storage::check_cluster_dir_placement(&cluster_dir, &data_dir, &log_dirs)
        .map_err(|e: String| anyhow::anyhow!(e))?;
    let namespace = env_or("KAAS_NAMESPACE", DEFAULT_NAMESPACE);
    let probe_addr = env_or("HEALTH_PROBE_BIND_ADDRESS", DEFAULT_PROBE_ADDR);

    info!(
        %namespace, data_dir = %data_dir.display(),
        cluster_dir = %cluster_dir.display(),
        %probe_addr,
        "starting kaas-operator"
    );

    let cancel = CancellationToken::new();
    install_signal_handlers(cancel.clone());

    let ready_flag = Arc::new(AtomicBool::new(false));
    let healthy_flag = Arc::new(AtomicBool::new(false));

    // Probe endpoints are bound BEFORE we wait for the kube client
    // so a slow apiserver doesn't fail the chart's readinessProbe.
    //
    // Dedicated OS thread + its own single-thread runtime (gh #187):
    // as a plain tokio::spawn the probe endpoints shared the worker
    // pool with every reconciler, so a reconcile burst under node
    // load starved /healthz past the liveness deadline and the
    // kubelet SIGKILLed a healthy operator — the same control-plane
    // starvation the broker fixed by isolating its cluster runtime
    // (97b5d97). Liveness must stay answerable no matter what the
    // reconcilers are doing.
    let probe_thread = std::thread::Builder::new()
        .name("probe-server".into())
        .spawn({
            let addr = probe_addr.clone();
            let healthy = healthy_flag.clone();
            let ready = ready_flag.clone();
            let cancel = cancel.clone();
            move || {
                let rt = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(rt) => rt,
                    Err(err) => {
                        // No runtime means no probe endpoints; the
                        // kubelet will restart us, which is the right
                        // outcome for a broken process anyway.
                        error!(%err, "probe-server runtime failed to build");
                        return;
                    }
                };
                rt.block_on(spawn_probe_server(addr, healthy, ready, cancel));
            }
        })
        .context("spawning probe-server thread")?;

    // Build the kube client. try_default reads KUBECONFIG or the
    // in-cluster ServiceAccount.
    let client = Client::try_default()
        .await
        .context("building kube::Client (no KUBECONFIG?)")?;
    healthy_flag.store(true, Ordering::Relaxed);
    // Ready doesn't wait for leadership. A rollout surges the new pod and stops the
    // old one only once the new one is Ready, so a standby that waited for the lease
    // would wait for ever: the old pod keeps it until it's stopped (gh #329).
    ready_flag.store(true, Ordering::Relaxed);
    info!("kube client connected");

    // Leader election. The startup sweep + reconcilers run on the
    // elected pod only: multiple replicas are supported but only
    // one drives the loop at a time.
    let identity = std::env::var("HOSTNAME").unwrap_or_else(|_| "kaas-operator-0".into());
    // Its wins aren't controller failovers (gh #290 verification).
    let election = KubeLeaseElection::new(
        client.clone(),
        namespace.clone(),
        LEADER_LEASE,
        identity.clone(),
    )
    .without_controller_metrics();
    // Timings: KubeLeaseElection's defaults (15/10/2), the one source of
    // truth shared with the broker's controller Lease (gh #281).
    info!(%identity, lease = LEADER_LEASE, "waiting for leader-election");
    // Its own token, not `cancel`: a SIGTERM keeps the lease until the reconcilers
    // have stopped, so no successor starts next to a running reconcile.
    let election_cancel = CancellationToken::new();
    let elected = CancellationToken::new();
    let lost = Arc::new(AtomicBool::new(false));
    let election_task = tokio::spawn(election.campaign(
        election_cancel.clone(),
        {
            let elected = elected.clone();
            move |epoch, _| {
                info!(%epoch, "elected leader; starting reconcilers");
                elected.cancel();
            }
        },
        {
            let election_cancel = election_cancel.clone();
            let lost = lost.clone();
            let cancel = cancel.clone();
            move || {
                if !election_cancel.is_cancelled() {
                    lost.store(true, Ordering::SeqCst);
                    cancel.cancel();
                }
            }
        },
    ));

    let tasks = tokio::select! {
        () = cancel.cancelled() => Vec::new(),
        () = elected.cancelled() => start_reconcilers(
            client.clone(),
            namespace.clone(),
            data_dir,
            cluster_dir,
            log_dirs,
            cancel.clone(),
        ),
    };

    // Wait until something asks us to stop. The reconcilers and the sweep
    // stop on the same token.
    cancel.cancelled().await;

    // Another candidate may already be reconciling, so don't wait for anything.
    // The reconcilers are idempotent, and the restart comes back as a candidate.
    if lost.load(Ordering::SeqCst) {
        error!(%identity, "lost the leader lease; exiting");
        std::process::exit(1);
    }

    info!("shutdown signal received; waiting for reconcilers");
    let drained = tokio::time::timeout(SHUTDOWN_GRACE, futures::future::join_all(tasks))
        .await
        .is_ok();
    if !drained {
        warn!(
            grace_seconds = SHUTDOWN_GRACE.as_secs(),
            "reconcilers still running after the shutdown grace; exiting anyway"
        );
    }

    // Last, so the next leader doesn't wait out the lease, and doesn't start early.
    election_cancel.cancel();
    if tokio::time::timeout(SHUTDOWN_API_BUDGET, election_task)
        .await
        .is_err()
    {
        warn!("lease release timed out; it will expire on its own");
    }

    // The probe thread exits via the axum graceful shutdown once
    // `cancel` fires; join it off the async runtime.
    let _ = tokio::time::timeout(
        SHUTDOWN_API_BUDGET,
        tokio::task::spawn_blocking(move || probe_thread.join()),
    )
    .await;

    // Flush pending OTLP pushes before the process dies.
    if let Err(err) = providers.shutdown() {
        warn!(%err, "observability shutdown reported error");
    }
    obs_cancel.cancel();

    // Pre-discover topic count for a final log line: useful when
    // chasing "did everything drain?" in pod logs.
    let count = tokio::time::timeout(SHUTDOWN_API_BUDGET, topic_count(&client, &namespace)).await;
    if let Ok(Ok(count)) = count {
        info!(topic_count = count, "operator exiting cleanly");
    } else {
        info!("operator exiting cleanly");
    }
    // Returning would drop the runtime, which waits for any stuck blocking task.
    if !drained {
        std::process::exit(1);
    }
    Ok(())
}

/// Everything only the leader runs: the orphan sweep and the three reconcilers.
fn start_reconcilers(
    client: Client,
    namespace: String,
    data_dir: PathBuf,
    cluster_dir: PathBuf,
    log_dirs: Vec<kaas_storage::LogDirInfo>,
    cancel: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    // Orphan sweep: an initial pass at leader-election, then a slow
    // periodic re-run. gh #205 / NFS rule 2: the sweep is resumable —
    // a directory that can't be removed this pass (ENOTEMPTY while a
    // broker still holds an FD, gh #76) lands in `failed` and is
    // retried on the next tick once the handles close, instead of
    // aborting the whole pass and stranding every other orphan until
    // the next operator restart.
    let sweep_client = client.clone();
    let sweep_ns = namespace.clone();
    let sweep_roots: Vec<PathBuf> = std::iter::once(data_dir.clone())
        .chain(log_dirs.iter().map(|d| d.path.clone()))
        .collect();
    let sweep_cluster_dir = cluster_dir.clone();
    let sweep_cancel = cancel.clone();
    let sweep_task = tokio::spawn(async move {
        let mut tick = tokio::time::interval(SWEEP_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = sweep_cancel.cancelled() => return,
                _ = tick.tick() => {}
            }
            match sweep::sweep_topics(&sweep_client, &sweep_ns, &sweep_roots).await {
                Ok(r) => {
                    if !r.removed.is_empty() {
                        info!(removed = ?r.removed, "removed orphan topic dirs");
                    }
                    if !r.failed.is_empty() {
                        warn!(failed = ?r.failed, "orphan topic dirs not reclaimed this pass; will retry");
                    }
                }
                Err(e) => error!(error = ?e, "topic sweep failed"),
            }
            match sweep::sweep_credentials(&sweep_client, &sweep_ns, &sweep_cluster_dir).await {
                Ok(removed) if !removed.is_empty() => {
                    info!(?removed, "removed orphan credentials")
                }
                Ok(_) => {}
                Err(e) => error!(error = ?e, "credentials sweep failed"),
            }
        }
    });

    // Spawn the three reconcilers. Each Controller::run returns a
    // stream of reconcile outcomes; we drain them concurrently in
    // tokio::select so a single shutdown signal stops the lot.
    let topic_ctx = Arc::new(KafkaTopicReconciler::new(
        client.clone(),
        data_dir.clone(),
        log_dirs.clone(),
    ));
    let user_ctx = Arc::new(KafkaUserReconciler::new(
        client.clone(),
        cluster_dir.clone(),
        namespace.clone(),
    ));
    let cluster_ctx = Arc::new(KafkaClusterReconciler::new(
        client.clone(),
        namespace.clone(),
    ));

    // Stops starting reconciles on the token, and ends once the running ones are
    // done (gh #329). Nothing stopped these streams, so a SIGTERM waited on them
    // until the kubelet's SIGKILL.
    let topic_stream = Controller::new(
        Api::<KafkaTopic>::namespaced(client.clone(), &namespace),
        WatcherConfig::default().any_semantic(),
    )
    .graceful_shutdown_on(cancel.clone().cancelled_owned())
    .run(
        kafkatopic_controller::reconcile_topic,
        kafkatopic_controller::error_policy,
        topic_ctx,
    );

    let user_stream = Controller::new(
        Api::<KafkaUser>::namespaced(client.clone(), &namespace),
        WatcherConfig::default().any_semantic(),
    )
    .graceful_shutdown_on(cancel.clone().cancelled_owned())
    .run(
        kafkauser_controller::reconcile_user,
        kafkauser_controller::error_policy,
        user_ctx,
    );

    let cluster_stream = Controller::new(
        Api::<KafkaCluster>::namespaced(client.clone(), &namespace),
        WatcherConfig::default().any_semantic(),
    )
    .graceful_shutdown_on(cancel.clone().cancelled_owned())
    .run(
        kafkacluster_controller::reconcile_cluster,
        kafkacluster_controller::error_policy,
        cluster_ctx,
    );

    // Box::pin to satisfy the Unpin bound on drain_*; the
    // Controller stream borrows internal pin-projected state from
    // futures_util, so it's not Unpin on its own.
    vec![
        sweep_task,
        tokio::spawn(drain_topic(Box::pin(topic_stream), cancel.clone())),
        tokio::spawn(drain_user(Box::pin(user_stream), cancel.clone())),
        tokio::spawn(drain_cluster(Box::pin(cluster_stream), cancel)),
    ]
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.into())
}

fn install_signal_handlers(cancel: CancellationToken) {
    let cancel_for_int = cancel.clone();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            info!("SIGINT received");
            cancel_for_int.cancel();
        }
    });
    #[cfg(unix)]
    tokio::spawn(async move {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            term.recv().await;
            info!("SIGTERM received");
            cancel.cancel();
        }
    });
}

async fn spawn_probe_server(
    addr: String,
    healthy: Arc<AtomicBool>,
    ready: Arc<AtomicBool>,
    cancel: CancellationToken,
) {
    let app = Router::new()
        .route(
            "/healthz",
            get(move || {
                let healthy = healthy.clone();
                async move {
                    if healthy.load(Ordering::Relaxed) {
                        (StatusCode::OK, "ok").into_response()
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "warming up").into_response()
                    }
                }
            }),
        )
        .route(
            "/readyz",
            get(move || {
                let ready = ready.clone();
                async move {
                    if ready.load(Ordering::Relaxed) {
                        (StatusCode::OK, "ready").into_response()
                    } else {
                        (StatusCode::SERVICE_UNAVAILABLE, "not ready").into_response()
                    }
                }
            }),
        );
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            error!(error = ?e, %addr, "probe server failed to bind");
            return;
        }
    };
    info!(%addr, "probe server listening");
    let _ = axum::serve(listener, app)
        .with_graceful_shutdown(async move { cancel.cancelled().await })
        .await;
}

// Per-CRD drain loops. We use one fn per kind rather than a single
// generic to keep the trait-bound noise low; controller-runtime's
// Stream::Item is the same shape per kind so the body is identical
// modulo the CRD type.
/// A stream ends on shutdown. Any other end stops the operator too.
fn stream_ended(kind: &str, cancel: &CancellationToken) {
    if cancel.is_cancelled() {
        info!(kind, "reconciler stopped");
    } else {
        warn!(kind, "reconciler stream ended");
        cancel.cancel();
    }
}

async fn drain_topic<S>(mut stream: S, cancel: CancellationToken)
where
    S: futures::Stream<
            Item = std::result::Result<
                (
                    kube::runtime::reflector::ObjectRef<KafkaTopic>,
                    kube::runtime::controller::Action,
                ),
                kube::runtime::controller::Error<
                    kaas_operator_controllers::ControllerError,
                    kube::runtime::watcher::Error,
                >,
            >,
        > + Unpin,
{
    while let Some(outcome) = stream.next().await {
        match outcome {
            Ok((obj, action)) => {
                tracing::debug!(kind = "KafkaTopic", ?obj, ?action, "reconciled")
            }
            Err(e) => tracing::warn!(kind = "KafkaTopic", error = %e, "reconcile error"),
        }
    }
    stream_ended("KafkaTopic", &cancel);
}

async fn drain_user<S>(mut stream: S, cancel: CancellationToken)
where
    S: futures::Stream<
            Item = std::result::Result<
                (
                    kube::runtime::reflector::ObjectRef<KafkaUser>,
                    kube::runtime::controller::Action,
                ),
                kube::runtime::controller::Error<
                    kaas_operator_controllers::ControllerError,
                    kube::runtime::watcher::Error,
                >,
            >,
        > + Unpin,
{
    while let Some(outcome) = stream.next().await {
        match outcome {
            Ok((obj, action)) => {
                tracing::debug!(kind = "KafkaUser", ?obj, ?action, "reconciled")
            }
            Err(e) => tracing::warn!(kind = "KafkaUser", error = %e, "reconcile error"),
        }
    }
    stream_ended("KafkaUser", &cancel);
}

async fn drain_cluster<S>(mut stream: S, cancel: CancellationToken)
where
    S: futures::Stream<
            Item = std::result::Result<
                (
                    kube::runtime::reflector::ObjectRef<KafkaCluster>,
                    kube::runtime::controller::Action,
                ),
                kube::runtime::controller::Error<
                    kaas_operator_controllers::ControllerError,
                    kube::runtime::watcher::Error,
                >,
            >,
        > + Unpin,
{
    while let Some(outcome) = stream.next().await {
        match outcome {
            Ok((obj, action)) => {
                tracing::debug!(kind = "KafkaCluster", ?obj, ?action, "reconciled")
            }
            Err(e) => tracing::warn!(kind = "KafkaCluster", error = %e, "reconcile error"),
        }
    }
    stream_ended("KafkaCluster", &cancel);
}

async fn topic_count(client: &Client, namespace: &str) -> Result<usize> {
    let api: Api<KafkaTopic> = Api::namespaced(client.clone(), namespace);
    let list = api.list(&ListParams::default()).await?;
    Ok(list.items.len())
}
