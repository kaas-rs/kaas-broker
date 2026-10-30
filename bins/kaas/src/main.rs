//! kaas broker binary.
//!
//! Phase 5: per-listener auth + cluster-wide authorization +
//! quotas + consumer-group coordinator + assignment-driven
//! ownership. Listens on every configured listener, serves
//! Produce / Fetch / ListOffsets / Metadata / ApiVersions /
//! InitProducerId / SASL + the full key 8–16 / 42 / 47
//! consumer-group surface against either an on-disk or in-memory
//! storage engine.
//!
//! Cluster bring-up lives in [`cluster::install`] — see that
//! module for the dev / single-broker-disk / cluster mode
//! decision tree.
//!
//! Storage selection:
//! - `KAAS_DATA_DIR` set → `DiskStorageEngine` rooted there.
//! - unset → `MemoryStorage` (dev mode).
//!
//! Auth selection (per-listener via `KAAS_LISTENERS`, cluster-wide
//! via `KAAS_AUTH_DISABLED` / `KAAS_AUTHORIZATION_TYPE` /
//! `KAAS_SUPER_USERS` / `KAAS_SSL_PRINCIPAL_MAPPING_RULES`).
//! `auth_disabled=true` forces `AllowAllAuthorizer` + `NoQuotaChecker`
//! across the board.

mod cluster;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use kaas_auth::{
    AclEngine, AllowAllAuthEngine, AllowAllAuthorizer, AuthEngine, AuthEngineSelector, Authorizer,
    CredentialLoader, NoQuotaChecker, OauthEngine, OauthValidator, PerListenerAuthEngine,
    PrincipalMapper, QuotaChecker, QuotaEnforcer, RealAuthEngine, SuperUserAuthorizer,
};
use kaas_broker::{
    AddOffsetsToTxnHandler, AddPartitionsToTxnHandler, AlterClientQuotasHandler,
    AlterReplicaLogDirsHandler, AlterUserScramCredentialsHandler, ApiVersionsHandler, Broker, Cli,
    CliTlsConfig, CreateAclsHandler, CreatePartitionsHandler, CreateTopicsHandler,
    DeleteAclsHandler, DeleteGroupsHandler, DeleteRecordsHandler, DeleteTopicsHandler,
    DescribeAclsHandler, DescribeClientQuotasHandler, DescribeClusterHandler,
    DescribeConfigsHandler, DescribeGroupsHandler, DescribeLogDirsHandler,
    DescribeUserScramCredentialsHandler, EndTxnHandler, FetchHandler, FindCoordinatorHandler,
    HeartbeatHandler, IncrementalAlterConfigsHandler, InitProducerIdHandler, JoinGroupHandler,
    LeaveGroupHandler, ListGroupsHandler, ListOffsetsHandler, ListenerEntry, MetadataHandler,
    OffsetCommitHandler, OffsetDeleteHandler, OffsetFetchHandler, ProduceHandler,
    SaslAuthenticateHandler, SaslHandshakeHandler, SyncGroupHandler, TopicRegistry,
    TxnOffsetCommitHandler, WriteTxnMarkersHandler,
};
use kaas_protocol::{Dispatcher, ListenerConfig, MtlsConfig, Server, ServerConfigBuilder};
use kaas_storage::{DiskStorageEngine, MemoryStorage, PartitionConfig, RealFs, StorageEngine};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConfig as RustlsServerConfig};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Hot-reload interval for credentials.json + acls.json. 10 s
/// mtime poll. Swapping in `notify` inotify is an open follow-up.
const RELOAD_INTERVAL_SECS: u64 = 10;

/// Init-container entry point — chown/chmod the data dir
/// to the broker uid/gid so the broker container (uid=65532) can
/// mkdir topic dirs at runtime even when the CSI provisioner
/// silently skipped fsGroup-driven perms (kaas#110).
///
/// Skips any KafkaTopic CR walk — the operator
/// creates partition dirs on first reconcile, and the storage
/// engine mkdirs lazily on Produce; the CR walk was an optimisation
/// to have the dirs pre-warm, not a correctness requirement.
fn run_init() -> Result<()> {
    let data_dir = std::env::var("KAAS_DATA_DIR").context("init: KAAS_DATA_DIR not set")?;
    let uid: u32 = std::env::var("KAAS_BROKER_UID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65532);
    let gid: u32 = std::env::var("KAAS_BROKER_GID")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(65532);

    let data_path = Path::new(&data_dir);
    std::fs::create_dir_all(data_path)
        .with_context(|| format!("init: mkdir {}", data_path.display()))?;

    ensure_data_dir_perms(data_path, uid, gid)
        .with_context(|| format!("init: chown/chmod {}", data_path.display()))?;

    // gh #221: the control-plane volume (KAAS_CLUSTER_DIR, phase 1)
    // and every pool log dir (KAAS_LOG_DIRS, phase 2) need the same
    // ownership fix as the data volume when they're separate mounts —
    // same CSI fsGroup caveats apply.
    let mut extra_roots: Vec<PathBuf> = Vec::new();
    if let Ok(cluster_dir) = std::env::var("KAAS_CLUSTER_DIR") {
        if !cluster_dir.is_empty() {
            extra_roots.push(PathBuf::from(cluster_dir));
        }
    }
    if let Ok(json) = std::env::var("KAAS_LOG_DIRS") {
        match kaas_storage::parse_log_dirs_json(&json) {
            Ok(dirs) => extra_roots.extend(dirs.into_iter().map(|d| d.path)),
            Err(e) => anyhow::bail!("init: {e}"),
        }
    }
    for root in &extra_roots {
        std::fs::create_dir_all(root).with_context(|| format!("init: mkdir {}", root.display()))?;
        ensure_data_dir_perms(root, uid, gid)
            .with_context(|| format!("init: chown/chmod {}", root.display()))?;
    }
    eprintln!(
        "kaas init: data_dir={} uid={} gid={} ok",
        data_dir, uid, gid
    );
    Ok(())
}

/// Layer B of the kaas#110 defence-in-depth stack: kubelet's
/// fsGroup (layer A) can silently fail on non-cooperating CSI
/// drivers; this init routine runs as root and makes the data dir
/// writable by the broker process. Layer C (the storage engine's
/// mkdir_all mode 0o775 at runtime) covers new subdirs.
///
/// Walk semantics: chown every entry to (uid, gid); chmod every
/// directory to 0o775 (setgid + 0775 so children inherit the dir's
/// group). Files keep their existing mode. Non-root dev-mode
/// invocations skip the chown pass — chmod EPERM is tolerated.
fn ensure_data_dir_perms(root: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{chown, PermissionsExt};
    // Root-level chmod first — always run so a warm restart on a
    // CSI-cooperating substrate still fixes a mis-configured dir.
    // Tolerate EPERM so dev-mode invocations don't fail.
    if let Err(e) = std::fs::set_permissions(root, std::fs::Permissions::from_mode(0o775)) {
        if e.kind() != std::io::ErrorKind::PermissionDenied {
            return Err(e);
        }
    }
    // Probe: try to chown the root. If we're non-root we get EPERM;
    // skip the recursive fix (dev mode already owns the dir).
    match chown(root, Some(uid), Some(gid)) {
        Ok(()) => walk_and_fix(root, uid, gid),
        Err(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        Err(e) => Err(e),
    }
}

fn walk_and_fix(dir: &Path, uid: u32, gid: u32) -> std::io::Result<()> {
    use std::os::unix::fs::{chown, PermissionsExt};
    // Root already chowned by ensure_data_dir_perms; just chmod +
    // descend.
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o775))?;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let file_type = entry.file_type()?;
        chown(&path, Some(uid), Some(gid))?;
        if file_type.is_dir() {
            walk_and_fix(&path, uid, gid)?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // `--init` mode short-circuits everything else. Called by the
    // Helm chart's `partition-init` init container.
    if std::env::args().nth(1).as_deref() == Some("--init") {
        return run_init();
    }

    // rustls 0.23 requires a CryptoProvider to be picked before the
    // first TLS build. We enabled `ring` at workspace level; the
    // provider isn't auto-installed. Any second-hand rustls user
    // (kube-rs → hyper-rustls) would panic without this. Ignore the
    // Err — it only fires if a provider was already installed by
    // another crate.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::from_env().context("parsing KAAS_* env")?;

    // Bring up OTel BEFORE the first tracing event so the OTel layer
    // sees every subsequent span. Bootstrap installs the global
    // MeterProvider + TracerProvider and populates the Metrics
    // registry; install_tracing then routes tracing macros through
    // the freshly-built tracer.
    let obs_cancel = CancellationToken::new();
    let providers = kaas_observability::bootstrap("kaas", obs_cancel.clone())
        .await
        .context("initialising observability")?;
    let log_format = std::env::var("KAAS_LOG_FORMAT").unwrap_or_else(|_| "json".into());
    kaas_observability::install_tracing(&cli.log_level, &log_format, providers.tracer.clone());

    info!(
        broker_id = cli.broker_id,
        cluster_id = cli.cluster_id.as_str(),
        listeners = cli.listeners.len(),
        data_dir = ?cli.data_dir,
        auth_disabled = cli.auth_disabled,
        authorization_type = cli.authorization_type.as_str(),
        "kaas starting",
    );

    let topics =
        Arc::new(TopicRegistry::from_env_json(&cli.topics_seed).context("parsing KAAS_TOPICS")?);
    // Registry before engine: it doubles as the placement resolver
    // (gh #221 phase 2) — the engine resolves partition roots through
    // the volume assignments the topic watch stashes into it.
    let (engine, disk_engine) = build_engine(
        cli.data_dir.clone(),
        cli.flush_interval_messages,
        cli.fsync_max_latency_ms,
        cli.log_dirs.clone(),
        topics.clone(),
    )?;
    if topics.is_empty() {
        warn!(
            "KAAS_TOPICS is empty — broker will serve metadata for zero topics. \
             Set the env var to a JSON array, e.g. [{{\"name\":\"t1\",\"partitions\":1}}]"
        );
    }

    let auth = build_auth(&cli)?;
    let broker = Arc::new(Broker::with_auth(
        engine.clone(),
        topics.clone(),
        cli.cluster_id.clone(),
        cli.broker_id,
        auth.authorizer.clone(),
        auth.quotas.clone(),
    ));

    // gh #219: persisted, cluster-unique producer IDs. Without this the
    // PID counter restarts at 1 on every boot and every broker issues
    // the same PIDs, so a fresh producer inherits a dead one's dedupe
    // window from `producer-state.snapshot` — records silently dropped
    // as duplicates, or rejected with OUT_OF_ORDER_SEQUENCE_NUMBER.
    // A failure here is non-fatal: we log and keep the in-memory
    // counter rather than refusing to serve.
    if let Some(cluster_dir) = cli.resolved_cluster_dir() {
        match kaas_broker::ProducerIdAllocator::open(&cluster_dir, cli.broker_id) {
            Ok(alloc) => {
                broker.install_producer_id_allocator(Arc::new(alloc));
                info!("installed persisted producer-id allocator");
            }
            Err(err) => warn!(
                %err,
                "producer-id allocator unavailable; falling back to the in-memory counter \
                 (PIDs may be reissued after a restart)"
            ),
        }
    }

    let cancel = CancellationToken::new();

    // Shared kube client — the CR writer, topic watcher, AND the
    // multi-broker cluster runtime all reuse it. Built once; a
    // failure degrades to single-broker behaviour rather than
    // crashing the pod.
    let kube_client = if std::env::var("MY_POD_NAME").is_ok() {
        match kube::Client::try_default().await {
            Ok(client) => Some(client),
            Err(err) => {
                warn!(
                    %err,
                    "kube client init failed; CreateTopics/admin handlers will refuse \
                     and the multi-broker runtime stays disabled"
                );
                None
            }
        }
    } else {
        None
    };

    // Topic-change trigger: watcher callbacks poke the controller's
    // AssignmentLoop (no-op unless this broker holds the Lease).
    let topic_notify = Arc::new(cluster::TopicChangeNotifier::default());

    // In cluster mode, wire the kube-backed TopicCRWriter so admin
    // handlers (CreateTopics, CreatePartitions,
    // IncrementalAlterConfigs) can patch KafkaTopic CRs. Dev-mode
    // leaves cr_writer as None and those handlers return
    // CLUSTER_AUTHORIZATION_FAILED.
    if let Some(client) = kube_client.clone() {
        let ns = std::env::var("KAAS_NAMESPACE").unwrap_or_else(|_| "default".into());
        // ArgoCD coexistence (gh #84 + gh #106, re-ported from the Go
        // broker): when the chart sets admin.argocd.enabled, CRs
        // minted from the Kafka admin protocol carry a tracking-id
        // (visible in the Application tree) + compare/sync options
        // (safe from selfHeal prune). Off by default — plain CRs.
        let argocd = kaas_broker::argocd::ArgoCdConfig::from_env();
        if argocd.enabled {
            info!(
                application = %argocd.application_name,
                "ArgoCD annotations enabled for broker-minted CRs"
            );
        }
        let writer =
            kaas_broker::topic_cr_writer::KubeTopicCRWriter::new(client.clone(), ns.clone())
                .with_argocd(argocd);
        broker.install_cr_writer(Arc::new(writer));
        info!("installed KubeTopicCRWriter for admin handlers");

        // ACL admin surface (gh #107 parity): CreateAcls / DeleteAcls
        // / DescribeAcls mutate KafkaUser.spec.authorization.acls.
        let acl_writer =
            kaas_broker::acl_cr_writer::KubeAclCRWriter::new(client.clone(), ns.clone());
        broker.install_acl_cr_writer(Arc::new(acl_writer));
        info!("installed KubeAclCRWriter for ACL admin handlers");

        // gh #252: SCRAM credential rotation (key 51) patches
        // KafkaUser.spec.authentication.scram; the operator
        // materialises credentials.json on reconcile.
        let user_writer =
            kaas_broker::user_cr_writer::KubeUserCRWriter::new(client.clone(), ns.clone());
        broker.install_user_cr_writer(Arc::new(user_writer));
        info!("installed KubeUserCRWriter for SCRAM credential admin");

        // Spawn the KafkaTopic CR watcher: feeds every
        // Apply/InitApply into `topics` so newly-created topics
        // become visible on the wire (Metadata), every Delete
        // removes them, and each mutation pokes the controller's
        // AssignmentLoop so new topics get partitions assigned on
        // the spot (gh #74) rather than on the next periodic tick.
        let topics_apply = topics.clone();
        let topics_delete = topics.clone();
        let topics_synced = topics.clone();
        // gh #224: per-topic in-flight guard for the migrate-annotation
        // driver so relist echoes don't stack concurrent drivers.
        let migrations_in_flight: Arc<parking_lot::Mutex<std::collections::HashSet<String>>> =
            Arc::new(parking_lot::Mutex::new(std::collections::HashSet::new()));
        let broker_migrate = broker.clone();
        let broker_delete = broker.clone();
        let engine_delete = engine.clone();
        let notify_apply = topic_notify.clone();
        let notify_delete = topic_notify.clone();
        let watch_cancel = cancel.clone();
        let watch_ns = ns;
        tokio::spawn(async move {
            if let Err(err) = kaas_k8s::kube_watchers::run_topic_watch(
                client,
                watch_ns,
                move |ev| {
                    let kaas_k8s::kube_watchers::TopicApply {
                        name,
                        partitions,
                        volume_assignments: volumes,
                        migrate_to,
                        topic_id,
                    } = ev;
                    let prev = topics_apply
                        .all()
                        .into_iter()
                        .find(|m| m.name == name)
                        .map(|m| m.partition_count);
                    topics_apply.insert(kaas_broker::TopicMeta {
                        name: name.to_string(),
                        partition_count: partitions,
                        topic_id: [0u8; 16],
                    });
                    // gh #221 phase 2: stash the reconciler-stamped
                    // placement — the engine resolves partition roots
                    // through the registry.
                    topics_apply.set_volume_assignments(name, volumes.clone());
                    // gh #241: and the incarnation, so the engine can
                    // refuse to open a directory the operator has yet
                    // to reclaim. Recorded even when absent — "CR seen,
                    // unstamped" is the state that must block.
                    topics_apply.set_topic_id(name, topic_id);
                    // gh #224: `kaas.rs/migrate-to-volume` annotation —
                    // drive this topic's partitions onto the target log
                    // dir, one at a time, through the same path the
                    // AlterReplicaLogDirs handler uses. Level-triggered
                    // and idempotent: already-placed partitions skip,
                    // non-led partitions are another broker's job, and
                    // an unknown target stops the pass (typo — surfaced
                    // in the broker log, not a crash loop).
                    if let Some(target) = migrate_to {
                        let target = target.to_string();
                        let name = name.to_string();
                        if migrations_in_flight.lock().insert(name.clone()) {
                            let broker = broker_migrate.clone();
                            let registry = topics_apply.clone();
                            let in_flight = migrations_in_flight.clone();
                            tokio::spawn(async move {
                                for p in 0..partitions {
                                    let placed = registry
                                        .volume_assignment(&name, p)
                                        .unwrap_or_else(|| "default".to_string());
                                    if placed == target {
                                        continue;
                                    }
                                    let code =
                                        kaas_broker::migrate_partition(&broker, &name, p, &target)
                                            .await;
                                    match code {
                                        0 => {
                                            // Rate limit: one partition per
                                            // pass interval, so a drain never
                                            // saturates the substrate.
                                            tokio::time::sleep(std::time::Duration::from_secs(3))
                                                .await;
                                        }
                                        57 => {
                                            warn!(
                                                topic = name.as_str(),
                                                target = target.as_str(),
                                                "migrate-to-volume: unknown log dir; stopping"
                                            );
                                            break;
                                        }
                                        _ => {} // not led here / transient — next echo retries
                                    }
                                }
                                in_flight.lock().remove(&name);
                            });
                        }
                    }
                    match prev {
                        None => {
                            notify_apply.notify(kaas_controller::AssignmentReason::TopicCreated)
                        }
                        Some(p) if p != partitions => {
                            notify_apply.notify(kaas_controller::AssignmentReason::TopicResized);
                        }
                        Some(_) => {} // re-list echo; nothing changed
                    }
                },
                move |name| {
                    topics_delete.remove(name);
                    // gh #219: drop the topic's partitions from the
                    // engine *without persisting*. Leaving it to the
                    // assignment recompute is too late on a
                    // delete→recreate (Streams' `application-reset`
                    // does exactly that): the recompute can coalesce
                    // both events into no ownership change at all,
                    // leaving the recreated topic served from the dead
                    // one's in-memory state — and a relinquish landing
                    // after the operator re-created the directory
                    // writes the dead incarnation's manifest into it.
                    // The spawn resolves in microseconds while the
                    // recreate's take-over is gated behind an
                    // assignment recompute + watch (~1 s), so the drop
                    // lands first with a wide margin.
                    let engine = engine_delete.clone();
                    let broker_purge = broker_delete.clone();
                    let topic = name.to_string();
                    tokio::spawn(async move {
                        let dropped = engine.abandon_topic(&topic).await;
                        if dropped > 0 {
                            info!(topic, dropped, "topic deleted; dropped open partitions");
                        }
                        // gh #240: a deleted topic's committed group
                        // offsets must go with it, the way Apache
                        // tombstones them out of `__consumer_offsets`.
                        // Left behind, a delete→recreate under the same
                        // name (Streams' `application-reset` every run)
                        // hands the new incarnation the dead one's
                        // offsets; if the new log is shorter, the group
                        // resumes past its end and idles forever with no
                        // error on either side. The Manager is absent in
                        // dev mode and for the brief window before the
                        // cluster runtime installs it — no coordinated
                        // groups exist then either.
                        if let Some(mgr) = broker_purge.coord_manager() {
                            let (purged, failed) = mgr.purge_topic_offsets(&topic);
                            if purged > 0 {
                                info!(
                                    topic,
                                    purged, "topic deleted; purged committed group offsets"
                                );
                            }
                            if !failed.is_empty() {
                                // Best-effort: the stale entries only
                                // bite a group that resumes on a
                                // recreated topic of the same name.
                                warn!(
                                    topic,
                                    groups = failed.join(","),
                                    "topic deleted; could not purge offsets for some groups"
                                );
                            }
                        }
                    });
                    notify_delete.notify(kaas_controller::AssignmentReason::TopicDeleted);
                },
                // gh #241: the first completed list makes the
                // registry's absences meaningful — before it, an
                // unknown topic means "we haven't looked yet" and the
                // engine must adopt whatever is on disk.
                move || topics_synced.mark_synced(),
                watch_cancel,
            )
            .await
            {
                warn!(%err, "topic watcher exited");
            }
        });
        info!("spawned KafkaTopic CR watcher");
    }

    // Client-facing port advertised for peer brokers (FindCoordinator
    // / Metadata) — the first listener's bind port.
    let client_port = cli
        .listeners
        .first()
        .and_then(|l| l.addr.rsplit(':').next())
        .and_then(|p| p.parse::<i32>().ok())
        .unwrap_or(9092);

    // Phase 5: bring up the consumer-group Manager + (in disk
    // mode) the Coordinator + AssignmentLoop + takeover drivers.
    // In cluster mode (MY_POD_NAME + kube client) this also wires
    // the multi-broker runtime: Lease election, heartbeats, peer
    // registry, and controller-only assignment writing.
    let cluster_rt = cluster::install(
        broker.clone(),
        topics.clone(),
        engine.clone(),
        cli.data_dir.clone(),
        cli.resolved_cluster_dir(),
        cli.broker_id,
        &cli.cluster_id,
        cancel.clone(),
        kube_client,
        client_port,
        topic_notify,
        cli.txn_num_slots,
    )?;

    // Spawn the credential / ACL reloader before the listeners go up
    // so the first served request sees the latest disk state.
    let _reload_task = spawn_reloader(auth.creds.clone(), auth.acls.clone());
    // JWKS fetch loops for oauth listeners (gh #42) — same slot in
    // the boot order and for the same reason: until the first fetch
    // lands, oauth clients are rejected rather than waved through.
    let _jwks_tasks = spawn_jwks_refreshers(&auth.oauth_validators);

    let dispatcher = build_dispatcher(
        broker.clone(),
        &cli.listeners,
        auth.engines.clone(),
        cli.auto_create_topics,
        cli.num_partitions,
        cli.max_message_bytes,
        auth.creds
            .clone()
            .map(|c| -> Arc<dyn kaas_auth::CredentialStore> { c }),
    );
    let listeners = parse_listeners(&cli.listeners, &auth.engines, &auth.principal_mapper)?;
    let server = Server::new(ServerConfigBuilder::new(listeners), Arc::new(dispatcher));

    let serve_cancel = cancel.clone();
    let serve = tokio::spawn(async move { server.serve(serve_cancel).await });

    // Spawn the axum /healthz + /readyz server on KAAS_HEALTH_ADDR
    // (chart default `:8080`). The chart's readinessProbe hits
    // http://<pod>:8080/readyz — without this, pods stay 0/1
    // Ready forever.
    let health_addr = std::env::var("KAAS_HEALTH_ADDR").unwrap_or_else(|_| ":8080".into());
    let health_addr = if let Some(stripped) = health_addr.strip_prefix(':') {
        format!("0.0.0.0:{stripped}")
    } else {
        health_addr
    };
    let health_cfg = kaas_observability::health::HealthConfig {
        broker_id: format!("kaas-{}", cli.broker_id),
        listeners: cli.listeners.iter().map(|l| l.name.clone()).collect(),
        // Live runtime fields (controller identity, partitions_led,
        // heartbeat/assignment ages). Reads broker.coordinator() per
        // request, so wiring order vs cluster bring-up doesn't
        // matter — dev mode simply renders the zero-value shape.
        source: Some(Arc::new(BrokerRuntimeState(broker.clone()))),
        // gh #208: gate /readyz on takeover completion whenever a data
        // dir is configured (i.e. a Coordinator exists). Dev/in-memory
        // mode (no data dir) skips the serving gate.
        cluster_mode: cli.data_dir.is_some(),
    };

    // gh #211: run the health server on its OWN thread + current-thread
    // runtime, never the main tokio runtime it reports on. A wedged
    // main runtime (gh #209/#210) must still answer /readyz — and it
    // answers "unready", because the main-liveness tick has gone stale.
    let health_cancel = cancel.clone();
    let health_thread = std::thread::Builder::new()
        .name("kaas-health".to_owned())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(err) => {
                    warn!(%err, "health runtime build failed; /readyz + /healthz DOWN");
                    return;
                }
            };
            rt.block_on(async move {
                let router = kaas_observability::health::health_router(health_cfg);
                match tokio::net::TcpListener::bind(&health_addr).await {
                    Ok(listener) => {
                        info!(addr = %health_addr, "health server listening (dedicated runtime)");
                        let _ = axum::serve(listener, router)
                            .with_graceful_shutdown(async move { health_cancel.cancelled().await })
                            .await;
                    }
                    Err(err) => warn!(%err, %health_addr, "health server bind failed"),
                }
            });
        });
    if let Err(err) = health_thread {
        warn!(%err, "health server thread spawn failed; /readyz + /healthz DOWN");
    }

    // gh #208: main-runtime liveness tick. Bumps a monotonic timestamp
    // every second FROM THE MAIN RUNTIME; if every main worker wedges,
    // this task stops running and /readyz (read off the dedicated
    // health runtime) reports unready. Spawned unconditionally so dev
    // mode is live too.
    {
        let tick_cancel = cancel.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    () = tick_cancel.cancelled() => break,
                    _ = tick.tick() => kaas_observability::record_main_tick(),
                }
            }
        });
    }

    // gh #250: retention. The cleaner existed, was unit-tested, and was
    // never constructed — so `retention.ms` / `retention.bytes` were
    // accepted by the CRD, echoed back by DescribeConfigs, and enforced
    // by nothing. Only DeleteRecords and topic deletion reclaimed disk.
    //
    // Disk engines only (there is nothing to reclaim in the dev-mode
    // in-memory engine), and leader-gated: a pass unlinks segment files
    // on the shared volume, so a non-leader running it is an NFS
    // substrate rule 3 violation and would silly-rename files the real
    // leader holds open (gh #76).
    if let Some(disk) = disk_engine.clone() {
        let interval_secs = cli.retention_check_interval_secs;
        if interval_secs == 0 {
            info!("KAAS_RETENTION_CHECK_INTERVAL=0 — retention sweep disabled");
        } else {
            // The cluster default a topic falls back to, and it must
            // equal what DescribeConfigs advertises as DEFAULT_CONFIG —
            // Apache enforces its 7-day `retention.ms` default, so a
            // topic with no explicit setting ages out there. kaas
            // advertised the same number and enforced nothing, which is
            // the same class of lie gh #250 was about, one level up.
            let defaults = kaas_storage::RetentionPolicy {
                retention_ms: Some(kaas_broker::topic_config_defaults::default_retention_ms()),
                retention_bytes: None, // Apache: -1, unlimited
            };
            let policy = Arc::new(kaas_storage::TopicConfigPolicySource::new(
                disk.clone(),
                defaults,
            ));
            let policy_for_cfg = policy.clone();
            let cleaner = kaas_storage::RetentionCleaner::with_policy_source(disk.clone(), policy)
                .with_ownership(Arc::new(CoordinatorOwnership(broker.clone())))
                .with_partition_config(Arc::new(move |topic, partition, base| {
                    policy_for_cfg.partition_config_for(topic, partition, base)
                }));
            let cleaner_cancel = cancel.clone();
            info!(interval_secs, "retention cleaner started");
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                // The first tick fires immediately; skip it so a
                // rolling restart doesn't have every broker sweep
                // while it is still taking over partitions.
                tick.tick().await;
                loop {
                    tokio::select! {
                        () = cleaner_cancel.cancelled() => break,
                        _ = tick.tick() => match cleaner.run_once().await {
                            Ok(0) => {}
                            Ok(n) => info!(partitions = n, "retention pass reclaimed"),
                            // Never fatal: a failed pass retries on the
                            // next tick, and the alternative is a broker
                            // that dies because a disk got full.
                            Err(err) => warn!(%err, "retention pass failed"),
                        },
                    }
                }
                info!("retention cleaner stopped");
            });
        }
    }

    // Flip the base /readyz gate once the accept loop is up. Readiness
    // now ALSO requires main-runtime liveness and (in cluster mode)
    // takeover completion — see health::compute_ready. If a listener
    // fails to bind we never call this and the pod stays unready.
    kaas_observability::set_ready(true);

    wait_for_shutdown_signal().await?;
    // gh #249: announce the drain BEFORE anything is torn down. The
    // heartbeat carries it on its next tick (~1 s), so the controller
    // moves this broker's partitions and group coordinatorships off
    // proactively instead of waiting out a heartbeat timeout after
    // the process is already gone. The broker keeps serving normally
    // meanwhile — draining is "I am leaving", not "I am unhealthy".
    kaas_observability::mark_draining();
    info!("shutdown signal received; cancelling listeners");
    cancel.cancel();
    match serve.await {
        Ok(Ok(())) => {}
        Ok(Err(err)) => warn!(%err, "server task ended with error"),
        Err(join_err) => warn!(%join_err, "server task join error"),
    }

    // Abort cluster background tasks before draining storage so
    // their in-flight writes don't race the drain.
    for handle in cluster_rt.tasks {
        handle.abort();
    }

    info!("draining storage engine");
    if let Err(err) = engine.drain().await {
        warn!(%err, "engine drain reported error");
    }

    // Flush pending OTLP pushes + span exports before the process
    // dies. Best-effort — errors go to stderr but don't fail exit.
    if let Err(err) = providers.shutdown() {
        warn!(%err, "observability shutdown reported error");
    }
    obs_cancel.cancel();

    info!("kaas exited cleanly");
    Ok(())
}

struct AuthSetup {
    authorizer: Arc<dyn Authorizer>,
    quotas: Arc<dyn QuotaChecker>,
    engines: Arc<PerListenerAuthEngine>,
    creds: Option<Arc<CredentialLoader>>,
    acls: Option<Arc<AclEngine>>,
    principal_mapper: Arc<PrincipalMapper>,
    /// One validator per oauth listener (gh #42); each needs a JWKS
    /// fetch loop spawned once the runtime is up.
    oauth_validators: Vec<Arc<OauthValidator>>,
}

fn build_auth(cli: &Cli) -> Result<AuthSetup> {
    let mapper = Arc::new(
        PrincipalMapper::parse(&cli.ssl_principal_mapping_rules)
            .context("parsing KAAS_SSL_PRINCIPAL_MAPPING_RULES")?,
    );

    if cli.auth_disabled {
        if cli.require_sasl {
            warn!(
                "KAAS_REQUIRE_SASL=true is ignored because KAAS_AUTH_DISABLED=true — \
                 the explicit off-switch outranks the requirement (gh #251)"
            );
        }
        info!("auth disabled — using AllowAllAuthorizer + NoQuotaChecker on every listener");
        let allow_all: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
        let engines = Arc::new(PerListenerAuthEngine::new(allow_all));
        return Ok(AuthSetup {
            authorizer: Arc::new(AllowAllAuthorizer),
            quotas: Arc::new(NoQuotaChecker),
            engines,
            creds: None,
            acls: None,
            principal_mapper: mapper,
            oauth_validators: Vec::new(),
        });
    }

    let cluster_dir = cli
        .resolved_cluster_dir()
        .unwrap_or_else(|| PathBuf::from("/data/__cluster"));
    let creds_path = cluster_dir.join("credentials.json");
    let acls_path = cluster_dir.join("acls.json");

    let creds = Arc::new(CredentialLoader::new(creds_path.clone()));
    if let Err(err) = creds.reload() {
        warn!(%err, path = %creds_path.display(), "auth: initial credentials reload failed (continuing)");
    }
    let acls = Arc::new(AclEngine::new(acls_path.clone()));
    if let Err(err) = acls.reload() {
        warn!(%err, path = %acls_path.display(), "auth: initial acls reload failed (continuing)");
    }

    // Per-listener engine map. Anonymous listeners (auth_type unset
    // or "none") use AllowAllAuthEngine; scram/plain/mtls listeners
    // use RealAuthEngine wrapped around the credential store.
    //
    // gh #251 (re-port of the Go broker's SKAFKA_REQUIRE_SASL): with
    // KAAS_REQUIRE_SASL=true, anonymous listeners get the real engine
    // too — its `requires_pre_auth` arms the dispatch gate (gh #124),
    // so every connection must finish SASL before any non-handshake
    // API. This env was chart-emitted and read by nothing from the
    // Rust cutover until now.
    let allow_all_engine: Arc<dyn AuthEngine> = Arc::new(AllowAllAuthEngine);
    let real_engine: Arc<dyn AuthEngine> =
        Arc::new(RealAuthEngine::new(creds.clone(), mapper.clone()));
    let anonymous_engine: Arc<dyn AuthEngine> = if cli.require_sasl {
        real_engine.clone()
    } else {
        allow_all_engine.clone()
    };
    // The fallback for listeners absent from the map follows the
    // same rule — a listener the map somehow misses must not dodge
    // the requirement.
    let mut engines_map = PerListenerAuthEngine::new(anonymous_engine.clone());
    let mut oauth_validators: Vec<Arc<OauthValidator>> = Vec::new();
    for lc in &cli.listeners {
        let engine_for_listener: Arc<dyn AuthEngine> =
            match lc.authentication_type.as_deref().unwrap_or("none") {
                "none" => {
                    if cli.require_sasl {
                        info!(
                            listener = lc.name.as_str(),
                            "KAAS_REQUIRE_SASL: anonymous listener now requires SASL"
                        );
                    }
                    anonymous_engine.clone()
                }
                "scram-sha-512" | "plain" | "mtls" => real_engine.clone(),
                "oauth" => {
                    // Parse guarantees `oauth` is Some for this type.
                    let Some(oc) = lc.oauth.clone() else {
                        anyhow::bail!(
                            "listener {:?}: authentication.type oauth without oauth config",
                            lc.name
                        );
                    };
                    if lc.tls.is_none() {
                        // The authenticate handler refuses OAUTHBEARER
                        // over plaintext, so this listener would be
                        // un-authenticatable — say so at boot.
                        warn!(
                            listener = lc.name.as_str(),
                            "oauth listener has tls: false — OAUTHBEARER is rejected over \
                             plaintext (bearer tokens are reusable credentials); clients will \
                             not be able to authenticate"
                        );
                    }
                    let validator = Arc::new(OauthValidator::new(kaas_auth::OauthConfig {
                        valid_issuer_uri: oc.valid_issuer_uri,
                        jwks_endpoint_uri: oc.jwks_endpoint_uri,
                        user_name_claim: oc.user_name_claim,
                        fallback_user_name_claim: oc.fallback_user_name_claim,
                        check_audience: oc.check_audience,
                        client_id: oc.client_id,
                        jwks_refresh_seconds: oc.jwks_refresh_seconds,
                        max_seconds_without_reauthentication: oc
                            .max_seconds_without_reauthentication,
                    }));
                    info!(
                        listener = lc.name.as_str(),
                        issuer = validator.config().valid_issuer_uri.as_str(),
                        jwks = validator.config().jwks_endpoint_uri.as_str(),
                        "oauth listener configured (gh #42)"
                    );
                    oauth_validators.push(validator.clone());
                    Arc::new(OauthEngine::new(validator))
                }
                other => {
                    warn!(
                    listener = lc.name.as_str(),
                    authentication_type = other,
                    require_sasl = cli.require_sasl,
                    "unknown authentication_type — falling back to the anonymous-listener engine"
                );
                    anonymous_engine.clone()
                }
            };
        engines_map.insert(lc.name.clone(), engine_for_listener);
    }
    let engines = Arc::new(engines_map);

    // Authorizer: AclEngine if "simple", AllowAll otherwise. Wrap in
    // SuperUserAuthorizer when super_users is non-empty.
    let base_authorizer: Arc<dyn Authorizer> = match cli.authorization_type.as_str() {
        "simple" => acls.clone(),
        "" => Arc::new(AllowAllAuthorizer),
        other => {
            warn!(
                authorization_type = other,
                "unknown KAAS_AUTHORIZATION_TYPE — falling back to AllowAll"
            );
            Arc::new(AllowAllAuthorizer)
        }
    };
    let authorizer: Arc<dyn Authorizer> = if cli.super_users.is_empty() {
        base_authorizer
    } else {
        Arc::new(SuperUserAuthorizer::new(
            cli.super_users.clone(),
            base_authorizer,
        ))
    };

    // Quotas: backed by the credential store so per-user limits in
    // credentials.json take effect.
    let quotas: Arc<dyn QuotaChecker> = Arc::new(QuotaEnforcer::new(creds.clone()));

    Ok(AuthSetup {
        authorizer,
        quotas,
        engines,
        creds: Some(creds),
        acls: Some(acls),
        principal_mapper: mapper,
        oauth_validators,
    })
}

fn spawn_reloader(
    creds: Option<Arc<CredentialLoader>>,
    acls: Option<Arc<AclEngine>>,
) -> Option<tokio::task::JoinHandle<()>> {
    let creds = creds?;
    let acls = acls?;
    Some(tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(RELOAD_INTERVAL_SECS));
        // First tick fires immediately; skip it since we reloaded at
        // boot.
        tick.tick().await;
        loop {
            tick.tick().await;
            if let Err(err) = creds.reload() {
                warn!(%err, "auth: credentials hot-reload failed");
            }
            if let Err(err) = acls.reload() {
                warn!(%err, "auth: acls hot-reload failed");
            }
        }
    }))
}

/// One JWKS fetch loop per oauth listener (gh #42). The validator
/// itself is pure-sync (it sits on the SASL hot path); this loop is
/// the only thing that does network I/O: fetch the JWKS document,
/// push it in via `install_jwks`, repeat every `jwksRefreshSeconds`.
/// Failures keep the previous key set — the failure mode of an
/// unreachable issuer is stale keys, never no validation. Two extra
/// wrinkles: until the first successful fetch we retry on a short
/// interval (boot race against the issuer), and a kid-miss recorded
/// by the validator (`take_refresh_hint`) pulls the next fetch
/// forward to ride out issuer key rotation, rate-limited so a flood
/// of bad tokens can't turn the broker into a JWKS-hammering client.
fn spawn_jwks_refreshers(validators: &[Arc<OauthValidator>]) -> Vec<tokio::task::JoinHandle<()>> {
    const BOOT_RETRY_SECS: u64 = 10;
    const HINT_POLL_SECS: u64 = 2;
    const HINT_MIN_PAUSE_SECS: u64 = 10;

    validators
        .iter()
        .cloned()
        .map(|v| {
            tokio::spawn(async move {
                let endpoint = v.config().jwks_endpoint_uri.clone();
                // Defence-in-depth (gh #42 review): a plaintext JWKS
                // endpoint lets an on-path attacker swap in signing
                // keys and mint tokens. HTTPS is the only safe choice;
                // http:// is accepted (Apache does too, for in-cluster
                // plaintext issuers) but called out loudly so a
                // misconfiguration is visible in the log rather than
                // silent.
                if !endpoint.trim_start().to_ascii_lowercase().starts_with("https://") {
                    warn!(
                        jwks = endpoint.as_str(),
                        "jwks: endpoint is not https — signing keys are fetched in plaintext; \
                         an on-path attacker could inject keys and forge tokens. Use an https \
                         JWKS URL in production."
                    );
                }
                let refresh = std::time::Duration::from_secs(v.config().jwks_refresh_seconds);
                let client = match reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(10))
                    .build()
                {
                    Ok(c) => c,
                    Err(err) => {
                        error!(%err, jwks = endpoint.as_str(), "jwks: HTTP client build failed — oauth listener will reject all tokens");
                        return;
                    }
                };
                loop {
                    let fetched = match client.get(&endpoint).send().await {
                        Ok(resp) => match resp.error_for_status() {
                            Ok(resp) => match resp.text().await {
                                Ok(body) => match v.install_jwks(&body) {
                                    Ok(n) => {
                                        info!(jwks = endpoint.as_str(), keys = n, "jwks: refreshed");
                                        true
                                    }
                                    Err(err) => {
                                        warn!(%err, jwks = endpoint.as_str(), "jwks: parse failed — keeping previous keys");
                                        false
                                    }
                                },
                                Err(err) => {
                                    warn!(%err, jwks = endpoint.as_str(), "jwks: body read failed");
                                    false
                                }
                            },
                            Err(err) => {
                                warn!(%err, jwks = endpoint.as_str(), "jwks: fetch returned error status");
                                false
                            }
                        },
                        Err(err) => {
                            warn!(%err, jwks = endpoint.as_str(), "jwks: fetch failed");
                            false
                        }
                    };

                    let wait = if fetched || v.has_keys() {
                        refresh
                    } else {
                        std::time::Duration::from_secs(BOOT_RETRY_SECS)
                    };
                    let fetch_done = tokio::time::Instant::now();
                    let deadline = fetch_done + wait;
                    loop {
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            break;
                        }
                        let poll = std::time::Duration::from_secs(HINT_POLL_SECS).min(deadline - now);
                        tokio::time::sleep(poll).await;
                        // kid-miss hint: refetch early, but never more
                        // than once per HINT_MIN_PAUSE_SECS.
                        if tokio::time::Instant::now() - fetch_done
                            >= std::time::Duration::from_secs(HINT_MIN_PAUSE_SECS)
                            && v.take_refresh_hint()
                        {
                            break;
                        }
                    }
                }
            })
        })
        .collect()
}

/// The engine plus, when it is disk-backed, the concrete handle.
type BuiltEngine = (Arc<dyn StorageEngine>, Option<Arc<DiskStorageEngine>>);

/// Returns the engine plus, when it is disk-backed, the concrete
/// handle. The retention cleaner needs the concrete type (it walks
/// partitions and reads topic config off the volume) and there is
/// nothing to clean in the dev-mode in-memory engine.
fn build_engine(
    data_dir: Option<PathBuf>,
    flush_interval_messages: i64,
    fsync_max_latency_ms: u64,
    log_dirs: Vec<kaas_storage::LogDirInfo>,
    placement: Arc<TopicRegistry>,
) -> Result<BuiltEngine> {
    match data_dir {
        Some(dir) => {
            std::fs::create_dir_all(&dir).context("creating KAAS_DATA_DIR")?;
            // KAAS_FLUSH_INTERVAL_MESSAGES is THE durability vs
            // throughput dial (mirrors Apache's
            // log.flush.interval.messages; default 1 = honest
            // acks=all). It was parsed by the CLI but dropped on the
            // floor here, silently running every deployment at
            // flush-per-batch — found in phase 9 A.3 as a 2.8×
            // throughput gap against the v0.1 flavor on identical
            // values (gh #152).
            // gh #256 (re-port of gh #95): the fsync-watchdog deadline.
            // 0 disables — an operator on a substrate slower than any
            // sane deadline turns the watchdog off instead of taking
            // spurious LEADER_NOT_AVAILABLE on every slow scrub.
            let fsync_max_latency = if fsync_max_latency_ms == 0 {
                None
            } else {
                Some(std::time::Duration::from_millis(fsync_max_latency_ms))
            };
            let cfg = PartitionConfig {
                flush_interval_messages,
                fsync_max_latency,
                ..PartitionConfig::default()
            };
            for d in &log_dirs {
                std::fs::create_dir_all(&d.path)
                    .with_context(|| format!("creating log dir {}", d.path.display()))?;
            }
            let engine =
                DiskStorageEngine::new(Arc::new(RealFs), dir, cfg).with_extra_log_dirs(log_dirs);
            engine.set_placement_resolver(placement.clone());
            // gh #241: same registry, second role — the incarnation
            // source for the identity gate on partition open.
            engine.set_identity_resolver(placement);
            let disk = Arc::new(engine);
            Ok((disk.clone(), Some(disk)))
        }
        None => Ok((Arc::new(MemoryStorage::new()), None)),
    }
}

fn build_dispatcher(
    broker: Arc<Broker>,
    listeners: &[ListenerEntry],
    engines: Arc<PerListenerAuthEngine>,
    auto_create_topics: bool,
    num_partitions: i32,
    max_message_bytes: usize,
    creds: Option<Arc<dyn kaas_auth::CredentialStore>>,
) -> Dispatcher {
    let mut d = Dispatcher::new();
    d.register(
        0,
        3,
        9,
        Arc::new(ProduceHandler::new(broker.clone()).with_max_message_bytes(max_message_bytes)),
    );
    d.register(1, 4, 12, Arc::new(FetchHandler::new(broker.clone())));
    d.register(2, 1, 7, Arc::new(ListOffsetsHandler::new(broker.clone())));
    d.register(
        3,
        1,
        10,
        Arc::new(
            MetadataHandler::new(broker.clone(), listeners)
                .with_auto_create(auto_create_topics, num_partitions),
        ),
    );
    // Phase 5 consumer-group surface (keys 8-16, 42, 47).
    d.register(8, 2, 8, Arc::new(OffsetCommitHandler::new(broker.clone())));
    d.register(9, 1, 8, Arc::new(OffsetFetchHandler::new(broker.clone())));
    d.register(
        10,
        0,
        4,
        Arc::new(FindCoordinatorHandler::new(broker.clone())),
    );
    d.register(11, 2, 9, Arc::new(JoinGroupHandler::new(broker.clone())));
    d.register(12, 0, 4, Arc::new(HeartbeatHandler::new(broker.clone())));
    d.register(13, 0, 5, Arc::new(LeaveGroupHandler::new(broker.clone())));
    d.register(14, 0, 5, Arc::new(SyncGroupHandler::new(broker.clone())));
    d.register(
        15,
        0,
        5,
        Arc::new(DescribeGroupsHandler::new(broker.clone())),
    );
    d.register(16, 0, 4, Arc::new(ListGroupsHandler::new(broker.clone())));
    // Admin surface (gh #152).
    d.register(20, 0, 5, Arc::new(DeleteTopicsHandler::new(broker.clone())));
    d.register(
        21,
        0,
        2,
        Arc::new(DeleteRecordsHandler::new(broker.clone())),
    );
    d.register(29, 0, 3, Arc::new(DescribeAclsHandler::new(broker.clone())));
    d.register(30, 0, 3, Arc::new(CreateAclsHandler::new(broker.clone())));
    d.register(31, 0, 3, Arc::new(DeleteAclsHandler::new(broker.clone())));
    d.register(
        34,
        0,
        2,
        Arc::new(AlterReplicaLogDirsHandler::new(broker.clone())),
    );
    d.register(
        35,
        0,
        4,
        Arc::new(DescribeLogDirsHandler::new(broker.clone())),
    );
    // KIP-700: what AdminClient.describeCluster() calls. Shares the
    // Metadata handler's listener table so both advertise the same
    // endpoints (gh #125).
    d.register(
        60,
        0,
        2,
        Arc::new(DescribeClusterHandler::new(broker.clone(), listeners)),
    );
    d.register(
        17,
        0,
        1,
        Arc::new(SaslHandshakeHandler::new(engines.clone())),
    );
    d.register(18, 0, 4, Arc::new(ApiVersionsHandler::new()));
    d.register(
        22,
        0,
        4,
        Arc::new(InitProducerIdHandler::new(broker.clone())),
    );
    // Phase 6 transactional surface (keys 24–28).
    d.register(
        24,
        0,
        3,
        Arc::new(AddPartitionsToTxnHandler::new(broker.clone())),
    );
    d.register(
        25,
        0,
        3,
        Arc::new(AddOffsetsToTxnHandler::new(broker.clone())),
    );
    d.register(26, 0, 3, Arc::new(EndTxnHandler::new(broker.clone())));
    d.register(
        27,
        0,
        1,
        Arc::new(WriteTxnMarkersHandler::new(broker.clone())),
    );
    d.register(
        28,
        0,
        3,
        Arc::new(TxnOffsetCommitHandler::new(broker.clone())),
    );
    d.register(
        36,
        0,
        2,
        Arc::new(SaslAuthenticateHandler::new(engines.clone())),
    );
    d.register(42, 0, 2, Arc::new(DeleteGroupsHandler::new(broker.clone())));
    // Phase 7 admin handlers (workstream D). All five take an
    // Arc<Broker>; the broker resolves the optional TopicCRWriter /
    // QuotaEnforcer slots per-call so dev-mode (slots unset) maps
    // cleanly to the documented wire error codes (31, 35).
    d.register(
        32,
        0,
        4,
        Arc::new(DescribeConfigsHandler::new(broker.clone())),
    );
    d.register(19, 0, 7, Arc::new(CreateTopicsHandler::new(broker.clone())));
    d.register(
        37,
        0,
        3,
        Arc::new(CreatePartitionsHandler::new(broker.clone())),
    );
    d.register(
        44,
        0,
        1,
        Arc::new(IncrementalAlterConfigsHandler::new(broker.clone())),
    );
    d.register(
        48,
        0,
        1,
        Arc::new(DescribeClientQuotasHandler::new(broker.clone())),
    );
    d.register(
        49,
        0,
        1,
        Arc::new(AlterClientQuotasHandler::new(broker.clone())),
    );
    // KIP-554 SCRAM credential admin (gh #252): describe answers from
    // the live credential store; alter rotates via the KafkaUser CR.
    d.register(
        50,
        0,
        0,
        Arc::new(DescribeUserScramCredentialsHandler::new(
            broker.clone(),
            creds,
        )),
    );
    d.register(
        51,
        0,
        0,
        Arc::new(AlterUserScramCredentialsHandler::new(broker.clone())),
    );
    d.register(47, 0, 0, Arc::new(OffsetDeleteHandler::new(broker)));
    d.set_auth(engines);
    d
}

fn parse_listeners(
    entries: &[ListenerEntry],
    engines: &Arc<PerListenerAuthEngine>,
    mapper: &Arc<PrincipalMapper>,
) -> Result<Vec<ListenerConfig>> {
    entries
        .iter()
        .map(|e| {
            let addr = e
                .addr
                .parse::<std::net::SocketAddr>()
                .with_context(|| format!("parsing listener addr {:?} for {}", e.addr, e.name))?;
            let tls_config = match &e.tls {
                None => None,
                Some(tc) => {
                    Some(Arc::new(load_tls(tc).with_context(|| {
                        format!("loading TLS for listener {}", e.name)
                    })?))
                }
            };
            let mtls = if matches!(e.authentication_type.as_deref(), Some("mtls")) {
                let engine = engines.for_listener(&e.name);
                Some(MtlsConfig {
                    engine,
                    mapper: mapper.clone(),
                })
            } else {
                None
            };
            Ok(ListenerConfig {
                name: e.name.clone(),
                addr,
                pre_bound: None,
                tls_config,
                mtls,
            })
        })
        .collect()
}

fn load_tls(cfg: &CliTlsConfig) -> Result<RustlsServerConfig> {
    let certs = load_certs(&cfg.cert_path)?;
    let key = load_private_key(&cfg.key_path)?;
    let builder = RustlsServerConfig::builder();
    let server = if let Some(ca_path) = &cfg.client_ca_path {
        let mut roots = RootCertStore::empty();
        for cert in load_certs(ca_path)? {
            roots
                .add(cert)
                .context("installing client-CA cert into trust store")?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .context("building client cert verifier")?;
        builder.with_client_cert_verifier(verifier)
    } else {
        builder.with_no_client_auth()
    };
    server
        .with_single_cert(certs, key)
        .context("rustls server config with cert + key")
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening cert file {}", path.display()))?;
    let mut r = std::io::BufReader::new(f);
    let mut out = Vec::new();
    for cert in rustls_pemfile::certs(&mut r) {
        out.push(cert.with_context(|| format!("parsing cert in {}", path.display()))?);
    }
    if out.is_empty() {
        anyhow::bail!("no PEM certificates found in {}", path.display());
    }
    Ok(out)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening key file {}", path.display()))?;
    let mut r = std::io::BufReader::new(f);
    let key = rustls_pemfile::private_key(&mut r)
        .with_context(|| format!("parsing private key in {}", path.display()))?
        .ok_or_else(|| anyhow::anyhow!("no private key in {}", path.display()))?;
    Ok(key)
}

async fn wait_for_shutdown_signal() -> Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut int = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    tokio::select! {
        _ = term.recv() => info!("SIGTERM received"),
        _ = int.recv()  => info!("SIGINT received"),
    }
    Ok(())
}

/// Leadership gate for the retention cleaner (gh #250).
///
/// Re-reads `broker.coordinator()` on every call for the same reason
/// [`BrokerRuntimeState`] does: the cleaner is spawned before the
/// cluster runtime installs a coordinator. No coordinator means dev
/// mode or single-broker, where this broker leads everything it has
/// open — so the answer is `true` and retention keeps working.
///
/// In cluster mode the gate answers `false` for everything until the
/// first `assignment.json` load, which delays the first sweep by one
/// poll rather than skipping it. That is the safe direction: the cost
/// of waiting is disk, the cost of guessing wrong is deleting a
/// partition another broker leads.
struct CoordinatorOwnership(Arc<Broker>);

impl kaas_storage::OwnershipSource for CoordinatorOwnership {
    fn owns_partition(&self, topic: &str, partition: i32) -> bool {
        match self.0.coordinator() {
            Some(c) => c.owns(topic, partition),
            None => true,
        }
    }
}

/// `/healthz` runtime-state adapter over the broker's cluster
/// [`Coordinator`](kaas_broker::Coordinator). Every accessor re-reads
/// `broker.coordinator()` so the health server can be spawned before
/// the cluster runtime installs one (dev mode never does — the
/// handler then renders the documented zero-value shape, same as the
/// v0.1 broker in local mode).
struct BrokerRuntimeState(Arc<Broker>);

impl BrokerRuntimeState {
    fn snapshot(&self) -> Option<std::sync::Arc<kaas_broker::Assignment>> {
        self.0.coordinator().and_then(|c| c.snapshot())
    }
}

impl kaas_observability::health::RuntimeState for BrokerRuntimeState {
    fn is_controller(&self) -> bool {
        let Some(c) = self.0.coordinator() else {
            return false;
        };
        matches!(c.snapshot(), Some(a) if a.controller == c.self_id())
    }
    fn controller_id(&self) -> String {
        self.snapshot()
            .map(|a| a.controller.clone())
            .unwrap_or_default()
    }
    fn controller_epoch(&self) -> i64 {
        self.snapshot().map_or(0, |a| a.controller_epoch)
    }
    fn heartbeat_rtt_ms(&self) -> i64 {
        // Not measured on the Rust client yet; renders as JSON null.
        -1
    }
    fn heartbeat_age_ms(&self) -> i64 {
        self.0
            .coordinator()
            .and_then(|c| c.last_heartbeat())
            .map_or(-1, |t| {
                i64::try_from(t.elapsed().as_millis()).unwrap_or(i64::MAX)
            })
    }
    fn assignment_version(&self) -> u64 {
        self.snapshot()
            .map_or(0, |a| u64::try_from(a.assignment_version).unwrap_or(0))
    }
    fn assignment_age_ms(&self) -> i64 {
        let Some(a) = self.snapshot() else { return -1 };
        chrono::DateTime::parse_from_rfc3339(&a.generated_at)
            .map(|t| (chrono::Utc::now() - t.with_timezone(&chrono::Utc)).num_milliseconds())
            .unwrap_or(-1)
    }
    fn partitions_led(&self) -> i32 {
        // gh #208: "led" now means *actually taken over* — assigned to
        // us AND open in the engine — not merely assigned. Diverges
        // from `partitions_assigned` while a takeover is mid-recovery.
        let Some(c) = self.0.coordinator() else {
            return 0;
        };
        let Some(a) = c.snapshot() else { return 0 };
        let open: std::collections::HashSet<(String, i32)> =
            self.0.engine.open_partition_keys().into_iter().collect();
        i32::try_from(
            a.partitions
                .iter()
                .filter(|p| p.broker == c.self_id())
                .filter(|p| open.contains(&(p.topic.clone(), p.partition)))
                .count(),
        )
        .unwrap_or(i32::MAX)
    }
    fn partitions_assigned(&self) -> i32 {
        let Some(c) = self.0.coordinator() else {
            return 0;
        };
        let Some(a) = c.snapshot() else { return 0 };
        i32::try_from(
            a.partitions
                .iter()
                .filter(|p| p.broker == c.self_id())
                .count(),
        )
        .unwrap_or(i32::MAX)
    }
    fn partitions_recovering(&self) -> i32 {
        // Assigned but not yet taken over.
        (self.partitions_assigned() - self.partitions_led()).max(0)
    }
    fn storage_stalled(&self) -> bool {
        // gh #168: any open partition whose committer is poisoned by a
        // fsync failure. Report-only — a stalled log dir doesn't gate
        // readiness (Apache serves the healthy dirs too), and the
        // gh #168 re-arm probe clears it when the substrate recovers.
        self.0.engine.stalled_partitions() > 0
    }
    fn serving(&self) -> bool {
        // gh #208: takeover of every assigned partition is complete.
        match self.0.coordinator() {
            Some(c) => kaas_broker::is_serving(&c, self.0.engine.as_ref()),
            None => true, // dev / no-cluster: nothing to take over
        }
    }
}
