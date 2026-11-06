//! gh #329: the operator's leader lease and its shutdown, on the real binary.
//!
//! The operator talks to a fake API server on loopback, and nothing else: the
//! child's environment is cleared, so it sees only the kubeconfig written here.
//! Don't relax that. The ambient kubeconfig on a developer's machine may be a live
//! cluster's admin, and `Client::try_default` will use it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs::File;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::process::{Child, Command, ExitStatus};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::Router;
use serde_json::{json, Value};

const NS: &str = "leader-lease-test";
const LEASES: &str = "/apis/coordination.k8s.io/v1/namespaces/leader-lease-test/leases";
const LEASE_NAME: &str = "kaas-operator-leader";
/// Past any test's end, so a lease stamped with it stays fresh without a clock.
const FAR_FUTURE: &str = "2099-01-01T00:00:00.000000Z";

/// The parts of an API server the operator uses: one Lease with
/// resourceVersion-conditional writes, and lists and watches of the CRDs.
#[derive(Default)]
struct FakeApi {
    lease: Mutex<Option<Value>>,
    version: AtomicU64,
    /// Every write of the Lease that landed, in order.
    lease_writes: Mutex<Vec<Value>>,
    /// Any request for a kaas.rs resource: only a leader's reconcilers and sweep
    /// make them.
    crd_requests: AtomicUsize,
    topics: Mutex<Vec<Value>>,
    /// Status patches that arrived. They never get an answer: a stuck reconcile.
    status_patches: AtomicUsize,
}

fn status(code: StatusCode, reason: &str) -> Response {
    let body = json!({
        "apiVersion": "v1",
        "kind": "Status",
        "status": "Failure",
        "reason": reason,
        "message": reason,
        "code": code.as_u16(),
    });
    (code, axum::Json(body)).into_response()
}

impl FakeApi {
    fn store_lease(&self, mut lease: Value) -> Value {
        let version = self.version.fetch_add(1, Ordering::SeqCst) + 1;
        lease["metadata"]["resourceVersion"] = json!(version.to_string());
        *self.lease.lock().unwrap() = Some(lease.clone());
        self.lease_writes.lock().unwrap().push(lease.clone());
        lease
    }

    fn holder(&self) -> Option<String> {
        let lease = self.lease.lock().unwrap();
        let holder = lease.as_ref()?["spec"]["holderIdentity"].as_str()?;
        Some(holder.to_owned())
    }

    /// Another candidate holds the Lease, freshly.
    fn set_holder(&self, holder: &str) {
        self.store_lease(json!({
            "apiVersion": "coordination.k8s.io/v1",
            "kind": "Lease",
            "metadata": {"name": LEASE_NAME, "namespace": NS},
            "spec": {
                "holderIdentity": holder,
                "leaseDurationSeconds": 15,
                "acquireTime": FAR_FUTURE,
                "renewTime": FAR_FUTURE,
                "leaseTransitions": 7,
            },
        }));
    }

    fn renewals_by(&self, holder: &str) -> usize {
        let writes = self.lease_writes.lock().unwrap();
        let renew_times: std::collections::HashSet<_> = writes
            .iter()
            .filter(|l| l["spec"]["holderIdentity"] == holder)
            .map(|l| l["spec"]["renewTime"].to_string())
            .collect();
        renew_times.len()
    }

    fn lease_response(&self, method: &Method, body: &[u8]) -> Response {
        let current = self.lease.lock().unwrap().clone();
        match *method {
            Method::GET => match current {
                Some(lease) => axum::Json(lease).into_response(),
                None => status(StatusCode::NOT_FOUND, "NotFound"),
            },
            Method::POST if current.is_some() => status(StatusCode::CONFLICT, "AlreadyExists"),
            Method::POST => {
                let lease = self.store_lease(serde_json::from_slice(body).unwrap());
                (StatusCode::CREATED, axum::Json(lease)).into_response()
            }
            Method::PUT => {
                let new: Value = serde_json::from_slice(body).unwrap();
                let read_at = &new["metadata"]["resourceVersion"];
                match current {
                    Some(lease) if &lease["metadata"]["resourceVersion"] == read_at => {
                        axum::Json(self.store_lease(new)).into_response()
                    }
                    _ => status(StatusCode::CONFLICT, "Conflict"),
                }
            }
            _ => status(StatusCode::METHOD_NOT_ALLOWED, "MethodNotAllowed"),
        }
    }
}

async fn handle(
    State(api): State<Arc<FakeApi>>,
    method: Method,
    uri: Uri,
    body: Bytes,
) -> Response {
    let path = uri.path();
    if path == LEASES || path.starts_with(&format!("{LEASES}/")) {
        return api.lease_response(&method, &body);
    }
    if !path.starts_with("/apis/kaas.rs/") {
        return status(StatusCode::NOT_FOUND, "NotFound");
    }
    api.crd_requests.fetch_add(1, Ordering::SeqCst);
    if uri.query().is_some_and(|q| q.contains("watch=true")) {
        return std::future::pending().await;
    }
    if method == Method::PATCH {
        api.status_patches.fetch_add(1, Ordering::SeqCst);
        return std::future::pending().await;
    }
    let plural = path.rsplit('/').next().unwrap_or_default();
    let items = if plural == "kafkatopics" {
        api.topics.lock().unwrap().clone()
    } else {
        Vec::new()
    };
    axum::Json(json!({
        "apiVersion": "kaas.rs/v1alpha1",
        "kind": "List",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }))
    .into_response()
}

async fn serve(api: Arc<FakeApi>) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = Router::new().fallback(handle).with_state(api);
    tokio::spawn(async move { axum::serve(listener, app).await });
    port
}

/// The real binary, pointed at the fake API server. Killed on drop.
struct Operator {
    child: Child,
    probe_port: u16,
    log: std::path::PathBuf,
}

impl Operator {
    fn start(api_port: u16, dir: &Path, identity: &str) -> Self {
        let kubeconfig = dir.join("kubeconfig");
        std::fs::write(
            &kubeconfig,
            format!(
                "apiVersion: v1\nkind: Config\ncurrent-context: fake\n\
                 clusters: [{{name: fake, cluster: {{server: 'http://127.0.0.1:{api_port}'}}}}]\n\
                 users: [{{name: fake, user: {{}}}}]\n\
                 contexts: [{{name: fake, context: {{cluster: fake, user: fake}}}}]\n"
            ),
        )
        .unwrap();
        let data = dir.join("data");
        std::fs::create_dir_all(&data).unwrap();
        let probe_port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = dir.join(format!("{identity}.log"));
        let out = File::create(&log).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_kaas-operator"))
            .env_clear()
            .env("KUBECONFIG", &kubeconfig)
            .env("HOME", dir)
            .env("HOSTNAME", identity)
            .env("KAAS_NAMESPACE", NS)
            .env("KAAS_DATA_DIR", &data)
            .env(
                "HEALTH_PROBE_BIND_ADDRESS",
                format!("127.0.0.1:{probe_port}"),
            )
            .stdout(out.try_clone().unwrap())
            .stderr(out)
            .spawn()
            .unwrap();
        Self {
            child,
            probe_port,
            log,
        }
    }

    fn log(&self) -> String {
        std::fs::read_to_string(&self.log).unwrap_or_default()
    }

    fn is_ready(&self) -> bool {
        let Ok(mut conn) = TcpStream::connect(("127.0.0.1", self.probe_port)) else {
            return false;
        };
        let _ = conn.set_read_timeout(Some(Duration::from_secs(1)));
        if write!(conn, "GET /readyz HTTP/1.0\r\nHost: x\r\n\r\n").is_err() {
            return false;
        }
        let mut answer = String::new();
        let _ = conn.read_to_string(&mut answer);
        answer.starts_with("HTTP/1.0 200") || answer.starts_with("HTTP/1.1 200")
    }

    fn sigterm(&self) {
        let sent = Command::new("kill")
            .arg("-TERM")
            .arg(self.child.id().to_string())
            .status()
            .unwrap();
        assert!(sent.success());
    }

    /// Its exit status, once it has exited, or `None` past `within`.
    async fn exit(&mut self, within: Duration) -> Option<ExitStatus> {
        let deadline = Instant::now() + within;
        while Instant::now() < deadline {
            if let Some(exited) = self.child.try_wait().unwrap() {
                return Some(exited);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        None
    }
}

impl Drop for Operator {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

async fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    condition()
}

/// Up as the leader, with its reconcilers started.
async fn leading(api: &FakeApi, operator: &Operator, identity: &str) {
    let up = eventually(Duration::from_secs(20), || {
        api.holder().as_deref() == Some(identity) && api.crd_requests.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(up, "never became leader:\n{}", operator.log());
}

/// It keeps its lease while it leads, and gives it back when told to stop, so a
/// successor doesn't wait out the 15 s. The lease used to be written once.
#[tokio::test(flavor = "multi_thread")]
async fn a_leader_renews_its_lease_and_releases_it_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let api = Arc::new(FakeApi::default());
    let mut operator = Operator::start(serve(api.clone()).await, dir.path(), "op-a");
    leading(&api, &operator, "op-a").await;

    let renewed = eventually(Duration::from_secs(15), || api.renewals_by("op-a") >= 3).await;
    assert!(renewed, "the lease wasn't renewed:\n{}", operator.log());

    operator.sigterm();
    let exited = operator.exit(Duration::from_secs(10)).await;
    assert!(
        exited.is_some_and(|e| e.success()),
        "SIGTERM should end it cleanly, got {exited:?}:\n{}",
        operator.log()
    );
    assert_eq!(
        api.holder().as_deref(),
        Some(""),
        "the lease wasn't released"
    );
}

/// A standby has to read as Ready, or a rollout never finishes: the Deployment
/// waits for the new pod before stopping the old one, and the new pod can't lead
/// until the old one stops. Until now it only worked because the lease went stale.
#[tokio::test(flavor = "multi_thread")]
async fn a_standby_is_ready_touches_nothing_and_stops_on_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let api = Arc::new(FakeApi::default());
    api.set_holder("op-other");
    let writes_before = api.lease_writes.lock().unwrap().len();
    let mut operator = Operator::start(serve(api.clone()).await, dir.path(), "op-b");

    let ready = eventually(Duration::from_secs(20), || operator.is_ready()).await;
    assert!(ready, "a standby must report ready:\n{}", operator.log());
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        api.crd_requests.load(Ordering::SeqCst),
        0,
        "a standby must not reconcile or sweep"
    );

    operator.sigterm();
    let exited = operator.exit(Duration::from_secs(10)).await;
    assert!(
        exited.is_some_and(|e| e.success()),
        "SIGTERM should end a standby, got {exited:?}:\n{}",
        operator.log()
    );
    assert_eq!(api.holder().as_deref(), Some("op-other"));
    assert_eq!(api.lease_writes.lock().unwrap().len(), writes_before);
}

/// Told it has lost, a leader stops at once and exits non-zero, so the pod restarts
/// as a candidate. It must not go on reconciling next to the new leader.
#[tokio::test(flavor = "multi_thread")]
async fn a_deposed_leader_stops() {
    let dir = tempfile::tempdir().unwrap();
    let api = Arc::new(FakeApi::default());
    let mut operator = Operator::start(serve(api.clone()).await, dir.path(), "op-c");
    leading(&api, &operator, "op-c").await;

    api.set_holder("op-other");

    let exited = operator.exit(Duration::from_secs(10)).await;
    assert!(
        exited.is_some_and(|e| !e.success()),
        "a deposed leader should exit non-zero, got {exited:?}:\n{}",
        operator.log()
    );
    assert_eq!(api.holder().as_deref(), Some("op-other"));
}

/// A reconcile that never returns can't hold up the exit. It stays the leader while
/// it waits, so no successor starts next to the reconcile, and releases last.
#[tokio::test(flavor = "multi_thread")]
async fn a_stuck_reconcile_cant_hold_up_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let api = Arc::new(FakeApi::default());
    api.topics.lock().unwrap().push(json!({
        "apiVersion": "kaas.rs/v1alpha1",
        "kind": "KafkaTopic",
        "metadata": {
            "name": "stuck",
            "namespace": NS,
            "uid": "u-1",
            "resourceVersion": "1",
            "generation": 1,
        },
        "spec": {"partitions": 1},
    }));
    let mut operator = Operator::start(serve(api.clone()).await, dir.path(), "op-d");
    leading(&api, &operator, "op-d").await;
    let stuck = eventually(Duration::from_secs(20), || {
        api.status_patches.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(stuck, "the reconcile never got stuck:\n{}", operator.log());

    operator.sigterm();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(
        api.holder().as_deref(),
        Some("op-d"),
        "released while a reconcile was still running"
    );

    let exited = operator.exit(Duration::from_secs(30)).await;
    assert!(
        exited.is_some_and(|e| !e.success()),
        "a stuck reconcile held up the exit, or it hid that it left one behind, \
         got {exited:?}:\n{}",
        operator.log()
    );
    assert_eq!(
        api.holder().as_deref(),
        Some(""),
        "the lease wasn't released"
    );
}
