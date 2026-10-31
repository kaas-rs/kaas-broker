//! Reconciler that materialises a `KafkaCluster` CR into the
//! external-listener plumbing:
//!
//! - A cert-manager `Certificate` (cert-manager.io/v1) with one SAN
//!   per broker hostname (+ optional bootstrap hostname).
//! - One `Service` per broker ordinal, selecting by
//!   `statefulset.kubernetes.io/pod-name`.
//! - One Gateway-API `TLSRoute` (gateway.networking.k8s.io/v1alpha2)
//!   per broker ordinal, matched by SNI hostname.
//!
//! Cleanup runs through K8s GC via OwnerReferences set at creation
//! time — no finalizers. When `external.enabled` is toggled off
//! while the cluster CR stays alive, `delete_external_resources`
//! sweeps the previously-created objects explicitly.
//!
//! Reads `spec.replicas` (chart-templated, never user-edited day-2)
//! to size the per-broker fan-out — see the module-level note in
//! [`kaas_operator_api::kafkacluster`].

use std::sync::Arc;
use std::time::Duration;

use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kaas_operator_api::{Condition, KafkaCluster, KafkaClusterStatus};
use kube::api::{ApiResource, DynamicObject, GroupVersionKind, Patch, PatchParams, PostParams};
use kube::core::ObjectMeta;
use kube::runtime::controller::Action;
use kube::{Api, Client};
use std::collections::BTreeMap;

use crate::conditions::{set_condition, READY};
use crate::errors::ControllerError;
use crate::observer::ReconcileObserver;

pub struct KafkaClusterReconciler {
    pub client: Client,
    pub namespace: String,
    pub observer: ReconcileObserver,
    /// Whether the apiserver serves the exact optional resources this
    /// reconciler touches, discovered once on first use (gh #187). A
    /// request against an unserved group/version/resource doesn't
    /// produce a typed NotFound — the apiserver answers a plain-text
    /// `404 page not found` that kube-rs can't parse — so a cluster
    /// without those CRDs paid 10+ dead API round-trips per
    /// reconcile. Group-level discovery is NOT enough: k3s ships
    /// Gateway API v1/v1beta1 while TLSRoute lives in the
    /// experimental channel's v1alpha2, so the group exists and the
    /// resource still 404s. Installing the CRDs later requires an
    /// operator restart to be noticed — an acceptable trade (same
    /// caching behaviour as controller-runtime's RESTMapper).
    tls_routes_served: tokio::sync::OnceCell<bool>,
    certificates_served: tokio::sync::OnceCell<bool>,
}

impl KafkaClusterReconciler {
    pub fn new(client: Client, namespace: String) -> Self {
        Self {
            client,
            namespace,
            observer: ReconcileObserver::new("KafkaCluster"),
            tls_routes_served: tokio::sync::OnceCell::new(),
            certificates_served: tokio::sync::OnceCell::new(),
        }
    }

    /// True when `plural` is served under `group_version`
    /// (e.g. `tlsroutes` under `gateway.networking.k8s.io/v1alpha2`).
    /// A 404 on the discovery endpoint means the group/version isn't
    /// served at all → definitively false. Any other discovery error
    /// fails open so the per-call error paths stay authoritative.
    async fn resource_served(client: &Client, group_version: &str, plural: &str) -> bool {
        match client.list_api_group_resources(group_version).await {
            Ok(list) => list.resources.iter().any(|r| r.name == plural),
            Err(kube::Error::Api(e)) if e.code == 404 => false,
            Err(err) => {
                tracing::warn!(%err, group_version, "resource discovery failed; assuming served");
                true
            }
        }
    }

    async fn tls_routes_served(&self) -> bool {
        *self
            .tls_routes_served
            .get_or_init(|| async {
                let gv = format!("{}/{}", tls_route_gvk().group, tls_route_gvk().version);
                Self::resource_served(&self.client, &gv, "tlsroutes").await
            })
            .await
    }

    async fn certificates_served(&self) -> bool {
        *self
            .certificates_served
            .get_or_init(|| async {
                let gv = format!("{}/{}", certificate_gvk().group, certificate_gvk().version);
                Self::resource_served(&self.client, &gv, "certificates").await
            })
            .await
    }

    pub async fn reconcile(&self, cluster: Arc<KafkaCluster>) -> Result<Action, ControllerError> {
        let started = std::time::Instant::now();
        let out = self.reconcile_inner(cluster).await;
        self.observer
            .record_duration(started.elapsed().as_secs_f64());
        out
    }

    async fn reconcile_inner(&self, cluster: Arc<KafkaCluster>) -> Result<Action, ControllerError> {
        if cluster.metadata.deletion_timestamp.is_some() {
            // K8s GC handles the owned resources via OwnerReferences;
            // nothing to do here.
            self.observer.bump_requeue();
            return Ok(Action::await_change());
        }

        let ext = &cluster.spec.listeners.external;
        if !ext.enabled {
            // External listener disabled — tear down any previously-
            // created plumbing (the cluster CR stays alive so GC
            // doesn't fire on its own).
            self.delete_external_resources(&cluster).await?;
            self.set_ready(&cluster, "ExternalDisabled", "external listener disabled")
                .await?;
            self.observer.bump_success();
            return Ok(Action::requeue(Duration::from_secs(300)));
        }

        // gh #186: `spec.replicas` is chart-templated from the same value
        // as the Helm-owned StatefulSet's replicas, so they only diverge
        // when someone hand-edits one of them — and a CR running ahead of
        // the StatefulSet fans out per-broker Services whose pod-name
        // selector matches nothing: zero-endpoint Services, silently.
        // Cap the fan-out at the smaller count (existing brokers stay
        // served) and surface the drift as Ready=False below.
        let sts_replicas = self.statefulset_replicas(&cluster).await;
        let (fanout, drifted_sts) = effective_fanout(cluster.spec.replicas, sts_replicas);

        // 1. cert-manager Certificate (if enabled).
        if ext.tls.cert_manager.enabled {
            if let Err(e) = self.reconcile_certificate(&cluster).await {
                self.fail_ready(&cluster, "CertificateError", &e.to_string())
                    .await?;
                self.observer.bump_error();
                return Err(e);
            }
        }

        // 2. Per-broker Services.
        for i in 0..fanout {
            if let Err(e) = self.reconcile_broker_service(&cluster, i).await {
                self.fail_ready(&cluster, "ServiceError", &e.to_string())
                    .await?;
                self.observer.bump_error();
                return Err(e);
            }
        }

        // 3. Per-broker TLSRoutes (Gateway API).
        if ext.gateway.enabled {
            for i in 0..fanout {
                if let Err(e) = self.reconcile_broker_tls_route(&cluster, i).await {
                    self.fail_ready(&cluster, "TLSRouteError", &e.to_string())
                        .await?;
                    self.observer.bump_error();
                    return Err(e);
                }
            }
        }

        // Drift found: report it and requeue on a short fuse — the heal
        // is a `helm upgrade` growing the StatefulSet, which this
        // reconciler can only poll for.
        if let Some(sts) = drifted_sts {
            self.fail_ready(
                &cluster,
                "ReplicasMismatch",
                &format!(
                    "CR replicas={}, StatefulSet replicas={sts} — run helm upgrade; \
                     per-broker resources capped at {fanout}",
                    cluster.spec.replicas
                ),
            )
            .await?;
            self.observer.bump_requeue();
            return Ok(Action::requeue(Duration::from_secs(30)));
        }

        // 4. Status: bootstrap server list + Ready=True.
        let bootstrap = build_bootstrap_servers(&cluster);
        self.patch_status(&cluster, |st| {
            st.bootstrap_servers = bootstrap.clone();
            set_condition(
                &mut st.conditions,
                Condition {
                    type_: READY.into(),
                    status: Condition::STATUS_TRUE.into(),
                    observed_generation: cluster.metadata.generation,
                    last_transition_time: String::new(),
                    reason: "ExternalListenerReady".into(),
                    message: format!(
                        "{} brokers advertised via external listener",
                        cluster.spec.replicas
                    ),
                },
            );
        })
        .await?;

        self.observer.bump_success();
        Ok(Action::requeue(Duration::from_secs(300)))
    }

    async fn reconcile_certificate(&self, cluster: &KafkaCluster) -> Result<(), ControllerError> {
        let cluster_name = cluster.metadata.name.clone().unwrap_or_default();
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let name = format!("{cluster_name}-broker-tls");

        let mut dns_names = broker_hostnames(cluster);
        let bootstrap = cluster.spec.listeners.external.bootstrap_hostname.clone();
        if !bootstrap.is_empty() {
            dns_names.push(bootstrap);
        }

        let spec = serde_json::json!({
            "secretName": name,
            "dnsNames": dns_names,
            "issuerRef": {
                "name": cluster.spec.listeners.external.tls.cert_manager.issuer_ref.name,
                "kind": cluster.spec.listeners.external.tls.cert_manager.issuer_ref.kind,
            },
        });

        self.apply_unstructured(cluster, ns, &name, &certificate_gvk(), spec)
            .await
    }

    async fn reconcile_broker_service(
        &self,
        cluster: &KafkaCluster,
        ordinal: i32,
    ) -> Result<(), ControllerError> {
        let cluster_name = cluster.metadata.name.clone().unwrap_or_default();
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let name = format!("{cluster_name}-broker-{ordinal}");
        let port = nonzero_or(cluster.spec.listeners.external.port, 9093);

        let mut selector = BTreeMap::new();
        selector.insert("app.kubernetes.io/name".into(), "kaas".into());
        selector.insert("app.kubernetes.io/instance".into(), cluster_name.clone());
        selector.insert(
            "statefulset.kubernetes.io/pod-name".into(),
            format!("{cluster_name}-{ordinal}"),
        );

        let desired = Service {
            metadata: ObjectMeta {
                name: Some(name.clone()),
                namespace: Some(ns.into()),
                owner_references: Some(vec![owner_ref_for(cluster)]),
                ..ObjectMeta::default()
            },
            spec: Some(ServiceSpec {
                type_: Some("ClusterIP".into()),
                selector: Some(selector.clone()),
                ports: Some(vec![ServicePort {
                    name: Some("kafka-tls".into()),
                    port,
                    target_port: Some(IntOrString::String("kafka-tls".into())),
                    ..ServicePort::default()
                }]),
                ..ServiceSpec::default()
            }),
            ..Service::default()
        };

        let api: Api<Service> = Api::namespaced(self.client.clone(), ns);
        match api.get(&name).await {
            Ok(mut existing) => {
                let spec = existing.spec.get_or_insert_with(ServiceSpec::default);
                spec.selector = Some(selector);
                spec.ports = desired.spec.as_ref().and_then(|s| s.ports.clone());
                api.replace(&name, &PostParams::default(), &existing)
                    .await?;
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                api.create(&PostParams::default(), &desired).await?;
                Ok(())
            }
            Err(e) => Err(ControllerError::Kube(e)),
        }
    }

    async fn reconcile_broker_tls_route(
        &self,
        cluster: &KafkaCluster,
        ordinal: i32,
    ) -> Result<(), ControllerError> {
        let cluster_name = cluster.metadata.name.clone().unwrap_or_default();
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let name = format!("{cluster_name}-broker-{ordinal}");
        let port = nonzero_or(cluster.spec.listeners.external.port, 9093);
        let hostname = format_hostname(&cluster.spec.listeners.external.hostname_pattern, ordinal);

        let mut parent_ref = serde_json::json!({
            "name": cluster.spec.listeners.external.gateway.gateway_ref.name,
        });
        let gw_ns = &cluster
            .spec
            .listeners
            .external
            .gateway
            .gateway_ref
            .namespace;
        if !gw_ns.is_empty() {
            parent_ref["namespace"] = serde_json::Value::String(gw_ns.clone());
        }

        let spec = serde_json::json!({
            "hostnames": [hostname],
            "parentRefs": [parent_ref],
            "rules": [{
                "backendRefs": [{
                    "name": name,
                    "port": port,
                }]
            }],
        });

        self.apply_unstructured(cluster, ns, &name, &tls_route_gvk(), spec)
            .await
    }

    /// Get-or-create-or-update for the cert-manager / Gateway-API
    /// DynamicObjects.
    /// Sets OwnerReferences + management labels on every write.
    async fn apply_unstructured(
        &self,
        cluster: &KafkaCluster,
        ns: &str,
        name: &str,
        gvk: &GroupVersionKind,
        spec: serde_json::Value,
    ) -> Result<(), ControllerError> {
        let cluster_name = cluster.metadata.name.clone().unwrap_or_default();
        let ar = ApiResource::from_gvk(gvk);
        let api: Api<DynamicObject> = Api::namespaced_with(self.client.clone(), ns, &ar);

        let mut labels = BTreeMap::new();
        labels.insert(
            "app.kubernetes.io/managed-by".into(),
            "kaas-operator".into(),
        );
        labels.insert("kaas.rs/cluster".into(), cluster_name);

        let mut desired = DynamicObject::new(name, &ar).within(ns);
        desired.metadata.labels = Some(labels.clone());
        desired.metadata.owner_references = Some(vec![owner_ref_for(cluster)]);
        desired.data = serde_json::json!({ "spec": spec });

        match api.get(name).await {
            Ok(mut existing) => {
                // Replace spec + labels; preserve everything else
                // (status, resourceVersion, etc.). Backfill the owner
                // reference if a pre-Phase-7 object is missing it.
                if let Some(obj) = existing.data.as_object_mut() {
                    obj.insert("spec".into(), spec);
                }
                existing.metadata.labels = Some(labels);
                if !has_controller_owner(&existing.metadata.owner_references, cluster) {
                    let mut refs = existing
                        .metadata
                        .owner_references
                        .take()
                        .unwrap_or_default();
                    refs.push(owner_ref_for(cluster));
                    existing.metadata.owner_references = Some(refs);
                }
                api.replace(name, &PostParams::default(), &existing).await?;
                Ok(())
            }
            Err(kube::Error::Api(e)) if e.code == 404 => {
                api.create(&PostParams::default(), &desired).await?;
                Ok(())
            }
            Err(e) => Err(ControllerError::Kube(e)),
        }
    }

    /// Delete the cert-manager Certificate + per-broker Services + TLSRoutes
    /// when the external listener is toggled off while the cluster CR stays
    /// alive. Uses an upper-bounded ordinal range to catch shrink cases.
    async fn delete_external_resources(
        &self,
        cluster: &KafkaCluster,
    ) -> Result<(), ControllerError> {
        let cluster_name = cluster.metadata.name.clone().unwrap_or_default();
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);

        // Certificate (cert-manager). Skipped when the cluster has no
        // cert-manager install — nothing could exist there (gh #187).
        if self.certificates_served().await {
            let cert_ar = ApiResource::from_gvk(&certificate_gvk());
            let cert_api: Api<DynamicObject> =
                Api::namespaced_with(self.client.clone(), ns, &cert_ar);
            let cert_name = format!("{cluster_name}-broker-tls");
            // Delete is best-effort; NotFound is fine. Anything else
            // bubbles so the reconciler logs and retries.
            delete_if_present(&cert_api, &cert_name).await?;
        }

        // Services + TLSRoutes per ordinal, with a 10-floor
        // upper bound to catch shrink scenarios.
        // TLSRoutes are skipped entirely when the Gateway API isn't
        // installed — every such delete was an unrouted plain-text
        // 404 the client had to error-parse (gh #187).
        let upper = cluster.spec.replicas.max(10);
        let svc_api: Api<Service> = Api::namespaced(self.client.clone(), ns);
        let routes_served = self.tls_routes_served().await;
        let route_ar = ApiResource::from_gvk(&tls_route_gvk());
        let route_api: Api<DynamicObject> =
            Api::namespaced_with(self.client.clone(), ns, &route_ar);

        for i in 0..upper {
            let name = format!("{cluster_name}-broker-{i}");
            delete_if_present(&svc_api, &name).await?;
            if routes_served {
                delete_if_present(&route_api, &name).await?;
            }
        }
        Ok(())
    }

    async fn patch_status(
        &self,
        cluster: &KafkaCluster,
        mutate: impl FnOnce(&mut KafkaClusterStatus),
    ) -> Result<(), ControllerError> {
        let Some(name) = cluster.metadata.name.as_deref() else {
            return Ok(());
        };
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let api: Api<KafkaCluster> = Api::namespaced(self.client.clone(), ns);

        let mut status = cluster.status.clone().unwrap_or_default();
        mutate(&mut status);
        // Server-side apply requires apiVersion + kind in the body —
        // without them the API server answers
        // `invalid object type: /, Kind=` (400).
        let body = serde_json::json!({
            "apiVersion": "kaas.rs/v1alpha1",
            "kind": "KafkaCluster",
            "status": status,
        });
        api.patch_status(
            name,
            &PatchParams::apply("kaas-operator").force(),
            &Patch::Apply(&body),
        )
        .await?;
        Ok(())
    }

    async fn set_ready(
        &self,
        cluster: &KafkaCluster,
        reason: &str,
        message: &str,
    ) -> Result<(), ControllerError> {
        let cond = Condition {
            type_: READY.into(),
            status: Condition::STATUS_TRUE.into(),
            observed_generation: cluster.metadata.generation,
            last_transition_time: String::new(),
            reason: reason.into(),
            message: message.into(),
        };
        self.patch_status(cluster, |st| {
            set_condition(&mut st.conditions, cond.clone())
        })
        .await
    }

    /// The Helm-owned broker StatefulSet's `spec.replicas`, or `None`
    /// when it can't be read — absent (operator running without the
    /// chart, tests) or a transient API error. `None` skips the
    /// gh #186 drift check rather than failing the reconcile: the
    /// check is defense-in-depth, not a serving dependency.
    async fn statefulset_replicas(&self, cluster: &KafkaCluster) -> Option<i32> {
        let name = cluster.metadata.name.as_deref()?;
        let ns = cluster
            .metadata
            .namespace
            .as_deref()
            .unwrap_or(&self.namespace);
        let api: Api<k8s_openapi::api::apps::v1::StatefulSet> =
            Api::namespaced(self.client.clone(), ns);
        match api.get_opt(name).await {
            Ok(sts) => sts.and_then(|s| s.spec.and_then(|spec| spec.replicas)),
            Err(err) => {
                tracing::warn!(%err, name, "statefulset read failed; skipping replicas drift check");
                None
            }
        }
    }

    async fn fail_ready(
        &self,
        cluster: &KafkaCluster,
        reason: &str,
        message: &str,
    ) -> Result<(), ControllerError> {
        let cond = Condition {
            type_: READY.into(),
            status: Condition::STATUS_FALSE.into(),
            observed_generation: cluster.metadata.generation,
            last_transition_time: String::new(),
            reason: reason.into(),
            message: message.into(),
        };
        self.patch_status(cluster, |st| {
            set_condition(&mut st.conditions, cond.clone())
        })
        .await
    }
}

// --- GVK helpers for the unstructured paths ------------------------
//
// `kube::core::GroupVersionKind` carries `String` fields, so it
// can't live as a `const`. Wrap each external-API GVK in a small fn
// so call sites read naturally. Each is called twice per reconcile
// at most — allocation cost is negligible.

fn certificate_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("cert-manager.io", "v1", "Certificate")
}
fn tls_route_gvk() -> GroupVersionKind {
    GroupVersionKind::gvk("gateway.networking.k8s.io", "v1alpha2", "TLSRoute")
}

// --- helpers --------------------------------------------------------

pub fn broker_hostnames(cluster: &KafkaCluster) -> Vec<String> {
    let pattern = &cluster.spec.listeners.external.hostname_pattern;
    (0..cluster.spec.replicas)
        .map(|i| format_hostname(pattern, i))
        .collect()
}

pub fn build_bootstrap_servers(cluster: &KafkaCluster) -> Vec<String> {
    let port = nonzero_or(cluster.spec.listeners.external.port, 9093);
    broker_hostnames(cluster)
        .into_iter()
        .map(|h| format!("{h}:{port}"))
        .collect()
}

/// Minimal printf-style formatter over a
/// `%d` template. Supports exactly one `%d` placeholder anywhere in
/// the pattern; on a malformed pattern, returns the literal pattern
/// verbatim.
fn format_hostname(pattern: &str, ordinal: i32) -> String {
    if let Some(idx) = pattern.find("%d") {
        let (head, rest) = pattern.split_at(idx);
        let tail = &rest[2..];
        format!("{head}{ordinal}{tail}")
    } else {
        pattern.to_string()
    }
}

fn nonzero_or(v: i32, fallback: i32) -> i32 {
    if v == 0 {
        fallback
    } else {
        v
    }
}

/// gh #186: how many broker ordinals to fan per-broker resources over,
/// given the CR's replicas and the StatefulSet's (when readable).
/// Returns `(fanout, Some(sts_replicas))` on drift — the fan-out is
/// capped at the smaller count so no Service ever selects a pod that
/// can't exist, while existing ordinals stay served.
fn effective_fanout(cr_replicas: i32, sts_replicas: Option<i32>) -> (i32, Option<i32>) {
    match sts_replicas {
        Some(sts) if sts != cr_replicas => (cr_replicas.min(sts), Some(sts)),
        _ => (cr_replicas, None),
    }
}

fn owner_ref_for(cluster: &KafkaCluster) -> OwnerReference {
    OwnerReference {
        api_version: format!(
            "{}/{}",
            kaas_operator_api::GROUP,
            kaas_operator_api::VERSION
        ),
        kind: "KafkaCluster".into(),
        name: cluster.metadata.name.clone().unwrap_or_default(),
        uid: cluster.metadata.uid.clone().unwrap_or_default(),
        controller: Some(true),
        block_owner_deletion: Some(true),
    }
}

fn has_controller_owner(refs: &Option<Vec<OwnerReference>>, cluster: &KafkaCluster) -> bool {
    let Some(refs) = refs else { return false };
    let uid = cluster.metadata.uid.as_deref().unwrap_or("");
    refs.iter()
        .any(|r| r.uid == uid && r.controller.unwrap_or(false))
}

/// Delete an object if it exists. NotFound is treated as success
/// (delete errors in this teardown path are ignored).
async fn delete_if_present<K>(api: &Api<K>, name: &str) -> Result<(), ControllerError>
where
    K: kube::Resource + Clone + serde::de::DeserializeOwned + std::fmt::Debug,
{
    match api.delete(name, &Default::default()).await {
        Ok(_) => Ok(()),
        Err(kube::Error::Api(e)) if e.code == 404 => Ok(()),
        Err(e) => Err(ControllerError::Kube(e)),
    }
}

pub async fn reconcile_cluster(
    cluster: Arc<KafkaCluster>,
    ctx: Arc<KafkaClusterReconciler>,
) -> Result<Action, ControllerError> {
    ctx.reconcile(cluster).await
}

pub fn error_policy(
    _cluster: Arc<KafkaCluster>,
    err: &ControllerError,
    ctx: Arc<KafkaClusterReconciler>,
) -> Action {
    tracing::warn!(error = %err, "KafkaCluster reconcile failed");
    ctx.observer.bump_error();
    Action::requeue(Duration::from_secs(10))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kaas_operator_api::{
        CertManagerConfig, ExternalListener, GatewayConfig, GatewayRef, InternalListener,
        IssuerRef, KafkaClusterListeners, KafkaClusterSpec, KafkaClusterStorage, ServiceConfig,
        TlsConfig,
    };

    fn cluster_with(replicas: i32, pattern: &str, port: i32) -> KafkaCluster {
        KafkaCluster {
            metadata: ObjectMeta {
                name: Some("sk".into()),
                namespace: Some("default".into()),
                uid: Some("uid-1".into()),
                ..ObjectMeta::default()
            },
            spec: KafkaClusterSpec {
                replicas,
                storage: KafkaClusterStorage::default(),
                listeners: KafkaClusterListeners {
                    internal: InternalListener { port: 9092 },
                    external: ExternalListener {
                        enabled: true,
                        port,
                        hostname_pattern: pattern.into(),
                        bootstrap_hostname: String::new(),
                        tls: TlsConfig {
                            cert_manager: CertManagerConfig::default(),
                        },
                        gateway: GatewayConfig {
                            enabled: false,
                            gateway_ref: GatewayRef::default(),
                        },
                        service: ServiceConfig::default(),
                    },
                },
            },
            status: None,
        }
    }

    #[test]
    fn format_hostname_substitutes_one_decimal_verb() {
        assert_eq!(
            format_hostname("broker-%d.kafka.example.com", 2),
            "broker-2.kafka.example.com"
        );
    }

    #[test]
    fn format_hostname_no_verb_returns_pattern_verbatim() {
        // A pattern with no verb renders verbatim.
        assert_eq!(format_hostname("static.host", 5), "static.host");
    }

    #[test]
    fn broker_hostnames_one_per_replica() {
        let c = cluster_with(3, "broker-%d.kafka", 9093);
        let hosts = broker_hostnames(&c);
        assert_eq!(
            hosts,
            vec!["broker-0.kafka", "broker-1.kafka", "broker-2.kafka"]
        );
    }

    #[test]
    fn build_bootstrap_servers_uses_external_port_with_fallback() {
        // Zero port → fallback to 9093.
        let c = cluster_with(2, "broker-%d.kafka", 0);
        assert_eq!(
            build_bootstrap_servers(&c),
            vec!["broker-0.kafka:9093", "broker-1.kafka:9093"]
        );

        // Explicit port wins.
        let c = cluster_with(2, "broker-%d.kafka", 19_093);
        assert_eq!(
            build_bootstrap_servers(&c),
            vec!["broker-0.kafka:19093", "broker-1.kafka:19093"]
        );
    }

    #[test]
    fn owner_ref_for_carries_controller_flag() {
        let c = cluster_with(1, "h%d", 9093);
        let owner = owner_ref_for(&c);
        assert_eq!(owner.kind, "KafkaCluster");
        assert_eq!(owner.uid, "uid-1");
        assert_eq!(owner.controller, Some(true));
        assert_eq!(owner.api_version, "kaas.rs/v1alpha1");
    }

    #[test]
    fn a_drifted_statefulset_caps_the_fanout_and_reports() {
        // CR ahead of the StatefulSet (the gh #186 hand-edit): cap.
        assert_eq!(effective_fanout(5, Some(3)), (3, Some(3)));
        // StatefulSet ahead of the CR (edit in the other direction —
        // extra pods exist but the CR doesn't claim them): still a
        // drift worth reporting; fan out only what the CR asks for.
        assert_eq!(effective_fanout(3, Some(5)), (3, Some(5)));
    }

    #[test]
    fn agreement_or_unreadable_statefulset_fans_out_the_cr_count() {
        assert_eq!(effective_fanout(3, Some(3)), (3, None));
        // No StatefulSet (tests, chart-less operator): fail open.
        assert_eq!(effective_fanout(3, None), (3, None));
    }

    #[test]
    fn nonzero_or_picks_fallback_only_on_zero() {
        assert_eq!(nonzero_or(0, 9093), 9093);
        assert_eq!(nonzero_or(1234, 9093), 1234);
    }

    #[test]
    fn gvk_helpers_match_unstructured_endpoints() {
        let cert = certificate_gvk();
        assert_eq!(cert.group, "cert-manager.io");
        assert_eq!(cert.version, "v1");
        assert_eq!(cert.kind, "Certificate");

        let route = tls_route_gvk();
        assert_eq!(route.group, "gateway.networking.k8s.io");
        assert_eq!(route.version, "v1alpha2");
        assert_eq!(route.kind, "TLSRoute");
    }

    #[test]
    fn issuer_ref_round_trips() {
        let r = IssuerRef {
            name: "ca-issuer".into(),
            kind: "ClusterIssuer".into(),
        };
        let json = serde_json::to_value(&r).unwrap();
        assert_eq!(json["name"], "ca-issuer");
        assert_eq!(json["kind"], "ClusterIssuer");
    }
}
