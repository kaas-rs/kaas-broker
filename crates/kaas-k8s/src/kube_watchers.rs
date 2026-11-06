//! Kube-bound implementations: `EndpointSlice` watcher,
//! `Lease`-poll epoch source, and the `Pod` readiness patcher.
//!
//! Each helper takes a [`kube::Client`] (or a closure that produces
//! one) and runs as a long-lived `tokio` task. Cancellation is via
//! [`tokio_util::sync::CancellationToken`] — same shape the broker
//! binary already uses everywhere else.
//!
//! The `KafkaTopic` watcher is parked for Phase 7, where the CR
//! type lands in `kaas-operator-api`. Phase 5 doesn't read the CR
//! anyway — the broker takes its topic catalog from
//! `KAAS_TOPICS` env JSON until the operator surfaces in
//! Phase 7.

// Module-gating is already done via `#[cfg(...)]` on the
// `pub mod kube_watchers;` in lib.rs; the file-scope
// `#![cfg(...)]` only repeats it and clippy flags it as
// duplicated.

use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::coordination::v1::Lease;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{Api, ListParams};
use kube::runtime::watcher::{watcher, Config as WatcherConfig, Event};
use kube::Client;
use thiserror::Error;
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::endpoints::{BrokerRegistry, EndpointSliceData, EndpointSliceEntry};
use kaas_broker::coordinator::LeaseEpochSource;

#[derive(Debug, Error)]
pub enum KubeWatchError {
    #[error("kube error: {0}")]
    Kube(#[from] kube::Error),

    #[error("kube watcher: {0}")]
    Other(String),
}

/// Lease-backed [`LeaseEpochSource`] — polls the
/// `kaas-controller` Lease on a 1 s cadence and surfaces
/// `spec.leaseTransitions` as the current controller epoch.
///
/// Phase 5 brokers read `current_epoch` on every
/// [`kaas_broker::Coordinator::apply_if_new`] call to reject writes
/// from a partitioned ex-controller. Updates are atomic and lock-
/// free; the poll loop is the only writer.
///
/// [`LeaseEpochSource`]:
///     kaas_broker::coordinator::LeaseEpochSource
#[derive(Debug)]
pub struct KubeLeaseEpoch {
    epoch: AtomicI64,
    holder: parking_lot::Mutex<Option<String>>,
}

impl Default for KubeLeaseEpoch {
    fn default() -> Self {
        Self::new()
    }
}

impl KubeLeaseEpoch {
    pub fn new() -> Self {
        Self {
            epoch: AtomicI64::new(0),
            holder: parking_lot::Mutex::new(None),
        }
    }

    pub fn current_epoch(&self) -> i64 {
        self.epoch.load(Ordering::Relaxed)
    }

    pub fn current_holder(&self) -> Option<String> {
        self.holder.lock().clone()
    }

    fn store(&self, epoch: i64, holder: Option<String>) {
        self.epoch.store(epoch, Ordering::Relaxed);
        *self.holder.lock() = holder;
    }
}

impl LeaseEpochSource for KubeLeaseEpoch {
    fn current_epoch(&self) -> i64 {
        Self::current_epoch(self)
    }
}

/// Poll the `kaas-controller` Lease on a 1 s cadence and update
/// the supplied [`KubeLeaseEpoch`]. Returns when `cancel` fires.
pub async fn run_lease_watch(
    client: Client,
    namespace: String,
    lease_name: String,
    epoch: Arc<KubeLeaseEpoch>,
    cancel: CancellationToken,
) {
    let api: Api<Lease> = Api::namespaced(client, &namespace);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tick.tick() => {
                match kaas_observability::record_k8s_call("Get", "Lease", api.get_opt(&lease_name)).await {
                    Ok(Some(lease)) => {
                        let transitions = lease
                            .spec
                            .as_ref()
                            .and_then(|s| s.lease_transitions)
                            .unwrap_or(0);
                        let holder = lease
                            .spec
                            .as_ref()
                            .and_then(|s| s.holder_identity.clone());
                        epoch.store(i64::from(transitions), holder);
                    }
                    Ok(None) => {
                        debug!(
                            lease = lease_name.as_str(),
                            "lease watch: lease not found yet (cluster still booting)"
                        );
                    }
                    Err(err) => {
                        warn!(%err, lease = lease_name.as_str(),
                              "lease watch: get failed; will retry next tick");
                    }
                }
            }
        }
    }
}

/// Stream `EndpointSlice` events for the headless service and feed
/// them into the registry. Selects slices by the standard
/// `kubernetes.io/service-name` label so the broker only sees its
/// own headless service's slices.
pub async fn run_endpoint_watch(
    client: Client,
    namespace: String,
    headless_service: String,
    registry: Arc<BrokerRegistry>,
    cancel: CancellationToken,
) {
    let api: Api<EndpointSlice> = Api::namespaced(client, &namespace);
    let lp =
        ListParams::default().labels(&format!("kubernetes.io/service-name={headless_service}"));
    let cfg = WatcherConfig {
        label_selector: Some(lp.label_selector.unwrap_or_default()),
        ..Default::default()
    };

    let mut stream = watcher(api, cfg).boxed();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            evt = stream.next() => {
                let evt = match evt {
                    None => {
                        debug!("endpoint watch: stream ended; restarting");
                        return;
                    }
                    Some(Ok(e)) => e,
                    Some(Err(err)) => {
                        warn!(%err, "endpoint watch: error from stream");
                        continue;
                    }
                };
                handle_endpoint_event(&registry, evt);
            }
        }
    }
}

fn handle_endpoint_event(registry: &BrokerRegistry, event: Event<EndpointSlice>) {
    match event {
        Event::Apply(slice) | Event::InitApply(slice) => {
            registry.apply_slice(&convert_slice(&slice));
        }
        Event::Delete(slice) => {
            registry.delete_slice(&convert_slice(&slice));
        }
        Event::Init => {}
        Event::InitDone => {}
    }
}

fn convert_slice(slice: &EndpointSlice) -> EndpointSliceData {
    let kafka_port = slice.ports.as_ref().and_then(|ps| {
        ps.iter()
            .find(|p| p.name.as_deref() == Some("kafka"))
            .and_then(|p| p.port)
    });
    let entries = slice
        .endpoints
        .iter()
        .filter_map(|ep| {
            let hostname = ep.hostname.clone()?;
            let address = ep.addresses.first()?.clone();
            let ready = ep
                .conditions
                .as_ref()
                .and_then(|c| c.ready)
                .unwrap_or(false);
            Some(EndpointSliceEntry {
                hostname,
                address,
                ready,
            })
        })
        .collect();
    EndpointSliceData {
        // gh #249: the registry deregisters ordinals that vanish from
        // the slice that owned them, so it needs to know which slice
        // this is. A nameless object can't own anything, and an empty
        // name simply never matches a recorded owner.
        name: slice.metadata.name.clone().unwrap_or_default(),
        entries,
        kafka_port,
    }
}

/// Patch the broker pod's `Status.Conditions` to flip the
/// `kaas.rs/PartitionsReady` readinessGate.
///
/// Returns `Ok(())` on the first successful patch; the caller can
/// retry on failure.
pub async fn patch_readiness(
    client: Client,
    namespace: String,
    pod_name: String,
    ready: bool,
) -> Result<(), KubeWatchError> {
    use k8s_openapi::api::core::v1::Pod;
    use kube::api::{Patch, PatchParams};

    let api: Api<Pod> = Api::namespaced(client, &namespace);
    let condition_status = if ready { "True" } else { "False" };
    // Server-side apply requires apiVersion + kind in the body —
    // without them the API server answers
    // `invalid object type: /, Kind=` (400).
    let patch = serde_json::json!({
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": { "name": pod_name, "namespace": namespace },
        "status": {
            "conditions": [
                {
                    "type": crate::readiness::READINESS_CONDITION,
                    "status": condition_status,
                    "lastTransitionTime": chrono::Utc::now()
                        .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                    "reason": "AssignmentApplied",
                    "message": "broker has applied at least one assignment.json",
                }
            ]
        }
    });
    kaas_observability::record_k8s_call(
        "Patch",
        "Pod",
        api.patch_status(
            &pod_name,
            &PatchParams::apply("kaas").force(),
            &Patch::Apply(&patch),
        ),
    )
    .await?;
    Ok(())
}

/// Backoff bounds for restarting the `KafkaTopic` watch stream.
const TOPIC_WATCH_BACKOFF_MIN: Duration = Duration::from_secs(1);
const TOPIC_WATCH_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Topics reported to `on_apply` and not yet retracted, plus the
/// relist currently being accumulated.
///
/// `known` deliberately outlives any single stream: kube ends a
/// watch stream for routine reasons (relist, API hiccup, apiserver
/// rollout), and a topic deleted while we were disconnected is only
/// discoverable by diffing the next relist against what we last
/// reported. See gh #202.
#[derive(Default)]
struct TopicWatchState {
    known: std::collections::HashSet<String>,
    /// `Some` between `Event::Init` and `Event::InitDone`.
    relist: Option<std::collections::HashSet<String>>,
    /// gh #241: set once a full list has completed. Latching — the
    /// caller is told once and never un-told, because a later
    /// disconnect doesn't un-know the topics already delivered.
    synced: bool,
}

/// One Apply/InitApply event, projected to what the broker needs. A
/// struct rather than positional arguments because the optional
/// fields are same-typed and silently swappable at the call site.
#[derive(Debug, Clone, Copy)]
pub struct TopicApply<'a> {
    /// The effective Kafka name (`spec.topicName` when set).
    pub name: &'a str,
    pub partitions: i32,
    /// gh #221 phase 2: `status.volumeAssignments`.
    pub volume_assignments: &'a std::collections::BTreeMap<String, String>,
    /// gh #224: the `kaas.rs/migrate-to-volume` annotation.
    pub migrate_to: Option<&'a str>,
    /// gh #241: `status.topicId`. `None` means the operator has not
    /// stamped this incarnation yet — distinct from the topic being
    /// unknown, and the engine's identity gate depends on the
    /// difference.
    pub topic_id: Option<&'a str>,
}

/// Stream `KafkaTopic` CR events into the broker's topic-registry
/// callback. Called with an `on_apply` closure that receives a
/// [`TopicApply`] on Apply/InitApply, an `on_delete` closure
/// that receives `name` on Delete — and, on relist, for any topic
/// that vanished while the watch was down — and an `on_synced` closure
/// fired once, when the first full list completes (gh #241: only then
/// does a topic's *absence* from the registry mean anything).
///
/// Runs until `cancel` fires. A stream that ends is restarted with
/// exponential backoff rather than terminating the watch: before
/// gh #202 this returned `Ok(())` on stream end and the caller never
/// restarted it, so topic tracking stopped silently and the registry
/// served deleted topics indefinitely.
pub async fn run_topic_watch<A, D, F, S>(
    client: Client,
    namespace: String,
    on_apply: A,
    on_delete: D,
    on_synced: S,
    cancel: CancellationToken,
) -> Result<(), KubeWatchError>
where
    A: Fn(TopicApply<'_>) + Send + Sync + 'static,
    D: Fn(&str) -> F + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
    S: Fn() + Send + Sync + 'static,
{
    let api: Api<kaas_operator_api::KafkaTopic> = Api::namespaced(client, &namespace);
    let mut state = TopicWatchState::default();
    let mut backoff = TOPIC_WATCH_BACKOFF_MIN;

    loop {
        if cancel.is_cancelled() {
            return Ok(());
        }
        let mut stream = watcher(api.clone(), WatcherConfig::default()).boxed();
        // A relist may have been cut short by the previous stream
        // ending; drop the partial set so it can't retract topics
        // it never finished enumerating.
        state.relist = None;

        loop {
            tokio::select! {
                () = cancel.cancelled() => return Ok(()),
                evt = stream.next() => {
                    match evt {
                        None => break,
                        Some(Ok(e)) => {
                            // The stream produced something, so the
                            // connection is healthy — don't carry a
                            // long backoff into the next restart.
                            backoff = TOPIC_WATCH_BACKOFF_MIN;
                            let was_synced = state.synced;
                            handle_topic_event(&on_apply, &on_delete, e, &mut state).await;
                            // gh #241: fire once, on the first completed
                            // list. From here on, a topic missing from
                            // the registry means "not delivered yet",
                            // not "unknown".
                            if state.synced && !was_synced {
                                on_synced();
                            }
                        }
                        Some(Err(err)) => {
                            warn!(%err, "topic watch: error from stream");
                            continue;
                        }
                    }
                }
            }
        }

        warn!(
            ?backoff,
            known_topics = state.known.len(),
            "topic watch: stream ended; restarting"
        );
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(TOPIC_WATCH_BACKOFF_MAX);
    }
}

/// gh #224: annotation that asks the brokers to drive this topic's
/// partitions onto one log dir via the AlterReplicaLogDirs path —
/// level-triggered and idempotent (partitions already on the target
/// are skipped), rate-limited by the driver. Remove the annotation
/// once `status.volumeAssignments` shows the move complete.
pub const MIGRATE_TO_VOLUME_ANNOTATION: &str = "kaas.rs/migrate-to-volume";

/// A delete's callback finishes before this returns, and so before the watch
/// handles the next event (gh #331). A recreate under the same name arrives on
/// this stream, so it can't be applied while the old incarnation is being
/// forgotten.
async fn handle_topic_event<A, D, F>(
    on_apply: &A,
    on_delete: &D,
    event: Event<kaas_operator_api::KafkaTopic>,
    state: &mut TopicWatchState,
) where
    A: Fn(TopicApply<'_>),
    D: Fn(&str) -> F,
    F: Future<Output = ()>,
{
    match event {
        Event::Apply(t) | Event::InitApply(t) => {
            // gh #219: register under the effective Kafka name
            // (`spec.topic_name` when set), NOT `metadata.name`. For a
            // non-RFC-1123 Kafka name — every uppercase Streams
            // repartition/changelog topic — `metadata.name` is a
            // synthetic `kaas-topic-<hash>` (gh #86). Registering by it
            // makes the topic invisible to clients under its real name:
            // Metadata returns "unknown partitions" and Streams loops
            // forever on TopicExists. `effective_topic_name` matches the
            // partition dir the operator creates, so registry + storage +
            // client all agree. (See the accessor's own doc contract.)
            let name = t.effective_topic_name();
            if name.is_empty() {
                return;
            }
            let name = name.to_owned();
            // gh #221 phase 2: forward the reconciler-stamped
            // partition→log-dir placement so the registry (the
            // engine's placement resolver) tracks it.
            static EMPTY: std::sync::LazyLock<std::collections::BTreeMap<String, String>> =
                std::sync::LazyLock::new(std::collections::BTreeMap::new);
            let volumes = t
                .status
                .as_ref()
                .map(|st| &st.volume_assignments)
                .unwrap_or(&EMPTY);
            let migrate_to = t
                .metadata
                .annotations
                .as_ref()
                .and_then(|a| a.get(MIGRATE_TO_VOLUME_ANNOTATION))
                .map(String::as_str);
            // gh #241: empty status.topicId means "not stamped yet",
            // which must reach the registry as such — the identity gate
            // reads it as "the operator has not reconciled this
            // incarnation", not as "unknown topic".
            let topic_id = t
                .status
                .as_ref()
                .map(|st| st.topic_id.as_str())
                .filter(|id| !id.is_empty());
            on_apply(TopicApply {
                name: &name,
                partitions: t.spec.partitions,
                volume_assignments: volumes,
                migrate_to,
                topic_id,
            });
            state.known.insert(name.clone());
            if let Some(relist) = state.relist.as_mut() {
                relist.insert(name);
            }
        }
        Event::Delete(t) => {
            let name = t.effective_topic_name();
            if name.is_empty() {
                return;
            }
            on_delete(name).await;
            state.known.remove(name);
        }
        Event::Init => {
            state.relist = Some(std::collections::HashSet::new());
        }
        Event::InitDone => {
            // The relist is the authoritative topic set. Anything we
            // previously reported that isn't in it was deleted —
            // possibly while this watch was disconnected, in which
            // case no Delete event ever arrives.
            let Some(relist) = state.relist.take() else {
                return;
            };
            for stale in state.known.difference(&relist) {
                debug!(topic = stale.as_str(), "topic watch: retracting on relist");
                on_delete(stale).await;
            }
            state.known = relist;
            // gh #241: only now is the registry's *absence* of a topic
            // meaningful — before the first completed list it means "we
            // haven't looked", after it means "not ours (yet)".
            state.synced = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::FutureExt;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions, EndpointPort};
    use kube::api::ObjectMeta;

    #[test]
    fn convert_slice_extracts_kafka_port_when_named() {
        let slice = EndpointSlice {
            metadata: ObjectMeta::default(),
            address_type: "IPv4".to_owned(),
            endpoints: vec![Endpoint {
                hostname: Some("kaas-1".to_owned()),
                addresses: vec!["10.0.0.5".to_owned()],
                conditions: Some(EndpointConditions {
                    ready: Some(true),
                    serving: None,
                    terminating: None,
                }),
                ..Default::default()
            }],
            ports: Some(vec![
                EndpointPort {
                    name: Some("kafka".to_owned()),
                    port: Some(9092),
                    ..Default::default()
                },
                EndpointPort {
                    name: Some("metrics".to_owned()),
                    port: Some(9090),
                    ..Default::default()
                },
            ]),
        };
        let data = convert_slice(&slice);
        assert_eq!(data.kafka_port, Some(9092));
        assert_eq!(data.entries.len(), 1);
        assert_eq!(data.entries[0].hostname, "kaas-1");
        assert_eq!(data.entries[0].address, "10.0.0.5");
        assert!(data.entries[0].ready);
    }

    #[test]
    fn convert_slice_skips_entries_without_hostname() {
        let slice = EndpointSlice {
            metadata: ObjectMeta::default(),
            address_type: "IPv4".to_owned(),
            endpoints: vec![Endpoint {
                hostname: None,
                addresses: vec!["10.0.0.5".to_owned()],
                ..Default::default()
            }],
            ports: None,
        };
        assert!(convert_slice(&slice).entries.is_empty());
    }

    #[test]
    fn kube_lease_epoch_starts_at_zero() {
        let e = KubeLeaseEpoch::new();
        assert_eq!(e.current_epoch(), 0);
        assert!(e.current_holder().is_none());
    }

    #[test]
    fn kube_lease_epoch_stores_value() {
        let e = KubeLeaseEpoch::new();
        e.store(7, Some("kaas-0".to_owned()));
        assert_eq!(e.current_epoch(), 7);
        assert_eq!(e.current_holder().as_deref(), Some("kaas-0"));
    }

    fn topic(name: &str, partitions: i32) -> kaas_operator_api::KafkaTopic {
        kaas_operator_api::KafkaTopic::new(
            name,
            kaas_operator_api::KafkaTopicSpec {
                topic_name: String::new(),
                partitions,
                config: kaas_operator_api::KafkaTopicConfig::default(),
                storage: None,
            },
        )
    }

    // A CR whose metadata.name is a synthetic hash and whose real Kafka
    // name lives in spec.topic_name (gh #86 shape for non-RFC-1123 names).
    fn topic_with_spec_name(
        meta_name: &str,
        kafka_name: &str,
        partitions: i32,
    ) -> kaas_operator_api::KafkaTopic {
        kaas_operator_api::KafkaTopic::new(
            meta_name,
            kaas_operator_api::KafkaTopicSpec {
                topic_name: kafka_name.to_owned(),
                partitions,
                config: kaas_operator_api::KafkaTopicConfig::default(),
                storage: None,
            },
        )
    }

    /// Drives `handle_topic_event` over a script of events and returns
    /// the names passed to `on_delete`, in order.
    fn deletes_from(events: Vec<Event<kaas_operator_api::KafkaTopic>>) -> Vec<String> {
        let deleted = std::cell::RefCell::new(Vec::new());
        let mut state = TopicWatchState::default();
        {
            let on_apply = |_: TopicApply<'_>| {};
            let on_delete = |n: &str| {
                deleted.borrow_mut().push(n.to_owned());
                std::future::ready(())
            };
            for evt in events {
                block_on(handle_topic_event(&on_apply, &on_delete, evt, &mut state));
            }
        }
        deleted.into_inner()
    }

    /// Drives `handle_topic_event` and returns the (name, partitions)
    /// pairs passed to `on_apply`, in order.
    fn applies_from(events: Vec<Event<kaas_operator_api::KafkaTopic>>) -> Vec<(String, i32)> {
        let applied = std::cell::RefCell::new(Vec::new());
        let mut state = TopicWatchState::default();
        {
            let on_apply =
                |a: TopicApply<'_>| applied.borrow_mut().push((a.name.to_owned(), a.partitions));
            let on_delete = |_: &str| std::future::ready(());
            for evt in events {
                block_on(handle_topic_event(&on_apply, &on_delete, evt, &mut state));
            }
        }
        applied.into_inner()
    }

    #[test]
    fn registers_synthetic_named_topic_under_effective_kafka_name() {
        // gh #219: a non-RFC-1123 Streams internal topic has a synthetic
        // metadata.name (kaas-topic-<hash>) with the real name in
        // spec.topic_name. The watch must register/retract it under the
        // REAL name (what clients ask Metadata for), not the hash — else
        // Metadata reports unknown partitions and Streams loops forever.
        let real = "app-KSTREAM-AGGREGATE-STATE-STORE-0000000003-repartition";
        let applied = applies_from(vec![Event::Apply(topic_with_spec_name(
            "kaas-topic-deadbeef",
            real,
            4,
        ))]);
        assert_eq!(applied, vec![(real.to_owned(), 4)]);
        let deleted = deletes_from(vec![Event::Delete(topic_with_spec_name(
            "kaas-topic-deadbeef",
            real,
            4,
        ))]);
        assert_eq!(deleted, vec![real.to_owned()]);
    }

    #[test]
    fn relist_retracts_topics_deleted_while_disconnected() {
        // t1 + t2 seen, then a relist that only carries t1 — t2 was
        // deleted while the watch was down, so no Delete ever arrives.
        let deleted = deletes_from(vec![
            Event::Apply(topic("t1", 1)),
            Event::Apply(topic("t2", 1)),
            Event::Init,
            Event::InitApply(topic("t1", 1)),
            Event::InitDone,
        ]);
        assert_eq!(deleted, vec!["t2".to_owned()]);
    }

    #[test]
    fn first_relist_retracts_nothing() {
        let deleted = deletes_from(vec![
            Event::Init,
            Event::InitApply(topic("t1", 1)),
            Event::InitApply(topic("t2", 1)),
            Event::InitDone,
        ]);
        assert!(deleted.is_empty(), "got {deleted:?}");
    }

    #[test]
    fn explicit_delete_is_not_repeated_on_next_relist() {
        let deleted = deletes_from(vec![
            Event::Apply(topic("t1", 1)),
            Event::Delete(topic("t1", 1)),
            Event::Init,
            Event::InitDone,
        ]);
        assert_eq!(deleted, vec!["t1".to_owned()]);
    }

    #[test]
    fn init_done_without_init_retracts_nothing() {
        // A relist cut short by a stream restart must not retract the
        // topics it never finished enumerating.
        let deleted = deletes_from(vec![
            Event::Apply(topic("t1", 1)),
            Event::Apply(topic("t2", 1)),
            Event::InitDone,
        ]);
        assert!(deleted.is_empty(), "got {deleted:?}");
    }

    /// gh #241: `status.topicId` reaches the callback, and an *unset*
    /// one arrives as `None` rather than `Some("")`. The registry turns
    /// that None into "CR seen, unstamped", which is what makes the
    /// engine wait for the operator's reclaim instead of opening a
    /// directory it is about to rename aside.
    #[test]
    fn apply_carries_the_status_topic_id() {
        let seen = std::cell::RefCell::new(Vec::new());
        let mut state = TopicWatchState::default();
        {
            let on_apply = |a: TopicApply<'_>| {
                seen.borrow_mut()
                    .push((a.name.to_owned(), a.topic_id.map(str::to_owned)));
            };
            let on_delete = |_: &str| std::future::ready(());

            let unstamped = topic("t1", 1);
            let mut stamped = topic("t2", 1);
            stamped.status = Some(kaas_operator_api::KafkaTopicStatus {
                topic_id: "uuid-1".to_owned(),
                ..Default::default()
            });
            // An explicitly-empty status id is still "unstamped".
            let mut empty = topic("t3", 1);
            empty.status = Some(kaas_operator_api::KafkaTopicStatus::default());

            for evt in [
                Event::Apply(unstamped),
                Event::Apply(stamped),
                Event::Apply(empty),
            ] {
                block_on(handle_topic_event(&on_apply, &on_delete, evt, &mut state));
            }
        }
        assert_eq!(
            seen.into_inner(),
            vec![
                ("t1".to_owned(), None),
                ("t2".to_owned(), Some("uuid-1".to_owned())),
                ("t3".to_owned(), None),
            ]
        );
    }

    /// gh #241: `synced` latches on the first completed list and never
    /// clears. Until it does, a topic missing from the registry means
    /// "we haven't looked", which is what lets a broker with an
    /// unreachable API server keep serving what's on disk.
    #[test]
    fn synced_latches_on_the_first_completed_list() {
        let mut state = TopicWatchState::default();
        let on_apply = |_: TopicApply<'_>| {};
        let on_delete = |_: &str| std::future::ready(());

        // A plain Apply, with no list around it, proves nothing.
        block_on(handle_topic_event(
            &on_apply,
            &on_delete,
            Event::Apply(topic("t1", 1)),
            &mut state,
        ));
        assert!(!state.synced, "an Apply alone is not a completed list");

        // An InitDone with no Init is a cut-short relist — also not a
        // completed list (same guard that keeps it from retracting).
        block_on(handle_topic_event(
            &on_apply,
            &on_delete,
            Event::InitDone,
            &mut state,
        ));
        assert!(!state.synced, "InitDone without Init must not latch");

        for e in [
            Event::Init,
            Event::InitApply(topic("t1", 1)),
            Event::InitDone,
        ] {
            block_on(handle_topic_event(&on_apply, &on_delete, e, &mut state));
        }
        assert!(state.synced);

        // A later stream restart must not un-sync it.
        block_on(handle_topic_event(
            &on_apply,
            &on_delete,
            Event::Init,
            &mut state,
        ));
        assert!(state.synced, "a fresh relist must not clear synced");
    }

    #[test]
    fn relist_keeps_surviving_topics_tracked() {
        // t1 survives two relists; only t2 is ever retracted.
        let deleted = deletes_from(vec![
            Event::Apply(topic("t1", 1)),
            Event::Apply(topic("t2", 1)),
            Event::Init,
            Event::InitApply(topic("t1", 1)),
            Event::InitDone,
            Event::Init,
            Event::InitApply(topic("t1", 1)),
            Event::InitDone,
        ]);
        assert_eq!(deleted, vec!["t2".to_owned()]);
    }

    /// gh #331: the next event waits for a delete's callback, both a plain delete
    /// and a relist's retraction. The offset purge rides that callback, and it
    /// must not race a recreate's commits: it would erase them, and the new
    /// incarnation's consumer would read -1 and reset.
    #[test]
    fn the_next_event_waits_for_a_delete_to_finish() {
        let cases = [
            (None, Event::Delete(topic("t", 1))),
            (Some(Event::Init), Event::InitDone),
        ];
        for (before, event) in cases {
            let (release, released) = futures::channel::oneshot::channel::<()>();
            let released = released.shared();
            let on_apply = |_: TopicApply<'_>| {};
            let on_delete = |_: &str| {
                let released = released.clone();
                async move {
                    let _ = released.await;
                }
            };
            let mut state = TopicWatchState::default();
            let setup = std::iter::once(Event::Apply(topic("t", 1))).chain(before);
            for e in setup {
                block_on(handle_topic_event(&on_apply, &on_delete, e, &mut state));
            }

            let mut handling =
                Box::pin(handle_topic_event(&on_apply, &on_delete, event, &mut state));
            assert!(
                handling.as_mut().now_or_never().is_none(),
                "the watch moved on before the delete callback finished"
            );
            release.send(()).expect("the callback is still waiting");
            block_on(handling);
        }
    }
}
