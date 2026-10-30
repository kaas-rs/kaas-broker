//! Live map of broker endpoints derived from `EndpointSlice` events.
//!
//! Two layers:
//!
//! 1. [`BrokerRegistry`] — the pure-state map keyed on
//!    StatefulSet ordinal. Apply events via
//!    [`BrokerRegistry::apply_slice`] / [`BrokerRegistry::delete_slice`].
//!    No kube dep — fully unit-testable.
//! 2. [`crate::kube_watchers::watch_endpoints`] — the kube-bound
//!    pump that consumes `kube::runtime::watcher` events and calls
//!    into the registry. Lives behind the `kube-watchers` feature.
//!
//! gh #97 / gh #128: each broker advertises the StatefulSet pod's
//! FQDN (e.g. `"kaas-1.kaas-headless.kaas.svc.cluster.local"`)
//! built from [`crate::identity::DnsConfig`] — NOT the pod IP from
//! `EndpointSlice.Endpoints[].Addresses[0]`, which would break under
//! pod restart. Tests that pass an empty `DnsConfig` fall back to
//! the raw address.

use std::collections::HashMap;

use parking_lot::RwLock;

use crate::identity::{parse_ordinal, DnsConfig};

/// One broker's endpoint.
///
/// `ready == false` is the **fenced** state (gh #249): the broker is
/// registered — its pod exists and the EndpointSlice still lists it —
/// but it is not serving. Metadata omits these rows (Apache omits
/// fenced brokers too); DescribeCluster v2 reports them with
/// `IsFenced = true`, which is the whole point of keeping them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerEndpoint {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub ready: bool,
}

/// One ready-or-not endpoint extracted from an `EndpointSlice` —
/// the kube-bound watcher converts each `Endpoints[]` row into one
/// of these before calling into [`BrokerRegistry::apply_slice`].
/// Splitting this out keeps the registry kube-free.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EndpointSliceEntry {
    /// Pod hostname (`EndpointSlice.Endpoints[].Hostname`), e.g.
    /// `"kaas-0"`. Used to extract the ordinal.
    pub hostname: String,
    /// First `Addresses[]` entry. Used as a fallback host when
    /// `DnsConfig` is unset.
    pub address: String,
    /// `EndpointSlice.Endpoints[].Conditions.Ready`.
    pub ready: bool,
}

/// A whole slice's worth of entries + the Kafka port the slice
/// advertises. Mirrors the kube `EndpointSlice` shape one-to-one.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct EndpointSliceData {
    /// `metadata.name` of the slice. A Service's endpoints may be
    /// sharded across several slices, so deregistration
    /// ([`BrokerRegistry::apply_slice`]) is scoped to the slice that
    /// owns the ordinal — otherwise one slice's update would evict
    /// every broker listed in the others.
    pub name: String,
    pub entries: Vec<EndpointSliceEntry>,
    pub kafka_port: Option<i32>,
}

#[derive(Debug, Default)]
struct Inner {
    brokers: HashMap<i32, BrokerEndpoint>,
    /// Which slice each ordinal was last seen in. See
    /// [`EndpointSliceData::name`].
    slice_of: HashMap<i32, String>,
}

/// Live broker endpoint map. The owning broker always appears at
/// `self.node_id`; peers come and go with `EndpointSlice` events.
pub struct BrokerRegistry {
    self_endpoint: BrokerEndpoint,
    dns: DnsConfig,
    inner: RwLock<Inner>,
}

impl std::fmt::Debug for BrokerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.read();
        f.debug_struct("BrokerRegistry")
            .field("self", &self.self_endpoint)
            .field("brokers", &inner.brokers.len())
            .finish()
    }
}

impl BrokerRegistry {
    pub fn new(self_endpoint: BrokerEndpoint, dns: DnsConfig) -> Self {
        let mut brokers = HashMap::new();
        brokers.insert(self_endpoint.node_id, self_endpoint.clone());
        Self {
            self_endpoint,
            dns,
            inner: RwLock::new(Inner {
                brokers,
                slice_of: HashMap::new(),
            }),
        }
    }

    /// All **registered** endpoints sorted by `node_id`, ready and
    /// fenced alike. Callers that want only servable brokers filter
    /// on `ready` — Metadata does, DescribeCluster v2 does not.
    pub fn all(&self) -> Vec<BrokerEndpoint> {
        let inner = self.inner.read();
        let mut out: Vec<BrokerEndpoint> = inner.brokers.values().cloned().collect();
        out.sort_by_key(|e| e.node_id);
        out
    }

    /// Number of registered brokers currently ready to serve.
    pub fn ready_count(&self) -> usize {
        self.inner
            .read()
            .brokers
            .values()
            .filter(|b| b.ready)
            .count()
    }

    /// Apply an `Added` / `Modified` `EndpointSlice` event.
    ///
    /// Every entry lands in the map keyed on its ordinal (extracted
    /// from `hostname`), **carrying its readiness rather than being
    /// filtered on it** (gh #249). A not-ready endpoint is a broker
    /// that exists but isn't serving — fenced — and dropping it is
    /// what made a degraded cluster look like a smaller one. Callers
    /// that need only servable brokers filter on `ready`.
    ///
    /// Ordinals this slice used to list and no longer does **are**
    /// removed: that is deregistration (scale-down, pod deleted), as
    /// distinct from fencing. The removal is scoped to the slice that
    /// owns the ordinal, so a sharded Service can't have one slice's
    /// update evict brokers listed in another.
    ///
    /// SELF is never touched: not inserted, not downgraded, not
    /// removed. A readiness-probe blip on this pod must not make it
    /// forget its own existence. (Observed live: self-eviction →
    /// controller balanced over an empty set → unassigned all
    /// partitions → the takeover storm failed the next probe too — a
    /// self-sustaining death spiral.)
    pub fn apply_slice(&self, slice: &EndpointSliceData) {
        let port = slice.kafka_port.unwrap_or(self.self_endpoint.port);
        let mut inner = self.inner.write();
        let mut seen: Vec<i32> = Vec::with_capacity(slice.entries.len());
        for ep in &slice.entries {
            let Some(ordinal) = parse_ordinal(&ep.hostname) else {
                continue;
            };
            if ordinal == self.self_endpoint.node_id {
                continue;
            }
            seen.push(ordinal);
            // gh #128: advertise the headless-DNS FQDN when
            // available; fall back to the raw address for
            // tests / dev where `DnsConfig` is empty.
            let host =
                if !self.dns.headless_service.is_empty() && !self.dns.pod_name_pattern.is_empty() {
                    self.dns.fqdn(ordinal)
                } else {
                    ep.address.clone()
                };
            inner.brokers.insert(
                ordinal,
                BrokerEndpoint {
                    node_id: ordinal,
                    host,
                    port,
                    ready: ep.ready,
                },
            );
            inner.slice_of.insert(ordinal, slice.name.clone());
        }
        // Deregister ordinals this slice owned but no longer
        // lists. Scoped by owning slice — see `slice_of`.
        let dropped: Vec<i32> = inner
            .slice_of
            .iter()
            .filter(|(ordinal, owner)| owner.as_str() == slice.name && !seen.contains(ordinal))
            .map(|(ordinal, _)| *ordinal)
            .collect();
        for ordinal in dropped {
            inner.brokers.remove(&ordinal);
            inner.slice_of.remove(&ordinal);
        }
    }

    /// Apply a `Deleted` `EndpointSlice` event. Every entry's
    /// ordinal is deregistered — a deleted slice means those
    /// endpoints are gone, not fenced.
    pub fn delete_slice(&self, slice: &EndpointSliceData) {
        let mut inner = self.inner.write();
        for ep in &slice.entries {
            if let Some(ordinal) = parse_ordinal(&ep.hostname) {
                // Same self-pin as apply_slice.
                if ordinal != self.self_endpoint.node_id {
                    inner.brokers.remove(&ordinal);
                    inner.slice_of.remove(&ordinal);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dns() -> DnsConfig {
        DnsConfig {
            namespace: "kaas".to_owned(),
            headless_service: "kaas-headless".to_owned(),
            pod_name_pattern: "kaas-{ordinal}".to_owned(),
            cluster_domain: "cluster.local".to_owned(),
        }
    }

    fn self_ep() -> BrokerEndpoint {
        BrokerEndpoint {
            node_id: 0,
            host: "kaas-0.kaas-headless.kaas.svc.cluster.local".to_owned(),
            port: 9092,
            ready: true,
        }
    }

    fn slice_with(entries: Vec<EndpointSliceEntry>) -> EndpointSliceData {
        named_slice("kaas-headless-abc", entries)
    }

    fn named_slice(name: &str, entries: Vec<EndpointSliceEntry>) -> EndpointSliceData {
        EndpointSliceData {
            name: name.to_owned(),
            entries,
            kafka_port: Some(9092),
        }
    }

    fn entry(hostname: &str, address: &str, ready: bool) -> EndpointSliceEntry {
        EndpointSliceEntry {
            hostname: hostname.to_owned(),
            address: address.to_owned(),
            ready,
        }
    }

    #[test]
    fn fresh_registry_has_self() {
        let r = BrokerRegistry::new(self_ep(), dns());
        assert_eq!(r.all().len(), 1);
        assert_eq!(r.all()[0], self_ep());
    }

    #[test]
    fn apply_slice_adds_peers_with_fqdn_host() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![
            EndpointSliceEntry {
                hostname: "kaas-1".to_owned(),
                address: "10.0.0.5".to_owned(),
                ready: true,
            },
            EndpointSliceEntry {
                hostname: "kaas-2".to_owned(),
                address: "10.0.0.6".to_owned(),
                ready: true,
            },
        ]));
        let all = r.all();
        assert_eq!(all.len(), 3);
        // Peers use the headless-DNS FQDN, not the raw address.
        assert_eq!(all[1].host, "kaas-1.kaas-headless.kaas.svc.cluster.local");
        assert_eq!(all[2].host, "kaas-2.kaas-headless.kaas.svc.cluster.local");
    }

    /// gh #249: a not-ready endpoint is FENCED, not gone. Dropping
    /// it is what made a degraded cluster look like a smaller one.
    #[test]
    fn not_ready_entries_are_kept_as_fenced() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![entry("kaas-1", "10.0.0.5", true)]));
        assert_eq!((r.all().len(), r.ready_count()), (2, 2));

        r.apply_slice(&slice_with(vec![entry("kaas-1", "10.0.0.5", false)]));
        assert_eq!(
            (r.all().len(), r.ready_count()),
            (2, 1),
            "still registered, no longer ready"
        );
        let peer = r.all().into_iter().find(|e| e.node_id == 1).unwrap();
        assert!(!peer.ready);
        // The host survives the transition, so a fenced broker is
        // still addressable in a DescribeCluster row.
        assert_eq!(peer.host, "kaas-1.kaas-headless.kaas.svc.cluster.local");

        // ...and it comes back ready without a re-add.
        r.apply_slice(&slice_with(vec![entry("kaas-1", "10.0.0.5", true)]));
        assert_eq!((r.all().len(), r.ready_count()), (2, 2));
    }

    /// The other half of the same contract: an ordinal that vanishes
    /// from its slice is DEREGISTERED (scale-down), not fenced
    /// forever. Without this, keeping not-ready entries would leak a
    /// scaled-away broker into every response for the process's life.
    #[test]
    fn entry_absent_from_its_slice_is_deregistered() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![
            entry("kaas-1", "10.0.0.5", true),
            entry("kaas-2", "10.0.0.6", true),
        ]));
        assert_eq!(r.all().len(), 3);

        // Scale 3 -> 2: kaas-2 leaves the slice entirely.
        r.apply_slice(&slice_with(vec![entry("kaas-1", "10.0.0.5", true)]));
        assert_eq!(r.all().len(), 2);
        assert!(r.all().iter().all(|e| e.node_id != 2));
    }

    /// Deregistration is scoped to the slice that owns the ordinal.
    /// A Service's endpoints may be sharded across slices, and a
    /// naive "absent means gone" sweep would have each slice evict
    /// the others' brokers on every update.
    #[test]
    fn sharded_slices_do_not_evict_each_other() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&named_slice(
            "shard-a",
            vec![entry("kaas-1", "10.0.0.5", true)],
        ));
        r.apply_slice(&named_slice(
            "shard-b",
            vec![entry("kaas-2", "10.0.0.6", true)],
        ));
        assert_eq!(r.all().len(), 3);

        // An unrelated update to shard-a must not touch kaas-2.
        r.apply_slice(&named_slice(
            "shard-a",
            vec![entry("kaas-1", "10.0.0.5", false)],
        ));
        assert_eq!(r.all().len(), 3);
        assert!(r.all().iter().any(|e| e.node_id == 2 && e.ready));
    }

    /// Self is pinned through every path: a probe blip on this pod
    /// must not fence or evict it (the gh #208 death spiral).
    #[test]
    fn self_is_never_fenced_or_deregistered() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![entry("kaas-0", "10.0.0.4", false)]));
        let me = r.all().into_iter().find(|e| e.node_id == 0).unwrap();
        assert!(me.ready, "self must stay ready");

        // Absent from its own slice, and even an explicit delete.
        r.apply_slice(&slice_with(vec![entry("kaas-1", "10.0.0.5", true)]));
        assert!(r.all().iter().any(|e| e.node_id == 0 && e.ready));
        r.delete_slice(&slice_with(vec![entry("kaas-0", "10.0.0.4", true)]));
        assert!(r.all().iter().any(|e| e.node_id == 0 && e.ready));
    }

    #[test]
    fn delete_slice_removes_every_ordinal() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "kaas-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        r.delete_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "kaas-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        assert_eq!(r.all().len(), 1);
    }

    #[test]
    fn empty_dns_falls_back_to_raw_address() {
        let empty_dns = DnsConfig {
            namespace: String::new(),
            headless_service: String::new(),
            pod_name_pattern: String::new(),
            cluster_domain: String::new(),
        };
        let r = BrokerRegistry::new(self_ep(), empty_dns);
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "kaas-1".to_owned(),
            address: "10.0.0.5".to_owned(),
            ready: true,
        }]));
        let all = r.all();
        let peer = all.iter().find(|e| e.node_id == 1).unwrap();
        assert_eq!(peer.host, "10.0.0.5");
    }

    #[test]
    fn unparseable_hostnames_are_skipped() {
        let r = BrokerRegistry::new(self_ep(), dns());
        r.apply_slice(&slice_with(vec![EndpointSliceEntry {
            hostname: "noordinal".to_owned(),
            address: "10.0.0.7".to_owned(),
            ready: true,
        }]));
        assert_eq!(r.all().len(), 1);
    }
}
