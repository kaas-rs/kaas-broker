//! DescribeCluster handler — API key 60 (KIP-700).
//!
//! What `AdminClient.describeCluster()` calls: cluster id, controller,
//! and the live broker set, without the per-topic payload Metadata
//! drags along. `kafka-cluster.sh cluster-id`, `kafka-configs.sh
//! --describe --entity-type brokers`, and most UIs' cluster panes land
//! here; before this key was registered they fell back to Metadata,
//! which answers the same three facts at the cost of every topic in the
//! registry.
//!
//! The broker rows come from the same catalog Metadata advertises
//! (`crate::listener_advert`) — per-listener port, peers at their
//! stable FQDN — because a client that reached one API on the authed
//! listener must not be handed the anonymous port by the other
//! (gh #125).
//!
//! ## Fenced brokers (KIP-1073, v2)
//!
//! A fenced broker is registered but not serving — its pod exists and
//! its EndpointSlice entry is still there, but it is not Ready. Those
//! rows are omitted unless a v2 request asks for them, matching
//! Apache and matching Metadata, which never advertises them. Asking
//! for them is the only way to tell a degraded cluster from a smaller
//! one. Self is never reported fenced: this broker is answering.
//!
//! ## Endpoint type (KIP-919)
//!
//! v1 requests name the endpoint type they want: `1` = brokers,
//! `2` = controllers. kaas serves broker endpoints only — there is no
//! controller endpoint to describe, since controller election runs on a
//! K8s Lease rather than a KRaft quorum (a documented non-goal) — so a
//! request for `2` gets `MISMATCHED_ENDPOINT_TYPE` and anything else
//! `UNSUPPORTED_ENDPOINT_TYPE`, both mirroring Apache's
//! `AuthHelper.computeDescribeClusterResponse`. v0 carries no such
//! field and decodes to the BROKER default, so neither error can reach
//! a v0 client.
//!
//! ## Authorization
//!
//! The API itself is not gated — Apache answers the broker list to any
//! authenticated principal, and a client that can't discover the
//! cluster can't do anything else either. Only the optional
//! `ClusterAuthorizedOperations` bitfield consults the authorizer, and
//! only when the client asked for it: `Describe` on the cluster
//! resource gates the field, then each supported operation is
//! evaluated in turn.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Principal, Resource};
use kaas_codec::api::acl_types::operation as acl_op;
use kaas_codec::api::describe_cluster::{self, endpoint_type, NOT_REQUESTED};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use crate::broker::Broker;
use crate::cli::ListenerEntry;
use crate::listener_advert::{advertised_brokers, controller_id, ListenerAdverts};

const ERR_MISMATCHED_ENDPOINT_TYPE: i16 = 114;
const ERR_UNSUPPORTED_ENDPOINT_TYPE: i16 = 115;

/// Cluster-scoped operations kaas can actually decide, paired with the
/// wire `AclOperation` code whose bit they set.
///
/// Apache's supported set for the CLUSTER resource also carries
/// `ClusterAction` and `IdempotentWrite`. kaas models neither: there is
/// no Kafka-RPC inter-broker surface for `ClusterAction` to guard
/// (peers talk gRPC heartbeats), and idempotent produce is gated by
/// `Write` on the topic, not by a cluster-level grant. Reporting a bit
/// the authorizer can't evaluate would be a guess, so those two are
/// left clear.
const CLUSTER_OPS: &[(Operation, i8)] = &[
    (Operation::Create, acl_op::CREATE),
    (Operation::Alter, acl_op::ALTER),
    (Operation::Describe, acl_op::DESCRIBE),
    (Operation::DescribeConfigs, acl_op::DESCRIBE_CONFIGS),
    (Operation::AlterConfigs, acl_op::ALTER_CONFIGS),
];

#[derive(Debug)]
pub struct DescribeClusterHandler {
    broker: Arc<Broker>,
    listeners: ListenerAdverts,
}

impl DescribeClusterHandler {
    pub fn new(broker: Arc<Broker>, listeners: &[ListenerEntry]) -> Self {
        Self {
            broker,
            listeners: ListenerAdverts::new(listeners),
        }
    }

    /// The 32-bit `AclOperation` bitfield for this principal on the
    /// cluster resource.
    ///
    /// Three distinct answers, and clients read them differently:
    /// [`NOT_REQUESTED`] (`i32::MIN`) when the client didn't ask, `0`
    /// when it asked but lacks `Describe` on the cluster, and the
    /// computed field otherwise. Collapsing the first two would tell a
    /// client that never asked that it is authorized for nothing.
    fn authorized_operations(&self, principal: &Principal, requested: bool) -> i32 {
        if !requested {
            return NOT_REQUESTED;
        }
        let cluster = Resource::cluster();
        if !self
            .broker
            .authorizer
            .authorize(principal, &cluster, Operation::Describe)
        {
            return 0;
        }
        // Each op the principal lacks is a clear bit, not a denial, so
        // the bitfield checks quietly (Apache's `authorizedOperations`).
        CLUSTER_OPS
            .iter()
            .filter(|(op, _)| {
                self.broker
                    .authorizer
                    .authorize_quietly(principal, &cluster, *op)
            })
            .fold(0, |bits, (_, code)| bits | (1 << code))
    }
}

#[async_trait]
impl Handler for DescribeClusterHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_cluster::decode_request(&mut body, version)?;
        let (listener_name, principal) = {
            let c = conn.lock();
            (
                c.listener_name.clone(),
                c.principal.clone().unwrap_or_else(Principal::anonymous),
            )
        };

        // kaas serves broker endpoints only; the request's type is
        // answered before anything else is computed.
        let resp = match req.endpoint_type {
            endpoint_type::BROKER => {
                let advert = self.listeners.for_listener(&listener_name);
                describe_cluster::Response {
                    cluster_id: self.broker.cluster_id.clone(),
                    controller_id: controller_id(&self.broker),
                    brokers: advertised_brokers(&self.broker, &advert)
                        .into_iter()
                        // KIP-1073: fenced rows only when asked for.
                        // The flag is v2-only, so a v0/v1 client
                        // decodes `false` and gets Apache's default.
                        .filter(|b| req.include_fenced_brokers || !b.fenced)
                        .map(|b| describe_cluster::Broker {
                            broker_id: b.node_id,
                            host: b.host,
                            port: b.port,
                            rack: None,
                            is_fenced: b.fenced,
                        })
                        .collect(),
                    cluster_authorized_operations: self.authorized_operations(
                        &principal,
                        req.include_cluster_authorized_operations,
                    ),
                    ..Default::default()
                }
            }
            endpoint_type::CONTROLLER => error_response(
                ERR_MISMATCHED_ENDPOINT_TYPE,
                "This endpoint is of type BROKER, but the request asked for CONTROLLER",
            ),
            other => error_response(
                ERR_UNSUPPORTED_ENDPOINT_TYPE,
                &format!("Unsupported endpoint type {other}"),
            ),
        };

        let mut out = BytesMut::new();
        describe_cluster::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

/// A top-level error carries no cluster id, controller, or brokers —
/// Apache returns the bare error shape, and a client that reads the
/// error code never looks at the rest.
fn error_response(error_code: i16, message: &str) -> describe_cluster::Response {
    describe_cluster::Response {
        error_code,
        error_message: Some(message.to_owned()),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::broker::{BrokerNode, ClusterBrokerView};
    use crate::topic_registry::TopicRegistry;
    use kaas_auth::{Authorizer, NoQuotaChecker};
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn conn(listener: &str) -> Mutex<ConnState> {
        Mutex::new(ConnState::new(
            listener,
            SocketAddr::from_str("127.0.0.1:9092").unwrap(),
        ))
    }

    fn listeners() -> Vec<ListenerEntry> {
        vec![
            ListenerEntry {
                name: "internal".to_owned(),
                addr: "0.0.0.0:9092".to_owned(),
                advertised_host: None,
                tls: None,
                authentication_type: None,
                oauth: None,
            },
            ListenerEntry {
                name: "authed".to_owned(),
                addr: "0.0.0.0:9095".to_owned(),
                advertised_host: Some("kaas-0.kaas-brokers".to_owned()),
                tls: None,
                authentication_type: None,
                oauth: None,
            },
        ]
    }

    fn broker() -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "kaas-test",
            0,
        ))
    }

    fn broker_with_auth(authorizer: Arc<dyn Authorizer>) -> Arc<Broker> {
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        Arc::new(Broker::with_auth(
            engine,
            Arc::new(TopicRegistry::new()),
            "kaas-test",
            0,
            authorizer,
            Arc::new(NoQuotaChecker),
        ))
    }

    fn request(include_ops: bool, ep: i8, version: i16) -> Bytes {
        request_full(include_ops, ep, false, version)
    }

    fn request_full(include_ops: bool, ep: i8, fenced: bool, version: i16) -> Bytes {
        let mut w = BytesMut::new();
        describe_cluster::encode_request(
            &mut w,
            &describe_cluster::Request {
                include_cluster_authorized_operations: include_ops,
                endpoint_type: ep,
                include_fenced_brokers: fenced,
            },
            version,
        )
        .unwrap();
        w.freeze()
    }

    async fn describe(
        h: &DescribeClusterHandler,
        listener: &str,
        include_ops: bool,
        ep: i8,
        version: i16,
    ) -> describe_cluster::Response {
        let out = h
            .handle(&conn(listener), version, request(include_ops, ep, version))
            .await
            .unwrap();
        describe_cluster::decode_response(&mut out.freeze(), version).unwrap()
    }

    #[derive(Debug)]
    struct StaticView(Vec<BrokerNode>);
    impl ClusterBrokerView for StaticView {
        fn brokers(&self) -> Vec<BrokerNode> {
            self.0.clone()
        }
    }

    #[tokio::test]
    async fn reports_cluster_id_and_self_in_dev_mode() {
        let h = DescribeClusterHandler::new(broker(), &listeners());
        let resp = describe(&h, "internal", false, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.cluster_id, "kaas-test");
        // No coordinator wired: self is the controller.
        assert_eq!(resp.controller_id, 0);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 0);
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.endpoint_type, endpoint_type::BROKER);
    }

    #[tokio::test]
    async fn advertises_the_live_broker_set_on_the_requesting_listener() {
        let b = broker();
        b.install_broker_view(Arc::new(StaticView(vec![
            BrokerNode {
                node_id: 0,
                host: "kaas-0.internal".to_owned(),
                port: 9092,
                fenced: false,
            },
            BrokerNode {
                node_id: 1,
                host: "kaas-1.kaas-brokers".to_owned(),
                port: 9092,
                fenced: false,
            },
        ])));
        let h = DescribeClusterHandler::new(b, &listeners());

        // Same rule as Metadata (gh #125): every row carries the port
        // of the listener the request arrived on, self keeps its own
        // advertised host, peers keep their FQDN.
        let resp = describe(&h, "authed", false, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.brokers.len(), 2);
        assert!(resp.brokers.iter().all(|b| b.port == 9095));
        assert_eq!(resp.brokers[0].host, "kaas-0.kaas-brokers");
        assert_eq!(resp.brokers[1].host, "kaas-1.kaas-brokers");
    }

    #[tokio::test]
    async fn v0_answers_without_an_endpoint_type() {
        let h = DescribeClusterHandler::new(broker(), &listeners());
        let resp = describe(&h, "internal", false, endpoint_type::BROKER, 0).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.cluster_id, "kaas-test");
    }

    #[tokio::test]
    async fn controller_endpoint_is_a_mismatch() {
        let h = DescribeClusterHandler::new(broker(), &listeners());
        let resp = describe(&h, "internal", false, endpoint_type::CONTROLLER, 1).await;
        assert_eq!(resp.error_code, ERR_MISMATCHED_ENDPOINT_TYPE);
        assert!(resp.error_message.is_some());
        assert!(resp.brokers.is_empty());
    }

    #[tokio::test]
    async fn unknown_endpoint_type_is_unsupported() {
        let h = DescribeClusterHandler::new(broker(), &listeners());
        let resp = describe(&h, "internal", false, endpoint_type::UNKNOWN, 1).await;
        assert_eq!(resp.error_code, ERR_UNSUPPORTED_ENDPOINT_TYPE);
        let resp = describe(&h, "internal", false, 7, 1).await;
        assert_eq!(resp.error_code, ERR_UNSUPPORTED_ENDPOINT_TYPE);
    }

    #[tokio::test]
    async fn authorized_operations_only_when_asked() {
        let h = DescribeClusterHandler::new(broker(), &listeners());

        // Not asked → the i32::MIN sentinel, which the client reads
        // back as "unknown" rather than "nothing allowed".
        let resp = describe(&h, "internal", false, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.cluster_authorized_operations, NOT_REQUESTED);

        // Asked, allow-all authorizer (the default) → every op kaas
        // can decide, and nothing it can't.
        let resp = describe(&h, "internal", true, endpoint_type::BROKER, 1).await;
        let expected = CLUSTER_OPS.iter().fold(0, |bits, (_, c)| bits | (1 << c));
        assert_eq!(resp.cluster_authorized_operations, expected);
        for code in [acl_op::CLUSTER_ACTION, acl_op::IDEMPOTENT_WRITE] {
            assert_eq!(
                resp.cluster_authorized_operations & (1 << code),
                0,
                "op {code} isn't modelled and must stay clear"
            );
        }
    }

    #[tokio::test]
    async fn denied_describe_yields_an_empty_bitfield_not_the_sentinel() {
        #[derive(Debug)]
        struct DenyAll;
        impl Authorizer for DenyAll {
            fn authorize(&self, _: &Principal, _: &Resource, _: Operation) -> bool {
                false
            }
        }
        let h = DescribeClusterHandler::new(broker_with_auth(Arc::new(DenyAll)), &listeners());
        let resp = describe(&h, "internal", true, endpoint_type::BROKER, 1).await;
        // Still a successful describe — only the bitfield is empty.
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.cluster_authorized_operations, 0);
        assert_eq!(resp.brokers.len(), 1);
    }

    #[tokio::test]
    async fn bitfield_reflects_partial_grants() {
        #[derive(Debug)]
        struct DescribeOnly;
        impl Authorizer for DescribeOnly {
            fn authorize(&self, _: &Principal, _: &Resource, op: Operation) -> bool {
                matches!(op, Operation::Describe)
            }
        }
        let h = DescribeClusterHandler::new(broker_with_auth(Arc::new(DescribeOnly)), &listeners());
        let resp = describe(&h, "internal", true, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.cluster_authorized_operations, 1 << acl_op::DESCRIBE);
    }

    /// An op the principal lacks is a clear bit, not a denial: only the
    /// `Describe` gate in front of the bitfield may report one.
    #[tokio::test]
    async fn the_bitfield_reports_no_denials() {
        #[derive(Debug, Default)]
        struct DescribeOnlyAudited {
            reported: Mutex<Vec<Operation>>,
        }
        impl Authorizer for DescribeOnlyAudited {
            fn authorize(&self, _: &Principal, _: &Resource, op: Operation) -> bool {
                let allowed = matches!(op, Operation::Describe);
                if !allowed {
                    self.reported.lock().push(op);
                }
                allowed
            }
            fn authorize_quietly(&self, _: &Principal, _: &Resource, op: Operation) -> bool {
                matches!(op, Operation::Describe)
            }
        }
        let authz = Arc::new(DescribeOnlyAudited::default());
        let h = DescribeClusterHandler::new(broker_with_auth(authz.clone()), &listeners());
        let resp = describe(&h, "internal", true, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.cluster_authorized_operations, 1 << acl_op::DESCRIBE);
        assert_eq!(*authz.reported.lock(), []);
    }

    // --- gh #249: fenced brokers -----------------------------------

    fn broker_with_fenced_peer() -> Arc<Broker> {
        let b = broker();
        b.install_broker_view(Arc::new(StaticView(vec![
            BrokerNode {
                node_id: 0,
                host: "kaas-0.internal".to_owned(),
                port: 9092,
                fenced: false,
            },
            BrokerNode {
                node_id: 1,
                host: "kaas-1.kaas-brokers".to_owned(),
                port: 9092,
                fenced: true,
            },
        ])));
        b
    }

    #[tokio::test]
    async fn fenced_brokers_are_omitted_unless_asked_for() {
        let h = DescribeClusterHandler::new(broker_with_fenced_peer(), &listeners());

        // v2 without the flag: Apache's default, fenced row dropped.
        let out = h
            .handle(
                &conn("internal"),
                2,
                request_full(false, endpoint_type::BROKER, false, 2),
            )
            .await
            .unwrap();
        let resp = describe_cluster::decode_response(&mut out.freeze(), 2).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 0);

        // v2 with the flag: the fenced broker appears, flagged. This
        // is the whole point — without it a degraded cluster is
        // indistinguishable from a smaller one.
        let out = h
            .handle(
                &conn("internal"),
                2,
                request_full(false, endpoint_type::BROKER, true, 2),
            )
            .await
            .unwrap();
        let resp = describe_cluster::decode_response(&mut out.freeze(), 2).unwrap();
        assert_eq!(resp.brokers.len(), 2);
        let fenced = resp.brokers.iter().find(|b| b.broker_id == 1).unwrap();
        assert!(fenced.is_fenced);
        assert!(
            !resp
                .brokers
                .iter()
                .find(|b| b.broker_id == 0)
                .unwrap()
                .is_fenced
        );
    }

    /// A v1 client cannot ask for fenced brokers — the field doesn't
    /// exist in its request — so it must never be handed one.
    #[tokio::test]
    async fn v1_never_sees_a_fenced_broker() {
        let h = DescribeClusterHandler::new(broker_with_fenced_peer(), &listeners());
        let resp = describe(&h, "internal", false, endpoint_type::BROKER, 1).await;
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 0);
    }

    /// Self answers the request, so it is never fenced — even if the
    /// endpoint view says this pod is not ready (a readiness blip, or
    /// the boot window before takeover completes).
    #[tokio::test]
    async fn self_is_never_reported_fenced() {
        let b = broker();
        b.install_broker_view(Arc::new(StaticView(vec![BrokerNode {
            node_id: 0,
            host: "kaas-0.internal".to_owned(),
            port: 9092,
            fenced: true,
        }])));
        let h = DescribeClusterHandler::new(b, &listeners());
        let out = h
            .handle(
                &conn("internal"),
                2,
                request_full(false, endpoint_type::BROKER, true, 2),
            )
            .await
            .unwrap();
        let resp = describe_cluster::decode_response(&mut out.freeze(), 2).unwrap();
        assert_eq!(resp.brokers.len(), 1);
        assert!(!resp.brokers[0].is_fenced);
    }
}
