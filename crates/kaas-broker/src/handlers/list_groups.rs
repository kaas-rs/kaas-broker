//! ListGroups handler (key 16).
//!
//! Snapshots every group this broker coordinates and emits the
//! `(group_id, protocol_type, group_state)` triple per group.
//! `states_filter` (v4+) is applied client-side to the snapshot.
//!
//! With no coord manager installed, returns an empty list +
//! `error_code = 0`. This mirrors Apache's empty-broker semantics
//! and keeps `kafka-consumer-groups --list` quiet during the boot
//! window.
//!
//! Authorization (gh #268) filters rather than errors, as in Apache:
//! `Describe` on the cluster lists every group; without it, only the
//! groups the principal may `Describe` are listed.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::list_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

#[derive(Debug)]
pub struct ListGroupsHandler {
    broker: Arc<Broker>,
}

impl ListGroupsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for ListGroupsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = list_groups::decode_request(&mut body, version)?;
        let filter = req.states_filter;
        let principal = principal_from(conn);
        let authz = &self.broker.authorizer;
        let cluster_describe =
            authz.authorize(&principal, &Resource::cluster(), Operation::Describe);
        let groups = match self.broker.coord_manager() {
            None => Vec::new(),
            Some(mgr) => mgr
                .list_groups()
                .into_iter()
                .filter(|g| filter.is_empty() || filter.iter().any(|f| f == g.state))
                .filter(|g| {
                    cluster_describe
                        || authz.authorize(&principal, &Resource::group(&g.id), Operation::Describe)
                })
                .map(|g| list_groups::ListedGroup {
                    group_id: g.id,
                    protocol_type: g.protocol_type,
                    group_state: g.state.to_owned(),
                })
                .collect(),
        };
        let resp = list_groups::Response {
            throttle_time_ms: 0,
            error_code: 0,
            groups,
        };
        let mut out = BytesMut::new();
        list_groups::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
