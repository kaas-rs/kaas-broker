//! Heartbeat handler (key 12).
//!
//! Decodes the codec request, dispatches to
//! [`Manager::heartbeat`] when a coord manager is installed, and
//! returns the wire-level error code on the response. With no
//! coord manager (Phase-3/4 dev mode) the handler returns
//! `NOT_COORDINATOR` (16) so the client retries via
//! `FindCoordinator`.
//!
//! Authorization (gh #268): `Read` on the group, before coordinator
//! routing — `GROUP_AUTHORIZATION_FAILED` (30) otherwise.
//!
//! [`Manager::heartbeat`]: kaas_coordinator::Manager::heartbeat

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::heartbeat;
use kaas_coordinator::HeartbeatRequest;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct HeartbeatHandler {
    broker: Arc<Broker>,
}

impl HeartbeatHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for HeartbeatHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = heartbeat::decode_request(&mut body, version)?;
        let authorized = self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::group(&req.group_id),
            Operation::Read,
        );
        let error_code = match self.broker.coord_manager() {
            _ if !authorized => ERR_GROUP_AUTHZ_FAILED,
            None => ERR_NOT_COORDINATOR,
            Some(mgr) => mgr.heartbeat(
                &req.group_id,
                HeartbeatRequest {
                    member_id: req.member_id,
                    generation_id: req.generation_id,
                    group_instance_id: req.group_instance_id,
                },
            ),
        };
        let resp = heartbeat::Response {
            throttle_time_ms: 0,
            error_code,
        };
        let mut out = BytesMut::new();
        heartbeat::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
