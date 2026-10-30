//! JoinGroup handler (key 11).
//!
//! Translates the codec request into [`kaas_coordinator::JoinRequest`],
//! dispatches to [`Manager::join_group`], and encodes the
//! [`JoinOutcome`] back onto the wire. Without a coord manager
//! installed, returns `NOT_COORDINATOR` (16).
//!
//! [`Manager::join_group`]: kaas_coordinator::Manager::join_group
//!
//! Authorization (gh #268): `Read` on the group, before coordinator
//! routing — `GROUP_AUTHORIZATION_FAILED` (30) otherwise. Ungated, any
//! principal could force a rebalance on any group.
//!
//! [`JoinOutcome`]: kaas_coordinator::JoinOutcome

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::join_group;
use kaas_coordinator::{JoinRequest, ProtocolMetadata};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct JoinGroupHandler {
    broker: Arc<Broker>,
}

impl JoinGroupHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for JoinGroupHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = join_group::decode_request(&mut body, version)?;
        let client_id = conn
            .lock()
            .principal
            .as_ref()
            .map(|p| p.name.clone())
            .unwrap_or_default();

        let authorized = self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::group(&req.group_id),
            Operation::Read,
        );

        let resp = match self.broker.coord_manager() {
            _ if !authorized => error_response(req.member_id, ERR_GROUP_AUTHZ_FAILED),
            None => error_response(req.member_id, ERR_NOT_COORDINATOR),
            Some(mgr) => {
                let cr = JoinRequest {
                    member_id: req.member_id,
                    group_instance_id: req.group_instance_id,
                    session_timeout_ms: req.session_timeout_ms,
                    rebalance_timeout_ms: req.rebalance_timeout_ms,
                    protocol_type: req.protocol_type.clone(),
                    protocols: req
                        .protocols
                        .into_iter()
                        .map(|p| ProtocolMetadata {
                            name: p.name,
                            metadata: p.metadata,
                        })
                        .collect(),
                    version,
                    client_id,
                    client_host: conn.lock().peer_addr.to_string(),
                };
                let out = mgr.join_group(&req.group_id, cr).await;
                join_group::Response {
                    throttle_time_ms: 0,
                    error_code: out.error_code,
                    generation_id: out.generation_id,
                    protocol_type: out.protocol_type,
                    protocol_name: out.protocol_name,
                    leader: out.leader,
                    skip_assignment: false,
                    member_id: out.member_id,
                    members: out
                        .members
                        .into_iter()
                        .map(|m| join_group::JoinGroupMember {
                            member_id: m.member_id,
                            group_instance_id: m.group_instance_id,
                            metadata: m.metadata,
                        })
                        .collect(),
                }
            }
        };
        let mut out = BytesMut::new();
        join_group::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn error_response(member_id: String, error_code: i16) -> join_group::Response {
    join_group::Response {
        throttle_time_ms: 0,
        error_code,
        generation_id: -1,
        protocol_type: String::new(),
        protocol_name: String::new(),
        leader: String::new(),
        skip_assignment: false,
        member_id,
        members: Vec::new(),
    }
}
