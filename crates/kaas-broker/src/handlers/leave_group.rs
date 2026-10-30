//! LeaveGroup handler (key 13).
//!
//! Collects member IDs from both v0..=v2 single-member and v3+
//! batch shapes, dispatches to [`Manager::leave_group`], and
//! returns per-member responses. With no coord manager installed,
//! returns the top-level `NOT_COORDINATOR` (16) so the client
//! retries via `FindCoordinator`.
//!
//! Authorization (gh #268): `Read` on the group, before coordinator
//! routing. A denial is the top-level `GROUP_AUTHORIZATION_FAILED`
//! (30) with no per-member results, as Apache's error response has.
//! Ungated, any principal could evict any member it could name.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::leave_group;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct LeaveGroupHandler {
    broker: Arc<Broker>,
}

impl LeaveGroupHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for LeaveGroupHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = leave_group::decode_request(&mut body, version)?;

        // Collect member IDs from both legacy and batch shapes.
        let mut member_ids: Vec<String> = Vec::new();
        if !req.member_id.is_empty() {
            member_ids.push(req.member_id.clone());
        }
        for m in &req.members {
            member_ids.push(m.member_id.clone());
        }

        let authorized = self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::group(&req.group_id),
            Operation::Read,
        );

        let resp = match self.broker.coord_manager() {
            _ if !authorized => leave_group::Response {
                throttle_time_ms: 0,
                error_code: ERR_GROUP_AUTHZ_FAILED,
                members: Vec::new(),
            },
            None => leave_group::Response {
                throttle_time_ms: 0,
                error_code: ERR_NOT_COORDINATOR,
                members: req
                    .members
                    .iter()
                    .map(|m| leave_group::LeaveMemberResponse {
                        member_id: m.member_id.clone(),
                        group_instance_id: m.group_instance_id.clone(),
                        error_code: ERR_NOT_COORDINATOR,
                    })
                    .collect(),
            },
            Some(mgr) => {
                let out = mgr.leave_group(&req.group_id, &member_ids);
                let members_lookup: std::collections::HashMap<String, Option<String>> = req
                    .members
                    .iter()
                    .map(|m| (m.member_id.clone(), m.group_instance_id.clone()))
                    .collect();
                let members_resp: Vec<leave_group::LeaveMemberResponse> = out
                    .members
                    .iter()
                    .filter(|(mid, _)| {
                        // Only include batch-shape entries on the
                        // response; the legacy single-member field
                        // travels at the top level.
                        members_lookup.contains_key(mid)
                    })
                    .map(|(mid, code)| leave_group::LeaveMemberResponse {
                        member_id: mid.clone(),
                        group_instance_id: members_lookup.get(mid).cloned().unwrap_or(None),
                        error_code: *code,
                    })
                    .collect();
                leave_group::Response {
                    throttle_time_ms: 0,
                    error_code: out.group_error,
                    members: members_resp,
                }
            }
        };
        let mut out = BytesMut::new();
        leave_group::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
