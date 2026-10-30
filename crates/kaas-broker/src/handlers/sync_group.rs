//! SyncGroup handler (key 14).
//!
//! Leader publishes per-member assignments; followers park on the
//! group's sync round. The Manager's sync future awaits the
//! leader's delivery (or a rebalance cancel). No coord manager →
//! `NOT_COORDINATOR` (16).
//!
//! Authorization (gh #268): `Read` on the group, before coordinator
//! routing — `GROUP_AUTHORIZATION_FAILED` (30) otherwise. Ungated, a
//! member id was enough to read another principal's assignment.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::sync_group;
use kaas_coordinator::{SyncAssignment, SyncRequest};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct SyncGroupHandler {
    broker: Arc<Broker>,
}

impl SyncGroupHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for SyncGroupHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = sync_group::decode_request(&mut body, version)?;

        let authorized = self.broker.authorizer.authorize(
            &principal_from(conn),
            &Resource::group(&req.group_id),
            Operation::Read,
        );

        let resp = match self.broker.coord_manager() {
            _ if !authorized => error_response(ERR_GROUP_AUTHZ_FAILED),
            None => error_response(ERR_NOT_COORDINATOR),
            Some(mgr) => {
                let cr = SyncRequest {
                    member_id: req.member_id,
                    generation_id: req.generation_id,
                    group_instance_id: req.group_instance_id,
                    protocol_type: req.protocol_type,
                    protocol_name: req.protocol_name,
                    assignments: req
                        .assignments
                        .into_iter()
                        .map(|a| SyncAssignment {
                            member_id: a.member_id,
                            // Per Apache's schema the request-side
                            // `assignment` field is nullable; the
                            // Manager carries non-null `Bytes`, so
                            // collapse `None` to empty bytes (the
                            // leader simply omitted that member's
                            // payload from the round).
                            assignment: a.assignment.unwrap_or_default(),
                        })
                        .collect(),
                };
                let out = mgr.sync_group(&req.group_id, cr).await;
                sync_group::Response {
                    throttle_time_ms: 0,
                    error_code: out.error_code,
                    protocol_type: out.protocol_type,
                    protocol_name: out.protocol_name,
                    assignment: out.assignment,
                }
            }
        };
        let mut out = BytesMut::new();
        sync_group::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn error_response(error_code: i16) -> sync_group::Response {
    sync_group::Response {
        throttle_time_ms: 0,
        error_code,
        protocol_type: String::new(),
        protocol_name: String::new(),
        assignment: Bytes::new(),
    }
}
