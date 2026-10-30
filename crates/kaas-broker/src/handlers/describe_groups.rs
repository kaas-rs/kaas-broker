//! DescribeGroups handler (key 15).
//!
//! Maps the Manager's per-group snapshot onto the codec response
//! shape. Groups this broker doesn't coordinate get the per-group
//! `NOT_COORDINATOR` (16) error.
//!
//! Authorization (gh #268): `Describe` on each group, checked before
//! the coordinator lookup. A denied group answers
//! `GROUP_AUTHORIZATION_FAILED` (30) with no state or members.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::describe_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct DescribeGroupsHandler {
    broker: Arc<Broker>,
}

impl DescribeGroupsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DescribeGroupsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_groups::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        let mgr = self.broker.coord_manager();
        let groups: Vec<describe_groups::DescribedGroup> = req
            .groups
            .into_iter()
            .map(|id| {
                if !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::group(&id),
                    Operation::Describe,
                ) {
                    return error_group(id, ERR_GROUP_AUTHZ_FAILED);
                }
                let snap = mgr
                    .as_ref()
                    .and_then(|m| m.describe_groups(std::slice::from_ref(&id)).pop().flatten());
                match snap {
                    None => error_group(id, ERR_NOT_COORDINATOR),
                    Some(s) => describe_groups::DescribedGroup {
                        error_code: 0,
                        group_id: s.id,
                        group_state: s.state.to_owned(),
                        protocol_type: s.protocol_type,
                        protocol_data: s.protocol_name,
                        members: s
                            .members
                            .into_iter()
                            .map(|m| describe_groups::DescribedGroupMember {
                                member_id: m.member_id,
                                group_instance_id: m.group_instance_id,
                                client_id: m.client_id,
                                client_host: String::new(),
                                member_metadata: Bytes::new(),
                                member_assignment: m.assignment,
                            })
                            .collect(),
                        authorized_operations: 0,
                    },
                }
            })
            .collect();
        let resp = describe_groups::Response {
            throttle_time_ms: 0,
            groups,
        };
        let mut out = BytesMut::new();
        describe_groups::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

fn error_group(group_id: String, error_code: i16) -> describe_groups::DescribedGroup {
    describe_groups::DescribedGroup {
        error_code,
        group_id,
        group_state: String::new(),
        protocol_type: String::new(),
        protocol_data: String::new(),
        members: Vec::new(),
        authorized_operations: 0,
    }
}
