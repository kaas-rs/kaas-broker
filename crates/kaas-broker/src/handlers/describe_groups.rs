//! DescribeGroups handler (key 15).
//!
//! Maps the Manager's per-group snapshot onto the codec response
//! shape. Groups this broker doesn't coordinate get the per-group
//! `NOT_COORDINATOR` (16) error.
//!
//! Authorization (gh #268): `Describe` on each group, checked before
//! the coordinator lookup. A denied group answers
//! `GROUP_AUTHORIZATION_FAILED` (30) with no state or members.
//!
//! `authorized_operations` (v3+, gh #284) is Apache's: `i32::MIN` ("not
//! computed") unless the request sets `include_authorized_operations`,
//! and on every error row; otherwise the bitfield over the GROUP
//! resource's supported operations, {READ, DESCRIBE, DELETE}.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::acl_types::operation as acl_op;
use kaas_codec::api::describe_cluster::NOT_REQUESTED;
use kaas_codec::api::describe_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

/// Apache's supported operations for the GROUP resource
/// (`AclEntry.supportedOperations`), with the `AclOperation` bit each sets
/// in `authorized_operations` (gh #284).
const GROUP_OPS: &[(Operation, i8)] = &[
    (Operation::Read, acl_op::READ),
    (Operation::Delete, acl_op::DELETE),
    (Operation::Describe, acl_op::DESCRIBE),
];

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
        // Each op the principal lacks is a clear bit, not a denial, so the
        // bitfield checks quietly (Apache's `authorizedOperations`).
        let group_ops = |id: &str| -> i32 {
            if !req.include_authorized_operations {
                return NOT_REQUESTED;
            }
            let resource = Resource::group(id);
            GROUP_OPS
                .iter()
                .filter(|(op, _)| {
                    self.broker
                        .authorizer
                        .authorize_quietly(&principal, &resource, *op)
                })
                .fold(0, |bits, (_, code)| bits | (1 << code))
        };
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
                        authorized_operations: group_ops(&id),
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
        authorized_operations: NOT_REQUESTED,
    }
}
