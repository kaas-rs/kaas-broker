//! DeleteGroups handler (key 42, gh #89).
//!
//! Authorization (gh #268): `Delete` on each group, checked before the
//! coordinator — a denied group answers `GROUP_AUTHORIZATION_FAILED`
//! (30) and is left alone.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{Operation, Resource};
use kaas_codec::api::delete_groups;
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NOT_COORDINATOR: i16 = 16;
const ERR_GROUP_AUTHZ_FAILED: i16 = 30;

#[derive(Debug)]
pub struct DeleteGroupsHandler {
    broker: Arc<Broker>,
}

impl DeleteGroupsHandler {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }
}

#[async_trait]
impl Handler for DeleteGroupsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = delete_groups::decode_request(&mut body, version)?;
        let principal = principal_from(conn);
        let mgr = self.broker.coord_manager();
        let results: Vec<delete_groups::DeleteGroupsResult> = req
            .group_names
            .into_iter()
            .map(|name| {
                let error_code = if !self.broker.authorizer.authorize(
                    &principal,
                    &Resource::group(&name),
                    Operation::Delete,
                ) {
                    ERR_GROUP_AUTHZ_FAILED
                } else {
                    mgr.as_ref()
                        .map_or(ERR_NOT_COORDINATOR, |m| m.delete_group(&name))
                };
                delete_groups::DeleteGroupsResult {
                    group_id: name,
                    error_code,
                }
            })
            .collect();
        let resp = delete_groups::Response {
            throttle_time_ms: 0,
            results,
        };
        let mut out = BytesMut::new();
        delete_groups::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}
