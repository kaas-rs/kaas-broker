//! DescribeUserScramCredentials handler — API key 50 (gh #252,
//! KIP-554).
//!
//! Answers from the live [`CredentialStore`] (the operator-written
//! `credentials.json`, hot-reloaded) — mechanism + iteration count
//! only, never salts or keys. `users: null` or an empty list describes
//! every user with SCRAM credentials; a named user without any answers
//! `RESOURCE_NOT_FOUND` (91); a user named twice answers
//! `DUPLICATE_RESOURCE` (92), all per Apache (`ScramImage.describe`).
//! Until gh #276 those codes went out as 83 and 81 —
//! `ELIGIBLE_LEADERS_NOT_AVAILABLE` and `GROUP_MAX_SIZE_REACHED` — and an
//! empty list described nobody.
//!
//! Authorization: `Operation::Describe` on the cluster resource —
//! denial is the *top-level* `CLUSTER_AUTHORIZATION_FAILED` (31),
//! matching Apache's shape for this API.

use std::collections::HashSet;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use kaas_auth::{CredentialStore, Operation, Resource};
use kaas_codec::api::describe_user_scram_credentials::{
    self, mechanism, CredentialInfo, DescribeUserResult, Response,
};
use kaas_protocol::{ConnState, Handler, HandlerError};
use parking_lot::Mutex;

use super::principal_from;
use crate::broker::Broker;

const ERR_NONE: i16 = 0;
const ERR_CLUSTER_AUTHZ_FAILED: i16 = 31;
const ERR_DUPLICATE_RESOURCE: i16 = 92;
const ERR_RESOURCE_NOT_FOUND: i16 = 91;

#[derive(Debug)]
pub struct DescribeUserScramCredentialsHandler {
    broker: Arc<Broker>,
    /// `None` in dev mode / auth-disabled — every lookup answers
    /// RESOURCE_NOT_FOUND rather than pretending an empty universe
    /// is an error.
    creds: Option<Arc<dyn CredentialStore>>,
}

impl DescribeUserScramCredentialsHandler {
    pub fn new(broker: Arc<Broker>, creds: Option<Arc<dyn CredentialStore>>) -> Self {
        Self { broker, creds }
    }
}

#[async_trait]
impl Handler for DescribeUserScramCredentialsHandler {
    async fn handle(
        &self,
        conn: &Mutex<ConnState>,
        version: i16,
        body: Bytes,
    ) -> Result<BytesMut, HandlerError> {
        let mut body = body;
        let req = describe_user_scram_credentials::decode_request(&mut body, version)?;

        let principal = principal_from(conn);
        if !self
            .broker
            .authorizer
            .authorize(&principal, &Resource::cluster(), Operation::Describe)
        {
            let resp = Response {
                throttle_time_ms: 0,
                error_code: ERR_CLUSTER_AUTHZ_FAILED,
                error_message: Some("Cluster authorization failed.".into()),
                results: vec![],
            };
            let mut out = BytesMut::new();
            describe_user_scram_credentials::encode_response(&mut out, &resp, version)?;
            return Ok(out);
        }

        let all = self
            .creds
            .as_ref()
            .map(|c| c.list_all_scram_users())
            .unwrap_or_default();

        // Apache reads an empty list like null: "if there are no users
        // listed then get all the users".
        let results: Vec<DescribeUserResult> = match req.users.filter(|u| !u.is_empty()) {
            // null / empty → everything with SCRAM credentials, sorted
            // for a stable wire order.
            None => {
                let mut users: Vec<_> = all.into_iter().collect();
                users.sort_by(|a, b| a.0.cmp(&b.0));
                users
                    .into_iter()
                    .map(|(user, info)| DescribeUserResult {
                        user,
                        error_code: ERR_NONE,
                        error_message: None,
                        credential_infos: vec![CredentialInfo {
                            mechanism: mechanism::SCRAM_SHA_512,
                            iterations: info.iterations,
                        }],
                    })
                    .collect()
            }
            Some(requested) => {
                let mut seen = HashSet::new();
                let mut dups = HashSet::new();
                for u in &requested {
                    if !seen.insert(u.clone()) {
                        dups.insert(u.clone());
                    }
                }
                // One result row per distinct requested user.
                let mut emitted = HashSet::new();
                requested
                    .into_iter()
                    .filter(|u| emitted.insert(u.clone()))
                    .map(|user| {
                        if dups.contains(&user) {
                            return DescribeUserResult {
                                user,
                                error_code: ERR_DUPLICATE_RESOURCE,
                                error_message: Some(
                                    "user appears more than once in the request".into(),
                                ),
                                credential_infos: vec![],
                            };
                        }
                        match all.get(&user) {
                            Some(info) => DescribeUserResult {
                                user,
                                error_code: ERR_NONE,
                                error_message: None,
                                credential_infos: vec![CredentialInfo {
                                    mechanism: mechanism::SCRAM_SHA_512,
                                    iterations: info.iterations,
                                }],
                            },
                            None => DescribeUserResult {
                                user,
                                error_code: ERR_RESOURCE_NOT_FOUND,
                                error_message: Some(
                                    "attempt to describe a user with no SCRAM credentials".into(),
                                ),
                                credential_infos: vec![],
                            },
                        }
                    })
                    .collect()
            }
        };

        let resp = Response {
            throttle_time_ms: 0,
            error_code: ERR_NONE,
            error_message: None,
            results,
        };
        let mut out = BytesMut::new();
        describe_user_scram_credentials::encode_response(&mut out, &resp, version)?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topic_registry::TopicRegistry;
    use kaas_auth::credentials::{ScramCreds, TestCred};
    use kaas_auth::CredentialLoader;
    use kaas_storage::{MemoryStorage, StorageEngine};
    use std::net::SocketAddr;
    use std::str::FromStr;

    fn handler() -> DescribeUserScramCredentialsHandler {
        let creds = CredentialLoader::new("/nonexistent/kaas-scram-describe-test");
        creds.install_for_test(vec![TestCred {
            username: "alice".to_owned(),
            auth_type: "scram-sha-512".to_owned(),
            scram: Some(ScramCreds {
                stored_key: vec![1; 64],
                server_key: vec![2; 64],
                salt: vec![3; 16],
                iterations: 4096,
            }),
            ..TestCred::default()
        }]);
        let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
        let broker = Arc::new(Broker::new(
            engine,
            Arc::new(TopicRegistry::new()),
            "test",
            0,
        ));
        DescribeUserScramCredentialsHandler::new(broker, Some(Arc::new(creds)))
    }

    async fn describe(users: Option<Vec<&str>>) -> Response {
        let req = describe_user_scram_credentials::Request {
            users: users.map(|u| u.into_iter().map(str::to_owned).collect()),
        };
        let mut body = BytesMut::new();
        describe_user_scram_credentials::encode_request(&mut body, &req, 0).unwrap();
        let conn = Mutex::new(ConnState::new(
            "scram",
            SocketAddr::from_str("127.0.0.1:9095").unwrap(),
        ));
        let mut out = handler()
            .handle(&conn, 0, body.freeze())
            .await
            .unwrap()
            .freeze();
        describe_user_scram_credentials::decode_response(&mut out, 0).unwrap()
    }

    /// gh #276: Apache's codes, written out — comparing against our own
    /// constants is how 83 / 81 went unnoticed.
    #[tokio::test]
    async fn unknown_and_duplicate_users_get_apaches_codes() {
        let resp = describe(Some(vec!["nobody", "alice", "alice"])).await;
        let code = |u: &str| {
            resp.results
                .iter()
                .find(|r| r.user == u)
                .map(|r| r.error_code)
        };
        assert_eq!(code("nobody"), Some(91), "RESOURCE_NOT_FOUND");
        assert_eq!(code("alice"), Some(92), "DUPLICATE_RESOURCE");
    }

    /// gh #276: an empty list describes everyone, as null does.
    #[tokio::test]
    async fn an_empty_user_list_describes_all_users() {
        for users in [None, Some(vec![])] {
            let resp = describe(users.clone()).await;
            assert_eq!(resp.results.len(), 1, "{users:?}");
            assert_eq!(resp.results[0].user, "alice");
            assert_eq!(resp.results[0].error_code, 0);
            assert_eq!(resp.results[0].credential_infos[0].iterations, 4096);
        }
    }
}
