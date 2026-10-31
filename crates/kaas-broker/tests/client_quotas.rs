//! Quota admin APIs (keys 48/49) against a broker built the way
//! `bins/kaas` builds it (gh #260).
//!
//! The admin handlers need the concrete `QuotaEnforcer`, while the
//! throttling path holds it as `Arc<dyn QuotaChecker>`. The handle the
//! handlers read used to come from a separate install call that nothing
//! made, so on a cluster that was throttling DescribeClientQuotas
//! answered "no quotas" and AlterClientQuotas failed every entry. These
//! tests go through `Broker::with_auth` with only the checker, so they
//! fail if the admin surface stops being derived from it.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use kaas_auth::credentials::TestCred;
use kaas_auth::{
    AllowAllAuthorizer, CredentialLoader, Principal, PrincipalKind, QuotaChecker, QuotaEnforcer,
    Quotas,
};
use kaas_broker::{AlterClientQuotasHandler, Broker, DescribeClientQuotasHandler, TopicRegistry};
use kaas_codec::api::describe_client_quotas::{entity_type, match_type};
use kaas_codec::api::{alter_client_quotas, describe_client_quotas};
use kaas_protocol::{ConnState, Handler};
use kaas_storage::{MemoryStorage, StorageEngine};
use parking_lot::Mutex;

const ALICE: &str = "alice";

fn broker_with_quota(producer: i64, consumer: i64) -> Arc<Broker> {
    let creds = CredentialLoader::new("/nonexistent/kaas-client-quotas-test");
    creds.install_for_test(vec![TestCred {
        username: ALICE.to_owned(),
        auth_type: "scram-sha-512".to_owned(),
        quotas: Some(Quotas {
            producer_max_byte_rate_per_broker: Some(producer),
            consumer_max_byte_rate_per_broker: Some(consumer),
            request_percentage: None,
        }),
        ..TestCred::default()
    }]);
    let quotas: Arc<dyn QuotaChecker> = Arc::new(QuotaEnforcer::new(Arc::new(creds)));
    let engine: Arc<dyn StorageEngine> = Arc::new(MemoryStorage::new());
    Arc::new(Broker::with_auth(
        engine,
        Arc::new(TopicRegistry::new()),
        "kaas-test",
        0,
        Arc::new(AllowAllAuthorizer),
        quotas,
    ))
}

fn conn() -> Mutex<ConnState> {
    let mut s = ConnState::new("scram", SocketAddr::from_str("127.0.0.1:9095").unwrap());
    s.principal = Some(alice());
    Mutex::new(s)
}

fn alice() -> Principal {
    Principal {
        name: ALICE.to_owned(),
        kind: PrincipalKind::User,
    }
}

async fn call(h: &impl Handler, version: i16, body: BytesMut) -> Bytes {
    h.handle(&conn(), version, body.freeze())
        .await
        .unwrap()
        .freeze()
}

/// `(key, value)` pairs DescribeClientQuotas reports for alice.
async fn describe_alice(broker: &Arc<Broker>) -> Vec<(String, f64)> {
    let req = describe_client_quotas::Request {
        components: vec![describe_client_quotas::ComponentData {
            entity_type: entity_type::USER.to_owned(),
            match_type: match_type::EXACT,
            match_: Some(ALICE.to_owned()),
        }],
        strict: false,
    };
    let mut body = BytesMut::new();
    describe_client_quotas::encode_request(&mut body, &req, 1).unwrap();
    let h = DescribeClientQuotasHandler::new(broker.clone());
    let mut out = call(&h, 1, body).await;
    let resp = describe_client_quotas::decode_response(&mut out, 1).unwrap();
    assert_eq!(resp.error_code, 0, "{resp:?}");
    let mut values: Vec<(String, f64)> = resp
        .entries
        .into_iter()
        .flat_map(|e| e.values)
        .map(|v| (v.key, v.value))
        .collect();
    values.sort_by(|a, b| a.0.cmp(&b.0));
    values
}

#[tokio::test]
async fn describe_reports_the_quota_that_is_enforced() {
    let broker = broker_with_quota(1_048_576, 2_097_152);
    assert_eq!(
        describe_alice(&broker).await,
        [
            ("consumer_byte_rate".to_owned(), 2_097_152.0),
            ("producer_byte_rate".to_owned(), 1_048_576.0),
        ]
    );
}

#[tokio::test]
async fn alter_changes_the_limit_that_throttles() {
    let broker = broker_with_quota(1_048_576, 2_097_152);
    // Within the configured 1 MiB/s: no throttle.
    assert_eq!(broker.quotas.check_produce_quota(&alice(), 10_000), 0);

    let req = alter_client_quotas::Request {
        entries: vec![alter_client_quotas::EntryData {
            entity: vec![alter_client_quotas::EntityData {
                entity_type: entity_type::USER.to_owned(),
                entity_name: Some(ALICE.to_owned()),
            }],
            ops: vec![alter_client_quotas::OpData {
                key: "producer_byte_rate".to_owned(),
                value: Some(1_000.0),
            }],
        }],
        validate_only: false,
    };
    let mut body = BytesMut::new();
    alter_client_quotas::encode_request(&mut body, &req, 1).unwrap();
    let h = AlterClientQuotasHandler::new(broker.clone());
    let mut out = call(&h, 1, body).await;
    let resp = alter_client_quotas::decode_response(&mut out, 1).unwrap();
    assert_eq!(resp.entries.len(), 1);
    assert_eq!(resp.entries[0].error_code, 0, "{resp:?}");

    assert_eq!(
        describe_alice(&broker).await,
        [
            ("consumer_byte_rate".to_owned(), 2_097_152.0),
            ("producer_byte_rate".to_owned(), 1_000.0),
        ]
    );
    // The same 10 KB now overdraws a 1000 B/s bucket: the override
    // reached the enforcer on the throttling path, not a copy.
    assert!(broker.quotas.check_produce_quota(&alice(), 10_000) > 0);
}
