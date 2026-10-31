//! Single declarative table of every API key the broker exposes.
//!
//! Drives the ApiVersions response in [`crate::api::api_versions`] and the
//! header-version lookup in [`crate::headers`]. The full 40-key table is
//! filled in over the course of Phase 1 — each per-API module's commit
//! adds its row here.

use crate::headers::HeaderVersion;

/// `ApiVersions` — the one key whose response header ignores the
/// flexible range.
const API_VERSIONS_KEY: i16 = 18;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ApiSpec {
    pub key: i16,
    pub min_version: i16,
    pub max_version: i16,
    /// `Some(v)` if the API is flexible (KIP-482) from version `v` onward;
    /// `None` if all supported versions are still legacy.
    pub min_flexible: Option<i16>,
}

impl ApiSpec {
    /// True if `version` is in the flexible range for this API.
    pub fn is_flexible(&self, version: i16) -> bool {
        matches!(self.min_flexible, Some(min) if version >= min)
    }

    /// Apache's generated `requestHeaderVersion`: v2 (tagged fields) on
    /// a flexible version, v1 (`client_id`) otherwise. Request header v0
    /// only ever served ControlledShutdown v0, which kaas doesn't
    /// register.
    pub fn request_header(&self, version: i16) -> HeaderVersion {
        if self.is_flexible(version) {
            HeaderVersion::V2
        } else {
            HeaderVersion::V1
        }
    }

    /// Apache's generated `responseHeaderVersion`: v1 (tagged fields) on
    /// a flexible version, v0 otherwise — except ApiVersions, whose
    /// response header is v0 at every version, because a client has to
    /// parse it before it knows which versions the broker speaks
    /// (KIP-511).
    ///
    /// Derived from `min_flexible` rather than written per module: the
    /// per-module copies shared one helper with the request side, which
    /// handed every non-flexible response a tagged-fields byte it must
    /// not have (gh #272).
    pub fn response_header(&self, version: i16) -> HeaderVersion {
        if self.key != API_VERSIONS_KEY && self.is_flexible(version) {
            HeaderVersion::V1
        } else {
            HeaderVersion::V0
        }
    }
}

/// Every API key the broker registers, with its supported version
/// range. One entry per per-API module.
pub const ALL: &[ApiSpec] = &[
    crate::api::produce::SPEC,
    crate::api::fetch::SPEC,
    crate::api::list_offsets::SPEC,
    crate::api::metadata::SPEC,
    crate::api::offset_commit::SPEC,
    crate::api::offset_fetch::SPEC,
    crate::api::find_coordinator::SPEC,
    crate::api::join_group::SPEC,
    crate::api::heartbeat::SPEC,
    crate::api::leave_group::SPEC,
    crate::api::sync_group::SPEC,
    crate::api::describe_groups::SPEC,
    crate::api::list_groups::SPEC,
    crate::api::sasl_handshake::SPEC,
    crate::api::init_producer_id::SPEC,
    crate::api::add_partitions_to_txn::SPEC,
    crate::api::add_offsets_to_txn::SPEC,
    crate::api::end_txn::SPEC,
    crate::api::write_txn_markers::SPEC,
    crate::api::txn_offset_commit::SPEC,
    crate::api::sasl_authenticate::SPEC,
    crate::api::api_versions::SPEC,
    crate::api::delete_groups::SPEC,
    crate::api::offset_delete::SPEC,
    // Phase 7 admin surface (workstream D)
    crate::api::describe_configs::SPEC,
    crate::api::create_partitions::SPEC,
    crate::api::create_topics::SPEC,
    crate::api::incremental_alter_configs::SPEC,
    crate::api::describe_client_quotas::SPEC,
    crate::api::alter_client_quotas::SPEC,
    // Admin surface (gh #152)
    crate::api::delete_topics::SPEC,
    crate::api::delete_records::SPEC,
    crate::api::describe_acls::SPEC,
    crate::api::create_acls::SPEC,
    crate::api::delete_acls::SPEC,
    crate::api::alter_replica_log_dirs::SPEC,
    crate::api::describe_log_dirs::SPEC,
    // KIP-700 cluster describe
    crate::api::describe_cluster::SPEC,
    // KIP-554 SCRAM credential admin (gh #252)
    crate::api::describe_user_scram_credentials::SPEC,
    crate::api::alter_user_scram_credentials::SPEC,
];

/// Look up the [`ApiSpec`] for a given API key, if registered.
pub fn lookup(key: i16) -> Option<&'static ApiSpec> {
    ALL.iter().find(|s| s.key == key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_returns_known_key() {
        let spec = lookup(18).expect("ApiVersions seeded in ALL");
        assert_eq!(spec.key, 18);
        assert_eq!(spec.min_version, 0);
        assert_eq!(spec.max_version, 4);
        assert_eq!(spec.min_flexible, Some(3));
    }

    #[test]
    fn lookup_returns_none_for_unknown_key() {
        assert!(lookup(999).is_none());
    }

    /// gh #272: the response header follows the flexible range, so a
    /// legacy version gets no tagged-fields byte. Values are Apache 3.7's
    /// `ApiKeys.{request,response}HeaderVersion`.
    #[test]
    fn header_versions_match_apache() {
        let cases: &[(i16, i16, HeaderVersion, HeaderVersion)] = &[
            // (key, version, request header, response header)
            (0, 8, HeaderVersion::V1, HeaderVersion::V0), // Produce, last legacy
            (0, 9, HeaderVersion::V2, HeaderVersion::V1),
            (1, 11, HeaderVersion::V1, HeaderVersion::V0), // Fetch
            (1, 12, HeaderVersion::V2, HeaderVersion::V1),
            (3, 8, HeaderVersion::V1, HeaderVersion::V0), // Metadata
            (3, 9, HeaderVersion::V2, HeaderVersion::V1),
            (10, 2, HeaderVersion::V1, HeaderVersion::V0), // FindCoordinator
            (17, 1, HeaderVersion::V1, HeaderVersion::V0), // SaslHandshake, never flexible
            (36, 1, HeaderVersion::V1, HeaderVersion::V0), // SaslAuthenticate
            (36, 2, HeaderVersion::V2, HeaderVersion::V1),
            (60, 0, HeaderVersion::V2, HeaderVersion::V1), // DescribeCluster, flexible from v0
            // ApiVersions: legacy request header carries client_id even at
            // v0; the response header stays v0 even once flexible.
            (18, 0, HeaderVersion::V1, HeaderVersion::V0),
            (18, 3, HeaderVersion::V2, HeaderVersion::V0),
        ];
        for &(key, version, req, resp) in cases {
            let spec = lookup(key).expect("registered");
            assert_eq!(
                spec.request_header(version),
                req,
                "request key={key} v{version}"
            );
            assert_eq!(
                spec.response_header(version),
                resp,
                "response key={key} v{version}"
            );
        }
    }

    #[test]
    fn flex_predicate() {
        let spec = lookup(18).expect("seeded");
        assert!(!spec.is_flexible(2));
        assert!(spec.is_flexible(3));
        assert!(spec.is_flexible(4));
    }

    /// Phase 6 exit criterion §A — registry pins to 24 entries:
    /// keys 0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 22,
    /// 24, 25, 26, 27, 28, 36, 42, 47. Bump this number when a new
    /// module lands.
    #[test]
    fn registry_size_phase7() {
        // Phase 7 workstream D added keys 32, 37, 44, 48, 49 — the
        // admin surface (Describe/AlterConfigs, CreatePartitions,
        // Describe/AlterClientQuotas). Phase 8 workstream C added
        // key 19 (CreateTopics) once the scripts smoke started
        // needing it. gh #152 added the
        // admin keys 20, 21, 29, 30, 31, 35 (DeleteTopics,
        // DeleteRecords, the ACL trio, DescribeLogDirs). Key 60
        // (DescribeCluster, KIP-700) followed, then keys 50 + 51
        // (KIP-554 SCRAM credential admin, gh #252).
        assert_eq!(ALL.len(), 40);
        let mut keys: Vec<i16> = ALL.iter().map(|s| s.key).collect();
        keys.sort_unstable();
        assert_eq!(
            keys,
            vec![
                0, 1, 2, 3, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 24, 25, 26,
                27, 28, 29, 30, 31, 32, 34, 35, 36, 37, 42, 44, 47, 48, 49, 50, 51, 60,
            ]
        );
    }
}
