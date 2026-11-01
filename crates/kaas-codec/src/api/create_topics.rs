//! CreateTopics — API key 19.
//!
//! Versions 0..=7. Flexible (KIP-482) from v5. Used by
//! `AdminClient.createTopics()` and `kafka-topics.sh --create`.
//!
//! Kaas routes CreateTopics through the operator by minting a
//! `KafkaTopic` CR (gh #51). The handler in `kaas-broker` maps the
//! wire request to a `TopicCRWriter::create_topic` call.
//! Replica-assignment and topic-level configs are parsed for
//! protocol fidelity but only `NumPartitions` is used at the
//! handler — kaas has no replicas and forwards configs via the
//! CR's `spec.config` map on a follow-up patch.
//!
//! v7 adds `topic_id` (UUID) on the response. Kaas echoes
//! the all-zeros UUID until the KafkaTopic reconciler stamps
//! `.status.topicID` (gh #105).

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_str, read_str, write_array_len, write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{read_bool, read_i16, read_i32, write_bool, write_i16, write_i32};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 7);
pub const MIN_FLEXIBLE: i16 = 5;

pub const SPEC: ApiSpec = ApiSpec {
    key: 19,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

// ---------- Request ----------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub topics: Vec<CreatableTopic>,
    /// `timeout.ms` — how long the broker waits for the topic dirs
    /// to materialise before responding.
    pub timeout_ms: i32,
    /// v1+. When `true`, validate + return would-be error codes
    /// without mutating state.
    pub validate_only: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopic {
    pub name: String,
    /// `-1` when Assignments carries explicit replica placement.
    pub num_partitions: i32,
    /// `-1` when Assignments carries explicit replica placement.
    /// Kaas has no replicas so any positive value is accepted
    /// and ignored at the handler.
    pub replication_factor: i16,
    /// Per-partition replica assignments. kaas has no replicas, so only
    /// the partition count they imply is used (gh #292).
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// Per-topic config overrides (retention.ms, cleanup.policy,
    /// ...). Handler forwards these through the CR patch.
    pub configs: Vec<CreatableTopicConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableReplicaAssignment {
    pub partition_index: i32,
    pub broker_ids: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopicConfig {
    pub name: String,
    /// `None` ↔ wire null.
    pub value: Option<String>,
}

// ---------- Response ----------

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    /// v2+. Zero on earlier versions (serialised absent).
    pub throttle_time_ms: i32,
    pub topics: Vec<CreatableTopicResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopicResult {
    pub name: String,
    /// v7+. All-zeros UUID until the operator stamps
    /// `.status.topicID`.
    pub topic_id: [u8; 16],
    pub error_code: i16,
    /// v1+. Operator-side reason on failure.
    pub error_message: Option<String>,
    /// v5+. Result of the create with the actual accepted values.
    /// Zero on earlier versions.
    pub num_partitions: i32,
    /// v5+.
    pub replication_factor: i16,
    /// v5+. Effective config values echoed back.
    pub configs: Vec<CreatableTopicConfigResult>,
}

impl CreatableTopicResult {
    pub fn new(name: impl Into<String>, error_code: i16) -> Self {
        Self {
            name: name.into(),
            topic_id: [0u8; 16],
            error_code,
            error_message: None,
            // Apache's schema default; a successful create overwrites
            // both via `with_created` (gh #292).
            num_partitions: -1,
            replication_factor: -1,
            configs: Vec::new(),
        }
    }

    pub fn with_error_message(mut self, msg: impl Into<String>) -> Self {
        self.error_message = Some(msg.into());
        self
    }

    pub fn with_created(mut self, num_partitions: i32, replication_factor: i16) -> Self {
        self.num_partitions = num_partitions;
        self.replication_factor = replication_factor;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreatableTopicConfigResult {
    pub name: String,
    pub value: Option<String>,
    /// v5+ config source. Kaas echoes `DEFAULT_CONFIG (5)`.
    pub read_only: bool,
    pub config_source: i8,
    pub is_sensitive: bool,
}

// ---------- decode / encode ----------

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut req = Request::default();
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let name = read_str(buf, flexible)?;
        let num_partitions = read_i32(buf)?;
        let replication_factor = read_i16(buf)?;

        let a = read_array_len(buf, flexible)?;
        let mut assignments = Vec::with_capacity(a);
        for _ in 0..a {
            let partition_index = read_i32(buf)?;
            let inner = read_array_len(buf, flexible)?;
            let mut broker_ids = Vec::with_capacity(inner);
            for _ in 0..inner {
                broker_ids.push(read_i32(buf)?);
            }
            if flexible {
                tagged::read(buf)?;
            }
            assignments.push(CreatableReplicaAssignment {
                partition_index,
                broker_ids,
            });
        }

        let c = read_array_len(buf, flexible)?;
        let mut configs = Vec::with_capacity(c);
        for _ in 0..c {
            let name = read_str(buf, flexible)?;
            let value = read_nullable_str(buf, flexible)?;
            if flexible {
                tagged::read(buf)?;
            }
            configs.push(CreatableTopicConfig { name, value });
        }

        if flexible {
            tagged::read(buf)?;
        }
        req.topics.push(CreatableTopic {
            name,
            num_partitions,
            replication_factor,
            assignments,
            configs,
        });
    }
    req.timeout_ms = read_i32(buf)?;
    if version >= 1 {
        req.validate_only = read_bool(buf)?;
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(req)
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    if version >= 2 {
        write_i32(buf, resp.throttle_time_ms);
    }
    write_array_len(buf, resp.topics.len(), flexible)?;
    for t in &resp.topics {
        write_str(buf, &t.name, flexible)?;
        if version >= 7 {
            buf.extend_from_slice(&t.topic_id);
        }
        write_i16(buf, t.error_code);
        if version >= 1 {
            write_nullable_str(buf, t.error_message.as_deref(), flexible)?;
        }
        if version >= 5 {
            // In-band topic-level configs of the created topic. Not
            // exposed via a topic-level API yet — write num_partitions
            // + replication_factor from the accepted request; leave
            // topic_config_error_code (v5+ only, tagged) unset.
            write_i32(buf, t.num_partitions);
            write_i16(buf, t.replication_factor);
            write_array_len(buf, t.configs.len(), flexible)?;
            for c in &t.configs {
                write_str(buf, &c.name, flexible)?;
                write_nullable_str(buf, c.value.as_deref(), flexible)?;
                buf.extend_from_slice(&[u8::from(c.read_only)]);
                buf.extend_from_slice(&[c.config_source.to_le_bytes()[0]]);
                buf.extend_from_slice(&[u8::from(c.is_sensitive)]);
                if flexible {
                    tagged::write_empty(buf);
                }
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    let mut resp = Response::default();
    if version >= 2 {
        resp.throttle_time_ms = read_i32(buf)?;
    }
    let n = read_array_len(buf, flexible)?;
    for _ in 0..n {
        let name = read_str(buf, flexible)?;
        let mut topic_id = [0u8; 16];
        if version >= 7 {
            if buf.len() < 16 {
                return Err(CodecError::UnexpectedEof);
            }
            topic_id.copy_from_slice(&buf[..16]);
            let _ = buf.split_to(16);
        }
        let error_code = read_i16(buf)?;
        let error_message = if version >= 1 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        let (num_partitions, replication_factor, configs) = if version >= 5 {
            let np = read_i32(buf)?;
            let rf = read_i16(buf)?;
            let c = read_array_len(buf, flexible)?;
            let mut out = Vec::with_capacity(c);
            for _ in 0..c {
                let name = read_str(buf, flexible)?;
                let value = read_nullable_str(buf, flexible)?;
                if buf.len() < 3 {
                    return Err(CodecError::UnexpectedEof);
                }
                let read_only = buf.split_to(1)[0] != 0;
                let config_source = i8::from_le_bytes([buf.split_to(1)[0]]);
                let is_sensitive = buf.split_to(1)[0] != 0;
                if flexible {
                    tagged::read(buf)?;
                }
                out.push(CreatableTopicConfigResult {
                    name,
                    value,
                    read_only,
                    config_source,
                    is_sensitive,
                });
            }
            (np, rf, out)
        } else {
            // Not on the wire below v5; Apache's schema default is -1.
            (-1, -1, Vec::new())
        };
        if flexible {
            tagged::read(buf)?;
        }
        resp.topics.push(CreatableTopicResult {
            name,
            topic_id,
            error_code,
            error_message,
            num_partitions,
            replication_factor,
            configs,
        });
    }
    if flexible {
        tagged::read(buf)?;
    }
    Ok(resp)
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;
    write_array_len(buf, req.topics.len(), flexible)?;
    for t in &req.topics {
        write_str(buf, &t.name, flexible)?;
        write_i32(buf, t.num_partitions);
        write_i16(buf, t.replication_factor);
        write_array_len(buf, t.assignments.len(), flexible)?;
        for a in &t.assignments {
            write_i32(buf, a.partition_index);
            write_array_len(buf, a.broker_ids.len(), flexible)?;
            for id in &a.broker_ids {
                write_i32(buf, *id);
            }
            if flexible {
                tagged::write_empty(buf);
            }
        }
        write_array_len(buf, t.configs.len(), flexible)?;
        for c in &t.configs {
            write_str(buf, &c.name, flexible)?;
            write_nullable_str(buf, c.value.as_deref(), flexible)?;
            if flexible {
                tagged::write_empty(buf);
            }
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    write_i32(buf, req.timeout_ms);
    if version >= 1 {
        write_bool(buf, req.validate_only);
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_request(version: i16) -> Request {
        Request {
            topics: vec![
                CreatableTopic {
                    name: "events".into(),
                    num_partitions: 3,
                    replication_factor: 1,
                    assignments: Vec::new(),
                    configs: if version >= 1 {
                        vec![CreatableTopicConfig {
                            name: "retention.ms".into(),
                            value: Some("604800000".into()),
                        }]
                    } else {
                        Vec::new()
                    },
                },
                CreatableTopic {
                    name: "alt".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    assignments: Vec::new(),
                    configs: Vec::new(),
                },
            ],
            timeout_ms: 30_000,
            validate_only: version >= 1,
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            topics: vec![
                CreatableTopicResult {
                    name: "events".into(),
                    topic_id: [0u8; 16],
                    error_code: 0,
                    error_message: None,
                    num_partitions: if version >= 5 { 3 } else { -1 },
                    replication_factor: if version >= 5 { 1 } else { -1 },
                    configs: Vec::new(),
                },
                CreatableTopicResult {
                    name: "alt".into(),
                    topic_id: [0u8; 16],
                    error_code: 36, // TOPIC_ALREADY_EXISTS
                    error_message: if version >= 1 {
                        Some("topic already exists".into())
                    } else {
                        None
                    },
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: Vec::new(),
                },
            ],
        }
    }

    fn roundtrip(version: i16) {
        let req = sample_request(version);
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty(), "trailing bytes on request v{version}");

        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn v0_roundtrip() {
        roundtrip(0);
    }
    #[test]
    fn v1_carries_validate_only_and_error_message() {
        roundtrip(1);
    }
    #[test]
    fn v2_has_throttle_time() {
        roundtrip(2);
    }
    #[test]
    fn v3_roundtrip() {
        roundtrip(3);
    }
    #[test]
    fn v4_roundtrip() {
        roundtrip(4);
    }
    #[test]
    fn v5_is_flexible() {
        roundtrip(5);
    }
    #[test]
    fn v6_roundtrip() {
        roundtrip(6);
    }
    #[test]
    fn v7_carries_topic_id() {
        roundtrip(7);
    }
}
