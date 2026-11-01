//! Metadata — API key 3. v1–v10, flexible from v9 (KIP-482).
//!
//! Clamped to the version range the broker registers (v1–v10). v11 dropped
//! `IncludeClusterAuthorizedOperations`; v12 lets a request name topics
//! by id alone. Both are out of scope — clients negotiating to v11+ fall
//! back to v10 via ApiVersions. The topic array is nullable from v1
//! (null = all topics, empty = none, gh #274).
//!
//! # Version map
//!
//! | v | Adds                                                          |
//! |--:|---------------------------------------------------------------|
//! | 1 | `rack` (broker) · `is_internal` (topic) · `controller_id` resp |
//! | 2 | `cluster_id` resp                                             |
//! | 3 | `throttle_time_ms` resp                                       |
//! | 4 | `allow_auto_topic_creation` req                               |
//! | 5 | `offline_replicas` (partition) resp                           |
//! | 7 | `leader_epoch` (partition) resp                               |
//! | 8 | `include_cluster_authorized_operations` + `include_topic_authorized_operations` req · `topic_authorized_operations` + `cluster_authorized_operations` resp |
//! | 9 | KIP-482 flexible encoding                                     |
//! | 10| `topic_id` (UUID, 16 raw bytes) on each request and response topic (gh #105); request topic `name` nullable. Apache still rejects a null name or non-zero id below v12 |

use bytes::BytesMut;

use crate::api::common::{
    read_array_len, read_nullable_array_len, read_nullable_str, read_str, write_array_len,
    write_nullable_str, write_str,
};
use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_i16, read_i32, read_i8, read_uuid, write_i16, write_i32, write_i8, write_uuid,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (1, 10);
pub const MIN_FLEXIBLE: i16 = 9;

pub const SPEC: ApiSpec = ApiSpec {
    key: 3,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

/// All-zero topic UUID — the gh #105 fallback for legacy CRs that
/// predate `Status.TopicID`. Same wire bytes as Apache's "null UUID".
pub const NULL_TOPIC_ID: [u8; 16] = [0; 16];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    /// `None` ↔ wire null = all topics. `Some(vec![])` asks for no
    /// topics, only brokers and cluster info. That is Apache's contract
    /// from v1, where the array became nullable; only v0, which kaas
    /// doesn't serve, read an empty list as "all topics" (gh #274).
    pub topics: Option<Vec<RequestTopic>>,
    /// v4+.
    pub allow_auto_topic_creation: bool,
    /// v8–v10. Removed in v11.
    pub include_cluster_authorized_operations: bool,
    /// v8+.
    pub include_topic_authorized_operations: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestTopic {
    /// v10+. All-zero below v10, and from a client that knows only the
    /// name. Lookup by id starts at v12, so at the versions kaas serves
    /// Apache rejects a non-zero id with `INVALID_REQUEST`.
    pub topic_id: [u8; 16],
    /// `None` ↔ wire null, possible from v10. Apache rejects it below
    /// v12 with `INVALID_REQUEST`.
    pub name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Response {
    /// v3+.
    pub throttle_time_ms: i32,
    pub brokers: Vec<Broker>,
    /// v2+. `None` ↔ wire null. Kaas emits its cluster id (never
    /// null) so `Some(String)` is the steady-state shape.
    pub cluster_id: Option<String>,
    /// v1+. `-1` when no controller (single-broker dev mode).
    pub controller_id: i32,
    pub topics: Vec<Topic>,
    /// v8–v10. Removed in v11.
    pub cluster_authorized_operations: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    /// v1+. `None` ↔ wire null. Empty string surfaces as `Some("")`.
    pub rack: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Topic {
    pub error_code: i16,
    pub name: String,
    /// v10+. All-zero (`NULL_TOPIC_ID`) until the operator mints a
    /// real UUID via `Status.TopicID` (gh #105).
    pub topic_id: [u8; 16],
    /// v1+.
    pub is_internal: bool,
    pub partitions: Vec<Partition>,
    /// v8+. ACL bitset; `0` when no authorizer is wired.
    pub topic_authorized_operations: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partition {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader_id: i32,
    /// v7+. `-1` when not available.
    pub leader_epoch: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
    /// v5+.
    pub offline_replicas: Vec<i32>,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let topics = match read_nullable_array_len(buf, flexible)? {
        None => None,
        Some(n) => {
            let mut topics = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let topic = if version >= 10 {
                    RequestTopic {
                        topic_id: read_uuid(buf)?,
                        name: read_nullable_str(buf, true)?,
                    }
                } else {
                    RequestTopic {
                        topic_id: NULL_TOPIC_ID,
                        name: Some(read_str(buf, flexible)?),
                    }
                };
                if flexible {
                    tagged::read(buf)?;
                }
                topics.push(topic);
            }
            Some(topics)
        }
    };

    // The field only exists in v4+. Apache's schema declares
    // `"default": "true"`, so a v0-v3 request means "auto-create
    // allowed" — pre-v4 clients had no way to opt out and the broker
    // config was the only gate. Decoding absent-as-false would make
    // kaas silently stricter than Apache for those versions.
    let allow_auto_topic_creation = if version >= 4 {
        read_i8(buf)? != 0
    } else {
        true
    };
    let include_cluster_authorized_operations = if (8..=10).contains(&version) {
        read_i8(buf)? != 0
    } else {
        false
    };
    let include_topic_authorized_operations = if version >= 8 {
        read_i8(buf)? != 0
    } else {
        false
    };

    if flexible {
        tagged::read(buf)?;
    }

    Ok(Request {
        topics,
        allow_auto_topic_creation,
        include_cluster_authorized_operations,
        include_topic_authorized_operations,
    })
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    if version >= 3 {
        write_i32(buf, resp.throttle_time_ms);
    }

    write_array_len(buf, resp.brokers.len(), flexible)?;
    for b in &resp.brokers {
        write_i32(buf, b.node_id);
        write_str(buf, &b.host, flexible)?;
        write_i32(buf, b.port);
        if version >= 1 {
            write_nullable_str(buf, b.rack.as_deref(), flexible)?;
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }

    if version >= 2 {
        write_nullable_str(buf, resp.cluster_id.as_deref(), flexible)?;
    }
    if version >= 1 {
        write_i32(buf, resp.controller_id);
    }

    write_array_len(buf, resp.topics.len(), flexible)?;
    for t in &resp.topics {
        write_i16(buf, t.error_code);
        // v9-v10 use compact string for topic name (non-nullable in
        // this version range; v12 makes it nullable but v12 is out of
        // Phase 3 scope).
        write_str(buf, &t.name, flexible)?;
        if version >= 10 {
            write_uuid(buf, &t.topic_id);
        }
        if version >= 1 {
            write_i8(buf, i8::from(t.is_internal));
        }
        write_partition_array(buf, &t.partitions, version, flexible)?;
        if version >= 8 {
            write_i32(buf, t.topic_authorized_operations);
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }

    if (8..=10).contains(&version) {
        write_i32(buf, resp.cluster_authorized_operations);
    }
    if flexible {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flexible = version >= MIN_FLEXIBLE;

    let throttle_time_ms = if version >= 3 { read_i32(buf)? } else { 0 };

    let broker_count = read_array_len(buf, flexible)?;
    let mut brokers = Vec::with_capacity(broker_count);
    for _ in 0..broker_count {
        let node_id = read_i32(buf)?;
        let host = read_str(buf, flexible)?;
        let port = read_i32(buf)?;
        let rack = if version >= 1 {
            read_nullable_str(buf, flexible)?
        } else {
            None
        };
        if flexible {
            tagged::read(buf)?;
        }
        brokers.push(Broker {
            node_id,
            host,
            port,
            rack,
        });
    }

    let cluster_id = if version >= 2 {
        read_nullable_str(buf, flexible)?
    } else {
        None
    };
    let controller_id = if version >= 1 { read_i32(buf)? } else { -1 };

    let topic_count = read_array_len(buf, flexible)?;
    let mut topics = Vec::with_capacity(topic_count);
    for _ in 0..topic_count {
        let error_code = read_i16(buf)?;
        let name = read_str(buf, flexible)?;
        let topic_id = if version >= 10 {
            read_uuid(buf)?
        } else {
            NULL_TOPIC_ID
        };
        let is_internal = if version >= 1 {
            read_i8(buf)? != 0
        } else {
            false
        };

        let part_count = read_array_len(buf, flexible)?;
        let mut partitions = Vec::with_capacity(part_count);
        for _ in 0..part_count {
            let p_error_code = read_i16(buf)?;
            let partition_index = read_i32(buf)?;
            let leader_id = read_i32(buf)?;
            let leader_epoch = if version >= 7 { read_i32(buf)? } else { -1 };
            let replica_nodes = read_int32_array(buf, flexible)?;
            let isr_nodes = read_int32_array(buf, flexible)?;
            let offline_replicas = if version >= 5 {
                read_int32_array(buf, flexible)?
            } else {
                Vec::new()
            };
            if flexible {
                tagged::read(buf)?;
            }
            partitions.push(Partition {
                error_code: p_error_code,
                partition_index,
                leader_id,
                leader_epoch,
                replica_nodes,
                isr_nodes,
                offline_replicas,
            });
        }

        // Below v8 the field isn't on the wire; Apache's schema default is
        // INT_MIN ("not computed"), not 0 ("nothing authorized") (gh #284).
        let topic_authorized_operations = if version >= 8 {
            read_i32(buf)?
        } else {
            i32::MIN
        };
        if flexible {
            tagged::read(buf)?;
        }
        topics.push(Topic {
            error_code,
            name,
            topic_id,
            is_internal,
            partitions,
            topic_authorized_operations,
        });
    }

    let cluster_authorized_operations = if (8..=10).contains(&version) {
        read_i32(buf)?
    } else {
        i32::MIN
    };

    if flexible {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        brokers,
        cluster_id,
        controller_id,
        topics,
        cluster_authorized_operations,
    })
}

// --- internal helpers ---

fn write_int32_slice(buf: &mut BytesMut, s: &[i32], flexible: bool) -> Result<(), CodecError> {
    write_array_len(buf, s.len(), flexible)?;
    for v in s {
        write_i32(buf, *v);
    }
    Ok(())
}

fn read_int32_array(buf: &mut Bytes, flexible: bool) -> Result<Vec<i32>, CodecError> {
    let n = read_array_len(buf, flexible)?;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(read_i32(buf)?);
    }
    Ok(out)
}

fn write_partition_array(
    buf: &mut BytesMut,
    parts: &[Partition],
    version: i16,
    flexible: bool,
) -> Result<(), CodecError> {
    write_array_len(buf, parts.len(), flexible)?;
    for p in parts {
        write_i16(buf, p.error_code);
        write_i32(buf, p.partition_index);
        write_i32(buf, p.leader_id);
        if version >= 7 {
            write_i32(buf, p.leader_epoch);
        }
        write_int32_slice(buf, &p.replica_nodes, flexible)?;
        write_int32_slice(buf, &p.isr_nodes, flexible)?;
        if version >= 5 {
            write_int32_slice(buf, &p.offline_replicas, flexible)?;
        }
        if flexible {
            tagged::write_empty(buf);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::common::write_nullable_array_len;

    fn named(names: &[&str]) -> Option<Vec<RequestTopic>> {
        Some(
            names
                .iter()
                .map(|n| RequestTopic {
                    topic_id: NULL_TOPIC_ID,
                    name: Some((*n).to_owned()),
                })
                .collect(),
        )
    }

    fn sample_request(version: i16) -> Request {
        Request {
            topics: named(&["events", "audit"]),
            // True at every version: v4+ round-trips the written byte,
            // v0-v3 has no field and decodes to the Apache default.
            allow_auto_topic_creation: true,
            include_cluster_authorized_operations: (8..=10).contains(&version),
            include_topic_authorized_operations: version >= 8,
        }
    }

    fn sample_response(version: i16) -> Response {
        Response {
            throttle_time_ms: 0,
            brokers: vec![Broker {
                node_id: 0,
                host: "broker-0.cluster.local".to_owned(),
                port: 9092,
                rack: if version >= 1 {
                    Some("rack-a".to_owned())
                } else {
                    None
                },
            }],
            cluster_id: if version >= 2 {
                Some("kaas-rust-dev".to_owned())
            } else {
                None
            },
            controller_id: if version >= 1 { 0 } else { -1 },
            topics: vec![Topic {
                error_code: 0,
                name: "events".to_owned(),
                topic_id: if version >= 10 {
                    [1; 16]
                } else {
                    NULL_TOPIC_ID
                },
                is_internal: false,
                partitions: vec![Partition {
                    error_code: 0,
                    partition_index: 0,
                    leader_id: 0,
                    leader_epoch: if version >= 7 { 5 } else { -1 },
                    replica_nodes: vec![0],
                    isr_nodes: vec![0],
                    offline_replicas: Vec::new(),
                }],
                topic_authorized_operations: if version >= 8 { 3576 } else { i32::MIN },
            }],
            cluster_authorized_operations: if (8..=10).contains(&version) {
                8096
            } else {
                i32::MIN
            },
        }
    }

    fn encode_request(req: &Request, version: i16) -> BytesMut {
        let flexible = version >= MIN_FLEXIBLE;
        let mut w = BytesMut::new();
        let topics = req.topics.as_deref();
        write_nullable_array_len(&mut w, topics.map(<[_]>::len), flexible).unwrap();
        for t in topics.unwrap_or_default() {
            if version >= 10 {
                write_uuid(&mut w, &t.topic_id);
                write_nullable_str(&mut w, t.name.as_deref(), true).unwrap();
            } else {
                write_str(&mut w, t.name.as_deref().unwrap(), flexible).unwrap();
            }
            if flexible {
                tagged::write_empty(&mut w);
            }
        }
        if version >= 4 {
            write_i8(&mut w, i8::from(req.allow_auto_topic_creation));
        }
        if (8..=10).contains(&version) {
            write_i8(&mut w, i8::from(req.include_cluster_authorized_operations));
        }
        if version >= 8 {
            write_i8(&mut w, i8::from(req.include_topic_authorized_operations));
        }
        if flexible {
            tagged::write_empty(&mut w);
        }
        w
    }

    fn roundtrip_request(version: i16) {
        let req = sample_request(version);
        let w = encode_request(&req, version);
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());
    }

    fn roundtrip_response(version: i16) {
        let resp = sample_response(version);
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn request_v1_roundtrip() {
        roundtrip_request(1);
    }
    #[test]
    fn request_v4_auto_create_present() {
        roundtrip_request(4);
    }
    #[test]
    fn request_below_v4_defaults_auto_create_true() {
        // No field on the wire at v1-v3; Apache's schema default is
        // true, so the broker's own auto.create.topics.enable is the
        // only gate for those clients.
        let w = encode_request(
            &Request {
                topics: named(&["events"]),
                allow_auto_topic_creation: false,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false,
            },
            3,
        );
        let mut r = w.freeze();
        assert!(decode_request(&mut r, 3).unwrap().allow_auto_topic_creation);
    }
    #[test]
    fn request_v4_auto_create_false_is_preserved() {
        // v4+ clients that opt out must stay opted out.
        let w = encode_request(
            &Request {
                topics: named(&["events"]),
                allow_auto_topic_creation: false,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false,
            },
            4,
        );
        let mut r = w.freeze();
        assert!(!decode_request(&mut r, 4).unwrap().allow_auto_topic_creation);
    }
    #[test]
    fn request_v8_authz_flags_present() {
        roundtrip_request(8);
    }
    #[test]
    fn request_v9_flexible_roundtrip() {
        roundtrip_request(9);
    }
    #[test]
    fn request_v10_topic_id_roundtrip() {
        roundtrip_request(10);
    }

    // gh #274: null and empty are different requests from v1. These are
    // hand-written bytes in the layout of Apache's MetadataRequest.json,
    // not encoder output, so a bug the encoder and decoder share can't
    // hide here (gh #272).

    #[test]
    fn request_v1_null_topics_is_all_topics() {
        let mut b = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff]);
        assert_eq!(decode_request(&mut b, 1).unwrap().topics, None);
        assert!(b.is_empty());
    }

    #[test]
    fn request_v1_empty_topics_is_no_topics() {
        let mut b = Bytes::from_static(&[0, 0, 0, 0]);
        assert_eq!(decode_request(&mut b, 1).unwrap().topics, Some(vec![]));
        assert!(b.is_empty());
    }

    #[test]
    fn request_v9_compact_null_and_empty_differ() {
        // topics, allow_auto_topic_creation, include_cluster_authorized_operations,
        // include_topic_authorized_operations, request tagged fields.
        let mut null = Bytes::from_static(&[0x00, 1, 0, 0, 0]);
        assert_eq!(decode_request(&mut null, 9).unwrap().topics, None);
        assert!(null.is_empty());
        let mut empty = Bytes::from_static(&[0x01, 1, 0, 0, 0]);
        assert_eq!(decode_request(&mut empty, 9).unwrap().topics, Some(vec![]));
        assert!(empty.is_empty());
    }

    #[test]
    fn request_v10_keeps_a_null_name_and_its_id() {
        let mut raw = vec![0x02]; // one topic
        raw.extend_from_slice(&[0xab; 16]); // topic_id
        raw.push(0x00); // name: compact null
        raw.push(0x00); // topic tagged fields
        raw.extend_from_slice(&[1, 0, 0, 0]); // flags + request tagged fields
        let mut b = Bytes::from(raw);
        let got = decode_request(&mut b, 10).unwrap();
        assert_eq!(
            got.topics,
            Some(vec![RequestTopic {
                topic_id: [0xab; 16],
                name: None,
            }])
        );
        assert!(b.is_empty());
    }

    #[test]
    fn response_v1_roundtrip() {
        roundtrip_response(1);
    }
    #[test]
    fn response_v3_throttle_present() {
        roundtrip_response(3);
    }
    #[test]
    fn response_v5_offline_replicas_present() {
        roundtrip_response(5);
    }
    #[test]
    fn response_v7_leader_epoch_present() {
        roundtrip_response(7);
    }
    #[test]
    fn response_v8_authz_ops_present() {
        roundtrip_response(8);
    }
    #[test]
    fn response_v9_flexible_roundtrip() {
        roundtrip_response(9);
    }
    #[test]
    fn response_v10_topic_id_roundtrip() {
        let mut resp = sample_response(10);
        resp.topics[0].topic_id = [0xab; 16];
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 10).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 10).unwrap();
        assert_eq!(got.topics[0].topic_id, [0xab; 16]);
    }

    #[test]
    fn null_cluster_id_roundtrips() {
        let mut resp = sample_response(9);
        resp.cluster_id = None;
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 9).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, 9).unwrap();
        assert_eq!(got.cluster_id, None);
    }
}
