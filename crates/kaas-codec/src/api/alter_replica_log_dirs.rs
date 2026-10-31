//! AlterReplicaLogDirs — API key 34 (KIP-113).
//!
//! Versions 0..=2, matching Apache 3.7's served range (gh #222);
//! flexible from v2. The verb that moves a replica between log dirs
//! on the same broker — in kaas, the gh #221 volume-pool migration
//! path: the leader copies the partition to the target pool volume
//! and flips the placement record.
//!
//! Request: `[{path, [{topic, [partitions]}]}]` — the *path* of the
//! destination log dir, as reported by `DescribeLogDirs`.
//! Response: per-partition error codes.

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_compact_array_len, read_compact_string, read_i16, read_i32, read_string,
    write_compact_array_len, write_compact_string, write_i16, write_i32, write_string,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 2);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 34,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

const fn flexible(version: i16) -> bool {
    version >= MIN_FLEXIBLE
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    pub dirs: Vec<RequestDir>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestDir {
    /// Absolute path of the destination log dir.
    pub path: String,
    pub topics: Vec<RequestTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    pub results: Vec<ResponseTopic>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseTopic {
    pub topic_name: String,
    pub partitions: Vec<ResponsePartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponsePartition {
    pub partition_index: i32,
    pub error_code: i16,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    let flex = flexible(version);
    let nd = if flex {
        read_compact_array_len(buf)?
    } else {
        usize::try_from(read_i32(buf)?).unwrap_or(0)
    };
    let mut dirs = Vec::with_capacity(nd);
    for _ in 0..nd {
        let path = if flex {
            read_compact_string(buf)?
        } else {
            read_string(buf)?
        };
        let nt = if flex {
            read_compact_array_len(buf)?
        } else {
            usize::try_from(read_i32(buf)?).unwrap_or(0)
        };
        let mut topics = Vec::with_capacity(nt);
        for _ in 0..nt {
            let name = if flex {
                read_compact_string(buf)?
            } else {
                read_string(buf)?
            };
            let np = if flex {
                read_compact_array_len(buf)?
            } else {
                usize::try_from(read_i32(buf)?).unwrap_or(0)
            };
            let mut partitions = Vec::with_capacity(np);
            for _ in 0..np {
                partitions.push(read_i32(buf)?);
            }
            if flex {
                tagged::read(buf)?;
            }
            topics.push(RequestTopic { name, partitions });
        }
        if flex {
            tagged::read(buf)?;
        }
        dirs.push(RequestDir { path, topics });
    }
    if flex {
        tagged::read(buf)?;
    }
    Ok(Request { dirs })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    let flex = flexible(version);
    if flex {
        write_compact_array_len(buf, req.dirs.len())?;
    } else {
        write_i32(buf, i32::try_from(req.dirs.len()).unwrap_or(i32::MAX));
    }
    for d in &req.dirs {
        if flex {
            write_compact_string(buf, &d.path)?;
            write_compact_array_len(buf, d.topics.len())?;
        } else {
            write_string(buf, &d.path)?;
            write_i32(buf, i32::try_from(d.topics.len()).unwrap_or(i32::MAX));
        }
        for t in &d.topics {
            if flex {
                write_compact_string(buf, &t.name)?;
                write_compact_array_len(buf, t.partitions.len())?;
            } else {
                write_string(buf, &t.name)?;
                write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
            }
            for p in &t.partitions {
                write_i32(buf, *p);
            }
            if flex {
                tagged::write_empty(buf);
            }
        }
        if flex {
            tagged::write_empty(buf);
        }
    }
    if flex {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn encode_response(
    buf: &mut BytesMut,
    resp: &Response,
    version: i16,
) -> Result<(), CodecError> {
    let flex = flexible(version);
    write_i32(buf, resp.throttle_time_ms);
    if flex {
        write_compact_array_len(buf, resp.results.len())?;
    } else {
        write_i32(buf, i32::try_from(resp.results.len()).unwrap_or(i32::MAX));
    }
    for t in &resp.results {
        if flex {
            write_compact_string(buf, &t.topic_name)?;
            write_compact_array_len(buf, t.partitions.len())?;
        } else {
            write_string(buf, &t.topic_name)?;
            write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
        }
        for p in &t.partitions {
            write_i32(buf, p.partition_index);
            write_i16(buf, p.error_code);
            if flex {
                tagged::write_empty(buf);
            }
        }
        if flex {
            tagged::write_empty(buf);
        }
    }
    if flex {
        tagged::write_empty(buf);
    }
    Ok(())
}

pub fn decode_response(buf: &mut Bytes, version: i16) -> Result<Response, CodecError> {
    let flex = flexible(version);
    let throttle_time_ms = read_i32(buf)?;
    let nt = if flex {
        read_compact_array_len(buf)?
    } else {
        usize::try_from(read_i32(buf)?).unwrap_or(0)
    };
    let mut results = Vec::with_capacity(nt);
    for _ in 0..nt {
        let topic_name = if flex {
            read_compact_string(buf)?
        } else {
            read_string(buf)?
        };
        let np = if flex {
            read_compact_array_len(buf)?
        } else {
            usize::try_from(read_i32(buf)?).unwrap_or(0)
        };
        let mut partitions = Vec::with_capacity(np);
        for _ in 0..np {
            let p = ResponsePartition {
                partition_index: read_i32(buf)?,
                error_code: read_i16(buf)?,
            };
            if flex {
                tagged::read(buf)?;
            }
            partitions.push(p);
        }
        if flex {
            tagged::read(buf)?;
        }
        results.push(ResponseTopic {
            topic_name,
            partitions,
        });
    }
    if flex {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_versions_roundtrip() {
        for version in 0..=2 {
            let req = Request {
                dirs: vec![RequestDir {
                    path: "/vols/fast".into(),
                    topics: vec![RequestTopic {
                        name: "orders".into(),
                        partitions: vec![0, 2],
                    }],
                }],
            };
            let mut w = BytesMut::new();
            encode_request(&mut w, &req, version).unwrap();
            let mut r = w.freeze();
            let got = decode_request(&mut r, version).unwrap();
            assert_eq!(got, req, "request v{version}");
            assert!(r.is_empty());

            let resp = Response {
                throttle_time_ms: 0,
                results: vec![ResponseTopic {
                    topic_name: "orders".into(),
                    partitions: vec![
                        ResponsePartition {
                            partition_index: 0,
                            error_code: 0,
                        },
                        ResponsePartition {
                            partition_index: 2,
                            error_code: 57,
                        },
                    ],
                }],
            };
            let mut w = BytesMut::new();
            encode_response(&mut w, &resp, version).unwrap();
            let mut r = w.freeze();
            let got = decode_response(&mut r, version).unwrap();
            assert_eq!(got, resp, "response v{version}");
            assert!(r.is_empty());
        }
    }
}
