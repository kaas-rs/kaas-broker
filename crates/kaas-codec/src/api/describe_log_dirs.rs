//! DescribeLogDirs — API key 35.
//!
//! Versions 0..=4, matching Apache 3.7's served range (gh #222):
//!
//! - v0–v1 — non-flexible baseline.
//! - v2+ — flexible encoding (KIP-482 compact strings/arrays + tagged
//!   fields).
//! - v3+ — top-level `ErrorCode` on the response.
//! - v4+ — per-log-dir `TotalBytes` / `UsableBytes` (KIP-827) — the
//!   per-volume capacity surface the gh #221 volume pool reports.
//!   `-1` = unknown (the documented sentinel).
//!
//! A null topics array on the request means "describe every log dir,
//! every topic".

use bytes::BytesMut;

use crate::api::registry::ApiSpec;
use crate::errors::CodecError;
use crate::primitives::{
    read_bool, read_compact_array_len, read_compact_string, read_i16, read_i32, read_i64,
    read_string, read_uvarint, write_bool, write_compact_array_len, write_compact_string,
    write_i16, write_i32, write_i64, write_string, write_uvarint,
};
use crate::tagged;
use crate::Bytes;

pub const VERSIONS: (i16, i16) = (0, 4);
pub const MIN_FLEXIBLE: i16 = 2;

pub const SPEC: ApiSpec = ApiSpec {
    key: 35,
    min_version: VERSIONS.0,
    max_version: VERSIONS.1,
    min_flexible: Some(MIN_FLEXIBLE),
};

const fn flexible(version: i16) -> bool {
    version >= MIN_FLEXIBLE
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Request {
    /// `None` ↔ the request's topics array was null ("describe
    /// everything").
    pub topics: Option<Vec<RequestTopic>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RequestTopic {
    pub name: String,
    pub partitions: Vec<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Response {
    pub throttle_time_ms: i32,
    /// v3+ top-level error code; 0 below v3.
    pub error_code: i16,
    pub results: Vec<LogDirResult>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirResult {
    pub error_code: i16,
    pub log_dir: String,
    pub topics: Vec<ResponseTopic>,
    /// KIP-827 (v4+): filesystem capacity of this log dir. `-1` =
    /// unknown.
    pub total_bytes: i64,
    /// KIP-827 (v4+): usable bytes remaining. `-1` = unknown.
    pub usable_bytes: i64,
}

impl Default for LogDirResult {
    fn default() -> Self {
        Self {
            error_code: 0,
            log_dir: String::new(),
            topics: Vec::new(),
            total_bytes: -1,
            usable_bytes: -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ResponseTopic {
    pub name: String,
    pub partitions: Vec<ResponsePartition>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ResponsePartition {
    pub partition_index: i32,
    pub partition_size: i64,
    pub offset_lag: i64,
    pub is_future_key: bool,
}

pub fn decode_request(buf: &mut Bytes, version: i16) -> Result<Request, CodecError> {
    if flexible(version) {
        // Compact nullable array: uvarint 0 = null, N+1 = length N.
        let raw = read_uvarint(buf)?;
        if raw == 0 {
            tagged::read(buf)?;
            return Ok(Request { topics: None });
        }
        let count = usize::try_from(raw - 1).map_err(|_| CodecError::LengthOverflow)?;
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            let name = read_compact_string(buf)?;
            let p_count = read_compact_array_len(buf)?;
            let mut partitions = Vec::with_capacity(p_count);
            for _ in 0..p_count {
                partitions.push(read_i32(buf)?);
            }
            tagged::read(buf)?;
            topics.push(RequestTopic { name, partitions });
        }
        tagged::read(buf)?;
        return Ok(Request {
            topics: Some(topics),
        });
    }
    let count = read_i32(buf)?;
    if count < 0 {
        return Ok(Request { topics: None });
    }
    let mut topics = Vec::with_capacity(usize::try_from(count).unwrap_or(0));
    for _ in 0..count {
        let name = read_string(buf)?;
        let p_count = read_i32(buf)?;
        let mut partitions = Vec::with_capacity(usize::try_from(p_count).unwrap_or(0));
        for _ in 0..p_count {
            partitions.push(read_i32(buf)?);
        }
        topics.push(RequestTopic { name, partitions });
    }
    Ok(Request {
        topics: Some(topics),
    })
}

pub fn encode_request(buf: &mut BytesMut, req: &Request, version: i16) -> Result<(), CodecError> {
    if flexible(version) {
        match req.topics.as_ref() {
            None => write_uvarint(buf, 0),
            Some(topics) => {
                write_uvarint(buf, u64::try_from(topics.len()).unwrap_or(0) + 1);
                for t in topics {
                    write_compact_string(buf, &t.name)?;
                    write_compact_array_len(buf, t.partitions.len())?;
                    for p in &t.partitions {
                        write_i32(buf, *p);
                    }
                    tagged::write_empty(buf);
                }
            }
        }
        tagged::write_empty(buf);
        return Ok(());
    }
    let Some(topics) = req.topics.as_ref() else {
        write_i32(buf, -1);
        return Ok(());
    };
    write_i32(buf, i32::try_from(topics.len()).unwrap_or(i32::MAX));
    for t in topics {
        write_string(buf, &t.name)?;
        write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
        for p in &t.partitions {
            write_i32(buf, *p);
        }
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
    if version >= 3 {
        write_i16(buf, resp.error_code);
    }
    if flex {
        write_compact_array_len(buf, resp.results.len())?;
    } else {
        write_i32(buf, i32::try_from(resp.results.len()).unwrap_or(i32::MAX));
    }
    for r in &resp.results {
        write_i16(buf, r.error_code);
        if flex {
            write_compact_string(buf, &r.log_dir)?;
            write_compact_array_len(buf, r.topics.len())?;
        } else {
            write_string(buf, &r.log_dir)?;
            write_i32(buf, i32::try_from(r.topics.len()).unwrap_or(i32::MAX));
        }
        for t in &r.topics {
            if flex {
                write_compact_string(buf, &t.name)?;
                write_compact_array_len(buf, t.partitions.len())?;
            } else {
                write_string(buf, &t.name)?;
                write_i32(buf, i32::try_from(t.partitions.len()).unwrap_or(i32::MAX));
            }
            for p in &t.partitions {
                write_i32(buf, p.partition_index);
                write_i64(buf, p.partition_size);
                write_i64(buf, p.offset_lag);
                write_bool(buf, p.is_future_key);
                if flex {
                    tagged::write_empty(buf);
                }
            }
            if flex {
                tagged::write_empty(buf);
            }
        }
        if version >= 4 {
            write_i64(buf, r.total_bytes);
            write_i64(buf, r.usable_bytes);
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
    let error_code = if version >= 3 { read_i16(buf)? } else { 0 };
    let n = if flex {
        read_compact_array_len(buf)?
    } else {
        usize::try_from(read_i32(buf)?).unwrap_or(0)
    };
    let mut results = Vec::with_capacity(n);
    for _ in 0..n {
        let dir_error_code = read_i16(buf)?;
        let log_dir = if flex {
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
                let p = ResponsePartition {
                    partition_index: read_i32(buf)?,
                    partition_size: read_i64(buf)?,
                    offset_lag: read_i64(buf)?,
                    is_future_key: read_bool(buf)?,
                };
                if flex {
                    tagged::read(buf)?;
                }
                partitions.push(p);
            }
            if flex {
                tagged::read(buf)?;
            }
            topics.push(ResponseTopic { name, partitions });
        }
        let (total_bytes, usable_bytes) = if version >= 4 {
            (read_i64(buf)?, read_i64(buf)?)
        } else {
            (-1, -1)
        };
        if flex {
            tagged::read(buf)?;
        }
        results.push(LogDirResult {
            error_code: dir_error_code,
            log_dir,
            topics,
            total_bytes,
            usable_bytes,
        });
    }
    if flex {
        tagged::read(buf)?;
    }
    Ok(Response {
        throttle_time_ms,
        error_code,
        results,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(version: i16) {
        let req = Request {
            topics: Some(vec![RequestTopic {
                name: "orders".into(),
                partitions: vec![0, 1, 2],
            }]),
        };
        let mut w = BytesMut::new();
        encode_request(&mut w, &req, version).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, version).unwrap();
        assert_eq!(got, req, "request v{version}");
        assert!(r.is_empty());

        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
            results: vec![LogDirResult {
                error_code: 0,
                log_dir: "/data".into(),
                topics: vec![ResponseTopic {
                    name: "orders".into(),
                    partitions: vec![ResponsePartition {
                        partition_index: 0,
                        partition_size: 4096,
                        offset_lag: 0,
                        is_future_key: false,
                    }],
                }],
                total_bytes: if version >= 4 { 1_000_000 } else { -1 },
                usable_bytes: if version >= 4 { 900_000 } else { -1 },
            }],
        };
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, version).unwrap();
        let mut r = w.freeze();
        let got = decode_response(&mut r, version).unwrap();
        assert_eq!(got, resp, "response v{version}");
        assert!(r.is_empty());
    }

    #[test]
    fn all_versions_roundtrip() {
        for v in 0..=4 {
            roundtrip(v);
        }
    }

    #[test]
    fn null_topics_means_describe_everything() {
        let mut w = BytesMut::new();
        encode_request(&mut w, &Request { topics: None }, 0).unwrap();
        assert_eq!(&w[..], &(-1i32).to_be_bytes());
        let mut r = w.freeze();
        let got = decode_request(&mut r, 0).unwrap();
        assert_eq!(got.topics, None);

        // Flexible shape: uvarint 0 (null) + empty tagged section.
        let mut w = BytesMut::new();
        encode_request(&mut w, &Request { topics: None }, 4).unwrap();
        let mut r = w.freeze();
        let got = decode_request(&mut r, 4).unwrap();
        assert_eq!(got.topics, None);
    }

    /// KIP-827 fields drop cleanly below v4 and survive at v4.
    #[test]
    fn capacity_fields_are_v4_gated() {
        let resp = Response {
            throttle_time_ms: 0,
            error_code: 0,
            results: vec![LogDirResult {
                log_dir: "/vols/fast".into(),
                total_bytes: 42,
                usable_bytes: 21,
                ..Default::default()
            }],
        };
        for v in [1, 3] {
            let mut w = BytesMut::new();
            encode_response(&mut w, &resp, v).unwrap();
            let got = decode_response(&mut w.freeze(), v).unwrap();
            assert_eq!(got.results[0].total_bytes, -1, "v{v} has no capacity");
        }
        let mut w = BytesMut::new();
        encode_response(&mut w, &resp, 4).unwrap();
        let got = decode_response(&mut w.freeze(), 4).unwrap();
        assert_eq!(got.results[0].total_bytes, 42);
        assert_eq!(got.results[0].usable_bytes, 21);
    }
}
