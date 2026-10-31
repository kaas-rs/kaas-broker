//! kaas-codec — Kafka wire frames, primitives, CRC32C, KIP-482 tagged fields.
//!
//! Kafka wire codec for every registered API key/version. Each module
//! round-trips its versions; `api::leave_group` also decodes a captured
//! Java-client request. A round trip can't catch a bug its encoder and
//! decoder share (gh #272), so header versions are pinned against
//! Apache's values in `api::registry`.
//!
//! # Byte-opacity contract
//!
//! RecordBatch payloads are byte-opaque throughout kaas-codec. There is **no
//! `Record` struct** in this crate — record-batch bytes flow as
//! `Option<bytes::Bytes>` and are never parsed. The only function that
//! touches batch bytes beyond the 61-byte v2 header is [`crc::verify_batch_crc`],
//! which treats them as opaque input to CRC32C.
//!
//! Nothing counts or alarms on a violation (gh #275): a code path that
//! starts decoding records wouldn't report itself. The invariant is held by
//! that structure plus the byte-identity tests on the disk engine and over
//! the wire (`bins/kaas/tests/smoke.rs`).

pub mod api;
pub mod crc;
pub mod errors;
pub mod frame;
pub mod headers;
pub mod primitives;
pub mod tagged;

pub use bytes::{Bytes, BytesMut};
pub use errors::CodecError;
pub use frame::{read_frame, write_frame, FrameError, FrameReader};
pub use headers::{
    decode_request_header, encode_response_header, HeaderVersion, RequestHeader, ResponseHeader,
};
