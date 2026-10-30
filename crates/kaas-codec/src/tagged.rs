//! KIP-482 tagged fields (the "flexible version" extension block at the tail
//! of every flexible request/response).
//!
//! Wire shape: `uvarint(num_fields) || for each: uvarint(tag) ||
//! uvarint(value_len) || value_bytes`.
//!
//! Most Apache 3.7 code paths emit `num_fields = 0`. Write that with
//! [`write_empty`]; read by [`read`] which discards unknown tags (Apache's
//! documented forward-compat contract).

use bytes::{Bytes, BytesMut};

use crate::errors::CodecError;
use crate::primitives::{read_uvarint, write_uvarint};

/// Read and discard every tagged field —
/// unknown tags are silently consumed, and known tags are not surfaced to
/// the caller.
pub fn read(buf: &mut Bytes) -> Result<(), CodecError> {
    let n = read_uvarint(buf)?;
    for _ in 0..n {
        let _tag = read_uvarint(buf)?;
        let size = read_uvarint(buf)?;
        let size_us = usize::try_from(size).map_err(|_| CodecError::LengthOverflow)?;
        if buf.len() < size_us {
            return Err(CodecError::UnexpectedEof);
        }
        let _ = buf.split_to(size_us);
    }
    Ok(())
}

/// The single-byte `uvarint(0)` that marks "no tagged fields". By far the
/// most common case on the wire.
pub fn write_empty(buf: &mut BytesMut) {
    write_uvarint(buf, 0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_block_roundtrips() {
        let mut buf = BytesMut::new();
        write_empty(&mut buf);
        let mut b = buf.freeze();
        assert!(read(&mut b).is_ok());
        assert!(b.is_empty());
    }

    #[test]
    fn unknown_tags_discarded_by_read() {
        // Two fields: tag 42 (3 bytes) and tag 99 (empty).
        let mut b = Bytes::from_static(&[2, 42, 3, 0xff, 0xee, 0xdd, 99, 0]);
        assert!(read(&mut b).is_ok());
        assert!(b.is_empty());
    }

    #[test]
    fn truncated_field_value_is_eof() {
        let mut b = Bytes::from_static(&[1, 42, 3, 0xff]);
        assert_eq!(read(&mut b), Err(CodecError::UnexpectedEof));
    }
}
