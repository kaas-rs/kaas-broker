//! Wire primitives: signed integers, varints, strings, bytes, arrays.
//!
//! Free functions over `&mut bytes::Bytes` (read side) and `&mut bytes::BytesMut`
//! (write side).
//!
//! ## Null sentinel encoding
//!
//! Legacy (non-flexible) APIs encode "null" as `-1` for the length prefix.
//! Compact (KIP-482 flexible) APIs use uvarint(0). Both encodings round-trip
//! through `Option<T>` here.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::errors::CodecError;

// ---------------------------------------------------------------------------
// fixed-width integers
// ---------------------------------------------------------------------------

#[inline]
fn require(buf: &Bytes, n: usize) -> Result<(), CodecError> {
    if buf.remaining() < n {
        Err(CodecError::UnexpectedEof)
    } else {
        Ok(())
    }
}

pub fn read_i8(buf: &mut Bytes) -> Result<i8, CodecError> {
    require(buf, 1)?;
    Ok(buf.get_i8())
}

pub fn read_i16(buf: &mut Bytes) -> Result<i16, CodecError> {
    require(buf, 2)?;
    Ok(buf.get_i16())
}

pub fn read_i32(buf: &mut Bytes) -> Result<i32, CodecError> {
    require(buf, 4)?;
    Ok(buf.get_i32())
}

pub fn read_i64(buf: &mut Bytes) -> Result<i64, CodecError> {
    require(buf, 8)?;
    Ok(buf.get_i64())
}

pub fn read_u8(buf: &mut Bytes) -> Result<u8, CodecError> {
    require(buf, 1)?;
    Ok(buf.get_u8())
}

pub fn read_f64(buf: &mut Bytes) -> Result<f64, CodecError> {
    require(buf, 8)?;
    Ok(buf.get_f64())
}

pub fn read_bool(buf: &mut Bytes) -> Result<bool, CodecError> {
    Ok(read_u8(buf)? != 0)
}

pub fn write_i8(buf: &mut BytesMut, v: i8) {
    buf.put_i8(v);
}
pub fn write_i16(buf: &mut BytesMut, v: i16) {
    buf.put_i16(v);
}
pub fn write_i32(buf: &mut BytesMut, v: i32) {
    buf.put_i32(v);
}
pub fn write_i64(buf: &mut BytesMut, v: i64) {
    buf.put_i64(v);
}
pub fn write_f64(buf: &mut BytesMut, v: f64) {
    buf.put_f64(v);
}
pub fn write_bool(buf: &mut BytesMut, v: bool) {
    buf.put_u8(u8::from(v));
}

// ---------------------------------------------------------------------------
// varints (uvarint = LEB128 / protobuf style; varint = zigzag-then-uvarint)
// ---------------------------------------------------------------------------

/// Maximum number of bytes a u64 occupies under uvarint encoding.
const MAX_UVARINT_LEN: usize = 10;

pub fn read_uvarint(buf: &mut Bytes) -> Result<u64, CodecError> {
    let mut x: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_UVARINT_LEN {
        let b = read_u8(buf)?;
        if b < 0x80 {
            // Last byte. Reject overflow encoding: 10th byte > 1 would
            // shift past bit 63.
            if i == MAX_UVARINT_LEN - 1 && b > 1 {
                return Err(CodecError::InvalidUvarint);
            }
            return Ok(x | (u64::from(b) << shift));
        }
        x |= u64::from(b & 0x7f) << shift;
        shift += 7;
    }
    Err(CodecError::InvalidUvarint)
}

pub fn write_uvarint(buf: &mut BytesMut, v: u64) {
    let mut x = v;
    while x >= 0x80 {
        let byte = u8::try_from(x & 0x7f).unwrap_or(0) | 0x80;
        buf.put_u8(byte);
        x >>= 7;
    }
    let last = u8::try_from(x & 0x7f).unwrap_or(0);
    buf.put_u8(last);
}

/// ZigZag encode: `(n << 1) ^ (n >> 63)`.
fn zigzag_encode(n: i64) -> u64 {
    let encoded = n.wrapping_shl(1) ^ (n >> 63);
    u64::from_le_bytes(encoded.to_le_bytes())
}

pub fn write_varint(buf: &mut BytesMut, v: i64) {
    write_uvarint(buf, zigzag_encode(v));
}

// ---------------------------------------------------------------------------
// length helpers
// ---------------------------------------------------------------------------

/// Convert a byte length into the i16 used for legacy (non-flexible) string
/// prefixes. Errors if the length doesn't fit Kafka's signed-i16 string limit.
fn len_i16(n: usize) -> Result<i16, CodecError> {
    i16::try_from(n).map_err(|_| CodecError::LengthOverflow)
}

/// Convert a byte length into the i32 used for legacy (non-flexible) byte
/// prefixes.
fn len_i32(n: usize) -> Result<i32, CodecError> {
    i32::try_from(n).map_err(|_| CodecError::LengthOverflow)
}

/// Convert a count into the uvarint `count + 1` used for compact lengths.
fn compact_len_u64(n: usize) -> Result<u64, CodecError> {
    u64::try_from(n)
        .map_err(|_| CodecError::LengthOverflow)?
        .checked_add(1)
        .ok_or(CodecError::LengthOverflow)
}

/// Convert a uvarint length prefix back into a usize, validating against the
/// buffer's remaining capacity.
fn checked_compact_len(u: u64, max: usize) -> Result<usize, CodecError> {
    // u was decoded as the wire value; subtract 1 to get the actual length.
    // u == 0 is the null sentinel and is handled by callers before this.
    let len = usize::try_from(u - 1).map_err(|_| CodecError::LengthOverflow)?;
    if len > max {
        Err(CodecError::InvalidLength {
            got: i64::try_from(len).unwrap_or(i64::MAX),
            max,
        })
    } else {
        Ok(len)
    }
}

// ---------------------------------------------------------------------------
// strings: legacy (int16-prefixed) and compact (uvarint-prefixed)
// ---------------------------------------------------------------------------

pub fn read_string(buf: &mut Bytes) -> Result<String, CodecError> {
    match read_nullable_string(buf)? {
        Some(s) => Ok(s),
        None => Err(CodecError::UnexpectedNull),
    }
}

pub fn read_nullable_string(buf: &mut Bytes) -> Result<Option<String>, CodecError> {
    let raw = read_i16(buf)?;
    if raw < 0 {
        return Ok(None);
    }
    let len = usize::try_from(raw).map_err(|_| CodecError::LengthOverflow)?;
    require(buf, len)?;
    let bytes = buf.copy_to_bytes(len);
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| CodecError::InvalidUtf8)?
        .to_owned();
    Ok(Some(s))
}

pub fn read_compact_string(buf: &mut Bytes) -> Result<String, CodecError> {
    match read_compact_nullable_string(buf)? {
        Some(s) => Ok(s),
        None => Err(CodecError::UnexpectedNull),
    }
}

pub fn read_compact_nullable_string(buf: &mut Bytes) -> Result<Option<String>, CodecError> {
    let u = read_uvarint(buf)?;
    if u == 0 {
        return Ok(None);
    }
    let len = checked_compact_len(u, buf.remaining())?;
    let bytes = buf.copy_to_bytes(len);
    let s = std::str::from_utf8(&bytes)
        .map_err(|_| CodecError::InvalidUtf8)?
        .to_owned();
    Ok(Some(s))
}

pub fn write_string(buf: &mut BytesMut, s: &str) -> Result<(), CodecError> {
    write_i16(buf, len_i16(s.len())?);
    buf.put_slice(s.as_bytes());
    Ok(())
}

pub fn write_nullable_string(buf: &mut BytesMut, s: Option<&str>) -> Result<(), CodecError> {
    match s {
        None => {
            write_i16(buf, -1);
            Ok(())
        }
        Some(s) => write_string(buf, s),
    }
}

pub fn write_compact_string(buf: &mut BytesMut, s: &str) -> Result<(), CodecError> {
    write_uvarint(buf, compact_len_u64(s.len())?);
    buf.put_slice(s.as_bytes());
    Ok(())
}

pub fn write_compact_nullable_string(
    buf: &mut BytesMut,
    s: Option<&str>,
) -> Result<(), CodecError> {
    match s {
        None => {
            write_uvarint(buf, 0);
            Ok(())
        }
        Some(s) => write_compact_string(buf, s),
    }
}

// ---------------------------------------------------------------------------
// bytes: legacy (int32-prefixed) and compact (uvarint-prefixed)
// ---------------------------------------------------------------------------

pub fn read_bytes(buf: &mut Bytes) -> Result<Bytes, CodecError> {
    match read_nullable_bytes(buf)? {
        Some(b) => Ok(b),
        None => Err(CodecError::UnexpectedNull),
    }
}

pub fn read_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, CodecError> {
    let raw = read_i32(buf)?;
    if raw < 0 {
        return Ok(None);
    }
    let len = usize::try_from(raw).map_err(|_| CodecError::LengthOverflow)?;
    require(buf, len)?;
    // `copy_to_bytes` increments refcount on the underlying arena — true
    // zero-copy (gh #132).
    Ok(Some(buf.copy_to_bytes(len)))
}

pub fn read_compact_bytes(buf: &mut Bytes) -> Result<Bytes, CodecError> {
    match read_compact_nullable_bytes(buf)? {
        Some(b) => Ok(b),
        None => Err(CodecError::UnexpectedNull),
    }
}

pub fn read_compact_nullable_bytes(buf: &mut Bytes) -> Result<Option<Bytes>, CodecError> {
    let u = read_uvarint(buf)?;
    if u == 0 {
        return Ok(None);
    }
    let len = checked_compact_len(u, buf.remaining())?;
    Ok(Some(buf.copy_to_bytes(len)))
}

pub fn write_bytes(buf: &mut BytesMut, b: &[u8]) -> Result<(), CodecError> {
    write_i32(buf, len_i32(b.len())?);
    buf.put_slice(b);
    Ok(())
}

pub fn write_nullable_bytes(buf: &mut BytesMut, b: Option<&[u8]>) -> Result<(), CodecError> {
    match b {
        None => {
            write_i32(buf, -1);
            Ok(())
        }
        Some(b) => write_bytes(buf, b),
    }
}

pub fn write_compact_bytes(buf: &mut BytesMut, b: &[u8]) -> Result<(), CodecError> {
    write_uvarint(buf, compact_len_u64(b.len())?);
    buf.put_slice(b);
    Ok(())
}

pub fn write_compact_nullable_bytes(
    buf: &mut BytesMut,
    b: Option<&[u8]>,
) -> Result<(), CodecError> {
    match b {
        None => {
            write_uvarint(buf, 0);
            Ok(())
        }
        Some(b) => write_compact_bytes(buf, b),
    }
}

// ---------------------------------------------------------------------------
// raw bytes + uuid
// ---------------------------------------------------------------------------

pub fn read_raw(buf: &mut Bytes, n: usize) -> Result<Bytes, CodecError> {
    require(buf, n)?;
    Ok(buf.copy_to_bytes(n))
}

pub fn read_uuid(buf: &mut Bytes) -> Result<[u8; 16], CodecError> {
    require(buf, 16)?;
    let mut out = [0u8; 16];
    buf.copy_to_slice(&mut out);
    Ok(out)
}

pub fn write_uuid(buf: &mut BytesMut, v: &[u8; 16]) {
    buf.put_slice(v);
}

// ---------------------------------------------------------------------------
// arrays
// ---------------------------------------------------------------------------

/// Read the length prefix of a legacy (int32) array. Returns the element
/// count; a negative wire value (null array) is normalised to 0
/// ("null = empty" wire semantics).
pub fn read_array_len(buf: &mut Bytes) -> Result<usize, CodecError> {
    let raw = read_i32(buf)?;
    if raw < 0 {
        Ok(0)
    } else {
        usize::try_from(raw).map_err(|_| CodecError::LengthOverflow)
    }
}

pub fn write_array_len(buf: &mut BytesMut, count: usize) -> Result<(), CodecError> {
    write_i32(buf, len_i32(count)?);
    Ok(())
}

/// Read the length prefix of a compact (KIP-482 flexible) array. Returns the
/// element count; uvarint(0) (null array) is normalised to 0.
pub fn read_compact_array_len(buf: &mut Bytes) -> Result<usize, CodecError> {
    let u = read_uvarint(buf)?;
    if u == 0 {
        return Ok(0);
    }
    usize::try_from(u - 1).map_err(|_| CodecError::LengthOverflow)
}

pub fn write_compact_array_len(buf: &mut BytesMut, count: usize) -> Result<(), CodecError> {
    write_uvarint(buf, compact_len_u64(count)?);
    Ok(())
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn enc<F: FnOnce(&mut BytesMut)>(f: F) -> Bytes {
        let mut buf = BytesMut::new();
        f(&mut buf);
        buf.freeze()
    }

    /// Inverse of [`write_varint`]. Nothing on the request path decodes
    /// a signed varint (they only appear inside RecordBatches, which stay
    /// byte-opaque), so the decoder lives here as the round-trip oracle.
    fn read_varint(buf: &mut Bytes) -> Result<i64, CodecError> {
        let u = read_uvarint(buf)?;
        // ZigZag decode: `(u >> 1) ^ -(u & 1)`, then a u64 → i64
        // bit-reinterpret without `as`.
        let v = (u >> 1) ^ (u & 1).wrapping_neg();
        Ok(i64::from_le_bytes(v.to_le_bytes()))
    }

    #[test]
    fn fixed_width_roundtrips() {
        for &v in &[0i8, 1, -1, i8::MAX, i8::MIN] {
            let mut b = enc(|w| write_i8(w, v));
            assert_eq!(read_i8(&mut b).unwrap(), v);
        }
        for &v in &[0i16, 1, -1, i16::MAX, i16::MIN] {
            let mut b = enc(|w| write_i16(w, v));
            assert_eq!(read_i16(&mut b).unwrap(), v);
        }
        for &v in &[0i32, 1, -1, i32::MAX, i32::MIN] {
            let mut b = enc(|w| write_i32(w, v));
            assert_eq!(read_i32(&mut b).unwrap(), v);
        }
        for &v in &[0i64, 1, -1, i64::MAX, i64::MIN] {
            let mut b = enc(|w| write_i64(w, v));
            assert_eq!(read_i64(&mut b).unwrap(), v);
        }
    }

    #[test]
    fn uvarint_known_values() {
        // Cross-reference values from the v0.1 test suite.
        for &v in &[
            0u64,
            1,
            127,
            128,
            255,
            300,
            u64::from(u32::MAX),
            u64::MAX / 2,
        ] {
            let mut b = enc(|w| write_uvarint(w, v));
            assert_eq!(read_uvarint(&mut b).unwrap(), v);
        }
    }

    #[test]
    fn varint_known_values() {
        for &v in &[
            0i64,
            1,
            -1,
            63,
            -64,
            i64::from(i32::MAX),
            i64::from(i32::MIN),
            i64::MAX,
            i64::MIN,
        ] {
            let mut b = enc(|w| write_varint(w, v));
            assert_eq!(read_varint(&mut b).unwrap(), v);
        }
    }

    #[test]
    fn nullable_string_roundtrip() {
        let mut b = enc(|w| write_nullable_string(w, Some("hello")).unwrap());
        assert_eq!(
            read_nullable_string(&mut b).unwrap().as_deref(),
            Some("hello")
        );

        let mut b = enc(|w| write_nullable_string(w, None).unwrap());
        assert_eq!(read_nullable_string(&mut b).unwrap(), None);
    }

    #[test]
    fn compact_nullable_string_roundtrip() {
        let mut b = enc(|w| write_compact_nullable_string(w, Some("hi")).unwrap());
        assert_eq!(
            read_compact_nullable_string(&mut b).unwrap().as_deref(),
            Some("hi")
        );

        let mut b = enc(|w| write_compact_nullable_string(w, None).unwrap());
        assert_eq!(read_compact_nullable_string(&mut b).unwrap(), None);
    }

    #[test]
    fn bytes_roundtrip() {
        let payload: &[u8] = &[1, 2, 3, 4, 5];
        let mut b = enc(|w| write_bytes(w, payload).unwrap());
        let got = read_bytes(&mut b).unwrap();
        assert_eq!(&got[..], payload);
    }

    #[test]
    fn compact_nullable_bytes_roundtrip() {
        let mut b = enc(|w| write_compact_nullable_bytes(w, Some(&[9])).unwrap());
        assert_eq!(
            read_compact_nullable_bytes(&mut b).unwrap().as_deref(),
            Some(&[9u8][..])
        );

        let mut b = enc(|w| write_compact_nullable_bytes(w, None).unwrap());
        assert_eq!(read_compact_nullable_bytes(&mut b).unwrap(), None);
    }

    #[test]
    fn array_roundtrip() {
        let vals: &[i32] = &[10, 20, 30, -1, i32::MAX];
        let mut b = enc(|w| {
            write_array_len(w, vals.len()).unwrap();
            for v in vals {
                write_i32(w, *v);
            }
        });
        let n = read_array_len(&mut b).unwrap();
        assert_eq!(n, vals.len());
        for v in vals {
            assert_eq!(read_i32(&mut b).unwrap(), *v);
        }
    }

    #[test]
    fn compact_array_roundtrip() {
        let vals: &[i32] = &[100, 200, 300];
        let mut b = enc(|w| {
            write_compact_array_len(w, vals.len()).unwrap();
            for v in vals {
                write_i32(w, *v);
            }
        });
        let n = read_compact_array_len(&mut b).unwrap();
        assert_eq!(n, vals.len());
        for v in vals {
            assert_eq!(read_i32(&mut b).unwrap(), *v);
        }
    }

    #[test]
    fn null_compact_array_decodes_as_empty() {
        let mut b = enc(|w| write_uvarint(w, 0));
        assert_eq!(read_compact_array_len(&mut b).unwrap(), 0);
    }

    #[test]
    fn unexpected_eof() {
        let mut b = Bytes::from_static(&[0]);
        assert_eq!(read_i32(&mut b), Err(CodecError::UnexpectedEof));
    }

    #[test]
    fn uvarint_overflow() {
        // 10 bytes with high bit set on byte 9 + payload 2 → overflow.
        let bad: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        let mut b = Bytes::copy_from_slice(bad);
        assert_eq!(read_uvarint(&mut b), Err(CodecError::InvalidUvarint));
    }

    // -- proptests -----------------------------------------------------------

    proptest! {
        #[test]
        fn pt_uvarint(v in any::<u64>()) {
            let mut b = enc(|w| write_uvarint(w, v));
            prop_assert_eq!(read_uvarint(&mut b).unwrap(), v);
        }

        #[test]
        fn pt_varint(v in any::<i64>()) {
            let mut b = enc(|w| write_varint(w, v));
            prop_assert_eq!(read_varint(&mut b).unwrap(), v);
        }

        #[test]
        fn pt_string(s in ".{0,1024}") {
            let mut b = enc(|w| write_string(w, &s).unwrap());
            prop_assert_eq!(read_string(&mut b).unwrap(), s);
        }

        #[test]
        fn pt_nullable_string(s in proptest::option::of(".{0,512}")) {
            let mut b = enc(|w| write_nullable_string(w, s.as_deref()).unwrap());
            prop_assert_eq!(read_nullable_string(&mut b).unwrap(), s);
        }

        #[test]
        fn pt_compact_string(s in ".{0,1024}") {
            let mut b = enc(|w| write_compact_string(w, &s).unwrap());
            prop_assert_eq!(read_compact_string(&mut b).unwrap(), s);
        }

        #[test]
        fn pt_compact_nullable_string(s in proptest::option::of(".{0,512}")) {
            let mut b = enc(|w| write_compact_nullable_string(w, s.as_deref()).unwrap());
            prop_assert_eq!(read_compact_nullable_string(&mut b).unwrap(), s);
        }

        #[test]
        fn pt_bytes(v in proptest::collection::vec(any::<u8>(), 0..1024)) {
            let mut b = enc(|w| write_bytes(w, &v).unwrap());
            let got = read_bytes(&mut b).unwrap();
            prop_assert_eq!(&got[..], &v[..]);
        }

        #[test]
        fn pt_compact_nullable_bytes(v in proptest::option::of(proptest::collection::vec(any::<u8>(), 0..512))) {
            let mut b = enc(|w| write_compact_nullable_bytes(w, v.as_deref()).unwrap());
            let got = read_compact_nullable_bytes(&mut b).unwrap();
            prop_assert_eq!(got.as_deref(), v.as_deref());
        }
    }
}
