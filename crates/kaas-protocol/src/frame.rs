//! Per-connection framing wrapper around [`kaas_codec::frame`].
//!
//! Reads a Kafka request frame, parses its header (selecting the
//! header-version via the registry lookup keyed on `api_key` +
//! `api_version`), and returns `(header, body_bytes)`. Writes a
//! response frame: `[size:i32][correlation_id:i32][maybe tagged
//! fields][body]`.

use bytes::{Bytes, BytesMut};
use kaas_codec::api::registry;
use kaas_codec::frame::{write_frame, FrameError, FrameReader, MAX_FRAME_SIZE};
use kaas_codec::headers::{decode_request_header, encode_response_header, HeaderVersion};
use kaas_codec::{CodecError, RequestHeader, ResponseHeader};
use thiserror::Error;
use tokio::io::{self, AsyncRead, AsyncWrite};

#[derive(Debug, Error)]
pub enum ProtoError {
    #[error(transparent)]
    Frame(#[from] FrameError),

    #[error(transparent)]
    Codec(#[from] CodecError),

    #[error("short frame: {got} bytes, need ≥4 for api_key+version")]
    ShortFrame { got: usize },

    #[error(transparent)]
    Io(#[from] io::Error),
}

/// Owns a tokio AsyncRead+AsyncWrite stream and a frame reader budget.
pub struct Connection<S> {
    stream: S,
    reader: FrameReader,
}

impl<S> std::fmt::Debug for Connection<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("reader", &self.reader)
            .finish()
    }
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            reader: FrameReader::new(MAX_FRAME_SIZE),
        }
    }

    pub fn with_max_frame(stream: S, max_frame_bytes: usize) -> Self {
        Self {
            stream,
            reader: FrameReader::new(max_frame_bytes),
        }
    }

    /// Read one request frame. Returns the parsed header and the
    /// remaining body bytes. The header-version is selected via
    /// [`registry::lookup`] on the peek'd `(api_key, api_version)`.
    /// For unknown API keys, falls back to `HeaderVersion::V1` so the
    /// correlation id can still be extracted and a wire-correct
    /// UNSUPPORTED_VERSION response emitted by the dispatcher.
    pub async fn read_request(&mut self) -> Result<(RequestHeader, Bytes), ProtoError> {
        let mut frame = self.reader.read(&mut self.stream).await?;
        if frame.len() < 4 {
            return Err(ProtoError::ShortFrame { got: frame.len() });
        }
        let api_key = i16::from_be_bytes([frame[0], frame[1]]);
        let api_version = i16::from_be_bytes([frame[2], frame[3]]);
        let hv = match registry::lookup(api_key) {
            Some(spec) if api_version >= spec.min_version && api_version <= spec.max_version => {
                spec.request_header(api_version)
            }
            // Unknown key or out-of-range version: V1 is the safe
            // fallback. It at least reads the client_id nullable
            // string, which every real client sends. If the wire
            // really was V2, the trailing tagged-fields block lands
            // inside `body` and the dispatcher's UNSUPPORTED_VERSION
            // response drops it on the floor.
            _ => HeaderVersion::V1,
        };
        let header = decode_request_header(&mut frame, hv)?;
        Ok((header, frame))
    }

    /// Write a response frame: `[size:i32][correlation_id:i32]
    /// [maybe tagged fields][body]`. `header_version` controls
    /// whether the empty tagged-fields block follows the correlation
    /// id — picked by the dispatcher from
    /// [`registry::ApiSpec::response_header`].
    pub async fn write_response(
        &mut self,
        correlation_id: i32,
        body: &[u8],
        header_version: HeaderVersion,
    ) -> Result<(), ProtoError> {
        let mut framed = BytesMut::with_capacity(body.len() + 8);
        encode_response_header(
            &mut framed,
            &ResponseHeader { correlation_id },
            header_version,
        );
        framed.extend_from_slice(body);
        write_frame(&mut self.stream, &framed).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use kaas_codec::headers::encode_request_header;
    use kaas_codec::primitives::write_compact_string;
    use kaas_codec::tagged;
    use kaas_codec::RequestHeader;
    use tokio::io::duplex;

    fn build_api_versions_v3_request_frame() -> BytesMut {
        // Header v2 (flexible) + ApiVersions v3 body
        // (compact_string client_software_name + version + tagged_fields).
        let mut hdr_buf = BytesMut::new();
        encode_request_header(
            &mut hdr_buf,
            &RequestHeader {
                api_key: 18,
                api_version: 3,
                correlation_id: 42,
                client_id: Some("test".to_owned()),
            },
            HeaderVersion::V2,
        )
        .unwrap();
        write_compact_string(&mut hdr_buf, "franz-go").unwrap();
        write_compact_string(&mut hdr_buf, "1.0.0").unwrap();
        tagged::write_empty(&mut hdr_buf);
        hdr_buf
    }

    #[tokio::test]
    async fn read_request_parses_header_via_registry() {
        let (mut a, b) = duplex(256);
        let body = build_api_versions_v3_request_frame();
        // Write a Kafka-framed message: [size:i32][body].
        tokio::spawn(async move {
            kaas_codec::frame::write_frame(&mut a, &body).await.unwrap();
        });
        let mut conn = Connection::new(b);
        let (hdr, _) = conn.read_request().await.unwrap();
        assert_eq!(hdr.api_key, 18);
        assert_eq!(hdr.api_version, 3);
        assert_eq!(hdr.correlation_id, 42);
        assert_eq!(hdr.client_id.as_deref(), Some("test"));
    }

    #[tokio::test]
    async fn write_response_emits_correlation_id_and_optional_tags() {
        let (a, mut b) = duplex(256);
        let mut conn = Connection::new(a);
        let body: &[u8] = &[0xde, 0xad, 0xbe, 0xef];
        conn.write_response(42, body, HeaderVersion::V1)
            .await
            .unwrap();

        // Read it back: [size:i32][correlation_id:i32][tagged_fields:1][body].
        let frame = kaas_codec::frame::read_frame(&mut b).await.unwrap();
        assert_eq!(frame.len(), 4 + 1 + body.len());
        assert_eq!(&frame[0..4], &42i32.to_be_bytes());
        assert_eq!(frame[4], 0, "v1 tagged-fields block = single 0 uvarint");
        assert_eq!(&frame[5..], body);
    }

    #[tokio::test]
    async fn write_response_v0_skips_tagged_fields() {
        let (a, mut b) = duplex(256);
        let mut conn = Connection::new(a);
        let body: &[u8] = &[0x01, 0x02];
        conn.write_response(7, body, HeaderVersion::V0)
            .await
            .unwrap();
        let frame = kaas_codec::frame::read_frame(&mut b).await.unwrap();
        assert_eq!(frame.len(), 4 + body.len());
        assert_eq!(&frame[0..4], &7i32.to_be_bytes());
    }

    #[tokio::test]
    async fn short_frame_rejected() {
        let (mut a, b) = duplex(64);
        tokio::spawn(async move {
            // Frame with body len < 4
            kaas_codec::frame::write_frame(&mut a, &[0u8; 2])
                .await
                .unwrap();
        });
        let mut conn = Connection::new(b);
        let err = conn.read_request().await.unwrap_err();
        assert!(matches!(err, ProtoError::ShortFrame { got: 2 }));
    }
}
