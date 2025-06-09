//! Tokio-compatible codecs for the protocol

use bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    errors::{ProtocolError, ProtocolResult},
    message::{Message, RawMessage},
    parser::MessageParser,
};

/// Codec for encoding and decoding protocol messages
///
/// This provides integration with tokio's framing system,
/// enabling efficient streaming message processing.
#[derive(Debug, Clone)]
pub struct MessageCodec {
    parser:         MessageParser,
    max_frame_size: usize,
}

impl MessageCodec {
    /// Create a new codec with default limits
    #[must_use]
    pub const fn new() -> Self {
        Self {
            parser:         MessageParser::new(),
            max_frame_size: crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Create a new codec with custom limits
    #[must_use]
    pub const fn with_limits(max_message_size: usize, max_header_size: usize) -> Self {
        Self {
            parser:         MessageParser::with_limits(max_message_size, max_header_size),
            max_frame_size: max_message_size,
        }
    }

    /// Get the current buffer size
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.parser.buffer_size()
    }
}

impl Default for MessageCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for MessageCodec {
    type Error = ProtocolError;
    type Item = Message;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Check frame size limit
        if src.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: src.len(),
                max:  self.max_frame_size,
            });
        }

        // Add new data to parser
        if !src.is_empty() {
            self.parser.push_data(src)?;
            src.clear();
        }

        // Try to parse a message
        self.parser.try_parse()
    }
}

impl Encoder<RawMessage> for MessageCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: RawMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.to_bytes()?;

        // Check frame size limit
        if bytes.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: bytes.len(),
                max:  self.max_frame_size,
            });
        }

        // Reserve space and write the message
        dst.reserve(bytes.len());
        dst.put(bytes);
        Ok(())
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = ProtocolError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.raw_bytes();

        // Check frame size limit
        if bytes.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: bytes.len(),
                max:  self.max_frame_size,
            });
        }

        // Reserve space and write the message
        dst.reserve(bytes.len());
        dst.put(bytes.as_ref());
        Ok(())
    }
}

/// A decoder-only codec for parsing incoming messages
///
/// This is useful when you only need to decode messages
/// and handle encoding separately.
#[derive(Debug)]
pub struct MessageDecoder {
    parser:         MessageParser,
    max_frame_size: usize,
}

impl MessageDecoder {
    /// Create a new decoder with default limits
    #[must_use]
    pub const fn new() -> Self {
        Self {
            parser:         MessageParser::new(),
            max_frame_size: crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Create a new decoder with custom limits
    #[must_use]
    pub const fn with_limits(max_message_size: usize, max_header_size: usize) -> Self {
        Self {
            parser:         MessageParser::with_limits(max_message_size, max_header_size),
            max_frame_size: max_message_size,
        }
    }

    /// Get the current buffer size
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.parser.buffer_size()
    }

    /// Clear the internal buffer
    pub fn clear(&mut self) {
        self.parser.clear();
    }
}

impl Default for MessageDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for MessageDecoder {
    type Error = ProtocolError;
    type Item = Message;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Check frame size limit
        if src.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: src.len(),
                max:  self.max_frame_size,
            });
        }

        // Add new data to parser
        if !src.is_empty() {
            self.parser.push_data(src)?;
            src.clear();
        }

        // Try to parse a message
        self.parser.try_parse()
    }
}

/// An encoder-only codec for sending messages
///
/// This is useful when you only need to encode messages
/// and handle decoding separately.
#[derive(Debug, Clone)]
pub struct MessageEncoder {
    max_frame_size: usize,
}

impl MessageEncoder {
    /// Create a new encoder with default limits
    #[must_use]
    pub const fn new() -> Self {
        Self {
            max_frame_size: crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Create a new encoder with custom frame size limit
    #[must_use]
    pub const fn with_max_frame_size(max_frame_size: usize) -> Self {
        Self { max_frame_size }
    }
}

impl Default for MessageEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder<RawMessage> for MessageEncoder {
    type Error = ProtocolError;

    fn encode(&mut self, item: RawMessage, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.to_bytes()?;

        // Check frame size limit
        if bytes.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: bytes.len(),
                max:  self.max_frame_size,
            });
        }

        // Reserve space and write the message
        dst.reserve(bytes.len());
        dst.put(bytes);
        Ok(())
    }
}

impl Encoder<Message> for MessageEncoder {
    type Error = ProtocolError;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let bytes = item.raw_bytes();

        // Check frame size limit
        if bytes.len() > self.max_frame_size {
            return Err(ProtocolError::MessageTooLarge {
                size: bytes.len(),
                max:  self.max_frame_size,
            });
        }

        // Reserve space and write the message
        dst.reserve(bytes.len());
        dst.put(bytes.as_ref());
        Ok(())
    }
}

/// Utility functions for working with the codecs
pub mod utils {
    use futures_util::{SinkExt, StreamExt};
    use tokio_util::codec::Framed;

    use super::{Message, MessageCodec, ProtocolError, ProtocolResult, RawMessage};

    /// Create a framed stream/sink from a tokio `AsyncRead` + `AsyncWrite`
    pub fn framed<T>(io: T) -> Framed<T, MessageCodec>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    {
        Framed::new(io, MessageCodec::new())
    }

    /// Create a framed stream/sink with custom limits
    pub fn framed_with_limits<T>(
        io: T,
        max_message_size: usize,
        max_header_size: usize,
    ) -> Framed<T, MessageCodec>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite,
    {
        Framed::new(
            io,
            MessageCodec::with_limits(max_message_size, max_header_size),
        )
    }

    /// Send a message using a framed sink
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying sink operation fails or if message
    /// encoding fails.
    pub async fn send_message<T>(
        sink: &mut Framed<T, MessageCodec>,
        message: RawMessage,
    ) -> ProtocolResult<()>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        sink.send(message).await.map_err(|e| ProtocolError::Io(e.to_string()))
    }

    /// Receive a message using a framed stream
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying stream operation fails or if message
    /// decoding fails.
    pub async fn receive_message<T>(
        stream: &mut Framed<T, MessageCodec>,
    ) -> ProtocolResult<Option<Message>>
    where
        T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    {
        match stream.next().await {
            Some(Ok(message)) => Ok(Some(message)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::{builder::MessageBuilder, types::*};

    #[test]
    fn test_codec_roundtrip() {
        let mut codec = MessageCodec::new();

        // Create a test message
        let raw_message = MessageBuilder::simple_publish(1000, "test.topic")
            .build_with_payload(&serde_json::json!({"data": "test"}))
            .unwrap();

        // Encode the message
        let mut buffer = BytesMut::new();
        codec.encode(raw_message, &mut buffer).unwrap();

        // Decode the message
        let decoded = codec.decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded.message_type(), MessageType::Publish);
        assert_eq!(decoded.client_id(), 1000);

        let header = decoded.header().unwrap();
        assert_eq!(header.topic, Some("test.topic".to_string()));
    }

    #[test]
    #[allow(clippy::similar_names)]
    fn test_decoder_only() {
        let mut decoder = MessageDecoder::new();

        // Create test data
        let raw_message = MessageBuilder::simple_subscribe(2000, "test.topic").build().unwrap();
        let bytes = raw_message.to_bytes().unwrap();

        // Decode the message
        let mut buffer = BytesMut::from(bytes.as_ref());
        let decoded = decoder.decode(&mut buffer).unwrap().unwrap();

        assert_eq!(decoded.message_type(), MessageType::Subscribe);
        assert_eq!(decoded.client_id(), 2000);
    }

    #[test]
    fn test_encoder_only() {
        let mut encoder = MessageEncoder::new();

        // Create a test message
        let raw_message = MessageBuilder::ping_message(3000).build().unwrap();

        // Encode the message
        let mut buffer = BytesMut::new();
        encoder.encode(raw_message, &mut buffer).unwrap();

        // Verify the buffer contains data
        assert!(!buffer.is_empty());

        // Quick verification of the encoded data
        assert_eq!(buffer[0], crate::constants::PROTOCOL_VERSION);
        assert_eq!(buffer[1], MessageType::Ping.to_u8());
    }

    #[test]
    fn test_streaming_parse() {
        let mut codec = MessageCodec::new();

        // Create test messages
        let msg1 = MessageBuilder::simple_publish(1000, "topic1").build().unwrap();
        let msg2 = MessageBuilder::simple_publish(2000, "topic2").build().unwrap();

        // Encode both messages
        let mut buffer1 = BytesMut::new();
        let mut buffer2 = BytesMut::new();
        codec.encode(msg1, &mut buffer1).unwrap();
        codec.encode(msg2, &mut buffer2).unwrap();

        // Combine into a single buffer (simulating network stream)
        let mut combined = BytesMut::new();
        combined.extend_from_slice(&buffer1);
        combined.extend_from_slice(&buffer2);

        // Decode both messages
        let decoded1 = codec.decode(&mut combined).unwrap().unwrap();
        let decoded2 = codec.decode(&mut combined).unwrap().unwrap();
        let decoded3 = codec.decode(&mut combined).unwrap();

        assert_eq!(decoded1.client_id(), 1000);
        assert_eq!(decoded2.client_id(), 2000);
        assert!(decoded3.is_none()); // No more messages
    }

    #[test]
    fn test_size_limits() {
        let mut codec = MessageCodec::with_limits(100, 50); // Very small limits

        // Create a large message that exceeds limits
        let large_payload = "x".repeat(200);
        let raw_message = MessageBuilder::simple_publish(1000, "test")
            .build_with_payload(&large_payload)
            .unwrap();

        // Encoding should fail due to size limit
        let mut buffer = BytesMut::new();
        let result = codec.encode(raw_message, &mut buffer);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProtocolError::MessageTooLarge { .. }
        ));
    }
}
