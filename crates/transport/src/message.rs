//! Message utilities for transport
//!
//! This module provides utilities for reading and writing protocol messages
//! over any transport stream. It bridges the gap between low-level transport
//! and high-level protocol messages.

use async_trait::async_trait;
use bytes::BytesMut;
use fleximq_protocol::{
    codec::{MessageDecoder, MessageEncoder},
    errors::ProtocolError,
    message::{Message, RawMessage},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_util::codec::{Decoder, Encoder};

use crate::{
    error::{TransportError, TransportResult},
    traits::{MessageTransport, TransportStream},
};

/// A message transport implementation that wraps any transport stream
pub struct MessageTransportAdapter<S> {
    stream:      S,
    decoder:     MessageDecoder,
    encoder:     MessageEncoder,
    read_buffer: BytesMut,
}

impl<S> MessageTransportAdapter<S>
where
    S: TransportStream,
{
    /// Create a new message transport adapter from any transport stream
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: MessageDecoder::new(),
            encoder: MessageEncoder::new(),
            read_buffer: BytesMut::with_capacity(8192), // 8KB initial buffer
        }
    }

    /// Create a new message transport adapter with custom buffer capacity
    pub fn with_capacity(stream: S, capacity: usize) -> Self {
        Self {
            stream,
            decoder: MessageDecoder::new(),
            encoder: MessageEncoder::new(),
            read_buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Create a new message transport adapter with custom limits
    pub fn with_limits(
        stream: S,
        max_message_size: usize,
        max_header_size: usize,
        buffer_capacity: usize,
    ) -> Self {
        Self {
            stream,
            decoder: MessageDecoder::with_limits(max_message_size, max_header_size),
            encoder: MessageEncoder::with_max_frame_size(max_message_size),
            read_buffer: BytesMut::with_capacity(buffer_capacity),
        }
    }

    /// Get a reference to the underlying stream
    pub const fn get_ref(&self) -> &S {
        &self.stream
    }

    /// Get a mutable reference to the underlying stream
    pub fn get_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Extract the inner stream, consuming the adapter
    pub fn into_inner(self) -> S {
        self.stream
    }
}

#[async_trait]
impl<S> MessageTransport for MessageTransportAdapter<S>
where
    S: TransportStream,
{
    async fn send_message(&mut self, message: &Message) -> TransportResult<()> {
        // Encode the message
        let mut buffer = BytesMut::new();
        self.encoder
            .encode(message.clone(), &mut buffer)
            .map_err(TransportError::Protocol)?;

        // Write the buffer to the stream
        self.stream
            .write_all(&buffer)
            .await
            .map_err(|e| TransportError::Io(format!("Error writing message: {e}")))?;

        // Flush to ensure it's sent
        self.stream
            .flush()
            .await
            .map_err(|e| TransportError::Io(format!("Error flushing after message write: {e}")))?;

        Ok(())
    }

    async fn receive_message(&mut self) -> TransportResult<Message> {
        // Keep reading until we can parse a complete message
        loop {
            // Try to decode a message from the buffer
            match self.decoder.decode(&mut self.read_buffer) {
                Ok(Some(message)) => return Ok(message),
                Ok(None) => {
                    // Need more data, continue reading
                }
                Err(e) => {
                    return Err(TransportError::Protocol(e));
                }
            }

            // Read more data into the buffer
            let mut temp_buf = [0u8; 4096];
            let bytes_read = self
                .stream
                .read(&mut temp_buf)
                .await
                .map_err(|e| TransportError::Io(format!("Error reading message data: {e}")))?;

            if bytes_read == 0 {
                // EOF reached with no complete message
                return Err(TransportError::ConnectionClosed);
            }

            // Extend the buffer with the new data
            self.read_buffer.extend_from_slice(&temp_buf[..bytes_read]);
        }
    }

    fn stream(&self) -> &dyn TransportStream {
        &self.stream
    }

    fn stream_mut(&mut self) -> &mut dyn TransportStream {
        &mut self.stream
    }

    async fn close(&mut self) -> TransportResult<()> {
        self.stream.close().await
    }
}

/// Extension trait to add message reading/writing to any `TransportStream`
#[async_trait]
pub trait MessageStreamExt: TransportStream {
    /// Read a protocol message from the stream
    async fn read_message(&mut self) -> TransportResult<Message>;

    /// Write a protocol message to the stream
    async fn write_message(&mut self, message: &RawMessage) -> TransportResult<()>;
}

// Implement for all transport streams
#[async_trait]
impl<T: TransportStream> MessageStreamExt for T {
    async fn read_message(&mut self) -> TransportResult<Message> {
        // Use direct decoding instead of the adapter
        let mut decoder = MessageDecoder::new();
        let mut buffer = BytesMut::with_capacity(8192);

        loop {
            // Try to decode a message from the buffer
            if let Some(message) = decoder.decode(&mut buffer)? {
                return Ok(message);
            }

            // Read more data into the buffer
            let mut temp_buf = [0u8; 4096];
            let bytes_read = self
                .read(&mut temp_buf)
                .await
                .map_err(|e| TransportError::Io(format!("Error reading message data: {e}")))?;

            if bytes_read == 0 {
                // EOF reached with no complete message
                return Err(TransportError::ConnectionClosed);
            }

            // Extend the buffer with the new data
            buffer.extend_from_slice(&temp_buf[..bytes_read]);
        }
    }

    async fn write_message(&mut self, message: &RawMessage) -> TransportResult<()> {
        // Use direct encoding instead of the adapter
        // let mut encoder = MessageEncoder::new();
        // let mut buffer = BytesMut::new();

        // Encode the message
        // encoder.encode(message.clone(), &mut
        // buffer).map_err(TransportError::Protocol)?;

        // Write the buffer to the stream
        self.write_all(&message.to_bytes()?)
            .await
            .map_err(|e| TransportError::Io(format!("Error writing message: {e}")))?;

        // Flush to ensure it's sent
        self.flush()
            .await
            .map_err(|e| TransportError::Io(format!("Error flushing after message write: {e}")))?;

        Ok(())
    }
}

// Helper struct to borrow a transport stream reference
pub struct StreamBorrow<'a, S: TransportStream> {
    stream: &'a mut S,
}

impl<'a, S: TransportStream> StreamBorrow<'a, S> {
    pub fn new(stream: &'a mut S) -> Self {
        Self { stream }
    }
}

#[async_trait]
impl<S: TransportStream> TransportStream for StreamBorrow<'_, S> {
    fn connection_info(&self) -> &crate::traits::ConnectionInfo {
        self.stream.connection_info()
    }

    fn is_connected(&self) -> bool {
        self.stream.is_connected()
    }

    async fn close(&mut self) -> TransportResult<()> {
        self.stream.close().await
    }

    fn timeouts(&self) -> crate::traits::ConnectionTimeouts {
        self.stream.timeouts()
    }
}

impl<S: TransportStream> AsyncRead for StreamBorrow<'_, S> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: We're not moving the stream
        unsafe { std::pin::Pin::new_unchecked(&mut *self.get_mut().stream) }.poll_read(cx, buf)
    }
}

impl<S: TransportStream> AsyncWrite for StreamBorrow<'_, S> {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        // Safety: We're not moving the stream
        unsafe { std::pin::Pin::new_unchecked(&mut *self.get_mut().stream) }.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        // Safety: We're not moving the stream
        unsafe { std::pin::Pin::new_unchecked(&mut *self.get_mut().stream) }.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        // Safety: We're not moving the stream
        unsafe { std::pin::Pin::new_unchecked(&mut *self.get_mut().stream) }.poll_shutdown(cx)
    }
}

/// Creates a `MessageTransportAdapter` from any `TransportStream`
pub fn message_transport<S: TransportStream>(stream: S) -> MessageTransportAdapter<S> {
    MessageTransportAdapter::new(stream)
}

/// Utility function to convert a protocol error to a transport error
#[must_use]
pub const fn protocol_error_to_transport(err: ProtocolError) -> TransportError {
    TransportError::Protocol(err)
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Instant};

    use fleximq_protocol::{builder::MessageBuilder, types::MessageType};
    use tokio::io::{AsyncReadExt, duplex};
    use tokio_util::codec::Decoder;

    use super::*;

    struct MockTransportStream {
        reader:    tokio::io::ReadHalf<tokio::io::DuplexStream>,
        writer:    tokio::io::WriteHalf<tokio::io::DuplexStream>,
        info:      crate::traits::ConnectionInfo,
        connected: bool,
    }

    impl MockTransportStream {
        fn new() -> (Self, tokio::io::DuplexStream) {
            let (client, server) = duplex(1024);
            let (reader, writer) = tokio::io::split(client);

            let info = crate::traits::ConnectionInfo {
                transport_type: "mock".to_string(),
                local_addr:     Some("local".to_string()),
                remote_addr:    Some("remote".to_string()),
                metadata:       HashMap::new(),
                established_at: Instant::now(),
            };

            (
                Self {
                    reader,
                    writer,
                    info,
                    connected: true,
                },
                server,
            )
        }
    }

    #[async_trait]
    impl TransportStream for MockTransportStream {
        fn connection_info(&self) -> &crate::traits::ConnectionInfo {
            &self.info
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        async fn close(&mut self) -> TransportResult<()> {
            self.connected = false;
            Ok(())
        }

        fn timeouts(&self) -> crate::traits::ConnectionTimeouts {
            crate::traits::ConnectionTimeouts::default()
        }
    }

    impl AsyncRead for MockTransportStream {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            let this = self.get_mut();
            // Safety: We're not moving the reader
            unsafe { std::pin::Pin::new_unchecked(&mut this.reader) }.poll_read(cx, buf)
        }
    }

    impl AsyncWrite for MockTransportStream {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &[u8],
        ) -> std::task::Poll<Result<usize, std::io::Error>> {
            let this = self.get_mut();
            // Safety: We're not moving the writer
            unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_write(cx, buf)
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            let this = self.get_mut();
            // Safety: We're not moving the writer
            unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_flush(cx)
        }

        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), std::io::Error>> {
            let this = self.get_mut();
            // Safety: We're not moving the writer
            unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_shutdown(cx)
        }
    }

    #[tokio::test]
    #[allow(clippy::similar_names)]
    async fn test_message_transport_adapter() {
        let (mut client, mut server) = MockTransportStream::new();

        // Create a test message
        let raw_message = MessageBuilder::simple_publish(1000, "test.topic").build().unwrap();

        // Write the message using the extension method
        client.write_message(&raw_message).await.unwrap();

        // Read the message from the server
        let mut buffer = BytesMut::with_capacity(1024);
        let mut temp_buf = [0u8; 1024];
        let bytes_read = server.read(&mut temp_buf).await.unwrap();
        buffer.extend_from_slice(&temp_buf[..bytes_read]);

        // Decode the message
        let mut decoder = MessageDecoder::new();
        let decoded = decoder.decode(&mut buffer).unwrap().unwrap();

        // Verify the message
        assert_eq!(decoded.message_type(), MessageType::Publish);
        assert_eq!(decoded.client_id(), 1000);
        assert_eq!(
            decoded.header().unwrap().topic.as_deref(),
            Some("test.topic")
        );
    }
}
