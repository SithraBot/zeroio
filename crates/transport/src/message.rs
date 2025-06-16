//! Message utilities for transport
//!
//! This module provides utilities for reading and writing protocol messages
//! over any transport stream. It bridges the gap between low-level transport
//! and high-level protocol messages.

use async_trait::async_trait;
use fleximq_protocol::{
    MessageCodec, MessageDecoder, MessageEncoder,
    codec::{codec, decoder, encoder},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, FramedRead, FramedWrite};

use crate::{error::TransportResult, traits::TransportStream};

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

/// Creates a new message reader for the given stream.
#[must_use]
#[inline]
pub fn reader<S: TransportStream>(stream: S) -> FramedRead<S, MessageDecoder> {
    FramedRead::new(stream, decoder())
}

/// Creates a new message writer for the given stream.
#[must_use]
#[inline]
pub fn writer<S: TransportStream>(stream: S) -> FramedWrite<S, MessageEncoder> {
    FramedWrite::new(stream, encoder())
}

pub fn sink<S: TransportStream>(stream: S) -> Framed<S, MessageCodec> {
    Framed::new(stream, codec())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, time::Instant};

    use bytes::BytesMut;
    use fleximq_protocol::{MessageDecoder, builder::MessageBuilder, types::MessageType};
    use tokio::io::{AsyncReadExt, AsyncWriteExt, duplex};
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
        let raw_message = MessageBuilder::simple_publish(1000, "test.topic")
            .build()
            .unwrap()
            .to_bytes()
            .unwrap();

        // Write the message using the extension method
        client.write_all(&raw_message).await.unwrap();

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
