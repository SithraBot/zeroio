//! In-memory channel transport for single-process testing and embedding.
//!
//! This transport uses `tokio::io::duplex` to create an in-memory full-duplex
//! byte stream. Peers connect to a URI of the form `chan://<name>` – the first
//! caller creates a new in-memory channel and waits for its counterpart; the
//! second caller receives the other end of the existing channel. This makes it
//! easy to spin up two peers communicating within the same binary without
//! touching the network stack.
//!
//! NOTE: This implementation is **not** intended for production use. It exists
//! solely for deterministic testing and examples.

use std::{collections::VecDeque, time::Instant};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf, ReadHalf, WriteHalf, duplex};
use url::Url;

use crate::{
    error::{TransportError, TransportResult},
    traits::{ConnectionInfo, ConnectionTimeouts, Transport, TransportStream},
};

/// Global registry mapping `channel` names to waiting endpoints.
static CHANNEL_REGISTRY: Lazy<dashmap::DashMap<String, VecDeque<DuplexStream>>> =
    Lazy::new(dashmap::DashMap::new);

/// A transport stream backed by a Tokio in-memory duplex.
pub struct ChannelStream {
    reader:    ReadHalf<DuplexStream>,
    writer:    WriteHalf<DuplexStream>,
    info:      ConnectionInfo,
    connected: bool,
}

impl ChannelStream {
    fn new(stream: DuplexStream, name: &str) -> Self {
        let (reader, writer) = tokio::io::split(stream);
        Self {
            reader,
            writer,
            info: ConnectionInfo {
                transport_type: "chan".into(),
                local_addr:     Some(format!("chan://{name}")),
                remote_addr:    Some(format!("chan://{name}")),
                metadata:       std::collections::HashMap::new(),
                established_at: Instant::now(),
            },
            connected: true,
        }
    }
}

#[async_trait]
impl TransportStream for ChannelStream {
    fn connection_info(&self) -> &ConnectionInfo {
        &self.info
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    async fn close(&mut self) -> TransportResult<()> {
        self.connected = false;
        Ok(())
    }

    fn timeouts(&self) -> ConnectionTimeouts {
        ConnectionTimeouts::default()
    }
}

impl AsyncRead for ChannelStream {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: We never move fields after pin
        let this = self.get_mut();
        unsafe { std::pin::Pin::new_unchecked(&mut this.reader) }.poll_read(cx, buf)
    }
}

impl AsyncWrite for ChannelStream {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        // Safety: We never move fields after pin
        let this = self.get_mut();
        unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_write(cx, buf)
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: We never move fields after pin
        let this = self.get_mut();
        unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_flush(cx)
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        // Safety: We never move fields after pin
        let this = self.get_mut();
        unsafe { std::pin::Pin::new_unchecked(&mut this.writer) }.poll_shutdown(cx)
    }
}

/// Channel transport implementation.
#[derive(Debug, Clone)]
pub struct ChannelTransport;

impl Default for ChannelTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ChannelTransport {
    #[must_use]
    /// Creates a new `ChannelTransport` instance.
    pub const fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Transport for ChannelTransport {
    type Stream = ChannelStream;

    async fn connect(&self, url: &str) -> TransportResult<Self::Stream> {
        // Parse and validate URL
        let parsed = Url::parse(url).map_err(|e| TransportError::InvalidUrl(e.to_string()))?;
        if parsed.scheme() != "chan" {
            return Err(TransportError::UnsupportedTransport(format!(
                "invalid scheme for channel transport: {}",
                parsed.scheme()
            )));
        }

        let name = parsed.host_str().unwrap_or("").to_string();
        if name.is_empty() {
            return Err(TransportError::InvalidUrl("channel name missing".into()));
        }

        // Look for a waiting endpoint first
        if let Some(mut entry) = CHANNEL_REGISTRY.get_mut(&name) {
            if let Some(stream) = entry.pop_front() {
                return Ok(ChannelStream::new(stream, &name));
            }
        }

        // No waiting peer – create a new duplex and store one half
        let (a, b) = duplex(8 * 1024); // 8 KiB buffer

        CHANNEL_REGISTRY.entry(name.clone()).or_default().push_back(a);

        Ok(ChannelStream::new(b, &name))
    }

    async fn listen(
        &self,
        _url: &str,
    ) -> TransportResult<Box<dyn crate::traits::TransportListener<Stream = Self::Stream>>> {
        Err(TransportError::UnsupportedTransport(
            "channel transport does not support listen".into(),
        ))
    }

    fn transport_type(&self) -> &'static str {
        "chan"
    }

    fn supports_url(&self, url: &str) -> bool {
        url.starts_with("chan://")
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use fleximq_protocol::{builder::MessageBuilder, types::MessageType};
    use futures_util::{SinkExt, StreamExt};

    use super::*;
    use crate::message::sink; // bring trait methods in scope

    #[tokio::test]
    async fn test_channel_transport_pair() {
        let transport = ChannelTransport::new();
        let url = "chan://test";

        // Spawn first side, which will wait for peer
        let t1_transport = transport.clone();
        let t1 = tokio::spawn(async move { t1_transport.connect(url).await.unwrap() });

        // Slight delay to ensure t1 waits
        tokio::time::sleep(Duration::from_millis(10)).await;

        let stream2 = transport.connect(url).await.unwrap();
        let mut m2 = sink(stream2);

        let stream1 = t1.await.unwrap();
        let mut m1 = sink(stream1);

        // Build simple publish message
        let raw = MessageBuilder::simple_publish(1, "test.topic").build().unwrap();
        let msg = raw.into_message().unwrap();

        m1.send(&msg).await.unwrap();
        let received = m2.next().await.unwrap().unwrap();

        assert_eq!(received.client_id(), msg.client_id());
        assert_eq!(received.message_type(), MessageType::Publish);
    }
}
