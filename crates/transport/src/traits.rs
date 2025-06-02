//! Core transport traits
//!
//! This module defines the core traits and interfaces for the transport layer.
//! The transport layer provides a unified API for different transport
//! mechanisms (TCP, IPC, WebSocket, STDIO) with a focus on performance, memory
//! efficiency, and proper async handling.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use fleximq_protocol::message::Message;
use tokio::io::{AsyncRead, AsyncWrite};

use crate::error::TransportResult;

/// Information about a transport connection
#[derive(Debug, Clone)]
pub struct ConnectionInfo {
    /// Transport type (tcp, ipc, ws, stdio)
    pub transport_type: String,
    /// Local address (if applicable)
    pub local_addr:     Option<String>,
    /// Remote address (if applicable)
    pub remote_addr:    Option<String>,
    /// Connection metadata
    pub metadata:       std::collections::HashMap<String, String>,
    /// Connection established timestamp
    pub established_at: std::time::Instant,
}

/// Trait for low-level transport streams that can send/receive raw data
#[async_trait]
pub trait TransportStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {
    /// Get connection information
    fn connection_info(&self) -> &ConnectionInfo;

    /// Check if the connection is still alive
    fn is_connected(&self) -> bool;

    /// Close the connection gracefully
    async fn close(&mut self) -> TransportResult<()>;

    /// Get connection timeout settings
    fn timeouts(&self) -> ConnectionTimeouts;
}

/// Connection timeout settings
#[derive(Debug, Clone, Copy)]
pub struct ConnectionTimeouts {
    /// Read timeout
    pub read_timeout:    Option<Duration>,
    /// Write timeout
    pub write_timeout:   Option<Duration>,
    /// Connection timeout (for establishing connections)
    pub connect_timeout: Option<Duration>,
}

impl Default for ConnectionTimeouts {
    fn default() -> Self {
        Self {
            read_timeout:    Some(Duration::from_secs(30)),
            write_timeout:   Some(Duration::from_secs(30)),
            connect_timeout: Some(Duration::from_secs(10)),
        }
    }
}

/// Main transport trait for different transport implementations
#[async_trait]
pub trait Transport: Send + Sync {
    /// The stream type this transport produces
    type Stream: TransportStream;

    /// Connect to a remote endpoint
    async fn connect(&self, url: &str) -> TransportResult<Self::Stream>;

    /// Listen for incoming connections (for server transports)
    async fn listen(
        &self,
        url: &str,
    ) -> TransportResult<Box<dyn TransportListener<Stream = Self::Stream>>>;

    /// Get the transport type name
    fn transport_type(&self) -> &str;

    /// Check if this transport supports the given URL scheme
    fn supports_url(&self, url: &str) -> bool;
}

/// Trait for transport listeners that accept incoming connections
#[async_trait]
pub trait TransportListener: Send + Sync {
    /// The stream type this listener produces
    type Stream: TransportStream;

    /// Accept a new incoming connection
    async fn accept(&mut self) -> TransportResult<Self::Stream>;

    /// Get the local address of the transport
    ///
    /// # Errors
    ///
    /// Returns an error if the local address cannot be determined (e.g., if the
    /// underlying socket is not bound or an I/O error occurs).
    fn local_addr(&self) -> TransportResult<String>;

    /// Closes the listener, stopping it from accepting new connections
    async fn close(&mut self) -> TransportResult<()>;
}

/// Extension trait for binary frame-based transport (low-level)
#[async_trait]
pub trait FramedTransport: TransportStream {
    /// Read a complete binary frame
    async fn read_frame(&mut self) -> TransportResult<Bytes>;

    /// Write a complete binary frame
    async fn write_frame(&mut self, data: Bytes) -> TransportResult<()>;
}

/// Higher-level message-based transport that directly works with protocol
/// Messages
#[async_trait]
pub trait MessageTransport: Send + Sync {
    /// Send a protocol message
    async fn send_message(&mut self, message: &Message) -> TransportResult<()>;

    /// Receive a protocol message
    async fn receive_message(&mut self) -> TransportResult<Message>;

    /// Get the underlying transport stream
    fn stream(&self) -> &dyn TransportStream;

    /// Get a mutable reference to the underlying transport stream
    fn stream_mut(&mut self) -> &mut dyn TransportStream;

    /// Close the transport
    async fn close(&mut self) -> TransportResult<()>;
}

/// Transport factory for creating transport instances
#[async_trait]
pub trait TransportFactory: Send + Sync {
    /// Create a transport instance from a URL
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid, the transport scheme is
    /// unsupported, or if there's an issue initializing the transport
    /// (e.g., network errors for TCP).
    fn create_transport(
        &self,
        url: &str,
    ) -> TransportResult<Arc<dyn Transport<Stream = Box<dyn TransportStream>>>>;

    /// Create a message transport instance from a URL
    async fn create_message_transport(
        &self,
        url: &str,
    ) -> TransportResult<Box<dyn MessageTransport>>;
}

// Implement TransportStream for Box<dyn TransportStream> to enable type erasure
#[async_trait]
impl TransportStream for Box<dyn TransportStream> {
    fn connection_info(&self) -> &ConnectionInfo {
        (**self).connection_info()
    }

    fn is_connected(&self) -> bool {
        (**self).is_connected()
    }

    async fn close(&mut self) -> TransportResult<()> {
        (**self).close().await
    }

    fn timeouts(&self) -> ConnectionTimeouts {
        (**self).timeouts()
    }
}
