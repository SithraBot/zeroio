//! Core transport traits

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
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
}

/// Trait for transport streams that can send/receive data
#[async_trait]
pub trait TransportStream: AsyncRead + AsyncWrite + Send + Sync + Unpin {
    /// Get connection information
    fn connection_info(&self) -> &ConnectionInfo;

    /// Check if the connection is still alive
    fn is_connected(&self) -> bool;

    /// Close the connection gracefully
    async fn close(&mut self) -> TransportResult<()>;
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

    /// Get the local address this listener is bound to
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if the local address cannot be retrieved.
    fn local_addr(&self) -> TransportResult<String>;

    /// Closes the listener, stopping it from accepting new connections.
    /// Depending on the transport, this might also release underlying
    /// resources.
    async fn close(&mut self) -> TransportResult<()>;
}

/// Extension trait for framed message reading/writing
#[async_trait]
pub trait FramedTransport: TransportStream {
    /// Read a complete message frame
    async fn read_frame(&mut self) -> TransportResult<Bytes>;

    /// Write a complete message frame
    async fn write_frame(&mut self, data: &[u8]) -> TransportResult<()>;
}

/// Transport factory for creating transport instances
pub trait TransportFactory: Send + Sync {
    /// Create a transport instance from a URL
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if a transport cannot be created for the given
    /// URL (e.g., unsupported scheme, invalid URL format).
    fn create_transport(
        &self,
        url: &str,
    ) -> TransportResult<Arc<dyn Transport<Stream = Box<dyn TransportStream>>>>;
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
}
