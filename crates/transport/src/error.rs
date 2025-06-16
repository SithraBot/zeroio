//! Transport error types
//!
//! This module defines the error types and results used throughout the
//! transport layer. It provides comprehensive error handling for all
//! transport-related operations, with proper conversions from underlying
//! libraries.

use std::{io, time::Duration};

use fleximq_protocol::errors::ProtocolError;
use thiserror::Error;
use tokio_tungstenite::tungstenite::Error as WsError;

/// Transport-related errors
#[derive(Debug, Error)]
pub enum TransportError {
    /// IO error from the underlying system
    #[error("IO error: {0}")]
    Io(String),

    /// Invalid URL or connection string
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Connection failure (could not establish connection)
    #[error("Connection failure: {0}")]
    ConnectionFailure(String),

    /// Protocol error (incorrect message format, etc.)
    #[error("Protocol error: {0}")]
    Protocol(#[from] ProtocolError),

    /// WebSocket specific error
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Connection dropped unexpectedly
    #[error("Connection dropped")]
    ConnectionDropped,

    /// Multiple errors occurred during transport operations
    #[error("Multiple errors occurred")]
    Multiple(Vec<TransportError>),

    /// Connection not found (for dynamic transport loading)
    #[error("Connection not found: {0}")]
    ConnectionNotFound(String),

    /// Operation timed out
    #[error("Operation timed out after {0:?}")]
    Timeout(Duration),

    /// Connection closed gracefully
    #[error("Connection closed")]
    ConnectionClosed,

    /// Unsupported transport type or URL scheme
    #[error("Unsupported transport: {0}")]
    UnsupportedTransport(String),

    /// Resource temporarily unavailable
    #[error("Resource unavailable: {0}")]
    ResourceUnavailable(String),

    /// Transport-specific error
    #[error("Transport error: {0}")]
    TransportError(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Access denied
    #[error("Access denied: {0}")]
    AccessDenied(String),

    /// Transport not found (for dynamic transport loading)
    #[error("Transport not found: {0}")]
    TransportNotFound(String),

    /// Connection pool error
    #[error("Connection pool error: {0}")]
    ConnectionPool(String),

    /// Message too large
    #[error("Message too large: {size} bytes (max: {max} bytes)")]
    MessageTooLarge { size: usize, max: usize },

    /// Other error (for extensibility)
    #[error("{0}")]
    Other(String),
}

/// Implement From<io::Error> for `TransportError`
impl From<io::Error> for TransportError {
    fn from(err: io::Error) -> Self {
        match err.kind() {
            io::ErrorKind::TimedOut => Self::Timeout(Duration::from_secs(30)),
            io::ErrorKind::ConnectionRefused => {
                Self::ConnectionFailure("Connection refused".to_string())
            }
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted => {
                Self::ConnectionDropped
            }
            io::ErrorKind::NotConnected => Self::ConnectionClosed,
            io::ErrorKind::WouldBlock => {
                Self::ResourceUnavailable("Operation would block".to_string())
            }
            io::ErrorKind::PermissionDenied => Self::AccessDenied("Permission denied".to_string()),
            _ => Self::Io(err.to_string()),
        }
    }
}

/// Implement From<url::ParseError> for `TransportError`
impl From<url::ParseError> for TransportError {
    fn from(err: url::ParseError) -> Self {
        Self::InvalidUrl(err.to_string())
    }
}

/// Implement From<`tokio_tungstenite::tungstenite::Error`> for `TransportError`
impl From<WsError> for TransportError {
    fn from(err: WsError) -> Self {
        match err {
            WsError::ConnectionClosed | WsError::AlreadyClosed => Self::ConnectionClosed,
            WsError::Io(io_err) => Self::from(io_err),
            WsError::Tls(tls_err) => Self::WebSocket(format!("TLS error: {tls_err}")),
            WsError::Capacity(_) => Self::MessageTooLarge {
                size: 0,           // We don't have direct access to actual size from the error
                max:  1024 * 1024, // Using 1MB as default max message size
            },
            WsError::Protocol(proto_err) => {
                Self::Protocol(ProtocolError::InvalidFormat(proto_err.to_string()))
            }
            // Handle generic resource limitation errors
            #[allow(unreachable_patterns)]
            WsError::Utf8 => Self::ResourceUnavailable("WebSocket resource error".to_string()),
            WsError::Http(e) => Self::ConnectionFailure(format!("HTTP error: {e:?}")),
            WsError::HttpFormat(e) => Self::ConnectionFailure(format!("HTTP format error: {e:?}")),
            WsError::Url(e) => Self::InvalidUrl(e.to_string()),
            _ => Self::WebSocket(err.to_string()),
        }
    }
}

/// Timeout error conversion helper
#[must_use]
pub const fn timeout_error(duration: Duration) -> TransportError {
    TransportError::Timeout(duration)
}

/// Alias for Result with `TransportError`
pub type TransportResult<T> = std::result::Result<T, TransportError>;

/// Handy extension trait for `TransportResult`
pub trait TransportResultExt<T> {
    /// Map an error to a different error message
    ///
    /// # Errors
    ///
    /// Returns an error if the original result was an error, with the context
    /// message prepended to certain error variants like `Io`,
    /// `ConnectionFailure`, and `InvalidUrl`.
    fn with_context<F, C>(self, context: F) -> TransportResult<T>
    where
        F: FnOnce() -> C,
        C: Into<String>;

    /// Attach connection info to an error message
    ///
    /// # Errors
    ///
    /// Returns an error if the original result was an error, with connection
    /// information prepended to certain error variants like `Io` and
    /// `ConnectionFailure`.
    fn with_connection_info(
        self,
        transport_type: &str,
        local: Option<&str>,
        remote: Option<&str>,
    ) -> TransportResult<T>;
}

impl<T> TransportResultExt<T> for TransportResult<T> {
    fn with_context<F, C>(self, context: F) -> Self
    where
        F: FnOnce() -> C,
        C: Into<String>,
    {
        self.map_err(|err| match err {
            TransportError::Io(msg) => TransportError::Io(format!("{}: {}", context().into(), msg)),
            TransportError::ConnectionFailure(msg) => {
                TransportError::ConnectionFailure(format!("{}: {}", context().into(), msg))
            }
            TransportError::InvalidUrl(msg) => {
                TransportError::InvalidUrl(format!("{}: {}", context().into(), msg))
            }
            _ => err,
        })
    }

    fn with_connection_info(
        self,
        transport_type: &str,
        local: Option<&str>,
        remote: Option<&str>,
    ) -> Self {
        self.map_err(|err| {
            let conn_info = match (local, remote) {
                (Some(l), Some(r)) => format!("[{transport_type} {l}->{r}]"),
                (Some(l), None) => format!("[{transport_type} {l}]"),
                (None, Some(r)) => format!("[{transport_type} ->{r}]"),
                (None, None) => format!("[{transport_type}]"),
            };

            match err {
                TransportError::Io(msg) => TransportError::Io(format!("{conn_info} {msg}")),
                TransportError::ConnectionFailure(msg) => {
                    TransportError::ConnectionFailure(format!("{conn_info} {msg}"))
                }
                TransportError::ConnectionDropped => TransportError::ConnectionDropped,
                _ => err,
            }
        })
    }
}
