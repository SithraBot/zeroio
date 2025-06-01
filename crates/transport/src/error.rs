//! Transport error types

use std::io;

use thiserror::Error;
use tokio_tungstenite::tungstenite::Error as TungsteniteError;

/// Transport-specific errors
#[derive(Debug, Error)]
pub enum TransportError {
    /// IO error from underlying transport
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// Invalid URL or address format
    #[error("Invalid URL: {0}")]
    InvalidUrl(String),

    /// Connection failed
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    /// Connection closed unexpectedly
    #[error("Connection closed")]
    ConnectionClosed,

    /// Transport not supported on this platform
    #[error("Transport not supported: {0}")]
    NotSupported(String),

    /// Message too large for transport
    #[error("Message too large: {size} bytes (max: {max})")]
    MessageTooLarge { size: usize, max: usize },

    /// Timeout occurred
    #[error("Operation timed out")]
    Timeout,

    /// Protocol error
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// WebSocket specific error
    // #[cfg(feature = "websocket")] // Temporarily removed as websocket feature is not defined
    #[error("WebSocket error: {0}")]
    WebSocket(String),

    /// Other errors
    #[error("{0}")]
    Other(String),
}

/// Result type alias for transport operations
pub type TransportResult<T> = Result<T, TransportError>;

impl From<TungsteniteError> for TransportError {
    fn from(err: TungsteniteError) -> Self {
        match err {
            TungsteniteError::Io(e) => TransportError::Io(e),
            TungsteniteError::Tls(e) => TransportError::ConnectionFailed(format!("TLS error: {e}")),
            TungsteniteError::Protocol(e) => {
                TransportError::ConnectionFailed(format!("Protocol error: {e}"))
            }
            TungsteniteError::Utf8 => TransportError::ConnectionFailed("Invalid UTF-8".to_string()),
            TungsteniteError::Capacity(e) => {
                TransportError::ConnectionFailed(format!("Capacity error: {e}"))
            }
            TungsteniteError::ConnectionClosed => {
                TransportError::ConnectionFailed("Connection closed".to_string())
            }
            TungsteniteError::AlreadyClosed => {
                TransportError::ConnectionFailed("Already closed".to_string())
            }
            TungsteniteError::Http(e) => {
                TransportError::ConnectionFailed(format!("HTTP error: {e:?}"))
            }
            TungsteniteError::HttpFormat(e) => {
                TransportError::ConnectionFailed(format!("HTTP format error: {e:?}"))
            }
            TungsteniteError::Url(e) => TransportError::InvalidUrl(e.to_string()),
            _ => TransportError::ConnectionFailed(format!("WebSocket error: {err}")),
        }
    }
}
