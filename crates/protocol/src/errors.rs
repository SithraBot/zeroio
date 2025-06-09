//! Protocol error types

use thiserror::Error;

/// Protocol operation result type
pub type ProtocolResult<T> = Result<T, ProtocolError>;

/// Protocol errors
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ProtocolError {
    /// Invalid protocol version
    #[error("Invalid protocol version: expected {expected}, got {actual}")]
    InvalidVersion { expected: u8, actual: u8 },

    /// Unknown message type
    #[error("Unknown message type: {0}")]
    UnknownMessageType(u8),

    /// Invalid message format
    #[error("Invalid message format: {0}")]
    InvalidFormat(String),

    /// Message too large
    #[error("Message size {size} exceeds maximum {max}")]
    MessageTooLarge { size: usize, max: usize },

    /// Header too large
    #[error("Header size {size} exceeds maximum {max}")]
    HeaderTooLarge { size: usize, max: usize },

    /// Invalid `ClientID`
    #[error("Invalid ClientID: {0}")]
    InvalidClientId(u32),

    /// Required header field missing
    #[error("Required header field missing: {field} for message type {message_type:?}")]
    MissingRequiredField {
        field:        String,
        message_type: String,
    },

    /// Forbidden header field present
    #[error("Forbidden header field present: {field} for message type {message_type:?}")]
    ForbiddenField {
        field:        String,
        message_type: String,
    },

    /// `MessagePack` serialization error
    #[error("MessagePack serialization error: {0}")]
    MessagePackSerialization(String),

    /// `MessagePack` deserialization error
    #[error("MessagePack deserialization error: {0}")]
    MessagePackDeserialization(String),

    /// Incomplete data
    #[error("Incomplete data: need {needed} bytes, got {available}")]
    IncompleteData { needed: usize, available: usize },

    /// Parse error
    #[error("Parse error: {0}")]
    ParseError(String),

    /// Invalid routing
    #[error("Invalid routing: {0}")]
    InvalidRouting(String),

    /// Invalid authentication
    #[error("Invalid authentication: {0}")]
    InvalidAuth(String),

    /// Buffer overflow
    #[error("Buffer overflow while writing message")]
    BufferOverflow,

    /// I/O error
    #[error("I/O error: {0}")]
    Io(String),
}

impl From<rmp_serde::encode::Error> for ProtocolError {
    fn from(err: rmp_serde::encode::Error) -> Self {
        Self::MessagePackSerialization(err.to_string())
    }
}

impl From<rmp_serde::decode::Error> for ProtocolError {
    fn from(err: rmp_serde::decode::Error) -> Self {
        Self::MessagePackDeserialization(err.to_string())
    }
}

impl From<std::io::Error> for ProtocolError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

impl From<rmpv::ext::Error> for ProtocolError {
    fn from(err: rmpv::ext::Error) -> Self {
        Self::MessagePackDeserialization(err.to_string())
    }
}
