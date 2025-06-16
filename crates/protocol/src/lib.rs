//! # `FlexiMQ` Protocol v1.0.0
//!
//! A high-performance, zero-copy implementation of the `FlexiMQ` protocol using
//! bytes and nom.
//!
//! ## Features
//! - Zero-copy parsing with lazy deserialization
//! - Async-friendly API with tokio integration
//! - Memory-efficient with `bytes::Bytes`
//! - Type-safe message construction
//! - `MessagePack` serialization for headers and payloads
#![allow(clippy::cast_possible_truncation)]

pub mod builder;
pub mod codec;
pub mod errors;
pub mod message;
pub mod parser;
pub mod types;

// Re-export commonly used types
pub use builder::MessageBuilder;
pub use bytes::{Bytes, BytesMut};
pub use codec::{MessageCodec, MessageDecoder, MessageEncoder};
pub use errors::{ProtocolError, ProtocolResult};
pub use message::{Message, RawMessage, TypedMessage};
pub use types::{
    Auth, ClientId, Header, HeaderField, MessageType, ProtocolVersion, RequestResponse, Routing,
    StatusCode,
};

/// Protocol constants
pub mod constants {
    /// Current protocol version
    pub const PROTOCOL_VERSION: u8 = 1;

    /// Base header size in bytes (fixed part)
    pub const BASE_HEADER_SIZE: usize = 34;

    /// Reserved field size
    pub const RESERVED_SIZE: usize = 16;

    /// Special `ClientID` values
    /// Unassigned `ClientID` value used before broker assignment.
    pub const CLIENT_ID_UNASSIGNED: u32 = 0;
    /// `ClientID` reserved for the broker.
    pub const CLIENT_ID_BROKER: u32 = 1;
    /// Minimum dynamic `ClientID` value for client allocations.
    pub const CLIENT_ID_MIN_DYNAMIC: u32 = 1000;
    /// Maximum dynamic `ClientID` value for client allocations.
    pub const CLIENT_ID_MAX_DYNAMIC: u32 = u32::MAX - 1;
    /// Reserved maximum `ClientID` value (sentinel).
    pub const CLIENT_ID_RESERVED_MAX: u32 = u32::MAX;

    /// Default limits
    /// Default maximum message size in bytes (1GB).
    pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    /// Default maximum header size in bytes (64KB).
    pub const DEFAULT_MAX_HEADER_SIZE: usize = 64 * 1024; // 64KB
}
