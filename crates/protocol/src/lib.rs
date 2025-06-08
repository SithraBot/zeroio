//! # FlexiMQ Protocol v1.0.0
//!
//! A high-performance, zero-copy implementation of the FlexiMQ protocol using
//! bytes and nom.
//!
//! ## Features
//! - Zero-copy parsing with lazy deserialization
//! - Async-friendly API with tokio integration
//! - Memory-efficient with bytes::Bytes
//! - Type-safe message construction
//! - MessagePack serialization for headers and payloads
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
    Auth, ClientId, Header, HeaderField, KeepAlive, MessageType, ProtocolVersion, RequestResponse,
    Routing, StatusCode,
};

/// Protocol constants
pub mod constants {
    /// Current protocol version
    pub const PROTOCOL_VERSION: u8 = 1;

    /// Base header size in bytes (fixed part)
    pub const BASE_HEADER_SIZE: usize = 34;

    /// Reserved field size
    pub const RESERVED_SIZE: usize = 16;

    /// Special ClientID values
    pub const CLIENT_ID_UNASSIGNED: u32 = 0;
    pub const CLIENT_ID_BROKER: u32 = 1;
    pub const CLIENT_ID_MIN_DYNAMIC: u32 = 1000;
    pub const CLIENT_ID_MAX_DYNAMIC: u32 = u32::MAX - 1;
    pub const CLIENT_ID_RESERVED_MAX: u32 = u32::MAX;

    /// Default limits
    pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    pub const DEFAULT_MAX_HEADER_SIZE: usize = 64 * 1024; // 64KB
    pub const DEFAULT_KEEPALIVE_INTERVAL: u32 = 30; // 30 seconds
}
