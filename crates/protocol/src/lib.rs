// Core protocol implementation
pub mod message;

// Core implementation
pub mod core;

// Re-export core types and traits
// Re-export specific implementations
// Re-export zero-copy types for high-performance scenarios
pub use message::{
    // Buffer management types
    AlignedBuffer,
    BorrowedMessage,
    BufferError,
    BufferManager,
    BufferMetrics,
    // Optimized processing types
    HeaderField,
    MappedBuffer,
    MessageAssembler,
    MessageMetadata,
    RingBuffer,
    StreamingEncoder,
    ZeroCopyDecode,
    ZeroCopyEncode,
};
pub use message::{
    Auth,
    CLIENT_ID_BROKER,
    CLIENT_ID_MIN_ASSIGNED,
    CLIENT_ID_RESERVED,
    CLIENT_ID_UNASSIGNED,
    DEFAULT_KEEPALIVE_INTERVAL,
    DEFAULT_KEEPALIVE_TIMEOUT_MULTIPLIER,
    Header,
    HeaderAccess,
    Keepalive,
    // Traits from traits module
    MessageDecode,
    MessageDeserializeError,
    MessageEncode,
    MessageEncodeError,
    // Core types from types module
    MessageType,
    MessageValidate,
    OwnedMessage,
    PayloadAccess,
    Reqrep,
    Routing,
    StatusCode,
};
pub use message::{decode::Message, encode::MessageBuilder};

// Protocol version
pub const PROTOCOL_VERSION: u8 = 1;

// Performance and limit constants
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
pub const MAX_HEADER_SIZE: usize = 64 * 1024; // 64KB
pub const BASE_HEADER_SIZE: usize = 34;
pub const DEFAULT_BUFFER_SIZE: usize = 8192;
pub const MAX_ROUTING_ENTRIES: usize = 1000;
