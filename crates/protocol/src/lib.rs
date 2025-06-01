// Core protocol implementation
pub mod message;

// Re-export core types and traits for convenience
pub use message::{
    // Buffer management for advanced use cases
    AlignedBuffer,
    // Core message types
    Auth,
    // Zero-copy types for performance-critical scenarios
    BorrowedMessage,
    BufferManager,
    BufferMetrics,
    // Constants
    CLIENT_ID_BROKER,
    CLIENT_ID_MIN_ASSIGNED,
    CLIENT_ID_RESERVED,
    CLIENT_ID_UNASSIGNED,
    DEFAULT_KEEPALIVE_INTERVAL,
    DEFAULT_KEEPALIVE_TIMEOUT_MULTIPLIER,
    Header,
    // Core traits
    HeaderAccess,
    Keepalive,
    MessageAssembler,
    // Performance optimization types
    MessageBuilderPool,
    MessageCache,
    MessageCacheStats,
    MessageDecode,
    // Error types
    MessageDeserializeError,
    MessageEncode,
    MessageEncodeError,
    MessagePresets,
    MessageType,
    MessageValidate,
    OwnedMessage,
    PayloadAccess,
    PoolStats,
    PooledMessageBuilder,
    Reqrep,
    RingBuffer,
    Routing,
    StatusCode,
    StreamingEncoder,
    ZeroCopyDecode,
    ZeroCopyEncode,
};
// Re-export specific implementations
pub use message::{decode::Message, encode::MessageBuilder};

// Protocol version
pub const PROTOCOL_VERSION: u8 = 1;

// Performance and limit constants
pub const MAX_MESSAGE_SIZE: usize = 1024 * 1024 * 1024; // 1GB
pub const MAX_HEADER_SIZE: usize = 64 * 1024; // 64KB
pub const BASE_HEADER_SIZE: usize = 34;
pub const DEFAULT_BUFFER_SIZE: usize = 8192;
pub const MAX_ROUTING_ENTRIES: usize = 1000;
