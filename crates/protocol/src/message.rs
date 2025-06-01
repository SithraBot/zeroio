pub mod buffer;
pub mod decode;
pub mod encode;
pub mod perf;
pub mod traits;
pub mod types;
pub mod zerocopy;

// Re-export core types for convenience
pub use buffer::{
    AlignedBuffer, BufferError, BufferManager, BufferMetrics, MessageAssembler, RingBuffer,
};
// Re-export performance optimization types
pub use encode::{MessageBuilderPool, PoolStats, PooledMessageBuilder};
// Re-export performance utilities
pub use perf::{CacheStats as MessageCacheStats, MessageCache, MessagePresets};
// Re-export optimized processing types
pub use traits::{
    HeaderAccess, Message, MessageDecode, MessageEncode, MessageValidate, OwnedMessage,
    PayloadAccess,
};
pub use types::{
    Auth, CLIENT_ID_BROKER, CLIENT_ID_MIN_ASSIGNED, CLIENT_ID_RESERVED, CLIENT_ID_UNASSIGNED,
    DEFAULT_KEEPALIVE_INTERVAL, DEFAULT_KEEPALIVE_TIMEOUT_MULTIPLIER, Header, Keepalive,
    MessageDeserializeError, MessageEncodeError, MessageType, Reqrep, Routing, StatusCode,
};
// Re-export zero-copy types for high-performance scenarios
pub use zerocopy::{BorrowedMessage, StreamingEncoder, ZeroCopyDecode, ZeroCopyEncode};
