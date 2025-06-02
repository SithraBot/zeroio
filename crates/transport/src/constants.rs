//! Constants used throughout the transport module

/// Default maximum message size (1MB)
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 1024 * 1024;

/// Default maximum header size (16KB)
pub const DEFAULT_MAX_HEADER_SIZE: usize = 16 * 1024;

/// Default connection timeout in seconds
pub const DEFAULT_CONNECTION_TIMEOUT_SECS: u64 = 30;

/// Default read timeout in seconds
pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 30;

/// Default write timeout in seconds
pub const DEFAULT_WRITE_TIMEOUT_SECS: u64 = 30;

/// Default keep-alive interval in seconds
pub const DEFAULT_KEEP_ALIVE_INTERVAL_SECS: u64 = 30;

/// Default TCP keep-alive interval in seconds
pub const DEFAULT_TCP_KEEP_ALIVE_SECS: u64 = 60;

/// Default connection cache size
pub const DEFAULT_CONNECTION_CACHE_SIZE: usize = 100;

/// Default buffer size for transport operations
pub const DEFAULT_BUFFER_SIZE: usize = 8 * 1024;

/// Default connect retry count
pub const DEFAULT_CONNECT_RETRY_COUNT: usize = 3;

/// Default connection pool size
pub const DEFAULT_CONNECTION_POOL_SIZE: usize = 10;

/// Default idle timeout for pooled connections (in seconds)
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300; // 5 minutes

/// Default worker thread count for connection management
pub const DEFAULT_WORKER_THREADS: usize = 4;

/// IPC permissions for Unix sockets (octal)
pub const DEFAULT_IPC_PERMISSIONS: u32 = 0o600;
