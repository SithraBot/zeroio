//! Transport manager for managing multiple transport types with caching

use std::sync::{Arc, Mutex};

use dashmap::DashMap;
use lru::LruCache;

use crate::{
    IpcTransport, StdioTransport, TcpTransport, WebSocketTransport,
    error::{TransportError, TransportResult},
    traits::{Transport, TransportStream},
};

/// Manages various transport implementations (TCP, IPC, WebSocket, STDIO)
/// and provides a unified interface for connecting.
///
/// It allows registration of different transport types and selects the
/// appropriate one based on the URL scheme for connection requests.
/// Connection caching is planned but currently not fully implemented in the
/// `connect` path.
pub struct TransportManager {
    /// Registered transports, keyed by their transport type string (e.g.,
    /// "tcp", "ipc").
    transports:
        DashMap<String, Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>>,
    /// Connection cache for frequently used connections.
    /// Note: This cache is not actively used by the `connect` method in the
    /// current implementation. See `connect` method's internal TODO for
    /// more details.
    _connection_cache: Arc<Mutex<LruCache<String, Arc<dyn TransportStream>>>>,
    /// Configuration for the transport manager, like cache settings.
    _config:           TransportManagerConfig,
}

#[derive(Debug, Clone)]
pub struct TransportManagerConfig {
    /// Maximum number of cached connections
    pub max_cached_connections: usize,
    /// Enable connection pooling
    pub enable_pooling:         bool,
}

impl Default for TransportManagerConfig {
    fn default() -> Self {
        Self {
            max_cached_connections: 100,
            enable_pooling:         true,
        }
    }
}

// Internal wrapper to store different `Transport` trait objects with their
// concrete stream types, allowing them to be stored in the `TransportManager`'s
// `transports` map which expects `Arc<dyn Transport<Stream = Box<dyn
// TransportStream>>>`.
struct TypeErasedTransport<T: Transport> {
    inner: T,
}

impl<T> Transport for TypeErasedTransport<T>
where
    T: Transport + Send + Sync + 'static,
    T::Stream: TransportStream + 'static,
{
    type Stream = Box<dyn TransportStream>;

    fn connect<'life0, 'life1, 'async_trait>(
        &'life0 self,
        url: &'life1 str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = TransportResult<Self::Stream>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            let stream = self.inner.connect(url).await?;
            Ok(Box::new(stream) as Box<dyn TransportStream>)
        })
    }

    fn listen<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _url: &'life1 str,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = TransportResult<
                        Box<dyn crate::traits::TransportListener<Stream = Self::Stream>>,
                    >,
                > + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async move {
            // For now, type-erased listeners are not supported by the manager framework.
            // Each transport needs to handle its own listener logic directly if it supports
            // listening.
            Err(TransportError::NotSupported(
                "Type-erased transport listeners are not supported via TransportManager."
                    .to_string(),
            ))
        })
    }

    fn transport_type(&self) -> &str {
        self.inner.transport_type()
    }

    fn supports_url(&self, url: &str) -> bool {
        self.inner.supports_url(url)
    }
}

impl TransportManager {
    /// Creates a new `TransportManager` with default configuration and
    /// registers all built-in transport types.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(TransportManagerConfig::default())
    }

    /// Creates a new `TransportManager` with the specified configuration and
    /// registers all built-in transport types.
    ///
    /// # Panics
    ///
    /// Panics if `config.max_cached_connections` is 0, as `LruCache` requires a
    /// non-zero capacity for its internal cache.
    #[must_use]
    pub fn with_config(config: TransportManagerConfig) -> Self {
        let mut manager = Self {
            transports:        DashMap::new(),
            _connection_cache: Arc::new(Mutex::new(LruCache::new(
                #[allow(clippy::expect_used)]
                std::num::NonZeroUsize::new(config.max_cached_connections)
                    .expect("max_cached_connections must be non-zero for LruCache"),
            ))),
            _config:           config,
        };

        // Register default transports
        manager.register_default_transports();
        manager
    }

    // Registers instances of the default built-in transports (TCP, IPC, STDIO,
    // WebSocket).
    fn register_default_transports(&mut self) {
        // TCP transport
        self.register_transport(Arc::new(TypeErasedTransport {
            inner: TcpTransport::new(),
        }));

        // IPC transport
        self.register_transport(Arc::new(TypeErasedTransport {
            inner: IpcTransport::new(),
        }));

        // STDIO transport
        self.register_transport(Arc::new(TypeErasedTransport {
            inner: StdioTransport::new(),
        }));

        // WebSocket transport
        // Note: WebSocket server functionality via listener might be limited or not
        // fully supported by all underlying WebSocket transport implementations
        // when accessed via manager.
        self.register_transport(Arc::new(TypeErasedTransport {
            inner: WebSocketTransport::new(),
        }));
    }

    /// Registers a custom transport implementation with the manager.
    /// The transport will be available for connection requests that match its
    /// supported URL schemes.
    pub fn register_transport(
        &self,
        transport: Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>,
    ) {
        let transport_type = transport.transport_type().to_string();
        self.transports.insert(transport_type, transport);
    }

    /// Connect to a URL using the appropriate transport registered with the
    /// manager.
    ///
    /// The manager identifies the correct transport based on the URL scheme
    /// (e.g., "tcp://", "ipc://").
    ///
    /// Note on Caching: Connection caching/pooling is intended for future
    /// enhancement but is currently bypassed in this method. Each call to
    /// `connect` establishes a new connection. Refer to internal TODOs for
    /// planned improvements.
    ///
    /// # Errors
    ///
    /// - `TransportError::NotSupported`: If no transport is registered that
    ///   supports the URL's scheme.
    /// - Transport-specific errors: Any error that occurs during the connection
    ///   attempt by the underlying transport (e.g., `ConnectionFailed`,
    ///   `InvalidUrl` from the specific transport).
    pub async fn connect(&self, url: &str) -> TransportResult<Box<dyn TransportStream>> {
        // TODO: Implement proper connection pooling and caching.
        // The current `_connection_cache` is not utilized by this `connect` method
        // directly. This would require handling potential `&mut self` needs for
        // cached streams or ensuring streams are `Clone` and it's safe to hand
        // out clones from a cache, or using interior mutability patterns.

        // Find the appropriate transport based on the URL scheme.
        let transport_to_use = self.find_transport_for_url(url).ok_or_else(|| {
            // Try to extract the scheme from the URL for a more informative error message.
            let scheme = url.split_once("://").map_or("unknown", |(s, _)| s);
            TransportError::NotSupported(format!(
                "No transport registered for URL scheme: '{scheme}' in URL '{url}'"
            ))
        })?;

        // Delegate the connection attempt to the selected transport.
        transport_to_use.connect(url).await
    }

    // Finds a registered transport that supports the scheme of the given URL.
    fn find_transport_for_url(
        &self,
        url: &str,
    ) -> Option<Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>> {
        for entry in &self.transports {
            if entry.value().supports_url(url) {
                return Some(Arc::clone(entry.value()));
            }
        }
        None
    }

    /// Retrieves a cached connection for the given URL, if one exists and is
    /// valid. Note: This method is currently not actively used by the
    /// public `connect` API.
    fn _get_cached_connection(&self, url: &str) -> Option<Arc<dyn TransportStream>> {
        #[allow(clippy::expect_used)]
        // expect is used for Mutex lock poisoning which is a panic condition.
        let mut cache = self
            ._connection_cache
            .lock()
            .expect("Connection cache lock poisoned during get");
        cache.get(url).cloned()
    }

    /// Adds a connection to the cache.
    /// Note: This method is currently not actively used by the public `connect`
    /// API.
    fn _cache_connection(&self, url: &str, stream: Arc<dyn TransportStream>) {
        #[allow(clippy::expect_used)]
        // expect is used for Mutex lock poisoning which is a panic condition.
        let mut cache = self
            ._connection_cache
            .lock()
            .expect("Connection cache lock poisoned during cache");
        cache.put(url.to_string(), stream);
    }

    /// Clears all connections from the transport manager's cache.
    ///
    /// # Panics
    ///
    /// Panics if the connection cache lock is poisoned (i.e., another thread
    /// panicked while holding the lock).
    pub fn clear_cache(&self) {
        #[allow(clippy::expect_used)] // Used for Mutex lock poisoning.
        let mut cache = self
            ._connection_cache
            .lock()
            .expect("Connection cache lock poisoned during clear");
        cache.clear();
    }

    /// Retrieves statistics about the current state of the connection cache.
    ///
    /// # Panics
    ///
    /// Panics if the connection cache lock is poisoned.
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        #[allow(clippy::expect_used)] // Used for Mutex lock poisoning.
        let cache = self
            ._connection_cache
            .lock()
            .expect("Connection cache lock poisoned during stats");
        CacheStats {
            size:     cache.len(),
            capacity: cache.cap().get(),
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of cached connections
    pub size:     usize,
    /// Maximum capacity
    pub capacity: usize,
}

impl Default for TransportManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_transport_manager_creation() {
        let manager = TransportManager::new();

        // Check that default transports are registered
        assert!(manager.find_transport_for_url("tcp://localhost:8080").is_some());
        assert!(manager.find_transport_for_url("ipc:///tmp/test.sock").is_some());
        assert!(manager.find_transport_for_url("stdio://").is_some());
        assert!(manager.find_transport_for_url("ws://localhost:8080").is_some());
    }

    #[tokio::test]
    async fn test_invalid_url() {
        let manager = TransportManager::new();

        let result = manager.connect("invalid://url").await;
        assert!(result.is_err());
    }
}
