//! Transport manager for managing multiple transport types with connection
//! caching

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use dashmap::DashMap;
use lru::LruCache;
use triomphe::Arc;

use crate::{
    ChannelTransport, IpcTransport, StdioTransport, TcpTransport, WebSocketTransport,
    error::{TransportError, TransportResult},
    traits::{Transport, TransportStream},
};

/// Connection cache entry with TTL
#[derive(Clone)]
struct CacheEntry {
    last_used:        Instant,
    connection_count: usize,
}

/// Manages various transport implementations with connection tracking for
/// performance
pub struct TransportManager {
    /// Registered transports, keyed by their transport type string
    transports:
        DashMap<String, Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>>,
    /// Connection usage tracking for optimization
    connection_stats: Arc<Mutex<LruCache<String, CacheEntry>>>,
    /// Cache TTL for connection reuse decisions
    cache_ttl:        Duration,
}

// Internal wrapper to store different Transport trait objects
struct TypeErasedTransport<T: Transport> {
    inner: T,
}

#[async_trait]
impl<T> Transport for TypeErasedTransport<T>
where
    T: Transport + Send + Sync + 'static,
    T::Stream: TransportStream + 'static,
{
    type Stream = Box<dyn TransportStream>;

    async fn connect(&self, url: &str) -> TransportResult<Self::Stream> {
        let stream = self.inner.connect(url).await?;
        Ok(Box::new(stream) as Box<dyn TransportStream>)
    }

    async fn listen(
        &self,
        _url: &str,
    ) -> TransportResult<Box<dyn crate::traits::TransportListener<Stream = Self::Stream>>> {
        Err(TransportError::UnsupportedTransport(
            "Type-erased transport listeners are not supported via TransportManager.".to_string(),
        ))
    }

    fn transport_type(&self) -> &str {
        self.inner.transport_type()
    }

    fn supports_url(&self, url: &str) -> bool {
        self.inner.supports_url(url)
    }
}

impl TransportManager {
    /// Creates a new `TransportManager` with connection tracking enabled
    #[must_use]
    pub fn new() -> Self {
        Self::with_cache_config(100, Duration::from_secs(300))
    }

    /// Creates a new `TransportManager` with specified cache configuration
    ///
    /// # Panics
    ///
    /// Will panic if `cache_size` is 0, but this is handled by using a safe
    /// fallback value
    #[must_use]
    pub fn with_cache_config(cache_size: usize, ttl: Duration) -> Self {
        let manager = Self {
            transports:       DashMap::new(),
            connection_stats: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(cache_size.max(1)).unwrap_or_else(|| {
                    #[allow(clippy::expect_used)]
                    std::num::NonZeroUsize::new(100).expect("100 is always non-zero")
                }),
            ))),
            cache_ttl:        ttl,
        };

        // Register default transports
        manager.register_default_transports();
        manager
    }

    fn new_transport<T>(
        transport: T,
    ) -> Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>
    where
        T: Transport + 'static,
    {
        use unsize::{CoerceUnsize, Coercion};
        let coercion = unsafe {
            fn _f<T>(
                x: *const TypeErasedTransport<T>,
            ) -> *const (dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync)
            where
                T: Transport + 'static,
            {
                x
            }
            Coercion::new(_f)
        };
        Arc::new(TypeErasedTransport { inner: transport }).unsize(coercion)
    }

    /// Register instances of the default built-in transports
    fn register_default_transports(&self) {
        // TCP transport
        self.register_transport(Self::new_transport(TcpTransport::new()));

        // IPC transport
        self.register_transport(Self::new_transport(IpcTransport::new()));

        // STDIO transport
        self.register_transport(Self::new_transport(StdioTransport::new()));

        // WebSocket transport
        self.register_transport(Self::new_transport(WebSocketTransport::new()));

        // In-process channel transport (for tests)
        self.register_transport(Self::new_transport(ChannelTransport::new()));
    }

    /// Register a custom transport implementation
    pub fn register_transport(
        &self,
        transport: Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>,
    ) {
        let transport_type = transport.transport_type().to_string();
        self.transports.insert(transport_type, transport);
    }

    /// Connect to a URL using the appropriate transport with usage tracking
    ///
    /// # Errors
    ///
    /// Returns `TransportError` if:
    /// - No transport supports the given URL
    /// - The connection fails
    pub async fn connect(&self, url: &str) -> TransportResult<impl TransportStream + 'static> {
        // Update connection statistics
        self.track_connection_attempt(url);

        // Create new connection
        let transport = self.find_transport_for_url(url).ok_or_else(|| {
            TransportError::UnsupportedTransport(format!("No transport for URL: {url}"))
        })?;

        let stream = transport.connect(url).await?;

        // Track successful connection
        self.track_successful_connection(url);

        Ok(stream)
    }

    /// Find the appropriate transport for a given URL
    #[must_use]
    pub fn find_transport_for_url(
        &self,
        url: &str,
    ) -> Option<Arc<dyn Transport<Stream = Box<dyn TransportStream>> + Send + Sync>> {
        for transport in &self.transports {
            if transport.value().supports_url(url) {
                return Some(Arc::clone(transport.value()));
            }
        }
        None
    }

    /// Track connection attempt for statistics
    fn track_connection_attempt(&self, url: &str) {
        if let Ok(mut stats) = self.connection_stats.lock() {
            let entry = stats.get_mut(url);
            if let Some(entry) = entry {
                entry.last_used = Instant::now();
            } else {
                stats.put(
                    url.to_string(),
                    CacheEntry {
                        last_used:        Instant::now(),
                        connection_count: 0,
                    },
                );
            }
        }
    }

    /// Track successful connection for statistics
    fn track_successful_connection(&self, url: &str) {
        if let Ok(mut stats) = self.connection_stats.lock() {
            if let Some(entry) = stats.get_mut(url) {
                entry.connection_count += 1;
                entry.last_used = Instant::now();
            }
        }
    }

    /// Get connection frequency for optimization hints
    #[must_use]
    pub fn get_connection_frequency(&self, url: &str) -> Option<usize> {
        self.connection_stats.lock().map_or(None, |stats| {
            stats.peek(url).map(|entry| entry.connection_count)
        })
    }

    /// Check if a URL is frequently accessed (good candidate for connection
    /// pooling)
    #[must_use]
    pub fn is_frequently_accessed(&self, url: &str) -> bool {
        self.get_connection_frequency(url).is_some_and(|count| count > 5)
    }

    /// Clear connection statistics
    pub fn clear_cache(&self) {
        if let Ok(mut stats) = self.connection_stats.lock() {
            stats.clear();
        }
    }

    /// Get cache statistics
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        self.connection_stats.lock().map_or(
            CacheStats {
                size:           0,
                capacity:       0,
                active_entries: 0,
            },
            |stats| {
                let now = Instant::now();
                let active_entries = stats
                    .iter()
                    .filter(|(_, entry)| now.duration_since(entry.last_used) < self.cache_ttl)
                    .count();

                CacheStats {
                    size: stats.len(),
                    capacity: stats.cap().get(),
                    active_entries,
                }
            },
        )
    }

    /// Clean up expired cache entries
    pub fn cleanup_expired_entries(&self) {
        if let Ok(mut stats) = self.connection_stats.lock() {
            let now = Instant::now();
            let expired_keys: Vec<String> = stats
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.last_used) >= self.cache_ttl)
                .map(|(key, _)| key.clone())
                .collect();

            for key in expired_keys {
                stats.pop(&key);
            }
        }
    }
}

/// Cache statistics with additional metrics
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of cached entries
    pub size:           usize,
    /// Maximum capacity
    pub capacity:       usize,
    /// Number of active (non-expired) entries
    pub active_entries: usize,
}

impl CacheStats {
    /// Calculate cache utilization ratio (0.0 to 1.0)
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn utilization(&self) -> f64 {
        if self.capacity > 0 {
            self.size as f64 / self.capacity as f64
        } else {
            0.0
        }
    }

    /// Calculate active entry ratio (0.0 to 1.0)
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn active_ratio(&self) -> f64 {
        if self.size > 0 {
            self.active_entries as f64 / self.size as f64
        } else {
            0.0
        }
    }
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
        assert!(!manager.transports.is_empty());

        let stats = manager.cache_stats();
        assert_eq!(stats.size, 0);
        assert_eq!(stats.capacity, 100);
    }

    #[tokio::test]
    async fn test_custom_cache_config() {
        let manager = TransportManager::with_cache_config(50, Duration::from_secs(600));
        let stats = manager.cache_stats();
        assert_eq!(stats.capacity, 50);
    }

    #[tokio::test]
    async fn test_invalid_url() {
        let manager = TransportManager::new();
        let result = manager.connect("invalid://test").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_connection_tracking() {
        let manager = TransportManager::new();
        let url = "tcp://example.com:8080";

        // Track some connection attempts (will fail, but that's OK for this test)
        manager.track_connection_attempt(url);
        manager.track_successful_connection(url);

        let frequency = manager.get_connection_frequency(url);
        assert_eq!(frequency, Some(1));

        let stats = manager.cache_stats();
        assert_eq!(stats.size, 1);
        #[allow(clippy::float_cmp)]
        let value = stats.utilization() == 0.01f64; // 1/100
        assert!(value);
    }

    #[tokio::test]
    async fn test_cache_cleanup() {
        let manager = TransportManager::new();

        manager.track_connection_attempt("tcp://test.com:80");
        assert_eq!(manager.cache_stats().size, 1);

        manager.clear_cache();
        assert_eq!(manager.cache_stats().size, 0);
    }
}
