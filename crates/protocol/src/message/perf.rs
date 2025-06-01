//! Performance optimization utilities for message processing

use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use bytes::Bytes;
use lru::LruCache;

use crate::message::{
    encode::MessageBuilder,
    types::{MessageEncodeError, StatusCode},
};

/// Cache for pre-built common messages to avoid repeated serialization
pub struct MessageCache {
    cache: Arc<Mutex<LruCache<String, CacheEntry>>>,
    ttl:   Duration,
    stats: Arc<Mutex<CacheStats>>,
}

/// Cache entry with timestamp for TTL
#[derive(Clone)]
struct CacheEntry {
    data:         Bytes,
    created:      Instant,
    access_count: usize,
}

/// Cache statistics for monitoring performance
#[derive(Debug, Default, Clone)]
pub struct CacheStats {
    pub hits:       usize,
    pub misses:     usize,
    pub evictions:  usize,
    pub total_size: usize,
}

impl CacheStats {
    /// Calculate hit ratio (0.0 to 1.0)
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total > 0 {
            self.hits as f64 / total as f64
        } else {
            0.0
        }
    }
}

impl MessageCache {
    /// Create a new message cache
    ///
    /// # Panics
    ///
    /// Will panic if capacity is 0, but this is handled by using a safe
    /// fallback value
    #[must_use]
    pub fn new(capacity: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(capacity.max(1)).unwrap_or_else(|| {
                    #[allow(clippy::expect_used)]
                    std::num::NonZeroUsize::new(100).expect("100 is always non-zero")
                }),
            ))),
            ttl,
            stats: Arc::new(Mutex::new(CacheStats::default())),
        }
    }

    /// Get or create a cached message
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if message building fails
    pub fn get_or_create<F>(&self, key: &str, builder_fn: F) -> Result<Bytes, MessageEncodeError>
    where
        F: FnOnce() -> Result<Bytes, MessageEncodeError>,
    {
        // Check cache first
        if let Ok(mut cache) = self.cache.lock() {
            if let Some(entry) = cache.get_mut(key) {
                // Check TTL
                if entry.created.elapsed() < self.ttl {
                    entry.access_count += 1;
                    if let Ok(mut stats) = self.stats.lock() {
                        stats.hits += 1;
                    }
                    return Ok(entry.data.clone());
                }
                // Expired, remove from cache
                cache.pop(key);
                if let Ok(mut stats) = self.stats.lock() {
                    stats.evictions += 1;
                }
            }
        }

        // Cache miss, create new message
        let data = builder_fn()?;
        let entry = CacheEntry {
            data:         data.clone(),
            created:      Instant::now(),
            access_count: 1,
        };

        // Store in cache
        if let Ok(mut cache) = self.cache.lock() {
            cache.put(key.to_string(), entry);
        }

        if let Ok(mut stats) = self.stats.lock() {
            stats.misses += 1;
            stats.total_size = stats.total_size.wrapping_add(data.len());
        }

        Ok(data)
    }

    /// Get cache statistics
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        self.stats.lock().map_or_else(|_| CacheStats::default(), |stats| stats.clone())
    }

    /// Clear expired entries manually
    pub fn cleanup_expired(&self) {
        if let Ok(mut cache) = self.cache.lock() {
            let now = Instant::now();
            let expired_keys: Vec<String> = cache
                .iter()
                .filter(|(_, entry)| now.duration_since(entry.created) >= self.ttl)
                .map(|(key, _)| key.clone())
                .collect();

            for key in expired_keys {
                cache.pop(&key);
                if let Ok(mut stats) = self.stats.lock() {
                    stats.evictions += 1;
                }
            }
        }
    }
}

impl Default for MessageCache {
    fn default() -> Self {
        Self::new(200, Duration::from_secs(300)) // 5 minutes TTL
    }
}

/// Pre-built message templates for common operations
pub struct MessagePresets {
    cache: MessageCache,
}

impl MessagePresets {
    /// Create new message presets with caching
    #[must_use]
    pub fn new() -> Self {
        Self {
            cache: MessageCache::default(),
        }
    }

    /// Get a cached PONG response
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if message building fails
    pub fn pong_response(
        &self,
        client_id: u32,
        timestamp: u64,
    ) -> Result<Bytes, MessageEncodeError> {
        let key = format!("pong_{client_id}");
        self.cache.get_or_create(&key, || {
            MessageBuilder::pong(client_id, timestamp).build_vec().map(Bytes::from)
        })
    }

    /// Get a cached error response
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if message building fails
    pub fn error_response(
        &self,
        client_id: u32,
        target_client_id: u32,
        request_id: &str,
        status: StatusCode,
    ) -> Result<Bytes, MessageEncodeError> {
        let key = format!("error_{}_{}", status as u16, client_id);
        self.cache.get_or_create(&key, || {
            MessageBuilder::simple_response(
                client_id,
                target_client_id,
                "/error".to_string(),
                request_id.to_string(),
                status,
            )
            .build_vec()
            .map(Bytes::from)
        })
    }

    /// Get a cached join message
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if message building fails
    pub fn join_message(&self) -> Result<Bytes, MessageEncodeError> {
        self.cache.get_or_create("join_basic", || {
            MessageBuilder::simple_join().build_vec().map(Bytes::from)
        })
    }

    /// Get a cached subscribe message
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if message building fails
    pub fn subscribe_message(
        &self,
        client_id: u32,
        topic: &str,
    ) -> Result<Bytes, MessageEncodeError> {
        let key = format!("subscribe_{client_id}_{topic}");
        self.cache.get_or_create(&key, || {
            MessageBuilder::simple_subscribe(client_id, topic.to_string())
                .build_vec()
                .map(Bytes::from)
        })
    }

    /// Get cache statistics
    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        self.cache.stats()
    }

    /// Cleanup expired cache entries
    pub fn cleanup(&self) {
        self.cache.cleanup_expired();
    }
}

impl Default for MessagePresets {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_cache() {
        let cache = MessageCache::new(10, Duration::from_secs(1));

        // Test cache miss and creation
        let result1 = cache.get_or_create("test_key", || Ok(Bytes::from("test_data"))).unwrap();
        assert_eq!(result1, Bytes::from("test_data"));

        // Test cache hit
        let result2 =
            cache.get_or_create("test_key", || Ok(Bytes::from("different_data"))).unwrap();
        assert_eq!(result2, Bytes::from("test_data")); // Should return cached data

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
    }

    #[test]
    fn test_message_presets() {
        let presets = MessagePresets::new();

        // Test pong response
        let pong = presets.pong_response(1234, 1_000_000).unwrap();
        assert!(!pong.is_empty());

        // Test join message
        let join = presets.join_message().unwrap();
        assert!(!join.is_empty());

        // Test cache hit on second call
        let pong2 = presets.pong_response(1234, 1_000_000).unwrap();
        assert_eq!(pong, pong2);
    }
}
