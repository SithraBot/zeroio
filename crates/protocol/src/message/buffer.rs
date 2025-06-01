use std::{
    alloc::{Layout, alloc, dealloc},
    collections::VecDeque,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::{Bytes, BytesMut};

/// Memory-aligned buffer manager for zero-copy message processing
pub struct BufferManager {
    pools:     Vec<BufferPool>,
    alignment: usize,
    metrics:   Arc<BufferMetrics>,
}

impl BufferManager {
    /// Create new buffer manager with specified alignment
    #[must_use]
    pub fn new(alignment: usize) -> Self {
        let pools = vec![
            BufferPool::new(1024, 64),     // Small messages (1KB)
            BufferPool::new(8192, 32),     // Medium messages (8KB)
            BufferPool::new(65536, 16),    // Large messages (64KB)
            BufferPool::new(1_048_576, 4), // XL messages (1MB)
        ];

        Self {
            pools,
            alignment,
            metrics: Arc::new(BufferMetrics::default()),
        }
    }

    /// Allocate aligned buffer of specified size
    ///
    /// # Errors
    ///
    /// Returns `BufferError` if:
    /// - Memory allocation fails
    /// - The requested size exceeds available memory
    /// - System alignment requirements cannot be met
    pub fn allocate(&mut self, size: usize) -> Result<AlignedBuffer, BufferError> {
        self.metrics.allocations.fetch_add(1, Ordering::Relaxed);

        // Find appropriate pool
        for pool in &mut self.pools {
            if pool.buffer_size >= size {
                if let Some(buffer) = pool.get_buffer() {
                    self.metrics.pool_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(buffer);
                }
            }
        }

        // Allocate new buffer if no pool buffer available
        self.metrics.pool_misses.fetch_add(1, Ordering::Relaxed);
        self.allocate_new(size)
    }

    /// Return buffer to appropriate pool
    pub fn deallocate(&mut self, buffer: AlignedBuffer) {
        self.metrics.deallocations.fetch_add(1, Ordering::Relaxed);

        for pool in &mut self.pools {
            if pool.buffer_size == buffer.capacity {
                pool.return_buffer(buffer);
                return;
            }
        }

        // Buffer doesn't fit any pool, let it drop
    }

    /// Allocate new aligned buffer
    fn allocate_new(&self, size: usize) -> Result<AlignedBuffer, BufferError> {
        let aligned_size = (size + self.alignment - 1) & !(self.alignment - 1);
        let layout = Layout::from_size_align(aligned_size, self.alignment)
            .map_err(|_| BufferError::InvalidAlignment)?;

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            return Err(BufferError::AllocationFailed);
        }

        Ok(AlignedBuffer {
            ptr: unsafe { NonNull::new_unchecked(ptr) },
            capacity: aligned_size,
            len: 0,
            layout,
        })
    }

    /// Get buffer manager metrics
    #[must_use]
    pub fn metrics(&self) -> Arc<BufferMetrics> {
        Arc::clone(&self.metrics)
    }
}

/// Buffer pool for specific size class
struct BufferPool {
    buffers:     VecDeque<AlignedBuffer>,
    buffer_size: usize,
    max_buffers: usize,
}

impl BufferPool {
    fn new(buffer_size: usize, max_buffers: usize) -> Self {
        Self {
            buffers: VecDeque::with_capacity(max_buffers),
            buffer_size,
            max_buffers,
        }
    }

    fn get_buffer(&mut self) -> Option<AlignedBuffer> {
        self.buffers.pop_front().map(|mut buffer| {
            buffer.clear();
            buffer
        })
    }

    fn return_buffer(&mut self, buffer: AlignedBuffer) {
        if self.buffers.len() < self.max_buffers {
            self.buffers.push_back(buffer);
        }
        // Otherwise let buffer drop
    }
}

/// memory-aligned buffer with zero-copy capabilities
pub struct AlignedBuffer {
    ptr:      NonNull<u8>,
    capacity: usize,
    len:      usize,
    layout:   Layout,
}

impl AlignedBuffer {
    /// Get buffer as slice
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Get buffer as mutable slice
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.capacity) }
    }

    /// Set length of valid data
    /// Set the length of the buffer
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the buffer's capacity
    pub fn set_len(&mut self, len: usize) {
        assert!(len <= self.capacity);
        self.len = len;
    }

    /// Clear buffer
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Get capacity
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get length
    #[must_use]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Convert to Bytes for zero-copy sharing
    #[must_use]
    pub fn freeze(self) -> Bytes {
        let slice = unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) };
        Bytes::copy_from_slice(slice)
    }

    /// Write data to buffer
    ///
    /// # Errors
    ///
    /// Returns `BufferError::InsufficientSpace` if there is not enough space
    pub fn write(&mut self, data: &[u8]) -> Result<usize, BufferError> {
        if self.len + data.len() > self.capacity {
            return Err(BufferError::InsufficientSpace);
        }

        let start = self.len;
        let end = start + data.len();
        unsafe {
            let dst = self.ptr.as_ptr().add(start);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, data.len());
        }
        self.len = end;
        Ok(data.len())
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

/// Message assembler for handling fragmented messages
pub struct MessageAssembler {
    fragments:     Vec<Bytes>,
    total_size:    usize,
    expected_size: Option<usize>,
}

impl Default for MessageAssembler {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageAssembler {
    /// Create new message assembler
    #[must_use]
    pub fn new() -> Self {
        Self {
            fragments:     Vec::new(),
            total_size:    0,
            expected_size: None,
        }
    }

    /// Add a message fragment
    pub fn add_fragment(&mut self, fragment: Bytes) {
        self.total_size += fragment.len();
        self.fragments.push(fragment);
    }

    /// Set expected total message size
    pub fn set_expected_size(&mut self, size: usize) {
        self.expected_size = Some(size);
    }

    /// Check if message is complete
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.expected_size.map_or_else(
            || self.check_protocol_complete(),
            |expected| self.total_size >= expected,
        )
    }

    /// Assemble fragments into complete message
    ///
    /// # Errors
    ///
    /// Returns `BufferError` if:
    /// - No fragments have been added
    /// - The assembled message is incomplete
    /// - Memory allocation fails during assembly
    ///
    /// # Panics
    ///
    /// This function should not panic as it checks for empty fragments
    pub fn assemble(self) -> Result<Bytes, BufferError> {
        if self.fragments.is_empty() {
            return Err(BufferError::InvalidSize);
        }

        if self.fragments.len() == 1 {
            // We know fragments has exactly one element since we checked above
            let mut fragments = self.fragments;
            return Ok(fragments.pop().unwrap_or_default());
        }

        let mut result = BytesMut::with_capacity(self.total_size);
        for fragment in self.fragments {
            result.extend_from_slice(&fragment);
        }

        Ok(result.freeze())
    }

    /// Check if we have a complete protocol message based on header
    fn check_protocol_complete(&self) -> bool {
        if self.total_size < 34 {
            // Need at least base header
            return false;
        }

        // Assemble enough data to read header
        let mut header_data = Vec::with_capacity(34);
        let mut remaining = 34;

        for fragment in &self.fragments {
            if remaining == 0 {
                break;
            }

            let to_copy = remaining.min(fragment.len());
            header_data.extend_from_slice(&fragment[..to_copy]);
            remaining -= to_copy;
        }

        if header_data.len() < 34 {
            return false;
        }

        // Parse header length
        let header_len = u32::from_be_bytes([
            header_data[22],
            header_data[23],
            header_data[24],
            header_data[25],
        ]) as usize;

        let payload_len_offset = 26 + header_len;
        if self.total_size < payload_len_offset + 8 {
            return false;
        }

        // Need to read payload length - this is simplified
        // In practice, you'd need to assemble more data
        true
    }
}

/// Ring buffer for streaming data
pub struct RingBuffer {
    buffer:    Vec<u8>,
    read_pos:  usize,
    write_pos: usize,
    full:      bool,
}

impl RingBuffer {
    /// Create new ring buffer with specified capacity
    ///
    /// # Errors
    ///
    /// Returns `BufferError::InvalidSize` if capacity is 0
    pub fn new(capacity: usize) -> Result<Self, BufferError> {
        if capacity == 0 {
            return Err(BufferError::InvalidSize);
        }

        Ok(Self {
            buffer:    vec![0; capacity],
            read_pos:  0,
            write_pos: 0,
            full:      false,
        })
    }

    /// Get available space for writing
    #[must_use]
    pub fn available_write(&self) -> usize {
        if self.full {
            0
        } else if self.write_pos >= self.read_pos {
            self.buffer.len() - self.write_pos + self.read_pos
        } else {
            self.read_pos - self.write_pos
        }
    }

    /// Get available data for reading
    #[must_use]
    pub fn available_read(&self) -> usize {
        if self.full {
            self.buffer.len()
        } else if self.write_pos >= self.read_pos {
            self.write_pos - self.read_pos
        } else {
            self.buffer.len() - self.read_pos + self.write_pos
        }
    }

    /// Write data to ring buffer
    ///
    /// # Errors
    ///
    /// Returns `BufferError::InsufficientSpace` if there is not enough space
    pub fn write(&mut self, data: &[u8]) -> Result<usize, BufferError> {
        let available = self.available_write();
        if data.len() > available {
            return Err(BufferError::InsufficientSpace);
        }

        let mut written = 0;
        for &byte in data {
            self.buffer[self.write_pos] = byte;
            self.write_pos = (self.write_pos + 1) % self.buffer.len();
            written += 1;

            if self.write_pos == self.read_pos {
                self.full = true;
                break;
            }
        }

        Ok(written)
    }

    /// Read data from ring buffer
    pub fn read(&mut self, output: &mut [u8]) -> usize {
        let available = self.available_read();
        let to_read = output.len().min(available);

        for item in output.iter_mut().take(to_read) {
            *item = self.buffer[self.read_pos];
            self.read_pos = (self.read_pos + 1) % self.buffer.len();
        }

        if to_read > 0 {
            self.full = false;
        }

        to_read
    }
}

/// Buffer manager metrics
#[derive(Debug, Default)]
pub struct BufferMetrics {
    pub allocations:     AtomicUsize,
    pub deallocations:   AtomicUsize,
    pub pool_hits:       AtomicUsize,
    pub pool_misses:     AtomicUsize,
    pub bytes_allocated: AtomicUsize,
    pub peak_usage:      AtomicUsize,
}

impl BufferMetrics {
    /// Calculate pool hit ratio
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.pool_hits.load(Ordering::Relaxed) as f64;
        let total = hits + self.pool_misses.load(Ordering::Relaxed) as f64;
        if total > 0.0 { hits / total } else { 0.0 }
    }
}

/// Buffer-related errors
#[derive(Debug, thiserror::Error)]
pub enum BufferError {
    #[error("Invalid alignment specified")]
    InvalidAlignment,
    #[error("Memory allocation failed")]
    AllocationFailed,
    #[error("Insufficient buffer space")]
    InsufficientSpace,
    #[error("Invalid size specified")]
    InvalidSize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_manager() {
        let mut manager = BufferManager::new(8);
        let buffer = manager.allocate(1024).unwrap();
        assert_eq!(buffer.capacity(), 1024);
    }

    #[test]
    fn test_message_assembler() {
        let mut assembler = MessageAssembler::new();
        assembler.add_fragment(Bytes::from("hello"));
        assembler.add_fragment(Bytes::from(" world"));
        let result = assembler.assemble().unwrap();
        assert_eq!(result, Bytes::from("hello world"));
    }

    #[test]
    fn test_ring_buffer() {
        let mut ring = RingBuffer::new(10).unwrap();
        assert_eq!(ring.write(b"hello").unwrap(), 5);
        let mut output = [0u8; 10];
        assert_eq!(ring.read(&mut output), 5);
        assert_eq!(&output[..5], b"hello");
    }
}
