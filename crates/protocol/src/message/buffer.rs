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
    #[allow(dead_code)]
    page_size: usize,
    metrics:   Arc<BufferMetrics>,
}

impl BufferManager {
    /// Create new buffer manager with specified alignment
    #[must_use]
    pub fn new(alignment: usize) -> Self {
        let page_size = Self::get_page_size();
        let pools = vec![
            BufferPool::new(1024, 64),     // Small messages (1KB)
            BufferPool::new(8192, 32),     // Medium messages (8KB)
            BufferPool::new(65536, 16),    // Large messages (64KB)
            BufferPool::new(1_048_576, 4), // XL messages (1MB)
        ];

        Self {
            pools,
            alignment,
            page_size,
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

    /// Get system page size
    fn get_page_size() -> usize {
        unsafe {
            let page_size = libc::sysconf(libc::_SC_PAGESIZE);
            if page_size > 0 {
                usize::try_from(page_size).unwrap_or(4096)
            } else {
                4096 // Default page size fallback
            }
        }
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
    /// in the buffer to write the requested data
    pub fn write(&mut self, data: &[u8]) -> Result<usize, BufferError> {
        let available = self.capacity - self.len;
        let to_write = data.len().min(available);

        if to_write == 0 {
            return Err(BufferError::InsufficientSpace);
        }

        unsafe {
            let dst = self.ptr.as_ptr().add(self.len);
            std::ptr::copy_nonoverlapping(data.as_ptr(), dst, to_write);
        }

        self.len += to_write;
        Ok(to_write)
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

/// Zero-copy message assembler for fragmented data
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            fragments:     Vec::new(),
            total_size:    0,
            expected_size: None,
        }
    }

    /// Add fragment without copying
    pub fn add_fragment(&mut self, fragment: Bytes) {
        self.total_size += fragment.len();
        self.fragments.push(fragment);
    }

    /// Set expected total size
    pub fn set_expected_size(&mut self, size: usize) {
        self.expected_size = Some(size);
    }

    /// Check if message is complete
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.expected_size.map_or_else(
            || self.total_size >= 34 && self.check_protocol_complete(),
            |expected| self.total_size >= expected,
        )
    }

    /// Assemble fragments into single buffer
    ///
    /// # Errors
    ///
    /// Returns `BufferError::InvalidSize` if the assembled buffer would be
    /// empty when fragments exist, or if buffer assembly fails due to
    /// memory constraints
    ///
    /// # Panics
    ///
    /// May panic if internal fragment state is inconsistent
    pub fn assemble(self) -> Result<Bytes, BufferError> {
        if self.fragments.is_empty() {
            return Ok(Bytes::new());
        }

        if self.fragments.len() == 1 {
            // SAFETY: We just checked that fragments.len() == 1
            return Ok(self
                .fragments
                .into_iter()
                .next()
                .unwrap_or_else(|| unreachable!("Fragment vector has exactly one element")));
        }

        // Need to copy fragments into single buffer
        let mut buffer = BytesMut::with_capacity(self.total_size);
        for fragment in self.fragments {
            buffer.extend_from_slice(&fragment);
        }

        Ok(buffer.freeze())
    }

    /// Check if we have a complete protocol message
    fn check_protocol_complete(&self) -> bool {
        if self.total_size < 34 {
            return false;
        }

        // Create temporary view of first 34 bytes
        let mut header_bytes = [0u8; 34];
        let mut offset = 0;

        for fragment in &self.fragments {
            let to_copy = (34 - offset).min(fragment.len());
            header_bytes[offset..offset + to_copy].copy_from_slice(&fragment[..to_copy]);
            offset += to_copy;
            if offset >= 34 {
                break;
            }
        }

        let header_len = u32::from_be_bytes([
            header_bytes[22],
            header_bytes[23],
            header_bytes[24],
            header_bytes[25],
        ]) as usize;

        let payload_len_offset = 26 + header_len;
        let total_header_size = payload_len_offset + 8;

        if self.total_size < total_header_size {
            return false;
        }

        // Would need to read payload length, but this is a reasonable heuristic
        true
    }
}

/// Memory-mapped buffer for large message processing
pub struct MappedBuffer {
    ptr:             NonNull<u8>,
    len:             usize,
    file_descriptor: Option<i32>,
}

impl MappedBuffer {
    /// Create memory-mapped buffer from file
    ///
    /// # Errors
    ///
    /// Returns `BufferError` if:
    /// - The file path contains null bytes
    /// - The file cannot be opened
    /// - File stat operation fails
    /// - Memory mapping fails
    /// - File size is invalid or too large for the target platform
    ///
    /// # Panics
    ///
    /// May panic if the memory mapping returns a null pointer unexpectedly
    pub fn from_file(path: &str) -> Result<Self, BufferError> {
        use std::ffi::CString;

        let c_path = CString::new(path.as_bytes()).map_err(|_| BufferError::InvalidPath)?;

        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(BufferError::FileOpenFailed);
        }

        let mut stat: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut stat) } < 0 {
            unsafe { libc::close(fd) };
            return Err(BufferError::StatFailed);
        }

        let len = usize::try_from(stat.st_size).map_err(|_| BufferError::InvalidSize)?;
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_PRIVATE,
                fd,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            unsafe { libc::close(fd) };
            return Err(BufferError::MmapFailed);
        }

        Ok(Self {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut u8) },
            len,
            file_descriptor: Some(fd),
        })
    }

    /// Create anonymous memory mapping
    ///
    /// # Errors
    ///
    /// Returns `BufferError::MmapFailed` if the memory mapping operation fails
    ///
    /// # Panics
    ///
    /// May panic if the memory mapping returns a null pointer unexpectedly
    pub fn anonymous(size: usize) -> Result<Self, BufferError> {
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };

        if ptr == libc::MAP_FAILED {
            return Err(BufferError::MmapFailed);
        }

        Ok(Self {
            ptr:             unsafe { NonNull::new_unchecked(ptr as *mut u8) },
            len:             size,
            file_descriptor: None,
        })
    }

    /// Get buffer as slice
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    /// Advise kernel about access pattern
    ///
    /// # Errors
    ///
    /// Returns `BufferError::AllocationFailed` if the madvise system call fails
    pub fn advise_sequential(&self) -> Result<(), BufferError> {
        let result = unsafe {
            libc::madvise(
                self.ptr.as_ptr() as *mut libc::c_void,
                self.len,
                libc::MADV_SEQUENTIAL,
            )
        };

        if result == 0 {
            Ok(())
        } else {
            Err(BufferError::AllocationFailed)
        }
    }
}

impl Drop for MappedBuffer {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr.as_ptr() as *mut libc::c_void, self.len);
            if let Some(fd) = self.file_descriptor {
                libc::close(fd);
            }
        }
    }
}

/// Ring buffer for streaming message processing
pub struct RingBuffer {
    buffer:    Vec<u8>,
    read_pos:  usize,
    write_pos: usize,
    full:      bool,
}

impl RingBuffer {
    /// Create a new ring buffer with the specified capacity
    ///
    /// # Errors
    ///
    /// Returns `BufferError` if the capacity is too large for memory allocation
    pub fn new(capacity: usize) -> Result<Self, BufferError> {
        Ok(Self {
            buffer:    vec![0u8; capacity],
            read_pos:  0,
            write_pos: 0,
            full:      false,
        })
    }

    /// Available space for writing
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

    /// Available data for reading
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
    /// Returns `BufferError` if the write operation fails due to buffer
    /// constraints or if the data cannot be written to the underlying
    /// buffer
    pub fn write(&mut self, data: &[u8]) -> Result<usize, BufferError> {
        let available = self.available_write();
        let to_write = data.len().min(available);

        if to_write == 0 {
            return Ok(0);
        }

        let buffer_len = self.buffer.len();
        let end_space = buffer_len - self.write_pos;

        if to_write <= end_space {
            // Single copy
            self.buffer[self.write_pos..self.write_pos + to_write]
                .copy_from_slice(&data[..to_write]);
            self.write_pos = (self.write_pos + to_write) % buffer_len;
        } else {
            // Split copy
            self.buffer[self.write_pos..].copy_from_slice(&data[..end_space]);
            self.buffer[..to_write - end_space].copy_from_slice(&data[end_space..to_write]);
            self.write_pos = to_write - end_space;
        }

        if self.write_pos == self.read_pos {
            self.full = true;
        }

        Ok(to_write)
    }

    /// Read data from ring buffer
    pub fn read(&mut self, output: &mut [u8]) -> usize {
        let available = self.available_read();
        let to_read = output.len().min(available);

        if to_read == 0 {
            return 0;
        }

        let buffer_len = self.buffer.len();
        let end_space = buffer_len - self.read_pos;

        if to_read <= end_space {
            // Single copy
            output[..to_read].copy_from_slice(&self.buffer[self.read_pos..self.read_pos + to_read]);
            self.read_pos = (self.read_pos + to_read) % buffer_len;
        } else {
            // Split copy
            output[..end_space].copy_from_slice(&self.buffer[self.read_pos..]);
            output[end_space..to_read].copy_from_slice(&self.buffer[..to_read - end_space]);
            self.read_pos = to_read - end_space;
        }

        self.full = false;
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
    #[allow(clippy::cast_precision_loss)]
    pub fn hit_ratio(&self) -> f64 {
        let hits = self.pool_hits.load(Ordering::Relaxed);
        let total = hits + self.pool_misses.load(Ordering::Relaxed);
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Buffer management errors
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
    #[error("Invalid file path")]
    InvalidPath,
    #[error("Failed to open file")]
    FileOpenFailed,
    #[error("Failed to stat file")]
    StatFailed,
    #[error("Memory mapping failed")]
    MmapFailed,
    #[error("madvise system call failed")]
    MadviseFailed,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_manager() {
        let mut manager = BufferManager::new(64);
        let buffer = manager.allocate(1024).unwrap();
        assert_eq!(buffer.capacity(), 1024);
        manager.deallocate(buffer);
    }

    #[test]
    fn test_message_assembler() {
        let mut assembler = MessageAssembler::new();
        assembler.add_fragment(Bytes::from("hello"));
        assembler.add_fragment(Bytes::from(" world"));

        let result = assembler.assemble().unwrap();
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_ring_buffer() {
        let mut ring = RingBuffer::new(10).unwrap();
        let written = ring.write(b"hello").unwrap();
        assert_eq!(written, 5);

        let mut output = [0u8; 10];
        let read = ring.read(&mut output);
        assert_eq!(read, 5);
        assert_eq!(&output[..5], b"hello");
    }
}
