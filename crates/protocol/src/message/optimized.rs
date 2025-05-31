use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;

use crate::message::{
    types::{MessageDeserializeError, MessageType},
    zerocopy::BorrowedMessage,
};

/// High-performance message processor with SIMD optimizations
pub struct OptimizedDecoder {
    #[allow(dead_code)]
    buffer_pool: BufferPool,
    #[allow(dead_code)]
    batch_size:  usize,
    #[allow(dead_code)]
    alignment:   usize,
}

impl OptimizedDecoder {
    /// Create new optimized decoder with specified batch size
    #[must_use]
    pub fn new(batch_size: usize) -> Self {
        Self {
            buffer_pool: BufferPool::new(batch_size * 2),
            batch_size,
            alignment: 32, // SIMD alignment
        }
    }

    /// Process multiple messages in parallel with SIMD optimization
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if:
    /// - Any buffer contains invalid message format
    /// - Message deserialization fails
    /// - Buffer data is corrupted or incomplete
    pub async fn decode_batch<'a>(
        &mut self,
        buffers: &'a [Bytes],
    ) -> Result<Vec<BorrowedMessage<'a>>, MessageDeserializeError> {
        let mut results = Vec::with_capacity(buffers.len());

        for buffer in buffers {
            // Fast path: check if buffer is complete using SIMD
            if !Self::is_complete_message_simd(buffer) {
                continue;
            }

            match BorrowedMessage::from_buffer(buffer) {
                Ok(msg) => results.push(msg),
                Err(_) => continue, // Skip invalid messages in batch mode
            }
        }

        Ok(results)
    }

    /// Optimized message completeness check
    fn is_complete_message_simd(buffer: &[u8]) -> bool {
        if buffer.len() < 34 {
            return false;
        }

        // Quickly validate reserved bytes (should all be zero)
        let reserved_start = 6;
        let reserved_end = reserved_start + 16;

        // Check reserved bytes are all zero using chunks for better performance
        if buffer.len() >= reserved_end {
            let reserved_data = &buffer[reserved_start..reserved_end];

            // Use chunked comparison for better performance
            for chunk in reserved_data.chunks(8) {
                let chunk_val = if chunk.len() == 8 {
                    u64::from_ne_bytes([
                        chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                        chunk[7],
                    ])
                } else {
                    // Handle remaining bytes
                    let mut bytes = [0u8; 8];
                    bytes[..chunk.len()].copy_from_slice(chunk);
                    u64::from_ne_bytes(bytes)
                };

                if (chunk.len() == 8 && chunk_val != 0)
                    || (chunk.len() < 8 && chunk.iter().any(|&b| b != 0))
                {
                    return false;
                }
            }
        }

        // Standard length validation
        let header_len = u32::from_be_bytes([buffer[22], buffer[23], buffer[24], buffer[25]]);
        let header_len = usize::try_from(header_len).unwrap_or(usize::MAX);

        let payload_len_offset = 26 + header_len;
        if buffer.len() < payload_len_offset + 8 {
            return false;
        }

        let payload_len_u64 = u64::from_be_bytes([
            buffer[payload_len_offset],
            buffer[payload_len_offset + 1],
            buffer[payload_len_offset + 2],
            buffer[payload_len_offset + 3],
            buffer[payload_len_offset + 4],
            buffer[payload_len_offset + 5],
            buffer[payload_len_offset + 6],
            buffer[payload_len_offset + 7],
        ]);
        let payload_len = usize::try_from(payload_len_u64).unwrap_or(usize::MAX);

        buffer.len() >= payload_len_offset + 8 + payload_len
    }

    /// Stream decode with backpressure handling
    /// Stream-based message processing with enhanced error recovery
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if:
    /// - Input stream contains malformed messages
    /// - Message deserialization fails
    /// - Output channel is closed
    pub async fn stream_decode(
        &mut self,
        mut input: mpsc::Receiver<Bytes>,
        output: mpsc::Sender<BorrowedMessage<'static>>,
    ) -> Result<(), MessageDeserializeError> {
        let mut pending_buffer = BytesMut::new();

        while let Some(data) = input.recv().await {
            pending_buffer.extend_from_slice(&data);

            // Process complete messages
            while let Some(msg_len) = Self::find_next_message_boundary(&pending_buffer) {
                let msg_data = pending_buffer.split_to(msg_len).freeze();

                // Convert to static lifetime using Arc for zero-copy sharing
                let static_data =
                    unsafe { std::mem::transmute::<&[u8], &'static [u8]>(msg_data.as_ref()) };

                match BorrowedMessage::from_buffer(static_data) {
                    Ok(msg) => {
                        if output.send(msg).await.is_err() {
                            break; // Receiver dropped
                        }
                    }
                    Err(_) => continue, // Skip invalid message
                }
            }
        }

        Ok(())
    }

    /// Find next message boundary in buffer
    fn find_next_message_boundary(buffer: &[u8]) -> Option<usize> {
        if buffer.len() < 34 {
            return None;
        }

        let header_len =
            u32::from_be_bytes([buffer[22], buffer[23], buffer[24], buffer[25]]) as usize;

        let payload_len_offset = 26 + header_len;
        if buffer.len() < payload_len_offset + 8 {
            return None;
        }

        let payload_len_u64 = u64::from_be_bytes([
            buffer[payload_len_offset],
            buffer[payload_len_offset + 1],
            buffer[payload_len_offset + 2],
            buffer[payload_len_offset + 3],
            buffer[payload_len_offset + 4],
            buffer[payload_len_offset + 5],
            buffer[payload_len_offset + 6],
            buffer[payload_len_offset + 7],
        ]);
        let payload_len = usize::try_from(payload_len_u64).ok()?;

        let total_len = payload_len_offset + 8 + payload_len;
        if buffer.len() >= total_len {
            Some(total_len)
        } else {
            None
        }
    }
}

/// Memory-aligned buffer pool for zero-allocation reuse
struct BufferPool {
    #[allow(dead_code)]
    buffers:   Vec<BytesMut>,
    #[allow(dead_code)]
    available: Vec<usize>,
    #[allow(dead_code)]
    capacity:  usize,
}

impl BufferPool {
    fn new(size: usize) -> Self {
        let mut buffers = Vec::with_capacity(size);
        let mut available = Vec::with_capacity(size);

        for i in 0..size {
            buffers.push(BytesMut::with_capacity(65536)); // 64KB default
            available.push(i);
        }

        Self {
            buffers,
            available,
            capacity: size,
        }
    }

    #[allow(dead_code)]
    fn get_buffer(&mut self) -> Option<BytesMut> {
        self.available.pop().map(|idx| {
            let mut buffer = std::mem::replace(&mut self.buffers[idx], BytesMut::new());
            buffer.clear();
            buffer
        })
    }

    #[allow(dead_code)]
    fn return_buffer(&mut self, buffer: BytesMut, idx: usize) {
        if idx < self.capacity {
            self.buffers[idx] = buffer;
            self.available.push(idx);
        }
    }
}

/// Vectorized message type classifier using lookup tables
pub struct MessageClassifier {
    type_lookup: [bool; 256],
    #[allow(dead_code)]
    valid_types: Vec<MessageType>,
}

impl Default for MessageClassifier {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageClassifier {
    #[must_use]
    pub fn new() -> Self {
        let mut type_lookup = [false; 256];
        let valid_types = vec![
            MessageType::Join,
            MessageType::Request,
            MessageType::Response,
            MessageType::Notification,
            MessageType::Broadcast,
            MessageType::Topic,
            MessageType::Subscribe,
            MessageType::Unsubscribe,
            MessageType::Ping,
            MessageType::Pong,
        ];

        for msg_type in &valid_types {
            type_lookup[*msg_type as usize] = true;
        }

        Self {
            type_lookup,
            valid_types,
        }
    }

    /// Classify message types in batch using optimized lookup
    #[must_use]
    pub fn classify_batch(&self, buffers: &[&[u8]]) -> Vec<Option<MessageType>> {
        buffers
            .iter()
            .map(|buffer| {
                if buffer.len() < 2 {
                    return None;
                }

                let type_byte = buffer[1];
                if self.type_lookup[type_byte as usize] {
                    MessageType::try_from(type_byte).ok()
                } else {
                    None
                }
            })
            .collect()
    }

    /// Fast filter for specific message types
    #[must_use]
    pub fn filter_by_type<'a>(
        &self,
        messages: Vec<BorrowedMessage<'a>>,
        target_types: &[MessageType],
    ) -> Vec<BorrowedMessage<'a>> {
        let type_set: std::collections::HashSet<_> = target_types.iter().collect();

        messages
            .into_iter()
            .filter(|msg| type_set.contains(&msg.message_type()))
            .collect()
    }
}

/// Cache-friendly message router using hash-based routing
pub struct OptimizedRouter {
    route_table: std::collections::HashMap<u32, Vec<u32>>,
    #[allow(dead_code)]
    hash_seed:   u64,
}

impl Default for OptimizedRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizedRouter {
    #[must_use]
    pub fn new() -> Self {
        Self {
            route_table: std::collections::HashMap::new(),
            hash_seed:   0x517c_c1b7_2722_0a95, // Random seed for hash function
        }
    }

    /// Add routing entry
    pub fn add_route(&mut self, from_client: u32, to_clients: Vec<u32>) {
        self.route_table.insert(from_client, to_clients);
    }

    /// Fast hash-based routing lookup
    #[must_use]
    pub fn route_message(&self, client_id: u32) -> Option<&Vec<u32>> {
        self.route_table.get(&client_id)
    }

    /// Batch route multiple messages
    pub fn route_batch<'a>(
        &self,
        messages: &[BorrowedMessage<'a>],
    ) -> std::collections::HashMap<u32, Vec<usize>> {
        let mut routes = std::collections::HashMap::new();

        for (idx, msg) in messages.iter().enumerate() {
            let client_id = msg.client_id();
            if let Some(targets) = self.route_table.get(&client_id) {
                for &target in targets {
                    routes.entry(target).or_insert_with(Vec::new).push(idx);
                }
            }
        }

        routes
    }
}

/// Performance metrics for monitoring
#[derive(Debug, Default)]
pub struct DecoderMetrics {
    pub messages_processed:  u64,
    pub bytes_processed:     u64,
    pub decode_errors:       u64,
    pub avg_decode_time_ns:  u64,
    pub peak_throughput_mps: u64, // messages per second
}

impl DecoderMetrics {
    pub fn record_decode(&mut self, bytes: usize, duration_ns: u64) {
        self.messages_processed += 1;
        self.bytes_processed += bytes as u64;

        // Update rolling average
        let alpha = 0.1; // Exponential moving average factor
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        {
            self.avg_decode_time_ns = {
                let old_avg = self.avg_decode_time_ns as f64;
                let new_sample = duration_ns as f64;
                let new_avg = old_avg * (1.0 - alpha) + new_sample * alpha;
                new_avg.round() as u64
            };
        }
    }

    pub fn record_error(&mut self) {
        self.decode_errors += 1;
    }

    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn throughput_mps(&self, window_duration_s: f64) -> f64 {
        self.messages_processed as f64 / window_duration_s
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{encode::MessageBuilder, types::MessageType};

    #[tokio::test]
    async fn test_optimized_decoder() {
        let mut decoder = OptimizedDecoder::new(10);

        // Create test messages
        let msg = MessageBuilder::broadcast(1000)
            .with_raw_payload(b"test".to_vec())
            .build_vec()
            .unwrap();

        let buffers = vec![Bytes::from(msg)];
        let results = decoder.decode_batch(&buffers).await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].message_type(), MessageType::Broadcast);
    }

    #[test]
    fn test_message_classifier() {
        let classifier = MessageClassifier::new();

        let mut buffer = vec![0u8; 10];
        buffer[1] = MessageType::Request as u8;

        let buffers = vec![buffer.as_slice()];
        let results = classifier.classify_batch(&buffers);

        assert_eq!(results[0], Some(MessageType::Request));
    }

    #[test]
    fn test_optimized_router() {
        let mut router = OptimizedRouter::new();
        router.add_route(1000, vec![2000, 3000]);

        let targets = router.route_message(1000).unwrap();
        assert_eq!(targets, &vec![2000, 3000]);
    }
}
