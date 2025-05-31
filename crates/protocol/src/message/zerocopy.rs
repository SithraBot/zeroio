use bytes::{BufMut, Bytes};

use crate::message::types::{Header, MessageDeserializeError, MessageEncodeError, MessageType};

/// Zero-copy message decoding trait that avoids unnecessary data copying
pub trait ZeroCopyDecode<'a> {
    /// Decode message metadata without copying payload data
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if the buffer contains invalid message
    /// format or if the metadata cannot be parsed from the buffer
    fn decode_metadata(buffer: &'a [u8]) -> Result<MessageMetadata<'a>, MessageDeserializeError>;

    /// Get raw header bytes without deserialization
    fn header_slice(&self) -> &'a [u8];

    /// Get raw payload bytes without deserialization
    fn payload_slice(&self) -> &'a [u8];

    /// Check if header needs to be parsed for a specific field
    fn has_header_field(&self, field: HeaderField) -> bool;
}

/// Zero-copy message encoding trait for streaming operations
pub trait ZeroCopyEncode {
    /// Calculate exact size needed without temporary allocations
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if the message structure is invalid
    /// or if size calculation overflows
    fn calculate_exact_size(&self) -> Result<usize, MessageEncodeError>;

    /// Write directly to buffer without intermediate allocations
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if the buffer has insufficient capacity
    /// or if encoding fails due to invalid message data
    fn write_to_buffer<B: BufMut>(&self, buf: &mut B) -> Result<(), MessageEncodeError>;

    /// Stream encode to writer
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if:
    /// - Writing to the output stream fails
    /// - Message encoding encounters invalid data
    /// - I/O operations are interrupted or fail
    fn stream_encode<W: std::io::Write>(&self, writer: W) -> Result<usize, MessageEncodeError>;
}

/// Metadata extracted from message buffer without full deserialization
#[derive(Debug, Clone)]
pub struct MessageMetadata<'a> {
    pub buffer:         &'a [u8],
    pub version:        u8,
    pub msg_type:       MessageType,
    pub client_id:      u32,
    pub header_offset:  usize,
    pub header_len:     u32,
    pub payload_offset: usize,
    pub payload_len:    u64,
}

/// Header field types for selective parsing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeaderField {
    Routing,
    Reqrep,
    Topic,
    Status,
    Auth,
    Keepalive,
}

/// Borrowed message that references buffer data directly
#[derive(Debug)]
pub struct BorrowedMessage<'a> {
    metadata:     MessageMetadata<'a>,
    header_cache: Option<Header>,
}

impl<'a> BorrowedMessage<'a> {
    /// Create from buffer with minimal validation
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if:
    /// - Buffer is too short to contain a valid message
    /// - Message format is invalid or corrupted
    /// - Header or payload length fields are invalid
    pub fn from_buffer(buffer: &'a [u8]) -> Result<Self, MessageDeserializeError> {
        let metadata = Self::parse_metadata(buffer)?;
        Ok(Self {
            metadata,
            header_cache: None,
        })
    }

    /// Parse message metadata from buffer
    fn parse_metadata(buffer: &'a [u8]) -> Result<MessageMetadata<'a>, MessageDeserializeError> {
        const VERSION_OFFSET: usize = 0;
        const TYPE_OFFSET: usize = 1;
        const CLIENT_ID_OFFSET: usize = 2;
        const RESERVED_OFFSET: usize = 6;
        const HEADER_LEN_OFFSET: usize = 22;
        const BASE_HEADER_SIZE: usize = 34;
        const PAYLOAD_LEN_SIZE: usize = 8;

        if buffer.len() < BASE_HEADER_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        let version = buffer[VERSION_OFFSET];
        if version != 1 {
            return Err(MessageDeserializeError::InvalidVersion);
        }

        let msg_type = MessageType::try_from(buffer[TYPE_OFFSET])?;
        let client_id = u32::from_be_bytes([
            buffer[CLIENT_ID_OFFSET],
            buffer[CLIENT_ID_OFFSET + 1],
            buffer[CLIENT_ID_OFFSET + 2],
            buffer[CLIENT_ID_OFFSET + 3],
        ]);

        // Quick validation of reserved bytes
        if !buffer[RESERVED_OFFSET..RESERVED_OFFSET + 16].iter().all(|&b| b == 0) {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Reserved bytes must be zero".to_string(),
            ));
        }

        let header_len = u32::from_be_bytes([
            buffer[HEADER_LEN_OFFSET],
            buffer[HEADER_LEN_OFFSET + 1],
            buffer[HEADER_LEN_OFFSET + 2],
            buffer[HEADER_LEN_OFFSET + 3],
        ]);

        let header_offset = BASE_HEADER_SIZE - PAYLOAD_LEN_SIZE;
        let payload_len_offset = header_offset + header_len as usize;

        if buffer.len() < payload_len_offset + PAYLOAD_LEN_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        let payload_len = u64::from_be_bytes([
            buffer[payload_len_offset],
            buffer[payload_len_offset + 1],
            buffer[payload_len_offset + 2],
            buffer[payload_len_offset + 3],
            buffer[payload_len_offset + 4],
            buffer[payload_len_offset + 5],
            buffer[payload_len_offset + 6],
            buffer[payload_len_offset + 7],
        ]);

        let payload_offset = payload_len_offset + PAYLOAD_LEN_SIZE;
        let expected_total = payload_offset
            + usize::try_from(payload_len)
                .map_err(|_| MessageDeserializeError::InvalidMessageType)?;

        if buffer.len() < expected_total {
            return Err(MessageDeserializeError::IncompletePayload);
        }

        Ok(MessageMetadata {
            buffer,
            version,
            msg_type,
            client_id,
            header_offset,
            header_len,
            payload_offset,
            payload_len,
        })
    }

    /// Get message type without any deserialization
    #[inline]
    #[must_use]
    pub fn message_type(&self) -> MessageType {
        self.metadata.msg_type
    }

    /// Get client ID without any deserialization
    #[inline]
    #[must_use]
    pub fn client_id(&self) -> u32 {
        self.metadata.client_id
    }

    /// Get version without any deserialization
    #[inline]
    #[must_use]
    pub fn version(&self) -> u8 {
        self.metadata.version
    }

    /// Get raw header bytes
    #[inline]
    #[must_use]
    pub fn header_bytes(&self) -> &'a [u8] {
        let start = self.metadata.header_offset;
        let end =
            start + usize::try_from(self.metadata.header_len).unwrap_or(self.metadata.buffer.len());
        &self.metadata.buffer[start..end]
    }

    /// Get raw payload bytes
    #[inline]
    #[must_use]
    pub fn payload_bytes(&self) -> &'a [u8] {
        let start = self.metadata.payload_offset;
        let end = start
            + usize::try_from(self.metadata.payload_len).unwrap_or(self.metadata.buffer.len());
        &self.metadata.buffer[start..end]
    }

    /// Lazy header deserialization with caching
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError::HeaderParseError` if:
    /// - Header bytes cannot be deserialized from MessagePack format
    /// - Header contains invalid or corrupted data
    /// - Required header fields are missing or malformed
    ///
    /// # Panics
    ///
    /// May panic if the header cache is in an inconsistent state after
    /// successful parsing, which should never occur in normal operation
    pub fn header(&mut self) -> Result<&Header, MessageDeserializeError> {
        if self.header_cache.is_none() {
            let header_bytes = self.header_bytes();
            if header_bytes.is_empty() {
                self.header_cache = Some(Header::default());
            } else {
                let header = rmp_serde::from_slice(header_bytes)
                    .map_err(|e| MessageDeserializeError::DeserializeError(e.to_string()))?;
                self.header_cache = Some(header);
            }
        }
        Ok(self.header_cache.as_ref().unwrap_or_else(|| {
            unreachable!("Header cache should be populated after successful parsing")
        }))
    }

    /// Check if header contains specific field without full deserialization
    #[must_use]
    pub fn has_header_field(&self, field: HeaderField) -> bool {
        let header_bytes = self.header_bytes();
        if header_bytes.is_empty() {
            return false;
        }

        // Use memchr for fast field detection
        let field_name: &[u8] = match field {
            HeaderField::Routing => b"routing",
            HeaderField::Reqrep => b"reqrep",
            HeaderField::Topic => b"topic",
            HeaderField::Status => b"status",
            HeaderField::Auth => b"auth",
            HeaderField::Keepalive => b"keepalive",
        };

        memchr::memmem::find(header_bytes, field_name).is_some()
    }

    /// Get total message size
    #[inline]
    #[must_use]
    pub fn total_size(&self) -> usize {
        self.metadata.payload_offset
            + usize::try_from(self.metadata.payload_len).unwrap_or(usize::MAX)
    }

    /// Convert to owned message if needed
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError::HeaderSerializeError` if header
    /// deserialization from MessagePack format fails due to corrupted or
    /// invalid header data
    ///
    /// # Panics
    ///
    /// Should not panic under normal circumstances as header cache consistency
    /// is maintained
    pub fn to_owned(&self) -> Result<crate::message::traits::OwnedMessage, MessageEncodeError> {
        let header = if let Some(cached_header) = &self.header_cache {
            cached_header.clone()
        } else {
            let header_bytes = self.header_bytes();
            if header_bytes.is_empty() {
                Header::default()
            } else {
                rmp_serde::from_slice(header_bytes)
                    .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))?
            }
        };

        Ok(crate::message::traits::OwnedMessage {
            version: self.metadata.version,
            msg_type: self.metadata.msg_type,
            client_id: self.metadata.client_id,
            header,
            payload: self.payload_bytes().to_vec(),
        })
    }
}

impl<'a> ZeroCopyDecode<'a> for BorrowedMessage<'a> {
    fn decode_metadata(buffer: &'a [u8]) -> Result<MessageMetadata<'a>, MessageDeserializeError> {
        Self::parse_metadata(buffer)
    }

    fn header_slice(&self) -> &'a [u8] {
        self.header_bytes()
    }

    fn payload_slice(&self) -> &'a [u8] {
        self.payload_bytes()
    }

    fn has_header_field(&self, field: HeaderField) -> bool {
        self.has_header_field(field)
    }
}

/// Streaming encoder for zero-copy message building
pub struct StreamingEncoder {
    version:      u8,
    msg_type:     MessageType,
    client_id:    u32,
    header_data:  Option<Bytes>,
    payload_data: Vec<u8>,
}

impl StreamingEncoder {
    /// Create new streaming encoder
    #[must_use]
    pub fn new(msg_type: MessageType, client_id: u32) -> Self {
        Self {
            version: 1,
            msg_type,
            client_id,
            header_data: None,
            payload_data: Vec::new(),
        }
    }

    /// Set header from pre-serialized bytes
    #[must_use]
    pub fn with_header_bytes(mut self, header: Bytes) -> Self {
        self.header_data = Some(header);
        self
    }

    /// Set header from object (will serialize once)
    /// Set header from structured data
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError::HeaderSerializeError` if header
    /// serialization to MessagePack fails
    pub fn with_header(mut self, header: &Header) -> Result<Self, MessageEncodeError> {
        let header_bytes = rmp_serde::to_vec(header)
            .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))?;
        self.header_data = Some(Bytes::from(header_bytes));
        Ok(self)
    }

    /// Set payload from bytes
    #[must_use]
    pub fn with_payload_bytes(mut self, payload: &[u8]) -> Self {
        self.payload_data = payload.to_vec();
        self
    }

    /// Set payload from owned vector  
    #[must_use]
    pub fn with_payload_owned(mut self, payload: Vec<u8>) -> Self {
        self.payload_data = payload;
        self
    }

    /// Set payload from serializable object
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError::PayloadSerializeError` if payload
    /// serialization to MessagePack fails
    pub fn with_payload<T: serde::Serialize>(
        mut self,
        payload: &T,
    ) -> Result<Self, MessageEncodeError> {
        let payload_bytes = rmp_serde::to_vec(payload)
            .map_err(|e| MessageEncodeError::PayloadSerializeError(e.to_string()))?;
        self.payload_data = payload_bytes;
        Ok(self)
    }
}

impl ZeroCopyEncode for StreamingEncoder {
    fn calculate_exact_size(&self) -> Result<usize, MessageEncodeError> {
        let header_len = self.header_data.as_ref().map_or(0, |h| h.len());
        let payload_len = self.payload_data.len();

        // Base header (34) + header data + payload
        Ok(34 + header_len + payload_len)
    }

    fn write_to_buffer<B: BufMut>(&self, buf: &mut B) -> Result<(), MessageEncodeError> {
        let header_bytes = self.header_data.as_ref().map_or(&[][..], |h| h.as_ref());
        let header_len = header_bytes.len();
        let payload_len = self.payload_data.len();

        // Check buffer capacity
        let total_size = 34 + header_len + payload_len;
        if buf.remaining_mut() < total_size {
            return Err(MessageEncodeError::MessageTooLarge(total_size));
        }

        // Write base header
        buf.put_u8(self.version);
        buf.put_u8(self.msg_type as u8);
        buf.put_u32(self.client_id);

        // Write 16 reserved bytes
        buf.put_bytes(0, 16);

        // Write header length
        buf.put_u32(u32::try_from(header_len).unwrap_or(u32::MAX));

        // Write header data
        buf.put_slice(header_bytes);

        // Write payload length
        buf.put_u64(payload_len as u64);

        // Write payload data
        buf.put_slice(&self.payload_data);

        Ok(())
    }

    fn stream_encode<W: std::io::Write>(&self, mut writer: W) -> Result<usize, MessageEncodeError> {
        let header_bytes = self.header_data.as_ref().map_or(&[][..], |h| h.as_ref());
        let header_len = header_bytes.len();
        let payload_len = self.payload_data.len();
        let mut written = 0;

        // Write base header
        writer
            .write_all(&[self.version])
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 1;

        writer
            .write_all(&[self.msg_type as u8])
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 1;

        writer
            .write_all(&self.client_id.to_be_bytes())
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 4;

        // Write 16 reserved bytes
        writer
            .write_all(&[0; 16])
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 16;

        // Write header length
        writer
            .write_all(&u32::try_from(header_len).unwrap_or(u32::MAX).to_be_bytes())
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 4;

        // Write header data
        writer
            .write_all(header_bytes)
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += header_len;

        // Write payload length
        writer
            .write_all(&(payload_len as u64).to_be_bytes())
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += 8;

        // Write payload data
        writer
            .write_all(&self.payload_data)
            .map_err(|e| MessageEncodeError::InvalidConfiguration(e.to_string()))?;
        written += payload_len;

        Ok(written)
    }
}

/// Utility functions for zero-copy operations
pub mod utils {
    use super::*;

    /// Extract message type from buffer without full parsing
    /// Get message type without full deserialization
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if:
    /// - Buffer is too short to contain message type field
    /// - Message type value is invalid or unsupported
    pub fn quick_message_type(buffer: &[u8]) -> Result<MessageType, MessageDeserializeError> {
        if buffer.len() < 21 {
            return Err(MessageDeserializeError::IncompleteHeader);
        }
        MessageType::try_from(buffer[1]).map_err(|_| MessageDeserializeError::InvalidMessageType)
    }

    /// Get client ID without full deserialization  
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError::IncompleteHeader` if buffer is too
    /// short to contain client ID field
    pub fn quick_client_id(buffer: &[u8]) -> Result<u32, MessageDeserializeError> {
        if buffer.len() < 21 {
            return Err(MessageDeserializeError::IncompleteHeader);
        }
        Ok(u32::from_be_bytes([
            buffer[2], buffer[3], buffer[4], buffer[5],
        ]))
    }

    /// Check if buffer contains a complete message
    #[must_use]
    pub fn is_complete_message(buffer: &[u8]) -> bool {
        if buffer.len() < 34 {
            return false;
        }

        let header_len =
            u32::from_be_bytes([buffer[22], buffer[23], buffer[24], buffer[25]]) as usize;
        let payload_len_offset = 26 + header_len;

        if buffer.len() < payload_len_offset + 8 {
            return false;
        }

        let payload_len = usize::try_from(u64::from_be_bytes([
            buffer[payload_len_offset],
            buffer[payload_len_offset + 1],
            buffer[payload_len_offset + 2],
            buffer[payload_len_offset + 3],
            buffer[payload_len_offset + 4],
            buffer[payload_len_offset + 5],
            buffer[payload_len_offset + 6],
            buffer[payload_len_offset + 7],
        ]))
        .unwrap_or(usize::MAX);

        buffer.len() >= payload_len_offset + 8 + payload_len
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;
    use crate::message::types::*;

    #[test]
    fn test_borrowed_message_basic() {
        // Create a simple message buffer
        let mut buffer = vec![0u8; 50];
        buffer[0] = 1; // version
        buffer[1] = 4; // broadcast type
        buffer[2..6].copy_from_slice(&1000u32.to_be_bytes()); // client_id
        buffer[22..26].copy_from_slice(&0u32.to_be_bytes()); // header_len
        buffer[26..34].copy_from_slice(&8u64.to_be_bytes()); // payload_len
        buffer[34..42].copy_from_slice(b"testdata"); // payload

        let msg = BorrowedMessage::from_buffer(&buffer).unwrap();
        assert_eq!(msg.message_type(), MessageType::Broadcast);
        assert_eq!(msg.client_id(), 1000);
        assert_eq!(msg.payload_bytes(), b"testdata");
    }

    #[test]
    fn test_streaming_encoder() {
        let encoder =
            StreamingEncoder::new(MessageType::Broadcast, 1000).with_payload_bytes(b"hello world");

        let mut buffer = BytesMut::with_capacity(100);
        encoder.write_to_buffer(&mut buffer).unwrap();

        let borrowed = BorrowedMessage::from_buffer(&buffer).unwrap();
        assert_eq!(borrowed.message_type(), MessageType::Broadcast);
        assert_eq!(borrowed.client_id(), 1000);
        assert_eq!(borrowed.payload_bytes(), b"hello world");
    }

    #[test]
    fn test_quick_utils() {
        let mut buffer = vec![0u8; 21];
        buffer[0] = 1; // version
        buffer[1] = 3; // notification
        buffer[2..6].copy_from_slice(&12345u32.to_be_bytes());

        assert_eq!(
            utils::quick_message_type(&buffer).unwrap(),
            MessageType::Notification
        );
        assert_eq!(utils::quick_client_id(&buffer).unwrap(), 12345);
    }
}
