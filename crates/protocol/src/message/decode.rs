use std::cell::RefCell;

use crate::message::traits::*;
// Re-export core types for backward compatibility
pub use crate::message::types::{
    Auth, Header, Keepalive, MessageDeserializeError, MessageEncodeError, MessageType, Reqrep,
    Routing, StatusCode,
};

// Buffer offset constants
const VERSION_OFFSET: usize = 0;
const TYPE_OFFSET: usize = 1;
const CLIENT_ID_OFFSET: usize = 2;
const RESERVED_OFFSET: usize = 6;
const HEADER_LEN_OFFSET: usize = 22;
const BASE_HEADER_SIZE: usize = 34;
const PAYLOAD_LEN_SIZE: usize = 8;

/// A zero-copy message parser that borrows from a buffer with header caching
#[derive(Debug)]
pub struct Message<'a> {
    // Full message buffer
    buffer:         &'a [u8],
    // Parsed base header fields
    pub version:    u8,
    pub msg_type:   MessageType,
    pub client_id:  u32,
    // Calculated offsets and lengths
    header_offset:  usize,
    header_len:     u32,
    payload_offset: usize,
    payload_len:    u64,
    // Cached deserialized header for performance
    header_cache:   RefCell<Option<Header>>,
}

impl<'a> MessageDecode<'a> for Message<'a> {
    fn from_buffer(buffer: &'a [u8]) -> Result<Self, MessageDeserializeError> {
        if buffer.len() < BASE_HEADER_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        // Parse base header
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

        // Verify reserved bytes are zero
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

        // Validate we have enough data for header
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
        let total_expected_len = payload_offset
            + usize::try_from(payload_len).map_err(|_| {
                MessageDeserializeError::InvalidHeaderData(format!(
                    "Payload length {payload_len} too large for this platform"
                ))
            })?;

        if buffer.len() < total_expected_len {
            return Err(MessageDeserializeError::IncompletePayload);
        }

        let message = Message {
            buffer,
            version,
            msg_type,
            client_id,
            header_offset,
            header_len,
            payload_offset,
            payload_len,
            header_cache: RefCell::new(None),
        };

        // Validate header structure
        message.validate_header()?;

        Ok(message)
    }

    fn from_buffer_mut(buffer: &'a mut [u8]) -> Result<Self, MessageDeserializeError> {
        Self::from_buffer(buffer)
    }

    fn message_type(&self) -> MessageType {
        self.msg_type
    }

    fn version(&self) -> u8 {
        self.version
    }

    fn client_id(&self) -> u32 {
        self.client_id
    }

    fn header_bytes(&self) -> &[u8] {
        &self.buffer[self.header_offset..self.header_offset + self.header_len as usize]
    }

    fn header(&self) -> Result<Header, MessageDeserializeError> {
        // Check cache first
        if let Some(cached_header) = self.header_cache.borrow().as_ref() {
            return Ok(cached_header.clone());
        }

        if self.header_len == 0 {
            let default_header = Header::default();
            *self.header_cache.borrow_mut() = Some(default_header.clone());
            return Ok(default_header);
        }

        let header_bytes = self.header_bytes();
        if header_bytes.is_empty() {
            let default_header = Header::default();
            *self.header_cache.borrow_mut() = Some(default_header.clone());
            return Ok(default_header);
        }

        let header: Header = rmp_serde::from_slice(header_bytes)
            .map_err(|e| MessageDeserializeError::InvalidHeaderData(e.to_string()))?;

        // Cache the parsed header
        *self.header_cache.borrow_mut() = Some(header.clone());
        Ok(header)
    }

    fn payload_bytes(&self) -> &[u8] {
        let payload_end = self.payload_offset + usize::try_from(self.payload_len).unwrap_or(0);
        &self.buffer[self.payload_offset..payload_end]
    }

    fn payload<T>(&self) -> Result<T, MessageDeserializeError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        if self.payload_len == 0 {
            return Err(MessageDeserializeError::IncompletePayload);
        }

        let payload_bytes = self.payload_bytes();
        rmp_serde::from_slice(payload_bytes)
            .map_err(|e| MessageDeserializeError::DeserializeError(e.to_string()))
    }

    fn header_len(&self) -> u32 {
        self.header_len
    }

    fn payload_len(&self) -> u64 {
        self.payload_len
    }

    fn total_size(&self) -> usize {
        self.payload_offset + usize::try_from(self.payload_len).unwrap_or(0)
    }
}

impl<'a> Message<'a> {
    /// Convert to owned message
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if:
    /// - Header deserialization fails
    /// - Message payload is invalid or corrupted
    /// - Required message fields are missing
    pub fn to_owned(
        &self,
    ) -> Result<crate::message::traits::OwnedMessage, crate::message::types::MessageEncodeError>
    {
        let header = self.header().map_err(|e| {
            crate::message::types::MessageEncodeError::HeaderSerializeError(e.to_string())
        })?;

        Ok(crate::message::traits::OwnedMessage {
            version: self.version,
            msg_type: self.msg_type,
            client_id: self.client_id,
            header,
            payload: self.payload_bytes().to_vec(),
        })
    }

    /// Check if header is cached (for performance monitoring)
    #[must_use]
    pub fn is_header_cached(&self) -> bool {
        self.header_cache.borrow().is_some()
    }

    /// Pre-cache the header to avoid repeated parsing
    ///
    /// # Errors
    ///
    /// Returns `MessageDeserializeError` if header parsing fails
    pub fn cache_header(&self) -> Result<(), MessageDeserializeError> {
        if !self.is_header_cached() {
            let _: Header = self.header()?; // This will populate the cache
        }
        Ok(())
    }

    /// Get header field without full deserialization (optimized for common
    /// cases)
    #[must_use]
    pub fn has_routing(&self) -> bool {
        if let Ok(header) = self.header() {
            header.routing.is_some()
        } else {
            false
        }
    }

    /// Quick check for keepalive messages
    #[must_use]
    pub fn has_keepalive(&self) -> bool {
        if let Ok(header) = self.header() {
            header.keepalive.is_some()
        } else {
            false
        }
    }

    /// Get message priority for processing optimization
    #[must_use]
    pub fn message_priority(&self) -> u8 {
        match self.msg_type {
            MessageType::Ping | MessageType::Pong => 0, // Highest priority
            MessageType::Response => 1,                 // High priority
            MessageType::Request => 2,                  // Medium priority
            MessageType::Notification => 3,             // Lower priority
            MessageType::Broadcast | MessageType::Topic => 4, // Low priority
            _ => 5,                                     // Lowest priority
        }
    }

    fn validate_header(&self) -> Result<(), MessageDeserializeError> {
        let header = self.header()?;

        match self.msg_type {
            MessageType::Join => Self::validate_join_header(&header),
            MessageType::Request | MessageType::Response => {
                Self::validate_request_response_header(&header)
            }
            MessageType::Notification => Self::validate_notification_header(&header),
            MessageType::Broadcast => Self::validate_broadcast_header(&header),
            MessageType::Topic => Self::validate_topic_header(&header),
            MessageType::Subscribe | MessageType::Unsubscribe => {
                Self::validate_subscription_header(&header)
            }
            MessageType::Ping | MessageType::Pong => self.validate_keepalive_header(&header),
        }
    }

    fn validate_join_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.routing.is_some()
            || header.reqrep.is_some()
            || header.topic.is_some()
            || header.keepalive.is_some()
        {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "JOIN messages cannot have routing, reqrep, topic, or keepalive fields".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_request_response_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.routing.is_none() || header.reqrep.is_none() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Request/Response messages require routing and reqrep fields".to_string(),
            ));
        }
        if header.topic.is_some() || header.auth.is_some() || header.keepalive.is_some() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Request/Response messages cannot have topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        // Validate routing has exactly one entry
        if let Some(ref routing) = header.routing {
            if routing.len() != 1 {
                return Err(MessageDeserializeError::InvalidRoutingData(
                    "Request/Response messages must have exactly one routing entry".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn validate_notification_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.routing.is_none() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Notification messages require routing field".to_string(),
            ));
        }
        if header.reqrep.is_some()
            || header.topic.is_some()
            || header.auth.is_some()
            || header.keepalive.is_some()
        {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Notification messages cannot have reqrep, topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_broadcast_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.routing.is_some()
            || header.reqrep.is_some()
            || header.topic.is_some()
            || header.auth.is_some()
            || header.keepalive.is_some()
        {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Broadcast messages cannot have routing, reqrep, topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_topic_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.topic.is_none() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Topic messages require topic field".to_string(),
            ));
        }
        if header.reqrep.is_some() || header.auth.is_some() || header.keepalive.is_some() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Topic messages cannot have reqrep, auth, or keepalive fields".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_subscription_header(header: &Header) -> Result<(), MessageDeserializeError> {
        if header.topic.is_none() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Subscribe/Unsubscribe messages require topic field".to_string(),
            ));
        }
        if header.routing.is_some()
            || header.reqrep.is_some()
            || header.status.is_some()
            || header.auth.is_some()
            || header.keepalive.is_some()
        {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Subscribe/Unsubscribe messages cannot have routing, reqrep, status, auth, or \
                 keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_keepalive_header(&self, header: &Header) -> Result<(), MessageDeserializeError> {
        if header.keepalive.is_none() {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Ping/Pong messages require keepalive field".to_string(),
            ));
        }
        if header.routing.is_some()
            || header.reqrep.is_some()
            || header.topic.is_some()
            || header.status.is_some()
            || header.auth.is_some()
        {
            return Err(MessageDeserializeError::InvalidHeaderData(
                "Ping/Pong messages cannot have routing, reqrep, topic, status, or auth fields"
                    .to_string(),
            ));
        }
        // Additional validation for PONG: ensure no interval
        if self.msg_type == MessageType::Pong {
            if let Some(ref keepalive) = header.keepalive {
                if keepalive.interval.is_some() {
                    return Err(MessageDeserializeError::InvalidHeaderData(
                        "PONG messages cannot have interval in keepalive field".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        message: String,
        value:   u32,
    }

    fn create_valid_message_bytes() -> Vec<u8> {
        let mut buffer = vec![0u8; 1024];
        let mut offset = 0;

        // Version
        buffer[offset] = 1;
        offset += 1;

        // Type (Broadcast)
        buffer[offset] = MessageType::Broadcast as u8;
        offset += 1;

        // Client ID
        let client_id = 1234u32;
        buffer[offset..offset + 4].copy_from_slice(&client_id.to_be_bytes());
        offset += 4;

        // Reserved (16 bytes)
        offset += 16;

        // Header length (empty header)
        let header_len = 0u32;
        buffer[offset..offset + 4].copy_from_slice(&header_len.to_be_bytes());
        offset += 4;

        // Payload length
        let payload_len = 0u64;
        buffer[offset..offset + 8].copy_from_slice(&payload_len.to_be_bytes());
        offset += 8;

        buffer.truncate(offset);
        buffer
    }

    fn create_ping_message_bytes() -> Vec<u8> {
        let mut buffer = vec![0u8; 1024];
        let mut offset = 0;

        // Version
        buffer[offset] = 1;
        offset += 1;

        // Type (Ping)
        buffer[offset] = MessageType::Ping as u8;
        offset += 1;

        // Client ID
        let client_id = 1234u32;
        buffer[offset..offset + 4].copy_from_slice(&client_id.to_be_bytes());
        offset += 4;

        // Reserved (16 bytes)
        offset += 16;

        // Create header with keepalive
        let header = Header {
            keepalive: Some(Keepalive {
                timestamp: 1_703_123_456_789,
                interval:  Some(30),
            }),
            ..Default::default()
        };

        let header_data = rmp_serde::to_vec(&header).unwrap();
        let header_len = u32::try_from(header_data.len()).unwrap_or(u32::MAX);

        // Header length
        buffer[offset..offset + 4].copy_from_slice(&header_len.to_be_bytes());
        offset += 4;

        // Header data
        buffer[offset..offset + header_data.len()].copy_from_slice(&header_data);
        offset += header_data.len();

        // Payload length
        let payload_len = 0u64;
        buffer[offset..offset + 8].copy_from_slice(&payload_len.to_be_bytes());
        offset += 8;

        buffer.truncate(offset);
        buffer
    }

    #[test]
    fn test_message_type_conversion() {
        assert_eq!(MessageType::try_from(0).unwrap(), MessageType::Join);
        assert_eq!(MessageType::try_from(1).unwrap(), MessageType::Request);
        assert_eq!(MessageType::try_from(2).unwrap(), MessageType::Response);
        assert_eq!(MessageType::try_from(3).unwrap(), MessageType::Notification);
        assert_eq!(MessageType::try_from(4).unwrap(), MessageType::Broadcast);
        assert_eq!(MessageType::try_from(5).unwrap(), MessageType::Topic);
        assert_eq!(MessageType::try_from(6).unwrap(), MessageType::Subscribe);
        assert_eq!(MessageType::try_from(7).unwrap(), MessageType::Unsubscribe);
        assert_eq!(MessageType::try_from(8).unwrap(), MessageType::Ping);
        assert_eq!(MessageType::try_from(9).unwrap(), MessageType::Pong);

        assert!(MessageType::try_from(10).is_err());
    }

    #[test]
    fn test_valid_broadcast_message() {
        let buffer = create_valid_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Broadcast);
        assert_eq!(message.client_id(), 1234);
        assert_eq!(message.header_len(), 0);
        assert_eq!(message.payload_len(), 0);
    }

    #[test]
    fn test_ping_message() {
        let buffer = create_ping_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Ping);
        assert_eq!(message.client_id(), 1234);

        let header = message.header().unwrap();
        assert!(header.keepalive.is_some());
        let keepalive = header.keepalive.unwrap();
        assert_eq!(keepalive.timestamp, 1_703_123_456_789);
        assert_eq!(keepalive.interval, Some(30));
    }

    #[test]
    fn test_invalid_version() {
        let mut buffer = create_valid_message_bytes();
        buffer[VERSION_OFFSET] = 2; // Invalid version

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidVersion)
        ));
    }

    #[test]
    fn test_incomplete_header() {
        let buffer = vec![0u8; 10]; // Too small for base header
        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::IncompleteHeader)
        ));
    }

    #[test]
    fn test_ping_without_keepalive_invalid() {
        let mut buffer = vec![0u8; 1024];
        let mut offset = 0;

        // Version
        buffer[offset] = 1;
        offset += 1;

        // Type (Ping)
        buffer[offset] = MessageType::Ping as u8;
        offset += 1;

        // Client ID
        let client_id = 1234u32;
        buffer[offset..offset + 4].copy_from_slice(&client_id.to_be_bytes());
        offset += 4;

        // Reserved (16 bytes)
        offset += 16;

        // Empty header (invalid for PING)
        let header_len = 0u32;
        buffer[offset..offset + 4].copy_from_slice(&header_len.to_be_bytes());
        offset += 4;

        // Payload length
        let payload_len = 0u64;
        buffer[offset..offset + 8].copy_from_slice(&payload_len.to_be_bytes());
        offset += 8;

        buffer.truncate(offset);

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidHeaderData(_))
        ));
    }
}
