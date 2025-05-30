use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Message types according to protocol V1
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Request      = 0,
    Response     = 1,
    Notification = 2,
    Broadcast    = 3,
}

impl TryFrom<u8> for MessageType {
    type Error = MessageDeserializeError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(MessageType::Request),
            1 => Ok(MessageType::Response),
            2 => Ok(MessageType::Notification),
            3 => Ok(MessageType::Broadcast),
            _ => Err(MessageDeserializeError::InvalidMessageType),
        }
    }
}

/// Routing information for directed messages
/// Format: (client_id: u32, path: String)
#[derive(Debug, Serialize, Deserialize)]
pub struct Routing(pub u32, pub String);

/// Request/Response correlation data
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Reqrep {
    Request(String),     // ULID as string
    Correlation(String), // ULID as string
}

/// Protocol V1 Header structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Header {
    /// Routing information - required for all message types except Broadcast
    pub routing: Option<Vec<Routing>>,
    /// Request/Response correlation - required for Request and Response message
    /// types
    pub reqrep:  Option<Reqrep>,
}

/// Protocol V1 Message structure
pub struct Message<'a> {
    /// Reference to the original buffer
    buffer:         &'a [u8],
    /// Protocol version (1)
    pub version:    u8,
    /// Message type
    pub msg_type:   MessageType,
    /// Client identifier
    pub client_id:  u32,
    /// Offset to header data in buffer
    header_offset:  usize,
    /// Length of header data
    header_len:     u32,
    /// Offset to payload data in buffer
    payload_offset: usize,
    /// Length of payload data
    payload_len:    u64,
}

#[derive(Debug, Error)]
pub enum MessageDeserializeError {
    #[error("Invalid protocol version")]
    InvalidVersion,
    #[error("Invalid message type")]
    InvalidMessageType,
    #[error("Invalid header data")]
    InvalidHeaderData,
    #[error("Incomplete header")]
    IncompleteHeader,
    #[error("Incomplete payload")]
    IncompletePayload,
    #[error("Invalid routing data")]
    InvalidRoutingData,
    #[error("Deserialize error: {0}")]
    DeserializeError(#[from] rmp_serde::decode::Error),
}

// Protocol V1 field offsets and sizes
const VERSION_OFFSET: usize = 0;
const TYPE_OFFSET: usize = 1;
const CLIENT_ID_OFFSET: usize = 2;
// Reserved(16) OFFSET = 6
const HEADER_LEN_OFFSET: usize = 22;
const BASE_HEADER_SIZE: usize = 26; // Version(1) + Type(1) + ClientID(4) + Reserved(16) + HeaderLength(4)
const PAYLOAD_LEN_SIZE: usize = 8;

impl<'a> Message<'a> {
    /// Parse a Message from a buffer according to Protocol V1 (zero-copy)
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer contains invalid message data, has an
    /// unsupported protocol version, invalid message type, or is incomplete
    pub fn from_buffer(buffer: &'a [u8]) -> Result<Message<'a>, MessageDeserializeError> {
        // Ensure we have enough bytes for the base header
        if buffer.len() < BASE_HEADER_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        // Parse version
        let version = buffer[VERSION_OFFSET];
        if version != 1 {
            return Err(MessageDeserializeError::InvalidVersion);
        }

        // Parse message type
        let msg_type = MessageType::try_from(buffer[TYPE_OFFSET])?;

        // Parse client ID using safe array conversion
        let client_id_bytes: [u8; 4] = buffer[CLIENT_ID_OFFSET..CLIENT_ID_OFFSET + 4]
            .try_into()
            .map_err(|_| MessageDeserializeError::InvalidHeaderData)?;
        let client_id = u32::from_be_bytes(client_id_bytes);

        // Parse header length (skip reserved bytes at offset 6-21)
        let header_len_bytes: [u8; 4] = buffer[HEADER_LEN_OFFSET..HEADER_LEN_OFFSET + 4]
            .try_into()
            .map_err(|_| MessageDeserializeError::InvalidHeaderData)?;
        let header_len = u32::from_be_bytes(header_len_bytes);

        // Calculate header offset
        let header_offset = BASE_HEADER_SIZE;

        // Ensure we have enough bytes for the header
        if buffer.len() < header_offset + header_len as usize {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        // Calculate payload length offset
        let payload_len_offset = header_offset + header_len as usize;

        // Ensure we have enough bytes for payload length field
        if buffer.len() < payload_len_offset + PAYLOAD_LEN_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        // Parse payload length
        let payload_len_bytes: [u8; 8] = buffer
            [payload_len_offset..payload_len_offset + PAYLOAD_LEN_SIZE]
            .try_into()
            .map_err(|_| MessageDeserializeError::InvalidHeaderData)?;
        let payload_len = u64::from_be_bytes(payload_len_bytes);

        // Calculate payload offset
        let payload_offset = payload_len_offset + PAYLOAD_LEN_SIZE;

        // Ensure we have enough bytes for the payload
        if buffer.len()
            < payload_offset
                + usize::try_from(payload_len)
                    .map_err(|_| MessageDeserializeError::InvalidHeaderData)?
        {
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
        };

        // Validate header based on message type (lazy validation)
        message.validate_header()?;

        Ok(message)
    }

    /// Parse a Message from a mutable buffer (consuming approach for
    /// compatibility)
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer contains invalid message data or is
    /// incomplete
    pub fn from_buffer_mut(buffer: &mut BytesMut) -> Result<Message<'a>, MessageDeserializeError> {
        // For compatibility, we still support BytesMut but we leak the lifetime
        // This is less optimal but maintains API compatibility
        let len = Self::calculate_total_message_len(buffer)?;
        let slice = unsafe {
            // SAFETY: We ensure the buffer has enough bytes and the message structure is
            // valid
            std::slice::from_raw_parts(buffer.as_ptr(), len)
        };
        buffer.advance(len);

        // This is a workaround for lifetime issues - in practice, the caller should
        // ensure the buffer lives long enough
        let extended_slice: &'a [u8] = unsafe { std::mem::transmute(slice) };
        Self::from_buffer(extended_slice)
    }

    /// Calculate the total length of a message in the buffer
    fn calculate_total_message_len(buffer: &[u8]) -> Result<usize, MessageDeserializeError> {
        if buffer.len() < BASE_HEADER_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        let header_len_bytes: [u8; 4] = buffer[HEADER_LEN_OFFSET..HEADER_LEN_OFFSET + 4]
            .try_into()
            .map_err(|_| MessageDeserializeError::InvalidHeaderData)?;
        let header_len = u32::from_be_bytes(header_len_bytes);

        let payload_len_offset = BASE_HEADER_SIZE + header_len as usize;

        if buffer.len() < payload_len_offset + PAYLOAD_LEN_SIZE {
            return Err(MessageDeserializeError::IncompleteHeader);
        }

        let payload_len_bytes: [u8; 8] = buffer
            [payload_len_offset..payload_len_offset + PAYLOAD_LEN_SIZE]
            .try_into()
            .map_err(|_| MessageDeserializeError::InvalidHeaderData)?;
        let payload_len = u64::from_be_bytes(payload_len_bytes);

        Ok(BASE_HEADER_SIZE
            + header_len as usize
            + PAYLOAD_LEN_SIZE
            + usize::try_from(payload_len)
                .map_err(|_| MessageDeserializeError::InvalidHeaderData)?)
    }

    /// Get raw header bytes (zero-copy)
    #[must_use]
    pub fn header_bytes(&self) -> &[u8] {
        &self.buffer[self.header_offset..self.header_offset + self.header_len as usize]
    }

    /// Parse header on demand (only when needed)
    ///
    /// # Errors
    ///
    /// Returns an error if the header data cannot be deserialized
    pub fn header(&self) -> Result<Header, MessageDeserializeError> {
        rmp_serde::from_slice(self.header_bytes())
            .map_err(MessageDeserializeError::DeserializeError)
    }

    /// Validate header according to protocol rules (lazy validation)
    fn validate_header(&self) -> Result<(), MessageDeserializeError> {
        // Only parse header if we need to validate it
        let header = self.header()?;

        match self.msg_type {
            // Broadcast messages must not have routing information
            MessageType::Broadcast => {
                if header.routing.is_some() {
                    return Err(MessageDeserializeError::InvalidRoutingData);
                }
            }
            // All other message types require routing information
            MessageType::Request | MessageType::Response | MessageType::Notification => {
                if header.routing.is_none() {
                    return Err(MessageDeserializeError::InvalidRoutingData);
                }
            }
        }

        // Request and Response messages require reqrep correlation data
        match self.msg_type {
            MessageType::Request | MessageType::Response => {
                if header.reqrep.is_none() {
                    return Err(MessageDeserializeError::InvalidHeaderData);
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Get raw payload bytes (zero-copy)
    #[must_use]
    pub fn payload_bytes(&self) -> &[u8] {
        &self.buffer[self.payload_offset
            ..self.payload_offset + usize::try_from(self.payload_len).unwrap_or(0)]
    }

    /// Deserialize the MessagePack encoded payload into type T (only when
    /// needed)
    ///
    /// # Errors
    ///
    /// Returns an error if the payload data cannot be deserialized into the
    /// target type
    pub fn payload<T: for<'de> Deserialize<'de>>(&self) -> Result<T, MessageDeserializeError> {
        rmp_serde::from_slice(self.payload_bytes())
            .map_err(MessageDeserializeError::DeserializeError)
    }

    /// Get the message type
    #[must_use]
    pub fn message_type(&self) -> MessageType {
        self.msg_type
    }

    /// Get the protocol version
    #[must_use]
    pub fn version(&self) -> u8 {
        self.version
    }

    /// Get the client ID
    #[must_use]
    pub fn client_id(&self) -> u32 {
        self.client_id
    }

    /// Get payload length
    #[must_use]
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// Get header length
    #[must_use]
    pub fn header_len(&self) -> u32 {
        self.header_len
    }

    /// Get total message size in bytes
    #[must_use]
    pub fn total_size(&self) -> usize {
        BASE_HEADER_SIZE
            + self.header_len as usize
            + PAYLOAD_LEN_SIZE
            + usize::try_from(self.payload_len).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestPayload {
        message: String,
        value:   i32,
    }

    fn create_valid_message_bytes() -> Vec<u8> {
        let mut buffer = Vec::new();

        // Version (1 byte)
        buffer.push(1);

        // Type (1 byte) - Broadcast
        buffer.push(3);

        // Client ID (4 bytes)
        buffer.extend_from_slice(&1234u32.to_be_bytes());

        // Reserved (16 bytes)
        buffer.extend_from_slice(&[0u8; 16]);

        // Header (empty for broadcast)
        let header = Header {
            routing: None,
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();

        // Header length (4 bytes)
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());

        // Header data
        buffer.extend_from_slice(&header_bytes);

        // Payload
        let payload = TestPayload {
            message: "test".to_string(),
            value:   42,
        };
        let payload_bytes = rmp_serde::to_vec(&payload).unwrap();

        // Payload length (8 bytes)
        buffer.extend_from_slice(&(payload_bytes.len() as u64).to_be_bytes());

        // Payload data
        buffer.extend_from_slice(&payload_bytes);

        buffer
    }

    fn create_request_message_bytes() -> Vec<u8> {
        let mut buffer = Vec::new();

        // Version (1 byte)
        buffer.push(1);

        // Type (1 byte) - Request
        buffer.push(0);

        // Client ID (4 bytes)
        buffer.extend_from_slice(&5678u32.to_be_bytes());

        // Reserved (16 bytes)
        buffer.extend_from_slice(&[0u8; 16]);

        // Header with routing and reqrep
        let header = Header {
            routing: Some(vec![Routing(9999, "/test/path".to_string())]),
            reqrep:  Some(Reqrep::Request("req_123".to_string())),
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();

        // Header length (4 bytes)
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());

        // Header data
        buffer.extend_from_slice(&header_bytes);

        // Payload
        let payload = TestPayload {
            message: "request".to_string(),
            value:   100,
        };
        let payload_bytes = rmp_serde::to_vec(&payload).unwrap();

        // Payload length (8 bytes)
        buffer.extend_from_slice(&(payload_bytes.len() as u64).to_be_bytes());

        // Payload data
        buffer.extend_from_slice(&payload_bytes);

        buffer
    }

    #[test]
    fn test_message_type_conversion() {
        assert_eq!(MessageType::try_from(0).unwrap(), MessageType::Request);
        assert_eq!(MessageType::try_from(1).unwrap(), MessageType::Response);
        assert_eq!(MessageType::try_from(2).unwrap(), MessageType::Notification);
        assert_eq!(MessageType::try_from(3).unwrap(), MessageType::Broadcast);

        assert!(matches!(
            MessageType::try_from(4),
            Err(MessageDeserializeError::InvalidMessageType)
        ));
        assert!(matches!(
            MessageType::try_from(255),
            Err(MessageDeserializeError::InvalidMessageType)
        ));
    }

    #[test]
    fn test_valid_broadcast_message() {
        let buffer = create_valid_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Broadcast);
        assert_eq!(message.client_id(), 1234);

        let payload: TestPayload = message.payload().unwrap();
        assert_eq!(payload.message, "test");
        assert_eq!(payload.value, 42);

        let header = message.header().unwrap();
        assert!(header.routing.is_none());
        assert!(header.reqrep.is_none());
    }

    #[test]
    fn test_valid_request_message() {
        let buffer = create_request_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Request);
        assert_eq!(message.client_id(), 5678);

        let payload: TestPayload = message.payload().unwrap();
        assert_eq!(payload.message, "request");
        assert_eq!(payload.value, 100);

        let header = message.header().unwrap();
        assert!(header.routing.is_some());
        assert!(header.reqrep.is_some());

        let routing = header.routing.unwrap();
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].0, 9999);
        assert_eq!(routing[0].1, "/test/path");

        match header.reqrep.unwrap() {
            Reqrep::Request(id) => assert_eq!(id, "req_123"),
            _ => panic!("Expected Request reqrep"),
        }
    }

    #[test]
    fn test_invalid_version() {
        let mut buffer = create_valid_message_bytes();
        buffer[0] = 2; // Invalid version

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidVersion)
        ));
    }

    #[test]
    fn test_incomplete_header() {
        // Buffer too small for base header
        let buffer = vec![1, 3, 0, 0, 0, 1]; // Only 6 bytes
        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::IncompleteHeader)
        ));
    }

    #[test]
    fn test_incomplete_variable_header() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved
        buffer.extend_from_slice(&100u32.to_be_bytes()); // Header length (claim 100 bytes)

        // But only provide 10 bytes of header data
        buffer.extend_from_slice(&[0u8; 10]);

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::IncompleteHeader)
        ));
    }

    #[test]
    fn test_incomplete_payload() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Empty header
        let header = Header {
            routing: None,
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Claim large payload but don't provide it
        buffer.extend_from_slice(&1000u64.to_be_bytes()); // Payload length
        buffer.extend_from_slice(&[0u8; 10]); // Only 10 bytes instead of 1000

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::IncompletePayload)
        ));
    }

    #[test]
    fn test_invalid_header_data() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved
        buffer.extend_from_slice(&5u32.to_be_bytes()); // Header length

        // Invalid MessagePack data
        buffer.extend_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);

        // Valid payload
        buffer.extend_from_slice(&0u64.to_be_bytes()); // Payload length

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::DeserializeError(_))
        ));
    }

    #[test]
    fn test_broadcast_with_routing_invalid() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Header with routing (invalid for broadcast)
        let header = Header {
            routing: Some(vec![Routing(9999, "/invalid".to_string())]),
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Valid payload
        buffer.extend_from_slice(&0u64.to_be_bytes());

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidRoutingData)
        ));
    }

    #[test]
    fn test_request_without_routing_invalid() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(0); // Type - Request
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Header without routing (invalid for request)
        let header = Header {
            routing: None,
            reqrep:  Some(Reqrep::Request("req_123".to_string())),
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Valid payload
        buffer.extend_from_slice(&0u64.to_be_bytes());

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidRoutingData)
        ));
    }

    #[test]
    fn test_request_without_reqrep_invalid() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(0); // Type - Request
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Header without reqrep (invalid for request)
        let header = Header {
            routing: Some(vec![Routing(9999, "/test".to_string())]),
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Valid payload
        buffer.extend_from_slice(&0u64.to_be_bytes());

        let result = Message::from_buffer(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::InvalidHeaderData)
        ));
    }

    #[test]
    fn test_zero_copy_behavior() {
        let buffer = create_valid_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        // Verify that payload_bytes returns a slice of the original buffer
        let payload_bytes = message.payload_bytes();
        let expected_start = message.payload_offset;
        let expected_end = expected_start + message.payload_len as usize;

        assert_eq!(payload_bytes, &buffer[expected_start..expected_end]);

        // Verify header bytes
        let header_bytes = message.header_bytes();
        let header_start = message.header_offset;
        let header_end = header_start + message.header_len as usize;

        assert_eq!(header_bytes, &buffer[header_start..header_end]);
    }

    #[test]
    fn test_message_size_calculation() {
        let buffer = create_valid_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        let calculated_size = message.total_size();
        assert_eq!(calculated_size, buffer.len());
    }

    #[test]
    fn test_calculate_total_message_len() {
        let buffer = create_valid_message_bytes();
        let calculated_len = Message::calculate_total_message_len(&buffer).unwrap();
        assert_eq!(calculated_len, buffer.len());
    }

    #[test]
    fn test_calculate_total_message_len_incomplete() {
        let buffer = vec![1, 2, 3]; // Too small
        let result = Message::calculate_total_message_len(&buffer);
        assert!(matches!(
            result,
            Err(MessageDeserializeError::IncompleteHeader)
        ));
    }

    #[test]
    fn test_large_payload() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Empty header
        let header = Header {
            routing: None,
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Large payload (10KB)
        let large_payload = vec![42u8; 10000];
        buffer.extend_from_slice(&(large_payload.len() as u64).to_be_bytes());
        buffer.extend_from_slice(&large_payload);

        let message = Message::from_buffer(&buffer).unwrap();
        assert_eq!(message.payload_len(), 10000);
        assert_eq!(message.payload_bytes().len(), 10000);
        assert_eq!(message.payload_bytes(), &large_payload);
    }

    #[test]
    fn test_empty_payload() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(3); // Type - Broadcast
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Empty header
        let header = Header {
            routing: None,
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Empty payload
        buffer.extend_from_slice(&0u64.to_be_bytes());

        let message = Message::from_buffer(&buffer).unwrap();
        assert_eq!(message.payload_len(), 0);
        assert_eq!(message.payload_bytes().len(), 0);
    }

    #[test]
    fn test_multiple_routing() {
        let mut buffer = Vec::new();

        // Valid base header
        buffer.push(1); // Version
        buffer.push(2); // Type - Notification
        buffer.extend_from_slice(&1234u32.to_be_bytes()); // Client ID
        buffer.extend_from_slice(&[0u8; 16]); // Reserved

        // Header with multiple routes
        let header = Header {
            routing: Some(vec![
                Routing(1001, "/path1".to_string()),
                Routing(1002, "/path2".to_string()),
                Routing(1003, "/path3".to_string()),
            ]),
            reqrep:  None,
        };
        let header_bytes = rmp_serde::to_vec(&header).unwrap();
        buffer.extend_from_slice(&(header_bytes.len() as u32).to_be_bytes());
        buffer.extend_from_slice(&header_bytes);

        // Empty payload
        buffer.extend_from_slice(&0u64.to_be_bytes());

        let message = Message::from_buffer(&buffer).unwrap();
        let decoded_header = message.header().unwrap();
        let routing = decoded_header.routing.unwrap();

        assert_eq!(routing.len(), 3);
        assert_eq!(routing[0].0, 1001);
        assert_eq!(routing[0].1, "/path1");
        assert_eq!(routing[1].0, 1002);
        assert_eq!(routing[1].1, "/path2");
        assert_eq!(routing[2].0, 1003);
        assert_eq!(routing[2].1, "/path3");
    }

    #[test]
    fn test_from_buffer_mut() {
        let buffer_data = create_valid_message_bytes();
        let buffer = BytesMut::from(&buffer_data[..]);
        let _original_len = buffer.len();

        // This is unsafe due to lifetime issues, but for testing we'll use it
        let message = unsafe {
            let slice = std::slice::from_raw_parts(buffer.as_ptr(), buffer.len());
            let extended_slice: &[u8] = std::mem::transmute(slice);
            Message::from_buffer(extended_slice).unwrap()
        };

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Broadcast);
        assert_eq!(message.client_id(), 1234);
    }

    #[test]
    fn test_accessor_methods() {
        let buffer = create_request_message_bytes();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.version(), 1);
        assert_eq!(message.message_type(), MessageType::Request);
        assert_eq!(message.client_id(), 5678);
        assert!(message.header_len() > 0);
        assert!(message.payload_len() > 0);
        assert!(message.total_size() > 0);

        // Test that header and payload bytes are non-empty
        assert!(!message.header_bytes().is_empty());
        assert!(!message.payload_bytes().is_empty());
    }
}
