use bytes::{BufMut, BytesMut};
use serde::Serialize;
use thiserror::Error;

// Re-export types from decode module
pub use crate::decode::{Header, MessageType, Reqrep, Routing};

#[derive(Debug, Error)]
pub enum MessageEncodeError {
    #[error("Invalid message configuration")]
    InvalidConfiguration,
    #[error("Header serialization error: {0}")]
    HeaderSerializeError(#[from] rmp_serde::encode::Error),
    #[error("Payload serialization error")]
    PayloadSerializeError,
    #[error("Message too large")]
    MessageTooLarge,
    #[error("Invalid routing data")]
    InvalidRoutingData,
    #[error("Missing required header data")]
    MissingHeaderData,
}

/// Protocol V1 Message Builder
pub struct MessageBuilder {
    /// Protocol version (always 1)
    version:   u8,
    /// Message type
    msg_type:  MessageType,
    /// Client identifier
    client_id: u32,
    /// Header data
    header:    Header,
    /// Payload data (pre-serialized)
    payload:   Vec<u8>,
}

// Protocol V1 constants
const PROTOCOL_VERSION: u8 = 1;
const RESERVED_SIZE: usize = 16;
const BASE_HEADER_SIZE: usize = 26; // Version(1) + Type(1) + ClientID(4) + Reserved(16) + HeaderLength(4)
const PAYLOAD_LEN_SIZE: usize = 8;

impl MessageBuilder {
    /// Create a new message builder
    #[must_use]
    pub fn new(msg_type: MessageType, client_id: u32) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type,
            client_id,
            header: Header {
                routing: None,
                reqrep:  None,
            },
            payload: Vec::new(),
        }
    }

    /// Create a request message
    #[must_use]
    pub fn request(client_id: u32, request_id: String) -> Self {
        let mut builder = Self::new(MessageType::Request, client_id);
        builder.header.reqrep = Some(Reqrep::Request(request_id));
        builder
    }

    /// Create a response message
    #[must_use]
    pub fn response(client_id: u32, correlation_id: String) -> Self {
        let mut builder = Self::new(MessageType::Response, client_id);
        builder.header.reqrep = Some(Reqrep::Correlation(correlation_id));
        builder
    }

    /// Create a notification message
    #[must_use]
    pub fn notification(client_id: u32) -> Self {
        Self::new(MessageType::Notification, client_id)
    }

    /// Create a broadcast message
    #[must_use]
    pub fn broadcast(client_id: u32) -> Self {
        Self::new(MessageType::Broadcast, client_id)
    }

    /// Add routing information
    #[must_use]
    pub fn with_routing(mut self, routing: Vec<Routing>) -> Self {
        self.header.routing = Some(routing);
        self
    }

    /// Add single route
    #[must_use]
    pub fn with_route(mut self, client_id: u32, path: String) -> Self {
        let routing = match self.header.routing.take() {
            Some(mut routes) => {
                routes.push(Routing(client_id, path));
                routes
            }
            None => vec![Routing(client_id, path)],
        };
        self.header.routing = Some(routing);
        self
    }

    /// Set payload from serializable data
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized
    pub fn with_payload<T: Serialize>(mut self, payload: &T) -> Result<Self, MessageEncodeError> {
        self.payload =
            rmp_serde::to_vec(payload).map_err(|_| MessageEncodeError::PayloadSerializeError)?;
        Ok(self)
    }

    /// Set payload from raw bytes
    #[must_use]
    pub fn with_raw_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Validate message configuration according to protocol rules
    fn validate(&self) -> Result<(), MessageEncodeError> {
        match self.msg_type {
            // Broadcast messages must not have routing information
            MessageType::Broadcast => {
                if self.header.routing.is_some() {
                    return Err(MessageEncodeError::InvalidRoutingData);
                }
            }
            // All other message types require routing information
            MessageType::Request | MessageType::Response | MessageType::Notification => {
                if self.header.routing.is_none() {
                    return Err(MessageEncodeError::InvalidRoutingData);
                }
            }
        }

        // Request and Response messages require reqrep correlation data
        match self.msg_type {
            MessageType::Request | MessageType::Response => {
                if self.header.reqrep.is_none() {
                    return Err(MessageEncodeError::MissingHeaderData);
                }
            }
            _ => {}
        }

        Ok(())
    }

    /// Calculate the total size needed for the encoded message
    ///
    /// # Errors
    ///
    /// Returns an error if the message is too large or has invalid structure
    pub fn calculate_size(&self) -> Result<usize, MessageEncodeError> {
        // Serialize header to get its size
        let header_bytes = rmp_serde::to_vec(&self.header)?;
        let header_len = header_bytes.len();
        let payload_len = self.payload.len();

        // Check for overflow
        if header_len > u32::MAX as usize {
            return Err(MessageEncodeError::MessageTooLarge);
        }
        if payload_len > usize::try_from(u64::MAX).unwrap_or(usize::MAX) {
            return Err(MessageEncodeError::MessageTooLarge);
        }

        Ok(BASE_HEADER_SIZE + header_len + PAYLOAD_LEN_SIZE + payload_len)
    }

    /// Build the message into a BytesMut buffer
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the message is too large
    pub fn build(self) -> Result<BytesMut, MessageEncodeError> {
        // Validate before building
        self.validate()?;

        // Serialize header
        let header_bytes = rmp_serde::to_vec(&self.header)?;
        let header_len = header_bytes.len();
        let payload_len = self.payload.len();

        // Check size limits
        if header_len > u32::MAX as usize {
            return Err(MessageEncodeError::MessageTooLarge);
        }
        if payload_len > usize::try_from(u64::MAX).unwrap_or(usize::MAX) {
            return Err(MessageEncodeError::MessageTooLarge);
        }

        // Calculate total size and allocate buffer
        let total_size = BASE_HEADER_SIZE + header_len + PAYLOAD_LEN_SIZE + payload_len;
        let mut buffer = BytesMut::with_capacity(total_size);

        // Write base header
        buffer.put_u8(self.version); // Version
        buffer.put_u8(self.msg_type as u8); // Type
        buffer.put_u32(self.client_id); // ClientID
        buffer.put_bytes(0, RESERVED_SIZE); // Reserved (16 zero bytes)
        buffer.put_u32(u32::try_from(header_len).map_err(|_| MessageEncodeError::MessageTooLarge)?); // HeaderLength

        // Write variable header
        buffer.put_slice(&header_bytes);

        // Write payload length and payload
        buffer.put_u64(payload_len as u64); // PayloadLength
        buffer.put_slice(&self.payload); // Payload

        Ok(buffer)
    }

    /// Build the message into a Vec<u8>
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the message is too large
    pub fn build_vec(self) -> Result<Vec<u8>, MessageEncodeError> {
        Ok(self.build()?.to_vec())
    }

    /// Build the message and write it to an existing buffer
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the message is too large
    pub fn build_into(self, buffer: &mut BytesMut) -> Result<(), MessageEncodeError> {
        let message_bytes = self.build()?;
        buffer.extend_from_slice(&message_bytes);
        Ok(())
    }
}

/// Convenience functions for quick message creation
impl MessageBuilder {
    /// Create a simple request with routing to a single endpoint
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized or the message is
    /// invalid
    pub fn simple_request(
        client_id: u32,
        target_client: u32,
        path: String,
        request_id: String,
        payload: &impl Serialize,
    ) -> Result<BytesMut, MessageEncodeError> {
        Self::request(client_id, request_id)
            .with_route(target_client, path)
            .with_payload(payload)?
            .build()
    }

    /// Create a simple response
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized or the message is
    /// invalid
    pub fn simple_response(
        client_id: u32,
        target_client: u32,
        path: String,
        correlation_id: String,
        payload: &impl Serialize,
    ) -> Result<BytesMut, MessageEncodeError> {
        Self::response(client_id, correlation_id)
            .with_route(target_client, path)
            .with_payload(payload)?
            .build()
    }

    /// Create a simple notification
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized or the message is
    /// invalid
    pub fn simple_notification(
        client_id: u32,
        target_client: u32,
        path: String,
        payload: &impl Serialize,
    ) -> Result<BytesMut, MessageEncodeError> {
        Self::notification(client_id)
            .with_route(target_client, path)
            .with_payload(payload)?
            .build()
    }

    /// Create a simple broadcast
    ///
    /// # Errors
    ///
    /// Returns an error if the payload cannot be serialized or the message is
    /// invalid
    pub fn simple_broadcast(
        client_id: u32,
        payload: &impl Serialize,
    ) -> Result<BytesMut, MessageEncodeError> {
        Self::broadcast(client_id).with_payload(payload)?.build()
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct TestPayload {
        message: String,
        value:   i32,
    }

    #[test]
    fn test_broadcast_message() {
        let payload = TestPayload {
            message: "Hello World".to_string(),
            value:   42,
        };

        let result = MessageBuilder::simple_broadcast(1234, &payload);
        assert!(result.is_ok());

        let message_bytes = result.unwrap();
        assert!(!message_bytes.is_empty());
    }

    #[test]
    fn test_request_message() {
        let payload = TestPayload {
            message: "Request data".to_string(),
            value:   100,
        };

        let result = MessageBuilder::simple_request(
            1234,
            5678,
            "/api/test".to_string(),
            "req_123".to_string(),
            &payload,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_broadcast_with_routing() {
        let result = MessageBuilder::broadcast(1234)
            .with_route(5678, "/invalid".to_string())
            .with_raw_payload(vec![1, 2, 3])
            .build();

        assert!(matches!(
            result,
            Err(MessageEncodeError::InvalidRoutingData)
        ));
    }

    #[test]
    fn test_request_without_reqrep() {
        let result = MessageBuilder::new(MessageType::Request, 1234)
            .with_route(5678, "/test".to_string())
            .with_raw_payload(vec![1, 2, 3])
            .build();

        assert!(matches!(result, Err(MessageEncodeError::MissingHeaderData)));
    }
}
