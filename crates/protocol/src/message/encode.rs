use once_cell::sync::OnceCell;

// Re-export core types for backward compatibility
pub use crate::message::types::{
    Auth, Header, Keepalive, MessageDeserializeError, MessageEncodeError, MessageType, Reqrep,
    Routing, StatusCode,
};
use crate::message::{traits::*, types::*};

/// Builder for constructing protocol messages
#[derive(Debug, Clone)]
pub struct MessageBuilder {
    version:     u8,
    msg_type:    MessageType,
    client_id:   u32,
    header:      Header,
    header_data: OnceCell<Vec<u8>>,
    payload:     Vec<u8>,
}

const PROTOCOL_VERSION: u8 = 1;
const RESERVED_SIZE: usize = 16;
const BASE_HEADER_SIZE: usize = 34;

impl MessageBuilder {
    fn get_header_data(&self) -> Result<&Vec<u8>, MessageEncodeError> {
        self.header_data.get_or_try_init(|| {
            if self.is_header_empty() {
                return Ok(Vec::new());
            }
            rmp_serde::to_vec(&self.header)
                .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))
        })
    }

    /// Create a new message builder with the specified type and client ID
    #[must_use]
    pub fn new(msg_type: MessageType, client_id: u32) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            msg_type,
            client_id,
            header: Header::default(),
            header_data: OnceCell::new(),
            payload: Vec::new(),
        }
    }

    /// Create a request message builder
    #[must_use]
    pub fn request(client_id: u32) -> Self {
        Self::new(MessageType::Request, client_id)
    }

    /// Create a response message builder
    #[must_use]
    pub fn response(client_id: u32) -> Self {
        Self::new(MessageType::Response, client_id)
    }

    /// Create a notification message builder
    #[must_use]
    pub fn notification(client_id: u32) -> Self {
        Self::new(MessageType::Notification, client_id)
    }

    /// Create a join message builder (typically with CLIENT_ID_UNASSIGNED)
    #[must_use]
    pub fn join() -> Self {
        Self::new(MessageType::Join, CLIENT_ID_UNASSIGNED)
    }

    /// Create a join message builder with specific client ID
    #[must_use]
    pub fn join_with_client_id(client_id: u32) -> Self {
        Self::new(MessageType::Join, client_id)
    }

    /// Create a broadcast message builder
    #[must_use]
    pub fn broadcast(client_id: u32) -> Self {
        Self::new(MessageType::Broadcast, client_id)
    }

    /// Create a topic message builder
    #[must_use]
    pub fn topic(client_id: u32, topic: String) -> Self {
        let mut builder = Self::new(MessageType::Topic, client_id);
        builder.header.topic = Some(topic);
        builder
    }

    /// Create a subscribe message builder
    #[must_use]
    pub fn subscribe(client_id: u32, topic: String) -> Self {
        let mut builder = Self::new(MessageType::Subscribe, client_id);
        builder.header.topic = Some(topic);
        builder
    }

    /// Create an unsubscribe message builder
    #[must_use]
    pub fn unsubscribe(client_id: u32, topic: String) -> Self {
        let mut builder = Self::new(MessageType::Unsubscribe, client_id);
        builder.header.topic = Some(topic);
        builder
    }

    /// Create a ping message builder
    #[must_use]
    pub fn ping(client_id: u32, timestamp: u64) -> Self {
        let mut builder = Self::new(MessageType::Ping, client_id);
        builder.header.keepalive = Some(Keepalive {
            timestamp,
            interval: None,
        });
        builder
    }

    /// Create a ping message builder with interval suggestion
    #[must_use]
    pub fn ping_with_interval(client_id: u32, timestamp: u64, interval: u32) -> Self {
        let mut builder = Self::new(MessageType::Ping, client_id);
        builder.header.keepalive = Some(Keepalive {
            timestamp,
            interval: Some(interval),
        });
        builder
    }

    /// Create a pong message builder
    #[must_use]
    pub fn pong(client_id: u32, timestamp: u64) -> Self {
        let mut builder = Self::new(MessageType::Pong, client_id);
        builder.header.keepalive = Some(Keepalive {
            timestamp,
            interval: None,
        });
        builder
    }

    /// Add routing information
    #[must_use]
    pub fn with_routing(mut self, routing: Vec<Routing>) -> Self {
        self.header.routing = Some(routing);
        self
    }

    /// Add a single route
    #[must_use]
    pub fn with_route(mut self, client_id: u32, path: String) -> Self {
        let route = Routing { client_id, path };
        match self.header.routing {
            Some(ref mut routes) => routes.push(route),
            None => self.header.routing = Some(vec![route]),
        }
        self
    }

    /// Set the payload from a serializable object
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError::PayloadSerializeError` if the payload
    /// cannot be serialized to MessagePack format.
    pub fn with_payload<T: serde::Serialize>(
        mut self,
        payload: &T,
    ) -> Result<Self, MessageEncodeError> {
        self.payload = rmp_serde::to_vec(payload)
            .map_err(|e| MessageEncodeError::PayloadSerializeError(e.to_string()))?;
        Ok(self)
    }

    /// Set the payload from raw bytes
    #[must_use]
    pub fn with_raw_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Add authentication information
    #[must_use]
    pub fn with_auth(mut self, auth: Auth) -> Self {
        self.header.auth = Some(auth);
        self
    }

    /// Add status code
    #[must_use]
    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.header.status = Some(status);
        self
    }

    /// Set topic
    #[must_use]
    pub fn with_topic(mut self, topic: String) -> Self {
        self.header.topic = Some(topic);
        self
    }

    /// Set request/response correlation
    #[must_use]
    pub fn with_reqrep(mut self, reqrep: Reqrep) -> Self {
        self.header.reqrep = Some(reqrep);
        self
    }

    /// Set keepalive information
    #[must_use]
    pub fn with_keepalive(mut self, keepalive: Keepalive) -> Self {
        self.header.keepalive = Some(keepalive);
        self
    }
}

impl MessageEncode for MessageBuilder {
    fn calculate_size(&self) -> Result<usize, MessageEncodeError> {
        let header_data = self.get_header_data()?;

        let total_size = BASE_HEADER_SIZE + header_data.len() + self.payload.len();

        if total_size > crate::MAX_MESSAGE_SIZE {
            return Err(MessageEncodeError::MessageTooLarge(total_size));
        }

        Ok(total_size)
    }

    fn build_into(&self, buffer: &mut [u8]) -> Result<usize, MessageEncodeError> {
        // Validate first
        self.validate()?;

        // Handle empty headers specially
        let header_data = self.get_header_data()?;

        let total_size = BASE_HEADER_SIZE + header_data.len() + self.payload.len();

        if buffer.len() < total_size {
            return Err(MessageEncodeError::MessageTooLarge(total_size));
        }

        let mut offset = 0;

        // Version (1 byte)
        buffer[offset] = self.version;
        offset += 1;

        // Type (1 byte)
        buffer[offset] = self.msg_type as u8;
        offset += 1;

        // Client ID (4 bytes, big-endian)
        buffer[offset..offset + 4].copy_from_slice(&self.client_id.to_be_bytes());
        offset += 4;

        // Reserved (16 bytes of zeros)
        buffer[offset..offset + RESERVED_SIZE].fill(0);
        offset += RESERVED_SIZE;

        // Header length (4 bytes, big-endian)
        let header_len = u32::try_from(header_data.len())
            .map_err(|_| MessageEncodeError::MessageTooLarge(header_data.len()))?;
        buffer[offset..offset + 4].copy_from_slice(&header_len.to_be_bytes());
        offset += 4;

        // Header data
        buffer[offset..offset + header_data.len()].copy_from_slice(header_data);
        offset += header_data.len();

        // Payload length (8 bytes, big-endian)
        let payload_len = self.payload.len() as u64;
        buffer[offset..offset + 8].copy_from_slice(&payload_len.to_be_bytes());
        offset += 8;

        // Payload data
        buffer[offset..offset + self.payload.len()].copy_from_slice(&self.payload);
        offset += self.payload.len();

        Ok(offset)
    }
}

impl MessageValidate for MessageBuilder {
    fn validate(&self) -> Result<(), MessageEncodeError> {
        match self.msg_type {
            MessageType::Join => self.validate_join_message(),
            MessageType::Request | MessageType::Response => {
                self.validate_request_response_message()
            }
            MessageType::Notification => self.validate_notification_message(),
            MessageType::Broadcast => self.validate_broadcast_message(),
            MessageType::Topic => self.validate_topic_message(),
            MessageType::Subscribe | MessageType::Unsubscribe => {
                self.validate_subscription_message()
            }
            MessageType::Ping | MessageType::Pong => self.validate_keepalive_message(),
        }
    }
}

impl MessageBuilder {
    fn validate_join_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.routing.is_some()
            || self.header.reqrep.is_some()
            || self.header.topic.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "JOIN messages cannot have routing, reqrep, topic, or keepalive fields".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_request_response_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.routing.is_none() || self.header.reqrep.is_none() {
            return Err(MessageEncodeError::MissingHeaderData(
                "Request/Response messages require routing and reqrep fields".to_string(),
            ));
        }
        if self.header.topic.is_some()
            || self.header.auth.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Request/Response messages cannot have topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        // Validate routing has exactly one entry
        if let Some(ref routing) = self.header.routing {
            if routing.len() != 1 {
                return Err(MessageEncodeError::InvalidRoutingData(
                    "Request/Response messages must have exactly one routing entry".to_string(),
                ));
            }
        }
        Ok(())
    }

    fn validate_notification_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.routing.is_none() {
            return Err(MessageEncodeError::MissingHeaderData(
                "Notification messages require routing field".to_string(),
            ));
        }
        if self.header.reqrep.is_some()
            || self.header.topic.is_some()
            || self.header.auth.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Notification messages cannot have reqrep, topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_broadcast_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.routing.is_some()
            || self.header.reqrep.is_some()
            || self.header.topic.is_some()
            || self.header.auth.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Broadcast messages cannot have routing, reqrep, topic, auth, or keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_topic_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.topic.is_none() {
            return Err(MessageEncodeError::MissingHeaderData(
                "Topic messages require topic field".to_string(),
            ));
        }
        if self.header.reqrep.is_some()
            || self.header.auth.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Topic messages cannot have reqrep, auth, or keepalive fields".to_string(),
            ));
        }
        Ok(())
    }

    fn validate_subscription_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.topic.is_none() {
            return Err(MessageEncodeError::MissingHeaderData(
                "Subscribe/Unsubscribe messages require topic field".to_string(),
            ));
        }
        if self.header.routing.is_some()
            || self.header.reqrep.is_some()
            || self.header.status.is_some()
            || self.header.auth.is_some()
            || self.header.keepalive.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Subscribe/Unsubscribe messages cannot have routing, reqrep, status, auth, or \
                 keepalive fields"
                    .to_string(),
            ));
        }
        Ok(())
    }

    fn validate_keepalive_message(&self) -> Result<(), MessageEncodeError> {
        if self.header.keepalive.is_none() {
            return Err(MessageEncodeError::MissingHeaderData(
                "Ping/Pong messages require keepalive field".to_string(),
            ));
        }
        if self.header.routing.is_some()
            || self.header.reqrep.is_some()
            || self.header.topic.is_some()
            || self.header.status.is_some()
            || self.header.auth.is_some()
        {
            return Err(MessageEncodeError::InvalidConfiguration(
                "Ping/Pong messages cannot have routing, reqrep, topic, status, or auth fields"
                    .to_string(),
            ));
        }
        // Additional validation for PONG: ensure no interval
        if self.msg_type == MessageType::Pong {
            if let Some(ref keepalive) = self.header.keepalive {
                if keepalive.interval.is_some() {
                    return Err(MessageEncodeError::InvalidConfiguration(
                        "PONG messages cannot have interval in keepalive field".to_string(),
                    ));
                }
            }
        }
        Ok(())
    }
}

impl HeaderAccess for MessageBuilder {
    fn header(&self) -> &Header {
        &self.header
    }

    fn header_mut(&mut self) -> &mut Header {
        &mut self.header
    }

    fn set_header(&mut self, header: Header) {
        self.header = header;
    }
}

impl PayloadAccess for MessageBuilder {
    fn payload_bytes(&self) -> &[u8] {
        &self.payload
    }

    fn set_payload_bytes(&mut self, payload: Vec<u8>) {
        self.payload = payload;
    }

    fn payload<T>(&self) -> Result<T, MessageDeserializeError>
    where
        T: for<'de> serde::Deserialize<'de>,
    {
        if self.payload.is_empty() {
            return Err(MessageDeserializeError::IncompletePayload);
        }

        rmp_serde::from_slice(&self.payload)
            .map_err(|e| MessageDeserializeError::DeserializeError(e.to_string()))
    }

    fn set_payload<T>(&mut self, payload: &T) -> Result<(), MessageEncodeError>
    where
        T: serde::Serialize,
    {
        self.payload = rmp_serde::to_vec(payload)
            .map_err(|e| MessageEncodeError::PayloadSerializeError(e.to_string()))?;
        Ok(())
    }
}

// Convenience methods for common message patterns
impl MessageBuilder {
    /// Create a simple request message
    #[must_use]
    pub fn simple_request(
        client_id: u32,
        target_client_id: u32,
        path: String,
        request_id: String,
    ) -> Self {
        Self::request(client_id)
            .with_route(target_client_id, path)
            .with_reqrep(Reqrep::Request { id: request_id })
    }

    /// Create a simple response message
    #[must_use]
    pub fn simple_response(
        client_id: u32,
        target_client_id: u32,
        path: String,
        request_id: String,
        status: StatusCode,
    ) -> Self {
        Self::response(client_id)
            .with_route(target_client_id, path)
            .with_reqrep(Reqrep::Correlation { id: request_id })
            .with_status(status)
    }

    /// Create a simple notification message
    #[must_use]
    pub fn simple_notification(client_id: u32, target_client_id: u32, path: String) -> Self {
        Self::notification(client_id).with_route(target_client_id, path)
    }

    /// Create a simple broadcast message
    #[must_use]
    pub fn simple_broadcast(client_id: u32) -> Self {
        Self::broadcast(client_id)
    }

    /// Create a simple join message without authentication
    #[must_use]
    pub fn simple_join() -> Self {
        Self::join()
    }

    /// Create a simple join message with token authentication
    #[must_use]
    pub fn simple_join_with_token(token: String) -> Self {
        Self::join().with_auth(Auth::Token { token })
    }

    /// Create a simple topic message for all subscribers
    #[must_use]
    pub fn simple_topic(client_id: u32, topic: String) -> Self {
        Self::topic(client_id, topic)
    }

    /// Create a simple subscribe message
    #[must_use]
    pub fn simple_subscribe(client_id: u32, topic: String) -> Self {
        Self::subscribe(client_id, topic)
    }

    /// Create a simple unsubscribe message
    #[must_use]
    pub fn simple_unsubscribe(client_id: u32, topic: String) -> Self {
        Self::unsubscribe(client_id, topic)
    }

    /// Create a simple ping with default interval
    #[must_use]
    pub fn simple_ping(client_id: u32) -> Self {
        let timestamp = u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis(),
        )
        .unwrap_or(0);
        Self::ping_with_interval(client_id, timestamp, DEFAULT_KEEPALIVE_INTERVAL)
    }

    /// Create a simple pong response
    #[must_use]
    pub fn simple_pong(client_id: u32, ping_timestamp: u64) -> Self {
        Self::pong(client_id, ping_timestamp)
    }

    /// Build the message into a new Vec<u8>
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if the message validation fails or if
    /// serialization fails.
    pub fn build_vec(&self) -> Result<Vec<u8>, MessageEncodeError> {
        let size = self.calculate_size()?;
        let mut buffer = vec![0u8; size];
        self.build_into(&mut buffer)?;
        Ok(buffer)
    }

    /// Calculate the size needed for the encoded message
    ///
    /// # Errors
    ///
    /// Returns `MessageEncodeError` if header serialization fails or if the
    /// message would be too large.
    pub fn calculate_size(&self) -> Result<usize, MessageEncodeError> {
        let header_data = if self.is_header_empty() {
            Vec::new()
        } else {
            rmp_serde::to_vec(&self.header)
                .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))?
        };

        let total_size = BASE_HEADER_SIZE + header_data.len() + self.payload.len();

        if total_size > crate::MAX_MESSAGE_SIZE {
            return Err(MessageEncodeError::MessageTooLarge(total_size));
        }

        Ok(total_size)
    }

    /// Check if the header is effectively empty (all fields are None)
    fn is_header_empty(&self) -> bool {
        self.header.routing.is_none()
            && self.header.reqrep.is_none()
            && self.header.topic.is_none()
            && self.header.status.is_none()
            && self.header.auth.is_none()
            && self.header.keepalive.is_none()
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::message::decode::Message;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        message: String,
        value:   u32,
    }

    #[test]
    fn test_broadcast_message() {
        let builder = MessageBuilder::simple_broadcast(1234);
        let buffer = builder.build_vec().unwrap();

        let message = Message::from_buffer(&buffer).unwrap();
        assert_eq!(message.message_type(), MessageType::Broadcast);
        assert_eq!(message.client_id(), 1234);
    }

    #[test]
    fn test_ping_pong_messages() {
        let timestamp = 1_703_123_456_789u64;

        // Test PING
        let ping_builder = MessageBuilder::ping_with_interval(1234, timestamp, 30);
        let ping_buffer = ping_builder.build_vec().unwrap();

        let ping_message = Message::from_buffer(&ping_buffer).unwrap();
        assert_eq!(ping_message.message_type(), MessageType::Ping);
        assert_eq!(ping_message.client_id(), 1234);

        let ping_header = ping_message.header().unwrap();
        assert!(ping_header.keepalive.is_some());
        let keepalive = ping_header.keepalive.unwrap();
        assert_eq!(keepalive.timestamp, timestamp);
        assert_eq!(keepalive.interval, Some(30));

        // Test PONG
        let response_builder = MessageBuilder::pong(5678, timestamp);
        let response_buffer = response_builder.build_vec().unwrap();

        let response_message = Message::from_buffer(&response_buffer).unwrap();
        assert_eq!(response_message.message_type(), MessageType::Pong);
        assert_eq!(response_message.client_id(), 5678);

        let response_header = response_message.header().unwrap();
        assert!(response_header.keepalive.is_some());
        let response_keepalive = response_header.keepalive.unwrap();
        assert_eq!(response_keepalive.timestamp, timestamp);
        assert_eq!(response_keepalive.interval, None);
    }

    #[test]
    fn test_request_message() {
        let builder = MessageBuilder::simple_request(
            1234,
            5678,
            "/api/test".to_string(),
            "req_12345".to_string(),
        );

        let buffer = builder.build_vec().unwrap();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.message_type(), MessageType::Request);
        assert_eq!(message.client_id(), 1234);

        let header = message.header().unwrap();
        assert!(header.routing.is_some());
        assert!(header.reqrep.is_some());

        let routing = header.routing.unwrap();
        assert_eq!(routing.len(), 1);
        assert_eq!(routing[0].client_id, 5678);
        assert_eq!(routing[0].path, "/api/test");
    }

    #[test]
    fn test_invalid_ping_without_keepalive() {
        let builder = MessageBuilder::new(MessageType::Ping, 1234);
        // Don't set keepalive - should fail validation

        let result = builder.build_vec();
        assert!(matches!(
            result,
            Err(MessageEncodeError::MissingHeaderData(_))
        ));
    }

    #[test]
    fn test_invalid_pong_with_interval() {
        let builder = MessageBuilder::ping_with_interval(1234, 1_703_123_456_789, 30);

        // Change type to PONG but keep interval - should fail validation
        let mut pong_builder = MessageBuilder::new(MessageType::Pong, 1234);
        pong_builder.header = builder.header;

        let result = pong_builder.build_vec();
        assert!(matches!(
            result,
            Err(MessageEncodeError::InvalidConfiguration(_))
        ));
    }

    #[test]
    fn test_encode_decode_compatibility() {
        let payload = TestPayload {
            message: "test".to_string(),
            value:   42,
        };

        let builder = MessageBuilder::simple_broadcast(1234).with_payload(&payload).unwrap();

        let buffer = builder.build_vec().unwrap();
        let message = Message::from_buffer(&buffer).unwrap();

        let decoded_payload: TestPayload = message.payload().unwrap();
        assert_eq!(decoded_payload, payload);
    }

    #[test]
    fn test_join_with_auth() {
        let auth = Auth::Token {
            token: "secret-token".to_string(),
        };

        let builder = MessageBuilder::simple_join().with_auth(auth);

        let buffer = builder.build_vec().unwrap();
        let message = Message::from_buffer(&buffer).unwrap();

        assert_eq!(message.message_type(), MessageType::Join);
        assert_eq!(message.client_id(), CLIENT_ID_UNASSIGNED);

        let header = message.header().unwrap();
        assert!(header.auth.is_some());
    }

    #[test]
    fn test_subscribe_unsubscribe() {
        let topic = "test_topic".to_string();

        // Test subscribe
        let sub_builder = MessageBuilder::simple_subscribe(1234, topic.clone());
        let sub_buffer = sub_builder.build_vec().unwrap();
        let sub_message = Message::from_buffer(&sub_buffer).unwrap();

        assert_eq!(sub_message.message_type(), MessageType::Subscribe);
        let sub_header = sub_message.header().unwrap();
        assert_eq!(sub_header.topic.unwrap(), topic);

        // Test unsubscribe
        let unsub_builder = MessageBuilder::simple_unsubscribe(1234, topic.clone());
        let unsub_buffer = unsub_builder.build_vec().unwrap();
        let unsub_message = Message::from_buffer(&unsub_buffer).unwrap();

        assert_eq!(unsub_message.message_type(), MessageType::Unsubscribe);
        let unsub_header = unsub_message.header().unwrap();
        assert_eq!(unsub_header.topic.unwrap(), topic);
    }
}
