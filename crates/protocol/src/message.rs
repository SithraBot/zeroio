//! Message structures with lazy parsing support

use std::sync::Arc;

use bytes::Bytes;

use crate::{
    RequestResponse, StatusCode,
    errors::{ProtocolError, ProtocolResult},
    types::{BaseHeader, ClientId, Header, MessageType},
};

/// A protocol message with lazy parsing capabilities
///
/// This structure uses zero-copy parsing where the header and payload
/// are only deserialized when accessed, reducing memory allocations
/// and improving performance for message routing scenarios.
#[derive(Debug, Clone)]
pub struct Message {
    /// Pre-parsed base header (always parsed for routing decisions)
    pub base_header: BaseHeader,

    /// Raw bytes containing the entire message
    raw_data: Bytes,

    /// Cached parsed header (lazy)
    header_cache: Arc<std::sync::RwLock<Option<Header>>>,

    /// Cached parsed payload (lazy)
    payload_cache: Arc<std::sync::RwLock<Option<rmpv::Value>>>,
}

impl Message {
    /// Create a new message from raw bytes
    ///
    /// This only parses the base header immediately, deferring
    /// header and payload parsing until needed.
    ///
    /// # Errors
    ///
    /// Returns an error if the data is incomplete or if base header parsing
    /// fails.
    pub fn from_bytes(data: Bytes) -> ProtocolResult<Self> {
        if data.len() < crate::constants::BASE_HEADER_SIZE {
            return Err(ProtocolError::IncompleteData {
                needed:    crate::constants::BASE_HEADER_SIZE,
                available: data.len(),
            });
        }

        // Parse base header immediately for routing decisions
        let base_header = crate::parser::parse_base_header(&data)?;

        // Validate message size
        let expected_size = base_header.total_message_size() as usize;
        if data.len() != expected_size {
            return Err(ProtocolError::IncompleteData {
                needed:    expected_size,
                available: data.len(),
            });
        }

        Ok(Message {
            base_header,
            raw_data: data,
            header_cache: Arc::new(std::sync::RwLock::new(None)),
            payload_cache: Arc::new(std::sync::RwLock::new(None)),
        })
    }

    /// Get the message type
    pub fn message_type(&self) -> MessageType {
        self.base_header.message_type
    }

    /// Get the client ID
    pub fn client_id(&self) -> ClientId {
        self.base_header.client_id
    }

    /// Get the header length
    pub fn header_length(&self) -> u32 {
        self.base_header.header_length
    }

    /// Get the payload length
    pub fn payload_length(&self) -> u64 {
        self.base_header.payload_length
    }

    /// Get the total message size
    pub fn total_size(&self) -> u64 {
        self.base_header.total_message_size()
    }

    /// Get the raw message bytes
    pub fn raw_bytes(&self) -> &Bytes {
        &self.raw_data
    }

    /// Parse and cache the header if not already cached
    ///
    /// This provides zero-copy access to the header when possible,
    /// only deserializing when first accessed.
    ///
    /// # Errors
    ///
    /// Returns an error if header deserialization fails.
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` for `header_cache` is poisoned.
    pub fn header(&self) -> ProtocolResult<Header> {
        // Try to read from cache first
        #[allow(clippy::unwrap_used)]
        {
            let cache = self.header_cache.read().unwrap();
            if let Some(ref header) = *cache {
                return Ok(header.clone());
            }
        }

        // Parse header if not cached
        let header = if self.base_header.header_length > 0 {
            let start = crate::constants::BASE_HEADER_SIZE;
            let end = start + self.base_header.header_length as usize;
            let header_bytes = self.raw_data.slice(start..end);
            rmp_serde::from_slice::<Header>(&header_bytes)?
        } else {
            Header::default()
        };

        // Cache the parsed header
        #[allow(clippy::unwrap_used)]
        {
            let mut cache = self.header_cache.write().unwrap();
            *cache = Some(header.clone());
        }

        Ok(header)
    }

    /// Parse and cache the payload if not already cached
    ///
    /// Returns the raw MessagePack value for maximum flexibility.
    /// Users can deserialize to specific types as needed.
    ///
    /// # Errors
    ///
    /// Returns an error if payload deserialization fails.
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` for `payload_cache` is poisoned.
    #[allow(clippy::unwrap_used)]
    pub fn payload(&self) -> ProtocolResult<Option<rmpv::Value>> {
        // Try to read from cache first
        #[allow(clippy::unwrap_used)]
        {
            let cache = self.payload_cache.read().unwrap();
            if cache.is_some() {
                return Ok(cache.clone());
            }
        }

        // Parse payload if not cached
        let payload = if self.base_header.payload_length > 0 {
            let start =
                crate::constants::BASE_HEADER_SIZE + self.base_header.header_length as usize;
            let end = start + self.base_header.payload_length as usize;
            let payload_bytes = self.raw_data.slice(start..end);
            Some(rmp_serde::from_slice::<rmpv::Value>(&payload_bytes)?)
        } else {
            None
        };

        // Cache the parsed payload
        {
            let mut cache = self.payload_cache.write().unwrap();
            *cache = payload.clone();
        }

        Ok(payload)
    }

    /// Deserialize payload to a specific type
    ///
    /// # Errors
    ///
    /// Returns an error if payload deserialization fails.
    pub fn payload_as<T>(&self) -> ProtocolResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        if self.base_header.payload_length == 0 {
            return Ok(None);
        }

        let start = crate::constants::BASE_HEADER_SIZE + self.base_header.header_length as usize;
        let end = start + self.base_header.payload_length as usize;
        let payload_bytes = self.raw_data.slice(start..end);

        let value = rmp_serde::from_slice::<T>(&payload_bytes)?;
        Ok(Some(value))
    }

    /// Get raw header bytes without parsing
    pub fn raw_header_bytes(&self) -> Bytes {
        if self.base_header.header_length == 0 {
            return Bytes::new();
        }

        let start = crate::constants::BASE_HEADER_SIZE;
        let end = start + self.base_header.header_length as usize;
        self.raw_data.slice(start..end)
    }

    /// Get raw payload bytes without parsing
    pub fn raw_payload_bytes(&self) -> Bytes {
        if self.base_header.payload_length == 0 {
            return Bytes::new();
        }

        let start = crate::constants::BASE_HEADER_SIZE + self.base_header.header_length as usize;
        let end = start + self.base_header.payload_length as usize;
        self.raw_data.slice(start..end)
    }

    /// Check if the message has a cached header
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` for `header_cache` is poisoned.
    #[allow(clippy::unwrap_used)]
    pub fn is_header_cached(&self) -> bool {
        self.header_cache.read().unwrap().is_some()
    }

    /// Check if the message has a cached payload
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` for `payload_cache` is poisoned.
    #[allow(clippy::unwrap_used)]
    pub fn is_payload_cached(&self) -> bool {
        self.payload_cache.read().unwrap().is_some()
    }

    /// Clear cached data to free memory
    ///
    /// # Panics
    ///
    /// Panics if the `RwLock` for `header_cache` or `payload_cache` is
    /// poisoned.
    #[allow(clippy::unwrap_used)]
    pub fn clear_cache(&self) {
        {
            let mut header_cache = self.header_cache.write().unwrap();
            *header_cache = None;
        }
        {
            let mut payload_cache = self.payload_cache.write().unwrap();
            *payload_cache = None;
        }
    }

    /// Create a response message with the same correlation ID
    ///
    /// This is a convenience method for creating REP messages that properly
    /// correlate with REQ messages.
    ///
    /// # Errors
    ///
    /// Returns an error if the original message is not a request,
    /// if the request header is missing, or if `RawMessage` creation fails.
    pub fn create_response(
        &self,
        sender_client_id: ClientId,
        status: Option<StatusCode>,
        payload: Option<&impl serde::Serialize>,
    ) -> ProtocolResult<RawMessage> {
        // Validate this is a request message
        if self.base_header.message_type != MessageType::Request {
            return Err(ProtocolError::InvalidFormat(
                "Cannot create response for non-request message".to_string(),
            ));
        }

        // Get the original request header to extract correlation info
        let req_header = self.header()?;
        let req_reqrep = req_header.reqrep.ok_or_else(|| {
            ProtocolError::InvalidFormat("Request message missing reqrep field".to_string())
        })?;

        // Get original routing to respond back
        let original_routing = req_header.routing.ok_or_else(|| {
            ProtocolError::InvalidFormat("Request message missing routing field".to_string())
        })?;

        if original_routing.is_empty() {
            return Err(ProtocolError::InvalidFormat(
                "Request message has empty routing".to_string(),
            ));
        }

        // Create response with correlation
        let mut response_header = Header::new();
        response_header.reqrep = Some(RequestResponse {
            req_type: crate::types::RequestResponseType::Correlation,
            id:       req_reqrep.id,
        });
        response_header.routing = Some(original_routing);
        response_header.status = status;

        RawMessage::new(
            MessageType::Response,
            sender_client_id,
            response_header,
            payload,
        )
    }
}

/// A raw message builder for creating new messages
///
/// This is used when constructing messages from scratch,
/// providing efficient serialization and validation.
#[derive(Debug, Clone)]
pub struct RawMessage {
    pub message_type: MessageType,
    pub client_id:    ClientId,
    pub header:       Header,
    pub payload_data: Option<Bytes>,
}

impl RawMessage {
    /// Create a new raw message
    ///
    /// # Errors
    ///
    /// Returns an error if payload serialization fails.
    pub fn new<T>(
        message_type: MessageType,
        client_id: ClientId,
        header: Header,
        payload: Option<&T>,
    ) -> ProtocolResult<Self>
    where
        T: serde::Serialize,
    {
        let payload_data = if let Some(payload) = payload {
            let bytes = rmp_serde::to_vec(payload)?;
            Some(Bytes::from(bytes))
        } else {
            None
        };

        Ok(RawMessage {
            message_type,
            client_id,
            header,
            payload_data,
        })
    }

    /// Serialize the message to bytes
    ///
    /// # Errors
    ///
    /// Returns an error if header serialization fails.
    pub fn to_bytes(&self) -> ProtocolResult<Bytes> {
        // Serialize header
        let header_bytes = if self.header == Header::default() {
            Vec::new()
        } else {
            rmp_serde::to_vec(&self.header)?
        };

        let header_length = header_bytes.len() as u32;
        let payload_length = self.payload_data.as_ref().map(|p| p.len()).unwrap_or(0) as u64;

        // Validate sizes
        if header_length > crate::constants::DEFAULT_MAX_HEADER_SIZE as u32 {
            return Err(ProtocolError::HeaderTooLarge {
                size: header_length as usize,
                max:  crate::constants::DEFAULT_MAX_HEADER_SIZE,
            });
        }

        let total_size =
            crate::constants::BASE_HEADER_SIZE as u64 + u64::from(header_length) + payload_length;
        if total_size > crate::constants::DEFAULT_MAX_MESSAGE_SIZE as u64 {
            return Err(ProtocolError::MessageTooLarge {
                size: total_size as usize,
                max:  crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
            });
        }

        // Create base header
        let base_header = BaseHeader::new(
            self.message_type,
            self.client_id,
            header_length,
            payload_length,
        );

        // Serialize to bytes
        let mut buffer = Vec::with_capacity(total_size as usize);

        // Write base header
        buffer.push(base_header.version);
        buffer.push(base_header.message_type.to_u8());
        buffer.extend_from_slice(&base_header.client_id.to_be_bytes());
        buffer.extend_from_slice(&base_header.reserved);
        buffer.extend_from_slice(&base_header.header_length.to_be_bytes());
        buffer.extend_from_slice(&base_header.payload_length.to_be_bytes());

        // Write header
        buffer.extend_from_slice(&header_bytes);

        // Write payload
        if let Some(ref payload_data) = self.payload_data {
            buffer.extend_from_slice(payload_data);
        }

        Ok(Bytes::from(buffer))
    }

    /// Convert to a parsed Message
    ///
    /// # Errors
    ///
    /// Returns an error if `Message::from_bytes` fails.
    pub fn into_message(self) -> ProtocolResult<Message> {
        let bytes = self.to_bytes()?;
        Message::from_bytes(bytes)
    }
}

/// A fully parsed message with all fields deserialized
///
/// This is useful when you know you need to access all parts
/// of the message and want to parse everything upfront.
#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub base_header: BaseHeader,
    pub header:      Header,
    pub payload:     Option<rmpv::Value>,
}

impl ParsedMessage {
    /// Create from a lazy Message by parsing all fields
    ///
    /// # Errors
    ///
    /// Returns an error if header or payload parsing fails.
    pub fn from_message(message: &Message) -> ProtocolResult<Self> {
        let header = message.header()?;
        let payload = message.payload()?;

        Ok(ParsedMessage {
            base_header: message.base_header.clone(),
            header,
            payload,
        })
    }

    /// Deserialize payload to a specific type
    ///
    /// # Errors
    ///
    /// Returns an error if payload deserialization fails.
    pub fn payload_as<T>(&self) -> ProtocolResult<Option<T>>
    where
        T: serde::de::DeserializeOwned,
    {
        match &self.payload {
            Some(value) => {
                let result = rmpv::ext::from_value(value.clone())?;
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }
}

impl From<ParsedMessage> for Message {
    #[allow(clippy::expect_used)]
    fn from(parsed: ParsedMessage) -> Self {
        // Create a RawMessage and convert to Message
        let raw = RawMessage::new(
            parsed.base_header.message_type,
            parsed.base_header.client_id,
            parsed.header,
            parsed.payload.as_ref(),
        )
        .expect("Failed to create RawMessage from ParsedMessage");

        raw.into_message().expect("Failed to convert RawMessage to Message")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    #[test]
    fn test_message_lazy_parsing() {
        let header = Header::new().with_topic("test");
        let payload = serde_json::json!({"test": "data"});

        let raw = RawMessage::new(MessageType::Publish, 1000, header, Some(&payload)).unwrap();

        let message = raw.into_message().unwrap();

        // Base header should be immediately available
        assert_eq!(message.message_type(), MessageType::Publish);
        assert_eq!(message.client_id(), 1000);

        // Header and payload should not be cached initially
        assert!(!message.is_header_cached());
        assert!(!message.is_payload_cached());

        // Access header - should be cached after first access
        let header = message.header().unwrap();
        assert_eq!(header.topic, Some("test".to_string()));
        assert!(message.is_header_cached());

        // Access payload - should be cached after first access
        let _payload = message.payload().unwrap().unwrap();
        assert!(message.is_payload_cached());
    }
}
