use crate::message::types::{Header, MessageDeserializeError, MessageEncodeError, MessageType};

/// Trait for decoding messages from binary data
pub trait MessageDecode<'a> {
    /// Decode a message from a byte buffer
    /// 
    /// # Errors
    /// 
    /// Returns `MessageDeserializeError` if the buffer contains invalid message data.
    fn from_buffer(buffer: &'a [u8]) -> Result<Self, MessageDeserializeError>
    where
        Self: Sized;

    /// Decode a message from a mutable byte buffer
    /// 
    /// # Errors
    /// 
    /// Returns `MessageDeserializeError` if the buffer contains invalid message data.
    fn from_buffer_mut(buffer: &'a mut [u8]) -> Result<Self, MessageDeserializeError>
    where
        Self: Sized,
    {
        Self::from_buffer(buffer)
    }

    /// Get the message type
    fn message_type(&self) -> MessageType;

    /// Get the protocol version
    fn version(&self) -> u8;

    /// Get the client ID
    fn client_id(&self) -> u32;

    /// Get the raw header bytes
    fn header_bytes(&self) -> &[u8];

    /// Get the decoded header
    /// 
    /// # Errors
    /// 
    /// Returns `MessageDeserializeError` if the header cannot be deserialized.
    fn header(&self) -> Result<Header, MessageDeserializeError>;

    /// Get the raw payload bytes
    fn payload_bytes(&self) -> &[u8];

    /// Get the decoded payload as a specific type
    /// 
    /// # Errors
    /// 
    /// Returns `MessageDeserializeError` if the payload cannot be deserialized to the target type.
    fn payload<T>(&self) -> Result<T, MessageDeserializeError>
    where
        T: for<'de> serde::Deserialize<'de>;

    /// Get the header length
    fn header_len(&self) -> u32;

    /// Get the payload length
    fn payload_len(&self) -> u64;

    /// Get the total message size
    fn total_size(&self) -> usize;
}

/// Trait for encoding messages to binary data
pub trait MessageEncode {
    /// Calculate the size needed for the encoded message
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if header serialization fails or message is too large.
    fn calculate_size(&self) -> Result<usize, MessageEncodeError>;

    /// Build the message into a new vector
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if encoding fails or validation fails.
    fn build_vec(&self) -> Result<Vec<u8>, MessageEncodeError> {
        let size = self.calculate_size()?;
        let mut buffer = vec![0u8; size];
        self.build_into(&mut buffer)?;
        Ok(buffer)
    }

    /// Build the message into an existing buffer
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if the buffer is too small or encoding fails.
    fn build_into(&self, buffer: &mut [u8]) -> Result<usize, MessageEncodeError>;

    /// Build the message and return the buffer slice
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if the buffer is too small or encoding fails.
    fn build<'a>(&self, buffer: &'a mut [u8]) -> Result<&'a [u8], MessageEncodeError> {
        let size = self.build_into(buffer)?;
        Ok(&buffer[..size])
    }
}

/// Trait for message validation
pub trait MessageValidate {
    /// Validate the message structure according to protocol rules
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if the message structure violates protocol rules.
    fn validate(&self) -> Result<(), MessageEncodeError>;
}

/// Trait for message header manipulation
pub trait HeaderAccess {
    /// Get a reference to the header
    fn header(&self) -> &Header;

    /// Get a mutable reference to the header
    fn header_mut(&mut self) -> &mut Header;

    /// Set the header
    fn set_header(&mut self, header: Header);
}

/// Trait for message payload manipulation
pub trait PayloadAccess {
    /// Get the payload as bytes
    fn payload_bytes(&self) -> &[u8];

    /// Set the payload from bytes
    fn set_payload_bytes(&mut self, payload: Vec<u8>);

    /// Get the payload as a specific type
    /// 
    /// # Errors
    /// 
    /// Returns `MessageDeserializeError` if the payload cannot be deserialized to the target type.
    fn payload<T>(&self) -> Result<T, MessageDeserializeError>
    where
        T: for<'de> serde::Deserialize<'de>;

    /// Set the payload from a serializable type
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if the payload cannot be serialized.
    fn set_payload<T>(&mut self, payload: &T) -> Result<(), MessageEncodeError>
    where
        T: serde::Serialize;
}

/// Combined trait for full message functionality
pub trait Message<'a>:
    MessageDecode<'a> + MessageEncode + MessageValidate + HeaderAccess + PayloadAccess
{
    /// Create a new message with the given type and client ID
    fn new(msg_type: MessageType, client_id: u32) -> Self;

    /// Clone the message data into a new owned message
    /// 
    /// # Errors
    /// 
    /// Returns `MessageEncodeError` if the message cannot be cloned or validated.
    fn to_owned(&self) -> Result<OwnedMessage, MessageEncodeError>;
}

/// Owned message type that doesn't borrow data
#[derive(Debug, Clone)]
pub struct OwnedMessage {
    pub version:   u8,
    pub msg_type:  MessageType,
    pub client_id: u32,
    pub header:    Header,
    pub payload:   Vec<u8>,
}

impl MessageEncode for OwnedMessage {
    fn calculate_size(&self) -> Result<usize, MessageEncodeError> {
        let header_data = rmp_serde::to_vec(&self.header)
            .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))?;

        // Base header (34) + header data + payload
        Ok(34 + header_data.len() + self.payload.len())
    }

    fn build_into(&self, buffer: &mut [u8]) -> Result<usize, MessageEncodeError> {
        let header_data = rmp_serde::to_vec(&self.header)
            .map_err(|e| MessageEncodeError::HeaderSerializeError(e.to_string()))?;

        let total_size = 34 + header_data.len() + self.payload.len();

        if buffer.len() < total_size {
            return Err(MessageEncodeError::MessageTooLarge(total_size));
        }

        let mut offset = 0;

        // Version
        buffer[offset] = self.version;
        offset += 1;

        // Type
        buffer[offset] = self.msg_type as u8;
        offset += 1;

        // Client ID (big-endian)
        buffer[offset..offset + 4].copy_from_slice(&self.client_id.to_be_bytes());
        offset += 4;

        // Reserved (16 bytes of zeros)
        buffer[offset..offset + 16].fill(0);
        offset += 16;

        // Header length (big-endian)
        let header_len = u32::try_from(header_data.len()).map_err(|_| {
            MessageEncodeError::MessageTooLarge(header_data.len())
        })?;
        buffer[offset..offset + 4].copy_from_slice(&header_len.to_be_bytes());
        offset += 4;

        // Header data
        buffer[offset..offset + header_data.len()].copy_from_slice(&header_data);
        offset += header_data.len();

        // Payload length (big-endian)
        let payload_len = self.payload.len() as u64;
        buffer[offset..offset + 8].copy_from_slice(&payload_len.to_be_bytes());
        offset += 8;

        // Payload
        buffer[offset..offset + self.payload.len()].copy_from_slice(&self.payload);
        offset += self.payload.len();

        Ok(offset)
    }
}

impl MessageValidate for OwnedMessage {
    fn validate(&self) -> Result<(), MessageEncodeError> {
        use crate::message::types::*;

        match self.msg_type {
            MessageType::Join => {
                if self.header.routing.is_some()
                    || self.header.reqrep.is_some()
                    || self.header.topic.is_some()
                    || self.header.keepalive.is_some()
                {
                    return Err(MessageEncodeError::InvalidConfiguration(
                        "JOIN messages cannot have routing, reqrep, topic, or keepalive fields"
                            .to_string(),
                    ));
                }
            }
            MessageType::Request | MessageType::Response => {
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
            }
            MessageType::Notification => {
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
                        "Notification messages cannot have reqrep, topic, auth, or keepalive \
                         fields"
                            .to_string(),
                    ));
                }
            }
            MessageType::Broadcast => {
                if self.header.routing.is_some()
                    || self.header.reqrep.is_some()
                    || self.header.topic.is_some()
                    || self.header.auth.is_some()
                    || self.header.keepalive.is_some()
                {
                    return Err(MessageEncodeError::InvalidConfiguration(
                        "Broadcast messages cannot have routing, reqrep, topic, auth, or \
                         keepalive fields"
                            .to_string(),
                    ));
                }
            }
            MessageType::Topic => {
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
            }
            MessageType::Subscribe | MessageType::Unsubscribe => {
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
                        "Subscribe/Unsubscribe messages cannot have routing, reqrep, status, \
                         auth, or keepalive fields"
                            .to_string(),
                    ));
                }
            }
            MessageType::Ping | MessageType::Pong => {
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
                        "Ping/Pong messages cannot have routing, reqrep, topic, status, or auth \
                         fields"
                            .to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl HeaderAccess for OwnedMessage {
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

impl PayloadAccess for OwnedMessage {
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
