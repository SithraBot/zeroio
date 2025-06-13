//! High-performance binary parser using nom

use bytes::Bytes;
use nom::{
    IResult, Parser,
    bytes::complete::take,
    combinator::verify,
    number::complete::{be_u8, be_u32, be_u64},
};

use crate::{
    constants::{
        BASE_HEADER_SIZE, DEFAULT_MAX_HEADER_SIZE, DEFAULT_MAX_MESSAGE_SIZE, PROTOCOL_VERSION,
        RESERVED_SIZE,
    },
    errors::{ProtocolError, ProtocolResult},
    types::{BaseHeader, MessageType},
};

/// Parse the base header from raw bytes
///
/// This is a fast, zero-copy operation that extracts the fixed-size
/// base header fields for routing decisions.
///
/// # Errors
///
/// Returns an error if parsing fails (e.g. insufficient data, invalid version).
pub fn parse_base_header(input: &[u8]) -> ProtocolResult<BaseHeader> {
    match parse_base_header_nom(input) {
        Ok((_, base_header)) => Ok(base_header),
        Err(e) => Err(ProtocolError::ParseError(format!(
            "Failed to parse base header: {e}"
        ))),
    }
}

/// Nom parser for the base header
fn parse_base_header_nom(input: &[u8]) -> IResult<&[u8], BaseHeader> {
    let (input, (version, message_type_u8, client_id, reserved, header_length, payload_length)) = (
        verify(be_u8, |&v| v == PROTOCOL_VERSION),
        be_u8,
        be_u32,
        take(RESERVED_SIZE),
        be_u32,
        be_u64,
    )
        .parse(input)?;

    // Convert message type
    let message_type = MessageType::from_u8(message_type_u8).ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag))
    })?;

    // Convert reserved bytes to array
    let mut reserved_array = [0u8; RESERVED_SIZE];
    reserved_array.copy_from_slice(reserved);

    let base_header = BaseHeader {
        version,
        message_type,
        client_id,
        reserved: reserved_array,
        header_length,
        payload_length,
    };

    Ok((input, base_header))
}

/// Parse a complete message from bytes
///
/// This performs incremental parsing, first extracting the base header,
/// then validating message length, and finally extracting the raw
/// header and payload sections without deserializing them.
///
/// # Errors
///
/// Returns an error if parsing fails or if there is unexpected data after the
/// message.
pub fn parse_message_bytes(input: &[u8]) -> ProtocolResult<MessageFrame> {
    match parse_message_frame_nom(input) {
        Ok((remaining, frame)) => {
            if !remaining.is_empty() {
                return Err(ProtocolError::ParseError(
                    "Unexpected data after message".to_string(),
                ));
            }
            Ok(frame)
        }
        Err(e) => Err(ProtocolError::ParseError(format!(
            "Failed to parse message: {e}"
        ))),
    }
}

/// A parsed message frame with raw header and payload bytes
#[derive(Debug, Clone)]
pub struct MessageFrame {
    pub base_header:   BaseHeader,
    pub header_bytes:  Bytes,
    pub payload_bytes: Bytes,
}

/// Nom parser for complete message frame
fn parse_message_frame_nom(input: &[u8]) -> IResult<&[u8], MessageFrame> {
    // Parse base header first
    let (input, base_header) = parse_base_header_nom(input)?;

    // Validate total message size
    let total_size = base_header.total_message_size() as usize;
    if input.len() + BASE_HEADER_SIZE != total_size {
        return Err(nom::Err::Error(nom::error::Error::new(
            input,
            nom::error::ErrorKind::LengthValue,
        )));
    }

    // Extract header bytes
    let (input, header_bytes) = take(base_header.header_length)(input)?;

    // Extract payload bytes
    let (input, payload_bytes) = take(base_header.payload_length)(input)?;

    let frame = MessageFrame {
        base_header,
        header_bytes: Bytes::copy_from_slice(header_bytes),
        payload_bytes: Bytes::copy_from_slice(payload_bytes),
    };

    Ok((input, frame))
}

/// Streaming parser for messages from a buffer
///
/// This is useful for network streams where messages may arrive
/// in fragments. It returns the parsed message and the number
/// of bytes consumed.
#[derive(Debug, Clone)]
pub struct MessageParser {
    /// Buffer for accumulating incoming data
    buffer:           Vec<u8>,
    /// Maximum message size allowed
    max_message_size: usize,
    /// Maximum header size allowed
    max_header_size:  usize,
}

impl MessageParser {
    /// Create a new streaming message parser
    #[must_use]
    pub const fn new() -> Self {
        Self::with_limits(DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_MAX_HEADER_SIZE)
    }

    /// Create a new parser with custom size limits
    #[must_use]
    pub const fn with_limits(max_message_size: usize, max_header_size: usize) -> Self {
        Self {
            buffer: Vec::new(),
            max_message_size,
            max_header_size,
        }
    }

    /// Add data to the parser buffer
    ///
    /// # Errors
    ///
    /// Returns an error if the buffer size limit is exceeded.
    pub fn push_data(&mut self, data: &[u8]) -> ProtocolResult<()> {
        // Check buffer size limits
        if self.buffer.len() + data.len() > self.max_message_size {
            return Err(ProtocolError::MessageTooLarge {
                size: self.buffer.len() + data.len(),
                max:  self.max_message_size,
            });
        }

        self.buffer.extend_from_slice(data);
        Ok(())
    }

    /// Try to parse a complete message from the buffer
    ///
    /// Returns the parsed message and removes the consumed bytes
    /// from the buffer. Returns None if a complete message is not
    /// yet available.
    ///
    /// # Errors
    ///
    /// Returns an error if parsing fails (e.g. message too large, header too
    /// large, or underlying message creation fails).
    pub fn try_parse(&mut self) -> ProtocolResult<Option<crate::message::Message>> {
        // Need at least base header size
        if self.buffer.len() < BASE_HEADER_SIZE {
            return Ok(None);
        }

        // Try to parse base header to get message size
        let Ok(base_header) = parse_base_header(&self.buffer[..BASE_HEADER_SIZE]) else {
            return Ok(None); // Wait for more data
        };

        // Validate header size limits
        if base_header.header_length > self.max_header_size as u32 {
            return Err(ProtocolError::HeaderTooLarge {
                size: base_header.header_length as usize,
                max:  self.max_header_size,
            });
        }

        // Check if we have the complete message
        let total_size = base_header.total_message_size() as usize;
        if total_size > self.max_message_size {
            return Err(ProtocolError::MessageTooLarge {
                size: total_size,
                max:  self.max_message_size,
            });
        }

        if self.buffer.len() < total_size {
            return Ok(None); // Wait for more data
        }

        // Extract the complete message
        let message_bytes = self.buffer.drain(..total_size).collect::<Vec<u8>>();
        let message = crate::message::Message::from_bytes(Bytes::from(message_bytes))?;

        Ok(Some(message))
    }

    /// Get the current buffer size
    #[must_use]
    pub fn buffer_size(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the internal buffer
    pub fn clear(&mut self) {
        self.buffer.clear();
    }

    /// Check if buffer is empty
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Peek at the buffer content without consuming
    #[must_use]
    pub fn buffer(&self) -> &[u8] {
        &self.buffer
    }

    /// Try to parse multiple messages from the buffer
    ///
    /// Returns all complete messages found and leaves incomplete
    /// data in the buffer.
    ///
    /// # Errors
    ///
    /// Returns an error if `try_parse` encounters an error.
    pub fn parse_all(&mut self) -> ProtocolResult<Vec<crate::message::Message>> {
        let mut messages = Vec::new();

        while let Some(message) = self.try_parse()? {
            messages.push(message);
        }

        Ok(messages)
    }
}

impl Default for MessageParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate message structure according to protocol rules
///
/// This checks that required header fields are present for the given
/// message type, and that no unexpected fields are set.
///
/// # Errors
///
/// Returns an error if the message structure is invalid for its type.
#[allow(clippy::too_many_lines)]
pub fn validate_message_structure(
    message_type: MessageType,
    header: &crate::types::Header,
) -> ProtocolResult<()> {
    use crate::types::{
        HeaderField,
        MessageType::{
            Broadcast, Join, Notification, Ping, Pong, Publish, Request, Response, Subscribe,
            Unsubscribe,
        },
    };

    // Define required and forbidden fields for each message type
    let (required, forbidden): (&[HeaderField], &[HeaderField]) = match message_type {
        Join => (
            &[HeaderField::ClientName],
            &[
                HeaderField::Routing,
                HeaderField::Reqrep,
                HeaderField::Topic,
                HeaderField::Keepalive,
                HeaderField::Status,
            ],
        ),
        Request | Response => (
            &[HeaderField::Routing, HeaderField::Reqrep],
            &[HeaderField::Topic, HeaderField::Auth, HeaderField::Keepalive],
        ),
        Notification => (
            &[HeaderField::Routing],
            &[
                HeaderField::Reqrep,
                HeaderField::Topic,
                HeaderField::Auth,
                HeaderField::Keepalive,
            ],
        ),
        Broadcast => (
            &[],
            &[
                HeaderField::Routing,
                HeaderField::Reqrep,
                HeaderField::Topic,
                HeaderField::Auth,
                HeaderField::Keepalive,
            ],
        ),
        Publish => (
            &[HeaderField::Topic],
            &[
                HeaderField::Routing,
                HeaderField::Reqrep,
                HeaderField::Auth,
                HeaderField::Keepalive,
            ],
        ),
        Subscribe | Unsubscribe => (
            &[HeaderField::Topic],
            &[
                HeaderField::Routing,
                HeaderField::Reqrep,
                HeaderField::Status,
                HeaderField::Auth,
                HeaderField::Keepalive,
            ],
        ),
        Ping | Pong => (
            &[HeaderField::Keepalive],
            &[
                HeaderField::Routing,
                HeaderField::Reqrep,
                HeaderField::Topic,
                HeaderField::Status,
                HeaderField::Auth,
            ],
        ),
    };

    // Check required fields
    for required_field in required {
        if !required_field.is_present_in(header) {
            return Err(ProtocolError::MissingRequiredField {
                field:        required_field.name().to_string(),
                message_type: message_type.as_str().to_string(),
            });
        }
    }

    // Check forbidden fields
    for forbidden_field in forbidden {
        if forbidden_field.is_present_in(header) {
            return Err(ProtocolError::ForbiddenField {
                field:        forbidden_field.name().to_string(),
                message_type: message_type.as_str().to_string(),
            });
        }
    }

    // Additional validation rules
    match message_type {
        Request | Response => {
            // REQ and REP must have exactly one routing entry
            if let Some(routing) = &header.routing {
                if routing.len() != 1 {
                    return Err(ProtocolError::InvalidRouting(format!(
                        "{} messages must have exactly one routing entry, got {}",
                        message_type.as_str(),
                        routing.len()
                    )));
                }

                // For Response messages, client_id must be present in routing
                if message_type == Response && routing[0].client_id.is_none() {
                    return Err(ProtocolError::InvalidRouting(
                        "REP messages must include client_id in routing".to_string(),
                    ));
                }
            }
        }
        Notification => {
            // NOTIF must have at least one routing entry
            if let Some(routing) = &header.routing {
                if routing.is_empty() {
                    return Err(ProtocolError::InvalidRouting(
                        "NOTIF messages must have at least one routing entry".to_string(),
                    ));
                }
            }
        }
        Ping | Pong => {
            // Validate keepalive timestamp for PONG correlation
            if message_type == Pong {
                if let Some(keepalive) = &header.keepalive {
                    // Basic timestamp validation (not too far in the future)
                    #[allow(clippy::cast_possible_truncation)]
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;

                    let max_future = now + 60_000; // 1 minute in the future
                    if keepalive.timestamp > max_future {
                        return Err(ProtocolError::InvalidFormat(
                            "PONG timestamp is too far in the future".to_string(),
                        ));
                    }
                }
            }
        }
        _ => {}
    }

    Ok(())
}

/// Fast message type detection without full parsing
///
/// This is useful for routing decisions where you only need
/// to know the message type and client ID.
///
/// Peek at the message type and total size from raw bytes
///
/// This allows for quick inspection of message metadata without
/// full parsing. It's useful for scenarios like dispatching
/// messages based on type or pre-allocating buffers.
///
/// # Errors
///
/// Returns an error if base header parsing fails.
pub fn peek_message_info(data: &[u8]) -> ProtocolResult<(MessageType, u32)> {
    if data.len() < BASE_HEADER_SIZE {
        return Err(ProtocolError::IncompleteData {
            needed:    BASE_HEADER_SIZE,
            available: data.len(),
        });
    }

    // Quick validation of version
    if data[0] != PROTOCOL_VERSION {
        return Err(ProtocolError::InvalidVersion {
            expected: PROTOCOL_VERSION,
            actual:   data[0],
        });
    }

    // Extract message type
    let message_type =
        MessageType::from_u8(data[1]).ok_or_else(|| ProtocolError::UnknownMessageType(data[1]))?;

    // Extract client ID (big-endian u32 at offset 2)
    let client_id = u32::from_be_bytes([data[2], data[3], data[4], data[5]]);

    Ok((message_type, client_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        message::{NIL, RawMessage},
        types::*,
    };

    #[test]
    fn test_base_header_parsing() {
        let header = Header::new().with_topic("test");
        let raw = RawMessage::new(MessageType::Publish, 1000, header, &NIL).unwrap();
        let bytes = raw.to_bytes().unwrap();

        let base_header = parse_base_header(&bytes).unwrap();
        assert_eq!(base_header.version, PROTOCOL_VERSION);
        assert_eq!(base_header.message_type, MessageType::Publish);
        assert_eq!(base_header.client_id, 1000);
    }

    #[test]
    fn test_streaming_parser() {
        let header = Header::new().with_topic("test");
        let payload = serde_json::json!({"data": "test"});
        let raw = RawMessage::new(MessageType::Publish, 1000, header, &payload).unwrap();
        let bytes = raw.to_bytes().unwrap();

        let mut parser = MessageParser::new();

        // Add data in chunks to test streaming
        let chunk_size = 10;
        for chunk in bytes.chunks(chunk_size) {
            parser.push_data(chunk).unwrap();
        }

        let message = parser.try_parse().unwrap().unwrap();
        assert_eq!(message.message_type(), MessageType::Publish);
        assert_eq!(message.client_id(), 1000);

        let header = message.header().unwrap();
        assert_eq!(header.topic, Some("test".to_string()));
    }

    #[test]
    fn test_message_validation() {
        // Valid REQ message
        let header =
            Header::new()
                .with_route("client-1234", "/api/test")
                .with_reqrep(RequestResponse {
                    req_type: RequestResponseType::Request,
                    id:       "test-id".to_string(),
                });

        validate_message_structure(MessageType::Request, &header).unwrap();

        // Invalid REQ message (missing routing)
        let header = Header::new().with_reqrep(RequestResponse {
            req_type: RequestResponseType::Request,
            id:       "test-id".to_string(),
        });

        let result = validate_message_structure(MessageType::Request, &header);
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProtocolError::MissingRequiredField { .. }
        ));
    }

    #[test]
    fn test_peek_message_info() {
        let header = Header::new().with_topic("test");
        let raw = RawMessage::new(MessageType::Subscribe, 5678, header, &NIL).unwrap();
        let bytes = raw.to_bytes().unwrap();

        let (msg_type, client_id) = peek_message_info(&bytes).unwrap();
        assert_eq!(msg_type, MessageType::Subscribe);
        assert_eq!(client_id, 5678);
    }
}
