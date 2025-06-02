# fleximq Protocol v1.0.0

A high-performance, zero-copy implementation of the fleximq protocol using `bytes` and `nom`.

## Features

- **Zero-copy parsing** with lazy deserialization for optimal performance
- **Async-friendly API** with full tokio integration
- **Memory-efficient** using `bytes::Bytes` for zero-copy operations
- **Type-safe message construction** with fluent builder API
- **Streaming support** for network protocols
- **MessagePack serialization** for headers and payloads
- **Protocol validation** ensuring message correctness

## Quick Start

### Basic Message Creation

```rust
use fleximq_protocol::{MessageBuilder, MessageType, StatusCode};

// Create a simple publish message
let publish_msg = MessageBuilder::simple_publish(1000, "news.updates")
    .build_with_payload(&serde_json::json!({
        "title": "Breaking News",
        "content": "Important update"
    }))
    .unwrap();

// Create a request message
let request_msg = MessageBuilder::simple_request(1000, 2000, "/api/users", "req-123")
    .build_with_payload(&serde_json::json!({
        "action": "get_user",
        "user_id": 456
    }))
    .unwrap();

// Create a response message
let response_msg = MessageBuilder::simple_response(2000, 1000, "/api/users", "req-123", StatusCode::OK)
    .build_with_payload(&serde_json::json!({
        "user": {
            "id": 456,
            "name": "John Doe"
        }
    }))
    .unwrap();
```

### Lazy Message Parsing

```rust
use fleximq_protocol::{Message, MessageType};

// Parse message from bytes (only base header is parsed immediately)
let message = Message::from_bytes(raw_bytes)?;

// Basic routing information is immediately available
println!("Message type: {:?}", message.message_type());
println!("Client ID: {}", message.client_id());
println!("Total size: {} bytes", message.total_size());

// Header is parsed only when accessed (lazy parsing)
let header = message.header()?; // Parses and caches header
println!("Topic: {:?}", header.topic);

// Payload is parsed only when accessed (lazy parsing)
if let Some(payload) = message.payload()? {
    // Process payload...
}

// Or deserialize payload to specific type
let user_data: Option<User> = message.payload_as::<User>()?;
```

### Streaming with Tokio

```rust
use fleximq_protocol::codec::{MessageCodec, utils};
use tokio_util::codec::Framed;
use futures_util::{SinkExt, StreamExt};

// Create a framed connection
let mut framed = utils::framed(tcp_stream);

// Send a message
let msg = MessageBuilder::simple_publish(client_id, "events")
    .build_with_payload(&event_data)?;

utils::send_message(&mut framed, msg).await?;

// Receive messages
while let Some(message) = utils::receive_message(&mut framed).await? {
    match message.message_type() {
        MessageType::Publish => {
            let header = message.header()?;
            println!("Received on topic: {:?}", header.topic);
        }
        MessageType::Request => {
            // Handle request and send response
            let response = message.create_response(
                my_client_id,
                Header::new().with_status(StatusCode::OK),
                Some(&response_data)
            )?;
            utils::send_message(&mut framed, response.into_message()?).await?;
        }
        _ => {}
    }
}
```

### Authentication

```rust
use fleximq_protocol::{MessageBuilder, Auth, constants::CLIENT_ID_UNASSIGNED};

// Token authentication
let join_msg = MessageBuilder::join_with_token(CLIENT_ID_UNASSIGNED, "auth-token-123")
    .build()?;

// Basic authentication
let join_msg = MessageBuilder::join_with_basic_auth(
    CLIENT_ID_UNASSIGNED,
    "username",
    "password"
).build()?;

// API Key authentication
let join_msg = MessageBuilder::join_with_api_key(CLIENT_ID_UNASSIGNED, "api-key-123")
    .build()?;
```

### Keep-alive (Ping/Pong)

```rust
use fleximq_protocol::{MessageBuilder, KeepAlive};

// Send a ping
let ping = MessageBuilder::ping_message(client_id).build()?;

// Respond to ping with pong
let ping_header = ping_message.header()?;
let ping_keepalive = ping_header.keepalive.unwrap();
let pong = MessageBuilder::pong_message(client_id, ping_keepalive.timestamp).build()?;
```

## Message Types

The protocol supports the following message types as defined in the v1.0.0 specification:

- **JOIN (0)**: Client registration with broker
- **REQ (1)**: Request message to specific client
- **REP (2)**: Response message with correlation
- **NOTIF (3)**: Notification to specific clients
- **BCAST (4)**: Broadcast message to all clients
- **PUB (5)**: Publish message to topic
- **SUB (6)**: Subscribe to topic
- **UNSUB (7)**: Unsubscribe from topic
- **PING (8)**: Keep-alive ping
- **PONG (9)**: Keep-alive pong response

## Performance Features

### Zero-Copy Operations

```rust
// Get raw bytes without parsing
let raw_header = message.raw_header_bytes();
let raw_payload = message.raw_payload_bytes();

// Check cache status
if !message.is_header_cached() {
    // Header hasn't been parsed yet
}

// Clear cache to free memory
message.clear_cache();
```

### Streaming Parser

```rust
use fleximq_protocol::parser::MessageParser;

let mut parser = MessageParser::new();

// Add data incrementally
parser.push_data(&chunk1)?;
parser.push_data(&chunk2)?;

// Parse all complete messages
let messages = parser.parse_all()?;
```

### Custom Codecs

```rust
use fleximq_protocol::codec::{MessageDecoder, MessageEncoder};

// Decode-only codec
let mut decoder = MessageDecoder::with_limits(1024 * 1024, 64 * 1024);

// Encode-only codec
let mut encoder = MessageEncoder::with_max_frame_size(1024 * 1024);
```

## Error Handling

The protocol provides comprehensive error handling:

```rust
use fleximq_protocol::{ProtocolError, ProtocolResult};

match result {
    Err(ProtocolError::InvalidVersion { expected, actual }) => {
        println!("Version mismatch: expected {}, got {}", expected, actual);
    }
    Err(ProtocolError::MessageTooLarge { size, max }) => {
        println!("Message too large: {} bytes (max: {})", size, max);
    }
    Err(ProtocolError::MissingRequiredField { field, message_type }) => {
        println!("Missing field '{}' for message type '{}'", field, message_type);
    }
    // ... other error types
    Ok(result) => {
        // Success
    }
}
```

## Protocol Validation

Messages are automatically validated according to the v1.0.0 specification:

```rust
// This will fail - REQ messages require routing
let invalid = MessageBuilder::request(1000)
    .as_request("req-id")
    .build(); // Error: Missing required field 'routing'

// This will fail - SUB messages cannot have status
let invalid = MessageBuilder::subscribe(1000)
    .topic("test")
    .status(StatusCode::OK)
    .build(); // Error: Forbidden field 'status'
```

## Constants

```rust
use fleximq_protocol::constants::*;

// Protocol version
const VERSION: u8 = PROTOCOL_VERSION; // 1

// Special client IDs
const UNASSIGNED: u32 = CLIENT_ID_UNASSIGNED; // 0
const BROKER: u32 = CLIENT_ID_BROKER; // 1

// Size limits
const MAX_MESSAGE: usize = DEFAULT_MAX_MESSAGE_SIZE; // 1GB
const MAX_HEADER: usize = DEFAULT_MAX_HEADER_SIZE; // 64KB
```

## Advanced Usage

### Custom Message Processing

```rust
use fleximq_protocol::{Message, ParsedMessage};

// Fully parse message upfront
let parsed = ParsedMessage::from_message(&message)?;

// Access all fields without additional parsing
println!("Status: {:?}", parsed.header.status);
if let Some(payload) = parsed.payload_as::<MyPayloadType>()? {
    // Process payload
}
```

### Message Inspection

```rust
use fleximq_protocol::parser::peek_message_info;

// Quick message type detection without full parsing
let (msg_type, client_id) = peek_message_info(&raw_bytes)?;

match msg_type {
    MessageType::Request => {
        // Route to request handler
    }
    MessageType::Publish => {
        // Route to pub/sub system
    }
    _ => {}
}
```

## Best Practices

1. **Use lazy parsing** for routing scenarios where you only need basic message info
2. **Clear caches** for long-lived messages to free memory
3. **Set appropriate size limits** based on your use case
4. **Validate messages** early using the builder API
5. **Use streaming parsers** for network protocols
6. **Leverage zero-copy operations** when forwarding messages

## License

This implementation follows the fleximq Protocol v1.0.0 specification.
