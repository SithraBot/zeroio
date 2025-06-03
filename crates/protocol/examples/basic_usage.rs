//! Basic usage example for FlexiMQ Protocol
//!
//! This example demonstrates the core features of the protocol implementation:
//! - Message creation with fluent builder API
//! - Lazy parsing for performance
//! - Streaming with tokio codecs
//! - Zero-copy operations

use fleximq_protocol::{
    Message, MessageBuilder, MessageType, ProtocolResult, StatusCode,
    constants::*,
    parser::{MessageParser, peek_message_info},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct UserData {
    id:    u32,
    name:  String,
    email: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    user_id:   u32,
    message:   String,
    timestamp: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct NewsArticle {
    title:        String,
    content:      String,
    author:       String,
    published_at: u64,
}

fn main() -> ProtocolResult<()> {
    println!("=== fleximq Protocol Basic Usage Example ===\n");

    // 1. Demonstrate message creation
    demonstrate_message_creation()?;

    // 2. Demonstrate lazy parsing
    demonstrate_lazy_parsing()?;

    // 3. Demonstrate streaming parsing
    demonstrate_streaming_parsing()?;

    // 4. Demonstrate zero-copy operations
    demonstrate_zero_copy()?;

    // 5. Demonstrate protocol validation
    demonstrate_validation()?;

    // 6. Demonstrate message correlation
    demonstrate_message_correlation()?;

    println!("✅ All examples completed successfully!");
    Ok(())
}

fn demonstrate_message_creation() -> ProtocolResult<()> {
    println!("🔨 Message Creation Examples:");

    // 1. JOIN message with authentication
    let join_msg =
        MessageBuilder::join_with_token(CLIENT_ID_UNASSIGNED, "example-client", "secure-token-123")
            .build()?;
    println!(
        "  📝 Created JOIN message: {} bytes",
        join_msg.to_bytes()?.len()
    );

    // 2. Publish message to a topic
    let news_article = NewsArticle {
        title:        "FlexiMQ Protocol Released".to_string(),
        content:      "A new high-performance messaging protocol...".to_string(),
        author:       "Protocol Team".to_string(),
        published_at: 1700000000,
    };

    let publish_msg = MessageBuilder::publish(1000)
        .with_topic("news.technology")
        .build_with_payload(&news_article)?;
    println!(
        "  📰 Created PUBLISH message: {} bytes",
        publish_msg.to_bytes()?.len()
    );

    // 3. Request-Response pattern
    let user_request = MessageBuilder::request(1000, "client-source", "/api/users/123", Some(2000))
        .as_request("req-001")
        .build()?;
    println!(
        "  📤 Created REQUEST message: {} bytes",
        user_request.to_bytes()?.len()
    );

    let user_data = UserData {
        id:    123,
        name:  "Alice Smith".to_string(),
        email: "alice@example.com".to_string(),
    };

    let user_response =
        MessageBuilder::response(2000, "client-target", "req-001", "/api/users/123", 1000)
            .status(StatusCode::OK)
            .build_with_payload(&user_data)?;
    println!(
        "  📥 Created RESPONSE message: {} bytes",
        user_response.to_bytes()?.len()
    );

    // 4. Broadcast message
    let broadcast_msg = MessageBuilder::broadcast(1).status(StatusCode::OK).build_with_payload(
        &serde_json::json!({
            "type": "system_announcement",
            "message": "Server maintenance scheduled for tonight"
        }),
    )?;
    println!(
        "  📢 Created BROADCAST message: {} bytes",
        broadcast_msg.to_bytes()?.len()
    );

    // 5. Keep-alive messages
    let ping_msg = MessageBuilder::ping_message(1000).build()?;
    println!(
        "  🏓 Created PING message: {} bytes",
        ping_msg.to_bytes()?.len()
    );

    let pong_msg = MessageBuilder::pong_message(2000, 1700000000123).build()?;
    println!(
        "  🏓 Created PONG message: {} bytes",
        pong_msg.to_bytes()?.len()
    );

    println!();
    Ok(())
}

fn demonstrate_lazy_parsing() -> ProtocolResult<()> {
    println!("⚡ Lazy Parsing Examples:");

    // Create a message with payload
    let chat_msg = ChatMessage {
        user_id:   123,
        message:   "Hello, FlexiMQ!".to_string(),
        timestamp: 1700000000,
    };

    let raw_msg =
        MessageBuilder::simple_publish(1000, "chat.general").build_with_payload(&chat_msg)?;

    let bytes = raw_msg.to_bytes()?;
    println!("  💾 Message size: {} bytes", bytes.len());

    // Parse message with lazy evaluation
    let message = Message::from_bytes(bytes)?;

    // Base header is immediately available (no parsing overhead)
    println!("  🚀 Immediate access:");
    println!("    - Message type: {:?}", message.message_type());
    println!("    - Client ID: {}", message.client_id());
    println!("    - Header length: {} bytes", message.header_length());
    println!("    - Payload length: {} bytes", message.payload_length());

    // Check cache status (nothing parsed yet)
    println!("  💾 Cache status:");
    println!("    - Header cached: {}", message.is_header_cached());
    println!("    - Payload cached: {}", message.is_payload_cached());

    // Parse header only when needed (lazy)
    let header = message.header()?;
    println!("  📋 After accessing header:");
    println!("    - Topic: {:?}", header.topic);
    println!("    - Header cached: {}", message.is_header_cached());
    println!("    - Payload cached: {}", message.is_payload_cached());

    // Parse payload only when needed (lazy)
    let parsed_chat: Option<ChatMessage> = message.payload_as()?;
    println!("  💬 After accessing payload:");
    println!("    - Message: {:?}", parsed_chat.unwrap().message);
    println!("    - Header cached: {}", message.is_header_cached());
    println!("    - Payload cached: {}", message.is_payload_cached());

    println!();
    Ok(())
}

fn demonstrate_streaming_parsing() -> ProtocolResult<()> {
    println!("🌊 Streaming Parser Examples:");

    // Create multiple messages
    let messages = vec![
        MessageBuilder::simple_publish(1000, "topic1").build()?,
        MessageBuilder::simple_subscribe(1001, "topic2").build()?,
        MessageBuilder::ping_message(1002).build()?,
    ];

    // Simulate network stream with partial data
    let mut all_bytes = Vec::new();
    for msg in &messages {
        all_bytes.extend_from_slice(&msg.to_bytes()?);
    }

    let mut parser = MessageParser::new();
    let mut parsed_count = 0;

    // Simulate receiving data in chunks
    let chunk_size = 20; // Small chunks to test streaming
    for (i, chunk) in all_bytes.chunks(chunk_size).enumerate() {
        println!("  📦 Processing chunk {}: {} bytes", i + 1, chunk.len());

        parser.push_data(chunk)?;

        // Try to parse messages from accumulated data
        let parsed_messages = parser.parse_all()?;
        for msg in parsed_messages {
            parsed_count += 1;
            println!(
                "    ✅ Parsed message #{}: {:?} from client {}",
                parsed_count,
                msg.message_type(),
                msg.client_id()
            );
        }

        println!("    📊 Buffer size: {} bytes", parser.buffer_size());
    }

    println!("  🎯 Total messages parsed: {parsed_count}");
    println!("  📊 Final buffer size: {} bytes", parser.buffer_size());

    println!();
    Ok(())
}

fn demonstrate_zero_copy() -> ProtocolResult<()> {
    println!("🔄 Zero-Copy Operations:");

    let original_data = serde_json::json!({
        "large_data": "x".repeat(1000),
        "metadata": {
            "version": "1.0",
            "timestamp": 1700000000
        }
    });

    let raw_msg =
        MessageBuilder::simple_publish(1000, "data.stream").build_with_payload(&original_data)?;

    let bytes = raw_msg.to_bytes()?;
    let message = Message::from_bytes(bytes)?;

    println!("  📏 Original message size: {} bytes", message.total_size());

    // Zero-copy access to raw sections
    let raw_header = message.raw_header_bytes();
    let raw_payload = message.raw_payload_bytes();

    println!("  🧩 Raw sections (zero-copy):");
    println!("    - Header bytes: {} bytes", raw_header.len());
    println!("    - Payload bytes: {} bytes", raw_payload.len());

    // Fast message info extraction without full parsing
    let raw_bytes = message.raw_bytes();
    let (msg_type, client_id) = peek_message_info(raw_bytes)?;
    println!("  🔍 Quick inspection (no parsing):");
    println!("    - Message type: {msg_type:?}");
    println!("    - Client ID: {client_id}");

    // Demonstrate cache management
    println!("  🗄️ Cache management:");
    let _header = message.header()?; // Parse and cache
    println!("    - Header cached: {}", message.is_header_cached());

    message.clear_cache(); // Free memory
    println!("    - After clear: {}", message.is_header_cached());

    println!();
    Ok(())
}

fn demonstrate_validation() -> ProtocolResult<()> {
    println!("✅ Protocol Validation Examples:");

    // Valid message examples
    println!("  ✅ Valid messages:");

    let valid_req = MessageBuilder::request(1000, "client-source", "/api/test", Some(2000))
        .as_request("req-123")
        .build();

    match valid_req {
        Ok(_) => println!("    ✅ Valid REQUEST message created"),
        Err(e) => println!("    ❌ Unexpected error: {e}"),
    }

    let valid_sub = MessageBuilder::subscribe(1000).with_topic("valid.topic").build();

    match valid_sub {
        Ok(_) => println!("    ✅ Valid SUBSCRIBE message created"),
        Err(e) => println!("    ❌ Unexpected error: {e}"),
    }

    // Invalid message examples
    println!("  ❌ Invalid messages (expected failures):");

    // REQ message missing routing
    let invalid_req = MessageBuilder::new(MessageType::Request, 1000).as_request("req-123").build();

    match invalid_req {
        Ok(_) => println!("    ❌ Should have failed!"),
        Err(e) => println!("    ✅ Correctly rejected: {e}"),
    }

    // SUB message with forbidden status field
    let invalid_sub = MessageBuilder::subscribe(1000)
        .with_topic("test.topic")
        .status(StatusCode::OK) // Forbidden for SUB
        .build();

    match invalid_sub {
        Ok(_) => println!("    ❌ Should have failed!"),
        Err(e) => println!("    ✅ Correctly rejected: {e}"),
    }

    println!();
    Ok(())
}

fn demonstrate_message_correlation() -> ProtocolResult<()> {
    println!("🔗 Message Correlation Examples:");

    // Create a request
    let request = MessageBuilder::request(1000, "client-source", "/api/data", Some(2000))
        .as_request("correlation-123")
        .build_with_payload(&serde_json::json!({
            "query": "SELECT * FROM users"
        }))?;

    let request_bytes = request.to_bytes()?;
    let request_msg = Message::from_bytes(request_bytes)?;

    println!("  📤 Created request with correlation ID: correlation-123");

    // Create a correlated response
    let response_data = serde_json::json!({
        "results": [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ],
        "count": 2
    });

    // Creating a response from the request message
    let response = request_msg.create_response(
        2000, // Responder client ID
        Some(StatusCode::OK),
        Some(&response_data),
    )?;

    println!("  📥 Created correlated response");

    // Verify correlation
    let response_msg = response.into_message()?;
    let response_header = response_msg.header()?;

    if let Some(reqrep) = &response_header.reqrep {
        println!("  🔗 Response correlation:");
        println!("    - Type: {:?}", reqrep.req_type);
        println!("    - ID: {}", reqrep.id);
        println!("    - Matches request: {}", reqrep.id == "correlation-123");
    }

    if let Some(status) = &response_header.status {
        println!(
            "  📊 Response status: {} ({})",
            status.0,
            if status.is_success() {
                "Success"
            } else {
                "Error"
            }
        );
    }

    println!();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_usage_example() {
        // Run the main function to ensure all examples work
        main().expect("Basic usage examples should complete successfully");
    }

    #[test]
    fn test_message_roundtrip() {
        let original = ChatMessage {
            user_id:   123,
            message:   "Test message".to_string(),
            timestamp: 1700000000,
        };

        let raw_msg = MessageBuilder::simple_publish(1000, "test.topic")
            .build_with_payload(&original)
            .unwrap();

        let bytes = raw_msg.to_bytes().unwrap();
        let message = Message::from_bytes(bytes).unwrap();

        let parsed: Option<ChatMessage> = message.payload_as().unwrap();
        let parsed = parsed.unwrap();

        assert_eq!(original.user_id, parsed.user_id);
        assert_eq!(original.message, parsed.message);
        assert_eq!(original.timestamp, parsed.timestamp);
    }

    #[test]
    fn test_protocol_constants() {
        assert_eq!(PROTOCOL_VERSION, 1);
        assert_eq!(CLIENT_ID_UNASSIGNED, 0);
        assert_eq!(CLIENT_ID_BROKER, 1);
        assert_eq!(BASE_HEADER_SIZE, 34);
    }
}
