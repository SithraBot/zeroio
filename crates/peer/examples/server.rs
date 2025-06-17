// use std::collections::HashMap;

use fleximq_peer::server::{RequestContext, ResponseContext, Router, ServerResult};
use fleximq_protocol::{Message, MessageBuilder, MessageType, StatusCode};

// A simple key-value store for demonstration
// type Store = std::sync::Arc<tokio::sync::RwLock<HashMap<String, String>>>;

async fn fallback_handler(ctx: RequestContext) -> ServerResult {
    let message = if ctx.message.message_type() == MessageType::Request {
        MessageBuilder::response_from_request(
            ctx.client_id,
            &ctx.message,
            Some(StatusCode::NOT_FOUND),
        )
        .build()
    } else {
        MessageBuilder::notification(ctx.client_id, "", "", Some(ctx.message.client_id()))
            .with_status(StatusCode::NOT_FOUND)
            .build()
    }?;
    Ok(Some(ResponseContext::new(message)))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    println!("Creating FlexiMQ Peer Server with comprehensive routing...");

    // Create a router with different types of handlers
    let _router = Router::new()
        // Fallback for unmatched routes
        .fallback(fallback_handler);
    Ok(())
}
