use serde::{Deserialize, Serialize};
use zeroio::decode::Message;
use zeroio::encode::MessageBuilder;

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct SimplePayload {
    message: String,
    value: i32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("ZeroIO Protocol V1 Examples\n");

    // 1. Broadcast Message
    let payload = SimplePayload {
        message: "Server maintenance".to_string(),
        value: 42,
    };

    let encoded = MessageBuilder::simple_broadcast(1001, &payload)?;
    let message = Message::from_buffer(&encoded)?;
    let decoded: SimplePayload = message.payload()?;
    
    println!("1. Broadcast: {} bytes -> {:?}", encoded.len(), decoded);

    // 2. Request/Response
    let request = SimplePayload {
        message: "Get user data".to_string(),
        value: 123,
    };

    let request_encoded = MessageBuilder::simple_request(
        2001, 3001, "/api/users".to_string(), "req_001".to_string(), &request
    )?;

    let request_msg = Message::from_buffer(&request_encoded)?;
    let request_decoded: SimplePayload = request_msg.payload()?;

    let response = SimplePayload {
        message: "User data".to_string(),
        value: 456,
    };

    let response_encoded = MessageBuilder::simple_response(
        3001, 2001, "/api/users".to_string(), "req_001".to_string(), &response
    )?;

    let response_msg = Message::from_buffer(&response_encoded)?;
    let response_decoded: SimplePayload = response_msg.payload()?;

    println!("2. Request: {:?} -> Response: {:?}", request_decoded, response_decoded);

    // 3. Notification
    let notification = SimplePayload {
        message: "User logged in".to_string(),
        value: 789,
    };

    let notif_encoded = MessageBuilder::simple_notification(
        4001, 5001, "/events".to_string(), &notification
    )?;

    let notif_msg = Message::from_buffer(&notif_encoded)?;
    let notif_decoded: SimplePayload = notif_msg.payload()?;

    println!("3. Notification: {:?}", notif_decoded);

    // 4. Multiple Routing
    let alert = SimplePayload {
        message: "Security alert".to_string(),
        value: 999,
    };

    let multi_encoded = MessageBuilder::notification(6001)
        .with_route(7001, "/security".to_string())
        .with_route(7002, "/logging".to_string())
        .with_payload(&alert)?
        .build()?;

    let multi_msg = Message::from_buffer(&multi_encoded)?;
    let multi_decoded: SimplePayload = multi_msg.payload()?;
    let header = multi_msg.header()?;
    let route_count = header.routing.as_ref().map_or(0, |r| r.len());

    println!("4. Multi-route ({} targets): {:?}", route_count, multi_decoded);

    // 5. Raw Binary Data
    let binary_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let raw_encoded = MessageBuilder::broadcast(8001)
        .with_raw_payload(binary_data.clone())
        .build()?;

    let raw_msg = Message::from_buffer(&raw_encoded)?;
    println!("5. Raw binary: {:02X?}", raw_msg.payload_bytes());

    println!("\nAll examples completed successfully!");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_examples() {
        main().unwrap();
    }
}