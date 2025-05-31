use serde::{Deserialize, Serialize};
use zeroio_protocol::{Auth, Message, MessageBuilder, MessageDecode, StatusCode};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct SimplePayload {
    message: String,
    value:   i32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("ZeroIO Protocol V1 Examples\n");

    // 1. Join Message (new in V1)
    let join_encoded =
        MessageBuilder::simple_join_with_token("my-token".to_string()).build_vec()?;
    let _join_message = Message::from_buffer(&join_encoded)?;
    println!("1. Join: {} bytes", join_encoded.len());

    // 2. Broadcast Message
    let payload = SimplePayload {
        message: "Server maintenance".to_string(),
        value:   42,
    };

    let encoded = MessageBuilder::simple_broadcast(1001).with_payload(&payload)?.build_vec()?;
    let message = Message::from_buffer(&encoded)?;
    let decoded: SimplePayload = message.payload()?;

    println!("2. Broadcast: {} bytes -> {:?}", encoded.len(), decoded);

    // 3. Request/Response
    let request = SimplePayload {
        message: "Get user data".to_string(),
        value:   123,
    };

    let request_encoded =
        MessageBuilder::simple_request(2001, 3001, "/api/users".to_string(), "req_001".to_string())
            .with_payload(&request)?
            .build_vec()?;

    let request_msg = Message::from_buffer(&request_encoded)?;
    let request_decoded: SimplePayload = request_msg.payload()?;

    let response = SimplePayload {
        message: "User data".to_string(),
        value:   456,
    };

    let response_encoded = MessageBuilder::simple_response(
        3001,
        2001,
        "/api/users".to_string(),
        "req_001".to_string(),
        StatusCode::Ok,
    )
    .with_payload(&response)?
    .build_vec()?;

    let response_msg = Message::from_buffer(&response_encoded)?;
    let response_decoded: SimplePayload = response_msg.payload()?;

    println!(
        "3. Request: {:?} -> Response: {:?}",
        request_decoded, response_decoded
    );

    // 4. Notification
    let notification = SimplePayload {
        message: "User logged in".to_string(),
        value:   789,
    };

    let notif_encoded = MessageBuilder::simple_notification(4001, 5001, "/events".to_string())
        .with_payload(&notification)?
        .build_vec()?;

    let notif_msg = Message::from_buffer(&notif_encoded)?;
    let notif_decoded: SimplePayload = notif_msg.payload()?;

    println!("4. Notification: {:?}", notif_decoded);

    // 5. Multiple Routing
    let alert = SimplePayload {
        message: "Security alert".to_string(),
        value:   999,
    };

    let multi_encoded = MessageBuilder::notification(6001)
        .with_route(7001, "/security".to_string())
        .with_route(7002, "/logging".to_string())
        .with_payload(&alert)?
        .build_vec()?;

    let multi_msg = Message::from_buffer(&multi_encoded)?;
    let multi_decoded: SimplePayload = multi_msg.payload()?;
    let header = multi_msg.header()?;
    let route_count = header.routing.as_ref().map_or(0, |r| r.len());

    println!(
        "5. Multi-route ({} targets): {:?}",
        route_count, multi_decoded
    );

    // 6. Topic Message (new in V1)
    let topic_payload = SimplePayload {
        message: "Topic update".to_string(),
        value:   123,
    };

    let topic_encoded = MessageBuilder::simple_topic(8001, "news".to_string())
        .with_payload(&topic_payload)?
        .build_vec()?;
    let topic_msg = Message::from_buffer(&topic_encoded)?;
    let topic_decoded: SimplePayload = topic_msg.payload()?;
    println!("6. Topic: {:?}", topic_decoded);

    // 7. Subscribe/Unsubscribe (new in V1)
    let sub_encoded = MessageBuilder::simple_subscribe(8002, "news".to_string()).build_vec()?;
    let _sub_msg = Message::from_buffer(&sub_encoded)?;
    println!("7. Subscribe to topic: news");

    let unsub_encoded = MessageBuilder::simple_unsubscribe(8002, "news".to_string()).build_vec()?;
    let _unsub_msg = Message::from_buffer(&unsub_encoded)?;
    println!("8. Unsubscribe from topic: news");

    // 9. Ping/Pong Messages (new keep-alive feature)
    let timestamp =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64;

    let ping_encoded = MessageBuilder::ping_with_interval(9001, timestamp, 30).build_vec()?;
    let _ping_msg = Message::from_buffer(&ping_encoded)?;
    println!("9. Ping: timestamp={}", timestamp);

    let pong_encoded = MessageBuilder::pong(9002, timestamp).build_vec()?;
    let _pong_msg = Message::from_buffer(&pong_encoded)?;
    println!("10. Pong: timestamp={}", timestamp);

    // 11. Raw Binary Data
    let binary_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let raw_encoded = MessageBuilder::broadcast(8001)
        .with_raw_payload(binary_data.clone())
        .build_vec()?;

    let raw_msg = Message::from_buffer(&raw_encoded)?;
    println!("11. Raw binary: {:02X?}", raw_msg.payload_bytes());

    // 12. Authentication Examples
    let basic_auth_encoded = MessageBuilder::simple_join()
        .with_auth(Auth::Basic {
            username: "user123".to_string(),
            password: "pass123".to_string(),
        })
        .build_vec()?;
    let _basic_msg = Message::from_buffer(&basic_auth_encoded)?;
    println!("12. Basic auth join");

    let api_key_encoded = MessageBuilder::simple_join()
        .with_auth(Auth::ApiKey {
            api_key: "sk-1234567890abcdef".to_string(),
        })
        .build_vec()?;
    let _api_msg = Message::from_buffer(&api_key_encoded)?;
    println!("13. API key auth join");

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
