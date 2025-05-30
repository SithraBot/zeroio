use serde::{Deserialize, Serialize};
use zeroio::decode::Message;
use zeroio::encode::{MessageBuilder, Reqrep};

#[derive(Serialize, Deserialize, Debug, PartialEq)]
struct TestData {
    msg: String,
    num: i32,
}

fn test_data() -> TestData {
    TestData {
        msg: "test".to_string(),
        num: 42,
    }
}

fn verify_roundtrip<T: Serialize + for<'a> Deserialize<'a> + PartialEq + std::fmt::Debug>(
    encoded: &[u8],
    expected_payload: &T,
    expected_client: u32,
    expected_type: zeroio::decode::MessageType,
) {
    let message = Message::from_buffer(encoded).unwrap();
    assert_eq!(message.version(), 1);
    assert_eq!(message.client_id(), expected_client);
    assert_eq!(message.message_type(), expected_type);
    
    let decoded: T = message.payload().unwrap();
    assert_eq!(&decoded, expected_payload);
}

#[test]
fn test_message_types() {
    let data = test_data();
    
    // Broadcast
    let broadcast = MessageBuilder::simple_broadcast(1001, &data).unwrap();
    verify_roundtrip(&broadcast, &data, 1001, zeroio::decode::MessageType::Broadcast);
    
    let msg = Message::from_buffer(&broadcast).unwrap();
    let header = msg.header().unwrap();
    assert!(header.routing.is_none());
    assert!(header.reqrep.is_none());
    
    // Request
    let request = MessageBuilder::simple_request(
        2001, 3001, "/api".to_string(), "req_123".to_string(), &data
    ).unwrap();
    verify_roundtrip(&request, &data, 2001, zeroio::decode::MessageType::Request);
    
    let msg = Message::from_buffer(&request).unwrap();
    let header = msg.header().unwrap();
    assert!(header.routing.is_some());
    assert!(matches!(header.reqrep, Some(Reqrep::Request(_))));
    
    // Response
    let response = MessageBuilder::simple_response(
        3001, 2001, "/api".to_string(), "req_123".to_string(), &data
    ).unwrap();
    verify_roundtrip(&response, &data, 3001, zeroio::decode::MessageType::Response);
    
    let msg = Message::from_buffer(&response).unwrap();
    let header = msg.header().unwrap();
    assert!(header.routing.is_some());
    assert!(matches!(header.reqrep, Some(Reqrep::Correlation(_))));
    
    // Notification
    let notification = MessageBuilder::simple_notification(
        4001, 5001, "/events".to_string(), &data
    ).unwrap();
    verify_roundtrip(&notification, &data, 4001, zeroio::decode::MessageType::Notification);
    
    let msg = Message::from_buffer(&notification).unwrap();
    let header = msg.header().unwrap();
    assert!(header.routing.is_some());
    assert!(header.reqrep.is_none());
}

#[test]
fn test_multiple_routing() {
    let data = test_data();
    
    let encoded = MessageBuilder::notification(6001)
        .with_route(7001, "/path1".to_string())
        .with_route(7002, "/path2".to_string())
        .with_route(7003, "/path3".to_string())
        .with_payload(&data)
        .unwrap()
        .build()
        .unwrap();
    
    verify_roundtrip(&encoded, &data, 6001, zeroio::decode::MessageType::Notification);
    
    let msg = Message::from_buffer(&encoded).unwrap();
    let header = msg.header().unwrap();
    let routing = header.routing.unwrap();
    assert_eq!(routing.len(), 3);
    assert_eq!(routing[0].0, 7001);
    assert_eq!(routing[0].1, "/path1");
    assert_eq!(routing[1].0, 7002);
    assert_eq!(routing[1].1, "/path2");
    assert_eq!(routing[2].0, 7003);
    assert_eq!(routing[2].1, "/path3");
}

#[test]
fn test_edge_cases() {
    // Empty payload
    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Empty;
    
    let empty = MessageBuilder::simple_broadcast(8001, &Empty).unwrap();
    verify_roundtrip(&empty, &Empty, 8001, zeroio::decode::MessageType::Broadcast);
    
    // Large payload
    let large_data = TestData {
        msg: "x".repeat(5000),
        num: 999,
    };
    let large = MessageBuilder::simple_broadcast(8002, &large_data).unwrap();
    verify_roundtrip(&large, &large_data, 8002, zeroio::decode::MessageType::Broadcast);
    
    // Raw binary
    let binary = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let raw = MessageBuilder::broadcast(8003)
        .with_raw_payload(binary.clone())
        .build()
        .unwrap();
    
    let msg = Message::from_buffer(&raw).unwrap();
    assert_eq!(msg.payload_bytes(), &binary);
}

#[test]
fn test_size_calculation() {
    let data = test_data();
    let builder = MessageBuilder::broadcast(9001).with_payload(&data).unwrap();
    let calculated = builder.calculate_size().unwrap();
    let actual = builder.build().unwrap();
    assert_eq!(calculated, actual.len());
}