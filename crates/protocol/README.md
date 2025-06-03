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

## Best Practices

1. **Use lazy parsing** for routing scenarios where you only need basic message info
2. **Clear caches** for long-lived messages to free memory
3. **Set appropriate size limits** based on your use case
4. **Validate messages** early using the builder API
5. **Use streaming parsers** for network protocols
6. **Leverage zero-copy operations** when forwarding messages

## License

This implementation follows the fleximq Protocol v1.0.0 specification.
