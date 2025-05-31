# zeroio

zeroio is a modern, high-performance communication library designed for distributed applications. It implements a custom binary protocol with centralized broker architecture, optimized for maximum throughput and minimal memory allocation. The library follows Rust's ownership principles while providing ZeroMQ-like communication patterns.

## Features

- **Clean Architecture**: Layered design separating protocol, core, and API concerns
- **Transport Agnostic**: Pluggable transport layer supporting TCP, IPC, WebSocket, and STDIO
- **Middleware System**: Extensible processing pipeline with built-in auth, logging, metrics, and rate limiting
- **Async-First Design**: Full tokio integration with proper cancellation support
- **Memory Efficient**: Buffer pools, minimal allocations, bounded message sizes
- **High-Performance Routing**: LRU caching, DashMap-based client registry, atomic metrics
- **Connection Management**: Automatic client ID assignment, connection pooling, idle cleanup
- **Multiple Transport Layers**: STDIO (primary), TCP, IPC, WebSocket support
- **Multiple Languages**: Rust, JavaScript(coming soon), and more
- **MessagePack Serialization**: for efficient structured data encoding
- **Eight Message Types**: Join, Request/Response, Notification, Broadcast, Topic Publish, Subscribe/Unsubscribe
- **Centralized Broker Architecture**: with high-performance message routing
- **Cross-Platform Binary Protocol**: with network byte order encoding
- **ULID Correlation IDs**: for efficient request tracking
- **Concurrent Data Structures**: for maximum throughput
- **Built-in Performance Monitoring**: with atomic metrics
- **Configurable Limits**: and timeouts for production deployments
- **Authentication Support**: with pluggable auth mechanisms

## Protocol

See [draft.md](draft.md) for complete protocol specification.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone https://github.com/your-org/zeroio.git
cd zeroio
cargo test --all-features
cargo test --package zeroio --lib protocol::tests
cargo test --package zeroio --lib router::tests
```

## License

This project is licensed under the Unlicense - see the [LICENSE](LICENSE) file for details.
