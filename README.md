# fleximq.rs (Rust Implementation of the fleximq Protocol)

`fleximq.rs` is a Rust implementation of the [fleximq protocol](https://github.com/fleximq/spec). This library aims to provide Rust developers with a modern, high-performance, and idiomatic set of tools for using the fleximq protocol in distributed applications.

For a detailed understanding of the fleximq protocol's design goals, architecture, and core concepts, please refer to the official [protocol specification repository's README](https://github.com/fleximq/spec/blob/main/README.md) ([Chinese version README_zh.md](https://github.com/fleximq/spec/blob/main/README_zh.md)).

## Features

- **Rust Implementation**: Leverages Rust's safety, concurrency, and performance for implementing the fleximq protocol.

- **Clean Architecture**: Well-defined layers for protocol handling, core logic, and API within the Rust codebase.

- **Pluggable Transport Layer**: Supports multiple transport backends (TCP, IPC, WebSocket, STDIO) for flexible deployment of Rust applications.

- **Extensible Middleware System**: Provides a Rust-based pipeline for custom processing (e.g., auth, logging, metrics).

- **Async-First with Tokio**: Deep integration with Tokio for idiomatic, high-performance asynchronous operations in Rust.

- **Memory-Efficient Design**: Employs buffer pools and Rust's ownership model to minimize allocations and manage memory safely.

- **High-Performance Broker Core**: Features an LRU cache for routing, a DashMap-based client registry, and atomic metrics in its Rust-based broker.

- **Robust Connection Management**: Includes automatic client ID assignment, connection pooling, and idle connection cleanup.

- **Concurrent by Design**: Built with Rust's concurrency primitives to maximize throughput safely.

- **Built-in Performance Metrics**: Offers atomic metrics for real-time monitoring of the library's state.

- **Production-Ready Configuration**: Allows fine-tuning of limits and timeouts for robust Rust deployments.

- **Flexible Authentication**: Supports pluggable authentication modules for the broker.

## Protocol

The fleximq protocol specification that this library implements can be found at [fleximq/spec/draft/v1.0.0.md](https://github.com/fleximq/spec/blob/main/draft/v1.0.0.md).

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone https://github.com/your-org/fleximq.git
cd fleximq
cargo test --all-features
cargo test --package fleximq --lib protocol::tests
cargo test --package fleximq --lib router::tests
```

## License

This project is licensed under the Unlicense - see the [LICENSE](LICENSE) file for details.
