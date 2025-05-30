# zeroio

A high-performance communication library for distributed applications, primarily designed for stdin/stdout communication in Rust.

## Overview

zeroio is a lightweight, efficient library designed primarily for stdin/stdout communication between distributed applications through a centralized broker. It provides a unified protocol that can work across different transport layers while maintaining simplicity and performance. All clients connect to a central broker that handles message routing and delivery. When using stdin/stdout or TCP transports, the protocol can potentially support other programming languages beyond Rust.

## Features

- **Primary stdin/stdout Support**: Optimized for standard input/output communication
- **Multiple Transport Layers**: Support for standard input/output, IPC, with TCP support planned
- **Cross-Language Potential**: stdin/stdout and TCP transports can support other programming languages
- **Efficient Serialization**: Uses MessagePack for fast and compact data serialization
- **Message Types**: Request/Response, Notification, and Broadcast patterns
- **Flexible Routing**: Route messages to specific clients or broadcast to all
- **Network Byte Order**: Consistent cross-platform binary format

## Protocol

zeroio implements a custom binary protocol with the following message types:

- **Request (0)**: Send a request and expect a response
- **Response (1)**: Reply to a request with correlation ID
- **Notification (2)**: One-way message without response
- **Broadcast (3)**: Message sent to all connected clients

## Message Format

```
┌─────────────┬──────────────┬─────────────┬─────────────┬───────────────┬────────────┬─────────────────┬─────────────┐
│ Version (1) │ Type (1)     │ ClientID(4) │ Reserved(16)│ HeaderLen (4) │ Header (?) │ PayloadLen (8)  │ Payload (?) │
└─────────────┴──────────────┴─────────────┴─────────────┴───────────────┴────────────┴─────────────────┴─────────────┘
```

All numeric values are encoded in network byte order (big-endian).

## Quick Start

Add zeroio to your `Cargo.toml`:

```toml
[dependencies]
zeroio = "0.1"
```

### Basic Usage

```rust
// Start a broker
todo!()

// Create a client and connect to broker
todo!()

// Send a request to a specific client
todo!()

// Send a notification to a specific client  
todo!()

// Broadcast a message (through broker)
todo!()
```

### Message Routing

Route messages to specific clients:

```rust
todo!()
```

## Transport Support

### Primary Transport

- **Standard I/O**: Direct stdin/stdout communication (main focus)

### Additional Transports

- **IPC**: Unix domain sockets and named pipes
- **TCP**: Network communication for distributed systems (planned)

### Future Possibilities

- **WebSocket**: Browser and web application support
- **Cross-language support**: Other programming languages via stdin/stdout and TCP

## Architecture

zeroio is designed with a centralized broker architecture:

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client A  │    │   Client B  │    │   Client C  │
├─────────────┤    ├─────────────┤    ├─────────────┤
│ zeroio API  │    │ zeroio API  │    │ zeroio API  │
└──────┬──────┘    └──────┬──────┘    └──────┬──────┘
       │                  │                  │
       └──────────────────┼──────────────────┘
                          │
              ┌───────────▼───────────┐
              │       Broker          │
              ├───────────────────────┤
              │    Message Router     │
              ├───────────────────────┤
              │      Protocol         │
              ├───────────────────────┤
              │     Transport         │
              │   (stdin/IPC/TCP)     │
              └───────────────────────┘
```

## Performance

- **Optimized for stdin/stdout** communication patterns
- **MessagePack** for efficient serialization
- **Async/await** support for non-blocking operations
- **Minimal memory allocation** where possible
- **Connection pooling** for high-throughput scenarios

## Use Cases

- **Process Communication**: stdin/stdout based communication between applications
- **Microservices**: Lightweight communication between local services via central broker
- **Plugin Systems**: Communication between main application and plugins through message broker
- **Data Processing Pipelines**: Efficient data flow between processing stages with centralized routing
- **Development Tools**: Communication between development servers and tools via broker
- **Cross-language Applications**: Connect applications written in different languages through unified broker

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Status

🚧 **Early Development** - API may change before 1.0 release

Current focus areas:
- [x] Core protocol implementation
- [ ] Standard I/O transport (primary focus)
- [ ] IPC transport layer
- [ ] TCP transport layer
- [ ] Async runtime integration
- [ ] Cross-language binding support
- [ ] Performance optimization
- [ ] Documentation and examples