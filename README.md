# zeroio

A high-performance communication library for distributed applications, primarily designed for stdin/stdout communication in Rust.

## Overview

zeroio is a lightweight communication library for distributed applications. It uses a centralized broker architecture where all clients connect to handle message routing and delivery. The protocol is designed primarily for stdin/stdout communication but can work across different transport layers.

## Features

- **stdin/stdout communication** (primary focus)
- **Multiple transport layers**: IPC and TCP support planned
- **MessagePack serialization** for efficient data encoding
- **Four message types**: Request/Response, Notification, and Broadcast
- **Centralized routing** through broker
- **Cross-platform binary format** with network byte order

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
// API under development - examples coming soon
todo!()
```

## Transport Support

### Primary Transport

- **Standard I/O**: Direct stdin/stdout communication (main focus)

### Planned Transports

- **IPC**: [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) for inter-process communication
- **TCP**: Network communication for distributed systems
- **WebSocket**: Browser and web application support

## Architecture

**Centralized broker architecture** - all clients connect to a central broker for message routing:

```
Client A ──┐
Client B ──┼── Broker ── Message Router ── Transport Layer
Client C ──┘
```

## Performance

- Optimized for stdin/stdout communication
- MessagePack serialization
- Async/await support (planned)
- Minimal memory allocation

## Use Cases

- Process communication via stdin/stdout
- Microservices coordination
- Plugin systems
- Data processing pipelines
- Development tool integration

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