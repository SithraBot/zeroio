# fleximq-transport

Transport layer implementation for the fleximq protocol, providing multiple transport mechanisms with a unified interface.

## Features

- **Multiple Transport Types**:

  - **TCP**: Traditional TCP socket connections
  - **IPC**: Inter-process communication (Unix domain sockets on Linux/macOS, Named pipes on Windows)
  - **STDIO**: Standard input/output for subprocess communication
  - **WebSocket**: WebSocket protocol (placeholder implementation)

- **Cross-Platform Support**: IPC transport works on both Unix-like systems and Windows
- **Async/Await**: Built on Tokio for high-performance async I/O
- **Transport Manager**: Unified interface for managing multiple transport types
- **Connection Caching**: LRU cache for frequently used connections (infrastructure in place)

## Transport URLs

- **TCP**: `tcp://hostname:port` (e.g., `tcp://localhost:8080`)
- **IPC**:
  - Unix: `ipc://path/to/socket` (e.g., `ipc://fleximq.sock`)
  - Windows: `ipc://pipe_name` (converted to `\\.\pipe\pipe_name`)
- **STDIO**: `stdio://command args` (e.g., `stdio://python script.py`)
- **WebSocket**: `ws://hostname:port/path` or `wss://` for secure (not yet implemented)

## Architecture

The transport layer follows a modular design:

1. **Transport Trait**: Core abstraction for all transport types
2. **TransportStream Trait**: Unified interface for reading/writing data
3. **TransportManager**: Manages multiple transport implementations
4. **Type Erasure**: Allows dynamic dispatch of different transport types

## Future Enhancements

- TLS/SSL support for secure connections
- Unix domain socket support on Windows 10+
- Automatic reconnection with exponential backoff
