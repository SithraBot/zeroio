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

## Usage

```rust
use fleximq_transport::{TransportManager, TransportStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create transport manager
    let manager = TransportManager::new();
    
    // Connect using TCP
    let mut tcp_stream = manager.connect("tcp://localhost:8080").await?;
    tcp_stream.write_all(b"Hello, TCP!").await?;
    
    // Connect using IPC
    let ipc_stream = manager.connect("ipc:///tmp/my_socket").await?;
    
    // Spawn subprocess using STDIO
    let mut stdio_stream = manager.connect("stdio://echo Hello World").await?;
    let mut output = Vec::new();
    stdio_stream.read_to_end(&mut output).await?;
    
    Ok(())
}
```

## Transport URLs

- **TCP**: `tcp://hostname:port` (e.g., `tcp://localhost:8080`)
- **IPC**: 
  - Unix: `ipc:///path/to/socket` (e.g., `ipc:///tmp/fleximq.sock`)
  - Windows: `ipc://pipe_name` (converted to `\\.\pipe\pipe_name`)
- **STDIO**: `stdio://command args` (e.g., `stdio://python script.py`)
- **WebSocket**: `ws://hostname:port/path` or `wss://` for secure (not yet implemented)

## Architecture

The transport layer follows a modular design:

1. **Transport Trait**: Core abstraction for all transport types
2. **TransportStream Trait**: Unified interface for reading/writing data
3. **TransportManager**: Manages multiple transport implementations
4. **Type Erasure**: Allows dynamic dispatch of different transport types

## Configuration

### TCP Transport
```rust
use fleximq_transport::tcp::{TcpTransport, TcpConfig};

let config = TcpConfig {
    nodelay: true,
    keepalive: Some(Duration::from_secs(30)),
    send_buffer_size: None,
    recv_buffer_size: None,
};
let transport = TcpTransport::with_config(config);
```

### IPC Transport
```rust
use fleximq_transport::ipc::{IpcTransport, IpcConfig};

let config = IpcConfig {
    buffer_size: 65536, // 64KB
    backlog: 128,
};
let transport = IpcTransport::with_config(config);
```

### STDIO Transport
```rust
use fleximq_transport::stdio::{StdioTransport, StdioConfig};

let config = StdioConfig {
    command: "python".to_string(),
    args: vec!["script.py".to_string()],
    env: vec![("MY_VAR".to_string(), "value".to_string())],
    working_dir: Some("/path/to/dir".to_string()),
};
let transport = StdioTransport::with_config(config);
```

## Performance Optimizations

- **Connection Pooling**: Infrastructure for LRU cache-based connection reuse
- **Zero-Copy**: Leverages Tokio's efficient async I/O
- **Platform-Specific**: Uses optimal system calls for each platform

## Future Enhancements

- Full WebSocket implementation with `tokio-tungstenite`
- Connection pooling with proper interior mutability
- TLS/SSL support for secure connections
- Unix domain socket support on Windows 10+
- Performance metrics and monitoring
- Automatic reconnection with exponential backoff 
