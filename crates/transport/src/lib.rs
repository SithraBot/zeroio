//! Transport layer implementation for fleximq protocol
//!
//! Supports multiple transport types:
//! - TCP: Traditional TCP socket connections
//! - IPC: Inter-process communication via Unix domain sockets (also supports
//!   Windows)
//! - WebSocket: WebSocket protocol over TCP
//! - STDIO: Standard input/output for subprocess communication
//! - Channel: In-process channel communication
#![allow(clippy::cast_possible_truncation)]

pub mod channel;
pub mod error;
pub mod ipc;
pub mod manager;
pub mod message;
pub mod stdio;
pub mod tcp;
pub mod traits;
pub mod websocket;

// Re-export main types
pub use channel::ChannelTransport;
pub use error::{TransportError, TransportResult};
pub use ipc::{IpcConfig, IpcTransport};
pub use manager::{CacheStats, TransportManager};
pub use message::{MessageStreamExt, MessageTransportAdapter, message_transport};
pub use stdio::StdioTransport;
// Re-export transport implementations
pub use tcp::{TcpConfig, TcpTransport};
pub use traits::{
    ConnectionInfo, FramedTransport, MessageTransport, Transport, TransportFactory,
    TransportListener, TransportStream,
};
pub use websocket::{WebSocketConfig, WebSocketTransport};
