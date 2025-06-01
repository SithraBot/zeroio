//! Windows-specific IPC implementation using named pipes

use std::path::Path;

use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};

use super::IpcConfig;
use crate::error::{TransportError, TransportResult};

/// Platform-specific stream type for Windows
pub type PlatformStream = NamedPipeClient;

/// Platform-specific listener type for Windows
pub struct PlatformListener {
    server:    NamedPipeServer,
    pipe_name: String,
    config:    IpcConfig,
}

impl PlatformListener {
    pub async fn accept(&mut self) -> TransportResult<PlatformStream> {
        // Wait for a client to connect to the current server instance
        self.server.connect().await?;

        // Create a new server instance for the next connection
        #[allow(clippy::cast_possible_truncation)]
        let new_server = ServerOptions::new()
            .first_pipe_instance(false)
            .in_buffer_size(self.config.buffer_size as u32)
            .out_buffer_size(self.config.buffer_size as u32)
            .max_instances(1)
            .create(&self.pipe_name)
            .map_err(|e| {
                TransportError::ConnectionFailed(format!("Failed to create new pipe instance: {e}"))
            })?;

        // Replace our server with the new one for future accepts
        let _connected_server = std::mem::replace(&mut self.server, new_server);

        // Create a client connection to communicate with the connected client
        let client = ClientOptions::new().open(&self.pipe_name).map_err(|e| {
            TransportError::ConnectionFailed(format!("Failed to create client stream: {e}"))
        })?;

        Ok(client)
    }

    pub async fn close(&mut self, _path: &Path) -> TransportResult<()> {
        // Named pipe server closes when dropped
        Ok(())
    }
}

/// Connect to a Windows named pipe
pub async fn connect(path: &Path, _config: &IpcConfig) -> TransportResult<PlatformStream> {
    let pipe_name = format_pipe_name(path);
    ClientOptions::new().open(&pipe_name).map_err(|e| {
        TransportError::ConnectionFailed(format!("Failed to connect to {pipe_name}: {e}"))
    })
}

/// Create a Windows named pipe listener
pub async fn listen(path: &Path, config: &IpcConfig) -> TransportResult<PlatformListener> {
    let pipe_name = format_pipe_name(path);

    #[allow(clippy::cast_possible_truncation)]
    let server = ServerOptions::new()
        .first_pipe_instance(true)
        .in_buffer_size(config.buffer_size as u32)
        .out_buffer_size(config.buffer_size as u32)
        .max_instances(1)
        .create(&pipe_name)
        .map_err(|e| {
            TransportError::ConnectionFailed(format!(
                "Failed to create named pipe {pipe_name}: {e}"
            ))
        })?;

    Ok(PlatformListener {
        server,
        pipe_name,
        config: config.clone(),
    })
}

/// Format a path as a Windows named pipe name
fn format_pipe_name(path: &Path) -> String {
    let pipe_name_cow = path.to_string_lossy();
    format!(r"\\.\pipe\{}", pipe_name_cow.replace('/', "\\"))
}
