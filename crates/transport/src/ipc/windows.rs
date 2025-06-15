//! Windows-specific IPC implementation using named pipes

use std::{fmt::Debug, path::Path, pin::Pin};

use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::windows::named_pipe::{ClientOptions, NamedPipeServer, ServerOptions},
};

use super::IpcConfig;
use crate::error::{TransportError, TransportResult};

/// Platform-specific stream type for Windows
pub type PlatformStream = Pin<Box<dyn PlatformStreamTrait>>;

pub trait PlatformStreamTrait: AsyncRead + AsyncWrite + Debug + Send + Sync {}
impl<T> PlatformStreamTrait for T where T: AsyncRead + AsyncWrite + Debug + Send + Sync {}

/// Platform-specific listener type for Windows
pub struct PlatformListener {
    server:    NamedPipeServer,
    pipe_name: String,
    config:    IpcConfig,
}

impl PlatformListener {
    pub async fn accept(&mut self) -> TransportResult<PlatformStream> {
        let new_server = ServerOptions::new()
            .first_pipe_instance(false)
            .in_buffer_size(self.config.buffer_size as u32)
            .out_buffer_size(self.config.buffer_size as u32)
            .max_instances(1)
            .create(&self.pipe_name)
            .map_err(|e| {
                TransportError::ConnectionFailure(format!("Error: {e} (path: {})", self.pipe_name))
            })?;

        let old_server = std::mem::replace(&mut self.server, new_server);
        old_server.connect().await?;

        Ok(Box::pin(old_server))
    }

    pub const fn close(_path: &Path) {
        // Named pipe server closes when dropped
    }
}

/// Connect to a Windows named pipe
#[allow(clippy::unused_async)]
pub async fn connect(path: &Path, _config: &IpcConfig) -> TransportResult<PlatformStream> {
    let pipe_name = format_pipe_name(path);
    let client = ClientOptions::new().open(&pipe_name).map_err(|e| {
        TransportError::ConnectionFailure(format!("Failed to connect to {pipe_name}: {e}"))
    })?;
    Ok(Box::pin(client))
}

/// Create a Windows named pipe listener
pub fn listen(path: &Path, config: &IpcConfig) -> TransportResult<PlatformListener> {
    let pipe_name = format_pipe_name(path);

    #[allow(clippy::cast_possible_truncation)]
    let server = ServerOptions::new()
        .first_pipe_instance(true)
        .in_buffer_size(config.buffer_size as u32)
        .out_buffer_size(config.buffer_size as u32)
        .max_instances(254)
        .create(&pipe_name)
        .map_err(|e| {
            TransportError::ConnectionFailure(format!(
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
