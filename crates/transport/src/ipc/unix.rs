//! Unix-specific IPC implementation using Unix domain sockets

use std::path::Path;

use tokio::net::{UnixListener, UnixStream};

use crate::error::{TransportError, TransportResult};

/// Platform-specific stream type for Unix
pub type PlatformStream = UnixStream;

/// Platform-specific listener type for Unix
pub struct PlatformListener {
    listener: UnixListener,
}

impl PlatformListener {
    pub async fn accept(&self) -> TransportResult<PlatformStream> {
        let (stream, _) = self.listener.accept().await?;
        Ok(stream)
    }

    pub fn close(path: &Path) {
        // UnixListener doesn't have a close method, cleanup happens on drop
        // We attempt to remove the socket file as a best effort.
        if path.exists() {
            if let Err(e) = std::fs::remove_file(path) {
                log::error!(
                    "Failed to remove IPC socket file '{}': {}",
                    path.display(),
                    e
                );
            }
        }
    }
}

/// Connect to a Unix domain socket
pub async fn connect(path: &Path) -> TransportResult<PlatformStream> {
    UnixStream::connect(path).await.map_err(|e| {
        TransportError::ConnectionFailure(format!("Failed to connect to {}: {}", path.display(), e))
    })
}

/// Create a Unix domain socket listener
pub fn listen(path: &Path) -> TransportResult<PlatformListener> {
    // Remove existing socket file if it exists
    if path.exists() {
        std::fs::remove_file(path).ok();
    }

    // Create parent directory if needed and it doesn't exist
    // This handles custom paths like /var/run/myapp/ or /tmp/nested/path/
    if let Some(parent) = path.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let listener = UnixListener::bind(path).map_err(|e| {
        TransportError::ConnectionFailure(format!("Failed to bind to {}: {}", path.display(), e))
    })?;

    Ok(PlatformListener { listener })
}
