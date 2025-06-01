//! STDIO transport implementation for subprocess communication

use std::{
    io,
    pin::Pin,
    process::Stdio,
    task::{Context, Poll},
};

use async_trait::async_trait;
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
    process::{Child, ChildStdin, ChildStdout, Command},
};

use crate::{
    error::{TransportError, TransportResult},
    traits::{ConnectionInfo, Transport, TransportListener, TransportStream},
};

/// STDIO transport implementation
#[derive(Debug, Clone)]
pub struct StdioTransport {
    /// Configuration for the subprocess managed by the STDIO transport.
    config: StdioConfig,
}

#[derive(Debug, Clone, Default)]
pub struct StdioConfig {
    /// Command to execute
    pub command:     String,
    /// Command arguments
    pub args:        Vec<String>,
    /// Environment variables
    pub env:         Vec<(String, String)>,
    /// Working directory
    pub working_dir: Option<String>,
}

impl Default for StdioTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl StdioTransport {
    /// Creates a new STDIO transport with default configuration (no command
    /// specified initially).
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: StdioConfig::default(),
        }
    }

    /// Creates a new STDIO transport with the specified subprocess
    /// configuration.
    #[must_use]
    pub fn with_config(config: StdioConfig) -> Self {
        Self { config }
    }

    /// Creates a new STDIO transport configured to run the specified command
    /// and arguments.
    #[must_use]
    pub fn with_command(command: String, args: Vec<String>) -> Self {
        Self {
            config: StdioConfig {
                command,
                args,
                ..Default::default()
            },
        }
    }
}

/// A stream representing communication with a subprocess via its standard input
/// and output.
pub struct StdioTransportStream {
    stdin:  ChildStdin,
    stdout: ChildStdout,
    child:  Child,
    info:   ConnectionInfo,
}

impl StdioTransportStream {
    /// Constructs a new StdioTransportStream from a spawned child process.
    fn new(mut child: Child, command: &str) -> TransportResult<Self> {
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| TransportError::ConnectionFailed("Failed to get stdin".to_string()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| TransportError::ConnectionFailed("Failed to get stdout".to_string()))?;

        let pid = child.id().unwrap_or(0);
        let info = ConnectionInfo {
            transport_type: "stdio".to_string(),
            local_addr:     Some(format!("stdio://process/{pid}")),
            remote_addr:    Some(command.to_string()),
            metadata:       std::collections::HashMap::new(),
        };

        Ok(Self {
            stdin,
            stdout,
            child,
            info,
        })
    }

    /// Checks if the underlying child process is still running.
    pub fn is_running(&mut self) -> bool {
        matches!(self.child.try_wait(), Ok(None))
    }
}

#[async_trait]
impl TransportStream for StdioTransportStream {
    fn connection_info(&self) -> &ConnectionInfo {
        &self.info
    }

    /// Checks if the STDIO stream is considered active.
    /// This typically means the stream object exists, as checking the live
    /// status of the child process without mutable access is non-trivial
    /// for this method.
    fn is_connected(&self) -> bool {
        // We can't easily check if the child is still running without mutability here.
        // Assume connected if the stream object exists and hasn't been explicitly
        // closed.
        true
    }

    /// Closes the communication with the subprocess.
    ///
    /// This involves shutting down the subprocess's stdin, waiting for it to
    /// exit gracefully (with a timeout), and killing it if the timeout is
    /// reached.
    ///
    /// # Errors
    ///
    /// Returns `TransportError::Io` if an error occurs during stdin shutdown,
    /// waiting for the child, or killing it.
    async fn close(&mut self) -> TransportResult<()> {
        // Close stdin to signal end of input to the child process.
        self.stdin.shutdown().await?;

        // Wait for child to exit gracefully, with a timeout.
        match tokio::time::timeout(std::time::Duration::from_secs(5), self.child.wait()).await {
            Ok(Ok(_exit_status)) => Ok(()), // Process exited gracefully.
            Ok(Err(e)) => Err(TransportError::Io(e)), // Error waiting for process.
            Err(_) => {
                // Timeout reached, forcefully kill the process.
                self.child.kill().await?;
                Ok(())
            }
        }
    }
}

impl AsyncRead for StdioTransportStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdout).poll_read(cx, buf)
    }
}

impl AsyncWrite for StdioTransportStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.stdin).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stdin).poll_shutdown(cx)
    }
}

/// A transport listener for STDIO.
///
/// STDIO transport is typically client-initiated (connecting to a subprocess)
/// and does not support traditional server-side listening for incoming
/// connections. Therefore, methods like `accept` will return
/// `TransportError::NotSupported`.
pub struct StdioTransportListener;

#[async_trait]
impl TransportListener for StdioTransportListener {
    type Stream = StdioTransportStream;

    /// Attempts to accept a new incoming connection.
    /// This is not supported for STDIO transport and will always return
    /// `TransportError::NotSupported`.
    async fn accept(&mut self) -> TransportResult<Self::Stream> {
        Err(TransportError::NotSupported(
            "STDIO transport does not support listening for incoming connections.".to_string(),
        ))
    }

    /// Gets the local address.
    /// This is not applicable/supported for STDIO listeners and will always
    /// return `TransportError::NotSupported`.
    fn local_addr(&self) -> TransportResult<String> {
        Err(TransportError::NotSupported(
            "Local address is not applicable for STDIO listeners.".to_string(),
        ))
    }

    /// Closes the listener.
    /// This is a no-op for STDIO transport as there are no active listening
    /// resources to release.
    async fn close(&mut self) -> TransportResult<()> {
        Ok(())
    }
}

#[async_trait]
impl Transport for StdioTransport {
    type Stream = StdioTransportStream;

    /// Connects to a subprocess by spawning it and using its standard
    /// input/output for communication.
    ///
    /// The `url` parameter determines the command to be executed:
    /// - If `url` is exactly "stdio://", the command configured in
    ///   `StdioTransport::config` is used. If no command is configured,
    ///   `TransportError::InvalidUrl` is returned.
    /// - If `url` starts with "stdio://" followed by a command and arguments
    ///   (e.g., "stdio://my_command --arg1 value1"), that command is executed,
    ///   overriding any configured command.
    /// - Otherwise, `TransportError::InvalidUrl` is returned.
    ///
    /// The subprocess's stderr is inherited by the parent process.
    ///
    /// # Errors
    ///
    /// Returns `TransportError::InvalidUrl` if the URL format is invalid or no
    /// command can be determined.
    /// Returns `TransportError::ConnectionFailed` if the subprocess fails to
    /// spawn (e.g., command not found, permissions).
    /// Returns `TransportError::ConnectionFailed` if the stdin/stdout handles
    /// cannot be obtained from the child process.
    async fn connect(&self, url: &str) -> TransportResult<Self::Stream> {
        // Parse the URL to determine the command and arguments.
        // STDIO URLs are special: "stdio://" uses configured command, "stdio://cmd
        // args..." overrides.
        let (command_to_run, args_to_use) = if url == "stdio://" {
            // Use command from StdioConfig if url is just "stdio://"
            if self.config.command.is_empty() {
                return Err(TransportError::InvalidUrl(
                    "STDIO URL is \"stdio://\" but no command is configured in StdioTransport."
                        .to_string(),
                ));
            }
            (self.config.command.clone(), self.config.args.clone())
        } else if let Some(cmd_part) = url.strip_prefix("stdio://") {
            // Parse command and arguments from the URL string itself.
            let parts: Vec<&str> = cmd_part.split_whitespace().collect();
            if parts.is_empty() || parts[0].is_empty() {
                return Err(TransportError::InvalidUrl(
                    "Invalid command specified in STDIO URL (e.g., empty or missing after \
                     stdio://)."
                        .to_string(),
                ));
            }
            (
                parts[0].to_string(),
                parts[1..].iter().map(|s| s.to_string()).collect(),
            )
        } else {
            return Err(TransportError::InvalidUrl(format!(
                "Invalid STDIO URL scheme. Expected \"stdio://\" or \"stdio://command...\", got: \
                 {url}"
            )));
        };

        // Build the command to spawn.
        let mut cmd = Command::new(&command_to_run);
        cmd.args(&args_to_use)
            .stdin(Stdio::piped())  // Capture stdin
            .stdout(Stdio::piped()) // Capture stdout
            .stderr(Stdio::inherit()); // Let child's stderr pass through to parent's stderr.

        // Set working directory if specified in config.
        if let Some(ref dir) = self.config.working_dir {
            cmd.current_dir(dir);
        }

        // Set environment variables from config.
        for (key, value) in &self.config.env {
            cmd.env(key, value);
        }

        // Spawn the child process.
        let child = cmd.spawn().map_err(|e| {
            TransportError::ConnectionFailed(format!(
                "Failed to spawn subprocess '{command_to_run}': {e:?}"
            ))
        })?;

        StdioTransportStream::new(child, &command_to_run)
    }

    /// Attempts to listen for incoming connections.
    /// This is not supported by the STDIO transport, as it is designed for
    /// client-initiated communication with a subprocess, not for accepting
    /// connections from other processes.
    ///
    /// # Errors
    ///
    /// Always returns `TransportError::NotSupported`.
    async fn listen(
        &self,
        _url: &str, // The URL is ignored as listening is not supported.
    ) -> TransportResult<Box<dyn TransportListener<Stream = Self::Stream>>> {
        Err(TransportError::NotSupported(
            "STDIO transport does not support listening (it is client-initiated only).".to_string(),
        ))
    }

    /// Returns the transport type name ("stdio").
    fn transport_type(&self) -> &str {
        "stdio"
    }

    /// Checks if this transport supports the given URL (i.e., starts with
    /// "stdio://").
    fn supports_url(&self, url: &str) -> bool {
        url.starts_with("stdio://")
    }
}
