use std::task::{Context, Poll};

use fleximq_protocol::{Message, RawMessage, StatusCode};
use fleximq_transport::TransportResult;
use futures_util::future::BoxFuture;
use tower::Service;

use crate::types::{SharedTransportStream, SharedTransportStreamsExt};

/// Request context containing the incoming message and transport metadata
#[derive(Debug, Clone)]
pub struct RequestContext {
    pub message:    Message,
    pub client_id:  u32,
    pub source_url: Option<String>,
}

/// Response that can be sent back to the client
#[derive(Debug, Clone)]
pub struct ResponseContext {
    pub message: RawMessage,
}

impl ResponseContext {
    /// Create a response from a message
    pub const fn new(message: RawMessage) -> Self {
        Self { message }
    }

    /// Create a response with status code
    ///
    /// If the message is not present, this method does nothing.
    #[must_use]
    pub const fn with_status(mut self, status: StatusCode) -> Self {
        self.message.header.status = Some(status);
        self
    }
}

/// Result type for server handlers
pub type ServerResult<T = Option<ResponseContext>> = Result<T, ServerError>;

/// Server error types
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("Transport error: {0}")]
    Transport(#[from] fleximq_transport::TransportError),
    #[error("Protocol error: {0}")]
    Protocol(#[from] fleximq_protocol::ProtocolError),
    #[error("Handler not found for path: {0}")]
    HandlerNotFound(String),
    #[error("Internal server error: {0}")]
    Internal(String),
}

/// Main server structure
pub struct Server {
    pub(crate) transport_stream: SharedTransportStream,
}

impl Server {
    /// Create a server from a router (internal use - called from peer.split)
    pub(crate) fn with_transport_streams(transport_stream: SharedTransportStream) -> Self {
        Self { transport_stream }
    }

    // pub async fn serve(&mut self) -> TransportResult<()> {
    //     todo!("")
    // }
}

// Service wrapper to handle the boxed trait object
struct ServiceWrapper<S> {
    inner: S,
}

impl<S> Service<RequestContext> for ServiceWrapper<S>
where
    S: Service<RequestContext, Response = Option<ResponseContext>, Error = ServerError>,
    S::Future: Send + 'static,
{
    type Error = ServerError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    type Response = Option<ResponseContext>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: RequestContext) -> Self::Future {
        Box::pin(self.inner.call(req))
    }
}

impl SharedTransportStreamsExt for Server {
    #[inline]
    async fn disconnect(self) -> TransportResult<()> {
        self.transport_stream.disconnect().await?;
        Ok(())
    }

    #[inline]
    async fn send_message(&mut self, message: &RawMessage) -> TransportResult<()> {
        self.transport_stream.send_message(message).await
    }

    #[inline]
    async fn receive_message(&mut self) -> TransportResult<Message> {
        self.transport_stream.receive_message().await
    }
}
