use std::{
    collections::HashMap,
    future::Future,
    task::{Context, Poll},
};

use fleximq_protocol::{Message, MessageType, RawMessage, StatusCode};
use fleximq_transport::TransportResult;
use futures_util::{FutureExt, future::BoxFuture};
use tower::{Layer, Service};
use triomphe::Arc;

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

/// Handler function type
pub type Handler<T = Option<ResponseContext>> =
    Box<dyn Fn(RequestContext) -> BoxFuture<'static, ServerResult<T>> + Send + Sync>;

/// Route matcher for different message types and paths
#[derive(Debug, Clone)]
pub struct Route {
    pub message_type: MessageType,
    pub path:         String,
}

impl Route {
    pub fn new(message_type: MessageType, path: impl Into<String>) -> Self {
        Self {
            message_type,
            path: path.into(),
        }
    }
}

/// Router for managing routes and handlers
#[derive(Default)]
pub struct Router {
    routes:   HashMap<Route, Handler>,
    fallback: Option<Handler>,
}

impl Router {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a route handler
    #[must_use]
    pub fn route<F, Fut>(mut self, route: Route, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<Option<ResponseContext>>> + Send + 'static,
    {
        let handler =
            Box::new(move |ctx| Box::pin(handler(ctx)) as BoxFuture<'static, ServerResult>);
        self.routes.insert(route, handler);
        self
    }

    /// Add a request handler
    #[must_use]
    pub fn request<F, Fut>(self, path: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<ResponseContext>> + Send + 'static,
    {
        self.route(Route::new(MessageType::Request, path), move |ctx| {
            handler(ctx).map(|res| res.map(Some))
        })
    }

    /// Add a notification handler
    #[must_use]
    pub fn notification<F, Fut>(self, path: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<()>> + Send + 'static,
    {
        self.route(Route::new(MessageType::Notification, path), move |ctx| {
            handler(ctx).map(|_| Ok(None))
        })
    }

    /// Add a publish handler
    #[must_use]
    pub fn topic<F, Fut>(self, topic: impl Into<String>, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<()>> + Send + 'static,
    {
        self.route(Route::new(MessageType::Publish, topic), move |ctx| {
            handler(ctx).map(|_| Ok(None))
        })
    }

    /// Add a broadcast handler
    #[must_use]
    pub fn broadcast<F, Fut>(self, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult<()>> + Send + 'static,
    {
        self.route(Route::new(MessageType::Broadcast, ""), move |ctx| {
            handler(ctx).map(|_| Ok(None))
        })
    }

    /// Set fallback handler for unmatched routes
    #[must_use]
    pub fn fallback<F, Fut>(mut self, handler: F) -> Self
    where
        F: Fn(RequestContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ServerResult> + Send + 'static,
    {
        let handler =
            Box::new(move |ctx| Box::pin(handler(ctx)) as BoxFuture<'static, ServerResult>);
        self.fallback = Some(handler);
        self
    }

    /// Apply a layer to the router
    pub fn layer<L>(self, layer: L) -> LayeredRouter<L::Service>
    where
        L: Layer<RouterService>,
    {
        let service = RouterService::new(self);
        LayeredRouter {
            service: layer.layer(service),
        }
    }

    /// Find handler for a route
    fn find_handler(&self, route: &Route) -> Option<&Handler> {
        self.routes.get(route).or(self.fallback.as_ref())
    }
}

impl std::hash::Hash for Route {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.message_type.to_u8().hash(state);
        self.path.hash(state);
    }
}

impl PartialEq for Route {
    fn eq(&self, other: &Self) -> bool {
        self.message_type.to_u8() == other.message_type.to_u8() && self.path == other.path
    }
}

impl Eq for Route {}

/// Router service that implements Tower's Service trait
pub struct RouterService {
    router: Arc<Router>,
}

impl RouterService {
    #[must_use]
    pub fn new(router: Router) -> Self {
        Self {
            router: Arc::new(router),
        }
    }
}

impl Service<RequestContext> for RouterService {
    type Error = ServerError;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;
    type Response = Option<ResponseContext>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: RequestContext) -> Self::Future {
        let router = Arc::clone(&self.router);

        if req.message.message_type() == MessageType::Notification {
            Box::pin(async move {
                if let Ok(header) = req.message.header() {
                    if let Some(routing) = &header.routing {
                        let message_type = req.message.message_type();
                        for route in routing {
                            let route = Route::new(message_type, route.path.clone());
                            if let Some(handler) = router.find_handler(&route) {
                                handler(req.clone()).await?;
                            }
                        }
                    }
                }
                Ok(None)
            })
        } else {
            Box::pin(async move {
                // Extract routing information from the message
                let path = match req.message.header() {
                    Ok(header) => header
                        .routing
                        .as_ref()
                        .and_then(|r| r.first())
                        .map(|r| r.path.clone())
                        .unwrap_or_default(),
                    Err(_) => String::new(),
                };

                let route = Route::new(req.message.message_type(), path);

                // Find and execute handler
                if let Some(handler) = router.find_handler(&route) {
                    handler(req).await
                } else {
                    Err(ServerError::HandlerNotFound(route.path))
                }
            })
        }
    }
}

/// Layered router with middleware applied
pub struct LayeredRouter<S> {
    service: S,
}

impl<S> LayeredRouter<S>
where
    S: Service<RequestContext, Response = ResponseContext, Error = ServerError>,
{
    pub fn into_service(self) -> S {
        self.service
    }
}

type BoxService = Box<
    dyn Service<
            RequestContext,
            Response = Option<ResponseContext>,
            Error = ServerError,
            Future = BoxFuture<'static, Result<Option<ResponseContext>, ServerError>>,
        > + Send
        + Sync,
>;

/// Main server structure
pub struct Server {
    pub(crate) transport_stream: SharedTransportStream,
    service:                     Option<BoxService>,
}

impl Server {
    /// Create a server from a router (internal use - called from peer.split)
    pub(crate) fn with_transport_streams(transport_stream: SharedTransportStream) -> Self {
        Self {
            transport_stream,
            service: None,
        }
    }

    /// Set the router service for this server
    #[must_use]
    pub fn with_router<S>(mut self, service: S) -> Self
    where
        S: Service<RequestContext, Response = Option<ResponseContext>, Error = ServerError>
            + Send
            + Sync
            + 'static,
        S::Future: Send + 'static,
    {
        let service = Box::new(ServiceWrapper { inner: service });
        self.service = Some(service);
        self
    }

    /// Create a server from a router
    #[must_use]
    pub fn from_router(transport_streams: SharedTransportStream, router: Router) -> Self {
        let service = RouterService::new(router);
        Self::with_transport_streams(transport_streams).with_router(service)
    }

    /// Process an incoming message
    ///
    /// # Errors
    ///
    /// * `ServerError::Internal` - If no service is configured
    pub async fn handle_message(
        &mut self,
        message: Message,
        source_url: Option<String>,
    ) -> ServerResult {
        let ctx = RequestContext {
            client_id: message.client_id(),
            message,
            source_url,
        };

        match &mut self.service {
            Some(service) => service.call(ctx).await,
            None => Err(ServerError::Internal("No service configured".to_string())),
        }
    }

    /// Start the server message processing loop
    ///
    /// # Errors
    ///
    /// * `TransportError` - If the transport fails
    pub async fn serve(&mut self) -> TransportResult<()> {
        loop {
            match self.receive_message().await {
                Ok(message) => {
                    match self.handle_message(message, None).await {
                        Ok(response) => {
                            if let Some(response) = response {
                                if let Err(e) = self.send_message(&response.message).await {
                                    log::error!("Failed to send response: {e}");
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("Handler error: {e}");
                            // TODO: Could send error response here
                        }
                    }
                }
                Err(e) => {
                    log::error!("Failed to receive message: {e}");
                    // TODO: Could implement reconnection logic here
                }
            }
        }
    }
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
