use dashmap::DashMap;
use fleximq_protocol::{Message, MessageCodec, RawMessage};
use fleximq_transport::{
    TransportError, TransportManager, TransportResult, TransportStream, message,
};
use futures_util::{SinkExt, StreamExt, future::select_all};
use tokio_util::codec::Framed;
use triomphe::Arc;

pub(crate) type SharedTransportStreams =
    Arc<DashMap<String, Framed<Box<dyn TransportStream>, MessageCodec>>>;

pub trait SharedTransportStreamsExt {
    /// Disconnects from a transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the disconnection fails.
    fn disconnect(&mut self, url: &str) -> impl Future<Output = TransportResult<&mut Self>> {
        async move {
            let stream = self.remove_stream(url);
            if let Some(mut stream) = stream {
                stream.close().await?;
            }
            Ok(self)
        }
    }

    /// Removes a transport stream.
    ///
    /// # None
    ///
    /// Returns an `None` if the stream was not found.
    fn remove_stream(&mut self, url: &str) -> Option<Box<dyn TransportStream + 'static>>;

    /// Adds a transport stream.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the stream is valid and will not be dropped
    /// while it is being used.
    ///
    /// The data to be streamed must be in the fleximq message format.
    unsafe fn add_stream(&mut self, url: &str, stream: impl TransportStream + 'static)
    -> &mut Self;

    /// Sends a message to all transport streams.
    ///
    /// # Errors
    ///
    /// Returns an error if any of the transport streams fail to send the
    /// message.
    fn send_message(&mut self, message: &RawMessage) -> impl Future<Output = TransportResult<()>>;

    /// Sends a message to a specific transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport stream is not found.
    fn send_message_to(
        &mut self,
        message: &RawMessage,
        url: &str,
    ) -> impl Future<Output = TransportResult<()>>;

    /// Receives a message from any transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if no transport stream is available.
    fn receive_message(&mut self) -> impl Future<Output = TransportResult<Message>>;

    /// Receives a message from a specific transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the transport stream is not found.
    fn receive_message_from(&mut self, url: &str)
    -> impl Future<Output = TransportResult<Message>>;
}

impl SharedTransportStreamsExt for SharedTransportStreams {
    fn remove_stream(&mut self, url: &str) -> Option<Box<dyn TransportStream + 'static>> {
        self.remove(url).unzip().1.map(Framed::into_inner)
    }

    unsafe fn add_stream(
        &mut self,
        url: &str,
        stream: impl TransportStream + 'static,
    ) -> &mut Self {
        let boxed_stream: Box<dyn TransportStream + 'static> = Box::new(stream);
        let framed = message::sink(boxed_stream);
        self.insert(url.to_string(), framed);
        self
    }

    async fn send_message(&mut self, message: &RawMessage) -> TransportResult<()> {
        let mut errors = Vec::with_capacity(self.len() / 2);
        for mut stream in self.iter_mut() {
            let result = stream.value_mut().send(message).await;
            if let Err(err) = result {
                errors.push(err.into());
            }
        }
        if !errors.is_empty() {
            return Err(TransportError::Multiple(errors));
        }
        Ok(())
    }

    async fn send_message_to(&mut self, message: &RawMessage, url: &str) -> TransportResult<()> {
        if let Some(mut stream) = self.get_mut(url) {
            Ok(stream.value_mut().send(message).await?)
        } else {
            Err(TransportError::ConnectionNotFound(url.to_string()))
        }
    }

    async fn receive_message(&mut self) -> TransportResult<Message> {
        if self.is_empty() {
            return Err(TransportError::ConnectionNotFound(
                "Connections is empty".to_string(),
            ));
        }

        let mut futures = Vec::new();

        for mut stream_ref in self.iter_mut() {
            futures.push(Box::pin(async move { stream_ref.value_mut().next().await }));
        }

        let (message, _, _) = select_all(futures).await;
        match message {
            Some(message) => Ok(message?),
            None => Err(TransportError::ConnectionNotFound(
                "No messages available".to_string(),
            )),
        }
    }

    async fn receive_message_from(&mut self, url: &str) -> TransportResult<Message> {
        if let Some(mut stream) = self.get_mut(url) {
            if let Some(result) = stream.value_mut().next().await {
                Ok(result?)
            } else {
                Err(TransportError::ConnectionClosed)
            }
        } else {
            Err(TransportError::ConnectionNotFound(url.to_string()))
        }
    }
}

pub struct Peer {
    transport_manager: TransportManager,
    transport_streams: SharedTransportStreams,
}

impl Default for Peer {
    fn default() -> Self {
        Self::new()
    }
}

impl Peer {
    /// Creates a new peer.
    #[must_use]
    pub fn new() -> Self {
        Self {
            transport_manager: TransportManager::new(),
            transport_streams: Arc::new(DashMap::new()),
        }
    }

    /// Connects to a transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    pub async fn connect(&mut self, url: &str) -> TransportResult<&mut Self> {
        let stream = self.transport_manager.connect(url).await?;
        unsafe {
            self.add_stream(url, stream);
        }
        Ok(self)
    }
}

impl SharedTransportStreamsExt for Peer {
    #[inline]
    async fn disconnect(&mut self, url: &str) -> TransportResult<&mut Self> {
        self.transport_streams.disconnect(url).await?;
        Ok(self)
    }

    #[inline]
    fn remove_stream(&mut self, url: &str) -> Option<Box<dyn TransportStream + 'static>> {
        self.transport_streams.remove_stream(url)
    }

    #[inline]
    unsafe fn add_stream(
        &mut self,
        url: &str,
        stream: impl TransportStream + 'static,
    ) -> &mut Self {
        unsafe {
            self.transport_streams.add_stream(url, stream);
        }
        self
    }

    #[inline]
    async fn send_message(&mut self, message: &RawMessage) -> TransportResult<()> {
        self.transport_streams.send_message(message).await
    }

    #[inline]
    async fn send_message_to(&mut self, message: &RawMessage, url: &str) -> TransportResult<()> {
        self.transport_streams.send_message_to(message, url).await
    }

    #[inline]
    async fn receive_message(&mut self) -> TransportResult<Message> {
        self.transport_streams.receive_message().await
    }

    #[inline]
    async fn receive_message_from(&mut self, url: &str) -> TransportResult<Message> {
        self.transport_streams.receive_message_from(url).await
    }
}
