use fleximq_protocol::{
    Auth, Message, MessageBuilder, RawMessage, RequestResponse, constants::CLIENT_ID_UNASSIGNED,
};
use fleximq_transport::{
    TransportError, TransportManager, TransportResult, TransportStream, message,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::RwLock;
use tokio_util::codec::Framed;
use triomphe::Arc;

use crate::{
    GLOBAL_ULID_GENERATOR,
    client::Client,
    server::Server,
    types::{SharedTransportStream, SharedTransportStreamsExt},
};

/// The main fleximq Peer struct.
///
/// This struct provides the high-level interface for sending and receiving
/// fleximq messages, managing connections, and handling various protocol
/// interactions.
pub struct Peer {
    transport_stream: SharedTransportStream,
    self_id:          u32,
}

impl Peer {
    /// Connects to a transport stream.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection fails.
    #[must_use]
    pub async fn from_stream(
        stream: impl TransportStream + 'static,
        name: impl Into<String>,
        auth: Option<Auth>,
    ) -> TransportResult<Self> {
        let mut framed: Framed<Box<dyn TransportStream>, _> = message::sink(Box::new(stream));
        let id = GLOBAL_ULID_GENERATOR
            .lock()
            .expect("lock error")
            .generate()
            .map_err(|e| TransportError::Other(e.to_string()))?
            .to_string();
        let join_message = MessageBuilder::join(CLIENT_ID_UNASSIGNED, name)
            .reqrep(RequestResponse::req(id.clone()));
        let join_message = if let Some(auth) = auth {
            join_message.auth(auth)
        } else {
            join_message
        };
        let join_message = join_message.build()?;
        framed.send(&join_message).await?;
        loop {
            let message = framed.next().await.transpose()?;
            let Some(message) = message else {
                continue;
            };
            let header = message.header()?;
            let Some(ref rep) = header.reqrep else {
                continue;
            };
            if rep.id != id {
                continue;
            }
            return Ok(Self {
                transport_stream: Arc::new(RwLock::new(framed)),
                self_id:          message.client_id(),
            });
        }
    }

    #[must_use]
    pub async fn connect(
        address: impl AsRef<str>,
        name: impl Into<String>,
        auth: Option<Auth>,
    ) -> TransportResult<Self> {
        let manager = TransportManager::new();
        let stream = manager.connect(address.as_ref()).await?;

        Self::from_stream(stream, name, auth).await
    }

    #[must_use]
    pub fn split(self) -> (Server, Client) {
        let server = Server::with_transport_streams(self.transport_stream.clone());
        let client = Client {
            transport_stream: self.transport_stream,
        };
        (server, client)
    }
}

impl SharedTransportStreamsExt for Peer {
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
