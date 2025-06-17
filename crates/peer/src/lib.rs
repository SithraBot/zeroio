//! fleximq Peer – ergonomic high-level API built on top of protocol &
//! transport.

use std::sync::Mutex;

pub mod client;

/// This module contains the core Peer implementation for fleximq.
/// It provides the high-level API for interacting with fleximq messages
/// and managing connections.
pub mod peer;
pub mod server;

pub static GLOBAL_ULID_GENERATOR: Mutex<ulid::Generator> = Mutex::new(ulid::Generator::new());

pub mod types {
    use fleximq_protocol::{Message, MessageCodec, RawMessage};
    use fleximq_transport::{TransportError, TransportResult, TransportStream};
    use futures_util::{SinkExt, StreamExt};
    use tokio::sync::RwLock;
    use tokio_util::codec::Framed;
    use triomphe::Arc;

    pub(crate) type SharedTransportStream =
        Arc<RwLock<Framed<Box<dyn TransportStream>, MessageCodec>>>;

    /// Extension trait for shared transport streams.
    ///
    /// This trait provides utility methods for managing a collection of
    /// framed transport streams, such as disconnecting from a specific stream.
    pub trait SharedTransportStreamsExt {
        /// Disconnects from a transport stream.
        ///
        /// # Errors
        ///
        /// Returns an error if the disconnection fails.
        fn disconnect(self) -> impl Future<Output = TransportResult<()>>;

        /// Sends a message to all transport streams.
        ///
        /// # Errors
        ///
        /// Returns an error if any of the transport streams fail to send the
        /// message.
        fn send_message(
            &mut self,
            message: &RawMessage,
        ) -> impl Future<Output = TransportResult<()>>;

        /// Receives a message from any transport stream.
        ///
        /// # Errors
        ///
        /// Returns an error if no transport stream is available.
        fn receive_message(&mut self) -> impl Future<Output = TransportResult<Message>>;
    }

    impl SharedTransportStreamsExt for SharedTransportStream {
        #[inline]
        async fn disconnect(self) -> TransportResult<()> {
            drop(self);
            Ok(())
        }

        #[inline]
        async fn send_message(&mut self, message: &RawMessage) -> TransportResult<()> {
            self.write().await.send(message).await?;
            Ok(())
        }

        async fn receive_message(&mut self) -> TransportResult<Message> {
            Ok(self.write().await.next().await.ok_or(TransportError::ConnectionClosed)??)
        }
    }
}
