use fleximq_protocol::{Message, RawMessage};
use fleximq_transport::TransportResult;

use crate::types::{SharedTransportStream, SharedTransportStreamsExt};

pub struct Client {
    pub(crate) transport_stream: SharedTransportStream,
}

impl SharedTransportStreamsExt for Client {
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
