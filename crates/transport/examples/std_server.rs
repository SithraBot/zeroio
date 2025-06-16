//! An example of a fleximq standard I/O server.
//!
//! This example demonstrates how to create a fleximq server that communicates
//! over standard input/output streams.

#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use fleximq_protocol::{MessageBuilder, MessageEncoder};
use fleximq_transport::{StdioTransport, Transport, TransportResult, TransportStream};
use futures_util::SinkExt;
use tokio_util::codec::FramedWrite;

#[tokio::main]
async fn main() -> TransportResult<()> {
    let stream = StdioTransport::new().connect("stdio://./manager_client").await?;
    let encoder = MessageEncoder::new();
    let mut writer = FramedWrite::new(stream, encoder);
    let message = MessageBuilder::broadcast(0).build_with_payload(&"hello world").unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        if let Err(e) = writer.send(&message).await {
            eprintln!("Error: {e}");
            writer.into_inner().close().await?;
            break;
        }
    }
    Ok(())
}
