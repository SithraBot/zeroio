#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use fleximq_protocol::MessageBuilder;
use fleximq_transport::{
    MessageStreamExt, StdioTransport, Transport, TransportResult, TransportStream,
};

#[tokio::main]
async fn main() -> TransportResult<()> {
    let mut stream = StdioTransport::new().connect("stdio://./manager_client").await?;
    let message = MessageBuilder::broadcast(0).build_with_payload(&"hello world").unwrap();
    loop {
        tokio::time::sleep(Duration::from_secs(3)).await;
        if let Err(e) = stream.write_message(&message).await {
            eprintln!("Error: {e}");
            stream.close().await.unwrap();
            break;
        }
    }
    Ok(())
}
