#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use fleximq_protocol::MessageBuilder;
use fleximq_transport::{
    MessageStreamExt, TcpTransport, Transport, TransportResult, TransportStream,
};

#[tokio::main]
async fn main() -> TransportResult<()> {
    let mut listener = TcpTransport::new().listen("tcp://0.0.0.0:2121").await?;
    let mut handles = vec![];
    loop {
        let mut stream = listener.accept().await?;
        if stream.is_connected() {
            println!("Client connected, info: {:?}", stream.connection_info());
        }
        let handle = tokio::spawn(async move {
            let message = MessageBuilder::broadcast(0).build_with_payload(&"hello world").unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                if let Err(e) = stream.write_message(&message).await {
                    eprintln!("Error: {e}");
                    stream.close().await.unwrap();
                    break;
                }
            }
        });
        handles.push(handle);
    }
}
