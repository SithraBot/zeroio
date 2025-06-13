#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use fleximq_transport::{MessageStreamExt, TransportResult, manager::TransportManager};
#[tokio::main]
async fn main() -> TransportResult<()> {
    let manager = TransportManager::new();
    let mut stream = manager.connect("ipc://hello_world").await.unwrap();
    let message = stream.read_message().await?;
    println!("message: {message:?}");
    stream.close().await?;
    Ok(())
}
