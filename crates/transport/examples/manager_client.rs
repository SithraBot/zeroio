#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use fleximq_protocol::MessageDecoder;
use fleximq_transport::{TransportResult, TransportStream, manager::TransportManager};
use futures_util::{StreamExt, future::select_ok};
use tokio_util::codec::FramedRead;
#[tokio::main]
async fn main() -> TransportResult<()> {
    let manager = TransportManager::new();
    let connect_try = [
        Box::pin(manager.connect("ipc://hello_world")),
        Box::pin(manager.connect("tcp://127.0.0.1:2121")),
        Box::pin(manager.connect("ws://127.0.0.1:1212")),
    ];
    let stream = if let Ok((stream, _)) = select_ok(connect_try).await {
        stream
    } else {
        manager.connect("stdio://").await?
    };
    let decoder = MessageDecoder::new();
    let mut reader = FramedRead::new(stream, decoder);
    let message = reader.next().await.unwrap()?;
    eprintln!("message: {message:?}");
    reader.into_inner().close().await?;
    Ok(())
}
