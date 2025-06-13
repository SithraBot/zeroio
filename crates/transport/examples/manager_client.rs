#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use fleximq_transport::{MessageStreamExt, TransportResult, manager::TransportManager};
use futures_util::future::select_ok;
#[tokio::main]
async fn main() -> TransportResult<()> {
    let manager = TransportManager::new();
    let connect_try = [
        Box::pin(manager.connect("ipc://hello_world")),
        Box::pin(manager.connect("tcp://127.0.0.1:2121")),
        Box::pin(manager.connect("ws://127.0.0.1:1212")),
    ];
    let mut stream = if let Ok((stream, _)) = select_ok(connect_try).await {
        stream
    } else {
        manager.connect("stdio://").await?
    };
    let message = stream.read_message().await?;
    eprintln!("message: {message:?}");
    stream.close().await?;
    Ok(())
}
