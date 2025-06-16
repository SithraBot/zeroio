//! An example of a fleximq Inter-Process Communication (IPC) server.
//!
//! This example demonstrates how to create a fleximq server that communicates
//! using IPC.

#![allow(clippy::nursery)]
#![allow(clippy::pedantic)]
#![allow(clippy::unwrap_used)]

use std::time::Duration;

use fleximq_protocol::MessageBuilder;
use fleximq_transport::{
    IpcTransport, Transport, TransportResult, TransportStream, message::writer,
};
use futures_util::SinkExt;

#[tokio::main]
async fn main() -> TransportResult<()> {
    let mut listener = IpcTransport::new().listen("ipc://hello_world").await?;
    let mut handles = vec![];
    loop {
        let stream = listener.accept().await?;
        if stream.is_connected() {
            println!("Client connected, info: {:?}", stream.connection_info());
        }
        let mut writer = writer(stream);
        let handle = tokio::spawn(async move {
            let message = MessageBuilder::broadcast(0).build_with_payload(&"hello world").unwrap();
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                if let Err(e) = writer.send(&message).await {
                    eprintln!("Error: {e}");
                    writer.into_inner().close().await.unwrap();
                    break;
                }
            }
        });
        handles.push(handle);
    }
}
