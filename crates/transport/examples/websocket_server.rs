// use std::net::SocketAddr; // Unused
use std::error::Error;

use futures_util::SinkExt;
// use futures_util::stream::{StreamExt, SplitSink, SplitStream}; // SplitSink, SplitStream
// unused
use futures_util::stream::StreamExt; // Keep StreamExt
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::protocol::Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let addr = "127.0.0.1:8080";
    let listener = TcpListener::bind(&addr).await?;
    println!("WebSocket server listening on {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(accept_connection(stream));
    }

    Ok(())
}

async fn accept_connection(stream: TcpStream) {
    if let Ok(ws_stream) = accept_async(stream).await {
        println!("New WebSocket connection established");
        let (mut write, mut read) = ws_stream.split();

        while let Some(message) = read.next().await {
            match message {
                Ok(msg) => {
                    println!("Received a message: {:?}", msg);
                    if msg.is_text() || msg.is_binary() {
                        let response = if let Ok(text) = msg.to_text() {
                            format!("Server received text: {}", text)
                        } else {
                            "Server received binary data".to_string()
                        };
                        if let Err(e) = write.send(Message::Text(response.into())).await {
                            println!("Error sending message: {}", e);
                            break;
                        }
                    } else if msg.is_close() {
                        println!("Client closed connection");
                        break;
                    }
                }
                Err(e) => {
                    println!("Error processing message: {}", e);
                    break;
                }
            }
        }
    } else {
        println!("Error during WebSocket handshake");
    }
}
