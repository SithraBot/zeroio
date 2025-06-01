use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroio_transport::{IpcTransport, Transport};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let transport = IpcTransport::new();
    let mut listener = transport.listen("ipc:///tmp/test.sock").await?;
    println!("IPC Server listening on /tmp/test.sock");

    loop {
        match listener.accept().await {
            Ok(mut stream) => {
                println!("Accepted new IPC connection.");
                tokio::spawn(async move {
                    let mut buffer = vec![0u8; 1024];
                    loop {
                        match stream.read(&mut buffer).await {
                            Ok(0) => {
                                println!("IPC Connection closed by client.");
                                break;
                            }
                            Ok(n) => {
                                let message = String::from_utf8_lossy(&buffer[..n]);
                                println!("Received from IPC client: {}", message);
                                if stream.write_all(&buffer[..n]).await.is_err() {
                                    eprintln!("Error writing to IPC client.");
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Error reading from IPC client: {}", e);
                                break;
                            }
                        }
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting IPC connection: {}", e);
            }
        }
    }
}
