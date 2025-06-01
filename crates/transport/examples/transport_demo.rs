//! Example demonstrating transport usage

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroio_transport::{TransportManager, TransportStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create transport manager
    let manager = TransportManager::new();

    println!("Transport Manager Demo");
    println!("=====================");

    // Example 1: TCP Transport
    println!("\n1. TCP Transport Example:");
    demo_tcp(&manager).await?;

    // Example 2: IPC Transport
    println!("\n2. IPC Transport Example:");
    demo_ipc(&manager).await?;

    // Example 3: STDIO Transport
    println!("\n3. STDIO Transport Example:");
    demo_stdio(&manager).await?;

    // Example 4: Cache Statistics
    println!("\n4. Cache Statistics:");
    let stats = manager.cache_stats();
    println!("   Cached connections: {}/{}", stats.size, stats.capacity);

    Ok(())
}

async fn demo_tcp(manager: &TransportManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("   Attempting to connect to tcp://localhost:8080");

    match manager.connect("tcp://localhost:8080").await {
        Ok(mut stream) => {
            println!("   ✓ Connected successfully!");
            println!("   Connection info: {:?}", stream.connection_info());

            // Example: Write and read
            stream.write_all(b"Hello, TCP!").await?;
            println!("   → Sent: Hello, TCP!");

            let mut buf = vec![0u8; 1024];
            match tokio::time::timeout(std::time::Duration::from_secs(1), stream.read(&mut buf))
                .await
            {
                Ok(Ok(n)) => {
                    println!("   ← Received: {}", String::from_utf8_lossy(&buf[..n]));
                }
                _ => {
                    println!("   (No response received - timeout)");
                }
            }
        }
        Err(e) => {
            println!("   ✗ Connection failed: {}", e);
            println!("   (This is expected if no TCP server is running on port 8080)");
        }
    }

    Ok(())
}

async fn demo_ipc(manager: &TransportManager) -> Result<(), Box<dyn std::error::Error>> {
    let ipc_path = if cfg!(windows) {
        "ipc://test_pipe"
    } else {
        "ipc:///tmp/zeroio_test.sock"
    };

    println!("   Attempting to connect to {}", ipc_path);

    match manager.connect(ipc_path).await {
        Ok(stream) => {
            println!("   ✓ Connected successfully!");
            println!("   Connection info: {:?}", stream.connection_info());
        }
        Err(e) => {
            println!("   ✗ Connection failed: {}", e);
            println!("   (This is expected if no IPC server is running)");
        }
    }

    Ok(())
}

async fn demo_stdio(manager: &TransportManager) -> Result<(), Box<dyn std::error::Error>> {
    println!("   Spawning subprocess: echo 'Hello from subprocess'");

    match manager.connect("stdio://echo Hello from subprocess").await {
        Ok(mut stream) => {
            println!("   ✓ Subprocess spawned successfully!");
            println!("   Connection info: {:?}", stream.connection_info());

            // Read output from subprocess
            let mut output = Vec::new();
            match tokio::time::timeout(
                std::time::Duration::from_secs(2),
                stream.read_to_end(&mut output),
            )
            .await
            {
                Ok(Ok(_)) => {
                    println!("   ← Output: {}", String::from_utf8_lossy(&output).trim());
                }
                _ => {
                    println!("   (Failed to read subprocess output)");
                }
            }
        }
        Err(e) => {
            println!("   ✗ Failed to spawn subprocess: {}", e);
        }
    }

    Ok(())
}
