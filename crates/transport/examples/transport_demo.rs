//! Example demonstrating transport usage with connection tracking

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use fleximq_transport::{TransportManager, TransportStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create transport manager with custom cache configuration
    let manager = TransportManager::with_cache_config(50, std::time::Duration::from_secs(600));

    println!("Transport Manager Demo with Connection Tracking");
    println!("==============================================");

    // Example 1: TCP Transport
    println!("\n1. TCP Transport Example:");
    demo_tcp(&manager).await?;

    // Example 2: IPC Transport
    println!("\n2. IPC Transport Example:");
    demo_ipc(&manager).await?;

    // Example 3: STDIO Transport
    println!("\n3. STDIO Transport Example:");
    demo_stdio(&manager).await?;

    // Example 4: Connection Statistics
    println!("\n4. Connection Statistics:");
    demo_stats(&manager);

    println!("\nDemo completed!");

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
        "ipc:///tmp/fleximq_test.sock"
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

fn demo_stats(manager: &TransportManager) {
    let stats = manager.cache_stats();
    println!(
        "   Connection tracking entries: {}/{}",
        stats.size, stats.capacity
    );
    println!("   Cache utilization: {:.1}%", stats.utilization() * 100.0);
    println!("   Active entries: {}", stats.active_entries);
    println!("   Active ratio: {:.1}%", stats.active_ratio() * 100.0);

    // Demo connection frequency tracking
    let test_urls = ["tcp://localhost:8080", "ipc:///tmp/fleximq_test.sock", "stdio://echo test"];

    for url in &test_urls {
        if let Some(frequency) = manager.get_connection_frequency(url) {
            println!("   {} accessed {} times", url, frequency);
            if manager.is_frequently_accessed(url) {
                println!("     → Frequently accessed (good for connection pooling)");
            }
        }
    }
}
