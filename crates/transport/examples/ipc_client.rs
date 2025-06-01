use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use fleximq_transport::{IpcTransport, Transport, TransportStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 创建 IPC 传输实例
    let transport = IpcTransport::new();

    // 连接到 IPC 服务器
    println!("正在连接到 IPC 服务器...");
    let mut stream = transport.connect("ipc:///tmp/test.sock").await?;
    println!("已连接到服务器");

    // 发送消息
    let message = "你好，IPC！";
    println!("发送消息: {}", message);
    stream.write_all(message.as_bytes()).await?;
    stream.flush().await?;

    // 接收响应
    let mut buffer = vec![0u8; 1024];
    let n = stream.read(&mut buffer).await?;
    let response = String::from_utf8_lossy(&buffer[..n]);
    println!("收到响应: {}", response);

    // 关闭连接
    stream.close().await?;
    println!("连接已关闭");

    Ok(())
}
