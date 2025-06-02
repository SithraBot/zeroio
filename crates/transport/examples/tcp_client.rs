use std::error::Error;

use fleximq_transport::{TcpTransport, Transport, TransportStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 创建 TCP 传输实例
    let transport = TcpTransport::new();

    // 连接到 TCP 服务器
    println!("正在连接到 TCP 服务器...");
    let mut stream = transport.connect("tcp://localhost:8080").await?;
    println!("已连接到服务器");

    // 发送消息
    let message = "你好，TCP！";
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
