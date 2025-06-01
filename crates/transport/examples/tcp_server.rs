use std::error::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zeroio_transport::{TcpTransport, Transport, TransportStream};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // 创建 TCP 传输实例
    let transport = TcpTransport::new();

    // 开始监听
    println!("TCP 服务器监听在 tcp://localhost:8080");
    let mut listener = transport.listen("tcp://localhost:8080").await?;

    // 接受连接
    while let Ok(mut stream) = listener.accept().await {
        println!("收到新的连接");

        // 处理连接
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 1024];

            // 读取消息
            if let Ok(n) = stream.read(&mut buffer).await {
                let message = String::from_utf8_lossy(&buffer[..n]);
                println!("收到消息: {}", message);

                // 发送回显消息
                let response = format!("服务器收到: {}", message);
                if let Err(e) = stream.write_all(response.as_bytes()).await {
                    println!("发送响应失败: {}", e);
                    return;
                }
                if let Err(e) = stream.flush().await {
                    println!("刷新缓冲区失败: {}", e);
                    return;
                }
            }

            // 关闭连接
            if let Err(e) = stream.close().await {
                println!("关闭连接失败: {}", e);
            }
        });
    }

    Ok(())
}
