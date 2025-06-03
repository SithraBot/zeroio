# fleximq.rs (fleximq 协议的 Rust 实现)

`fleximq.rs` 是 [fleximq 协议](https://github.com/fleximq/spec) 的 Rust 实现。本库旨在为 Rust 开发者提供一套现代、高性能且符合 Rust 语言特性的工具，以便在分布式应用中运用 fleximq 协议进行通信。

要详细了解 fleximq 协议本身的设计目标、架构和核心概念，请查阅其官方 [协议规范仓库的 README](https://github.com/fleximq/spec/blob/main/README.md)（[中文版 README_zh.md](https://github.com/fleximq/spec/blob/main/README_zh.md)）。

## 特性

- **Rust 实现**: 充分利用 Rust 的安全性、并发性和性能来实现 fleximq 协议。

- **清晰的架构**: 在 Rust 代码库中为协议处理、核心逻辑和 API 定义了清晰的层次。

- **可插拔传输层**: 支持多种传输后端 (TCP, IPC, WebSocket, STDIO)，便于 Rust 应用的灵活部署。

- **可扩展中间件系统**: 提供基于 Rust 的处理流水线，用于自定义处理（例如，认证、日志、指标）。

- **异步优先与 Tokio 集成**: 与 Tokio 深度集成，以实现符合 Rust 习惯用法的高性能异步操作。

- **内存高效设计**: 利用缓冲池和 Rust 的所有权模型来最小化内存分配并安全地管理内存。

- **高性能代理核心**: 其基于 Rust 的代理具有 LRU 路由缓存、基于 DashMap 的客户端注册表和原子化指标。

- **健壮的连接管理**: 包括自动客户端 ID 分配、连接池和空闲连接清理。

- **并发设计**: 使用 Rust 的并发原语构建，以安全地最大化吞吐量。

- **内置性能指标**: 提供原子化指标，用于实时监控库的状态。

- **生产就绪配置**: 允许对限制和超时进行微调，以实现稳健的 Rust 部署。

- **灵活的认证机制**: 支持代理的可插拔认证模块。

## 协议

本库所实现的 fleximq 协议规范详见：[fleximq/spec/draft/v1.0.0.md](https://github.com/fleximq/spec/blob/main/draft/v1.0.0.md)。

## 贡献

欢迎贡献！请参阅 [CONTRIBUTING_zh.md](CONTRIBUTING_zh.md)（如果此文件不存在，则请参考 [CONTRIBUTING.md](CONTRIBUTING.md)）了解贡献指南。

### 开发设置

```bash
git clone https://github.com/fleximq/fleximq.rs.git
cd fleximq.rs
cargo test --all-features
cargo test --package fleximq --lib protocol::tests
cargo test --package fleximq --lib router::tests
```

## 许可证

本项目采用 Unlicense 许可证 - 详情请参阅 [LICENSE](LICENSE) 文件。

## 代办

- [x] protocol
- [x] transport
- [ ] broker
- [ ] client
