# 为 fleximq 做贡献

我们欢迎所有贡献，以使该项目变得更好。

## 代码所有权和许可

**重要提示**：通过向本项目贡献代码，您同意您的贡献成为项目的一部分，并且任何人都可以根据项目的许可条款使用。您的代码将与项目的其余部分在相同的许可下提供。

## 贡献方式

- 代码：错误修复、功能、性能改进
- 文档：README、注释、示例
- 测试：编写测试、报告错误
- 设计：协议和 API 改进

## 开始

### 开发设置

```bash
git clone https://github.com/fleximq/fleximq.rs.git
cd fleximq.rs
cargo build
cargo test --workspace
```

### 进行更改

```bash
git checkout -b feature/your-feature-name
# 进行更改，添加测试，更新文档
cargo test --workspace
cargo clippy --workspace
cargo fmt --check
```

## 代码指南

### 代码标准

- 遵循 `cargo fmt` 格式化
- 解决所有 `cargo clippy --workspace` 警告
- 为新功能编写测试
- 文档化公共 API
- 使用有意义的名称

项目使用自定义的 clippy 和 rustfmt 配置以确保一致性。

### 协议变更

- 所有协议变更都必须通过 [RFC 流程](https://github.com/fleximq/rfcs)进行讨论和批准。
- 批准的 RFC 将导致对[协议草案](https://github.com/fleximq/spec)的更新。
- 在获得主流实现支持后，草案将成为正式规范。
- 保持向后兼容性。
- 考虑跨语言影响。

## 提交更改

### 拉取请求 (Pull Requests)

- 更新 API 更改的文档
- 为您的更改添加测试
- 确保所有测试在本地通过
- 编写清晰的提交信息
- 保持 PR 专注于单个功能
- 描述更改的内容和原因

## 报告问题

### 错误报告

包括：

- 操作系统和 Rust 版本 (`rustc --version`)
- 重现步骤
- 预期行为与实际行为

### 功能请求

描述：

- 用例及其工作原理
- 考虑过的替代方案
- 您是否会实现它

## 沟通

使用 GitHub Issues 进行错误和功能跟踪，使用 Pull Requests 进行代码审查。

## 行为准则

保持尊重，提供建设性的反馈，专注于技术优点。 