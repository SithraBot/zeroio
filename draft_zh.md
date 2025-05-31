# zeroio 协议 V1

> 注意：本协议中所有多字节数值均采用网络字节序（大端序）编码。
> 所有结构化数据（Header 和 Payload）均使用 MessagePack 格式进行序列化。

## 基础格式

| 字段          | 类型   | 大小（字节） | 描述                                 |
| ------------- | ------ | ------------ | ------------------------------------ |
| Version       | uint8  | 1            | 协议版本 (1)                         |
| Type          | uint8  | 1            | 消息类型 (0-7)                       |
| ClientID      | uint32 | 4            | 唯一客户端标识符（32位大端序）       |
| Reserved      | bytes  | 16           | 预留字节供未来使用（必须为零）       |
| HeaderLength  | uint32 | 4            | 头部段的字节长度（32位大端序）       |
| Header        | bytes  | 可变         | 可变长度头部数据（MessagePack 编码） |
| PayloadLength | uint64 | 8            | 负载段的字节长度（64位大端序）       |
| Payload       | bytes  | 可变         | 可变长度负载数据（MessagePack 编码） |

**基础头部总大小：34 字节（26 + 8 用于负载长度）**

## 消息类型

| 类型  | 名称         | 值  | 描述                 |
| ----- | ------------ | --- | -------------------- |
| JOIN  | Join         | 0   | 加入组/连接到代理    |
| REQ   | Request      | 1   | 发送请求给单个客户端 |
| REP   | Response     | 2   | 发送对请求的响应     |
| NOTIF | Notification | 3   | 发送通知给特定客户端 |
| BCAST | Broadcast    | 4   | 向所有客户端发送广播 |
| PUB   | Topic        | 5   | 向主题发布消息       |
| SUB   | Subscribe    | 6   | 订阅主题             |
| UNSUB | Unsubscribe  | 7   | 取消订阅主题         |
| PING  | Ping         | 8   | 连接保活心跳         |
| PONG  | Pong         | 9   | 连接保活回应         |

## 头部结构

头部是使用 MessagePack 编码的结构，包含以下可选字段：

```
头部映射：
  "routing" => 路由对象数组（可选）
  "reqrep" => 请求/响应对象（可选）
  "topic" => 字符串（可选）
  "status" => 整数（可选）
  "auth" => 认证对象（可选）
  "keepalive" => 保活对象（可选）

路由对象：
  "client_id" => 32位无符号整数
  "path" => 字符串

请求/响应对象：
  "type" => 字符串（"request" 或 "correlation"）
  "id" => 字符串（唯一标识符）

认证对象：
  "type" => 字符串（"token"、"basic" 或 "api_key"）
  "token" => 字符串（用于令牌认证）
  "username" => 字符串（用于基本认证）
  "password" => 字符串（用于基本认证）
  "api_key" => 字符串（用于 API 密钥认证）

保活对象：
  "timestamp" => 64位无符号整数（Unix时间戳，毫秒）
  "interval" => 32位无符号整数（建议的间隔秒数，可选）
```

### 头部字段要求

| 消息类型 | 必需字段        | 可选字段          | 禁止字段                             |
| -------- | --------------- | ----------------- | ------------------------------------ |
| JOIN     | -               | auth              | routing, reqrep, topic, keepalive   |
| REQ      | routing, reqrep | status            | topic, auth, keepalive               |
| REP      | routing, reqrep | status            | topic, auth, keepalive               |
| NOTIF    | routing         | status            | reqrep, topic, auth, keepalive       |
| BCAST    | -               | status            | routing, reqrep, topic, auth, keepalive |
| PUB      | topic           | routing, status   | reqrep, auth, keepalive              |
| SUB      | topic           | -                 | routing, reqrep, status, auth, keepalive |
| UNSUB    | topic           | -                 | routing, reqrep, status, auth, keepalive |
| PING     | keepalive       | -                 | routing, reqrep, topic, status, auth |
| PONG     | keepalive       | -                 | routing, reqrep, topic, status, auth |

### 路由格式

- `client_id`：目标客户端标识符（32位无符号整数）
- `path`：目标端点的路由路径字符串

### 请求/响应关联

- 对于 REQ 消息：`type` 必须为 "request"
- 对于 REP 消息：`type` 必须为 "correlation"，且 `id` 必须与原始请求匹配
- `id`：用于关联的唯一标识符（推荐：ULID）

## 状态码

协议使用类似 HTTP 的状态码：

### 成功码（200-299）

- **200 OK**：请求成功完成
- **201 Created**：资源创建成功
- **202 Accepted**：请求已接受处理

### 客户端错误码（400-499）

- **400 Bad Request**：无效的请求格式或参数
- **401 Unauthorized**：需要认证
- **403 Forbidden**：访问被拒绝
- **404 Not Found**：资源未找到
- **405 Method Not Allowed**：不支持的操作
- **408 Request Timeout**：请求超时

### 服务器错误码（500-599）

- **500 Internal Server Error**：服务器遇到错误
- **501 Not Implemented**：功能未实现
- **502 Bad Gateway**：上游响应无效
- **503 Service Unavailable**：服务暂时不可用
- **504 Gateway Timeout**：上游超时

### 协议特定码（600-699）

- **600 Client Not Found**：目标客户端不存在
- **601 Topic Not Found**：指定主题不存在
- **602 Invalid Routing**：路由信息无效
- **603 Subscription Failed**：订阅主题失败

## 认证类型

### 令牌认证

- `type`："token"
- `token`：Bearer 令牌字符串

### 基本认证

- `type`："basic"
- `username`：用户名字符串
- `password`：密码字符串

### API 密钥认证

- `type`："api_key"
- `api_key`：API 密钥字符串

## 头部示例

### 加入消息

**无认证：**

- 空头部映射

**使用令牌认证：**

- auth.type = "token"
- auth.token = "authentication-token"

### 请求消息

- routing[0].client_id = 1234
- routing[0].path = "/api/users"
- reqrep.type = "request"
- reqrep.id = "unique-request-id"

### 响应消息

- routing[0].client_id = 5678
- routing[0].path = "/api/users"
- reqrep.type = "correlation"
- reqrep.id = "unique-request-id"
- status = 200

### 主题消息

- topic = "news_updates"

### 订阅消息

- topic = "news_updates"

### 通知消息

- routing[0].client_id = 1234
- routing[0].path = "/notifications"
- routing[1].client_id = 5678
- routing[1].path = "/alerts"

### 心跳消息

- keepalive.timestamp = 1703123456789
- keepalive.interval = 30

### 回应消息

- keepalive.timestamp = 1703123456789

## 连接保活

协议包含内置的保活机制，使用 PING 和 PONG 消息来维持连接健康并检测断开连接。

### 保活机制

- **PING 消息**：由任一方发送以测试连接活性
- **PONG 消息**：响应 PING 消息以确认连接健康
- **时间戳**：消息创建时的 Unix 时间戳（毫秒）
- **间隔**：建议的保活间隔（秒）（可选，仅在 PING 中）

### 保活规则

1. **PING 发起**：客户端或代理都可以通过发送 PING 发起保活
2. **PONG 响应**：接收方必须用包含相同时间戳的 PONG 响应
3. **超时检测**：发送方应在合理超时时间内未收到 PONG 时认为连接已断开
4. **间隔建议**：PING 可包含后续保活的建议间隔
5. **自动保活**：实现可以定期自动发送 PING 消息

### 保活示例

**PING 消息：**
```
头部：{
  "keepalive": {
    "timestamp": 1703123456789,
    "interval": 30
  }
}
负载：（空）
```

**PONG 响应：**
```
头部：{
  "keepalive": {
    "timestamp": 1703123456789
  }
}
负载：（空）
```

### 保活最佳实践

- 默认保活间隔：30 秒
- PONG 超时：3倍保活间隔
- 连接失败时指数退避
- 保活失败时优雅降级

## 传输支持

| 传输      | URL 方案 | 描述              | 连接发起方 | ClientID 分配 |
| --------- | -------- | ----------------- | ---------- | ------------- |
| TCP       | tcp://   | TCP 套接字连接    | 客户端     | 代理          |
| IPC       | ipc://   | 进程间通信        | 客户端     | 代理          |
| WebSocket | ws://    | WebSocket 连接    | 客户端     | 代理          |
| STDIO     | stdio:// | 标准输入/输出管道 | 任一方     | 代理          |

### 传输 URL

- **TCP**：`tcp://hostname:port`（例如 `tcp://localhost:8080`）
- **IPC**：`ipc:///path/to/socket`（例如 `ipc:///tmp/zeroio.sock`）
- **WebSocket**：`ws://hostname:port/path`（例如 `ws://localhost:8080/ws`）
- **STDIO**：`stdio://`（标准输入/输出管道）

## ClientID 分配和握手

### ClientID 值

协议定义了用于系统操作的特殊 ClientID 值：

- **0 (UNASSIGNED)**：客户端在代理分配前的 JOIN 消息中使用
- **1 (BROKER)**：保留给代理内部操作使用
- **1000-4294967294**：代理分配给客户端的有效范围
- **4294967295**：保留供未来使用

### 加入握手过程

1. **初始 JOIN 消息**：客户端发送 ClientID = 0 (UNASSIGNED) 的 JOIN 消息
   - 二进制格式：基础头部字节 2-5 位置为 `0x00 0x00 0x00 0x00`
2. **认证**：代理验证可选的认证凭据
3. **ClientID 分配**：代理从有效范围（≥1000）分配唯一 ClientID
4. **响应**：代理发送在基础头部包含已分配 ClientID 的响应
   - 示例：对于 ClientID 1001，响应在字节 2-5 包含 `0x00 0x00 0x03 0xE9`
5. **确认**：所有后续消息在基础头部使用已分配的 ClientID

### ClientID 分配规则

- 客户端在初始 JOIN 消息中必须使用 ClientID = 0 (`0x00 0x00 0x00 0x00`)
- 代理必须为常规客户端分配 ClientID ≥ 1000
- ClientID 在代理范围内必须唯一
- ClientID 分配在连接生命周期内持续有效
- 重新连接的客户端会接收新的 ClientID 分配

### ClientID 传输示例

**JOIN 消息（客户端 → 代理）：**

```
基础头部：
[0x01] [0x00] [0x00 0x00 0x00 0x00] [16个零字节] [header_len] [payload_len]
 版本   类型  ClientID=0(UNASSIGNED)    预留字节     可变长度      可变长度
```

**响应消息（代理 → 客户端）：**

```
基础头部：
[0x01] [0x02] [0x00 0x00 0x03 0xE8] [16个零字节] [header_len] [payload_len]
 版本   类型  ClientID=1000(已分配)     预留字节     可变长度      可变长度
```

**后续消息（客户端 → 任意方）：**

```
基础头部：
[0x01] [0x01] [0x00 0x00 0x03 0xE8] [16个零字节] [header_len] [payload_len]
 版本   类型  ClientID=1000(已分配)     预留字节     可变长度      可变长度
```

## 连接流程

1. **传输连接**：客户端建立传输层连接
2. **加入握手**：客户端发送 ClientID=0 和可选认证的 JOIN 消息
3. **ClientID 分配**：代理验证凭据并分配唯一 ClientID（≥1000）
4. **分配响应**：代理响应已分配的 ClientID 或错误状态
5. **保活设置**：通过 PING/PONG 可选激活保活机制
6. **消息交换**：客户端使用已分配的 ClientID 交换消息
7. **主题管理**：客户端可订阅/取消订阅主题进行发布/订阅
8. **连接监控**：持续的保活消息以维持连接健康
9. **干净断开**：正确的连接终止或超时处理

## 协议规则

### 消息验证

- REQ 和 REP 消息必须恰好有一个路由条目
- NOTIF 消息可以有多个路由条目
- PUB 消息带有 routing 字段时仅针对指定订阅者，不带 routing 字段时针对所有订阅者
- BCAST 消息不得有路由条目
- SUB 和 UNSUB 消息必须有 topic 字段
- PING 和 PONG 消息必须有带时间戳的 keepalive 字段
- PONG 时间戳必须与对应的 PING 时间戳匹配
- 响应中的状态码是可选的，省略时默认为 200

### 连接要求

- 所有连接都需要 JOIN 握手进行客户端注册
- ClientID 在代理范围内必须唯一
- 认证是可选的，但可在代理中配置
- 代理可基于认证、容量或策略拒绝连接
- 保活是可选的，但建议用于长期连接
- 连接超时应至少为保活间隔的 3 倍

### 主题规则

- 主题名称是 UTF-8 编码字符串
- 主题名称区分大小写
- 空主题名称是有效的
- 客户端仅接收已订阅主题的 PUB 消息
- 取消订阅立即移除订阅
- 对同一主题的多次订阅是幂等的

## 消息大小限制

- **最大消息大小**：16MB（16,777,216 字节）
- **最大头部大小**：64KB（65,536 字节）
- **最大负载大小**：16MB - 头部大小 - 基础头部大小
- **最小消息大小**：34 字节（空头部和负载）
- **保活消息大小**：约 60 字节（基础头部 + 保活头部）

## 实现注意事项

- 所有整数使用网络字节序（大端序）
- MessagePack 用于结构化数据编码（头部和负载）
- 基础头部字段使用固定二进制编码以提高解析效率
- 所有字符串必须是 UTF-8 编码
- 实现应根据类型特定规则验证消息结构
- 基础头部中的预留字节必须设置为零，解析时忽略
- 实现可对消息/头部大小施加额外限制以进行资源管理
