# fleximq 协议 V1

> 注意：本协议中所有多字节数值均采用网络字节序（大端序）编码。
> 所有结构化数据（Header 和 Payload）均使用 MessagePack 格式进行序列化。

## 1. 引言

### 1.1. 协议版本

当前协议版本为 1。这由基础格式中的 `Version` 字段指明。

### 1.2. 主要设计目标

旨在提供一个通用的、编程语言无关的、跨平台的消息传递协议。它支持多种通信模式，如请求/响应、发布/订阅和通知，可运行于多种传输层之上。该协议注重效率和清晰的语义定义，使其适用于分布式系统、插件系统，并为物联网（IoT）应用进行了优化。与 MQTT 等协议相比，fleximq 支持更广泛也更复杂的通信模式。

### 1.3. 编码

- **数值**: 所有多字节数值均采用网络字节序（大端序）编码。
- **结构化数据**: Header 和 Payload 部分使用 MessagePack 格式进行序列化。
- **字符串**: 所有字符串必须使用 UTF-8 编码。

## 2. 核心消息结构

### 2.1. 基础格式

| 字段          | 类型   | 大小（字节） | 描述                                 |
| ------------- | ------ | ------------ | ------------------------------------ |
| Version       | uint8  | 1            | 协议版本 (1)                         |
| Type          | uint8  | 1            | 消息类型 (参见章节 3.1)              |
| ClientID      | uint32 | 4            | 唯一客户端标识符（32位大端序）       |
| Reserved      | bytes  | 16           | 预留字节供未来使用（必须为零）       |
| HeaderLength  | uint32 | 4            | 头部段的字节长度（32位大端序）       |
| Header        | bytes  | 可变         | 可变长度头部数据（MessagePack 编码） |
| PayloadLength | uint64 | 8            | 负载段的字节长度（64位大端序）       |
| Payload       | bytes  | 可变         | 可变长度负载数据（MessagePack 编码） |

**基础头部总大小: 34 字节** (1字节 Version + 1字节 Type + 4字节 ClientID + 16字节 Reserved + 4字节 HeaderLength + 8字节 PayloadLength)。

### 2.2. 头部 (Header)

#### 2.2.1. 通用结构

头部是一个 MessagePack 编码的映射（map），包含各种可选字段，为消息提供元数据。这些字段的存在与否及其必要性取决于消息的 `Type`。

```
头部映射 (概念结构):
  "routing"   => 路由对象数组 (可选)
  "reqrep"    => 请求/响应对象 (可选)
  "topic"     => 字符串 (可选)
  "status"    => 整数 (可选, 参见章节 5.1 状态码)
  "auth"      => 认证对象 (可选, 参见章节 5.2 认证类型)
  "keepalive" => 保活对象 (可选, 参见章节 3.7 连接保活)
```

#### 2.2.2. 通用头部字段

##### `routing` (路由对象)

指定消息的目标接收方。

```
路由对象:
  "client_id" => 32位无符号整数 (目标客户端ID)
  "path"      => 字符串 (用于目标端点/处理器的路由路径字符串)
```

_示例:_

```json
{
  "client_id": 1234,
  "path": "/api/endpoint"
}
```

- `client_id`: 标识此路由信息相关的特定客户端。
- `path`: 一个由应用程序定义的字符串，接收方客户端可以用它来将消息导向特定的内部处理器、资源或函数。其解释是上下文相关的，取决于应用程序逻辑。

##### `reqrep` (请求/响应对象)

用于关联请求和响应。

```
请求/响应对象:
  "type" => 字符串 ("request" 或 "correlation")
  "id"   => 字符串 (用于关联的唯一标识符, 推荐使用 ULID)
```

_示例 (请求):_

```json
{
  "type": "request",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

_示例 (响应关联):_

```json
{
  "type": "correlation",
  "id": "01H2XQMGPE7ZPXXNKQD9XE8S6R"
}
```

- 对于 REQ 消息: `type` 必须为 "request"。
- 对于 REP 消息: `type` 必须为 "correlation"，且 `id` 必须与原始 REQ 消息的 `id` 匹配。

##### `topic` (字符串)

指定发布/订阅消息的主题。
_示例: "news_updates"_

##### `status` (整数)

指示操作的结果或状态，通常用于 REP 消息或错误的 NOTIF 消息。值基于类似 HTTP 的状态码 (参见章节 5.1)。

##### `auth` (认证对象)

包含用于客户端身份验证的凭据。有关不同认证类型的详细信息，请参见章节 5.2。

```
认证对象:
  "type"     => 字符串 ("token", "basic", 或 "api_key")
  "token"    => 字符串 (用于令牌认证)
  "username" => 字符串 (用于基本认证)
  "password" => 字符串 (用于基本认证)
  "api_key"  => 字符串 (用于 API 密钥认证)
```

##### `keepalive` (保活对象)

用于 PING 和 PONG 消息中，进行连接健康监控。参见章节 3.7。

```
保活对象:
  "timestamp" => 64位无符号整数 (消息创建时的Unix时间戳，毫秒)
  "interval"  => 32位无符号整数 (建议的保活间隔秒数，可选，仅在 PING 中)
```

#### 2.2.3. 各消息类型的头部字段要求

| 消息类型 | 必需字段        | 可选字段        | 禁止字段                                  |
| -------- | --------------- | --------------- | ----------------------------------------- |
| JOIN     | -               | auth            | routing, reqrep, topic, keepalive, status |
| REQ      | routing, reqrep | status          | topic, auth, keepalive                    |
| REP      | routing, reqrep | status          | topic, auth, keepalive                    |
| NOTIF    | routing         | status          | reqrep, topic, auth, keepalive            |
| BCAST    | -               | status          | routing, reqrep, topic, auth, keepalive   |
| PUB      | topic           | routing, status | reqrep, auth, keepalive                   |
| SUB      | topic           | -               | routing, reqrep, status, auth, keepalive  |
| UNSUB    | topic           | -               | routing, reqrep, status, auth, keepalive  |
| PING     | keepalive       | -               | routing, reqrep, topic, status, auth      |
| PONG     | keepalive       | -               | routing, reqrep, topic, status, auth      |

### 2.3. 负载 (Payload)

#### 2.3.1. 通用描述

`Payload` 字段承载消息的主要数据。它是一个使用 MessagePack 编码的可变长度字节序列。负载内部数据的结构和解释由应用程序定义。基础格式中的 `PayloadLength` 字段指定此负载数据的大小（字节）。如果消息没有主要数据要传递，`PayloadLength` 可以为零，此时 `Payload` 字段为空。

## 3. 消息类型与通信模式

### 3.1. 消息类型概览

| 类型  | 名称         | 值  | 描述                 | 相关头部字段 (典型)           |
| ----- | ------------ | --- | -------------------- | ----------------------------- |
| JOIN  | Join         | 0   | 加入组/连接到代理    | `auth` (可选)                 |
| REQ   | Request      | 1   | 发送请求给单个客户端 | `routing`, `reqrep`           |
| REP   | Response     | 2   | 发送对请求的响应     | `routing`, `reqrep`, `status` |
| NOTIF | Notification | 3   | 发送通知给特定客户端 | `routing`                     |
| BCAST | Broadcast    | 4   | 向所有客户端发送广播 | `status` (可选)               |
| PUB   | Topic        | 5   | 向主题发布消息       | `topic`, `routing` (可选)     |
| SUB   | Subscribe    | 6   | 订阅主题             | `topic`                       |
| UNSUB | Unsubscribe  | 7   | 取消订阅主题         | `topic`                       |
| PING  | Ping         | 8   | 连接保活心跳         | `keepalive`                   |
| PONG  | Pong         | 9   | 连接保活回应         | `keepalive`                   |

_(为快速参考添加了"相关头部字段"列，从详细需求表中简化而来)_

### 3.2. 连接建立 (JOIN)

- **目的**: 客户端用于向代理发起连接并注册自身。这是客户端在建立传输层连接后必须发送的第一条消息。
- **头部**: 如果代理要求认证，可以包含一个 `auth` 对象。
- **基础头部中的 ClientID**: 客户端发送时必须为 0 (UNASSIGNED)。代理以分配的 ClientID 作为响应 (参见章节 4.1)。

_示例 (客户端发送带令牌认证的 JOIN 消息):_

```
基础头部: Version=1, Type=JOIN, ClientID=0, HeaderLength=X, PayloadLength=0
头部: {
  "auth": {
    "type": "token",
    "token": "authentication-token"
  }
}
负载: (空)
```

### 3.3. 请求/响应 (REQ, REP)

- **目的与流程**: 使一个客户端能够向另一个客户端（或由客户端代表的服务）发送请求并接收响应。
  1. 客户端 A 向客户端 B 发送 REQ 消息。`reqrep` 头部字段包含 `type: "request"` 和一个唯一的 `id`。
  2. 客户端 B 处理请求并向客户端 A 发回 REP 消息。`reqrep` 头部字段包含 `type: "correlation"` 和与 REQ 消息相同的 `id`。
- **REQ 头部**: - `routing`: 必须包含一个路由条目，指定目标客户端和路径。- `reqrep`: `type` 必须为 "request"，`id` 必须是唯一标识符。- `status`: 可选，在 REQ 中很少使用。
  _示例 (REQ 消息):_

```
基础头部: Version=1, Type=REQ, ClientID=客户端A的ID, HeaderLength=X, PayloadLength=Y
头部: {
  "routing": [{ "client_id": 目标客户端ID或代理服务ID, "path": "/api/users" }],
  "reqrep": { "type": "request", "id": "unique-request-id" }
}
负载: { "user_data": "..." } // 示例负载
```

- **REP 头部**: - `routing`: 必须包含一个路由条目，指定原始请求者。- `reqrep`: `type` 必须为 "correlation"，`id` 必须与相应 REQ 的 `id` 匹配。- `status`: 可选 (如果省略则默认为 200 OK)，指示请求的结果 (参见章节 5.1)。
  _示例 (REP 消息):_

```
基础头部: Version=1, Type=REP, ClientID=客户端B的ID, HeaderLength=X, PayloadLength=Z
头部: {
  "routing": [{ "client_id": 原始请求者客户端A的ID, "path": "/api/users" }], // 响应中路径可能被省略或仅供参考
  "reqrep": { "type": "correlation", "id": "unique-request-id" },
  "status": 200
}
负载: { "result": "..." } // 示例负载
```

- **关联**: `reqrep` 中的 `id` 字段对于将响应与请求匹配至关重要。推荐使用 ULID 以保证唯一性。

### 3.4. 通知 (NOTIF)

- **目的**: 允许客户端或代理向一个或多个特定客户端发送单向信息性消息。不期望响应。
- **头部**: - `routing`: 包含一个或多个路由条目，每个条目指定目标 `client_id` 和 `path`。- `status`: 可选，可用于指示通知的性质或严重性（例如，错误状态）。
  _示例 (向多个客户端发送 NOTIF 消息):_

```
基础头部: Version=1, Type=NOTIF, ClientID=发送者ID, HeaderLength=X, PayloadLength=Y
头部: {
  "routing": [
    { "client_id": 1234, "path": "/notifications" },
    { "client_id": 5678, "path": "/alerts" }
  ],
  "status": 600 // 示例：如果某个目标有问题则为 Client Not Found，或一般信息
}
负载: { "message": "系统即将维护" } // 示例负载
```

### 3.5. 广播 (BCAST)

- **目的**: 允许客户端或代理向所有已连接且已授权的客户端发送消息。
- **头部**: - `status`: 可选，可以为广播提供上下文。- 不允许 `routing` 字段，因为消息隐式地发给所有客户端。
  _注: 代理负责分发 BCAST 消息。客户端通常向代理发送 BCAST，然后由代理分发。_
  _示例 (BCAST 消息):_

```
基础头部: Version=1, Type=BCAST, ClientID=发送者ID, HeaderLength=X, PayloadLength=Y
头部: {
  "status": 200 // 可选，例如一般性公告
}
负载: { "announcement": "新功能已上线！" } // 示例负载
```

### 3.6. 发布/订阅 (PUB, SUB, UNSUB)

- **目的与流程**: 实现发布-订阅消息模式。
  1. 对某类消息感兴趣的客户端向代理发送 SUB 消息，指定一个 `topic`。
  2. 发布者客户端向代理发送 PUB 消息，指定一个 `topic` 和消息内容。
  3. 代理将 PUB 消息传递给当前订阅了该 `topic` 的所有客户端。
  4. 客户端可以发送 UNSUB 消息以停止接收某个 `topic` 的消息。
- **PUB 头部**: - `topic`: 必需，指定消息的主题。- `routing`: 可选。如果存在，代理可能会使用它来进一步筛选主题上的哪些订阅者接收消息（例如，如果订阅者也注册了特定的路径，或者路由到管理该主题的特定代理实例）。_说明: 默认情况下，如果 `routing` 缺失，消息将发送给该 `topic` 的所有订阅者。如果 `routing` 存在，其对 PUB 的解释可以是代理特定的，可能针对订阅者的子集或处理该主题分片的特定代理。_ - `status`: 可选。
  _示例 (PUB 消息):_

```
基础头部: Version=1, Type=PUB, ClientID=发布者ID, HeaderLength=X, PayloadLength=Y
头部: {
  "topic": "news_updates",
  // "routing": [{ "client_id": 某个特定订阅者ID或代理服务ID, "path": "/regional" }] // 可选路由
}
负载: { "article": "新发现..." } // 示例负载
```

- **SUB 头部**: - `topic`: 必需，指定要订阅的主题。
  _示例 (SUB 消息):_

```
基础头部: Version=1, Type=SUB, ClientID=订阅者ID, HeaderLength=X, PayloadLength=0
头部: {
  "topic": "news_updates"
}
负载: (空)
```

- **UNSUB 头部**: - `topic`: 必需，指定要取消订阅的主题。
  _示例 (UNSUB 消息):_

```
基础头部: Version=1, Type=UNSUB, ClientID=订阅者ID, HeaderLength=X, PayloadLength=0
头部: {
  "topic": "news_updates"
}
负载: (空)
```

### 3.7. 连接保活 (PING, PONG)

协议包含内置的保活机制，使用 PING 和 PONG 消息来维持连接健康并检测断开连接。

- **目的**:
  - **PING 消息**: 由任一方（客户端或代理）发送以测试连接活性。
  - **PONG 消息**: 响应 PING 消息以确认连接健康。
- **头部**: PING 和 PONG 消息都必须包含一个 `keepalive` 对象。
  - `keepalive.timestamp`: PING/PONG 消息创建时的 Unix 时间戳（毫秒）。
  - `keepalive.interval`: (可选，仅 PING) 建议的后续保活间隔秒数。

#### 3.7.1. 保活机制与规则

1.  **PING 发起**: 客户端或代理都可以通过发送 PING 消息发起保活。
2.  **PONG 响应**: PING 消息的接收方必须用 PONG 消息响应。PONG 消息中的 `keepalive.timestamp` 必须与收到的 PING 消息中的 `timestamp` 完全相同。
3.  **超时检测**: PING 的发送方应启动一个计时器。如果在合理的超时期限内（例如，PONG 超时时间）未收到相应的 PONG，发送方应认为连接已断开或已降级。
4.  **间隔建议**: PING 消息可以包含一个 `keepalive.interval` 字段，以建议未来保活检查的新间隔。接收方没有义务必须遵守此建议，但可以用它来调整自己的保活计时器。
5.  **自动保活**: 如果没有其他数据流量，实现可以定期自动发送 PING 消息，以防止中间设备因空闲而超时，并主动检查连接健康状况。

#### 3.7.2. 保活示例

**PING 消息:**

```
基础头部: Version=1, Type=PING, ClientID=我的ID, HeaderLength=X, PayloadLength=0
头部: {
  "keepalive": {
    "timestamp": 1703123456789, // 当前毫秒时间戳
    "interval": 30              // 建议下次 ping 间隔30秒
  }
}
负载: (空)
```

**PONG 响应:**

```
基础头部: Version=1, Type=PONG, ClientID=对端ID, HeaderLength=X, PayloadLength=0
头部: {
  "keepalive": {
    "timestamp": 1703123456789 // 必须匹配 PING 中的时间戳
  }
}
负载: (空)
```

#### 3.7.3. 保活最佳实践

- **默认间隔**: 通常的默认保活间隔是 30 秒。
- **PONG 超时**: 接收 PONG 的超时时间通常应为保活间隔的倍数（例如，间隔的 2 到 3 倍）。这考虑了网络延迟。对于 30 秒的间隔，PONG 超时时间可能是 60-90 秒。
- **连接失败**: 实现应定义处理持续丢失 PONG 的连接的策略，例如在声明连接死亡前尝试有限次数的重试，并可能对重连尝试使用指数退避。
- **优雅降级**: 应用程序应能优雅地处理保活失败，例如尝试重新连接或通知用户。

## 4. 连接管理

### 4.1. 客户端ID (ClientID)

#### 4.1.1. 特殊客户端ID值

协议为系统操作和客户端标识定义了特定的 `ClientID` 值：

- **0 (UNASSIGNED)**: `0x00000000`。客户端在其初始 JOIN 消息中使用，此时代理尚未分配唯一的 `ClientID`。
- **1 (BROKER)**: `0x00000001`。保留给源自代理或明确针对代理本身的用于管理或内部操作的消息。_说明: ClientID=1 的确切用法可能因代理实现而异，例如，用于代理间通信或系统消息。_
- **1000 - 4294967294** (`0x000003E8` - `0xFFFFFFFE`): 代理向已连接客户端动态分配 `ClientID` 的有效范围。
- **4294967295 (MAX_UINT32)**: `0xFFFFFFFF`。保留供将来使用或用于特殊广播/多播范围（如果定义）。

#### 4.1.2. 客户端ID分配规则

- 客户端在其初始 JOIN 消息的基础头部的 `ClientID` 字段中必须使用 `ClientID = 0` (UNASSIGNED)。
- 代理必须为每个成功认证并接受的客户端从有效动态范围（1000 至 4294967294，含边界）分配一个唯一的 `ClientID`。
- 分配的 `ClientID` 在单个代理实例的范围内必须唯一。
- 分配的 `ClientID` 在连接的生命周期内保持不变。
- 如果客户端断开连接然后重新连接，它必须再次执行加入握手过程，并且通常会收到一个新的、唯一的 `ClientID`。代理不应立即重用 `ClientID`，以避免可能正在经历临时断开的客户端发生消息错投。

### 4.2. 加入握手过程

加入握手是客户端向代理注册并获取唯一 `ClientID` 的强制性过程。

1.  **传输连接**: 客户端首先与代理建立传输层连接（例如 TCP、WebSocket）。
2.  **初始 JOIN 消息**:
    - 客户端向代理发送 JOIN 消息。
    - 此 JOIN 消息的基础头部中的 `ClientID` 字段必须设置为 0 (UNASSIGNED)。
    - 如果客户端希望进行身份验证或代理要求身份验证，则 JOIN 消息的头部部分可以包含一个 `auth` 对象。
3.  **代理处理**:
    - 代理接收 JOIN 消息。
    - 如果存在 `auth` 对象，代理将验证身份验证凭据。如果身份验证失败或要求身份验证但未提供，代理可能会发回带有适当错误 `status` 的 REP 消息（例如，401 Unauthorized, 403 Forbidden）然后关闭连接，或者直接关闭连接。
    - 如果身份验证成功（或不需要），代理会从有效动态范围（例如 ≥1000）为客户端分配一个唯一的 `ClientID`。
4.  **代理响应 (分配或拒绝)**:
    - **成功**: 代理向客户端发回 REP 消息（或专门的 JOIN_ACK 类型，如果定义了的话，尽管通常在此处使用 REP）。
      - 此响应消息的基础头部中的 `ClientID` 字段设置为新分配给客户端的唯一 `ClientID`。
      - 此 REP 消息的 `Header` 可能包含一个 `status` 代码（例如，200 OK 或 201 Created），并且如果由代理定义，还可以选择性地包含其他连接参数或确认详细信息。
      - 如果客户端的 JOIN 消息包含了 `reqrep.id`，则头部中的 `reqrep` 字段应该进行关联。
    - **失败 (认证后)**: 如果无法分配 `ClientID`（例如，代理达到容量限制），代理会发送带有适当错误 `status` 的 REP 消息（例如，503 Service Unavailable）并可能关闭连接。
5.  **客户端确认和使用**:
    - 客户端接收代理的响应。
    - 如果响应指示成功并包含已分配的 `ClientID`，则客户端在当前连接期间发送的所有后续消息的基础头部中的 `ClientID` 字段都必须使用此分配的 `ClientID`。
    - 如果响应指示错误，客户端应适当处理（例如，显示错误、重试、中止）。

#### 4.2.1. 客户端ID传输示例

**1. JOIN 消息 (客户端 → 代理):**
客户端使用 `ClientID = 0` 发起连接。

```
基础头部:
[0x01] [0x00] [0x00 0x00 0x00 0x00] [16个零字节] [header_len] [payload_len]
 版本 类型    ClientID=0(UNASSIGNED)  预留字节      (可变)     (可变)
```

_头部 (示例，带令牌认证):_

```json
{
  "auth": { "type": "token", "token": "client-auth-token" }
}
```

_负载: (JOIN 通常为空)_

**2. 响应消息 (代理 → 客户端):**
代理响应，分配 `ClientID = 1000` (示例)。

```
基础头部:
[0x01] [0x02] [0x00 0x00 0x03 0xE8] [16个零字节] [header_len] [payload_len]
 版本 类型    ClientID=1000(已分配)   预留字节      (可变)     (可变)
```

_(注: 原始示例使用 Type=REP (0x02)。这是确认 JOIN 的一种常见方式)_
_头部 (示例):_

```json
{
  "status": 200 // OK
  // "reqrep": { "type": "correlation", "id": "if_client_sent_one" } // 如果客户端的JOIN消息中包含reqrep.id
}
```

_负载: (通常为空或包含代理信息)_

**3. 后续消息 (客户端 → 代理/其他客户端):**
客户端现在使用其分配的 `ClientID = 1000`。

```
基础头部:
[0x01] [0x01] [0x00 0x00 0x03 0xE8] [16个零字节] [header_len] [payload_len]
 版本 类型    ClientID=1000(已分配)   预留字节      (可变)     (可变)
```

_(示例: 这可能是一条 REQ 消息, Type=0x01)_

### 4.3. 连接流程概要

1.  **传输连接**: 客户端与代理建立传输层连接（例如 TCP、WebSocket）。
2.  **加入握手**:
    - 客户端发送 JOIN 消息 (ClientID=0, Header 中可选 `auth`)。
    - 代理验证 (如果提供了 `auth`)，分配唯一的 ClientID (例如 >=1000)。
    - 代理在其基础头部的 `ClientID` 字段中包含已分配的 ClientID，并在其 Header 中包含成功 `status`（或加入失败时的错误 `status`）来发送响应 (例如 REP 消息)。
3.  **操作阶段**:
    - 客户端对所有后续消息使用已分配的 ClientID。
    - 客户端可以根据协议规则交换 REQ/REP、PUB/SUB、NOTIF、BCAST 消息。
    - 客户端或代理都可以使用可选的保活机制 (PING/PONG) 来监控连接健康状况。
4.  **断开连接**:
    - 连接可以被干净地终止（例如，客户端发送最后一条消息然后关闭，或者代理发起断开）。
    - 连接可能由于错误、超时（包括保活失败）或网络问题而终止。
    - 实现应优雅地处理突然断开的情况。

## 5. 数据表示详情

### 5.1. 状态码

协议在消息头部的可选 `status` 字段中使用类似 HTTP 的状态码。这些代码为操作结果提供标准化的反馈。如果响应消息（例如 REP）省略了 `status` 字段，则默认为 `200 OK`。

#### 5.1.1. 成功码 (200-299)

- **200 OK**: 请求成功完成。(如果 REP 中省略 `status`，则为默认值)
- **201 Created**: 资源已成功创建（例如，在导致实体创建的 REQ 之后）。
- **202 Accepted**: 请求已接受处理，但处理尚未完成（例如，用于异步操作）。
- **204 No Content**: 请求成功完成，但负载中没有数据可返回。(PayloadLength 将为 0)。

#### 5.1.2. 客户端错误码 (400-499)

- **400 Bad Request**: 请求格式错误、包含无效语法或在头部或负载中包含无效参数。
- **401 Unauthorized**: 需要身份验证，客户端未提供凭据或提供的凭据无效。
- **403 Forbidden**: 已通过身份验证的客户端无权执行请求的操作或访问请求的资源。
- **404 Not Found**: 找不到请求的资源（例如，由路由中的 `path` 或 `topic` 指定）。
- **405 Method Not Allowed**: 目标资源不支持该操作（由消息类型或路径指示）。
- **408 Request Timeout**: 客户端未在服务器准备等待的时间内生成请求。(在本协议中不太常见，通常由传输层或保活机制处理超时)。_说明: 此状态通常指客户端发送超时，而 504 是服务器端上游超时。_
- **409 Conflict**: 由于与目标资源的当前状态冲突，无法完成请求。
- **413 Payload Too Large**: 请求负载大于服务器愿意或能够处理的负载。
- **429 Too Many Requests**: 用户在给定时间内发送了过多的请求（"速率限制"）。

#### 5.1.3. 服务器错误码 (500-599)

- **500 Internal Server Error**: 服务器遇到了阻止其完成请求的意外情况。
- **501 Not Implemented**: 服务器不支持完成请求所需的功能（例如，无法识别的消息类型或特性）。
- **502 Bad Gateway**: 服务器在充当网关或代理时，从其为完成请求而访问的上游服务器收到了无效响应。
- **503 Service Unavailable**: 由于过载或维护，服务器暂时无法处理请求。
- **504 Gateway Timeout**: 服务器在充当网关或代理时，未及时从上游服务器收到响应。

#### 5.1.4. 协议特定码 (600-699)

这些代码由本协议定义，用于标准 HTTP 类代码未涵盖的情况。

- **600 Client Not Found**: `routing` 对象中指定的目标 `client_id` 与活动且已知的客户端不对应。
- **601 Topic Not Found**: PUB、SUB 或 UNSUB 消息中指定的 `topic` 不存在或代理无法识别。
- **602 Invalid Routing**: 消息头中提供的 `routing` 信息无效、格式错误或无法处理（例如，在预期位置缺少 `client_id` 或 `path`）。
- **603 Subscription Failed**: 代理未能处理 SUB 请求，原因并非主题未找到（例如，客户端超出订阅限制，主题授权失败）。
- **604 Authentication Failed**: 一般身份验证失败，未被 401 覆盖（例如，格式错误的 auth 对象，不支持的认证类型）。
- **605 Join Rejected**: JOIN 请求被代理拒绝，原因并非身份验证（例如，代理达到最大容量，客户端 IP 被禁止）。

### 5.2. 认证类型

认证详细信息包含在消息头部的 `auth` 对象中，主要在 JOIN 消息期间使用。

#### 5.2.1. 令牌认证 (Token Authentication)

- `auth.type`: "token"
- `auth.token`: 持票人令牌 (Bearer token) 字符串。
  _头部片段示例:_
  ```json
  {
    "auth": {
      "type": "token",
      "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
    }
  }
  ```

#### 5.2.2. 基本认证 (Basic Authentication)

- `auth.type`: "basic"
- `auth.username`: 用户名字符串。
- `auth.password`: 密码字符串。
  _头部片段示例:_
  ```json
  {
    "auth": {
      "type": "basic",
      "username": "user123",
      "password": "securePassword!"
    }
  }
  ```
  _注意: 如果传输层未加密，基本认证以可能不安全的方式传输凭据。强烈建议与 TLS/WSS 一起使用。_

#### 5.2.3. API 密钥认证 (API Key Authentication)

- `auth.type`: "api_key"
- `auth.api_key`: API 密钥字符串。
  _头部片段示例:_
  ```json
  {
    "auth": {
      "type": "api_key",
      "api_key": "abcdef1234567890zyxwvu"
    }
  }
  ```

## 6. 传输层

### 6.1. 支持的传输方式

该协议设计为可在各种传输机制上运行。传输方式的选择会影响连接在较低级别的建立和管理方式。

| 传输方式  | URL 方案   | 描述                                      | 连接发起方    | ClientID 分配方 |
| :-------- | :--------- | :---------------------------------------- | :------------ | :-------------- |
| TCP       | `tcp://`   | 标准 TCP 套接字连接。                     | 客户端        | 代理 (Broker)   |
| IPC       | `ipc://`   | 进程间通信 (例如 Unix 套接字, 命名管道)。 | 客户端        | 代理 (Broker)   |
| WebSocket | `ws://`    | WebSocket 连接 (通常通过 HTTP/HTTPS)。    | 客户端        | 代理 (Broker)   |
| STDIO     | `stdio://` | 用于子进程的标准输入/输出管道。           | 代理 (Broker) | 代理 (Broker)   |

- **连接发起方**: 指定通常由哪一方发起传输级连接。
- **ClientID 分配方**: 指示在协议握手期间哪一方负责分配唯一的 `ClientID` (始终是代理，除了 STDIO，在 STDIO 中，如果代理派生客户端，则代理可以预先分配或使用简化方案)。_STDIO 说明: 代理分配 ClientID，但由于它派生进程，分配可以是派生机制的一部分，而不是典型的 JOIN 消息响应。_

### 6.2. 传输 URL

这些示例说明了连接到代理的常见 URL 格式。

- **TCP**: `tcp://hostname:port`
  - 示例: `tcp://localhost:8080`, `tcp://192.168.1.100:7890`
- **IPC**: `ipc:///path/to/socket` (类 Unix) 或 `ipc://pipename` (类 Windows)
  - 示例 (Unix): `ipc:///tmp/fleximq.sock`
  - 示例 (Windows): `ipc://fleximq_pipe`
  - 有关详细的 IPC 路径处理，请参见章节 6.3。
- **WebSocket**: `ws://hostname:port/path` (或 `wss://` 用于安全 WebSocket)
  - 示例: `ws://localhost:8080/ws`, `wss://example.com/messaging`
- **STDIO**: `stdio://`
  - 此方案指示客户端将通过其标准输入和标准输出流与代理通信。它通常在代理进程派生客户端进程时使用。不适用主机名或端口。

### 6.3. IPC 路径处理

IPC 传输需要对 Unix 域套接字和 Windows 命名管道进行特定于平台的路径处理，以确保跨平台兼容性和易用性。

#### 6.3.1. Unix 域套接字

- **路径格式**: 接受绝对路径 (例如 `/var/run/app.sock`) 或相对路径 (例如 `app.sock`)。
- **相对路径的默认目录**: 如果提供相对路径 (例如 `socket.sock`)，则通常相对于默认目录（如 `/tmp`）进行解析（从而得到 `/tmp/socket.sock`）。此默认值可能因实现而异。
- **目录创建**: 实现（特别是监听器/代理端）应在套接字文件的父目录尚不存在时自动创建它们，但仅限于需要创建的那部分路径。例如，如果创建 `/var/run/app/socket.sock` 并且 `/var/run/app` 不存在，则应创建它。但是，如果 `/var` 或 `/var/run` 不存在，则不应尝试创建它们。
- **路径保留**: 绝对路径按原样使用，确保套接字位置可预测。
- **清理**: 监听器在正常关闭时应移除套接字文件，以防止后续监听器启动时出现问题。

#### 6.3.2. Windows 命名管道

- **路径格式**: 可以接受简单的管道名称 (例如 `myapp_pipe`) 或文件样式的路径 (例如 `C:\temp\app.pipe`)。
- **内部管道命名空间**: 所有管道名称在内部都会转换为 Windows 命名管道命名空间格式：`\\.\pipe\{提取的名称}`。
- **从文件样式路径中提取名称**: 如果给出文件样式路径 (例如 `C:\temp\app.pipe`)，通常会提取文件的基本名称 (例如 `app`) 以形成管道名称。文件扩展名 (如 `.pipe`) 通常会被去除。
- **简化命名**: 为清晰起见，鼓励使用简单名称 (例如 `ipc://my_pipe_name`)。
- **实例管理**: 通常，监听器（代理）会创建一个管道实例来处理传入的客户端连接。客户端连接到这个众所周知的管道名称。

#### 6.3.3. 跨平台 URL 示例

- **Unix (绝对路径)**: `ipc:///var/run/app/socket.sock` → 监听器使用路径 `/var/run/app/socket.sock`。
- **Unix (相对路径)**: `ipc://app.sock` → 监听器可能使用 `/tmp/app.sock` (取决于实现的默认设置)。
- **Windows (文件样式路径)**: `ipc://C:\mydata\pipes\app.pipe` → 监听器使用 `\\.\pipe\app` (或 `\\.\pipe\app.pipe`，取决于提取逻辑)。
- **Windows (简单名称)**: `ipc://myapp_service` → 监听器使用 `\\.\pipe\myapp_service`。

### 6.4. STDIO 传输详情

STDIO 传输专门设计用于代理进程启动并直接与客户端子进程通信的场景。

- **连接发起方**: 从代理派生客户端进程并通过其标准管道建立通信通道的意义上说，代理始终是连接发起方。
- **通信通道**: 代理写入客户端的 `stdin` 并从客户端的 `stdout` 读取。反之，客户端写入其 `stdout` 并从其 `stdin` 读取。
- **使用场景**: 非常适合紧密耦合的系统，其中代理管理作为单独可执行文件运行的工作进程、专用处理程序或插件。
- **进程生命周期**: 客户端子进程的生命周期通常由代理管理。如果通信通道关闭或发生错误，代理可能会终止子进程。
- **安全性**: 除非在派生过程中明确修改，否则客户端子进程会继承派生它的代理进程的安全权限和环境变量。
- **STDIO下的ClientID分配**: 由于代理派生客户端，它可以通过多种方式分配 `ClientID`：
  1.  **环境变量**: 在派生客户端时将 ClientID作为环境变量传递。
  2.  **命令行参数**: 将 ClientID 作为命令行参数传递给客户端进程。
  3.  **初始消息**: 代理可以通过 `stdin` 向客户端发送第一条消息（在典型 JOIN 之外），提供其 `ClientID`。
      如果在 STDIO 设置中预先分配了 `ClientID`，则带有 `ClientID=0` 的标准 JOIN 消息可能会被绕过或简化。如果仍需要标准握手，代理将充当响应 JOIN 的授权方。

## 7. 操作注意事项

### 7.1. 消息大小限制

为确保稳定并防止资源耗尽，实现应定义并强制执行消息大小限制。以下是建议的限制，但可以配置。

- **最大总消息大小**: 1GB (1,073,741,824 字节)。这是基础头部、头部和负载的总和。
- **最大头部大小 (`HeaderLength`)**: 64KB (65,536 字节)。
- **最大负载大小 (`PayloadLength`)**: 计算方式为：最大总消息大小 - 基础头部大小 (34 字节) - 实际 `HeaderLength`。实际上接近 1GB。
- **最小消息大小**: 34 字节。这对应于具有空头部 (`HeaderLength` = 0) 和空负载 (`PayloadLength` = 0) 的消息。
- **典型保活消息大小 (PING/PONG)**: 大约 50-70 字节，具体取决于 `keepalive` 对象的 MessagePack 编码。例如，基础头部 (34 字节) + HeaderLength (4 字节) + 典型的 `keepalive` 对象 (例如，`{"keepalive":{"timestamp":1234567890123,"interval":30}}` 约 15-30 字节)。

### 7.2. 消息大小计算示例

**1. 带令牌认证的 JOIN 消息:**

- 基础头部字段 (Version, Type, ClientID, Reserved, HeaderLength, PayloadLength): 34 字节 (固定部分，不包括可变头部和负载本身)。
- 头部数据 (MessagePack 编码的 `auth` 对象):
  - `auth` 对象: `{"auth":{"type":"token","token":"authentication-token-string"}}`
  - 短令牌的 MessagePack 大小示例: 约 40-80 字节。这是可变的。本例中假设约为 **60 字节**。
  - 因此，基础头部中的 `HeaderLength` 字段 = 60。
- 负载数据: JOIN 通常为空。
  - 基础头部中的 `PayloadLength` 字段 = 0。
- **总预估消息大小**: 34 字节 (基础) + 60 字节 (头部) + 0 字节 (负载) = **约 94 字节**。

**2. 典型的 REQ/REP 消息 (例如 API 调用):**

- 基础头部字段: 34 字节。
- 头部数据 (MessagePack 编码的 `routing` 和 `reqrep` 对象):
  - `routing` 对象: `{"routing":[{"client_id":1234,"path":"/api/users/get"}]}`
  - `reqrep` 对象: `{"reqrep":{"type":"request","id":"01H2XQMGPE7ZPXXNKQD9XE8S6R"}}`
  - 这些组合的 MessagePack 大小示例: 约 80-120 字节。本例中假设约为 **100 字节**。
  - 因此，基础头部中的 `HeaderLength` 字段 = 100。
- 负载数据: 可变，取决于实际的请求/响应数据。
  - 令 `PayloadSize` 为 MessagePack 编码的负载的大小。
  - 基础头部中的 `PayloadLength` 字段 = `PayloadSize`。
- **总预估消息大小**: 34 字节 (基础) + 100 字节 (头部) + `PayloadSize` = **约 134 字节 + PayloadSize**。

### 7.3. 通用协议规则

#### 7.3.1. 消息验证规则

实现（代理和客户端）必须根据这些规则验证消息。验证失败的消息应被拒绝，可能会返回一个包含 `400 Bad Request` 或更具体错误状态的 REP 消息。

- **REQ 和 REP 消息**: 其头部的 `routing` 数组中必须恰好有一个条目。
- **NOTIF 消息**: `routing` 数组中可以有一个或多个条目。如果 `routing` 为空或不存在，则对于 NOTIF 而言是协议违规。
- **PUB 消息**:
  - 头部必须有 `topic` 字段。
  - 头部的 `routing` 字段是可选的。
    - 如果 `routing` 不存在，消息通常会发布给该 `topic` 的所有订阅者。
    - 如果 `routing` 存在，其解释可以是代理特定的（例如，针对订阅者的子集、特定的代理实例或主题内的基于路径的过滤）。实现必须明确定义此行为。
- **BCAST 消息**: 头部不得有 `routing` 字段。
- **SUB 和 UNSUB 消息**: 头部必须有 `topic` 字段。它们不得包含 `routing`, `reqrep`, `status`, `auth`, 或 `keepalive` 字段。
- **PING 和 PONG 消息**: 头部必须有 `keepalive` 字段，其中必须包含 `timestamp`。它们不得包含 `routing`, `reqrep`, `topic`, 或 `auth` 字段。`status` 也不适用。
- **PONG `timestamp`**: PONG 消息的 `keepalive` 对象中的 `timestamp` 必须与触发它的相应 PING 消息中的 `timestamp` 匹配。
- **`status` 代码**: 通常在响应消息（如 REP）中是可选的。如果在 REP 消息中省略，则表示 `200 OK`。对于 `status` 是可选的其他消息（例如 NOTIF, BCAST, PUB），其缺失意味着除了消息的固有目的之外，没有传递特定的状态。
- **预留字节**: 基础头部中的 16 个 `Reserved` 字节必须由发送方设置为零，并在解析时由接收方忽略。这允许未来的协议扩展，而不会破坏此字段的向后兼容性。

#### 7.3.2. 连接生命周期与要求

- **JOIN 握手**: 所有客户端连接（如前所述，STDIO 可能例外）都必须以加入握手过程（章节 4.2）开始，以进行客户端注册和 `ClientID` 分配。
- **ClientID 唯一性**: 分配的 `ClientID` 在代理的操作范围内必须唯一。
- **认证**: 虽然协议定义了 `auth` 头部字段，但认证的强制执行由代理的配置决定。代理可以允许匿名访问或要求特定的认证类型。
- **代理拒绝**: 代理可以基于各种标准拒绝 JOIN 尝试或后续消息：认证失败、无效消息格式、资源限制（例如，最大连接数）或其他策略决策（例如，IP 黑名单）。
- **保活**: 虽然定义了 PING/PONG 消息，但它们用于主动保活是可选的，但对于长连接或跨越具有空闲超时的网络的连接，则强烈建议使用。客户端和代理都可以发起 PING。
- **连接超时**: 如果使用保活，期望 PONG 响应的超时应可配置，并且通常应至少是 PING 间隔的 2-3 倍，以适应网络延迟。

#### 7.3.3. 主题与订阅规则

- **主题名称**: 是 UTF-8 编码的字符串。主题名称的有效字符和最大长度的定义可以是特定于实现的，但它们应该是实用的。
- **大小写敏感性**: 主题名称默认区分大小写（例如，"MyTopic" 与 "mytopic" 不同）。实现可以将不区分大小写匹配作为可配置选项提供，但应明确记录此行为。
- **空主题名称**: 协议本身不禁止空字符串作为主题名称。但是，代理可以根据策略拒绝空主题名称。通常建议使用有意义的非空主题名称。
- **订阅范围**: 客户端仅接收其主动且成功订阅的主题的 PUB 消息。
- **取消订阅**: UNSUB 消息应导致立即移除客户端对指定主题的订阅。在代理处理 UNSUB 之后，客户端不应再收到该主题的 PUB 消息。
- **订阅的幂等性**: 来自同一客户端的对同一主题的多个 SUB 消息通常是幂等的。客户端保持订阅状态；再次发送 SUB 通常除了可能刷新订阅计时器（如果代理实现了此类计时器）之外没有其他效果。
- **通配符订阅**: 当前协议规范未明确定义主题订阅的通配符语法（例如 `news.*` 或 `updates/#`）。如果某个实现支持通配符主题，则必须明确定义其语法和匹配语义。

### 7.4. 实现说明

- **整数编码**: 所有多字节整数（例如 `ClientID`, `HeaderLength`, `PayloadLength`, 时间戳, 间隔）必须以网络字节序（大端序）编码。
- **结构化数据序列化**: `Header` (如果存在且 `HeaderLength` > 0) 和 `Payload` (如果存在且 `PayloadLength` > 0) 部分必须使用 MessagePack 进行序列化。
- **基础头部解析**: 基础头部字段 (Version, Type, ClientID, Reserved, HeaderLength, PayloadLength) 使用固定的二进制编码和位置，从而允许在可能反序列化 MessagePack 头部之前进行高效的初始解析。
- **字符串编码**: 协议内的所有字符串（例如，`path`, `topic`, `id`, `token`, `username`, `password`, `api_key` 等头部字段中的字符串，以及 MessagePack 编码的 Payload 内的任何字符串）都必须是 UTF-8 编码的。
- **消息结构验证**: 实现（客户端和代理）必须根据消息的 `Type` 和章节 7.3.1 中的规则严格验证传入消息的结构。这包括检查必需的头部字段、禁止的字段和数据类型。
- **资源管理**: 为了有效地管理资源并防止滥用，实现可以对消息大小、头部大小、路由条目数量或其他参数施加比章节 7.1 中建议的更严格的限制。此类限制应可配置并明确记录。
- **错误处理**: 健壮的错误处理至关重要。实现应在 REP 消息中使用适当的 `status` 代码来表示错误，并优雅地处理意外数据或断开连接。
- **可扩展性**: 基础头部中的 `Reserved` 字段旨在用于未来的协议演进。Header 和 Payload 使用 MessagePack 也为添加新的可选字段提供了一定的灵活性，而不会破坏忽略未知字段的旧客户端。
