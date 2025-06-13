## fleximq-peer API 形状

仅供参考，所有命名方式与最终实现无关w

1. 核心类型

```rust
// 核心Peer类型，支持构建器模式
pub struct Peer {
    // 内部字段...
}

// 请求处理上下文，类似axum的Context
pub struct Context {
    message: Message,
    state: Arc<State>,
    // 其他上下文...
}

// 共享状态容器
pub struct State(Arc<AnyMap>);

// 路由器，类似axum的Router
pub struct Router<S = State> {
    // 内部路由表...
    routes: DashMap<String, Box<dyn Handler<S>>>,
    state: S,
}

// 来自 tower 的 service 等内容
use tower::*;
```

2. Peer 的构建器API

```rust
impl Peer {
    // 创建新的Peer实例
    pub fn new(client_name: impl Into<String>) -> Self { ... }
    
    // 连接到broker
    pub fn connect(self, transport: impl Transport) -> Self { ... }
    
    // 设置认证
    pub fn with_auth(self, auth: Auth) -> Self { ... }
    
    // 设置消息处理路由
    pub fn with_router(self, router: Router) -> Self { ... }
    
    // 设置状态
    pub fn with_state(self, state: impl Into<State>) -> Self { ... }
    
    // 添加全局中间件
    pub fn layer<M>(self, middleware: M) -> Self
    where
        M: Middleware<State>,
    { ... }
    
    // 启动对等体
    pub async fn run(self) -> Result<(), Error> { ... }
}
```

3. 路由API

```rust
impl<S> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    // 创建新路由器
    pub fn new() -> Self { ... }
    
    // 注册路径处理器
    pub fn route(self, path: &str, handler: impl Handler<S>) -> Self { ... }
    
    // 快捷方法：处理请求消息，基于注册路径处理器方法
    pub fn request(self, path: &str, handler: impl Handler<S>) -> Self { ... }
    
    // 快捷方法：处理通知消息，基于注册路径处理器方法
    pub fn notification(self, path: &str, handler: impl Handler<S>) -> Self { ... }
    
    // 快捷方法：处理广播消息，基于注册路径处理器方法
    pub fn broadcast(self, handler: impl Handler<S>) -> Self { ... }
    
    // 添加中间件
    pub fn layer<M>(self, middleware: M) -> Self
    where
        M: Middleware<S>,
    { ... }
    
    // 嵌套路由器
    pub fn nest(self, prefix: &str, router: Router<S>) -> Self { ... }
    
    // 合并状态
    pub fn with_state<S2>(self, state: S2) -> Router<S2>
    where
        S2: Clone + Send + Sync + 'static,
    { ... }
}
```

4. 消息发送API

```rust
impl Peer {
    // 发送请求并接收响应
    pub async fn request(
        &self,
        target: impl Into<String>,
        path: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<Message, Error> { ... }
    
    // 发送通知到相应路由（不需要响应）
    pub async fn notify(
        &self,
        target: impl Into<String>,
        path: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<(), Error> { ... }
    
    // 广播消息到所有 client（不需要响应）
    pub async fn broadcast(
        &self,
        payload: impl Into<Bytes>,
    ) -> Result<(), Error> { ... }
    
    // 发布消息到主题
    pub async fn publish(
        &self,
        topic: impl Into<String>,
        payload: impl Into<Bytes>,
    ) -> Result<(), Error> { ... }
    
    // 订阅主题(只有第一次订阅向 broker 发送订阅消a)
    pub async fn subscribe(&self, topic: impl Into<String>, subscription: Handler) -> Result<Subscription, Error> { ... }
    
    // 取消订阅主题(如果有多个订阅，则全部取消后再向 broker 发送取消消息)
    pub async fn unsubscribe(&self, subscription: Subscription) -> Result<(), Error> { ... }
}
```

5. 处理器提取器（Extractors，使用MessagePack）

```rust
// 消息载荷提取
pub struct Payload<T>(pub T);

// MessagePack提取
pub struct MessagePack<T>(pub T);

// 消息头信息提取
pub struct HeaderInfo { ... }

// 从消息中提取数据的特质
pub trait Extractor<S> { ... }

// 为提取器实现FromContext特质
impl<T, S> Extractor<S> for Payload<T> { ... }
// 还有很多有用的提取器...

// 为函数中间件和函数处理器使用协变泛型参数自动从Context中提取数据
pub fn handler<T, R, S>(Fn(T) -> R) -> impl Handler<S>
where
    T: Extractor<S>,
{ ... }

// 实现了Handler特质的类型也可以使用提取器
```