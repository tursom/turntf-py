# AsyncClient 运行时语义

这篇文档专门解释 `turntf-py` 里 `AsyncClient` 的运行时行为、消息时序和共享协议约束。它不重复 API 列表，而是重点说明几个最容易写错的地方：

- `AsyncClient` 与 `AsyncHTTPClient` 的关系
- 双轨登录 selector
- 异步生命周期和重连边界
- `session_ref`
- `save_message -> save_cursor -> ack`
- `resolve_user_sessions` 与 `target_session`
- 错误传播、回调语义与测试覆盖

## 1. 组件模型

`AsyncClient` 是一个“WebSocket 主连接 + HTTP 辅助客户端”的组合体。

### `Config`

`Config` 决定：

- WebSocket 要连到哪个 `base_url`
- 首帧登录使用哪个 `credentials`，即 `(node_id, user_id, password)` 或 `(login_name, password)`
- 消息与游标持久化落到哪个 `cursor_store`
- 断线后是否重连，以及退避参数
- 回调交给哪个 `handler`

### `client.http`

`AsyncClient` 内部总会构造一个 `AsyncHTTPClient`，并通过 `client.http` 暴露出来。它主要用于：

- `client.login()` / `client.login_with_password()`
- 需要 Bearer token 的 HTTP 管理能力

需要再次强调：

`AsyncClient.login()` 只是在“当前对象里顺手提供一个 HTTP 登录入口”。它不会改变当前 WebSocket 身份，也不会把返回的 token 带到 WebSocket RPC 中。

无论 HTTP 还是 WebSocket，登录 selector 都必须满足：

- 旧方式：提供 `node_id` 与 `user_id`
- 新方式：提供 `login_name`
- 两种方式必须二选一，`username` 不参与认证

### `CursorStore`

`CursorStore` 是 SDK 与业务持久层的边界：

```python
class CursorStore(Protocol):
    async def load_seen_messages(self) -> list[MessageCursor]: ...
    async def save_message(self, message: Message) -> None: ...
    async def save_cursor(self, cursor: MessageCursor) -> None: ...
```

这三个方法必须满足的职责分别是：

- `load_seen_messages()`
  返回“已经可靠持久化”的消息游标集合，供下次登录填入 `LoginRequest.seen_messages`
- `save_message()`
  落地完整消息体
- `save_cursor()`
  落地对应的 `(node_id, seq)` 游标

默认的 `MemoryCursorStore` 只适合测试、demo 和单进程调试。它不会跨进程、跨重启保留游标，因此不能提供真正的断线恢复能力。

## 2. 生命周期

### 2.1 构造阶段

构造 `AsyncClient(Config(...))` 时不会发生网络 I/O。这个阶段只做：

- `base_url` 非空校验
- `credentials.node_id` / `credentials.user_id` 正整数校验
- `credentials.password` 校验
- 对若干配置项回填默认值

如果你传了外部 `httpx.AsyncClient` 给 `Config.http_client`，SDK 会复用它，但不会负责关闭它。

### 2.2 `connect()` 的真实语义

`await client.connect()` 的行为可以拆成四步：

1. 如果客户端已 `close()`，立刻抛 `ClosedError`
2. 如果当前已经连通，直接返回
3. 启动内部后台任务 `_run()`
4. 等待“首次连接结果”

所谓“首次连接结果”，指的是：

- 首次登录成功：`connect()` 返回
- 首次连接遭遇不可恢复错误：`connect()` 直接把错误抛给调用方

不可恢复错误的典型例子：

- 登录阶段收到 `unauthorized`
- `reconnect=False` 且首次建连失败
- 客户端已经被 `close()`

一个很重要的边界是：

`connect()` 只负责等待第一次连接建立，不负责等待之后每一次重连。

也就是说：

- 第一次 `await client.connect()` 成功以后
- 如果稍后发生断线
- SDK 会在后台重连
- 但再次调用 `connect()` 不会等到“下一次重连成功”

如果业务需要在每次重连成功后刷新状态，应监听 `handler.on_login()`。

### 2.3 登录成功后会发生什么

每次成功登录时，SDK 会：

1. 读取服务端 `LoginResponse`
2. 提取 `user`、`protocol_version`、`session_ref`
3. 更新 `client.login_info`
4. 更新 `client.session_ref`
5. 调用 `handler.on_login(info)`
6. 启动 ping 循环

这里有两个值得注意的点：

- `handler.on_login()` 是被 `await` 的，它如果很慢，会直接拖慢 `connect()` 返回和后续重连完成
- `session_ref` 在每次重新登录后都可能变化，不能把第一次登录拿到的值永久缓存

### 2.4 正常运行阶段

长连接建立后，`AsyncClient` 可以做两类事情：

- 处理服务端主动推送：`MessagePushed`、`PacketPushed`
- 处理请求响应式 RPC：`send_message`、`ping`、`list_users`、`resolve_user_sessions` 等

内部通过自增 `request_id` 维护 pending RPC 映射。每次 `_rpc()` 都会：

1. 生成新的 `request_id`
2. 注册一个 `Future`
3. 发送 protobuf 请求
4. 用 `asyncio.wait_for(..., timeout=request_timeout)` 等待结果

因此 `request_timeout` 超时后，调用方看到的是标准的 `asyncio.TimeoutError`，而不是 SDK 自定义异常。

### 2.5 断线与自动重连

当读写 websocket 失败，或者底层连接关闭时：

1. SDK 会把当前 `_ws` 清空
2. 把所有 pending RPC 以 `DisconnectedError` 失败
3. 调用 `handler.on_disconnect(error)`
4. 如果允许重连，则按指数退避继续重试

退避算法是：

- 从 `initial_reconnect_delay` 开始
- 每次乘 2
- 不超过 `max_reconnect_delay`

不会自动重连的情况：

- `close()` 之后
- `reconnect=False`
- 登录阶段服务端返回 `unauthorized`
- 当前错误被视为 `ClosedError`

重连时，SDK 会在每次登录前重新调用一次：

```python
seen = await cursor_store.load_seen_messages()
```

然后把这些游标塞进新的 `LoginRequest.seen_messages`。这意味着：

- 可靠重连依赖你的 `CursorStore` 真正保存了已处理游标
- pending RPC 不会自动重放
- 自动重连只负责恢复“连接”，不负责恢复“尚未完成的业务语义”

如果你的业务需要强保证，应该在应用层自行设计幂等重试。

### 2.6 `close()`

`await client.close()` 会：

- 标记客户端为已关闭
- 让首次连接等待者以 `ClosedError` 结束
- 让所有 pending RPC 以 `ClosedError` 结束
- 关闭当前 websocket
- 取消后台运行任务
- 关闭内部 `AsyncHTTPClient`

`close()` 是幂等的；重复调用不会再抛新的异常。

## 3. 回调、存储和顺序保证

### 3.1 `MessagePushed` 的顺序

收到服务端 `MessagePushed` 时，Python SDK 固定按下面顺序执行：

1. `message = message_from_proto(...)`
2. `await cursor_store.save_message(message)`
3. `await cursor_store.save_cursor(message.cursor())`
4. 如果 `ack_messages=True`，发送 `AckMessage`
5. `await handler.on_message(message)`

这正是接入文档要求的顺序。

原因是：

- `AckMessage` 只是在“当前连接内”提示服务端这条消息已见
- 真正影响重连去重的是下次登录时重新上报的 `seen_messages`
- 如果先 ack、后落库，断线时可能已经告诉服务端“我处理过”，但本地实际上还没持久化

因此，不要把这个顺序理解成“性能实现细节”，它是共享协议语义的一部分。

### 3.2 `send_message()` 的顺序

调用 `await client.send_message(...)` 后，如果服务端返回的是持久化消息 `SendMessageResponse.message`，SDK 也会执行：

1. `save_message`
2. `save_cursor`
3. 完成调用方等待的 future

这里不会额外发送 `AckMessage`，因为这不是服务端主动推送的 `MessagePushed`。

### 3.3 `PacketPushed` 的差异

`PacketPushed` 是瞬时包推送，它和持久化消息有本质区别：

- 没有 `(node_id, seq)` 消息游标
- 不参与 `save_cursor`
- 不参与 `AckMessage`
- 不参与 `seen_messages`
- 不会在重连后自动历史补发

如果业务需要本地去重，应自行基于 `packet_id` 维护短期去重表。

### 3.4 回调的执行模型

所有 `Handler` 回调都是在 SDK 内部被 `await` 的：

- `on_login`
- `on_message`
- `on_packet`
- `on_error`
- `on_disconnect`

这意味着：

- 回调太慢会阻塞读循环
- `on_message` 太慢会推迟下一条消息的处理
- `save_message()` / `save_cursor()` 太慢也会拖慢 ack 与后续消息

推荐做法是：

- 在回调里尽快把事件放到内部队列
- 用独立任务消费耗时逻辑
- 不要在回调里执行长期阻塞操作

具体到触发时机：

- `on_error`
  用于处理 envelope 解析 / 映射错误、`AckMessage` 发送异常，以及“即将进入下一轮重连退避前”的可重试连接错误
- `on_disconnect`
  只在当前读循环真正结束时触发一次，表示这一轮 websocket 生命周期已经终止

另外，SDK 会吞掉回调抛出的异常，因此：

- 回调异常不会中断连接
- 回调异常不会向外冒泡给业务调用方
- 需要日志、监控和告警时，应在回调内部显式记录

## 4. `session_ref`、`resolve_user_sessions` 与 `target_session`

### 4.1 `session_ref` 是什么

`session_ref` 表示“当前这次登录成功后，对应的在线 session 身份”。它由两部分组成：

- `serving_node_id`
- `session_id`

Python SDK 暴露路径：

- `client.session_ref`
- `client.login_info.session_ref`
- `handler.on_login(info).session_ref`

### 4.2 为什么要关注它

如果你的业务要：

- 记录当前连接是谁
- 给自己的其他设备做诊断
- 做点对点瞬时投递

就需要使用 `session_ref`。

最重要的约束是：

`session_ref` 只对“当前在线连接”有效。每次重连成功后，它都可能变成一个新的值。

因此，正确做法是：

- 在 `on_login()` 中读取最新的 `session_ref`
- 用新值覆盖旧缓存

而不是：

- 在第一次连接成功时读一次
- 然后永久复用

### 4.3 `resolve_user_sessions`

`resolve_user_sessions(user)` 会返回 `ResolvedUserSessions`，里面有两组信息：

- `presence`
  按 serving node 聚合的在线概览，例如“某节点上有几个 session、transport hint 是什么”
- `sessions`
  具体的 `ResolvedSession` 列表，可直接拿来做定向投递

每个 `ResolvedSession` 包含：

- `session`
  也就是 `SessionRef`
- `transport`
  当前连接的传输类型提示
- `transient_capable`
  是否支持瞬时包投递

### 4.4 `target_session`

`send_packet()` 支持：

```python
await client.send_packet(
    target=user_ref,
    body=b"...",
    delivery_mode=DeliveryMode.ROUTE_RETRY,
    target_session=session_ref,
)
```

语义是：

- 只对瞬时包生效
- 只把这条瞬时包路由到目标用户的指定在线 session

一个常见的使用流程是：

1. `resolved = await client.resolve_user_sessions(target_user)`
2. 选择一个 `resolved.sessions[i]`
3. 如果它 `transient_capable`，取出 `resolved.sessions[i].session`
4. 把它作为 `target_session` 传给 `send_packet()`

需要特别区分：

- `send_message()` 发送的是持久化消息，不支持 `target_session`
- `AsyncHTTPClient.post_packet()` 是 HTTP 入口，也不支持 `target_session`

此外，发送方拿到的 `RelayAccepted` 只表示：

- 服务端已经接受这次瞬时包请求
- 它已经进入本地路由层

它不表示目标用户一定已经处理完成。

## 5. 错误模型

### 5.1 本地校验错误

以下情况一般会直接抛 `ValueError`：

- `base_url` 为空
- `credentials.node_id` / `user_id` 非正整数
- `target.node_id` / `target.user_id` 非正整数
- `body` 为空
- `target_session.session_id` 为空
- `delivery_mode` 不是 `BEST_EFFORT` 或 `ROUTE_RETRY`

### 5.2 连接与传输错误

`ConnectionError` 用来包装：

- websocket 建连失败
- websocket 读失败
- websocket 写失败
- HTTP 请求失败

这里的底层异常会保存在 `.cause` 中。

### 5.3 协议错误

`ProtocolError` 表示“返回值结构不符合 SDK 预期”，例如：

- 登录响应里缺 `session_ref`
- 返回了无法解析的 protobuf 帧
- HTTP 返回了无效 JSON
- 收到不支持的 server envelope

如果你在接入阶段遇到这类错误，优先检查服务端版本、proto 是否同步，以及代理层是否篡改了响应体。

### 5.4 服务端业务错误

`ServerError` 是服务端显式返回的协议错误，包含：

- `code`
- `server_message`
- `request_id`

其中：

- `request_id == 0` 一般表示非某个具体 RPC 的顶层错误，比如登录阶段失败
- `request_id != 0` 表示某个具体 pending RPC 返回错误

登录阶段返回 `unauthorized` 时，SDK 会停止后续重连。

### 5.5 连接状态错误

- `NotConnectedError`
  尚未连接成功就发起 WS RPC
- `DisconnectedError`
  断线时所有 pending RPC 会收到这个错误
- `ClosedError`
  客户端已关闭

一个很关键的现实语义是：

断线时 pending RPC 会失败，但不会在重连后自动重新发送。业务必须自己判断这些操作是否需要重试，以及如何保证幂等。

## 6. `AsyncHTTPClient` 与错误处理的补充

虽然这篇文档主讲 `AsyncClient`，但有两个和 HTTP 入口有关的细节值得一并说明：

1. `AsyncHTTPClient` 使用 Bearer token 做鉴权，不共享 `AsyncClient` 的 WS 登录状态
2. `AsyncHTTPClient` 对“意外 HTTP 状态码”的处理是抛 `ProtocolError`，而不是 `ServerError`

也就是说，在 HTTP 场景里：

- 网络故障通常是 `ConnectionError`
- 状态码不对、JSON 不对通常是 `ProtocolError`

## 7. 测试与 proto 生成

`turntf-py/tests/test_client.py` 当前覆盖了这些关键共享语义：

- 登录成功后返回 `session_ref`
- `MessagePushed` 的 `save_message -> save_cursor -> ack`
- `send_message()` 的持久化响应
- `Ping` 请求 / 响应匹配
- `resolve_user_sessions()` 返回 `presence` 和 `sessions`
- `send_packet(..., target_session=...)` 的定向瞬时投递
- `unauthorized` 停止自动重连
- 重连时重新上报 `seen_messages`
- `transient_only` 和 `/ws/realtime` 路径

`turntf-py/tests/test_http.py` 主要覆盖：

- 登录时密码哈希
- Bearer token 注入
- `bytes` 的 base64 编解码
- `profile_json` / `config_json` 的 JSON 编解码
- HTTP 消息与瞬时包请求形状

如果你修改了 `turntf-py/proto/client.proto`，应在 `turntf-py/` 目录下执行：

```bash
./scripts/gen_proto.sh
```

脚本会把生成结果写入 `turntf/_generated/client_pb2.py`。修改 proto 后，建议至少重新运行一次：

```bash
pytest
```
