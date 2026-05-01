# turntf-py

`turntf-py` 是 turntf 的异步 Python SDK，面向 `asyncio` 场景，封装了两类客户端能力：

- HTTP JSON 管理与查询客户端 `AsyncHTTPClient`
- WebSocket + Protobuf 长连接客户端 `AsyncClient`

它的目标是让业务代码直接使用 Python API，而不是自己处理：

- Bearer token 注入
- JSON / base64 编解码
- WebSocket 建连与重连
- protobuf envelope 收发
- 请求 ID 与响应匹配
- `seen_messages`、`session_ref` 和消息游标的共享语义

当前 Python SDK 第一版只支持 WebSocket，不支持 ZeroMQ。协议里的 `sync_mode` 当前也不暴露给公开 API，这一点与现有 Go SDK 保持一致。

更细的运行时语义见 [docs/async-client.md](docs/async-client.md)。

## 模块定位

如果你的业务是脚本、后台任务、管理面板，或者只需要一次性的管理 / 查询请求，优先使用 `AsyncHTTPClient`。

如果你的业务需要：

- 长连接接收消息
- 自动重连
- `session_ref`
- `resolve_user_sessions`
- `target_session` 定向瞬时投递
- 本地消息持久化与 `seen_messages` 去重

优先使用 `AsyncClient`。

## 主要能力

`AsyncHTTPClient` 主要覆盖：

- HTTP 登录
- 用户 / channel 创建、查询、更新、删除
- 订阅与黑名单管理
- 历史消息查询与 HTTP 发消息
- HTTP 瞬时包投递
- 集群节点、节点在线用户、事件、运维状态与指标查询

`AsyncClient` 在长连接建立后主要覆盖：

- WebSocket 首帧登录
- 自动重连与重登录
- `MessagePushed` / `PacketPushed`
- `send_message()`、`send_packet()`、`ping()`
- `resolve_user_sessions()`
- 用户、订阅、黑名单、历史消息、事件、集群与运维查询类 WS RPC
- `session_ref`、`seen_messages`、`CursorStore` 相关的共享语义封装

## 安装

运行时安装：

```bash
pip install turntf
```

本地开发安装：

```bash
cd turntf-py
pip install -e .[dev]
```

当前要求：

- Python `>= 3.11`
- `bcrypt`
- `httpx`
- `protobuf`
- `websockets>=16`

## `AsyncHTTPClient` 与 `AsyncClient` 的分工

| 维度 | `AsyncHTTPClient` | `AsyncClient` |
| --- | --- | --- |
| 传输 | HTTP JSON | WebSocket + Protobuf |
| 认证方式 | 先 `login()` 拿 Bearer token，再按请求携带 | `Config.credentials` 作为 WS 首帧登录身份 |
| 典型场景 | 启动脚本、后台管理、调试、无状态查询 | 常驻连接、收发消息、自动重连、按 session 投递 |
| 自动重连 | 不提供 | 提供指数退避重连 |
| `session_ref` | 不涉及 | 登录成功后可通过 `client.session_ref` 获取 |
| `resolve_user_sessions` | 不提供 | 提供 |
| `target_session` | 不提供 | `send_packet(..., target_session=...)` 支持 |
| 本地游标 / `seen_messages` | 不涉及 | 通过 `CursorStore` 提供 |

需要特别注意：

`AsyncClient.login()` 只是复用内部 `AsyncHTTPClient` 调用 HTTP `/auth/login`，返回 Bearer token。它不会修改 `Config.credentials`，也不会把 token 注入后续 WebSocket RPC。`AsyncClient` 真正的连接身份始终由 `Config.credentials` 决定。

HTTP 和 WebSocket 登录都支持双轨 selector：

- 旧方式：`node_id + user_id + password`
- 新方式：`login_name + password`

两种 selector 必须二选一，`username` 不参与认证。

## 快速开始

### 只使用 `AsyncHTTPClient`

```python
import asyncio

from turntf import AsyncHTTPClient, CreateUserRequest, UserRef, plain_password


async def main() -> None:
    async with AsyncHTTPClient("http://127.0.0.1:8080") as client:
        token = await client.login(4096, 1, "root")

        user = await client.create_user(
            token,
            CreateUserRequest(
                username="alice",
                login_name="alice.login",
                password=plain_password("alice-password"),
                profile_json=b'{"tier":"gold"}',
                role="user",
            ),
        )
        print("created:", user)

        fetched = await client.get_user(token, UserRef(node_id=4096, user_id=user.user_id))
        print("fetched:", fetched.username)


asyncio.run(main())
```

### 使用 `AsyncClient` 建立长连接

```python
import asyncio

from turntf import (
    AsyncClient,
    Config,
    Credentials,
    DeliveryMode,
    MemoryCursorStore,
    NopHandler,
    UserRef,
    plain_password,
)


class PrintHandler(NopHandler):
    async def on_login(self, info) -> None:  # type: ignore[override]
        print("login:", info.protocol_version, info.session_ref)

    async def on_message(self, message) -> None:  # type: ignore[override]
        print("message:", message.cursor(), message.body)

    async def on_packet(self, packet) -> None:  # type: ignore[override]
        print("packet:", packet.packet_id, packet.target_session)

    async def on_disconnect(self, error) -> None:  # type: ignore[override]
        print("disconnect:", error)


async def main() -> None:
    async with AsyncClient(
        Config(
            base_url="http://127.0.0.1:8080",
            credentials=Credentials(
                login_name="alice.login",
                password=plain_password("alice-password"),
            ),
            cursor_store=MemoryCursorStore(),
            handler=PrintHandler(),
        )
    ) as client:
        await client.connect()

        print("current session:", client.session_ref)

        await client.send_message(
            UserRef(node_id=4096, user_id=1025),
            b"hello",
        )

        resolved = await client.resolve_user_sessions(
            UserRef(node_id=4096, user_id=1025),
        )
        if resolved.sessions:
            session = resolved.sessions[0]
            await client.send_packet(
                UserRef(node_id=4096, user_id=1025),
                b"ping",
                DeliveryMode.ROUTE_RETRY,
                target_session=session.session,
            )


asyncio.run(main())
```

`async with AsyncClient(...)` 只负责在退出时调用 `close()`，不会自动 `connect()`。长连接必须显式 `await client.connect()`。

## `AsyncClient` 关键配置项

`AsyncClient` 通过 `Config` 配置运行行为：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `base_url` | 无 | 必填，支持 `http://`、`https://`、`ws://`、`wss://` |
| `credentials` | 无 | 必填，WebSocket 首帧登录使用 `(node_id, user_id, password)` 或 `(login_name, password)` |
| `cursor_store` | `MemoryCursorStore()` | 消息持久化与游标存储；默认只适合测试和单进程调试 |
| `handler` | `NopHandler()` | 登录、消息、瞬时包、错误、断线回调 |
| `http_client` | `None` | 可注入复用的 `httpx.AsyncClient`；注入后由调用方负责关闭 |
| `reconnect` | `True` | 是否在断线后自动重连 |
| `initial_reconnect_delay` | `1.0` | 首次重连退避秒数；传 `<= 0` 会回退到 `1.0` |
| `max_reconnect_delay` | `30.0` | 最大重连退避秒数；传 `<= 0` 会回退到 `30.0` |
| `ping_interval` | `30.0` | 应用层 ping 周期；传 `<= 0` 会回退到 `30.0` |
| `request_timeout` | `10.0` | 单次 RPC 超时秒数，超时后抛 `asyncio.TimeoutError` |
| `ack_messages` | `True` | 收到 `MessagePushed` 后是否自动发送 `AckMessage` |
| `transient_only` | `False` | 原样写入 `LoginRequest.transient_only` |
| `realtime_stream` | `False` | `False` 走 `/ws/client`，`True` 走 `/ws/realtime` |

密码字段不是明文字符串，而是 `PasswordInput`。通常使用：

- `plain_password("secret")`：先做 bcrypt 哈希，再上送协议
- `hashed_password(existing_hash)`：直接复用已有哈希值

## 异步生命周期

`AsyncClient` 的生命周期和普通 HTTP 客户端不同，建议按下面理解：

1. 构造对象
   此时不会建立任何网络连接。
2. `await client.connect()`
   SDK 启动后台连接循环，并等待“首次连接结果”。
3. 首次登录成功
   SDK 设置 `login_info` / `session_ref`，调用 `handler.on_login()`，然后 `connect()` 返回。
4. 正常运行
   业务可调用 `send_message()`、`send_packet()`、`resolve_user_sessions()` 等 WS RPC。
5. 断线
   所有进行中的 pending RPC 会以 `DisconnectedError` 失败；如果允许重连，后台任务会继续指数退避重连。
6. 重连成功
   SDK 会重新登录、刷新 `session_ref`，再次触发 `handler.on_login()`。
7. `await client.close()`
   关闭 WebSocket、取消后台任务、让 pending RPC 以 `ClosedError` 结束。

重要语义：

- `connect()` 是“首次连接屏障”，不是“每次重连完成屏障”。
- 首次连接成功后，如果后续发生断线，重连在后台继续进行；再次调用 `connect()` 不会等待下一次重连完成。
- 如果你需要感知每一次重连成功，请在 `handler.on_login()` 里刷新状态，而不是依赖重复调用 `connect()`。

更完整的运行时说明见 [docs/async-client.md](docs/async-client.md)。

## 重要共享语义

### `session_ref`

登录成功后，服务端会返回当前在线连接对应的 `session_ref`。Python SDK 会把它放到：

- `client.login_info.session_ref`
- `client.session_ref`
- `handler.on_login(info)` 的 `info.session_ref`

这个值会在每次成功重连后变化。如果业务要做定向瞬时投递，应该在每次 `on_login()` 时刷新本地缓存。

### `save_message -> save_cursor -> ack`

收到 `MessagePushed` 时，SDK 固定按下面顺序执行：

1. `cursor_store.save_message(message)`
2. `cursor_store.save_cursor(message.cursor())`
3. 如果 `ack_messages=True`，发送 `AckMessage`
4. 调用 `handler.on_message(message)`

这条顺序不能倒置。可靠重连依赖下次登录时把已落地游标放回 `seen_messages`；如果先 ack 再落库，断线后可能丢消息。

另外：

- `send_message()` 收到 `SendMessageResponse.message` 时，也会先执行 `save_message -> save_cursor`
- `PacketPushed` 没有消息游标，不参与 `save_cursor`、`AckMessage` 和 `seen_messages`

### `resolve_user_sessions` 与 `target_session`

如果你要把瞬时包只投递到目标用户的某一个在线连接，推荐流程是：

1. `resolved = await client.resolve_user_sessions(user_ref)`
2. 从 `resolved.sessions` 里选择具体 `SessionRef`
3. 调用 `await client.send_packet(..., target_session=session_ref)`

其中：

- `resolved.presence` 是按节点聚合的在线概览
- `resolved.sessions` 是可直接用于投递的具体 session 列表
- `ResolvedSession.transient_capable` 表示该 session 是否支持瞬时投递
- `target_session` 只对瞬时包有效，不适用于持久化 `send_message()`

HTTP 客户端的 `post_packet()` 不支持 `target_session`；需要按 session 定向时，请使用 `AsyncClient.send_packet()`。

## 错误处理

常见错误类型如下：

- `ValueError`
  本地参数校验失败，例如空 `base_url`、空消息体、非法 `UserRef` / `SessionRef`
- `ServerError`
  WebSocket 协议层返回的服务端错误，包含 `code`、`server_message`、`request_id`
- `ProtocolError`
  返回包结构不符合预期，或 HTTP 状态码 / JSON / protobuf 帧异常
- `ConnectionError`
  HTTP 请求、WebSocket 建连、读写失败
- `NotConnectedError`
  还未连接成功就发起 WS RPC
- `DisconnectedError`
  连接中断，进行中的 pending RPC 被失败
- `ClosedError`
  客户端已经关闭
- `asyncio.TimeoutError`
  单次 RPC 超过 `request_timeout`

补充说明：

- 登录阶段如果服务端返回 `unauthorized`，SDK 会停止自动重连。
- 断线后 pending RPC 不会自动重放；需要业务按幂等语义自行重试。
- `Handler` 回调里的异常会被 SDK 吞掉，不会向外传播，也不会中断连接循环；如果你需要告警或日志，请在回调内部自行处理。

## `AsyncHTTPClient` 的使用边界

`AsyncHTTPClient` 更适合下面这些工作：

- 先用 root 登录拿 token
- 创建用户 / channel
- 订阅、黑名单、历史查询、集群查询
- 一次性的 HTTP 发消息或发瞬时包

HTTP 客户端会自动处理：

- `Authorization: Bearer <token>`
- `bytes` 消息体的 base64 编解码
- `profile_json` / `config_json` 的 JSON 编解码

但它不负责：

- 自动重连
- `session_ref`
- `resolve_user_sessions`
- `target_session`
- 本地游标与 `seen_messages`

## 测试

在 `turntf-py/` 目录下运行：

```bash
pytest
```

当前测试覆盖了这些核心行为：

- HTTP 登录、Bearer token 注入和 JSON / base64 编解码
- WebSocket 登录、`session_ref` 返回与 `Ping`
- `MessagePushed` 的 `save_message -> save_cursor -> ack` 顺序
- `send_message()` 返回持久化消息
- `resolve_user_sessions()` 与 `target_session` 定向瞬时投递
- `unauthorized` 停止自动重连
- 重连时通过 `seen_messages` 上报已持久化游标
- `/ws/realtime` 与 `transient_only` 登录参数

## 重新生成 protobuf

仓库已经带上生成结果。如果你修改了 [proto/client.proto](proto/client.proto)，可以在 `turntf-py/` 目录下重新生成：

```bash
./scripts/gen_proto.sh
```

脚本依赖：

- `protoc` 已安装并出现在 `PATH`

生成结果会写入：

- `turntf/_generated/client_pb2.py`

## 包内容

- `turntf/`
  SDK 公开 API、错误类型、数据模型、游标存储接口
- `turntf/_generated/`
  由 `proto/client.proto` 生成的 protobuf 类型
- `tests/`
  HTTP 与 WebSocket 客户端测试
- `scripts/gen_proto.sh`
  Python protobuf 生成脚本
