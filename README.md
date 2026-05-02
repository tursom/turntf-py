# turntf-py

## 概述

`turntf-py` 是 turntf 的异步 Python SDK，基于 `asyncio`，封装了两类客户端能力：

- **`AsyncHTTPClient`** -- HTTP JSON 管理与查询客户端
- **`AsyncClient`** -- WebSocket + Protobuf 长连接客户端

SDK 的核心目标是让业务代码直接使用 Python API，而不需要自行处理：

- Bearer token 注入
- JSON / base64 编解码
- WebSocket 建连与重连
- Protobuf envelope 收发
- 请求 ID 与响应匹配
- `seen_messages`、`session_ref` 和消息游标的共享语义

当前 Python SDK 第一版只支持 WebSocket，不支持 ZeroMQ。协议里的 `sync_mode` 当前也不暴露给公开 API，这一点与现有 Go SDK 保持一致。

密码字段统一封装为 `PasswordInput` 类型，提供两种构造方式：

- `plain_password("secret")` -- 先做 bcrypt 哈希再上送
- `hashed_password(existing_hash)` -- 直接复用已有的 bcrypt 哈希值

---

## 安装

```bash
pip install turntf
```

本地开发安装：

```bash
cd turntf-py
pip install -e .[dev]
```

**运行依赖：**

- Python `>= 3.11`
- `bcrypt`
- `httpx`
- `protobuf`
- `websockets>=16`

---

## 快速开始

### AsyncHTTPClient -- HTTP JSON 客户端

`AsyncHTTPClient` 适用于管理类、后台任务、脚本等场景。通过 `async with` 管理生命周期，初始化后先 `login()` 获取 token，后续操作携带 token 即可。

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

### AsyncClient -- WebSocket 长连接客户端

`AsyncClient` 适用于常驻连接、实时收发消息、自动重连场景。需要自定义 `Handler` 处理推送事件，通过 `Config` 配置连接参数。

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
    async def on_login(self, info) -> None:
        print("login:", info.protocol_version, info.session_ref)

    async def on_message(self, message) -> None:
        print("message:", message.cursor(), message.body)

    async def on_packet(self, packet) -> None:
        print("packet:", packet.packet_id, packet.target_session)

    async def on_disconnect(self, error) -> None:
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

> `async with AsyncClient(...)` 只负责在退出时调用 `close()`，不会自动 `connect()`。长连接必须显式 `await client.connect()`。

---

## API 概览

### 公开导出

所有公开类型均通过 `turntf/__init__.py` 导出，导入方式：

```python
from turntf import (
    # 客户端
    AsyncClient, AsyncHTTPClient,
    # 配置
    Config, Credentials,
    # 处理器
    Handler, NopHandler,
    # 密码
    PasswordInput, plain_password, hashed_password, hash_password,
    # 游标存储
    CursorStore, MemoryCursorStore,
    # 数据模型
    User, UserRef, SessionRef, Message, Packet, MessageCursor,
    Attachment, AttachmentType, Subscription, BlacklistEntry,
    Event, ClusterNode, LoggedInUser,
    LoginInfo, DeliveryMode,
    ResolvedSession, ResolvedUserSessions, OnlineNodePresence,
    CreateUserRequest, UpdateUserRequest, DeleteUserResult,
    UpsertUserMetadataRequest, ScanUserMetadataRequest,
    UserMetadata, UserMetadataScanResult,
    RelayAccepted,
    OperationsStatus, PeerStatus, PeerOriginStatus,
    MessageTrimStatus, EventLogTrimStatus, ProjectionStatus,
    # 异常
    TurntfError,
    ServerError, ProtocolError, ConnectionError,
    NotConnectedError, DisconnectedError, ClosedError,
)
```

### 错误类型

| 异常 | 说明 |
| --- | --- |
| `TurntfError` | 所有 SDK 异常的基类 |
| `ServerError` | 服务端返回的错误，含 `code`、`server_message`、`request_id` |
| `ProtocolError` | 协议层异常，如 HTTP 状态码/JSON/Protobuf 帧异常 |
| `ConnectionError` | HTTP 请求或 WebSocket 建连/读写失败 |
| `NotConnectedError` | 尚未连接成功就发起 WS RPC |
| `DisconnectedError` | 连接中断，pending RPC 被失败 |
| `ClosedError` | 客户端已关闭 |
| `asyncio.TimeoutError` | 单次 RPC 超过 `request_timeout` |

补充说明：

- 登录阶段如服务端返回 `unauthorized`，SDK 会停止自动重连
- 断线后 pending RPC 不会自动重放，需业务按幂等语义自行重试
- `Handler` 回调中的异常会被 SDK 吞掉，不向外传播也不中断连接循环

---

## 模块定位与选型

| 维度 | `AsyncHTTPClient` | `AsyncClient` |
| --- | --- | --- |
| 传输 | HTTP JSON | WebSocket + Protobuf |
| 认证方式 | `login()` 获取 Bearer token，按请求携带 | `Config.credentials` 作为 WS 首帧登录身份 |
| 典型场景 | 启动脚本、后台管理、调试、无状态查询 | 常驻连接、实时收发消息、自动重连、定向投递 |
| 自动重连 | 不提供 | 提供指数退避自动重连 |
| `session_ref` | 不涉及 | 登录后通过 `client.session_ref` 获取 |
| `resolve_user_sessions` | 不提供 | 提供 |
| `target_session` | 不提供 | `send_packet(..., target_session=...)` 支持 |
| 本地游标 / `seen_messages` | 不涉及 | 通过 `CursorStore` 提供 |

### 何时使用 AsyncHTTPClient

适合脚本、后台任务、管理面板，或只需要一次性管理/查询请求的场景：

- 先用 root 登录获取 token
- 创建用户 / channel
- 订阅、黑名单、历史查询、集群查询
- 一次性的 HTTP 发消息或发瞬时包

HTTP 客户端自动处理 `Authorization: Bearer <token>`、`bytes` 消息体的 base64 编解码、`profile_json` / `config_json` 的 JSON 编解码。

### 何时使用 AsyncClient

适合常驻连接、实时通信的场景：

- 长连接接收实时消息和瞬时包
- 自动重连与重登录
- `session_ref` 管理
- 按 session 定向瞬时投递
- 本地消息持久化与 `seen_messages` 去重

> **重要**：`AsyncClient.login()` 只是复用内部 `AsyncHTTPClient` 调用 HTTP `/auth/login` 返回 Bearer token。它不会修改 `Config.credentials`，也不会把 token 注入后续 WebSocket RPC。`AsyncClient` 真正的连接身份始终由 `Config.credentials` 决定。

登录认证同时支持双轨 selector：

- 旧方式：`node_id + user_id + password`
- 新方式：`login_name + password`

两种 selector 必须二选一，`username` 不参与认证。

---

## 文档导航

- [AsyncClient 运行时说明](docs/async-client.md) -- 异步生命周期、重连行为、`Config` 配置项详解
- [AsyncHTTPClient 使用指南](docs/http-client.md) -- HTTP 客户端完整 API 参考
- [SDK 开发指南](docs/development.md) -- 本地开发、测试与贡献

### AsyncClient 关键配置项

`AsyncClient` 通过 `Config` 配置运行行为：

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `base_url` | 无 | 必填，支持 `http://`、`https://`、`ws://`、`wss://` |
| `credentials` | 无 | 必填，WebSocket 首帧登录身份凭据 |
| `cursor_store` | `MemoryCursorStore()` | 消息持久化与游标存储；默认仅适合测试和单进程调试 |
| `handler` | `NopHandler()` | 登录、消息、瞬时包、错误、断线回调 |
| `http_client` | `None` | 可注入复用的 `httpx.AsyncClient`；注入后由调用方负责关闭 |
| `reconnect` | `True` | 是否在断线后自动重连 |
| `initial_reconnect_delay` | `1.0` | 首次重连退避秒数 |
| `max_reconnect_delay` | `30.0` | 最大重连退避秒数 |
| `ping_interval` | `30.0` | 应用层 ping 周期 |
| `request_timeout` | `10.0` | 单次 RPC 超时秒数 |
| `ack_messages` | `True` | 收到 `MessagePushed` 后是否自动发送 `AckMessage` |
| `transient_only` | `False` | 原样写入 `LoginRequest.transient_only` |
| `realtime_stream` | `False` | `False` 走 `/ws/client`，`True` 走 `/ws/realtime` |

### 异步生命周期

`AsyncClient` 的生命周期和普通 HTTP 客户端不同，建议按以下顺序理解：

1. **构造对象** -- 不建立任何网络连接
2. **`await client.connect()`** -- 启动后台连接循环，等待首次连接结果
3. **首次登录成功** -- SDK 设置 `login_info` / `session_ref`，调用 `handler.on_login()`，`connect()` 返回
4. **正常运行** -- 业务调用 `send_message()`、`send_packet()`、`resolve_user_sessions()` 等 WS RPC
5. **断线** -- 所有 pending RPC 以 `DisconnectedError` 失败；如允许重连，后台任务继续指数退避
6. **重连成功** -- 重新登录、刷新 `session_ref`，再次触发 `handler.on_login()`
7. **`await client.close()`** -- 关闭 WebSocket、取消后台任务、pending RPC 以 `ClosedError` 结束

**重要语义：** `connect()` 是"首次连接屏障"，不是"每次重连完成屏障"。如需感知每次重连成功，请在 `handler.on_login()` 中刷新状态。

### 共享语义

#### `session_ref`

登录成功后服务端返回当前连接的 `session_ref`，可通过以下方式获取：

- `client.login_info.session_ref`
- `client.session_ref`
- `handler.on_login(info)` 的 `info.session_ref`

该值在每次成功重连后变化。做定向瞬时投递时，应在每次 `on_login()` 时刷新本地缓存。

#### `save_message -> save_cursor -> ack`

收到 `MessagePushed` 时，SDK 严格按以下顺序执行：

1. `cursor_store.save_message(message)`
2. `cursor_store.save_cursor(message.cursor())`
3. 如果 `ack_messages=True`，发送 `AckMessage`
4. 调用 `handler.on_message(message)`

此顺序不能倒置。可靠重连依赖下次登录时将已落地游标放回 `seen_messages`；先 ack 再落库可能在断线后丢消息。

另外：

- `send_message()` 收到 `SendMessageResponse.message` 时，同样执行 `save_message -> save_cursor`
- `PacketPushed` 没有消息游标，不参与 `save_cursor`、`AckMessage` 和 `seen_messages`

#### `resolve_user_sessions` 与 `target_session`

将瞬时包定向投递到目标用户的某个在线连接：

1. `resolved = await client.resolve_user_sessions(user_ref)`
2. 从 `resolved.sessions` 中选择具体的 `SessionRef`
3. `await client.send_packet(..., target_session=session_ref)`

其中：

- `resolved.presence` 是按节点聚合的在线概览
- `resolved.sessions` 是可直接投递的具体 session 列表
- `ResolvedSession.transient_capable` 表示该 session 是否支持瞬时投递
- `target_session` 仅对瞬时包有效，不适用于持久化 `send_message()`

HTTP 客户端的 `post_packet()` 不支持 `target_session`；需要按 session 定向时请使用 `AsyncClient.send_packet()`。

---

## 构建与测试

### 运行测试

```bash
cd turntf-py
pytest
```

当前测试覆盖的核心行为：

- HTTP 登录、Bearer token 注入和 JSON / base64 编解码
- WebSocket 登录、`session_ref` 返回与 `Ping`
- `MessagePushed` 的 `save_message -> save_cursor -> ack` 顺序
- `send_message()` 返回持久化消息
- `resolve_user_sessions()` 与 `target_session` 定向瞬时投递
- `unauthorized` 停止自动重连
- 重连时通过 `seen_messages` 上报已持久化游标
- `/ws/realtime` 与 `transient_only` 登录参数

### 重新生成 protobuf

仓库已包含生成结果。修改 [proto/client.proto](proto/client.proto) 后，在 `turntf-py/` 目录下重新生成：

```bash
./scripts/gen_proto.sh
```

**依赖：** `protoc` 已安装并出现在 `PATH` 中。

生成结果写入 `turntf/_generated/client_pb2.py`。

### 包目录结构

```
turntf-py/
  turntf/               SDK 公开 API、错误类型、数据模型、游标存储接口
  turntf/_generated/    由 proto/client.proto 生成的 protobuf 类型
  tests/                HTTP 与 WebSocket 客户端测试
  scripts/gen_proto.sh  Python protobuf 生成脚本
  docs/                 运行时说明与使用指南
  proto/client.proto    协议定义
```
