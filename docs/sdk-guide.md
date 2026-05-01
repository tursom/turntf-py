# turntf-py SDK 使用指南

本文档是 turntf-py SDK 的整体使用指南，帮助你理解两个客户端的分工、如何根据业务场景选择合适的客户端，以及常见的使用模式。

## 目录

1. [SDK 架构概览](#1-sdk-架构概览)
2. [场景选择指南](#2-场景选择指南)
3. [认证方式](#3-认证方式)
4. [数据模型速查](#4-数据模型速查)
5. [常见使用模式](#5-常见使用模式)
6. [错误处理](#6-错误处理)
7. [进阶话题](#7-进阶话题)

---

## 1. SDK 架构概览

turntf-py SDK 提供两套客户端：

```
turntf-py SDK
├── AsyncHTTPClient     HTTP JSON 管理/查询客户端
│   ├── login()         → 获取 Bearer token
│   ├── CRUD 操作       → user / metadata / message / packet
│   └── 查询操作         → cluster / events / ops / metrics
│
└── AsyncClient         WebSocket + Protobuf 实时长连接客户端
    ├── connect()       → 建立 WS 连接并登录
    ├── send_message()  → 发送持久化消息
    ├── send_packet()   → 发送瞬时包
    ├── 推送回调         → on_message / on_packet / on_login / on_disconnect
    ├── 查询 RPC        → 与 AsyncHTTPClient 覆盖范围大致相同
    └── 共享语义         → session_ref / resolve_user_sessions / seen_messages
```

**关键设计原则**：两套客户端是独立的。`AsyncClient` 内部包含一个 `AsyncHTTPClient`（通过 `client.http` 访问），但 HTTP 登录获取的 token 不会自动注入 WebSocket RPC。WebSocket 的身份始终由 `Config.credentials` 决定。

---

## 2. 场景选择指南

### 优先使用 `AsyncHTTPClient`

适合以下场景：

- **一次性管理操作**：创建用户、创建 channel、管理订阅和黑名单
- **后台脚本**：定时任务、批量操作、数据迁移
- **管理面板后端**：用户列表、历史消息查询、集群监控
- **无状态查询**：只需查询不需要维持长连接
- **调试和测试**：快速验证服务端 API

### 优先使用 `AsyncClient`

适合以下场景：

- **常驻连接**：你的程序需要长时间在线、持续接收消息
- **实时推送**：需要接收 `MessagePushed` 或 `PacketPushed`
- **定向投递**：需要 `target_session` 把瞬时包投递到指定在线 session
- **自动重连**：连接断开后需要自动恢复
- **session 感知**：需要查询用户在线状态 `resolve_user_sessions`
- **消息去重**：需要 `seen_messages` 机制在重连后避免重复处理历史消息

### 混合使用

```python
async def main() -> None:
    # 使用 AsyncClient 建立长连接
    async with AsyncClient(config) as client:
        await client.connect()

        # 使用 HTTP 客户端做管理操作
        token = await client.http.login(4096, 1, "root")
        users = await client.http.list_node_logged_in_users(token, 4096)

        # 使用 WS 做实时操作
        msg = await client.send_message(user_ref, b"hello")
        resolved = await client.resolve_user_sessions(user_ref)
```

---

## 3. 认证方式

### HTTP 客户端认证

1. 调用 `client.login(node_id, user_id, password)` 获取 Bearer token
2. 将 token 作为第一个参数传入后续所有管理/查询方法
3. 密码明文会被 SDK 自动做 bcrypt 哈希后再发送

```python
token = await client.login(4096, 1, "root")
user = await client.get_user(token, UserRef(node_id=4096, user_id=1025))
```

### WebSocket 客户端认证

1. 在 `Config.credentials` 中配置 `(node_id, user_id, password)`
2. 连接时 SDK 自动在 WebSocket 首帧发送 `LoginRequest`
3. 密码通过 `plain_password()` 或 `hashed_password()` 构造

```python
config = Config(
    base_url="http://127.0.0.1:8080",
    credentials=Credentials(
        node_id=4096,
        user_id=1025,
        password=plain_password("my-password"),
    ),
)
```

**重要**：`AsyncClient.login()` 只是调用 HTTP 接口获取 Bearer token，不会修改 WebSocket 连接使用的身份凭据。

### 密码处理

```python
from turntf import plain_password, hashed_password, hash_password

# 推荐：传入明文，SDK 自动做 bcrypt
pw = plain_password("my-password")

# 已有 bcrypt 哈希值
pw = hashed_password("$2b$12$...")

# 仅哈希（不构造 PasswordInput）
hashed = hash_password("my-password")
```

---

## 4. 数据模型速查

### 基础标识类型

| 类型 | 字段 | 用途 |
| --- | --- | --- |
| `UserRef` | `node_id` + `user_id` | 标识一个用户或 channel |
| `SessionRef` | `serving_node_id` + `session_id` | 标识一个在线连接 session |
| `MessageCursor` | `node_id` + `seq` | 消息游标，用于 seen_messages 去重 |
| `Credentials` | `node_id` + `user_id` + `password` | WebSocket 登录凭据 |

### 核心数据模型

| 类型 | 主要字段 | 说明 |
| --- | --- | --- |
| `User` | `node_id`, `user_id`, `username`, `role`, `profile_json`, ... | 用户或 channel |
| `Message` | `recipient`, `node_id`, `seq`, `sender`, `body`, `created_at_hlc` | 持久化消息，包含游标 |
| `Packet` | `packet_id`, `recipient`, `sender`, `body`, `delivery_mode`, `target_session` | 瞬时包 |
| `RelayAccepted` | `packet_id`, `recipient`, `delivery_mode`, `target_session` | 瞬时包投递确认 |
| `Subscription` | `subscriber`, `channel`, `subscribed_at`, `deleted_at` | 频道订阅关系 |
| `BlacklistEntry` | `owner`, `blocked`, `blocked_at`, `deleted_at` | 用户黑名单条目 |
| `Attachment` | `owner`, `subject`, `attachment_type`, `config_json` | 用户间关联关系 |
| `UserMetadata` | `owner`, `key`, `value`, `expires_at` | 用户元数据 KV |
| `ResolvedUserSessions` | `user`, `presence[]`, `sessions[]` | 用户在线 session 解析结果 |
| `LoginInfo` | `user`, `protocol_version`, `session_ref` | WebSocket 登录成功信息 |

### 枚举类型

| 枚举 | 值 | 用途 |
| --- | --- | --- |
| `DeliveryMode` | `BEST_EFFORT`, `ROUTE_RETRY` | 瞬时包投递模式 |
| `AttachmentType` | `CHANNEL_SUBSCRIPTION`, `USER_BLACKLIST`, `CHANNEL_MANAGER`, `CHANNEL_WRITER` | 关联类型 |

---

## 5. 常见使用模式

### 模式一：只做一次性管理操作

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
                password=plain_password("alice-password"),
                profile_json=b'{"tier":"gold"}',
                role="user",
            ),
        )
        print("created user:", user.user_id)

        fetched = await client.get_user(token, UserRef(node_id=4096, user_id=user.user_id))
        print("fetched:", fetched.username)


asyncio.run(main())
```

### 模式二：常驻连接接收消息

```python
import asyncio
from turntf import AsyncClient, Config, Credentials, MemoryCursorStore, NopHandler, UserRef, plain_password


class MyHandler(NopHandler):
    async def on_login(self, info) -> None:
        print("连接成功，session:", info.session_ref)

    async def on_message(self, message) -> None:
        print("收到消息:", message.body.decode())

    async def on_disconnect(self, error) -> None:
        print("连接断开:", error)


async def main() -> None:
    async with AsyncClient(
        Config(
            base_url="http://127.0.0.1:8080",
            credentials=Credentials(
                node_id=4096,
                user_id=1025,
                password=plain_password("alice-password"),
            ),
            cursor_store=MemoryCursorStore(),
            handler=MyHandler(),
        )
    ) as client:
        await client.connect()
        await asyncio.Future()  # 保持运行


asyncio.run(main())
```

### 模式三：定向瞬时投递

```python
# 1. 解析用户在线 session
resolved = await client.resolve_user_sessions(UserRef(node_id=4096, user_id=1025))

# 2. 选择特定 session
if resolved.sessions:
    session = resolved.sessions[0]
    if session.transient_capable:
        # 3. 定向投递
        result = await client.send_packet(
            UserRef(node_id=4096, user_id=1025),
            b"notification",
            DeliveryMode.ROUTE_RETRY,
            target_session=session.session,
        )
        print("投递成功:", result.packet_id)
```

### 模式四：自定义游标持久化

```python
from turntf.store import CursorStore
from turntf.types import Message, MessageCursor


class RedisCursorStore(CursorStore):
    """生产环境：使用 Redis 持久化消息游标"""

    def __init__(self, redis_client):
        self._redis = redis_client

    async def load_seen_messages(self) -> list[MessageCursor]:
        # 从 Redis 读取已处理的游标
        ...

    async def save_message(self, message: Message) -> None:
        # 保存消息体
        ...

    async def save_cursor(self, cursor: MessageCursor) -> None:
        # 保存游标
        ...
```

---

## 6. 错误处理

### 异常层次

```
TurntfError
├── ServerError         服务端显式返回的业务错误
│   └── unauthorized    登录被拒（SDK 会停止重连）
├── ProtocolError       返回值结构异常（不可解析的 JSON、缺失字段）
├── ConnectionError     网络传输层错误（建连失败、读写异常）
│   └── 内含 .cause 保留底层异常
├── NotConnectedError   未连接就发起 WS RPC
├── DisconnectedError   连接中断，pending RPC 失败
└── ClosedError         客户端已关闭
```

### 典型处理模式

```python
from turntf.errors import ServerError, ConnectionError, ClosedError

try:
    msg = await client.send_message(target, body)
except ServerError as e:
    print(f"服务端错误 [{e.code}]: {e.server_message}")
except ConnectionError as e:
    print(f"网络错误 ({e.op}): {e.cause}")
except ClosedError:
    print("客户端已关闭")
except asyncio.TimeoutError:
    print("RPC 超时")
```

**重要注意事项**：

- 断线后 pending RPC 不会自动重放，需要业务自行重试
- `Handler` 回调内部的异常会被 SDK 吞掉，不会中断连接
- 登录阶段返回 `unauthorized` 后 SDK 会永久停止自动重连
- 重连只会恢复连接，不会恢复未完成的业务操作

---

## 7. 进阶话题

### 连接生命周期理解

`AsyncClient` 的连接生命周期与普通 HTTP 客户端有本质区别：

1. **构造**：零网络 I/O，仅做参数校验
2. **connect()**：启动后台连接循环，等待首次登录成功
3. **正常运行**：SDK 内部维护 `_read_loop` + `_ping_loop`
4. **断线**：后台自动指数退避重连，pending RPC 以 `DisconnectedError` 失败
5. **close()**：终止后台任务，关闭 WebSocket

`connect()` 的语义是"首次连接屏障"——它只等待第一次连接建立成功，不等待后续每次重连。

### 消息顺序保证

`MessagePushed` 的处理顺序固定为：

```
save_message → save_cursor → ack → handler.on_message
```

这个顺序不可倒置。先落库再 ack 保证即使落库后断线，下次重连时 `seen_messages` 已包含已处理游标，不会重复消费。

### session_ref 的生存期

`session_ref` 在每次登录成功后由服务端分配。每次重连后 `session_ref` 都可能变化。如果需要缓存当前连接的 session 身份，应在 `handler.on_login()` 中更新，而非只读一次。

---

## 参考文档

- [AsyncClient 运行时语义](async-client.md) — AsyncClient 的详细运行时行为
- [HTTP 客户端使用指南](http-client.md) — AsyncHTTPClient 的详细使用说明
- [开发环境搭建](development.md) — 本地开发和测试环境配置
