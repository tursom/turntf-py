# AsyncHTTPClient 使用指南

本文档详细介绍 `AsyncHTTPClient` 的使用方法、API 规范和常见注意事项。

## 目录

1. [概述](#1-概述)
2. [快速开始](#2-快速开始)
3. [API 参考](#3-api-参考)
4. [编解码规则](#4-编解码规则)
5. [认证与授权](#5-认证与授权)
6. [错误处理](#6-错误处理)
7. [最佳实践](#7-最佳实践)

---

## 1. 概述

`AsyncHTTPClient` 封装了 turntf 服务端的 HTTP JSON API，提供 RESTful 风格的管理和查询接口。

### 特点

- **基于 httpx**：使用 `httpx.AsyncClient` 做异步 HTTP 请求
- **Bearer token 认证**：所有操作（除 login 外）需要先获取 token
- **自动编解码**：自动处理 bytes 的 base64 编码、profile_json 的 JSON 编解码
- **不维护状态**：每次请求都独立携带 token，不保存连接状态
- **轻量**：适合一次性操作和后台管理脚本

### 与 AsyncClient 的关系

- `AsyncClient` 内部包含一个 `AsyncHTTPClient` 实例（`client.http`）
- HTTP 和 WS 使用独立的认证机制，token 不会自动注入 WS RPC
- HTTP 不提供自动重连、session_ref、resolve_user_sessions 等 WS 特有功能

---

## 2. 快速开始

### 独立使用

```python
import asyncio
from turntf import AsyncHTTPClient, CreateUserRequest, UserRef, plain_password


async def main() -> None:
    async with AsyncHTTPClient("http://127.0.0.1:8080") as client:
        # 登录获取 token
        token = await client.login(4096, 1, "root")
        print("token:", token)

        # 创建用户
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
        print("created:", user.user_id)

        # 查询用户
        fetched = await client.get_user(token, UserRef(node_id=4096, user_id=user.user_id))
        print("username:", fetched.username)

        # 更新用户
        updated = await client.update_user(
            token,
            UserRef(node_id=4096, user_id=user.user_id),
            UpdateUserRequest(profile_json=b'{"tier":"platinum"}'),
        )
        print("updated:", updated.profile_json)


asyncio.run(main())
```

### 通过 AsyncClient 访问

```python
async with AsyncClient(config) as ws_client:
    await ws_client.connect()
    token = await ws_client.http.login(4096, 1, "root")
    users = await ws_client.http.list_node_logged_in_users(token, 4096)
```

### 注入外部 httpx 客户端

```python
import httpx

custom = httpx.AsyncClient(timeout=30.0, proxy="http://proxy:8080")
client = AsyncHTTPClient("http://127.0.0.1:8080", client=custom)
# 使用后需自行关闭 custom
await client.close()   # 不会关闭 custom，因为不是内部创建
await custom.aclose()
```

---

## 3. API 参考

### 构造

```python
AsyncHTTPClient(
    base_url: str,
    *,
    client: httpx.AsyncClient | None = None,
    timeout: float | None = 10.0,
)
```

- `base_url`：服务端地址，支持 `http://` 和 `https://`
- `client`：可注入复用 `httpx.AsyncClient`；注入后由调用方负责关闭
- `timeout`：默认请求超时；传 `None` 表示无超时（仅在未注入 `client` 时生效）

### 认证

#### `login(node_id=None, user_id=None, password=None, *, login_name=None) -> str`

- 使用明文密码登录，SDK 自动做 bcrypt 哈希
- 支持 `node_id + user_id` 或 `login_name` 两种 selector，且必须二选一
- 返回 Bearer token

#### `login_with_password(node_id=None, user_id=None, password: PasswordInput | None = None, *, login_name=None) -> str`

- 使用 `PasswordInput` 对象登录
- 支持 `node_id + user_id` 或 `login_name` 两种 selector，且必须二选一
- 适用于已有哈希值的情况

### 用户管理

#### `create_user(token, request: CreateUserRequest) -> User`

- `request.role` 必填，不能为空
- `request.password` 可选
- `request.login_name` 可选；创建可登录用户时可一并绑定登录名
- `request.profile_json` 可选，必须是有效 JSON bytes

#### `create_channel(token, request: CreateUserRequest) -> User`

- 与 `create_user` 类似，`role` 默认为 `"channel"`

#### `get_user(token, target: UserRef) -> User`

- 查询用户信息

#### `update_user(token, target: UserRef, request: UpdateUserRequest) -> User`

- 只更新传入的字段（partial update）
- `request.login_name=None` 表示不改，`request.login_name=""` 表示解绑
- `request.password` 需要是 `PasswordInput` 类型

#### `delete_user(token, target: UserRef) -> DeleteUserResult`

- 删除用户，返回操作状态

### 订阅与黑名单

#### `subscribe_channel(token, subscriber: UserRef, channel: UserRef) -> Subscription`

- 订阅频道

#### `unsubscribe_channel(token, subscriber: UserRef, channel: UserRef) -> Subscription`

- 取消订阅

#### `list_subscriptions(token, subscriber: UserRef) -> list[Subscription]`

- 列出所有订阅

#### `block_user(token, owner: UserRef, blocked: UserRef) -> BlacklistEntry`

- 屏蔽用户

#### `unblock_user(token, owner: UserRef, blocked: UserRef) -> BlacklistEntry`

- 取消屏蔽

#### `list_blocked_users(token, owner: UserRef) -> list[BlacklistEntry]`

- 列出黑名单

### 消息与瞬时包

#### `post_message(token, target: UserRef, body: bytes) -> Message`

- 发送持久化消息
- `body` 自动做 base64 编码

#### `post_packet(token, target_node_id, relay_target: UserRef, body: bytes, mode: DeliveryMode) -> RelayAccepted`

- 发送 HTTP 瞬时包
- `target_node_id` 必须与 `relay_target.node_id` 一致
- 返回 202 状态码表示接受
- **不支持** `target_session` 定向投递

#### `list_messages(token, target: UserRef, limit=0) -> list[Message]`

- 查询历史消息
- `limit=0` 表示使用服务端默认值

### 用户元数据

#### `get_user_metadata(token, owner: UserRef, key: str) -> UserMetadata`

- 查询指定 key 的元数据

#### `upsert_user_metadata(token, owner: UserRef, key: str, request: UpsertUserMetadataRequest) -> UserMetadata`

- 创建或更新元数据
- `request.value` 自动做 base64 编码

#### `delete_user_metadata(token, owner: UserRef, key: str) -> UserMetadata`

- 删除元数据

#### `scan_user_metadata(token, owner: UserRef, request) -> UserMetadataScanResult`

- 按前缀扫描元数据
- 支持 `prefix`、`after`、`limit` 参数

### 集群与运维

#### `list_cluster_nodes(token) -> list[ClusterNode]`

- 查询集群节点列表

#### `list_node_logged_in_users(token, node_id) -> list[LoggedInUser]`

- 查询指定节点上的在线用户

#### `list_events(token, after=0, limit=0) -> list[Event]`

- 查询事件日志

#### `operations_status(token) -> OperationsStatus`

- 查询运维状态信息
- 包含消息窗口、事件序列、写门控、冲突统计、peer 状态等

#### `metrics(token) -> str`

- 查询 Prometheus 格式的指标文本

---

## 4. 编解码规则

`AsyncHTTPClient` 自动处理的编解码：

| 字段类型 | 传输格式 | 编解码方式 |
| --- | --- | --- |
| `body` (bytes) | base64 字符串 | 发送时 `base64.b64encode(body).decode()`；接收时 `base64.b64decode(value)` |
| `profile_json` (bytes) | JSON 对象 | 发送时 `json.loads()` 反序列化为 dict；接收时 `json.dumps()` 序列化为 bytes |
| `config_json` (bytes) | JSON 对象 | 同上 |
| `event_json` (bytes) | JSON 对象 | 同上 |
| `value` (UserMetadata) | base64 字符串 | 发送时 `base64.b64encode(value)`；接收时 `base64.b64decode(value)` |
| `token` | Bearer token | 发送时写入 `Authorization: Bearer <token>` 头 |

**注意事项**：
- 如果 `profile_json` 或 `config_json` 为空 bytes（`b""`），不会出现在请求体中
- 接收时如果对应字段不存在，解析为空 bytes（`b""`）

---

## 5. 认证与授权

### 获取 token

```python
token = await client.login(4096, 1, "root")
```

- `node_id`：服务端节点 ID（通常是正整数）
- `user_id`：登录用户 ID
- `password`：明文密码（SDK 自动做 bcrypt 哈希）

### 使用 token

```python
user = await client.get_user(token, UserRef(node_id=4096, user_id=1025))
```

- token 作为第一个参数传入
- SDK 自动在 HTTP 请求头中添加 `Authorization: Bearer <token>`
- token 有效期由服务端控制，过期后服务端返回 401 / 403

### token 失效处理

```python
try:
    user = await client.get_user(token, target)
except ProtocolError as e:
    # 检查 HTTP 状态码
    if "401" in str(e):
        token = await client.login(4096, 1, "root")  # 重新登录
        user = await client.get_user(token, target)   # 重试
```

---

## 6. 错误处理

### HTTP 状态码检查

`_do_json()` 和 `_do_text()` 方法接受 `statuses` 参数，只接受指定的 HTTP 状态码：

```python
# 只接受 200 和 201
response = await self._do_json("POST", "/users", token, body, {200, 201})
```

不在 `statuses` 集合中的状态码会抛出 `ProtocolError`。

### 异常类型

| 场景 | 异常类型 |
| --- | --- |
| 网络连接失败 | `ConnectionError`（内含 `.cause` 保留 `httpx.HTTPError`） |
| 非预期 HTTP 状态码 | `ProtocolError`（包含状态码和响应体文本） |
| 无效 JSON 响应 | `ProtocolError` |
| 缺失必填字段 | `ProtocolError` |
| 参数校验失败 | `ValueError` |

### 自定义错误处理

```python
from turntf.errors import ConnectionError, ProtocolError

try:
    user = await client.get_user(token, ref)
except ConnectionError as e:
    print(f"网络错误: {e.op}, 原因: {e.cause}")
    # 重试或降级
except ProtocolError as e:
    print(f"协议错误: {e.protocol_message}")
    # 检查服务端版本或 API 兼容性
except ValueError as e:
    print(f"参数错误: {e}")
```

---

## 7. 最佳实践

### 复用连接

`AsyncHTTPClient` 在内部维护 `httpx.AsyncClient`，推荐复用：

```python
# 作为上下文管理器使用（自动关闭）
async with AsyncHTTPClient("http://127.0.0.1:8080") as client:
    token = await client.login(...)
    # 多次调用
    user1 = await client.get_user(token, ref1)
    user2 = await client.get_user(token, ref2)
```

### token 缓存

```python
class AdminClient:
    def __init__(self, base_url: str, node_id: int, password: str):
        self._http = AsyncHTTPClient(base_url)
        self._node_id = node_id
        self._password = password
        self._token: str | None = None

    async def _ensure_token(self) -> str:
        if self._token is None:
            self._token = await self._http.login(self._node_id, 1, self._password)
        return self._token

    async def get_user(self, ref: UserRef) -> User:
        token = await self._ensure_token()
        return await self._http.get_user(token, ref)
```

### 使用自定义超时

```python
# 全局超时
client = AsyncHTTPClient("http://127.0.0.1:8080", timeout=30.0)

# 注入自定义 httpx 客户端实现更精细控制
import httpx
transport = httpx.AsyncHTTPTransport(retries=3)
custom = httpx.AsyncClient(transport=transport, timeout=httpx.Timeout(30.0, connect=5.0))
client = AsyncHTTPClient("http://127.0.0.1:8080", client=custom)
```

### 连接池配置

HTTP 客户端的连接池行为由底层 `httpx.AsyncClient` 控制。默认情况下：
- 最大连接池大小：10（httpx 默认）
- 如果遇到高并发场景，建议注入自定义 `httpx.AsyncClient` 调整连接池参数

```python
limits = httpx.Limits(max_connections=100, max_keepalive_connections=20)
custom = httpx.AsyncClient(limits=limits)
```

---

## 参考

- [SDK 使用指南](sdk-guide.md) — SDK 整体概览和场景选择
- [AsyncClient 运行时语义](async-client.md) — WebSocket 客户端详细行为
- [开发环境搭建](development.md) — 本地开发配置
