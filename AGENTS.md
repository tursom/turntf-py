# turntf-py SDK 开发指南

## 项目概览

`turntf-py` 是 turntf 分布式通知服务的 Python 异步 SDK，面向 `asyncio` 场景。

- **Python 版本要求**：`>= 3.11`
- **运行时依赖**：`bcrypt`、`httpx`、`protobuf`、`websockets>=16`
- **异步模型**：基于 `asyncio` 的 async/await 协程，所有 I/O 操作均为异步
- **通信协议**：两套传输通道
  - HTTP JSON：管理/查询接口（`AsyncHTTPClient`）
  - WebSocket + Protobuf：实时长连接（`AsyncClient`）
- **版本**：当前 0.1.0（开发阶段）

本 SDK 位于 monorepo `sdk/turntf-py/` 目录下，作为根仓库的 submodule 挂载。根仓库的 AGENTS.md 见 `/root/dev/sys/turntf/AGENTS.md`。

---

## 构建、测试与代码检查

### 安装

```bash
# 开发安装（含 dev 依赖）
cd sdk/turntf-py
pip install -e .[dev]

# 仅运行时安装
pip install turntf
```

### 运行测试

```bash
# 运行全部测试
pytest

# 运行特定测试文件
pytest tests/test_http.py
pytest tests/test_client.py

# 显示详细输出
pytest -v

# 运行匹配名称的测试
pytest -k "test_login"
```

测试使用自定义 mock 层（`FakeConnection`）模拟 WebSocket 行为，不依赖真实 turntf 服务端。测试文件位于 `tests/` 目录：
- `tests/test_http.py`：HTTP 客户端测试（密码哈希、Bearer token、JSON/base64 编解码）
- `tests/test_client.py`：WebSocket 客户端测试（登录、消息推送、重连、pending RPC）

### 代码检查

当前项目没有配置 linter（如 ruff、flake8）。提交前建议手动检查：

```bash
# 使用 mypy 做类型检查（如已安装）
mypy turntf/

# 使用 ruff 做 lint（如已安装）
ruff check turntf/
```

建议后续引入 ruff 统一 lint 和格式化规则。

---

## Proto 生成

SDK 的协议定义位于 `proto/client.proto`，生成后的 Python 代码位于 `turntf/_generated/client_pb2.py`。

### 重新生成

```bash
# 在 turntf-py/ 目录下执行
./scripts/gen_proto.sh
```

### 前置要求

- `protoc` 编译器已安装并出现在 `PATH`
- protoc Python 插件已安装：`pip install grpcio-tools`（或直接使用 `protoc` 原生 python_out）

### 注意事项

- 生成结果已经提交到仓库，日常开发通常不需要重新生成
- 修改 `proto/client.proto` 后必须重新生成并同步 `client_pb2.py`
- 修改 proto 后建议同步检查 Go 服务端（`turntf/`）和其他 SDK 是否需要同步更新
- 生成脚本只输出 Python 代码，不生成 `__init__.py`（`_generated/__init__.py` 已手动创建为空文件）

---

## 包结构

```
turntf-py/
├── proto/
│   └── client.proto              # Protocol Buffers 协议定义
├── scripts/
│   └── gen_proto.sh              # Proto 生成脚本
├── tests/
│   ├── test_client.py            # AsyncClient 测试
│   └── test_http.py              # AsyncHTTPClient 测试
├── turntf/
│   ├── __init__.py               # 公开 API 导出
│   ├── _generated/
│   │   ├── __init__.py
│   │   └── client_pb2.py         # protoc 生成的 Python 代码
│   ├── client.py                 # AsyncClient：WebSocket 长连接客户端
│   ├── http.py                   # AsyncHTTPClient：HTTP JSON 客户端
│   ├── mapping.py                # Proto ↔ Python 类型转换 + HTTP JSON ↔ Python 类型转换
│   ├── types.py                  # 所有数据模型（dataclass + 枚举）
│   ├── errors.py                 # 自定义异常层次
│   ├── password.py               # 密码处理（bcrypt 哈希、PasswordInput）
│   ├── store.py                  # 游标持久化接口（CursorStore 协议）
│   └── validation.py             # 本地参数校验
├── pyproject.toml
├── README.md
└── AGENTS.md                     # 本文件
```

### 各模块职责

| 模块 | 职责 |
| --- | --- |
| `client.py` | `AsyncClient` 主类、`Config` 配置类、`Handler`/`NopHandler` 回调基类、WebSocket 连接循环、自动重连、pending RPC 管理、消息持久化与 ack 流程 |
| `http.py` | `AsyncHTTPClient` 主类、HTTP JSON 请求封装、Bearer token 注入、base64/JSON 编解码 |
| `mapping.py` | 双向转换：protobuf 类型 ↔ Python dataclass、HTTP JSON 字典 ↔ Python dataclass。包含 `_from_proto` 和 `_from_http` 两个命名族 |
| `types.py` | 所有数据模型（`User`、`Message`、`Packet`、`Credentials`、`SessionRef`、`UserRef` 等）和枚举（`DeliveryMode`、`AttachmentType`） |
| `errors.py` | 异常层次：`TurntfError` → `ClosedError`/`NotConnectedError`/`DisconnectedError`/`ServerError`/`ProtocolError`/`ConnectionError` |
| `password.py` | `PasswordInput` 值对象（`source` + `encoded`）、`plain_password()`、`hashed_password()`、`hash_password()` |
| `store.py` | `CursorStore` 协议接口（`load_seen_messages`/`save_message`/`save_cursor`）、`MemoryCursorStore` 内存实现 |
| `validation.py` | 参数校验函数：`validate_positive_int`、`validate_user_ref`、`validate_session_ref`、`validate_delivery_mode`、`validate_user_metadata_key` 等 |

---

## 关键 API 表面

### `AsyncClient`（WebSocket 长连接客户端）

`AsyncClient` 是 SDK 的核心类，封装了 WebSocket 长连接的全部生命周期。

**构造参数**：通过 `Config` 配置类传入

```python
config = Config(
    base_url="http://127.0.0.1:8080",
    credentials=Credentials(
        node_id=4096,
        user_id=1025,
        password=plain_password("password"),
    ),
    cursor_store=MemoryCursorStore(),
    handler=NopHandler(),
)
client = AsyncClient(config)
```

**核心方法**：

| 方法 | 说明 |
| --- | --- |
| `await client.connect()` | 建立 WebSocket 连接并登录。只等待首次连接结果，不等待后续重连 |
| `await client.close()` | 关闭连接，取消后台任务 |
| `await client.send_message(target, body)` | 发送持久化消息，返回 `Message` |
| `await client.send_packet(target, body, mode, *, target_session)` | 发送瞬时包，返回 `RelayAccepted` |
| `await client.ping()` | 发送应用层心跳 |
| `await client.resolve_user_sessions(user)` | 查询用户在线 session 列表 |
| `await client.login(node_id, user_id, password)` | HTTP 登录，返回 Bearer token |
| `await client.create_user(request)` | 创建用户 |
| `await client.create_channel(request)` | 创建 channel |
| `await client.get_user(target)` | 查询用户 |
| `await client.update_user(target, request)` | 更新用户 |
| `await client.delete_user(target)` | 删除用户 |
| `await client.subscribe_channel(subscriber, channel)` | 订阅 channel |
| `await client.unsubscribe_channel(subscriber, channel)` | 取消订阅 |
| `await client.list_subscriptions(subscriber)` | 列出订阅 |
| `await client.block_user(owner, blocked)` | 屏蔽用户 |
| `await client.unblock_user(owner, blocked)` | 取消屏蔽 |
| `await client.list_blocked_users(owner)` | 列出黑名单 |
| `await client.list_messages(target, limit)` | 查询历史消息 |
| `await client.list_events(after, limit)` | 查询事件日志 |
| `await client.list_cluster_nodes()` | 查询集群节点 |
| `await client.list_node_logged_in_users(node_id)` | 查询节点在线用户 |
| `await client.operations_status()` | 查询运维状态 |
| `await client.metrics()` | 查询指标文本 |
| `await client.get_user_metadata(owner, key)` | 查询用户元数据 |
| `await client.upsert_user_metadata(owner, key, request)` | 写入用户元数据 |
| `await client.delete_user_metadata(owner, key)` | 删除用户元数据 |
| `await client.scan_user_metadata(owner, request)` | 扫描用户元数据 |

**属性**：
- `client.http` — 内部 `AsyncHTTPClient` 实例
- `client.session_ref` — 当前连接的 `SessionRef`（登录成功后可用）
- `client.login_info` — 当前 `LoginInfo`（登录成功后可用）

**生命周期**：
1. `AsyncClient(Config(...))` — 只做参数校验，无网络 I/O
2. `await client.connect()` — 建立连接并等待首次登录成功
3. 正常运行 — 调用 WS RPC 或等待推送
4. `await client.close()` — 关闭连接

`async with AsyncClient(config) as client:` 只负责退出时调用 `close()`，不会自动 `connect()`。

### `AsyncHTTPClient`（HTTP JSON 客户端）

`AsyncHTTPClient` 提供 RESTful HTTP JSON API 的封装，适合脚本、后台管理、无状态查询。

**构造方式**：

```python
# 独立使用
async with AsyncHTTPClient("http://127.0.0.1:8080") as client:
    token = await client.login(4096, 1, "root")
    user = await client.get_user(token, UserRef(node_id=4096, user_id=1025))

# 通过 AsyncClient 内部访问
client = AsyncClient(config)
await client.connect()
token = await client.http.login(4096, 1, "root")
```

**核心方法**（全部需要 Bearer token 参数）：

| 方法 | 说明 |
| --- | --- |
| `await client.login(node_id, user_id, password)` | 登录获取 token |
| `await client.create_user(token, request)` | 创建用户 |
| `await client.get_user(token, target)` | 查询用户 |
| `await client.update_user(token, target, request)` | 更新用户 |
| `await client.delete_user(token, target)` | 删除用户 |
| `await client.post_message(token, target, body)` | 发消息 |
| `await client.post_packet(token, target_node_id, relay_target, body, mode)` | 发瞬时包 |
| `await client.list_messages(token, target, limit)` | 查询历史消息 |
| `await client.list_cluster_nodes(token)` | 查询集群节点 |
| `await client.list_node_logged_in_users(token, node_id)` | 查询节点在线用户 |
| `await client.list_events(token, after, limit)` | 查询事件日志 |
| `await client.operations_status(token)` | 查询运维状态 |
| `await client.metrics(token)` | 查询指标文本 |
| `await client.subscribe_channel(token, subscriber, channel)` | 订阅 channel |
| `await client.unsubscribe_channel(token, subscriber, channel)` | 取消订阅 |
| `await client.list_subscriptions(token, subscriber)` | 列出订阅 |
| `await client.block_user(token, owner, blocked)` | 屏蔽用户 |
| `await client.unblock_user(token, owner, blocked)` | 取消屏蔽 |
| `await client.list_blocked_users(token, owner)` | 列出黑名单 |
| `await client.get_user_metadata(token, owner, key)` | 查询用户元数据 |
| `await client.upsert_user_metadata(token, owner, key, request)` | 写入用户元数据 |
| `await client.delete_user_metadata(token, owner, key)` | 删除用户元数据 |
| `await client.scan_user_metadata(token, owner, request)` | 扫描用户元数据 |

---

## 发布到 PyPI

### 构建

```bash
# 安装构建工具
pip install build twine

# 构建源码分发包和 wheel
python -m build
```

构建产物在 `dist/` 目录。

### 发布

```bash
# 上传到 PyPI
twine upload dist/*

# 先上传到 TestPyPI 做验证
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

### 版本管理

- 版本号在 `pyproject.toml` 的 `[project] version` 字段中定义
- 遵循 [SemVer](https://semver.org/) 语义化版本规范
- 发布前需更新版本号并创建对应 git tag

### 注意事项

- 首次发布前需在 PyPI 注册项目名 `turntf`
- 需要配置 PyPI API token 或用户名密码
- `pyproject.toml` 中的 `readme = "README.md"` 会在 PyPI 页面显示

---

## 代码规范

### 类型注解

- 所有公开 API 必须使用完整的类型注解
- 使用 `from __future__ import annotations` 启用延迟求值
- `dataclass` 使用 `slots=True` 减少内存开销
- `frozen=True` 用于不可变值对象（`Credentials`、`UserRef`、`SessionRef`、`MessageCursor` 等）
- 可变模型类使用 `slots=True` 但不使用 `frozen=True`

### 异步模式

- 所有 I/O 方法都是 `async` 的
- 同步方法只用于纯计算或参数校验（如 `validate_*`、`_next_request_id`）
- 内部锁使用 `asyncio.Lock()`
- 异步上下文管理器支持：`__aenter__` / `__aexit__`
- Future 使用 `asyncio.get_running_loop().create_future()`
- 超时使用 `asyncio.wait_for(..., timeout=...)`

### 错误处理规范

- 自定义异常继承自 `TurntfError(Exception)`
- 网络/协议错误使用 SDK 自定义异常，而非标准库异常
- `Handler` 回调内的异常被 SDK 吞掉，不向外传播
- 断线后 pending RPC 不会自动重放
- 连接状态异常层次：`ClosedError` > `DisconnectedError` > `NotConnectedError`

### 命名约定

- 类名：`PascalCase`
- 方法/函数：`snake_case`
- 私有方法：`_leading_underscore`
- 内部属性：`_leading_underscore`（如 `_ws`、`_pending`）
- 常量：`UPPER_SNAKE_CASE`
- 模块内部辅助函数：`_leading_underscore`
- 公开 API 通过 `__init__.py` 的 `__all__` 控制导出

### Protobuf 命名

- 生成的 `client_pb2.py` 中的消息类使用 `from ._generated import client_pb2 as pb` 别名导入
- 包内使用 `pb.SomeMessage` 形式访问
- 转换函数命名族：`*_from_proto`、`*_to_proto`、`*_from_http`
- 枚举/字段名保持与 proto 定义一致

---

## 提交规范

### Git 作者

所有 git 提交的作者必须是 `tursom <tursom@foxmail.com>`。

### 提交信息格式

```
<type>: <简短描述>

- <详细说明>
```

建议的类型前缀：
- `feat`：新功能
- `fix`：错误修复
- `docs`：文档变更
- `chore`：构建/工具/依赖
- `refactor`：重构
- `test`：测试
- `style`：代码格式

### 提交前检查清单

- [ ] 测试是否通过：`pytest`
- [ ] 是否有未使用的导入或变量
- [ ] 类型注解是否完整
- [ ] 公开 API 变更是否需要同步更新 README.md 或 docs/
- [ ] proto 修改后是否重新生成了 `client_pb2.py`
- [ ] 跨 SDK 改动是否需要同步通知 Go 或其他语言 SDK

---

## 附加说明

### 已知约束

- 当前只支持 WebSocket 传输，不支持 ZeroMQ
- `sync_mode` 协议字段当前不暴露给公开 API
- `MemoryCursorStore` 仅适合测试和单进程调试，生产环境需实现持久化 `CursorStore`
- HTTP 客户端的 `post_packet()` 不支持 `target_session` 定向投递

### 与 Go SDK 的差异

- Python SDK 第一版功能覆盖度与 Go SDK 接近但不完全同步
- 核心共享语义（消息持久化顺序、ack、重连、seen_messages、session_ref）保持一致
- 修改涉及共享协议语义时，需检查各 SDK 是否需要同步更新
