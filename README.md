# turntf-py

`turntf-py` 是 turntf 的异步 Python SDK，封装两类客户端能力：

- HTTP JSON 管理与查询客户端
- WebSocket + Protobuf 长连接客户端

第一版仅支持 WebSocket，不支持 ZeroMQ。`sync_mode` 虽然在协议中存在，但当前公开 API 不暴露该参数，行为与现有 `turntf-go` 保持一致。

## 安装

```bash
pip install turntf
```

## 功能

- HTTP 登录
- WebSocket 首帧登录
- 自动重连与重登录
- `seen_messages` 重放去重
- `MessagePushed` 自动执行 `保存消息 -> 保存游标 -> ack`
- `SendMessage`
- `SendPacket`
- `Ping`
- WS 用户、订阅、黑名单、消息、事件、集群和运维 RPC

## 快速开始

### `AsyncHTTPClient`

```python
import asyncio

from turntf import AsyncHTTPClient, CreateUserRequest, plain_password


async def main() -> None:
    client = AsyncHTTPClient("http://127.0.0.1:8080")
    token = await client.login(4096, 1, "root")

    user = await client.create_user(
        token,
        CreateUserRequest(
            username="alice",
            password=plain_password("alice-password"),
            role="user",
        ),
    )
    print(user)
    await client.close()


asyncio.run(main())
```

### `AsyncClient`

```python
import asyncio

from turntf import (
    AsyncClient,
    Config,
    Credentials,
    MemoryCursorStore,
    UserRef,
    plain_password,
)


async def main() -> None:
    client = AsyncClient(
        Config(
            base_url="http://127.0.0.1:8080",
            credentials=Credentials(
                node_id=4096,
                user_id=1025,
                password=plain_password("alice-password"),
            ),
            cursor_store=MemoryCursorStore(),
        )
    )

    token = await client.login(4096, 1, "root")
    await client.connect()
    await client.send_message(UserRef(node_id=4096, user_id=1025), b"hello")
    await client.close()


asyncio.run(main())
```
