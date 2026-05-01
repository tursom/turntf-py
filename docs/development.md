# 开发环境搭建

本文档说明如何搭建 turntf-py SDK 的本地开发环境。

## 目录

1. [环境要求](#1-环境要求)
2. [初始设置](#2-初始设置)
3. [开发工作流程](#3-开发工作流程)
4. [测试](#4-测试)
5. [Proto 生成](#5-proto-生成)
6. [常见问题](#6-常见问题)

---

## 1. 环境要求

### 必需

- **Python** `>= 3.11`
- **pip**（Python 包管理器）
- **Git**（clone 仓库）

### 可选

- **protoc**：如果需要重新生成 protobuf 代码（见 [Proto 生成](#5-proto-生成)）
- **Docker**：如果需要本地运行 turntf 服务端做集成测试
- **make**：如果使用 Makefile 辅助命令（当前项目未使用）

---

## 2. 初始设置

### 2.1 克隆仓库

```bash
git clone <repository-url>
cd <repository>/sdk/turntf-py
```

如果从 monorepo 根目录进入：

```bash
cd /root/dev/sys/turntf/sdk/turntf-py
```

### 2.2 创建虚拟环境

推荐使用虚拟环境隔离依赖：

```bash
# 使用 venv
python3.11 -m venv .venv
source .venv/bin/activate

# 或使用 virtualenv
virtualenv -p python3.11 .venv
source .venv/bin/activate

# 或使用 conda
conda create -n turntf-py python=3.11
conda activate turntf-py
```

### 2.3 安装 SDK 和开发依赖

```bash
# 开发安装（可编辑模式 + dev 依赖）
pip install -e .[dev]

# 如果上面命令不生效，尝试：
pip install -e ".[dev]"
```

这会安装：
- 运行时依赖：`bcrypt`、`httpx`、`protobuf`、`websockets>=16`
- 开发依赖：`pytest>=8`

### 2.4 验证安装

```bash
python -c "import turntf; print(turntf.__version__)"  # 应看到 0.1.0
pytest --version  # 应看到 pytest 版本信息
```

---

## 3. 开发工作流程

### 3.1 日常开发循环

```bash
# 1. 确保虚拟环境已激活
source .venv/bin/activate

# 2. 编辑代码（turntf/ 下的源文件）

# 3. 运行测试
pytest

# 4. 重复 2-3 直到测试通过
```

### 3.2 代码结构导航

```
turntf/              # SDK 源代码
├── __init__.py      # 公开 API 导出
├── client.py        # AsyncClient 主类
├── http.py          # AsyncHTTPClient 主类
├── mapping.py       # 类型转换
├── types.py         # 数据模型
├── errors.py        # 异常定义
├── password.py      # 密码处理
├── store.py         # 游标存储接口
└── validation.py    # 参数校验

tests/               # 测试代码
├── test_client.py   # AsyncClient 测试
└── test_http.py     # AsyncHTTPClient 测试

proto/               # 协议定义
└── client.proto

scripts/             # 工具脚本
└── gen_proto.sh
```

### 3.3 测试模拟框架

测试不依赖真实 turntf 服务端。`test_client.py` 使用 `FakeConnection` 模拟 WebSocket 连接：

```python
class FakeConnection:
    """模拟 WebSocket 连接的测试辅助类。提供 client_send / client_recv /
    server_send / server_recv 四个方向的队列，测试通过操作队列来模拟
    客户端与假服务端之间的消息交换。"""
```

查看 `FakeConnection` 的完整实现见 `tests/test_client.py`。它支持：
- 模拟登录响应
- 模拟消息推送
- 模拟瞬时包推送
- 模拟连接断开
- 验证发送的 protobuf 消息内容

### 3.4 代码风格建议

当前项目未配置自动代码格式化工具。建议遵循以下约定：

- **行宽**：不超过 120 字符
- **引号**：双引号（`"`）用于字符串，单引号（`'`）仅在字符串包含双引号时使用
- **导入顺序**：标准库 → 第三方库 → 本地模块，每组用空行分隔（参考 `client.py` 的导入风格）
- **类型注解**：所有公开方法必须有类型注解
- **文档字符串**：公开类和方法建议有 docstring（当前部分代码缺少，欢迎补充）

建议后续引入：

```bash
# 格式化
pip install ruff
ruff format turntf/ tests/

# Lint
ruff check turntf/ tests/

# 类型检查
pip install mypy
mypy turntf/
```

---

## 4. 测试

### 4.1 运行测试

```bash
# 运行全部测试
pytest

# 详细输出
pytest -v

# 运行单个测试文件
pytest tests/test_http.py
pytest tests/test_client.py

# 运行匹配名称的测试
pytest -k "test_login"
pytest -k "test_send_message"

# 显示 print 输出
pytest -s

# 停在第一个失败
pytest -x
```

### 4.2 测试覆盖

当前的测试覆盖范围：

**test_http.py**：
- 密码哈希（bcrypt 运算）
- Bearer token 注入（HTTP Authorization 头）
- bytes 的 base64 编解码
- profile_json / config_json 的 JSON 编解码
- HTTP 消息和瞬时包请求形状

**test_client.py**：
- 登录成功和 session_ref 返回
- Ping 请求/响应匹配
- MessagePushed 的 save_message → save_cursor → ack 顺序
- send_message() 返回持久化消息
- resolve_user_sessions() 返回 presence 和 sessions
- send_packet() 的 target_session 定向投递
- unauthorized 登录失败停止自动重连
- 重连时重新上报 seen_messages
- transient_only 和 /ws/realtime 路径

### 4.3 编写新测试

测试使用 pytest 的 `def test_*` 函数风格，通过 async 函数 + 事件循环驱动：

```python
def test_my_feature() -> None:
    async def main() -> None:
        # 测试逻辑
        result = await something()
        assert result == expected

    asyncio.run(main())
```

对于 `AsyncClient` 测试，使用 `FakeWebSocket` mock 类模拟服务端行为：

```python
import pytest
from turntf import AsyncClient, Config, Credentials, NopHandler, UserRef, plain_password
from turntf._generated import client_pb2 as pb


def test_custom_scenario() -> None:
    async def main() -> None:
        fake = FakeConnection("/ws/client")
        client = _make_client(fake)
        await client.connect()

        # client_send 是客户端发送给服务端的消息
        env = await fake.client_send()
        assert env.HasField("login")

        # server_send 是服务端发送给客户端的消息
        await fake.server_send(pb.ServerEnvelope(
            login_response=pb.LoginResponse(...)
        ))

        # 验证 client_recv 是客户端收到的消息  
        # ... 具体模式参考 test_client.py

    asyncio.run(main())
```

---

## 5. Proto 生成

### 5.1 安装 protoc

```bash
# Ubuntu/Debian
apt install protobuf-compiler

# macOS
brew install protobuf

# 验证
protoc --version
```

### 5.2 重新生成

```bash
cd turntf-py/
./scripts/gen_proto.sh
```

生成结果写入 `turntf/_generated/client_pb2.py`。

### 5.3 什么时候需要重新生成

- 修改了 `proto/client.proto` 后
- 从上游同步了新的 proto 定义
- 如果 proto 变更影响了共享协议语义，需要同步检查：
  - 服务端 `turntf/` 的 Go proto 是否需要同步更新
  - Go SDK（`sdk/turntf-go`）是否需要同步
  - 其他 SDK 是否需要同步

### 5.4 重新生成后的检查

```bash
# 1. 运行测试
pytest

# 2. 确认无 protobuf 解析错误
python -c "from turntf._generated import client_pb2"

# 3. 检查生成文件是否在 git 中（应该被跟踪）
git status
```

---

## 6. 常见问题

### 6.1 安装问题

**Q**: `pip install -e .[dev]` 报错 `zsh: no matches found`

**A**: 在 zsh 中，方括号有特殊含义。需要用引号包裹：

```bash
pip install -e ".[dev]"
```

**Q**: `bcrypt` 安装失败

**A**: `bcrypt` 需要 C 编译器。安装系统构建工具：

```bash
# Ubuntu/Debian
apt install build-essential python3-dev

# macOS
xcode-select --install
```

### 6.2 运行时问题

**Q**: 运行测试时提示 `ModuleNotFoundError: No module named 'turntf'`

**A**: 没有执行开发安装。执行：

```bash
pip install -e ".[dev]"
```

**Q**: WebSocket 连接失败 `ConnectionError: dial`

**A**: 确认 turntf 服务端已经在 `base_url` 上运行。检查：

- 服务端地址和端口是否正确
- 服务端是否启用了 WebSocket 支持
- 网络是否可达

### 6.3 开发环境问题

**Q**: 如何调试测试？

**A**: 使用 `pytest -s` 允许 print 输出，或在测试中设置断点：

```python
def test_my_feature() -> None:
    async def main() -> None:
        breakpoint()  # 或 import pdb; pdb.set_trace()
        ...

    asyncio.run(main())
```

**Q**: 如何查询当前 Python 版本？

```bash
python --version
# 需要 >= 3.11
```

**Q**: 如何查看已安装的依赖版本？

```bash
pip list | grep -E "bcrypt|httpx|protobuf|websockets|pytest"
```

---

## 参考

- [SDK 使用指南](sdk-guide.md) — SDK 整体概览和场景选择
- [AsyncClient 运行时语义](async-client.md) — WebSocket 客户端详细行为
- [HTTP 客户端使用指南](http-client.md) — HTTP 客户端详细使用说明
