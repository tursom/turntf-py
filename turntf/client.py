from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse, urlunparse

import httpx
import websockets
from google.protobuf.message import DecodeError

from ._generated import client_pb2 as pb
from .errors import (
    ClosedError,
    ConnectionError,
    DisconnectedError,
    NotConnectedError,
    ProtocolError,
    ServerError,
)
from .http import AsyncHTTPClient
from .mapping import (
    attachment_from_proto,
    attachment_type_to_proto,
    attachments_from_proto,
    blacklist_entries_from_proto,
    blacklist_entry_from_proto,
    cluster_nodes_from_proto,
    cursor_to_proto,
    delivery_mode_to_proto,
    events_from_proto,
    logged_in_users_from_proto,
    message_from_proto,
    messages_from_proto,
    operations_status_from_proto,
    packet_from_proto,
    relay_accepted_from_proto,
    resolved_user_sessions_from_proto,
    session_ref_from_proto,
    session_ref_to_proto,
    subscription_from_proto,
    subscriptions_from_proto,
    user_from_proto,
    user_metadata_from_proto,
    user_metadata_scan_result_from_proto,
    user_ref_to_proto,
)
from .password import PasswordInput, plain_password
from .relay import Relay
from .store import CursorStore, MemoryCursorStore
from .types import (
    Attachment,
    AttachmentType,
    BlacklistEntry,
    ClusterNode,
    CreateUserRequest,
    Credentials,
    DeleteUserResult,
    DeliveryMode,
    Event,
    LoggedInUser,
    LoginInfo,
    Message,
    OperationsStatus,
    Packet,
    RelayAccepted,
    ResolvedUserSessions,
    ScanUserMetadataRequest,
    SessionRef,
    Subscription,
    UpdateUserRequest,
    UpsertUserMetadataRequest,
    User,
    UserMetadata,
    UserMetadataScanResult,
    UserRef,
)
from .validation import (
    validate_delivery_mode,
    validate_login_selector,
    validate_positive_int,
    validate_session_ref,
    validate_user_metadata_key,
    validate_user_metadata_scan_request,
    validate_user_ref,
)


class Handler:
    """事件处理器基类。

    定义 WebSocket 连接生命周期中的各类事件回调。
    继承此类并重写相应方法来处理特定事件。

    所有方法都是异步的，默认实现为空操作（no-op）。
    """

    async def on_login(self, info: LoginInfo) -> None:
        """登录成功后的回调。

        Args:
            info: 登录信息，包含用户信息、协议版本和会话引用。
        """
        return None

    async def on_message(self, message: Message) -> None:
        """收到持久化消息时的回调。

        Args:
            message: 接收到的 Message 对象。
        """
        return None

    async def on_packet(self, packet: Packet) -> None:
        """收到瞬时数据包时的回调。

        Args:
            packet: 接收到的 Packet 对象。
        """
        return None

    async def on_error(self, error: BaseException) -> None:
        """处理过程中发生错误时的回调。

        Args:
            error: 发生的异常对象。
        """
        return None

    async def on_disconnect(self, error: BaseException) -> None:
        """WebSocket 连接断开时的回调。

        Args:
            error: 导致断开连接的异常对象。
        """
        return None


class NopHandler(Handler):
    """空的处理器实现。

    继承自 Handler，但所有回调方法均为空操作。
    当未提供自定义 Handler 时作为默认值使用。
    """
    pass


@dataclass(slots=True)
class Config:
    """AsyncClient 的配置项。

    包含连接 turntf 服务器所需的全部配置参数。

    Attributes:
        base_url: 服务器基础 URL，如 ``http://localhost:8080``。
        credentials: 登录凭据。
        cursor_store: 游标存储实现，用于断线重连时恢复消息状态。
                      默认为 MemoryCursorStore（内存存储）。
        handler: 事件处理器，处理收到的消息和数据包。
                 默认为 NopHandler（空操作）。
        http_client: 可选的 httpx.AsyncClient 实例。不提供时自动创建。
        reconnect: 是否启用自动重连，默认 True。
        initial_reconnect_delay: 首次重连延迟（秒），默认 1.0。
        max_reconnect_delay: 最大重连延迟（秒），默认 30.0。
        ping_interval: WebSocket ping 发送间隔（秒），默认 30.0。
        request_timeout: RPC 请求超时时间（秒），默认 10.0。
        ack_messages: 是否自动确认消息，默认 True。
                      启用后客户端会自动向服务器发送消息确认。
        transient_only: 是否只接收瞬时消息，默认 False。
        realtime_stream: 是否使用实时流式 WebSocket 端点，默认 False。
    """
    base_url: str
    credentials: Credentials
    cursor_store: CursorStore | None = None
    handler: Handler | None = None
    http_client: httpx.AsyncClient | None = None
    reconnect: bool = True
    initial_reconnect_delay: float = 1.0
    max_reconnect_delay: float = 30.0
    ping_interval: float = 30.0
    request_timeout: float = 10.0
    ack_messages: bool = True
    transient_only: bool = False
    realtime_stream: bool = False


class AsyncClient:
    """turntf WebSocket 异步客户端。

    通过 WebSocket 协议与 turntf 服务器建立长连接，
    支持发送和接收持久化消息和瞬时数据包。

    使用方式：

    .. code-block:: python

        config = Config(
            base_url="http://localhost:8080",
            credentials=Credentials(
                node_id=1,
                user_id=100,
                password=plain_password("my_password"),
            ),
        )
        async with AsyncClient(config) as client:
            await client.connect()
            # 发送消息、接收推送等

    客户端支持自动重连（可通过 Config 配置），
    断线后会自动恢复消息推送状态。
    """

    def __init__(self, config: Config) -> None:
        """初始化 AsyncClient。

        验证配置参数的有效性，包括 base_url 和 credentials。

        Args:
            config: 客户端配置对象。

        Raises:
            ValueError: 如果 base_url 为空或凭据参数无效。
        """
        if config.base_url.strip() == "":
            raise ValueError("base_url is required")
        validate_login_selector(
            node_id=config.credentials.node_id if config.credentials.node_id != 0 else None,
            user_id=config.credentials.user_id if config.credentials.user_id != 0 else None,
            login_name=config.credentials.login_name,
            field="credentials",
        )
        config.credentials.password.validate()

        self._cfg = Config(
            base_url=config.base_url,
            credentials=config.credentials,
            cursor_store=config.cursor_store or MemoryCursorStore(),
            handler=config.handler or NopHandler(),
            http_client=config.http_client,
            reconnect=config.reconnect,
            initial_reconnect_delay=config.initial_reconnect_delay
            if config.initial_reconnect_delay > 0
            else 1.0,
            max_reconnect_delay=config.max_reconnect_delay if config.max_reconnect_delay > 0 else 30.0,
            ping_interval=config.ping_interval if config.ping_interval > 0 else 30.0,
            request_timeout=config.request_timeout if config.request_timeout > 0 else 10.0,
            ack_messages=config.ack_messages,
            transient_only=config.transient_only,
            realtime_stream=config.realtime_stream,
        )

        self._http = AsyncHTTPClient(self._cfg.base_url, client=self._cfg.http_client)
        self._write_lock = asyncio.Lock()
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._request_id = 0
        self._ws: Any | None = None
        self._run_task: asyncio.Task[None] | None = None
        self._first_connect: asyncio.Future[None] | None = None
        self._closed = False
        self._connected = False
        self._login_info: LoginInfo | None = None
        self._stop_reconnect = False
        self._relay: Relay | None = None

    @property
    def http(self) -> AsyncHTTPClient:
        """获取 HTTP API 客户端。

        用于执行需要认证令牌的 HTTP REST 操作。

        Returns:
            AsyncHTTPClient 实例。
        """
        return self._http

    @property
    def login_info(self) -> LoginInfo | None:
        """获取登录成功后的信息。

        Returns:
            如果已成功登录则返回 LoginInfo，否则返回 None。
        """
        return self._login_info

    @property
    def session_ref(self) -> SessionRef | None:
        """获取当前会话引用。

        Returns:
            如果已成功登录则返回当前会话的 SessionRef，否则返回 None。
        """
        if self._login_info is None:
            return None
        return self._login_info.session_ref

    def relay(self) -> Relay:
        """获取关联的 Relay 管理器（懒初始化）。

        Returns:
            Relay 管理器实例。
        """
        if self._relay is None:
            self._relay = Relay(self)
        return self._relay

    async def __aenter__(self) -> "AsyncClient":
        """异步上下文管理器入口。

        Returns:
            AsyncClient 实例自身。
        """
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """异步上下文管理器出口，自动关闭客户端。"""
        await self.close()

    async def login(
        self,
        node_id: int | None = None,
        user_id: int | None = None,
        password: str | None = None,
        *,
        login_name: str | None = None,
    ) -> str:
        """使用 HTTP API 和明文密码登录，获取认证令牌。

        这是一个便捷方法，内部调用 login_with_password。

        Args:
            node_id: 节点 ID（与 login_name 二选一）。
            user_id: 用户 ID（与 login_name 二选一）。
            password: 明文密码。
            login_name: 登录名（与 node_id/user_id 二选一）。

        Returns:
            认证令牌（JWT token）字符串。
        """
        return await self._http.login(
            node_id=node_id,
            user_id=user_id,
            password=password,
            login_name=login_name,
        )

    async def login_with_password(
        self,
        node_id: int | None = None,
        user_id: int | None = None,
        password: PasswordInput | None = None,
        *,
        login_name: str | None = None,
    ) -> str:
        """使用 HTTP API 和 PasswordInput 密码对象登录，获取认证令牌。

        Args:
            node_id: 节点 ID（与 login_name 二选一）。
            user_id: 用户 ID（与 login_name 二选一）。
            password: PasswordInput 密码输入对象。
            login_name: 登录名（与 node_id/user_id 二选一）。

        Returns:
            认证令牌（JWT token）字符串。
        """
        return await self._http.login_with_password(
            node_id=node_id,
            user_id=user_id,
            password=password,
            login_name=login_name,
        )

    async def connect(self) -> None:
        """建立 WebSocket 连接并登录。

        首次调用时启动后台运行循环，建立 WebSocket 连接
        并发送登录请求进行身份验证。

        支持自动重连：如果连接断开且配置允许，会以指数退避策略重试。

        Raises:
            ClosedError: 如果客户端已关闭。
        """
        if self._closed:
            raise ClosedError()
        if self._connected and self._ws is not None:
            return
        if self._run_task is None or self._run_task.done():
            loop = asyncio.get_running_loop()
            self._first_connect = loop.create_future()
            self._run_task = loop.create_task(self._run())
        assert self._first_connect is not None
        await self._first_connect

    async def close(self) -> None:
        """关闭客户端，断开 WebSocket 连接并释放资源。

        执行清理操作：
        1. 标记客户端为已关闭
        2. 关闭 WebSocket 连接
        3. 取消所有待处理的 RPC 请求
        4. 关闭 HTTP 客户端
        """
        if self._closed:
            await self._await_run_task()
            await self._http.close()
            return
        self._closed = True
        self._signal_first_connect(ClosedError())
        self._fail_all_pending(ClosedError())
        ws = self._ws
        self._ws = None
        self._connected = False
        self._login_info = None
        if ws is not None:
            await self._safe_close_ws(ws)
        if self._run_task is not None:
            self._run_task.cancel()
            await self._await_run_task()
        await self._http.close()

    async def ping(self) -> None:
        """发送 WebSocket ping 心跳请求。

        用于保持连接活跃或检测连接是否正常。
        内部通过 RPC 机制发送 ping 并等待 pong 响应。
        """
        await self._rpc(lambda request_id: pb.ClientEnvelope(ping=pb.Ping(request_id=request_id)))

    async def send_message(self, target: UserRef, body: bytes) -> Message:
        """发送持久化消息。

        消息会被服务器持久化存储，并通过 WebSocket 推送给目标用户。

        Args:
            target: 目标用户引用。
            body: 消息体字节数据。

        Returns:
            已发送的消息对象，包含服务器分配的元数据（node_id, seq 等）。

        Raises:
            ValueError: 如果 target 无效或 body 为空。
            NotConnectedError: 如果未连接到服务器。
        """
        validate_user_ref(target, "target")
        if len(body) == 0:
            raise ValueError("body is required")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                send_message=pb.SendMessageRequest(
                    request_id=request_id,
                    target=user_ref_to_proto(target),
                    body=body,
                    sync_mode=pb.CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED,
                )
            )
        )
        if not isinstance(result, Message):
            raise ProtocolError("missing message in send response")
        return result

    async def post_message(self, target: UserRef, body: bytes) -> Message:
        """发送持久化消息（``send_message`` 的别名）。

        Args:
            target: 目标用户引用。
            body: 消息体字节数据。

        Returns:
            已发送的消息对象。
        """
        return await self.send_message(target, body)

    async def send_packet(
        self,
        target: UserRef,
        body: bytes,
        delivery_mode: DeliveryMode,
        *,
        target_session: SessionRef | None = None,
    ) -> RelayAccepted:
        """发送瞬时数据包（非持久化消息）。

        数据包不会被持久化存储，适用于实时通信场景。
        如果目标用户离线，数据包可能会丢失。

        Args:
            target: 目标用户引用。
            body: 数据包体字节数据。
            delivery_mode: 投递模式（BEST_EFFORT 或 ROUTE_RETRY）。
            target_session: 可选的目与会话引用，指定后只投递到该会话。

        Returns:
            中继确认信息，表示服务器已接受并开始转发电。

        Raises:
            ValueError: 如果参数无效。
            NotConnectedError: 如果未连接到服务器。
        """
        validate_user_ref(target, "target")
        if len(body) == 0:
            raise ValueError("body is required")
        validate_delivery_mode(delivery_mode)

        def build(request_id: int) -> pb.ClientEnvelope:
            request = pb.SendMessageRequest(
                request_id=request_id,
                target=user_ref_to_proto(target),
                body=body,
                delivery_kind=pb.CLIENT_DELIVERY_KIND_TRANSIENT,
                delivery_mode=delivery_mode_to_proto(delivery_mode),
                sync_mode=pb.CLIENT_MESSAGE_SYNC_MODE_UNSPECIFIED,
            )
            if target_session is not None:
                validate_session_ref(target_session, "target_session")
                request.target_session.CopyFrom(session_ref_to_proto(target_session))
            return pb.ClientEnvelope(send_message=request)

        result = await self._rpc(build)
        if not isinstance(result, RelayAccepted):
            raise ProtocolError("missing transient_accepted in send response")
        return result

    async def post_packet(
        self,
        target: UserRef,
        body: bytes,
        delivery_mode: DeliveryMode,
        *,
        target_session: SessionRef | None = None,
    ) -> RelayAccepted:
        """发送瞬时数据包（``send_packet`` 的别名）。

        Args:
            target: 目标用户引用。
            body: 数据包体字节数据。
            delivery_mode: 投递模式。
            target_session: 可选的目与会话引用。

        Returns:
            中继确认信息。
        """
        return await self.send_packet(
            target,
            body,
            delivery_mode,
            target_session=target_session,
        )

    async def create_user(self, request: CreateUserRequest) -> User:
        """创建新用户。

        通过 WebSocket 在服务器上创建一个新用户。

        Args:
            request: 创建用户的请求参数（用户名和角色为必填）。

        Returns:
            创建成功的 User 对象。

        Raises:
            ValueError: 如果 username 或 role 为空。
        """
        if request.username == "":
            raise ValueError("username is required")
        if request.role == "":
            raise ValueError("role is required")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                create_user=pb.CreateUserRequest(
                    request_id=request_id,
                    username=request.username,
                    password="" if request.password is None else request.password.wire_value(),
                    profile_json=request.profile_json,
                    role=request.role,
                    login_name=request.login_name,
                )
            )
        )
        if not isinstance(result, User):
            raise ProtocolError("missing user in create_user_response")
        return result

    async def create_channel(self, request: CreateUserRequest) -> User:
        """创建频道（以用户形式）。

        频道本质上用户的一种特殊类型，角色默认为 "channel"。

        Args:
            request: 创建频道的请求参数。

        Returns:
            创建成功的 User 对象（角色为 "channel"）。
        """
        role = request.role or "channel"
        return await self.create_user(
            CreateUserRequest(
                username=request.username,
                password=request.password,
                profile_json=request.profile_json,
                role=role,
                login_name=request.login_name,
            )
        )

    async def get_user(self, target: UserRef) -> User:
        """获取用户信息。

        Args:
            target: 目标用户的引用标识。

        Returns:
            用户的详细信息。
        """
        validate_user_ref(target, "target")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                get_user=pb.GetUserRequest(request_id=request_id, user=user_ref_to_proto(target))
            )
        )
        if not isinstance(result, User):
            raise ProtocolError("missing user in get_user_response")
        return result

    async def update_user(self, target: UserRef, request: UpdateUserRequest) -> User:
        """更新用户信息。

        只更新请求中设置了值的字段。

        Args:
            target: 目标用户的引用标识。
            request: 包含要更新字段的请求参数。

        Returns:
            更新后的 User 对象。
        """
        validate_user_ref(target, "target")

        def build(request_id: int) -> pb.ClientEnvelope:
            message = pb.UpdateUserRequest(request_id=request_id, user=user_ref_to_proto(target))
            if request.username is not None:
                message.username.CopyFrom(pb.StringField(value=request.username))
            if request.login_name is not None:
                message.login_name.CopyFrom(pb.StringField(value=request.login_name))
            if request.password is not None:
                message.password.CopyFrom(pb.StringField(value=request.password.wire_value()))
            if request.profile_json is not None:
                message.profile_json.CopyFrom(pb.BytesField(value=request.profile_json))
            if request.role is not None:
                message.role.CopyFrom(pb.StringField(value=request.role))
            return pb.ClientEnvelope(update_user=message)

        result = await self._rpc(build)
        if not isinstance(result, User):
            raise ProtocolError("missing user in update_user_response")
        return result

    async def delete_user(self, target: UserRef) -> DeleteUserResult:
        """删除用户。

        Args:
            target: 要删除的目标用户引用。

        Returns:
            删除操作的结果。
        """
        validate_user_ref(target, "target")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                delete_user=pb.DeleteUserRequest(request_id=request_id, user=user_ref_to_proto(target))
            )
        )
        if not isinstance(result, DeleteUserResult):
            raise ProtocolError("missing status in delete_user_response")
        return result

    async def get_user_metadata(self, owner: UserRef, key: str) -> UserMetadata:
        """获取指定用户的元数据。

        Args:
            owner: 元数据所有者的用户引用。
            key: 元数据键名。

        Returns:
            用户元数据。

        Raises:
            ValueError: 如果 owner 或 key 无效。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                get_user_metadata=pb.GetUserMetadataRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    key=key,
                )
            )
        )
        if not isinstance(result, UserMetadata):
            raise ProtocolError("missing metadata in get_user_metadata_response")
        return result

    async def upsert_user_metadata(
        self,
        owner: UserRef,
        key: str,
        request: UpsertUserMetadataRequest,
    ) -> UserMetadata:
        """创建或更新用户元数据。

        如果指定键的元数据已存在则更新，不存在则创建。

        Args:
            owner: 元数据所有者的用户引用。
            key: 元数据键名。
            request: 包含 value 和可选的 expires_at。

        Returns:
            创建或更新后的用户元数据。

        Raises:
            ValueError: 如果 owner、key 或 request.value 无效。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        if request.value is None:
            raise ValueError("request.value is required")

        def build(request_id: int) -> pb.ClientEnvelope:
            message = pb.UpsertUserMetadataRequest(
                request_id=request_id,
                owner=user_ref_to_proto(owner),
                key=key,
                value=request.value,
            )
            if request.expires_at is not None:
                message.expires_at.CopyFrom(pb.StringField(value=request.expires_at))
            return pb.ClientEnvelope(upsert_user_metadata=message)

        result = await self._rpc(build)
        if not isinstance(result, UserMetadata):
            raise ProtocolError("missing metadata in upsert_user_metadata_response")
        return result

    async def delete_user_metadata(self, owner: UserRef, key: str) -> UserMetadata:
        """删除用户元数据。

        Args:
            owner: 元数据所有者的用户引用。
            key: 要删除的元数据键名。

        Returns:
            被删除的用户元数据。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                delete_user_metadata=pb.DeleteUserMetadataRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    key=key,
                )
            )
        )
        if not isinstance(result, UserMetadata):
            raise ProtocolError("missing metadata in delete_user_metadata_response")
        return result

    async def scan_user_metadata(
        self,
        owner: UserRef,
        request: ScanUserMetadataRequest | None = None,
    ) -> UserMetadataScanResult:
        """扫描用户元数据。

        支持按前缀过滤和分页扫描。

        Args:
            owner: 元数据所有者的用户引用。
            request: 扫描请求参数。为 None 时使用默认值（扫描全部）。

        Returns:
            扫描结果，包含元数据项列表和下一页游标。
        """
        validate_user_ref(owner, "owner")
        if request is None:
            request = ScanUserMetadataRequest()
        validate_user_metadata_scan_request(request, "request")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                scan_user_metadata=pb.ScanUserMetadataRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    prefix=request.prefix,
                    after=request.after,
                    limit=request.limit,
                )
            )
        )
        if not isinstance(result, UserMetadataScanResult):
            raise ProtocolError("missing items in scan_user_metadata_response")
        return result

    async def upsert_attachment(
        self,
        owner: UserRef,
        subject: UserRef,
        attachment_type: AttachmentType,
        config_json: bytes = b"{}",
    ) -> Attachment:
        """创建或更新用户附件关系。

        附件关系表示两个用户之间的关联（如频道订阅、黑名单等）。

        Args:
            owner: 附件所有者用户引用。
            subject: 附件目标用户引用。
            attachment_type: 附件关系类型。
            config_json: 配置信息的 JSON 字节数据，默认 ``b"{}"``。

        Returns:
            创建或更新后的附件关系。
        """
        validate_user_ref(owner, "owner")
        validate_user_ref(subject, "subject")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                upsert_user_attachment=pb.UpsertUserAttachmentRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    subject=user_ref_to_proto(subject),
                    attachment_type=attachment_type_to_proto(attachment_type),
                    config_json=config_json,
                )
            )
        )
        if not isinstance(result, Attachment):
            raise ProtocolError("missing attachment in upsert_user_attachment_response")
        return result

    async def delete_attachment(
        self,
        owner: UserRef,
        subject: UserRef,
        attachment_type: AttachmentType,
    ) -> Attachment:
        """删除用户附件关系。

        Args:
            owner: 附件所有者用户引用。
            subject: 附件目标用户引用。
            attachment_type: 附件关系类型。

        Returns:
            被删除的附件关系。
        """
        validate_user_ref(owner, "owner")
        validate_user_ref(subject, "subject")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                delete_user_attachment=pb.DeleteUserAttachmentRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    subject=user_ref_to_proto(subject),
                    attachment_type=attachment_type_to_proto(attachment_type),
                )
            )
        )
        if not isinstance(result, Attachment):
            raise ProtocolError("missing attachment in delete_user_attachment_response")
        return result

    async def list_attachments(
        self,
        owner: UserRef,
        attachment_type: AttachmentType | None = None,
    ) -> list[Attachment]:
        """列出用户的所有附件关系。

        Args:
            owner: 附件所有者用户引用。
            attachment_type: 可选的附件类型过滤，None 时返回所有类型。

        Returns:
            附件关系列表。
        """
        validate_user_ref(owner, "owner")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_user_attachments=pb.ListUserAttachmentsRequest(
                    request_id=request_id,
                    owner=user_ref_to_proto(owner),
                    attachment_type=pb.ATTACHMENT_TYPE_UNSPECIFIED
                    if attachment_type is None
                    else attachment_type_to_proto(attachment_type),
                )
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_user_attachments_response")
        return result

    async def subscribe_channel(self, subscriber: UserRef, channel: UserRef) -> Subscription:
        """订阅频道。

        Args:
            subscriber: 订阅者用户引用。
            channel: 要订阅的频道用户引用。

        Returns:
            创建的订阅关系。
        """
        attachment = await self.upsert_attachment(
            subscriber,
            channel,
            AttachmentType.CHANNEL_SUBSCRIPTION,
            b"{}",
        )
        return Subscription(
            subscriber=attachment.owner,
            channel=attachment.subject,
            subscribed_at=attachment.attached_at,
            deleted_at=attachment.deleted_at,
            origin_node_id=attachment.origin_node_id,
        )

    async def create_subscription(self, subscriber: UserRef, channel: UserRef) -> Subscription:
        """创建频道订阅（``subscribe_channel`` 的别名）。

        Args:
            subscriber: 订阅者用户引用。
            channel: 要订阅的频道用户引用。

        Returns:
            创建的订阅关系。
        """
        return await self.subscribe_channel(subscriber, channel)

    async def unsubscribe_channel(self, subscriber: UserRef, channel: UserRef) -> Subscription:
        """取消频道订阅。

        Args:
            subscriber: 订阅者用户引用。
            channel: 要取消订阅的频道用户引用。

        Returns:
            被取消的订阅关系。
        """
        attachment = await self.delete_attachment(
            subscriber,
            channel,
            AttachmentType.CHANNEL_SUBSCRIPTION,
        )
        return Subscription(
            subscriber=attachment.owner,
            channel=attachment.subject,
            subscribed_at=attachment.attached_at,
            deleted_at=attachment.deleted_at,
            origin_node_id=attachment.origin_node_id,
        )

    async def list_subscriptions(self, subscriber: UserRef) -> list[Subscription]:
        """列出用户的所有频道订阅。

        Args:
            subscriber: 订阅者用户引用。

        Returns:
            用户的所有订阅关系列表。
        """
        items = await self.list_attachments(subscriber, AttachmentType.CHANNEL_SUBSCRIPTION)
        return [
            Subscription(
                subscriber=attachment.owner,
                channel=attachment.subject,
                subscribed_at=attachment.attached_at,
                deleted_at=attachment.deleted_at,
                origin_node_id=attachment.origin_node_id,
            )
            for attachment in items
        ]

    async def block_user(self, owner: UserRef, blocked: UserRef) -> BlacklistEntry:
        """将用户加入黑名单。

        Args:
            owner: 黑名单所有者用户引用。
            blocked: 要被拉黑的用户引用。

        Returns:
            创建的黑名单条目。
        """
        attachment = await self.upsert_attachment(
            owner,
            blocked,
            AttachmentType.USER_BLACKLIST,
            b"{}",
        )
        return BlacklistEntry(
            owner=attachment.owner,
            blocked=attachment.subject,
            blocked_at=attachment.attached_at,
            deleted_at=attachment.deleted_at,
            origin_node_id=attachment.origin_node_id,
        )

    async def unblock_user(self, owner: UserRef, blocked: UserRef) -> BlacklistEntry:
        """将用户移出黑名单。

        Args:
            owner: 黑名单所有者用户引用。
            blocked: 要被移出黑名单的用户引用。

        Returns:
            被移除的黑名单条目。
        """
        attachment = await self.delete_attachment(
            owner,
            blocked,
            AttachmentType.USER_BLACKLIST,
        )
        return BlacklistEntry(
            owner=attachment.owner,
            blocked=attachment.subject,
            blocked_at=attachment.attached_at,
            deleted_at=attachment.deleted_at,
            origin_node_id=attachment.origin_node_id,
        )

    async def list_blocked_users(self, owner: UserRef) -> list[BlacklistEntry]:
        """列出用户黑名单中的所有条目。

        Args:
            owner: 黑名单所有者用户引用。

        Returns:
            黑名单条目列表。
        """
        items = await self.list_attachments(owner, AttachmentType.USER_BLACKLIST)
        return [
            BlacklistEntry(
                owner=attachment.owner,
                blocked=attachment.subject,
                blocked_at=attachment.attached_at,
                deleted_at=attachment.deleted_at,
                origin_node_id=attachment.origin_node_id,
            )
            for attachment in items
        ]

    async def list_messages(self, target: UserRef, limit: int = 0) -> list[Message]:
        """列出指定用户的持久化消息。

        Args:
            target: 目标用户引用。
            limit: 返回消息的最大数量，0 表示使用服务器默认值。

        Returns:
            消息列表。
        """
        validate_user_ref(target, "target")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_messages=pb.ListMessagesRequest(
                    request_id=request_id,
                    user=user_ref_to_proto(target),
                    limit=limit,
                )
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_messages_response")
        return result

    async def list_events(self, after: int = 0, limit: int = 0) -> list[Event]:
        """列出领域事件。

        支持按序列号偏移和数量限制进行分页查询。

        Args:
            after: 起始事件序列号（包含），0 表示从最早开始。
            limit: 返回事件的最大数量，0 表示使用服务器默认值。

        Returns:
            事件列表。
        """
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_events=pb.ListEventsRequest(request_id=request_id, after=after, limit=limit)
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_events_response")
        return result

    async def list_cluster_nodes(self) -> list[ClusterNode]:
        """列出集群中的所有节点。

        Returns:
            集群节点列表。
        """
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_cluster_nodes=pb.ListClusterNodesRequest(request_id=request_id)
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_cluster_nodes_response")
        return result

    async def list_node_logged_in_users(self, node_id: int) -> list[LoggedInUser]:
        """列出指定节点上当前登录的所有用户。

        Args:
            node_id: 节点 ID。

        Returns:
            已登录用户列表。
        """
        validate_positive_int(node_id, "node_id")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_node_logged_in_users=pb.ListNodeLoggedInUsersRequest(
                    request_id=request_id,
                    node_id=node_id,
                )
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_node_logged_in_users_response")
        return result

    async def resolve_user_sessions(self, user: UserRef) -> ResolvedUserSessions:
        """解析用户的所有在线会话。

        返回指定用户在各节点上的在线状态和详细会话信息。

        Args:
            user: 目标用户引用。

        Returns:
            包含在线节点分布和会话列表的解析结果。
        """
        validate_user_ref(user, "user")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                resolve_user_sessions=pb.ResolveUserSessionsRequest(
                    request_id=request_id,
                    user=user_ref_to_proto(user),
                )
            )
        )
        if not isinstance(result, ResolvedUserSessions):
            raise ProtocolError("missing sessions in resolve_user_sessions_response")
        return result

    async def operations_status(self) -> OperationsStatus:
        """获取节点运行状态。

        包含消息窗口大小、事件序列号、写入门控状态、节点对等信息。

        Returns:
            节点的当前运行状态。
        """
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                operations_status=pb.OperationsStatusRequest(request_id=request_id)
            )
        )
        if not isinstance(result, OperationsStatus):
            raise ProtocolError("missing status in operations_status_response")
        return result

    async def metrics(self) -> str:
        """获取服务器指标信息。

        Returns:
            指标信息的纯文本内容。
        """
        result = await self._rpc(lambda request_id: pb.ClientEnvelope(metrics=pb.MetricsRequest(request_id=request_id)))
        if not isinstance(result, str):
            raise ProtocolError("missing text in metrics_response")
        return result

    async def _run(self) -> None:
        delay = self._cfg.initial_reconnect_delay
        try:
            while True:
                err = await self._connect_and_serve()
                if err is None:
                    delay = self._cfg.initial_reconnect_delay
                    if self._closed:
                        return
                    continue
                if self._closed or not self._should_retry(err):
                    self._signal_first_connect(err)
                    self._fail_all_pending(err)
                    return
                await self._safe_handler_call(self._cfg.handler.on_error, err)
                try:
                    await asyncio.sleep(delay)
                except asyncio.CancelledError:
                    self._fail_all_pending(ClosedError())
                    return
                delay = min(delay * 2, self._cfg.max_reconnect_delay)
        except asyncio.CancelledError:
            self._fail_all_pending(ClosedError())

    async def _connect_and_serve(self) -> BaseException | None:
        if self._closed:
            return ClosedError()

        ws = None
        ping_task: asyncio.Task[None] | None = None
        try:
            seen = await self._cfg.cursor_store.load_seen_messages()
            ws = await self._dial()
            login_request = pb.LoginRequest(
                password=self._cfg.credentials.password.wire_value(),
                transient_only=self._cfg.transient_only,
            )
            if self._cfg.credentials.login_name != "":
                login_request.login_name = self._cfg.credentials.login_name
            else:
                login_request.user.CopyFrom(
                    user_ref_to_proto(
                        UserRef(node_id=self._cfg.credentials.node_id, user_id=self._cfg.credentials.user_id)
                    )
                )
            login_request.seen_messages.extend(cursor_to_proto(cursor) for cursor in seen)
            await self._write_proto(ws, pb.ClientEnvelope(login=login_request))
            server_env = await self._read_proto(ws)
            login_info = self._expect_login(server_env)

            self._ws = ws
            self._connected = True
            await self._safe_handler_call(self._cfg.handler.on_login, login_info)
            self._signal_first_connect(None)

            ping_task = asyncio.create_task(self._ping_loop())
            read_err = await self._read_loop(ws)
            self._connected = False
            self._login_info = None
            if self._ws is ws:
                self._ws = None
            self._fail_all_pending(DisconnectedError())
            await self._safe_handler_call(self._cfg.handler.on_disconnect, read_err)
            await self._safe_close_ws(ws)
            return read_err
        except BaseException as exc:
            if ws is not None:
                await self._safe_close_ws(ws)
            return exc
        finally:
            self._connected = False
            if self._ws is ws:
                self._ws = None
            if ping_task is not None:
                ping_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await ping_task

    async def _dial(self) -> Any:
        ws_url = websocket_url(self._cfg.base_url, self._cfg.realtime_stream)
        try:
            return await websockets.connect(ws_url)
        except Exception as exc:  # pragma: no cover - library-specific subclasses vary
            raise ConnectionError("dial", exc) from exc

    def _expect_login(self, env: pb.ServerEnvelope) -> LoginInfo:
        body = env.WhichOneof("body")
        if body == "login_response":
            response = env.login_response
            if not response.HasField("session_ref"):
                raise ProtocolError("missing session_ref in login_response")
            info = LoginInfo(
                user=user_from_proto(response.user),
                protocol_version=response.protocol_version,
                session_ref=session_ref_from_proto(response.session_ref),
            )
            self._login_info = info
            return info
        if body == "error":
            error = env.error
            self._stop_reconnect = error.code == "unauthorized"
            raise ServerError(error.code, error.message, error.request_id)
        raise ProtocolError("expected login_response or error")

    async def _read_loop(self, ws: Any) -> BaseException:
        while True:
            try:
                env = await self._read_proto(ws)
            except BaseException as exc:
                return exc
            try:
                await self._handle_server_envelope(env)
            except BaseException as exc:
                await self._safe_handler_call(self._cfg.handler.on_error, exc)

    async def _handle_server_envelope(self, env: pb.ServerEnvelope) -> None:
        body = env.WhichOneof("body")
        if body == "message_pushed":
            msg = message_from_proto(env.message_pushed.message)
            await self._persist_message(msg)
            if self._cfg.ack_messages:
                try:
                    await self._send_envelope(
                        pb.ClientEnvelope(ack_message=pb.AckMessage(cursor=cursor_to_proto(msg.cursor())))
                    )
                except (NotConnectedError, ClosedError):
                    pass
                except BaseException as exc:
                    await self._safe_handler_call(self._cfg.handler.on_error, exc)
            await self._safe_handler_call(self._cfg.handler.on_message, msg)
            return
        if body == "packet_pushed":
            packet = packet_from_proto(env.packet_pushed.packet)
            if self._relay is not None:
                handled = await self._relay.handle_packet(packet)
                if handled:
                    return
            await self._safe_handler_call(self._cfg.handler.on_packet, packet)
            return
        if body == "send_message_response":
            response = env.send_message_response
            inner = response.WhichOneof("body")
            if inner == "message":
                msg = message_from_proto(response.message)
                await self._persist_message(msg)
                self._resolve_pending(response.request_id, value=msg)
            elif inner == "transient_accepted":
                self._resolve_pending(
                    response.request_id,
                    value=relay_accepted_from_proto(response.transient_accepted),
                )
            else:
                self._resolve_pending(
                    response.request_id,
                    error=ProtocolError("empty send_message_response"),
                )
            return
        if body == "pong":
            self._resolve_pending(env.pong.request_id, value=None)
            return
        if body == "create_user_response":
            self._resolve_pending(env.create_user_response.request_id, value=user_from_proto(env.create_user_response.user))
            return
        if body == "get_user_response":
            self._resolve_pending(env.get_user_response.request_id, value=user_from_proto(env.get_user_response.user))
            return
        if body == "update_user_response":
            self._resolve_pending(
                env.update_user_response.request_id,
                value=user_from_proto(env.update_user_response.user),
            )
            return
        if body == "delete_user_response":
            self._resolve_pending(
                env.delete_user_response.request_id,
                value=DeleteUserResult(
                    status=env.delete_user_response.status,
                    user=UserRef(
                        node_id=env.delete_user_response.user.node_id,
                        user_id=env.delete_user_response.user.user_id,
                    ),
                ),
            )
            return
        if body == "get_user_metadata_response":
            self._resolve_pending(
                env.get_user_metadata_response.request_id,
                value=user_metadata_from_proto(env.get_user_metadata_response.metadata),
            )
            return
        if body == "upsert_user_metadata_response":
            self._resolve_pending(
                env.upsert_user_metadata_response.request_id,
                value=user_metadata_from_proto(env.upsert_user_metadata_response.metadata),
            )
            return
        if body == "delete_user_metadata_response":
            self._resolve_pending(
                env.delete_user_metadata_response.request_id,
                value=user_metadata_from_proto(env.delete_user_metadata_response.metadata),
            )
            return
        if body == "scan_user_metadata_response":
            self._resolve_pending(
                env.scan_user_metadata_response.request_id,
                value=user_metadata_scan_result_from_proto(env.scan_user_metadata_response),
            )
            return
        if body == "list_messages_response":
            self._resolve_pending(
                env.list_messages_response.request_id,
                value=messages_from_proto(list(env.list_messages_response.items)),
            )
            return
        if body == "upsert_user_attachment_response":
            self._resolve_pending(
                env.upsert_user_attachment_response.request_id,
                value=attachment_from_proto(env.upsert_user_attachment_response.attachment),
            )
            return
        if body == "delete_user_attachment_response":
            self._resolve_pending(
                env.delete_user_attachment_response.request_id,
                value=attachment_from_proto(env.delete_user_attachment_response.attachment),
            )
            return
        if body == "list_user_attachments_response":
            self._resolve_pending(
                env.list_user_attachments_response.request_id,
                value=attachments_from_proto(list(env.list_user_attachments_response.items)),
            )
            return
        if body == "list_events_response":
            self._resolve_pending(
                env.list_events_response.request_id,
                value=events_from_proto(list(env.list_events_response.items)),
            )
            return
        if body == "list_cluster_nodes_response":
            self._resolve_pending(
                env.list_cluster_nodes_response.request_id,
                value=cluster_nodes_from_proto(list(env.list_cluster_nodes_response.items)),
            )
            return
        if body == "list_node_logged_in_users_response":
            self._resolve_pending(
                env.list_node_logged_in_users_response.request_id,
                value=logged_in_users_from_proto(list(env.list_node_logged_in_users_response.items)),
            )
            return
        if body == "resolve_user_sessions_response":
            self._resolve_pending(
                env.resolve_user_sessions_response.request_id,
                value=resolved_user_sessions_from_proto(env.resolve_user_sessions_response),
            )
            return
        if body == "operations_status_response":
            self._resolve_pending(
                env.operations_status_response.request_id,
                value=operations_status_from_proto(env.operations_status_response.status),
            )
            return
        if body == "metrics_response":
            self._resolve_pending(env.metrics_response.request_id, value=env.metrics_response.text)
            return
        if body == "error":
            server_error = ServerError(env.error.code, env.error.message, env.error.request_id)
            if env.error.request_id != 0:
                self._resolve_pending(env.error.request_id, error=server_error)
                return
            raise server_error
        if body == "login_response":
            raise ProtocolError("unexpected login_response after authentication")
        raise ProtocolError("unsupported server envelope")

    async def _send_envelope(self, env: pb.ClientEnvelope) -> None:
        if self._closed:
            raise ClosedError()
        if self._ws is None:
            raise NotConnectedError()
        await self._write_proto(self._ws, env)

    async def _write_proto(self, ws: Any, message: Any) -> None:
        payload = message.SerializeToString()
        async with self._write_lock:
            try:
                await ws.send(payload)
            except websockets.ConnectionClosed as exc:
                if self._closed:
                    raise ClosedError() from exc
                raise ConnectionError("write", exc) from exc

    async def _read_proto(self, ws: Any) -> pb.ServerEnvelope:
        try:
            payload = await ws.recv()
        except websockets.ConnectionClosed as exc:
            if self._closed:
                raise ClosedError() from exc
            raise ConnectionError("read", exc) from exc
        if isinstance(payload, str):
            raise ProtocolError("invalid protobuf frame")
        env = pb.ServerEnvelope()
        try:
            env.ParseFromString(payload)
        except DecodeError as exc:
            raise ProtocolError("invalid protobuf frame") from exc
        return env

    async def _ping_loop(self) -> None:
        while True:
            await asyncio.sleep(self._cfg.ping_interval)
            try:
                await self.ping()
            except (NotConnectedError, ClosedError, DisconnectedError):
                return
            except BaseException as exc:
                await self._safe_handler_call(self._cfg.handler.on_error, exc)

    async def _persist_message(self, message: Message) -> None:
        await self._cfg.cursor_store.save_message(message)
        await self._cfg.cursor_store.save_cursor(message.cursor())

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _register_pending(self, request_id: int) -> asyncio.Future[Any]:
        if self._closed:
            raise ClosedError()
        future = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        return future

    async def _rpc(self, build: Any) -> Any:
        request_id = self._next_request_id()
        future = self._register_pending(request_id)
        try:
            await self._send_envelope(build(request_id))
            return await asyncio.wait_for(future, timeout=self._cfg.request_timeout)
        finally:
            self._pending.pop(request_id, None)

    def _resolve_pending(self, request_id: int, *, value: Any = None, error: BaseException | None = None) -> None:
        future = self._pending.get(request_id)
        if future is None or future.done():
            return
        if error is not None:
            future.set_exception(error)
        else:
            future.set_result(value)

    def _fail_all_pending(self, error: BaseException) -> None:
        for request_id, future in list(self._pending.items()):
            if future.done():
                continue
            future.set_exception(_copy_error(error))

    def _should_retry(self, error: BaseException) -> bool:
        if self._closed:
            return False
        if self._stop_reconnect:
            return False
        if not self._cfg.reconnect:
            return False
        if isinstance(error, ServerError) and error.unauthorized():
            return False
        return not isinstance(error, ClosedError)

    def _signal_first_connect(self, error: BaseException | None) -> None:
        if self._first_connect is None or self._first_connect.done():
            return
        if error is None:
            self._first_connect.set_result(None)
        else:
            self._first_connect.set_exception(_copy_error(error))

    async def _safe_handler_call(self, callback: Any, *args: Any) -> None:
        try:
            await callback(*args)
        except Exception:
            return None

    async def _safe_close_ws(self, ws: Any) -> None:
        try:
            await ws.close()
        except Exception:
            return None

    async def _await_run_task(self) -> None:
        if self._run_task is None:
            return
        try:
            await self._run_task
        except asyncio.CancelledError:
            return


def websocket_url(base: str, realtime: bool) -> str:
    """从 HTTP 基础 URL 生成 WebSocket 连接 URL。

    将 ``http://`` 转换为 ``ws://``，将 ``https://`` 转换为 ``wss://``，
    并根据 realtime 参数选择不同的 WebSocket 端点路径。

    普通客户端连接路径为 ``/ws/client``，
    实时流连接路径为 ``/ws/realtime``。

    Args:
        base: HTTP 基础 URL，如 ``http://localhost:8080``。
        realtime: 是否为实时流连接。

    Returns:
        可用于 WebSocket 连接的 URL 字符串。

    Raises:
        ValueError: 如果 base URL 的 scheme 不受支持。
    """
    parsed = urlparse(base)
    scheme = parsed.scheme
    if scheme == "http":
        scheme = "ws"
    elif scheme == "https":
        scheme = "wss"
    elif scheme not in {"ws", "wss"}:
        raise ValueError(f"unsupported base URL scheme {parsed.scheme!r}")
    path = "/ws/realtime" if realtime else "/ws/client"
    base_path = parsed.path.rstrip("/")
    if base_path in {"", "/"}:
        final_path = path
    else:
        final_path = f"{base_path}{path}"
    return urlunparse((scheme, parsed.netloc, final_path, "", "", ""))


def _copy_error(error: BaseException) -> BaseException:
    if isinstance(error, ClosedError):
        return ClosedError()
    if isinstance(error, NotConnectedError):
        return NotConnectedError()
    if isinstance(error, DisconnectedError):
        return DisconnectedError()
    if isinstance(error, ServerError):
        return ServerError(error.code, error.server_message, error.request_id)
    if isinstance(error, ProtocolError):
        return ProtocolError(error.protocol_message)
    if isinstance(error, ConnectionError):
        return ConnectionError(error.op, error.cause)
    return error
