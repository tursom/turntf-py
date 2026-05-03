from __future__ import annotations

import base64
import json
from typing import Any, Iterable
from urllib.parse import quote, urlencode

import httpx

from .errors import ConnectionError, ProtocolError
from .mapping import (
    attachment_from_http,
    blacklist_entry_from_http,
    cluster_node_from_http,
    delete_user_result_from_http,
    event_from_http,
    logged_in_user_from_http,
    message_from_http,
    operations_status_from_http,
    relay_accepted_from_http,
    subscription_from_http,
    user_from_http,
    user_metadata_from_http,
    user_metadata_scan_result_from_http,
    users_from_http,
)
from .password import PasswordInput, plain_password
from .types import (
    Attachment,
    AttachmentType,
    BlacklistEntry,
    ClusterNode,
    CreateUserRequest,
    DeleteUserResult,
    DeliveryMode,
    Event,
    ListUsersRequest,
    LoggedInUser,
    Message,
    OperationsStatus,
    ScanUserMetadataRequest,
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
    normalize_list_users_request,
    resolve_user_metadata_upsert_value,
    validate_positive_int,
    validate_user_metadata_key,
    validate_user_metadata_scan_request,
    validate_user_ref,
)


class AsyncHTTPClient:
    """基于 HTTP API 的 turntf 异步客户端。

    提供通过 HTTP REST 接口与 turntf 服务器交互的功能，
    包括用户管理、消息收发、元数据管理、附件管理等。

    创建实例后需要先调用 ``login`` 获取认证令牌（token），
    然后将 token 传给后续的 API 调用。

    Attributes:
        base_url: 服务器基础 URL（不包含尾部斜杠）。
    """

    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float | None = 10.0,
    ) -> None:
        """初始化 AsyncHTTPClient。

        Args:
            base_url: 服务器基础 URL，如 ``http://localhost:8080``。
            client: 可选的 httpx.AsyncClient 实例。不提供时会自动创建。
            timeout: HTTP 请求超时时间（秒），默认 10.0 秒。

        Raises:
            ValueError: 如果 base_url 为空字符串。
        """
        if base_url.strip() == "":
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def __aenter__(self) -> "AsyncHTTPClient":
        """异步上下文管理器入口。

        Returns:
            AsyncHTTPClient 实例自身。
        """
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        """异步上下文管理器出口，自动关闭 HTTP 客户端。"""
        await self.close()

    async def close(self) -> None:
        """关闭 HTTP 客户端，释放连接资源。

        仅当客户端由本实例创建时（非外部传入）才执行关闭操作。
        """
        if self._owns_client:
            await self._client.aclose()

    async def login(
        self,
        node_id: int | None = None,
        user_id: int | None = None,
        password: str | None = None,
        *,
        login_name: str | None = None,
    ) -> str:
        """使用明文密码登录并获取认证令牌。

        Args:
            node_id: 节点 ID（与 login_name 二选一）。
            user_id: 用户 ID（与 login_name 二选一）。
            password: 明文密码。
            login_name: 登录名（与 node_id/user_id 二选一）。

        Returns:
            认证令牌（JWT token）字符串。

        Raises:
            ValueError: 如果密码为 None 或登录选择器参数无效。
            ProtocolError: 如果服务器响应格式异常。
        """
        if password is None:
            raise ValueError("password is required")
        return await self.login_with_password(
            node_id=node_id,
            user_id=user_id,
            password=plain_password(password),
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
        """使用 PasswordInput 密码对象登录并获取认证令牌。

        Args:
            node_id: 节点 ID（与 login_name 二选一）。
            user_id: 用户 ID（与 login_name 二选一）。
            password: PasswordInput 密码输入对象。
            login_name: 登录名（与 node_id/user_id 二选一）。

        Returns:
            认证令牌（JWT token）字符串。

        Raises:
            ValueError: 如果密码为 None 或登录选择器参数无效。
            ProtocolError: 如果服务器响应格式异常。
        """
        if password is None:
            raise ValueError("password is required")
        normalized_login_name = validate_login_selector(
            node_id=node_id,
            user_id=user_id,
            login_name=login_name,
            field="login",
        )
        body: dict[str, Any] = {"password": password.wire_value()}
        if normalized_login_name != "":
            body["login_name"] = normalized_login_name
        else:
            assert node_id is not None
            assert user_id is not None
            body["node_id"] = node_id
            body["user_id"] = user_id
        response = await self._do_json(
            "POST",
            "/auth/login",
            "",
            body,
            {200},
        )
        if not isinstance(response, dict):
            raise ProtocolError("unexpected login response")
        token = response.get("token")
        if not isinstance(token, str) or token == "":
            raise ProtocolError("empty token in login response")
        return token

    async def create_user(self, token: str, request: CreateUserRequest) -> User:
        """创建新用户。

        使用 HTTP API 在服务器上创建一个新用户或频道。

        Args:
            token: 认证令牌。
            request: 创建用户的请求参数。

        Returns:
            创建成功的 User 对象。

        Raises:
            ValueError: 如果 username 或 role 为空。
        """
        if request.username == "":
            raise ValueError("username is required")
        if request.role == "":
            raise ValueError("role is required")
        body: dict[str, Any] = {"username": request.username, "role": request.role}
        if request.login_name != "":
            body["login_name"] = request.login_name
        if request.password is not None:
            body["password"] = request.password.wire_value()
        if request.profile_json:
            body["profile"] = _json_bytes_to_value(request.profile_json, "profile_json")
        response = await self._do_json("POST", "/users", token, body, {200, 201})
        return user_from_http(_expect_dict(response, "create user response"))

    async def create_channel(self, token: str, request: CreateUserRequest) -> User:
        """创建频道（以用户形式）。

        频道本质上是具有 "channel" 角色的特殊用户。
        如果 request.role 未设置，则默认使用 "channel"。

        Args:
            token: 认证令牌。
            request: 创建频道的请求参数。

        Returns:
            创建成功的 User 对象（角色为 "channel"）。
        """
        role = request.role or "channel"
        return await self.create_user(
            token,
            CreateUserRequest(
                username=request.username,
                password=request.password,
                profile_json=request.profile_json,
                role=role,
                login_name=request.login_name,
            ),
        )

    async def list_users(
        self,
        token: str,
        request: ListUsersRequest | None = None,
        *,
        name: str | None = None,
        uid: UserRef | None = None,
    ) -> list[User]:
        """列出当前用户可通讯的活跃用户。

        支持按名称子串和精确 uid 组合过滤。普通用户看到其他联系人时，
        服务端可能会把 ``login_name`` 脱敏为空字符串；若目标用户或频道写入
        ``system.visible_to_others=false``，普通用户的列表结果里也可能看不到它。
        该列表只反映“当前可见的候选对象”，不表示知道 uid 后一定不能继续发消息。

        Args:
            token: 认证令牌。
            request: 可选的过滤请求对象。
            name: 可选的名称过滤关键字。与 request 二选一。
            uid: 可选的精确用户过滤。与 request 二选一。

        Returns:
            当前用户可通讯的用户列表。
        """
        normalized = normalize_list_users_request(request, name=name, uid=uid, field="request")
        query: dict[str, str] = {}
        if normalized.name != "":
            query["name"] = normalized.name
        if normalized.uid is not None:
            query["uid"] = f"{normalized.uid.node_id}:{normalized.uid.user_id}"
        suffix = f"?{urlencode(query)}" if query else ""
        response = await self._do_json("GET", f"/users{suffix}", token, None, {200})
        items = _items_from_payload(response, "users", "items")
        user_items: list[dict[str, Any]] = []
        for item in items:
            user_items.append(_expect_dict(item, "user item"))
        return users_from_http(user_items)

    async def get_user(self, token: str, target: UserRef) -> User:
        """获取用户信息。

        Args:
            token: 认证令牌。
            target: 目标用户的引用标识。

        Returns:
            用户的详细信息。
        """
        validate_user_ref(target, "target")
        response = await self._do_json(
            "GET",
            f"/nodes/{target.node_id}/users/{target.user_id}",
            token,
            None,
            {200},
        )
        return user_from_http(_expect_dict(response, "get user response"))

    async def update_user(self, token: str, target: UserRef, request: UpdateUserRequest) -> User:
        """更新用户信息。

        只更新请求中设置了值的字段，未设置的字段保持不变。

        Args:
            token: 认证令牌。
            target: 目标用户的引用标识。
            request: 包含要更新字段的请求参数。

        Returns:
            更新后的 User 对象。
        """
        validate_user_ref(target, "target")
        body: dict[str, Any] = {}
        if request.username is not None:
            body["username"] = request.username
        if request.login_name is not None:
            body["login_name"] = request.login_name
        if request.password is not None:
            body["password"] = request.password.wire_value()
        if request.profile_json is not None:
            body["profile"] = _json_bytes_to_value(request.profile_json, "profile_json")
        if request.role is not None:
            body["role"] = request.role
        response = await self._do_json(
            "PATCH",
            f"/nodes/{target.node_id}/users/{target.user_id}",
            token,
            body,
            {200},
        )
        return user_from_http(_expect_dict(response, "update user response"))

    async def delete_user(self, token: str, target: UserRef) -> DeleteUserResult:
        """删除用户。

        Args:
            token: 认证令牌。
            target: 要删除的目标用户引用。

        Returns:
            删除操作的结果。
        """
        validate_user_ref(target, "target")
        response = await self._do_json(
            "DELETE",
            f"/nodes/{target.node_id}/users/{target.user_id}",
            token,
            None,
            {200},
        )
        return delete_user_result_from_http(_expect_dict(response, "delete user response"))

    async def get_user_metadata(self, token: str, owner: UserRef, key: str) -> UserMetadata:
        """获取指定用户的元数据。

        Args:
            token: 认证令牌。
            owner: 元数据所有者的用户引用。owner 可以是普通用户，也可以是 channel。
            key: 元数据键名。

        Returns:
            用户元数据。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        response = await self._do_json(
            "GET",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/metadata/{quote(key, safe='')}",
            token,
            None,
            {200},
        )
        return user_metadata_from_http(_expect_dict(response, "get user metadata response"))

    async def upsert_user_metadata(
        self,
        token: str,
        owner: UserRef,
        key: str,
        request: UpsertUserMetadataRequest,
    ) -> UserMetadata:
        """创建或更新用户元数据。

        如果指定键的元数据已存在则更新，不存在则创建。

        Args:
            token: 认证令牌。
            owner: 元数据所有者的用户引用。owner 可以是普通用户，也可以是 channel。
            key: 元数据键名。
            request: 包含 ``value`` 或 ``typed_value`` 二选一，以及可选的 expires_at。

        Returns:
            创建或更新后的用户元数据。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        raw_value = resolve_user_metadata_upsert_value(request, key, "request")
        body = _user_metadata_request_body(request, raw_value)
        response = await self._do_json(
            "PUT",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/metadata/{quote(key, safe='')}",
            token,
            body,
            {200, 201},
        )
        return user_metadata_from_http(_expect_dict(response, "upsert user metadata response"))

    async def delete_user_metadata(self, token: str, owner: UserRef, key: str) -> UserMetadata:
        """删除用户元数据。

        Args:
            token: 认证令牌。
            owner: 元数据所有者的用户引用。owner 可以是普通用户，也可以是 channel。
            key: 要删除的元数据键名。

        Returns:
            被删除的用户元数据。
        """
        validate_user_ref(owner, "owner")
        validate_user_metadata_key(key, "key")
        response = await self._do_json(
            "DELETE",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/metadata/{quote(key, safe='')}",
            token,
            None,
            {200},
        )
        return user_metadata_from_http(_expect_dict(response, "delete user metadata response"))

    async def scan_user_metadata(
        self,
        token: str,
        owner: UserRef,
        request: ScanUserMetadataRequest | None = None,
    ) -> UserMetadataScanResult:
        """扫描用户元数据。

        支持按前缀过滤和分页扫描。

        Args:
            token: 认证令牌。
            owner: 元数据所有者的用户引用。owner 可以是普通用户，也可以是 channel。
            request: 扫描请求参数，包含 prefix、after 和 limit。

        Returns:
            扫描结果，包含元数据项列表和下一页游标。
        """
        validate_user_ref(owner, "owner")
        if request is None:
            request = ScanUserMetadataRequest()
        validate_user_metadata_scan_request(request, "request")
        query: dict[str, str] = {}
        if request.prefix != "":
            query["prefix"] = request.prefix
        if request.after != "":
            query["after"] = request.after
        if request.limit > 0:
            query["limit"] = str(request.limit)
        suffix = f"?{urlencode(query)}" if query else ""
        response = await self._do_json(
            "GET",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/metadata{suffix}",
            token,
            None,
            {200},
        )
        return user_metadata_scan_result_from_http(_expect_dict(response, "scan user metadata response"))

    async def create_subscription(self, token: str, user: UserRef, channel: UserRef) -> Subscription:
        """创建频道订阅关系。

        Args:
            token: 认证令牌。
            user: 订阅者用户引用。
            channel: 要订阅的频道用户引用。

        Returns:
            创建的订阅关系。
        """
        attachment = await self.upsert_attachment(
            token,
            user,
            channel,
            AttachmentType.CHANNEL_SUBSCRIPTION,
            b"{}",
        )
        return subscription_from_http(_attachment_payload(attachment))

    async def subscribe_channel(self, token: str, subscriber: UserRef, channel: UserRef) -> Subscription:
        """订阅频道（``create_subscription`` 的别名）。

        Args:
            token: 认证令牌。
            subscriber: 订阅者用户引用。
            channel: 要订阅的频道用户引用。

        Returns:
            创建的订阅关系。
        """
        return await self.create_subscription(token, subscriber, channel)

    async def unsubscribe_channel(self, token: str, subscriber: UserRef, channel: UserRef) -> Subscription:
        """取消频道订阅。

        Args:
            token: 认证令牌。
            subscriber: 订阅者用户引用。
            channel: 要取消订阅的频道用户引用。

        Returns:
            被取消的订阅关系。
        """
        attachment = await self.delete_attachment(
            token,
            subscriber,
            channel,
            AttachmentType.CHANNEL_SUBSCRIPTION,
        )
        return subscription_from_http(_attachment_payload(attachment))

    async def list_subscriptions(self, token: str, subscriber: UserRef) -> list[Subscription]:
        """列出用户的所有频道订阅。

        Args:
            token: 认证令牌。
            subscriber: 订阅者用户引用。

        Returns:
            用户的所有订阅关系列表。
        """
        items = await self.list_attachments(token, subscriber, AttachmentType.CHANNEL_SUBSCRIPTION)
        return [subscription_from_http(_attachment_payload(item)) for item in items]

    async def list_messages(
        self,
        token: str,
        target: UserRef,
        limit: int = 0,
        peer_node_id: int | str | None = None,
        peer_user_id: int | str | None = None,
    ) -> list[Message]:
        """列出指定用户的持久化消息。

        Args:
            token: 认证令牌。
            target: 目标用户引用（支持 0 作为 "当前用户" 的哨兵值）。
            limit: 返回消息的最大数量，0 表示使用服务器默认值。
            peer_node_id: 可选的 peer 节点 ID，用于按会话过滤查询。
            peer_user_id: 可选的 peer 用户 ID，用于按会话过滤查询。

        Returns:
            消息列表。
        """
        query_parts: list[str] = []
        if limit > 0:
            query_parts.append(f"limit={limit}")
        if peer_node_id is not None and peer_user_id is not None:
            query_parts.append(f"peer_node_id={peer_node_id}")
            query_parts.append(f"peer_user_id={peer_user_id}")
        query = f"?{'&'.join(query_parts)}" if query_parts else ""
        response = await self._do_json(
            "GET",
            f"/nodes/{target.node_id}/users/{target.user_id}/messages{query}",
            token,
            None,
            {200},
        )
        items = _items_from_payload(response, "items")
        return [message_from_http(_expect_dict(item, "message item")) for item in items]

    async def post_message(self, token: str, target: UserRef, body: bytes) -> Message:
        """发送持久化消息。

        发送的消息会被服务器持久化存储，并投递给目标用户。

        Args:
            token: 认证令牌。
            target: 目标用户引用。
            body: 消息体字节数据。

        Returns:
            已发送的消息。
        """
        validate_user_ref(target, "target")
        if len(body) == 0:
            raise ValueError("body is required")
        response = await self._do_json(
            "POST",
            f"/nodes/{target.node_id}/users/{target.user_id}/messages",
            token,
            {"body": base64.b64encode(body).decode("ascii")},
            {200, 201},
        )
        return message_from_http(_expect_dict(response, "post message response"))

    async def post_packet(
        self,
        token: str,
        target_node_id: int,
        relay_target: UserRef,
        body: bytes,
        mode: DeliveryMode,
    ) -> RelayAccepted:
        """发送瞬时数据包（非持久化消息）。

        数据包不会被持久化存储，适用于实时通信场景。
        如果目标用户离线，数据包可能会丢失。

        Args:
            token: 认证令牌。
            target_node_id: 目标节点 ID（必须与 relay_target.node_id 一致）。
            relay_target: 中继目标的用户引用。
            body: 数据包体字节数据。
            mode: 投递模式。

        Returns:
            中继确认信息。

        Raises:
            ValueError: 如果参数无效或节点 ID 不匹配。
        """
        validate_positive_int(target_node_id, "target_node_id")
        validate_user_ref(relay_target, "relay_target")
        if target_node_id != relay_target.node_id:
            raise ValueError(
                f"target node ID {target_node_id} does not match target user node_id {relay_target.node_id}"
            )
        if len(body) == 0:
            raise ValueError("body is required")
        validate_delivery_mode(mode)
        response = await self._do_json(
            "POST",
            f"/nodes/{relay_target.node_id}/users/{relay_target.user_id}/messages",
            token,
            {
                "body": base64.b64encode(body).decode("ascii"),
                "delivery_kind": "transient",
                "delivery_mode": mode.value,
            },
            {202},
        )
        return relay_accepted_from_http(_expect_dict(response, "post packet response"))

    async def list_cluster_nodes(self, token: str) -> list[ClusterNode]:
        """列出集群中的所有节点。

        Args:
            token: 认证令牌。

        Returns:
            集群节点列表。
        """
        response = await self._do_json("GET", "/cluster/nodes", token, None, {200})
        items = _items_from_payload(response, "nodes", "items")
        return [cluster_node_from_http(_expect_dict(item, "cluster node item")) for item in items]

    async def list_node_logged_in_users(self, token: str, node_id: int) -> list[LoggedInUser]:
        """列出指定节点上当前登录的所有用户。

        Args:
            token: 认证令牌。
            node_id: 节点 ID。

        Returns:
            已登录用户列表。
        """
        validate_positive_int(node_id, "node_id")
        response = await self._do_json(
            "GET",
            f"/cluster/nodes/{node_id}/logged-in-users",
            token,
            None,
            {200},
        )
        items = _items_from_payload(response, "items")
        return [logged_in_user_from_http(_expect_dict(item, "logged-in user item")) for item in items]

    async def block_user(self, token: str, owner: UserRef, blocked: UserRef) -> BlacklistEntry:
        """将用户加入黑名单。

        Args:
            token: 认证令牌。
            owner: 黑名单所有者用户引用。
            blocked: 要被拉黑的用户引用。

        Returns:
            创建的黑名单条目。
        """
        attachment = await self.upsert_attachment(
            token,
            owner,
            blocked,
            AttachmentType.USER_BLACKLIST,
            b"{}",
        )
        return blacklist_entry_from_http(_attachment_payload(attachment))

    async def unblock_user(self, token: str, owner: UserRef, blocked: UserRef) -> BlacklistEntry:
        """将用户移出黑名单。

        Args:
            token: 认证令牌。
            owner: 黑名单所有者用户引用。
            blocked: 要被移出黑名单的用户引用。

        Returns:
            被移除的黑名单条目。
        """
        attachment = await self.delete_attachment(
            token,
            owner,
            blocked,
            AttachmentType.USER_BLACKLIST,
        )
        return blacklist_entry_from_http(_attachment_payload(attachment))

    async def list_blocked_users(self, token: str, owner: UserRef) -> list[BlacklistEntry]:
        """列出用户黑名单中的所有条目。

        Args:
            token: 认证令牌。
            owner: 黑名单所有者用户引用。

        Returns:
            黑名单条目列表。
        """
        items = await self.list_attachments(token, owner, AttachmentType.USER_BLACKLIST)
        return [blacklist_entry_from_http(_attachment_payload(item)) for item in items]

    async def upsert_attachment(
        self,
        token: str,
        owner: UserRef,
        subject: UserRef,
        attachment_type: AttachmentType,
        config_json: bytes,
    ) -> Attachment:
        """创建或更新用户附件关系。

        附件关系表示两个用户之间的关联（如频道订阅、黑名单等）。

        Args:
            token: 认证令牌。
            owner: 附件所有者用户引用。
            subject: 附件目标用户引用。
            attachment_type: 附件关系类型。
            config_json: 配置信息的 JSON 字节数据。

        Returns:
            创建或更新后的附件关系。
        """
        validate_user_ref(owner, "owner")
        validate_user_ref(subject, "subject")
        response = await self._do_json(
            "PUT",
            (
                f"/nodes/{owner.node_id}/users/{owner.user_id}/attachments/"
                f"{attachment_type.value}/{subject.node_id}/{subject.user_id}"
            ),
            token,
            {"config_json": {} if len(config_json) == 0 else _json_bytes_to_value(config_json, "config_json")},
            {200, 201},
        )
        return attachment_from_http(_expect_dict(response, "upsert attachment response"))

    async def delete_attachment(
        self,
        token: str,
        owner: UserRef,
        subject: UserRef,
        attachment_type: AttachmentType,
    ) -> Attachment:
        """删除用户附件关系。

        Args:
            token: 认证令牌。
            owner: 附件所有者用户引用。
            subject: 附件目标用户引用。
            attachment_type: 附件关系类型。

        Returns:
            被删除的附件关系。
        """
        validate_user_ref(owner, "owner")
        validate_user_ref(subject, "subject")
        response = await self._do_json(
            "DELETE",
            (
                f"/nodes/{owner.node_id}/users/{owner.user_id}/attachments/"
                f"{attachment_type.value}/{subject.node_id}/{subject.user_id}"
            ),
            token,
            None,
            {200},
        )
        return attachment_from_http(_expect_dict(response, "delete attachment response"))

    async def list_attachments(
        self,
        token: str,
        owner: UserRef,
        attachment_type: AttachmentType | None = None,
    ) -> list[Attachment]:
        """列出用户的所有附件关系。

        Args:
            token: 认证令牌。
            owner: 附件所有者用户引用。
            attachment_type: 可选的附件类型过滤，为 None 时返回所有类型。

        Returns:
            附件关系列表。
        """
        validate_user_ref(owner, "owner")
        query = f"?attachment_type={attachment_type.value}" if attachment_type is not None else ""
        response = await self._do_json(
            "GET",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/attachments{query}",
            token,
            None,
            {200},
        )
        items = _items_from_payload(response, "items")
        return [attachment_from_http(_expect_dict(item, "attachment item")) for item in items]

    async def list_events(self, token: str, after: int = 0, limit: int = 0) -> list[Event]:
        """列出领域事件。

        支持按序列号偏移和数量限制进行分页查询。

        Args:
            token: 认证令牌。
            after: 起始事件序列号（包含），0 表示从最早开始。
            limit: 返回事件的最大数量，0 表示使用服务器默认值。

        Returns:
            事件列表。
        """
        query_parts: list[str] = []
        if after != 0:
            query_parts.append(f"after={after}")
        if limit > 0:
            query_parts.append(f"limit={limit}")
        query = f"?{'&'.join(query_parts)}" if query_parts else ""
        response = await self._do_json("GET", f"/events{query}", token, None, {200})
        items = _items_from_payload(response, "items")
        return [event_from_http(_expect_dict(item, "event item")) for item in items]

    async def operations_status(self, token: str) -> OperationsStatus:
        """获取节点运行状态。

        包含消息窗口大小、事件序列号、写入门控状态、节点对等信息。

        Args:
            token: 认证令牌。

        Returns:
            节点的当前运行状态。
        """
        response = await self._do_json("GET", "/ops/status", token, None, {200})
        return operations_status_from_http(_expect_dict(response, "operations status response"))

    async def metrics(self, token: str) -> str:
        """获取服务器指标信息。

        Args:
            token: 认证令牌。

        Returns:
            指标信息的纯文本内容。
        """
        return await self._do_text("GET", "/metrics", token, {200})

    async def _do_json(
        self,
        method: str,
        path: str,
        token: str,
        body: dict[str, Any] | str | None,
        statuses: set[int],
    ) -> Any:
        """发送 HTTP 请求并解析 JSON 响应。

        Args:
            method: HTTP 方法（如 "GET"、"POST"）。
            path: 请求路径。
            token: 认证令牌。
            body: 请求体 JSON（字典或已序列化字符串，可选）。
            statuses: 期望的 HTTP 状态码集合。

        Returns:
            解析后的 JSON 数据，如果响应为空则返回 None。

        Raises:
            ProtocolError: 如果响应格式异常或状态码不在期望集合中。
            ConnectionError: 如果网络请求失败。
        """
        text = await self._request(method, path, token, body, statuses)
        if text.strip() == "":
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"invalid JSON response: {exc}") from exc

    async def _do_text(self, method: str, path: str, token: str, statuses: set[int]) -> str:
        """发送 HTTP 请求并返回纯文本响应。

        Args:
            method: HTTP 方法。
            path: 请求路径。
            token: 认证令牌。
            statuses: 期望的 HTTP 状态码集合。

        Returns:
            响应体的原始文本。
        """
        return await self._request(method, path, token, None, statuses)

    async def _request(
        self,
        method: str,
        path: str,
        token: str,
        body: dict[str, Any] | str | None,
        statuses: Iterable[int],
    ) -> str:
        """执行 HTTP 请求并返回响应文本。

        Args:
            method: HTTP 方法。
            path: 请求路径。
            token: 认证令牌，为空字符串时不添加 Authorization 头。
            body: 请求体 JSON（字典或已序列化字符串，可选）。
            statuses: 期望的 HTTP 状态码集合。

        Returns:
            响应体的原始文本。

        Raises:
            ConnectionError: 如果网络请求失败。
            ProtocolError: 如果响应状态码不在期望集合中。
        """
        headers: dict[str, str] = {}
        if token != "":
            headers["Authorization"] = f"Bearer {token}"
        request_kwargs: dict[str, Any] = {}
        if body is not None:
            headers["Content-Type"] = "application/json"
            if isinstance(body, str):
                request_kwargs["content"] = body.encode("utf-8")
            else:
                request_kwargs["json"] = body
        try:
            response = await self._client.request(
                method,
                self.base_url + path,
                headers=headers,
                **request_kwargs,
            )
        except httpx.HTTPError as exc:
            raise ConnectionError(f"{method} {path}", exc) from exc
        if response.status_code not in statuses:
            raise ProtocolError(f"unexpected HTTP status {response.status_code}: {response.text.strip()}")
        return response.text


def _user_metadata_request_body(
    request: UpsertUserMetadataRequest,
    raw_value: bytes,
) -> dict[str, Any] | str:
    if request.typed_value is None:
        body: dict[str, Any] = {"value": base64.b64encode(raw_value).decode("ascii")}
        if request.expires_at is not None:
            body["expires_at"] = request.expires_at
        return body

    parts = [f'"typed_value":{request.typed_value.to_http_json()}']
    if request.expires_at is not None:
        parts.append(
            '"expires_at":'
            + json.dumps(request.expires_at, ensure_ascii=False, separators=(",", ":"))
        )
    return "{" + ",".join(parts) + "}"


def _json_bytes_to_value(data: bytes, field: str) -> Any:
    """将 JSON 字节数据解析为 Python 对象。

    Args:
        data: JSON 格式的字节数据。
        field: 字段名称，用于错误消息。

    Returns:
        解析后的 Python 对象。

    Raises:
        ValueError: 如果数据不是有效的 JSON。
    """
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} must be valid JSON") from exc


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    """验证值是否为字典类型。

    Args:
        value: 要验证的值。
        label: 描述标签，用于错误消息。

    Returns:
        原始字典值。

    Raises:
        ProtocolError: 如果 value 不是字典类型。
    """
    if not isinstance(value, dict):
        raise ProtocolError(f"unexpected {label}")
    return value


def _items_from_payload(value: Any, *keys: str) -> list[Any]:
    """从响应数据中提取列表项。

    兼容两种格式：
    - 直接返回列表
    - 从字典中按指定键名提取列表

    Args:
        value: 响应数据。
        *keys: 要尝试提取列表的键名（按顺序尝试）。

    Returns:
        提取出的列表。

    Raises:
        ProtocolError: 如果无法从响应中提取列表。
    """
    if isinstance(value, list):
        return value
    if not isinstance(value, dict):
        raise ProtocolError("unexpected list response")
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, list):
            return candidate
    raise ProtocolError("missing items in list response")


def _attachment_payload(attachment: Attachment) -> dict[str, Any]:
    """将 Attachment 对象转换为适合 HTTP 请求的字典格式。

    Args:
        attachment: Attachment 对象。

    Returns:
        可用于 HTTP 请求的字典格式数据。
    """
    return {
        "owner": {"node_id": attachment.owner.node_id, "user_id": attachment.owner.user_id},
        "subject": {"node_id": attachment.subject.node_id, "user_id": attachment.subject.user_id},
        "attachment_type": attachment.attachment_type.value,
        "config_json": _json_bytes_to_value(attachment.config_json, "config_json")
        if attachment.config_json
        else {},
        "attached_at": attachment.attached_at,
        "deleted_at": attachment.deleted_at,
        "origin_node_id": attachment.origin_node_id,
    }
