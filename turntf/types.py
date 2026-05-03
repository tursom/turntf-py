from __future__ import annotations

import base64
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .password import PasswordInput

MetadataNumberValue = str | int | float
MetadataJSONScalar = None | bool | int | float | str
MetadataJSONValue = MetadataJSONScalar | list["MetadataJSONValue"] | dict[str, "MetadataJSONValue"]

USER_METADATA_KEY_VISIBLE_TO_OTHERS = "system.visible_to_others"


class DeliveryMode(str, Enum):
    """消息投递模式枚举。

    控制消息（Packet）在集群节点之间的投递行为：

    - ``UNSPECIFIED``: 未指定（默认值，通常用于占位）
    - ``BEST_EFFORT``: 尽最大努力投递，不保证重试
    - ``ROUTE_RETRY``: 路由重试模式，会尝试重新路由投递
    """
    UNSPECIFIED = ""
    BEST_EFFORT = "best_effort"
    ROUTE_RETRY = "route_retry"


class AttachmentType(str, Enum):
    """附件类型枚举。

    定义了用户之间关联关系的不同类型：

    - ``CHANNEL_MANAGER``: 频道管理员关系
    - ``CHANNEL_WRITER``: 频道写入者关系
    - ``CHANNEL_SUBSCRIPTION``: 频道订阅关系
    - ``USER_BLACKLIST``: 用户黑名单关系
    """
    CHANNEL_MANAGER = "channel_manager"
    CHANNEL_WRITER = "channel_writer"
    CHANNEL_SUBSCRIPTION = "channel_subscription"
    USER_BLACKLIST = "user_blacklist"


@dataclass(slots=True)
class MetadataTypedValue:
    """metadata 的 typed 视图。

    HTTP metadata 请求可用该结构表达 typed_value，请求体会按 ``kind`` 选择对应字段。
    响应侧当服务端或 SDK 能稳定解释原始 bytes 时，也会把 typed 视图填充到这里。

    Attributes:
        kind: 值类型，支持 ``bytes``、``bool``、``string``、``number``、``json``。
        bytes_value: ``kind == "bytes"`` 时使用的原始字节值。
        bool_value: ``kind == "bool"`` 时使用的布尔值。
        string_value: ``kind == "string"`` 时使用的字符串值。
        number_value: ``kind == "number"`` 时使用的 JSON number。
        json_value: ``kind == "json"`` 时使用的 JSON 值。
    """

    kind: str
    bytes_value: bytes | None = None
    bool_value: bool | None = None
    string_value: str | None = None
    number_value: MetadataNumberValue | None = None
    json_value: MetadataJSONValue | None = None

    @classmethod
    def of_bytes(cls, value: bytes) -> "MetadataTypedValue":
        return cls(kind="bytes", bytes_value=bytes(value))

    @classmethod
    def of_bool(cls, value: bool) -> "MetadataTypedValue":
        return cls(kind="bool", bool_value=value)

    @classmethod
    def of_string(cls, value: str) -> "MetadataTypedValue":
        return cls(kind="string", string_value=value)

    @classmethod
    def of_number(cls, value: MetadataNumberValue) -> "MetadataTypedValue":
        return cls(kind="number", number_value=value)

    @classmethod
    def of_json(cls, value: MetadataJSONValue) -> "MetadataTypedValue":
        return cls(kind="json", json_value=value)

    def normalized_kind(self) -> str:
        return self.kind.strip()

    def to_raw_value(self) -> bytes:
        """按 turntf metadata 语义把 typed 值编码为原始 bytes。"""
        kind = self.normalized_kind()
        if kind == "bytes":
            if self.bytes_value is None:
                raise ValueError("bytes_value is required")
            return bytes(self.bytes_value)
        if kind == "bool":
            if not isinstance(self.bool_value, bool):
                raise ValueError("bool_value is required")
            return b"true" if self.bool_value else b"false"
        if kind == "string":
            if self.string_value is None or not isinstance(self.string_value, str):
                raise ValueError("string_value is required")
            return json.dumps(self.string_value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if kind == "number":
            return _normalize_metadata_number_text(self.number_value).encode("utf-8")
        if kind == "json":
            return _compact_metadata_json_text(self.json_value).encode("utf-8")
        raise ValueError(f"unsupported kind {self.kind!r}")

    def to_http_json(self) -> str:
        """将 typed 值编码为 HTTP 请求体中的 ``typed_value`` JSON 片段。"""
        kind = self.normalized_kind()
        if kind == "bytes":
            if self.bytes_value is None:
                raise ValueError("bytes_value is required")
            payload = base64.b64encode(self.bytes_value).decode("ascii")
            return '{"kind":"bytes","bytes_value":' + json.dumps(payload) + "}"
        if kind == "bool":
            if not isinstance(self.bool_value, bool):
                raise ValueError("bool_value is required")
            return '{"kind":"bool","bool_value":' + json.dumps(self.bool_value) + "}"
        if kind == "string":
            if self.string_value is None or not isinstance(self.string_value, str):
                raise ValueError("string_value is required")
            return (
                '{"kind":"string","string_value":'
                + json.dumps(self.string_value, ensure_ascii=False, separators=(",", ":"))
                + "}"
            )
        if kind == "number":
            return (
                '{"kind":"number","number_value":'
                + _normalize_metadata_number_text(self.number_value)
                + "}"
            )
        if kind == "json":
            return (
                '{"kind":"json","json_value":'
                + _compact_metadata_json_text(self.json_value)
                + "}"
            )
        raise ValueError(f"unsupported kind {self.kind!r}")

    @classmethod
    def from_raw_value(cls, raw: bytes) -> "MetadataTypedValue | None":
        """从原始 metadata bytes 派生稳定的 typed 视图。"""
        try:
            text = raw.decode("utf-8")
        except UnicodeDecodeError:
            return None
        stripped = text.strip()
        if stripped == "":
            return None
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, bool):
            return cls.of_bool(parsed)
        if isinstance(parsed, str):
            return cls.of_string(parsed)
        if isinstance(parsed, (int, float)) and not isinstance(parsed, bool):
            return cls.of_number(stripped)
        return cls.of_json(parsed)


def _normalize_metadata_number_text(value: MetadataNumberValue | None) -> str:
    if value is None or isinstance(value, bool):
        raise ValueError("number_value is required")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        try:
            return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        except ValueError as exc:
            raise ValueError("number_value must be a JSON number") from exc
    if not isinstance(value, str):
        raise ValueError("number_value must be a string, int, or float")
    stripped = value.strip()
    if stripped == "":
        raise ValueError("number_value is required")
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        raise ValueError("number_value must be a JSON number") from exc
    if isinstance(parsed, bool) or not isinstance(parsed, (int, float)):
        raise ValueError("number_value must be a JSON number")
    return stripped


def _compact_metadata_json_text(value: MetadataJSONValue | None) -> str:
    if value is None:
        raise ValueError("json_value is required")
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValueError("json_value must be valid JSON") from exc


@dataclass(slots=True, frozen=True, init=False)
class Credentials:
    """用户登录凭据。

    包含用于连接 turntf 服务器的身份验证信息。
    可以通过 (node_id, user_id) 或 login_name 两种方式标识用户。

    Attributes:
        node_id: 用户所在的节点 ID（使用 ID 登录方式时为 0）。
        user_id: 用户 ID（使用 ID 登录方式时为 0）。
        password: 经过包装的密码输入对象。
        login_name: 登录名（使用 login_name 登录方式时设置）。
    """
    node_id: int
    user_id: int
    password: PasswordInput
    login_name: str

    def __init__(
        self,
        node_id: int = 0,
        user_id: int = 0,
        password: PasswordInput | None = None,
        login_name: str = "",
    ) -> None:
        """初始化 Credentials。

        Args:
            node_id: 节点 ID（可选）。
            user_id: 用户 ID（可选）。
            password: 密码输入对象，不能为 None。
            login_name: 登录名（可选，替代 node_id/user_id 方式）。

        Raises:
            TypeError: 如果 password 为 None。
        """
        if password is None:
            raise TypeError("password is required")
        object.__setattr__(self, "node_id", node_id)
        object.__setattr__(self, "user_id", user_id)
        object.__setattr__(self, "password", password)
        object.__setattr__(self, "login_name", login_name.strip())


@dataclass(slots=True, frozen=True)
class UserRef:
    """用户引用标识。

    通过 node_id 和 user_id 的组合唯一标识 turntf 集群中的一个用户。

    Attributes:
        node_id: 用户所属节点的 ID。
        user_id: 用户在节点内的 ID。
    """
    node_id: int
    user_id: int


@dataclass(slots=True, frozen=True)
class SessionRef:
    """会话引用标识。

    唯一标识一个用户会话，用于向特定会话发送消息或查询会话状态。

    Attributes:
        serving_node_id: 提供服务的节点 ID。
        session_id: 会话的唯一标识字符串。
    """
    serving_node_id: int
    session_id: str


@dataclass(slots=True, frozen=True)
class MessageCursor:
    """消息游标，用于定位消息在消息序列中的位置。

    由节点 ID 和序列号组成，可用于消息的定位、
    去重和断点续传。

    Attributes:
        node_id: 消息所在的节点 ID。
        seq: 消息在该节点上的序列号。
    """
    node_id: int
    seq: int


@dataclass(slots=True)
class User:
    """用户信息。

    表示 turntf 平台中的一个用户或频道（channel）。

    Attributes:
        node_id: 用户所属节点 ID。
        user_id: 用户 ID。
        username: 用户名。
        role: 用户角色，如 "user"、"channel" 等。
        profile_json: 用户资料的 JSON 数据（字节形式）。
        system_reserved: 是否为系统保留用户。
        created_at: 创建时间（HLC 时间戳字符串）。
        updated_at: 最后更新时间（HLC 时间戳字符串）。
        origin_node_id: 创建该用户的原始节点 ID。
        login_name: 登录名（可选）。
    """
    node_id: int
    user_id: int
    username: str
    role: str
    profile_json: bytes
    system_reserved: bool
    created_at: str
    updated_at: str
    origin_node_id: int
    login_name: str = ""


@dataclass(slots=True)
class Message:
    """持久化消息。

    表示一条已持久化存储的消息，包含完整的收发双方信息和消息体。

    Attributes:
        recipient: 消息接收者的用户引用。
        node_id: 消息所在的节点 ID。
        seq: 消息在节点上的序列号。
        sender: 消息发送者的用户引用。
        body: 消息体（字节数据）。
        created_at_hlc: 创建时间的混合逻辑时钟（HLC）时间戳。
    """
    recipient: UserRef
    node_id: int
    seq: int
    sender: UserRef
    body: bytes
    created_at_hlc: str

    def cursor(self) -> MessageCursor:
        """获取此消息对应的游标。

        Returns:
            包含相同 node_id 和 seq 的 MessageCursor 对象。
        """
        return MessageCursor(node_id=self.node_id, seq=self.seq)


@dataclass(slots=True)
class Packet:
    """瞬时数据包（非持久化消息）。

    表示一个不会被持久化存储的瞬时消息，
    通过 ``send_packet`` 发送，适用于实时通信场景。
    如果目标用户离线，消息可能会丢失。

    Attributes:
        packet_id: 数据包 ID。
        source_node_id: 源节点 ID。
        target_node_id: 目标节点 ID。
        recipient: 接收者用户引用。
        sender: 发送者用户引用。
        body: 数据包体（字节数据）。
        delivery_mode: 投递模式。
        target_session: 目标会话引用（可选），指定后只投递到该会话。
    """
    packet_id: int
    source_node_id: int
    target_node_id: int
    recipient: UserRef
    sender: UserRef
    body: bytes
    delivery_mode: DeliveryMode
    target_session: SessionRef | None = None


@dataclass(slots=True)
class RelayAccepted:
    """数据包中继确认。

    当发送的瞬时数据包被服务器接受并开始中继时返回的确认信息。

    Attributes:
        packet_id: 被接受的数据包 ID。
        source_node_id: 源节点 ID。
        target_node_id: 目标节点 ID。
        recipient: 接收者用户引用。
        delivery_mode: 投递模式。
        target_session: 目标会话引用（可选）。
    """
    packet_id: int
    source_node_id: int
    target_node_id: int
    recipient: UserRef
    delivery_mode: DeliveryMode
    target_session: SessionRef | None = None


@dataclass(slots=True)
class Attachment:
    """用户附件关系。

    表示两个用户之间的一种关联关系（如订阅频道、黑名单等）。
    通过 attachment_type 区分不同的关系类型。

    Attributes:
        owner: 附件所有者用户引用。
        subject: 附件目标用户引用。
        attachment_type: 附件关系类型。
        config_json: 配置信息的 JSON 数据（字节形式）。
        attached_at: 创建时间（HLC 时间戳字符串）。
        deleted_at: 删除时间（HLC 时间戳字符串），空字符串表示未删除。
        origin_node_id: 创建此附件关系的原始节点 ID。
    """
    owner: UserRef
    subject: UserRef
    attachment_type: AttachmentType
    config_json: bytes
    attached_at: str
    deleted_at: str
    origin_node_id: int


@dataclass(slots=True)
class UserMetadata:
    """用户元数据键值对。

    用户可附加自定义的元数据，以键值对形式存储。
    支持设置过期时间，过期后元数据自动失效。

    Attributes:
        owner: 元数据所有者的用户引用。
        key: 元数据键名。
        value: 元数据值（字节数据）。
        updated_at: 最后更新时间（HLC 时间戳字符串）。
        deleted_at: 删除时间（HLC 时间戳字符串），空字符串表示未删除。
        expires_at: 过期时间，空字符串表示永不过期。
        origin_node_id: 创建此元数据的原始节点 ID。
        typed_value: 根据原始 bytes 派生出的稳定 typed 视图；无法稳定解释时为 None。
    """
    owner: UserRef
    key: str
    value: bytes
    updated_at: str
    deleted_at: str
    expires_at: str
    origin_node_id: int
    typed_value: MetadataTypedValue | None = None


@dataclass(slots=True)
class UserMetadataScanResult:
    """用户元数据扫描结果。

    用于分页扫描用户元数据的结果集。

    Attributes:
        items: 扫描到的元数据项列表。
        count: 结果总数（可能与 items 数量不同）。
        next_after: 下一页游标值，为空字符串表示没有更多数据。
    """
    items: list[UserMetadata] = field(default_factory=list)
    count: int = 0
    next_after: str = ""


@dataclass(slots=True)
class Subscription:
    """频道订阅关系。

    表示一个用户（订阅者）订阅了另一个用户（频道）的关系。

    Attributes:
        subscriber: 订阅者的用户引用。
        channel: 被订阅的频道用户引用。
        subscribed_at: 订阅时间（HLC 时间戳字符串）。
        deleted_at: 取消订阅时间，空字符串表示未取消。
        origin_node_id: 创建此订阅的原始节点 ID。
    """
    subscriber: UserRef
    channel: UserRef
    subscribed_at: str
    deleted_at: str
    origin_node_id: int


@dataclass(slots=True)
class BlacklistEntry:
    """黑名单条目。

    表示一个用户将另一个用户加入黑名单的关系。

    Attributes:
        owner: 黑名单所有者的用户引用。
        blocked: 被拉黑的用户引用。
        blocked_at: 拉黑时间（HLC 时间戳字符串）。
        deleted_at: 移出黑名单时间，空字符串表示仍在黑名单中。
        origin_node_id: 创建此黑名单条目的原始节点 ID。
    """
    owner: UserRef
    blocked: UserRef
    blocked_at: str
    deleted_at: str
    origin_node_id: int


@dataclass(slots=True)
class Event:
    """领域事件。

    表示系统中发生的一个领域事件，用于事件溯源和审计日志。

    Attributes:
        sequence: 事件序列号（全局递增）。
        event_id: 事件 ID。
        event_type: 事件类型标识。
        aggregate: 聚合类型名称。
        aggregate_node_id: 聚合所属节点 ID。
        aggregate_id: 聚合 ID。
        hlc: 事件发生的 HLC 时间戳。
        origin_node_id: 产生此事件的原始节点 ID。
        event_json: 事件数据的 JSON 字节表示。
    """
    sequence: int
    event_id: int
    event_type: str
    aggregate: str
    aggregate_node_id: int
    aggregate_id: int
    hlc: str
    origin_node_id: int
    event_json: bytes


@dataclass(slots=True)
class ClusterNode:
    """集群节点信息。

    表示 turntf 集群中的一个节点。

    Attributes:
        node_id: 节点 ID。
        is_local: 是否为当前客户端连接的本节点。
        configured_url: 节点配置的 URL 地址。
        source: 节点信息来源（如 "config"、"discovery" 等）。
    """
    node_id: int
    is_local: bool
    configured_url: str
    source: str


@dataclass(slots=True)
class LoggedInUser:
    """已登录用户信息。

    表示某个节点上当前登录的用户信息。

    Attributes:
        node_id: 用户登录的节点 ID。
        user_id: 用户 ID。
        username: 用户名。
        login_name: 登录名（可选）。
    """
    node_id: int
    user_id: int
    username: str
    login_name: str = ""


@dataclass(slots=True)
class OnlineNodePresence:
    """节点在线状态。

    表示用户在某个节点上的在线情况摘要。

    Attributes:
        serving_node_id: 服务节点 ID。
        session_count: 该节点上的会话数量。
        transport_hint: 传输方式提示（如 "ws"、"wss" 等）。
    """
    serving_node_id: int
    session_count: int
    transport_hint: str


@dataclass(slots=True)
class ResolvedSession:
    """已解析的会话信息。

    包含会话引用及其传输能力和特性。

    Attributes:
        session: 会话引用。
        transport: 传输方式。
        transient_capable: 是否支持瞬时消息投递。
    """
    session: SessionRef
    transport: str
    transient_capable: bool


@dataclass(slots=True)
class ResolvedUserSessions:
    """用户所有会话的解析结果。

    包含用户的在线节点分布情况和具体的会话列表。

    Attributes:
        user: 用户引用。
        presence: 各节点的在线状态列表。
        sessions: 已解析的会话列表。
    """
    user: UserRef
    presence: list[OnlineNodePresence] = field(default_factory=list)
    sessions: list[ResolvedSession] = field(default_factory=list)

    @property
    def count(self) -> int:
        """获取会话总数。

        Returns:
            会话数量。
        """
        return len(self.sessions)


@dataclass(slots=True)
class MessageTrimStatus:
    """消息修剪状态。

    表示消息窗口的清理状态信息。

    Attributes:
        trimmed_total: 已修剪（清理）的消息总数。
        last_trimmed_at: 最后一次修剪的时间（HLC 时间戳字符串）。
    """
    trimmed_total: int
    last_trimmed_at: str


@dataclass(slots=True)
class EventLogTrimStatus:
    """事件日志修剪状态。

    表示事件日志的清理状态信息。

    Attributes:
        trimmed_total: 已修剪的事件总数。
        last_trimmed_at: 最后一次修剪的时间（HLC 时间戳字符串）。
    """
    trimmed_total: int
    last_trimmed_at: str


@dataclass(slots=True)
class ProjectionStatus:
    """投影处理状态。

    表示事件投影的处理进度信息。

    Attributes:
        pending_total: 待处理的事件数量。
        last_failed_at: 最后一次处理失败的时间（HLC 时间戳字符串）。
    """
    pending_total: int
    last_failed_at: str


@dataclass(slots=True)
class PeerOriginStatus:
    """对等节点来源状态。

    表示集群中对等节点在某个来源上的同步状态。

    Attributes:
        origin_node_id: 来源节点 ID。
        acked_event_id: 已确认的事件 ID。
        applied_event_id: 已应用的事件 ID。
        unconfirmed_events: 未确认的事件数量。
        cursor_updated_at: 游标更新时间。
        remote_last_event_id: 远程节点的最新事件 ID。
        pending_catchup: 是否正在追赶同步。
    """
    origin_node_id: int
    acked_event_id: int
    applied_event_id: int
    unconfirmed_events: int
    cursor_updated_at: str
    remote_last_event_id: int
    pending_catchup: bool


@dataclass(slots=True)
class PeerStatus:
    """对等节点状态。

    表示集群中对等节点的详细连接和同步状态。

    Attributes:
        node_id: 节点 ID。
        configured_url: 配置的 URL。
        source: 节点信息来源。
        discovered_url: 自动发现的 URL。
        discovery_state: 发现状态。
        last_discovered_at: 最后发现时间。
        last_connected_at: 最后连接时间。
        last_discovery_error: 最后的发现错误信息。
        connected: 是否已连接。
        session_direction: 会话方向（"inbound"/"outbound"）。
        origins: 各来源的同步状态列表。
        pending_snapshot_partitions: 待处理的快照分区数。
        remote_snapshot_version: 远程快照版本。
        remote_message_window_size: 远程消息窗口大小。
        clock_offset_ms: 时钟偏移（毫秒）。
        last_clock_sync: 最后时钟同步时间。
        snapshot_digests_sent_total: 已发送的快照摘要总数。
        snapshot_digests_received_total: 已接收的快照摘要总数。
        snapshot_chunks_sent_total: 已发送的快照块总数。
        snapshot_chunks_received_total: 已接收的快照块总数。
        last_snapshot_digest_at: 最后发送快照摘要的时间。
        last_snapshot_chunk_at: 最后发送快照块的时间。
    """
    node_id: int
    configured_url: str
    source: str
    discovered_url: str
    discovery_state: str
    last_discovered_at: str
    last_connected_at: str
    last_discovery_error: str
    connected: bool
    session_direction: str
    origins: list[PeerOriginStatus] = field(default_factory=list)
    pending_snapshot_partitions: int = 0
    remote_snapshot_version: str = ""
    remote_message_window_size: int = 0
    clock_offset_ms: int = 0
    last_clock_sync: str = ""
    snapshot_digests_sent_total: int = 0
    snapshot_digests_received_total: int = 0
    snapshot_chunks_sent_total: int = 0
    snapshot_chunks_received_total: int = 0
    last_snapshot_digest_at: str = ""
    last_snapshot_chunk_at: str = ""


@dataclass(slots=True)
class OperationsStatus:
    """运行状态信息。

    提供节点当前的运行状况，包括消息窗口、事件序列、写入门控等指标。

    Attributes:
        node_id: 节点 ID。
        message_window_size: 消息窗口大小。
        last_event_sequence: 最后事件序列号。
        write_gate_ready: 写入门控是否就绪。
        conflict_total: 冲突总数。
        message_trim: 消息修剪状态。
        projection: 投影处理状态。
        peers: 对等节点状态列表。
        event_log_trim: 事件日志修剪状态（可选）。
    """
    node_id: int
    message_window_size: int
    last_event_sequence: int
    write_gate_ready: bool
    conflict_total: int
    message_trim: MessageTrimStatus
    projection: ProjectionStatus
    peers: list[PeerStatus] = field(default_factory=list)
    event_log_trim: EventLogTrimStatus | None = None


@dataclass(slots=True)
class DeleteUserResult:
    """删除用户操作的结果。

    Attributes:
        status: 删除操作的状态描述。
        user: 被删除用户的引用。
    """
    status: str
    user: UserRef


@dataclass(slots=True)
class LoginInfo:
    """登录成功后的信息。

    包含登录成功后返回的用户信息、协议版本和会话引用。

    Attributes:
        user: 登录用户的信息。
        protocol_version: 协商的协议版本。
        session_ref: 当前会话的引用。
    """
    user: User
    protocol_version: str
    session_ref: SessionRef


@dataclass(slots=True)
class CreateUserRequest:
    """创建用户请求。

    用于创建新用户或新频道的请求参数。

    Attributes:
        username: 用户名，不能为空。
        password: 密码输入（可选），不为 None 时设置初始密码。
        profile_json: 用户资料的 JSON 数据（字节形式，可选）。
        role: 用户角色，如 "user"、"channel" 等（可选）。
        login_name: 登录名（可选）。
    """
    username: str
    password: PasswordInput | None = None
    profile_json: bytes = b""
    role: str = ""
    login_name: str = ""


@dataclass(slots=True)
class UpdateUserRequest:
    """更新用户请求。

    用于更新已有用户信息的请求参数。
    所有字段均为可选，只更新被设置的字段。

    Attributes:
        username: 新用户名（可选）。
        password: 新密码输入（可选）。
        profile_json: 新的用户资料 JSON 数据（字节形式，可选）。
        role: 新角色（可选）。
        login_name: 新登录名（可选）。
    """
    username: str | None = None
    password: PasswordInput | None = None
    profile_json: bytes | None = None
    role: str | None = None
    login_name: str | None = None


@dataclass(slots=True)
class ListUsersRequest:
    """查询当前用户可通讯列表的过滤参数。

    供 HTTP `GET /users` 和 WebSocket `list_users` RPC 共用。

    Attributes:
        name: 名称过滤，大小写不敏感子串匹配。空字符串表示不过滤。
        uid: 可选的精确用户过滤。`None` 表示不过滤；
             `UserRef(0, 0)` 也会被视为“不按 uid 过滤”的兼容写法。
    """

    name: str = ""
    uid: UserRef | None = None


@dataclass(slots=True)
class UpsertUserMetadataRequest:
    """创建或更新用户元数据请求。

    用于写入或更新用户元数据的请求参数。

    Attributes:
        value: 原始 metadata bytes。与 ``typed_value`` 二选一。
        expires_at: 过期时间（可选），为空表示永不过期。
        typed_value: HTTP typed_value 视图。与 ``value`` 二选一；
                     WebSocket/protobuf 客户端会在本地先把它编码成 raw bytes，再走原有 wire 结构。
    """
    value: bytes | None = None
    expires_at: str | None = None
    typed_value: MetadataTypedValue | None = None


@dataclass(slots=True)
class ScanUserMetadataRequest:
    """扫描用户元数据请求。

    用于分页扫描用户元数据的查询参数。

    Attributes:
        prefix: 键名前缀过滤（可选），只返回匹配该前缀的元数据。
        after: 分页游标，返回该游标之后的元数据（可选）。
        limit: 返回结果的最大数量，0 表示使用服务器默认值（最大 1000）。
    """
    prefix: str = ""
    after: str = ""
    limit: int = 0


class Reliability(Enum):
    """Relay 连接可靠性等级。

    控制 RelayConnection 的数据传输保证级别。

    - ``BEST_EFFORT``: 无 ACK，无重传，无去重，无排序。延迟最低，适合实时音视频帧。
    - ``AT_LEAST_ONCE``: ACK + 重传，不保证去重和排序。适合幂等指令。
    - ``RELIABLE_ORDERED``: ACK + 重传 + 去重 + 严格有序。适合文件传输和聊天消息。
    """
    BEST_EFFORT = 0
    AT_LEAST_ONCE = 1
    RELIABLE_ORDERED = 2


class RelayState(Enum):
    """RelayConnection 的当前状态。

    - ``CLOSED``: 初始状态或已关闭。
    - ``OPENING``: 已发送 OPEN，等待 OPEN_ACK。
    - ``OPEN``: 连接已建立，可收发数据。
    - ``CLOSING``: 已发送 CLOSE，等待确认。
    """
    CLOSED = 0
    OPENING = 1
    OPEN = 2
    CLOSING = 3


class RelayKind(Enum):
    """Relay 协议帧的类型枚举，对应 proto RelayKind。

    - ``UNSPECIFIED``: 未指定。
    - ``OPEN``: 打开连接请求。
    - ``OPEN_ACK``: 打开连接确认。
    - ``DATA``: 数据帧。
    - ``ACK``: 确认帧。
    - ``CLOSE``: 关闭连接。
    - ``PING``: 心跳探测。
    - ``ERROR``: 错误通知。
    """
    UNSPECIFIED = 0
    OPEN = 1
    OPEN_ACK = 2
    DATA = 3
    ACK = 4
    CLOSE = 5
    PING = 6
    ERROR = 7


@dataclass(slots=True)
class RelayConfig:
    """RelayConnection 的配置。

    Attributes:
        reliability: 可靠性等级，默认 ``Reliability.RELIABLE_ORDERED``。
        window_size: 发送窗口大小（在途未确认帧数上限），范围 1-256，默认 16。
                     BestEffort 模式下忽略此配置。
        open_timeout_ms: OPEN 等待 OPEN_ACK 超时毫秒数，默认 10000。
        close_timeout_ms: CLOSE 等待确认超时毫秒数，默认 5000。
        ack_timeout_ms: DATA 等待 ACK 超时毫秒数，默认 3000。
                        BestEffort 模式下忽略此配置。
        max_retransmits: 最大重传次数，默认 5。BestEffort 模式下忽略此配置。
        idle_timeout_ms: 无数据超时断开毫秒数，0 表示不超时。
        send_buffer_size: 发送缓冲区字节数，默认 65536。
        send_timeout_ms: Send 操作超时毫秒数（缓冲区满时等待上限），0 表示不超时。
        receive_timeout_ms: Receive 操作超时毫秒数（无数据等待上限），0 表示不超时。
        delivery_mode: Packet 投递模式，默认 ``DeliveryMode.ROUTE_RETRY``。
    """
    reliability: Reliability = Reliability.RELIABLE_ORDERED
    window_size: int = 16
    open_timeout_ms: int = 10000
    close_timeout_ms: int = 5000
    ack_timeout_ms: int = 3000
    max_retransmits: int = 5
    idle_timeout_ms: int = 0
    send_buffer_size: int = 65536
    send_timeout_ms: int = 0
    receive_timeout_ms: int = 0
    delivery_mode: DeliveryMode = DeliveryMode.ROUTE_RETRY


@dataclass(slots=True)
class RelayEnvelope:
    """Relay 协议的帧类型，与 proto RelayEnvelope 对应。

    Attributes:
        relay_id: 连接的唯一标识。
        kind: 帧类型。
        sender_session: 发送方会话引用。
        target_session: 目标方会话引用。
        seq: 序列号（用于可靠传输）。
        ack_seq: 确认序列号（累积 ACK）。
        payload: 帧负载数据。
        sent_at_ms: 发送时间戳（毫秒）。
    """
    relay_id: str = ""
    kind: RelayKind = RelayKind.UNSPECIFIED
    sender_session: SessionRef = field(default_factory=lambda: SessionRef(serving_node_id=0, session_id=""))
    target_session: SessionRef = field(default_factory=lambda: SessionRef(serving_node_id=0, session_id=""))
    seq: int = 0
    ack_seq: int = 0
    payload: bytes = b""
    sent_at_ms: int = 0
