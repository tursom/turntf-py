from __future__ import annotations

import base64
import json
from typing import Any

from ._generated import client_pb2 as pb
from .errors import ProtocolError
from .types import (
    Attachment,
    AttachmentType,
    BlacklistEntry,
    ClusterNode,
    DeliveryMode,
    DeleteUserResult,
    Event,
    EventLogTrimStatus,
    LoggedInUser,
    MetadataTypedValue,
    Message,
    MessageCursor,
    MessageTrimStatus,
    OnlineNodePresence,
    OperationsStatus,
    Packet,
    PeerOriginStatus,
    PeerStatus,
    ProjectionStatus,
    RelayAccepted,
    RelayEnvelope,
    RelayKind,
    ResolvedSession,
    ResolvedUserSessions,
    SessionRef,
    Subscription,
    User,
    UserMetadata,
    UserMetadataScanResult,
    UserRef,
)

_MISSING = object()


def delivery_mode_to_proto(mode: DeliveryMode) -> int:
    """将 DeliveryMode 枚举转换为 protobuf 枚举值。

    Args:
        mode: DeliveryMode 枚举值。

    Returns:
        对应的 protobuf 整型枚举值。
    """
    if mode == DeliveryMode.BEST_EFFORT:
        return pb.CLIENT_DELIVERY_MODE_BEST_EFFORT
    if mode == DeliveryMode.ROUTE_RETRY:
        return pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY
    return pb.CLIENT_DELIVERY_MODE_UNSPECIFIED


def delivery_mode_from_proto(mode: int) -> DeliveryMode:
    """将 protobuf 枚举值转换为 DeliveryMode 枚举。

    Args:
        mode: protobuf 传递模式整型值。

    Returns:
        对应的 DeliveryMode 枚举值。
    """
    if mode == pb.CLIENT_DELIVERY_MODE_BEST_EFFORT:
        return DeliveryMode.BEST_EFFORT
    if mode == pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY:
        return DeliveryMode.ROUTE_RETRY
    return DeliveryMode.UNSPECIFIED


def attachment_type_to_proto(attachment_type: AttachmentType) -> int:
    """将 AttachmentType 枚举转换为 protobuf 枚举值。

    Args:
        attachment_type: AttachmentType 枚举值。

    Returns:
        对应的 protobuf 整型枚举值。
    """
    if attachment_type == AttachmentType.CHANNEL_MANAGER:
        return pb.ATTACHMENT_TYPE_CHANNEL_MANAGER
    if attachment_type == AttachmentType.CHANNEL_WRITER:
        return pb.ATTACHMENT_TYPE_CHANNEL_WRITER
    if attachment_type == AttachmentType.CHANNEL_SUBSCRIPTION:
        return pb.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION
    if attachment_type == AttachmentType.USER_BLACKLIST:
        return pb.ATTACHMENT_TYPE_USER_BLACKLIST
    return pb.ATTACHMENT_TYPE_UNSPECIFIED


def attachment_type_from_proto(attachment_type: int) -> AttachmentType:
    """将 protobuf 枚举值转换为 AttachmentType 枚举。

    Args:
        attachment_type: protobuf 附件类型整型值。

    Returns:
        对应的 AttachmentType 枚举值。

    Raises:
        ProtocolError: 如果不支持的附件类型值。
    """
    if attachment_type == pb.ATTACHMENT_TYPE_CHANNEL_MANAGER:
        return AttachmentType.CHANNEL_MANAGER
    if attachment_type == pb.ATTACHMENT_TYPE_CHANNEL_WRITER:
        return AttachmentType.CHANNEL_WRITER
    if attachment_type == pb.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION:
        return AttachmentType.CHANNEL_SUBSCRIPTION
    if attachment_type == pb.ATTACHMENT_TYPE_USER_BLACKLIST:
        return AttachmentType.USER_BLACKLIST
    raise ProtocolError(f"unsupported attachment type {attachment_type}")


def user_ref_to_proto(ref: UserRef) -> pb.UserRef:
    """将 UserRef 对象转换为 protobuf 消息。

    Args:
        ref: UserRef 对象。

    Returns:
        protobuf UserRef 消息。
    """
    return pb.UserRef(node_id=ref.node_id, user_id=ref.user_id)


def session_ref_to_proto(ref: SessionRef) -> pb.SessionRef:
    """将 SessionRef 对象转换为 protobuf 消息。

    Args:
        ref: SessionRef 对象。

    Returns:
        protobuf SessionRef 消息。
    """
    return pb.SessionRef(serving_node_id=ref.serving_node_id, session_id=ref.session_id)


def cursor_to_proto(cursor: MessageCursor) -> pb.MessageCursor:
    """将 MessageCursor 对象转换为 protobuf 消息。

    Args:
        cursor: MessageCursor 对象。

    Returns:
        protobuf MessageCursor 消息。
    """
    return pb.MessageCursor(node_id=cursor.node_id, seq=cursor.seq)


def cursor_from_proto(cursor: pb.MessageCursor | None) -> MessageCursor:
    """将 protobuf 消息转换为 MessageCursor 对象。

    Args:
        cursor: protobuf MessageCursor 消息，可能为 None。

    Returns:
        MessageCursor 对象，如果输入为 None 则返回空游标 (0, 0)。
    """
    if cursor is None:
        return MessageCursor(node_id=0, seq=0)
    return MessageCursor(node_id=cursor.node_id, seq=cursor.seq)


def user_ref_from_proto(ref: pb.UserRef | None) -> UserRef:
    """将 protobuf 消息转换为 UserRef 对象。

    Args:
        ref: protobuf UserRef 消息，可能为 None。

    Returns:
        UserRef 对象，如果输入为 None 则返回 (0, 0)。
    """
    if ref is None:
        return UserRef(node_id=0, user_id=0)
    return UserRef(node_id=ref.node_id, user_id=ref.user_id)


def session_ref_from_proto(ref: pb.SessionRef | None) -> SessionRef:
    """将 protobuf 消息转换为 SessionRef 对象。

    Args:
        ref: protobuf SessionRef 消息，可能为 None。

    Returns:
        SessionRef 对象。

    Raises:
        ProtocolError: 如果 ref 为 None 或内容无效。
    """
    if ref is None:
        raise ProtocolError("missing session_ref")
    if ref.serving_node_id <= 0 or ref.session_id.strip() == "":
        raise ProtocolError("invalid session_ref")
    return SessionRef(serving_node_id=ref.serving_node_id, session_id=ref.session_id)


def user_from_proto(user: pb.User | None) -> User:
    """将 protobuf 消息转换为 User 对象。

    Args:
        user: protobuf User 消息，可能为 None。

    Returns:
        User 对象。

    Raises:
        ProtocolError: 如果 user 为 None。
    """
    if user is None:
        raise ProtocolError("missing user")
    return User(
        node_id=user.node_id,
        user_id=user.user_id,
        username=user.username,
        role=user.role,
        profile_json=bytes(user.profile_json),
        system_reserved=user.system_reserved,
        created_at=user.created_at,
        updated_at=user.updated_at,
        origin_node_id=user.origin_node_id,
        login_name=user.login_name,
    )


def user_metadata_from_proto(metadata: pb.UserMetadata | None) -> UserMetadata:
    """将 protobuf 消息转换为 UserMetadata 对象。

    Args:
        metadata: protobuf UserMetadata 消息，可能为 None。

    Returns:
        UserMetadata 对象。

    Raises:
        ProtocolError: 如果 metadata 为 None。
    """
    if metadata is None:
        raise ProtocolError("missing user_metadata")
    value = bytes(metadata.value)
    return UserMetadata(
        owner=user_ref_from_proto(metadata.owner),
        key=metadata.key,
        value=value,
        updated_at=metadata.updated_at,
        deleted_at=metadata.deleted_at,
        expires_at=metadata.expires_at,
        origin_node_id=metadata.origin_node_id,
        typed_value=MetadataTypedValue.from_raw_value(value),
    )


def message_from_proto(message: pb.Message | None) -> Message:
    """将 protobuf 消息转换为 Message 对象。

    Args:
        message: protobuf Message 消息，可能为 None。

    Returns:
        Message 对象。

    Raises:
        ProtocolError: 如果 message 为 None。
    """
    if message is None:
        raise ProtocolError("missing message")
    return Message(
        recipient=user_ref_from_proto(message.recipient),
        node_id=message.node_id,
        seq=message.seq,
        sender=user_ref_from_proto(message.sender),
        body=bytes(message.body),
        created_at_hlc=message.created_at_hlc,
    )


def packet_from_proto(packet: pb.Packet | None) -> Packet:
    """将 protobuf 消息转换为 Packet 对象。

    Args:
        packet: protobuf Packet 消息，可能为 None。

    Returns:
        Packet 对象。

    Raises:
        ProtocolError: 如果 packet 为 None。
    """
    if packet is None:
        raise ProtocolError("missing packet")
    return Packet(
        packet_id=packet.packet_id,
        source_node_id=packet.source_node_id,
        target_node_id=packet.target_node_id,
        recipient=user_ref_from_proto(packet.recipient),
        sender=user_ref_from_proto(packet.sender),
        body=bytes(packet.body),
        delivery_mode=delivery_mode_from_proto(packet.delivery_mode),
        target_session=session_ref_from_proto(packet.target_session)
        if packet.HasField("target_session")
        else None,
    )


def relay_accepted_from_proto(accepted: pb.TransientAccepted | None) -> RelayAccepted:
    """将 protobuf 消息转换为 RelayAccepted 对象。

    Args:
        accepted: protobuf TransientAccepted 消息，可能为 None。

    Returns:
        RelayAccepted 对象。

    Raises:
        ProtocolError: 如果 accepted 为 None。
    """
    if accepted is None:
        raise ProtocolError("missing transient_accepted")
    return RelayAccepted(
        packet_id=accepted.packet_id,
        source_node_id=accepted.source_node_id,
        target_node_id=accepted.target_node_id,
        recipient=user_ref_from_proto(accepted.recipient),
        delivery_mode=delivery_mode_from_proto(accepted.delivery_mode),
        target_session=session_ref_from_proto(accepted.target_session)
        if accepted.HasField("target_session")
        else None,
    )


def attachment_from_proto(attachment: pb.Attachment | None) -> Attachment:
    """将 protobuf 消息转换为 Attachment 对象。

    Args:
        attachment: protobuf Attachment 消息，可能为 None。

    Returns:
        Attachment 对象。

    Raises:
        ProtocolError: 如果 attachment 为 None。
    """
    if attachment is None:
        raise ProtocolError("missing attachment")
    return Attachment(
        owner=user_ref_from_proto(attachment.owner),
        subject=user_ref_from_proto(attachment.subject),
        attachment_type=attachment_type_from_proto(attachment.attachment_type),
        config_json=bytes(attachment.config_json),
        attached_at=attachment.attached_at,
        deleted_at=attachment.deleted_at,
        origin_node_id=attachment.origin_node_id,
    )


def subscription_from_attachment(attachment: Attachment) -> Subscription:
    """从 Attachment 对象创建 Subscription 对象。

    Args:
        attachment: Attachment 对象（必须是频道订阅类型的附件）。

    Returns:
        从 Attachment 转换得到的 Subscription 对象。
    """
    return Subscription(
        subscriber=attachment.owner,
        channel=attachment.subject,
        subscribed_at=attachment.attached_at,
        deleted_at=attachment.deleted_at,
        origin_node_id=attachment.origin_node_id,
    )


def blacklist_entry_from_attachment(attachment: Attachment) -> BlacklistEntry:
    """从 Attachment 对象创建 BlacklistEntry 对象。

    Args:
        attachment: Attachment 对象（必须是黑名单类型的附件）。

    Returns:
        从 Attachment 转换得到的 BlacklistEntry 对象。
    """
    return BlacklistEntry(
        owner=attachment.owner,
        blocked=attachment.subject,
        blocked_at=attachment.attached_at,
        deleted_at=attachment.deleted_at,
        origin_node_id=attachment.origin_node_id,
    )


def subscription_from_proto(subscription: pb.Attachment | None) -> Subscription:
    """从 protobuf Attachment 消息创建 Subscription 对象。

    Args:
        subscription: protobuf Attachment 消息，可能为 None。

    Returns:
        Subscription 对象。
    """
    return subscription_from_attachment(attachment_from_proto(subscription))


def blacklist_entry_from_proto(entry: pb.Attachment | None) -> BlacklistEntry:
    """从 protobuf Attachment 消息创建 BlacklistEntry 对象。

    Args:
        entry: protobuf Attachment 消息，可能为 None。

    Returns:
        BlacklistEntry 对象。
    """
    return blacklist_entry_from_attachment(attachment_from_proto(entry))


def event_from_proto(event: pb.Event | None) -> Event:
    """将 protobuf 消息转换为 Event 对象。

    Args:
        event: protobuf Event 消息，可能为 None。

    Returns:
        Event 对象。

    Raises:
        ProtocolError: 如果 event 为 None。
    """
    if event is None:
        raise ProtocolError("missing event")
    return Event(
        sequence=event.sequence,
        event_id=event.event_id,
        event_type=event.event_type,
        aggregate=event.aggregate,
        aggregate_node_id=event.aggregate_node_id,
        aggregate_id=event.aggregate_id,
        hlc=event.hlc,
        origin_node_id=event.origin_node_id,
        event_json=bytes(event.event_json),
    )


def cluster_node_from_proto(node: pb.ClusterNode | None) -> ClusterNode:
    """将 protobuf 消息转换为 ClusterNode 对象。

    Args:
        node: protobuf ClusterNode 消息，可能为 None。

    Returns:
        ClusterNode 对象。

    Raises:
        ProtocolError: 如果 node 为 None。
    """
    if node is None:
        raise ProtocolError("missing cluster node")
    return ClusterNode(
        node_id=node.node_id,
        is_local=node.is_local,
        configured_url=node.configured_url,
        source=node.source,
    )


def logged_in_user_from_proto(user: pb.LoggedInUser | None) -> LoggedInUser:
    """将 protobuf 消息转换为 LoggedInUser 对象。

    Args:
        user: protobuf LoggedInUser 消息，可能为 None。

    Returns:
        LoggedInUser 对象。

    Raises:
        ProtocolError: 如果 user 为 None。
    """
    if user is None:
        raise ProtocolError("missing logged-in user")
    return LoggedInUser(
        node_id=user.node_id,
        user_id=user.user_id,
        username=user.username,
        login_name=user.login_name,
    )


def online_node_presence_from_proto(presence: pb.OnlineNodePresence | None) -> OnlineNodePresence:
    """将 protobuf 消息转换为 OnlineNodePresence 对象。

    Args:
        presence: protobuf OnlineNodePresence 消息，可能为 None。

    Returns:
        OnlineNodePresence 对象。

    Raises:
        ProtocolError: 如果 presence 为 None。
    """
    if presence is None:
        raise ProtocolError("missing online node presence")
    return OnlineNodePresence(
        serving_node_id=presence.serving_node_id,
        session_count=presence.session_count,
        transport_hint=presence.transport_hint,
    )


def resolved_session_from_proto(session: pb.ResolvedSession | None) -> ResolvedSession:
    """将 protobuf 消息转换为 ResolvedSession 对象。

    Args:
        session: protobuf ResolvedSession 消息，可能为 None。

    Returns:
        ResolvedSession 对象。

    Raises:
        ProtocolError: 如果 session 为 None 或缺少会话引用。
    """
    if session is None:
        raise ProtocolError("missing resolved session")
    if not session.HasField("session"):
        raise ProtocolError("missing resolved session_ref")
    return ResolvedSession(
        session=session_ref_from_proto(session.session),
        transport=session.transport,
        transient_capable=session.transient_capable,
    )


def resolved_user_sessions_from_proto(
    response: pb.ResolveUserSessionsResponse | None,
) -> ResolvedUserSessions:
    """将 protobuf 响应消息转换为 ResolvedUserSessions 对象。

    Args:
        response: protobuf ResolveUserSessionsResponse 消息，可能为 None。

    Returns:
        ResolvedUserSessions 对象。

    Raises:
        ProtocolError: 如果 response 为 None。
    """
    if response is None:
        raise ProtocolError("missing resolve_user_sessions_response")
    return ResolvedUserSessions(
        user=user_ref_from_proto(response.user),
        presence=[online_node_presence_from_proto(item) for item in response.presence],
        sessions=[resolved_session_from_proto(item) for item in response.items],
    )


def operations_status_from_proto(status: pb.OperationsStatus | None) -> OperationsStatus:
    """将 protobuf 消息转换为 OperationsStatus 对象。

    Args:
        status: protobuf OperationsStatus 消息，可能为 None。

    Returns:
        OperationsStatus 对象。

    Raises:
        ProtocolError: 如果 status 为 None。
    """
    if status is None:
        raise ProtocolError("missing operations status")
    return OperationsStatus(
        node_id=status.node_id,
        message_window_size=status.message_window_size,
        last_event_sequence=status.last_event_sequence,
        write_gate_ready=status.write_gate_ready,
        conflict_total=status.conflict_total,
        message_trim=message_trim_status_from_proto(status.message_trim),
        projection=projection_status_from_proto(status.projection),
        peers=[peer_status_from_proto(item) for item in status.peers],
        event_log_trim=event_log_trim_status_from_proto(status.event_log_trim)
        if status.HasField("event_log_trim")
        else None,
    )


def message_trim_status_from_proto(status: pb.MessageTrimStatus | None) -> MessageTrimStatus:
    """将 protobuf 消息转换为 MessageTrimStatus 对象。

    Args:
        status: protobuf MessageTrimStatus 消息，可能为 None。

    Returns:
        MessageTrimStatus 对象，如果输入为 None 则返回零值。
    """
    if status is None:
        return MessageTrimStatus(trimmed_total=0, last_trimmed_at="")
    return MessageTrimStatus(trimmed_total=status.trimmed_total, last_trimmed_at=status.last_trimmed_at)


def event_log_trim_status_from_proto(status: pb.EventLogTrimStatus | None) -> EventLogTrimStatus:
    """将 protobuf 消息转换为 EventLogTrimStatus 对象。

    Args:
        status: protobuf EventLogTrimStatus 消息，可能为 None。

    Returns:
        EventLogTrimStatus 对象，如果输入为 None 则返回零值。
    """
    if status is None:
        return EventLogTrimStatus(trimmed_total=0, last_trimmed_at="")
    return EventLogTrimStatus(trimmed_total=status.trimmed_total, last_trimmed_at=status.last_trimmed_at)


def projection_status_from_proto(status: pb.ProjectionStatus | None) -> ProjectionStatus:
    """将 protobuf 消息转换为 ProjectionStatus 对象。

    Args:
        status: protobuf ProjectionStatus 消息，可能为 None。

    Returns:
        ProjectionStatus 对象，如果输入为 None 则返回零值。
    """
    if status is None:
        return ProjectionStatus(pending_total=0, last_failed_at="")
    return ProjectionStatus(pending_total=status.pending_total, last_failed_at=status.last_failed_at)


def peer_origin_status_from_proto(status: pb.PeerOriginStatus | None) -> PeerOriginStatus:
    """将 protobuf 消息转换为 PeerOriginStatus 对象。

    Args:
        status: protobuf PeerOriginStatus 消息，可能为 None。

    Returns:
        PeerOriginStatus 对象，如果输入为 None 则返回零值。
    """
    if status is None:
        return PeerOriginStatus(
            origin_node_id=0,
            acked_event_id=0,
            applied_event_id=0,
            unconfirmed_events=0,
            cursor_updated_at="",
            remote_last_event_id=0,
            pending_catchup=False,
        )
    return PeerOriginStatus(
        origin_node_id=status.origin_node_id,
        acked_event_id=status.acked_event_id,
        applied_event_id=status.applied_event_id,
        unconfirmed_events=status.unconfirmed_events,
        cursor_updated_at=status.cursor_updated_at,
        remote_last_event_id=status.remote_last_event_id,
        pending_catchup=status.pending_catchup,
    )


def peer_status_from_proto(status: pb.PeerStatus | None) -> PeerStatus:
    """将 protobuf 消息转换为 PeerStatus 对象。

    Args:
        status: protobuf PeerStatus 消息，可能为 None。

    Returns:
        PeerStatus 对象，如果输入为 None 则返回零值。
    """
    if status is None:
        return PeerStatus(
            node_id=0,
            configured_url="",
            source="",
            discovered_url="",
            discovery_state="",
            last_discovered_at="",
            last_connected_at="",
            last_discovery_error="",
            connected=False,
            session_direction="",
        )
    return PeerStatus(
        node_id=status.node_id,
        configured_url=status.configured_url,
        source=status.source,
        discovered_url=status.discovered_url,
        discovery_state=status.discovery_state,
        last_discovered_at=status.last_discovered_at,
        last_connected_at=status.last_connected_at,
        last_discovery_error=status.last_discovery_error,
        connected=status.connected,
        session_direction=status.session_direction,
        origins=[peer_origin_status_from_proto(item) for item in status.origins],
        pending_snapshot_partitions=status.pending_snapshot_partitions,
        remote_snapshot_version=status.remote_snapshot_version,
        remote_message_window_size=status.remote_message_window_size,
        clock_offset_ms=status.clock_offset_ms,
        last_clock_sync=status.last_clock_sync,
        snapshot_digests_sent_total=status.snapshot_digests_sent_total,
        snapshot_digests_received_total=status.snapshot_digests_received_total,
        snapshot_chunks_sent_total=status.snapshot_chunks_sent_total,
        snapshot_chunks_received_total=status.snapshot_chunks_received_total,
        last_snapshot_digest_at=status.last_snapshot_digest_at,
        last_snapshot_chunk_at=status.last_snapshot_chunk_at,
    )


def messages_from_proto(items: list[pb.Message]) -> list[Message]:
    """批量将 protobuf Message 列表转换为 Message 对象列表。

    Args:
        items: protobuf Message 消息列表。

    Returns:
        Message 对象列表。
    """
    return [message_from_proto(item) for item in items]


def attachments_from_proto(items: list[pb.Attachment]) -> list[Attachment]:
    """批量将 protobuf Attachment 列表转换为 Attachment 对象列表。

    Args:
        items: protobuf Attachment 消息列表。

    Returns:
        Attachment 对象列表。
    """
    return [attachment_from_proto(item) for item in items]


def subscriptions_from_proto(items: list[pb.Attachment]) -> list[Subscription]:
    """批量将 protobuf Attachment 列表转换为 Subscription 对象列表。

    Args:
        items: protobuf Attachment 消息列表。

    Returns:
        Subscription 对象列表。
    """
    return [subscription_from_proto(item) for item in items]


def blacklist_entries_from_proto(items: list[pb.Attachment]) -> list[BlacklistEntry]:
    """批量将 protobuf Attachment 列表转换为 BlacklistEntry 对象列表。

    Args:
        items: protobuf Attachment 消息列表。

    Returns:
        BlacklistEntry 对象列表。
    """
    return [blacklist_entry_from_proto(item) for item in items]


def events_from_proto(items: list[pb.Event]) -> list[Event]:
    """批量将 protobuf Event 列表转换为 Event 对象列表。

    Args:
        items: protobuf Event 消息列表。

    Returns:
        Event 对象列表。
    """
    return [event_from_proto(item) for item in items]


def cluster_nodes_from_proto(items: list[pb.ClusterNode]) -> list[ClusterNode]:
    """批量将 protobuf ClusterNode 列表转换为 ClusterNode 对象列表。

    Args:
        items: protobuf ClusterNode 消息列表。

    Returns:
        ClusterNode 对象列表。
    """
    return [cluster_node_from_proto(item) for item in items]


def logged_in_users_from_proto(items: list[pb.LoggedInUser]) -> list[LoggedInUser]:
    """批量将 protobuf LoggedInUser 列表转换为 LoggedInUser 对象列表。

    Args:
        items: protobuf LoggedInUser 消息列表。

    Returns:
        LoggedInUser 对象列表。
    """
    return [logged_in_user_from_proto(item) for item in items]


def users_from_proto(items: list[pb.User]) -> list[User]:
    """批量将 protobuf User 列表转换为 User 对象列表。

    Args:
        items: protobuf User 消息列表。

    Returns:
        User 对象列表。
    """
    return [user_from_proto(item) for item in items]


def user_metadata_scan_result_from_proto(
    response: pb.ScanUserMetadataResponse | None,
) -> UserMetadataScanResult:
    """将 protobuf 扫描响应转换为 UserMetadataScanResult 对象。

    Args:
        response: protobuf ScanUserMetadataResponse 消息，可能为 None。

    Returns:
        UserMetadataScanResult 对象。

    Raises:
        ProtocolError: 如果 response 为 None。
    """
    if response is None:
        raise ProtocolError("missing scan_user_metadata_response")
    return UserMetadataScanResult(
        items=[user_metadata_from_proto(item) for item in response.items],
        count=response.count,
        next_after=response.next_after,
    )


def online_presence_from_proto(items: list[pb.OnlineNodePresence]) -> list[OnlineNodePresence]:
    """批量将 protobuf OnlineNodePresence 列表转换为 OnlineNodePresence 对象列表。

    Args:
        items: protobuf OnlineNodePresence 消息列表。

    Returns:
        OnlineNodePresence 对象列表。
    """
    return [online_node_presence_from_proto(item) for item in items]


def resolved_sessions_from_proto(items: list[pb.ResolvedSession]) -> list[ResolvedSession]:
    """批量将 protobuf ResolvedSession 列表转换为 ResolvedSession 对象列表。

    Args:
        items: protobuf ResolvedSession 消息列表。

    Returns:
        ResolvedSession 对象列表。
    """
    return [resolved_session_from_proto(item) for item in items]


def user_ref_from_http(data: dict[str, Any] | None) -> UserRef:
    """从 HTTP JSON 字典解析 UserRef 对象。

    Args:
        data: 包含 ``node_id`` 和 ``user_id``（或 ``id``）的字典。

    Returns:
        UserRef 对象，如果输入不是字典则返回 (0, 0)。
    """
    if not isinstance(data, dict):
        return UserRef(node_id=0, user_id=0)
    return UserRef(node_id=_int_value(data.get("node_id")), user_id=_int_value(data.get("user_id") or data.get("id")))


def session_ref_from_http(data: dict[str, Any] | None) -> SessionRef | None:
    """从 HTTP JSON 字典解析 SessionRef 对象。

    Args:
        data: 包含 ``serving_node_id`` 和 ``session_id`` 的字典。

    Returns:
        SessionRef 对象，如果数据无效则返回 None。
    """
    if not isinstance(data, dict):
        return None
    serving_node_id = _int_value(data.get("serving_node_id"))
    session_id = _str_value(data.get("session_id"))
    if serving_node_id <= 0 or session_id == "":
        return None
    return SessionRef(serving_node_id=serving_node_id, session_id=session_id)


def user_from_http(data: dict[str, Any]) -> User:
    """从 HTTP JSON 字典解析 User 对象。

    Args:
        data: 用户信息的 JSON 字典。

    Returns:
        User 对象。
    """
    profile = data.get("profile")
    if profile is None and "profile_json" in data:
        profile = data.get("profile_json")
    return User(
        node_id=_int_value(data.get("node_id")),
        user_id=_int_value(data.get("user_id") or data.get("id")),
        username=_str_value(data.get("username")),
        role=_str_value(data.get("role")),
        profile_json=_json_value_to_bytes(profile),
        system_reserved=bool(data.get("system_reserved", False)),
        created_at=_str_value(data.get("created_at")),
        updated_at=_str_value(data.get("updated_at")),
        origin_node_id=_int_value(data.get("origin_node_id")),
        login_name=_str_value(data.get("login_name")),
    )


def users_from_http(items: list[dict[str, Any]]) -> list[User]:
    """批量将 HTTP User 字典列表转换为 User 对象列表。

    Args:
        items: HTTP JSON 用户字典列表。

    Returns:
        User 对象列表。
    """
    return [user_from_http(item) for item in items]


def _user_metadata_typed_value_from_http(data: Any, raw_value: bytes) -> MetadataTypedValue | None:
    derived = MetadataTypedValue.from_raw_value(raw_value)
    if derived is not None:
        return derived
    if not isinstance(data, dict):
        return None

    kind = _str_value(data.get("kind")).strip()
    if kind == "":
        return None
    if kind == "bytes":
        return MetadataTypedValue.of_bytes(_base64_to_bytes(data.get("bytes_value")))
    if kind == "bool":
        bool_value = data.get("bool_value")
        if not isinstance(bool_value, bool):
            raise ProtocolError("unexpected metadata typed_value.bool_value")
        return MetadataTypedValue.of_bool(bool_value)
    if kind == "string":
        string_value = data.get("string_value")
        if not isinstance(string_value, str):
            raise ProtocolError("unexpected metadata typed_value.string_value")
        return MetadataTypedValue.of_string(string_value)
    if kind == "number":
        number_value = data.get("number_value")
        if isinstance(number_value, bool) or not isinstance(number_value, (int, float, str)):
            raise ProtocolError("unexpected metadata typed_value.number_value")
        return MetadataTypedValue.of_number(number_value)
    if kind == "json":
        json_value = data.get("json_value", _MISSING)
        if json_value is _MISSING:
            raise ProtocolError("unexpected metadata typed_value.json_value")
        return MetadataTypedValue.of_json(json_value)
    raise ProtocolError(f"unsupported metadata typed_value kind {kind!r}")


def user_metadata_from_http(data: dict[str, Any]) -> UserMetadata:
    """从 HTTP JSON 字典解析 UserMetadata 对象。

    Args:
        data: 元数据的 JSON 字典。

    Returns:
        UserMetadata 对象。
    """
    value = _base64_to_bytes(data.get("value"))
    return UserMetadata(
        owner=user_ref_from_http(data.get("owner")),
        key=_str_value(data.get("key")),
        value=value,
        updated_at=_str_value(data.get("updated_at")),
        deleted_at=_str_value(data.get("deleted_at")),
        expires_at=_str_value(data.get("expires_at")),
        origin_node_id=_int_value(data.get("origin_node_id")),
        typed_value=_user_metadata_typed_value_from_http(data.get("typed_value"), value),
    )


def user_metadata_scan_result_from_http(data: dict[str, Any]) -> UserMetadataScanResult:
    """从 HTTP JSON 字典解析 UserMetadataScanResult 对象。

    Args:
        data: 元数据扫描结果的 JSON 字典。

    Returns:
        UserMetadataScanResult 对象。

    Raises:
        ProtocolError: 如果响应中缺少 items 字段或格式不正确。
    """
    items_value = data.get("items")
    if not isinstance(items_value, list):
        raise ProtocolError("missing items in scan_user_metadata response")
    items: list[UserMetadata] = []
    for item in items_value:
        if not isinstance(item, dict):
            raise ProtocolError("unexpected user metadata item")
        items.append(user_metadata_from_http(item))
    count = data.get("count")
    return UserMetadataScanResult(
        items=items,
        count=len(items) if count is None or count == "" else _int_value(count),
        next_after=_str_value(data.get("next_after")),
    )


def message_from_http(data: dict[str, Any]) -> Message:
    """从 HTTP JSON 字典解析 Message 对象。

    Args:
        data: 消息的 JSON 字典。

    Returns:
        Message 对象。
    """
    created_at_hlc = _str_value(data.get("created_at_hlc"))
    if created_at_hlc == "":
        created_at_hlc = _str_value(data.get("created_at"))
    return Message(
        recipient=user_ref_from_http(data.get("recipient")),
        node_id=_int_value(data.get("node_id")),
        seq=_int_value(data.get("seq")),
        sender=user_ref_from_http(data.get("sender")),
        body=_base64_to_bytes(data.get("body")),
        created_at_hlc=created_at_hlc,
    )


def relay_accepted_from_http(data: dict[str, Any]) -> RelayAccepted:
    """从 HTTP JSON 字典解析 RelayAccepted 对象。

    Args:
        data: 中继确认的 JSON 字典。

    Returns:
        RelayAccepted 对象。
    """
    return RelayAccepted(
        packet_id=_int_value(data.get("packet_id")),
        source_node_id=_int_value(data.get("source_node_id")),
        target_node_id=_int_value(data.get("target_node_id")),
        recipient=user_ref_from_http(data.get("recipient")),
        delivery_mode=DeliveryMode(_str_value(data.get("delivery_mode")) or DeliveryMode.UNSPECIFIED.value),
        target_session=session_ref_from_http(data.get("target_session")),
    )


def attachment_from_http(data: dict[str, Any]) -> Attachment:
    """从 HTTP JSON 字典解析 Attachment 对象。

    Args:
        data: 附件的 JSON 字典。

    Returns:
        Attachment 对象。
    """
    attachment_type = _str_value(data.get("attachment_type"))
    return Attachment(
        owner=user_ref_from_http(data.get("owner")),
        subject=user_ref_from_http(data.get("subject")),
        attachment_type=AttachmentType(attachment_type),
        config_json=_json_value_to_bytes(data.get("config_json") if "config_json" in data else {}),
        attached_at=_str_value(data.get("attached_at")),
        deleted_at=_str_value(data.get("deleted_at")),
        origin_node_id=_int_value(data.get("origin_node_id")),
    )


def subscription_from_http(data: dict[str, Any]) -> Subscription:
    """从 HTTP JSON 字典解析 Subscription 对象。

    Args:
        data: 订阅信息的 JSON 字典。

    Returns:
        Subscription 对象。
    """
    return subscription_from_attachment(attachment_from_http(data))


def blacklist_entry_from_http(data: dict[str, Any]) -> BlacklistEntry:
    """从 HTTP JSON 字典解析 BlacklistEntry 对象。

    Args:
        data: 黑名单条目的 JSON 字典。

    Returns:
        BlacklistEntry 对象。
    """
    return blacklist_entry_from_attachment(attachment_from_http(data))


def event_from_http(data: dict[str, Any]) -> Event:
    """从 HTTP JSON 字典解析 Event 对象。

    Args:
        data: 事件信息的 JSON 字典。

    Returns:
        Event 对象。
    """
    event_value = data.get("event")
    if event_value is None and "event_json" in data:
        event_value = data.get("event_json")
    return Event(
        sequence=_int_value(data.get("sequence")),
        event_id=_int_value(data.get("event_id")),
        event_type=_str_value(data.get("event_type")),
        aggregate=_str_value(data.get("aggregate")),
        aggregate_node_id=_int_value(data.get("aggregate_node_id")),
        aggregate_id=_int_value(data.get("aggregate_id")),
        hlc=_str_value(data.get("hlc")),
        origin_node_id=_int_value(data.get("origin_node_id")),
        event_json=_json_value_to_bytes(event_value),
    )


def cluster_node_from_http(data: dict[str, Any]) -> ClusterNode:
    """从 HTTP JSON 字典解析 ClusterNode 对象。

    Args:
        data: 集群节点信息的 JSON 字典。

    Returns:
        ClusterNode 对象。
    """
    return ClusterNode(
        node_id=_int_value(data.get("node_id")),
        is_local=bool(data.get("is_local", False)),
        configured_url=_str_value(data.get("configured_url")),
        source=_str_value(data.get("source")),
    )


def logged_in_user_from_http(data: dict[str, Any]) -> LoggedInUser:
    """从 HTTP JSON 字典解析 LoggedInUser 对象。

    Args:
        data: 已登录用户信息的 JSON 字典。

    Returns:
        LoggedInUser 对象。
    """
    return LoggedInUser(
        node_id=_int_value(data.get("node_id")),
        user_id=_int_value(data.get("user_id")),
        username=_str_value(data.get("username")),
        login_name=_str_value(data.get("login_name")),
    )


def operations_status_from_http(data: dict[str, Any]) -> OperationsStatus:
    """从 HTTP JSON 字典解析 OperationsStatus 对象。

    Args:
        data: 运行状态信息的 JSON 字典。

    Returns:
        OperationsStatus 对象。
    """
    return OperationsStatus(
        node_id=_int_value(data.get("node_id")),
        message_window_size=_int_value(data.get("message_window_size")),
        last_event_sequence=_int_value(data.get("last_event_sequence")),
        write_gate_ready=bool(data.get("write_gate_ready", False)),
        conflict_total=_int_value(data.get("conflict_total")),
        message_trim=message_trim_status_from_http(data.get("message_trim")),
        projection=projection_status_from_http(data.get("projection")),
        peers=[peer_status_from_http(item) for item in _list_value(data.get("peers"))],
        event_log_trim=event_log_trim_status_from_http(data.get("event_log_trim"))
        if isinstance(data.get("event_log_trim"), dict)
        else None,
    )


def message_trim_status_from_http(data: Any) -> MessageTrimStatus:
    """从 HTTP JSON 数据解析 MessageTrimStatus 对象。

    Args:
        data: 消息修剪状态的 JSON 数据。

    Returns:
        MessageTrimStatus 对象，如果输入不是字典则返回零值。
    """
    if not isinstance(data, dict):
        return MessageTrimStatus(trimmed_total=0, last_trimmed_at="")
    return MessageTrimStatus(
        trimmed_total=_int_value(data.get("trimmed_total")),
        last_trimmed_at=_str_value(data.get("last_trimmed_at")),
    )


def event_log_trim_status_from_http(data: Any) -> EventLogTrimStatus:
    """从 HTTP JSON 数据解析 EventLogTrimStatus 对象。

    Args:
        data: 事件日志修剪状态的 JSON 数据。

    Returns:
        EventLogTrimStatus 对象，如果输入不是字典则返回零值。
    """
    if not isinstance(data, dict):
        return EventLogTrimStatus(trimmed_total=0, last_trimmed_at="")
    return EventLogTrimStatus(
        trimmed_total=_int_value(data.get("trimmed_total")),
        last_trimmed_at=_str_value(data.get("last_trimmed_at")),
    )


def projection_status_from_http(data: Any) -> ProjectionStatus:
    """从 HTTP JSON 数据解析 ProjectionStatus 对象。

    Args:
        data: 投影处理状态的 JSON 数据。

    Returns:
        ProjectionStatus 对象，如果输入不是字典则返回零值。
    """
    if not isinstance(data, dict):
        return ProjectionStatus(pending_total=0, last_failed_at="")
    return ProjectionStatus(
        pending_total=_int_value(data.get("pending_total")),
        last_failed_at=_str_value(data.get("last_failed_at")),
    )


def peer_origin_status_from_http(data: Any) -> PeerOriginStatus:
    """从 HTTP JSON 数据解析 PeerOriginStatus 对象。

    Args:
        data: 对等节点来源状态的 JSON 数据。

    Returns:
        PeerOriginStatus 对象，如果输入不是字典则返回零值。
    """
    if not isinstance(data, dict):
        return PeerOriginStatus(
            origin_node_id=0,
            acked_event_id=0,
            applied_event_id=0,
            unconfirmed_events=0,
            cursor_updated_at="",
            remote_last_event_id=0,
            pending_catchup=False,
        )
    return PeerOriginStatus(
        origin_node_id=_int_value(data.get("origin_node_id")),
        acked_event_id=_int_value(data.get("acked_event_id")),
        applied_event_id=_int_value(data.get("applied_event_id")),
        unconfirmed_events=_int_value(data.get("unconfirmed_events")),
        cursor_updated_at=_str_value(data.get("cursor_updated_at")),
        remote_last_event_id=_int_value(data.get("remote_last_event_id")),
        pending_catchup=bool(data.get("pending_catchup", False)),
    )


def peer_status_from_http(data: Any) -> PeerStatus:
    """从 HTTP JSON 数据解析 PeerStatus 对象。

    Args:
        data: 对等节点状态的 JSON 数据。

    Returns:
        PeerStatus 对象，如果输入不是字典则返回零值。
    """
    if not isinstance(data, dict):
        return PeerStatus(
            node_id=0,
            configured_url="",
            source="",
            discovered_url="",
            discovery_state="",
            last_discovered_at="",
            last_connected_at="",
            last_discovery_error="",
            connected=False,
            session_direction="",
        )
    return PeerStatus(
        node_id=_int_value(data.get("node_id")),
        configured_url=_str_value(data.get("configured_url")),
        source=_str_value(data.get("source")),
        discovered_url=_str_value(data.get("discovered_url")),
        discovery_state=_str_value(data.get("discovery_state")),
        last_discovered_at=_str_value(data.get("last_discovered_at")),
        last_connected_at=_str_value(data.get("last_connected_at")),
        last_discovery_error=_str_value(data.get("last_discovery_error")),
        connected=bool(data.get("connected", False)),
        session_direction=_str_value(data.get("session_direction")),
        origins=[peer_origin_status_from_http(item) for item in _list_value(data.get("origins"))],
        pending_snapshot_partitions=_int_value(data.get("pending_snapshot_partitions")),
        remote_snapshot_version=_str_value(data.get("remote_snapshot_version")),
        remote_message_window_size=_int_value(data.get("remote_message_window_size")),
        clock_offset_ms=_int_value(data.get("clock_offset_ms")),
        last_clock_sync=_str_value(data.get("last_clock_sync")),
        snapshot_digests_sent_total=_int_value(data.get("snapshot_digests_sent_total")),
        snapshot_digests_received_total=_int_value(data.get("snapshot_digests_received_total")),
        snapshot_chunks_sent_total=_int_value(data.get("snapshot_chunks_sent_total")),
        snapshot_chunks_received_total=_int_value(data.get("snapshot_chunks_received_total")),
        last_snapshot_digest_at=_str_value(data.get("last_snapshot_digest_at")),
        last_snapshot_chunk_at=_str_value(data.get("last_snapshot_chunk_at")),
    )


def delete_user_result_from_http(data: dict[str, Any]) -> DeleteUserResult:
    """从 HTTP JSON 字典解析 DeleteUserResult 对象。

    Args:
        data: 删除用户操作结果的 JSON 字典。

    Returns:
        DeleteUserResult 对象。
    """
    user = data.get("user")
    if isinstance(user, dict):
        ref = user_ref_from_http(user)
    else:
        ref = UserRef(node_id=_int_value(data.get("node_id")), user_id=_int_value(data.get("user_id") or data.get("id")))
    return DeleteUserResult(status=_str_value(data.get("status")), user=ref)


def _json_value_to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode("utf-8") if _looks_like_json_text(value) else json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _looks_like_json_text(value: str) -> bool:
    stripped = value.strip()
    if stripped == "":
        return False
    return stripped[0] in "{[\"0123456789tfn-"


def _base64_to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    if not isinstance(value, str):
        raise ProtocolError(f"expected base64 string body, got {type(value).__name__}")
    return base64.b64decode(value)


def _int_value(value: Any) -> int:
    if value is None or value == "":
        return 0
    return int(value)


def _str_value(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _list_value(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


# --- Relay 协议手动 Protobuf 序列化 ---
# 由于 relay.proto 引用 client.proto 且 protoc 不可用，
# 实现兼容的手动 Protobuf Wire Format 编码/解码。


def _encode_varint(value: int) -> bytes:
    """编码无符号整数为 Protobuf varint。"""
    buf = bytearray()
    value = value & 0xFFFFFFFFFFFFFFFF
    while value > 0x7F:
        buf.append((value & 0x7F) | 0x80)
        value >>= 7
    buf.append(value & 0x7F)
    return bytes(buf)


def _decode_varint(data: bytes, offset: int) -> tuple[int, int]:
    """解码 Protobuf varint，返回 (value, new_offset)。"""
    value = 0
    shift = 0
    while True:
        if offset >= len(data):
            raise ValueError("truncated varint")
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return value, offset
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long")


def _encode_tag(field_number: int, wire_type: int) -> bytes:
    """编码 Protobuf 字段 tag。"""
    return _encode_varint((field_number << 3) | wire_type)


def _skip_field(data: bytes, offset: int, wire_type: int) -> int:
    """跳过当前字段，返回新的 offset。"""
    if wire_type == 0:  # varint
        _, offset = _decode_varint(data, offset)
    elif wire_type == 1:  # 64-bit
        offset += 8
    elif wire_type == 2:  # length-delimited
        length, offset = _decode_varint(data, offset)
        offset += length
    elif wire_type == 5:  # 32-bit
        offset += 4
    else:
        raise ValueError(f"unsupported wire type {wire_type} in skip")
    return offset


_RELAY_SESSION_EMPTY = SessionRef(serving_node_id=0, session_id="")


def _encode_session_ref(ref: SessionRef) -> bytes:
    """编码 SessionRef 为 Protobuf 消息字节。"""
    if ref.serving_node_id == 0 and not ref.session_id:
        return b""
    buf = bytearray()
    if ref.serving_node_id != 0:
        buf.extend(_encode_tag(1, 0))
        buf.extend(_encode_varint(ref.serving_node_id))
    if ref.session_id:
        sid_bytes = ref.session_id.encode("utf-8")
        buf.extend(_encode_tag(2, 2))
        buf.extend(_encode_varint(len(sid_bytes)))
        buf.extend(sid_bytes)
    return bytes(buf)


def _decode_session_ref(data: bytes, offset: int) -> tuple[SessionRef, int]:
    """解码 Protobuf 字节为 SessionRef。"""
    serving_node_id = 0
    session_id = ""
    while offset < len(data):
        tag, offset = _decode_varint(data, offset)
        field = tag >> 3
        wire = tag & 0x7
        if field == 1 and wire == 0:
            raw, offset = _decode_varint(data, offset)
            # 符号扩展 int64
            serving_node_id = raw if raw < 0x8000000000000000 else raw - 0x10000000000000000
        elif field == 2 and wire == 2:
            length, offset = _decode_varint(data, offset)
            if offset + length > len(data):
                raise ValueError("truncated SessionRef session_id")
            session_id = data[offset:offset + length].decode("utf-8")
            offset += length
        else:
            offset = _skip_field(data, offset, wire)
    return SessionRef(serving_node_id=serving_node_id, session_id=session_id), offset


def encode_relay_envelope(env: RelayEnvelope) -> bytes:
    """将 RelayEnvelope 编码为 Protobuf 字节序列。

    Args:
        env: 待编码的 RelayEnvelope 对象。

    Returns:
        编码后的 Protobuf 字节序列。
    """
    buf = bytearray()

    if env.relay_id:
        rid_bytes = env.relay_id.encode("utf-8")
        buf.extend(_encode_tag(1, 2))
        buf.extend(_encode_varint(len(rid_bytes)))
        buf.extend(rid_bytes)

    if env.kind != RelayKind.UNSPECIFIED:
        buf.extend(_encode_tag(2, 0))
        buf.extend(_encode_varint(env.kind.value))

    if env.sender_session != _RELAY_SESSION_EMPTY:
        sender_bytes = _encode_session_ref(env.sender_session)
        buf.extend(_encode_tag(3, 2))
        buf.extend(_encode_varint(len(sender_bytes)))
        buf.extend(sender_bytes)

    if env.target_session != _RELAY_SESSION_EMPTY:
        target_bytes = _encode_session_ref(env.target_session)
        buf.extend(_encode_tag(4, 2))
        buf.extend(_encode_varint(len(target_bytes)))
        buf.extend(target_bytes)

    if env.seq != 0:
        buf.extend(_encode_tag(5, 0))
        buf.extend(_encode_varint(env.seq))

    if env.ack_seq != 0:
        buf.extend(_encode_tag(6, 0))
        buf.extend(_encode_varint(env.ack_seq))

    if env.payload:
        buf.extend(_encode_tag(7, 2))
        buf.extend(_encode_varint(len(env.payload)))
        buf.extend(env.payload)

    if env.sent_at_ms != 0:
        buf.extend(_encode_tag(8, 0))
        buf.extend(_encode_varint(env.sent_at_ms))

    return bytes(buf)


def decode_relay_envelope(data: bytes) -> RelayEnvelope:
    """从 Protobuf 字节序列解码 RelayEnvelope。

    Args:
        data: 编码后的 Protobuf 字节序列。

    Returns:
        解码后的 RelayEnvelope 对象。

    Raises:
        ProtocolError: 如果数据格式无效。
    """
    relay_id = ""
    kind = RelayKind.UNSPECIFIED
    sender_session = SessionRef(serving_node_id=0, session_id="")
    target_session = SessionRef(serving_node_id=0, session_id="")
    seq = 0
    ack_seq = 0
    payload = b""
    sent_at_ms = 0

    offset = 0
    try:
        while offset < len(data):
            tag, offset = _decode_varint(data, offset)
            field = tag >> 3
            wire = tag & 0x7

            if field == 1 and wire == 2:
                length, offset = _decode_varint(data, offset)
                if offset + length > len(data):
                    raise ValueError("truncated relay_id")
                relay_id = data[offset:offset + length].decode("utf-8")
                offset += length
            elif field == 2 and wire == 0:
                raw, offset = _decode_varint(data, offset)
                kind = RelayKind(raw)
            elif field == 3 and wire == 2:
                length, offset = _decode_varint(data, offset)
                sender_session, _ = _decode_session_ref(data[offset:offset + length], 0)
                offset += length
            elif field == 4 and wire == 2:
                length, offset = _decode_varint(data, offset)
                target_session, _ = _decode_session_ref(data[offset:offset + length], 0)
                offset += length
            elif field == 5 and wire == 0:
                seq, offset = _decode_varint(data, offset)
            elif field == 6 and wire == 0:
                ack_seq, offset = _decode_varint(data, offset)
            elif field == 7 and wire == 2:
                length, offset = _decode_varint(data, offset)
                if offset + length > len(data):
                    raise ValueError("truncated payload")
                payload = data[offset:offset + length]
                offset += length
            elif field == 8 and wire == 0:
                raw, offset = _decode_varint(data, offset)
                # 符号扩展 int64
                sent_at_ms = raw if raw < 0x8000000000000000 else raw - 0x10000000000000000
            else:
                offset = _skip_field(data, offset, wire)
    except (ValueError, IndexError, UnicodeDecodeError) as exc:
        raise ProtocolError(f"invalid relay envelope: {exc}") from exc

    return RelayEnvelope(
        relay_id=relay_id,
        kind=kind,
        sender_session=sender_session,
        target_session=target_session,
        seq=seq,
        ack_seq=ack_seq,
        payload=payload,
        sent_at_ms=sent_at_ms,
    )
