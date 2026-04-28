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
    ResolvedSession,
    ResolvedUserSessions,
    SessionRef,
    Subscription,
    User,
    UserRef,
)


def delivery_mode_to_proto(mode: DeliveryMode) -> int:
    if mode == DeliveryMode.BEST_EFFORT:
        return pb.CLIENT_DELIVERY_MODE_BEST_EFFORT
    if mode == DeliveryMode.ROUTE_RETRY:
        return pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY
    return pb.CLIENT_DELIVERY_MODE_UNSPECIFIED


def delivery_mode_from_proto(mode: int) -> DeliveryMode:
    if mode == pb.CLIENT_DELIVERY_MODE_BEST_EFFORT:
        return DeliveryMode.BEST_EFFORT
    if mode == pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY:
        return DeliveryMode.ROUTE_RETRY
    return DeliveryMode.UNSPECIFIED


def attachment_type_to_proto(attachment_type: AttachmentType) -> int:
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
    return pb.UserRef(node_id=ref.node_id, user_id=ref.user_id)


def session_ref_to_proto(ref: SessionRef) -> pb.SessionRef:
    return pb.SessionRef(serving_node_id=ref.serving_node_id, session_id=ref.session_id)


def cursor_to_proto(cursor: MessageCursor) -> pb.MessageCursor:
    return pb.MessageCursor(node_id=cursor.node_id, seq=cursor.seq)


def cursor_from_proto(cursor: pb.MessageCursor | None) -> MessageCursor:
    if cursor is None:
        return MessageCursor(node_id=0, seq=0)
    return MessageCursor(node_id=cursor.node_id, seq=cursor.seq)


def user_ref_from_proto(ref: pb.UserRef | None) -> UserRef:
    if ref is None:
        return UserRef(node_id=0, user_id=0)
    return UserRef(node_id=ref.node_id, user_id=ref.user_id)


def session_ref_from_proto(ref: pb.SessionRef | None) -> SessionRef:
    if ref is None:
        raise ProtocolError("missing session_ref")
    if ref.serving_node_id <= 0 or ref.session_id.strip() == "":
        raise ProtocolError("invalid session_ref")
    return SessionRef(serving_node_id=ref.serving_node_id, session_id=ref.session_id)


def user_from_proto(user: pb.User | None) -> User:
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
    )


def message_from_proto(message: pb.Message | None) -> Message:
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
    return Subscription(
        subscriber=attachment.owner,
        channel=attachment.subject,
        subscribed_at=attachment.attached_at,
        deleted_at=attachment.deleted_at,
        origin_node_id=attachment.origin_node_id,
    )


def blacklist_entry_from_attachment(attachment: Attachment) -> BlacklistEntry:
    return BlacklistEntry(
        owner=attachment.owner,
        blocked=attachment.subject,
        blocked_at=attachment.attached_at,
        deleted_at=attachment.deleted_at,
        origin_node_id=attachment.origin_node_id,
    )


def subscription_from_proto(subscription: pb.Attachment | None) -> Subscription:
    return subscription_from_attachment(attachment_from_proto(subscription))


def blacklist_entry_from_proto(entry: pb.Attachment | None) -> BlacklistEntry:
    return blacklist_entry_from_attachment(attachment_from_proto(entry))


def event_from_proto(event: pb.Event | None) -> Event:
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
    if node is None:
        raise ProtocolError("missing cluster node")
    return ClusterNode(
        node_id=node.node_id,
        is_local=node.is_local,
        configured_url=node.configured_url,
        source=node.source,
    )


def logged_in_user_from_proto(user: pb.LoggedInUser | None) -> LoggedInUser:
    if user is None:
        raise ProtocolError("missing logged-in user")
    return LoggedInUser(node_id=user.node_id, user_id=user.user_id, username=user.username)


def online_node_presence_from_proto(presence: pb.OnlineNodePresence | None) -> OnlineNodePresence:
    if presence is None:
        raise ProtocolError("missing online node presence")
    return OnlineNodePresence(
        serving_node_id=presence.serving_node_id,
        session_count=presence.session_count,
        transport_hint=presence.transport_hint,
    )


def resolved_session_from_proto(session: pb.ResolvedSession | None) -> ResolvedSession:
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
    if response is None:
        raise ProtocolError("missing resolve_user_sessions_response")
    return ResolvedUserSessions(
        user=user_ref_from_proto(response.user),
        presence=[online_node_presence_from_proto(item) for item in response.presence],
        sessions=[resolved_session_from_proto(item) for item in response.items],
    )


def operations_status_from_proto(status: pb.OperationsStatus | None) -> OperationsStatus:
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
    if status is None:
        return MessageTrimStatus(trimmed_total=0, last_trimmed_at="")
    return MessageTrimStatus(trimmed_total=status.trimmed_total, last_trimmed_at=status.last_trimmed_at)


def event_log_trim_status_from_proto(status: pb.EventLogTrimStatus | None) -> EventLogTrimStatus:
    if status is None:
        return EventLogTrimStatus(trimmed_total=0, last_trimmed_at="")
    return EventLogTrimStatus(trimmed_total=status.trimmed_total, last_trimmed_at=status.last_trimmed_at)


def projection_status_from_proto(status: pb.ProjectionStatus | None) -> ProjectionStatus:
    if status is None:
        return ProjectionStatus(pending_total=0, last_failed_at="")
    return ProjectionStatus(pending_total=status.pending_total, last_failed_at=status.last_failed_at)


def peer_origin_status_from_proto(status: pb.PeerOriginStatus | None) -> PeerOriginStatus:
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
    return [message_from_proto(item) for item in items]


def attachments_from_proto(items: list[pb.Attachment]) -> list[Attachment]:
    return [attachment_from_proto(item) for item in items]


def subscriptions_from_proto(items: list[pb.Attachment]) -> list[Subscription]:
    return [subscription_from_proto(item) for item in items]


def blacklist_entries_from_proto(items: list[pb.Attachment]) -> list[BlacklistEntry]:
    return [blacklist_entry_from_proto(item) for item in items]


def events_from_proto(items: list[pb.Event]) -> list[Event]:
    return [event_from_proto(item) for item in items]


def cluster_nodes_from_proto(items: list[pb.ClusterNode]) -> list[ClusterNode]:
    return [cluster_node_from_proto(item) for item in items]


def logged_in_users_from_proto(items: list[pb.LoggedInUser]) -> list[LoggedInUser]:
    return [logged_in_user_from_proto(item) for item in items]


def online_presence_from_proto(items: list[pb.OnlineNodePresence]) -> list[OnlineNodePresence]:
    return [online_node_presence_from_proto(item) for item in items]


def resolved_sessions_from_proto(items: list[pb.ResolvedSession]) -> list[ResolvedSession]:
    return [resolved_session_from_proto(item) for item in items]


def user_ref_from_http(data: dict[str, Any] | None) -> UserRef:
    if not isinstance(data, dict):
        return UserRef(node_id=0, user_id=0)
    return UserRef(node_id=_int_value(data.get("node_id")), user_id=_int_value(data.get("user_id") or data.get("id")))


def session_ref_from_http(data: dict[str, Any] | None) -> SessionRef | None:
    if not isinstance(data, dict):
        return None
    serving_node_id = _int_value(data.get("serving_node_id"))
    session_id = _str_value(data.get("session_id"))
    if serving_node_id <= 0 or session_id == "":
        return None
    return SessionRef(serving_node_id=serving_node_id, session_id=session_id)


def user_from_http(data: dict[str, Any]) -> User:
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
    )


def message_from_http(data: dict[str, Any]) -> Message:
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
    return RelayAccepted(
        packet_id=_int_value(data.get("packet_id")),
        source_node_id=_int_value(data.get("source_node_id")),
        target_node_id=_int_value(data.get("target_node_id")),
        recipient=user_ref_from_http(data.get("recipient")),
        delivery_mode=DeliveryMode(_str_value(data.get("delivery_mode")) or DeliveryMode.UNSPECIFIED.value),
        target_session=session_ref_from_http(data.get("target_session")),
    )


def attachment_from_http(data: dict[str, Any]) -> Attachment:
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
    return subscription_from_attachment(attachment_from_http(data))


def blacklist_entry_from_http(data: dict[str, Any]) -> BlacklistEntry:
    return blacklist_entry_from_attachment(attachment_from_http(data))


def event_from_http(data: dict[str, Any]) -> Event:
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
    return ClusterNode(
        node_id=_int_value(data.get("node_id")),
        is_local=bool(data.get("is_local", False)),
        configured_url=_str_value(data.get("configured_url")),
        source=_str_value(data.get("source")),
    )


def logged_in_user_from_http(data: dict[str, Any]) -> LoggedInUser:
    return LoggedInUser(
        node_id=_int_value(data.get("node_id")),
        user_id=_int_value(data.get("user_id")),
        username=_str_value(data.get("username")),
    )


def operations_status_from_http(data: dict[str, Any]) -> OperationsStatus:
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
    if not isinstance(data, dict):
        return MessageTrimStatus(trimmed_total=0, last_trimmed_at="")
    return MessageTrimStatus(
        trimmed_total=_int_value(data.get("trimmed_total")),
        last_trimmed_at=_str_value(data.get("last_trimmed_at")),
    )


def event_log_trim_status_from_http(data: Any) -> EventLogTrimStatus:
    if not isinstance(data, dict):
        return EventLogTrimStatus(trimmed_total=0, last_trimmed_at="")
    return EventLogTrimStatus(
        trimmed_total=_int_value(data.get("trimmed_total")),
        last_trimmed_at=_str_value(data.get("last_trimmed_at")),
    )


def projection_status_from_http(data: Any) -> ProjectionStatus:
    if not isinstance(data, dict):
        return ProjectionStatus(pending_total=0, last_failed_at="")
    return ProjectionStatus(
        pending_total=_int_value(data.get("pending_total")),
        last_failed_at=_str_value(data.get("last_failed_at")),
    )


def peer_origin_status_from_http(data: Any) -> PeerOriginStatus:
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
