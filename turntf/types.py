from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

from .password import PasswordInput


class DeliveryMode(str, Enum):
    UNSPECIFIED = ""
    BEST_EFFORT = "best_effort"
    ROUTE_RETRY = "route_retry"


@dataclass(slots=True, frozen=True)
class Credentials:
    node_id: int
    user_id: int
    password: PasswordInput


@dataclass(slots=True, frozen=True)
class UserRef:
    node_id: int
    user_id: int


@dataclass(slots=True, frozen=True)
class MessageCursor:
    node_id: int
    seq: int


@dataclass(slots=True)
class User:
    node_id: int
    user_id: int
    username: str
    role: str
    profile_json: bytes
    system_reserved: bool
    created_at: str
    updated_at: str
    origin_node_id: int


@dataclass(slots=True)
class Message:
    recipient: UserRef
    node_id: int
    seq: int
    sender: UserRef
    body: bytes
    created_at_hlc: str

    def cursor(self) -> MessageCursor:
        return MessageCursor(node_id=self.node_id, seq=self.seq)


@dataclass(slots=True)
class Packet:
    packet_id: int
    source_node_id: int
    target_node_id: int
    recipient: UserRef
    sender: UserRef
    body: bytes
    delivery_mode: DeliveryMode


@dataclass(slots=True)
class RelayAccepted:
    packet_id: int
    source_node_id: int
    target_node_id: int
    recipient: UserRef
    delivery_mode: DeliveryMode


@dataclass(slots=True)
class Subscription:
    subscriber: UserRef
    channel: UserRef
    subscribed_at: str
    deleted_at: str
    origin_node_id: int


@dataclass(slots=True)
class BlacklistEntry:
    owner: UserRef
    blocked: UserRef
    blocked_at: str
    deleted_at: str
    origin_node_id: int


@dataclass(slots=True)
class Event:
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
    node_id: int
    is_local: bool
    configured_url: str
    source: str


@dataclass(slots=True)
class LoggedInUser:
    node_id: int
    user_id: int
    username: str


@dataclass(slots=True)
class MessageTrimStatus:
    trimmed_total: int
    last_trimmed_at: str


@dataclass(slots=True)
class EventLogTrimStatus:
    trimmed_total: int
    last_trimmed_at: str


@dataclass(slots=True)
class ProjectionStatus:
    pending_total: int
    last_failed_at: str


@dataclass(slots=True)
class PeerOriginStatus:
    origin_node_id: int
    acked_event_id: int
    applied_event_id: int
    unconfirmed_events: int
    cursor_updated_at: str
    remote_last_event_id: int
    pending_catchup: bool


@dataclass(slots=True)
class PeerStatus:
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
    status: str
    user: UserRef


@dataclass(slots=True)
class LoginInfo:
    user: User
    protocol_version: str


@dataclass(slots=True)
class CreateUserRequest:
    username: str
    password: PasswordInput | None = None
    profile_json: bytes = b""
    role: str = ""


@dataclass(slots=True)
class UpdateUserRequest:
    username: str | None = None
    password: PasswordInput | None = None
    profile_json: bytes | None = None
    role: str | None = None
