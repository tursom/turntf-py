from __future__ import annotations

from .types import DeliveryMode, Message, MessageCursor, SessionRef, UserRef


def validate_positive_int(value: int, field: str) -> None:
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} is required")


def validate_user_ref(ref: UserRef, field: str = "user") -> None:
    validate_positive_int(ref.node_id, f"{field}.node_id")
    validate_positive_int(ref.user_id, f"{field}.user_id")


def validate_delivery_mode(mode: DeliveryMode) -> None:
    if mode not in {DeliveryMode.BEST_EFFORT, DeliveryMode.ROUTE_RETRY}:
        raise ValueError(f"invalid delivery_mode {mode!r}")


def validate_session_ref(ref: SessionRef, field: str = "session_ref") -> None:
    validate_positive_int(ref.serving_node_id, f"{field}.serving_node_id")
    if ref.session_id.strip() == "":
        raise ValueError(f"{field}.session_id is required")


def cursor_for_message(message: Message) -> MessageCursor:
    return MessageCursor(node_id=message.node_id, seq=message.seq)
