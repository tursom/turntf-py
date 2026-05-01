from __future__ import annotations

import re

from .types import DeliveryMode, Message, MessageCursor, ScanUserMetadataRequest, SessionRef, UserRef

_USER_METADATA_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]*$")
_USER_METADATA_KEY_MAX_LENGTH = 128
_USER_METADATA_SCAN_LIMIT_MAX = 1000


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


def validate_user_metadata_key(value: str, field: str = "key") -> None:
    _validate_user_metadata_key_fragment(value, field, allow_empty=False)


def validate_optional_user_metadata_key_fragment(value: str, field: str) -> None:
    _validate_user_metadata_key_fragment(value, field, allow_empty=True)


def validate_user_metadata_scan_limit(value: int, field: str = "limit") -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    if value > _USER_METADATA_SCAN_LIMIT_MAX:
        raise ValueError(f"{field} cannot exceed {_USER_METADATA_SCAN_LIMIT_MAX}")


def validate_user_metadata_scan_request(request: ScanUserMetadataRequest, field: str = "request") -> None:
    validate_optional_user_metadata_key_fragment(request.prefix, f"{field}.prefix")
    validate_optional_user_metadata_key_fragment(request.after, f"{field}.after")
    validate_user_metadata_scan_limit(request.limit, f"{field}.limit")
    if request.prefix != "" and request.after != "" and not request.after.startswith(request.prefix):
        raise ValueError(f"{field}.after must use the same prefix as {field}.prefix")


def validate_session_ref(ref: SessionRef, field: str = "session_ref") -> None:
    validate_positive_int(ref.serving_node_id, f"{field}.serving_node_id")
    if ref.session_id.strip() == "":
        raise ValueError(f"{field}.session_id is required")


def cursor_for_message(message: Message) -> MessageCursor:
    return MessageCursor(node_id=message.node_id, seq=message.seq)


def _validate_user_metadata_key_fragment(value: str, field: str, *, allow_empty: bool) -> None:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    if value == "":
        if allow_empty:
            return
        raise ValueError(f"{field} is required")
    if len(value) > _USER_METADATA_KEY_MAX_LENGTH:
        raise ValueError(f"{field} cannot exceed {_USER_METADATA_KEY_MAX_LENGTH} characters")
    if not _USER_METADATA_KEY_PATTERN.fullmatch(value):
        raise ValueError(f"{field} contains unsupported characters")
