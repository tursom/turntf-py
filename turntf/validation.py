from __future__ import annotations

import re

from .types import (
    DeliveryMode,
    ListUsersRequest,
    Message,
    MessageCursor,
    ScanUserMetadataRequest,
    SessionRef,
    UserRef,
)

_USER_METADATA_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._:-]*$")
_USER_METADATA_KEY_MAX_LENGTH = 128
_USER_METADATA_SCAN_LIMIT_MAX = 1000


def validate_positive_int(value: int, field: str) -> None:
    """验证整数值是否为正整数。

    Args:
        value: 要验证的整数值。
        field: 字段名称，用于错误消息中标识参数位置。

    Raises:
        ValueError: 如果 value 不是整数类型或 <= 0。
    """
    if not isinstance(value, int):
        raise ValueError(f"{field} must be an integer")
    if value <= 0:
        raise ValueError(f"{field} is required")


def validate_login_selector(
    *,
    node_id: int | None = None,
    user_id: int | None = None,
    login_name: str | None = None,
    field: str = "login",
) -> str:
    """验证登录选择器参数。

    登录时必须且只能提供一种身份标识方式：
    要么同时提供 (node_id, user_id)，要么提供 login_name。

    Args:
        node_id: 节点 ID（可选）。
        user_id: 用户 ID（可选）。
        login_name: 登录名（可选）。
        field: 字段名称前缀，用于错误消息标识。

    Returns:
        规范化的 login_name 字符串（去除首尾空格）。

    Raises:
        ValueError: 如果既没有提供 (node_id, user_id) 也没有提供 login_name，
                    或者同时提供了两者。
    """
    normalized_login_name = "" if login_name is None else login_name.strip()
    has_id_selector = node_id is not None or user_id is not None
    has_login_name_selector = normalized_login_name != ""
    if has_id_selector == has_login_name_selector:
        raise ValueError(f"{field} must provide exactly one of (node_id,user_id) or login_name")
    if has_id_selector:
        validate_positive_int(0 if node_id is None else node_id, f"{field}.node_id")
        validate_positive_int(0 if user_id is None else user_id, f"{field}.user_id")
    return normalized_login_name


def validate_user_ref(ref: UserRef, field: str = "user") -> None:
    """验证 UserRef 引用的有效性。

    确保引用的 node_id 和 user_id 均为正整数。

    Args:
        ref: 要验证的 UserRef 对象。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果 ref.node_id 或 ref.user_id 无效。
    """
    validate_positive_int(ref.node_id, f"{field}.node_id")
    validate_positive_int(ref.user_id, f"{field}.user_id")


def normalize_list_users_request(
    request: ListUsersRequest | None = None,
    *,
    name: str | None = None,
    uid: UserRef | None = None,
    field: str = "request",
) -> ListUsersRequest:
    """规范化用户列表过滤参数。

    支持直接传入 ``ListUsersRequest``，也支持通过关键字参数传入 ``name`` / ``uid``。
    ``uid = UserRef(0, 0)`` 会被视为“不按 uid 过滤”的兼容写法。

    Args:
        request: 可选的过滤请求对象。
        name: 可选的名称过滤关键字。
        uid: 可选的精确用户过滤。
        field: 字段名称前缀，用于错误消息标识。

    Returns:
        规范化后的 ``ListUsersRequest``。

    Raises:
        ValueError: 如果同时混用 request 与独立关键字，或参数格式非法。
    """
    if request is not None and (name is not None or uid is not None):
        raise ValueError(f"{field} must be provided either as request or name/uid keyword filters")
    if request is None:
        request = ListUsersRequest(name="" if name is None else name, uid=uid)
    if not isinstance(request.name, str):
        raise ValueError(f"{field}.name must be a string")

    normalized_uid = request.uid
    if normalized_uid is not None:
        if not isinstance(normalized_uid, UserRef):
            raise ValueError(f"{field}.uid must be a UserRef")
        if normalized_uid.node_id == 0 and normalized_uid.user_id == 0:
            normalized_uid = None
        else:
            validate_user_ref(normalized_uid, f"{field}.uid")

    return ListUsersRequest(name=request.name.strip(), uid=normalized_uid)


def validate_delivery_mode(mode: DeliveryMode) -> None:
    """验证投递模式是否为有效的非默认值。

    有效的投递模式为 ``BEST_EFFORT`` 和 ``ROUTE_RETRY``。
    ``UNSPECIFIED`` 不被视为有效值。

    Args:
        mode: 要验证的 DeliveryMode 枚举值。

    Raises:
        ValueError: 如果 mode 不是有效的投递模式。
    """
    if mode not in {DeliveryMode.BEST_EFFORT, DeliveryMode.ROUTE_RETRY}:
        raise ValueError(f"invalid delivery_mode {mode!r}")


def validate_user_metadata_key(value: str, field: str = "key") -> None:
    """验证用户元数据的键名是否有效。

    键名必须符合正则表达式 ``^[A-Za-z0-9._:-]*$``，
    且长度不超过 128 个字符。

    Args:
        value: 要验证的键名字符串。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果键名无效。
    """
    _validate_user_metadata_key_fragment(value, field, allow_empty=False)


def validate_optional_user_metadata_key_fragment(value: str, field: str) -> None:
    """验证可选的用户元数据键名片段是否有效。

    与 ``validate_user_metadata_key`` 类似，但允许空字符串。

    Args:
        value: 要验证的键名片段字符串。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果键名片段无效。
    """
    _validate_user_metadata_key_fragment(value, field, allow_empty=True)


def validate_user_metadata_scan_limit(value: int, field: str = "limit") -> None:
    """验证用户元数据扫描的 limit 参数。

    limit 必须为非负整数，且不能超过 1000。

    Args:
        value: 要验证的 limit 值。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果 limit 无效或超出范围。
    """
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    if value > _USER_METADATA_SCAN_LIMIT_MAX:
        raise ValueError(f"{field} cannot exceed {_USER_METADATA_SCAN_LIMIT_MAX}")


def validate_user_metadata_scan_request(request: ScanUserMetadataRequest, field: str = "request") -> None:
    """验证用户元数据扫描请求参数。

    验证 prefix、after 和 limit 字段的有效性。
    如果同时设置了 prefix 和 after，则 after 必须以 prefix 开头。

    Args:
        request: 要验证的 ScanUserMetadataRequest 对象。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果扫描请求参数无效。
    """
    validate_optional_user_metadata_key_fragment(request.prefix, f"{field}.prefix")
    validate_optional_user_metadata_key_fragment(request.after, f"{field}.after")
    validate_user_metadata_scan_limit(request.limit, f"{field}.limit")
    if request.prefix != "" and request.after != "" and not request.after.startswith(request.prefix):
        raise ValueError(f"{field}.after must use the same prefix as {field}.prefix")


def validate_session_ref(ref: SessionRef, field: str = "session_ref") -> None:
    """验证 SessionRef 会话引用的有效性。

    确保 `serving_node_id` 为正整数且 `session_id` 非空。

    Args:
        ref: 要验证的 SessionRef 对象。
        field: 字段名称，用于错误消息标识。

    Raises:
        ValueError: 如果会话引用无效。
    """
    validate_positive_int(ref.serving_node_id, f"{field}.serving_node_id")
    if ref.session_id.strip() == "":
        raise ValueError(f"{field}.session_id is required")


def cursor_for_message(message: Message) -> MessageCursor:
    """从消息对象创建对应的游标。

    Args:
        message: Message 对象。

    Returns:
        包含相同 node_id 和 seq 的 MessageCursor 对象。
    """
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
