from __future__ import annotations

import base64
import json
from typing import Any, Iterable

import httpx

from .errors import ConnectionError, ProtocolError
from .mapping import (
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
)
from .password import PasswordInput, plain_password
from .types import (
    BlacklistEntry,
    ClusterNode,
    CreateUserRequest,
    DeleteUserResult,
    DeliveryMode,
    Event,
    LoggedInUser,
    Message,
    OperationsStatus,
    Subscription,
    UpdateUserRequest,
    User,
    UserRef,
)
from .validation import validate_delivery_mode, validate_positive_int, validate_user_ref


class AsyncHTTPClient:
    def __init__(
        self,
        base_url: str,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float | None = 10.0,
    ) -> None:
        if base_url.strip() == "":
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)

    async def __aenter__(self) -> "AsyncHTTPClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def login(self, node_id: int, user_id: int, password: str) -> str:
        return await self.login_with_password(node_id, user_id, plain_password(password))

    async def login_with_password(self, node_id: int, user_id: int, password: PasswordInput) -> str:
        validate_positive_int(node_id, "node_id")
        validate_positive_int(user_id, "user_id")
        response = await self._do_json(
            "POST",
            "/auth/login",
            "",
            {
                "node_id": node_id,
                "user_id": user_id,
                "password": password.wire_value(),
            },
            {200},
        )
        if not isinstance(response, dict):
            raise ProtocolError("unexpected login response")
        token = response.get("token")
        if not isinstance(token, str) or token == "":
            raise ProtocolError("empty token in login response")
        return token

    async def create_user(self, token: str, request: CreateUserRequest) -> User:
        if request.username == "":
            raise ValueError("username is required")
        if request.role == "":
            raise ValueError("role is required")
        body: dict[str, Any] = {"username": request.username, "role": request.role}
        if request.password is not None:
            body["password"] = request.password.wire_value()
        if request.profile_json:
            body["profile"] = _json_bytes_to_value(request.profile_json, "profile_json")
        response = await self._do_json("POST", "/users", token, body, {200, 201})
        return user_from_http(_expect_dict(response, "create user response"))

    async def create_channel(self, token: str, request: CreateUserRequest) -> User:
        role = request.role or "channel"
        return await self.create_user(
            token,
            CreateUserRequest(
                username=request.username,
                password=request.password,
                profile_json=request.profile_json,
                role=role,
            ),
        )

    async def get_user(self, token: str, target: UserRef) -> User:
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
        validate_user_ref(target, "target")
        body: dict[str, Any] = {}
        if request.username is not None:
            body["username"] = request.username
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
        validate_user_ref(target, "target")
        response = await self._do_json(
            "DELETE",
            f"/nodes/{target.node_id}/users/{target.user_id}",
            token,
            None,
            {200},
        )
        return delete_user_result_from_http(_expect_dict(response, "delete user response"))

    async def create_subscription(self, token: str, user: UserRef, channel: UserRef) -> Subscription:
        validate_user_ref(user, "user")
        validate_user_ref(channel, "channel")
        response = await self._do_json(
            "POST",
            f"/nodes/{user.node_id}/users/{user.user_id}/subscriptions",
            token,
            {"channel_node_id": channel.node_id, "channel_user_id": channel.user_id},
            {200, 201},
        )
        return subscription_from_http(_expect_dict(response, "create subscription response"))

    async def subscribe_channel(self, token: str, subscriber: UserRef, channel: UserRef) -> Subscription:
        return await self.create_subscription(token, subscriber, channel)

    async def unsubscribe_channel(self, token: str, subscriber: UserRef, channel: UserRef) -> Subscription:
        validate_user_ref(subscriber, "subscriber")
        validate_user_ref(channel, "channel")
        response = await self._do_json(
            "DELETE",
            f"/nodes/{subscriber.node_id}/users/{subscriber.user_id}/subscriptions/{channel.node_id}/{channel.user_id}",
            token,
            None,
            {200},
        )
        return subscription_from_http(_expect_dict(response, "unsubscribe channel response"))

    async def list_subscriptions(self, token: str, subscriber: UserRef) -> list[Subscription]:
        validate_user_ref(subscriber, "subscriber")
        response = await self._do_json(
            "GET",
            f"/nodes/{subscriber.node_id}/users/{subscriber.user_id}/subscriptions",
            token,
            None,
            {200},
        )
        items = _items_from_payload(response, "items")
        return [subscription_from_http(_expect_dict(item, "subscription item")) for item in items]

    async def list_messages(self, token: str, target: UserRef, limit: int = 0) -> list[Message]:
        validate_user_ref(target, "target")
        query = f"?limit={limit}" if limit > 0 else ""
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
        response = await self._do_json("GET", "/cluster/nodes", token, None, {200})
        items = _items_from_payload(response, "nodes", "items")
        return [cluster_node_from_http(_expect_dict(item, "cluster node item")) for item in items]

    async def list_node_logged_in_users(self, token: str, node_id: int) -> list[LoggedInUser]:
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
        validate_user_ref(owner, "owner")
        validate_user_ref(blocked, "blocked")
        response = await self._do_json(
            "POST",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/blacklist",
            token,
            {"blocked_node_id": blocked.node_id, "blocked_user_id": blocked.user_id},
            {200, 201},
        )
        return blacklist_entry_from_http(_expect_dict(response, "block user response"))

    async def unblock_user(self, token: str, owner: UserRef, blocked: UserRef) -> BlacklistEntry:
        validate_user_ref(owner, "owner")
        validate_user_ref(blocked, "blocked")
        response = await self._do_json(
            "DELETE",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/blacklist/{blocked.node_id}/{blocked.user_id}",
            token,
            None,
            {200},
        )
        return blacklist_entry_from_http(_expect_dict(response, "unblock user response"))

    async def list_blocked_users(self, token: str, owner: UserRef) -> list[BlacklistEntry]:
        validate_user_ref(owner, "owner")
        response = await self._do_json(
            "GET",
            f"/nodes/{owner.node_id}/users/{owner.user_id}/blacklist",
            token,
            None,
            {200},
        )
        items = _items_from_payload(response, "items")
        return [blacklist_entry_from_http(_expect_dict(item, "blocked user item")) for item in items]

    async def list_events(self, token: str, after: int = 0, limit: int = 0) -> list[Event]:
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
        response = await self._do_json("GET", "/ops/status", token, None, {200})
        return operations_status_from_http(_expect_dict(response, "operations status response"))

    async def metrics(self, token: str) -> str:
        return await self._do_text("GET", "/metrics", token, {200})

    async def _do_json(
        self,
        method: str,
        path: str,
        token: str,
        body: dict[str, Any] | None,
        statuses: set[int],
    ) -> Any:
        text = await self._request(method, path, token, body, statuses)
        if text.strip() == "":
            return None
        try:
            return json.loads(text)
        except json.JSONDecodeError as exc:
            raise ProtocolError(f"invalid JSON response: {exc}") from exc

    async def _do_text(self, method: str, path: str, token: str, statuses: set[int]) -> str:
        return await self._request(method, path, token, None, statuses)

    async def _request(
        self,
        method: str,
        path: str,
        token: str,
        body: dict[str, Any] | None,
        statuses: Iterable[int],
    ) -> str:
        headers: dict[str, str] = {}
        if token != "":
            headers["Authorization"] = f"Bearer {token}"
        try:
            response = await self._client.request(
                method,
                self.base_url + path,
                headers=headers,
                json=body,
            )
        except httpx.HTTPError as exc:
            raise ConnectionError(f"{method} {path}", exc) from exc
        if response.status_code not in statuses:
            raise ProtocolError(f"unexpected HTTP status {response.status_code}: {response.text.strip()}")
        return response.text


def _json_bytes_to_value(data: bytes, field: str) -> Any:
    try:
        return json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"{field} must be valid JSON") from exc


def _expect_dict(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ProtocolError(f"unexpected {label}")
    return value


def _items_from_payload(value: Any, *keys: str) -> list[Any]:
    if isinstance(value, list):
        return value
    if not isinstance(value, dict):
        raise ProtocolError("unexpected list response")
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, list):
            return candidate
    raise ProtocolError("missing items in list response")
