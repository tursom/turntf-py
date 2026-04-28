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
    user_ref_to_proto,
)
from .password import PasswordInput, plain_password
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
    SessionRef,
    Subscription,
    UpdateUserRequest,
    User,
    UserRef,
)
from .validation import validate_delivery_mode, validate_positive_int, validate_session_ref, validate_user_ref


class Handler:
    async def on_login(self, info: LoginInfo) -> None:
        return None

    async def on_message(self, message: Message) -> None:
        return None

    async def on_packet(self, packet: Packet) -> None:
        return None

    async def on_error(self, error: BaseException) -> None:
        return None

    async def on_disconnect(self, error: BaseException) -> None:
        return None


class NopHandler(Handler):
    pass


@dataclass(slots=True)
class Config:
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
    def __init__(self, config: Config) -> None:
        if config.base_url.strip() == "":
            raise ValueError("base_url is required")
        validate_positive_int(config.credentials.node_id, "credentials.node_id")
        validate_positive_int(config.credentials.user_id, "credentials.user_id")
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

    @property
    def http(self) -> AsyncHTTPClient:
        return self._http

    @property
    def login_info(self) -> LoginInfo | None:
        return self._login_info

    @property
    def session_ref(self) -> SessionRef | None:
        if self._login_info is None:
            return None
        return self._login_info.session_ref

    async def __aenter__(self) -> "AsyncClient":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def login(self, node_id: int, user_id: int, password: str) -> str:
        return await self._http.login(node_id, user_id, password)

    async def login_with_password(self, node_id: int, user_id: int, password: PasswordInput) -> str:
        return await self._http.login_with_password(node_id, user_id, password)

    async def connect(self) -> None:
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
        await self._rpc(lambda request_id: pb.ClientEnvelope(ping=pb.Ping(request_id=request_id)))

    async def send_message(self, target: UserRef, body: bytes) -> Message:
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
        return await self.send_message(target, body)

    async def send_packet(
        self,
        target: UserRef,
        body: bytes,
        delivery_mode: DeliveryMode,
        *,
        target_session: SessionRef | None = None,
    ) -> RelayAccepted:
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
        return await self.send_packet(
            target,
            body,
            delivery_mode,
            target_session=target_session,
        )

    async def create_user(self, request: CreateUserRequest) -> User:
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
                )
            )
        )
        if not isinstance(result, User):
            raise ProtocolError("missing user in create_user_response")
        return result

    async def create_channel(self, request: CreateUserRequest) -> User:
        role = request.role or "channel"
        return await self.create_user(
            CreateUserRequest(
                username=request.username,
                password=request.password,
                profile_json=request.profile_json,
                role=role,
            )
        )

    async def get_user(self, target: UserRef) -> User:
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
        validate_user_ref(target, "target")

        def build(request_id: int) -> pb.ClientEnvelope:
            message = pb.UpdateUserRequest(request_id=request_id, user=user_ref_to_proto(target))
            if request.username is not None:
                message.username.CopyFrom(pb.StringField(value=request.username))
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
        validate_user_ref(target, "target")
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                delete_user=pb.DeleteUserRequest(request_id=request_id, user=user_ref_to_proto(target))
            )
        )
        if not isinstance(result, DeleteUserResult):
            raise ProtocolError("missing status in delete_user_response")
        return result

    async def upsert_attachment(
        self,
        owner: UserRef,
        subject: UserRef,
        attachment_type: AttachmentType,
        config_json: bytes = b"{}",
    ) -> Attachment:
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
        return await self.subscribe_channel(subscriber, channel)

    async def unsubscribe_channel(self, subscriber: UserRef, channel: UserRef) -> Subscription:
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
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_events=pb.ListEventsRequest(request_id=request_id, after=after, limit=limit)
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_events_response")
        return result

    async def list_cluster_nodes(self) -> list[ClusterNode]:
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                list_cluster_nodes=pb.ListClusterNodesRequest(request_id=request_id)
            )
        )
        if not isinstance(result, list):
            raise ProtocolError("missing items in list_cluster_nodes_response")
        return result

    async def list_node_logged_in_users(self, node_id: int) -> list[LoggedInUser]:
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
        result = await self._rpc(
            lambda request_id: pb.ClientEnvelope(
                operations_status=pb.OperationsStatusRequest(request_id=request_id)
            )
        )
        if not isinstance(result, OperationsStatus):
            raise ProtocolError("missing status in operations_status_response")
        return result

    async def metrics(self) -> str:
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
                user=user_ref_to_proto(
                    UserRef(node_id=self._cfg.credentials.node_id, user_id=self._cfg.credentials.user_id)
                ),
                password=self._cfg.credentials.password.wire_value(),
                transient_only=self._cfg.transient_only,
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
            await self._safe_handler_call(self._cfg.handler.on_packet, packet_from_proto(env.packet_pushed.packet))
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
