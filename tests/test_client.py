from __future__ import annotations

import asyncio
from types import SimpleNamespace

import bcrypt

from turntf import (
    AsyncClient,
    Config,
    CreateUserRequest,
    Credentials,
    DeliveryMode,
    Message,
    MessageCursor,
    NopHandler,
    Packet,
    ScanUserMetadataRequest,
    SessionRef,
    UpdateUserRequest,
    UpsertUserMetadataRequest,
    UserRef,
    plain_password,
)
from turntf._generated import client_pb2 as pb
from turntf.client import websocket_url
from turntf.errors import ConnectionError as TurntfConnectionError
from turntf.errors import ServerError
from turntf.store import MemoryCursorStore


def pb_session_ref(session_id: str, serving_node_id: int = 4096) -> pb.SessionRef:
    return pb.SessionRef(serving_node_id=serving_node_id, session_id=session_id)


class FakeConnection:
    def __init__(self, path: str) -> None:
        self.request = SimpleNamespace(path=path)
        self._client_to_server: asyncio.Queue[object] = asyncio.Queue()
        self._server_to_client: asyncio.Queue[object] = asyncio.Queue()
        self.closed = False

    async def client_send(self, message: pb.ClientEnvelope) -> None:
        await self._client_to_server.put(message)

    async def client_recv(self) -> pb.ServerEnvelope:
        item = await self._server_to_client.get()
        if isinstance(item, BaseException):
            raise item
        assert isinstance(item, pb.ServerEnvelope)
        return item

    async def server_recv(self) -> pb.ClientEnvelope:
        item = await self._client_to_server.get()
        assert isinstance(item, pb.ClientEnvelope)
        return item

    async def server_send(self, message: pb.ServerEnvelope) -> None:
        await self._server_to_client.put(message)

    async def fail_client(self, error: BaseException) -> None:
        await self._server_to_client.put(error)

    async def close(self) -> None:
        self.closed = True


class FakeDialer:
    def __init__(self) -> None:
        self.connections: asyncio.Queue[FakeConnection] = asyncio.Queue()

    async def dial(self, path: str) -> FakeConnection:
        connection = FakeConnection(path)
        await self.connections.put(connection)
        return connection


class FakeAsyncClient(AsyncClient):
    def __init__(self, config: Config, dialer: FakeDialer) -> None:
        super().__init__(config)
        self._dialer = dialer

    async def _dial(self) -> FakeConnection:  # type: ignore[override]
        return await self._dialer.dial(websocket_url(self._cfg.base_url, self._cfg.realtime_stream))

    async def _write_proto(self, ws: FakeConnection, message: object) -> None:  # type: ignore[override]
        assert isinstance(message, pb.ClientEnvelope)
        await ws.client_send(message)

    async def _read_proto(self, ws: FakeConnection) -> pb.ServerEnvelope:  # type: ignore[override]
        return await ws.client_recv()

    async def _safe_close_ws(self, ws: FakeConnection) -> None:  # type: ignore[override]
        await ws.close()


class RecordingStore:
    def __init__(self) -> None:
        self.cursors: list[MessageCursor] = []
        self.saved: list[str] = []

    async def load_seen_messages(self) -> list[MessageCursor]:
        return list(self.cursors)

    async def save_message(self, message: Message) -> None:
        self.saved.append("message")
        if message.cursor() not in self.cursors:
            self.cursors.append(message.cursor())

    async def save_cursor(self, cursor: MessageCursor) -> None:
        self.saved.append("cursor")
        if cursor not in self.cursors:
            self.cursors.append(cursor)


class RecordingHandler(NopHandler):
    def __init__(self) -> None:
        self.logins: list[tuple[int, str, str]] = []
        self.messages: list[Message] = []
        self.packets: list[Packet] = []
        self.errors: list[str] = []
        self.disconnects: list[str] = []

    async def on_login(self, info) -> None:  # type: ignore[override]
        self.logins.append((info.user.user_id, info.protocol_version, info.session_ref.session_id))

    async def on_message(self, message: Message) -> None:
        self.messages.append(message)

    async def on_packet(self, packet: Packet) -> None:
        self.packets.append(packet)

    async def on_error(self, error: BaseException) -> None:
        self.errors.append(str(error))

    async def on_disconnect(self, error: BaseException) -> None:
        self.disconnects.append(str(error))


def test_client_login_message_ack_send_and_ping() -> None:
    async def main() -> None:
        dialer = FakeDialer()
        store = RecordingStore()
        handler = RecordingHandler()
        acked: list[tuple[int, int]] = []

        async def server_logic() -> None:
            conn = await dialer.connections.get()
            login = await conn.server_recv()
            assert login.login.user.node_id == 4096
            assert login.login.user.user_id == 1025
            assert login.login.password != "alice-password"
            assert bcrypt.checkpw(b"alice-password", login.login.password.encode("utf-8"))
            assert list(login.login.seen_messages) == []

            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(
                            node_id=4096,
                            user_id=1025,
                            username="alice",
                            role="user",
                            login_name="alice.login",
                        ),
                        protocol_version="client-v1alpha1",
                        session_ref=pb_session_ref("sess-alice"),
                    )
                )
            )
            await conn.server_send(
                pb.ServerEnvelope(
                    message_pushed=pb.MessagePushed(
                        message=pb.Message(
                            recipient=pb.UserRef(node_id=4096, user_id=1025),
                            node_id=4096,
                            seq=7,
                            sender=pb.UserRef(node_id=4096, user_id=1),
                            body=b"\xff\x00",
                            created_at_hlc="hlc1",
                        )
                    )
                )
            )

            ack = await conn.server_recv()
            acked.append((ack.ack_message.cursor.node_id, ack.ack_message.cursor.seq))

            send_request = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    send_message_response=pb.SendMessageResponse(
                        request_id=send_request.send_message.request_id,
                        message=pb.Message(
                            recipient=pb.UserRef(node_id=4096, user_id=1025),
                            node_id=4096,
                            seq=8,
                            sender=pb.UserRef(node_id=4096, user_id=1025),
                            body=send_request.send_message.body,
                            created_at_hlc="hlc2",
                        ),
                    )
                )
            )

            ping_request = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(pong=pb.Pong(request_id=ping_request.ping.request_id))
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    password=plain_password("alice-password"),
                ),
                cursor_store=store,
                handler=handler,
                request_timeout=1.0,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            assert client.session_ref == SessionRef(serving_node_id=4096, session_id="sess-alice")
            assert client.login_info is not None
            assert client.login_info.user.login_name == "alice.login"
            message = await client.send_message(UserRef(node_id=4096, user_id=1025), b"payload")
            assert message.seq == 8
            await client.ping()
            await server_task
        finally:
            await client.close()

        assert acked == [(4096, 7)]
        assert handler.logins == [(1025, "client-v1alpha1", "sess-alice")]
        assert len(handler.messages) == 1
        assert store.saved == ["message", "cursor", "message", "cursor"]

    asyncio.run(main())


def test_client_transient_only_and_realtime_path() -> None:
    async def main() -> None:
        dialer = FakeDialer()
        observed: dict[str, object] = {}

        async def server_logic() -> None:
            conn = await dialer.connections.get()
            observed["path"] = conn.request.path
            login = await conn.server_recv()
            observed["transient_only"] = login.login.transient_only
            observed["login_name"] = login.login.login_name
            observed["has_user"] = login.login.HasField("user")
            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(
                            node_id=4096,
                            user_id=1025,
                            username="alice",
                            role="user",
                            login_name="alice.login",
                        ),
                        protocol_version="client-v1alpha1",
                        session_ref=pb_session_ref("sess-transient"),
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="https://turntf.test/base",
                credentials=Credentials(
                    login_name="alice.login",
                    password=plain_password("alice-password"),
                ),
                transient_only=True,
                realtime_stream=True,
                request_timeout=1.0,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            await server_task
        finally:
            await client.close()

        assert observed["transient_only"] is True
        assert observed["login_name"] == "alice.login"
        assert observed["has_user"] is False
        assert observed["path"] == "wss://turntf.test/base/ws/realtime"

    asyncio.run(main())


def test_client_resolve_user_sessions_and_session_targeted_packets() -> None:
    async def main() -> None:
        dialer = FakeDialer()
        handler = RecordingHandler()

        async def server_logic() -> None:
            conn = await dialer.connections.get()
            await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha2",
                        session_ref=pb_session_ref("sess-self"),
                    )
                )
            )
            await conn.server_send(
                pb.ServerEnvelope(
                    packet_pushed=pb.PacketPushed(
                        packet=pb.Packet(
                            packet_id=91,
                            source_node_id=8192,
                            target_node_id=12288,
                            recipient=pb.UserRef(node_id=8192, user_id=2048),
                            sender=pb.UserRef(node_id=4096, user_id=1025),
                            body=b"packet-body",
                            delivery_mode=pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY,
                            target_session=pb_session_ref("sess-targeted", serving_node_id=12288),
                        )
                    )
                )
            )

            resolve_req = await conn.server_recv()
            assert resolve_req.resolve_user_sessions.user.node_id == 8192
            assert resolve_req.resolve_user_sessions.user.user_id == 2048
            await conn.server_send(
                pb.ServerEnvelope(
                    resolve_user_sessions_response=pb.ResolveUserSessionsResponse(
                        request_id=resolve_req.resolve_user_sessions.request_id,
                        user=resolve_req.resolve_user_sessions.user,
                        presence=[
                            pb.OnlineNodePresence(
                                serving_node_id=12288,
                                session_count=2,
                                transport_hint="ws",
                            )
                        ],
                        items=[
                            pb.ResolvedSession(
                                session=pb_session_ref("sess-a", serving_node_id=12288),
                                transport="ws",
                                transient_capable=True,
                            ),
                            pb.ResolvedSession(
                                session=pb_session_ref("sess-b", serving_node_id=12288),
                                transport="ws",
                                transient_capable=False,
                            ),
                        ],
                        count=2,
                    )
                )
            )

            packet_req = await conn.server_recv()
            assert packet_req.send_message.delivery_kind == pb.CLIENT_DELIVERY_KIND_TRANSIENT
            assert packet_req.send_message.delivery_mode == pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY
            assert packet_req.send_message.target_session.serving_node_id == 12288
            assert packet_req.send_message.target_session.session_id == "sess-b"
            await conn.server_send(
                pb.ServerEnvelope(
                    send_message_response=pb.SendMessageResponse(
                        request_id=packet_req.send_message.request_id,
                        transient_accepted=pb.TransientAccepted(
                            packet_id=77,
                            source_node_id=4096,
                            target_node_id=12288,
                            recipient=packet_req.send_message.target,
                            delivery_mode=pb.CLIENT_DELIVERY_MODE_ROUTE_RETRY,
                            target_session=packet_req.send_message.target_session,
                        ),
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    password=plain_password("alice-password"),
                ),
                handler=handler,
                request_timeout=1.0,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            assert client.login_info is not None
            assert client.login_info.session_ref == SessionRef(serving_node_id=4096, session_id="sess-self")

            resolved = await client.resolve_user_sessions(UserRef(node_id=8192, user_id=2048))
            assert resolved.user == UserRef(node_id=8192, user_id=2048)
            assert resolved.count == 2
            assert resolved.presence[0].serving_node_id == 12288
            assert resolved.presence[0].session_count == 2
            assert resolved.sessions[0].transient_capable is True
            assert resolved.sessions[1].session == SessionRef(serving_node_id=12288, session_id="sess-b")

            accepted = await client.send_packet(
                UserRef(node_id=8192, user_id=2048),
                b"\x10\x20",
                DeliveryMode.ROUTE_RETRY,
                target_session=resolved.sessions[1].session,
            )
            assert accepted.packet_id == 77
            assert accepted.target_session == SessionRef(serving_node_id=12288, session_id="sess-b")

            await server_task
        finally:
            await client.close()

        assert len(handler.packets) == 1
        assert handler.packets[0].target_session == SessionRef(
            serving_node_id=12288,
            session_id="sess-targeted",
        )

    asyncio.run(main())


def test_client_unauthorized_stops_reconnect() -> None:
    async def main() -> None:
        dialer = FakeDialer()
        attempts = 0

        async def server_logic() -> None:
            nonlocal attempts
            conn = await dialer.connections.get()
            attempts += 1
            await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(error=pb.Error(code="unauthorized", message="bad credentials"))
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(node_id=4096, user_id=1025, password=plain_password("wrong")),
                reconnect=True,
                initial_reconnect_delay=0.01,
                max_reconnect_delay=0.02,
                request_timeout=0.5,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            try:
                await client.connect()
            except ServerError as exc:
                assert exc.unauthorized()
            else:
                raise AssertionError("expected unauthorized error")
            await server_task
            await asyncio.sleep(0.1)
            assert attempts == 1
        finally:
            await client.close()

    asyncio.run(main())


def test_client_reconnect_uses_seen_messages() -> None:
    async def main() -> None:
        dialer = FakeDialer()
        store = RecordingStore()
        attempts = 0
        second_seen: list[tuple[int, int]] = []

        async def server_logic() -> None:
            nonlocal attempts

            first = await dialer.connections.get()
            attempts += 1
            await first.server_recv()
            await first.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha1",
                        session_ref=pb_session_ref("sess-reconnect-1"),
                    )
                )
            )
            await first.server_send(
                pb.ServerEnvelope(
                    message_pushed=pb.MessagePushed(
                        message=pb.Message(
                            recipient=pb.UserRef(node_id=4096, user_id=1025),
                            node_id=4096,
                            seq=11,
                            sender=pb.UserRef(node_id=4096, user_id=1),
                            body=b"hello",
                            created_at_hlc="hlc1",
                        )
                    )
                )
            )
            await first.server_recv()
            await first.fail_client(TurntfConnectionError("read", RuntimeError("disconnect")))

            second = await dialer.connections.get()
            attempts += 1
            login = await second.server_recv()
            second_seen.extend((cursor.node_id, cursor.seq) for cursor in login.login.seen_messages)
            await second.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha1",
                        session_ref=pb_session_ref("sess-reconnect-2"),
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    password=plain_password("alice-password"),
                ),
                cursor_store=store,
                reconnect=True,
                initial_reconnect_delay=0.01,
                max_reconnect_delay=0.02,
                request_timeout=0.5,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            await server_task
            assert attempts == 2
            assert second_seen == [(4096, 11)]
        finally:
            await client.close()

    asyncio.run(main())


def test_client_user_metadata_rpcs() -> None:
    async def main() -> None:
        dialer = FakeDialer()

        def metadata_message(
            key: str,
            value: bytes,
            updated_at: str,
            *,
            deleted_at: str = "",
            expires_at: str = "",
        ) -> pb.UserMetadata:
            return pb.UserMetadata(
                owner=pb.UserRef(node_id=4096, user_id=1025),
                key=key,
                value=value,
                updated_at=updated_at,
                deleted_at=deleted_at,
                expires_at=expires_at,
                origin_node_id=4096,
            )

        async def server_logic() -> None:
            conn = await dialer.connections.get()
            await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha2",
                        session_ref=pb_session_ref("sess-meta"),
                    )
                )
            )

            get_req = await conn.server_recv()
            assert get_req.get_user_metadata.owner.node_id == 4096
            assert get_req.get_user_metadata.owner.user_id == 1025
            assert get_req.get_user_metadata.key == "prefs.theme"
            await conn.server_send(
                pb.ServerEnvelope(
                    get_user_metadata_response=pb.GetUserMetadataResponse(
                        request_id=get_req.get_user_metadata.request_id,
                        metadata=metadata_message(
                            "prefs.theme",
                            b"\x01\x02\x03",
                            "hlc-meta-1",
                            expires_at="2026-05-01T00:00:00Z",
                        ),
                    )
                )
            )

            upsert_req = await conn.server_recv()
            assert upsert_req.upsert_user_metadata.key == "prefs.theme"
            assert upsert_req.upsert_user_metadata.value == b"\x09\x08\x07"
            assert upsert_req.upsert_user_metadata.expires_at.value == "2026-05-01T00:00:00Z"
            await conn.server_send(
                pb.ServerEnvelope(
                    upsert_user_metadata_response=pb.UpsertUserMetadataResponse(
                        request_id=upsert_req.upsert_user_metadata.request_id,
                        metadata=metadata_message(
                            "prefs.theme",
                            bytes(upsert_req.upsert_user_metadata.value),
                            "hlc-meta-2",
                            expires_at=upsert_req.upsert_user_metadata.expires_at.value,
                        ),
                    )
                )
            )

            delete_req = await conn.server_recv()
            assert delete_req.delete_user_metadata.key == "prefs.theme"
            await conn.server_send(
                pb.ServerEnvelope(
                    delete_user_metadata_response=pb.DeleteUserMetadataResponse(
                        request_id=delete_req.delete_user_metadata.request_id,
                        metadata=metadata_message(
                            "prefs.theme",
                            b"\x09\x08\x07",
                            "hlc-meta-3",
                            deleted_at="hlc-meta-delete",
                            expires_at="2026-05-01T00:00:00Z",
                        ),
                    )
                )
            )

            scan_req = await conn.server_recv()
            assert scan_req.scan_user_metadata.prefix == "prefs."
            assert scan_req.scan_user_metadata.after == "prefs.theme"
            assert scan_req.scan_user_metadata.limit == 2
            await conn.server_send(
                pb.ServerEnvelope(
                    scan_user_metadata_response=pb.ScanUserMetadataResponse(
                        request_id=scan_req.scan_user_metadata.request_id,
                        items=[
                            metadata_message(
                                "prefs.theme",
                                b"\x01\x02\x03",
                                "hlc-meta-1",
                                expires_at="2026-05-01T00:00:00Z",
                            ),
                            metadata_message("prefs.volume", b"\x04\x05", "hlc-meta-4"),
                        ],
                        count=2,
                        next_after="prefs.volume",
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    password=plain_password("alice-password"),
                ),
                request_timeout=1.0,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            owner = UserRef(node_id=4096, user_id=1025)

            metadata = await client.get_user_metadata(owner, "prefs.theme")
            assert metadata.value == b"\x01\x02\x03"
            assert metadata.expires_at == "2026-05-01T00:00:00Z"

            upserted = await client.upsert_user_metadata(
                owner,
                "prefs.theme",
                UpsertUserMetadataRequest(
                    value=b"\x09\x08\x07",
                    expires_at="2026-05-01T00:00:00Z",
                ),
            )
            assert upserted.updated_at == "hlc-meta-2"
            assert upserted.value == b"\x09\x08\x07"

            deleted = await client.delete_user_metadata(owner, "prefs.theme")
            assert deleted.deleted_at == "hlc-meta-delete"

            scanned = await client.scan_user_metadata(
                owner,
                ScanUserMetadataRequest(prefix="prefs.", after="prefs.theme", limit=2),
            )
            assert scanned.count == 2
            assert scanned.next_after == "prefs.volume"
            assert scanned.items[1].value == b"\x04\x05"

            await server_task
        finally:
            await client.close()

    asyncio.run(main())


def test_client_management_rpcs_and_password_hashing() -> None:
    async def main() -> None:
        dialer = FakeDialer()

        async def server_logic() -> None:
            conn = await dialer.connections.get()
            await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha2",
                        session_ref=pb_session_ref("sess-admin"),
                    )
                )
            )

            create_req = await conn.server_recv()
            assert bcrypt.checkpw(b"alice-password", create_req.create_user.password.encode("utf-8"))
            assert create_req.create_user.login_name == "alice.login"
            await conn.server_send(
                pb.ServerEnvelope(
                    create_user_response=pb.CreateUserResponse(
                        request_id=create_req.create_user.request_id,
                        user=pb.User(
                            node_id=4096,
                            user_id=2025,
                            username="alice",
                            role="user",
                            login_name="alice.login",
                        ),
                    )
                )
            )

            update_req = await conn.server_recv()
            assert bcrypt.checkpw(b"new-password", update_req.update_user.password.value.encode("utf-8"))
            assert update_req.update_user.HasField("login_name")
            assert update_req.update_user.login_name.value == ""
            await conn.server_send(
                pb.ServerEnvelope(
                    update_user_response=pb.UpdateUserResponse(
                        request_id=update_req.update_user.request_id,
                        user=pb.User(
                            node_id=4096,
                            user_id=2025,
                            username="alice-2",
                            role="admin",
                            login_name="",
                        ),
                    )
                )
            )

            subscribe_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    upsert_user_attachment_response=pb.UpsertUserAttachmentResponse(
                        request_id=subscribe_req.upsert_user_attachment.request_id,
                        attachment=pb.Attachment(
                            owner=subscribe_req.upsert_user_attachment.owner,
                            subject=subscribe_req.upsert_user_attachment.subject,
                            attachment_type=pb.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
                            config_json=b"{}",
                            attached_at="hlc-sub",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            list_subs_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_user_attachments_response=pb.ListUserAttachmentsResponse(
                        request_id=list_subs_req.list_user_attachments.request_id,
                        items=[
                            pb.Attachment(
                                owner=pb.UserRef(node_id=4096, user_id=1025),
                                subject=pb.UserRef(node_id=4096, user_id=9001),
                                attachment_type=pb.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
                                config_json=b"{}",
                                attached_at="hlc-sub",
                                origin_node_id=4096,
                            )
                        ],
                        count=1,
                    )
                )
            )

            unsubscribe_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    delete_user_attachment_response=pb.DeleteUserAttachmentResponse(
                        request_id=unsubscribe_req.delete_user_attachment.request_id,
                        attachment=pb.Attachment(
                            owner=unsubscribe_req.delete_user_attachment.owner,
                            subject=unsubscribe_req.delete_user_attachment.subject,
                            attachment_type=pb.ATTACHMENT_TYPE_CHANNEL_SUBSCRIPTION,
                            config_json=b"{}",
                            attached_at="hlc-sub",
                            deleted_at="hlc-unsub",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            block_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    upsert_user_attachment_response=pb.UpsertUserAttachmentResponse(
                        request_id=block_req.upsert_user_attachment.request_id,
                        attachment=pb.Attachment(
                            owner=block_req.upsert_user_attachment.owner,
                            subject=block_req.upsert_user_attachment.subject,
                            attachment_type=pb.ATTACHMENT_TYPE_USER_BLACKLIST,
                            config_json=b"{}",
                            attached_at="hlc-blocked",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            list_blocked_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_user_attachments_response=pb.ListUserAttachmentsResponse(
                        request_id=list_blocked_req.list_user_attachments.request_id,
                        items=[
                            pb.Attachment(
                                owner=pb.UserRef(node_id=4096, user_id=1025),
                                subject=pb.UserRef(node_id=4096, user_id=2027),
                                attachment_type=pb.ATTACHMENT_TYPE_USER_BLACKLIST,
                                config_json=b"{}",
                                attached_at="hlc-blocked",
                                origin_node_id=4096,
                            )
                        ],
                        count=1,
                    )
                )
            )

            unblock_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    delete_user_attachment_response=pb.DeleteUserAttachmentResponse(
                        request_id=unblock_req.delete_user_attachment.request_id,
                        attachment=pb.Attachment(
                            owner=unblock_req.delete_user_attachment.owner,
                            subject=unblock_req.delete_user_attachment.subject,
                            attachment_type=pb.ATTACHMENT_TYPE_USER_BLACKLIST,
                            config_json=b"{}",
                            attached_at="hlc-blocked",
                            deleted_at="hlc-unblocked",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            nodes_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_cluster_nodes_response=pb.ListClusterNodesResponse(
                        request_id=nodes_req.list_cluster_nodes.request_id,
                        items=[
                            pb.ClusterNode(node_id=4096, is_local=True),
                            pb.ClusterNode(
                                node_id=8192,
                                is_local=False,
                                configured_url="ws://127.0.0.1:9081/internal/cluster/ws",
                                source="discovered",
                            ),
                        ],
                        count=2,
                    )
                )
            )

            users_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_node_logged_in_users_response=pb.ListNodeLoggedInUsersResponse(
                        request_id=users_req.list_node_logged_in_users.request_id,
                        target_node_id=4096,
                        items=[
                            pb.LoggedInUser(
                                node_id=4096,
                                user_id=1025,
                                username="alice",
                                login_name="alice.login",
                            ),
                            pb.LoggedInUser(node_id=4096, user_id=1026, username="bob", login_name=""),
                        ],
                        count=2,
                    )
                )
            )

            events_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_events_response=pb.ListEventsResponse(
                        request_id=events_req.list_events.request_id,
                        items=[
                            pb.Event(
                                sequence=1,
                                event_id=2,
                                event_type="user_created",
                                aggregate="user",
                                aggregate_node_id=4096,
                                aggregate_id=2025,
                                hlc="hlc-event",
                                origin_node_id=4096,
                                event_json=b'{"tier":"gold"}',
                            )
                        ],
                        count=1,
                    )
                )
            )

            ops_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    operations_status_response=pb.OperationsStatusResponse(
                        request_id=ops_req.operations_status.request_id,
                        status=pb.OperationsStatus(
                            node_id=4096,
                            peers=[
                                pb.PeerStatus(
                                    node_id=8192,
                                    configured_url="ws://127.0.0.1:9081/internal/cluster/ws",
                                    connected=True,
                                    source="discovered",
                                    discovered_url="ws://127.0.0.1:9081/internal/cluster/ws",
                                    discovery_state="connected",
                                    last_discovered_at="hlc-discovered",
                                    last_connected_at="hlc-connected",
                                    last_discovery_error="previous error",
                                )
                            ],
                        ),
                    )
                )
            )

            metrics_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    metrics_response=pb.MetricsResponse(
                        request_id=metrics_req.metrics.request_id,
                        text="notifier_write_gate_ready 1\n",
                    )
                )
            )

            delete_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    delete_user_response=pb.DeleteUserResponse(
                        request_id=delete_req.delete_user.request_id,
                        status="deleted",
                        user=delete_req.delete_user.user,
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    password=plain_password("alice-password"),
                ),
                request_timeout=1.0,
                ping_interval=3600.0,
            ),
            dialer,
        )
        try:
            await client.connect()
            created = await client.create_user(
                CreateUserRequest(
                    username="alice",
                    login_name="alice.login",
                    password=plain_password("alice-password"),
                    role="user",
                )
            )
            assert created.user_id == 2025
            assert created.login_name == "alice.login"

            updated = await client.update_user(
                UserRef(node_id=4096, user_id=2025),
                UpdateUserRequest(
                    password=plain_password("new-password"),
                    username="alice-2",
                    role="admin",
                    login_name="",
                ),
            )
            assert updated.username == "alice-2"
            assert updated.login_name == ""

            subscription = await client.subscribe_channel(
                UserRef(node_id=4096, user_id=1025),
                UserRef(node_id=4096, user_id=9001),
            )
            assert subscription.channel.user_id == 9001

            subscriptions = await client.list_subscriptions(UserRef(node_id=4096, user_id=1025))
            assert len(subscriptions) == 1

            removed = await client.unsubscribe_channel(
                UserRef(node_id=4096, user_id=1025),
                UserRef(node_id=4096, user_id=9001),
            )
            assert removed.deleted_at == "hlc-unsub"

            blocked = await client.block_user(
                UserRef(node_id=4096, user_id=1025),
                UserRef(node_id=4096, user_id=2027),
            )
            assert blocked.blocked.user_id == 2027

            blocked_users = await client.list_blocked_users(UserRef(node_id=4096, user_id=1025))
            assert len(blocked_users) == 1

            unblocked = await client.unblock_user(
                UserRef(node_id=4096, user_id=1025),
                UserRef(node_id=4096, user_id=2027),
            )
            assert unblocked.deleted_at == "hlc-unblocked"

            nodes = await client.list_cluster_nodes()
            assert nodes[1].source == "discovered"

            users = await client.list_node_logged_in_users(4096)
            assert users[1].user_id == 1026
            assert users[0].login_name == "alice.login"

            events = await client.list_events()
            assert events[0].event_json == b'{"tier":"gold"}'

            status = await client.operations_status()
            assert status.peers[0].discovery_state == "connected"

            metrics = await client.metrics()
            assert "notifier_write_gate_ready" in metrics

            deleted = await client.delete_user(UserRef(node_id=4096, user_id=2025))
            assert deleted.status == "deleted"

            await server_task
        finally:
            await client.close()

    asyncio.run(main())


def test_client_rejects_mixed_credentials_selectors() -> None:
    try:
        FakeAsyncClient(
            Config(
                base_url="http://turntf.test",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
                    login_name="alice.login",
                    password=plain_password("alice-password"),
                ),
            ),
            FakeDialer(),
        )
    except ValueError as exc:
        assert "exactly one of (node_id,user_id) or login_name" in str(exc)
    else:
        raise AssertionError("expected validation error")


def test_memory_cursor_store_and_generated_proto_import() -> None:
    async def main() -> None:
        store = MemoryCursorStore()
        message = Message(
            recipient=UserRef(node_id=4096, user_id=1025),
            node_id=4096,
            seq=1,
            sender=UserRef(node_id=4096, user_id=1),
            body=b"hello",
            created_at_hlc="hlc1",
        )
        await store.save_message(message)
        await store.save_cursor(message.cursor())
        await store.save_cursor(message.cursor())
        seen = await store.load_seen_messages()
        assert seen == [MessageCursor(node_id=4096, seq=1)]
        stored = await store.message(MessageCursor(node_id=4096, seq=1))
        assert stored is not None
        assert stored.body == b"hello"
        assert pb.ClientEnvelope() is not None
        assert websocket_url("http://127.0.0.1:8080", False) == "ws://127.0.0.1:8080/ws/client"
        assert websocket_url("https://127.0.0.1:8080/base", True) == "wss://127.0.0.1:8080/base/ws/realtime"

    asyncio.run(main())
