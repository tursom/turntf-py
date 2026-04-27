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
    UpdateUserRequest,
    UserRef,
    plain_password,
)
from turntf._generated import client_pb2 as pb
from turntf.client import websocket_url
from turntf.errors import ConnectionError as TurntfConnectionError
from turntf.errors import ServerError
from turntf.store import MemoryCursorStore


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
        self.logins: list[tuple[int, str]] = []
        self.messages: list[Message] = []
        self.packets: list[int] = []
        self.errors: list[str] = []
        self.disconnects: list[str] = []

    async def on_login(self, info) -> None:  # type: ignore[override]
        self.logins.append((info.user.user_id, info.protocol_version))

    async def on_message(self, message: Message) -> None:
        self.messages.append(message)

    async def on_packet(self, packet: Packet) -> None:
        self.packets.append(packet.packet_id)

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
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha1",
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
            message = await client.send_message(UserRef(node_id=4096, user_id=1025), b"payload")
            assert message.seq == 8
            await client.ping()
            await server_task
        finally:
            await client.close()

        assert acked == [(4096, 7)]
        assert handler.logins == [(1025, "client-v1alpha1")]
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
            await conn.server_send(
                pb.ServerEnvelope(
                    login_response=pb.LoginResponse(
                        user=pb.User(node_id=4096, user_id=1025, username="alice", role="user"),
                        protocol_version="client-v1alpha1",
                    )
                )
            )

        server_task = asyncio.create_task(server_logic())
        client = FakeAsyncClient(
            Config(
                base_url="https://turntf.test/base",
                credentials=Credentials(
                    node_id=4096,
                    user_id=1025,
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
        assert observed["path"] == "wss://turntf.test/base/ws/realtime"

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
                    )
                )
            )

            create_req = await conn.server_recv()
            assert bcrypt.checkpw(b"alice-password", create_req.create_user.password.encode("utf-8"))
            await conn.server_send(
                pb.ServerEnvelope(
                    create_user_response=pb.CreateUserResponse(
                        request_id=create_req.create_user.request_id,
                        user=pb.User(node_id=4096, user_id=2025, username="alice", role="user"),
                    )
                )
            )

            update_req = await conn.server_recv()
            assert bcrypt.checkpw(b"new-password", update_req.update_user.password.value.encode("utf-8"))
            await conn.server_send(
                pb.ServerEnvelope(
                    update_user_response=pb.UpdateUserResponse(
                        request_id=update_req.update_user.request_id,
                        user=pb.User(node_id=4096, user_id=2025, username="alice-2", role="admin"),
                    )
                )
            )

            subscribe_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    subscribe_channel_response=pb.SubscribeChannelResponse(
                        request_id=subscribe_req.subscribe_channel.request_id,
                        subscription=pb.Subscription(
                            subscriber=subscribe_req.subscribe_channel.subscriber,
                            channel=subscribe_req.subscribe_channel.channel,
                            subscribed_at="hlc-sub",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            list_subs_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_subscriptions_response=pb.ListSubscriptionsResponse(
                        request_id=list_subs_req.list_subscriptions.request_id,
                        items=[
                            pb.Subscription(
                                subscriber=pb.UserRef(node_id=4096, user_id=1025),
                                channel=pb.UserRef(node_id=4096, user_id=9001),
                                subscribed_at="hlc-sub",
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
                    unsubscribe_channel_response=pb.UnsubscribeChannelResponse(
                        request_id=unsubscribe_req.unsubscribe_channel.request_id,
                        subscription=pb.Subscription(
                            subscriber=unsubscribe_req.unsubscribe_channel.subscriber,
                            channel=unsubscribe_req.unsubscribe_channel.channel,
                            subscribed_at="hlc-sub",
                            deleted_at="hlc-unsub",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            block_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    block_user_response=pb.BlockUserResponse(
                        request_id=block_req.block_user.request_id,
                        entry=pb.BlacklistEntry(
                            owner=block_req.block_user.owner,
                            blocked=block_req.block_user.blocked,
                            blocked_at="hlc-blocked",
                            origin_node_id=4096,
                        ),
                    )
                )
            )

            list_blocked_req = await conn.server_recv()
            await conn.server_send(
                pb.ServerEnvelope(
                    list_blocked_users_response=pb.ListBlockedUsersResponse(
                        request_id=list_blocked_req.list_blocked_users.request_id,
                        items=[
                            pb.BlacklistEntry(
                                owner=pb.UserRef(node_id=4096, user_id=1025),
                                blocked=pb.UserRef(node_id=4096, user_id=2027),
                                blocked_at="hlc-blocked",
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
                    unblock_user_response=pb.UnblockUserResponse(
                        request_id=unblock_req.unblock_user.request_id,
                        entry=pb.BlacklistEntry(
                            owner=unblock_req.unblock_user.owner,
                            blocked=unblock_req.unblock_user.blocked,
                            blocked_at="hlc-blocked",
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
                            pb.LoggedInUser(node_id=4096, user_id=1025, username="alice"),
                            pb.LoggedInUser(node_id=4096, user_id=1026, username="bob"),
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
                    password=plain_password("alice-password"),
                    role="user",
                )
            )
            assert created.user_id == 2025

            updated = await client.update_user(
                UserRef(node_id=4096, user_id=2025),
                UpdateUserRequest(password=plain_password("new-password"), username="alice-2", role="admin"),
            )
            assert updated.username == "alice-2"

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
