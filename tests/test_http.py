from __future__ import annotations

import asyncio
import base64
import json

import bcrypt
import httpx

from turntf import AsyncHTTPClient, CreateUserRequest, DeliveryMode, UpdateUserRequest, UserRef, plain_password


def test_http_client_requests_and_encoding() -> None:
    async def main() -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            method = request.method
            body = json.loads(request.content.decode("utf-8")) if request.content else None

            if path == "/auth/login":
                assert method == "POST"
                assert body is not None
                password = body["password"]
                assert password != "root"
                assert bcrypt.checkpw(b"root", password.encode("utf-8"))
                return httpx.Response(200, json={"token": "admin-token"})

            if path == "/users" and method == "POST":
                assert request.headers["Authorization"] == "Bearer admin-token"
                assert body is not None
                password = body["password"]
                assert password != "alice-password"
                assert bcrypt.checkpw(b"alice-password", password.encode("utf-8"))
                assert "profile" in body
                assert "profile_json" not in body
                return httpx.Response(
                    201,
                    json={
                        "node_id": 4096,
                        "user_id": 1025,
                        "username": body["username"],
                        "role": body["role"],
                        "profile": {"tier": "gold"},
                        "created_at": "hlc-created",
                    },
                )

            if path == "/nodes/4096/users/1025" and method == "GET":
                return httpx.Response(
                    200,
                    json={
                        "node_id": 4096,
                        "user_id": 1025,
                        "username": "alice",
                        "role": "user",
                        "profile": {"tier": "gold"},
                    },
                )

            if path == "/nodes/4096/users/1025" and method == "PATCH":
                assert body is not None
                password = body["password"]
                assert password != "new-password"
                assert bcrypt.checkpw(b"new-password", password.encode("utf-8"))
                return httpx.Response(
                    200,
                    json={
                        "node_id": 4096,
                        "user_id": 1025,
                        "username": body["username"],
                        "role": body["role"],
                        "profile": body["profile"],
                    },
                )

            if path == "/nodes/4096/users/1025" and method == "DELETE":
                return httpx.Response(200, json={"status": "deleted", "node_id": 4096, "user_id": 1025})

            if path == "/nodes/4096/users/1025/attachments/channel_subscription/4096/2025" and method == "PUT":
                return httpx.Response(
                    201,
                    json={
                        "owner": {"node_id": 4096, "user_id": 1025},
                        "subject": {"node_id": 4096, "user_id": 2025},
                        "attachment_type": "channel_subscription",
                        "config_json": {},
                        "attached_at": "hlc-sub",
                        "origin_node_id": 4096,
                    },
                )

            if path == "/nodes/4096/users/1025/attachments" and method == "GET" and request.url.params.get("attachment_type") == "channel_subscription":
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                "owner": {"node_id": 4096, "user_id": 1025},
                                "subject": {"node_id": 4096, "user_id": 2025},
                                "attachment_type": "channel_subscription",
                                "config_json": {},
                                "attached_at": "hlc-sub",
                                "origin_node_id": 4096,
                            }
                        ],
                        "count": 1,
                    },
                )

            if path == "/nodes/4096/users/1025/attachments/channel_subscription/4096/2025" and method == "DELETE":
                return httpx.Response(
                    200,
                    json={
                        "owner": {"node_id": 4096, "user_id": 1025},
                        "subject": {"node_id": 4096, "user_id": 2025},
                        "attachment_type": "channel_subscription",
                        "config_json": {},
                        "attached_at": "hlc-sub",
                        "deleted_at": "hlc-unsub",
                        "origin_node_id": 4096,
                    },
                )

            if path == "/nodes/4096/users/1025/messages" and method == "GET":
                assert request.url.params["limit"] == "20"
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                "recipient": {"node_id": 4096, "user_id": 1025},
                                "node_id": 4096,
                                "seq": 3,
                                "sender": {"node_id": 4096, "user_id": 1},
                                "body": base64.b64encode(b"\xff\x00").decode("ascii"),
                                "created_at": "hlc1",
                            }
                        ],
                        "count": 1,
                    },
                )

            if path == "/nodes/4096/users/1025/messages" and method == "POST":
                assert body is not None
                assert body["body"] == base64.b64encode(b"\xff\x00").decode("ascii")
                return httpx.Response(
                    201,
                    json={
                        "recipient": {"node_id": 4096, "user_id": 1025},
                        "node_id": 4096,
                        "seq": 4,
                        "sender": {"node_id": 4096, "user_id": 1},
                        "body": base64.b64encode(b"\xff\x00").decode("ascii"),
                        "created_at": "hlc2",
                    },
                )

            if path == "/nodes/8192/users/1025/messages" and method == "POST":
                assert body is not None
                assert body["delivery_kind"] == "transient"
                assert body["delivery_mode"] == "route_retry"
                return httpx.Response(
                    202,
                    json={
                        "packet_id": 77,
                        "source_node_id": 4096,
                        "target_node_id": 8192,
                        "recipient": {"node_id": 8192, "user_id": 1025},
                        "delivery_mode": "route_retry",
                    },
                )

            if path == "/cluster/nodes":
                return httpx.Response(
                    200,
                    json=[
                        {"node_id": 4096, "is_local": True},
                        {
                            "node_id": 8192,
                            "is_local": False,
                            "configured_url": "ws://127.0.0.1:9081/internal/cluster/ws",
                            "source": "discovered",
                        },
                    ],
                )

            if path == "/cluster/nodes/4096/logged-in-users":
                return httpx.Response(
                    200,
                    json=[
                        {"node_id": 4096, "user_id": 1025, "username": "alice"},
                        {"node_id": 4096, "user_id": 1026, "username": "bob"},
                    ],
                )

            if path == "/nodes/4096/users/1025/attachments/user_blacklist/4096/1027" and method == "PUT":
                return httpx.Response(
                    201,
                    json={
                        "owner": {"node_id": 4096, "user_id": 1025},
                        "subject": {"node_id": 4096, "user_id": 1027},
                        "attachment_type": "user_blacklist",
                        "config_json": {},
                        "attached_at": "hlc-blocked",
                        "origin_node_id": 4096,
                    },
                )

            if path == "/nodes/4096/users/1025/attachments" and method == "GET" and request.url.params.get("attachment_type") == "user_blacklist":
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                "owner": {"node_id": 4096, "user_id": 1025},
                                "subject": {"node_id": 4096, "user_id": 1027},
                                "attachment_type": "user_blacklist",
                                "config_json": {},
                                "attached_at": "hlc-blocked",
                                "origin_node_id": 4096,
                            }
                        ],
                        "count": 1,
                    },
                )

            if path == "/nodes/4096/users/1025/attachments/user_blacklist/4096/1027" and method == "DELETE":
                return httpx.Response(
                    200,
                    json={
                        "owner": {"node_id": 4096, "user_id": 1025},
                        "subject": {"node_id": 4096, "user_id": 1027},
                        "attachment_type": "user_blacklist",
                        "config_json": {},
                        "attached_at": "hlc-blocked",
                        "deleted_at": "hlc-unblocked",
                        "origin_node_id": 4096,
                    },
                )

            if path == "/events":
                return httpx.Response(
                    200,
                    json={
                        "items": [
                            {
                                "sequence": 1,
                                "event_id": 2,
                                "event_type": "user_created",
                                "aggregate": "user",
                                "aggregate_node_id": 4096,
                                "aggregate_id": 1025,
                                "hlc": "hlc-event",
                                "origin_node_id": 4096,
                                "event": {"tier": "gold"},
                            }
                        ],
                        "count": 1,
                    },
                )

            if path == "/ops/status":
                return httpx.Response(
                    200,
                    json={
                        "node_id": 4096,
                        "message_window_size": 128,
                        "last_event_sequence": 99,
                        "write_gate_ready": True,
                        "conflict_total": 1,
                        "message_trim": {"trimmed_total": 2, "last_trimmed_at": "hlc-trim"},
                        "projection": {"pending_total": 3, "last_failed_at": "hlc-fail"},
                        "event_log_trim": {"trimmed_total": 4, "last_trimmed_at": "hlc-log-trim"},
                        "peers": [
                            {
                                "node_id": 8192,
                                "configured_url": "ws://127.0.0.1:9081/internal/cluster/ws",
                                "source": "discovered",
                                "discovered_url": "ws://127.0.0.1:9081/internal/cluster/ws",
                                "discovery_state": "connected",
                                "last_discovered_at": "hlc-discovered",
                                "last_connected_at": "hlc-connected",
                                "last_discovery_error": "previous error",
                                "connected": True,
                                "session_direction": "outbound",
                                "origins": [
                                    {
                                        "origin_node_id": 4096,
                                        "acked_event_id": 9,
                                        "applied_event_id": 8,
                                        "unconfirmed_events": 1,
                                        "cursor_updated_at": "hlc-cursor",
                                        "remote_last_event_id": 10,
                                        "pending_catchup": False,
                                    }
                                ],
                            }
                        ],
                    },
                )

            if path == "/metrics":
                return httpx.Response(200, text="notifier_write_gate_ready 1\n")

            raise AssertionError(f"unexpected route: {method} {path}")

        transport = httpx.MockTransport(handler)
        inner = httpx.AsyncClient(transport=transport)
        client = AsyncHTTPClient("http://turntf.test", client=inner)

        token = await client.login(4096, 1, "root")
        assert token == "admin-token"

        user = await client.create_user(
            token,
            CreateUserRequest(
                username="alice",
                password=plain_password("alice-password"),
                profile_json=b'{"tier":"gold"}',
                role="user",
            ),
        )
        assert user.user_id == 1025
        assert user.profile_json == b'{"tier":"gold"}'

        fetched = await client.get_user(token, UserRef(node_id=4096, user_id=1025))
        assert fetched.username == "alice"

        updated = await client.update_user(
            token,
            UserRef(node_id=4096, user_id=1025),
            UpdateUserRequest(
                username="alice-2",
                password=plain_password("new-password"),
                profile_json=b'{"tier":"platinum"}',
                role="admin",
            ),
        )
        assert updated.username == "alice-2"
        assert updated.profile_json == b'{"tier":"platinum"}'

        subscription = await client.create_subscription(
            token,
            UserRef(node_id=4096, user_id=1025),
            UserRef(node_id=4096, user_id=2025),
        )
        assert subscription.channel.user_id == 2025

        subscriptions = await client.list_subscriptions(token, UserRef(node_id=4096, user_id=1025))
        assert len(subscriptions) == 1

        removed = await client.unsubscribe_channel(
            token,
            UserRef(node_id=4096, user_id=1025),
            UserRef(node_id=4096, user_id=2025),
        )
        assert removed.deleted_at == "hlc-unsub"

        messages = await client.list_messages(token, UserRef(node_id=4096, user_id=1025), 20)
        assert len(messages) == 1
        assert messages[0].body == b"\xff\x00"
        assert messages[0].created_at_hlc == "hlc1"

        message = await client.post_message(token, UserRef(node_id=4096, user_id=1025), b"\xff\x00")
        assert message.seq == 4

        packet = await client.post_packet(
            token,
            8192,
            UserRef(node_id=8192, user_id=1025),
            b"\xff\x00",
            DeliveryMode.ROUTE_RETRY,
        )
        assert packet.packet_id == 77

        nodes = await client.list_cluster_nodes(token)
        assert len(nodes) == 2
        assert nodes[1].source == "discovered"

        users = await client.list_node_logged_in_users(token, 4096)
        assert [user.username for user in users] == ["alice", "bob"]

        blocked = await client.block_user(
            token,
            UserRef(node_id=4096, user_id=1025),
            UserRef(node_id=4096, user_id=1027),
        )
        assert blocked.blocked.user_id == 1027

        blocked_items = await client.list_blocked_users(token, UserRef(node_id=4096, user_id=1025))
        assert len(blocked_items) == 1

        unblocked = await client.unblock_user(
            token,
            UserRef(node_id=4096, user_id=1025),
            UserRef(node_id=4096, user_id=1027),
        )
        assert unblocked.deleted_at == "hlc-unblocked"

        events = await client.list_events(token)
        assert events[0].event_json == b'{"tier":"gold"}'

        status = await client.operations_status(token)
        assert status.event_log_trim is not None
        assert status.event_log_trim.trimmed_total == 4
        assert status.peers[0].origins[0].remote_last_event_id == 10

        metrics = await client.metrics(token)
        assert "notifier_write_gate_ready" in metrics

        deleted = await client.delete_user(token, UserRef(node_id=4096, user_id=1025))
        assert deleted.status == "deleted"
        assert deleted.user.user_id == 1025

        await client.close()
        await inner.aclose()

    asyncio.run(main())


def test_http_client_list_node_logged_in_users_requires_node_id() -> None:
    async def main() -> None:
        inner = httpx.AsyncClient(transport=httpx.MockTransport(lambda _: httpx.Response(200)))
        client = AsyncHTTPClient("http://127.0.0.1:8080", client=inner)
        try:
            try:
                await client.list_node_logged_in_users("token", 0)
            except ValueError as exc:
                assert "node_id is required" in str(exc)
            else:
                raise AssertionError("expected validation error")
        finally:
            await client.close()
            await inner.aclose()

    asyncio.run(main())
