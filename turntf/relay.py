"""Relay 点对点传输层。

在 Client 的瞬时消息（Packet）之上实现点对点连接，
支持三种可靠性模式：BestEffort、AtLeastOnce、ReliableOrdered。
"""

from __future__ import annotations

import asyncio
import os
import time
from typing import TYPE_CHECKING, AsyncIterator, Callable

from .errors import RelayError
from .mapping import decode_relay_envelope, encode_relay_envelope
from .types import (
    DeliveryMode,
    Packet,
    RelayConfig,
    RelayEnvelope,
    RelayKind,
    RelayState,
    Reliability,
    SessionRef,
    UserRef,
)

if TYPE_CHECKING:
    from .client import AsyncClient

# Relay 错误码常量
RELAY_ERROR_OPEN_TIMEOUT = "open_timeout"
RELAY_ERROR_ACK_TIMEOUT = "ack_timeout"
RELAY_ERROR_MAX_RETRANSMIT = "max_retransmit"
RELAY_ERROR_IDLE_TIMEOUT = "idle_timeout"
RELAY_ERROR_REMOTE_CLOSE = "remote_close"
RELAY_ERROR_CLIENT_CLOSED = "client_closed"
RELAY_ERROR_PROTOCOL = "protocol_error"
RELAY_ERROR_DUPLICATE_OPEN = "duplicate_open"
RELAY_ERROR_NOT_CONNECTED = "not_connected"
RELAY_ERROR_SEND_TIMEOUT = "send_timeout"
RELAY_ERROR_RECEIVE_TIMEOUT = "receive_timeout"

# 接收队列最大容量
_RECV_QUEUE_MAXSIZE = 256


def _new_relay_id() -> str:
    """生成新的随机 relay 连接 ID。"""
    return os.urandom(16).hex()


def _now_ms() -> int:
    """返回当前时间的毫秒时间戳。"""
    return int(time.time() * 1000)


class Relay:
    """Relay 连接管理器。

    管理基于 AsyncClient 的 relay 连接，负责入站连接分发和出站连接创建。
    每个 AsyncClient 实例关联一个 Relay 管理器（懒初始化）。
    """

    def __init__(self, client: AsyncClient) -> None:
        """初始化 Relay 管理器。

        Args:
            client: 关联的 AsyncClient 实例。
        """
        self._client = client
        self._conns: dict[str, "RelayConnection"] = {}
        self._lock = asyncio.Lock()
        self._on_conn: Callable[["RelayConnection"], None] | None = None

    def on_connection(self, handler: Callable[["RelayConnection"], None]) -> None:
        """注册入站 relay 连接的处理器。

        每个新入站连接会调用 handler。

        Args:
            handler: 接收 RelayConnection 的回调函数。
        """
        self._on_conn = handler

    async def connect(
        self,
        target: UserRef,
        config: RelayConfig | None = None,
    ) -> RelayConnection:
        """向目标用户发起 relay 连接。

        自动解析目标用户的在线会话并选择支持瞬时消息的会话。

        Args:
            target: 目标用户的 UserRef。
            config: 连接配置，为 None 时使用默认配置。

        Returns:
            已建立的 RelayConnection。

        Raises:
            RelayError: 连接失败时抛出。
        """
        sessions = await self._client.resolve_user_sessions(target)

        target_session: SessionRef | None = None
        for s in sessions.sessions:
            if s.transient_capable:
                target_session = s.session
                break

        if target_session is None:
            raise RelayError(
                RELAY_ERROR_NOT_CONNECTED,
                "no transient-capable session found for target user",
            )

        cfg = config if config is not None else RelayConfig()
        relay_id = _new_relay_id()

        login_info = self._client.login_info
        if login_info is None:
            raise RelayError(RELAY_ERROR_NOT_CONNECTED, "client not logged in")

        conn = RelayConnection(
            relay=self,
            relay_id=relay_id,
            state=RelayState.OPENING,
            config=cfg,
            remote_peer=target,
            remote_session=target_session,
            my_session=login_info.session_ref,
        )

        async with self._lock:
            self._conns[relay_id] = conn

        open_env = RelayEnvelope(
            relay_id=relay_id,
            kind=RelayKind.OPEN,
            sender_session=login_info.session_ref,
            target_session=target_session,
            sent_at_ms=_now_ms(),
        )
        await conn._send_relay_envelope(open_env)

        conn._start_background_tasks()

        try:
            await asyncio.wait_for(
                conn._open_event.wait(),
                timeout=cfg.open_timeout_ms / 1000,
            )
        except asyncio.TimeoutError:
            conn._abort(
                RelayError(
                    RELAY_ERROR_OPEN_TIMEOUT,
                    "OPEN timeout waiting for OPEN_ACK",
                ),
            )
            raise RelayError(
                RELAY_ERROR_OPEN_TIMEOUT,
                "OPEN timeout waiting for OPEN_ACK",
            ) from None

        return conn

    async def accept_incoming(self, env: RelayEnvelope) -> None:
        """将入站 OPEN 帧转换为新的 RelayConnection 并通知用户处理器。

        Args:
            env: 入站的 OPEN 帧。
        """
        cfg = RelayConfig()
        login_info = self._client.login_info
        if login_info is None:
            return

        conn = RelayConnection(
            relay=self,
            relay_id=env.relay_id,
            state=RelayState.OPEN,
            config=cfg,
            remote_peer=UserRef(node_id=0, user_id=0),
            remote_session=env.sender_session,
            my_session=login_info.session_ref,
        )
        conn._open_event.set()

        # Check for duplicate OPEN outside the store
        async with self._lock:
            existing = self._conns.get(env.relay_id)

        if existing is not None:
            # 字典序小的保留，大的关闭
            if env.relay_id < existing.relay_id:
                existing._abort(
                    RelayError(
                        RELAY_ERROR_DUPLICATE_OPEN,
                        "concurrent OPEN, keeping lower relay_id",
                    ),
                )
            else:
                conn._abort(
                    RelayError(
                        RELAY_ERROR_DUPLICATE_OPEN,
                        "concurrent OPEN, keeping lower relay_id",
                    ),
                )
                return

        async with self._lock:
            self._conns[env.relay_id] = conn

        handler = self._on_conn

        conn._start_background_tasks()

        open_ack_env = RelayEnvelope(
            relay_id=env.relay_id,
            kind=RelayKind.OPEN_ACK,
            sender_session=conn._my_session,
            target_session=conn._remote_session,
            sent_at_ms=_now_ms(),
        )
        await conn._send_relay_envelope(open_ack_env)

        if handler is not None:
            await handler(conn)

    async def handle_packet(self, packet: Packet) -> bool:
        """检查 packet body 是否为 relay 帧，是则分发到对应连接。

        Args:
            packet: 收到的数据包。

        Returns:
            如果 packet body 是 relay 帧则返回 True，否则返回 False。
        """
        try:
            env = decode_relay_envelope(packet.body)
        except Exception:
            return False

        async with self._lock:
            conn = self._conns.get(env.relay_id)

        if env.kind == RelayKind.OPEN:
            if conn is None:
                await self.accept_incoming(env)
            return True

        if env.kind == RelayKind.OPEN_ACK:
            if conn is not None and conn.state == RelayState.OPENING:
                conn._state = RelayState.OPEN
                conn._remote_session = env.sender_session
                conn._open_event.set()
            return True

        if env.kind == RelayKind.CLOSE:
            if conn is not None:
                conn._handle_close(
                    RelayError(
                        RELAY_ERROR_REMOTE_CLOSE,
                        "remote peer closed connection",
                    ),
                )
            return True

        if env.kind == RelayKind.ERROR:
            if conn is not None:
                err_msg = (
                    env.payload.decode("utf-8", errors="replace")
                    if env.payload
                    else "unknown"
                )
                conn._handle_close(
                    RelayError(
                        RELAY_ERROR_PROTOCOL,
                        f"remote peer error: {err_msg}",
                    ),
                )
            return True

        if conn is not None:
            await conn._handle_envelope(env)
        return True

    async def remove_connection(self, relay_id: str) -> None:
        """从管理器中移除连接。

        Args:
            relay_id: 要移除的连接 ID。
        """
        async with self._lock:
            self._conns.pop(relay_id, None)


class RelayConnection:
    """Relay 点对点连接。

    提供可靠或尽力而为的数据传输通道。
    状态机: Closed -> Opening -> Open -> Closing -> Closed
    """

    def __init__(
        self,
        relay: Relay,
        relay_id: str,
        state: RelayState,
        config: RelayConfig,
        remote_peer: UserRef,
        remote_session: SessionRef,
        my_session: SessionRef,
    ) -> None:
        """初始化 RelayConnection。

        Args:
            relay: 所属的 Relay 管理器。
            relay_id: 连接的唯一标识。
            state: 初始状态。
            config: 连接配置。
            remote_peer: 对端用户引用。
            remote_session: 对端会话引用。
            my_session: 本端会话引用。
        """
        self._relay = relay
        self._relay_id = relay_id
        self._state = state
        self._config = config
        self._remote_peer = remote_peer
        self._remote_session = remote_session
        self._my_session = my_session

        # 滑动窗口状态
        self._send_base: int = 0
        self._next_seq: int = 0
        self._unacked: dict[int, bytes] = {}
        self._expected_seq: int = 1
        self._recv_buf: dict[int, bytes] = {}
        self._retrans_cnt: int = 0

        # 队列
        chunk_size = 1024
        queue_size = max(1, config.send_buffer_size // chunk_size)
        self._send_queue: asyncio.Queue[bytes | None] = asyncio.Queue(
            maxsize=queue_size,
        )
        self._recv_queue: asyncio.Queue[bytes] = asyncio.Queue(
            maxsize=_RECV_QUEUE_MAXSIZE,
        )

        # 事件和条件变量
        self._open_event = asyncio.Event()
        self._close_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._window_cond = asyncio.Condition(self._lock)

        # 后台任务
        self._send_task: asyncio.Task[None] | None = None
        self._retransmit_task: asyncio.Task[None] | None = None

        # 关闭回调
        self._on_close: list[Callable[[RelayError | None], None]] = []

    def _start_background_tasks(self) -> None:
        """启动发送循环和重传定时器后台任务。"""
        self._send_task = asyncio.create_task(self._send_loop())
        if self._config.reliability != Reliability.BEST_EFFORT:
            self._retransmit_task = asyncio.create_task(self._retransmit_loop())

    @property
    def relay_id(self) -> str:
        """返回连接的唯一标识。"""
        return self._relay_id

    @property
    def state(self) -> RelayState:
        """返回当前连接状态。"""
        return self._state

    @property
    def remote_peer(self) -> UserRef:
        """返回对端用户引用。"""
        return self._remote_peer

    @property
    def remote_session(self) -> SessionRef:
        """返回对端会话引用。"""
        return self._remote_session

    # --- 公开 API ---

    async def send(self, data: bytes) -> None:
        """发送数据。行为取决于配置的可靠性等级。

        当发送缓冲区满时，若 send_timeout_ms > 0 则最多等待该时长后抛出超时异常。

        Args:
            data: 要发送的字节数据。

        Raises:
            RelayError: 如果连接未打开，或发送超时。
        """
        if len(data) == 0:
            return
        if self._state != RelayState.OPEN:
            raise RelayError(
                RELAY_ERROR_NOT_CONNECTED, "connection not open",
            )
        if self._config.send_timeout_ms > 0:
            try:
                await asyncio.wait_for(
                    self._send_queue.put(data),
                    timeout=self._config.send_timeout_ms / 1000.0,
                )
            except asyncio.TimeoutError:
                raise RelayError(
                    RELAY_ERROR_SEND_TIMEOUT,
                    "send timeout waiting for buffer space",
                ) from None
        else:
            await self._send_queue.put(data)

    async def receive(self) -> bytes | None:
        """接收对端发送的数据。

        Returns:
            接收到的数据，如果连接已关闭则返回 None。
        """
        get_task = asyncio.create_task(self._recv_queue.get())
        close_task = asyncio.create_task(self._close_event.wait())
        done, pending = await asyncio.wait(
            [get_task, close_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, asyncio.QueueEmpty):
                pass
        if get_task in done and not get_task.cancelled():
            try:
                return get_task.result()
            except asyncio.QueueEmpty:
                return None
        return None

    async def receive_timeout(self, timeout_ms: int = 0) -> bytes:
        """从连接接收数据，支持超时。

        超时后抛出 RelayError("receive_timeout")。
        timeout_ms 为 0 时使用配置的 receive_timeout_ms，
        若两者均为 0 则无限等待。

        Args:
            timeout_ms: 超时毫秒数，0 表示使用配置值。

        Returns:
            接收到的字节数据。

        Raises:
            RelayError: 如果接收超时。
        """
        t = timeout_ms or self._config.receive_timeout_ms
        try:
            if t > 0:
                return await asyncio.wait_for(
                    self._recv_queue.get(),
                    timeout=t / 1000.0,
                )
            else:
                return await self._recv_queue.get()
        except asyncio.TimeoutError:
            raise RelayError(
                RELAY_ERROR_RECEIVE_TIMEOUT,
                "receive timeout",
            ) from None

    def __aiter__(self) -> AsyncIterator[bytes]:
        """支持 ``async for`` 迭代接收数据。"""
        return _RelayAsyncIterator(self)

    async def close(self) -> None:
        """优雅关闭连接，发送 CLOSE 帧并清理状态。"""
        if self._state != RelayState.OPEN:
            return
        self._state = RelayState.CLOSING

        close_env = RelayEnvelope(
            relay_id=self._relay_id,
            kind=RelayKind.CLOSE,
            sender_session=self._my_session,
            target_session=self._remote_session,
            sent_at_ms=_now_ms(),
        )
        await self._send_relay_envelope(close_env)
        self._handle_close(None)

    def abort(self, reason: RelayError | None = None) -> None:
        """强制关闭连接，不等待确认。

        Args:
            reason: 关闭原因。
        """
        self._abort(reason)

    def on_close(self, fn: Callable[[RelayError | None], None]) -> None:
        """注册连接关闭回调。

        Args:
            fn: 关闭时调用的回调函数，接收可选的 RelayError 参数。
        """
        self._on_close.append(fn)

    # --- 内部方法 ---

    def _abort(self, reason: RelayError | None = None) -> None:
        """内部强制关闭实现。"""
        self._handle_close(reason)

    def _handle_close(self, reason: RelayError | None = None) -> None:
        """内部关闭处理。

        Args:
            reason: 关闭原因。
        """
        if self._state == RelayState.CLOSED:
            return
        self._state = RelayState.CLOSED
        callbacks = list(self._on_close)

        self._close_event.set()

        if self._send_task is not None:
            self._send_task.cancel()
        if self._retransmit_task is not None:
            self._retransmit_task.cancel()

        # 清理完成后安排从管理器中移除
        asyncio.create_task(self._relay.remove_connection(self._relay_id))

        for fn in callbacks:
            try:
                fn(reason)
            except Exception:
                pass

    async def _send_relay_envelope(self, env: RelayEnvelope) -> None:
        """发送 relay 帧（封装为 Packet）。

        Args:
            env: 要发送的 RelayEnvelope。
        """
        body = encode_relay_envelope(env)
        dm = self._config.delivery_mode
        if dm == DeliveryMode.UNSPECIFIED:
            dm = DeliveryMode.ROUTE_RETRY
        await self._relay._client.send_packet(
            target=self._remote_peer,
            body=body,
            delivery_mode=dm,
            target_session=self._remote_session,
        )

    async def _handle_envelope(self, env: RelayEnvelope) -> None:
        """处理接收到的 relay 帧。

        Args:
            env: 接收到的 RelayEnvelope。
        """
        if env.kind == RelayKind.DATA:
            await self._handle_data(env)
        elif env.kind == RelayKind.ACK:
            await self._handle_ack(env)
        elif env.kind == RelayKind.PING:
            await self._handle_ping(env)

    async def _handle_data(self, env: RelayEnvelope) -> None:
        """处理 DATA 帧。

        Args:
            env: 包含数据的 RelayEnvelope。
        """
        if self._config.reliability == Reliability.BEST_EFFORT:
            try:
                self._recv_queue.put_nowait(env.payload)
            except asyncio.QueueFull:
                pass

        elif self._config.reliability == Reliability.AT_LEAST_ONCE:
            ack_env = RelayEnvelope(
                relay_id=self._relay_id,
                kind=RelayKind.ACK,
                sender_session=self._my_session,
                target_session=self._remote_session,
                ack_seq=env.seq,
                sent_at_ms=_now_ms(),
            )
            await self._send_relay_envelope(ack_env)
            try:
                self._recv_queue.put_nowait(env.payload)
            except asyncio.QueueFull:
                pass

        elif self._config.reliability == Reliability.RELIABLE_ORDERED:
            async with self._lock:
                if env.seq == self._expected_seq:
                    self._deliver_ordered(env.payload)
                    self._expected_seq += 1
                    while self._expected_seq in self._recv_buf:
                        data = self._recv_buf.pop(self._expected_seq)
                        self._deliver_ordered(data)
                        self._expected_seq += 1
                elif env.seq > self._expected_seq:
                    if env.seq - self._expected_seq < self._config.window_size:
                        self._recv_buf[env.seq] = env.payload

            ack_env = RelayEnvelope(
                relay_id=self._relay_id,
                kind=RelayKind.ACK,
                sender_session=self._my_session,
                target_session=self._remote_session,
                ack_seq=env.seq,
                sent_at_ms=_now_ms(),
            )
            await self._send_relay_envelope(ack_env)

    def _deliver_ordered(self, data: bytes) -> None:
        """非阻塞地投递有序数据到接收队列。

        Args:
            data: 要投递的数据。
        """
        try:
            self._recv_queue.put_nowait(data)
        except asyncio.QueueFull:
            pass

    async def _handle_ack(self, env: RelayEnvelope) -> None:
        """处理 ACK 帧。

        Args:
            env: 包含确认信息的 RelayEnvelope。
        """
        if self._config.reliability == Reliability.BEST_EFFORT:
            return

        async with self._window_cond:
            if env.ack_seq >= self._send_base:
                for seq in range(self._send_base, env.ack_seq + 1):
                    self._unacked.pop(seq, None)
                self._send_base = env.ack_seq + 1
                self._retrans_cnt = 0
                self._window_cond.notify_all()

    async def _handle_ping(self, env: RelayEnvelope) -> None:
        """处理 PING 帧，回复 ERROR 帧。

        Args:
            env: PING 帧。
        """
        err_env = RelayEnvelope(
            relay_id=self._relay_id,
            kind=RelayKind.ERROR,
            sender_session=self._my_session,
            target_session=self._remote_session,
            sent_at_ms=_now_ms(),
        )
        await self._send_relay_envelope(err_env)

    async def _send_loop(self) -> None:
        """发送循环后台任务。

        从发送队列取出数据，根据可靠性模式处理后发送。
        """
        try:
            while True:
                data = await self._send_queue.get()

                if self._config.reliability == Reliability.BEST_EFFORT:
                    env = RelayEnvelope(
                        relay_id=self._relay_id,
                        kind=RelayKind.DATA,
                        sender_session=self._my_session,
                        target_session=self._remote_session,
                        payload=data,
                        sent_at_ms=_now_ms(),
                    )
                    await self._send_relay_envelope(env)
                    continue

                # AtLeastOnce / ReliableOrdered: 滑动窗口发送
                async with self._window_cond:
                    while self._next_seq - self._send_base >= self._config.window_size:
                        try:
                            await asyncio.wait_for(
                                self._window_cond.wait(),
                                timeout=self._config.ack_timeout_ms / 1000,
                            )
                        except asyncio.TimeoutError:
                            pass

                    seq = self._next_seq
                    self._next_seq += 1
                    self._unacked[seq] = data
                    if self._send_base == 0:
                        self._send_base = seq

                env = RelayEnvelope(
                    relay_id=self._relay_id,
                    kind=RelayKind.DATA,
                    sender_session=self._my_session,
                    target_session=self._remote_session,
                    seq=seq,
                    payload=data,
                    sent_at_ms=_now_ms(),
                )
                await self._send_relay_envelope(env)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            self._handle_close(
                RelayError(
                    RELAY_ERROR_PROTOCOL,
                    f"send loop error: {exc}",
                ),
            )

    async def _retransmit_loop(self) -> None:
        """重传定时器后台任务。

        定期检查未确认帧并进行重传，达到最大重传次数时断开连接。
        仅在 AtLeastOnce / ReliableOrdered 模式下运行。
        """
        try:
            while True:
                await asyncio.sleep(self._config.ack_timeout_ms / 1000)
                ok = await self._retransmit()
                if not ok:
                    return
        except asyncio.CancelledError:
            pass

    async def _retransmit(self) -> bool:
        """重传所有未确认帧。

        Returns:
            True 表示成功执行重传，False 表示已达最大重传次数、连接已关闭。
        """
        async with self._lock:
            if not self._unacked:
                return True

            self._retrans_cnt += 1
            if self._retrans_cnt > self._config.max_retransmits:
                self._handle_close(
                    RelayError(
                        RELAY_ERROR_MAX_RETRANSMIT,
                        "max retransmits exceeded",
                    ),
                )
                return False

            send_base = self._send_base
            next_seq = self._next_seq
            unacked = dict(self._unacked)

        for seq in range(send_base, next_seq):
            data = unacked.get(seq)
            if data is None:
                continue
            try:
                env = RelayEnvelope(
                    relay_id=self._relay_id,
                    kind=RelayKind.DATA,
                    sender_session=self._my_session,
                    target_session=self._remote_session,
                    seq=seq,
                    payload=data,
                    sent_at_ms=_now_ms(),
                )
                await self._send_relay_envelope(env)
            except Exception:
                pass
        return True


class _RelayAsyncIterator:
    """RelayConnection 的异步迭代器实现，支持 ``async for``。"""

    def __init__(self, conn: RelayConnection) -> None:
        """初始化迭代器。

        Args:
            conn: 要迭代的 RelayConnection。
        """
        self._conn = conn

    def __aiter__(self) -> _RelayAsyncIterator:
        return self

    async def __anext__(self) -> bytes:
        data = await self._conn.receive()
        if data is None:
            raise StopAsyncIteration
        return data
