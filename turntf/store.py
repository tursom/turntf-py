from __future__ import annotations

import asyncio
from typing import Protocol

from .types import Message, MessageCursor, UserRef


class CursorStore(Protocol):
    """游标存储协议的接口定义。

    用于持久化消息游标，以便在客户端重连时能够恢复已接收消息的状态，
    避免重复接收或遗漏消息。

    实现此协议的类型需要提供三个异步方法：
    ``load_seen_messages``、``save_message`` 和 ``save_cursor``。
    """

    async def load_seen_messages(self) -> list[MessageCursor]:
        """加载所有已看到的游标列表。

        在 WebSocket 重连时被调用，将已记录的游标列表发送给服务器，
        使服务器知道哪些消息已被接收。

        Returns:
            已记录的 MessageCursor 列表。
        """
        ...

    async def save_message(self, message: Message) -> None:
        """保存一条消息到存储中。

        Args:
            message: 需要保存的 Message 对象。
        """
        ...

    async def save_cursor(self, cursor: MessageCursor) -> None:
        """保存一个游标到存储中。

        Args:
            cursor: 需要保存的 MessageCursor 对象。
        """
        ...


class MemoryCursorStore:
    """基于内存的游标存储实现。

    使用字典和列表在内存中存储消息和游标信息。
    适用于单次会话的场景，程序重启后数据会丢失。

    注意：如果需要在进程重启后保持游标状态，
    建议实现自己的 ``CursorStore``，使用文件或数据库进行持久化。
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._messages: dict[MessageCursor, Message] = {}
        self._order: list[MessageCursor] = []

    async def load_seen_messages(self) -> list[MessageCursor]:
        """加载所有已记录的游标列表。

        Returns:
            按记录顺序排列的 MessageCursor 列表。
        """
        async with self._lock:
            return list(self._order)

    async def save_message(self, message: Message) -> None:
        """保存消息到内存存储中。

        Args:
            message: 需要保存的 Message 对象。
        """
        async with self._lock:
            self._messages[message.cursor()] = _clone_message(message)

    async def save_cursor(self, cursor: MessageCursor) -> None:
        """保存游标到内存存储中。

        如果游标尚未记录，则会创建一个占位消息并追加到顺序列表中。

        Args:
            cursor: 需要保存的 MessageCursor 对象。
        """
        async with self._lock:
            if cursor not in self._messages:
                self._messages[cursor] = Message(
                    recipient=UserRef(node_id=0, user_id=0),
                    node_id=cursor.node_id,
                    seq=cursor.seq,
                    sender=UserRef(node_id=0, user_id=0),
                    body=b"",
                    created_at_hlc="",
                )
            if cursor not in self._order:
                self._order.append(cursor)

    async def has_cursor(self, cursor: MessageCursor) -> bool:
        """检查游标是否已存在。

        Args:
            cursor: 要检查的 MessageCursor 对象。

        Returns:
            如果游标已存在则返回 True，否则返回 False。
        """
        async with self._lock:
            return cursor in self._messages

    async def message(self, cursor: MessageCursor) -> Message | None:
        """根据游标获取对应的消息。

        Args:
            cursor: 要查询的 MessageCursor 对象。

        Returns:
            如果找到则返回消息的副本，否则返回 None。
        """
        async with self._lock:
            message = self._messages.get(cursor)
            return None if message is None else _clone_message(message)


def _clone_message(message: Message) -> Message:
    return Message(
        recipient=UserRef(node_id=message.recipient.node_id, user_id=message.recipient.user_id),
        node_id=message.node_id,
        seq=message.seq,
        sender=UserRef(node_id=message.sender.node_id, user_id=message.sender.user_id),
        body=bytes(message.body),
        created_at_hlc=message.created_at_hlc,
    )
