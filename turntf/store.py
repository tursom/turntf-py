from __future__ import annotations

import asyncio
from typing import Protocol

from .types import Message, MessageCursor, UserRef


class CursorStore(Protocol):
    async def load_seen_messages(self) -> list[MessageCursor]:
        ...

    async def save_message(self, message: Message) -> None:
        ...

    async def save_cursor(self, cursor: MessageCursor) -> None:
        ...


class MemoryCursorStore:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._messages: dict[MessageCursor, Message] = {}
        self._order: list[MessageCursor] = []

    async def load_seen_messages(self) -> list[MessageCursor]:
        async with self._lock:
            return list(self._order)

    async def save_message(self, message: Message) -> None:
        async with self._lock:
            self._messages[message.cursor()] = _clone_message(message)

    async def save_cursor(self, cursor: MessageCursor) -> None:
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
        async with self._lock:
            return cursor in self._messages

    async def message(self, cursor: MessageCursor) -> Message | None:
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
