"""Bounded JSON frames over a blocking Pipe; no polling Queue or pickle payloads."""

import json
from multiprocessing.connection import Connection, wait

from omega.domain.errors import ProtocolError

MAX_MESSAGE_BYTES = 65_536


def send(connection: Connection, message: dict) -> None:
    try:
        raw = json.dumps(message, allow_nan=False, separators=(",", ":")).encode()
    except (TypeError, ValueError):
        raise ProtocolError("IPC accepts JSON values only") from None
    if len(raw) > MAX_MESSAGE_BYTES:
        raise ProtocolError("IPC frame exceeds byte limit")
    connection.send_bytes(raw)


def receive(connection: Connection, timeout: float | None = None) -> dict:
    if not wait([connection], timeout):
        raise TimeoutError("IPC deadline elapsed")
    try:
        raw = connection.recv_bytes(MAX_MESSAGE_BYTES)
        message = json.loads(raw)
        if not isinstance(message, dict) or not isinstance(message.get("id"), str):
            raise ProtocolError("IPC message identity required")
        return message
    except (OSError, ValueError, UnicodeError):
        raise ProtocolError("invalid or oversized IPC frame") from None
