"""Private bounded request/response sockets for the market collector and data manager."""

import json
import math
import os
import socket
import socketserver
import stat
import struct
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from threading import BoundedSemaphore, Thread
from uuid import uuid4

from omega.domain.errors import (
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.infrastructure.candles import DatabaseUnavailable
from omega.runtime.control import InstanceLock

MAX_FRAME = 16 * 1024 * 1024


class BackfillPaused(TransportError):
    pass


ERRORS = {
    error.__name__: error
    for error in (
        ProtocolError,
        StorageError,
        TransportError,
        ValidationError,
        DatabaseUnavailable,
        BackfillPaused,
    )
}


def _read_exact(sock, n, deadline):
    result = bytearray()
    while len(result) < n:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("market IPC frame deadline expired")
        sock.settimeout(remaining)
        chunk = sock.recv(min(65536, n - len(result)))
        if not chunk:
            raise TransportError("incomplete market IPC frame")
        result.extend(chunk)
    return result


def _reject_constant(_value):
    raise ValueError("non-finite JSON constant")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("non-finite JSON number")
    return number


def _unique_fields(pairs):
    fields = dict(pairs)
    if len(fields) != len(pairs):
        raise ValueError("duplicate JSON field")
    return fields


def _read(sock, timeout):
    deadline = time.monotonic() + timeout
    size = struct.unpack("!I", _read_exact(sock, 4, deadline))[0]
    if not 1 <= size <= MAX_FRAME:
        raise ProtocolError("market IPC frame exceeds its bound")
    try:
        return json.loads(
            _read_exact(sock, size, deadline),
            parse_constant=_reject_constant,
            parse_float=_finite_float,
            object_pairs_hook=_unique_fields,
        )
    except (UnicodeError, ValueError, RecursionError):
        raise ProtocolError("invalid market IPC document") from None


def _write(sock, document):
    try:
        data = json.dumps(document, allow_nan=False, separators=(",", ":")).encode()
    except (OverflowError, TypeError, ValueError):
        raise ProtocolError("market IPC accepts finite JSON values") from None
    if not 1 <= len(data) <= MAX_FRAME:
        raise ProtocolError("market IPC frame exceeds its bound")
    sock.sendall(struct.pack("!I", len(data)))
    sock.sendall(data)


class MarketSocket:
    def __init__(self, path, *, response_timeout=10):
        self.path = os.fspath(path)
        self.response_timeout = response_timeout

    def call(self, operation, **arguments):
        identity = uuid4().hex
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                connection.settimeout(2)
                connection.connect(self.path)
                connection.settimeout(self.response_timeout)
                _write(connection, {"id": identity, "operation": operation, "arguments": arguments})
                reply = _read(connection, self.response_timeout)
        except (OSError, TransportError):
            raise DatabaseUnavailable("local market peer is unavailable") from None
        if not isinstance(reply, dict) or reply.get("id") != identity:
            raise ProtocolError("market IPC response identity mismatch")
        if "value" in reply:
            return reply["value"]
        if reply.get("error") == "RateLimited":
            raise RateLimited(reply.get("retry_after"))
        error = ERRORS.get(reply.get("error"), ProtocolError)
        raise error("market IPC operation failed")


class _MarketServer(socketserver.TCPServer):
    address_family = getattr(socket, "AF_UNIX", socket.AF_INET)
    request_queue_size = 16

    def __init__(self, path, dispatch, capacity):
        self.dispatch = dispatch
        self.slots = BoundedSemaphore(capacity)
        super().__init__(path, _MarketRequest)
        self.pool = ThreadPoolExecutor(max_workers=capacity, thread_name_prefix="market-ipc")

    def process_request(self, request, address):
        if not self.slots.acquire(blocking=False):
            self.shutdown_request(request)
            return
        try:
            self.pool.submit(self._handle, request, address)
        except BaseException:
            self.slots.release()
            raise

    def _handle(self, request, address):
        try:
            self.finish_request(request, address)
        except Exception:
            self.handle_error(request, address)
        finally:
            self.shutdown_request(request)
            self.slots.release()

    def server_close(self):
        super().server_close()
        if hasattr(self, "pool"):
            self.pool.shutdown()


class _MarketRequest(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            request = _read(self.request, 12)
            if (
                not isinstance(request, dict)
                or not isinstance(request.get("id"), str)
                or not isinstance(request.get("operation"), str)
                or not isinstance(request.get("arguments"), dict)
            ):
                raise ProtocolError("invalid market IPC request")
            reply = {"id": request["id"]}
            try:
                reply["value"] = self.server.dispatch(request["operation"], request["arguments"])
            except RateLimited as error:
                reply.update(error="RateLimited", retry_after=error.retry_after)
            except tuple(ERRORS.values()) as error:
                reply["error"] = type(error).__name__
            except InterruptedError:
                reply["error"] = "TransportError"
            except (KeyError, TypeError, ValueError, ArithmeticError):
                reply["error"] = "ValidationError"
            self.request.settimeout(12)
            _write(self.request, reply)
        except (OSError, ProtocolError, TransportError):
            pass


@contextmanager
def serve_market_socket(path, dispatch, *, capacity=8):
    if not hasattr(socket, "AF_UNIX"):
        raise ValidationError("this platform does not support market Unix sockets")
    path = Path(path)
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = path.parent.lstat()
    if not stat.S_ISDIR(directory.st_mode) or directory.st_mode & 0o077:
        raise StorageError("market IPC directory must be private")
    with_lock = InstanceLock(path.with_suffix(".lock"))
    try:
        try:
            stale = path.lstat()
        except FileNotFoundError:
            pass
        else:
            if not stat.S_ISSOCK(stale.st_mode):
                raise StorageError("market IPC entry is not a socket")
            path.unlink()
        with _MarketServer(os.fspath(path), dispatch, capacity) as server:
            path.chmod(0o600)
            thread = Thread(target=server.serve_forever, name=path.stem, daemon=True)
            thread.start()
            try:
                yield
            finally:
                server.shutdown()
                thread.join()
        path.unlink(missing_ok=True)
    finally:
        with_lock.close()
