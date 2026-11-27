"""Optional authenticated read-only API hosted by the standalone collector."""

import hmac
import os
import re
import socket
import threading
from http.server import BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlsplit

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.collector_api import (
    BATCH_PATH,
    HISTORY_PATH,
    MAX_COLLECTOR_REQUEST_TARGET,
    MAX_COLLECTOR_RESPONSE_BYTES,
    STREAM_PATH,
    CollectorResponseTooLarge,
    encode_collector_response,
    read_collector_batch,
    read_collector_history,
    read_collector_stream,
)
from omega.runtime.collector_series_api import (
    SERIES_AGGREGATE_PATH,
    SERIES_DEFINITION_PATH,
    SERIES_STREAM_PATH,
    read_series_aggregate,
    read_series_definition,
    read_series_stream,
)
from omega.runtime.local_http import BoundedHTTPServer

MAX_REQUEST_TARGET = MAX_COLLECTOR_REQUEST_TARGET
MAX_RESPONSE_BYTES = MAX_COLLECTOR_RESPONSE_BYTES


def read_api_token(path: Path) -> str:
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(1025)
        if len(raw) > 1024:
            raise ValidationError("invalid collector API token file")
        token = raw.decode("ascii").strip()
    except (OSError, UnicodeError):
        raise ValidationError("collector API token file is unavailable") from None
    if not re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token):
        raise ValidationError("invalid collector API token file")
    return token


class _CollectorHTTPServer(BoundedHTTPServer):
    def server_bind(self):
        if os.name != "nt":
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        super().server_bind()


class CollectorAPIServer:
    """Lifecycle wrapper; the HTTP worker pool and timeout come from local_http."""

    def __init__(self, source, token: str, port: int = 0, *, series_store=None):
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValidationError("invalid collector API port")
        if not isinstance(token, str) or not re.fullmatch(r"[A-Za-z0-9_-]{43,128}", token):
            raise ValidationError("invalid collector API token")
        self.source, self.token = source, token
        owner = self

        class Handler(BaseHTTPRequestHandler):
            server_version = "OmegaCollector"
            sys_version = ""

            def log_message(self, _format, *_args):
                return

            def do_GET(self):
                target = self.path
                if not isinstance(target, str) or len(target) > MAX_REQUEST_TARGET:
                    self._send(414, b"")
                    return
                authorization = self.headers.get_all("Authorization", [])
                supplied = authorization[0] if len(authorization) == 1 else ""
                expected = "Bearer " + owner.token
                try:
                    authorized = hmac.compare_digest(supplied, expected)
                except (TypeError, UnicodeError):
                    authorized = False
                if not authorized:
                    self._send(401, b"", extra={"WWW-Authenticate": "Bearer"})
                    return
                try:
                    parsed = urlsplit(target)
                except ValueError:
                    self._send(400, b"")
                    return
                if parsed.fragment or parsed.scheme or parsed.netloc:
                    self._send(400, b"")
                    return
                sources = {
                    HISTORY_PATH: (read_collector_history, owner.source),
                    BATCH_PATH: (read_collector_batch, owner.source),
                    STREAM_PATH: (read_collector_stream, owner.source),
                    SERIES_STREAM_PATH: (read_series_stream, series_store),
                    SERIES_DEFINITION_PATH: (read_series_definition, series_store),
                    SERIES_AGGREGATE_PATH: (read_series_aggregate, series_store),
                }
                reader, source = sources.get(parsed.path, (None, None))
                if source is None:
                    self._send(404, b"")
                    return
                try:
                    payload = encode_collector_response(reader(source, parsed.query))
                    if not isinstance(payload, bytes) or len(payload) > MAX_RESPONSE_BYTES:
                        raise ValueError("bounded response exceeded")
                except CollectorResponseTooLarge:
                    self._send(413 if parsed.path == BATCH_PATH else 400, b"")
                    return
                except ValidationError:
                    self._send(400, b"")
                    return
                except LookupError:
                    missing_series = parsed.path in {
                        SERIES_AGGREGATE_PATH,
                        SERIES_DEFINITION_PATH,
                        SERIES_STREAM_PATH,
                    }
                    self._send(404 if missing_series else 500, b"")
                    return
                except StorageError:
                    self._send(503, b"")
                    return
                except Exception:
                    self._send(500, b"")
                    return
                self._send(200, payload, content_type="application/json; charset=utf-8")

            def _send(
                self,
                status,
                body,
                *,
                content_type="application/json; charset=utf-8",
                extra=None,
            ):
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.send_header("Connection", "close")
                for key, value in (extra or {}).items():
                    self.send_header(key, value)
                self.end_headers()
                try:
                    self.wfile.write(body)
                except OSError:
                    pass

        self.server = _CollectorHTTPServer(("127.0.0.1", port), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._closed = False

    @property
    def port(self) -> int:
        return self.server.server_port

    def start(self) -> None:
        self.thread.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self.thread.is_alive():
                self.server.shutdown()
                self.thread.join(timeout=6)
                if self.thread.is_alive():
                    raise RuntimeError("collector API server did not stop")
        finally:
            self.server.server_close()
