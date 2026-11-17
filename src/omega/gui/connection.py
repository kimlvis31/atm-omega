"""Non-blocking loopback client; the GUI owns no broker, DB or strategy objects."""

# Imports
import json
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QObject, QTimer, QUrl, Signal
from PySide6.QtNetwork import QNetworkAccessManager, QNetworkProxy, QNetworkReply, QNetworkRequest

from omega.gui.models import read_connection, validate_state

MAX_RESPONSE_BYTES = 4 * 1024 * 1024


class CoreClient(QObject):
    stateReceived = Signal(dict)
    candlesReceived = Signal(str, dict)
    modulesReceived = Signal(dict)
    # _finished validates the decoded dict once. Share that read-only snapshot
    # with tagged consumers, rather than converting its whole nested catalog to
    # and from QVariantMap for every unrelated page connected to this signal.
    resultReceived = Signal(str, object)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)

    def __init__(self, root: Path, parent: QObject | None = None):
        super().__init__(parent)
        self.root = root.resolve()
        self.manager = QNetworkAccessManager(self)
        self.manager.setProxy(QNetworkProxy(QNetworkProxy.ProxyType.NoProxy))
        self.url = ""
        self._token = ""
        self._generation = 0
        self._pending: dict[str, QNetworkReply] = {}
        self._attached = False
        self._closed = False
        self.state: dict = {}
        self.timer = QTimer(self)
        self.timer.setInterval(1500)
        self.timer.timeout.connect(self.poll)

    @property
    def attached(self) -> bool:
        return self._attached

    def start(self) -> None:
        self._closed = False
        self.timer.start()
        self.poll()

    def stop(self) -> None:
        self._closed = True
        self.timer.stop()
        self._generation += 1
        for reply in tuple(self._pending.values()):
            reply.abort()
        self._pending.clear()
        self._set_attached(False)

    def _set_attached(self, attached: bool) -> None:
        if self._attached != attached:
            self._attached = attached
            self.connectionChanged.emit(attached)

    def poll(self) -> None:
        if self._closed or "state" in self._pending:
            return
        try:
            url, token = read_connection(self.root)
        except (OSError, ValueError, KeyError, TypeError, UnicodeError):
            self._set_attached(False)
            self.failed.emit(
                "state", "Core is not available. Start the core and keep this window open."
            )
            return
        if (url, token) != (self.url, self._token):
            self._generation += 1
            for reply in tuple(self._pending.values()):
                reply.abort()
            self._pending.clear()
            self.url, self._token = url, token
        self.request("GET", "/api/state", tag="state")

    def request(self, method: str, path: str, payload: dict | None = None, *, tag: str) -> None:
        if self._closed or tag in self._pending:
            return
        if not self.url:
            self.failed.emit(tag, "Core is disconnected")
            return
        if not path.startswith("/api/") or "\r" in path or "\n" in path:
            raise ValueError("invalid API path")
        request = QNetworkRequest(QUrl(self.url + path))
        request.setRawHeader(b"Authorization", ("Bearer " + self._token).encode("ascii"))
        request.setRawHeader(b"Accept", b"application/json")
        request.setTransferTimeout(5000)
        request.setAttribute(
            QNetworkRequest.Attribute.RedirectPolicyAttribute,
            QNetworkRequest.RedirectPolicy.ManualRedirectPolicy,
        )
        generation = self._generation
        if method == "GET":
            reply = self.manager.get(request)
        elif method == "POST":
            request.setHeader(QNetworkRequest.KnownHeaders.ContentTypeHeader, "application/json")
            reply = self.manager.post(request, json.dumps(payload or {}).encode("utf-8"))
        else:
            raise ValueError("only GET and POST are supported")
        reply.setReadBufferSize(MAX_RESPONSE_BYTES + 1)
        self._pending[tag] = reply
        reply.readyRead.connect(
            lambda: reply.abort() if reply.bytesAvailable() > MAX_RESPONSE_BYTES else None
        )
        reply.finished.connect(lambda: self._finished(reply, tag, generation))

    def _finished(self, reply: QNetworkReply, tag: str, generation: int) -> None:
        if self._pending.get(tag) is reply:
            self._pending.pop(tag, None)
        try:
            if self._closed or generation != self._generation:
                return
            status = reply.attribute(QNetworkRequest.Attribute.HttpStatusCodeAttribute)
            if reply.error() != QNetworkReply.NetworkError.NoError:
                if tag == "state":
                    self._set_attached(False)
                self.failed.emit(tag, f"Core request failed ({status or 'connection/timeout'})")
                return
            raw = bytes(reply.readAll())
            if len(raw) > MAX_RESPONSE_BYTES:
                raise ValueError("core response exceeds client limit")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("core response must be an object")
            if status != 200 and status != 201 and status != 202:
                self.failed.emit(tag, str(data.get("error", f"HTTP {status}"))[:300])
                return
            if tag == "state":
                validate_state(data)
                self._set_attached(True)
                self.state = data
                self.stateReceived.emit(data)
            elif tag.startswith("candles:"):
                self.candlesReceived.emit(tag.removeprefix("candles:"), data)
            elif tag == "modules":
                self.modulesReceived.emit(data)
            else:
                self.resultReceived.emit(tag, data)
        except (ValueError, TypeError, UnicodeError) as error:
            if tag == "state":
                self._set_attached(False)
            self.failed.emit(tag, str(error)[:300])
        finally:
            reply.deleteLater()

    def command(self, kind: str, payload: dict | None = None) -> str:
        operation = uuid4().hex
        payload = dict(payload or {})
        if kind in {"pause", "resume"}:
            core = self.state.get("core", {})
            if not self.attached or not core.get("epoch"):
                self.failed.emit("command:" + operation, "Fresh core state is required")
                return operation
            payload.update(
                expected_epoch=core["epoch"],
                expected_revision=core.get("control_revision"),
                expires_at_ms=core.get("server_time_ms", 0) + 30_000,
            )
        self.request(
            "POST",
            "/api/commands",
            {"id": operation, "kind": kind, "payload": payload},
            tag="command:" + operation,
        )
        return operation

    def submit_job(self, kind: str, parameters: dict) -> str:
        tag = "new_job:" + uuid4().hex
        self.request("POST", "/api/jobs", {"kind": kind, "parameters": parameters}, tag=tag)
        return tag
