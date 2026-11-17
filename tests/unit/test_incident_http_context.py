"""Actual HTTP handler failures expose bounded context rather than raw requests."""

import json
from io import BytesIO

import pytest

from omega.domain.errors import AuthorizationDenied
from omega.runtime.service import CoreService


def _wire(service, monkeypatch):
    import omega.runtime.service as module

    class Server:
        server_port = 8765

        def serve_forever(self):
            pass

        def shutdown(self):
            pass

        def server_close(self):
            pass

    class Connection:
        def __init__(self, request):
            self.request = request
            self.output = bytearray()

        def makefile(self, *_args):
            return BytesIO(self.request)

        def sendall(self, data):
            self.output.extend(data)

    monkeypatch.setattr(module, "BoundedHTTPServer", lambda *_: Server())
    monkeypatch.setattr(service.notifications, "start", lambda: None)
    service.start()

    def send(raw):
        connection = Connection(raw)
        service.handler_class(connection, ("127.0.0.1", 1), service.server)
        return bytes(connection.output)

    return send


@pytest.mark.parametrize(
    "failure", [OSError(13, "SECRET_IN_EXCEPTION"), TimeoutError("SECRET_IN_EXCEPTION")]
)
def test_handler_failure_keeps_503_critical_without_query_body_or_token(
    tmp_path, monkeypatch, failure
):
    service = CoreService(tmp_path)
    try:
        send = _wire(service, monkeypatch)
        monkeypatch.setattr(service, "request", lambda *_args: (_ for _ in ()).throw(failure))
        body = b'{"password":"SECRET_IN_BODY"}'
        raw = (
            "POST /api/command?private=SECRET_IN_QUERY HTTP/1.1\r\n"
            "Host: 127.0.0.1:8765\r\nContent-Type: application/json\r\n"
            f"Authorization: Bearer {service.token}\r\nContent-Length: {len(body)}\r\n\r\n"
        ).encode() + body
        response = send(raw)
        assert response.startswith(b"HTTP/1.0 503")
        row = service.control.snapshot()["incidents"][0]
        assert row["code"] == "CONTROL_TRANSPORT_OR_STORAGE_FAILURE"
        assert row["severity"] == "critical" and row["observation_scope"] == "current_core"
        assert row["context"]["stage"] == "operation" and row["context"]["operation"] == "POST"
        text = json.dumps(row) + response.decode()
        assert "SECRET_IN_" not in text and service.token not in text
    finally:
        service.close()


@pytest.mark.parametrize("typed_denial", [False, True])
def test_handler_policy_denial_is_403_without_storage_alarm(tmp_path, monkeypatch, typed_denial):
    service = CoreService(tmp_path)
    try:
        send = _wire(service, monkeypatch)
        if typed_denial:
            monkeypatch.setattr(
                service,
                "request",
                lambda *_args: (_ for _ in ()).throw(AuthorizationDenied("SECRET_POLICY")),
            )
        response = send(
            b"GET /api/state HTTP/1.1\r\nHost: 127.0.0.1:8765\r\n"
            b"Authorization: Bearer wrong\r\n\r\n"
        )
        assert response.startswith(b"HTTP/1.0 403")
        assert b"PERMISSION_DENIED" in response and b"SECRET_POLICY" not in response
        assert service.control.snapshot()["incidents"] == []
        assert issubclass(AuthorizationDenied, PermissionError)
        assert not isinstance(PermissionError(13, "storage"), AuthorizationDenied)
    finally:
        service.close()
