"""Deterministic socket/TLS/DNS doubles; never resolve or contact a real endpoint."""

import socket
import ssl
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.webpush_sender import (
    BoundedResolver,
    PinnedSession,
    WebPushSender,
    validate_endpoint,
    validate_subject,
)

ENDPOINT = "https://fcm.googleapis.com/fcm/send/synthetic-token"


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://fcm.googleapis.com/fcm/send/token",
        "https://fcm.googleapis.com.evil/fcm/send/token",
        "https://user@fcm.googleapis.com/fcm/send/token",
        "https://127.0.0.1/private",
        "https://[::1]/private",
        "https://fcm.googleapis.com:443/fcm/send/token",
        "https://FCM.googleapis.com/fcm/send/token",
        "https://fcm.googleapis.com./fcm/send/token",
        ENDPOINT + "?",
        ENDPOINT + "#",
        ENDPOINT + "?token=secret",
        ENDPOINT + "\n",
        "https://fcm.googleapis.com/fcm/send/%2e%2e",
        "https://fcm.googleapis.com/other/token",
        "https://fcm.googleapis.com\\evil/fcm/send/token",
        True,
        None,
    ],
)
def test_endpoint_input_is_strict_provider_and_path_policy(endpoint):
    with pytest.raises(ValidationError):
        validate_endpoint(endpoint)


@pytest.mark.parametrize(
    "endpoint",
    [
        ENDPOINT,
        "https://updates.push.services.mozilla.com/wpush/v2/synthetic-token",
        "https://updates.push.services.mozilla.com/push/v1/synthetic-token",
        "https://web.push.apple.com/synthetic-token",
    ],
)
def test_known_provider_paths_are_accepted_without_resolving(endpoint):
    assert validate_endpoint(endpoint).scheme == "https"


@pytest.mark.parametrize(
    "addresses",
    [
        ["127.0.0.1"],
        ["10.1.1.1"],
        ["169.254.169.254"],
        ["224.0.0.1"],
        ["0.0.0.0"],
        ["8.8.8.8", "127.0.0.1"],
        ["192.0.2.1"],
        ["100.64.0.1"],
    ],
)
def test_every_resolved_address_must_be_public_not_only_the_first(monkeypatch, addresses):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kw: [
            (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, 443))
            for address in addresses
        ],
    )
    resolver = BoundedResolver()
    with pytest.raises(ValueError):
        resolver.resolve("fcm.googleapis.com", time.monotonic() + 1)
    resolver.close()


def test_dns_timeout_contains_one_rootless_slot_and_late_result_never_connects(monkeypatch):
    entered, release = threading.Event(), threading.Event()
    calls = []

    def dns(host, port, **_kwargs):
        calls.append((host, port))
        entered.set()
        release.wait(2)
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))]

    monkeypatch.setattr(socket, "getaddrinfo", dns)
    resolver = BoundedResolver()
    try:
        with pytest.raises(TimeoutError):
            resolver.resolve("fcm.googleapis.com", time.monotonic() + 0.02)
        assert entered.wait(1)
        with pytest.raises(TimeoutError):
            resolver.resolve("web.push.apple.com", time.monotonic() + 1)
        assert resolver.close() == {"rootless_dns_alive": True}
        assert calls == [("fcm.googleapis.com", 443)]
    finally:
        release.set()
        resolver.thread.join(timeout=2)
    assert resolver.close() == {"rootless_dns_alive": False}
    with pytest.raises(TimeoutError):
        resolver.resolve("fcm.googleapis.com", time.monotonic() + 1)


class Resolver:
    def __init__(self):
        self.calls = []

    def resolve(self, host, deadline):
        self.calls.append(host)
        assert deadline > time.monotonic()
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, ("8.8.8.8", 443))]

    def close(self):
        return {"rootless_dns_alive": False}


class FakeSocket:
    def __init__(self, response=b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\n\r\n"):
        self.address, self.writes, self.response = None, [], response
        self.closed = False

    def settimeout(self, timeout):
        assert 0 < timeout <= 5

    def connect(self, address):
        self.address = address

    def getpeername(self):
        return self.address

    def sendall(self, packet):
        self.writes.append(packet)

    def do_handshake(self):
        pass

    def recv(self, limit):
        value, self.response = self.response[:limit], self.response[limit:]
        return value

    def shutdown(self, _how):
        pass

    def close(self):
        self.closed = True


class TLS:
    check_hostname, verify_mode = True, ssl.CERT_REQUIRED

    def __init__(self):
        self.hosts = []

    def wrap_socket(self, sock, *, server_hostname, do_handshake_on_connect):
        assert do_handshake_on_connect is False
        self.hosts.append(server_hostname)
        return sock


def transport(monkeypatch, response=None):
    sock = FakeSocket() if response is None else FakeSocket(response)
    made = []

    def new_socket(*args):
        made.append(args)
        return sock

    monkeypatch.setattr(socket, "socket", new_socket)
    resolver, tls = Resolver(), TLS()
    session = PinnedSession(resolver=resolver, context=tls)
    session.authorize = lambda: True
    return session, sock, resolver, tls, made


def test_transport_pins_exact_sockaddr_preserves_tls_name_and_ignores_proxy_env(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:9")
    monkeypatch.setenv("ALL_PROXY", "http://127.0.0.1:9")
    session, sock, resolver, tls, made = transport(monkeypatch)
    response = session.post(ENDPOINT, data=b"encrypted", headers={"TTL": "60"}, timeout=5)
    assert response.status_code == 201
    assert resolver.calls == ["fcm.googleapis.com"]
    assert sock.address == ("8.8.8.8", 443) and tls.hosts == ["fcm.googleapis.com"]
    assert len(made) == 1 and sock.closed
    assert b"Host: fcm.googleapis.com\r\n" in sock.writes[0]
    assert b"POST /fcm/send/synthetic-token HTTP/1.1\r\n" in sock.writes[0]
    assert response.text == "" and response.headers == {}
    session.close()


def test_redirect_is_returned_without_following_or_leaking_location(monkeypatch):
    session, sock, resolver, _tls, made = transport(
        monkeypatch, b"HTTP/1.1 302 Found\r\nLocation: https://127.0.0.1/private\r\n\r\n"
    )
    result = session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5)
    assert result.status_code == 302 and result.headers == {}
    assert len(made) == len(resolver.calls) == len(sock.writes) == 1
    session.close()


def test_revoked_after_resolution_and_tls_never_submits_http(monkeypatch):
    session, sock, _resolver, _tls, _made = transport(monkeypatch)
    session.authorize = lambda: False
    with pytest.raises(PermissionError):
        session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5)
    assert sock.writes == [] and sock.closed
    session.close()


@pytest.mark.parametrize(
    "headers",
    [
        {"Authorization": "private\r\nInjected: yes"},
        {"Host": "evil"},
        {"Proxy-Authorization": "private"},
        {"Authorization": "x" * 4097},
    ],
)
def test_no_header_injection_or_proxy_authorization(monkeypatch, headers):
    session, sock, *_rest = transport(monkeypatch)
    with pytest.raises(ValueError):
        session.post(ENDPOINT, data=b"encrypted", headers=headers, timeout=5)
    assert sock.writes == []
    session.close()


def test_tls_verification_cannot_be_disabled():
    tls = TLS()
    tls.check_hostname = False
    with pytest.raises(ValueError):
        PinnedSession(resolver=Resolver(), context=tls)


@pytest.mark.parametrize(
    "subject",
    [
        "",
        None,
        "https://contact.example/path",
        "http://contact.example",
        "mailto:fake",
        "mailto:user@example.test\n",
    ],
)
def test_invalid_or_missing_owner_subject_rejected_before_optional_import_or_key(tmp_path, subject):
    with pytest.raises(ValidationError):
        WebPushSender(tmp_path, subject=subject)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("subject", ["https://contact.example", "mailto:synthetic@example.test"])
def test_owner_subject_uses_upstream_strict_supported_shape(subject):
    assert validate_subject(subject) == subject


@pytest.mark.parametrize("address", ["::1", "fe80::1", "fc00::1", "::ffff:8.8.8.8"])
def test_ipv6_private_link_local_or_mapped_destination_is_rejected(monkeypatch, address):
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kw: [
            (socket.AF_INET6, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (address, 443, 0, 0))
        ],
    )
    resolver = BoundedResolver()
    with pytest.raises(ValueError):
        resolver.resolve("fcm.googleapis.com", time.monotonic() + 1)
    resolver.close()


def test_encryption_adapter_uses_public_supplied_session_and_strict_claims(monkeypatch, tmp_path):
    calls = []
    private = object()

    class Vapid:
        def __init__(self, *, private_key):
            assert private_key is private

    class Error(Exception):
        pass

    def webpush(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status_code=201)

    session = SimpleNamespace(authorize=None, close=lambda: {"rootless_dns_alive": False})
    monkeypatch.setitem(sys.modules, "py_vapid", SimpleNamespace(Vapid=Vapid))
    monkeypatch.setitem(
        sys.modules, "pywebpush", SimpleNamespace(webpush=webpush, WebPushException=Error)
    )
    monkeypatch.setattr(
        "omega.infrastructure.webpush_sender.load_vapid", lambda _root: (private, "public")
    )
    sender = WebPushSender(tmp_path, subject="mailto:synthetic@example.test", session=session)
    before = int(time.time())
    assert sender.send({"endpoint": ENDPOINT, "keys": {}}, "generic", 9999, lambda: True) == 201
    call = calls[0]
    assert call["requests_session"] is session
    assert call["content_encoding"] == "aes128gcm"
    assert call["ttl"] == 300 and call["timeout"] == 5.0 and call["verbose"] is False
    assert call["vapid_claims"]["aud"] == "https://fcm.googleapis.com"
    assert before < call["vapid_claims"]["exp"] <= int(time.time()) + 86400
    assert call["vapid_claims"]["sub"] == "mailto:synthetic@example.test"
    assert session.authorize() is False
    assert list(tmp_path.iterdir()) == []


class OriginalCloseFailure(FakeSocket):
    def __init__(self):
        super().__init__()
        self.close_attempts = 0

    def close(self):
        self.close_attempts += 1
        self.closed = True
        if self.close_attempts == 1:
            # Apparent closure does not undo the original operation's failure.
            raise OSError("synthetic original close failure")


class SeparateTLS(TLS):
    def __init__(self, tls):
        super().__init__()
        self.tls = tls

    def wrap_socket(self, raw, *, server_hostname, do_handshake_on_connect):
        assert do_handshake_on_connect is False
        self.hosts.append(server_hostname)
        return self.tls


@pytest.mark.parametrize("failed_owner", ["raw", "tls"])
def test_original_close_failure_retains_exact_owners_and_sticky_dispatch_fence(
    monkeypatch, failed_owner
):
    raw = OriginalCloseFailure() if failed_owner == "raw" else FakeSocket()
    tls = OriginalCloseFailure() if failed_owner == "tls" else FakeSocket()
    created = []

    def new_socket(*_args):
        created.append(raw)
        return raw

    monkeypatch.setattr(socket, "socket", new_socket)
    resolver = Resolver()
    session = PinnedSession(resolver=resolver, context=SeparateTLS(tls))
    session.authorize = lambda: True
    with pytest.raises(StorageError):
        session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5)
    assert len(tls.writes) == 1
    assert session._retirement_fault is True
    assert [owner["socket"] for owner in session._owners] == [raw, tls]
    assert session.sock is tls
    failed = raw if failed_owner == "raw" else tls
    assert failed.close_attempts == 1
    assert any(owner["close_failed"] for owner in session._owners)
    with pytest.raises(StorageError):
        session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5)
    assert resolver.calls == ["fcm.googleapis.com"] and created == [raw]
    with pytest.raises(StorageError):
        session.close()
    assert failed.close_attempts == 2 and raw.closed and tls.closed
    assert all(owner["close_ack"] for owner in session._owners)
    # A later success cannot erase the first failure or manufacture a clean receipt.
    with pytest.raises(StorageError):
        session.close()
    assert failed.close_attempts == 2 and session._retirement_fault is True
    assert [owner["socket"] for owner in session._owners] == [raw, tls]


@pytest.mark.parametrize("tls_close_fails", [False, True])
def test_concurrent_close_during_tls_handoff_cannot_lose_late_actual_tls_owner(
    monkeypatch, tls_close_fails
):
    raw = FakeSocket()
    tls = OriginalCloseFailure() if tls_close_fails else FakeSocket()
    entered, release, close_done = threading.Event(), threading.Event(), threading.Event()
    outcomes, receipts, failures = [], [], []

    class PausedHandoff(SeparateTLS):
        def wrap_socket(self, raw, *, server_hostname, do_handshake_on_connect):
            result = super().wrap_socket(
                raw,
                server_hostname=server_hostname,
                do_handshake_on_connect=do_handshake_on_connect,
            )
            # The actual TLS object exists here, before wrap_socket has returned it.
            entered.set()
            if not release.wait(2):
                raise TimeoutError("synthetic handoff test did not release")
            return result

    monkeypatch.setattr(socket, "socket", lambda *_args: raw)
    session = PinnedSession(resolver=Resolver(), context=PausedHandoff(tls))
    session.authorize = lambda: True

    def attempt():
        try:
            outcomes.append(session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5))
        except Exception as error:
            failures.append(("post", type(error)))

    def close():
        try:
            receipts.append(session.close())
        except Exception as error:
            failures.append(("close", type(error)))
        finally:
            close_done.set()

    poster = threading.Thread(target=attempt)
    closer = threading.Thread(target=close)
    poster.start()
    try:
        assert entered.wait(1)
        closer.start()
        assert session._closing.wait(1)
        assert not close_done.wait(0.05)
    finally:
        release.set()
        poster.join(timeout=2)
        if closer.ident is not None:
            closer.join(timeout=2)
    assert not poster.is_alive() and not closer.is_alive()
    assert raw.closed and tls.closed and tls.writes == [] and outcomes == []
    assert [owner["socket"] for owner in session._owners] == [raw, tls]
    assert all(owner["close_ack"] for owner in session._owners)
    if tls_close_fails:
        assert receipts == []
        assert ("post", StorageError) in failures and ("close", StorageError) in failures
        with pytest.raises(StorageError):
            session.close()
    else:
        assert receipts == [{"rootless_dns_alive": False}]
        assert failures == [("post", TimeoutError)]


def test_close_waits_for_registered_tls_attempt_and_prevents_post_handshake_dispatch(monkeypatch):
    raw = FakeSocket()
    entered, release, close_done = threading.Event(), threading.Event(), threading.Event()
    receipts, failures = [], []

    class PausedHandshake(FakeSocket):
        def do_handshake(self):
            entered.set()
            if not release.wait(2):
                raise TimeoutError("synthetic handshake test did not release")

    tls = PausedHandshake()
    monkeypatch.setattr(socket, "socket", lambda *_args: raw)
    session = PinnedSession(resolver=Resolver(), context=SeparateTLS(tls))
    session.authorize = lambda: True

    def attempt():
        try:
            session.post(ENDPOINT, data=b"encrypted", headers={}, timeout=5)
        except Exception as error:
            failures.append(type(error))

    def close():
        try:
            receipts.append(session.close())
        finally:
            close_done.set()

    poster, closer = threading.Thread(target=attempt), threading.Thread(target=close)
    poster.start()
    try:
        assert entered.wait(1)
        closer.start()
        assert session._closing.wait(1)
        assert not close_done.wait(0.05)
    finally:
        release.set()
        poster.join(timeout=2)
        if closer.ident is not None:
            closer.join(timeout=2)
    assert not poster.is_alive() and not closer.is_alive()
    assert raw.closed and tls.closed and tls.writes == []
    assert failures == [TimeoutError] and receipts == [{"rootless_dns_alive": False}]
