"""All tests are isolated: no exchange network, credentials or legacy app imports."""

import socket
from contextvars import ContextVar

import pytest


@pytest.fixture(autouse=True)
def deny_network(monkeypatch):
    original = socket.socket.connect
    original_pair = socket.socketpair
    building_self_pipe = ContextVar("building_asyncio_self_pipe", default=False)

    def local_pair(*args, **kwargs):
        # Windows implements socketpair with a private loopback connection.
        token = building_self_pipe.set(True)
        try:
            return original_pair(*args, **kwargs)
        finally:
            building_self_pipe.reset(token)

    monkeypatch.setattr(socket, "socketpair", local_pair)

    def guarded(sock, address):
        internal_pair = building_self_pipe.get() and address[0] in {"127.0.0.1", "::1"}
        if sock.family in {socket.AF_INET, socket.AF_INET6} and not internal_pair:
            raise AssertionError("network calls are forbidden in this test suite")
        return original(sock, address)

    monkeypatch.setattr(socket.socket, "connect", guarded)

    def deny_dns(*args, **kwargs):
        raise AssertionError("DNS/network calls are forbidden in this test suite")

    monkeypatch.setattr(socket, "getaddrinfo", deny_dns)
