"""Defensive bounds for the private market IPC framing protocol."""

import struct

import pytest

from omega.domain.errors import ProtocolError
from omega.runtime import market_ipc


class FragmentedSocket:
    def __init__(self, chunks):
        self.chunks = list(chunks)
        self.pending = b""
        self.timeouts = []

    def settimeout(self, timeout):
        self.timeouts.append(timeout)
        if hasattr(self, "clock"):
            self.clock[0] += 0.6

    def recv(self, size):
        if not self.pending:
            self.pending = self.chunks.pop(0)
        result, self.pending = self.pending[:size], self.pending[size:]
        return result


def test_read_accepts_a_frame_delivered_in_fragments():
    payload = b'{"value":7}'
    wire = struct.pack("!I", len(payload)) + payload
    sock = FragmentedSocket([wire[:1], wire[1:3], wire[3:7], wire[7:]])

    assert market_ipc._read(sock, timeout=5) == {"value": 7}


def test_read_deadline_is_not_reset_by_fragment_progress(monkeypatch):
    now = [0.0]
    monkeypatch.setattr(market_ipc.time, "monotonic", lambda: now[0])
    sock = FragmentedSocket([b"\x00", b"\x00", b"\x00", b"\x02"])
    sock.clock = now

    with pytest.raises(TimeoutError, match="deadline expired"):
        market_ipc._read(sock, timeout=1)

    assert sock.timeouts == pytest.approx([1.0, 0.4])


@pytest.mark.parametrize("constant", [b"NaN", b"Infinity", b"-Infinity"])
def test_read_rejects_non_finite_json_constants(constant):
    sock = FragmentedSocket([struct.pack("!I", len(constant)), constant])

    with pytest.raises(ProtocolError, match="invalid market IPC document"):
        market_ipc._read(sock, timeout=5)


@pytest.mark.parametrize("document", [b"1e9999", b'{"key":1,"key":2}'])
def test_read_rejects_overflow_and_duplicate_object_fields(document):
    sock = FragmentedSocket([struct.pack("!I", len(document)), document])

    with pytest.raises(ProtocolError, match="invalid market IPC document"):
        market_ipc._read(sock, timeout=5)
