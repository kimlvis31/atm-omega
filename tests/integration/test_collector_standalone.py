"""Verify that a standalone collector serves the shared HTTP contract over loopback."""

import _socket
import http.client
import json
import socket
import subprocess
import sys
import threading
from queue import Queue
from urllib.parse import urlencode

from omega.runtime import collector_api, collector_server


def owned_connector(port):
    def connect(address, timeout, source_address=None, **_options):
        assert address == ("127.0.0.1", port) and source_address is None
        value = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        value.settimeout(timeout)
        try:
            _socket.socket.connect(value, address)
        except BaseException:
            value.close()
            raise
        return value

    return connect


def get_owned_loopback(port, path, token=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=3)

    connection._create_connection = owned_connector(port)
    try:
        headers = {} if token is None else {"Authorization": f"Bearer {token}"}
        connection.request("GET", path, headers=headers)
        response = connection.getresponse()
        return response.status, response.read()
    finally:
        connection.close()


def test_authenticated_loopback_collector_api_serves_only_named_read_routes(monkeypatch):
    token = "A" * 43
    seen = []

    def history(source, query):
        seen.append((source, query, "history"))
        return {"kind": "history"}

    def stream(source, query):
        seen.append((source, query, "stream"))
        return {"kind": "stream"}

    monkeypatch.setattr(collector_server, "read_collector_history", history)
    monkeypatch.setattr(collector_server, "read_collector_stream", stream)
    source = object()
    api = collector_server.CollectorAPIServer(source, token, 0)
    api.start()
    try:
        history_path = collector_server.HISTORY_PATH + "?instrument=binance:spot:ETHUSDT"
        status, body = get_owned_loopback(api.port, history_path, token)
        assert status == 200 and json.loads(body) == {"kind": "history"}
        stream_path = collector_server.STREAM_PATH + "?instrument=binance:spot:ETHUSDT"
        assert get_owned_loopback(api.port, stream_path, token)[0] == 200
        assert get_owned_loopback(api.port, collector_server.HISTORY_PATH)[0] == 401
        assert get_owned_loopback(api.port, "/unlisted", token)[0] == 404
        assert [entry[2] for entry in seen] == ["history", "stream"]
        assert all(entry[0] is source for entry in seen)
    finally:
        api.close()
    assert not api.thread.is_alive()


def test_oversized_batch_uses_413_but_history_keeps_400(monkeypatch):
    token = "C" * 43
    large = {"payload": "x" * collector_api.MAX_COLLECTOR_RESPONSE_BYTES}
    monkeypatch.setattr(collector_server, "read_collector_batch", lambda *_: large)
    monkeypatch.setattr(collector_server, "read_collector_history", lambda *_: large)
    api = collector_server.CollectorAPIServer(object(), token, 0)
    api.start()
    try:
        batch = collector_server.BATCH_PATH + "?instrument=binance%3Aspot%3ABTCUSDT"
        history = collector_server.HISTORY_PATH + "?instrument=binance%3Aspot%3ABTCUSDT"
        assert get_owned_loopback(api.port, batch, token)[0] == 413
        assert get_owned_loopback(api.port, history, token)[0] == 400
    finally:
        api.close()


def test_independent_collect_command_serves_history_and_resumes_without_core(tmp_path):
    token_path = tmp_path / "collector-token"
    token = "B" * 43
    token_path.write_text(token, encoding="utf-8")
    database = tmp_path / "other-database.toml"
    script = """
import asyncio
import sys
from decimal import Decimal
from pathlib import Path

from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure import candle_feed
from omega.ports.collection import MinuteObservation
from omega.runtime import collect_cli

token, database = sys.argv[1:]
instrument = Instrument("binance", Market.SPOT, "ETHUSDT")
minute = 60_000

class Store:
    def __init__(self, settings):
        assert settings == Path(database)
    def close(self):
        pass

class Worker:
    def __init__(self, *args, **kwargs):
        pass
    async def run(self, stop):
        await asyncio.to_thread(sys.stdin.readline)

class Source:
    def __init__(self, root, *, config_path):
        assert root == Path(database).parent and config_path == Path(database)
    def close(self):
        pass
    def read_observations(self, keys, start, end, *, limit, as_of_ms):
        assert keys == [instrument.key]
        result = []
        for index, received in [(0, minute + 9), (1, 4 * minute), (2, 3 * minute)]:
            opened = index * minute
            if start <= opened < end and received <= as_of_ms:
                price = Decimal("3.125")
                candle = Candle(
                    instrument, opened, opened + minute, price, price, price, price, Decimal("2")
                )
                result.append(MinuteObservation(candle, "websocket", received))
        return tuple(result)

collect_cli.load_database_settings = lambda path: path
collect_cli.PostgresCandleStore = Store
collect_cli.CollectionWorker = Worker
candle_feed.CommittedCandleSource = Source
sys.argv = [
    "omega-collect", "--market", "spot", "--symbols", "ETHUSDT", "--db-config", database,
    "--api-port", "0", "--api-token-file", token,
]
collect_cli.main()
"""
    ready = Queue()
    with subprocess.Popen(
        [sys.executable, "-I", "-B", "-c", script, str(token_path), str(database)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
    ) as child:
        reader = threading.Thread(target=lambda: ready.put(child.stdout.readline()), daemon=True)
        reader.start()
        try:
            port = json.loads(ready.get(timeout=8))["port"]

            def fetch(path, **parameters):
                query = urlencode({"instrument": "binance:spot:ETHUSDT", **parameters})
                status, body = get_owned_loopback(port, f"{path}?{query}", token)
                assert status == 200
                return json.loads(body)

            history = fetch(
                collector_server.HISTORY_PATH, start_ms=0, end_ms=60_000, as_of_ms=60_009
            )
            assert history["rows"][0]["open"] == "3.125"
            assert history["rows"][0]["received_at_ms"] == 60_009
            assert history["rows"][0]["source"] == "websocket"

            first = fetch(collector_server.STREAM_PATH, next_ms=0, as_of_ms=180_000)
            assert [row["open_ms"] for row in first["rows"]] == [0]
            assert first["state"] == "waiting_for_gap" and first["next_ms"] == 60_000
            resumed = fetch(
                collector_server.STREAM_PATH, next_ms=first["next_ms"], as_of_ms=240_000
            )
            assert [row["open_ms"] for row in resumed["rows"]] == [60_000, 120_000]
            assert resumed["next_ms"] == 180_000
        finally:
            if child.poll() is None:
                child.stdin.close()
                child.stdin = None
            try:
                _stdout, stderr = child.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.communicate(timeout=5)
                raise
            reader.join(timeout=2)
        assert child.returncode == 0, stderr
