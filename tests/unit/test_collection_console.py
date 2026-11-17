"""Console collection uses the existing authenticated owner and original action ID."""

import json
from io import BytesIO
from uuid import UUID

import pytest

from omega.runtime.console import ControlConsole

KEY = "binance:usdm:BTCUSDT"
SPOT = "binance:spot:ETHUSDT"


def capture(tmp_path, monkeypatch):
    output, calls = [], []
    console = ControlConsole(tmp_path, output=output.append)

    def request(*args):
        calls.append(args)
        return {"state": "queued"}

    monkeypatch.setattr(console, "request", request)
    return console, output, calls


def test_start_posts_exact_existing_payload_and_prints_generated_action_id(tmp_path, monkeypatch):
    console, output, calls = capture(tmp_path, monkeypatch)
    assert console.execute(
        f"collection start {KEY} {SPOT} {KEY} --historical --lookback-minutes 750"
    ) == {"state": "queued"}
    assert len(calls) == 1
    method, path, body = calls[0]
    assert method == "POST" and path == "/api/database/actions"
    assert UUID(body["id"]).hex == body["id"]
    assert output == ["Collection operation " + body["id"]]
    assert body == {
        "id": body["id"],
        "kind": "collection.set",
        "parameters": {
            "instruments": sorted([KEY, SPOT]),
            "stream": True,
            "historical": True,
            "lookback_minutes": 750,
        },
    }


def test_stop_and_explicit_set_do_not_infer_modes_from_stale_status(tmp_path, monkeypatch):
    console, _, calls = capture(tmp_path, monkeypatch)
    console.execute(f"collection stop {KEY} --id stop-original")
    assert calls[-1][2] == {
        "id": "stop-original",
        "kind": "collection.set",
        "parameters": {
            "instruments": [KEY],
            "stream": False,
            "historical": False,
            "lookback_minutes": 500,
        },
    }
    console.execute(
        f"collection set {KEY} --stream on --historical off "
        "--lookback-minutes 300 --id original-set"
    )
    assert calls[-1][2]["parameters"] == {
        "instruments": [KEY],
        "stream": True,
        "historical": False,
        "lookback_minutes": 300,
    }
    assert all(call[0] == "POST" for call in calls)  # No non-atomic read/modify/write.


def test_status_uses_database_action_journal_not_financial_operation(tmp_path, monkeypatch):
    console, _, calls = capture(tmp_path, monkeypatch)
    console.execute("collection status --market spot")
    console.execute("collection status saved-operation")
    assert calls == [
        ("GET", "/api/database?market=spot"),
        ("GET", "/api/database/actions/saved-operation"),
    ]


@pytest.mark.parametrize(
    "text",
    [
        "collection start",
        "collection start BTCUSDT",
        "collection start binance:usdm:btcusdt",
        "collection start other:usdm:BTCUSDT",
        "collection start binance:cash_equity:BTCUSDT",
        "collection start binance:spot:../BTC",
        f"collection start {KEY} --lookback-minutes 0",
        f"collection start {KEY} --lookback-minutes 20001",
        f"collection start {KEY} --lookback-minutes 1.5",
        f"collection start {KEY} --id ../foreign",
        f"collection stop {KEY} --historical",
        f"collection set {KEY} --historical on --lookback-minutes 500",
        f"collection set {KEY} --stream off --historical on --lookback-minutes 500",
        "collection status ../foreign",
        "collection status saved --market spot",
        "collection restart",
        "collection start " + " ".join([KEY] * 101),
    ],
)
def test_invalid_keys_limits_and_ambiguous_modes_fail_before_any_request(
    tmp_path, monkeypatch, text
):
    console, output, calls = capture(tmp_path, monkeypatch)
    with pytest.raises(ValueError):
        console.execute(text)
    assert calls == [] and output == []


def test_uncertain_post_retains_id_without_automatic_replay_or_status_retry(tmp_path, monkeypatch):
    console, output, calls = capture(tmp_path, monkeypatch)

    def failed(*args):
        calls.append(args)
        raise OSError("injected lost acceptance response")

    monkeypatch.setattr(console, "request", failed)
    with pytest.raises(OSError):
        console.execute(f"collection start {KEY} --id original-request")
    assert len(calls) == 1 and calls[0][2]["id"] == "original-request"
    assert output[0] == "Collection operation original-request"
    assert "collection status original-request" in output[1]
    assert "Do not repost blindly" in output[1]


def test_collection_help_needs_no_core_or_transport(tmp_path, monkeypatch):
    console, output, calls = capture(tmp_path, monkeypatch)
    assert "collection --help" in console.execute("help")
    assert "canonical" in console.execute("collection --help")
    assert "history requires streaming" in console.execute("collection start --help")
    assert calls == [] and output == []


def test_collection_reuses_console_authentication_and_json_boundary(tmp_path, monkeypatch):
    secrets = tmp_path / "secrets"
    secrets.mkdir()
    (secrets / "core_token").write_text("synthetic-local-console-token", encoding="utf-8")
    calls = []

    class LocalHTTP:
        def __init__(self, host, port, **_kwargs):
            assert (host, port) == ("127.0.0.1", 8765)

        def request(self, method, path, body, headers):
            assert headers["Authorization"] == "Bearer synthetic-local-console-token"
            assert headers["Content-Type"] == "application/json"
            calls.append((method, path, json.loads(body)))

        def getresponse(self):
            response = BytesIO(b'{"state":"queued","id":"auth-request"}')
            response.status = 200
            return response

        def close(self):
            pass

    monkeypatch.setattr(
        "omega.runtime.console.probe_core",
        lambda *_args, **_kwargs: {"url": "http://127.0.0.1:8765"},
    )
    monkeypatch.setattr("omega.runtime.console.http.client.HTTPConnection", LocalHTTP)
    console = ControlConsole(tmp_path, output=lambda _line: None)
    assert console.execute(f"collection start {KEY} --id auth-request")["state"] == "queued"
    assert len(calls) == 1 and calls[0][:2] == ("POST", "/api/database/actions")
    assert calls[0][2]["parameters"]["instruments"] == [KEY]
