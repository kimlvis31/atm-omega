"""Display LOD must preserve financial extrema; client input cannot execute model code."""

import json

import pytest

from omega.gui.models import (
    Candle,
    IncidentTracker,
    graph_layers,
    parse_candles,
    read_artifact,
    read_connection,
    reduce_candles,
    validate_endpoint,
    validate_state,
)
from omega.gui.presentation import MESSAGES, THEMES, stylesheet, translate
from omega.mobile import load_asset


def candle(index=0, **changes):
    return {
        "open_time_ms": 1_700_000_000_000 + index * 60_000,
        "open": "10",
        "high": "12",
        "low": "8",
        "close": "11",
        "volume": "100",
    } | changes


def test_lod_preserves_source_ohlc_extrema_and_volume():
    source = parse_candles(
        [candle(index, high=str(12 + index), volume=str(index)) for index in range(100)]
    )
    reduced = reduce_candles(source, 7)
    assert len(reduced) == 7
    assert reduced[0].open == source[0].open
    assert reduced[-1].close == source[-1].close
    assert max(row.high for row in reduced) == max(row.high for row in source)
    assert min(row.low for row in reduced) == min(row.low for row in source)
    assert sum(row.volume for row in reduced) == sum(row.volume for row in source)
    assert len(source) == 100


@pytest.mark.parametrize(
    "changes",
    [{"high": "9"}, {"low": "12"}, {"close": "NaN"}, {"volume": "-1"}, {"open_time_ms": -1}],
)
def test_invalid_candle_rejected(changes):
    with pytest.raises(ValueError):
        Candle.from_mapping(candle(**changes))


def test_duplicate_or_out_of_order_candles_rejected():
    with pytest.raises(ValueError, match="unique"):
        parse_candles([candle(), candle()])
    with pytest.raises(ValueError, match="sorted"):
        parse_candles([candle(1), candle(0)])
    with pytest.raises(ValueError, match="positive"):
        reduce_candles((), 0)


@pytest.mark.parametrize(
    "url",
    [
        "https://example.com:8000",
        "http://127.0.0.1.evil:80",
        "http://user@127.0.0.1:80",
        "http://127.0.0.1:80/redirect",
        "file:///etc/passwd",
        "http://127.0.0.1",
        "http://127.0.0.1:3?q=x",
    ],
)
def test_owner_token_only_sent_to_loopback_origin(url):
    with pytest.raises(ValueError):
        validate_endpoint(url)


def test_descriptor_cannot_select_a_different_secret_file(tmp_path):
    (tmp_path / "state").mkdir()
    (tmp_path / "secrets").mkdir()
    (tmp_path / "state" / "core.json").write_text(
        json.dumps({"url": "http://127.0.0.1:8001", "token_path": "other"}), encoding="utf-8"
    )
    (tmp_path / "secrets" / "core_token").write_text("owner-test", encoding="utf-8")
    assert read_connection(tmp_path) == ("http://127.0.0.1:8001", "owner-test")


def test_artifact_viewer_is_inert_and_supports_portable_records(tmp_path):
    source = tmp_path / "source.jsonl"
    source.write_text('{"metric":"loss","value":0.5}\n', encoding="utf-8")
    assert read_artifact(source)[0] == [{"metric": "loss", "value": 0.5}]
    python = tmp_path / "malicious.py"
    python.write_text("raise RuntimeError('must never run')", encoding="utf-8")
    with pytest.raises(ValueError, match="supports JSON"):
        read_artifact(python)


def test_acknowledgement_is_revision_scoped_and_not_recovery():
    tracker = IncidentTracker()
    first = {"id": "x", "revision": 1, "severity": "critical"}
    assert tracker.update([first]) == [first]
    tracker.acknowledge(first)
    assert tracker.critical() == []
    assert len(tracker.active) == 1
    worse = first | {"revision": 2}
    assert tracker.update([worse]) == [worse]
    assert tracker.critical() == [worse]
    tracker.update([worse | {"resolved": True}])
    assert tracker.critical() == []


def test_structure_rendering_is_bounded_and_localized():
    layers = graph_layers({"layers": [{"name": "dense", "units": 100000}] * 500})
    assert len(layers) == 16
    assert all(size == 24 for _, size in layers)
    assert set(MESSAGES["ko"]) == set(MESSAGES["en"])
    assert translate("dashboard", "missing") == "Dashboard"
    assert translate("unknown-key") == "unknown-key"
    assert THEMES["dark"]["background"] in stylesheet("invalid")


def test_mobile_asset_server_has_an_exact_allowlist_and_no_cdn():
    html, content_type = load_asset("/mobile/")
    assert "text/html" in content_type
    assert b"/mobile/app.js" in html
    assert b"https://" not in html
    script, _ = load_asset("/mobile/app.js")
    assert b"expected_epoch" in script
    assert b"expires_at_ms" in script
    assert b"innerHTML" not in script
    with pytest.raises(LookupError):
        load_asset("/mobile/../../secrets/core_token")


def test_malformed_state_cannot_enable_controls():
    valid = {
        "schema_version": 1,
        "core": {"paused": True, "epoch": "x", "control_revision": 0, "server_time_ms": 1000},
        "market": {},
        "settings": {},
        "accounts": [],
        "strategies": [],
        "jobs": [],
        "incidents": [],
    }
    validate_state(valid)
    for changes in (
        {"accounts": None},
        {"jobs": ["bad"]},
        {"core": {}},
        {"schema_version": True},
        {"accounts": [{}] * 11},
    ):
        with pytest.raises(ValueError):
            validate_state(valid | changes)
