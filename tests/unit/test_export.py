"""Committed-observation export fidelity and failure checks using a fake store."""

import csv
import json
from dataclasses import replace
from decimal import Decimal

import pytest

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.ports.collection import MinuteObservation
from omega.runtime import export_cli

INSTRUMENT = Instrument("binance", Market.SPOT, "BTCUSDT")


class Store:
    def __init__(self, observations):
        self.observations = observations
        self.calls = []

    def latest_observations(self, instrument, limit):
        self.calls.append((instrument, limit))
        return self.observations


def observation(minute=0, raw=None):
    candle = Candle(
        INSTRUMENT,
        minute * 60_000,
        (minute + 1) * 60_000,
        Decimal("100.123456789012345678"),
        Decimal("101.000000000000000001"),
        Decimal("99.000000000000000001"),
        Decimal("100.987654321098765432"),
        Decimal("0.000000000000000001"),
    )
    return MinuteObservation(candle, "rest", (minute + 1) * 60_000 + 123, raw)


def test_csv_preserves_exact_decimals_and_committed_source(tmp_path):
    store = Store([observation(), observation(1)])
    target = tmp_path / "nested" / "candles.csv"
    report = export_cli.export_observations(store, INSTRUMENT, target, limit=2)
    with target.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert store.calls == [(INSTRUMENT, 2)]
    assert len(rows) == report["rows"] == 2
    assert report["source"] == "committed_postgresql"
    assert report["representation"] == "normalized_ohlcv"
    assert report["raw_missing"] is None
    assert rows[0] == {
        "instrument": INSTRUMENT.key,
        "open_time_ms": "0",
        "open": "100.123456789012345678",
        "high": "101.000000000000000001",
        "low": "99.000000000000000001",
        "close": "100.987654321098765432",
        "volume": "1E-18",
    }
    assert rows[1]["open_time_ms"] == "60000"
    assert list(target.parent.glob(".omega-export-*.partial")) == []


def test_raw_preserves_original_payload_and_explicitly_marks_missing(tmp_path):
    raw = '{"price":"100.123456789012345678", "note":"원본"}'
    target = tmp_path / "received.jsonl"
    report = export_cli.export_observations(
        Store([observation(raw=raw), observation(1)]),
        INSTRUMENT,
        target,
        raw=True,
    )
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    assert rows[0] == {
        "instrument": INSTRUMENT.key,
        "open_time_ms": 0,
        "source": "rest",
        "received_at_ms": 60_123,
        "raw_payload": raw,
        "raw_available": True,
        "raw_source": None,
        "raw_received_at_ms": None,
        "raw_origin_known": False,
    }
    assert rows[1]["raw_payload"] is None
    assert rows[1]["raw_available"] is False
    assert rows[1]["raw_source"] is None
    assert rows[1]["raw_received_at_ms"] is None
    assert rows[1]["raw_origin_known"] is False
    assert report["raw_missing"] == 1
    assert report["representation"] == "raw_payload"
    assert list(target.parent.glob(".omega-export-*.partial")) == []


def test_raw_origin_is_distinct_from_canonical_candle_origin(tmp_path):
    raw = '{"stream":"btcusdt@kline_1m","event":"late raw payload"}'
    canonical = observation(raw=raw)
    enriched = replace(canonical, raw_source="websocket", raw_received_at_ms=90_456)
    target = tmp_path / "received.jsonl"
    report = export_cli.export_observations(Store([enriched]), INSTRUMENT, target, raw=True)
    assert json.loads(target.read_text(encoding="utf-8")) == {
        "instrument": INSTRUMENT.key,
        "open_time_ms": 0,
        "source": "rest",
        "received_at_ms": 60_123,
        "raw_payload": raw,
        "raw_available": True,
        "raw_source": "websocket",
        "raw_received_at_ms": 90_456,
        "raw_origin_known": True,
    }
    assert report["raw_missing"] == 0


def test_raw_transport_clock_before_close_is_preserved_without_rewriting_acceptance(tmp_path):
    enriched = replace(
        observation(raw="actual early frame"), raw_source="websocket", raw_received_at_ms=59_600
    )
    target = tmp_path / "early.jsonl"
    export_cli.export_observations(Store([enriched]), INSTRUMENT, target, raw=True)
    row = json.loads(target.read_text())
    assert row["received_at_ms"] == 60_123
    assert row["raw_received_at_ms"] == 59_600
    assert row["raw_origin_known"] is True


@pytest.mark.parametrize("raw", [False, True])
def test_existing_dataset_is_never_replaced(tmp_path, raw):
    target = tmp_path / "source.csv"
    original = b"user dataset\n"
    target.write_bytes(original)
    with pytest.raises(FileExistsError):
        export_cli.export_observations(Store([observation()]), INSTRUMENT, target, raw=raw)
    assert target.read_bytes() == original


def test_no_committed_observations_does_not_create_an_output(tmp_path):
    target = tmp_path / "missing" / "dataset.csv"
    with pytest.raises(ValidationError, match="no committed"):
        export_cli.export_observations(Store([]), INSTRUMENT, target)
    assert not target.exists()


@pytest.mark.parametrize("raw", [False, True])
def test_failed_durable_flush_does_not_publish_final_dataset(tmp_path, monkeypatch, raw):
    target = tmp_path / "dataset.csv"

    def fail(_descriptor):
        raise OSError("disk full at fsync")

    monkeypatch.setattr(export_cli.os, "fsync", fail)
    with pytest.raises(OSError, match="disk full"):
        export_cli.export_observations(Store([observation()]), INSTRUMENT, target, raw=raw)
    assert not target.exists(), "failed export is visible under its final dataset name"
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("raw", [False, True])
def test_concurrently_created_dataset_is_preserved_and_temporary_is_removed(
    tmp_path, monkeypatch, raw
):
    target = tmp_path / "dataset.csv"
    original = b"concurrent user dataset\n"
    real_link = export_cli.os.link
    publications = []

    def race(source, destination):
        # This runs after the initial exists check and durable temporary write.
        assert not target.exists()
        payload = source.read_text(encoding="utf-8")
        if raw:
            assert json.loads(payload)["raw_available"] is False
        else:
            assert "100.123456789012345678" in payload
        publications.append(source)
        target.write_bytes(original)
        return real_link(source, destination)

    monkeypatch.setattr(export_cli.os, "link", race)
    with pytest.raises(FileExistsError):
        export_cli.export_observations(Store([observation()]), INSTRUMENT, target, raw=raw)
    assert len(publications) == 1
    assert target.read_bytes() == original
    assert list(tmp_path.iterdir()) == [target]


@pytest.mark.parametrize("raw", [False, True])
def test_mid_write_failure_removes_partial_and_final_output(tmp_path, monkeypatch, raw):
    target = tmp_path / "dataset.csv"
    function = "dumps" if raw else "candle_row"
    owner = export_cli.json if raw else export_cli
    original = getattr(owner, function)
    calls = []

    def fail_second(*args, **kwargs):
        calls.append(True)
        if len(calls) == 2:
            raise OSError("interrupted second row")
        return original(*args, **kwargs)

    monkeypatch.setattr(owner, function, fail_second)
    with pytest.raises(OSError, match="interrupted second row"):
        export_cli.export_observations(
            Store([observation(), observation(1)]), INSTRUMENT, target, raw=raw
        )
    assert len(calls) == 2
    assert list(tmp_path.iterdir()) == []


def test_unsupported_exclusive_publish_cleans_temporary_without_final_output(tmp_path, monkeypatch):
    target = tmp_path / "dataset.csv"

    def fail(*args, **kwargs):
        raise OSError("hard links unsupported by destination filesystem")

    monkeypatch.setattr(export_cli.os, "link", fail)
    with pytest.raises(OSError, match="hard links unsupported"):
        export_cli.export_observations(Store([observation()]), INSTRUMENT, target)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("limit", ["0", "10001"])
def test_cli_invalid_limit_does_not_open_database(tmp_path, monkeypatch, capsys, limit):
    def forbidden(*args, **kwargs):
        pytest.fail("invalid export opened database settings")

    monkeypatch.setattr(export_cli, "load_database_settings", forbidden)
    assert (
        export_cli.main(
            [
                "--instrument",
                INSTRUMENT.key,
                "--output",
                str(tmp_path / "data.csv"),
                "--limit",
                limit,
            ]
        )
        == 1
    )
    assert "Export failed" in capsys.readouterr().out


def test_cli_write_failure_closes_store_and_reports_failure(tmp_path, monkeypatch, capsys):
    store = Store([observation()])
    closed = []
    store.close = lambda: closed.append(True)
    monkeypatch.setattr(export_cli, "load_database_settings", lambda _path: object())
    monkeypatch.setattr(export_cli, "PostgresCandleStore", lambda _settings: store)

    def fail(_descriptor):
        raise OSError("durable write failed")

    monkeypatch.setattr(export_cli.os, "fsync", fail)
    assert (
        export_cli.main(
            [
                "--instrument",
                INSTRUMENT.key,
                "--output",
                str(tmp_path / "data.csv"),
            ]
        )
        == 1
    )
    assert closed == [True]
    output = capsys.readouterr().out
    assert "Export failed" in output
    assert '"rows"' not in output
