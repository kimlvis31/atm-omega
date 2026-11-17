"""Opt-in REST capture wiring and original-receipt collection; no public network/DB."""

import asyncio
import threading
from types import SimpleNamespace

import pytest
from test_market_data_worker import Process

from omega.application.collection import MinuteCollector
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure import market_data
from omega.infrastructure.binance.streams import StreamPlan
from omega.ports.collection import MinuteObservation
from omega.runtime import collect_cli, market_data_service, market_data_worker
from omega.runtime.collection import CollectionWorker
from omega.runtime.config import load_master

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def captured():
    p = decimal("100")
    candle = Candle(INSTRUMENT, 540000, 600000, p, p, p, p, decimal("2"))
    return MinuteObservation(candle, "rest", 600020, '[540000,"100"]', "rest", 600010)


class Store:
    def __init__(self):
        self.rows = []

    def put(self, observation):
        self.rows.append(observation)
        return True


def test_master_capture_is_default_off_and_strict_explicit_boolean(tmp_path):
    assert load_master(tmp_path).capture_rest_raw is False
    assert not list(tmp_path.iterdir())
    (tmp_path / "master.config.toml").write_text(
        '[collection]\ncapture_rest_raw=true\n[paths]\ndata_dir="./observations"\n'
    )
    config = load_master(tmp_path)
    assert config.capture_rest_raw is True and config.paths["data_dir"] == tmp_path / "observations"
    assert not (tmp_path / "observations").exists()


@pytest.mark.parametrize("value", ['"true"', "1", "0", "[]"])
def test_invalid_capture_flag_is_not_coerced(tmp_path, value):
    (tmp_path / "master.config.toml").write_text("[collection]\ncapture_rest_raw=" + value)
    with pytest.raises(ValidationError):
        load_master(tmp_path)


@pytest.mark.parametrize("capture", [False, True])
def test_owned_worker_receives_raw_flags_only_when_enabled(tmp_path, monkeypatch, capture):
    calls = []
    monkeypatch.setattr(
        market_data_worker.subprocess,
        "Popen",
        lambda command, **_kwargs: calls.append(command) or Process(),
    )
    options = (
        {"capture_rest_raw": True, "raw_directory": tmp_path / "raw-originals"} if capture else {}
    )
    owner = market_data_worker.OwnedCollectors(tmp_path, **options)
    try:
        owner.configure("usdm", [INSTRUMENT.key], 500, True)
        command = calls[0]
        assert ("--capture-rest-raw" in command) is capture
        assert ("--raw-directory" in command) is capture
        if capture:
            assert command[command.index("--raw-directory") + 1] == str(tmp_path / "raw-originals")
        assert not (tmp_path / "data/raw").exists()
    finally:
        owner.close()


def test_unsupported_capture_history_fails_before_fetch_threads():
    store = Store()
    collector = MinuteCollector({INSTRUMENT: 540000}, store, lambda _bar: None)
    with pytest.raises(ValidationError, match="original observation"):
        CollectionWorker(
            StreamPlan(Market.USDM, (INSTRUMENT,)), object(), collector, capture_rest_raw=True
        )
    assert store.rows == [] and collector.cursor[INSTRUMENT] == 540000


def test_capture_resamples_actual_canonical_clock_after_fetch_and_keeps_raw_receipt():
    now, calls, store = [600000], [], Store()

    class History:
        def closed_observations(self, instrument, start, end, *, now_ms):
            calls.append((instrument, start, end, now_ms))
            now[0] = 600050
            return (captured(),)

        def closed_minutes(self, *_args, **_kwargs):
            pytest.fail("capture must not silently fall back to normalized candles")

    collector = MinuteCollector({INSTRUMENT: 540000}, store, lambda _bar: None)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        History(),
        collector,
        clock_ms=lambda: now[0],
        capture_rest_raw=True,
    )
    asyncio.run(worker.repair())
    assert calls == [(INSTRUMENT, 540000, 600000, 600000)]
    assert len(store.rows) == 1 and store.rows[0].received_at_ms == 600050
    assert store.rows[0].raw_received_at_ms == 600010
    assert store.rows[0].raw_source == "rest" and store.rows[0].raw == captured().raw
    assert collector.cursor[INSTRUMENT] == 600000


def test_default_worker_keeps_original_closed_minutes_and_pre_fetch_clock_semantics():
    now, clock_reads, store = [600000], [], Store()

    class History:
        def closed_minutes(self, *_args, **_kwargs):
            now[0] = 600050
            return (captured().candle,)

        def closed_observations(self, *_args, **_kwargs):
            pytest.fail("default collection must not request raw capture")

    def clock():
        clock_reads.append(now[0])
        return now[0]

    collector = MinuteCollector({INSTRUMENT: 540000}, store, lambda _bar: None)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)), History(), collector, clock_ms=clock
    )
    asyncio.run(worker.repair())
    assert clock_reads == [600000]
    assert store.rows == [MinuteObservation(captured().candle, "rest", 600000)]


@pytest.mark.parametrize("fault", ["archive", "clock"])
def test_capture_archive_failure_or_backward_clock_never_publishes_or_clamps(fault):
    now, store = [600000], Store()

    class History:
        def closed_observations(self, *_args, **_kwargs):
            if fault == "archive":
                raise StorageError("original page archive publication failed")
            now[0] = 599999
            return (captured(),)

    collector = MinuteCollector({INSTRUMENT: 540000}, store, lambda _bar: None)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        History(),
        collector,
        clock_ms=lambda: now[0],
        capture_rest_raw=True,
    )
    with pytest.raises(StorageError if fault == "archive" else ProtocolError):
        asyncio.run(worker.repair())
    assert store.rows == [] and collector.cursor[INSTRUMENT] == 540000


def test_cancelled_capture_reaps_original_fetch_and_never_publishes_late_result():
    entered, release, returned = threading.Event(), threading.Event(), threading.Event()
    store = Store()

    class History:
        def closed_observations(self, *_args, **_kwargs):
            entered.set()
            assert release.wait(3)
            returned.set()
            return (captured(),)

    collector = MinuteCollector({INSTRUMENT: 540000}, store, lambda _bar: None)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        History(),
        collector,
        clock_ms=lambda: 600050,
        capture_rest_raw=True,
    )

    async def run():
        task = asyncio.create_task(worker.repair())
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            task.cancel()
            await asyncio.sleep(0.01)
            assert not task.done() and not returned.is_set()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            release.set()
            await asyncio.gather(task, return_exceptions=True)

    asyncio.run(run())
    assert returned.is_set() and store.rows == [] and collector.cursor[INSTRUMENT] == 540000


def test_backend_history_keeps_legacy_constructor_call_and_forwards_archive_only_on(monkeypatch):
    calls = []
    monkeypatch.setattr(market_data, "BinanceClient", lambda *_args, **_kwargs: object())
    monkeypatch.setattr(
        market_data, "BinanceMarketData", lambda *args, **kwargs: calls.append((args, kwargs))
    )
    market_data.MarketDataBackend._history("usdm")
    archive = object()
    market_data.MarketDataBackend._history("spot", raw_archive=archive)
    assert calls[0][1] == {} and calls[1][1] == {"raw_archive": archive}


def test_manual_capture_refetch_uses_original_observations_and_actual_post_fetch_time(
    tmp_path, monkeypatch
):
    backend = market_data.MarketDataBackend(tmp_path, capture_rest_raw=True)
    now = [600000]

    def original(*_args, **_kwargs):
        now[0] = 600050
        return (captured(),)

    monkeypatch.setattr(
        backend, "history", lambda _market: SimpleNamespace(closed_observations=original)
    )
    monkeypatch.setattr(market_data.time, "time_ns", lambda: now[0] * 1000000)
    monkeypatch.setattr(
        backend,
        "_repair",
        lambda instrument, _lookback, fetch, _progress, _stop: fetch(
            instrument, 540000, 600000, 600000
        ),
    )
    rows = backend.refetch(INSTRUMENT.key, 500, lambda _: None, threading.Event())
    assert rows[0].received_at_ms == 600050 and rows[0].raw_received_at_ms == 600010
    assert rows[0].raw == captured().raw


@pytest.mark.parametrize("capture", [False, True])
def test_market_service_composes_capture_options_without_changing_default_callargs(
    tmp_path, monkeypatch, capture
):
    calls = []

    def backend(*args, **kwargs):
        calls.append(("backend", args, kwargs))
        return object()

    def collectors(*args, **kwargs):
        calls.append(("collectors", args, kwargs))
        return SimpleNamespace(close=lambda: None)

    monkeypatch.setattr(market_data_service, "MarketDataBackend", backend)
    monkeypatch.setattr(market_data_service, "OwnedCollectors", collectors)
    options = {"capture_rest_raw": True, "raw_directory": tmp_path / "capture"} if capture else {}
    service = market_data_service.MarketDataService(tmp_path, **options)
    try:
        assert calls == [(name, (tmp_path,), options) for name in ("backend", "collectors")]
    finally:
        service.close()


def test_standalone_raw_directory_requires_explicit_capture_before_database_open(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        collect_cli,
        "load_database_settings",
        lambda *_args: pytest.fail("invalid flags must not open the database"),
    )
    args = collect_cli.parser().parse_args(
        [
            "--market",
            "usdm",
            "--symbols",
            "BTCUSDT",
            "--raw-directory",
            str(tmp_path / "capture"),
        ]
    )
    with pytest.raises(ValidationError):
        asyncio.run(collect_cli.collect(args))
    assert not (tmp_path / "capture").exists()


@pytest.mark.parametrize("directory", ["bad\x00path", "bad\npath", 1, True])
def test_invalid_raw_directory_is_rejected_before_any_database_or_archive_access(directory):
    args = SimpleNamespace(capture_rest_raw=True, raw_directory=directory)
    with pytest.raises(ValidationError, match="filesystem path"):
        asyncio.run(collect_cli.collect(args))


def test_real_rest_producer_bridge_collector_preserves_original_page_and_receipts(tmp_path):
    from urllib.parse import parse_qs, urlsplit

    from omega.infrastructure.binance.budget import RequestBudget, Window
    from omega.infrastructure.binance.client import BinanceClient
    from omega.infrastructure.binance.market import BinanceMarketData
    from omega.infrastructure.binance.transport import HttpResponse
    from omega.infrastructure.raw_archive import FileRawArchive

    fragment = (
        '[540000, "100.00", "102.00", "99.00", "101.00", "2.000", '
        '599999, "202.000", 3, "1.0", "101.0", "0"]'
    )
    body = ("[\n  " + fragment + "\n]\n").encode("utf-8")
    requests, clock_reads, delivered, store = [], [], [], Store()
    archive = FileRawArchive(tmp_path / "original-pages")

    class Transport:
        def send(self, request):
            requests.append(request)
            return HttpResponse(200, body, {"Content-Type": "application/json"}, 600010)

    def producer_clock():
        clock_reads.append(("producer", 600020))
        return 600020

    accepted_times = iter((600000, 600050))

    def bridge_clock():
        value = next(accepted_times)
        clock_reads.append(("bridge", value))
        return value

    client = BinanceClient(
        "https://fapi.binance.com",
        Transport(),
        clock_ms=producer_clock,
        budget=RequestBudget((Window("binance-ip", 100, 60),), clock=lambda: 600.0),
    )
    history = BinanceMarketData(Market.USDM, client, raw_archive=archive)
    collector = MinuteCollector({INSTRUMENT: 540000}, store, delivered.append)
    worker = CollectionWorker(
        StreamPlan(Market.USDM, (INSTRUMENT,)),
        history,
        collector,
        clock_ms=bridge_clock,
        capture_rest_raw=True,
    )
    asyncio.run(worker.repair())

    assert clock_reads == [("bridge", 600000), ("producer", 600020), ("bridge", 600050)]
    assert len(requests) == 1
    (request,) = requests
    route = urlsplit(request.url)
    assert request.method == "GET" and request.body is None and request.headers == {}
    assert route.netloc == "fapi.binance.com" and route.path == "/fapi/v1/klines"
    assert parse_qs(route.query) == {
        "symbol": ["BTCUSDT"],
        "interval": ["1m"],
        "startTime": ["540000"],
        "endTime": ["599999"],
        "limit": ["1500"],
    }
    receipt_paths = list((archive.root / "receipts").rglob("*.json"))
    assert len(receipt_paths) == 1
    archived = archive.get(receipt_paths[0].stem)
    assert archived.record.body == body
    assert archived.record.instrument == INSTRUMENT and archived.record.channel == "klines_1m"
    assert archived.record.received_at_ms == archived.receipt.received_at_ms == 600010
    assert dict(archived.record.request_parameters) == {
        "symbol": "BTCUSDT",
        "interval": "1m",
        "startTime": "540000",
        "endTime": "599999",
        "limit": "1500",
    }
    assert archived.record.headers == (("content-type", "application/json"),)
    (observation,) = store.rows
    assert observation.raw == fragment and observation.raw.encode("utf-8") in body
    assert observation.source == observation.raw_source == "rest"
    assert observation.raw_received_at_ms == 600010 and observation.received_at_ms == 600050
    assert observation.candle == Candle(
        INSTRUMENT,
        540000,
        600000,
        decimal("100.00"),
        decimal("102.00"),
        decimal("99.00"),
        decimal("101.00"),
        decimal("2.000"),
    )
    assert delivered == [observation.candle]
    assert collector.cursor == {INSTRUMENT: 600000} and collector.fault is None
