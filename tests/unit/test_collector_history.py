import asyncio
from dataclasses import dataclass
from decimal import Decimal
from threading import Event
from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError, StorageError
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import AUXILIARY_BATCH_SIZE
from omega.runtime.collect_aux_live import LiveAuxiliary
from omega.runtime.collector_aux_history import (
    DAY,
    MINUTE,
    DailyAuxiliaryHistory,
    depth_fields,
    trade_provenance,
)
from omega.runtime.collector_history import HistoryWorker, discover

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")


def test_confirmed_origins_do_not_query_binance_again():
    worker = HistoryWorker.__new__(HistoryWorker)
    worker.inventory = {INSTRUMENT: "unused"}
    worker.stop = Event()
    worker.rest = {}
    worker.vision = object()

    class Db:
        def execute(self, query):
            assert query.startswith("SELECT")
            return tuple(
                (INSTRUMENT.key, channel, "verified", None, 60_000)
                for channel in ("kline", "agg_trade", "depth", "metrics")
            )

    worker.check_origins(Db())


def test_kline_discovery_uses_first_observation_with_both_sources():
    rest = SimpleNamespace(first_available_minute=lambda _: 60_000)
    archive = SimpleNamespace(
        timestamp_ms=120_000,
        row_number=2,
        receipt=SimpleNamespace(
            object_key="candle.zip", zip_sha256="a" * 64, received_at_ms=600000
        ),
    )
    vision = SimpleNamespace(first_row=lambda *_, **__: archive)
    state, first, rest_first, archive_first, evidence = discover(INSTRUMENT, "kline", rest, vision)
    assert (state, first, rest_first, archive_first) == ("verified", 60000, 60000, 120000)
    assert (evidence["vision_key"], evidence["vision_row"]) == ("candle.zip", 2)


@dataclass
class Depth:
    values: tuple[str, ...]


def test_archive_depth_conserves_each_native_side_in_eta_bands():
    fields, coverage = depth_fields(
        Depth(("unused", percent, "1", value))
        for percent, value in (("-1", "100"), ("-5", "300"), ("1", "100"), ("5", "500"))
    )
    assert coverage == 1
    assert fields["DEPTH_BIDS0"] == Decimal(20)
    assert fields["DEPTH_BIDS1"] == Decimal(80)
    assert sum(value for key, value in fields.items() if "BIDS" in key) == Decimal(300)
    assert sum(value for key, value in fields.items() if "ASKS" in key) == Decimal(500)


def test_non_monotonic_archive_depth_is_not_stored_as_valid():
    with pytest.raises(ProtocolError):
        depth_fields((Depth(("unused", "1", "1", "100")), Depth(("unused", "2", "1", "90"))))


def test_archive_trade_uses_native_flow_contract_and_actual_receipt():
    trade = trade_provenance(
        SimpleNamespace(
            instrument=INSTRUMENT,
            timestamp_ms=120001,
            values=("7", "12.50", "0.2", "20", "21", "120001", "true"),
            receipt=SimpleNamespace(received_at_ms=500000),
        )
    )
    assert trade.source == INSTRUMENT.key + ":aggTrade"
    assert trade.available_at_ms == 500000
    assert trade.trade_time_ms == 120001
    assert trade.buyer_is_maker and trade.trade_count == 2 and trade.quantity == Decimal("0.2")


def test_history_checkpoint_only_advances_after_every_batch_commits(tmp_path, monkeypatch):
    limit = AUXILIARY_BATCH_SIZE
    start = DAY
    end = start + (limit + 1) * MINUTE
    rows = list(range(limit + 1))
    key = INSTRUMENT.key + "|depth"
    vision = SimpleNamespace(files=lambda *_: ["archive"])

    def now():
        return end + 2 * DAY

    monkeypatch.setattr(DailyAuxiliaryHistory, "_end", staticmethod(lambda *_: end))
    monkeypatch.setattr(DailyAuxiliaryHistory, "_depth_day", lambda *_: (rows, len(rows)))

    class Store:
        fail = True
        committed = set()
        batch_sizes = []

        def put_history_batch(self, instrument, batch):
            assert instrument == INSTRUMENT
            self.batch_sizes.append(len(batch))
            if len(batch) == 1 and self.fail:
                raise StorageError("history batch commit failed")
            inserted = len(set(batch) - self.committed)
            self.committed.update(batch)
            return inserted

    store = Store()
    history = DailyAuxiliaryHistory(tmp_path, vision, {}, clock_ms=now, stop=Event())
    with pytest.raises(StorageError):
        history.step(None, store, INSTRUMENT, "depth", start)
    assert store.committed == set(rows[:limit])

    resumed = DailyAuxiliaryHistory(tmp_path, vision, {}, clock_ms=now, stop=Event())
    assert resumed.progress[key]["next_ms"] == start
    store.fail = False
    result = resumed.step(None, store, INSTRUMENT, "depth", start)
    assert result["written"] == 1
    assert result["verified"] == len(rows)
    assert resumed.progress[key]["next_ms"] == end
    assert store.batch_sizes == [limit, 1, limit, 1]


def test_auxiliary_stream_retries_same_batch_and_flushes_shutdown_queue():
    async def scenario():
        stop = asyncio.Event()
        instance = LiveAuxiliary(None, {})
        row = SimpleNamespace(
            channel="trade_flow", source="test", window_start_ms=0, available_at_ms=1
        )
        updated = SimpleNamespace(**{**vars(row), "available_at_ms": 2})
        other = SimpleNamespace(**{**vars(row), "channel": "depth"})
        attempts = []

        def put_batch(batch):
            attempts.append(tuple(value for _, value in batch))
            if len(attempts) == 1:
                raise StorageError("temporary")
            return len(batch)

        instance._put_batch = put_batch
        try:
            await instance.write(INSTRUMENT, row)
            await instance.write(INSTRUMENT, other)
            await instance.write(INSTRUMENT, updated)
            await instance.pending.put(None)
            await asyncio.wait_for(instance._save_stream(stop), 2)
            assert attempts == [(row, other), (row, other), (updated,)]
        finally:
            instance.storage.shutdown()

    asyncio.run(scenario())


def test_failed_archive_listing_does_not_record_source_absence(tmp_path, monkeypatch):
    start = DAY
    end = start + DAY
    now = end + MINUTE
    probes = []

    def files(*_):
        if not probes:
            raise OSError("temporary list failure")
        return ()

    def note(*args):
        probes.append(args)

    monkeypatch.setattr(DailyAuxiliaryHistory, "_end", staticmethod(lambda *_: end))
    monkeypatch.setattr("omega.runtime.collector_aux_history.note_source_check", note)
    history = DailyAuxiliaryHistory(
        tmp_path, SimpleNamespace(files=files), {}, clock_ms=lambda: now, stop=Event()
    )
    with pytest.raises(OSError):
        history.step(None, None, INSTRUMENT, "depth", start)
    assert probes == []
    assert history.progress[INSTRUMENT.key + "|depth"]["next_ms"] == start

    probes.append("listing now succeeds")
    assert history.step(None, None, INSTRUMENT, "depth", start) is False
    assert probes[1][3:] == ("vision", start, end, "unlisted", now)
    assert history.progress[INSTRUMENT.key + "|depth"]["state"] == "waiting_archive"
