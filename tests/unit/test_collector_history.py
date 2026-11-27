from dataclasses import dataclass
from decimal import Decimal
from threading import Event
from types import SimpleNamespace

import pytest

from omega.domain.errors import ProtocolError
from omega.domain.models import Instrument, Market
from omega.runtime.collector_aux_history import depth_fields
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
                (INSTRUMENT.key, channel, "verified", None)
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
