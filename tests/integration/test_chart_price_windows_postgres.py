"""Prepared isolated-PG cutoff proof for the candle-only MAIN display path."""

import os
import threading
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.catalog import create_features
from omega.domain.candle_content import candle_content_pin
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_series import ChartSeriesService

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_100_000  # Aligned three-minute boundary.
END = START + 18 * MINUTE
RECEIVED = END + 100000


def test_price_main_postgres_cutoff_pins_gap_reset_and_worker_cleanup(tmp_path):
    config = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not config:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    settings = load_database_settings(Path(config))
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    (tmp_path / "database.config.toml").write_bytes(Path(config).read_bytes())
    instrument = Instrument("binance", Market.USDM, "TEST_PRICE_" + uuid4().hex[:20].upper())
    store, sources, clock = PostgresCandleStore(settings), [], [RECEIVED - 1]

    def forbidden_auxiliary_store(settings):
        pytest.fail("price windows must not construct an auxiliary store")

    class TrackedSource(CommittedAnalysisFrameSource):
        def __init__(self):
            super().__init__(tmp_path, store_factory=forbidden_auxiliary_store)
            self.close_threads, self.read_threads = [], []
            sources.append(self)

        def read(self, *args, **kwargs):
            self.read_threads.append(threading.get_ident())
            return super().read(*args, **kwargs)

        def close(self):
            self.close_threads.append(threading.get_ident())
            return super().close()

    worker = ChartSeriesService(TrackedSource, clock_ms=lambda: clock[0])
    candles = tuple(
        Candle(
            instrument,
            START + minute * MINUTE,
            START + (minute + 1) * MINUTE,
            *([Decimal(100 + minute)] * 4),
            Decimal(10),
        )
        for minute in range(18)
    )
    definitions = [
        {"analysis_code": "SMA", "line_index": 0, "nSamples": 2, "price_precision": 8},
        {
            "analysis_code": "BOL",
            "line_index": 0,
            "nSamples": 2,
            "price_precision": 8,
            "MAType": "EMA",
            "bandWidth": -2,
        },
    ]
    specification = {
        "instrument": instrument.key,
        "kind": "PRICE_WINDOWS",
        "period_minutes": 3,
        "start_ms": START,
        "end_ms": END,
        "display_start_ms": START + 6 * MINUTE,
        "definitions": definitions,
    }

    def completed(identity):
        worker.submit(identity, specification)
        with worker.condition:
            assert worker.condition.wait_for(
                lambda: worker.jobs[identity]["state"] not in {"queued", "running"}, timeout=10
            )
        status = worker.status(identity)
        assert status["state"] == "complete", status
        return status["result"]

    def pin(rows, start):
        return candle_content_pin(
            instrument.key,
            start,
            END,
            [
                (row.open_time_ms, row.open, row.high, row.low, row.close, row.volume)
                for row in rows
                if row.open_time_ms >= start
            ],
        )

    def graph():
        return create_features(
            instrument,
            ("1_BOL_0_BOLLOW", "1_BOL_0_BOLHIGH", "1_BOL_0_MA", "1_SMA_0_SMA"),
            {
                "window_averages": [{"interval_id": 1, "analysis_code": "SMA", "nSamples": 2}],
                "bollinger": [{"interval_id": 1, "nSamples": 2, "MAType": "EMA", "bandWidth": -2}],
            },
        )

    try:
        for minute, candle in enumerate(candles):
            if minute != 7:
                store.put(
                    MinuteObservation(candle, "rest", RECEIVED, raw='{"fixture":"price-main"}')
                )
        before = completed("price_before_receipt")
        assert before["rows"] == [] and before["quality"]["missing_minutes"] == 18
        clock[0] = RECEIVED
        after = completed("price_at_receipt")
        observed = [candle for minute, candle in enumerate(candles) if minute != 7]
        assert after["source_checksum"] == pin(observed, START)
        assert after["display_content_pin"] == pin(observed, specification["display_start_ms"])
        assert after["quality"] == {
            "requested_minutes": 18,
            "observed_minutes": 17,
            "missing_minutes": 1,
            "reset_count": 1,
            "complete_bars": 3,
        }
        assert [row["open_time_ms"] for row in after["rows"]] == [
            START + i * MINUTE for i in (9, 12, 15)
        ]
        assert all(row["segment"] == 1 for row in after["rows"])
        assert after["rows"][0]["ready"] == [False, False]
        assert after["rows"][-1]["ready"] == [True, True]
        assert all(
            row["available_at_ms"] == RECEIVED > row["close_time_ms"] for row in after["rows"]
        )
        current, expected = graph(), []
        for candle in observed:
            if candle.open_time_ms == START + 8 * MINUTE:
                current = graph()
            output = current.update(candle)
            opening = candle.close_time_ms - 3 * MINUTE
            if output["CLOSED_1"] and opening >= specification["display_start_ms"]:
                expected.append([output[key] for key in after["columns"]])
        assert [row["values"] for row in after["rows"]] == expected

        # A later committed missing minute changes only a new fixed-cutoff job.
        store.put(
            MinuteObservation(candles[7], "rest", RECEIVED + 1, raw='{"fixture":"late-minute"}')
        )
        still_before = completed("price_before_later_receipt")
        assert still_before["display_content_pin"] == after["display_content_pin"]
        clock[0] += 1
        later = completed("price_after_later_receipt")
        assert later["quality"]["reset_count"] == later["quality"]["missing_minutes"] == 0
        assert later["quality"]["complete_bars"] == 4
        assert later["display_content_pin"] == pin(candles, specification["display_start_ms"])
        assert later["display_content_pin"] != after["display_content_pin"]
        assert worker.status("price_at_receipt")["result"] == after
        assert len(sources) == 4
        for source in sources:
            assert len(source.close_threads) == 1
            assert set(source.read_threads) == set(source.close_threads)
            assert source.close_threads[0] != threading.get_ident()
            assert source._auxiliary_store is None
    finally:
        worker.close()
        store.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
