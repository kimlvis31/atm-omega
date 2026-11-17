"""Actual committed candle availability -> common display producer worker."""

import os
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.catalog import create_features
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_series import ChartSeriesService

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000
END = START + 120 * MINUTE
RECEIVED = END + 100000


def test_actual_database_cutoff_excludes_late_candles_then_displays_shared_closed_series(tmp_path):
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
    instrument = Instrument("binance", Market.USDM, "TEST_CHART_" + uuid4().hex[:20].upper())
    store = PostgresCandleStore(settings)
    clock = [RECEIVED - 1]
    worker = ChartSeriesService(
        lambda: CommittedAnalysisFrameSource(tmp_path), clock_ms=lambda: clock[0]
    )
    bars = tuple(
        Candle(
            instrument,
            START + minute * MINUTE,
            START + (minute + 1) * MINUTE,
            Decimal(100 + minute % 7),
            Decimal(110 + minute % 7),
            Decimal(90 + minute % 7),
            Decimal(102 + minute % 7),
            Decimal(10),
        )
        for minute in range(120)
    )
    specification = {
        "instrument": instrument.key,
        "start_ms": START,
        "end_ms": END,
        "period_minutes": 3,
        "kind": "DMIxADX",
        "nSamples": 3,
    }

    def completed(identity):
        worker.submit(identity, specification)
        with worker.condition:
            assert worker.condition.wait_for(
                lambda: worker.jobs[identity]["state"] not in {"queued", "running"}, timeout=10
            )
        result = worker.status(identity)
        assert result["state"] == "complete", result
        return result["result"]

    try:
        for candle in bars:
            store.put(MinuteObservation(candle, "rest", RECEIVED, raw='{"fixture":"chart-series"}'))
        before = completed("before_receipt")
        assert before["rows"] == []
        assert before["quality"]["missing_minutes"] == 120
        clock[0] = RECEIVED
        after = completed("at_receipt")
        graph = create_features(
            instrument,
            ("1_DMIxADX_0_DMIxADX",),
            {"momentum": [{"interval_id": 1, "analysis_code": "DMIxADX", "nSamples": 3}]},
        )
        expected = []
        for candle in bars:
            outputs = graph.update(candle)
            if outputs["CLOSED_1"]:
                expected.append((candle.close_time_ms - 3 * MINUTE, outputs["1_DMIxADX_0_DMIxADX"]))
        assert [(row["open_time_ms"], row["DMIxADX"]) for row in after["rows"]] == expected
        assert any(row["DMIxADX"] is not None for row in after["rows"])
        assert after["quality"]["observed_minutes"] == 120
        assert after["quality"]["missing_minutes"] == 0
        assert all(
            row["available_at_ms"] == RECEIVED > row["close_time_ms"] for row in after["rows"]
        )
        assert worker.status("before_receipt")["result"] == before
    finally:
        worker.close()
        store.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
