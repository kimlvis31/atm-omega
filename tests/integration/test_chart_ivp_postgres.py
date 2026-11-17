"""Committed isolated-PG availability is honored by selected IVP snapshots."""

import os
from decimal import Decimal
from importlib import import_module
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.volume_profile import LegacyVolumeProfileFeatures, ProfileConfiguration
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_series import ChartSeriesService

pytestmark = pytest.mark.database


def test_actual_committed_cutoff_selected_ivp_never_reads_later_receipts(tmp_path):
    pytest.importorskip("scipy")
    # The cutoff assertion measures committed-data visibility, not cold dependency imports.
    import_module("scipy.ndimage")
    import_module("scipy.signal")
    settings_path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    assert settings_path, "explicit isolated DB configuration required"
    settings = load_database_settings(Path(settings_path))
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    (tmp_path / "database.config.toml").write_bytes(Path(settings_path).read_bytes())
    instrument = Instrument("binance", Market.USDM, "TEST_IVP_" + uuid4().hex[:20].upper())
    start, minute = 1700000040000, 60000
    end = start + 12 * minute
    received = end + 100000
    clock = [received - 1]
    store = PostgresCandleStore(settings)
    owner = ChartSeriesService(
        lambda: CommittedAnalysisFrameSource(tmp_path), clock_ms=lambda: clock[0]
    )
    bars = tuple(
        Candle(
            instrument,
            start + i * minute,
            start + (i + 1) * minute,
            Decimal(100 + i % 3),
            Decimal(103 + i % 3),
            Decimal(99 + i % 3),
            Decimal(101 + i % 3),
            Decimal(10),
        )
        for i in range(12)
    )
    spec = {
        "instrument": instrument.key,
        "kind": "IVP",
        "start_ms": start,
        "end_ms": end,
        "period_minutes": 1,
        "nSamples": 3,
        "gammaFactor": 0.02,
        "deltaFactor": 1.0,
        "prominence": 0.1,
        "distance": 5,
        "height": 0.5,
        "price_precision": 8,
        "max_divisions": 2048,
    }

    def completed(identity):
        owner.submit(identity, spec)
        with owner.condition:
            assert owner.condition.wait_for(
                lambda: owner.jobs[identity]["state"] not in {"queued", "running"}, 10
            )
        value = owner.status(identity)
        assert value["state"] == "complete", value
        return value["result"]

    try:
        for candle in bars:
            store.put(MinuteObservation(candle, "rest", received, raw='{"fixture":"selected-IVP"}'))
        before = completed("before")
        assert before["rows"][0]["state"] == "missing"
        assert before["quality"]["missing_minutes"] == 12
        clock[0] = received
        after = completed("after")
        graph = LegacyVolumeProfileFeatures(
            instrument, (ProfileConfiguration(0, nSamples=3, gammaFactor=0.02, max_divisions=2048),)
        )
        for candle in bars:
            graph.update(candle)
        assert (
            after["rows"][0]["filtered"]
            == graph.state["raw"][0]["volumePriceLevelProfile_Filtered"]
        )
        assert after["rows"][0]["state"] == "ready"
        assert after["rows"][0]["available_at_ms"] == received
        assert owner.status("before")["result"] == before
    finally:
        owner.close()
        store.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
