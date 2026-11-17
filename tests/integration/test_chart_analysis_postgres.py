"""Isolated PostgreSQL native revision fixtures -> actual chart worker providers.

The persisted values are synthetic test facts, not observed exchange history.
"""

import os
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.analysis.order_flow import DEPTH_FIELDS
from omega.analysis.short_term_scalar import ScalarConfiguration
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_series import ChartSeriesService

pytestmark = pytest.mark.database
MINUTE = 60000
START = 1_700_000_040_000
END = START + 60 * MINUTE
RECEIVED = END + 100000


@pytest.fixture
def native_case(tmp_path):
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    config = Path(path).resolve()
    settings = load_database_settings(config)
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
    (tmp_path / "database.config.toml").write_bytes(config.read_bytes())
    instrument = Instrument("binance", Market.USDM, "TEST_CHART_SI_" + uuid4().hex[:20].upper())
    candles, auxiliary = PostgresCandleStore(settings), PostgresAuxiliaryStore(settings)
    depth_last = receipt_last = None
    try:
        for index in range(60):
            opening = START + index * MINUTE
            closing = opening + MINUTE
            base = 100 + index % 7
            candle = Candle(
                instrument,
                opening,
                closing,
                *map(Decimal, (base, base + 4, base - 2, base + 1, 10)),
            )
            candles.put(MinuteObservation(candle, "rest", RECEIVED, raw='{"fixture":"chart-si"}'))
            depth = AuxiliaryInput(
                "depth",
                tuple(
                    (name, Decimal(index + offset + 1)) for offset, name in enumerate(DEPTH_FIELDS)
                ),
                opening,
                closing,
                closing - 1,
                RECEIVED,
                source=instrument.key + ":binance_depth_ws_v1",
            )
            receipt_last = auxiliary.put(instrument, depth)
            depth_last = depth
            flow = AuxiliaryInput(
                "trade_flow",
                (
                    ("AGGTRADE_NOTIONALBUY", Decimal(index * 3 + 10)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(index + 5)),
                ),
                opening,
                closing,
                closing,
                RECEIVED,
                source=instrument.key + ":aggTrade",
            )
            auxiliary.put(instrument, flow)
            if index % 5 == 0:
                for channel, name, value, suffix in (
                    (
                        "open_interest",
                        "METRIC_OPENINTEREST",
                        Decimal(1000 + index),
                        "open_interest_history",
                    ),
                    (
                        "long_short_ratio",
                        "METRIC_LONGSHORTRATIO",
                        Decimal("1.2") + Decimal(index) / 1000,
                        "long_short_ratio",
                    ),
                ):
                    auxiliary.put(
                        instrument,
                        AuxiliaryInput(
                            channel,
                            ((name, value),),
                            opening,
                            closing,
                            opening,
                            RECEIVED,
                            source=instrument.key + ":" + suffix,
                        ),
                    )
        yield tmp_path, instrument, auxiliary, depth_last, receipt_last
    finally:
        auxiliary.close()
        candles.close()
        # Only this fixture's UUID namespace; never a broad table/volume cleanup.
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
            connection.execute(
                "DELETE FROM market.auxiliary_minutes WHERE instrument=%s", (instrument.key,)
            )


@pytest.mark.parametrize("kind", ("WOI", "MST", "STS"))
def test_real_snapshot_chart_worker_masks_before_receipt_and_pins_selected_native_revision(
    native_case, kind
):
    root, instrument, auxiliary, depth, receipt = native_case
    extra = (
        {
            key: value
            for key, value in asdict(ScalarConfiguration(0)).items()
            if key != "interval_id"
        }
        if kind == "STS"
        else {
            "definitions": [{"line_index": 0, "nSamples": 3}, {"line_index": 9, "nSamples": 7}],
            "quote_precision": 8,
        }
    )
    if kind == "MST":
        extra["maximum_metric_age_ms"] = 330000
    specification = {
        "instrument": instrument.key,
        "kind": kind,
        "start_ms": START,
        "end_ms": END,
        "period_minutes": 1,
        **extra,
    }
    clock, calls = [RECEIVED - 1], []

    def factory():
        source = CommittedAnalysisFrameSource(root)
        read = source.read_revisions

        def retained(ranges, **kwargs):
            calls.append(tuple(row.channel for row in ranges))
            return read(ranges, **kwargs)

        source.read_revisions = retained
        return source

    worker = ChartSeriesService(factory, clock_ms=lambda: clock[0])

    def complete(identity):
        worker.submit(identity, specification)
        with worker.condition:
            assert worker.condition.wait_for(
                lambda: worker.jobs[identity]["state"] not in {"queued", "running", "cancelling"},
                timeout=30,
            )
        row = worker.status(identity)
        assert row["state"] == "complete", row
        return row["result"]

    try:
        before = complete("before_receipt")
        assert before["rows"] == [] and before["quality"]["missing_minutes"] == 60
        assert calls == []
        # Keep the two lineage comparisons at the same explicit fixture cutoff.
        clock[0] = RECEIVED + 1
        first = complete("received")
        assert len(first["rows"]) == 60
        assert first["quality"]["observed_minutes"] == 60
        assert all(row["input_quality"]["0"]["ready"] for row in first["rows"])
        assert any(
            value is not None for row in first["rows"] for value in row["values"]["0"].values()
        )
        assert first["source_checksum_scope"] == "committed_market_frames_native_revisions_v1"
        assert first["knowledge_mode"] == "retrospective_display"
        assert all(
            row["available_at_ms"] == RECEIVED + 1 and row["source_available_at_ms"] == RECEIVED
            for row in first["rows"]
        )
        assert (
            calls == [("open_interest", "long_short_ratio", "trade_flow")]
            if kind == "MST"
            else calls == [("depth",)]
        )
        revised = replace(depth, available_at_ms=RECEIVED + 1)
        auxiliary.put(instrument, revised, expected_previous=receipt.checksum)
        second = complete("revision")
        # A depth-only revision must not change a metric/flow-only MST lineage.
        assert (first["source_checksum"] == second["source_checksum"]) == (kind == "MST")
        if kind != "MST":
            assert (
                first["rows"][-1]["auxiliary_checksum"] != second["rows"][-1]["auxiliary_checksum"]
            )
        assert worker.status("received")["result"] == first
    finally:
        worker.close()
