"""Actual isolated committed candles/flow revisions -> NES display worker."""

import os
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.chart_series import ChartSeriesService

pytestmark = pytest.mark.database


@pytest.mark.parametrize("market", [Market.USDM, Market.SPOT])
def test_actual_nes_receipt_and_late_revision_respect_fixed_cutoff_and_preserve_prior_snapshot(
    tmp_path, market
):
    config = os.environ.get("OMEGA_TEST_DB_CONFIG")
    assert config, "explicit isolated DB config required"
    settings = load_database_settings(Path(config))
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    (tmp_path / "database.config.toml").write_bytes(Path(config).read_bytes())
    instrument = Instrument("binance", market, "TEST_NES_" + uuid4().hex[:20].upper())
    start, minute = 1700000040000, 60000
    end = start + 12 * minute
    candle_receipt, flow_receipt, revised_receipt = end + 1000, end + 2000, end + 3000
    clock = [candle_receipt - 1]
    store = PostgresCandleStore(settings)
    auxiliary = PostgresAuxiliaryStore(settings)
    owner = ChartSeriesService(
        lambda: CommittedAnalysisFrameSource(tmp_path), clock_ms=lambda: clock[0]
    )
    bars = tuple(
        Candle(
            instrument,
            start + i * minute,
            start + (i + 1) * minute,
            *map(Decimal, (100, 104, 98, 102, 5)),
        )
        for i in range(12)
    )
    observations = []
    specification = {
        "instrument": instrument.key,
        "kind": "NES",
        "start_ms": start,
        "end_ms": end,
        "period_minutes": 1,
        "nSamples": 3,
        "quote_precision": 8,
    }

    def complete(identity):
        owner.submit(identity, specification)
        with owner.condition:
            assert owner.condition.wait_for(
                lambda: owner.jobs[identity]["state"] not in {"queued", "running"}, 10
            )
        document = owner.status(identity)
        assert document["state"] == "complete", document
        return document["result"]

    try:
        for index, bar in enumerate(bars):
            store.put(MinuteObservation(bar, "rest", candle_receipt, raw='{"fixture":"chart-NES"}'))
            observation = AuxiliaryInput(
                "trade_flow",
                (
                    ("AGGTRADE_NOTIONALBUY", Decimal(100 + index)),
                    ("AGGTRADE_NOTIONALSELL", Decimal(50)),
                ),
                bar.open_time_ms,
                bar.close_time_ms,
                bar.close_time_ms,
                flow_receipt,
                source=instrument.key + ":aggTrade",
            )
            receipt = auxiliary.put(instrument, observation)
            observations.append((observation, receipt))
        unavailable = complete("no_candles")
        assert unavailable["rows"] == []
        clock[0] = candle_receipt
        missing = complete("no_flow")
        assert len(missing["rows"]) == 12
        assert all(
            row["NES"] is None and not row["input_ready"] and row["source_available_at_ms"] is None
            for row in missing["rows"]
        )
        clock[0] = flow_receipt
        known = complete("known")
        assert known["quality"]["auxiliary_observed_minutes"] == 12
        assert known["rows"][-1]["NES"] is not None
        assert all(row["source_available_at_ms"] == flow_receipt for row in known["rows"])
        original, receipt = observations[5]
        correction = replace(
            original,
            available_at_ms=revised_receipt,
            values=(("AGGTRADE_NOTIONALBUY", Decimal(900)), ("AGGTRADE_NOTIONALSELL", Decimal(50))),
        )
        auxiliary.put(instrument, correction, expected_previous=receipt.checksum)
        still_known = complete("same_cutoff")
        assert still_known == known
        clock[0] = revised_receipt
        revised = complete("revised")
        assert revised["source_checksum"] != known["source_checksum"]
        assert revised["rows"][5]["auxiliary_checksum"] != known["rows"][5]["auxiliary_checksum"]
        assert revised["rows"][5]["source_available_at_ms"] == revised_receipt
        assert owner.status("known")["result"] == known
        auxiliary_factories = []

        def forbidden_auxiliary(settings):
            auxiliary_factories.append(settings)
            raise AssertionError("TPD must not open an auxiliary DB store")

        tpd = ChartSeriesService(
            lambda: CommittedAnalysisFrameSource(tmp_path, store_factory=forbidden_auxiliary),
            clock_ms=lambda: clock[0],
        )
        try:
            tpd.submit(
                "candle_only",
                {
                    key: value
                    for key, value in specification.items()
                    if key not in {"quote_precision", "nSamples", "kind"}
                }
                | {"kind": "TPD", "viewLength": 2, "nSamples": 2, "nSamplesMA": 2},
            )
            with tpd.condition:
                assert tpd.condition.wait_for(
                    lambda: tpd.jobs["candle_only"]["state"] not in {"queued", "running"}, 10
                )
            tpd_result = tpd.status("candle_only")
            assert tpd_result["state"] == "complete", tpd_result
            assert tpd_result["result"]["rows"][-1]["TPD"] == 0
            assert auxiliary_factories == []
        finally:
            tpd.close()
    finally:
        owner.close()
        auxiliary.close()
        store.close()
        with connect_database(settings) as connection:
            connection.execute(
                "DELETE FROM market.auxiliary_minutes WHERE instrument=%s AND dataset=%s",
                (instrument.key, instrument.key + ":aggTrade"),
            )
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
