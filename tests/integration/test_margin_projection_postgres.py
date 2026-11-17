"""Isolated PG candles → real worker selector → actual TEF fills without unused raw reads.

All prices and clocks are synthetic. The frame-read counter delegates to the
real PostgreSQL source; no exchange, account keys or production namespace is used.
"""

import os
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from database_safety import isolated_test_port
from test_margin_analysis_projection_runtime import bar, configuration

from omega.domain.margin import FillEvent, MarginBracket, MarginBracketSnapshot
from omega.domain.models import Instrument, Market
from omega.infrastructure.analysis_frames import CommittedAnalysisFrameSource
from omega.infrastructure.candles import PostgresCandleStore, connect_database
from omega.infrastructure.postgres.config import database_destination_id, load_database_settings
from omega.ports.collection import MinuteObservation
from omega.runtime.margin_session import MarginTradingSession
from omega.runtime.margin_trading import MarginTradingService

pytestmark = pytest.mark.database
START = 1_700_000_040_000
MINUTE = 60000


def test_isolated_pg_projection_skips_unused_sources_and_still_produces_real_fills(
    request, tmp_path
):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated PostgreSQL opt-in")
    configured = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not configured:
        pytest.fail("OMEGA_TEST_DB_CONFIG must name the isolated test database")
    config_path = Path(configured).resolve()
    settings = load_database_settings(config_path)
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    )
    namespace = uuid4().hex
    instrument = Instrument(
        "binance", Market.USDM, "TEST_PGPROJ_" + namespace[:20].upper() + "USDT"
    )
    root = tmp_path / namespace
    root.mkdir()
    (root / "database.config.toml").write_bytes(config_path.read_bytes())
    rows = tuple(
        replace(
            bar(index),
            instrument=instrument,
            open_time_ms=START + index * MINUTE,
            close_time_ms=START + (index + 1) * MINUTE,
        )
        for index in range(14)
    )
    account, strategies = configuration(projected=True)
    account["account_id"] = "pg-projection-" + namespace
    account["positions"][0]["instrument"] = instrument.key
    strategies[0]["instrument"] = instrument.key
    original = deepcopy(strategies)
    source = candles = selected = full = None
    owned = False
    try:
        with connect_database(settings) as connection:
            assert connection.execute("SELECT current_database(),current_user").fetchone() == (
                "omega_test",
                "omega_app",
            )
            for table in ("market.candles_1m", "market.auxiliary_minutes"):
                assert connection.execute(
                    "SELECT count(*) FROM " + table + " WHERE instrument=%s", (instrument.key,)
                ).fetchone() == (0,)
        owned = True
        candles = PostgresCandleStore(settings)
        for candle in rows:
            candles.put(MinuteObservation(candle, "rest", candle.close_time_ms))
        source = CommittedAnalysisFrameSource(root)
        read_frames = source.read_frames
        calls = []

        def counted(*args, **kwargs):
            calls.append((args, kwargs))
            return read_frames(*args, **kwargs)

        source.read_frames = counted
        selector = SimpleNamespace(source=source)
        selected = MarginTradingSession(root / "selected", account, strategies, START, create=True)
        reference = (
            MarginBracketSnapshot(
                instrument,
                (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
                START,
                START,
                "explicit_synthetic_test_reference",
                "a" * 64,
            ),
        )
        selected.resume("explicit-test-start")
        assert selected.analysis_input_channels == {}
        for expected in rows:
            loaded = source.read(
                (instrument.key,),
                expected.open_time_ms,
                expected.close_time_ms,
                limit=1,
                as_of_ms=expected.close_time_ms,
            )
            assert loaded == (expected,)
            options, reason = MarginTradingService._analysis_inputs(
                selector, selected, {}, loaded, as_of_ms=expected.close_time_ms
            )
            assert options == {} and reason is None
            selected.run_once(
                loaded, as_of_ms=expected.close_time_ms, brackets=reference, **options
            )
        assert not calls
        assert source._auxiliary_store is None
        assert source.destination_id == database_destination_id(settings)
        assert any(
            isinstance(item.fact, FillEvent)
            for item in selected.execution.snapshot().account.ledger
        )
        assert selected._meta["configuration"]["strategies"] == strategies == original

        # Positive control proves the counter/source is live: the original full
        # profile really requests its unobserved auxiliary dependencies.
        original_rows = deepcopy(strategies)
        del original_rows[0]["analysis_execution"]
        original_account = deepcopy(account)
        original_account["account_id"] += "-full"
        full = MarginTradingSession(
            root / "full", original_account, original_rows, START, create=True
        )
        assert set(full.analysis_input_channels[instrument.key]) == {
            "trade_flow",
            "depth",
            "metric",
        }
        options, _reason = MarginTradingService._analysis_inputs(
            selector, full, {}, (rows[0],), as_of_ms=rows[0].close_time_ms
        )
        assert len(calls) == 1 and len(options["prepared_frames"]) == 1
        assert options["prepared_frames"][0].frame.candle == rows[0]
    finally:
        failures = []
        for resource in (selected, full, source, candles):
            if resource is not None:
                try:
                    resource.close()
                except Exception as error:
                    failures.append(error)
        if owned and not failures:
            with connect_database(settings) as connection:
                assert connection.execute("SELECT current_database(),current_user").fetchone() == (
                    "omega_test",
                    "omega_app",
                )
                for table in ("market.auxiliary_minutes", "market.candles_1m"):
                    connection.execute(
                        "DELETE FROM " + table + " WHERE instrument=%s", (instrument.key,)
                    )
                    assert connection.execute(
                        "SELECT count(*) FROM " + table + " WHERE instrument=%s", (instrument.key,)
                    ).fetchone() == (0,)
        if failures:
            raise ExceptionGroup(
                "Owned projection test handles did not close; UUID rows kept", failures
            )
