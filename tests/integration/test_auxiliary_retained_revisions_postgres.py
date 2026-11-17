"""Opt-in isolated PG retains every native revision; no accounts, candles or exchange."""

import os
from dataclasses import replace
from decimal import Decimal as D
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import PostgresAuxiliaryStore
from omega.infrastructure.candles import connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.ports.auxiliary import AuxiliaryReadRange

pytestmark = pytest.mark.database
START = 1700000100000


def test_actual_pg_retained_revisions_preserve_prior_cutoffs_and_original_edges(request):
    if not request.config.getoption("--database-tests"):
        pytest.skip("requires explicit isolated database opt-in")
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("OMEGA_TEST_DB_CONFIG must name the isolated test database")
    settings = load_database_settings(Path(path).resolve())
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail(
            "refusing native revisions test outside isolated localhost omega_test/omega_app"
        )
    instrument = Instrument("binance", Market.USDM, "TEST_REV_" + uuid4().hex[:16].upper() + "USDT")
    source = instrument.key + ":open_interest_history"
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database(),current_user").fetchone() == (
            "omega_test",
            "omega_app",
        )
        assert connection.execute(
            "SELECT count(*) FROM market.auxiliary_minutes WHERE instrument=%s", (instrument.key,)
        ).fetchone() == (0,)
    owner = None
    try:
        owner = PostgresAuxiliaryStore(settings)
        first = AuxiliaryInput(
            "open_interest",
            (("METRIC_OPENINTEREST", D(100)), ("SOURCE_PERIOD_MS", 300000)),
            START,
            START + 60000,
            START,
            START + 1,
            source=source,
        )
        r1 = owner.put(instrument, first)
        second = replace(
            first,
            values=(("METRIC_OPENINTEREST", D(0)), ("SOURCE_PERIOD_MS", 300000)),
            available_at_ms=START + 2,
        )
        r2 = owner.put(instrument, second, expected_previous=r1.checksum)
        third = replace(
            first,
            values=(("METRIC_OPENINTEREST", D(200)), ("SOURCE_PERIOD_MS", 300000)),
            available_at_ms=START + 3,
        )
        r3 = owner.put(instrument, third, expected_previous=r2.checksum)
        span = (AuxiliaryReadRange(instrument, "open_interest", source, START, START + 60000),)
        prior = owner.read_revisions(span, as_of_ms=START + 1)
        assert len(prior) == 1 and prior[0].observation == first
        both = owner.read_revisions(span, as_of_ms=START + 2)
        assert [row.revision for row in both] == [1, 2] and both[1].observation == second
        all_rows = owner.read_revisions(span, as_of_ms=START + 3)
        assert [row.revision for row in all_rows] == [1, 2, 3]
        assert [row.checksum for row in all_rows] == [r1.checksum, r2.checksum, r3.checksum]
        assert [row.previous_checksum for row in all_rows] == [None, r1.checksum, r2.checksum]
        assert [row.observation.available_at_ms for row in all_rows] == [
            START + 1,
            START + 2,
            START + 3,
        ]
        selected = owner.read_ranges(span, as_of_ms=START + 3)
        assert len(selected) == 1 and selected[0] == all_rows[-1]
        assert owner.read_revisions(span, as_of_ms=START) == ()
    finally:
        # Close success is required before deleting only this fresh UUID evidence.
        if owner is not None:
            owner.close()
        with connect_database(settings) as connection:
            with connection.transaction():
                connection.execute(
                    "DELETE FROM market.auxiliary_minutes WHERE instrument=%s", (instrument.key,)
                )
            assert connection.execute(
                "SELECT count(*) FROM market.auxiliary_minutes WHERE instrument=%s",
                (instrument.key,),
            ).fetchone() == (0,)
