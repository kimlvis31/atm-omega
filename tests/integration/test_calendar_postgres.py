"""Opt-in synthetic calendar observations in the isolated Omega test database."""

import os
from pathlib import Path
from uuid import uuid4

import pytest
from database_safety import isolated_test_port

from omega.domain.calendar import MINUTE_MS, calendar_boundary
from omega.domain.models import Instrument, Market
from omega.infrastructure.calendar_candles import CalendarCandleReader
from omega.infrastructure.candles import EPOCH, connect_database
from omega.infrastructure.postgres.config import load_database_settings
from omega.infrastructure.postgres.schema import migrate

pytestmark = pytest.mark.database


@pytest.fixture(scope="module")
def calendar_database():
    path = os.environ.get("OMEGA_TEST_DB_CONFIG")
    if not path:
        pytest.fail("explicit isolated OMEGA_TEST_DB_CONFIG is required")
    settings = load_database_settings(Path(path))
    if (settings.host, settings.port, settings.dbname, settings.user) != (
        "127.0.0.1",
        isolated_test_port(),
        "omega_test",
        "omega_app",
    ):
        pytest.fail("refusing calendar test outside isolated Omega DB")
    with connect_database(settings) as connection:
        assert connection.execute("SELECT current_database()").fetchone() == ("omega_test",)
        migrate(connection)
    return settings


@pytest.mark.parametrize(
    "period,begin,end,gap",
    [
        ("1w", "2026-01-05T00:00:00+00:00", "2026-01-12T00:00:00+00:00", None),
        ("1M", "2024-02-01T00:00:00+00:00", "2024-03-01T00:00:00+00:00", None),
        ("1M", "2024-02-01T00:00:00+00:00", "2024-03-01T00:00:00+00:00", 123),
    ],
)
def test_real_sql_complete_week_leap_month_and_missing_minute(
    calendar_database, period, begin, end, gap
):
    from datetime import datetime

    start, stop = datetime.fromisoformat(begin), datetime.fromisoformat(end)
    start_ms, end_ms = (
        int((start - EPOCH).total_seconds() * 1000),
        int((stop - EPOCH).total_seconds() * 1000),
    )
    count = (end_ms - start_ms) // MINUTE_MS
    instrument = Instrument("binance", Market.USDM, "CAL" + uuid4().hex[:24].upper())
    with connect_database(calendar_database) as connection:
        try:
            connection.execute(
                """
                INSERT INTO market.candles_1m
                  (instrument,open_time,close_time,o,h,l,c,v,source,received_ms)
                SELECT %s, %s::timestamptz + idx*interval '1 minute',
                       %s::timestamptz + (idx+1)*interval '1 minute',
                       100,120,80,101,0.5,'rest',%s
                FROM generate_series(0,%s) AS idx WHERE idx<>%s
            """,
                (
                    instrument.key,
                    start,
                    start,
                    end_ms + 1,
                    count - 1,
                    gap if gap is not None else -1,
                ),
            )
            reader = CalendarCandleReader(calendar_database)
            result = reader.read(instrument, period, 1, end_ms=end_ms, closed_until_ms=end_ms)
            assert result["quality"]["source_minutes"] == count - (gap is not None)
            quality = result["quality"]["bars"][0]
            assert quality["start_ms"] == calendar_boundary(start_ms, period)
            assert quality["expected_minutes"] == count
            if gap is None:
                assert quality["complete"] and result["quality"]["missing_minutes"] == 0
                assert result["candles"] == [
                    {
                        "open_time_ms": start_ms,
                        "open": "100",
                        "high": "120",
                        "low": "80",
                        "close": "101",
                        "volume": str(count / 2),
                    }
                ]
            else:
                assert not result["candles"] and not quality["complete"]
                assert result["quality"]["missing_minutes"] == 1
        finally:
            connection.execute(
                "DELETE FROM market.candles_1m WHERE instrument=%s", (instrument.key,)
            )
