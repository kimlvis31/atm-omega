"""Independent UTC calendar/readonly display-query checks using grouped DB rows."""

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from types import SimpleNamespace

import pytest

from omega.domain.calendar import calendar_query_window
from omega.domain.errors import StorageError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.calendar_candles import CalendarCandleReader, format_calendar
from omega.infrastructure.postgres.schema import MIGRATIONS, migration_sql
from omega.runtime.calendar_view import CalendarView

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def instant(year, month, day):
    return int((datetime(year, month, day, tzinfo=UTC) - EPOCH).total_seconds() * 1000)


def group(begin, end, count=None):
    return [
        EPOCH + timedelta(milliseconds=begin),
        count or (end - begin) // 60_000,
        EPOCH + timedelta(milliseconds=begin),
        EPOCH + timedelta(milliseconds=end - 60_000),
        *(Decimal(value) for value in ("100.01", "120.02", "80.03", "110.04", "12345.6789")),
    ]


def test_leap_february_exact_amounts_are_one_complete_month_and_week_is_10080_minutes():
    begin, end = instant(2024, 2, 1), instant(2024, 3, 1)
    assert calendar_query_window(end, "1M", 1) == (begin, end)
    result = format_calendar(INSTRUMENT, "1M", begin, end, end, [group(begin, end)])
    assert result["quality"]["source_minutes"] == 29 * 1440
    assert result["quality"]["status"] == "observed"
    assert result["candles"][0]["open"] == "100.01"
    assert result["candles"][0]["volume"] == "12345.6789"
    begin, end = instant(2024, 2, 26), instant(2024, 3, 4)
    result = format_calendar(INSTRUMENT, "1w", begin, end, end, [group(begin, end)])
    assert result["quality"]["bars"][0]["expected_minutes"] == 10080
    assert len(result["candles"]) == 1


@pytest.mark.parametrize("period,limit", [("1M", 13), ("1w", 53), ("1M", True)])
def test_calendar_query_caps_are_validated_before_db_work(period, limit):
    with pytest.raises(ValueError):
        calendar_query_window(instant(2024, 3, 1), period, limit)


def test_caps_include_only_requested_calendar_buckets_and_reject_unaligned_end():
    end = instant(2025, 1, 1)
    assert calendar_query_window(end, "1M", 12) == (instant(2024, 1, 1), end)
    monday = instant(2024, 12, 30)
    assert calendar_query_window(monday, "1w", 52)[0] == monday - 52 * 7 * 1440 * 60_000
    with pytest.raises(ValueError):
        calendar_query_window(end + 1, "1M", 1)


def test_current_partial_month_has_no_false_complete_candle_and_no_false_gap():
    begin, end = instant(2024, 2, 1), instant(2024, 2, 15)
    result = format_calendar(INSTRUMENT, "1M", begin, end, end, [group(begin, end)])
    quality = result["quality"]
    assert result["candles"] == [] and quality["status"] == "partial"
    assert quality["missing_minutes"] == 0 and quality["partial_periods"] == 1
    assert quality["bars"][0]["suffix_partial"] and not quality["bars"][0]["is_closed"]


def test_missing_internal_minute_and_fully_absent_bucket_are_gaps_not_zero_bars():
    begin, end = instant(2024, 2, 1), instant(2024, 3, 1)
    row = group(begin, end)
    row[1] -= 1
    result = format_calendar(INSTRUMENT, "1M", begin, end, end, [row])
    assert result["candles"] == [] and result["quality"]["missing_minutes"] == 1
    absent = format_calendar(INSTRUMENT, "1M", begin, end, end, [])
    assert absent["quality"]["missing_minutes"] == 29 * 1440
    assert absent["quality"]["bars"][0]["observed_minutes"] == 0


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.__setitem__(1, True),
        lambda row: row.__setitem__(1, row[1] + 1),
        lambda row: row.__setitem__(5, Decimal("NaN")),
        lambda row: row.__setitem__(6, Decimal("111")),
        lambda row: row.__setitem__(8, Decimal("-1")),
        lambda row: row.__setitem__(4, 100.01),
        lambda row: row.__setitem__(2, row[2].replace(tzinfo=None)),
    ],
)
def test_grouped_db_counts_times_and_amounts_must_be_valid(mutation):
    begin, end = instant(2024, 2, 1), instant(2024, 3, 1)
    row = group(begin, end)
    mutation(row)
    with pytest.raises(StorageError):
        format_calendar(INSTRUMENT, "1M", begin, end, end, [row])


def test_duplicate_and_out_of_requested_range_groups_are_rejected():
    begin, end = instant(2024, 2, 1), instant(2024, 3, 1)
    row = group(begin, end)
    with pytest.raises(StorageError):
        format_calendar(INSTRUMENT, "1M", begin, end, end, [row, row])
    with pytest.raises(StorageError):
        format_calendar(INSTRUMENT, "1M", begin, end, end, [group(instant(2024, 1, 1), begin)])


class Connection:
    def __init__(self, rows, *, checksum=True, fail=False):
        self.rows, self.checksum, self.fail = rows, checksum, fail
        self.calls, self.fetch_limit, self.closed = [], None, False

    @contextmanager
    def transaction(self):
        yield

    def execute(self, sql, parameters=None):
        self.calls.append((sql, parameters))
        if sql.lstrip().startswith("WITH bucketed") and self.fail:
            raise OSError("synthetic query failure")
        return self

    def fetchall(self):
        return [
            (
                version,
                sha256(migration_sql(version).encode()).hexdigest() if self.checksum else "wrong",
            )
            for version in MIGRATIONS
        ]

    def fetchone(self):
        return (1,)

    def fetchmany(self, limit):
        self.fetch_limit = limit
        return self.rows[:limit]

    def close(self):
        self.closed = True


def test_reader_uses_readonly_checksum_validation_parameters_and_bounded_fetch_then_closes():
    begin, end = instant(2024, 2, 1), instant(2024, 3, 1)
    connection = Connection([group(begin, end)])
    reader = CalendarCandleReader(object(), connect=lambda _settings: connection)
    assert reader.read(INSTRUMENT, "1M", 1, end_ms=end, closed_until_ms=end)["candles"]
    assert connection.calls[0] == ("SET TRANSACTION READ ONLY", None)
    sql, values = connection.calls[-1]
    assert INSTRUMENT.key not in sql and "date_trunc(%s" in sql
    assert values[:2] == ("month", INSTRUMENT.key)
    assert values[2:] == (
        EPOCH + timedelta(milliseconds=begin),
        EPOCH + timedelta(milliseconds=end),
        EPOCH + timedelta(milliseconds=end),
    )
    assert connection.fetch_limit == 2 and connection.closed


@pytest.mark.parametrize("options", [{"checksum": False}, {"fail": True}])
def test_reader_closes_on_bad_migration_checksum_or_query_failure(options):
    connection = Connection([], **options)
    with pytest.raises(StorageError):
        CalendarCandleReader(None, connect=lambda _settings: connection).read(
            INSTRUMENT, "1M", 1, end_ms=instant(2024, 3, 1), closed_until_ms=instant(2024, 3, 1)
        )
    assert connection.closed


def test_reader_rejects_excess_group_result_and_future_query_before_connect():
    end = instant(2024, 3, 1)
    connection = Connection([group(instant(2024, 2, 1), end)] * 2)
    reader = CalendarCandleReader(None, connect=lambda _settings: connection)
    with pytest.raises(StorageError, match="bounded"):
        reader.read(INSTRUMENT, "1M", 1, end_ms=end, closed_until_ms=end)
    assert connection.closed
    with pytest.raises(ValidationError, match="not yet closed"):
        CalendarCandleReader(None, connect=lambda _: pytest.fail("must not connect")).read(
            INSTRUMENT, "1M", 1, end_ms=end, closed_until_ms=end - 60_000
        )


def test_view_cache_expires_at_two_seconds_and_returns_independent_copies(monkeypatch, tmp_path):
    from omega.runtime import calendar_view

    current, calls = [0.0], []
    monkeypatch.setattr(calendar_view, "time", SimpleNamespace(monotonic=lambda: current[0]))
    monkeypatch.setattr(calendar_view, "load_database_settings", lambda _: object())

    class Reader:
        def __init__(self, _settings):
            pass

        def read(self, *args, **options):
            calls.append((args, options))
            return {"candles": [{"close": "100"}], "quality": {"status": "observed"}}

    view = CalendarView(tmp_path, clock_ms=lambda: instant(2024, 3, 1), reader_factory=Reader)
    first = view(INSTRUMENT.key, "1M", 1)
    first["candles"][0]["close"] = "corrupted caller copy"
    current[0] = 1.999
    assert view(INSTRUMENT.key, "1M", 1)["candles"][0]["close"] == "100"
    assert len(calls) == 1
    current[0] = 2
    view(INSTRUMENT.key, "1M", 1)
    assert len(calls) == 2


def test_view_unavailable_is_explicit_and_invalid_future_range_is_not_hidden(tmp_path):
    end = instant(2024, 3, 1)
    view = CalendarView(tmp_path, enabled=False, clock_ms=lambda: end)
    result = view(INSTRUMENT.key, "1M", 1)
    assert result["source"] == result["quality"]["status"] == "unavailable"
    assert result["candles"] == []
    with pytest.raises(ValidationError, match="future"):
        view(INSTRUMENT.key, "1M", 1, end_ms=end + 60_000)
    with pytest.raises(ValidationError):
        view(INSTRUMENT.key, "1M", 13)
