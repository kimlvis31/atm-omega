"""UTC calendar display semantics; complete minute fixtures are synthetic observations."""

import os
import time
from datetime import UTC, datetime, timedelta

import pytest

from omega.gui.calendar_bars import (
    aggregate_calendar,
    aggregate_calendar_with_quality,
    calendar_boundary,
    calendar_bounds,
    calendar_ticks,
    covered_minutes,
    utc_date,
)
from omega.gui.models import Candle

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MINUTE = 60000


def stamp(year, month, day=1, hour=0, minute=0):
    return (datetime(year, month, day, hour, minute, tzinfo=UTC) - EPOCH) // timedelta(
        milliseconds=1
    )


def minutes(begin, end):
    return tuple(Candle(timestamp, 10, 12, 9, 11, 1) for timestamp in range(begin, end, MINUTE))


@pytest.mark.parametrize(
    "date,expected",
    [
        ((2021, 1, 1), (2020, 12, 28)),
        ((2026, 1, 4), (2025, 12, 29)),
        ((2026, 1, 5), (2026, 1, 5)),
        ((1970, 1, 1), (1969, 12, 29)),
    ],
)
def test_iso_week_starts_monday_utc_including_year_boundaries(date, expected):
    opening, ending = calendar_bounds(stamp(*date), "1w")
    assert opening == stamp(*expected)
    assert utc_date(opening).weekday() == 0
    assert ending - opening == 7 * 1440 * MINUTE


@pytest.mark.parametrize("year,days", [(2023, 28), (2024, 29), (1900, 28), (2000, 29)])
def test_february_calendar_length_obeys_leap_year_and_century_rules(year, days):
    opening, ending = calendar_bounds(stamp(year, 2, 17, 16), "1M")
    assert opening == stamp(year, 2)
    assert ending == stamp(year, 3)
    assert (ending - opening) // MINUTE == days * 1440


@pytest.mark.parametrize(
    "count,expected",
    [
        (-1, (2026, 2)),
        (-3, (2025, 12)),
        (-15, (2024, 12)),
        (-27, (2023, 12)),
        (10, (2027, 1)),
        (0, (2026, 3)),
    ],
)
def test_signed_month_movement_never_uses_truncation_toward_zero(count, expected):
    assert calendar_boundary(stamp(2026, 3, 31, 23, 59), "1M", count) == stamp(*expected)


@pytest.mark.parametrize("zone", ["Asia/Seoul", "Pacific/Honolulu", "America/New_York"])
def test_calendar_rules_ignore_host_timezone(zone, monkeypatch):
    original = os.environ.get("TZ")
    try:
        monkeypatch.setenv("TZ", zone)
        if hasattr(time, "tzset"):
            time.tzset()
        # UTC Sunday: already Monday in Seoul; still Sunday on the US west coast.
        timestamp = stamp(2026, 1, 4, 23, 59)
        assert calendar_boundary(timestamp, "1w") == stamp(2025, 12, 29)
        assert calendar_boundary(timestamp, "1M") == stamp(2026, 1)
    finally:
        if original is None:
            monkeypatch.delenv("TZ", raising=False)
        else:
            monkeypatch.setenv("TZ", original)
        if hasattr(time, "tzset"):
            time.tzset()


def test_calendar_computation_never_uses_local_mktime_or_naive_fromtimestamp(monkeypatch):
    def forbidden(*_):
        raise AssertionError("host-local conversion used")

    monkeypatch.setattr(time, "mktime", forbidden)
    monkeypatch.setattr(time, "localtime", forbidden)
    assert calendar_boundary(stamp(1969, 12, 31), "1M", -13) == stamp(1968, 11)
    assert utc_date(-MINUTE) == datetime(1969, 12, 31, 23, 59, tzinfo=UTC)


@pytest.mark.parametrize(
    "period,opening,ending",
    [
        ("1w", (2025, 12, 29), (2026, 1, 5)),
        ("1M", (2024, 2, 1), (2024, 3, 1)),
        ("1M", (2025, 2, 1), (2025, 3, 1)),
    ],
)
def test_only_complete_closed_calendar_source_produces_immutable_ohlcv(period, opening, ending):
    begin, end = stamp(*opening), stamp(*ending)
    source = minutes(begin, end)
    assert not aggregate_calendar(source[:-1], period)
    assert not aggregate_calendar(source[1:], period)
    assert not aggregate_calendar(source[:123] + source[124:], period)
    result = aggregate_calendar(source, period)
    assert len(result) == 1
    assert result[0] == Candle(begin, 10, 12, 9, 11, len(source))
    assert covered_minutes(result, period) == len(source)
    # A future partial bucket cannot revise a previously complete calendar bar.
    assert aggregate_calendar(source + minutes(end, end + MINUTE), period) == result
    assert source[0].open_time_ms == begin and source[-1].open_time_ms == end - MINUTE


def test_calendar_ticks_are_true_boundaries_not_epoch_thursday_or_thirty_day_steps():
    weeks = calendar_ticks(stamp(2025, 12, 28), stamp(2026, 2), "1w")
    assert weeks and all(utc_date(value).weekday() == 0 for value in weeks)
    months = calendar_ticks(stamp(2024, 1, 15), stamp(2024, 5), "1M")
    assert months == tuple(stamp(2024, month) for month in (2, 3, 4, 5))
    assert len(set(b - a for a, b in zip(months, months[1:], strict=False))) == 3


def test_live_api_snapshot_limit_is_insufficient_and_not_interpolated():
    source = minutes(stamp(2026, 1, 5), stamp(2026, 1, 5) + 10000 * MINUTE)
    assert not aggregate_calendar(source, "1w")
    assert not aggregate_calendar(source, "1M")


def test_invalid_calendar_input_and_datetime_overflow_are_explicit():
    source = minutes(stamp(2024, 2), stamp(2024, 2) + 3 * MINUTE)
    with pytest.raises(ValueError, match="unique"):
        aggregate_calendar(source + source, "1M")
    with pytest.raises(ValueError, match="aligned"):
        aggregate_calendar((Candle(1, 10, 12, 9, 11, 1),), "1w")
    with pytest.raises(ValueError, match="unsupported"):
        calendar_boundary(stamp(2026, 1), "30d")
    with pytest.raises(ValueError):
        calendar_boundary(stamp(2026, 1), "1M", True)
    with pytest.raises(ValueError, match="outside"):
        calendar_boundary(stamp(1, 1), "1M", -1)


@pytest.mark.parametrize("year,days", [(2023, 28), (2024, 29), (1900, 28), (2000, 29)])
def test_quality_uses_full_actual_february_and_no_fabricated_missing_prefix(year, days):
    begin, end = stamp(year, 2), stamp(year, 3)
    result = aggregate_calendar_with_quality(
        minutes(begin, end), "1M", begin_ms=begin, end_ms=end, closed_until_ms=end
    )
    row = result.quality[0]
    assert result.candles == aggregate_calendar(minutes(begin, end), "1M")
    assert row.expected_minutes == row.requested_minutes == row.closed_minutes == days * 1440
    assert row.observed_minutes == days * 1440 and row.missing_minutes == 0
    assert row.complete and row.is_closed and not row.prefix_partial and not row.suffix_partial


def test_quality_separates_first_partial_prefix_internal_gap_and_absent_calendar_week():
    first, second, third, end = (
        stamp(2025, 12, 29),
        stamp(2026, 1, 5),
        stamp(2026, 1, 12),
        stamp(2026, 1, 19),
    )
    begin = first + MINUTE
    source = (
        minutes(begin, second) + minutes(third, third + MINUTE) + minutes(third + 2 * MINUTE, end)
    )
    result = aggregate_calendar_with_quality(
        source, "1w", begin_ms=begin, end_ms=end, closed_until_ms=end
    )
    assert not result.candles
    prefix, absent, gapped = result.quality
    assert (prefix.start_ms, absent.start_ms, gapped.start_ms) == (first, second, third)
    assert prefix.prefix_partial and prefix.missing_minutes == 0
    assert prefix.observed_minutes == prefix.closed_minutes == 10079
    assert not prefix.complete
    assert absent.observed_minutes == 0 and absent.missing_minutes == 10080
    assert not absent.prefix_partial and not absent.suffix_partial
    assert gapped.missing_minutes == 1 and not gapped.complete


def test_quality_current_month_closed_minutes_are_not_future_gaps_or_a_complete_bar():
    begin, end, cutoff = stamp(2026, 12), stamp(2027, 2), stamp(2027, 1, 1, 0, 2)
    source = minutes(begin, cutoff)
    result = aggregate_calendar_with_quality(
        source, "1M", begin_ms=begin, end_ms=end, closed_until_ms=cutoff
    )
    assert len(result.candles) == 1 and result.candles[0].open_time_ms == begin
    closed, current = result.quality
    assert closed.complete and closed.is_closed
    assert current.start_ms == stamp(2027, 1) and current.end_ms == end
    assert current.expected_minutes == current.requested_minutes == 31 * 1440
    assert current.closed_minutes == current.observed_minutes == 2
    assert current.missing_minutes == 0 and not current.is_closed and not current.complete
    assert not current.suffix_partial  # Calendar scope was fully requested, time has not elapsed.
    assert (
        aggregate_calendar_with_quality(
            (), "1M", begin_ms=begin, end_ms=end, closed_until_ms=begin - MINUTE
        )
        .quality[0]
        .closed_minutes
        == 0
    )


def test_quality_requested_tail_is_distinct_from_missing_closed_data_and_empty_range():
    begin, end = stamp(2026, 1, 5), stamp(2026, 1, 5, 0, 2)
    result = aggregate_calendar_with_quality(
        minutes(begin, end - MINUTE), "1w", begin_ms=begin, end_ms=end, closed_until_ms=end
    )
    row = result.quality[0]
    assert row.suffix_partial and row.is_closed is False
    assert row.requested_minutes == row.closed_minutes == 2
    assert row.observed_minutes == row.missing_minutes == 1
    empty = aggregate_calendar_with_quality(
        (), "1w", begin_ms=begin, end_ms=begin, closed_until_ms=end
    )
    assert not empty.candles and not empty.quality


@pytest.mark.parametrize("replacement", ["duplicate", "conflict", "reversed", "future", "outside"])
def test_quality_rejects_unordered_conflicting_or_unconfirmed_observations(replacement):
    begin, end = stamp(2026, 1, 5), stamp(2026, 1, 5, 0, 2)
    source = minutes(begin, end)
    if replacement == "duplicate":
        source += source[-1:]
    elif replacement == "conflict":
        source += (Candle(end - MINUTE, 10, 13, 9, 12, 2),)
    elif replacement == "reversed":
        source = source[::-1]
    elif replacement == "future":
        source += minutes(end, end + MINUTE)
    else:
        source = minutes(begin - MINUTE, end)
    with pytest.raises(ValueError):
        aggregate_calendar_with_quality(
            source, "1w", begin_ms=begin, end_ms=end + MINUTE, closed_until_ms=end
        )


@pytest.mark.parametrize(
    "field,value", [("begin_ms", True), ("end_ms", 1), ("closed_until_ms", 0.0)]
)
def test_quality_rejects_ambiguous_request_boundaries(field, value):
    parameters = dict(begin_ms=0, end_ms=MINUTE, closed_until_ms=MINUTE)
    parameters[field] = value
    with pytest.raises(ValueError, match="aligned"):
        aggregate_calendar_with_quality((), "1w", **parameters)


@pytest.mark.parametrize(
    "values", [(10, 9, 8, 11, 1), (10, 12, 9, 11, -1), (10, float("inf"), 9, 11, 1)]
)
def test_quality_does_not_aggregate_invalid_ohlcv(values):
    with pytest.raises(ValueError, match="OHLCV"):
        aggregate_calendar_with_quality(
            (Candle(0, *values),), "1w", begin_ms=0, end_ms=MINUTE, closed_until_ms=MINUTE
        )


@pytest.fixture
def chart():
    pytest.importorskip("PySide6")
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from omega.gui.charts import ChartDrawer

    app = QApplication.instance() or QApplication(["calendar-render-test"])
    value = ChartDrawer()
    value.resize(900, 600)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def test_renderer_calendar_membership_range_and_gap_aware_ema(chart):
    source = tuple(
        Candle(
            calendar_boundary(stamp(2023, 1), "1M", offset),
            10 + offset,
            12 + offset,
            9 + offset,
            11 + offset,
            100,
        )
        for offset in range(24)
    )
    chart.set_calendar_period("1M")
    chart.set_candles(source)
    chart.set_options(ema=True)
    chart.fit_all()
    chart._geometry()
    assert chart._time_bounds() == (stamp(2023, 1), stamp(2025, 1))
    assert chart._ema[:19] == (None,) * 19 and chart._ema[19] is not None
    chart.set_cursor(stamp(2024, 2, 29, 23, 59))
    assert chart._visible()[chart.cursor].open_time_ms == stamp(2024, 2)
    chart.set_cursor(stamp(2024, 3))
    assert chart._visible()[chart.cursor].open_time_ms == stamp(2024, 3)
    chart.set_candles(source[:13] + source[14:])  # February 2024 is absent.
    chart.set_cursor(stamp(2024, 2, 15))
    assert chart.cursor == -1
    chart.set_time_range(stamp(2024, 2), stamp(2024, 3) - 1)
    assert not chart._visible()
    chart.fit_all()
    assert all(value is None for value in chart._ema)
    assert not chart.grab().isNull()


def test_calendar_range_emission_uses_real_month_end_and_fixed_mode_remains_compatible(chart):
    source = (Candle(stamp(2024, 2), 10, 12, 9, 11, 100),)
    chart.set_calendar_period("1M")
    chart.set_candles(source)
    ranges = []
    chart.rangeChanged.connect(lambda first, last: ranges.append((first, last)))
    chart.fit_all()
    assert ranges[-1] == (stamp(2024, 2), stamp(2024, 3) - 1)
    assert chart._bar_center(stamp(2024, 2)) == (stamp(2024, 2) + stamp(2024, 3)) / 2
    chart.set_interval_minutes(1)
    chart.set_candles(minutes(stamp(2024, 2), stamp(2024, 2) + 2 * MINUTE))
    assert chart.calendar_period is None
    chart._geometry()
    assert chart._time_bounds()[1] == stamp(2024, 2) + 2 * MINUTE
