"""Calendar HTTP response wiring; DTO fixtures are not database integration proof."""

from datetime import UTC, datetime

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from omega.gui.calendar_bars import calendar_bounds
from omega.gui.charts_page import ChartsPage


class Client(QObject):
    candlesReceived = Signal(str, dict)
    connectionChanged = Signal(bool)
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)

    def __init__(self):
        super().__init__()
        self.attached = True
        self.calls = []

    def request(self, method, path, *, tag):
        self.calls.append((method, path, tag))


@pytest.fixture(scope="module")
def app():
    return QApplication.instance() or QApplication(["calendar-api-tests"])


@pytest.fixture
def page(app):
    value = ChartsPage(Client())
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def stamp(year, month, day=1):
    return int(datetime(year, month, day, tzinfo=UTC).timestamp()) * 1000


def response(instrument="binance:usdm:BTCUSDT", period="1w"):
    begin, end = calendar_bounds(stamp(2024, 2, 7), period)
    count = (end - begin) // 60000
    return {
        "schema_version": 1,
        "instrument": instrument,
        "period": period,
        "source": "database",
        "begin_ms": begin,
        "end_ms": end,
        "closed_until_ms": end,
        "candles": [
            {
                "open_time_ms": begin,
                "open": "10",
                "high": "12",
                "low": "9",
                "close": "11",
                "volume": "1234",
            }
        ],
        "quality": {
            "status": "observed",
            "source_minutes": count,
            "missing_minutes": 0,
            "partial_periods": 0,
            "bars": [
                {
                    "start_ms": begin,
                    "end_ms": end,
                    "expected_minutes": count,
                    "requested_minutes": count,
                    "closed_minutes": count,
                    "observed_minutes": count,
                    "missing_minutes": 0,
                    "prefix_partial": False,
                    "suffix_partial": False,
                    "is_closed": True,
                    "complete": True,
                }
            ],
        },
    }


def calendar_calls(page):
    return [call for call in page.client.calls if "/api/chart-candles?" in call[1]]


def deliver(page, data=None, index=-1):
    tag = calendar_calls(page)[index][2]
    page.client.resultReceived.emit(tag, data or response())
    return tag


@pytest.mark.parametrize("period,limit", [("1w", 52), ("1M", 12)])
def test_calendar_selection_requests_bounded_aggregate_and_does_not_reaggregate(
    page, period, limit
):
    pane = page.panes[0]
    pane.set_timeframe(period)
    method, path, tag = calendar_calls(page)[-1]
    assert method == "GET" and f"period={period}&limit={limit}" in path
    assert "instrument=binance%3Ausdm%3ABTCUSDT" in path
    assert tag.startswith("chart-calendar:")
    assert "10,000" not in pane.detail.text()
    data = response(period=period)
    deliver(page, data)
    assert len(pane.chart.candles) == 1  # A server aggregate is not a one-minute source row.
    assert pane.chart.candles[0].volume == 1234
    assert pane.chart.calendar_period == period
    assert pane.source_kind == "database" and "완전봉 1" in pane.detail.text()
    assert not pane.raw
    assert len(pane._series["VOL"]) == 1


def test_shared_instrument_period_coalesces_pending_request_and_updates_each_pane(page):
    page.panes[0].set_timeframe("1w")
    page.add_chart("binance:usdm:BTCUSDT")
    page.panes[1].set_timeframe("1w")
    page.refresh()
    assert len(calendar_calls(page)) == 1
    deliver(page)
    assert all(len(pane.chart.candles) == 1 for pane in page.panes)
    page.refresh()
    assert len(calendar_calls(page)) == 2  # An explicit refresh is not cached forever.


def test_late_period_and_symbol_results_do_not_replace_current_selection(page):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    week_tag = calendar_calls(page)[-1][2]
    pane.set_timeframe("1M")
    month_tag = calendar_calls(page)[-1][2]
    page.client.resultReceived.emit(week_tag, response())
    assert pane.minutes == "1M" and not pane.chart.candles
    pane.selection.setText("binance:spot:ETHUSDT")
    page.client.resultReceived.emit(month_tag, response(period="1M"))
    assert not pane.chart.candles
    deliver(page, response("binance:spot:ETHUSDT", "1M"))
    assert len(pane.chart.candles) == 1 and pane.selection.text() == "binance:spot:ETHUSDT"


def test_removed_pane_and_disconnect_fence_late_responses(page):
    page.add_chart("binance:spot:ETHUSDT")
    removed = page.panes[-1]
    removed.set_timeframe("1w")
    tag = calendar_calls(page)[-1][2]
    page._remove_pane(removed)
    page.client.resultReceived.emit(tag, response("binance:spot:ETHUSDT"))
    pane = page.panes[0]
    pane.set_timeframe("1M")
    tag = calendar_calls(page)[-1][2]
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    page.client.resultReceived.emit(tag, response(period="1M"))
    assert not pane.chart.candles
    assert "연결 끊김" in pane.source_label.text()


def test_minute_snapshot_cannot_replace_calendar_bars_but_is_retained_for_fixed_period(page):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    deliver(page)
    calendar = pane.chart.candles
    minute = {
        "open_time_ms": 0,
        "open": "100",
        "high": "101",
        "low": "99",
        "close": "100",
        "volume": "1",
    }
    page.client.candlesReceived.emit(
        pane.selection.text(),
        {"source": "manual_paper", "candles": [minute], "quality": {"missing_minutes": 9}},
    )
    assert pane.chart.candles == calendar and pane.source_kind == "database"
    assert pane.quality["missing_minutes"] == 0
    detail = pane.detail.text()
    page.client.failed.emit("candles:" + pane.selection.text(), "old minute request failed")
    assert pane.detail.text() == detail
    pane.set_timeframe(1)
    assert pane.chart.candles[0].close == 100 and pane.source_kind == "manual_paper"
    assert pane.quality["missing_minutes"] == 9


@pytest.mark.parametrize(
    "kind",
    [
        "identity",
        "period",
        "schema",
        "boundary",
        "unconfirmed",
        "missing_count",
        "incomplete_flag",
        "summary_count",
        "duplicate",
    ],
)
def test_invalid_calendar_result_is_explicit_failure_and_does_not_destroy_last_snapshot(page, kind):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    deliver(page)
    previous = pane.chart.candles
    page.refresh()
    data = response()
    if kind == "identity":
        data["instrument"] = "binance:spot:ETHUSDT"
    elif kind == "period":
        data["period"] = "1M"
    elif kind == "schema":
        data["schema_version"] = True
    elif kind == "boundary":
        data["quality"]["bars"][0]["start_ms"] += 60000
    elif kind == "unconfirmed":
        data["closed_until_ms"] -= 60000
    elif kind == "missing_count":
        data["quality"]["bars"][0]["missing_minutes"] = 1
    elif kind == "incomplete_flag":
        data["quality"]["bars"][0]["complete"] = False
    elif kind == "summary_count":
        data["quality"]["source_minutes"] = 0
    else:
        data["candles"] *= 2
    deliver(page, data)
    assert pane.chart.candles == previous
    assert pane._calendar_state == "failed"
    assert "조회 불가" in pane.detail.text() and "10,000" not in pane.detail.text()


@pytest.mark.parametrize("state", ["gapped", "partial", "unavailable"])
def test_empty_calendar_quality_is_not_fabricated_as_zero_price_or_complete(page, state):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    data = response()
    data["candles"] = []
    quality = data["quality"]
    quality["status"] = state
    if state == "unavailable":
        data["source"] = "unavailable"
        quality.update(source_minutes=0, missing_minutes=0, bars=[])
    else:
        bar = quality["bars"][0]
        if state == "partial":
            data["closed_until_ms"] = data["begin_ms"] + 60000
            bar.update(closed_minutes=1, observed_minutes=1, is_closed=False, complete=False)
            quality.update(source_minutes=1, partial_periods=1)
        else:
            bar.update(observed_minutes=0, missing_minutes=bar["closed_minutes"], complete=False)
            quality.update(source_minutes=0, missing_minutes=bar["missing_minutes"])
    deliver(page, data)
    assert not pane.chart.candles and pane._calendar_state == "ready"
    assert "10,000" not in pane.detail.text()
    assert ("조회 불가" in pane.detail.text()) == (state == "unavailable")
    if state == "partial":
        assert "부분 구간 1" in pane.detail.text() and pane.quality["missing_minutes"] == 0


def test_unsupported_endpoint_failure_and_old_epoch_result_are_not_success(page):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    tag = calendar_calls(page)[-1][2]
    page.client.failed.emit(tag, "HTTP 404")
    assert pane._calendar_state == "failed" and "404" in pane.detail.text()
    page.update_state({"core": {"epoch": "first"}})
    page.refresh()
    tag = calendar_calls(page)[-1][2]
    page.update_state({"core": {"epoch": "second"}})
    page.client.resultReceived.emit(tag, response())
    assert not pane.chart.candles and not pane._calendar_data


def test_missing_period_quality_cannot_hide_a_fully_absent_interval(page):
    pane = page.panes[0]
    pane.set_timeframe("1w")
    data = response()
    data["begin_ms"] -= 7 * 1440 * 60000
    deliver(page, data)
    assert pane._calendar_state == "failed" and not pane.chart.candles


@pytest.mark.parametrize("width", [800, 1360])
def test_calendar_labels_and_period_controls_remain_accessible_at_window_sizes(page, app, width):
    page.resize(width, 900)
    page.show()
    pane = page.panes[0]
    pane.set_timeframe("1M")
    deliver(page, response(period="1M"))
    app.processEvents()
    pane.period_scroll.horizontalScrollBar().setValue(100000)
    app.processEvents()
    assert pane.period_buttons["1M"].width() >= pane.period_buttons["1M"].sizeHint().width()
    assert pane.detail.wordWrap()
    assert not pane.detail.text().startswith("1M은 완전한 달력 구간")
