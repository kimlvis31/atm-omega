"""Display aggregation is causal and separate from the immutable strategy/data source."""

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional Qt chart dependency is not installed",
)


@pytest.fixture
def api():
    from omega.gui import charts

    return charts


def source_rows(count, *, begin=0):
    from omega.gui.models import Candle

    return tuple(
        Candle((begin + index) * 60_000, 10 + index, 13 + index, 9 + index, 11 + index, index + 1)
        for index in range(count)
    )


def payload(rows, **metadata):
    from dataclasses import asdict

    return {
        "candles": [asdict(row) for row in rows],
        "source": "database",
        "quality": {"missing_minutes": 0},
        **metadata,
    }


@pytest.fixture(scope="module")
def qt_app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from omega.gui.presentation import configure_fonts

    app = QApplication.instance() or QApplication(["chart-workspace-tests"])
    configure_fonts(app)
    return app


@pytest.fixture
def workspace(qt_app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.charts_page import ChartsPage

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.attached = True
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    page = ChartsPage(Client())
    page.resize(1200, 850)
    page.show()
    qt_app.processEvents()
    yield page
    page.close()
    page.deleteLater()
    qt_app.processEvents()


def test_complete_utc_aggregation_preserves_ohlcv_and_input_identity(api):
    rows = source_rows(10)
    before = rows
    assert api.aggregate_utc(rows, 1) is rows
    result = api.aggregate_utc(rows, 5)
    assert len(result) == 2
    assert result[0].open_time_ms == 0
    assert result[0].open == rows[0].open
    assert result[0].high == max(row.high for row in rows[:5])
    assert result[0].low == min(row.low for row in rows[:5])
    assert result[0].close == rows[4].close
    assert result[0].volume == sum(row.volume for row in rows[:5])
    assert rows is before and len(rows) == 10


def test_missing_and_partial_minutes_never_become_complete_bars(api):
    rows = source_rows(12)
    assert len(api.aggregate_utc(rows, 5)) == 2
    missing = rows[:3] + rows[4:]
    result = api.aggregate_utc(missing, 5)
    assert [row.open_time_ms for row in result] == [5 * 60_000]
    leading_partial = source_rows(9, begin=1)
    assert [row.open_time_ms for row in api.aggregate_utc(leading_partial, 5)] == [5 * 60_000]
    assert api.aggregate_utc(source_rows(4), 5) == ()


@pytest.mark.parametrize("minutes", [3, 5, 15, 30, 60, 120, 240, 360, 480, 720, 1440, 4320])
def test_utc_session_boundaries_and_closed_bucket_causality(api, minutes):
    rows = source_rows(minutes * 2 + 1)
    before_close = api.aggregate_utc(rows[: minutes - 1], minutes)
    assert not before_close
    first = api.aggregate_utc(rows[:minutes], minutes)
    after = api.aggregate_utc(rows, minutes)
    assert len(after) == 2
    assert first[0] == after[0]
    assert after[1].open_time_ms == minutes * 60_000


def test_invalid_utc_input_cannot_be_silently_realigned(api):
    from dataclasses import replace

    rows = source_rows(5)
    with pytest.raises(ValueError, match="aligned"):
        api.aggregate_utc((replace(rows[0], open_time_ms=1),) + rows[1:], 5)
    with pytest.raises(ValueError, match="unique"):
        api.aggregate_utc(rows + rows, 5)
    with pytest.raises(ValueError, match="unsupported"):
        api.aggregate_utc(rows, 7)


def test_embedded_title_and_narrow_interval_strip_remain_usable(workspace, qt_app):
    from omega.gui.charts import TIMEFRAMES
    from omega.gui.presentation import stylesheet

    assert not workspace.title.isHidden()
    workspace.set_shell_embedded(True)
    assert workspace.title.isHidden()
    assert not workspace.notice.isHidden()
    assert not workspace.layout_mode.isHidden()
    pane = workspace.panes[0]
    assert set(pane.period_buttons) == set(TIMEFRAMES)
    assert len(pane.period_buttons) == 15
    assert {"1w", "1M"} <= pane.period_buttons.keys()
    workspace.resize(430, 680)
    workspace.setStyleSheet(stylesheet("light"))
    qt_app.processEvents()
    qt_app.processEvents()
    pane.period_scroll.horizontalScrollBar().setValue(10000)
    qt_app.processEvents()
    assert pane.period_scroll.horizontalScrollBar().maximum() > 0
    button = pane.period_buttons["1M"]
    assert all(
        button.width() >= button.sizeHint().width() for button in pane.period_buttons.values()
    )
    assert pane.period_scroll.viewport().height() >= button.sizeHint().height()
    assert (
        pane.period_scroll.viewport()
        .rect()
        .contains(button.mapTo(pane.period_scroll.viewport(), button.rect().center()))
    )
    button.click()
    assert pane.minutes == "1M"
    assert "10,000" in pane.detail.text() and "40,320" in pane.detail.text()
    workspace.set_shell_embedded(False)
    assert not workspace.title.isHidden()


def test_calendar_selection_preserves_source_and_api_bound(workspace):
    rows = source_rows(10000)
    before = payload(rows)
    instrument = workspace.rows[0][1].text()
    workspace.accept_candles(instrument, before)
    pane = workspace.panes[0]
    pane.set_timeframe("1w")
    assert pane.raw == rows and not pane.chart.candles
    assert "10,080" in pane.detail.text()
    pane.set_timeframe("1M")
    assert not pane.chart.candles and "40,320" in pane.detail.text()
    assert pane.source_kind == "database"
    assert all("limit=10000" in args[1] for args, _ in workspace.client.calls)


def test_timeframe_reselection_keeps_view_and_new_period_keeps_right_edge(workspace):
    rows = source_rows(500)
    instrument = workspace.rows[0][1].text()
    workspace.accept_candles(instrument, payload(rows))
    pane = workspace.panes[0]
    pane.chart.set_time_range(100 * 60000, 300 * 60000 - 1)
    before = pane.chart._time_bounds()
    pane.set_timeframe(1)
    assert pane.chart._time_bounds() == before
    pane.set_timeframe(5)
    assert pane.chart._time_bounds()[1] == before[1]
    assert pane.chart._manual_price_range is None
    assert pane.raw == rows


def test_display_ema_has_explicit_warmup_and_no_future_leakage(api):
    rows = source_rows(40)
    first = api.display_ema(rows[:25])
    after = api.display_ema(rows)
    assert first == after[:25]
    assert first[:19] == (None,) * 19
    assert first[19] == pytest.approx(sum(row.close for row in rows[:20]) / 20)
    assert len(first) == 25


def test_display_ema_restarts_warmup_after_missing_minutes(api):
    rows = source_rows(25) + source_rows(25, begin=30)
    result = api.display_ema(rows)
    assert result[24] is not None
    assert result[25:44] == (None,) * 19
    assert result[44] is not None


def test_chart_lod_volume_cache_and_no_overlap_bounds(api, qt_app):
    chart = api.ChartDrawer()
    rows = source_rows(10000)
    chart.resize(700, 500)
    chart.set_candles(rows)
    chart.set_options(ema=True)
    chart.fit_all()
    chart._geometry()
    cached = chart._paths
    assert len(chart._display) <= int(chart._plot().width() / 3)
    assert sum(path.elementCount() for path in chart._volume_paths) <= len(chart._display) * 6
    chart._geometry()
    assert chart._paths is cached
    assert not chart.grab().isNull()
    chart.set_time_range(20_000 * 60_000, 21_000 * 60_000)
    chart._geometry()
    assert chart._display == ()
    assert chart.candles is rows
    chart.set_candles(rows + source_rows(100, begin=20_000))
    chart._geometry()
    assert chart._display
    chart.reset_view()
    chart._geometry()
    assert chart._display
    volume_price_height = chart._plot().height()
    chart.set_options(volume=False)
    assert chart._plot().height() > volume_price_height
    chart.close()


def test_workspace_timeframe_controls_keep_source_and_report_missing_buckets(workspace):
    rows = source_rows(10)
    workspace.accept_candles("binance:usdm:BTCUSDT", payload(rows))
    pane = workspace.panes[0]
    raw = pane.raw
    pane.set_timeframe(5)
    assert len(pane.chart.candles) == 2
    assert pane.raw is raw
    pane.set_timeframe(60)
    assert pane.chart.candles == ()
    assert "60" in pane.chart.empty_message
    pane.set_timeframe(1)
    assert pane.chart.candles is raw


def test_workspace_layout_switching_preserves_panes_and_deduplicates_queries(workspace, qt_app):
    workspace.add_chart("binance:usdm:BTCUSDT")
    workspace.add_chart("binance:spot:ETHUSDT")
    panes = list(workspace.panes)
    workspace.client.calls.clear()
    workspace.refresh()
    assert len(workspace.client.calls) == 2
    for mode, count in (("columns", 1), ("rows", 3), ("grid", 2)):
        workspace.layout_mode.setCurrentIndex(workspace.layout_mode.findData(mode))
        qt_app.processEvents()
        assert len(workspace._split_rows) == count
        assert workspace.panes == panes
    for _ in range(10):
        workspace.add_chart()
    assert len(workspace.rows) == 6
    assert not workspace.add_button.isEnabled()


def test_symbol_market_selection_and_mismatched_response(workspace):
    pane = workspace.panes[0]
    pane.symbol.setCurrentText("ETHUSDT")
    pane.symbol.lineEdit().returnPressed.emit()
    assert pane.selection.text() == "binance:usdm:ETHUSDT"
    pane.market.setCurrentIndex(pane.market.findData("spot"))
    assert pane.selection.text() == "binance:spot:ETHUSDT"
    workspace.accept_candles(pane.selection.text(), payload(source_rows(5), instrument="wrong"))
    assert not pane.chart.candles
    assert "mismatch" in pane.detail.text()


def test_linked_cursors_respect_bucket_membership_and_do_not_bridge_gaps(workspace):
    workspace.add_chart("binance:spot:BTCUSDT")
    for pane in workspace.panes:
        workspace.accept_candles(pane.selection.text(), payload(source_rows(10)))
    first, second = workspace.panes
    second.set_timeframe(5)
    workspace._link_cursor(first.chart, 7 * 60_000)
    assert second.chart.cursor == 1
    workspace._link_cursor(first.chart, 11 * 60_000)
    assert second.chart.cursor == -1
    first.chart.set_candles(source_rows(2) + source_rows(2, begin=5))
    first.chart.set_cursor(3 * 60_000)
    assert first.chart.cursor == -1


def test_snapshot_quality_and_disconnect_are_explicit(workspace):
    pane = workspace.panes[0]
    workspace.accept_candles(
        pane.selection.text(),
        payload(
            source_rows(10),
            source="demo",
            quality={"missing_minutes": 2, "interpolation_rate": 0.02},
        ),
    )
    assert "DEMO" in pane.source_label.text()
    assert "2.0%" in pane.quality_badge.text()
    workspace._connection_changed(False)
    assert "연결 끊김" in pane.source_label.text()
    assert pane.chart.candles


def test_nice_ticks_and_formatting_are_bounded_for_extreme_scales(api):
    for low, high in ((0, 1), (80000, 82000), (-0.1, 0.1), (1e-300, 2e-300)):
        ticks = api.nice_ticks(low, high, 6)
        assert len(ticks) <= 16
        assert all(low <= value <= high for value in ticks)
    assert api.nice_ticks(1, 1) == ()
    assert api.format_value(81050, compact=True).endswith("K")
    assert all(timestamp % 60_000 == 0 for timestamp in api.time_ticks(1, 987_654))
