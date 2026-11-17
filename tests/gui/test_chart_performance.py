"""Work-count regressions for chart caches; no timing or speedup assumptions."""

import importlib.util
import os
from copy import deepcopy
from dataclasses import asdict

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-work-count-tests"])


def candles(count=2000, *, begin=0):
    from omega.gui.models import Candle

    return tuple(
        Candle((begin + i) * 60000, 10 + i % 5, 19, 9, 11 + i % 4, i % 3 + 1) for i in range(count)
    )


@pytest.fixture
def page(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.charts_page import ChartsPage

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        failed = Signal(str, str)
        attached = True

        def request(self, *args, **kwargs):
            pass

    view = ChartsPage(Client())
    view.add_chart("binance:usdm:BTCUSDT")
    view.accept_candles(
        "binance:usdm:BTCUSDT",
        {"candles": [asdict(row) for row in candles()], "source": "database"},
    )
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


def test_unlinked_range_preserves_sibling_si_cache(page, monkeypatch):
    page.linked.setChecked(False)
    first, sibling = page.panes
    calls = []
    for plot in sibling.si_plots:
        plot._geometry()
        marker = plot._path_key
        monkeypatch.setattr(plot, "refresh_view", lambda *_: calls.append("refresh"))
        assert marker is not None
    old = [plot._path_key for plot in sibling.si_plots]
    first.chart.rangeChanged.emit(60000, 180 * 60000 - 1)
    assert calls == []
    assert [plot._path_key for plot in sibling.si_plots] == old


def test_linked_range_updates_only_target_once_and_keeps_y(page, monkeypatch):
    first, sibling = page.panes
    calls = []
    for plot in sibling.si_plots:
        plot._manual_range = (-7.0, 11.0)
        original = plot.refresh_view
        monkeypatch.setattr(
            plot, "refresh_view", lambda *args, f=original: (calls.append(1), f(*args))
        )
    first.chart.rangeChanged.emit(60000, 180 * 60000 - 1)
    assert sibling.chart._view_times == (60000, 180 * 60000)
    assert len(calls) == 3
    assert all(plot._manual_range == (-7.0, 11.0) for plot in sibling.si_plots)


def test_timestamp_snapshot_reused_until_data_changes(app):
    from omega.gui.charts import ChartDrawer

    chart = ChartDrawer()
    rows = candles(500)
    chart.set_candles(rows)
    original = chart.timestamps
    for _ in range(8):
        chart.set_time_range(60_000, 120 * 60_000)
        chart._geometry()
        chart.set_cursor(120 * 60_000)
        chart.set_candles(tuple(list(rows)))
        assert chart.timestamps is original
    chart.set_candles(rows + candles(1, begin=500))
    assert chart.timestamps is not original and len(chart.timestamps) == 501
    chart.set_candles(())
    assert chart.timestamps == ()
    chart.close()


def test_set_candles_snapshots_mutable_sequence_before_cache(app):
    from omega.gui.charts import ChartDrawer

    chart = ChartDrawer()
    values = list(candles(10))
    chart.set_candles(values)
    values.append(candles(1, begin=10)[0])
    assert len(chart.candles) == 10
    chart.set_candles(values)
    assert len(chart.candles) == 11 and chart.timestamps[-1] == 600000
    values[0] = candles(1, begin=-1)[0]
    assert chart.timestamps[0] == 0
    chart.close()


def test_profile_draw_changes_reuse_numeric_series(page, monkeypatch):
    from omega.gui import charts_page

    pane = page.panes[0]
    calls = []
    original = charts_page.display_indicators
    monkeypatch.setattr(
        charts_page,
        "display_indicators",
        lambda *args, **kwargs: (calls.append(1), original(*args, **kwargs))[1],
    )
    series = pane._series
    profile = deepcopy(pane.profile)
    profile["timezone"] = "UTC+09:00"
    profile["ema"] = True
    profile["ema_period"] = 7
    profile["slots"] = list(reversed(profile["slots"]))
    profile["slots"][0]["visible"] = True
    pane.apply_profile(profile)
    pane._toggle_volume(False)
    assert calls == []
    assert pane._series is series


@pytest.mark.parametrize(
    "key,value",
    [
        ("mfi_period", 7),
        ("mmacd_periods", [3, 7]),
        ("signal_period", 5),
        ("enabled", {"VOL": True, "MFI": False, "MMACD": True}),
    ],
)
def test_numeric_profile_changes_recompute_once(page, monkeypatch, key, value):
    from omega.gui import charts_page

    pane = page.panes[0]
    calls = []
    original = charts_page.display_indicators
    monkeypatch.setattr(
        charts_page,
        "display_indicators",
        lambda *args, **kwargs: (calls.append(1), original(*args, **kwargs))[1],
    )
    profile = deepcopy(pane.profile)
    profile[key] = profile[key] | value if key == "enabled" else value
    pane.apply_profile(profile)
    assert calls == [1]
    assert pane._series == original(pane.chart.candles, profile, pane.chart._bar_end)
    pane.apply_profile(profile)
    assert calls == [1]


def test_appended_data_and_interval_change_invalidate_numeric_cache(page, monkeypatch):
    from omega.gui import charts_page

    pane = page.panes[0]
    calls = []
    original = charts_page.display_indicators
    monkeypatch.setattr(
        charts_page,
        "display_indicators",
        lambda *args, **kwargs: (calls.append(1), original(*args, **kwargs))[1],
    )
    pane.set_snapshot(candles(2001), "database", {})
    assert calls == [1]
    pane.set_snapshot(candles(2001), "database", {})
    assert calls == [1]
    pane.set_timeframe(5)
    assert calls == [1, 1]
    assert pane._series == original(pane.chart.candles, pane.profile, pane.chart._bar_end)


def test_unchanged_plot_series_preserves_cached_paths(page):
    pane = page.panes[0]
    plot = pane.si_plots[0]
    plot._geometry()
    key = plot._path_key
    pane.apply_profile(deepcopy(pane.profile))
    assert plot._path_key is key


def test_unlinked_cursor_only_repaints_its_own_si(page, monkeypatch):
    page.linked.setChecked(False)
    first, sibling = page.panes
    own, other = [], []
    for plot in first.si_plots:
        monkeypatch.setattr(plot, "update", lambda: own.append(1))
    for plot in sibling.si_plots:
        monkeypatch.setattr(plot, "update", lambda: other.append(1))
    first.chart.cursorChanged.emit(1999 * 60000)
    assert own == [1, 1, 1] and other == []


def test_cached_ema_readout_does_not_scan_source_timestamps(app):
    from PySide6.QtGui import QImage, QPainter

    from omega.gui.charts import ChartDrawer
    from omega.gui.models import Candle

    class TrackedCandle(Candle):
        reads = 0

        def __getattribute__(self, name):
            if name == "open_time_ms":
                type(self).reads += 1
            return super().__getattribute__(name)

    chart = ChartDrawer()
    chart.resize(800, 400)
    chart.set_candles(tuple(TrackedCandle(i * 60000, 10, 13, 9, 12, 1) for i in range(10000)))
    chart.set_options(ema=True)
    chart._geometry()
    image = QImage(800, 400, QImage.Format.Format_ARGB32)
    painter = QPainter(image)
    TrackedCandle.reads = 0
    for _ in range(5):
        chart._draw_readout(painter, chart._colors())
    painter.end()
    # Dates/current bar use a constant number of timestamp reads; no history walk.
    assert TrackedCandle.reads <= 15
    chart.close()


def test_gap_replacement_and_clear_replace_series_without_stale_values(page):
    from omega.gui.chart_indicators import display_indicators

    pane = page.panes[0]
    rows = candles(70) + candles(60, begin=75)
    pane.set_snapshot(rows, "database", {})
    assert pane._series == display_indicators(rows, pane.profile, pane.chart._bar_end)
    assert pane.chart.timestamps[70] == 75 * 60000
    pane.set_snapshot((), "database", {})
    assert pane.chart.timestamps == ()
    assert all(values == () for lines in pane._series.values() for values in lines.values())


@pytest.mark.parametrize("width", [800, 1360])
def test_chart_cache_changes_keep_layout_accessible(page, app, tmp_path, width):
    from pathlib import Path

    from PySide6.QtGui import QFont, QFontDatabase

    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font.is_file():
        registered = QFontDatabase.addApplicationFont(str(font))
        app.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    page.set_presentation("light", "en")
    page.resize(width, 900)
    page.show()
    page.panes[0]._toggle_ema(True)
    app.processEvents()
    assert page.panes[0].chart.width() >= 260
    assert page.grab().save(str(tmp_path / f"chart-caches-{width}.png"))
