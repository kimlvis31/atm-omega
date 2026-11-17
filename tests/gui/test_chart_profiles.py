"""Named chart drafts, truthful observed overlays and independent SI interactions."""

import copy
import importlib.util
import json
from dataclasses import asdict
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-profile-tests"])


def candles(count=160, start=0):
    from omega.gui.models import Candle

    return tuple(
        Candle((start + i) * 60000, 10 + i % 7, 19, 9, 11 + i % 5, 2 + i % 3) for i in range(count)
    )


def client():
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    return Client()


@pytest.fixture
def page(app, tmp_path):
    from omega.gui.charts_page import ChartsPage

    value = ChartsPage(client(), root=tmp_path)
    value.resize(1100, 950)
    value.show()
    app.processEvents()
    rows = candles()
    value.accept_candles(
        "binance:usdm:BTCUSDT",
        {"candles": [asdict(row) for row in rows], "source": "database", "quality": {}},
    )
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def test_apply_is_temporary_save_restores_named_profile_and_six_pane_workspace(page, app):
    from omega.gui.charts_page import ChartsPage

    pane = page.panes[0]
    editor = pane.settings_panel
    editor.zone.setCurrentText("UTC+09:00")
    editor.ema.setChecked(True)
    editor.ema_period.setValue(7)
    editor.allocations[0].setCurrentText("MFI")
    assert editor.apply()
    assert pane.chart.timezone == "UTC+09:00" and pane.chart.ema_period == 7
    assert not page.store.path.exists()
    for _ in range(5):
        page.add_chart("binance:spot:ETHUSDT")
    page.layout_mode.setCurrentIndex(page.layout_mode.findData("rows"))
    page.linked.setChecked(False)
    editor.profile_name.setText("나의 차트")
    editor.save()
    saved = page.store.load()
    assert "나의 차트" in saved["profiles"] and len(saved["panes"]) == 6
    reopened = ChartsPage(client(), root=page.store.path.parent.parent)
    assert len(reopened.panes) == 6 and not reopened.linked.isChecked()
    assert reopened.layout_mode.currentData() == "rows"
    assert reopened.panes[0].profile == pane.profile
    assert reopened.panes[0].profile["slots"][0]["indicator"] == "MFI"
    reopened.close()
    reopened.deleteLater()
    app.processEvents()


def test_slot_swap_preserves_visibility_and_calculation_is_independent(page):
    pane, editor = page.panes[0], page.panes[0].settings_panel
    editor.allocations[0].setCurrentText("MFI")
    assert [combo.currentText() for combo in editor.allocations] == ["MFI", "VOL", "MMACD"]
    assert editor.displays[0].isChecked() and not editor.displays[1].isChecked()
    editor.enabled["MFI"].setChecked(False)
    assert editor.apply()
    assert not pane.si_plots[0].isHidden() and not pane.si_plots[0].enabled
    assert all(value is None for value in pane._series["MFI"]["MFI"])
    assert len(pane.chart.candles) == len(pane.raw) == 160


def test_invalid_draft_and_failed_save_preserve_applied_profile_and_disk(page, monkeypatch):
    pane, editor = page.panes[0], page.panes[0].settings_panel
    assert page.save_workspace()
    original_bytes, original_profile = page.store.path.read_bytes(), copy.deepcopy(pane.profile)
    editor.mmacd_periods.setText("2,2")
    assert not editor.apply() and pane.profile == original_profile
    editor.mmacd_periods.setText("2,4,7")
    editor.profile_name.setText("test")
    monkeypatch.setattr(page.store, "save", lambda _: (_ for _ in ()).throw(OSError("read only")))
    editor.save()
    assert "read only" in editor.status.text()
    assert pane.profile == original_profile and not page.profiles
    assert page.store.path.read_bytes() == original_bytes


@pytest.mark.parametrize(
    "patch",
    [
        {"ema_period": True},
        {"mfi_period": 1},
        {"mmacd_periods": [2, 2]},
        {"timezone": "UTC+14:15"},
        {"timezone": "UTC-13:00"},
        {"timezone": "bad"},
        {"enabled": {"VOL": 1, "MFI": True, "MMACD": True}},
        {"slots": [{"indicator": "VOL", "visible": True}] * 3},
    ],
)
def test_invalid_profiles_are_rejected(patch):
    from omega.gui.chart_profiles import validate_profile

    with pytest.raises(ValueError):
        validate_profile(patch)


def test_malformed_oversized_or_unhashable_workspace_falls_back_visibly(app, tmp_path):
    from omega.gui.charts_page import ChartsPage

    path = tmp_path / "state" / "chart.workspace.json"
    path.parent.mkdir()
    for encoded in (
        "[]",
        "x" * 131073,
        json.dumps(
            {
                "schema_version": 1,
                "panes": [{"instrument": "binance:spot:BTCUSDT", "timeframe": []}],
            }
        ),
    ):
        path.write_text(encoded, encoding="utf-8")
        value = ChartsPage(client(), root=tmp_path)
        assert len(value.panes) == 1 and "ignored" in value.notice.text()
        assert path.read_text(encoding="utf-8") == encoded
        value.close()
        value.deleteLater()
    app.processEvents()


def test_timezone_only_changes_labels_not_utc_aggregation_or_source(page):
    from omega.gui.chart_profiles import display_date

    pane = page.panes[0]
    pane.set_timeframe(5)
    before, raw = pane.chart.candles, pane.raw
    profile = copy.deepcopy(pane.profile)
    profile["timezone"] = "UTC+05:45"
    pane.apply_profile(profile)
    assert pane.chart.candles is before and pane.raw is raw
    assert display_date(0, "UTC+05:45").hour == 5
    assert display_date(0, "UTC+05:45").minute == 45
    assert display_date(0, "LOCAL").timestamp() == 0


def test_mfi_display_matches_recorded_eta_reference_and_restarts_after_gap():
    from omega.gui.chart_indicators import display_indicators
    from omega.gui.chart_profiles import default_profile
    from omega.gui.models import Candle

    reference = json.loads(Path("tests/fixtures/momentum-legacy.json").read_text())
    case = next(
        case
        for case in reference["cases"]
        if case["analysis_code"] == "MFI" and case["interval_id"] == 0
    )
    rows = tuple(Candle(i * 60000, *map(float, row)) for i, row in enumerate(reference["inputs"]))
    profile = default_profile()
    profile["mfi_period"] = case["nSamples"]
    actual = display_indicators(rows, profile, lambda timestamp: timestamp + 60000)["MFI"]["MFI"]
    for index, expected in enumerate(case["expected"]):
        assert actual[index] == pytest.approx(expected["MFI"], abs=1e-14)
    gapped = rows[:30] + rows[40:]
    values = display_indicators(gapped, profile, lambda timestamp: timestamp + 60000)["MFI"]["MFI"]
    assert all(value is None for value in values[30 : 30 + case["nSamples"]])
    prefix = display_indicators(rows[:80], profile, lambda timestamp: timestamp + 60000)["MFI"][
        "MFI"
    ]
    assert prefix == actual[:80]


def test_mmacd_display_matches_recorded_eta_delta_and_keeps_warmup():
    from omega.gui.chart_indicators import display_indicators
    from omega.gui.chart_profiles import default_profile
    from omega.gui.models import Candle

    reference = next(
        case
        for case in json.loads(Path("tests/fixtures/analysis-legacy.json").read_text())
        if case["source"].startswith("multi_")
    )
    rows = tuple(
        Candle(i * 60000, *([step["price"]] * 4), 1) for i, step in enumerate(reference["steps"])
    )
    profile = default_profile()
    profile.update(mmacd_periods=[2, 4, 7], signal_period=3)
    result = display_indicators(
        rows, profile, lambda timestamp: timestamp + 60000, price_precision=4
    )
    for index, step in enumerate(reference["steps"]):
        assert result["MMACD"]["MSDELTA"][index] == pytest.approx(
            step["expected"]["MSDELTA"], abs=1e-12
        )
    assert result["MMACD"]["MMACD"][5] is None
    assert result["MMACD"]["MMACD"][6] is not None
    assert result["MMACD"]["SIGNAL"][8] is None
    assert result["MMACD"]["SIGNAL"][9] is not None


def test_fixed_selection_readout_is_separate_from_hover_and_survives_timeframe(page):
    pane = page.panes[0]
    pane.chart.selected_timestamp = 30 * 60000
    pane.chart.selectionChanged.emit(pane.chart.selected_timestamp)
    first = pane.selected_readout.text()
    pane.chart.set_cursor(70 * 60000)
    assert pane.selected_readout.text() == first
    pane.set_timeframe(5)
    assert pane.chart.selected_timestamp == 30 * 60000
    assert "1970/01/01 00:30" in pane.selected_readout.text()


def strategy(identity="example", instrument="binance:usdm:BTCUSDT", count=1):
    return {
        "instance": identity,
        "account_id": "paper",
        "instrument": instrument,
        "fill_count": count,
        "revision": count,
    }


def test_observed_fill_response_is_scoped_and_late_results_cannot_cross_strategy_or_epoch(page):
    pane = page.panes[0]
    page.update_state({"core": {"epoch": "a"}, "strategies": [strategy(), strategy("other")]})
    pane.trade_overlay.setCurrentIndex(1)
    first_tag = pane._fill_tag
    fill = {"timestamp_ms": 60000, "price": "12", "side": "buy"}
    page.client.resultReceived.emit(first_tag, {**strategy(), "fills": [fill]})
    assert pane.chart.fill_markers == ((60000, 12, "buy"),)
    pane.trade_overlay.setCurrentIndex(2)
    page.client.resultReceived.emit(first_tag, {**strategy(), "fills": [fill]})
    assert not pane.chart.fill_markers
    stale = pane._fill_tag
    page.update_state({"core": {"epoch": "b"}, "strategies": [strategy(), strategy("other")]})
    assert pane._fill_tag != stale
    page.client.resultReceived.emit(stale, {**strategy("other"), "fills": [fill]})
    assert not pane.chart.fill_markers
    page.client.resultReceived.emit(pane._fill_tag, {**strategy("other"), "fills": [fill]})
    assert len(pane.chart.fill_markers) == 1
    assert "PAPER" in pane.overlay_status.text()
    assert all(args[0] == "GET" for args, _ in page.client.calls)


@pytest.mark.parametrize(
    "record",
    [
        {"timestamp_ms": True, "price": "12", "side": "buy"},
        {"timestamp_ms": 60000, "price": "NaN", "side": "sell"},
        {"timestamp_ms": 60000, "price": "12", "side": "hold"},
    ],
)
def test_invalid_fill_never_draws_and_reports_error(page, record):
    pane = page.panes[0]
    page.update_state({"strategies": [strategy()]})
    pane.trade_overlay.setCurrentIndex(1)
    page.client.resultReceived.emit(pane._fill_tag, {**strategy(), "fills": [record]})
    assert not pane.chart.fill_markers
    assert "invalid" in pane.overlay_status.text()


def test_failed_or_disconnected_overlay_can_recover_without_reposting(page):
    pane = page.panes[0]
    state = {"strategies": [strategy()]}
    page.update_state(state)
    pane.trade_overlay.setCurrentIndex(1)
    first_tag = pane._fill_tag
    page.client.failed.emit(first_tag, "unavailable")
    assert pane._fill_tag is None and "unavailable" in pane.overlay_status.text()
    page.update_state(state)
    assert pane._fill_tag and pane._fill_tag != first_tag
    page.client.connectionChanged.emit(False)
    assert pane._fill_tag is None and "연결 끊김" in pane.overlay_status.text()


def test_si_plot_mouse_pan_axis_zoom_y_fit_and_gap_lod_are_independent(page, app):
    from PySide6.QtCore import QPointF, Qt
    from PySide6.QtGui import QMouseEvent

    pane = page.panes[0]
    pane.chart.fit_all()
    plot = pane.si_plots[0]
    app.processEvents()
    plot._geometry()
    baseline = pane.chart._time_bounds()
    y = plot.plot().center().y()
    x = plot.plot().center().x()

    def event(kind, position, button=Qt.MouseButton.LeftButton):
        return QMouseEvent(kind, position, position, button, button, Qt.KeyboardModifier.NoModifier)

    kind = QMouseEvent.Type
    plot.mousePressEvent(event(kind.MouseButtonPress, QPointF(x, y)))
    plot.mouseMoveEvent(event(kind.MouseMove, QPointF(x - 30, y + 30)))
    plot.mouseReleaseEvent(event(kind.MouseButtonRelease, QPointF(x - 30, y + 30)))
    assert pane.chart._time_bounds() != baseline
    assert pane.chart._manual_price_range is None and plot._manual_range is None
    plot._geometry()
    x = plot.plot().right() + 15
    plot.mousePressEvent(event(kind.MouseButtonPress, QPointF(x, y)))
    plot.mouseMoveEvent(event(kind.MouseMove, QPointF(x, y + 25)))
    plot.mouseReleaseEvent(event(kind.MouseButtonRelease, QPointF(x, y + 25)))
    assert plot._manual_range is not None and pane.chart._manual_price_range is None
    temporal = pane.chart._time_bounds()
    plot.mousePressEvent(event(kind.MouseButtonPress, QPointF(x, y), Qt.MouseButton.RightButton))
    plot.mouseReleaseEvent(
        event(kind.MouseButtonRelease, QPointF(x, y), Qt.MouseButton.RightButton)
    )
    assert plot._manual_range is None and pane.chart._time_bounds() == temporal


def test_si_lod_preserves_hidden_gap_and_does_not_rebuild_on_hover(page, app):
    from omega.gui.chart_indicators import display_indicators

    pane = page.panes[0]
    rows = candles(12000)
    pane.chart.set_candles(rows[:5000] + rows[5001:])
    pane.chart.fit_all()
    profile = copy.deepcopy(pane.profile)
    profile["mfi_period"] = 2
    series = display_indicators(pane.chart.candles, profile, pane.chart._bar_end)
    plot = pane.si_plots[0]
    plot.set_series("MFI", series["MFI"])
    app.processEvents()
    plot._geometry()
    path = plot._paths[0][1]
    moves = sum(path.elementAt(index).isMoveTo() for index in range(path.elementCount()))
    assert moves >= 2
    assert plot._render_points <= plot.width() * 4
    key, original_path = plot._path_key, plot._paths
    pane.chart.set_cursor(8000 * 60000)
    pane.chart.cursorChanged.emit(8000 * 60000)
    app.processEvents()
    assert plot._path_key == key and plot._paths is original_path


def test_si_wheel_requires_clicked_plot_axis_is_noop_and_selection_shares_timestamp(page):
    from PySide6.QtCore import QPoint, QPointF, Qt
    from PySide6.QtGui import QMouseEvent, QWheelEvent

    pane, plot = page.panes[0], page.panes[0].si_plots[0]
    pane.chart.fit_all()
    plot._geometry()
    position = plot.plot().center()
    wheel = QWheelEvent(
        position,
        position,
        QPoint(),
        QPoint(0, 120),
        Qt.MouseButton.NoButton,
        Qt.KeyboardModifier.NoModifier,
        Qt.ScrollPhase.NoScrollPhase,
        False,
    )
    before = pane.chart._time_bounds()
    plot.wheelEvent(wheel)
    assert pane.chart._time_bounds() == before
    press = QMouseEvent(
        QMouseEvent.Type.MouseButtonPress,
        position,
        position,
        Qt.MouseButton.LeftButton,
        Qt.MouseButton.LeftButton,
        Qt.KeyboardModifier.NoModifier,
    )
    release = QMouseEvent(
        QMouseEvent.Type.MouseButtonRelease,
        position,
        position,
        Qt.MouseButton.LeftButton,
        Qt.MouseButton.NoButton,
        Qt.KeyboardModifier.NoModifier,
    )
    plot.mousePressEvent(press)
    plot.mouseReleaseEvent(release)
    assert pane.chart.selected_timestamp is not None
    assert not pane.selected_readout.isHidden()
    plot.wheelEvent(wheel)
    assert pane.chart._time_bounds() != before
    after = pane.chart._time_bounds()
    axis = QPointF(plot.plot().right() + 15, position.y())
    plot.mousePressEvent(
        QMouseEvent(
            QMouseEvent.Type.MouseButtonPress,
            axis,
            axis,
            Qt.MouseButton.LeftButton,
            Qt.MouseButton.LeftButton,
            Qt.KeyboardModifier.NoModifier,
        )
    )
    plot.wheelEvent(wheel)
    assert pane.chart._time_bounds() == after


def test_workspace_validation_is_pure_and_profile_delete_does_not_change_applied_chart(page):
    from omega.gui.chart_profiles import validate_workspace

    snapshot = page.workspace_snapshot()
    before = copy.deepcopy(snapshot)
    assert validate_workspace(snapshot) == before and snapshot == before
    pane = page.panes[0]
    page.save_profile(pane, "test", pane.profile)
    applied = copy.deepcopy(pane.profile)
    page.delete_profile("test")
    assert pane.profile == applied and not page.profiles
    assert not page.store.load()["profiles"]


def test_narrow_chart_scrolls_all_si_and_settings_without_overlapping_plots(page, app):
    pane, editor = page.panes[0], page.panes[0].settings_panel
    for display in editor.displays:
        display.setChecked(True)
    assert editor.apply()
    pane.settings_button.setChecked(True)
    page.resize(800, 560)
    app.processEvents()
    assert page.size().width() == 800 and page.size().height() == 560
    assert pane.chart.geometry().bottom() < pane.si_plots[0].geometry().top()
    assert pane.si_plots[0].geometry().bottom() < pane.si_plots[1].geometry().top()
    assert pane.si_plots[1].geometry().bottom() < pane.si_plots[2].geometry().top()
    assert page.scroll.verticalScrollBar().maximum() > 0
    assert pane.settings_scroll.geometry().bottom() < pane.chart.geometry().top()
