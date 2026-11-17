"""Dispatch real Qt mouse/wheel events against Eta-compatible chart sections."""

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None,
    reason="optional Qt chart dependency is not installed",
)
BEGIN = 1_700_000_000_000 // 60000 * 60000


def source_rows(count=400):
    from omega.gui.models import Candle

    return tuple(
        Candle(
            BEGIN + index * 60000,
            100 + index / 10,
            102 + index / 10,
            99 + index / 10,
            101 + index / 10,
            10 + index,
        )
        for index in range(count)
    )


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from omega.gui.presentation import configure_fonts

    value = QApplication.instance() or QApplication(["chart-input-tests"])
    configure_fonts(value)
    return value


@pytest.fixture
def chart(app):
    from omega.gui.charts import ChartDrawer

    value = ChartDrawer()
    value.resize(900, 650)
    value.set_candles(source_rows())
    value.fit_all()
    value.show()
    app.processEvents()
    value._geometry()
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def mouse(chart, kind, point, *, button=None, held=None):
    from PySide6.QtCore import QEvent, QPointF, Qt
    from PySide6.QtGui import QMouseEvent
    from PySide6.QtWidgets import QApplication

    event = QMouseEvent(
        getattr(QEvent.Type, kind),
        QPointF(point),
        QPointF(point),
        button or Qt.MouseButton.NoButton,
        held or Qt.MouseButton.NoButton,
        Qt.KeyboardModifier.NoModifier,
    )
    QApplication.sendEvent(chart, event)
    chart._geometry()


def position(chart, section):
    from PySide6.QtCore import QPointF

    price, volume = chart._plot(), chart._volume_plot()
    return {
        "price": price.center(),
        "volume": volume.center(),
        "price_axis": QPointF(price.right() + 25, price.center().y()),
        "volume_axis": QPointF(volume.right() + 25, volume.center().y()),
        "time_axis": QPointF(price.center().x(), chart.height() - 18),
    }[section]


def click(chart, section, *, right=False, point=None):
    from PySide6.QtCore import Qt

    button = Qt.MouseButton.RightButton if right else Qt.MouseButton.LeftButton
    point = point or position(chart, section)
    mouse(chart, "MouseButtonPress", point, button=button, held=button)
    mouse(chart, "MouseButtonRelease", point, button=button)


def drag(chart, section, dx, dy):
    from PySide6.QtCore import QPointF, Qt

    point, button = position(chart, section), Qt.MouseButton.LeftButton
    mouse(chart, "MouseButtonPress", point, button=button, held=button)
    mouse(chart, "MouseMove", point + QPointF(dx, dy), held=button)
    mouse(chart, "MouseButtonRelease", point + QPointF(dx, dy), button=button)


def wheel(chart, section, steps=1):
    from PySide6.QtCore import QPoint, Qt
    from PySide6.QtGui import QWheelEvent
    from PySide6.QtWidgets import QApplication

    point = position(chart, section)
    event = QWheelEvent(
        point,
        point,
        QPoint(),
        QPoint(0, steps * 120),
        Qt.MouseButton.NoButton,
        Qt.KeyboardModifier.NoModifier,
        Qt.ScrollPhase.ScrollUpdate,
        False,
    )
    QApplication.sendEvent(chart, event)
    chart._geometry()


def span(bounds):
    return bounds[1] - bounds[0]


def test_price_plot_diagonal_drag_pans_time_and_price_without_mutating_source(chart):
    original, horizontal, vertical = chart.candles, chart._time_bounds(), chart._price_range
    drag(chart, "price", 60, -25)
    assert span(chart._time_bounds()) == span(horizontal)
    assert chart._time_bounds()[0] < horizontal[0]
    assert chart._price_range[0] < vertical[0]
    assert span(chart._price_range) == pytest.approx(span(vertical))
    assert chart._manual_price_range is not None
    assert chart._manual_volume_range is None
    assert chart.candles is original and chart.selected_timestamp is None


def test_price_axis_vertical_zoom_holds_time_and_center_while_wheel_is_noop(chart):
    horizontal, vertical, volume = chart._time_bounds(), chart._price_range, chart._volume_range
    drag(chart, "price_axis", 0, -35)
    assert chart._time_bounds() == horizontal
    assert span(chart._price_range) < span(vertical)
    assert sum(chart._price_range) == pytest.approx(sum(vertical))
    assert chart._volume_range == volume
    after = chart._price_range
    wheel(chart, "price")  # The hover location does not replace the last-click axis.
    assert chart._time_bounds() == horizontal and chart._price_range == after


def test_axis_drag_uses_only_its_intended_dimension(chart):
    horizontal, vertical = chart._time_bounds(), chart._price_range
    drag(chart, "price_axis", 60, 0)
    assert chart._time_bounds() == horizontal and chart._price_range == vertical
    assert chart._manual_price_range is None
    drag(chart, "time_axis", 0, 40)
    assert chart._time_bounds() == horizontal and chart._price_range == vertical


def test_time_axis_horizontal_drag_zoom_is_right_anchored_and_manual_y_survives(chart):
    drag(chart, "price", 0, -25)
    manual, horizontal = chart._price_range, chart._time_bounds()
    drag(chart, "time_axis", 70, 0)
    assert chart._time_bounds()[1] == horizontal[1]
    assert span(chart._time_bounds()) > span(horizontal)
    assert chart._price_range == manual


def test_wheel_requires_click_routes_to_last_section_and_zooms_at_right_edge(chart):
    horizontal = chart._time_bounds()
    wheel(chart, "price")
    assert chart._time_bounds() == horizontal
    click(chart, "time_axis")
    mouse(chart, "MouseMove", position(chart, "price"))
    wheel(chart, "price")
    panned = chart._time_bounds()
    assert panned[0] < horizontal[0] and span(panned) == span(horizontal)
    click(chart, "price")
    wheel(chart, "time_axis")
    assert chart._time_bounds()[1] == panned[1]
    assert span(chart._time_bounds()) < span(panned)


def test_volume_plot_has_no_vertical_pan_and_its_axis_scales_independently(chart):
    horizontal, vertical, volume = chart._time_bounds(), chart._price_range, chart._volume_range
    drag(chart, "volume", 0, -25)
    assert chart._time_bounds() == horizontal and chart._price_range == vertical
    assert chart._volume_range == volume and chart._manual_volume_range is None
    drag(chart, "volume_axis", 0, -20)
    assert chart._volume_range[0] == 0 and chart._volume_range[1] < volume[1]
    assert chart._time_bounds() == horizontal and chart._price_range == vertical
    after = chart._volume_range
    wheel(chart, "price")
    assert chart._volume_range == after and chart._time_bounds() == horizontal


def test_right_click_fits_only_that_vertical_range(chart):
    drag(chart, "price", 0, -25)
    drag(chart, "volume_axis", 0, -20)
    horizontal, volume = chart._time_bounds(), chart._volume_range
    click(chart, "price", right=True)
    assert chart._manual_price_range is None
    assert chart._volume_range == volume and chart._time_bounds() == horizontal
    click(chart, "volume", right=True)
    assert chart._manual_volume_range is None and chart._time_bounds() == horizontal


def test_fixed_timestamp_persists_while_hover_changes_and_same_bar_toggles_it(chart):
    from PySide6.QtCore import QPointF

    point = position(chart, "price")
    click(chart, "price", point=point)
    selected, paths = chart.selected_timestamp, chart._paths
    assert selected is not None
    mouse(chart, "MouseMove", point + QPointF(100, 0))
    assert chart.selected_timestamp == selected
    assert chart._cached_visible[chart.cursor].open_time_ms != selected
    assert chart._paths is paths
    click(chart, "price", point=point)
    assert chart.selected_timestamp is None and chart._paths is paths


def test_gap_click_never_snaps_to_previous_bar_and_backfill_keeps_selected_utc(chart):
    from PySide6.QtCore import QPointF

    complete = chart.candles
    chart.set_candles(complete[:150] + complete[151:])
    chart._geometry()
    point = QPointF(
        chart._x(chart._bar_center(complete[250].open_time_ms)), chart._plot().center().y()
    )
    click(chart, "price", point=point)
    assert chart.selected_timestamp == complete[250].open_time_ms
    gap = QPointF(chart._x(chart._bar_center(complete[150].open_time_ms)), point.y())
    assert chart._timestamp_at(gap) is None
    click(chart, "price", point=gap)
    assert chart.selected_timestamp == complete[250].open_time_ms
    chart.set_candles(complete)
    chart._geometry()
    assert chart.selected_timestamp == complete[250].open_time_ms
    assert not chart.grab().isNull()


def test_manual_axes_survive_pan_and_new_observations(chart):
    drag(chart, "price_axis", 0, -30)
    drag(chart, "volume_axis", 0, -20)
    price, volume = chart._price_range, chart._volume_range
    drag(chart, "volume", -30, 0)
    chart.set_candles(source_rows(450))
    chart._geometry()
    assert chart._price_range == price and chart._volume_range == volume


def test_uneven_lod_groups_do_not_sum_inconsistent_duration_volume_into_fake_spikes(chart):
    assert len(chart._display) < len(chart._cached_visible)
    assert chart._volume_range[1] == max(row.volume for row in chart._cached_visible)
    assert sum(path.elementCount() for path in chart._volume_paths) <= len(chart._display) * 6


def test_leave_and_hide_clear_hover_and_wheel_focus_but_keep_fixed_selection(chart, app):
    from PySide6.QtCore import QEvent
    from PySide6.QtWidgets import QApplication

    click(chart, "price")
    selected = chart.selected_timestamp
    mouse(chart, "MouseMove", position(chart, "price"))
    QApplication.sendEvent(chart, QEvent(QEvent.Type.Leave))
    assert chart.cursor == -1 and chart._last_selected_section is None
    before = chart._time_bounds()
    wheel(chart, "price")
    assert chart._time_bounds() == before
    click(chart, "price_axis")
    mouse(chart, "MouseMove", position(chart, "price"))
    chart.hide()
    assert chart.cursor == -1 and chart._last_selected_section is None
    chart.show()
    app.processEvents()
    assert chart.selected_timestamp == selected


def test_price_wheel_keeps_calendar_right_edge_without_replacing_calendar_source(chart):
    from omega.gui.calendar_bars import calendar_boundary
    from omega.gui.models import Candle

    rows = tuple(
        Candle(
            calendar_boundary(BEGIN, "1M", index),
            10 + index,
            12 + index,
            9 + index,
            11 + index,
            100,
        )
        for index in range(24)
    )
    chart.set_calendar_period("1M")
    chart.set_candles(rows)
    chart.fit_all()
    chart._geometry()
    before = chart._time_bounds()
    click(chart, "price")
    wheel(chart, "price")
    assert chart._time_bounds()[1] == before[1]
    assert span(chart._time_bounds()) < span(before)
    assert chart.candles is rows


def test_double_click_resets_axis_only_and_does_not_reset_plot_time(chart):
    from PySide6.QtCore import Qt

    drag(chart, "price", 30, -25)
    before = chart._time_bounds()
    mouse(
        chart,
        "MouseButtonDblClick",
        position(chart, "price_axis"),
        button=Qt.MouseButton.LeftButton,
    )
    assert chart._manual_price_range is None and chart._time_bounds() == before
    mouse(chart, "MouseButtonDblClick", position(chart, "price"), button=Qt.MouseButton.LeftButton)
    mouse(chart, "MouseButtonRelease", position(chart, "price"), button=Qt.MouseButton.LeftButton)
    assert chart._time_bounds() == before


@pytest.mark.parametrize("size,physical_step", [((1920, 1080), 60), ((2560, 1440), 80)])
def test_time_axis_wheel_converts_eta_scene_delta_to_viewport_pixels(chart, size, physical_step):
    chart.resize(*size)
    chart._geometry()
    before = chart._time_bounds()
    click(chart, "time_axis")
    wheel(chart, "price")
    expected = physical_step / chart._plot().width() * span(before)
    assert before[0] - chart._time_bounds()[0] == pytest.approx(expected, abs=1)
    assert span(chart._time_bounds()) == span(before)


def test_ema_lod_never_connects_across_hidden_gap_and_warmup_between_sampled_points(chart):
    rows = source_rows(50000)
    chart.set_candles(rows[:1000] + rows[1001:])
    chart.set_options(ema=True)
    chart.fit_all()
    chart._geometry()
    assert len(chart._cached_visible) // int(chart._plot().width()) > 20
    moves = sum(
        chart._ema_path.elementAt(index).isMoveTo()
        for index in range(chart._ema_path.elementCount())
    )
    assert moves == 2
