"""Bounded QPainter renderers; source candles are never modified by display LOD."""

# Imports
import bisect
import math

from PySide6.QtCore import QPointF, QRectF, Qt, Signal
from PySide6.QtGui import QColor, QMouseEvent, QPainter, QPainterPath, QPen, QWheelEvent
from PySide6.QtWidgets import QWidget

from omega.gui.calendar_bars import (
    CALENDAR_PERIODS,
    aggregate_calendar,
    calendar_bounds,
    calendar_ticks,
)
from omega.gui.chart_profiles import chart_timezone, display_date
from omega.gui.models import Candle, graph_layers, reduce_candles
from omega.gui.presentation import THEMES, translate

MINUTE_MS = 60_000
TIMEFRAMES = {
    "1m": 1,
    "3m": 3,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "2h": 120,
    "4h": 240,
    "6h": 360,
    "8h": 480,
    "12h": 720,
    "1d": 1440,
    "3d": 4320,
    "1w": "1w",
    "1M": "1M",
}


def aggregate_utc(candles: tuple[Candle, ...], minutes: int | str) -> tuple[Candle, ...]:
    """Display-only complete UTC buckets. Missing minutes are never interpolated here."""
    if minutes in CALENDAR_PERIODS:
        return aggregate_calendar(candles, minutes)
    if type(minutes) is not int or minutes not in TIMEFRAMES.values():
        raise ValueError("unsupported display timeframe")
    if any(row.open_time_ms % MINUTE_MS for row in candles):
        raise ValueError("display aggregation requires UTC-aligned one-minute candles")
    if any(a.open_time_ms >= b.open_time_ms for a, b in zip(candles, candles[1:], strict=False)):
        raise ValueError("display input must be sorted with unique timestamps")
    if minutes == 1:
        return candles
    width = minutes * MINUTE_MS
    buckets: dict[int, list[Candle]] = {}
    for row in candles:
        buckets.setdefault(row.open_time_ms // width * width, []).append(row)
    result = []
    for timestamp, rows in buckets.items():
        if len(rows) != minutes or any(
            row.open_time_ms != timestamp + index * MINUTE_MS for index, row in enumerate(rows)
        ):
            continue
        result.append(
            Candle(
                timestamp,
                rows[0].open,
                max(row.high for row in rows),
                min(row.low for row in rows),
                rows[-1].close,
                math.fsum(row.volume for row in rows),
            )
        )
    return tuple(result)


def display_ema(
    candles: tuple[Candle, ...],
    nSamples: int = 20,
    *,
    interval_ms: int = MINUTE_MS,
    calendar_period: str | None = None,
) -> tuple[float | None, ...]:
    """SMA-seeded display EMA. It is neither imported from nor sent to strategy analysis."""
    if nSamples < 1:
        raise ValueError("EMA period must be positive")
    result: list[float | None] = []
    mean = 0.0
    alpha = 2 / (nSamples + 1)
    count = 0
    previous = None
    for row in candles:
        expected = (
            calendar_bounds(previous, calendar_period)[1]
            if calendar_period and previous is not None
            else previous + interval_ms
            if previous is not None
            else None
        )
        if previous is not None and row.open_time_ms != expected:
            count, mean = 0, 0.0
        previous = row.open_time_ms
        count += 1
        if count <= nSamples:
            mean += (row.close - mean) / count
        else:
            mean += alpha * (row.close - mean)
        result.append(mean if count >= nSamples else None)
    return tuple(result)


def nice_ticks(low: float, high: float, target: int = 5) -> tuple[float, ...]:
    if not all(math.isfinite(value) for value in (low, high)) or high <= low:
        return ()
    rough = (high - low) / max(2, min(12, target))
    if rough <= 0 or not math.isfinite(rough):
        return ()
    power = 10 ** math.floor(math.log10(rough))
    if power == 0:
        return low, high
    step = next(value * power for value in (1, 2, 2.5, 5, 10) if value * power >= rough)
    first = math.ceil(low / step) * step
    return tuple(first + index * step for index in range(16) if first + index * step <= high)


def format_value(value: float, *, compact: bool = False) -> str:
    absolute = abs(value)
    if compact and absolute >= 1_000_000:
        return f"{value / 1_000_000:,.3f}M"
    if compact and absolute >= 10_000:
        return f"{value / 1000:,.3f}K"
    if absolute >= 100:
        return f"{value:,.2f}"
    if absolute >= 1:
        return f"{value:,.4f}".rstrip("0").rstrip(".")
    return f"{value:.7g}"


def time_ticks(begin: int, end: int, target: int = 5) -> tuple[int, ...]:
    if end <= begin:
        return ()
    rough = (end - begin) / max(2, min(10, target))
    steps = (1, 2, 5, 10, 15, 30, 60, 120, 240, 360, 720, 1440, 2880, 10080, 43200)
    step = next(
        (value * MINUTE_MS for value in steps if value * MINUTE_MS >= rough),
        math.ceil(rough / (43200 * MINUTE_MS)) * 43200 * MINUTE_MS,
    )
    first = (begin + step - 1) // step * step
    return tuple(range(first, end + 1, step))[:12]


class ChartDrawer(QWidget):
    """Eta-style price/volume workspace with bounded, cached painter geometry."""

    rangeChanged = Signal(object, object)
    cursorChanged = Signal(object)
    selectionChanged = Signal(object)

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setMinimumSize(260, 210)
        self.setMouseTracking(True)
        self.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
        self.candles: tuple[Candle, ...] = ()
        self._timestamps: tuple[int, ...] = ()
        self.theme, self.language = "light", "ko"
        self.nSamples, self.offset, self.cursor = 180, 0, -1
        self.interval_ms = MINUTE_MS
        self.calendar_period = None
        self.show_volume, self.show_ema = True, False
        self.ema_period, self.timezone = 20, "UTC"
        self.fill_markers = ()
        self.price_overlays = {}
        self.price_window_snapshot = ((), (), {})
        self._price_window_times = ()
        self.volume_profile = None
        self._price_overlay_times = {}
        self._swing_key, self._swing_segments = None, ()
        self.empty_message = ""
        self._drag = None
        self._press = None
        self._last_selected_section = None
        self._hover_section = None
        self.selected_timestamp = None
        self._manual_price_range = self._manual_volume_range = None
        self._volume_range = (0.0, 1.0)
        self._mouse_y = None
        self._cache_key = None
        self._paths = None
        self._volume_paths = None
        self._ema_path = QPainterPath()
        self._price_range = (0.0, 1.0)
        self._display: tuple[Candle, ...] = ()
        self._ema: tuple[float | None, ...] = ()
        self._view_times = None
        self._range_empty = False
        self._data_revision = 0
        self._cached_visible: tuple[Candle, ...] = ()
        self._visible_timestamps: tuple[int, ...] = ()

    @property
    def timestamps(self) -> tuple[int, ...]:
        """Immutable index of the currently accepted data snapshot."""
        return self._timestamps

    def set_candles(self, candles: tuple[Candle, ...]) -> None:
        # Accept sequence callers without retaining a mutable container. Candle rows
        # themselves are frozen; a later explicit submission sees any list edits.
        candles = tuple(candles)
        if self.calendar_period and any(
            calendar_bounds(row.open_time_ms, self.calendar_period)[0] != row.open_time_ms
            for row in candles
        ):
            raise ValueError("calendar display candles must open on UTC calendar boundaries")
        if candles == self.candles:
            return
        # Preserve an independently selected historical view while new snapshots append.
        old_stop = self._visible()[-1].open_time_ms if self.offset and self._visible() else None
        self.candles = candles
        self._timestamps = tuple(row.open_time_ms for row in candles)
        self._ema = display_ema(
            candles,
            self.ema_period,
            interval_ms=self.interval_ms,
            calendar_period=self.calendar_period,
        )
        self._data_revision += 1
        if self._view_times is not None:
            self._cache_key = None
            self.set_time_range(self._view_times[0], self._view_times[1] - 1)
            return
        if old_stop is not None:
            last = bisect.bisect_right(self.timestamps, old_stop)
            self.offset = max(0, len(candles) - last)
        self.offset = min(self.offset, max(0, len(candles) - self.nSamples))
        self.cursor = -1
        self._cache_key = None
        self.update()

    def set_interval_minutes(self, minutes: int) -> None:
        if type(minutes) is not int or minutes not in TIMEFRAMES.values():
            raise ValueError("unsupported display timeframe")
        self.interval_ms = minutes * MINUTE_MS
        self.calendar_period = None
        self._view_times = None
        self._range_empty = False
        self._cache_key = None

    def set_calendar_period(self, period: str) -> None:
        if period not in CALENDAR_PERIODS:
            raise ValueError("unsupported calendar display interval")
        self.calendar_period = period
        self._view_times = None
        self._range_empty = False
        self._cache_key = None

    def _bar_end(self, timestamp: int) -> int:
        return (
            calendar_bounds(timestamp, self.calendar_period)[1]
            if self.calendar_period
            else timestamp + self.interval_ms
        )

    def _bar_center(self, timestamp: int) -> float:
        return (timestamp + self._bar_end(timestamp)) / 2

    def set_options(self, *, volume: bool | None = None, ema: bool | None = None) -> None:
        if volume is not None:
            self.show_volume = volume
            if not volume and self._hover_section == "volume":
                self._clear_hover()
            if not volume and self._last_selected_section in {"volume", "volume_axis"}:
                self._last_selected_section = None
        if ema is not None:
            self.show_ema = ema
        self._cache_key = None
        self.update()

    def set_presentation(self, theme: str, language: str) -> None:
        self.theme = theme if theme in THEMES else "light"
        self.language = language
        self.update()

    def set_display_preferences(self, *, timezone="UTC", ema_period=20):
        chart_timezone(timezone)
        if type(ema_period) is not int or not 2 <= ema_period <= 2000:
            raise ValueError("EMA display period must be between 2 and 2000")
        self.timezone = timezone
        if self.ema_period != ema_period:
            self.ema_period = ema_period
            self._ema = display_ema(
                self.candles,
                ema_period,
                interval_ms=self.interval_ms,
                calendar_period=self.calendar_period,
            )
            self._cache_key = None
        self.update()

    def set_fill_markers(self, records):
        markers = []
        if not isinstance(records, list) or len(records) > 1000:
            raise ValueError("chart accepts at most 1000 observed fills")
        for row in records:
            try:
                timestamp, price = row["timestamp_ms"], float(row["price"])
                side = str(row["side"]).lower()
                if type(timestamp) is not int or not 0 <= timestamp <= 253_402_300_799_000:
                    raise ValueError("invalid fill timestamp")
                if side not in {"buy", "sell"} or not math.isfinite(price) or price <= 0:
                    raise ValueError("invalid fill side or price")
            except (TypeError, KeyError, OverflowError) as error:
                raise ValueError("invalid observed fill") from error
            markers.append((timestamp, price, side))
        self.fill_markers = tuple(markers)
        self.update()

    def _visible(self) -> tuple[Candle, ...]:
        if self._range_empty:
            return ()
        stop = max(0, len(self.candles) - self.offset)
        return self.candles[max(0, stop - self.nSamples) : stop]

    def _plot(self) -> QRectF:
        header = 68 if self.width() < 620 else 52
        height = max(1, self.height() - header - 42)
        price_height = height * (0.76 if self.show_volume else 1)
        return QRectF(12, header, max(1, self.width() - 104), max(1, price_height))

    def _volume_plot(self) -> QRectF:
        price = self._plot()
        return QRectF(
            price.left(),
            price.bottom() + 18,
            price.width(),
            max(1, self.height() - price.bottom() - 55),
        )

    def _time_bounds(self) -> tuple[int, int]:
        if self._view_times is not None:
            return self._view_times
        rows = self._cached_visible
        return (rows[0].open_time_ms, self._bar_end(rows[-1].open_time_ms)) if rows else (0, 1)

    def _x(self, timestamp: float) -> float:
        begin, end = self._time_bounds()
        return (
            self._plot().left() + (timestamp - begin) / max(1, end - begin) * self._plot().width()
        )

    def _y(self, price: float) -> float:
        low, high = self._price_range
        plot = self._plot()
        return plot.bottom() - (price - low) / (high - low) * plot.height()

    def _geometry(self) -> None:
        key = (
            self.width(),
            self.height(),
            self._data_revision,
            self.nSamples,
            self.offset,
            self.show_volume,
            self.show_ema,
            self.interval_ms,
            self.calendar_period,
            self._view_times,
            self._range_empty,
            self._manual_price_range,
            self._manual_volume_range,
        )
        if key == self._cache_key:
            return
        self._cache_key = key
        self._cached_visible = self._visible()
        stop = max(0, len(self.candles) - self.offset)
        self._visible_timestamps = (
            () if self._range_empty else self.timestamps[max(0, stop - self.nSamples) : stop]
        )
        plot = self._plot()
        self._display = reduce_candles(self._cached_visible, max(1, int(plot.width() / 3)))
        rising, falling, volume_up, volume_down = (QPainterPath() for _ in range(4))
        self._ema_path = QPainterPath()
        self._paths, self._volume_paths = (rising, falling), (volume_up, volume_down)
        if not self._display:
            return
        low, high = min(row.low for row in self._display), max(row.high for row in self._display)
        spread = max(high - low, abs(high) * 0.0001, 1e-10)
        if not math.isfinite(spread) or not math.isfinite(high + spread * 0.08):
            self._display = ()
            return
        self._price_range = self._manual_price_range or (low - spread * 0.08, high + spread * 0.08)
        time_begin, time_end = self._time_bounds()
        volume_plot = self._volume_plot()
        self._volume_range = self._manual_volume_range or (
            0.0,
            max(row.volume for row in self._cached_visible) or 1,
        )
        volume_low, volume_high = self._volume_range
        for group_index, row in enumerate(self._display):
            duration = self._bar_end(row.open_time_ms) - row.open_time_ms
            pixel_step = plot.width() * duration / max(1, time_end - time_begin)
            body_width = max(1, min(plot.width() / len(self._display) * 0.68, pixel_step * 0.72))
            path = rising if row.close >= row.open else falling
            x = self._x(self._bar_center(row.open_time_ms))
            path.moveTo(x, self._y(row.high))
            path.lineTo(x, self._y(row.low))
            top = min(self._y(row.open), self._y(row.close))
            path.addRect(
                QRectF(
                    x - body_width / 2,
                    top,
                    body_width,
                    max(1, abs(self._y(row.open) - self._y(row.close))),
                )
            )
            vpath = volume_up if row.close >= row.open else volume_down
            baseline = (
                volume_plot.bottom()
                + volume_low / (volume_high - volume_low) * volume_plot.height()
            )
            # Uneven LOD groups must not create artificial volume spikes by
            # summing one minute in one pixel and two minutes in the next.
            group_start = group_index * len(self._cached_visible) // len(self._display)
            group_stop = (group_index + 1) * len(self._cached_visible) // len(self._display)
            peak_volume = max(
                self._cached_visible[index].volume for index in range(group_start, group_stop)
            )
            height = peak_volume / (volume_high - volume_low) * volume_plot.height()
            vpath.addRect(QRectF(x - body_width / 2, baseline - height, body_width, height))
        if self.show_ema:
            stop = len(self.candles) - self.offset
            first = max(0, stop - self.nSamples)
            stride = max(1, len(self._cached_visible) // max(1, int(plot.width())))
            connected = False
            previous_sample = None
            for index in range(first, stop, stride):
                value = self._ema[index]
                if value is None:
                    connected = False
                    previous_sample = index
                    continue
                if connected and any(
                    self._ema[skipped] is None for skipped in range(previous_sample + 1, index)
                ):
                    connected = False
                point = QPointF(
                    self._x(self._bar_center(self.candles[index].open_time_ms)), self._y(value)
                )
                if connected:
                    self._ema_path.lineTo(point)
                else:
                    self._ema_path.moveTo(point)
                    connected = True
                previous_sample = index

    def _colors(self) -> dict:
        colors = THEMES[self.theme]
        return colors | {
            "chart_background": colors.get("chart_background", colors["background"]),
            "chart_grid": colors.get("chart_grid", colors["grid"]),
            "chart_axis": colors.get("chart_axis", colors["muted"]),
            "chart_crosshair": colors.get("chart_crosshair", colors["muted"]),
            "chart_ema": colors.get("chart_ema", "#6979c9"),
        }

    def paintEvent(self, _event) -> None:
        painter = QPainter(self)
        colors = self._colors()
        painter.fillRect(self.rect(), QColor(colors["chart_background"]))
        self._geometry()
        plot = self._plot()
        if not self._display:
            painter.setPen(QColor(colors["muted"]))
            painter.drawText(
                self.rect().adjusted(20, 20, -20, -20),
                Qt.AlignmentFlag.AlignCenter | Qt.TextFlag.TextWordWrap,
                self.empty_message or translate("no_candles", self.language),
            )
            return
        # [1]: Price and time grids adapt to viewport size instead of a fixed pixel density.
        painter.setPen(QPen(QColor(colors["chart_grid"]), 1))
        for price in nice_ticks(*self._price_range, max(3, int(plot.height() / 68))):
            y = self._y(price)
            painter.drawLine(QPointF(plot.left(), y), QPointF(plot.right(), y))
            painter.setPen(QColor(colors["chart_axis"]))
            painter.drawText(QPointF(plot.right() + 10, y + 4), format_value(price, compact=True))
            painter.setPen(QPen(QColor(colors["chart_grid"]), 1))
        self._draw_time_axis(painter, colors)
        self._draw_selected(painter, colors)
        painter.save()
        painter.setClipRect(plot.adjusted(-1, -1, 1, 1))
        self._draw_price_windows(painter, bands=True)
        for path, color in zip(self._paths or (), ("positive", "negative"), strict=False):
            painter.setPen(QPen(QColor(colors[color]), 1))
            painter.setBrush(QColor(colors[color]))
            painter.drawPath(path)
        if self.show_ema:
            painter.setRenderHint(QPainter.RenderHint.Antialiasing)
            painter.setPen(QPen(QColor(colors["chart_ema"]), 1.3))
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.drawPath(self._ema_path)
            painter.setRenderHint(QPainter.RenderHint.Antialiasing, False)
        self._draw_fill_markers(painter, colors)
        self._draw_price_overlays(painter)
        self._draw_price_windows(painter, bands=False)
        self._draw_volume_profile(painter)
        painter.restore()
        if self.show_volume:
            vplot = self._volume_plot()
            painter.save()
            painter.setClipRect(vplot)
            for path, color in zip(
                self._volume_paths or (), ("positive", "negative"), strict=False
            ):
                painter.setPen(Qt.PenStyle.NoPen)
                fill = QColor(colors[color])
                fill.setAlpha(135)
                painter.setBrush(fill)
                painter.drawPath(path)
            painter.restore()
            painter.setPen(QColor(colors["chart_axis"]))
            painter.drawText(QPointF(plot.left(), vplot.top() - 5), "VOLUME")
            painter.drawText(
                QPointF(plot.right() + 10, vplot.top() + 10),
                format_value(self._volume_range[1], compact=True),
            )
        # [2]: Snapshot's latest close is distinct from the price at the mouse pointer.
        latest = self.candles[-1]
        if self._price_range[0] <= latest.close <= self._price_range[1]:
            y = self._y(latest.close)
            color = QColor(
                colors["positive"] if latest.close >= latest.open else colors["negative"]
            )
            line = QColor(color)
            line.setAlpha(130)
            painter.setPen(QPen(line, 1, Qt.PenStyle.DashLine))
            painter.drawLine(QPointF(plot.left(), y), QPointF(plot.right(), y))
            painter.fillRect(QRectF(plot.right() + 3, y - 10, 88, 20), color)
            painter.setPen(QColor("#14201b") if self.theme == "light" else QColor("#101b16"))
            painter.drawText(
                QRectF(plot.right() + 6, y - 10, 82, 20),
                Qt.AlignmentFlag.AlignVCenter,
                ("최근 " if self.language == "ko" else "LAST ")
                + format_value(latest.close, compact=True),
            )
        self._draw_readout(painter, colors)
        footer = []
        if len(self._display) < len(self._cached_visible):
            footer.append(
                f"LOD {len(self._cached_visible)} → {len(self._display)} · OHLC extrema / V max"
            )
        if self.selected_timestamp is not None:
            selected = display_date(self.selected_timestamp, self.timezone)
            footer.append(
                ("선택 " if self.language == "ko" else "Selected ")
                + selected.strftime("%Y/%m/%d %H:%M %Z")
            )
        if footer:
            painter.setPen(QColor(colors["muted"]))
            painter.drawText(
                QPointF(12, self.height() - 4),
                " · ".join(footer),
            )

    def _draw_time_axis(self, painter: QPainter, colors: dict) -> None:
        plot = self._plot()
        begin, end = self._time_bounds()
        span = end - begin
        target = max(2, int(plot.width() / 150))
        ticks = (
            calendar_ticks(begin, end, self.calendar_period, target)
            if self.calendar_period
            else time_ticks(begin, end, target)
        )
        for timestamp in ticks:
            x = self._x(timestamp)
            painter.setPen(QPen(QColor(colors["chart_grid"]), 1))
            bottom = self._volume_plot().bottom() if self.show_volume else plot.bottom()
            painter.drawLine(QPointF(x, plot.top()), QPointF(x, bottom))
            date = display_date(min(timestamp, 253_402_300_799_000), self.timezone)
            pattern = (
                "%Y/%m"
                if self.calendar_period == "1M"
                else "%y/%m/%d"
                if self.calendar_period == "1w"
                else "%m/%d %H:%M"
                if span >= 86_400_000
                else "%H:%M"
            )
            text = date.strftime(pattern)
            label_x = max(plot.left(), min(x - 24, plot.right() - 90))
            painter.setPen(QColor(colors["chart_axis"]))
            painter.drawText(QPointF(label_x, self.height() - 19), text)
        painter.drawText(QPointF(plot.right() + 10, self.height() - 19), self.timezone)

    def _draw_readout(self, painter: QPainter, colors: dict) -> None:
        plot = self._plot()
        visible = self._cached_visible
        row = visible[self.cursor] if 0 <= self.cursor < len(visible) else visible[-1]
        date = display_date(row.open_time_ms, self.timezone).strftime("%Y/%m/%d %H:%M %Z")
        painter.setPen(QColor(colors["text"]))
        date_width = self.width() - (160 if self.show_ema else 24)
        painter.drawText(
            QPointF(12, 17),
            painter.fontMetrics().elidedText(
                date, Qt.TextElideMode.ElideRight, max(80, date_width)
            ),
        )
        first = f"O {format_value(row.open)}    H {format_value(row.high)}"
        last = f"L {format_value(row.low)}    C {format_value(row.close)}"
        if self.width() < 620:
            painter.drawText(QPointF(12, 35), first)
            painter.drawText(QPointF(12, 53), last)
        else:
            painter.drawText(
                QRectF(12, 22, self.width() - 24, 27),
                Qt.AlignmentFlag.AlignVCenter,
                first + "    " + last,
            )
        if self.show_ema:
            index = bisect.bisect_left(self.timestamps, row.open_time_ms)
            value = self._ema[index] if index < len(self._ema) else None
            label = f"EMA{self.ema_period} " + (
                format_value(value) if value is not None else "warmup"
            )
            painter.setPen(QColor(colors["chart_ema"]))
            painter.drawText(QPointF(max(100, self.width() - 145), 17), label)
        if self.show_volume:
            painter.setPen(QColor(colors["chart_axis"]))
            painter.drawText(QPointF(80, self._volume_plot().top() - 5), format_value(row.volume))
        if 0 <= self.cursor < len(visible):
            x = self._x(self._bar_center(row.open_time_ms))
            painter.setPen(QPen(QColor(colors["chart_crosshair"]), 1, Qt.PenStyle.DashLine))
            painter.drawLine(
                QPointF(x, plot.top()),
                QPointF(x, self._volume_plot().bottom() if self.show_volume else plot.bottom()),
            )
            if (
                self._hover_section == "price"
                and self._mouse_y is not None
                and plot.top() <= self._mouse_y <= plot.bottom()
            ):
                painter.drawLine(
                    QPointF(plot.left(), self._mouse_y), QPointF(plot.right(), self._mouse_y)
                )
                low, high = self._price_range
                value = high - (self._mouse_y - plot.top()) / plot.height() * (high - low)
                painter.fillRect(
                    QRectF(plot.right() + 3, self._mouse_y - 10, 88, 20), QColor(colors["surface"])
                )
                painter.setPen(QColor(colors["text"]))
                painter.drawText(
                    QRectF(plot.right() + 6, self._mouse_y - 10, 82, 20),
                    Qt.AlignmentFlag.AlignVCenter,
                    format_value(value, compact=True),
                )
            if self._hover_section == "volume" and self._mouse_y is not None:
                vplot = self._volume_plot()
                low, high = self._volume_range
                value = high - (self._mouse_y - vplot.top()) / vplot.height() * (high - low)
                painter.drawLine(
                    QPointF(vplot.left(), self._mouse_y), QPointF(vplot.right(), self._mouse_y)
                )
                painter.drawText(
                    QPointF(vplot.right() + 8, self._mouse_y), format_value(value, compact=True)
                )

    def _draw_selected(self, painter, colors):
        if self.selected_timestamp is None or not self._cached_visible:
            return
        times = self._visible_timestamps
        index = bisect.bisect_right(times, self.selected_timestamp) - 1
        if index < 0 or self.selected_timestamp >= self._bar_end(times[index]):
            return
        plot = self._plot()
        left, right = self._x(times[index]), self._x(self._bar_end(times[index]))
        band = QColor(colors["accent"])
        band.setAlpha(45)
        bottom = self._volume_plot().bottom() if self.show_volume else plot.bottom()
        painter.save()
        painter.setClipRect(QRectF(plot.left(), plot.top(), plot.width(), bottom - plot.top()))
        painter.fillRect(QRectF(left, plot.top(), max(1, right - left), bottom - plot.top()), band)
        painter.restore()

    def _draw_fill_markers(self, painter, colors):
        begin, end = self._time_bounds()
        for timestamp, price, side in self.fill_markers:
            if not begin <= timestamp < end:
                continue
            x, y = self._x(timestamp), self._y(price)
            direction = 1 if side == "buy" else -1
            path = QPainterPath(QPointF(x, y))
            path.lineTo(x - 5, y + direction * 9)
            path.lineTo(x + 5, y + direction * 9)
            path.closeSubpath()
            painter.setPen(QPen(QColor(colors["text"]), 1))
            painter.setBrush(QColor(colors["positive"] if side == "buy" else colors["negative"]))
            painter.drawPath(path)

    def set_price_windows(self, rows, definitions, settings):
        # Controller owns strict DTO parsing and immutable numeric tuples. This
        # seam changes presentation only, never candle fit, gestures or producers.
        value = rows, definitions, settings
        if value != self.price_window_snapshot:
            self.price_window_snapshot = value
            self._price_window_times = tuple(row[0] for row in rows)
            self.update()

    def _draw_price_windows(self, painter, *, bands):
        # Lazy import keeps the old charts/indicator dependency direction intact.
        from omega.gui.chart_price_windows import draw_price_windows

        draw_price_windows(self, painter, bands=bands)

    def set_price_overlay(self, kind, rows, *, color, width):
        """Accept only immutable display tuples, never a CA's latest scalar value.

        PSAR rows: (bar open, bar close, value, segment). SWING rows additionally
        carry pivot time/price/type/confirmation time before the source segment.
        """
        if kind not in {"PSAR", "SWING"}:
            raise ValueError("unsupported price overlay")
        rows = tuple(tuple(row) for row in rows) if not isinstance(rows, tuple) else rows
        expected = 4 if kind == "PSAR" else 7
        if len(rows) > 10000 or any(type(row) is not tuple or len(row) != expected for row in rows):
            raise ValueError("invalid bounded price overlay rows")
        old = self.price_overlays.get(kind)
        if (
            not QColor(color).isValid()
            or type(width) not in {int, float}
            or not math.isfinite(width)
            or not 0.25 <= width <= 10
        ):
            raise ValueError("invalid price overlay style")
        if old is None or old[0] is not rows:
            previous = -1
            for row in rows:
                opening, close, segment = row[0], row[1], row[-1]
                if (
                    type(opening) is not int
                    or type(close) is not int
                    or not previous < opening < close <= 253_402_300_799_000
                    or type(segment) is not int
                    or segment < 0
                ):
                    raise ValueError("invalid price overlay candle")
                if kind == "PSAR":
                    if row[2] is not None and (
                        type(row[2]) not in {int, float} or not math.isfinite(row[2])
                    ):
                        raise ValueError("invalid PSAR display value")
                elif not all(value is None for value in row[2:6]):
                    pivot, price, side, confirmed = row[2:6]
                    if (
                        type(pivot) is not int
                        or not 0 <= pivot <= opening
                        or type(price) not in {int, float}
                        or not math.isfinite(price)
                        or price <= 0
                        or type(side) is not int
                        or side not in {-1, 1}
                        or type(confirmed) is not int
                        or not pivot < confirmed <= close
                    ):
                        raise ValueError("invalid confirmed SWING display value")
                previous = opening
        value = rows, color, width
        if old == value:
            return
        if old is None or old[0] is not rows:
            self._price_overlay_times[kind] = tuple(row[0] for row in rows)
            self._swing_key = None
        self.price_overlays[kind] = value
        self.update()

    def confirmed_swing_segments(self):
        rows = self.price_overlays.get("SWING", ((), None, None))[0]
        key = id(rows), self.selected_timestamp, self._data_revision, self.interval_ms
        if key == self._swing_key:
            return self._swing_segments
        self._swing_key, self._swing_segments = key, ()
        if self.selected_timestamp is None:
            return ()
        index = bisect.bisect_right(self.timestamps, self.selected_timestamp) - 1
        if index < 0 or self.selected_timestamp >= self._bar_end(self.timestamps[index]):
            return ()
        cutoff = self._bar_end(self.timestamps[index])
        previous, seen, segments = None, set(), []
        for _opening, close, pivot, price, _kind, confirmed, segment in rows:
            if close > cutoff:
                break
            if pivot is None or confirmed is None or confirmed > cutoff:
                continue
            identity = segment, pivot, price
            if identity in seen:
                continue
            seen.add(identity)
            point = self._bar_center(pivot), price
            if previous is not None and previous[0] == segment:
                segments.append((previous[1], point))
            previous = segment, point
        self._swing_segments = tuple(segments)
        return self._swing_segments

    def _draw_price_overlays(self, painter):
        begin, end = self._time_bounds()
        psar = self.price_overlays.get("PSAR")
        if psar:
            rows, color, width = psar
            times = self._price_overlay_times["PSAR"]
            painter.setPen(QPen(QColor(color), width * 3))
            for opening, close, value, _segment in rows[
                bisect.bisect_left(times, begin) : bisect.bisect_right(times, end)
            ]:
                if value is not None:
                    y = self._y(value)
                    if not math.isfinite(y):
                        continue
                    # Eta uses a horizontal dash covering 70% of each candle.
                    margin = (close - opening) * 0.15
                    painter.drawLine(
                        QPointF(self._x(opening + margin), y),
                        QPointF(self._x(close - margin), y),
                    )
        swing = self.price_overlays.get("SWING")
        if swing:
            painter.setPen(QPen(QColor(swing[1]), swing[2]))
            for start, stop in self.confirmed_swing_segments():
                if stop[0] < begin or start[0] > end:
                    continue
                ys = self._y(start[1]), self._y(stop[1])
                if not all(math.isfinite(y) for y in ys):
                    continue
                painter.drawLine(
                    QPointF(self._x(start[0]), ys[0]),
                    QPointF(self._x(stop[0]), ys[1]),
                )

    def set_volume_profile(self, row, *, color, width, visible=True):
        from omega.gui.chart_volume_profile import checked_row

        if (
            type(visible) is not bool
            or not QColor(color).isValid()
            or type(width) not in {int, float}
            or not 0.05 <= width <= 0.5
        ):
            raise ValueError("invalid selected IVP style")
        checked = None if row is None else checked_row(row)
        self.volume_profile = None if checked is None else (checked, color, width, visible)
        self.update()

    def volume_profile_rectangles(self):
        """Price-aligned and right-fixed; independent of the horizontal viewport."""
        if self.volume_profile is None or self.selected_timestamp is None:
            return ()
        row, _color, width, visible = self.volume_profile
        if (
            not visible
            or row["state"] != "ready"
            or not row["open_time_ms"] <= self.selected_timestamp < row["close_time_ms"]
        ):
            return ()
        plot, result = self._plot(), []
        division = row["divisionHeight"]
        for index, value in enumerate(row["filtered"]):
            upper, lower = self._y((index + 1) * division), self._y(index * division)
            if (
                value <= 0
                or lower < plot.top()
                or upper > plot.bottom()
                or not all(math.isfinite(y) for y in (upper, lower))
            ):
                continue
            # Normalize before scaling: finite large volumes must not overflow
            # the intermediate product and create an infinite QRectF.
            pixels = plot.width() * width * (value / row["maximum"])
            result.append(QRectF(plot.right() - pixels, upper, pixels, max(0, lower - upper)))
        return tuple(result)

    def _draw_volume_profile(self, painter):
        if self.volume_profile is None:
            return
        color = QColor(self.volume_profile[1])
        color.setAlpha(90)
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(color)
        for rect in self.volume_profile_rectangles():
            painter.drawRect(rect)

    def clear_selection(self):
        if self.selected_timestamp is not None:
            self.selected_timestamp = None
            self.selectionChanged.emit(None)
            self.update()

    def fit_price(self):
        self._manual_price_range = None
        self._cache_key = None
        self.update()

    def fit_volume(self):
        self._manual_volume_range = None
        self._cache_key = None
        self.update()

    def _emit_range(self) -> None:
        if self._view_times is not None:
            self.rangeChanged.emit(self._view_times[0], self._view_times[1] - 1)
            return
        rows = self._visible()
        if rows:
            self.rangeChanged.emit(rows[0].open_time_ms, self._bar_end(rows[-1].open_time_ms) - 1)

    def set_time_range(self, start: int, stop: int) -> None:
        if not self.candles or stop < start:
            return
        times = self.timestamps
        first = max(0, bisect.bisect_right(times, start) - 1)
        if self._bar_end(times[first]) <= start:
            first += 1
        last = bisect.bisect_right(times, stop)
        self._range_empty = first >= last
        self._view_times = start, stop + 1
        self.nSamples = max(1, last - first)
        self.offset = max(0, len(self.candles) - last)
        self.cursor = -1
        self.update()

    def set_cursor(self, timestamp: int | None) -> None:
        self._geometry()
        self._mouse_y = None
        self.cursor = -1
        if timestamp is not None and self._cached_visible:
            times = self._visible_timestamps
            index = bisect.bisect_right(times, timestamp) - 1
            if 0 <= index < len(times) and timestamp < self._bar_end(times[index]):
                self.cursor = index
        self.update()

    def fit_all(self) -> None:
        self._clear_hover()
        self.nSamples = max(1, len(self.candles))
        self.offset = 0
        self._view_times = None
        self._range_empty = False
        self._manual_price_range = self._manual_volume_range = None
        self.update()
        self._emit_range()

    def reset_view(self, *, end_ms: int | None = None, reset_y: bool = True) -> None:
        self._clear_hover()
        self.nSamples, self.offset = 180, 0
        self._view_times = None
        self._range_empty = False
        if reset_y:
            self._manual_price_range = self._manual_volume_range = None
        if end_ms is not None and self.candles:
            duration = self._bar_end(self.candles[-1].open_time_ms) - self.candles[-1].open_time_ms
            self.set_time_range(max(0, end_ms - 180 * duration), end_ms - 1)
        self.update()
        self._emit_range()

    def _hit_section(self, point) -> str | None:
        plot = self._plot()
        if plot.contains(point):
            return "price"
        if self.show_volume and self._volume_plot().contains(point):
            return "volume"
        if plot.right() < point.x() <= self.width():
            if plot.top() <= point.y() <= plot.bottom():
                return "price_axis"
            if (
                self.show_volume
                and self._volume_plot().top() <= point.y() <= self._volume_plot().bottom()
            ):
                return "volume_axis"
        if (
            plot.left() <= point.x() <= plot.right()
            and self.height() - 34 <= point.y() <= self.height()
        ):
            return "time_axis"
        return None

    def _timestamp_at(self, point):
        if self._hit_section(point) not in {"price", "volume"}:
            return None
        begin, end = self._time_bounds()
        timestamp = begin + (point.x() - self._plot().left()) / self._plot().width() * (end - begin)
        times = self._visible_timestamps
        index = bisect.bisect_right(times, timestamp) - 1
        return (
            times[index]
            if 0 <= index < len(times) and timestamp < self._bar_end(times[index])
            else None
        )

    def _change_time_view(self, begin, end):
        span = max(1, round(end - begin))
        begin = max(0, min(253_402_300_799_000 - span, round(begin)))
        end = begin + span
        self.set_time_range(begin, end - 1)
        self._emit_range()

    def _pan_time(self, dx, bounds):
        if not dx:
            return
        first, last = bounds
        delta = -dx / max(1, self._plot().width()) * (last - first)
        self._change_time_view(first + delta, last + delta)

    def _zoom_time(self, factor, bounds):
        first, last = bounds
        if not self.candles:
            return
        unit = self._bar_end(self.candles[-1].open_time_ms) - self.candles[-1].open_time_ms
        minimum = min(last - first, max(1, unit * self._plot().width() / 500))
        maximum = max(minimum, unit * self._plot().width() / 0.75)
        span = max(minimum, min(maximum, (last - first) * factor, last))
        self.set_time_range(max(0, round(last - span)), last - 1)
        self._emit_range()

    def _clear_hover(self):
        changed = self.cursor >= 0
        self.cursor, self._mouse_y, self._hover_section = -1, None, None
        if changed:
            self.cursorChanged.emit(None)
        self.update()

    def wheelEvent(self, event: QWheelEvent) -> None:
        self._geometry()
        if self.candles:
            delta = event.angleDelta().y() / 120
            section = self._last_selected_section
            if section in {"price", "volume"} and delta:
                self._zoom_time(math.exp(max(-6, min(6, -delta * 0.18))), self._time_bounds())
            elif section == "time_axis" and delta:
                # Eta's 500 is a 16000×9000 scene delta, not 500 Qt pixels.
                window = self.window()
                scale = min(window.width() / 16000, window.height() / 9000)
                self._pan_time(delta * 500 * scale, self._time_bounds())
            # Price/volume y-axis wheels are intentionally no-ops in the Eta profile.
        event.accept()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        self._geometry()
        section = self._hit_section(event.position())
        self._last_selected_section = section
        if not self.candles or section is None:
            return
        self._press = {
            "point": QPointF(event.position()),
            "section": section,
            "button": event.button(),
            "time": self._time_bounds(),
            "price": self._price_range,
            "volume": self._volume_range,
            "dragged": False,
        }
        if event.button() == Qt.MouseButton.LeftButton:
            self.setCursor(Qt.CursorShape.ClosedHandCursor)
        event.accept()

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        self._geometry()
        if self._press is not None:
            press = self._press
            dx, dy = (
                event.position().x() - press["point"].x(),
                event.position().y() - press["point"].y(),
            )
            if abs(dx) + abs(dy) >= 4:
                press["dragged"] = True
            if press["dragged"] and press["button"] == Qt.MouseButton.LeftButton:
                self._drag = press
                section = press["section"]
                if section in {"price", "volume"}:
                    self._pan_time(dx, press["time"])
                if section == "price" and dy:
                    low, high = press["price"]
                    delta = dy / max(1, self._plot().height()) * (high - low)
                    candidate = low + delta, high + delta
                    if all(math.isfinite(value) for value in candidate):
                        self._manual_price_range = candidate
                elif section in {"price_axis", "volume_axis"} and dy:
                    factor = math.exp(
                        max(
                            -6,
                            min(
                                6,
                                dy
                                / max(
                                    1,
                                    self._plot().height()
                                    if section == "price_axis"
                                    else self._volume_plot().height(),
                                )
                                * 2,
                            ),
                        )
                    )
                    low, high = press["price"] if section == "price_axis" else press["volume"]
                    if section == "price_axis":
                        middle, span = (low + high) / 2, (high - low) * factor
                        candidate = middle - span / 2, middle + span / 2
                        if (
                            all(math.isfinite(value) for value in candidate)
                            and candidate[0] < candidate[1]
                        ):
                            self._manual_price_range = candidate
                    elif math.isfinite(high * factor) and high * factor > 0:
                        self._manual_volume_range = (0.0, high * factor)
                elif section == "time_axis" and dx:
                    factor = math.exp(max(-6, min(6, dx / max(1, self._plot().width()) * 2)))
                    self._zoom_time(factor, press["time"])
                self._clear_hover()
                self.update()
            return
        timestamp = self._timestamp_at(event.position())
        old_timestamp = (
            self._cached_visible[self.cursor].open_time_ms
            if 0 <= self.cursor < len(self._cached_visible)
            else None
        )
        self._hover_section = self._hit_section(event.position()) if timestamp is not None else None
        self._mouse_y = event.position().y() if timestamp is not None else None
        times = self._visible_timestamps
        self.cursor = bisect.bisect_left(times, timestamp) if timestamp is not None else -1
        if timestamp != old_timestamp:
            self.cursorChanged.emit(timestamp)
        self.update()

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        press, self._press, self._drag = self._press, None, None
        self.setCursor(Qt.CursorShape.CrossCursor)
        if (
            press is None
            or press["dragged"]
            or (event.position() - press["point"]).manhattanLength() >= 4
        ):
            return
        section = press["section"]
        if event.button() == Qt.MouseButton.RightButton:
            if section in {"price", "price_axis"}:
                self.fit_price()
            elif section in {"volume", "volume_axis"}:
                self.fit_volume()
        elif event.button() == Qt.MouseButton.LeftButton and section in {"price", "volume"}:
            self._geometry()
            timestamp = self._timestamp_at(event.position())
            if timestamp is not None:
                self.selected_timestamp = (
                    None if self.selected_timestamp == timestamp else timestamp
                )
                self.selectionChanged.emit(self.selected_timestamp)
                self.update()

    def mouseDoubleClickEvent(self, event: QMouseEvent) -> None:
        if event.button() != Qt.MouseButton.LeftButton:
            self.mousePressEvent(event)
            return
        section = self._hit_section(event.position())
        self._last_selected_section = section
        if section == "price_axis":
            self._press = None
            self.fit_price()
        elif section == "volume_axis":
            self._press = None
            self.fit_volume()
        elif section == "time_axis":
            self._press = None
            self.reset_view(reset_y=False)
        else:
            self.mousePressEvent(event)

    def leaveEvent(self, _event) -> None:
        self._last_selected_section = None
        if self._press is None:
            self._drag = None
        self._clear_hover()

    def hideEvent(self, event) -> None:
        self._press = self._drag = self._last_selected_section = None
        self.setCursor(Qt.CursorShape.CrossCursor)
        self._clear_hover()
        super().hideEvent(event)


class NetworkStructureView(QWidget):
    """CPU-only 3D layer summaries; connections come only from recorded edges."""

    nodeSelected = Signal(dict)

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setMinimumSize(280, 230)
        self.layers: list[tuple[str, int]] = []
        self.node_details = ()
        self.recorded_edges = ()
        self.edge_recording = "not_recorded"
        self.omitted_edges = self.omitted_nodes = 0
        self.selected_node = None
        self._hit_nodes, self._rendered_edges = (), ()
        self.angle = -0.35
        self.tilt = 0.2
        self.zoom = 1.0
        self.theme = "dark"
        self.language = "ko"
        self._drag = None
        self._press = None

    def set_structure(self, structure: object) -> None:
        previous_id = (
            self.node_details[self.selected_node]["id"]
            if self.selected_node is not None and self.selected_node < len(self.node_details)
            else None
        )
        layers = graph_layers(structure)
        source = (
            structure.get("layers", structure.get("nodes", []))
            if isinstance(structure, dict)
            else []
        )
        rows = (
            [row for row in source[:16] if isinstance(row, dict)]
            if isinstance(source, list)
            else []
        )
        details, indices, duplicate = [], {}, set()
        for index, (row, (label, size)) in enumerate(zip(rows, layers, strict=True)):
            identity = row.get("id", row.get("name"))
            identity = identity if type(identity) is str and 0 < len(identity) <= 128 else None
            if identity is not None:
                if identity in indices:
                    duplicate.add(identity)
                indices[identity] = index
            shape = row.get("shape")
            shape = (
                tuple(shape[:8])
                if type(shape) is list
                and all(v is None or type(v) is int and 0 <= v <= 2**63 - 1 for v in shape[:8])
                else None
            )
            units = row.get(
                "units", row.get("out_features", row.get("size", shape[-1] if shape else None))
            )
            units = units if type(units) is int and 0 < units <= 2**63 - 1 else None
            if units is None:
                layers[index] = label, 1  # One symbolic node, never invented neuron counts.
                size = 1
            parameters = row.get("parameters")
            parameters = (
                parameters if type(parameters) is int and 0 <= parameters <= 2**63 - 1 else None
            )
            details.append(
                {
                    "id": identity,
                    "label": label,
                    "type": str(row.get("type", "unknown"))[:64],
                    "shape": shape,
                    "reported_units": units,
                    "display_points": size,
                    "parameters": parameters,
                }
            )
        edges, omitted = [], 0
        raw_edges = structure.get("edges") if isinstance(structure, dict) else None
        if raw_edges is None:
            self.edge_recording = "not_recorded"
        elif not isinstance(raw_edges, list):
            self.edge_recording = "invalid"
        else:
            self.edge_recording = "recorded"
            omitted = max(0, len(raw_edges) - 512)
            for edge in raw_edges[:512]:
                start, end = (
                    (edge.get("source"), edge.get("target"))
                    if isinstance(edge, dict)
                    else (None, None)
                )
                if (
                    type(start) is not str
                    or type(end) is not str
                    or start not in indices
                    or end not in indices
                    or start in duplicate
                    or end in duplicate
                ):
                    omitted += 1
                    continue
                pair = indices[start], indices[end]
                if pair not in edges:
                    edges.append(pair)
        self.layers, self.node_details, self.recorded_edges = layers, tuple(details), tuple(edges)
        self.omitted_edges, self.omitted_nodes = (
            omitted,
            max(0, len(source) - len(rows)) if isinstance(source, list) else 0,
        )
        self.selected_node = next(
            (
                i
                for i, row in enumerate(details)
                if previous_id is not None and row["id"] == previous_id
            ),
            None,
        )
        self._hit_nodes, self._rendered_edges = (), ()
        self.nodeSelected.emit(
            {} if self.selected_node is None else dict(details[self.selected_node])
        )
        self.update()

    def _project(self, x: float, y: float, z: float) -> QPointF:
        x2 = x * math.cos(self.angle) + z * math.sin(self.angle)
        z2 = -x * math.sin(self.angle) + z * math.cos(self.angle)
        y2 = y * math.cos(self.tilt) - z2 * math.sin(self.tilt)
        scale = min(self.width() / 5, self.height() / 3) * self.zoom
        return QPointF(self.width() / 2 + x2 * scale, self.height() / 2 + y2 * scale)

    def paintEvent(self, _event) -> None:
        colors = THEMES[self.theme]
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        painter.fillRect(self.rect(), QColor(colors["background"]))
        painter.setPen(QColor(colors["muted"]))
        painter.drawText(
            QRectF(8, self.height() - 57, self.width() - 16, 52),
            Qt.AlignmentFlag.AlignCenter,
            translate("graph_hint", self.language) + "\n" + self._edge_hint(),
        )
        if not self.layers:
            painter.drawText(
                self.rect(), Qt.AlignmentFlag.AlignCenter, translate("no_graph", self.language)
            )
            return
        nodes, centers, hits = [], [], []
        for index, (label, size) in enumerate(self.layers):
            x = (index / max(1, len(self.layers) - 1) - 0.5) * 3.2
            positions = []
            for node in range(size):
                theta = node * math.tau / size
                radius = 0 if size == 1 else 0.6
                positions.append(
                    self._project(x, math.cos(theta) * radius, math.sin(theta) * radius)
                )
                hits.append((positions[-1], index))
            nodes.append(positions)
            centers.append(self._project(x, 0, 0))
            painter.setPen(QColor(colors["text"]))
            painter.drawText(self._project(x, -0.95, 0), label)
        painter.setPen(QPen(QColor(colors["muted"]), 1.2))
        for source, target in self.recorded_edges:
            self._draw_recorded_edge(
                painter,
                centers[source],
                centers[target],
                source == target,
                abs(source - target) > 1,
            )
        self._rendered_edges = self.recorded_edges
        self._hit_nodes = tuple(hits)
        painter.setPen(QColor(colors["accent"]))
        painter.setBrush(QColor(colors["accent"]))
        for index, layer in enumerate(nodes):
            painter.setBrush(
                QColor(colors["text"] if index == self.selected_node else colors["accent"])
            )
            for point in layer:
                painter.drawEllipse(point, 4, 4)

    def _edge_hint(self):
        if self.edge_recording == "not_recorded":
            message = (
                "연결 기록 없음 · 연결을 추측하지 않음"
                if self.language == "ko"
                else "No recorded edges · no connections inferred"
            )
        elif self.edge_recording == "invalid":
            message = (
                "잘못된 연결 기록 · 연결 미표시"
                if self.language == "ko"
                else "Invalid edge record · connections hidden"
            )
        else:
            message = ("기록된 연결 " if self.language == "ko" else "Recorded edges ") + str(
                len(self.recorded_edges)
            )
        return message + f" · omitted nodes={self.omitted_nodes}, edges={self.omitted_edges}"

    @staticmethod
    def _draw_recorded_edge(painter, start, end, self_loop, curved):
        if self_loop:
            finish = end + QPointF(5, 5)
            control = end + QPointF(30, 34)
            path = QPainterPath(start + QPointF(5, -5))
            path.cubicTo(start + QPointF(36, -36), control, finish)
            painter.drawPath(path)
            direction = finish - control
        elif curved:
            control = end + QPointF(0, -35)
            path = QPainterPath(start)
            path.cubicTo(start + QPointF(0, -35), control, end)
            painter.drawPath(path)
            finish, direction = end, end - control
        else:
            painter.drawLine(start, end)
            finish, direction = end, end - start
        length = math.hypot(direction.x(), direction.y())
        if length:
            x, y = direction.x() / length, direction.y() / length
            painter.drawLine(finish, finish + QPointF(-x * 8 - y * 4, -y * 8 + x * 4))
            painter.drawLine(finish, finish + QPointF(-x * 8 + y * 4, -y * 8 - x * 4))

    def mousePressEvent(self, event: QMouseEvent) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self._drag = self._press = event.position()

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        if self._drag is not None:
            delta = event.position() - self._drag
            self.angle += delta.x() / 150
            self.tilt = max(-1.3, min(1.3, self.tilt + delta.y() / 150))
            self._drag = event.position()
            self.update()

    def mouseReleaseEvent(self, event: QMouseEvent) -> None:
        if self._press is not None and (event.position() - self._press).manhattanLength() <= 4:
            close = [
                (
                    math.hypot(point.x() - event.position().x(), point.y() - event.position().y()),
                    index,
                )
                for point, index in self._hit_nodes
            ]
            distance, index = min(close, default=(float("inf"), None))
            self.selected_node = index if distance <= 12 else None
            self.nodeSelected.emit(
                {} if self.selected_node is None else dict(self.node_details[self.selected_node])
            )
            self.update()
        self._drag = None
        self._press = None

    def wheelEvent(self, event: QWheelEvent) -> None:
        factor = 1.1 if event.angleDelta().y() > 0 else 0.9
        self.zoom = max(0.25, min(3.0, self.zoom * factor))
        self.update()


class MetricPlot(QWidget):
    """Small bounded line plot for recorded scalar training metrics."""

    def __init__(self, parent: QWidget | None = None):
        super().__init__(parent)
        self.setMinimumHeight(150)
        self.values: list[float] = []
        self.label = ""
        self.theme = "dark"

    def set_values(self, label: str, values: list) -> None:
        self.label = label
        self.values = []
        for item in values[-2000:]:
            try:
                value = float(item)
            except (ValueError, TypeError, OverflowError):
                continue
            if math.isfinite(value):
                self.values.append(value)
        self.update()

    def paintEvent(self, _event) -> None:
        colors = THEMES[self.theme]
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor(colors["background"]))
        painter.setPen(QColor(colors["text"]))
        painter.drawText(12, 20, self.label or "Recorded scalar metric")
        if not self.values:
            return
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        low, high = min(self.values), max(self.values)
        spread = max(high - low, abs(high) * 0.001, 1e-12)
        path = QPainterPath()
        for index, value in enumerate(self.values):
            point = QPointF(
                12 + index / max(1, len(self.values) - 1) * (self.width() - 28),
                self.height() - 20 - (value - low) / spread * (self.height() - 55),
            )
            if index == 0:
                path.moveTo(point)
            else:
                path.lineTo(point)
        painter.setPen(QPen(QColor(colors["accent"]), 2))
        painter.drawPath(path)
        painter.drawText(
            12, self.height() - 3, f"n={len(self.values)}  min={low:.6g}  max={high:.6g}"
        )
