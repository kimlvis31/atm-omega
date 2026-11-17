"""Display-only Eta SI panes with a shared UTC range and independent vertical axes."""

# Imports
import bisect
import json
import math
import re
from collections import deque
from hashlib import sha256
from itertools import combinations
from uuid import uuid4

from PySide6.QtCore import QObject, QPointF, QRectF, Qt, QTimer
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import QWidget

from omega.gui.chart_lookup_policy import ChartLookupPolicy
from omega.gui.chart_profiles import ANALYSIS_KINDS, AUXILIARY_INDICATORS, validate_profile
from omega.gui.charts import format_value, nice_ticks


def display_indicators(candles, profile, bar_end, *, price_precision=8) -> dict:
    """Use only displayed complete bars, restart each kernel after a source gap.

    MFI retains Eta's 0..1 scale and zero-negative-flow result of 1. MMACD is
    the sum of all configured fast-minus-slow EMA pairs, not conventional MACD.
    Values are for display; no result is sent to the strategy engine.
    """
    profile = validate_profile(profile)
    result = {
        "VOL": {"VOL": []},
        "MFI": {"MFI": []},
        "MMACD": {"MMACD": [], "SIGNAL": [], "MSDELTA": []},
    }
    nSamples = profile["mfi_period"]
    periods = profile["mmacd_periods"]
    maximum, signal_period = max(periods), profile["signal_period"]
    money = deque(maxlen=nSamples)
    closes = deque(maxlen=maximum)
    mas, count, signal, previous_time, previous_tp = {}, 0, None, None, None
    for row in candles:
        if previous_time is not None and row.open_time_ms != bar_end(previous_time):
            money.clear()
            closes.clear()
            mas, count, signal, previous_tp = {}, 0, None, None
        result["VOL"]["VOL"].append(row.volume if profile["enabled"]["VOL"] else None)
        typical = (row.high + row.low + row.close) / 3
        positive = typical * row.volume if previous_tp is not None and typical > previous_tp else 0
        negative = typical * row.volume if previous_tp is not None and typical < previous_tp else 0
        money.append((positive, negative))
        mfi = None
        if profile["enabled"]["MFI"] and count >= nSamples:
            plus, minus = math.fsum(p for p, _ in money), math.fsum(m for _, m in money)
            mfi = 1.0 if minus == 0 else 1 - 1 / (1 + plus / minus)
        result["MFI"]["MFI"].append(mfi)
        closes.append(row.close)
        if profile["enabled"]["MMACD"]:
            for period in periods:
                previous = mas.get(period)
                if previous is None:
                    mas[period] = (
                        round(sum(reversed(list(closes)[-period:])) / period, price_precision)
                        if len(closes) >= period
                        else None
                    )
                else:
                    k = 2 / (period + 1)
                    mas[period] = round(row.close * k + previous * (1 - k), price_precision)
            mmacd = (
                math.fsum(mas[short] - mas[long] for short, long in combinations(periods, 2))
                if count >= maximum - 1
                else None
            )
            if count >= maximum + signal_period - 1:
                k = 2 / (signal_period + 1)
                signal = mmacd if signal is None else mmacd * k + signal * (1 - k)
            delta = mmacd - signal if signal is not None else None
        else:
            mmacd = delta = None
        result["MMACD"]["MMACD"].append(mmacd)
        result["MMACD"]["SIGNAL"].append(signal)
        result["MMACD"]["MSDELTA"].append(delta)
        previous_time, previous_tp = row.open_time_ms, typical
        count += 1
    return {
        code: {name: tuple(values) for name, values in lines.items()}
        for code, lines in result.items()
    } | {"DMIxADX": {"DMIxADX": (None,) * len(candles)}}


class PriceOverlayRequest(QObject):
    """One pane/producer display job; unknown submission only queries the same ID."""

    def __init__(self, pane, kind, label, retry):
        super().__init__(pane)
        self.pane, self.kind, self.label, self.retry = pane, kind, label, retry
        self.key = self.identity = self.tag = self.params = None
        self.state, self.rows, self.serial = "off", (), 0
        self.metadata = {}
        self.lookup_policy = ChartLookupPolicy()
        self.timer = QTimer(self)
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.poll)
        retry.clicked.connect(self.request_again)
        if hasattr(pane.owner.client, "resultReceived"):
            pane.owner.client.resultReceived.connect(self.result)
        pane.owner.client.failed.connect(self.failed)

    def context(self):
        config = self.pane.profile[self.kind.lower()]
        numeric = (
            ("start", "acceleration", "maximum", "price_precision")
            if self.kind == "PSAR"
            else ("swing_range",)
        )
        return self.pane._producer_context(
            self.kind, {key: config[key] for key in numeric}, config["enabled"]
        )

    def alive(self):
        return (
            not self.pane.owner._series_closed
            and self.pane in self.pane.owner.panes
            and self.pane.owner.client.attached
        )

    def _sync_lookup_polling(self):
        if (
            self.identity
            and self.state == "unknown"
            and not self.lookup_policy.permits_auto("unknown")
        ):
            self.state = "lookup_paused"
        lookup_state = (
            "running" if self.kind in ANALYSIS_KINDS and self.state == "cancelling" else self.state
        )
        active = self.alive() and not self.tag and self.lookup_policy.permits_auto(lookup_state)
        if active:
            if not self.timer.isActive():
                self.timer.start()
        else:
            self.timer.stop()

    def _manual_lookup(self):
        if self.state != "lookup_paused":
            return False
        if self.alive() and self.identity and not self.tag:
            context, _ = self.context()
            if context and context[0] == self.key:
                self.send("GET")
        self._sync_lookup_polling()
        return True

    def ensure(self):
        context, reason = self.context()
        if context is None:
            self.state, self.rows, self.tag = reason, (), None
            if reason != "disconnected":
                self.key = self.identity = None
        else:
            key, params = context
            if key != self.key:
                self.key, self.params, self.identity = key, params, uuid4().hex
                self.state, self.rows = "queued", ()
                self.send("POST")
            elif self.state == "disconnected":
                self.state = "unknown"
        config = self.pane.profile[self.kind.lower()]
        self.pane.chart.set_price_overlay(
            self.kind, self.rows, color=config["color"], width=config["width"]
        )
        self._sync_lookup_polling()
        self.status()

    def send(self, method):
        self.lookup_policy.bind(self.identity)
        if not self.alive() or self.tag and method != "POST":
            self._sync_lookup_polling()
            return
        self.serial += 1
        self.tag = f"price-series:{id(self)}:{self.serial}"
        self._sync_lookup_polling()
        client = self.pane.owner.client
        if method == "POST":
            client.request(
                method,
                "/api/chart-series",
                {"id": self.identity, "parameters": self.params},
                tag=self.tag,
            )
        else:
            client.request(method, "/api/chart-series/" + self.identity, tag=self.tag)

    def poll(self):
        self._sync_lookup_polling()
        if (
            not self.lookup_policy.permits_auto(
                "running"
                if self.kind in ANALYSIS_KINDS and self.state == "cancelling"
                else self.state
            )
            or not self.identity
            or self.tag
            or not self.alive()
            or not self.pane.isVisible()
        ):
            return
        context, _ = self.context()
        if context and context[0] == self.key:
            self.send("GET")
        else:
            self.timer.stop()

    def request_again(self):
        if self._manual_lookup():
            return
        if self.state in {"failed", "cancelled", "expired"}:
            self.key = None
            self.ensure()

    def result(self, tag, response):
        if tag != self.tag or not self.alive():
            if not self.alive():
                self._sync_lookup_polling()
            return
        self.tag = None
        context, _ = self.context()
        if not context or context[0] != self.key:
            self.timer.stop()
            return
        try:
            expected_hash = sha256(
                json.dumps(
                    self.params, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode()
            ).hexdigest()
            if (
                type(response) is not dict
                or response.get("id") != self.identity
                or response.get("parameters") != self.params
                or json.dumps(response.get("parameters"), sort_keys=True)
                != json.dumps(self.params, sort_keys=True)
                or response.get("core_epoch") != self.pane.owner._series_epoch
                or response.get("spec_hash") != expected_hash
                or response.get("state")
                not in (
                    {"queued", "running", "complete", "failed", "cancelled", "expired"}
                    | ({"cancelling"} if self.kind in ANALYSIS_KINDS else set())
                )
            ):
                raise ValueError("price series identity changed")
            self.state = response["state"]
            if self.state == "complete":
                self.rows = self.parse(response.get("result"))
            elif self.state in {"failed", "cancelled", "expired"}:
                self.rows = ()
        except (ValueError, TypeError, KeyError, OverflowError):
            self.state = "lookup_paused" if self.lookup_policy.record_failure() else "unknown"
            self.rows = ()
        else:
            self.lookup_policy.accept_response()
        self.ensure()

    def failed(self, tag, _message):
        if tag == self.tag and self.alive():
            self.tag = None
            context, _ = self.context()
            if not context or context[0] != self.key:
                self.timer.stop()
                return
            self.rows = ()
            self.state = "lookup_paused" if self.lookup_policy.record_failure() else "unknown"
            self.ensure()
        else:
            if not self.alive():
                self._sync_lookup_polling()

    def parse(self, result):
        params = self.params
        config = {
            key: value
            for key, value in params.items()
            if key not in {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
        }
        if (
            type(result) is not dict
            or result.get("source") != "database"
            or result.get("instrument") != params["instrument"]
            or result.get("kind") != self.kind
            or type(result.get("period_minutes")) is not int
            or result["period_minutes"] != params["period_minutes"]
            or any(
                result.get(key) != value or type(result.get(key)) is bool
                for key, value in config.items()
            )
            or result.get("knowledge_mode") != "retrospective_display"
            or type(result.get("as_of_ms")) is not int
            or type(result.get("produced_at_ms")) is not int
            or not params["end_ms"]
            <= result["as_of_ms"]
            <= result["produced_at_ms"]
            <= 253_402_300_799_000
            or re.fullmatch(r"[a-f0-9]{64}", str(result.get("source_checksum"))) is None
            or type(result.get("quality")) is not dict
            or type(result.get("rows")) is not list
            or len(result["rows"]) > 10000
        ):
            raise ValueError("invalid retrospective price series")
        width, previous, previous_segment, mapped = params["period_minutes"] * 60000, None, 0, []
        display_times = set(self.pane.chart.timestamps)
        for row in result["rows"]:
            if type(row) is not dict:
                raise ValueError("invalid price series row")
            opening, close, segment = (
                row.get("open_time_ms"),
                row.get("close_time_ms"),
                row.get("segment"),
            )
            if (
                type(opening) is not int
                or type(close) is not int
                or opening % width
                or close != opening + width
                or not params["start_ms"] <= opening < close <= params["end_ms"]
                or type(row.get("available_at_ms")) is not int
                or row["available_at_ms"] != result["produced_at_ms"]
                or type(segment) is not int
                or not 0 <= segment <= 10000
                or previous is not None
                and (
                    opening <= previous
                    or segment < previous_segment
                    or opening != previous + width
                    and segment == previous_segment
                )
            ):
                raise ValueError("invalid closed price series row or gap segment")
            if self.kind == "PSAR":
                value, dcc = row.get("PSAR"), row.get("DCC")
                if (
                    "PSAR" not in row
                    or "DCC" not in row
                    or value is not None
                    and (type(value) not in {int, float} or not math.isfinite(value))
                    or dcc is not None
                    and (type(dcc) is not int or dcc < 0)
                ):
                    raise ValueError("invalid PSAR value")
                parsed = opening, close, value, segment
            else:
                keys = ("LSTIMESTAMP", "LSPRICE", "LSTYPE", "CONFIRMED_AT_MS")
                if any(key not in row for key in keys):
                    raise ValueError("missing SWING fields")
                pivot, price, side, confirmed = (row[key] for key in keys)
                if not all(value is None for value in (pivot, price, side, confirmed)):
                    if (
                        type(pivot) is not int
                        or pivot * 1000 % width
                        or not params["start_ms"] <= pivot * 1000 <= opening
                        or type(price) not in {int, float}
                        or not math.isfinite(price)
                        or price <= 0
                        or type(side) is not int
                        or side not in {-1, 1}
                        or type(confirmed) is not int
                        or not pivot * 1000 < confirmed <= close
                    ):
                        raise ValueError("invalid or future SWING pivot")
                parsed = (
                    opening,
                    close,
                    None if pivot is None else pivot * 1000,
                    price,
                    side,
                    confirmed,
                    segment,
                )
            if opening in display_times:
                mapped.append(parsed)
            previous, previous_segment = opening, segment
        self.metadata = {
            key: result[key]
            for key in (
                "source_checksum",
                "as_of_ms",
                "produced_at_ms",
                "knowledge_mode",
                "quality",
            )
        }
        return tuple(mapped)

    def status(self):
        self._sync_lookup_polling()
        visible = self.pane.profile[self.kind.lower()]["enabled"]
        self.label.setVisible(visible)
        self.retry.setVisible(
            visible and self.state in {"failed", "cancelled", "expired", "lookup_paused"}
        )
        self.retry.setText(
            ("같은 요청 조회" if self.pane.owner.language == "ko" else "Check same request")
            if self.state == "lookup_paused"
            else "다시 계산 / Recalculate"
        )
        labels = {
            "source_unavailable": "DB 관측만 지원 / DB observations only; no demo mixing",
            "period_unavailable": "1m~1d만 지원 / 3d/week/month unavailable",
            "api_unavailable": "코어 시계열 API 미지원 / Series API unavailable",
            "awaiting_data": "새 코어의 봉 대기 / Awaiting fresh-core candles",
            "range_unavailable": "원본 최대10000분 필요 / Bounded source range required",
            "disconnected": "연결 끊김 / Disconnected",
            "queued": "계산 대기 / Queued",
            "running": "계산 중 / Running",
            "unknown": "결과 미확인 · 같은 ID 조회 / Unknown outcome; same-ID lookup",
            "lookup_paused": (
                "연속 조회 실패 · 자동 조회 중지 · 같은 요청을 직접 조회하세요"
                if self.pane.owner.language == "ko"
                else "Lookup paused after repeated failures. Check the same request manually."
            ),
            "complete": (
                "DB 과거 표시 계산 · 당시 인지 아님 / "
                "Retrospective display, not historical knowledge"
            ),
            "failed": "계산 실패 / Calculation failed",
            "cancelled": "취소 / Cancelled",
            "expired": "결과 만료 · 값 제거 / Expired; values cleared",
        }
        text = self.kind + " · " + labels.get(self.state, self.state)
        if self.kind == "SWING" and self.state == "complete":
            text += " · 봉 선택 시 확인된 pivot 표시 / Select a bar for confirmed pivots"
        if self.state == "complete" and not any(row[2] is not None for row in self.rows):
            text += " · 준비 부족 또는 값 없음 / Warmup or no values"
        self.label.setText(text)
        self.label.setToolTip(
            json.dumps(self.metadata, ensure_ascii=False)[:4000] if self.state == "complete" else ""
        )


class IndicatorPlot(QWidget):
    """A display pane owns only its own Y range, never the source or a strategy."""

    def __init__(self, chart, code="VOL", parent=None):
        super().__init__(parent)
        self.chart, self.code = chart, code
        self.unit_label = ""
        self.lines = {}
        self.line_styles, self.histogram, self._style_revision = {}, False, 0
        self._histogram_keys = {}
        self.enabled = True
        self._range, self._manual_range = (0.0, 1.0), None
        self._press, self._focus_section, self._hover_y = None, None, None
        self._path_key, self._paths = None, []
        self._sample_paths = {}
        self._render_points = 0
        self.setMouseTracking(True)
        self.setMinimumHeight(115)
        self.setMaximumHeight(220)
        self.chart.rangeChanged.connect(self.refresh_view)
        self.chart.cursorChanged.connect(lambda *_args: self.update())
        self.chart.selectionChanged.connect(lambda *_args: self.update())

    def set_series(self, code, lines, enabled=True):
        if self.code != code:
            self._manual_range = None
        self.code, self.lines, self.enabled = code, lines, enabled
        self.refresh_view()

    def set_styles(self, styles, *, histogram=False):
        if self.line_styles != styles or self.histogram != histogram:
            self.line_styles = {name: dict(value) for name, value in styles.items()}
            self.histogram = histogram
            self._style_revision += 1
            self.refresh_view()

    def refresh_view(self, *_args):
        self._path_key = None
        self.update()

    def plot(self):
        return QRectF(12, 23, max(1, self.width() - 104), max(1, self.height() - 30))

    def _section(self, point):
        if self.plot().contains(point):
            return "plot"
        if (
            self.plot().top() <= point.y() <= self.plot().bottom()
            and point.x() > self.plot().right()
        ):
            return "axis"
        return None

    def _x(self, timestamp):
        begin, end = self.chart._time_bounds()
        return self.plot().left() + (timestamp - begin) / max(1, end - begin) * self.plot().width()

    def _y(self, value):
        low, high = self._range
        return self.plot().bottom() - (value - low) / (high - low) * self.plot().height()

    def _geometry(self):
        self.chart._geometry()
        key = (
            self.size(),
            self.chart._data_revision,
            self.chart._time_bounds(),
            self.code,
            id(self.lines),
            self._manual_range,
            self._style_revision,
        )
        if key == self._path_key:
            return
        self._path_key = key
        times = self.chart.timestamps
        begin, end = self.chart._time_bounds()
        first, stop = bisect.bisect_left(times, begin), bisect.bisect_left(times, end)
        if first and self.chart._bar_end(times[first - 1]) > begin:
            first -= 1
        values = [
            value
            for line in self.lines.values()
            for value in line[first:stop]
            if value is not None and math.isfinite(value)
        ]
        if self._manual_range:
            self._range = self._manual_range
        elif self.code == "MFI":
            self._range = (-0.05, 1.05)
        elif values:
            low, high = min([0.0, *values]), max([0.0, *values])
            spread = max(high - low, abs(high) * 0.01, 1e-10)
            self._range = (
                (0, high + spread * 0.08)
                if self.code == "VOL"
                else (low - spread * 0.08, high + spread * 0.08)
            )
        else:
            self._range = (-1.0, 1.0) if self.histogram else (0.0, 1.0)
        self._paths, self._render_points = [], 0
        self._sample_paths = {}
        self._histogram_keys = {}
        # Bound vertices by viewport width while retaining both extrema per pixel.
        for name, line in self.lines.items():
            path, samples, groups = QPainterPath(), QPainterPath(), {}
            for index in range(first, stop):
                value = line[index]
                if value is not None and math.isfinite(value):
                    column = int(self._x(self.chart._bar_center(times[index])))
                    groups.setdefault(column, []).append((index, value))
            if self.code == "VOL":
                rising, falling = QPainterPath(), QPainterPath()
                for x, group in groups.items():
                    index, maximum = max(group, key=lambda item: item[1])
                    row = self.chart.candles[index]
                    vpath = rising if row.close >= row.open else falling
                    width = max(
                        1,
                        min(
                            12,
                            (self.chart._bar_end(times[index]) - times[index])
                            / max(1, end - begin)
                            * self.plot().width()
                            * 0.72,
                        ),
                    )
                    vpath.addRect(
                        QRectF(
                            x - width / 2,
                            self._y(maximum),
                            width,
                            max(0, self._y(0) - self._y(maximum)),
                        )
                    )
                self._paths.extend((("VOL_UP", rising), ("VOL_DOWN", falling)))
                self._render_points += rising.elementCount() + falling.elementCount()
                continue
            elif self.histogram:
                paths = {
                    (positive, growing): QPainterPath()
                    for positive in (True, False)
                    for growing in (True, False)
                }
                for x, group in groups.items():
                    # Retain both signed extrema at dense zoom, not only the tallest bar.
                    picks = {
                        min(group, key=lambda item: item[1]),
                        max(group, key=lambda item: item[1]),
                    }
                    for index, value in sorted(picks):
                        previous = (
                            line[index - 1]
                            if (index > 0 and times[index] == self.chart._bar_end(times[index - 1]))
                            else None
                        )
                        growing = previous is None or abs(previous) <= abs(value)
                        bar_width = max(
                            1,
                            min(
                                12,
                                (self.chart._bar_end(times[index]) - times[index])
                                / max(1, end - begin)
                                * self.plot().width()
                                * 0.9,
                            ),
                        )
                        y, zero = self._y(value), self._y(0)
                        paths[value >= 0, growing].addRect(
                            QRectF(
                                x - bar_width / 2, min(y, zero), bar_width, max(1, abs(y - zero))
                            )
                        )
                for (positive, growing), histogram_path in paths.items():
                    path_name = f"{name}:{positive}:{growing}"
                    self._histogram_keys[path_name] = name, positive, growing
                    self._paths.append((path_name, histogram_path))
                    self._render_points += histogram_path.elementCount()
                continue
            else:
                previous = None
                for _x, group in sorted(groups.items()):
                    picks = {
                        group[0],
                        group[-1],
                        min(group, key=lambda item: item[1]),
                        max(group, key=lambda item: item[1]),
                    }
                    for index, value in sorted(picks):
                        point = QPointF(
                            self._x(self.chart._bar_center(times[index])), self._y(value)
                        )
                        continuous = previous is not None and all(
                            line[i] is not None and times[i] == self.chart._bar_end(times[i - 1])
                            for i in range(previous + 1, index + 1)
                        )
                        if continuous:
                            path.lineTo(point)
                        else:
                            path.moveTo(point)
                            # A sparse actual observation must remain visible even
                            # when both neighbouring bars are missing. A separate
                            # cached path keeps the line's continuity unchanged.
                            if self.code in (*AUXILIARY_INDICATORS, "NES", "TPD", *ANALYSIS_KINDS):
                                samples.addEllipse(point, 1.8, 1.8)
                        previous = index
            self._paths.append((name, path))
            self._sample_paths[name] = samples
            self._render_points += path.elementCount() + samples.elementCount()

    def paintEvent(self, _event):
        self._geometry()
        painter, colors = QPainter(self), self.chart._colors()
        painter.fillRect(self.rect(), QColor(colors["chart_background"]))
        painter.setPen(QColor(colors["text"]))
        label = self.code + (" · 화면용" if self.chart.language == "ko" else " · display only")
        if self.unit_label:
            label += " · " + self.unit_label
        if not self.enabled:
            label += " · OFF"
        painter.drawText(QPointF(12, 16), label)
        header_right = 12 + painter.fontMetrics().horizontalAdvance(label)
        plot = self.plot()
        for value in nice_ticks(*self._range, 3):
            y = self._y(value)
            painter.setPen(QPen(QColor(colors["chart_grid"]), 1))
            painter.drawLine(QPointF(plot.left(), y), QPointF(plot.right(), y))
            painter.setPen(QColor(colors["chart_axis"]))
            painter.drawText(QPointF(plot.right() + 8, y + 4), format_value(value, compact=True))
        painter.save()
        painter.setClipRect(plot)
        palette = (colors["chart_ema"], colors["accent"], colors["negative"])
        for index, (_name, path) in enumerate(self._paths):
            color = palette[index % len(palette)]
            style = self.line_styles.get(_name, {})
            alpha, width = style.get("opacity", 255), style.get("width", 1.2)
            if self.code == "VOL":
                color = colors["positive"] if _name == "VOL_UP" else colors["negative"]
            elif self.histogram:
                owner, positive, growing = self._histogram_keys[_name]
                style = self.line_styles.get(owner, {})
                color = style.get(
                    "positive_color" if positive else "negative_color",
                    colors["positive"] if positive else colors["negative"],
                )
                alpha = style.get("opacity", 255)
                if not growing:
                    alpha //= 2
            else:
                color = style.get("color", color)
            ink = QColor(color)
            ink.setAlpha(alpha)
            painter.setPen(QPen(ink, width))
            painter.setBrush(ink if self.code == "VOL" or self.histogram else Qt.BrushStyle.NoBrush)
            painter.drawPath(path)
            samples = self._sample_paths.get(_name)
            if samples is not None and not samples.isEmpty():
                painter.setBrush(ink)
                painter.drawPath(samples)
        selected = self.chart.selected_timestamp
        if selected is not None:
            times = self.chart.timestamps
            index = bisect.bisect_right(times, selected) - 1
            if index >= 0 and selected < self.chart._bar_end(times[index]):
                band = QColor(colors["accent"])
                band.setAlpha(45)
                painter.fillRect(
                    QRectF(
                        self._x(times[index]),
                        plot.top(),
                        max(1, self._x(self.chart._bar_end(times[index])) - self._x(times[index])),
                        plot.height(),
                    ),
                    band,
                )
        visible = self.chart._cached_visible
        row = visible[self.chart.cursor] if 0 <= self.chart.cursor < len(visible) else None
        if row is not None:
            x = self._x(self.chart._bar_center(row.open_time_ms))
            painter.setPen(QPen(QColor(colors["chart_crosshair"]), 1, Qt.PenStyle.DashLine))
            painter.drawLine(QPointF(x, plot.top()), QPointF(x, plot.bottom()))
        if self._hover_y is not None:
            painter.drawLine(
                QPointF(plot.left(), self._hover_y), QPointF(plot.right(), self._hover_y)
            )
        painter.restore()
        if self._hover_y is not None:
            low, high = self._range
            value = high - (self._hover_y - plot.top()) / plot.height() * (high - low)
            painter.fillRect(
                QRectF(plot.right() + 3, self._hover_y - 10, 88, 20), QColor(colors["surface"])
            )
            painter.setPen(QColor(colors["text"]))
            painter.drawText(
                QPointF(plot.right() + 8, self._hover_y + 4), format_value(value, compact=True)
            )
        if row is not None:
            index = bisect.bisect_left(self.chart.timestamps, row.open_time_ms)
            label = "  ".join(
                f"{name} {format_value(line[index])}"
                for name, line in self.lines.items()
                if line[index] is not None
            )
            painter.setPen(QColor(colors["text"]))
            left = max(min(180, self.width() / 2), header_right + 12)
            if self.width() - left > 40:
                painter.drawText(
                    QPointF(left, 16),
                    painter.fontMetrics().elidedText(
                        label, Qt.TextElideMode.ElideRight, int(self.width() - left - 12)
                    ),
                )

    def mousePressEvent(self, event):
        self._geometry()
        self._focus_section = self._section(event.position())
        self._press = (
            QPointF(event.position()),
            self._focus_section,
            self.chart._time_bounds(),
            self._range,
            event.button(),
        )
        event.accept()

    def mouseMoveEvent(self, event):
        if self._press is not None and self._press[4] == Qt.MouseButton.LeftButton:
            point, section, bounds, vertical, _button = self._press
            dx, dy = event.position().x() - point.x(), event.position().y() - point.y()
            if section == "plot":
                self.chart._pan_time(dx * self.chart._plot().width() / self.plot().width(), bounds)
            elif section == "axis":
                factor = math.exp(max(-6, min(6, dy / self.plot().height() * 2)))
                low, high = vertical
                mid, span = (low + high) / 2, (high - low) * factor
                candidate = (
                    (0.0, high * factor) if self.code == "VOL" else (mid - span / 2, mid + span / 2)
                )
                if all(math.isfinite(value) for value in candidate) and candidate[0] < candidate[1]:
                    self._manual_range = candidate
            self.refresh_view()
            return
        if self._section(event.position()) == "plot":
            begin, end = self.chart._time_bounds()
            instant = begin + (event.position().x() - self.plot().left()) / self.plot().width() * (
                end - begin
            )
            self.chart.set_cursor(round(instant))
            visible = self.chart._cached_visible
            timestamp = (
                visible[self.chart.cursor].open_time_ms
                if 0 <= self.chart.cursor < len(visible)
                else None
            )
            self.chart.cursorChanged.emit(timestamp)
            self._hover_y = event.position().y()
            self.update()
        else:
            self._hover_y = None
            self.update()

    def mouseReleaseEvent(self, event):
        press, self._press = self._press, None
        if press is None or (event.position() - press[0]).manhattanLength() >= 4:
            return
        if event.button() == Qt.MouseButton.RightButton:
            self._manual_range = None
            self.refresh_view()
        elif event.button() == Qt.MouseButton.LeftButton and press[1] == "plot":
            begin, end = self.chart._time_bounds()
            instant = begin + (event.position().x() - self.plot().left()) / self.plot().width() * (
                end - begin
            )
            times = self.chart.timestamps
            index = bisect.bisect_right(times, instant) - 1
            if index >= 0 and instant < self.chart._bar_end(times[index]):
                self.chart.selected_timestamp = (
                    None if self.chart.selected_timestamp == times[index] else times[index]
                )
                self.chart.selectionChanged.emit(self.chart.selected_timestamp)
                self.chart.update()

    def wheelEvent(self, event):
        if self._focus_section == "plot" and self.chart.candles:
            delta = event.angleDelta().y() / 120
            self.chart._zoom_time(
                math.exp(max(-6, min(6, -delta * 0.18))), self.chart._time_bounds()
            )
        event.accept()

    def mouseDoubleClickEvent(self, event):
        if self._section(event.position()) == "axis":
            self._manual_range = None
            self.refresh_view()

    def leaveEvent(self, _event):
        self._focus_section, self._hover_y = None, None
        self.chart.set_cursor(None)
        self.chart.cursorChanged.emit(None)
        self.update()
