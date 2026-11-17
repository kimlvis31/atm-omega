"""Indexed MAIN price-window display snapshots and structured presentation draft.

Numerical values come only from the core's common producer. Exact wire content,
not the float display model, fences every accepted result.
"""

import bisect
import copy
import json
import math
import re
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

from PySide6.QtCore import QPointF, Qt
from PySide6.QtGui import QColor, QPainterPath, QPen
from PySide6.QtWidgets import QCheckBox, QComboBox, QFormLayout, QLabel, QLineEdit, QWidget

from omega.domain.candle_content import CONTENT_SCOPE, candle_content_pin
from omega.gui.chart_indicators import PriceOverlayRequest
from omega.gui.chart_profiles import default_price_windows, validate_price_windows

SCOPE = CONTENT_SCOPE
FAMILIES = ("sma", "wma", "bol")
PERIODS = (1, 3, 5, 15, 30, 60, 120, 240, 360, 480, 720, 1440)
RESULT_FIELDS = {
    "instrument",
    "kind",
    "period_minutes",
    "definitions",
    "replay_start_ms",
    "display_start_ms",
    "end_ms",
    "source",
    "as_of_ms",
    "produced_at_ms",
    "knowledge_mode",
    "source_checksum_scope",
    "source_checksum",
    "display_content_pin",
    "columns",
    "rows",
    "quality",
}


def _integer(value, maximum=253402300799000):
    return type(value) is int and 0 <= value <= maximum


def _finite(value):
    try:
        return type(value) in {int, float} and math.isfinite(value)
    except OverflowError:
        return False


def _digest(value):
    return type(value) is str and re.fullmatch(r"[a-f0-9]{64}", value) is not None


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def exact_snapshot_pin(instrument, wire_rows):
    """Return immutable range/pin before any OHLCV conversion to display floats.

    Legacy-only callers may still display a float DTO. That DTO deliberately
    cannot verify this overlay and never receives a float-derived fallback pin.
    """
    if type(wire_rows) is not list or not 1 <= len(wire_rows) <= 10000:
        raise ValueError("exact candle wire evidence unavailable")
    keys = ("open_time_ms", "open", "high", "low", "close", "volume")
    rows = tuple(tuple(row[key] for key in keys) for row in wire_rows)
    start, end = rows[0][0], rows[-1][0]
    if not _integer(start) or not _integer(end):
        raise ValueError("invalid exact candle timestamp")
    end += 60000
    pin = candle_content_pin(instrument, start, end, rows)
    # The shared pin validates exact types/expansion; geometry is a caller duty.
    for row in rows:
        opening, high, low, close, volume = (Decimal(value) for value in row[1:])
        if low > min(opening, close) or high < max(opening, close) or volume < 0:
            raise ValueError("invalid exact candle geometry")
    return start, end, pin


def definitions(settings):
    """Canonical calculation identity; visibility and style never enter a job."""
    result = []
    for code in FAMILIES:
        family = settings[code]
        if not family["enabled"]:
            continue
        for index, line in enumerate(family["lines"]):
            if line["active"]:
                result.append(
                    {
                        "analysis_code": code.upper(),
                        "line_index": index,
                        "nSamples": line["nSamples"],
                        "price_precision": settings["price_precision"],
                        **(
                            {"MAType": family["MAType"], "bandWidth": line["bandWidth"]}
                            if code == "bol"
                            else {}
                        ),
                    }
                )
    return sorted(result, key=lambda item: (item["analysis_code"], item["line_index"]))


def output_columns(items, period):
    interval_id = PERIODS.index(period)
    return [
        f"{interval_id}_{item['analysis_code']}_{item['line_index']}_{field}"
        for item in items
        for field in (
            ("BOLLOW", "BOLHIGH", "MA")
            if item["analysis_code"] == "BOL"
            else (item["analysis_code"],)
        )
    ]


class PriceWindowsRequest(PriceOverlayRequest):
    """One batched job per pane, independent of the old single-producer paths."""

    def __init__(self, pane, label, retry):
        self.origins = {}
        super().__init__(pane, "PRICE_WINDOWS", label, retry)

    def context(self):
        pane, settings = self.pane, self.pane.profile["price_windows"]
        items = definitions(settings)
        context, reason = pane._producer_context(self.kind, {}, bool(items))
        if context is None:
            return None, reason
        if pane._price_raw_epoch != pane.owner._series_epoch:
            return None, "source_unverifiable"
        evidence = pane._price_content
        if evidence is None:
            return None, "source_unverifiable"
        params = context[1]
        if evidence[:2] != (params["start_ms"], params["end_ms"]):
            return None, "source_unverifiable"
        pair = params["instrument"], params["period_minutes"]
        origin = settings["origin_ms"]
        if origin is None:
            if pair not in self.origins and len(self.origins) >= 128:
                return None, "origin_capacity"
            origin = self.origins.setdefault(pair, params["start_ms"])
        if not 0 <= origin <= params["start_ms"] < params["end_ms"]:
            return None, "origin_range"
        minutes = (params["end_ms"] - origin) // 60000
        slots = len(output_columns(items, params["period_minutes"]))
        if minutes > 10000:
            return None, "origin_range"
        if ((minutes + pane.minutes - 1) // pane.minutes) * slots > 20000:
            return None, "scalar_capacity"
        params.update(start_ms=origin, display_start_ms=evidence[0], definitions=items)
        return (
            (pane.owner._series_epoch, pane._raw_revision, evidence, _encoded(params)),
            params,
        ), "ready"

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
                self.state, self.rows, self.metadata = "queued", (), {}
                self.send("POST")
            elif self.state == "disconnected":
                self.state = "unknown"
        self.pane.chart.set_price_windows(
            self.rows,
            self.params["definitions"] if self.params else (),
            self.pane.profile["price_windows"],
        )
        self._sync_lookup_polling()
        self.status()

    def result(self, tag, response):
        if tag != self.tag or not self.alive():
            if not self.alive():
                self._sync_lookup_polling()
            return
        self.tag = None
        context, _ = self.context()
        if context is None or context[0] != self.key:
            self.timer.stop()
            return
        try:
            if (
                type(response) is not dict
                or response.get("id") != self.identity
                or _encoded(response.get("parameters")) != _encoded(self.params)
                or response.get("core_epoch") != self.pane.owner._series_epoch
                or response.get("spec_hash") != sha256(_encoded(self.params).encode()).hexdigest()
                or response.get("state")
                not in {"queued", "running", "complete", "failed", "cancelled", "expired"}
            ):
                raise ValueError("price-window job identity changed")
            self.state = response["state"]
            if self.state == "complete":
                value = response.get("result")
                if (
                    type(value) is dict
                    and _digest(value.get("display_content_pin"))
                    and value["display_content_pin"] != self.pane._price_content[2]
                ):
                    self.state, self.rows = "source_changed", ()
                else:
                    self.rows = self.parse(value)
                    self.lookup_policy.accept_response()
            elif self.state in {"failed", "cancelled", "expired"}:
                self.rows = ()
                self.lookup_policy.accept_response()
            else:
                self.lookup_policy.accept_response()
        except (ValueError, TypeError, KeyError, OverflowError):
            # A malformed completed DTO is terminal until explicit retry, not a
            # polling/resubmission loop that can repaint the previous context.
            self.state, self.rows = "failed", ()
        self.ensure()
        self.pane._selection_readout(self.pane.chart.selected_timestamp)

    def request_again(self):
        if self._manual_lookup():
            return
        if self.state in {"source_unverifiable", "awaiting_data", "source_changed"}:
            self.key = self.tag = None
            self.pane.owner.refresh()
        elif self.state in {"failed", "cancelled", "expired"}:
            self.key = None
            self.ensure()

    def failed(self, tag, message):
        super().failed(tag, message)
        if self.alive():
            self.pane._selection_readout(self.pane.chart.selected_timestamp)

    def parse(self, result):
        spec = self.params
        columns = output_columns(spec["definitions"], spec["period_minutes"])
        if (
            type(result) is not dict
            or set(result) != RESULT_FIELDS
            or any(
                type(result[key]) is not type(spec[key]) or result[key] != spec[key]
                for key in ("instrument", "kind", "period_minutes", "display_start_ms", "end_ms")
            )
            or _encoded(result["definitions"]) != _encoded(spec["definitions"])
            or type(result["replay_start_ms"]) is not int
            or result["replay_start_ms"] != spec["start_ms"]
            or result["source"] != "database"
            or result["knowledge_mode"] != "retrospective_display"
            or result["source_checksum_scope"] != SCOPE
            or not _digest(result["source_checksum"])
            or not _digest(result["display_content_pin"])
            or result["display_content_pin"] != self.pane._price_content[2]
            or not _integer(result["as_of_ms"])
            or not _integer(result["produced_at_ms"])
            or not spec["end_ms"] <= result["as_of_ms"] <= result["produced_at_ms"]
            or type(result["columns"]) is not list
            or result["columns"] != columns
            or type(result["rows"]) is not list
            or len(result["rows"]) > 10000
            or len(result["rows"]) * len(columns) > 20000
        ):
            raise ValueError("invalid bounded price-window snapshot")
        quality = result["quality"]
        if (
            type(quality) is not dict
            or set(quality)
            != {
                "requested_minutes",
                "observed_minutes",
                "missing_minutes",
                "reset_count",
                "complete_bars",
            }
            or any(not _integer(value, 10000) for value in quality.values())
            or quality["requested_minutes"] != (spec["end_ms"] - spec["start_ms"]) // 60000
            or quality["observed_minutes"] + quality["missing_minutes"]
            != quality["requested_minutes"]
            or quality["reset_count"] > quality["observed_minutes"]
            or quality["complete_bars"] != len(result["rows"])
            or quality["complete_bars"] * spec["period_minutes"] > quality["observed_minutes"]
        ):
            raise ValueError("invalid price-window quality")
        width, previous, segment, rows = spec["period_minutes"] * 60000, None, 0, []
        for row in result["rows"]:
            if type(row) is not dict or set(row) != {
                "open_time_ms",
                "close_time_ms",
                "available_at_ms",
                "segment",
                "values",
                "ready",
            }:
                raise ValueError("invalid price-window row fields")
            opening, close, current = row["open_time_ms"], row["close_time_ms"], row["segment"]
            if (
                not _integer(opening)
                or not _integer(close)
                or opening % width
                or close != opening + width
                or not spec["display_start_ms"] <= opening < close <= spec["end_ms"]
                or type(row["available_at_ms"]) is not int
                or row["available_at_ms"] != result["produced_at_ms"]
                or not _integer(current, 10000)
                or current > quality["reset_count"]
                or previous is not None
                and (
                    opening <= previous
                    or current < segment
                    or opening != previous + width
                    and current == segment
                )
                or type(row["values"]) is not list
                or len(row["values"]) != len(columns)
                or any(value is not None and not _finite(value) for value in row["values"])
                or type(row["ready"]) is not list
                or len(row["ready"]) != len(spec["definitions"])
                or any(type(value) is not bool for value in row["ready"])
            ):
                raise ValueError("invalid price-window complete row or gap")
            offset = 0
            for item, ready in zip(spec["definitions"], row["ready"], strict=True):
                count = 3 if item["analysis_code"] == "BOL" else 1
                if ready != all(
                    value is not None for value in row["values"][offset : offset + count]
                ):
                    raise ValueError("price-window readiness differs from published outputs")
                offset += count
            rows.append((opening, close, current, tuple(row["values"]), tuple(row["ready"])))
            previous, segment = opening, current
        self.metadata = {
            key: result[key]
            for key in (
                "source_checksum",
                "display_content_pin",
                "as_of_ms",
                "produced_at_ms",
                "quality",
                "replay_start_ms",
                "knowledge_mode",
            )
        }
        return tuple(rows)

    def readout(self, timestamp):
        items = definitions(self.pane.profile["price_windows"])
        row = next((row for row in self.rows if row[0] == timestamp), None)
        result, offset = [], 0
        for index, item in enumerate(items):
            code, line = item["analysis_code"], item["line_index"]
            count = 3 if code == "BOL" else 1
            label = f"{code}[{line}] N={item['nSamples']} precision={item['price_precision']}"
            if code == "BOL":
                label += f" {item['MAType']} bandWidth={item['bandWidth']}"
            if self.state != "complete" or row is None:
                label += " " + (self.state if self.state != "complete" else "missing")
            else:
                values = row[3][offset : offset + count]
                label += " " + ("ready" if row[4][index] else "warmup")
                for field, value in zip(
                    ("BOLLOW", "BOLHIGH", "MA") if code == "BOL" else (code,), values, strict=True
                ):
                    label += f" {field}=" + ("null" if value is None else f"{value:.8g}")
            result.append(label)
            offset += count
        return result

    def status(self):
        self._sync_lookup_polling()
        ko = self.pane.owner.language == "ko"
        enabled = any(self.pane.profile["price_windows"][code]["enabled"] for code in FAMILIES)
        self.label.setVisible(enabled)
        refresh = self.state in {"source_unverifiable", "awaiting_data", "source_changed"}
        self.retry.setVisible(
            enabled
            and (refresh or self.state in {"failed", "cancelled", "expired", "lookup_paused"})
        )
        self.retry.setText(
            ("같은 요청 조회" if ko else "Check same request")
            if self.state == "lookup_paused"
            else ("원본 재조회" if ko else "Refresh source")
            if refresh
            else ("다시 계산" if ko else "Recalculate")
        )
        messages = {
            "lookup_paused": (
                "연속 조회 실패 · 자동 조회 중지 · 같은 요청을 직접 조회하세요",
                "Automatic lookup paused after repeated failures. Check the same request manually.",
            ),
            "off": ("계산 꺼짐", "Calculation off"),
            "source_unverifiable": ("원본 데이터 재조회 필요", "Refresh source candles to verify"),
            "awaiting_data": ("새 코어 원본 재조회 필요", "Refresh current core candles"),
            "source_changed": ("원본 변경 · 재조회 필요", "Source changed; refresh required"),
            "source_unavailable": (
                "DB 원본만 지원 · 합성 데이터 제외",
                "Database source required; demo unsupported",
            ),
            "period_unavailable": ("1분~1일 봉만 지원", "Supported periods: 1 minute to 1 day"),
            "api_unavailable": ("코어 시계열 조회 기능 필요", "Core chart-series support required"),
            "range_unavailable": (
                "최대10000분의 원본 범위 필요",
                "Source range must fit10000minutes",
            ),
            "disconnected": ("코어 연결 끊김", "Core disconnected"),
            "queued": ("계산 대기", "Queued"),
            "running": ("계산 중", "Calculating"),
            "unknown": (
                "결과 미확인 · 같은 요청 조회 중",
                "Outcome unknown; checking the same request",
            ),
            "failed": ("계산 실패 · 명시적으로 다시 계산", "Calculation failed; retry explicitly"),
            "cancelled": ("계산 취소됨", "Calculation cancelled"),
            "expired": ("결과 만료 · 다시 계산 필요", "Result expired; recalculate"),
            "origin_range": (
                "UTC 시작점 지정 필요 · 최대10000분",
                "Set origin; maximum10000minutes",
            ),
            "origin_capacity": ("UTC 시작점을 직접 지정하세요", "Set an explicit UTC origin"),
            "scalar_capacity": (
                "출력 예산 초과 · 최대20000개 값",
                "Output budget exceeded; maximum20000values",
            ),
            "complete": (
                "과거 표시 · 당시 인지 아님",
                "Retrospective display, not historical knowledge",
            ),
        }
        message = messages.get(self.state, ("상태 확인 필요", "Status unavailable"))[0 if ko else 1]
        self.label.setText("SMA / WMA / BOL · " + message)
        self.label.setToolTip(str(self.metadata)[:4000] if self.state == "complete" else "")


class PriceWindowsEditor(QWidget):
    """Ten indexed drafts per family through one compact editor, never raw JSON."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self._draft, self._selection, self._loading = default_price_windows(), ("sma", 0), False
        form = QFormLayout(self)
        form.setContentsMargins(0, 0, 0, 0)
        self.family, self.index = QComboBox(), QComboBox()
        self.family.addItems([code.upper() for code in FAMILIES])
        self.index.addItems([str(index) for index in range(10)])
        self.origin, self.precision = QLineEdit(), QLineEdit()
        self.origin.setMaxLength(32)
        self.precision.setMaxLength(2)
        self.master, self.active, self.visible = QCheckBox(), QCheckBox(), QCheckBox()
        self.ma_type = QComboBox()
        self.ma_type.addItems(["SMA", "WMA", "EMA"])
        self.center, self.band = QCheckBox(), QCheckBox()
        self.fields = {
            key: QLineEdit()
            for key in ("nSamples", "color", "opacity", "stroke_width", "bandWidth")
        }
        for field in self.fields.values():
            field.setMaxLength(64)
        self.labels = []
        for korean, english, field in (
            ("지표 종류", "SMA / WMA / BOL", self.family),
            ("선 번호 (0–9)", "Line index (0–9)", self.index),
            ("계산 시작점 (UTC밀리초)", "Origin (UTC milliseconds)", self.origin),
            ("가격 소수 자릿수", "Price precision", self.precision),
            ("지표 전체 계산", "Family master", self.master),
            ("이 선 계산", "Line calculation", self.active),
            ("이 선 표시", "Line visible", self.visible),
            ("BOL 전체 중심 MA", "BOL center MA (family)", self.ma_type),
            ("BOL 중심선 표시", "BOL center visible", self.center),
            ("BOL 채움 표시", "BOL band visible", self.band),
            ("표본 수", "Samples", self.fields["nSamples"]),
            ("색상 #RRGGBB", "Color #RRGGBB", self.fields["color"]),
            ("불투명도 0–255", "Opacity 0–255", self.fields["opacity"]),
            ("선 굵기 0.25–10px", "Stroke width 0.25–10px", self.fields["stroke_width"]),
            (
                "BOL 편차 배수 (음수·0 허용)",
                "BOL deviation multiplier (signed)",
                self.fields["bandWidth"],
            ),
        ):
            label = QLabel()
            label.setWordWrap(True)
            self.labels.append((label, korean, english))
            form.addRow(label, field)
        self.note = QLabel()
        self.note.setWordWrap(True)
        form.addRow(self.note)
        self.family.currentTextChanged.connect(self._select)
        self.index.currentIndexChanged.connect(self._select)
        self.set_draft(self._draft)
        self.set_presentation("en")

    def set_presentation(self, language):
        """Translate presentation only, retaining even invalid unsaved text."""
        ko = language == "ko"
        for label, korean, english in self.labels:
            label.setText(korean if ko else english)
        self.note.setText(
            "계산과 표시는 별도입니다. 적용은 임시이며 저장은 별도로 누릅니다. "
            "시작점 빈칸은 종목·주기별 최초 분봉에 고정됩니다. 고급 시작점은 UTC밀리초 "
            "정수로 입력하며 최대10000분 범위를 지원합니다."
            if ko
            else "Calculation and visibility are separate. Apply is temporary; Save is explicit. "
            "A blank origin fixes the first loaded minute per instrument and period. "
            "Advanced origins use integer UTC milliseconds; the range supports up to10000minutes."
        )
        self.origin.setPlaceholderText(
            "빈칸 = 최초 분봉에 고정" if ko else "Blank = fixed first minute"
        )
        self.origin.setToolTip(
            "고급 설정: UTC밀리초 정수, 60000의 배수. 빈칸이면 최초 분봉 시작점을 유지하며 "
            "화면 이동에 따라 자동 변경하지 않습니다."
            if ko
            else "Advanced: integer UTC milliseconds, a multiple of60000. Blank keeps the "
            "first loaded minute fixed; moving the viewport never slides this origin."
        )
        self.fields["bandWidth"].setToolTip(
            "표준편차 배수입니다. 음수와0을 그대로 유지하며 픽셀 선 굵기와 다릅니다."
            if ko
            else "Standard-deviation multiplier. Negative and zero values are preserved; "
            "this is separate from pixel stroke width."
        )

    def _store(self):
        code, index = self._selection
        family, line = self._draft[code], self._draft[code]["lines"][index]
        family["enabled"] = self.master.isChecked()
        if code == "bol":
            family.update(
                MAType=self.ma_type.currentText(),
                show_center=self.center.isChecked(),
                show_band=self.band.isChecked(),
            )
        line.update(active=self.active.isChecked(), visible=self.visible.isChecked())
        # Keep invalid text intact while switching rows; whole-draft validation
        # happens atomically in draft(), before profile application or disk I/O.
        for key, field in self.fields.items():
            if key != "bandWidth" or code == "bol":
                line[key] = field.text().strip()

    def _select(self, *_args):
        if self._loading:
            return
        self._store()
        self._selection = self.family.currentText().lower(), self.index.currentIndex()
        self._fill()

    def _fill(self):
        code, index = self._selection
        family, line = self._draft[code], self._draft[code]["lines"][index]
        self.master.setChecked(family["enabled"])
        self.active.setChecked(line["active"])
        self.visible.setChecked(line["visible"])
        self.ma_type.setCurrentText(self._draft["bol"]["MAType"])
        self.center.setChecked(self._draft["bol"]["show_center"])
        self.band.setChecked(self._draft["bol"]["show_band"])
        for field in (self.ma_type, self.center, self.band, self.fields["bandWidth"]):
            field.setEnabled(code == "bol")
        for key, field in self.fields.items():
            field.setText(str(line.get(key, "")))

    def set_draft(self, value):
        value = validate_price_windows(value)
        self._loading = True
        self._draft = value
        self.origin.setText("" if value["origin_ms"] is None else str(value["origin_ms"]))
        self.precision.setText(str(value["price_precision"]))
        self._fill()
        self._loading = False

    def draft(self):
        self._store()
        value = copy.deepcopy(self._draft)
        value["origin_ms"] = int(self.origin.text()) if self.origin.text().strip() else None
        value["price_precision"] = int(self.precision.text())
        for code in FAMILIES:
            for line in value[code]["lines"]:
                for key in ("nSamples", "opacity"):
                    line[key] = int(line[key])
                line["stroke_width"] = float(line["stroke_width"])
                if code == "bol" and type(line["bandWidth"]) is str:
                    # Preserve native int versus float request identity, including
                    # values on untouched indexed rows loaded from a saved profile.
                    text = line["bandWidth"]
                    line["bandWidth"] = (
                        int(text) if re.fullmatch(r"[+-]?\d+", text) else float(text)
                    )
        return validate_price_windows(value)


def _crossing(start, end, axis, boundary):
    # Scale before subtraction so opposite finite extremes cannot overflow.
    scale = max(abs(start[axis]), abs(end[axis]), abs(boundary), 1)
    fraction = (boundary / scale - start[axis] / scale) / (end[axis] / scale - start[axis] / scale)
    return tuple(
        boundary if index == axis else (1 - fraction) * start[index] + fraction * end[index]
        for index in (0, 1)
    )


def _clip_polygon(points, bounds):
    """Clip in data space before QPainter sees any potentially huge coordinates."""
    for axis, boundary, keep_above in bounds:
        clipped = []
        if not points:
            break
        previous = points[-1]
        previous_inside = previous[axis] >= boundary if keep_above else previous[axis] <= boundary
        for point in points:
            inside = point[axis] >= boundary if keep_above else point[axis] <= boundary
            if inside != previous_inside:
                clipped.append(_crossing(previous, point, axis, boundary))
            if inside:
                clipped.append(point)
            previous, previous_inside = point, inside
        points = clipped
    return points


def _clip_line(start, end, bounds):
    for axis, boundary, keep_above in bounds:
        first = start[axis] >= boundary if keep_above else start[axis] <= boundary
        last = end[axis] >= boundary if keep_above else end[axis] <= boundary
        if not first and not last:
            return None
        if first != last:
            cross = _crossing(start, end, axis, boundary)
            if first:
                end = cross
            else:
                start = cross
    return start, end


def draw_price_windows(chart, painter, *, bands):
    """Paint only adjacent retained bar centers; hidden LOD bars break segments.

    Each band is the original four-vertex polygon, with named signed endpoints.
    Geometry is bounded by the chart's existing viewport candle LOD.
    """
    rows, items, settings = chart.price_window_snapshot
    if not rows:
        return
    times = chart._price_window_times
    retained = {row.open_time_ms for row in chart._display}
    begin, end = chart._time_bounds()
    low, high = chart._price_range
    bounds = ((0, begin, True), (0, end, False), (1, low, True), (1, high, False))
    first = max(0, bisect.bisect_left(times, begin) - 1)
    stop = min(len(rows), bisect.bisect_right(times, end) + 1)
    offset = 0
    painter.save()
    painter.setClipRect(chart._plot())
    for item in items:
        code, index = item["analysis_code"].lower(), item["line_index"]
        family, line = settings[code], settings[code]["lines"][index]
        count = 3 if code == "bol" else 1
        if not family["enabled"] or not line["active"] or not line["visible"]:
            offset += count
            continue
        if bands and (code != "bol" or not family["show_band"]):
            offset += count
            continue
        if not bands and code == "bol" and not family["show_center"]:
            offset += count
            continue
        color = QColor(line["color"])
        color.setAlpha(line["opacity"] if bands or code != "bol" else 255)
        painter.setPen(Qt.PenStyle.NoPen if bands else QPen(color, line["stroke_width"]))
        painter.setBrush(color if bands else Qt.BrushStyle.NoBrush)
        for position in range(max(1, first), stop):
            left, right = rows[position - 1], rows[position]
            if (
                left[1] != right[0]
                or left[2] != right[2]
                or left[0] not in retained
                or right[0] not in retained
            ):
                continue
            fields = (offset, offset + 1) if bands else (offset + 2 if code == "bol" else offset,)
            if any(row[3][field] is None for row in (left, right) for field in fields):
                continue
            x1, x2 = (left[0] + left[1]) / 2, (right[0] + right[1]) / 2
            if bands:
                lower1, upper1 = (left[3][field] for field in fields)
                lower2, upper2 = (right[3][field] for field in fields)
                if lower1 == upper1 and lower2 == upper2:
                    continue
                points = _clip_polygon(
                    [(x1, lower1), (x2, lower2), (x2, upper2), (x1, upper1)], bounds
                )
                if len(points) < 3:
                    continue
                points = [(chart._x(x), chart._y(y)) for x, y in points]
                if not all(math.isfinite(value) for point in points for value in point):
                    continue
                path = QPainterPath(QPointF(*points[0]))
                for point in points[1:]:
                    path.lineTo(QPointF(*point))
                path.closeSubpath()
                painter.drawPath(path)
            else:
                segment = _clip_line((x1, left[3][fields[0]]), (x2, right[3][fields[0]]), bounds)
                if segment is not None:
                    points = [(chart._x(x), chart._y(y)) for x, y in segment]
                    if all(math.isfinite(value) for point in points for value in point):
                        painter.drawLine(QPointF(*points[0]), QPointF(*points[1]))
        offset += count
    painter.restore()
