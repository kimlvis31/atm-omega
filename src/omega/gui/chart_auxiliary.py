"""Bounded API-only observed SI series. Missing observations remain missing."""

import json
import math
import re
from copy import deepcopy
from urllib.parse import urlencode

from PySide6.QtCore import QObject

from omega.gui.chart_profiles import AUXILIARY_FIELDS

PERIODS = {1, 3, 5, 15, 30, 60, 120, 240, 360, 480, 720, 1440}
MAX_TIME = 253_402_300_799_000
UNITS = {
    "METRIC_OPENINTEREST": "contracts",
    "METRIC_OPENINTERESTVALUE": "quote_asset",
    "METRIC_LONGSHORTRATIO": "ratio",
    "METRIC_LONGACCOUNT": "fraction",
    "METRIC_SHORTACCOUNT": "fraction",
    "FUNDING_RATE": "signed_rate",
    "DEPTH_BIDS": "quote_asset",
    "DEPTH_ASKS": "quote_asset",
    **{
        f"AGGTRADE_{mode}{side}": unit
        for mode, unit in (
            ("NOTIONAL", "quote_asset"),
            ("QUANTITY", "base_asset"),
            ("NTRADES", "count"),
        )
        for side in ("BUY", "SELL")
    },
}


def _integer(value):
    return type(value) is int and 0 <= value <= MAX_TIME


class AuxiliaryChartRequest(QObject):
    def __init__(self, pane, kind, label, retry):
        super().__init__(pane)
        self.pane, self.kind, self.label, self.retry = pane, kind, label, retry
        self.key = self.tag = self.parameters = None
        self.serial, self.state, self.metadata = 0, "off", {}
        self.points, self.units = {}, {}
        self._line_key, self._lines = None, {}
        retry.clicked.connect(self.request_again)
        client = pane.owner.client
        if hasattr(client, "resultReceived"):
            client.resultReceived.connect(self.result)
        client.failed.connect(self.failed)

    def context(self):
        pane, owner = self.pane, self.pane.owner
        if not pane.profile["enabled"][self.kind]:
            return None, "off"
        if not owner.client.attached:
            return None, "disconnected"
        if not owner._auxiliary_supported or not owner._series_epoch:
            return None, "api_unavailable"
        if pane._raw_epoch != owner._series_epoch:
            return None, "awaiting_data"
        if pane._raw_source != "database":
            return None, "source_unavailable"
        if pane.minutes not in PERIODS:
            return None, "period_unavailable"
        if not pane.raw or len(pane.raw) > 10000:
            return None, "range_unavailable"
        params = {
            "instrument": pane.selection.text().strip(),
            "kind": self.kind,
            "start_ms": pane.raw[0].open_time_ms,
            "end_ms": pane.raw[-1].open_time_ms + 60000,
            "period_minutes": pane.minutes,
        }
        if params["end_ms"] - params["start_ms"] > 10000 * 60000:
            return None, "range_unavailable"
        source = pane.profile["auxiliary"][self.kind]["source"]
        if source:
            params["source"] = source
        return ((owner._series_epoch, pane._raw_revision, tuple(params.items())), params), "ready"

    def clear(self, state):
        self.tag, self.points, self.units, self.metadata = None, {}, {}, {}
        self._line_key, self._lines, self.state = None, {}, state

    def ensure(self):
        context, reason = self.context()
        if self.pane.owner._series_closed:
            self.key = None
            self.clear("closed")
        elif context is None:
            self.key = None
            self.clear(reason)
        elif context[0] != self.key:
            self.key, self.parameters = context
            self.clear("loading")
            self.serial += 1
            self.tag = f"chart-auxiliary:{id(self)}:{self.serial}"
            self.pane.owner.client.request(
                "GET", "/api/chart-observations?" + urlencode(self.parameters), tag=self.tag
            )
        self.status()

    def request_again(self):
        self.key = None
        self.pane._refresh_indicators()

    def current(self):
        context, _ = self.context()
        return (
            not self.pane.owner._series_closed
            and self.pane in self.pane.owner.panes
            and context is not None
            and context[0] == self.key
        )

    def result(self, tag, response):
        if tag != self.tag or not self.current():
            return
        self.tag = None
        try:
            self.parse(response)
        except (ValueError, TypeError, KeyError, OverflowError):
            self.clear("failed")
        self.pane._refresh_indicators()

    def failed(self, tag, _message):
        if tag != self.tag or not self.current():
            return
        self.clear("failed")
        self.pane._refresh_indicators()

    def parse(self, response):
        params = self.parameters
        if (
            type(response) is not dict
            or response.get("schema_version") != 1
            or type(response.get("schema_version")) is not int
            or response.get("scope") != "retrospective_auxiliary_chart"
            or response.get("core_epoch") != self.pane.owner._series_epoch
            or response.get("state") not in {"ready", "partial", "unavailable", "unsupported"}
            or any(
                response.get(key) != params[key]
                for key in ("instrument", "kind", "start_ms", "end_ms", "period_minutes")
            )
            or not _integer(response.get("as_of_ms"))
            or any(
                type(response.get(key)) is not int
                for key in ("start_ms", "end_ms", "period_minutes")
            )
            or type(response.get("source")) is not str
            or not 1 <= len(response["source"]) <= 96
            or not response["source"].isprintable()
            or "source" in params
            and params["source"] != response["source"]
        ):
            raise ValueError("auxiliary chart identity changed")
        series, units = response.get("series"), response.get("units")
        quality, gaps, lineage = (
            response.get("quality"),
            response.get("gaps"),
            response.get("lineage"),
        )
        if (
            type(series) is not dict
            or len(series) > 64
            or type(units) is not dict
            or len(units) > 64
            or set(series) - units.keys()
            or set(units) - set(AUXILIARY_FIELDS[self.kind])
            or any(
                type(name) is not str or re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", name) is None
                for name in units
            )
            or any(type(value) is not str or not 1 <= len(value) <= 48 for value in units.values())
            or any(value != UNITS[name] for name, value in units.items())
            or type(quality) is not dict
            or set(quality) != {"expected_bars", "observed_bars", "missing_bars", "partial_bars"}
            or any(type(value) is not int or not 0 <= value <= 10000 for value in quality.values())
            or type(gaps) is not list
            or len(gaps) > 10000
            or type(lineage) is not list
            or len(lineage) > 10000
            or type(response.get("warnings")) is not list
            or len(response["warnings"]) > 100
            or any(type(value) is not str or len(value) > 1024 for value in response["warnings"])
        ):
            raise ValueError("invalid auxiliary chart metadata")
        step = params["period_minutes"] * 60000
        points = {}
        for name, rows in series.items():
            if type(rows) is not list or len(rows) > 10000:
                raise ValueError("auxiliary chart point capacity exceeded")
            values, previous = {}, None
            for row in rows:
                if (
                    type(row) is not list
                    or len(row) != 2
                    or not _integer(row[0])
                    or row[0] % step
                    or not params["start_ms"] <= row[0]
                    or row[0] + step > params["end_ms"]
                    or previous is not None
                    and row[0] <= previous
                    or type(row[1]) is not str
                    or not 1 <= len(row[1]) <= 128
                ):
                    raise ValueError("invalid auxiliary chart point")
                number = float(row[1])
                if not math.isfinite(number) or abs(number) > 1e100:
                    raise ValueError("auxiliary chart value exceeds display range")
                values[row[0]], previous = number, row[0]
            points[name] = values
        for gap in gaps:
            if (
                type(gap) is not list
                or len(gap) != 2
                or not all(_integer(v) for v in gap)
                or not params["start_ms"] <= gap[0] < gap[1] <= params["end_ms"]
            ):
                raise ValueError("invalid auxiliary chart gap")
        for row in lineage:
            if (
                type(row) is not dict
                or not _integer(row.get("open_time_ms"))
                or not _integer(row.get("available_at_ms"))
                or row["available_at_ms"] > response["as_of_ms"]
                or not params["start_ms"] <= row["open_time_ms"] < params["end_ms"]
                or type(row.get("revision")) is not int
                or row["revision"] < 1
                or type(row.get("checksum")) is not str
                or re.fullmatch(r"[a-f0-9]{64}", row["checksum"]) is None
                or row.get("quality")
                not in {"observed", "partial", "missing", "unsupported", "carried"}
                or type(row.get("coverage")) is not str
                or len(row["coverage"]) > 128
            ):
                raise ValueError("invalid auxiliary chart lineage")
            coverage = float(row["coverage"])
            if not math.isfinite(coverage) or not 0 <= coverage <= 1:
                raise ValueError("invalid auxiliary chart coverage")
        if response["state"] in {"unavailable", "unsupported"} and any(points.values()):
            raise ValueError("unavailable auxiliary data cannot have displayed points")
        self.points, self.units, self.state = points, deepcopy(units), response["state"]
        self.metadata = {
            key: deepcopy(response[key])
            for key in ("as_of_ms", "source", "quality", "gaps", "lineage", "warnings")
        }
        self._line_key = None

    def lines(self):
        fields = self.pane.profile["auxiliary"][self.kind]["fields"]
        key = id(self.points), tuple(fields), self.pane.chart._data_revision
        if key != self._line_key:
            units = {self.units[field] for field in fields if field in self.units}
            values = self.points if len(units) <= 1 else {}
            self._lines = {
                field: tuple(
                    values.get(field, {}).get(row.open_time_ms) for row in self.pane.chart.candles
                )
                for field in fields
            }
            self._line_key = key
        return self._lines

    def unit(self):
        values = {
            self.units.get(field, UNITS[field])
            for field in self.pane.profile["auxiliary"][self.kind]["fields"]
        }
        return next(iter(values)) if len(values) == 1 else ""

    def status(self):
        ko = self.pane.owner.language == "ko"
        enabled = self.pane.profile["enabled"][self.kind]
        self.label.setVisible(enabled)
        self.retry.setVisible(enabled)
        words = {
            "off": ("꺼짐", "off"),
            "loading": ("조회 중", "loading"),
            "ready": ("저장 관측 · 과거 가용성 보증 아님", "stored observations; retrospective"),
            "partial": ("일부 관측 · 결측 유지", "partial observations; gaps retained"),
            "unavailable": ("관측 없음", "no observations"),
            "unsupported": ("미지원 관측/범위", "unsupported observation/range"),
            "disconnected": ("연결 끊김", "disconnected"),
            "awaiting_data": ("새 코어의 봉 대기", "awaiting current core candles"),
            "source_unavailable": ("DB 관측 봉이 필요함", "database candles required"),
            "api_unavailable": ("코어 API 미지원", "core API unavailable"),
            "range_unavailable": ("최대 10000분 범위", "at most 10000 raw minutes"),
            "period_unavailable": ("1분~1일만 지원", "1m–1d supported"),
            "failed": ("조회 실패 · 수동 재요청", "request failed; manual retry"),
        }
        text = words.get(self.state, (self.state, self.state))[0 if ko else 1]
        quality = self.metadata.get("quality")
        if quality:
            text += (
                f" · {quality['observed_bars']}/{quality['expected_bars']}"
                f" · {'결측' if ko else 'missing'} {quality['missing_bars']}"
                f" · {'부분' if ko else 'partial'} {quality['partial_bars']}"
            )
        if self.kind == "DEPTH":
            text += (
                " · 정규화 12구간 합계 (heatmap 아님)"
                if ko
                else " · normalized 12-bin totals (not heatmap)"
            )
        self.label.setText(f"{self.kind} · {text}" + (" · " + self.unit() if self.unit() else ""))
        self.label.setToolTip(json.dumps(self.metadata, ensure_ascii=False)[:16000])
        self.retry.setText("재조회" if ko else "Retry")
        self.retry.setEnabled(enabled and self.state != "loading")
