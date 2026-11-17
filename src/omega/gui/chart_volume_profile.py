"""API-only selected IVP; fixed replay origin and no numerical/DB work in Qt."""

import bisect
import json
import math
import re
from copy import deepcopy
from uuid import uuid4

from omega.gui.chart_indicators import PriceOverlayRequest

SETTINGS = (
    "nSamples",
    "gammaFactor",
    "deltaFactor",
    "prominence",
    "distance",
    "height",
    "price_precision",
    "max_divisions",
)


def checked_row(value, *, maximum_divisions=2048):
    """Validate the bounded drawing payload, keeping seed/missing/zero distinct."""
    fields = {
        "open_time_ms",
        "close_time_ms",
        "available_at_ms",
        "state",
        "gammaFactor",
        "betaFactor",
        "divisionHeight",
        "filtered",
        "maximum",
        "divisions",
        "boundaries",
        "near_boundaries",
    }
    if type(value) is not dict or set(value) != fields:
        raise ValueError("invalid selected IVP row")
    row = deepcopy(value)
    if (
        any(
            type(row[key]) is not int
            for key in ("open_time_ms", "close_time_ms", "available_at_ms")
        )
        or not 0
        <= row["open_time_ms"]
        < row["close_time_ms"]
        <= row["available_at_ms"]
        <= 253402300799000
        or type(row["state"]) is not str
        or row["state"] not in {"ready", "seed", "missing", "zero"}
        or type(row["gammaFactor"]) not in {int, float}
        or not math.isfinite(row["gammaFactor"])
        or not 0.005 <= row["gammaFactor"] <= 0.1
    ):
        raise ValueError("invalid IVP selection or state")
    empty = ("betaFactor", "divisionHeight", "filtered", "maximum", "divisions", "boundaries")
    if row["state"] in {"missing", "seed"}:
        expected_near = None if row["state"] == "missing" else [None] * 10
        if any(row[key] is not None for key in empty) or row["near_boundaries"] != expected_near:
            raise ValueError("unready IVP must not manufacture profile values")
        return row
    if (
        type(row["divisions"]) is not int
        or not 1 <= row["divisions"] <= maximum_divisions
        or any(
            type(row[key]) not in {int, float} or not math.isfinite(row[key]) or row[key] <= 0
            for key in ("betaFactor", "divisionHeight")
        )
        or type(row["near_boundaries"]) is not list
        or len(row["near_boundaries"]) != 10
    ):
        raise ValueError("invalid seeded IVP scale")
    if row["state"] == "zero":
        if (
            any(row[key] is not None for key in ("filtered", "maximum", "boundaries"))
            or row["near_boundaries"] != [None] * 10
        ):
            raise ValueError("zero-volume IVP must remain distinct from a profile")
        return row
    filtered, boundaries, near = row["filtered"], row["boundaries"], row["near_boundaries"]
    if (
        type(filtered) is not list
        or len(filtered) != row["divisions"]
        or any(type(n) not in {int, float} or not math.isfinite(n) or n < 0 for n in filtered)
        or type(row["maximum"]) not in {int, float}
        or not math.isfinite(row["maximum"])
        or row["maximum"] <= 0
        or max(filtered) != row["maximum"]
        or type(boundaries) is not list
        or any(type(n) is not int or not 0 <= n < row["divisions"] for n in boundaries)
        or boundaries != sorted(set(boundaries))
        or any(
            n is not None and (type(n) not in {int, float} or not math.isfinite(n) or n < 0)
            for n in near
        )
    ):
        raise ValueError("invalid IVP profile values")
    return row


class VolumeProfileRequest(PriceOverlayRequest):
    def __init__(self, pane, label, retry):
        self.origins = {}
        super().__init__(pane, "IVP", label, retry)
        pane.chart.selectionChanged.connect(self.ensure)

    def context(self):
        pane, config = self.pane, self.pane.profile["ivp"]
        context, reason = pane._producer_context("IVP", {}, config["enabled"])
        if context is None:
            return None, reason
        selected = pane.chart.selected_timestamp
        index = (
            bisect.bisect_right(pane.chart.timestamps, selected) - 1 if selected is not None else -1
        )
        if index < 0 or selected >= pane.chart._bar_end(pane.chart.timestamps[index]):
            return None, "select_bar"
        params = context[1]
        pair = (params["instrument"], params["period_minutes"])
        start = config["origin_ms"]
        if start is None:
            if pair not in self.origins and len(self.origins) >= 128:
                return None, "origin_capacity"
            start = self.origins.setdefault(pair, pane.raw[0].open_time_ms)
        end = pane.chart._bar_end(pane.chart.timestamps[index])
        if not start < end <= pane.raw[-1].open_time_ms + 60000 or end - start > 10000 * 60000:
            return None, "origin_range"
        params.update(start_ms=start, end_ms=end, **{key: config[key] for key in SETTINGS})
        # New later candles and viewport changes cannot rewrite a pinned historical profile.
        key = (pane.owner._series_epoch, tuple(params.items()))
        return (key, params), "ready"

    def ensure(self, *_args):
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
        config = self.pane.profile["ivp"]
        self.pane.chart.set_volume_profile(
            self.rows[0] if self.rows else None,
            color=config["color"],
            width=config["width"],
            visible=config["visible"],
        )
        self._sync_lookup_polling()
        self.status()

    def parse(self, result):
        params = self.params
        if (
            type(result) is not dict
            or result.get("source") != "database"
            or result.get("kind") != "IVP"
            or result.get("instrument") != params["instrument"]
            or type(result.get("period_minutes")) is not int
            or result["period_minutes"] != params["period_minutes"]
            or any(
                type(result.get(key)) is not type(params[key]) or result.get(key) != params[key]
                for key in SETTINGS
            )
            or result.get("replay_start_ms") != params["start_ms"]
            or type(result.get("replay_start_ms")) is not int
            or result.get("selected_close_ms") != params["end_ms"]
            or type(result.get("selected_close_ms")) is not int
            or result.get("knowledge_mode") != "retrospective_display"
            or any(type(result.get(key)) is not int for key in ("as_of_ms", "produced_at_ms"))
            or not params["end_ms"]
            <= result["as_of_ms"]
            <= result["produced_at_ms"]
            <= 253402300799000
            or re.fullmatch("[a-f0-9]{64}", str(result.get("source_checksum"))) is None
            or type(result.get("quality")) is not dict
            or type(result.get("rows")) is not list
            or len(result["rows"]) != 1
        ):
            raise ValueError("invalid retrospective IVP snapshot")
        row = checked_row(result["rows"][0], maximum_divisions=params["max_divisions"])
        quality = result["quality"]
        counters = {
            "requested_minutes",
            "observed_minutes",
            "missing_minutes",
            "reset_count",
            "complete_bars",
        }
        if (
            set(quality) != counters
            or any(type(value) is not int or not 0 <= value <= 10000 for value in quality.values())
            or quality["requested_minutes"] != (params["end_ms"] - params["start_ms"]) // 60000
            or quality["observed_minutes"] + quality["missing_minutes"]
            != quality["requested_minutes"]
            or quality["reset_count"] > quality["observed_minutes"]
            or quality["complete_bars"] * params["period_minutes"] > quality["observed_minutes"]
            or row["state"] in {"ready", "zero"}
            and quality["complete_bars"] < params["nSamples"]
        ):
            raise ValueError("inconsistent bounded IVP quality counters")
        if (
            row["close_time_ms"] != params["end_ms"]
            or row["open_time_ms"] != params["end_ms"] - params["period_minutes"] * 60000
            or row["available_at_ms"] != result["produced_at_ms"]
            or row["gammaFactor"] != params["gammaFactor"]
        ):
            raise ValueError("IVP selected bar changed")
        self.metadata = {
            key: result[key]
            for key in (
                "replay_start_ms",
                "selected_close_ms",
                "source_checksum",
                "as_of_ms",
                "produced_at_ms",
                "knowledge_mode",
                "quality",
            )
        }
        return (row,)

    def status(self):
        self._sync_lookup_polling()
        enabled = self.pane.profile["ivp"]["enabled"]
        self.label.setVisible(enabled)
        self.retry.setVisible(
            enabled and self.state in {"failed", "cancelled", "expired", "lookup_paused"}
        )
        self.retry.setText(
            ("같은 요청 조회" if self.pane.owner.language == "ko" else "Check same request")
            if self.state == "lookup_paused"
            else "다시 계산 / Recalculate"
        )
        message = {
            "lookup_paused": (
                "연속 조회 실패 · 자동 조회 중지 · 같은 요청을 직접 조회하세요"
                if self.pane.owner.language == "ko"
                else "Lookup paused after repeated failures. Check the same request manually."
            ),
            "select_bar": "봉을 선택하세요 / Select a closed bar",
            "origin_range": (
                "고정 시작점부터 최대10000분 / Fixed replay origin: maximum10000minutes"
            ),
            "origin_capacity": "시작점을 직접 지정하세요 / Set an explicit replay origin",
            "complete": "OHLCV 해석 분포 · 과거 표시용 / Interpreted OHLCV; retrospective display",
        }.get(self.state, self.state)
        if self.rows:
            row = self.rows[0]
            message += (
                f" · {row['state']} · divisions={row['divisions']}"
                f" · γ={row['gammaFactor']} · β={row['betaFactor']}"
            )
        self.label.setText("IVP · " + message)
        self.label.setToolTip(
            json.dumps(self.metadata, ensure_ascii=False)[:4000] if self.rows else ""
        )
