"""Core-produced NES/TPD SI snapshots, never computed from raw auxiliary UI data."""

import json
import math
import re
from uuid import uuid4

from omega.domain.chart_analysis import (
    ANALYSIS_FIELDS,
    ANALYSIS_KINDS,
    analysis_parameters,
    analysis_source_minutes,
)
from omega.domain.errors import ValidationError
from omega.gui.chart_indicators import PriceOverlayRequest
from omega.gui.chart_profiles import (
    analysis_options,
    analysis_profile,
    display_date,
    indicator_enabled,
)

FIELDS = {"NES": ("nSamples", "quote_precision"), "TPD": ("viewLength", "nSamples", "nSamplesMA")}
LIMITS = {"NES": 1440, "TPD": 10000}


def _integer(value, lower=0, upper=253402300799000):
    return type(value) is int and lower <= value <= upper


def _digest(value):
    return type(value) is str and re.fullmatch("[a-f0-9]{64}", value) is not None


class DerivedSeriesRequest(PriceOverlayRequest):
    def __init__(self, pane, kind, label, retry):
        self.origins, self._line_key, self._lines = {}, None, {}
        self.row_quality = {}
        self._line_indices = {}
        self._cancelled_identity = None
        self._failure_reason = ""
        super().__init__(pane, kind, label, retry)

    def context(self):
        if self.kind in ANALYSIS_KINDS:
            return self._analysis_context()
        pane, config = self.pane, self.pane.profile[self.kind.lower()]
        context, reason = pane._producer_context(self.kind, {}, pane.profile["enabled"][self.kind])
        if context is None:
            return None, reason
        params = context[1]
        pair = params["instrument"], params["period_minutes"]
        origin = config["origin_ms"]
        if origin is None:
            if pair not in self.origins and len(self.origins) >= 128:
                return None, "origin_capacity"
            origin = self.origins.setdefault(pair, pane.raw[0].open_time_ms)
        if not origin < params["end_ms"] or params["end_ms"] - origin > LIMITS[self.kind] * 60000:
            return None, "origin_range"
        params.update(start_ms=origin, **{key: config[key] for key in FIELDS[self.kind]})
        return (
            (pane.owner._series_epoch, pane._raw_revision, tuple(params.items())),
            params,
        ), "ready"

    def _analysis_context(self):
        pane = self.pane
        config = analysis_profile(pane.profile, self.kind)
        context, reason = pane._producer_context(
            self.kind, {}, indicator_enabled(pane.profile, self.kind)
        )
        if context is None:
            return None, reason
        if self.kind not in pane.owner._analysis_kinds:
            return None, "kind_unavailable"
        options = analysis_options(self.kind, config)
        if self.kind != "STS" and not options["definitions"]:
            return None, "no_active_lines"
        params = context[1]
        width = params["period_minutes"] * 60000
        end = params["end_ms"] // width * width
        if not end:
            return None, "awaiting_data"
        pair = params["instrument"], params["period_minutes"]
        origin = config["origin_ms"]
        if origin is None:
            if pair not in self.origins and len(self.origins) >= 128:
                return None, "origin_capacity"
            origin = self.origins.setdefault(pair, max(0, end - 128 * width))
        if (
            not origin < end
            or end - origin > analysis_source_minutes(params["period_minutes"]) * 60000
        ):
            return None, "origin_range"
        try:
            params = analysis_parameters(
                self.kind, params | options | {"start_ms": origin, "end_ms": end}
            )
        except ValidationError:
            return None, "invalid_configuration"
        canonical = json.dumps(params, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return ((pane.owner._series_epoch, pane._raw_revision, canonical), params), "ready"

    def cancel(self):
        """Best-effort retirement never replaces the request identity/epoch fence."""
        if (
            self.kind in ANALYSIS_KINDS
            and self.identity
            and self.identity != self._cancelled_identity
            and self.state in {"queued", "running", "unknown", "lookup_paused"}
            and self.pane.owner.client.attached
            and self.key
            and self.key[0] == self.pane.owner._series_epoch
        ):
            self._cancelled_identity = self.identity
            self.pane.owner.client.request(
                "POST",
                "/api/chart-series/" + self.identity + "/cancel",
                {},
                tag=f"derived-cancel:{id(self)}:{self.identity}",
            )

    def ensure(self):
        context, reason = self.context()
        if self.kind in ANALYSIS_KINDS and (
            context is None
            and reason != "disconnected"
            or context is not None
            and context[0] != self.key
        ):
            self.cancel()
        if context is None:
            self.state, self.rows, self.tag = reason, (), None
            if reason != "disconnected":
                self.key = self.identity = None
        else:
            key, params = context
            if key != self.key:
                self._failure_reason = ""
                self.key, self.params, self.identity = key, params, uuid4().hex
                self.state, self.rows = "queued", ()
                self.send("POST")
            elif self.state == "disconnected":
                self.state = "unknown"
        if not self.rows:
            self.row_quality = {}
            if self.kind in ANALYSIS_KINDS:
                self.metadata = {}
        self._sync_lookup_polling()
        self.status()

    def result(self, tag, response):
        previous = self.rows
        candidate_error = (
            response.get("error")
            if (self.kind in ANALYSIS_KINDS and tag == self.tag and type(response) is dict)
            else None
        )
        super().result(tag, response)
        if self.kind in ANALYSIS_KINDS and self.state == "failed" and type(candidate_error) is str:
            self._failure_reason = candidate_error[:500]
            self.status()
        if previous is not self.rows:
            self.pane._refresh_indicators()

    def failed(self, tag, message):
        previous = self.rows
        super().failed(tag, message)
        if previous is not self.rows:
            self.pane._refresh_indicators()

    def parse(self, result):
        if self.kind in ANALYSIS_KINDS:
            return self._parse_analysis(result)
        spec = self.params
        bound = LIMITS[self.kind]
        if (
            type(result) is not dict
            or result.get("source") != "database"
            or result.get("kind") != self.kind
            or result.get("instrument") != spec["instrument"]
            or type(result.get("period_minutes")) is not int
            or result["period_minutes"] != spec["period_minutes"]
            or any(
                type(result.get(key)) is not type(spec[key]) or result.get(key) != spec[key]
                for key in FIELDS[self.kind]
            )
            or result.get("knowledge_mode") != "retrospective_display"
            or not _integer(result.get("as_of_ms"))
            or not _integer(result.get("produced_at_ms"))
            or not spec["end_ms"] <= result["as_of_ms"] <= result["produced_at_ms"]
            or not _digest(result.get("source_checksum"))
            or not _integer(result.get("replay_start_ms"))
            or result["replay_start_ms"] != spec["start_ms"]
            or type(result.get("rows")) is not list
            or len(result["rows"]) > bound
            or type(result.get("quality")) is not dict
        ):
            raise ValueError("invalid retrospective derived SI snapshot")
        quality = result["quality"]
        counts = {
            "requested_minutes",
            "observed_minutes",
            "missing_minutes",
            "reset_count",
            "complete_bars",
        }
        if self.kind == "NES":
            counts |= {
                "auxiliary_observed_minutes",
                "auxiliary_partial_minutes",
                "auxiliary_missing_minutes",
            }
        if (
            set(quality) != counts
            or any(not _integer(value, upper=bound) for value in quality.values())
            or quality["requested_minutes"] != (spec["end_ms"] - spec["start_ms"]) // 60000
            or quality["observed_minutes"] + quality["missing_minutes"]
            != quality["requested_minutes"]
            or quality["reset_count"] > quality["observed_minutes"]
            or quality["complete_bars"] != len(result["rows"])
            or quality["complete_bars"] * spec["period_minutes"] > quality["observed_minutes"]
        ):
            raise ValueError("invalid bounded derived SI quality counters")
        if self.kind == "NES" and (
            result.get("auxiliary_source") != spec["instrument"] + ":aggTrade"
            or result.get("source_checksum_scope") != "committed_candles_trade_flow_revisions_v1"
            or sum(quality[key] for key in counts if key.startswith("auxiliary_"))
            != quality["observed_minutes"]
        ):
            raise ValueError("NES source or coverage changed")
        width = spec["period_minutes"] * 60000
        previous, segment, rows = None, 0, []
        row_quality = {}
        for row in result["rows"]:
            fields = {
                "open_time_ms",
                "close_time_ms",
                "available_at_ms",
                "segment",
                self.kind,
                "ABSMA",
                "ABSMAREL",
            }
            if self.kind == "NES":
                fields |= {
                    "input_ready",
                    "input_reason",
                    "observed_minutes",
                    "required_minutes",
                    "source_available_at_ms",
                    "auxiliary_checksum",
                }
            if type(row) is not dict or set(row) != fields:
                raise ValueError("unexpected derived SI fields")
            opening, close = row["open_time_ms"], row["close_time_ms"]
            if (
                not _integer(opening)
                or not _integer(close)
                or opening % width
                or close != opening + width
                or not spec["start_ms"] <= opening < close <= spec["end_ms"]
                or not _integer(row["available_at_ms"])
                or row["available_at_ms"] != result["produced_at_ms"]
                or not _integer(row["segment"], upper=bound)
                or previous is not None
                and (
                    opening <= previous
                    or row["segment"] < segment
                    or opening != previous + width
                    and row["segment"] == segment
                )
                or any(
                    value is not None
                    and (type(value) not in {int, float} or not math.isfinite(value))
                    for value in (row[self.kind], row["ABSMA"], row["ABSMAREL"])
                )
                or row["ABSMA"] is not None
                and row["ABSMA"] < 0
            ):
                raise ValueError("invalid derived SI row, finite value or gap")
            if self.kind == "NES":
                available = row["source_available_at_ms"]
                if (
                    type(row["input_ready"]) is not bool
                    or type(row["input_reason"]) is not str
                    or row["input_reason"]
                    not in {
                        "observed",
                        "missing",
                        "incomplete",
                        "partial",
                        "stale",
                        "unsupported",
                        "carried",
                        "missing_fields",
                        "partial_prefix",
                    }
                    or not _integer(row["observed_minutes"], upper=spec["period_minutes"])
                    or type(row["required_minutes"]) is not int
                    or row["required_minutes"] != spec["period_minutes"]
                    or not _digest(row["auxiliary_checksum"])
                    or available is not None
                    and (not _integer(available) or not opening <= available <= result["as_of_ms"])
                    or row["input_ready"]
                    and (
                        row["observed_minutes"] != row["required_minutes"]
                        or available is None
                        or available < close
                    )
                    or not row["input_ready"]
                    and any(row[key] is not None for key in (self.kind, "ABSMA", "ABSMAREL"))
                ):
                    raise ValueError("invalid NES availability, quality or held-value masking")
            rows.append((opening, row[self.kind], row["ABSMA"], row["ABSMAREL"]))
            if self.kind == "NES":
                row_quality[opening] = (
                    row["input_ready"],
                    row["input_reason"],
                    row["observed_minutes"],
                    row["required_minutes"],
                )
            else:
                row_quality[opening] = (
                    True,
                    "observed",
                    spec["period_minutes"],
                    spec["period_minutes"],
                )
            previous, segment = opening, row["segment"]
        self.metadata = {
            key: result[key]
            for key in (
                "source_checksum",
                "as_of_ms",
                "produced_at_ms",
                "quality",
                "replay_start_ms",
                "knowledge_mode",
            )
        }
        self.row_quality = row_quality
        return tuple(rows)

    def _parse_analysis(self, result):
        spec = self.params
        bound = analysis_source_minutes(spec["period_minutes"])
        echoes = set(spec) - {"start_ms", "end_ms"}
        fields = echoes | {
            "series_schema_version",
            "source",
            "auxiliary_sources",
            "source_checksum_scope",
            "replay_start_ms",
            "as_of_ms",
            "produced_at_ms",
            "knowledge_mode",
            "source_checksum",
            "rows",
            "quality",
        }
        sources = sorted(
            spec["instrument"] + suffix
            for suffix in (
                (":aggTrade", ":open_interest_history", ":long_short_ratio")
                if self.kind == "MST"
                else (":binance_depth_ws_v1",)
            )
        )
        if (
            type(result) is not dict
            or set(result) != fields
            or type(result["series_schema_version"]) is not int
            or result["series_schema_version"] != 1
            or json.dumps({key: result[key] for key in echoes}, sort_keys=True, allow_nan=False)
            != json.dumps({key: spec[key] for key in echoes}, sort_keys=True, allow_nan=False)
            or result["source"] != "database"
            or result["auxiliary_sources"] != sources
            or result["source_checksum_scope"] != "committed_market_frames_native_revisions_v1"
            or result["knowledge_mode"] != "retrospective_display"
            or not _integer(result["as_of_ms"])
            or not _integer(result["produced_at_ms"])
            or not spec["end_ms"] <= result["as_of_ms"] <= result["produced_at_ms"]
            or not _digest(result["source_checksum"])
            or result["source_checksum"] == "0" * 64
            or type(result["replay_start_ms"]) is not int
            or result["replay_start_ms"] != spec["start_ms"]
            or type(result["rows"]) is not list
            or len(result["rows"]) > bound // spec["period_minutes"]
            or type(result["quality"]) is not dict
        ):
            raise ValueError("invalid native analysis SI snapshot")
        quality = result["quality"]
        if (
            set(quality)
            != {
                "requested_minutes",
                "observed_minutes",
                "missing_minutes",
                "reset_count",
                "complete_bars",
            }
            or any(not _integer(value, upper=bound) for value in quality.values())
            or quality["requested_minutes"] != (spec["end_ms"] - spec["start_ms"]) // 60000
            or quality["observed_minutes"] + quality["missing_minutes"]
            != quality["requested_minutes"]
            or quality["reset_count"] > quality["observed_minutes"]
            or quality["complete_bars"] != len(result["rows"])
            or quality["complete_bars"] * spec["period_minutes"] > quality["observed_minutes"]
        ):
            raise ValueError("invalid analysis SI coverage")
        line_ids = (
            {"0"}
            if self.kind == "STS"
            else {str(line["line_index"]) for line in spec["definitions"]}
        )
        width = spec["period_minutes"] * 60000
        rows, row_quality, previous, segment = [], {}, None, 0
        for row in result["rows"]:
            if type(row) is not dict or set(row) != {
                "open_time_ms",
                "close_time_ms",
                "segment",
                "available_at_ms",
                "values",
                "input_quality",
                "source_available_at_ms",
                "auxiliary_checksum",
            }:
                raise ValueError("unexpected native analysis SI row fields")
            opening, close = row["open_time_ms"], row["close_time_ms"]
            available = row["source_available_at_ms"]
            if (
                not _integer(opening)
                or not _integer(close)
                or opening % width
                or close != opening + width
                or not spec["start_ms"] <= opening < close <= spec["end_ms"]
                or not _integer(row["available_at_ms"])
                or row["available_at_ms"] != result["produced_at_ms"]
                or not _integer(row["segment"], upper=bound)
                or previous is not None
                and (
                    opening <= previous
                    or row["segment"] < segment
                    or opening != previous + width
                    and row["segment"] == segment
                )
                or not _digest(row["auxiliary_checksum"])
                or row["auxiliary_checksum"] == "0" * 64
                or available is not None
                and (not _integer(available) or available > result["as_of_ms"])
                or type(row["values"]) is not dict
                or set(row["values"]) != line_ids
                or type(row["input_quality"]) is not dict
                or set(row["input_quality"]) != line_ids
            ):
                raise ValueError("invalid analysis SI time, lineage or line roster")
            values, details = {}, {}
            for line_id in sorted(line_ids):
                line, quality_line = row["values"][line_id], row["input_quality"][line_id]
                if type(line) is not dict or set(line) != set(ANALYSIS_FIELDS[self.kind]):
                    raise ValueError("invalid analysis SI public fields")
                ready = self._input_ready(quality_line)
                if (
                    any(
                        value is not None
                        and (
                            type(value) not in {int, float}
                            or not math.isfinite(value)
                            or self.kind in {"MST", "STS"}
                            and not -1 <= value <= 1
                        )
                        for value in line.values()
                    )
                    or not ready
                    and any(value is not None for value in line.values())
                ):
                    raise ValueError("invalid finite or unready analysis SI values")
                if self.kind == "WOI" and line["ABSMA"] is not None and line["ABSMA"] < 0:
                    raise ValueError("negative WOI absolute average")
                if ready and (available is None or self.kind != "MST" and available < close):
                    raise ValueError("ready analysis SI lacks actual source availability")
                values[line_id], details[line_id] = dict(line), dict(quality_line)
            rows.append((opening, values))
            row_quality[opening] = details
            previous, segment = opening, row["segment"]
        self.row_quality = row_quality
        self.metadata = {key: result[key] for key in fields - echoes - {"rows"}}
        return tuple(rows)

    def _input_ready(self, quality):
        depth_reasons = {
            "observed",
            "missing",
            "incomplete",
            "partial",
            "stale",
            "unsupported",
            "carried",
            "missing_fields",
            "partial_prefix",
        }
        if type(quality) is not dict or type(quality.get("ready")) is not bool:
            raise ValueError("invalid analysis SI readiness")
        if self.kind != "MST":
            if (
                set(quality) != {"ready", "channel", "reason"}
                or quality["channel"] != "depth"
                or type(quality["reason"]) is not str
                or quality["reason"] not in depth_reasons
                or quality["ready"] != (quality["reason"] == "observed")
            ):
                raise ValueError("invalid native depth quality")
        else:
            if (
                set(quality)
                != {
                    "ready",
                    "metric_reason",
                    "metric_age_ms",
                    "trade_flow_reason",
                    "partial_prefix",
                }
                or type(quality["partial_prefix"]) is not bool
                or quality["partial_prefix"]
                or type(quality["metric_reason"]) is not str
                or quality["metric_reason"]
                not in (depth_reasons - {"partial_prefix"}) | {"nonpositive"}
                or type(quality["trade_flow_reason"]) is not str
                or quality["trade_flow_reason"] not in depth_reasons - {"partial_prefix"}
                or quality["metric_age_ms"] is not None
                and not _integer(quality["metric_age_ms"])
                or quality["ready"]
                != (
                    quality["metric_reason"] in {"observed", "carried"}
                    and quality["trade_flow_reason"] == "observed"
                )
                or quality["ready"]
                and (
                    not _integer(
                        quality["metric_age_ms"], upper=self.params["maximum_metric_age_ms"]
                    )
                )
            ):
                raise ValueError("invalid native metric or flow quality")
        return quality["ready"]

    def missing_label(self, stamp, name=None):
        if self.state != "complete":
            return self.state
        if stamp not in self.row_quality:
            return "missing"
        if self.kind in ANALYSIS_KINDS:
            line_id = self._line_indices.get(name, "0")
            quality = self.row_quality[stamp].get(line_id)
            if quality is None:
                return "missing"
            if not quality["ready"]:
                if self.kind == "MST":
                    return f"metric:{quality['metric_reason']} flow:{quality['trade_flow_reason']}"
                return quality["reason"]
        if self.kind == "NES":
            quality = self.row_quality.get(stamp)
            if quality is None:
                return "missing"
            ready, reason, observed, required = quality
            if not ready:
                return f"{reason} [{observed}/{required}]"
        return "warmup"

    def lines(self):
        if self.kind in ANALYSIS_KINDS:
            return self._analysis_lines()
        field = self.pane.profile[self.kind.lower()]["display"]
        index = (self.kind, "ABSMA", "ABSMAREL").index(field) + 1
        key = id(self.rows), self.pane.chart._data_revision, field
        if key != self._line_key:
            values = {row[0]: row[index] for row in self.rows}
            self._lines = {field: tuple(values.get(stamp) for stamp in self.pane.chart.timestamps)}
            self._line_key = key
        return self._lines

    def _analysis_lines(self):
        config = analysis_profile(self.pane.profile, self.kind)
        field = config["display"]
        indices = (
            (("0",) if config["visible"] else ())
            if self.kind == "STS"
            else tuple(
                str(index)
                for index, line in enumerate(config["lines"])
                if line["active"] and line["visible"]
            )
        )
        key = id(self.rows), self.pane.chart._data_revision, field, indices
        if key != self._line_key:
            values = {row[0]: row[1] for row in self.rows}
            self._lines, self._line_indices = {}, {}
            for line_id in indices:
                name = (
                    f"{self.kind} {line_id} · {field}" if self.kind != "STS" else f"STS · {field}"
                )
                self._line_indices[name] = line_id
                self._lines[name] = tuple(
                    values.get(stamp, {}).get(line_id, {}).get(field)
                    for stamp in self.pane.chart.timestamps
                )
            self._line_key = key
        return self._lines

    def styles(self):
        if self.kind not in ANALYSIS_KINDS:
            return {}
        self.lines()
        config = analysis_profile(self.pane.profile, self.kind)
        return {
            name: (
                {key: config[key] for key in ("positive_color", "negative_color", "opacity")}
                if self.kind == "STS"
                else {
                    key: config["lines"][int(line_id)][key] for key in ("color", "opacity", "width")
                }
            )
            for name, line_id in self._line_indices.items()
        }

    def unit(self):
        if self.kind in ANALYSIS_KINDS:
            field = analysis_profile(self.pane.profile, self.kind)["display"]
            return "quote_asset" if self.kind == "WOI" and field != "ABSMAREL" else "dimensionless"
        if self.pane.profile[self.kind.lower()]["display"] == "ABSMAREL":
            return "dimensionless"
        return "quote_asset" if self.kind == "NES" else "signed_return_bias"

    def status(self):
        self._sync_lookup_polling()
        enabled = indicator_enabled(self.pane.profile, self.kind)
        self.label.setVisible(enabled)
        self.retry.setVisible(
            enabled and self.state in {"failed", "expired", "cancelled", "lookup_paused"}
        )
        self.retry.setText(
            ("같은 요청 조회" if self.pane.owner.language == "ko" else "Check same request")
            if self.state == "lookup_paused"
            else "다시 계산 / Recalculate"
        )
        if self.kind in ANALYSIS_KINDS:
            self._analysis_status()
            return
        message = {
            "lookup_paused": (
                "연속 조회 실패 · 자동 조회 중지 · 같은 요청을 직접 조회하세요"
                if self.pane.owner.language == "ko"
                else "Lookup paused after repeated failures. Check the same request manually."
            ),
            "origin_range": (
                f"고정 시작점 지정 / Set explicit origin; max{LIMITS[self.kind]}minutes"
            ),
            "origin_capacity": "시작점 직접 지정 / Set explicit origin",
            "complete": (
                "과거 표시 · 당시 인지 아님 / Retrospective display, not historical knowledge"
            ),
        }.get(self.state, self.state)
        if self.kind == "NES":
            message += " · source " + self.pane.selection.text() + ":aggTrade"
        self.label.setText(self.kind + " · " + message)
        self.label.setToolTip(str(self.metadata)[:4000] if self.rows else "")

    def _analysis_status(self):
        ko = self.pane.owner.language == "ko"
        captions = {
            "no_active_lines": ("활성 계산선 없음", "No active calculation lines"),
            "kind_unavailable": (
                "코어가 이 분석 지표를 지원하지 않습니다",
                "Core does not support this analysis SI",
            ),
            "origin_range": (
                "고정 시작점의 범위 초과 · 시작점을 직접 지정하세요",
                "Fixed-origin range exceeded; choose an explicit origin",
            ),
            "origin_capacity": ("시작점 직접 지정", "Set an explicit origin"),
            "lookup_paused": (
                "자동 조회 중지 · 같은 요청 조회",
                "Lookup paused; check the same request",
            ),
            "complete": (
                "과거 표시 · 당시 인지 아님",
                "Retrospective display, not historical knowledge",
            ),
            "source_unavailable": ("DB 자료 미준비", "Database source unavailable"),
            "period_unavailable": ("이 주기 미지원", "Period unavailable"),
            "api_unavailable": ("API 미지원", "API unavailable"),
            "awaiting_data": ("현재 코어의 확정봉 대기", "Awaiting current-core complete candles"),
            "invalid_configuration": ("계산 설정 확인 필요", "Check calculation settings"),
            "cancelling": ("취소 처리 중", "Cancelling"),
            "failed": (
                "계산 실패 · 범위와 자료를 확인하세요",
                "Calculation failed; check window and inputs",
            ),
        }
        message = captions.get(self.state, (self.state, self.state))[0 if ko else 1]
        if self.state == "failed" and self._failure_reason:
            message += " · " + self._failure_reason
        if self.params and self.key:
            start = display_date(self.params["start_ms"], "UTC").strftime("%Y/%m/%d %H:%M UTC")
            minutes = (self.params["end_ms"] - self.params["start_ms"]) // 60000
            message += f" · {start} · {minutes}min"
        self.label.setText(self.kind + " · " + message)
        self.label.setToolTip(str(self.metadata)[:4000] if self.rows else "")
