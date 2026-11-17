"""Native WOI/MST/STS display snapshots using existing quality-aware providers."""

import json
import math
from copy import deepcopy
from hashlib import sha256

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.catalog import create_features
from omega.domain.chart_analysis import ANALYSIS_FIELDS
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import market_frame_checksum
from omega.infrastructure.chart_analysis_frames import ChartAnalysisFrameSource

MINUTE = 60000
CHUNK_MINUTES = 512
SCOPE = "committed_market_frames_native_revisions_v1"
_GROUP = {"WOI": "order_flow", "MST": "market_sentiment", "STS": "short_term_scalar"}


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _definition(spec, iid):
    kind = spec["kind"]
    if kind == "STS":
        rows = [
            {
                "interval_id": iid,
                **{
                    key: value
                    for key, value in spec.items()
                    if key not in {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
                },
            }
        ]
        fields = {"0": {field: f"{iid}_STS_{field}" for field in ANALYSIS_FIELDS[kind]}}
    else:
        rows, fields = [], {}
        for definition in spec["definitions"]:
            line = definition["line_index"]
            row = {"interval_id": iid, **definition, "quote_precision": spec["quote_precision"]}
            if kind == "WOI":
                row["analysis_code"] = kind
                suffixes = ("WOI", "WOIABSMA", "WOIABSMAREL")
            else:
                row["maximum_metric_age_ms"] = spec["maximum_metric_age_ms"]
                suffixes = ANALYSIS_FIELDS[kind]
            rows.append(row)
            fields[str(line)] = {
                name: f"{iid}_{kind}_{line}_{suffix}"
                for name, suffix in zip(ANALYSIS_FIELDS[kind], suffixes, strict=True)
            }
    return {_GROUP[kind]: rows}, fields


def _quality(graph, kind, iid, line):
    key = f"{iid}_STS" if kind == "STS" else f"{iid}_{kind}_{line}"
    details = graph.input_quality[_GROUP[kind]]["details"][key]
    if kind != "MST":
        return deepcopy(details)
    return {
        "ready": details["ready"],
        "metric_reason": details["metric"],
        "metric_age_ms": details["metric_age_ms"],
        "trade_flow_reason": details["trade_flow"],
        "partial_prefix": details["partial_prefix"],
    }


def calculate_analysis(owner, spec, instrument):
    kind, period = spec["kind"], spec["period_minutes"]
    iid = next(key for key, value in INTERVAL_MINUTES.items() if value == period)
    configuration, fields = _definition(spec, iid)
    public = tuple(field for row in fields.values() for field in row.values())

    def create():
        return create_features(instrument, public, configuration)

    start, end, as_of = spec["start_ms"], spec["end_ms"], owner._now()
    if end > as_of // MINUTE * MINUTE:
        raise ValidationError("chart analysis target includes an unfinished minute")
    channels = ("metric", "trade_flow") if kind == "MST" else ("depth",)
    suffixes = (
        ("aggTrade", "long_short_ratio", "open_interest_history")
        if kind == "MST"
        else ("binance_depth_ws_v1",)
    )
    sources = sorted(instrument.key + ":" + suffix for suffix in suffixes)
    graph, previous, rows, observed, resets = create(), None, [], 0, 0
    digest, bar_digest, bar_open, bar_available = sha256(), sha256(), None, None
    source = ChartAnalysisFrameSource(owner.source_factory(), check_stop=owner._check_stop)
    try:
        for cursor in range(start, end, CHUNK_MINUTES * MINUTE):
            owner._check_stop()
            stop = min(end, cursor + CHUNK_MINUTES * MINUTE)
            prepared = source.read_frames(
                instrument,
                cursor,
                stop,
                as_of_ms=as_of,
                channels=channels,
                maximum_metric_age_ms=spec.get("maximum_metric_age_ms", 0),
                retrospective_until_ms=end,
                limit=(stop - cursor) // MINUTE,
            )
            for selected in prepared:
                owner._check_stop()
                frame, candle = selected.frame, selected.frame.candle
                if (
                    candle.open_time_ms % MINUTE
                    or candle.close_time_ms - candle.open_time_ms != MINUTE
                    or frame.as_of_ms != as_of
                    or frame.reference_time_ms != candle.close_time_ms
                    or tuple(value.channel for value in frame.inputs) != channels
                    or previous is not None
                    and candle.open_time_ms <= previous.open_time_ms
                ):
                    raise ProtocolError("chart analysis frame identity, clock or channels changed")
                if previous is not None and previous.close_time_ms != candle.open_time_ms:
                    graph, resets = create(), resets + 1
                opening = candle.open_time_ms // (period * MINUTE) * (period * MINUTE)
                if opening != bar_open:
                    bar_open, bar_digest, bar_available = opening, sha256(), None
                checksum = _encoded(
                    {
                        "frame": market_frame_checksum(frame),
                        "pins": [
                            {
                                "instrument": pin.instrument.key,
                                "channel": pin.observation.channel,
                                "source": pin.observation.source,
                                "open_time_ms": pin.observation.window_start_ms,
                                "revision": pin.revision,
                                "checksum": pin.checksum,
                                "previous_checksum": pin.previous_checksum,
                                "event_time_ms": pin.observation.event_time_ms,
                                "available_at_ms": pin.observation.available_at_ms,
                            }
                            for pin in selected.pins
                        ],
                    }
                )
                digest.update(checksum)
                bar_digest.update(checksum)
                if selected.pins:
                    bar_available = max(
                        bar_available or 0,
                        *(pin.observation.available_at_ms for pin in selected.pins),
                    )
                observed += 1
                output = graph.update_frame(frame)
                if output.get(f"CLOSED_{iid}") is True and opening >= start:
                    values, quality = {}, {}
                    for line, field_map in fields.items():
                        quality[line] = _quality(graph, kind, iid, line)
                        values[line] = {key: output.get(field) for key, field in field_map.items()}
                        if not quality[line]["ready"]:
                            values[line] = dict.fromkeys(field_map)
                        for key, value in values[line].items():
                            if value is not None and (
                                type(value) not in {int, float}
                                or not math.isfinite(value)
                                or kind in {"MST", "STS"}
                                and not -1 <= value <= 1
                                or kind == "WOI"
                                and key == "ABSMA"
                                and value < 0
                            ):
                                raise ProtocolError(
                                    "chart analysis producer returned an invalid value"
                                )
                    rows.append(
                        {
                            "open_time_ms": opening,
                            "close_time_ms": candle.close_time_ms,
                            "segment": resets,
                            "values": values,
                            "input_quality": quality,
                            "source_available_at_ms": bar_available,
                            "auxiliary_checksum": bar_digest.hexdigest(),
                        }
                    )
                previous = candle
    finally:
        try:
            source.close()
        except Exception as failure:
            owner._record_analysis_cleanup_failure(failure)
            raise
    owner._check_stop()
    produced = owner._now()
    if produced < as_of:
        raise ValidationError("chart analysis completion clock precedes its fixed cutoff")
    for row in rows:
        row["available_at_ms"] = produced
    return {
        "series_schema_version": 1,
        "instrument": instrument.key,
        "kind": kind,
        "period_minutes": period,
        **{
            key: deepcopy(value)
            for key, value in spec.items()
            if key not in {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
        },
        "source": "database",
        "auxiliary_sources": sources,
        "source_checksum_scope": SCOPE,
        "replay_start_ms": start,
        "as_of_ms": as_of,
        "produced_at_ms": produced,
        "knowledge_mode": "retrospective_display",
        "source_checksum": digest.hexdigest(),
        "rows": rows,
        "quality": {
            "requested_minutes": (end - start) // MINUTE,
            "observed_minutes": observed,
            "missing_minutes": (end - start) // MINUTE - observed,
            "reset_count": resets,
            "complete_bars": len(rows),
        },
    }
