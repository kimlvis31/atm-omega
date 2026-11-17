"""Bounded canonical-source NES display snapshots; no collector or financial state."""

import json
import math
from hashlib import sha256

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.catalog import create_features
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import market_frame_checksum
from omega.ports.analysis_inputs import PreparedMarketFrame

MINUTE = 60000
CHUNK_MINUTES = 64
SCOPE = "committed_candles_trade_flow_revisions_v1"


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _checked(prepared, instrument, start, end, as_of, source):
    if not isinstance(prepared, PreparedMarketFrame):
        raise ProtocolError("NES needs typed committed market frames")
    frame, pins = prepared.frame, prepared.pins
    candle = frame.candle
    observation = frame.input("trade_flow")
    if (
        candle.instrument != instrument
        or not start <= candle.open_time_ms < end
        or frame.as_of_ms != as_of
        or not frame.retrospective
        or frame.reference_time_ms != candle.close_time_ms
        or len(frame.inputs) != 1
        or observation is None
        or observation.source != source
        or len(pins) > 1
        or any(pin.instrument != instrument or pin.observation != observation for pin in pins)
        or observation.usable_at(as_of)
        and len(pins) != 1
        or not pins
        and (observation.quality != "missing" or observation.values or observation.coverage != 0)
    ):
        raise ProtocolError("NES frame source, cutoff or native revision pin conflicts")
    return frame, observation


def calculate_nes(owner, spec, instrument):
    period = spec["period_minutes"]
    intervalID = next(key for key, value in INTERVAL_MINUTES.items() if value == period)
    public = tuple(f"{intervalID}_NES_0_{suffix}" for suffix in ("NES", "NESABSMA", "NESABSMAREL"))
    configuration = {
        "order_flow": [
            {
                "interval_id": intervalID,
                "analysis_code": "NES",
                "line_index": 0,
                "nSamples": spec["nSamples"],
                "quote_precision": spec["quote_precision"],
            }
        ]
    }

    def create():
        return create_features(instrument, public, configuration)

    graph, previous, rows = create(), None, []
    start, end, as_of = spec["start_ms"], spec["end_ms"], owner._now()
    if end > as_of // MINUTE * MINUTE:
        raise ValidationError("NES target includes an unfinished minute")
    canonical = instrument.key + ":aggTrade"
    observed = resets = auxiliary_observed = auxiliary_partial = 0
    digest, bar_digest, bar_open, bar_observed, bar_available = sha256(), sha256(), None, 0, None
    source = owner.source_factory()
    try:
        if not callable(getattr(source, "read_frames", None)):
            raise ProtocolError("NES source does not support committed auxiliary frames")
        for cursor in range(start, end, CHUNK_MINUTES * MINUTE):
            owner._check_stop()
            stop = min(end, cursor + CHUNK_MINUTES * MINUTE)
            frames = source.read_frames(
                instrument,
                cursor,
                stop,
                as_of_ms=as_of,
                channels=("trade_flow",),
                maximum_metric_age_ms=0,
                retrospective_until_ms=end,
                limit=(stop - cursor) // MINUTE,
            )
            if type(frames) is not tuple or len(frames) > (stop - cursor) // MINUTE:
                raise ProtocolError("NES source exceeds its bounded chunk")
            for prepared in frames:
                owner._check_stop()
                frame, observation = _checked(prepared, instrument, cursor, stop, as_of, canonical)
                candle = frame.candle
                if previous is not None and candle.open_time_ms <= previous.open_time_ms:
                    raise ProtocolError("NES source returned duplicate or unordered candles")
                if previous is not None and previous.close_time_ms != candle.open_time_ms:
                    graph, resets = create(), resets + 1
                opening = candle.open_time_ms // (period * MINUTE) * (period * MINUTE)
                if opening != bar_open:
                    bar_open, bar_digest, bar_observed, bar_available = opening, sha256(), 0, None
                checksum = _encoded(
                    {
                        "frame": market_frame_checksum(frame),
                        "pins": [
                            {
                                "revision": pin.revision,
                                "checksum": pin.checksum,
                                "source": pin.observation.source,
                            }
                            for pin in prepared.pins
                        ],
                    }
                )
                digest.update(checksum)
                bar_digest.update(checksum)
                usable = observation.usable_at(as_of)
                observed += 1
                auxiliary_observed += int(usable)
                auxiliary_partial += int(not usable and observation.coverage > 0)
                bar_observed += int(usable)
                if prepared.pins:
                    bar_available = max(bar_available or 0, observation.available_at_ms)
                result = graph.update_frame(frame)
                if result.get(f"CLOSED_{intervalID}") is True and opening >= start:
                    values = [result.get(key) for key in public]
                    if any(
                        value is not None
                        and (type(value) not in {int, float} or not math.isfinite(value))
                        for value in values
                    ):
                        raise ProtocolError("NES producer returned a nonfinite value")
                    quality = graph.input_quality["order_flow"]["details"][f"{intervalID}_NES_0"]
                    rows.append(
                        {
                            "open_time_ms": opening,
                            "close_time_ms": candle.close_time_ms,
                            "segment": resets,
                            **dict(zip(("NES", "ABSMA", "ABSMAREL"), values, strict=True)),
                            "input_ready": quality["ready"],
                            "input_reason": quality["reason"],
                            "observed_minutes": bar_observed,
                            "required_minutes": period,
                            "source_available_at_ms": bar_available,
                            "auxiliary_checksum": bar_digest.hexdigest(),
                        }
                    )
                previous = candle
    finally:
        source.close()
    produced = owner._now()
    if produced < as_of:
        raise ValidationError("NES completion clock precedes its fixed input cutoff")
    for row in rows:
        row["available_at_ms"] = produced
    return {
        "instrument": instrument.key,
        "kind": "NES",
        "period_minutes": period,
        "nSamples": spec["nSamples"],
        "quote_precision": spec["quote_precision"],
        "source": "database",
        "auxiliary_source": canonical,
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
            "auxiliary_observed_minutes": auxiliary_observed,
            "auxiliary_partial_minutes": auxiliary_partial,
            "auxiliary_missing_minutes": observed - auxiliary_observed - auxiliary_partial,
        },
    }
