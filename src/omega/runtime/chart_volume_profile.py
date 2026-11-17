"""One selected closed-bar IVP snapshot from a fixed, explicitly bounded origin."""

import json
from hashlib import sha256

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.volume_profile import LegacyVolumeProfileFeatures, ProfileConfiguration
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle

MINUTE = 60000
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


def calculate_profile(owner, spec, instrument):
    """Executed only on the existing chart worker; no per-bar profile retention."""
    period = spec["period_minutes"]
    intervalID = next(key for key, value in INTERVAL_MINUTES.items() if value == period)
    config = ProfileConfiguration(intervalID, **{key: spec[key] for key in SETTINGS})

    def create():
        return LegacyVolumeProfileFeatures(instrument, (config,))

    as_of = owner._now()
    start, end = spec["start_ms"], spec["end_ms"]
    if end > as_of // MINUTE * MINUTE:
        raise ValidationError("IVP selected bar is not closed at the knowledge cutoff")
    graph, previous, selected = create(), None, None
    observed = resets = complete = 0
    digest = sha256()
    source = owner.source_factory()
    try:
        for cursor in range(start, end, 512 * MINUTE):
            owner._check_stop()
            stop = min(end, cursor + 512 * MINUTE)
            candles = source.read((instrument.key,), cursor, stop, limit=512, as_of_ms=as_of)
            if type(candles) is not tuple or len(candles) > (stop - cursor) // MINUTE:
                raise ProtocolError("invalid bounded IVP source snapshot")
            for candle in candles:
                owner._check_stop()
                if (
                    not isinstance(candle, Candle)
                    or candle.instrument != instrument
                    or not cursor <= candle.open_time_ms < stop
                    or candle.close_time_ms > as_of
                    or previous is not None
                    and candle.open_time_ms <= previous.open_time_ms
                ):
                    raise ProtocolError("IVP source identity/order conflict")
                if previous is not None and previous.close_time_ms != candle.open_time_ms:
                    graph, resets = create(), resets + 1
                values = graph.update(candle)
                digest.update(
                    json.dumps(
                        {
                            "instrument": instrument.key,
                            "open_time_ms": candle.open_time_ms,
                            **{
                                key: str(getattr(candle, key))
                                for key in ("open", "high", "low", "close", "volume")
                            },
                        },
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    ).encode()
                )
                observed += 1
                if values[f"CLOSED_{intervalID}"]:
                    complete += 1
                    if candle.close_time_ms == end:
                        selected = graph.state["raw"][intervalID]
                previous = candle
    finally:
        source.close()
    produced = owner._now()
    if produced < as_of:
        raise ValidationError("IVP completion precedes its knowledge cutoff")
    filtered = None if selected is None else selected["volumePriceLevelProfile_Filtered"]
    seeded = selected is not None and selected["volumePriceLevelProfile"] is not None
    state = (
        "missing"
        if selected is None
        else "seed"
        if not seeded
        else "zero"
        if filtered is None
        else "ready"
    )
    row = {
        "open_time_ms": end - period * MINUTE,
        "close_time_ms": end,
        "available_at_ms": produced,
        "state": state,
        "gammaFactor": config.gammaFactor,
        "betaFactor": None if selected is None else selected["betaFactor"],
        "divisionHeight": None if selected is None else selected["divisionHeight"],
        "filtered": filtered,
        "maximum": None if selected is None else selected["volumePriceLevelProfile_Filtered_Max"],
        "divisions": None if not seeded else len(selected["volumePriceLevelProfile"]),
        "boundaries": None if selected is None else selected["volumePriceLevelProfile_Boundaries"],
        "near_boundaries": None
        if selected is None
        else list(selected["volumePriceLevelProfile_NearBoundaries"]),
    }
    return {
        "instrument": instrument.key,
        "period_minutes": period,
        "kind": "IVP",
        **{key: spec[key] for key in SETTINGS},
        "replay_start_ms": start,
        "selected_close_ms": end,
        "source": "database",
        "as_of_ms": as_of,
        "produced_at_ms": produced,
        "knowledge_mode": "retrospective_display",
        "source_checksum": digest.hexdigest(),
        "rows": [row],
        "quality": {
            "requested_minutes": (end - start) // MINUTE,
            "observed_minutes": observed,
            "missing_minutes": (end - start) // MINUTE - observed,
            "reset_count": resets,
            "complete_bars": complete,
        },
    }
