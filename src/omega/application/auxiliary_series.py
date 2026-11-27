"""Bounded retrospective projections of stored auxiliary observations.

Display series retain actual knowledge metadata and do not calculate trading state.
Native Binance metric pairs use the Eta minute mapping.
"""

from decimal import Context, Decimal, localcontext

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary

MINUTE = 60000
MAX_MINUTES = 10000
MAX_TIME = 253_402_300_799_000
KINDS = {
    "OPENINTEREST": ("open_interest", "open_interest_history"),
    "LONGSHORTRATIO": ("long_short_ratio", "long_short_ratio"),
    "AGGTRADE": ("trade_flow", "aggTrade"),
    "DEPTH": ("depth", "binance_depth_ws_v1"),
    "FUNDING": ("funding_rate", "funding_rate"),
}
UNITS = {
    "OPENINTEREST": {
        "METRIC_OPENINTEREST": "contracts",
        "METRIC_OPENINTERESTVALUE": "quote_asset",
    },
    "LONGSHORTRATIO": {
        "METRIC_LONGSHORTRATIO": "ratio",
        "METRIC_LONGACCOUNT": "fraction",
        "METRIC_SHORTACCOUNT": "fraction",
    },
    "AGGTRADE": {
        "AGGTRADE_NOTIONALBUY": "quote_asset",
        "AGGTRADE_NOTIONALSELL": "quote_asset",
        "AGGTRADE_QUANTITYBUY": "base_asset",
        "AGGTRADE_QUANTITYSELL": "base_asset",
        "AGGTRADE_NTRADESBUY": "count",
        "AGGTRADE_NTRADESSELL": "count",
    },
    "DEPTH": {"DEPTH_BIDS": "quote_asset", "DEPTH_ASKS": "quote_asset"},
    "FUNDING": {"FUNDING_RATE": "signed_rate"},
}
_MATH = Context(prec=512)


def _instrument(key):
    try:
        provider, market, symbol = key.split(":")
        result = Instrument(provider, Market(market), symbol)
        if provider != "binance" or result.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        return result
    except (TypeError, ValueError, AttributeError):
        raise ValidationError("chart observations require a canonical Binance instrument") from None


def validate_parameters(value: dict, *, as_of_ms: int) -> dict:
    """Validate ordinary errors separately from unsupported display capabilities."""
    required = {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
    if (
        type(value) is not dict
        or not required <= value.keys()
        or value.keys() - required - {"source"}
        or type(value["kind"]) is not str
        or value["kind"] not in KINDS
    ):
        raise ValidationError("invalid chart observation parameters")
    _instrument(value["instrument"])
    if (
        type(as_of_ms) is not int
        or not 0 <= as_of_ms <= MAX_TIME
        or any(type(value[key]) is not int for key in ("start_ms", "end_ms", "period_minutes"))
        or not 0 <= value["start_ms"] < value["end_ms"] <= MAX_TIME
        or value["start_ms"] % MINUTE
        or value["end_ms"] % MINUTE
        or value["period_minutes"] <= 0
        or value["end_ms"] > as_of_ms // MINUTE * MINUTE
    ):
        raise ValidationError("chart observations require closed whole-minute bounds and period")
    if "source" in value and (
        type(value["source"]) is not str
        or not 1 <= len(value["source"]) <= 96
        or not value["source"].isprintable()
    ):
        raise ValidationError("invalid chart observation source")
    return dict(value)


def unsupported_reason(specification):
    if specification["end_ms"] - specification["start_ms"] > MAX_MINUTES * MINUTE:
        return "Requested raw range exceeds the 10000-minute chart limit."
    if specification["period_minutes"] not in INTERVAL_MINUTES.values():
        return "Only fixed periods from 1 minute through 1 day are supported."
    if _instrument(specification["instrument"]).market == Market.SPOT and specification["kind"] in {
        "OPENINTEREST",
        "LONGSHORTRATIO",
        "FUNDING",
    }:
        return "This auxiliary channel is unavailable for the spot market."
    return None


def auxiliary_ranges(specification: dict, source: str) -> tuple[AuxiliaryReadRange, ...]:
    return (
        AuxiliaryReadRange(
            _instrument(specification["instrument"]),
            KINDS[specification["kind"]][0],
            source,
            specification["start_ms"],
            specification["end_ms"],
        ),
    )


def empty_result(specification, *, source, as_of_ms, state, warning=None):
    period = specification["period_minutes"] * MINUTE
    start, end = specification["start_ms"], specification["end_ms"]
    expected = (end - 1) // period - start // period + 1
    warnings = [
        "Retrospective stored observations use current knowledge; actual availability is retained."
    ]
    if specification["kind"] == "DEPTH":
        warnings.append("Depth is the sum of stored quote-notional bins, not a raw book heatmap.")
    if warning:
        warnings.append(warning)
    return {
        "schema_version": 1,
        "scope": "retrospective_auxiliary_chart",
        **{
            key: specification[key]
            for key in ("instrument", "kind", "period_minutes", "start_ms", "end_ms")
        },
        "as_of_ms": as_of_ms,
        "source": source,
        "state": state,
        "series": {key: [] for key in UNITS[specification["kind"]]},
        "units": dict(UNITS[specification["kind"]]),
        "quality": {
            "expected_bars": expected,
            "observed_bars": 0,
            "missing_bars": expected,
            "partial_bars": 0,
        },
        "gaps": [[start, end]],
        "lineage": [],
        "warnings": warnings,
    }


def _number(value, unit):
    if unit == "count":
        valid = type(value) is int and 0 <= value <= 2**63 - 1
    else:
        valid = (
            type(value) is Decimal
            and value.is_finite()
            and len(value.as_tuple().digits) <= 128
            and abs(value.as_tuple().exponent) <= 128
            and (unit == "signed_rate" or value >= 0)
            and (unit != "fraction" or value <= 1)
        )
    if not valid:
        raise ProtocolError("stored chart observation has invalid numeric field values")
    return value


def _fields(observation, kind):
    if observation.quality != "observed" or observation.coverage != 1:
        return {}
    values = observation.mapping()
    if kind == "DEPTH":
        names = tuple(f"DEPTH_{side}{index}" for side in ("BIDS", "ASKS") for index in range(6))
        if not set(names) <= values.keys():
            return {}
        with localcontext(_MATH):
            return {
                f"DEPTH_{side}": sum(
                    (_number(values[f"DEPTH_{side}{index}"], "quote_asset") for index in range(6)),
                    Decimal(0),
                )
                for side in ("BIDS", "ASKS")
            }
    fields = {
        name: _number(values[name], unit) for name, unit in UNITS[kind].items() if name in values
    }
    if kind == "OPENINTEREST":
        return fields if "METRIC_OPENINTEREST" in fields else {}
    return fields if fields.keys() == UNITS[kind].keys() else {}


def project_auxiliary_series(
    specification: dict, observations: tuple[StoredAuxiliary, ...], *, source: str, as_of_ms: int
) -> dict:
    """Project verified minute rows without carry, future samples or mixed units."""
    spec = validate_parameters(specification, as_of_ms=as_of_ms)
    reason = unsupported_reason(spec)
    result = empty_result(
        spec,
        source=source,
        as_of_ms=as_of_ms,
        state="unsupported" if reason else "unavailable",
        warning=reason,
    )
    if reason:
        return result
    if type(observations) is not tuple or len(observations) > MAX_MINUTES:
        raise ProtocolError("stored chart observations exceed their bounded row contract")
    kind, period = spec["kind"], spec["period_minutes"] * MINUTE
    requested = auxiliary_ranges(spec, source)[0]
    groups, seen, encoded_size = {}, set(), 0
    for stored in observations:
        if not isinstance(stored, StoredAuxiliary):
            raise ProtocolError("stored chart observation requires a typed revision")
        # Revalidate the public revision contract even for an injected source.
        StoredAuxiliary(
            stored.instrument,
            stored.observation,
            stored.revision,
            stored.checksum,
            stored.previous_checksum,
        )
        observation = stored.observation
        payload = encode_observation(stored.instrument, observation)
        encoded_size += len(payload)
        if (
            encoded_size > 32 * 1024 * 1024
            or observation_checksum(payload) != stored.checksum
            or stored.instrument != requested.instrument
            or observation.channel != requested.channel
            or observation.source != source
            or not requested.start_ms <= observation.window_start_ms < requested.end_ms
            or observation.available_at_ms > as_of_ms
            or observation.window_start_ms in seen
            or observation.quality == "observed"
            and kind != "AGGTRADE"
            and not observation.window_start_ms
            <= observation.event_time_ms
            < observation.window_end_ms
        ):
            raise ProtocolError("stored chart observation conflicts with its verified query")
        seen.add(observation.window_start_ms)
        opening = observation.window_start_ms // period * period
        groups.setdefault(opening, []).append(
            (observation.event_time_ms, observation.window_start_ms, _fields(observation, kind))
        )
        result["lineage"].append(
            {
                "open_time_ms": observation.window_start_ms,
                "available_at_ms": observation.available_at_ms,
                "revision": stored.revision,
                "checksum": stored.checksum,
                "quality": observation.quality,
                "coverage": str(observation.coverage),
            }
        )
    result["lineage"].sort(key=lambda row: row["open_time_ms"])
    return _finish_series(spec, result, groups, bool(observations))


def project_eta_metric_series(spec, rows, *, source, as_of_ms):
    """Display closed 1m slots from the same paired snapshot used by SQL readers."""
    result = empty_result(spec, source=source, as_of_ms=as_of_ms, state="unavailable")
    kind, period = spec["kind"], spec["period_minutes"] * MINUTE
    groups, seen_minutes, source_blocks = {}, set(), set()
    for row in rows:
        opening, metric_time = row["open_ms"], row["metric_time_ms"]
        available = row["available_ms"]
        if (
            opening in seen_minutes
            or not spec["start_ms"] <= opening < spec["end_ms"]
            or not metric_time <= opening < metric_time + 5 * MINUTE
            or not metric_time <= available <= as_of_ms
            or row["close_ms"] != opening + MINUTE
            or row["close_ms"] > as_of_ms
        ):
            raise ProtocolError("mapped metric minute contradicts its range or availability")
        seen_minutes.add(opening)
        fields = {
            name: _number(Decimal(row["metrics"][name]), unit)
            for name, unit in UNITS[kind].items()
            if name in row["metrics"]
        }
        groups.setdefault(opening // period * period, []).append((metric_time, opening, fields))
        if metric_time not in source_blocks:
            source_blocks.add(metric_time)
            primary = "open_interest" if kind == "OPENINTEREST" else "long_short"
            result["lineage"].append(
                {
                    "open_time_ms": metric_time,
                    "available_at_ms": available,
                    "revision": row[primary + "_revision"],
                    "checksum": row[primary + "_checksum"],
                    "open_interest_checksum": row["open_interest_checksum"],
                    "long_short_checksum": row["long_short_checksum"],
                    "quality": "observed",
                    "coverage": "1",
                }
            )
    result["lineage"].sort(key=lambda row: row["open_time_ms"])
    return _finish_series(spec, result, groups, bool(rows))


def _finish_series(spec, result, groups, has_rows):
    kind, period = spec["kind"], spec["period_minutes"] * MINUTE
    gaps, observed, partial = [], 0, 0
    for opening in range(spec["start_ms"] // period * period, spec["end_ms"], period):
        rows = groups.get(opening, [])
        complete = [(event, minute, fields) for event, minute, fields in rows if fields]
        fields = {}
        if opening < spec["start_ms"] or opening + period > spec["end_ms"]:
            # The chart displays complete candle buckets. Preserve raw lineage
            # at request edges, but do not publish a point for a partial bucket.
            partial += 1
        elif kind == "AGGTRADE":
            if len(complete) == spec["period_minutes"]:
                with localcontext(_MATH):
                    fields = {
                        name: sum((values[name] for _, _, values in complete), Decimal(0))
                        for name in UNITS[kind]
                    }
            elif rows:
                partial += 1
        elif complete:
            _, _, fields = max(complete, key=lambda row: row[:2])
            partial += int(len(complete) != len(rows))
        elif rows:
            partial += 1
        if fields:
            observed += 1
            for name, value in fields.items():
                result["series"][name].append([opening, str(value)])
        else:
            left, right = max(opening, spec["start_ms"]), min(opening + period, spec["end_ms"])
            if gaps and gaps[-1][1] == left:
                gaps[-1][1] = right
            else:
                gaps.append([left, right])
    result["gaps"] = gaps
    result["quality"].update(
        observed_bars=observed,
        missing_bars=result["quality"]["expected_bars"] - observed,
        partial_bars=partial,
    )
    result["state"] = (
        "ready" if not gaps and not partial else "partial" if has_rows else "unavailable"
    )
    return result
