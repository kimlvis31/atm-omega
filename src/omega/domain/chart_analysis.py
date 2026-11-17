"""Pure bounded chart-analysis requests; no calculation, storage or GUI imports."""

import math
from copy import deepcopy

from omega.domain.errors import ValidationError
from omega.domain.models import Instrument, Market

ANALYSIS_KINDS = ("WOI", "MST", "STS")
ANALYSIS_FIELDS = {
    "WOI": ("WOI", "ABSMA", "ABSMAREL"),
    "MST": ("MST", "PRICESTR", "OISTR", "LSSTR", "TAKERSTR", "MSTBASE"),
    "STS": ("S", "M", "F", "D", "W"),
}
ANALYSIS_PERIODS = (1, 3, 5, 15, 30, 60, 120, 240, 360, 480, 720, 1440)
MAX_TIME = 253_402_300_799_000
MINUTE = 60000
_COMMON = {"instrument", "kind", "start_ms", "end_ms", "period_minutes"}
_WINDOWS = (
    "MMACDFast",
    "MMACDSlow",
    "MMACDSignal",
    "DMIxADXNSamples",
    "MFINSamples",
    "WOINSamples",
    "WOIBaseNSamples",
)
_WEIGHTS = ("WMMACD", "WMFI", "WDMIxADX", "WWOI")
_SCALAR = {*_WINDOWS, *_WEIGHTS, "OSCScale", "price_precision", "quote_precision"}


def _integer(value, lower, upper):
    return type(value) is int and lower <= value <= upper


def analysis_source_minutes(period_minutes):
    if type(period_minutes) is not int or period_minutes not in ANALYSIS_PERIODS:
        raise ValidationError("unsupported chart analysis period")
    return max(1440, 128 * period_minutes)


def analysis_parameters(kind, mapping):
    """Validate a full explicit request and return independently owned canonical JSON."""
    if type(kind) is not str or kind not in ANALYSIS_KINDS or type(mapping) is not dict:
        raise ValidationError("invalid chart analysis kind or request")
    additional = (
        _SCALAR
        if kind == "STS"
        else {"definitions", "quote_precision"}
        | ({"maximum_metric_age_ms"} if kind == "MST" else set())
    )
    if set(mapping) != _COMMON | additional or mapping.get("kind") != kind:
        raise ValidationError("chart analysis needs its exact kind and parameter keys")
    try:
        provider, market, symbol = mapping["instrument"].split(":")
        instrument = Instrument(provider, Market(market), symbol)
        if provider != "binance" or instrument.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        if instrument.key != mapping["instrument"]:
            raise ValueError
    except (AttributeError, TypeError, ValueError):
        raise ValidationError("chart analysis requires a canonical Binance instrument") from None
    start, end, period = (mapping[key] for key in ("start_ms", "end_ms", "period_minutes"))
    bound = analysis_source_minutes(period)
    if (
        not _integer(start, 0, MAX_TIME)
        or not _integer(end, 0, MAX_TIME)
        or start >= end
        or start % MINUTE
        or end % MINUTE
        or (end - start) // MINUTE > bound
    ):
        raise ValidationError("chart analysis replay exceeds its explicit source-minute window")
    if not _integer(mapping["quote_precision"], 0, 16):
        raise ValidationError("invalid chart analysis quote precision")
    result = deepcopy(mapping)
    if kind != "STS":
        definitions = mapping["definitions"]
        if type(definitions) is not list or not 1 <= len(definitions) <= 10:
            raise ValidationError("chart analysis needs one to ten active lines")
        seen, samples = set(), 0
        for row in definitions:
            if (
                type(row) is not dict
                or set(row) != {"line_index", "nSamples"}
                or not _integer(row["line_index"], 0, 9)
                or not _integer(row["nSamples"], 2, 100000 if kind == "WOI" else 20000)
                or row["line_index"] in seen
            ):
                raise ValidationError("invalid or duplicate chart analysis line")
            seen.add(row["line_index"])
            samples += row["nSamples"]
        if samples * (5 if kind == "MST" else 1) > 100000:
            raise ValidationError("chart analysis retained sample capacity exceeded")
        if kind == "MST" and not _integer(mapping["maximum_metric_age_ms"], 0, 86400000):
            raise ValidationError("invalid chart analysis metric freshness")
        result["definitions"].sort(key=lambda row: row["line_index"])
    else:
        if (
            any(not _integer(mapping[key], 2, 20000) for key in _WINDOWS)
            or mapping["MMACDFast"] >= mapping["MMACDSlow"]
            or not _integer(mapping["price_precision"], 0, 16)
            or sum(
                mapping[key]
                for key in ("MMACDSlow", "MFINSamples", "DMIxADXNSamples", "WOINSamples")
            )
            > 100000
        ):
            raise ValidationError("invalid bounded chart scalar windows or precision")
        for key in ("OSCScale", *_WEIGHTS):
            value = mapping[key]
            if type(value) not in {int, float}:
                raise ValidationError("chart scalar weights and scale must be finite numbers")
            try:
                number = float(value)
            except (OverflowError, ValueError):
                raise ValidationError("chart scalar weight exceeds finite arithmetic") from None
            if not math.isfinite(number) or number < 0:
                raise ValidationError("chart scalar weights and scale must be finite numbers")
            result[key] = number
        total = sum(result[key] for key in _WEIGHTS)
        if result["OSCScale"] <= 0 or not math.isfinite(total) or total <= 0:
            raise ValidationError("chart scalar needs positive scale and weight total")
    return result
