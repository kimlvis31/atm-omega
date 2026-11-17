"""Bounded candle-only MAIN display snapshots using accepted price producers."""

# Imports
import math
from copy import deepcopy

from omega.analysis.aggregation import INTERVAL_MINUTES
from omega.analysis.catalog import create_features
from omega.analysis.price_windows import BollingerConfiguration, WindowAverageConfiguration
from omega.domain.candle_content import CONTENT_SCOPE, candle_content_pin
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle

MINUTE = 60000
_FIELDS = ("open", "high", "low", "close", "volume")
_DEFINITION = {"analysis_code", "line_index", "nSamples", "price_precision"}


def validate_price_window_settings(specification) -> dict:
    """Validate additional settings after the service's common range validation."""
    if type(specification) is not dict:
        raise ValidationError("invalid price-window display settings")
    definitions = specification.get("definitions")
    start, end = specification.get("start_ms"), specification.get("end_ms")
    display, period = specification.get("display_start_ms"), specification.get("period_minutes")
    if (
        type(start) is not int
        or type(end) is not int
        or type(display) is not int
        or not start <= display < end
        or display % MINUTE
        or type(period) is not int
        or period not in INTERVAL_MINUTES.values()
        or type(definitions) is not list
        or not 1 <= len(definitions) <= 30
    ):
        raise ValidationError("price windows require a display range and one to thirty definitions")
    seen, slots, retained = set(), 0, 0
    for definition in definitions:
        if type(definition) is not dict:
            raise ValidationError("invalid price-window definition")
        code = definition.get("analysis_code")
        if type(code) is not str or code not in {"SMA", "WMA", "BOL"}:
            raise ValidationError("invalid price-window analysis code")
        keys = _DEFINITION | ({"MAType", "bandWidth"} if code == "BOL" else set())
        if set(definition) != keys:
            raise ValidationError("invalid price-window definition fields")
        parameters = {key: value for key, value in definition.items() if key != "analysis_code"}
        if code == "BOL":
            BollingerConfiguration(0, **parameters)
        else:
            WindowAverageConfiguration(0, analysis_code=code, **parameters)
        identity = (code, definition["line_index"])
        if identity in seen:
            raise ValidationError("duplicate price-window definition")
        seen.add(identity)
        slots += 3 if code == "BOL" else 1
        retained += definition["nSamples"] + 3
    if retained > 100000:
        raise ValidationError("price-window retained state exceeds100000entries")
    minutes = (end - start) // MINUTE
    if ((minutes + period - 1) // period) * slots > 20000:
        raise ValidationError("price-window response exceeds20000scalar slots")
    result = deepcopy(specification)
    result["definitions"].sort(key=lambda row: (row["analysis_code"], row["line_index"]))
    return result


def _graph_settings(definitions, intervalID):
    columns, lengths, configuration = [], [], {"window_averages": [], "bollinger": []}
    for definition in definitions:
        code, line = definition["analysis_code"], definition["line_index"]
        suffixes = ("BOLLOW", "BOLHIGH", "MA") if code == "BOL" else (code,)
        columns.extend(f"{intervalID}_{code}_{line}_{suffix}" for suffix in suffixes)
        lengths.append(len(suffixes))
        parameters = {"interval_id": intervalID, **definition}
        if code == "BOL":
            del parameters["analysis_code"]
        configuration["bollinger" if code == "BOL" else "window_averages"].append(parameters)
    return columns, lengths, configuration


def _validate_candle(candle, instrument, cursor, stop, previous, as_of_ms):
    if (
        not isinstance(candle, Candle)
        or candle.instrument != instrument
        or type(candle.open_time_ms) is not int
        or type(candle.close_time_ms) is not int
        or not cursor <= candle.open_time_ms < stop
        or candle.open_time_ms % MINUTE
        or candle.close_time_ms != candle.open_time_ms + MINUTE
        or candle.close_time_ms > as_of_ms
        or previous is not None
        and candle.open_time_ms <= previous.close_time_ms - MINUTE
    ):
        raise ProtocolError("price-window source returned conflicting candle identity/order")
    # Revalidate the immutable DTO at the source boundary, including corrupted
    # instances constructed without their domain validator.
    try:
        Candle(
            instrument,
            candle.open_time_ms,
            candle.close_time_ms,
            *(getattr(candle, field) for field in _FIELDS),
        )
    except (ValidationError, ArithmeticError, TypeError, ValueError) as exc:
        raise ProtocolError("price-window source returned invalid candle values") from exc


def _finite(value):
    if value is None:
        return True
    if type(value) not in {int, float}:
        return False
    try:
        return math.isfinite(value)
    except OverflowError:
        return False


def calculate_price_windows(owner, specification, instrument) -> dict:
    """Own one worker-thread source and one shared aggregation graph per segment."""
    specification = validate_price_window_settings(specification)
    period, definitions = specification["period_minutes"], specification["definitions"]
    intervalID = next(key for key, value in INTERVAL_MINUTES.items() if value == period)
    columns, lengths, configuration = _graph_settings(definitions, intervalID)
    start, end, display = (
        specification["start_ms"],
        specification["end_ms"],
        specification["display_start_ms"],
    )
    as_of_ms = owner._now()
    if end > as_of_ms // MINUTE * MINUTE:
        raise ValidationError("chart series knowledge clock moved before its target")

    def create_graph():
        return create_features(instrument, columns, configuration)

    graph, previous, resets, rows, content = create_graph(), None, 0, [], []
    source = owner.source_factory()
    try:
        for cursor in range(start, end, 512 * MINUTE):
            owner._check_stop()
            stop = min(end, cursor + 512 * MINUTE)
            candles = source.read((instrument.key,), cursor, stop, limit=512, as_of_ms=as_of_ms)
            if type(candles) is not tuple or len(candles) > (stop - cursor) // MINUTE:
                raise ProtocolError("price-window source returned an invalid bounded snapshot")
            for candle in candles:
                owner._check_stop()
                _validate_candle(candle, instrument, cursor, stop, previous, as_of_ms)
                if previous is not None and previous.close_time_ms != candle.open_time_ms:
                    graph, resets = create_graph(), resets + 1
                content.append((candle.open_time_ms, *(getattr(candle, key) for key in _FIELDS)))
                feature = graph.update(candle)
                # CLOSED comes from the common graph: a fresh partial prefix
                # cannot seed its price kernel or appear as a complete bar.
                opening = candle.close_time_ms - period * MINUTE
                if (
                    opening >= display
                    and candle.close_time_ms % (period * MINUTE) == 0
                    and feature.get(f"CLOSED_{intervalID}") is True
                ):
                    values = [feature.get(key) for key in columns]
                    if not all(_finite(value) for value in values):
                        raise ProtocolError("price-window producer returned a nonfinite value")
                    ready, offset = [], 0
                    for length in lengths:
                        ready.append(
                            all(value is not None for value in values[offset : offset + length])
                        )
                        offset += length
                    rows.append(
                        {
                            "open_time_ms": opening,
                            "close_time_ms": candle.close_time_ms,
                            "segment": resets,
                            "values": values,
                            "ready": ready,
                        }
                    )
                previous = candle
        owner._check_stop()
        replay_pin = candle_content_pin(instrument.key, start, end, content)
        display_pin = candle_content_pin(
            instrument.key, display, end, [row for row in content if row[0] >= display]
        )
    finally:
        source.close()
    produced_at_ms = owner._now()
    if produced_at_ms < as_of_ms:
        raise ValidationError("chart series completion clock precedes its knowledge cutoff")
    for row in rows:
        row["available_at_ms"] = produced_at_ms
    requested = (end - start) // MINUTE
    return {
        "instrument": instrument.key,
        "kind": "PRICE_WINDOWS",
        "period_minutes": period,
        "definitions": definitions,
        "replay_start_ms": start,
        "display_start_ms": display,
        "end_ms": end,
        "source": "database",
        "as_of_ms": as_of_ms,
        "produced_at_ms": produced_at_ms,
        "knowledge_mode": "retrospective_display",
        "source_checksum_scope": CONTENT_SCOPE,
        "source_checksum": replay_pin,
        "display_content_pin": display_pin,
        "columns": columns,
        "rows": rows,
        "quality": {
            "requested_minutes": requested,
            "observed_minutes": len(content),
            "missing_minutes": requested - len(content),
            "reset_count": resets,
            "complete_bars": len(rows),
        },
    }
