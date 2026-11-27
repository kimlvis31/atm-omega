"""Versioned loopback contract for committed collector candles."""

import json
import re
import time
from urllib.parse import parse_qs

from omega.domain.errors import ValidationError
from omega.infrastructure.market_data import instrument_key
from omega.runtime.analysis_manifest import candle_values

HISTORY_PATH = "/api/collector/v1/candles"
BATCH_PATH = "/api/collector/v1/candles-batch"
STREAM_PATH = "/api/collector/v1/candle-stream"
MAX_COLLECTOR_RESPONSE_BYTES = 65_536
MAX_COLLECTOR_REQUEST_TARGET = 2_048


class CollectorResponseTooLarge(ValidationError):
    """A valid collector response cannot fit the shared byte budget."""


def _read_query(raw_query, required, optional):
    query = parse_qs(raw_query, keep_blank_values=True)
    if (
        set(query) - required - optional
        or not required <= set(query)
        or any(len(values) != 1 or not values[0] for values in query.values())
    ):
        raise ValidationError("invalid collector query fields")
    instrument = query["instrument"][0]
    if len(instrument) > 128 or instrument_key(instrument).key != instrument:
        raise ValidationError("invalid collector instrument")
    return instrument, query


def _row(observation):
    candle = observation.candle
    return {
        **candle_values(candle),
        "close_ms": candle.close_time_ms,
        "source": observation.source,
        "received_at_ms": observation.received_at_ms,
    }


def encode_collector_response(result):
    encoded = json.dumps(result, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(encoded) > MAX_COLLECTOR_RESPONSE_BYTES:
        raise CollectorResponseTooLarge("collector response exceeds byte limit")
    return encoded


def read_collector_history(source, query):
    instrument, values = _read_query(
        query, {"instrument", "start_ms", "end_ms", "as_of_ms"}, {"limit"}
    )
    try:
        start_ms = int(values["start_ms"][0])
        end_ms = int(values["end_ms"][0])
        as_of_ms = int(values["as_of_ms"][0])
        limit = int(values.get("limit", ["256"])[0])
    except ValueError:
        raise ValidationError("invalid collector history query values") from None
    if not 1 <= limit <= 256:
        raise ValidationError("collector history limit outside bounds")
    observations = source.read_observations(
        [instrument], start_ms, end_ms, limit=limit, as_of_ms=as_of_ms
    )
    result = {"version": 1, "instrument": instrument, "rows": [_row(row) for row in observations]}
    encode_collector_response(result)
    return result


def read_collector_batch(source, raw_query):
    """Read one bounded committed snapshot for several canonical instruments."""
    if len(BATCH_PATH) + 1 + len(raw_query) > MAX_COLLECTOR_REQUEST_TARGET:
        raise ValidationError("collector batch request target exceeds limit")
    try:
        query = parse_qs(raw_query, keep_blank_values=True, strict_parsing=True)
    except ValueError:
        raise ValidationError("invalid collector batch query syntax") from None
    required = {"instrument", "start_ms", "end_ms", "as_of_ms"}
    optional = {"limit", "destination_id"}
    if (
        set(query) - required - optional
        or not required <= set(query)
        or any(len(query[key]) != 1 or not query[key][0] for key in required - {"instrument"})
        or not 1 <= len(query["instrument"]) <= 32
        or any(not value for value in query["instrument"])
    ):
        raise ValidationError("invalid collector batch query fields")
    instruments = query["instrument"]
    canonical = []
    for value in instruments:
        if len(value) > 128 or instrument_key(value).key != value or value in canonical:
            raise ValidationError("collector batch instruments must be unique canonical keys")
        canonical.append(value)
    if "limit" in query and (len(query["limit"]) != 1 or not query["limit"][0]):
        raise ValidationError("collector batch limit must be a scalar")
    if "destination_id" in query and (
        len(query["destination_id"]) != 1
        or re.fullmatch(r"[0-9a-f]{64}", query["destination_id"][0]) is None
    ):
        raise ValidationError("collector batch destination ID must be a lowercase SHA-256")
    try:
        start_ms = int(query["start_ms"][0])
        end_ms = int(query["end_ms"][0])
        as_of_ms = int(query["as_of_ms"][0])
        limit = int(query.get("limit", ["256"])[0])
    except ValueError:
        raise ValidationError("invalid collector batch query values") from None
    if (
        not 0 <= start_ms < end_ms <= 253_402_300_799_000
        or start_ms % 60_000
        or end_ms % 60_000
        or not 0 <= as_of_ms <= 253_402_300_799_000
        or not 1 <= limit <= 256
    ):
        raise ValidationError("collector batch range, cutoff or limit outside bounds")
    destination_id = query.get("destination_id", [None])[0]
    if destination_id is None:
        observations = source.read_observations(
            canonical, start_ms, end_ms, limit=limit, as_of_ms=as_of_ms
        )
    else:
        observations = source.read_observations(
            canonical,
            start_ms,
            end_ms,
            limit=limit,
            as_of_ms=as_of_ms,
            destination_id=destination_id,
        )
    result = {
        "version": 1,
        "instruments": canonical,
        "rows": [_row(row) for row in observations],
    }
    if destination_id is not None:
        result["destination_id"] = destination_id
    encode_collector_response(result)
    return result


def read_collector_stream(source, query):
    from omega.runtime.collector_stream import read_committed_page

    instrument, values = _read_query(
        query, {"instrument", "next_ms"}, {"as_of_ms", "limit", "wait_ms"}
    )
    try:
        next_ms = int(values["next_ms"][0])
        cutoff = int(values["as_of_ms"][0]) if "as_of_ms" in values else None
        limit = int(values.get("limit", ["64"])[0])
        wait_ms = int(values.get("wait_ms", ["0"])[0])
    except ValueError:
        raise ValidationError("invalid collector stream query values") from None
    if not 0 <= wait_ms <= 1_000 or (cutoff is not None and wait_ms):
        raise ValidationError("invalid collector stream wait for the selected cutoff")

    deadline = time.monotonic() + wait_ms / 1_000
    while True:
        as_of_ms = time.time_ns() // 1_000_000 if cutoff is None else cutoff
        page = read_committed_page(source, instrument, next_ms, as_of_ms=as_of_ms, limit=limit)
        remaining = deadline - time.monotonic()
        if page.observations or remaining <= 0:
            break
        time.sleep(min(0.25, remaining))
    result = {
        "version": 1,
        "instrument": instrument,
        "rows": [_row(row) for row in page.observations],
        "as_of_ms": as_of_ms,
        "next_ms": page.next_ms,
        "state": page.state,
    }
    encode_collector_response(result)
    return result
