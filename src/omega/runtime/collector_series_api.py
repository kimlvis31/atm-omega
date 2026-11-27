"""Resume contract for committed base-bar series with explicit trading sessions."""

import re
from contextlib import closing
from urllib.parse import parse_qs

from omega.domain.bar_series import bar_time, validate_series_id
from omega.domain.errors import ValidationError

SERIES_STREAM_PATH = "/api/collector/v2/bar-stream"
SERIES_DEFINITION_PATH = "/api/collector/v2/bar-series"
SERIES_AGGREGATE_PATH = "/api/collector/v2/bar-aggregate"
MAX_PAGE_SIZE = 128
BAR_VALUES = ("open", "high", "low", "close", "volume")


def _values(raw_query, required, optional):
    try:
        query = parse_qs(
            raw_query,
            keep_blank_values=True,
            strict_parsing=True,
            max_num_fields=len(required) + len(optional),
        )
    except ValueError:
        raise ValidationError("invalid base-bar query") from None
    if not required <= set(query) <= required | optional or any(
        len(values) != 1 or not values[0] for values in query.values()
    ):
        raise ValidationError("invalid base-bar query")
    values = {key: entries[0] for key, entries in query.items()}
    validate_series_id(values["series_id"])
    return values


def _integer(value):
    if re.fullmatch(r"[0-9]{1,15}", value) is None:
        raise ValidationError("invalid base-bar numeric query field")
    return int(value)


def read_series_definition(open_store, raw_query: str) -> dict:
    values = _values(raw_query, {"series_id"}, {"session_offset", "session_limit"})
    series_id = values["series_id"]
    offset = _integer(values.get("session_offset", "0"))
    limit = _integer(values.get("session_limit", "256"))
    if not 1 <= limit <= 512:
        raise ValidationError("invalid base-bar session page size")
    with closing(open_store()) as store:
        definition = store.load(series_id).document()
    sessions = definition.pop("sessions")
    session_count = None if sessions is None else len(sessions)
    if offset and (session_count is None or offset >= session_count):
        raise ValidationError("invalid base-bar session offset")
    next_offset = None
    if sessions is not None:
        definition["sessions"] = sessions[offset : offset + limit]
        if offset + limit < session_count:
            next_offset = offset + limit
    else:
        definition["sessions"] = None
    return {
        "version": 2,
        "series_id": series_id,
        "definition": definition,
        "session_count": session_count,
        "session_offset": offset,
        "next_session_offset": next_offset,
    }


def read_series_aggregate(open_store, raw_query: str) -> dict:
    from omega.infrastructure.bar_series_rollup import rollup_series

    keys = ("start_ms", "end_ms", "interval_ms", "as_of_ms")
    values = _values(raw_query, {"series_id", *keys}, set())
    bounds = [_integer(values[key]) for key in keys]
    with closing(open_store()) as store:
        series = store.load(values["series_id"])
        return rollup_series(store, series, *bounds)


def read_series_stream(open_store, raw_query: str) -> dict:
    values = _values(raw_query, {"series_id", "next_ms", "as_of_ms"}, {"limit"})
    series_id = values["series_id"]
    cursor = _integer(values["next_ms"])
    cutoff = _integer(values["as_of_ms"])
    limit = _integer(values.get("limit", "64"))
    if not bar_time(cursor) or not bar_time(cutoff) or not 1 <= limit <= MAX_PAGE_SIZE:
        raise ValidationError("invalid base-bar stream bounds")
    with closing(open_store()) as store:
        series = store.load(series_id)
        rows, following, state = _read_page(store, series, cursor, cutoff, limit)
    return {
        "version": 2,
        "series_id": series_id,
        "instrument": series.instrument.key,
        "feed": series.feed,
        "interval_ms": series.schedule.interval_ms,
        "calendar_version": series.calendar_version,
        "effective_from_ms": series.effective_from_ms,
        "as_of_ms": cutoff,
        "next_ms": following,
        "state": state,
        "rows": [
            {
                "open_ms": row.candle.open_time_ms,
                "close_ms": row.candle.close_time_ms,
                **{name: str(getattr(row.candle, name)) for name in BAR_VALUES},
                "source": row.source,
                "received_at_ms": row.received_at_ms,
            }
            for row in rows
        ],
    }


def _read_page(store, series, cursor, cutoff, limit):
    schedule = series.schedule
    if cursor < series.effective_from_ms or cursor % 60_000:
        raise ValidationError("cursor precedes the active series or is unaligned")
    if not schedule.contains_open(cursor):
        return (), *_calendar_cursor(schedule, cursor)
    window = schedule.contiguous_missing_range(cursor, cutoff, chunk=limit)
    if window is None:
        return (), cursor, "caught_up"
    _, end_ms = window
    rows = store.read_observations(
        series.series_id, cursor, end_ms, as_of_ms=cutoff, limit=limit, include_raw=False
    )
    contiguous = []
    for row in rows:
        if row.candle.open_time_ms != cursor:
            break
        contiguous.append(row)
        cursor = row.candle.close_time_ms
    if not schedule.contains_open(cursor):
        following, state = _calendar_cursor(schedule, cursor)
    elif schedule.contiguous_missing_range(cursor, cutoff, chunk=1) is None:
        following, state = cursor, "caught_up"
    else:
        following, state = cursor, "page_full" if len(contiguous) == limit else "waiting_for_gap"
    return contiguous, following, state


def _calendar_cursor(schedule, cursor):
    if schedule.is_terminal_cursor(cursor) or not schedule.coverage_known(cursor):
        return cursor, "calendar_unavailable"
    prior = cursor - schedule.interval_ms
    if prior >= 0 and schedule.contains_open(prior):
        return schedule.next_expected(prior), "session_closed"
    raise ValidationError("cursor must be a bar opening or a trading session close")
