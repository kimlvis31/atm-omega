"""Optional actual REST page archive and exact kline fragments; no normalized reserialization."""

import json
import re
from decimal import Decimal
from hashlib import sha256

from omega.domain.errors import ProtocolError, StorageError
from omega.infrastructure.binance.client import (
    DecodedResponse,
    _invalid_response_constant,
    _unique_response_object,
)
from omega.infrastructure.binance.parsing import closed_candles
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.raw_archive import MAX_BODY_BYTES
from omega.ports.collection import MinuteObservation
from omega.ports.raw_data import PublicRawResponse, RawArchiveReceipt

MAX_ROW_BYTES = 65_536
MAX_TIME = 253_402_300_799_000
_SPACE = frozenset(" \t\r\n")


def _skip_space(text, cursor):
    while cursor < len(text) and text[cursor] in _SPACE:
        cursor += 1
    return cursor


def exact_kline_fragments(body, decoded, *, limit):
    """Parse row boundaries in original UTF-8 text; retain its spelling/whitespace."""
    if type(body) is not bytes or not 0 < len(body) <= MAX_BODY_BYTES:
        raise ProtocolError("REST raw page exceeds its byte limit")
    if type(limit) is not int or not 1 <= limit <= 1500:
        raise ProtocolError("REST raw page has an invalid row limit")
    if type(decoded) is not list or len(decoded) > limit:
        raise ProtocolError("REST raw page row count differs from its limit")
    decoder = json.JSONDecoder(
        parse_float=Decimal,
        parse_constant=_invalid_response_constant,
        object_pairs_hook=_unique_response_object,
    )
    try:
        text = body.decode("utf-8")
        cursor = _skip_space(text, 0)
        if cursor >= len(text) or text[cursor] != "[":
            raise ValueError("expected outer array")
        cursor = _skip_space(text, cursor + 1)
        rows = []
        if cursor < len(text) and text[cursor] == "]":
            cursor += 1
        else:
            while True:
                if len(rows) >= limit:
                    raise ValueError("too many rows")
                begin = cursor
                row, cursor = decoder.raw_decode(text, cursor)
                fragment = text[begin:cursor]
                if (
                    type(row) is not list
                    or not 7 <= len(row) <= 32
                    or type(row[0]) is not int
                    or type(row[6]) is not int
                    or any(
                        type(value) not in {str, int, Decimal, bool, type(None)} for value in row
                    )
                    or len(fragment.encode("utf-8")) > MAX_ROW_BYTES
                ):
                    raise ValueError("invalid or oversized kline row")
                rows.append((row, fragment))
                cursor = _skip_space(text, cursor)
                if cursor < len(text) and text[cursor] == "]":
                    cursor += 1
                    break
                if cursor >= len(text) or text[cursor] != ",":
                    raise ValueError("invalid row boundary")
                cursor = _skip_space(text, cursor + 1)
        if _skip_space(text, cursor) != len(text) or len(rows) != len(decoded):
            raise ValueError("page differs from decoded row count")
        for (row, _fragment), expected in zip(rows, decoded, strict=True):
            if (
                type(expected) is not list
                or len(row) != len(expected)
                or any(
                    type(value) is not type(other) or value != other
                    for value, other in zip(row, expected, strict=True)
                )
            ):
                raise ValueError("decoded values differ from original body")
        return tuple(rows)
    except (ValueError, TypeError, UnicodeError, RecursionError, OverflowError):
        raise ProtocolError("REST raw klines need a bounded exact UTF-8 page") from None


def _response(reply):
    if not isinstance(reply, DecodedResponse) or not isinstance(reply.response, HttpResponse):
        raise ProtocolError("REST raw capture needs an actual HTTP response envelope")
    response = reply.response
    if (
        type(response.status) is not int
        or response.status != 200
        or type(response.body) is not bytes
        or not 0 < len(response.body) <= MAX_BODY_BYTES
        or type(response.received_at_ms) is not int
        or not 0 <= response.received_at_ms <= MAX_TIME
    ):
        raise ProtocolError("REST raw capture needs bounded body and actual transport receipt")
    if (
        type(response.headers) is not dict
        or len(response.headers) > 128
        or any(
            type(key) is not str
            or type(value) is not str
            or not 0 < len(key) <= 256
            or len(value) > 8192
            or not key.isprintable()
            or any(char in value for char in "\r\n\0")
            for key, value in response.headers.items()
        )
    ):
        raise ProtocolError("REST raw response headers exceed public metadata bounds")
    headers, seen = [], set()
    for key, value in response.headers.items():
        name = key.lower()
        if name in {"date", "content-type", "retry-after"} or re.fullmatch(
            r"x-mbx-used-weight(?:-\d+[smhd])?", name
        ):
            if name in seen or not 1 <= len(value) <= 2048 or not value.isprintable():
                raise ProtocolError("REST raw public response header is ambiguous or invalid")
            seen.add(name)
            headers.append((name, value))
    return response, tuple(sorted(headers))


def capture_rest_page(
    reply, instrument, parameters, start_ms, end_ms, *, now_ms, accepted_at_ms, archive
):
    """Use the existing archive publication receipt; no unconditional 8MiB readback."""
    response, headers = _response(reply)
    record = PublicRawResponse(
        instrument,
        "klines_1m",
        tuple(sorted((key, str(value)) for key, value in parameters.items())),
        response.received_at_ms,
        response.body,
        headers,
    )
    try:
        receipt = archive.put(record)
    except StorageError:
        raise
    except Exception:
        raise StorageError("REST raw page archive publication failed") from None
    if (
        not isinstance(receipt, RawArchiveReceipt)
        or type(receipt.archive_id) is not str
        or re.fullmatch(r"[a-f0-9]{64}", receipt.archive_id) is None
        or receipt.body_sha256 != sha256(response.body).hexdigest()
        or type(receipt.received_at_ms) is not int
        or receipt.received_at_ms != response.received_at_ms
        or type(receipt.inserted) is not bool
    ):
        raise StorageError("REST raw page publication receipt differs from original response")
    if (
        type(accepted_at_ms) is not int
        or not 0 <= accepted_at_ms <= MAX_TIME
        or accepted_at_ms < response.received_at_ms
        or accepted_at_ms < now_ms
    ):
        raise ProtocolError("local clock moved backward while accepting REST raw page")
    rows = exact_kline_fragments(response.body, reply.data, limit=parameters["limit"])
    candles = closed_candles(instrument, reply.data, start_ms, end_ms, now_ms)
    fragments = {}
    for row, fragment in rows:
        fragments.setdefault(row[0], fragment)
    return tuple(
        MinuteObservation(
            candle,
            "rest",
            accepted_at_ms,
            fragments[candle.open_time_ms],
            "rest",
            response.received_at_ms,
        )
        for candle in candles
    )
