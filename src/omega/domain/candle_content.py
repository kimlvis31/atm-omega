"""Exact bounded OHLCV content identity, independent of display floats and DB revisions."""

import json
import re
from collections.abc import Iterable
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from unicodedata import category

from omega.domain.errors import ValidationError

CONTENT_SCOPE = "omega_chart_candle_content_v1"
_MINUTE = 60_000
_MAX_TIME = 253_402_300_799_000
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?\Z")


def _decimal_text(value: Decimal | int | str) -> str:
    if type(value) is int:
        if value.bit_length() > 426:
            raise ValidationError("candle content integer exceeds the exact decimal budget")
        number = Decimal(value)
    elif type(value) is str:
        if not 0 < len(value) <= 512 or _DECIMAL_TEXT.fullmatch(value) is None:
            raise ValidationError("candle content requires bounded decimal strings")
        try:
            number = Decimal(value)
        except InvalidOperation:
            raise ValidationError("invalid candle content decimal") from None
    elif type(value) is Decimal:
        number = value
    else:
        raise ValidationError("candle content requires exact numbers, not display floats")
    if not number.is_finite():
        raise ValidationError("candle content decimal must be finite")
    parts = number.as_tuple()
    if len(parts.digits) > 128 or not -128 <= parts.exponent <= 128:
        raise ValidationError("candle content decimal exceeds the digit or exponent budget")
    if number.is_zero():
        return "0"
    # Fixed-point formatting is exact and ignores Decimal's arithmetic context.
    result = format(number, "f")
    if "." in result:
        result = result.rstrip("0").rstrip(".")
    if len(result) > 256:
        raise ValidationError("candle content canonical decimal exceeds the output budget")
    return result


def candle_content_pin(
    instrument_key: str,
    start_ms: int,
    end_ms: int,
    rows: Iterable[
        tuple[
            int,
            Decimal | int | str,
            Decimal | int | str,
            Decimal | int | str,
            Decimal | int | str,
            Decimal | int | str,
        ]
    ],
) -> str:
    """Hash an exact half-open minute range, including absent observations through its scope.

    Callers validate candle business geometry. This identifies content, not a
    transaction, receipt, database revision or historical availability time.
    """
    if (
        type(instrument_key) is not str
        or not 0 < len(instrument_key) <= 256
        or any(category(character) == "Cc" for character in instrument_key)
        or type(start_ms) is not int
        or type(end_ms) is not int
        or not 0 <= start_ms < end_ms <= _MAX_TIME
        or start_ms % _MINUTE
        or end_ms % _MINUTE
        or end_ms - start_ms > 10_000 * _MINUTE
    ):
        raise ValidationError("candle content requires a bounded instrument and minute range")
    canonical, previous = [], None
    for index, row in enumerate(rows):
        if index >= 10_000 or type(row) not in {tuple, list} or len(row) != 6:
            raise ValidationError("candle content rows exceed the bounded OHLCV shape")
        timestamp = row[0]
        if (
            type(timestamp) is not int
            or not start_ms <= timestamp < end_ms
            or timestamp % _MINUTE
            or previous is not None
            and timestamp <= previous
        ):
            raise ValidationError("candle content timestamps must be ordered unique scoped minutes")
        canonical.append((timestamp, *(_decimal_text(value) for value in row[1:])))
        previous = timestamp
    payload = {
        "scope": CONTENT_SCOPE,
        "instrument": instrument_key,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "rows": canonical,
    }
    encoded = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return sha256(encoded).hexdigest()
