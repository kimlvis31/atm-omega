"""Immutable selected frames passed into paper transactions, without storage IO."""

import json
import re
from dataclasses import dataclass

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame


def input_receipt(value):
    """Validate a small opaque durable reference; runtime verifies its actual data."""
    if (
        type(value) is not dict
        or set(value) != {"version", "binding", "policy", "head", "frame_digest", "end_ms"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or any(
            type(value[key]) is not str or re.fullmatch(r"[a-f0-9]{64}", value[key]) is None
            for key in ("binding", "frame_digest")
        )
        or type(value["end_ms"]) is not int
        or not 60000 <= value["end_ms"] <= 253_402_300_799_000
        or value["end_ms"] % 60000
        or type(value["policy"]) is not dict
        or type(value["head"]) is not dict
        or len(json.dumps(value, allow_nan=False).encode()) > 4096
    ):
        raise ValidationError("invalid paper selected-input receipt")
    return value


@dataclass(frozen=True, slots=True)
class PaperInputBatch:
    frames: tuple[MarketFeatureFrame, ...]
    receipt_json: str | None
    reason: str = ""

    def __post_init__(self):
        if (
            type(self.frames) is not tuple
            or not 1 <= len(self.frames) <= 512
            or any(not isinstance(frame, MarketFeatureFrame) for frame in self.frames)
            or type(self.reason) is not str
            or len(self.reason) > 96
            or self.reason
            and re.fullmatch(r"[a-z][a-z0-9_]*", self.reason) is None
        ):
            raise ValidationError("invalid immutable paper input batch")
        instrument = self.frames[0].candle.instrument
        cursor, as_of = self.frames[0].candle.open_time_ms, 0
        for frame in self.frames:
            if (
                frame.candle.instrument != instrument
                or frame.candle.open_time_ms != cursor
                or frame.as_of_ms < as_of
            ):
                raise ValidationError("paper selected frames must be contiguous and ordered")
            cursor, as_of = frame.candle.close_time_ms, frame.as_of_ms
        if self.receipt_json is None:
            if not self.reason:
                raise ValidationError("unrecorded paper inputs require an explicit failure reason")
        else:
            if type(self.receipt_json) is not str or len(self.receipt_json.encode()) > 4096:
                raise ValidationError("paper input receipt exceeds its bound")
            try:
                receipt = input_receipt(json.loads(self.receipt_json))
            except (ValueError, TypeError, RecursionError):
                raise ValidationError("invalid serialized paper input receipt") from None
            if receipt["end_ms"] != cursor:
                raise ValidationError("paper receipt does not match the selected cursor")

    @property
    def receipt(self):
        return None if self.receipt_json is None else json.loads(self.receipt_json)
