"""Single-owner collection storage; ingestion does not know SQL or WebSockets."""

from dataclasses import dataclass, replace
from typing import Protocol

from omega.domain.errors import ProtocolError
from omega.domain.models import Candle, Instrument


@dataclass(frozen=True)
class MinuteObservation:
    """Canonical acceptance and optional original transport provenance.

    ``received_at_ms`` is the actual local acceptance time, never earlier than
    the closed candle. ``raw_received_at_ms`` preserves when bytes arrived;
    with a small host/exchange clock skew that transport time can precede the
    candle boundary. Neither timestamp is clamped to a fabricated close time.
    """

    candle: Candle
    source: str
    received_at_ms: int
    raw: str | None = None
    raw_source: str | None = None
    raw_received_at_ms: int | None = None


class CandleStore(Protocol):
    def get(self, instrument: Instrument, open_time_ms: int) -> Candle | None: ...

    def put(self, observation: MinuteObservation) -> bool:
        """Commit before publication; equal duplicate is false, conflict raises."""
        ...


class MinuteObservationHistory(Protocol):
    """Optional original REST capture; the ordinary Candle-only history port remains."""

    def closed_observations(
        self, instrument: Instrument, start_ms: int, end_ms: int, *, now_ms: int
    ) -> tuple[MinuteObservation, ...]:
        """Archive actual bounded response pages before returning exact row fragments."""
        ...


def accept_rest_observations(
    rows, instrument, start_ms, end_ms, received_at_ms, *, require_raw=True
):
    """Validate optional captured rows and resample only canonical acceptance time."""
    if (
        type(rows) is not tuple
        or len(rows) > 20_000
        or not isinstance(instrument, Instrument)
        or any(
            type(value) is not int or not 0 <= value <= 253_402_300_799_000
            for value in (start_ms, end_ms, received_at_ms)
        )
        or start_ms >= end_ms
        or start_ms % 60_000
        or end_ms % 60_000
    ):
        raise ProtocolError("invalid captured REST range, rows or actual acceptance time")
    result = []
    for row in rows:
        if (
            type(row) is not MinuteObservation
            or not isinstance(row.candle, Candle)
            or row.candle.instrument != instrument
            or type(row.candle.open_time_ms) is not int
            or not start_ms <= row.candle.open_time_ms < end_ms
            or type(row.candle.close_time_ms) is not int
            or not row.candle.close_time_ms <= end_ms
            or type(row.source) is not str
            or row.source not in {"rest", "vision"}
            or type(row.received_at_ms) is not int
            or not row.candle.close_time_ms <= row.received_at_ms <= received_at_ms
        ):
            raise ProtocolError("captured REST observation or original receipt is invalid")
        if row.raw is None:
            if require_raw or row.raw_source is not None or row.raw_received_at_ms is not None:
                raise ProtocolError("captured history raw provenance is incomplete")
        else:
            if (
                type(row.raw) is not str
                or not row.raw
                or row.raw_source != row.source
                or type(row.raw_received_at_ms) is not int
                or not 0 <= row.raw_received_at_ms <= row.received_at_ms
            ):
                raise ProtocolError("captured history raw provenance is invalid")
            try:
                raw_bytes = row.raw.encode("utf-8")
            except UnicodeError:
                raise ProtocolError("captured history row is not exact UTF-8 text") from None
            if len(raw_bytes) > 65_536:
                raise ProtocolError("captured history row exceeds its byte limit")
        result.append(replace(row, received_at_ms=received_at_ms))
    return tuple(result)
