"""Public, credential-free minute stream boundary and strict Binance decoding."""

import json
import re
from dataclasses import dataclass

from omega.domain.errors import ProtocolError, TransportError, ValidationError
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.ports.collection import MinuteObservation


@dataclass(frozen=True)
class StreamPlan:
    market: Market
    instruments: tuple[Instrument, ...]

    def __post_init__(self):
        if self.market not in {Market.SPOT, Market.USDM} or not 1 <= len(self.instruments) <= 128:
            raise ValidationError("one public socket supports 1..128 unique minute subscriptions")
        if len(set(self.instruments)) != len(self.instruments) or any(
            i.provider != "binance" or i.market != self.market for i in self.instruments
        ):
            raise ValidationError("mixed or duplicate stream subscriptions")

    @property
    def url(self) -> str:
        root = (
            "wss://stream.binance.com:443/stream"
            if self.market == Market.SPOT
            else "wss://fstream.binance.com/market/stream"
        )
        names = "/".join(f"{i.symbol.lower()}@kline_1m" for i in self.instruments)
        return root + "?streams=" + names

    def decode(
        self, raw: str | bytes, *, received_at_ms: int, future_tolerance_ms: int = 0
    ) -> MinuteObservation | None:
        if (
            type(future_tolerance_ms) is not int
            or not 0 <= future_tolerance_ms <= 2000
            or type(received_at_ms) is not int
            or received_at_ms < 0
        ):
            raise ValidationError("invalid stream receipt clock or future tolerance")
        if not isinstance(raw, (str, bytes)) or len(raw) > 65_536:
            raise ProtocolError("invalid public stream frame size")
        try:
            text = raw.decode("utf-8") if isinstance(raw, bytes) else raw
            data = json.loads(text)
            payload = data["data"]
            if payload.get("e") == "serverShutdown":
                raise TransportError("public stream server requested reconnect")
            k = payload["k"]
            symbol = payload["s"]
            instrument = next((i for i in self.instruments if i.symbol == symbol), None)
            if (
                instrument is None
                or k["s"] != symbol
                or k["i"] != "1m"
                or payload["e"] != "kline"
                or data["stream"] != f"{symbol.lower()}@kline_1m"
                or type(k["x"]) is not bool
            ):
                raise ValueError
            if any(type(v) is not int or v < 0 for v in (k["t"], k["T"], payload["E"])):
                raise ValueError
            if not k["x"]:
                return None  # Partial minutes cannot reach a strategy.
            candle = Candle(
                instrument,
                k["t"],
                k["T"] + 1,
                *(decimal(k[key]) for key in ("o", "h", "l", "c", "v")),
            )
            if candle.close_time_ms > received_at_ms + future_tolerance_ms or payload["E"] < k["T"]:
                raise ValueError
            return MinuteObservation(candle, "websocket", received_at_ms, text)
        except (ValueError, TypeError, KeyError, UnicodeError, AttributeError, ValidationError):
            raise ProtocolError("invalid public minute stream payload") from None

    def decode_protection(
        self,
        raw,
        *,
        received_at_ms,
        generation,
        connection,
        sequence,
        raw_received_at_ms=None,
        on_future=None,
    ):
        """Validate before deferring; only the actual acceptance clock creates a DTO."""
        from omega.domain.protection import ProtectionObservation

        if not isinstance(raw, (str, bytes)) or len(raw) > 65536:
            raise ProtocolError("invalid public protection stream frame size")
        if on_future is not None and not callable(on_future):
            raise ValidationError("future partial observer must be callable")
        try:
            value = json.loads(raw)
            payload = value["data"]
            if payload.get("e") == "serverShutdown":
                return None
            k, symbol = payload["k"], payload["s"]
            instrument = next((i for i in self.instruments if i.symbol == symbol), None)
            if (
                instrument is None
                or instrument.provider != "binance"
                or instrument.market not in {Market.SPOT, Market.USDM}
                or k["s"] != symbol
                or k["i"] != "1m"
                or payload["e"] != "kline"
                or value["stream"] != f"{symbol.lower()}@kline_1m"
                or type(k["x"]) is not bool
                or type(k["T"]) is not int
            ):
                raise ValueError
            if k["x"]:
                return None
            event, opening, closing = payload["E"], k["t"], k["T"] + 1
            if (
                type(generation) is not str
                or re.fullmatch(r"[0-9a-f]{32}", generation) is None
                or any(
                    type(v) is not int or not 0 <= v <= 2**53 - 1
                    for v in (received_at_ms, connection, sequence, event, opening, closing)
                )
                or connection < 1
                or sequence < 1
                or opening % 60000
                or closing != opening + 60000
                or not opening <= event < closing
            ):
                raise ValueError
            last, high, low = (decimal(k[key]) for key in ("c", "h", "l"))
            if (
                any(
                    v <= 0
                    or len(v.as_tuple().digits) > 18
                    or not -18 <= v.as_tuple().exponent <= 18
                    for v in (last, high, low)
                )
                or not low <= last <= high
            ):
                raise ValueError
            if raw_received_at_ms is not None and (
                type(raw_received_at_ms) is not int
                or raw_received_at_ms < 0
                or raw_received_at_ms > received_at_ms
                or not 0 < event - raw_received_at_ms <= 2000
            ):
                raise ValueError
            if event > received_at_ms:
                if event - received_at_ms > 2000:
                    raise ValueError
                if on_future is not None and raw_received_at_ms is None:
                    on_future(instrument, event)
                return None
            return ProtectionObservation(
                instrument,
                generation,
                connection,
                sequence,
                event,
                received_at_ms,
                opening,
                closing,
                last,
                high,
                low,
                source="binance_ws_kline_1m_partial",
                price_kind="kline_last",
                raw_received_at_ms=raw_received_at_ms,
            )
        except (ValueError, TypeError, KeyError, UnicodeError, AttributeError, ValidationError):
            raise ProtocolError("invalid public partial protection payload") from None


def connect_minutes(plan: StreamPlan):
    """Lazy optional dependency; library echoes server ping payloads automatically."""
    from websockets.asyncio.client import connect

    # No proxy discovery, compression amplification, auth, redirects or app-level pings.
    # URL is constructed solely from validated instruments and fixed public origins.
    return connect(
        plan.url,
        proxy=None,
        compression=None,
        ping_interval=None,
        open_timeout=10,
        close_timeout=5,
        max_size=65_536,
        max_queue=32,
    )
