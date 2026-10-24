"""Public, credential-free minute stream boundary and strict Binance decoding."""

import json
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

    def decode(self, raw: str | bytes, *, received_at_ms: int) -> MinuteObservation | None:
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
            if candle.close_time_ms > received_at_ms or payload["E"] < k["T"]:
                raise ValueError
            return MinuteObservation(candle, "websocket", received_at_ms, text)
        except (ValueError, TypeError, KeyError, UnicodeError, AttributeError, ValidationError):
            raise ProtocolError("invalid public minute stream payload") from None


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
