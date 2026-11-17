import json

import pytest

from omega.domain.errors import ProtocolError, TransportError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.streams import StreamPlan, connect_minutes


def payload(start=0, *, closed=True, symbol="BTCUSDT"):
    return json.dumps(
        {
            "stream": f"{symbol.lower()}@kline_1m",
            "data": {
                "e": "kline",
                "E": start + 60_001,
                "s": symbol,
                "k": {
                    "s": symbol,
                    "i": "1m",
                    "t": start,
                    "T": start + 59_999,
                    "o": "100",
                    "h": "110",
                    "l": "90",
                    "c": "101",
                    "v": "2",
                    "x": closed,
                },
            },
        }
    )


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_strict_closed_minutes_and_distinct_markets(market):
    plan = StreamPlan(market, (Instrument("binance", market, "BTCUSDT"),))
    observation = plan.decode(payload(), received_at_ms=60_002)
    assert observation.candle.close_time_ms == 60_000
    assert observation.candle.instrument.market == market
    assert observation.raw == payload()
    assert plan.decode(payload(closed=False), received_at_ms=60_002) is None
    expected = (
        "stream.binance.com:443/stream"
        if market == Market.SPOT
        else "fstream.binance.com/market/stream"
    )
    assert expected in plan.url


@pytest.mark.parametrize(
    "change",
    [
        lambda p: p["data"]["k"].update(x="true"),
        lambda p: p["data"]["k"].update(i="5m"),
        lambda p: p["data"]["k"].update(s="ETHUSDT"),
        lambda p: p["data"]["k"].update(h="NaN"),
        lambda p: p["data"]["k"].update(t=True),
        lambda p: p.update(stream="ethusdt@kline_1m"),
        lambda p: p["data"].update(E=0),
    ],
)
def test_malformed_payloads_fail_closed(change):
    plan = StreamPlan(Market.SPOT, (Instrument("binance", Market.SPOT, "BTCUSDT"),))
    data = json.loads(payload())
    change(data)
    with pytest.raises(ProtocolError):
        plan.decode(json.dumps(data), received_at_ms=60_002)


def test_unsubscribed_future_oversized_and_server_shutdown():
    plan = StreamPlan(Market.SPOT, (Instrument("binance", Market.SPOT, "BTCUSDT"),))
    for raw, now in [
        (payload(symbol="ETHUSDT"), 60_002),
        (payload(), 50_000),
        ("x" * 65_537, 60_002),
        ("[]", 60_002),
    ]:
        with pytest.raises(ProtocolError):
            plan.decode(raw, received_at_ms=now)
    with pytest.raises(TransportError):
        plan.decode('{"data":{"e":"serverShutdown"}}', received_at_ms=60_002)


def test_connection_options_and_no_auth(monkeypatch):
    import websockets.asyncio.client

    captured = {}

    def fake(url, **kwargs):
        captured.update(url=url, **kwargs)
        return "connection"

    monkeypatch.setattr(websockets.asyncio.client, "connect", fake)
    plan = StreamPlan(Market.SPOT, (Instrument("binance", Market.SPOT, "BTCUSDT"),))
    assert connect_minutes(plan) == "connection"
    assert captured["proxy"] is None and captured["ping_interval"] is None
    assert captured["max_queue"] == 32 and captured["max_size"] == 65_536
    assert "additional_headers" not in captured


def test_subscription_boundaries():
    i = Instrument("binance", Market.SPOT, "BTCUSDT")
    for instruments in [
        (),
        (i, i),
        (Instrument("binance", Market.USDM, "BTCUSDT"),),
        tuple(Instrument("binance", Market.SPOT, f"X{n:04}") for n in range(129)),
    ]:
        with pytest.raises(ValidationError):
            StreamPlan(Market.SPOT, instruments)


def test_future_tolerance_is_explicit_bounded_and_does_not_change_strict_default():
    plan = StreamPlan(Market.SPOT, (Instrument("binance", Market.SPOT, "BTCUSDT"),))
    with pytest.raises(ProtocolError):
        plan.decode(payload(), received_at_ms=59_600)
    observation = plan.decode(payload(), received_at_ms=59_600, future_tolerance_ms=400)
    assert observation.candle.close_time_ms == 60_000
    assert observation.received_at_ms == 59_600  # Parsing itself never invents a receipt.
    with pytest.raises(ProtocolError):
        plan.decode(payload(), received_at_ms=57_999, future_tolerance_ms=2000)
    for invalid in (True, -1, 2001, 0.5):
        with pytest.raises(ValidationError):
            plan.decode(payload(), received_at_ms=60_000, future_tolerance_ms=invalid)
