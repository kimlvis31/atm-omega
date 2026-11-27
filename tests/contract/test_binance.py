import hashlib
import hmac
import json
from decimal import Decimal as D
from urllib.parse import parse_qs, urlsplit

import pytest

from omega.domain.errors import (
    BrokerRejected,
    OrderOutcomeUnknown,
    ProtocolError,
    RateLimited,
    TradingBlocked,
    TransportError,
    ValidationError,
)
from omega.domain.models import Instrument, Market, OrderKind, OrderRequest, Side
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.market import SymbolRules
from omega.infrastructure.binance.parsing import trade_fill
from omega.infrastructure.binance.spot import SpotAdapter
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.binance.usdm import UsdmAdapter


class Transport:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.requests = []

    def send(self, request):
        self.requests.append(request)
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        if isinstance(value, HttpResponse):
            return value
        return HttpResponse(200, json.dumps(value).encode())


def client(transport, market=Market.SPOT, *, enabled=True, budget=True):
    return BinanceClient(
        "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com",
        transport,
        credentials=Credentials("test-key", "test-secret"),
        enable_trading=enabled,
        budget=RequestBudget((Window("ip", 10_000, 60), Window("orders", 100, 60)))
        if budget
        else None,
        ip_scope="ip",
        order_scope="orders",
        clock_ms=lambda: 120_000,
    )


def info(market):
    return {
        "symbols": [
            {
                "symbol": "BTCUSDT",
                "baseAsset": "BTC",
                "quoteAsset": "USDT",
                "status": "TRADING",
                "contractType": "PERPETUAL",
                "filters": [
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.001",
                        "maxQty": "1000",
                        "stepSize": "0.001",
                    },
                    {
                        "filterType": "PRICE_FILTER",
                        "minPrice": "0.01",
                        "maxPrice": "1000000",
                        "tickSize": "0.01",
                    },
                ],
            }
        ]
    }


def observation(status="NEW", market=Market.SPOT):
    return {
        "symbol": "BTCUSDT",
        "clientOrderId": "omega-1",
        "orderId": 7,
        "status": status,
        "executedQty": "0",
        "cummulativeQuoteQty": "0",
        "cumQuote": "0",
    }


def order(market=Market.SPOT, *, reduce=False):
    return OrderRequest(
        Instrument("binance", market, "BTCUSDT"),
        "omega-1",
        Side.BUY,
        OrderKind.LIMIT,
        D("0.01"),
        D("10000"),
        reduce,
    )


def test_hmac_signs_exact_transmitted_bytes_and_secrets_are_redacted():
    transport = Transport({"ok": True})
    c = client(transport)
    c.request("GET", "/api/v3/account", {"symbol": "BTCUSDT"}, signed=True)
    request = transport.requests[0]
    query = urlsplit(request.url).query
    wire, signature = query.rsplit("&signature=", 1)
    assert signature == hmac.new(b"test-secret", wire.encode(), hashlib.sha256).hexdigest()
    assert parse_qs(wire)["timestamp"] == ["120000"]
    assert "test-key" not in repr(request) and "signature" not in repr(request)
    assert "test-secret" not in repr(c.credentials)


@pytest.mark.parametrize(
    "host",
    [
        "http://api.binance.com",
        "https://evil.example",
        "https://api.binance.com@evil.example",
        "https://api.binance.com/private",
    ],
)
def test_private_origin_is_not_configurable_to_arbitrary_hosts(host):
    with pytest.raises(ValidationError):
        BinanceClient(host, Transport())


@pytest.mark.parametrize("enabled,budget", [(False, True), (True, False)])
def test_writes_require_explicit_enable_and_budget(enabled, budget):
    transport = Transport()
    with pytest.raises(TradingBlocked):
        client(transport, enabled=enabled, budget=budget).request("POST", "/api/v3/order")
    assert not transport.requests


@pytest.mark.parametrize(
    "response",
    [
        TransportError("timeout"),
        HttpResponse(503, b'{"code":-1000}'),
        HttpResponse(200, b"broken"),
        HttpResponse(400, b'{"code":-1007}'),
    ],
)
def test_uncertain_submission_is_never_retried(response):
    transport = Transport(response)
    with pytest.raises(OrderOutcomeUnknown):
        client(transport).request("POST", "/api/v3/order", signed=True, order=True)
    assert len(transport.requests) == 1


def test_rejection_does_not_expose_provider_echoed_credentials():
    transport = Transport(HttpResponse(400, b'{"code":-2010,"msg":"test-secret"}'))
    with pytest.raises(BrokerRejected) as error:
        client(transport).request("POST", "/api/v3/order", signed=True)
    assert error.value.code == -2010
    assert "test-secret" not in str(error.value)


@pytest.mark.parametrize("status", [429, 418])
def test_rate_limit_cooldown_shared_by_clients(status):
    now = [120.0]
    budget = RequestBudget((Window("binance-ip", 600, 60),), clock=lambda: now[0])
    spot_transport = Transport(HttpResponse(status, b"<html>limited</html>", {"Retry-After": "30"}))
    futures_transport = Transport({"serverTime": 150_000})
    spot = BinanceClient("https://api.binance.com", spot_transport, budget=budget)
    futures = BinanceClient("https://fapi.binance.com", futures_transport, budget=budget)

    with pytest.raises(RateLimited) as provider:
        spot.request("GET", "/api/v3/time")
    assert provider.value.retry_after == 30
    now[0] = 149
    with pytest.raises(RateLimited) as local:
        futures.request("GET", "/fapi/v1/time")
    assert local.value.retry_after == 1
    assert len(spot_transport.requests) == 1 and futures_transport.requests == []
    now[0] = 150
    assert futures.request("GET", "/fapi/v1/time") == {"serverTime": 150_000}


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_spot_and_usdm_submit_normalized_limit_orders(market):
    responses = [info(market)]
    if market == Market.USDM:
        responses += [{"dualSidePosition": False}, {"multiAssetsMargin": False}]
    responses += [observation(market=market)]
    transport = Transport(*responses)
    adapter = (
        SpotAdapter("account", client(transport))
        if market == Market.SPOT
        else UsdmAdapter("account", client(transport, market))
    )
    adapter.refresh_symbols()
    if market == Market.USDM:
        adapter.verify_one_way()
    result = adapter.submit(order(market))
    assert result.provider_order_id == "7"
    params = parse_qs(transport.requests[-1].body.decode())
    assert params["quantity"] == ["0.01"] and params["newClientOrderId"] == ["omega-1"]
    assert "stopPrice" not in params and "closePosition" not in params
    if market == Market.USDM:
        assert params["positionSide"] == ["BOTH"]
    else:
        assert "positionSide" not in params and "reduceOnly" not in params


def test_usdm_does_not_change_hedge_mode_implicitly():
    transport = Transport({"dualSidePosition": True}, {"multiAssetsMargin": False})
    adapter = UsdmAdapter("account", client(transport, Market.USDM))
    with pytest.raises(TradingBlocked):
        adapter.verify_one_way()
    assert all(r.method == "GET" for r in transport.requests)


def test_usdm_requires_actual_mode_verification():
    adapter = UsdmAdapter("account", client(Transport(info(Market.USDM)), Market.USDM))
    adapter.refresh_symbols()
    with pytest.raises(TradingBlocked):
        adapter.submit(order(Market.USDM))


def test_spot_rejects_futures_reduce_only_flag():
    adapter = SpotAdapter("account", client(Transport()))
    with pytest.raises(TradingBlocked):
        adapter.submit(order(reduce=True))


def test_candles_exclude_open_minute_and_keep_gaps():
    def row(t):
        return [t, "100", "110", "90", "105", "1", t + 59_999]

    transport = Transport([row(0), row(120_000)])
    adapter = SpotAdapter("account", client(transport))
    candles = adapter.market_data.closed_minutes(order().instrument, 0, 180_000, now_ms=150_000)
    assert [c.open_time_ms for c in candles] == [0]
    assert candles[0].close_time_ms == 60_000
    params = parse_qs(urlsplit(transport.requests[0].url).query)
    assert params["interval"] == ["1m"] and params["endTime"] == ["119999"]


def test_trade_ids_include_account_and_market_scope():
    row = {
        "symbol": "BTCUSDT",
        "id": 1,
        "orderId": 7,
        "isBuyer": True,
        "qty": "1",
        "price": "100",
        "commission": "0.1",
        "commissionAsset": "BNB",
        "time": 100,
    }
    a = trade_fill("A", order().instrument, row)
    b = trade_fill("B", order().instrument, row)
    c = trade_fill("A", order(Market.USDM).instrument, {**row, "side": "BUY"})
    assert len({a.key, b.key, c.key}) == 3
    assert a.fee_asset == "BNB" and a.fee == D("0.1")


def test_order_schema_failure_after_post_is_unknown():
    transport = Transport(info(Market.SPOT), {"orderId": 7})
    adapter = SpotAdapter("account", client(transport))
    adapter.refresh_symbols()
    with pytest.raises(OrderOutcomeUnknown):
        adapter.submit(order())


def test_unverified_mode_is_blocked_even_near_unix_epoch():
    c = client(Transport(), Market.USDM)
    c.clock_ms = lambda: 0
    adapter = UsdmAdapter("account", c)
    with pytest.raises(TradingBlocked):
        adapter.submit(order(Market.USDM))


def test_ack_with_another_client_id_is_not_accepted():
    payload = {**observation(), "clientOrderId": "someone-else"}
    transport = Transport(info(Market.SPOT), payload)
    adapter = SpotAdapter("account", client(transport))
    adapter.refresh_symbols()
    with pytest.raises(OrderOutcomeUnknown):
        adapter.submit(order())


@pytest.mark.parametrize("mode", ["CROSSED", "ISOLATED"])
def test_futures_margin_mode_is_an_explicit_separate_operation(mode):
    transport = Transport({"dualSidePosition": False}, {"multiAssetsMargin": False}, {"code": 200})
    adapter = UsdmAdapter("account", client(transport, Market.USDM))
    adapter.verify_one_way()
    adapter.set_margin_mode(order(Market.USDM).instrument, mode)
    params = parse_qs(transport.requests[-1].body.decode())
    assert params["marginType"] == [mode]
    assert urlsplit(transport.requests[-1].url).path == "/fapi/v1/marginType"


def test_shared_budget_is_atomic_across_windows_and_does_not_multiply_by_accounts():
    now = [0.0]
    budget = RequestBudget((Window("ip", 3, 60), Window("account", 1, 10)), clock=lambda: now[0])
    budget.reserve({"ip": 2, "account": 1})
    with pytest.raises(RateLimited):
        budget.reserve({"ip": 1, "account": 1})
    budget.reserve({"ip": 1})  # The failed multi-scope admission consumed nothing.
    with pytest.raises(RateLimited):
        budget.reserve({"ip": 1})
    now[0] = 60
    budget.reserve({"ip": 3, "account": 1})


def test_account_data_is_normalized_without_a_provider_dict_in_the_core():
    payload = {"canTrade": True, "balances": [{"asset": "BTC", "free": "1.1", "locked": "0.2"}]}
    snapshot = SpotAdapter("A", client(Transport(payload))).account()
    assert snapshot.account_id == "A" and snapshot.market == Market.SPOT
    assert snapshot.balances[0].wallet == D("1.3")
    assert snapshot.balances[0].available == D("1.1")
    assert snapshot.balances[0].margin_balance is None


def test_futures_snapshot_keeps_signed_position_and_wallet_distinct_from_margin():
    payload = {
        "canTrade": True,
        "assets": [{"asset": "USDT", "walletBalance": "1000", "availableBalance": "750"}],
        "positions": [
            {
                "symbol": "BTCUSDT",
                "positionSide": "BOTH",
                "positionAmt": "-0.01",
                "entryPrice": "50000",
                "unrealizedProfit": "-5",
            }
        ],
    }
    snapshot = UsdmAdapter("A", client(Transport(payload), Market.USDM)).account()
    assert snapshot.balances[0].wallet == 1000 and snapshot.balances[0].available == 750
    assert snapshot.balances[0].margin_balance is None
    assert snapshot.positions[0].signed_quantity == D("-0.01")


def test_missing_permission_is_not_invented_as_trading_ready():
    with pytest.raises(ProtocolError):
        SpotAdapter("A", client(Transport({"balances": []}))).account()


def test_provider_used_weight_headers_update_shared_local_admission():
    budget = RequestBudget((Window("ip", 3, 60),), clock=lambda: 0)
    transport = Transport(HttpResponse(200, b"{}", {"X-MBX-USED-WEIGHT-1M": "3"}))
    c = client(transport)
    c.budget = budget
    c.request("GET", "/api/v3/time")
    with pytest.raises(RateLimited):
        c.request("GET", "/api/v3/time")
    assert len(transport.requests) == 1


def test_small_futures_reduce_only_exit_is_not_blocked_by_entry_min_notional():
    instrument = order(Market.USDM).instrument
    rules = SymbolRules(
        instrument, "BTC", "USDT", "TRADING", ({"filterType": "MIN_NOTIONAL", "notional": "200"},)
    )
    with pytest.raises(ValidationError):
        rules.validate(order(Market.USDM))
    rules.validate(order(Market.USDM, reduce=True))


@pytest.mark.parametrize("invalid", [{"executedQty": "-1"}, {"status": "UNKNOWN_NEW_STATUS"}])
def test_financially_invalid_ack_is_unknown_and_not_a_clean_rejection(invalid):
    transport = Transport(info(Market.SPOT), {**observation(), **invalid})
    adapter = SpotAdapter("A", client(transport))
    adapter.refresh_symbols()
    with pytest.raises(OrderOutcomeUnknown):
        adapter.submit(order())
