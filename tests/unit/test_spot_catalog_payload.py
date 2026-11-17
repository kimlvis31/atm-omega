"""Bounded Spot catalog asks only to omit unused permission-set payloads."""

import json
from copy import deepcopy
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from omega.domain.models import Market
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.execution_rules import rules_snapshot
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.transport import HttpResponse


class Transport:
    def __init__(self, payload):
        self.payload, self.requests = payload, []

    def send(self, request):
        self.requests.append(request)
        return HttpResponse(200, json.dumps(self.payload).encode())


class Budget(RequestBudget):
    def __init__(self):
        super().__init__((Window("ip", 100, 60),))
        self.costs = []

    def reserve(self, costs):
        self.costs.append(dict(costs))
        return super().reserve(costs)


def symbol(name="BTCUSDT", *, contract="PERPETUAL"):
    return {
        "symbol": name,
        "baseAsset": "BTC",
        "quoteAsset": "USDT",
        "status": "TRADING",
        "contractType": contract,
        "filters": [
            {"filterType": "LOT_SIZE", "minQty": "0.001", "maxQty": "1000", "stepSize": "0.001"},
            {
                "filterType": "PRICE_FILTER",
                "minPrice": "0.01",
                "maxPrice": "1000000",
                "tickSize": "0.01",
            },
        ],
    }


def catalog(payload, market=Market.SPOT):
    transport, budget = Transport(payload), Budget()
    host = "https://api.binance.com" if market == Market.SPOT else "https://fapi.binance.com"
    client = BinanceClient(host, transport, budget=budget, ip_scope="ip")
    adapter = BinanceMarketData(market, client)
    with patch.object(client, "request", wraps=client.request) as observed:
        rules = adapter.symbols()
        invocation = observed.call_args
    return rules, adapter, transport, budget, invocation


def test_spot_catalog_uses_unsigned_get_canonical_false_and_existing_weight():
    rules, adapter, transport, budget, invocation = catalog({"symbols": [symbol()]})
    (request,) = transport.requests
    assert request.method == "GET" and request.headers == {} and request.body is None
    assert request.url == "https://api.binance.com/api/v3/exchangeInfo?showPermissionSets=false"
    assert parse_qs(urlsplit(request.url).query) == {"showPermissionSets": ["false"]}
    assert invocation.args == ("GET", "/api/v3/exchangeInfo")
    assert invocation.kwargs == {"params": {"showPermissionSets": "false"}, "weight": 20}
    assert budget.costs == [{"ip": 20}]
    assert len(rules) == 1 and rules[0].instrument.key == "binance:spot:BTCUSDT"
    assert adapter.last_symbol_diagnostics["state"] == "ready"


def test_omitting_permission_sets_keeps_normalized_execution_rules_identical():
    original = symbol()
    original["permissionSets"] = [["SPOT", "MARGIN"], ["TRD_GRP_001"]]
    reduced = deepcopy(original)
    del reduced["permissionSets"]
    full, *_ = catalog({"symbols": [original]})
    omitted, *_ = catalog({"symbols": [reduced]})
    assert full == omitted
    assert (
        rules_snapshot(full[0], observed_at_ms=900100).document()
        == rules_snapshot(omitted[0], observed_at_ms=900100).document()
    )
    assert omitted[0].filters == tuple(original["filters"])


def test_usdm_call_identity_wire_weight_and_contract_filter_are_unchanged():
    rules, adapter, transport, budget, invocation = catalog(
        {
            "symbols": [
                symbol(),
                symbol("BTCUSDT_261225", contract="CURRENT_QUARTER"),
                symbol("OTHERUSDT", contract="TRADIFI_PERPETUAL"),
            ]
        },
        Market.USDM,
    )
    (request,) = transport.requests
    assert request.url == "https://fapi.binance.com/fapi/v1/exchangeInfo"
    assert request.method == "GET" and request.headers == {} and request.body is None
    assert invocation.args == ("GET", "/fapi/v1/exchangeInfo")
    assert invocation.kwargs == {"weight": 1}
    assert budget.costs == [{"ip": 1}]
    assert [rule.instrument.key for rule in rules] == ["binance:usdm:BTCUSDT"]
    assert adapter.last_symbol_diagnostics["excluded_contracts"] == {
        "CURRENT_QUARTER": 1,
        "TRADIFI_PERPETUAL": 1,
    }
