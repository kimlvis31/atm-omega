"""Real adapters/client, existing memory transport and synthetic credentials only."""

import hashlib
import hmac
import json
from decimal import Decimal as D
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest
import test_binance as fixtures

from omega.domain.errors import (
    BrokerRejected,
    ProtocolError,
    RateLimited,
    TransportError,
    ValidationError,
)
from omega.domain.execution_inventory import ConditionalOrderInventory, OpenOrderInventory
from omega.domain.models import Market
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials
from omega.infrastructure.binance.spot import SpotAdapter
from omega.infrastructure.binance.transport import HttpResponse
from omega.infrastructure.binance.usdm import UsdmAdapter


def ordinary(market, symbol="BTCUSDT", identity=7, client_id="external:one"):
    row = dict(
        symbol=symbol,
        orderId=identity,
        clientOrderId=client_id,
        status="PARTIALLY_FILLED",
        side="BUY",
        type="LIMIT_MAKER",
        origQty="1.000000000001",
        executedQty=".25",
        price="100.000000000001",
        time=1,
        updateTime=2,
    )
    row["cummulativeQuoteQty" if market is Market.SPOT else "cumQuote"] = "25.00000000000001"
    if market is Market.USDM:
        row.update(type="LIMIT", reduceOnly=True, positionSide="LONG", closePosition=False)
    return row


def conditional():
    return dict(
        symbol="BNBUSDT",
        algoId=19,
        clientAlgoId="external-algo",
        algoType="CONDITIONAL",
        orderType="UNKNOWN_FUTURE_TYPE",
        algoStatus="UNINTERPRETED_NATIVE_STATE",
        quantity={"opaque": "not a financial quantity"},
        triggerPrice=None,
    )


def response(payload, *, received=101):
    return HttpResponse(200, json.dumps(payload, indent=2).encode(), received_at_ms=received)


def adapter(transport, market, *, weight_limit=10000, clock=None):
    values = iter([100, 100, 102])
    client = BinanceClient(
        "https://api.binance.com" if market is Market.SPOT else "https://fapi.binance.com",
        transport,
        credentials=Credentials("inventory-test-key", "inventory-test-secret"),
        budget=RequestBudget((Window("ip", weight_limit, 60),), clock=lambda: 0),
        ip_scope="ip",
        clock_ms=clock or (lambda: next(values)),
    )
    assert client.enable_trading is False
    return (
        SpotAdapter("synthetic", client)
        if market is Market.SPOT
        else UsdmAdapter("synthetic", client)
    )


def assert_inventory_gets(transport, paths):
    assert [urlsplit(request.url).path for request in transport.requests] == paths
    for request in transport.requests:
        assert request.method == "GET" and request.body is None
        wire = urlsplit(request.url).query
        values = parse_qs(wire)
        assert "symbol" not in values and "limit" not in values
        assert set(values) == {"timestamp", "recvWindow", "signature"}
        unsigned, signature = wire.rsplit("&signature=", 1)
        expected = hmac.new(b"inventory-test-secret", unsigned.encode(), hashlib.sha256).hexdigest()
        assert signature == expected and request.headers["X-MBX-APIKEY"] == "inventory-test-key"
    assert not any(urlsplit(request.url).path.endswith("/order") for request in transport.requests)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_full_account_rows_and_actual_body_receipt_survive_without_known_order_query(market):
    payload = [ordinary(market), ordinary(market, "ETHBTC", 7, "external:one")]
    raw = response(payload)
    transport = fixtures.Transport(raw)
    result = adapter(transport, market).open_orders()
    assert type(result) is OpenOrderInventory and len(result.orders) == 2
    assert [row.source_row_index for row in result.orders] == [0, 1]
    assert {row.observation.instrument.symbol for row in result.orders} == {"BTCUSDT", "ETHBTC"}
    assert result.orders[0].observation.cumulative_quote == D("25.00000000000001")
    assert result.orders[0].original_quantity == D("1.000000000001")
    assert result.provenance.account_id == "synthetic" and result.provenance.market is market
    assert result.provenance.origin == (
        "https://api.binance.com" if market is Market.SPOT else "https://fapi.binance.com"
    )
    assert (
        result.provenance.requested_at_ms,
        result.provenance.received_at_ms,
        result.provenance.completed_at_ms,
    ) == (100, 101, 102)
    assert result.provenance.response_sha256 == hashlib.sha256(raw.body).hexdigest()
    assert result.scope == "all_symbols_open_orders_endpoint_only"
    assert not hasattr(result, "manual_resume_ready") and not hasattr(result, "history_complete")
    assert_inventory_gets(
        transport, ["/api/v3/openOrders" if market is Market.SPOT else "/fapi/v1/openOrders"]
    )


def test_usdm_conditional_endpoint_keeps_native_identity_opaque_and_separate():
    transport = fixtures.Transport(response([conditional()]))
    result = adapter(transport, Market.USDM).open_conditional_orders()
    assert type(result) is ConditionalOrderInventory and result.orders[0].algo_id == "19"
    row = result.orders[0]
    assert row.native_status == "UNINTERPRETED_NATIVE_STATE"
    assert not hasattr(row, "observation") and not hasattr(row, "quantity")
    assert "external_conditional" in result.scope
    assert_inventory_gets(transport, ["/fapi/v1/openAlgoOrders"])


@pytest.mark.parametrize(
    "market,conditional_read,weight",
    [
        (Market.SPOT, False, 80),
        (Market.USDM, False, 40),
        (Market.USDM, True, 40),
    ],
)
def test_all_symbols_endpoint_weight_is_reserved_before_transport(market, conditional_read, weight):
    transport = fixtures.Transport(response([]))
    owner = adapter(transport, market, weight_limit=weight - 1)
    with pytest.raises(RateLimited):
        (owner.open_conditional_orders if conditional_read else owner.open_orders)()
    assert transport.requests == []
    exact = adapter(transport, market, weight_limit=weight)
    inventory = (exact.open_conditional_orders if conditional_read else exact.open_orders)()
    assert inventory.orders == () and len(transport.requests) == 1


@pytest.mark.parametrize("received", [None, True, -1, 99, 103])
def test_empty_array_without_actual_compatible_receipt_is_not_an_empty_inventory(received):
    transport = fixtures.Transport(response([], received=received))
    with pytest.raises(ProtocolError):
        adapter(transport, Market.SPOT).open_orders()


@pytest.mark.parametrize("change", ["duplicate", "missing", "amount", "id", "terminal", "alias"])
def test_bad_ordinary_rows_do_not_publish_partial_inventory(change):
    row = ordinary(Market.SPOT)
    if change == "missing":
        del row["origQty"]
    elif change == "amount":
        row["executedQty"] = 0.25
    elif change == "id":
        row["orderId"] = True
    elif change == "terminal":
        row["status"] = "FILLED"
    elif change == "alias":
        row["cumQuote"] = "999"
    payload = [row, dict(row)] if change == "duplicate" else [ordinary(Market.SPOT, "ETHBTC"), row]
    with pytest.raises(ProtocolError):
        adapter(fixtures.Transport(response(payload)), Market.SPOT).open_orders()


@pytest.mark.parametrize(
    "payload",
    [
        {"orders": []},
        [None],
        [ordinary(Market.SPOT)] * 1001,
    ],
)
def test_wrong_shape_or_capacity_never_becomes_empty_or_truncated_success(payload):
    with pytest.raises(ProtocolError):
        adapter(fixtures.Transport(response(payload)), Market.SPOT).open_orders()


@pytest.mark.parametrize("body", [b'[{"symbol":"A","symbol":"B"}]', b"[NaN]", b"{bad"])
def test_existing_client_strict_json_errors_are_preserved(body):
    with pytest.raises(ProtocolError):
        adapter(
            fixtures.Transport(HttpResponse(200, body, received_at_ms=101)), Market.SPOT
        ).open_orders()


@pytest.mark.parametrize(
    "raw,error",
    [
        (HttpResponse(503, b"temporary"), TransportError),
        (HttpResponse(429, b"", {"Retry-After": "3"}), RateLimited),
        (HttpResponse(400, b'{"code":-2015}'), BrokerRejected),
        (HttpResponse(206, b"[]", received_at_ms=101), ProtocolError),
        (HttpResponse(200, b"[]", {"Content-Range": "bytes 0-1/100"}, 101), ProtocolError),
        (HttpResponse(200, b"[]", {"Content-Length": "100"}, 101), ProtocolError),
    ],
)
def test_existing_read_errors_propagate_instead_of_inventory_absence(raw, error):
    with pytest.raises(error):
        adapter(fixtures.Transport(raw), Market.USDM).open_conditional_orders()


def test_body_over_eight_mib_rejects_even_if_json_is_an_empty_array():
    raw = HttpResponse(200, b"[" + b" " * 8_388_608 + b"]", received_at_ms=101)
    with pytest.raises(ProtocolError):
        adapter(fixtures.Transport(raw), Market.SPOT).open_orders()


def test_existing_single_order_query_keeps_legacy_optional_receipt_behavior():
    transport = fixtures.Transport(fixtures.observation(market=Market.SPOT))
    owner = adapter(transport, Market.SPOT, clock=lambda: 100)
    row = owner.query(fixtures.order().instrument, "omega-1")
    assert row.client_order_id == "omega-1"
    assert urlsplit(transport.requests[0].url).path == "/api/v3/order"


@pytest.mark.parametrize(
    "field,value",
    [
        ("reduceOnly", "false"),
        ("positionSide", None),
        ("closePosition", 0),
    ],
)
def test_usdm_ordinary_inventory_does_not_invent_missing_or_untyped_controls(field, value):
    row = ordinary(Market.USDM) | {field: value}
    with pytest.raises(ProtocolError):
        adapter(fixtures.Transport(response([row])), Market.USDM).open_orders()


@pytest.mark.parametrize("field,value", [("algoId", True), ("clientAlgoId", None)])
def test_opaque_conditional_still_requires_its_actual_native_identity(field, value):
    row = conditional() | {field: value}
    with pytest.raises(ProtocolError):
        adapter(fixtures.Transport(response([row])), Market.USDM).open_conditional_orders()


def test_spot_does_not_advertise_a_separate_usdm_conditional_reader():
    owner = adapter(fixtures.Transport(), Market.SPOT)
    assert not hasattr(owner, "open_conditional_orders")


def test_binance_inventory_rejects_foreign_origin_before_signing_or_transport():
    transport = fixtures.Transport()
    owner = adapter(transport, Market.SPOT)
    owner.client.base_url = "https://api.other.example"
    with pytest.raises(ValidationError, match="another market origin"):
        owner.open_orders()
    assert transport.requests == []


def test_inventory_rejects_structural_fake_response_even_with_valid_empty_body_and_times():
    class StructuralReceiptTransport(fixtures.Transport):
        def send(self, request):
            self.requests.append(request)
            return SimpleNamespace(status=200, headers={}, body=b"[]", received_at_ms=101)

    transport = StructuralReceiptTransport()
    with pytest.raises(ProtocolError, match="native HTTP response receipt"):
        adapter(transport, Market.SPOT).open_orders()
    assert len(transport.requests) == 1
