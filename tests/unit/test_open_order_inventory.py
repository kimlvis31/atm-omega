"""Endpoint scope and provenance do not become execution/recovery permission."""

from dataclasses import replace
from decimal import Decimal as D

import pytest

from omega.domain.errors import ValidationError
from omega.domain.execution_inventory import (
    ConditionalOrderInventory,
    ExternalConditionalOrder,
    InventoryProvenance,
    OpenOrder,
    OpenOrderInventory,
)
from omega.domain.models import Instrument, Market, OrderObservation, OrderStatus, Side


def source(market=Market.SPOT, *, conditional=False):
    endpoint = (
        "/api/v3/openOrders"
        if market is Market.SPOT
        else ("/fapi/v1/openAlgoOrders" if conditional else "/fapi/v1/openOrders")
    )
    origin = "https://api.binance.com" if market is Market.SPOT else "https://fapi.binance.com"
    return InventoryProvenance(
        "synthetic",
        market,
        origin,
        endpoint,
        100,
        101,
        102,
        "a" * 64,
        provider="binance",
        endpoint_kind="conditional" if conditional else "ordinary",
    )


def order():
    instrument = Instrument("binance", Market.SPOT, "BTCUSDT")
    return OpenOrder(
        OrderObservation(
            instrument,
            "external/client:1",
            "7",
            OrderStatus.PARTIALLY_FILLED,
            D(".25"),
            D("25.000001"),
        ),
        Side.BUY,
        "LIMIT_MAKER",
        D(1),
        D(100),
        1,
        2,
        None,
        None,
        None,
        0,
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"received_at_ms": None},
        {"received_at_ms": True},
        {"received_at_ms": 99},
        {"completed_at_ms": 100},
        {"requested_at_ms": -1},
        {"response_sha256": ""},
    ],
)
def test_even_empty_inventory_requires_real_bounded_response_provenance(changes):
    with pytest.raises(ValidationError):
        OpenOrderInventory(replace(source(), **changes), ())


def test_inventory_preserves_every_row_and_rejects_duplicate_or_reordered_source_identity():
    first = order()
    value = OpenOrderInventory(source(), (first,))
    assert value.orders[0].observation.cumulative_quote == D("25.000001")
    assert value.orders[0].order_type == "LIMIT_MAKER"
    assert value.scope == "all_symbols_open_orders_endpoint_only"
    for rows in [
        (first, replace(first, source_row_index=1)),
        (replace(first, source_row_index=1),),
    ]:
        with pytest.raises(ValidationError):
            OpenOrderInventory(source(), rows)


def test_conditional_native_identity_cannot_be_an_ordinary_order_or_financial_fact():
    row = ExternalConditionalOrder(
        Instrument("binance", Market.USDM, "BNBUSDT"),
        "9",
        "external-algo",
        "CONDITIONAL",
        "FUTURE_UNKNOWN_TYPE",
        "UNKNOWN_NATIVE_STATE",
        0,
    )
    result = ConditionalOrderInventory(source(Market.USDM, conditional=True), (row,))
    assert result.orders == (row,) and "external_conditional" in result.scope
    assert not hasattr(row, "observation") and not hasattr(row, "quantity")
    assert not hasattr(result, "manual_resume_ready") and not hasattr(result, "history_complete")
    with pytest.raises(ValidationError):
        OpenOrderInventory(result.provenance, (row,))
    with pytest.raises(ValidationError):
        ConditionalOrderInventory(source(Market.USDM), (row,))


@pytest.mark.parametrize(
    "changes",
    [
        {"original_quantity": D(".1")},
        {"price": D(-1)},
        {"created_at_ms": True},
        {"updated_at_ms": 0},
        {"reduce_only": "false"},
    ],
)
def test_inconsistent_amounts_provider_time_or_invented_spot_controls_reject(changes):
    with pytest.raises(ValidationError):
        replace(order(), **changes)


def test_generic_contract_accepts_other_provider_and_uses_kind_not_endpoint_suffix():
    provenance = replace(
        source(),
        provider="other",
        origin="https://api.other.example:8443",
        endpoint="/custom/openAlgoOrders",
        endpoint_kind="ordinary",
    )
    row = order()
    row = replace(
        row,
        observation=replace(
            row.observation,
            instrument=Instrument("other", Market.SPOT, "BTCUSDT"),
            provider_order_id="native-order:uuid",
        ),
    )
    assert OpenOrderInventory(provenance, (row,)).orders == (row,)
    with pytest.raises(ValidationError):
        OpenOrderInventory(replace(provenance, provider="binance"), (row,))


@pytest.mark.parametrize("market", [Market.SPOT, Market.CASH_EQUITY])
def test_generic_conditional_identity_and_market_are_not_binance_numeric_or_usdm_rules(market):
    provenance = replace(
        source(),
        provider="other",
        market=market,
        origin="https://other.example",
        endpoint="/open",
        endpoint_kind="conditional",
    )
    row = ExternalConditionalOrder(
        Instrument("other", market, "TEST"),
        "opaque-algo:uuid",
        "opaque-client",
        "NATIVE_KIND",
        "NATIVE_TYPE",
        "UNINTERPRETED_STATUS",
        0,
    )
    assert ConditionalOrderInventory(provenance, (row,)).orders == (row,)


@pytest.mark.parametrize(
    "changes",
    [
        {"origin": "http://other.example"},
        {"origin": "https://user:secret@other.example"},
        {"endpoint": "/open?signature=private"},
        {"endpoint_kind": "unknown"},
    ],
)
def test_generic_source_shape_rejects_credentials_queries_and_ambiguous_kind(changes):
    with pytest.raises(ValidationError):
        replace(source(), **changes)
