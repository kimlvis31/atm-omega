"""Full bounded endpoint responses; no order mutation or financial interpretation."""

from hashlib import sha256

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.execution_inventory import (
    MAX_INVENTORY_ROWS,
    ConditionalOrderInventory,
    ExternalConditionalOrder,
    InventoryProvenance,
    OpenOrder,
    OpenOrderInventory,
)
from omega.domain.models import Instrument, Market, Side, decimal
from omega.infrastructure.binance.parsing import order_observation
from omega.infrastructure.binance.transport import HttpResponse

MAX_INVENTORY_BODY_BYTES = 8_388_608


def _text(row, name):
    value = row[name]
    if type(value) is not str or not 1 <= len(value) <= 128 or not value.isprintable():
        raise ValidationError("inventory response text is malformed")
    return value


def _provider_id(value):
    if type(value) is not int or not 0 < value < 10**20:
        raise ValidationError("inventory response ID is not a native positive integer")
    return str(value)


def _amount(row, name):
    value = row[name]
    if type(value) is not str:
        raise ValidationError("inventory response amounts require decimal strings")
    return decimal(value)


def _ordinary(row, market, index):
    instrument = Instrument("binance", market, _text(row, "symbol"))
    _text(row, "clientOrderId")
    _provider_id(row["orderId"])
    _amount(row, "executedQty")
    quote_key = "cummulativeQuoteQty" if market is Market.SPOT else "cumQuote"
    quote = _amount(row, quote_key)
    alias = "cumQuote" if market is Market.SPOT else "cummulativeQuoteQty"
    if alias in row and _amount(row, alias) != quote:
        raise ValidationError("inventory cumulative quote aliases conflict")
    if market is Market.USDM and (
        type(row["reduceOnly"]) is not bool
        or type(row["closePosition"]) is not bool
        or row["positionSide"] not in ("BOTH", "LONG", "SHORT")
    ):
        raise ValidationError("Binance futures inventory requires its explicit native controls")
    return OpenOrder(
        order_observation(instrument, row),
        Side(_text(row, "side")),
        _text(row, "type"),
        _amount(row, "origQty"),
        _amount(row, "price"),
        row["time"],
        row["updateTime"],
        row["reduceOnly"] if market is Market.USDM else None,
        row["positionSide"] if market is Market.USDM else None,
        row["closePosition"] if market is Market.USDM else None,
        index,
    )


def _conditional(row, market, index):
    # Only these native identity labels are interpreted. Trigger, quantity,
    # actual execution and future fields remain opaque, including unknown types.
    return ExternalConditionalOrder(
        Instrument("binance", market, _text(row, "symbol")),
        _provider_id(row["algoId"]),
        _text(row, "clientAlgoId"),
        _text(row, "algoType"),
        _text(row, "orderType"),
        _text(row, "algoStatus"),
        index,
    )


def read_inventory(market, account_id, client, *, conditional=False):
    if type(market) is not Market or market not in (Market.SPOT, Market.USDM):
        raise ValidationError("unsupported inventory market")
    if type(conditional) is not bool or conditional and market is not Market.USDM:
        raise ValidationError("separate conditional inventory is USD-M only")
    origin = "https://api.binance.com" if market is Market.SPOT else "https://fapi.binance.com"
    if client.base_url != origin:
        raise ValidationError("inventory client belongs to another market origin")
    endpoint = (
        "/api/v3/openOrders"
        if market is Market.SPOT
        else ("/fapi/v1/openAlgoOrders" if conditional else "/fapi/v1/openOrders")
    )
    requested = client.clock_ms()
    decoded = client.request_with_response(
        "GET", endpoint, signed=True, weight=80 if market is Market.SPOT else 40
    )
    # Real transport bounds bytes before decoding; custom transports must honor
    # their own allocation limit. Also reject oversized already-decoded receipts.
    response = decoded.response
    if type(response) is not HttpResponse:
        raise ProtocolError("inventory requires a native HTTP response receipt")
    payload = decoded.data
    if response.status != 200 or any(key.lower() == "content-range" for key in response.headers):
        raise ProtocolError("inventory endpoint did not return a whole successful response")
    if type(response.body) is not bytes or len(response.body) > MAX_INVENTORY_BODY_BYTES:
        raise ProtocolError("inventory response exceeded its byte capacity")
    for key, value in response.headers.items():
        if key.lower() == "content-length":
            if (
                type(value) is not str
                or len(value.strip()) > 20
                or not value.strip().isascii()
                or not value.strip().isdigit()
                or int(value.strip()) != len(response.body)
            ):
                raise ProtocolError("inventory response length is incomplete or malformed")
    if type(payload) is not list or len(payload) > MAX_INVENTORY_ROWS:
        raise ProtocolError("inventory requires a complete bounded endpoint array")
    if any(type(row) is not dict for row in payload):
        raise ProtocolError("inventory contains a non-object row")
    try:
        rows = tuple(
            (_conditional if conditional else _ordinary)(row, market, index)
            for index, row in enumerate(payload)
        )
        provenance = InventoryProvenance(
            account_id,
            market,
            client.base_url,
            endpoint,
            requested,
            response.received_at_ms,
            client.clock_ms(),
            sha256(response.body).hexdigest(),
            provider="binance",
            endpoint_kind="conditional" if conditional else "ordinary",
        )
        return (ConditionalOrderInventory if conditional else OpenOrderInventory)(provenance, rows)
    except (KeyError, TypeError, ValueError, ArithmeticError, ValidationError):
        raise ProtocolError("unsupported or incomplete open-order inventory response") from None
