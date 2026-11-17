"""Original response bytes survive decoding; malformed JSON cannot look successful."""

from decimal import Decimal

import pytest

from omega.domain.errors import OrderOutcomeUnknown, ProtocolError
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.transport import HttpResponse


class Transport:
    def __init__(self, body):
        self.response = HttpResponse(200, body, {"Content-Type": "application/json"})
        self.calls = []

    def send(self, request):
        self.calls.append(request)
        return self.response


def client(body):
    transport = Transport(body)
    return BinanceClient(
        "https://api.binance.com", transport, budget=RequestBudget((Window("binance-ip", 10, 60),))
    ), transport


def test_decode_retains_actual_wire_body_whitespace_and_decimal_representation():
    raw = b' { "price" : 1.2300, "quantity":"0.0100" }\r\n'
    public, transport = client(raw)
    result = public.request_with_response("GET", "/api/v3/ticker/price", {"symbol": "ETHUSDT"})
    assert result.data == {"price": Decimal("1.2300"), "quantity": "0.0100"}
    assert result.response is transport.response and result.response.body == raw
    assert raw.decode().strip() not in repr(result)
    assert len(transport.calls) == 1
    assert public.request("GET", "/api/v3/ticker/price") == result.data


@pytest.mark.parametrize("raw", [b'{"p":1,"p":2}', b'{"p":NaN}', b'{"p":Infinity}', b"[" * 1500])
def test_invalid_json_is_protocol_failure_for_reads_and_unknown_for_mutations(raw):
    public, transport = client(raw)
    with pytest.raises(ProtocolError):
        public.request_with_response("GET", "/api/v3/ticker/price")
    public.enable_trading = True  # Offline fake transport only; no exchange connection.
    with pytest.raises(OrderOutcomeUnknown):
        public.request_with_response("POST", "/api/v3/order")
    assert len(transport.calls) == 2
