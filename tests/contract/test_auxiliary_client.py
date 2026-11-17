"""Public auxiliary GETs with synthetic transport; no account or exchange connection."""

import json
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest

from omega.domain.errors import ProtocolError, RateLimited, TransportError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient, Credentials, DecodedResponse
from omega.infrastructure.binance.transport import HttpResponse

NOW = 1_793_000_100_000
USDM = Instrument("binance", Market.USDM, "ETHUSDT")
SPOT = Instrument("binance", Market.SPOT, "ETHUSDT")


class Transport:
    def __init__(self, responses):
        self.responses, self.requests = list(responses), []

    def send(self, request):
        self.requests.append(request)
        result = self.responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


class Budget(RequestBudget):
    def __init__(self):
        super().__init__(
            (Window("binance-ip", 600, 60), Window("binance-ip", 1000, 300)),
            clock=lambda: NOW / 1000,
        )
        self.reservations = []

    def reserve(self, costs):
        self.reservations.append(dict(costs))
        return super().reserve(costs)


def response(data, *, headers=None, status=200):
    return HttpResponse(status, json.dumps(data, separators=(",", ":")).encode(), headers or {})


def trade(**changes):
    return {
        "a": 10,
        "p": "100.01",
        "q": ".03",
        "nq": ".02",
        "f": 30,
        "l": 31,
        "T": NOW - 100,
        "m": False,
        **changes,
    }


def current(**changes):
    return {"symbol": "ETHUSDT", "openInterest": "123.456", "time": NOW - 100, **changes}


def statistic(*, ratio=False, **changes):
    data = (
        {"longShortRatio": "2", "longAccount": ".6667", "shortAccount": ".3333"}
        if ratio
        else {"sumOpenInterest": "123.456", "sumOpenInterestValue": "246.912"}
    )
    return {"symbol": "ETHUSDT", "timestamp": NOW - 300_000, **data, **changes}


def create(*responses, market=Market.USDM, clock=None):
    transport, budget = Transport(responses), Budget()
    base = "https://fapi.binance.com" if market == Market.USDM else "https://api.binance.com"
    client = BinanceClient(base, transport, budget=budget, clock_ms=clock or (lambda: NOW))
    return BinanceAuxiliaryClient(market, client), transport, budget


@pytest.mark.parametrize(
    "instrument,weight,path", [(USDM, 20, "/fapi/v1/aggTrades"), (SPOT, 4, "/api/v3/aggTrades")]
)
def test_one_bounded_public_trade_get_uses_matching_origin_weight_and_keeps_batch_raw(
    instrument, weight, path
):
    reply = response(
        [trade()],
        headers={
            "Date": "fixture-date",
            "Content-Type": "application/json",
            "X-MBX-USED-WEIGHT-1M": "20",
            "Set-Cookie": "synthetic-cookie",
            "Authorization": "synthetic-omit",
        },
    )
    source, transport, budget = create(reply, market=instrument.market)
    batch = source.agg_trades(instrument, from_id=10, limit=1)
    assert len(transport.requests) == 1 and transport.requests[0].method == "GET"
    request = transport.requests[0]
    assert urlsplit(request.url).path == path
    assert parse_qs(urlsplit(request.url).query) == {
        "symbol": ["ETHUSDT"],
        "limit": ["1"],
        "fromId": ["10"],
    }
    assert request.body is None and request.headers == {}
    assert budget.reservations == [{"binance-ip": weight}]
    assert batch.body is reply.body and batch.events[0].raw is None and batch.saturated
    assert batch.events[0].source == "rest" and batch.events[0].non_rpi_quantity is not None
    assert batch.received_at_ms == NOW and batch.events[0].available_at_ms == NOW
    assert dict(batch.headers) == {
        "date": "fixture-date",
        "content-type": "application/json",
        "x-mbx-used-weight-1m": "20",
    }


def test_half_open_request_is_converted_to_inclusive_end_and_empty_is_not_coverage_proof():
    source, transport, _ = create(response([]))
    batch = source.agg_trades(USDM, start_ms=NOW - 3_600_000, end_ms=NOW, limit=1000)
    query = parse_qs(urlsplit(transport.requests[0].url).query)
    assert query["endTime"] == [str(NOW - 1)] and query["startTime"] == [str(NOW - 3_600_000)]
    assert batch.events == () and not batch.saturated


@pytest.mark.parametrize(
    "options",
    [
        {"limit": 0},
        {"limit": True},
        {"limit": 1001},
        {"from_id": True},
        {"from_id": -1},
        {"from_id": 1, "start_ms": NOW - 10, "end_ms": NOW},
        {"start_ms": NOW - 1},
        {"end_ms": NOW},
        {"start_ms": NOW, "end_ms": NOW},
        {"start_ms": NOW - 3_600_001, "end_ms": NOW},
        {"start_ms": NOW - 2 * 86_400_000 - 1, "end_ms": NOW - 2 * 86_400_000 + 10},
        {"start_ms": NOW, "end_ms": NOW + 1},
    ],
)
def test_invalid_trade_request_is_rejected_before_transport(options):
    source, transport, budget = create()
    with pytest.raises(ValidationError):
        source.agg_trades(USDM, **options)
    assert transport.requests == [] and budget.reservations == []


@pytest.mark.parametrize(
    "data",
    [
        {},
        None,
        [trade(a=True)],
        [trade(), trade()],
        [trade(a=11), trade(a=10)],
        [trade(T=NOW + 1)],
        [trade(s="BTCUSDT")],
        [trade(p="NaN")],
    ],
)
def test_invalid_schema_duplicates_order_and_query_range_never_return_a_success_batch(data):
    source, _, _ = create(response(data))
    with pytest.raises(ProtocolError):
        source.agg_trades(USDM, start_ms=NOW - 1000, end_ms=NOW)


@pytest.mark.parametrize(
    "method,path,data",
    [
        ("current_open_interest", "/fapi/v1/openInterest", current()),
        ("open_interest_history", "/futures/data/openInterestHist", [statistic()]),
        ("long_short_ratio", "/futures/data/globalLongShortAccountRatio", [statistic(ratio=True)]),
    ],
)
def test_futures_statistics_native_point_or_five_minute_period_is_preserved(method, path, data):
    source, transport, budget = create(response(data))
    options = (
        {} if method == "current_open_interest" else {"start_ms": NOW - 600_000, "end_ms": NOW}
    )
    batch = getattr(source, method)(USDM, **options)
    assert urlsplit(transport.requests[0].url).path == path
    query = parse_qs(urlsplit(transport.requests[0].url).query)
    item = batch.events[0]
    assert item.raw is None and item.available_at_ms == NOW
    assert item.source_period_ms == (None if method == "current_open_interest" else 300_000)
    if method != "current_open_interest":
        assert query["period"] == ["5m"] and item.event_time_ms == NOW - 300_000
    assert budget.reservations == [{"binance-ip": 1}]  # Zero-weight stats still consume admission.


@pytest.mark.parametrize(
    "method", ["current_open_interest", "open_interest_history", "long_short_ratio"]
)
def test_spot_derivative_channels_fail_as_unsupported_before_any_request(method):
    source, transport, _ = create(market=Market.SPOT)
    options = (
        {} if method == "current_open_interest" else {"start_ms": NOW - 600_000, "end_ms": NOW}
    )
    with pytest.raises(ValidationError, match="unsupported"):
        getattr(source, method)(SPOT, **options)
    assert transport.requests == [] and source.capabilities == {"agg_trades"}
    assert "funding_rate" not in source.capabilities and "depth" not in source.capabilities


@pytest.mark.parametrize(
    "field,value",
    [
        ("credentials", Credentials("fixture-only", "fixture-only")),
        ("enable_trading", True),
        ("budget", None),
        ("ip_scope", "other-ip"),
        ("base_url", "https://api.binance.com"),
    ],
)
def test_constructor_and_later_session_changes_cannot_enable_private_or_wrong_origin_requests(
    field, value
):
    source, transport, _ = create(response([]))
    setattr(source.client, field, value)
    with pytest.raises(ValidationError):
        BinanceAuxiliaryClient(Market.USDM, source.client)
    with pytest.raises(ValidationError):
        source.agg_trades(USDM)
    assert transport.requests == []


@pytest.mark.parametrize("status", [418, 429])
def test_html_rate_limit_creates_shared_cooldown_and_never_automatically_retries(status):
    source, transport, _ = create(
        HttpResponse(status, b"<html>limited</html>", {"Retry-After": "7"})
    )
    with pytest.raises(RateLimited) as first:
        source.agg_trades(USDM)
    assert first.value.retry_after == 7
    with pytest.raises(RateLimited):
        source.agg_trades(USDM)
    assert len(transport.requests) == 1


def test_server_error_or_transport_failure_has_no_hidden_retry():
    for failure in (
        HttpResponse(503, b"<html>unavailable</html>"),
        TransportError("synthetic timeout"),
    ):
        source, transport, _ = create(failure)
        with pytest.raises(TransportError):
            source.agg_trades(USDM)
        assert len(transport.requests) == 1


def test_payload_and_decoded_response_mismatch_or_wrong_envelope_is_rejected():
    source, _, _ = create()
    for value in (
        DecodedResponse([trade(a=9)], response([trade()])),
        SimpleNamespace(data=[], response=object()),
        DecodedResponse([], HttpResponse(201, b"[]")),
    ):
        source.client.request_with_response = lambda *_args, result=value, **_options: result
        with pytest.raises(ProtocolError):
            source.agg_trades(USDM)


def test_batch_received_and_normalized_availability_use_actual_clock_samples():
    clock = iter((NOW, NOW + 10, NOW + 12))
    source, _, _ = create(response([trade()]), clock=lambda: next(clock))
    batch = source.agg_trades(USDM)
    assert batch.received_at_ms == NOW + 10 and batch.events[0].available_at_ms == NOW + 12
    clock = iter((NOW, NOW + 10, NOW + 9))
    source, _, _ = create(response([trade()]), clock=lambda: next(clock))
    with pytest.raises(ProtocolError, match="backward"):
        source.agg_trades(USDM)


def test_statistic_missing_fields_overlimit_duplicates_and_old_range_are_not_zero_filled():
    for data in ([{}], [statistic(), statistic()], {"unexpected": "object"}):
        source, _, _ = create(response(data))
        with pytest.raises(ProtocolError):
            source.open_interest_history(USDM, start_ms=NOW - 600_000, end_ms=NOW)
    source, transport, _ = create()
    with pytest.raises(ValidationError):
        source.long_short_ratio(USDM, start_ms=NOW - 29 * 86_400_000, end_ms=NOW)
    assert not transport.requests


def test_wire_json_duplicate_fields_and_oversized_body_are_rejected():
    for reply in (
        HttpResponse(200, b'[{"a":1,"a":2}]'),
        HttpResponse(200, b"[]" + b" " * (4 * 1024 * 1024)),
    ):
        source, _, _ = create(reply)
        with pytest.raises(ProtocolError):
            source.agg_trades(USDM)


def test_small_host_clock_skew_preserves_source_time_but_never_fakes_domain_readiness():
    source, _, _ = create(response([trade(T=NOW + 400)]))
    batch = source.agg_trades(USDM)
    item = batch.events[0]
    assert item.trade_time_ms == NOW + 400 and item.available_at_ms == NOW
    with pytest.raises(ValidationError):
        item.as_flow_event()
    source, _, _ = create(response(current(time=NOW + 400)))
    item = source.current_open_interest(USDM).events[0]
    assert item.event_time_ms == NOW + 400 and item.available_at_ms == NOW


@pytest.mark.parametrize(
    "headers", [{"date": 1}, {"Date": "now\r\ncookie:synthetic"}, {"date": "x" * 8193}]
)
def test_invalid_header_types_or_injection_are_not_preserved_as_public_metadata(headers):
    source, _, _ = create(response([], headers=headers))
    with pytest.raises(ProtocolError):
        source.agg_trades(USDM)


def test_batch_repr_does_not_dump_response_body_or_all_parsed_events():
    source, _, _ = create(response([trade()]))
    batch = source.agg_trades(USDM)
    rendered = repr(batch)
    assert "body=" not in rendered and "events=" not in rendered
    assert "AuxTradeEvent" not in rendered and "100.01" not in rendered
