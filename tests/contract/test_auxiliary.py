"""Synthetic public payload contracts; no exchange request or account access."""

import json
from dataclasses import FrozenInstanceError
from decimal import Decimal

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary import (
    agg_trade,
    funding_rate,
    long_short_ratio,
    open_interest,
    open_interest_history,
)

SPOT = Instrument("binance", Market.SPOT, "ETHUSDT")
USDM = Instrument("binance", Market.USDM, "ETHUSDT")


def trade(**updates):
    return {
        "a": 7,
        "p": "100.000000000000000001",
        "q": "0.03",
        "f": 11,
        "l": 14,
        "T": 60_000,
        "m": True,
        **updates,
    }


def test_trade_identity_exact_values_and_original_bytes_are_distinct_from_normalized_dict():
    payload = trade(e="aggTrade", s="ETHUSDT", E=60_003)
    event = agg_trade(USDM, payload, available_at_ms=60_001, source="websocket")
    assert event.price == Decimal("100.000000000000000001")
    assert event.quantity == Decimal("0.03") and event.trade_count == 4
    assert event.buyer_is_maker and event.aggregate_id == 7
    assert event.raw is None and event.event_time_ms == 60_003
    assert event.available_at_ms == 60_001  # Clock skew is preserved, never clamped.
    raw = json.dumps({"stream": "ethusdt@aggTrade", "data": payload}).encode()
    assert agg_trade(USDM, raw, available_at_ms=60_001, source="websocket").raw is raw
    with pytest.raises(FrozenInstanceError):
        event.aggregate_id = 8


@pytest.mark.parametrize("instrument", [SPOT, USDM])
def test_rest_trade_has_no_invented_event_time_or_raw_payload(instrument):
    event = agg_trade(instrument, trade(m=False), available_at_ms=60_001)
    assert event.event_time_ms is None and event.raw is None and event.source == "rest"
    assert not event.buyer_is_maker


def test_optional_non_rpi_quantity_is_preserved_without_overwriting_total_quantity():
    event = agg_trade(USDM, trade(nq=".02"), available_at_ms=60_001)
    assert event.non_rpi_quantity == Decimal(".02") and event.quantity == Decimal(".03")
    assert agg_trade(SPOT, trade(), available_at_ms=60_001).non_rpi_quantity is None
    for value in (".04", "-1", "NaN", True):
        with pytest.raises(ProtocolError):
            agg_trade(USDM, trade(nq=value), available_at_ms=60_001)


def test_transport_source_is_explicit_and_stream_metadata_is_required():
    event = agg_trade(USDM, trade(E=60_000), available_at_ms=60_001, source="rest")
    assert event.source == "rest"
    with pytest.raises(ProtocolError):
        agg_trade(USDM, trade(), available_at_ms=60_001, source="websocket")


@pytest.mark.parametrize(
    "update",
    [
        {"a": True},
        {"a": -1},
        {"a": "7"},
        {"a": 2**63},
        {"f": 15},
        {"l": False},
        {"T": -1},
        {"E": None},
        {"m": 1},
        {"p": "NaN"},
        {"p": "Infinity"},
        {"p": 100.0},
        {"p": "0"},
        {"q": "-1"},
        {"q": "0"},
        {"q": True},
        {"s": "BTCUSDT"},
        {"e": "trade"},
    ],
)
def test_malformed_trade_fails_without_coercing_or_filling_missing_values(update):
    with pytest.raises(ProtocolError):
        agg_trade(USDM, trade(**update), available_at_ms=60_001)


@pytest.mark.parametrize(
    "payload",
    [b'{"a":1,"a":2}', b"\xff", b"[]", b"{}", b" " * 1_048_577, "{}", None],
    ids=["duplicate", "encoding", "array", "missing", "oversized", "text", "none"],
)
def test_payload_shape_duplicate_fields_encoding_and_size_are_bounded(payload):
    with pytest.raises(ProtocolError):
        agg_trade(USDM, payload, available_at_ms=60_001)


@pytest.mark.parametrize("available", [True, -1, 1.5, "60000"])
def test_receipt_timestamp_is_not_silently_coerced(available):
    with pytest.raises(ProtocolError):
        agg_trade(USDM, trade(), available_at_ms=available)


def test_current_oi_and_historical_oi_have_distinct_units_period_and_source_time():
    current = open_interest(
        USDM, {"symbol": "ETHUSDT", "openInterest": "123.4", "time": 90}, available_at_ms=100
    )
    past = open_interest_history(
        USDM,
        {
            "symbol": "ETHUSDT",
            "sumOpenInterest": "100.5",
            "sumOpenInterestValue": "20100",
            "timestamp": 0,
        },
        available_at_ms=100,
        period_ms=300_000,
    )
    assert current.values == (Decimal("123.4"),) and current.source_period_ms is None
    assert current.event_time_ms == 90 and current.available_at_ms == 100
    assert past.values == (Decimal("100.5"), Decimal("20100"))
    assert past.event_time_ms == 0 and past.source_period_ms == 300_000


def test_account_ratio_and_negative_settled_funding_remain_independent_samples():
    ratio = long_short_ratio(
        USDM,
        {"longShortRatio": "2", "longAccount": "0.6667", "shortAccount": "0.3333", "timestamp": 0},
        available_at_ms=100,
        period_ms=300_000,
    )
    funding = funding_rate(
        USDM,
        {"symbol": "ETHUSDT", "fundingRate": "-0.000001", "fundingTime": 50},
        available_at_ms=100,
    )
    assert ratio.values == (Decimal("2"), Decimal(".6667"), Decimal(".3333"))
    assert funding.values == (Decimal("-.000001"),) and funding.channel == "funding_rate"
    assert funding.event_time_ms == 50 and funding.source_period_ms is None


STATISTICS = [
    (open_interest, {"symbol": "ETHUSDT", "openInterest": "0", "time": 0}, {}),
    (
        open_interest_history,
        {"symbol": "ETHUSDT", "sumOpenInterest": "0", "sumOpenInterestValue": "0", "timestamp": 0},
        {"period_ms": 300_000},
    ),
    (
        long_short_ratio,
        {"longShortRatio": "0", "longAccount": "0", "shortAccount": "1", "timestamp": 0},
        {"period_ms": 300_000},
    ),
    (funding_rate, {"symbol": "ETHUSDT", "fundingRate": "0", "fundingTime": 0}, {}),
]


@pytest.mark.parametrize("parser,payload,options", STATISTICS)
def test_derivatives_statistics_are_unsupported_for_spot_not_fake_zero(parser, payload, options):
    with pytest.raises(ValidationError, match="unsupported"):
        parser(SPOT, payload, available_at_ms=100, **options)
    record = parser(USDM, json.dumps(payload).encode(), available_at_ms=100, **options)
    assert record.raw is not None and record.source == "rest"


@pytest.mark.parametrize("parser,payload,options", STATISTICS)
@pytest.mark.parametrize("field,value", [("symbol", "BTCUSDT"), ("timestamp", True)])
def test_statistics_reject_wrong_identity_or_timestamp(parser, payload, options, field, value):
    row = dict(payload)
    if field == "timestamp":
        field = next(key for key in ("time", "timestamp", "fundingTime") if key in row)
    row[field] = value
    with pytest.raises(ProtocolError):
        parser(USDM, row, available_at_ms=100, **options)


@pytest.mark.parametrize("value", ["NaN", "Infinity", 0.1, True])
def test_funding_does_not_accept_nonfinite_or_inexact_float_rates(value):
    with pytest.raises(ProtocolError):
        funding_rate(
            USDM, {"symbol": "ETHUSDT", "fundingRate": value, "fundingTime": 0}, available_at_ms=100
        )


def test_missing_metric_is_not_synthesized_and_invalid_proportions_are_rejected():
    with pytest.raises(ProtocolError):
        open_interest_history(
            USDM,
            {"symbol": "ETHUSDT", "sumOpenInterest": "1", "timestamp": 0},
            available_at_ms=100,
            period_ms=300_000,
        )
    with pytest.raises(ProtocolError):
        long_short_ratio(
            USDM,
            {"longShortRatio": "2", "longAccount": "1.1", "shortAccount": "0", "timestamp": 0},
            available_at_ms=100,
            period_ms=300_000,
        )
