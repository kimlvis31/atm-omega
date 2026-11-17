"""Minute provenance cannot be reassigned, made mutable or made available early."""

import pickle
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal

import pytest

from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "BTCUSDT")
CANDLE = Candle(INSTRUMENT, 60000, 120000, *(Decimal(v) for v in (10, 11, 9, 10, 1)))


def sample(**changes):
    return AuxiliaryInput(
        **{
            "channel": "trade_flow",
            "values": (("SELL", Decimal(3)), ("BUY", Decimal(4))),
            "window_start_ms": 60000,
            "window_end_ms": 120000,
            "event_time_ms": 119999,
            "available_at_ms": 120010,
            "source": "binance_aggtrade",
            **changes,
        }
    )


def test_frame_has_canonical_immutable_fields_and_supports_normal_spawn_serialization():
    value = sample()
    assert value.values == (("BUY", Decimal(4)), ("SELL", Decimal(3)))
    frame = MarketFeatureFrame(CANDLE, 120010, (value,))
    assert pickle.loads(pickle.dumps(frame)) == frame  # IPC encoding, not disk model loading.
    with pytest.raises(FrozenInstanceError):
        frame.inputs = ()
    value.mapping()["BUY"] = Decimal(999)
    assert frame.input("trade_flow").mapping()["BUY"] == 4
    assert frame.input("depth") is None


def test_observations_cannot_be_used_before_available_or_moved_to_another_minute():
    value = sample()
    assert not value.usable_at(120000) and value.usable_at(120010)
    with pytest.raises(ValidationError, match="availability"):
        MarketFeatureFrame(CANDLE, 120000, (value,))
    with pytest.raises(ValidationError, match="range"):
        MarketFeatureFrame(
            CANDLE, 180000, (replace(value, window_start_ms=120000, window_end_ms=180000),)
        )
    with pytest.raises(ValidationError):
        MarketFeatureFrame(CANDLE, 120010, (value, value))


@pytest.mark.parametrize("quality", ["missing", "unsupported"])
def test_absent_channels_are_explicit_and_do_not_pretend_to_be_observed_zeros(quality):
    value = sample(quality=quality, values=(), coverage=Decimal(0))
    assert not value.usable_at(120010)
    assert value.mapping() == {}
    with pytest.raises(ValidationError):
        sample(quality=quality, values=(("BUY", Decimal(0)),), coverage=Decimal(0))


@pytest.mark.parametrize(
    "quality,coverage", [("partial", "0.5"), ("carried", "1"), ("observed", "0.9")]
)
def test_partial_and_carried_are_not_complete_raw_observations(quality, coverage):
    assert not sample(quality=quality, coverage=Decimal(coverage)).usable_at(120010)


@pytest.mark.parametrize(
    "changes",
    [
        {"values": [("BUY", Decimal(1))]},
        {"values": (("BUY", True),)},
        {"values": (("BUY", 1.5),)},
        {"values": (("BUY", Decimal("NaN")),)},
        {"values": (("BUY", 1), ("BUY", 2))},
        {"channel": "../depth"},
        {"window_start_ms": True},
        {"window_end_ms": 180000},
        {"available_at_ms": 119998},
        {"coverage": Decimal("1.01")},
        {"source": ""},
        {"quality": "repaired_as_raw"},
    ],
)
def test_malformed_auxiliary_input_is_rejected_at_the_shared_boundary(changes):
    with pytest.raises(ValidationError):
        sample(**changes)
