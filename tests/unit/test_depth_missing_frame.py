"""The real sampler's empty minute is missing evidence, never a protocol error."""

from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256

import pytest
from test_depth_frames import Store, native

from omega.application.auxiliary_codec import encode_observation
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import ProtocolError
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.binance.depth_sampling import DepthMinuteSampler
from omega.ports.auxiliary import StoredAuxiliary


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("quality", ["missing", "unsupported"])
def test_finalized_empty_sampler_preserves_close_event_and_actual_availability(market, quality):
    instrument = Instrument("binance", market, "BTCUSDT")
    sampler = DepthMinuteSampler(instrument, price_precision=2, quote_precision=8)
    observation = sampler.finalize(0, as_of_ms=60005).observation
    assert observation.quality == "missing" and observation.event_time_ms == 60000
    if quality == "unsupported":
        observation = replace(observation, quality=quality)
    pin = StoredAuxiliary(
        instrument,
        observation,
        1,
        sha256(encode_observation(instrument, observation)).hexdigest(),
        None,
    )
    candle = Candle(instrument, 0, 60000, D(100), D(101), D(99), D(100), D(10))
    builder = MarketFrameBuilder(Store(pin))
    unknown = builder.prepare(candle, as_of_ms=60004, channels=("depth",))
    assert not unknown.pins and unknown.frame.input("depth").quality == "missing"
    known = builder.prepare(candle, as_of_ms=60005, channels=("depth",))
    assert known.pins == (pin,) and known.frame.input("depth") == observation
    assert observation.event_time_ms == 60000 and observation.available_at_ms == 60005
    assert not observation.usable_at(60005)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
@pytest.mark.parametrize("quality,offset", [("observed", 0), ("partial", 0), ("observed", 1)])
def test_nonempty_depth_keeps_strict_inside_minute_event_requirement(market, quality, offset):
    instrument = Instrument("binance", market, "BTCUSDT")
    original = native(instrument, opening=0, quality=quality, available=60005)
    observation = replace(original.observation, event_time_ms=60000 + offset)
    pin = replace(
        original,
        observation=observation,
        checksum=sha256(encode_observation(instrument, observation)).hexdigest(),
    )
    candle = Candle(instrument, 0, 60000, D(100), D(101), D(99), D(100), D(10))
    with pytest.raises(ProtocolError, match="finalized minute"):
        MarketFrameBuilder(Store(pin)).prepare(candle, as_of_ms=60005, channels=("depth",))
