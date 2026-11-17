"""Exact depth-minute selection/replay, including Spot and late revisions."""

from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.catalog import create_features
from omega.analysis.order_flow import DEPTH_FIELDS
from omega.application.market_frame_builder import MarketFrameBuilder
from omega.domain.errors import ProtocolError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.ports.auxiliary import StoredAuxiliary
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy
from omega.runtime.analysis_replay import replay


class Store:
    def __init__(self, *rows):
        self.rows = list(rows)

    def check_destination(self, expected):
        assert expected == "a" * 64

    def get(self, instrument, channel, source, opening, *, as_of_ms):
        rows = [
            r
            for r in self.rows
            if r.instrument == instrument
            and r.observation.channel == channel
            and r.observation.source == source
            and r.observation.window_start_ms == opening
            and r.observation.available_at_ms <= as_of_ms
        ]
        return max(rows, key=lambda r: r.revision, default=None)

    get_auxiliary = get


def native(
    instrument, opening=600000, *, quality="observed", available=None, revision=1, previous=None
):
    observation = AuxiliaryInput(
        "depth",
        tuple((key, Decimal(200 if "BIDS" in key else 100)) for key in DEPTH_FIELDS),
        opening,
        opening + 60000,
        opening + 59000,
        available or opening + 60001,
        quality=quality,
        coverage=Decimal(1) if quality == "observed" else Decimal(".5"),
        source=instrument.key + ":binance_depth_ws_v1",
    )
    return StoredAuxiliary(
        instrument,
        observation,
        revision,
        sha256(encode_observation(instrument, observation)).hexdigest(),
        previous,
    )


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_depth_pin_replay_preserves_old_revision_and_works_for_spot(tmp_path, market):
    instrument = Instrument("binance", market, "ETHUSDT")
    row = native(instrument)
    candle = Candle(instrument, 600000, 660000, *(Decimal(v) for v in (100, 102, 99, 101, 10)))
    store = Store(row)
    policy = InputPolicy("a" * 64, ("depth",))
    builder = MarketFrameBuilder(store)
    prepared = builder.prepare(candle, as_of_ms=660005, channels=("depth",))
    manifest = AnalysisInputManifest(tmp_path)
    try:
        reference = manifest.stage("1" * 32, "2" * 32, 1, (prepared,), policy)
        store.rows.append(native(instrument, available=670000, revision=2, previous=row.checksum))
        restored = replay(
            manifest, reference, store, expected_analysis_id="1" * 32, expected_policy=policy
        )
        assert restored == (prepared,)
        assert restored[0].pins[0].revision == 1
        assert (
            builder.prepare(candle, as_of_ms=660000, channels=("depth",))
            .frame.input("depth")
            .quality
            == "missing"
        )
    finally:
        manifest.close()


def test_partial_depth_keeps_values_but_cannot_become_ready_or_a_zero_signal():
    instrument = Instrument("binance", Market.USDM, "ETHUSDT")
    first = native(instrument, quality="partial")
    store = Store(first)
    graph = create_features(
        instrument,
        {"0_WOI_0_WOI"},
        {"order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}]},
    )
    candle = Candle(instrument, 600000, 660000, *(Decimal(v) for v in (100, 102, 99, 101, 10)))
    prepared = MarketFrameBuilder(store).prepare(candle, as_of_ms=660005, channels=("depth",))
    assert prepared.frame.input("depth").mapping()
    assert not prepared.frame.input("depth").usable_at(660005)
    assert graph.update_frame(prepared.frame)["0_WOI_0_WOI"] is None


def test_wrong_minute_or_unfinalized_depth_is_protocol_error():
    instrument = Instrument("binance", Market.USDM, "ETHUSDT")
    row = native(instrument)
    candle = Candle(instrument, 600000, 660000, *(Decimal(v) for v in (100, 102, 99, 101, 10)))

    class Wrong(Store):
        def get(self, *args, **kwargs):
            return replace(row, observation=replace(row.observation, available_at_ms=659999))

    with pytest.raises(ProtocolError, match="finalized minute"):
        MarketFrameBuilder(Wrong()).prepare(candle, as_of_ms=660005, channels=("depth",))
