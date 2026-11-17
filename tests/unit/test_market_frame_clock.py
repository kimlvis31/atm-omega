"""Retrospective evaluation never forges actual knowledge/availability timestamps."""

import json
from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256

import pytest

from omega.analysis.aggregation import CandleAggregation
from omega.analysis.graph import FeatureNode
from omega.analysis.market_graph import MarketFeatureGraph
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.infrastructure.postgres.config import DatabaseSettings, database_destination_id

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def frame():
    bar = Candle(INSTRUMENT, 0, 60000, *(Decimal(v) for v in ("10", "12", "8", "11", "4")))
    return MarketFeatureFrame(bar, 1_000_000)


def graph():
    bars = CandleAggregation(INSTRUMENT, (0,))
    return MarketFeatureGraph(
        INSTRUMENT, (FeatureNode("bars", bars, public_fields=bars.public_fields),)
    )


@pytest.mark.parametrize("retro", [False, True])
def test_checkpoint_roundtrip_keeps_explicit_clock_and_old_live_tag_schema(retro):
    current = graph()
    value = frame()
    if retro:
        value = replace(value, reference_time_ms=value.candle.close_time_ms)
    current.update_frame(value)
    saved = json.loads(json.dumps(current.checkpoint()))
    row = saved["payload"]["state"]["_last_frame"]["market_frame"]
    assert set(row) == {"candle", "as_of_ms", "inputs"} | (
        {"reference_time_ms"} if retro else set()
    )
    restored = graph().restored(saved, INSTRUMENT)
    assert restored.checkpoint() == saved
    assert restored._last_frame.as_of_ms == 1_000_000
    assert restored._last_frame.retrospective is retro
    assert restored._last_frame.feature_time_ms == (60000 if retro else 1_000_000)


@pytest.mark.parametrize("reference", [0, 60001, True, 60000.0, -1])
def test_only_explicit_bar_close_can_be_a_retrospective_clock(reference):
    with pytest.raises(ValidationError):
        replace(frame(), reference_time_ms=reference)


def test_actual_availability_is_still_required_in_retrospective_mode():
    value = frame()
    source = AuxiliaryInput(
        "metric",
        (("METRIC_OPENINTEREST", Decimal(1)),),
        0,
        60000,
        0,
        1_000_001,
        source="actual_late_receipt",
    )
    with pytest.raises(ValidationError, match="availability"):
        replace(value, inputs=(source,), reference_time_ms=60000)


def test_one_minute_cannot_change_evaluation_clock_without_explicit_replay():
    current = graph()
    value = frame()
    current.update_frame(value)
    before = current.checkpoint()
    with pytest.raises(ValidationError, match="immutable"):
        current.update_frame(replace(value, reference_time_ms=60000))
    assert current.checkpoint() == before


def test_invalid_reference_in_a_resigned_checkpoint_is_rejected_atomically():
    current = graph()
    current.update_frame(frame())
    before = current.checkpoint()
    corrupt = deepcopy(before)
    corrupt["payload"]["state"]["_last_frame"]["market_frame"]["reference_time_ms"] = 60001
    corrupt["sha256"] = sha256(
        json.dumps(
            corrupt["payload"], sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    with pytest.raises(ValidationError):
        current.restored(corrupt, INSTRUMENT)
    assert current.checkpoint() == before


def test_database_destination_identity_is_stable_across_credentials_but_not_namespaces():
    settings = DatabaseSettings("127.0.0.1", 55433, "omega_test", "omega_app", "first")
    identity = database_destination_id(settings)
    assert identity == database_destination_id(
        replace(settings, password="second", connect_timeout_s=1)
    )
    assert identity != database_destination_id(replace(settings, dbname="omega_test_peer"))
