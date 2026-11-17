"""Independent per-minute execution and checkpoint cursor consistency regressions."""

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
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.modules import ModuleDescriptor

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")


def frame(index):
    return MarketFeatureFrame(
        Candle(
            INSTRUMENT,
            index * 60000,
            (index + 1) * 60000,
            *(Decimal(value) for value in ("10", "12", "8", "11", "4")),
        ),
        (index + 1) * 60000,
    )


def aggregation_graph():
    provider = CandleAggregation(INSTRUMENT, (0,))
    return MarketFeatureGraph(
        INSTRUMENT, (FeatureNode("bars", provider, public_fields=provider.public_fields),)
    )


def resign(saved):
    saved["sha256"] = sha256(
        json.dumps(
            saved["payload"], sort_keys=True, allow_nan=False, separators=(",", ":")
        ).encode()
    ).hexdigest()
    return saved


def test_later_asof_with_identical_closed_inputs_does_not_execute_stateful_node_twice():
    class Counter:
        descriptor = ModuleDescriptor("counter", "1", frozenset({"OHLCV"}), True)

        def __init__(self):
            self.count = 0

        def update(self, candle):
            self.count += 1
            return {"count": self.count}

    current = MarketFeatureGraph(INSTRUMENT, (FeatureNode("counter", Counter()),))
    first = frame(0)
    assert current.update_frame(first) == {"count": 1}
    later = replace(first, as_of_ms=first.as_of_ms + 100)
    assert current.update_frame(later) == {"count": 1}
    assert current._nodes[0].feature.count == 1
    assert current.update_frame(frame(1)) == {"count": 2}


@pytest.mark.parametrize("case", ["lagging_node", "cold_graph_warm_node"])
def test_valid_checksum_checkpoint_rejects_graph_cursor_that_disagrees_with_provider(case):
    original = aggregation_graph()
    original.update_frame(frame(0))
    early = original.checkpoint()
    original.update_frame(frame(1))
    late = original.checkpoint()
    invalid = deepcopy(late)
    if case == "lagging_node":
        # Replaying t1 against the t0 provider computes the right public output,
        # but must not validate a restored graph whose actual provider remains t0.
        invalid["payload"]["state"]["_nodes"] = early["payload"]["state"]["_nodes"]
    else:
        cold = aggregation_graph().checkpoint()
        for name in ("_last", "_last_frame", "_output"):
            invalid["payload"]["state"][name] = cold["payload"]["state"][name]
    before = original.checkpoint()
    with pytest.raises(ValidationError):
        original.restored(resign(invalid), INSTRUMENT)
    assert original.checkpoint() == before


@pytest.mark.parametrize("nodes", [(object(),), (FeatureNode("bad", object()),)])
def test_constructor_rejects_untyped_or_non_updatable_node_definitions(nodes):
    with pytest.raises(ValidationError):
        MarketFeatureGraph(INSTRUMENT, nodes)


@pytest.mark.parametrize(
    "descriptor",
    [object(), ModuleDescriptor("bad", "1", "OHLCV"), ModuleDescriptor("bad", "1", frozenset({1}))],
)
def test_constructor_does_not_expand_malformed_descriptor_fields_into_channels(descriptor):
    class Provider:
        def update(self, candle):
            return {"value": 1}

    provider = Provider()
    provider.descriptor = descriptor
    with pytest.raises(ValidationError):
        MarketFeatureGraph(INSTRUMENT, (FeatureNode("bad", provider),))
