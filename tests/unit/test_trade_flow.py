"""Trade flow aggregation boundaries and evidence; all source events are synthetic."""

from dataclasses import replace
from decimal import Decimal, localcontext

import pytest

from omega.analysis.order_flow import LegacyOrderFlowFeatures, OrderFlowConfiguration
from omega.application.trade_flow import TradeFlowAggregator, TradeFlowCoverage
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import MarketFeatureFrame, TradeFlowEvent
from omega.domain.models import Candle, Instrument, Market

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
SOURCE = "binance_usdm_agg_trade"


def event(identifier, when, **changes):
    return TradeFlowEvent(
        **{
            "instrument": INSTRUMENT,
            "aggregate_id": identifier,
            "first_trade_id": identifier * 3,
            "last_trade_id": identifier * 3 + 2,
            "price": Decimal("100.01"),
            "quantity": Decimal(".03"),
            "buyer_is_maker": False,
            "trade_time_ms": when,
            "available_at_ms": when + 1,
            "source": SOURCE,
            **changes,
        }
    )


def aggregator(**options):
    return TradeFlowAggregator(INSTRUMENT, 60_000, source=SOURCE, generation=1, **options)


def finalize(target, *, coverage=None, as_of_ms=120_001):
    return target.finalize(60_000, as_of_ms=as_of_ms, generation=1, coverage=coverage)


def test_exact_flow_maker_direction_underlying_trade_count_and_half_open_minute():
    target = aggregator()
    buy = event(2, 60_000)
    sell = event(3, 119_999, quantity=Decimal(".07"), buyer_is_maker=True)
    after = event(4, 120_000)
    for item in (after, sell, buy):
        target.accept(item, generation=1)  # Receipt order is deliberately reversed.
    frame = finalize(target, coverage=TradeFlowCoverage(event(1, 59_999), after, 1))
    assert frame.channel == "trade_flow" and frame.quality == "observed" and frame.coverage == 1
    assert frame.mapping() == {
        "AGGTRADE_QUANTITYBUY": Decimal(".03"),
        "AGGTRADE_QUANTITYSELL": Decimal(".07"),
        "AGGTRADE_NOTIONALBUY": Decimal("3.0003"),
        "AGGTRADE_NOTIONALSELL": Decimal("7.0007"),
        "AGGTRADE_NTRADESBUY": 3,
        "AGGTRADE_NTRADESSELL": 3,
        "AGGTRADE_EVENTCOUNT": 2,
    }
    assert frame.window_start_ms == 60_000 and frame.window_end_ms == 120_000
    assert frame.available_at_ms == 120_001 and frame.usable_at(120_001)
    assert not frame.usable_at(120_000)
    assert target._events == {4: after}  # Boundary event belongs to the next minute.


def test_adjacent_bracketing_ids_prove_observed_zero_but_timer_or_bool_does_not():
    target = aggregator()
    proof = TradeFlowCoverage(event(1, 50_000), event(2, 120_000), 1)
    frame = finalize(target, coverage=proof)
    assert frame.quality == "observed" and frame.mapping()["AGGTRADE_QUANTITYBUY"] == 0
    assert frame.mapping()["AGGTRADE_EVENTCOUNT"] == 0
    missing = finalize(aggregator())
    assert missing.quality == "missing" and missing.values == () and missing.coverage == 0
    with pytest.raises(ValidationError):
        finalize(aggregator(), coverage=True)


def test_zero_id_anchor_and_equal_trade_times_are_valid_without_inventing_a_gap():
    target = aggregator()
    target.accept(event(1, 60_000), generation=1)
    target.accept(event(2, 60_000), generation=1)
    result = finalize(target, coverage=TradeFlowCoverage(event(0, 59_999), event(3, 120_000), 1))
    assert result.quality == "observed" and result.mapping()["AGGTRADE_NTRADESBUY"] == 6


def test_reconnection_id_gap_is_partial_even_when_observed_prices_look_complete():
    target = aggregator()
    target.accept(event(2, 60_000), generation=1)
    target.accept(event(4, 119_999), generation=1)  # ID3 could have been missed.
    frame = finalize(target, coverage=TradeFlowCoverage(event(1, 59_999), event(5, 120_000), 1))
    assert frame.quality == "partial" and frame.coverage == 0
    assert frame.mapping()["AGGTRADE_EVENTCOUNT"] == 2 and not frame.usable_at(120_001)
    absent = finalize(
        aggregator(), coverage=TradeFlowCoverage(event(1, 59_999), event(3, 120_000), 1)
    )
    assert absent.quality == "missing" and absent.values == ()


def test_same_trade_delivered_twice_is_idempotent_but_conflicting_id_is_rejected_atomically():
    target = aggregator()
    item = event(2, 60_000)
    assert target.accept(item, generation=1)
    assert not target.accept(replace(item, available_at_ms=70_000), generation=1)
    with pytest.raises(ProtocolError, match="conflicting"):
        target.accept(replace(item, quantity=Decimal(".04")), generation=1)
    frame = finalize(target)
    assert frame.mapping()["AGGTRADE_QUANTITYBUY"] == Decimal(".03")
    assert not target.accept(item, generation=1)  # Retained exact finalized duplicate.
    with pytest.raises(ProtocolError, match="finalized"):
        target.accept(event(99, 60_000), generation=1)


def test_late_repair_cannot_silently_rewrite_a_finalized_partial_minute():
    target = aggregator()
    target.accept(event(2, 60_000), generation=1)
    first = finalize(target)
    with pytest.raises(ProtocolError):
        target.accept(event(3, 70_000), generation=1)
    with pytest.raises(ValidationError):
        finalize(target)
    assert first.mapping()["AGGTRADE_EVENTCOUNT"] == 1 and target.next_open_ms == 120_000


def test_unavailable_event_and_anchor_rejections_preserve_buffer_and_delivery_cursor():
    target = aggregator()
    item = event(2, 60_000, available_at_ms=130_000)
    target.accept(item, generation=1)
    with pytest.raises(ValidationError, match="not yet available"):
        finalize(target)
    assert target.next_open_ms == 60_000 and target._events == {2: item}
    proof = TradeFlowCoverage(event(1, 59_999), event(3, 120_000, available_at_ms=140_000), 1)
    with pytest.raises(ValidationError, match="not yet available"):
        finalize(target, as_of_ms=130_001, coverage=proof)
    frame = finalize(target, as_of_ms=140_001, coverage=proof)
    assert frame.quality == "observed" and target.next_open_ms == 120_000


@pytest.mark.parametrize(
    "before,after",
    [
        (event(1, 60_000), event(3, 120_000)),
        (event(1, 59_999), event(3, 119_999)),
        (event(3, 59_999), event(1, 120_000)),
        (event(1, 59_999), event(3, 130_000, available_at_ms=130_001)),
    ],
)
def test_invalid_or_future_coverage_anchors_do_not_advance_minute(before, after):
    target = aggregator()
    with pytest.raises((ProtocolError, ValidationError)):
        finalize(target, coverage=TradeFlowCoverage(before, after, 1))
    assert target.next_open_ms == 60_000


def test_coverage_cannot_exclude_known_trade_ids_or_hide_reversed_trade_time():
    target = aggregator()
    target.accept(event(5, 60_000), generation=1)
    with pytest.raises(ProtocolError, match="exclude"):
        finalize(target, coverage=TradeFlowCoverage(event(1, 59_999), event(2, 120_000), 1))
    target = aggregator()
    target.accept(event(2, 80_000), generation=1)
    target.accept(event(3, 70_000), generation=1)
    with pytest.raises(ProtocolError, match="reverse"):
        finalize(target, coverage=TradeFlowCoverage(event(1, 59_999), event(4, 120_000), 1))


def test_coverage_conflict_and_source_mismatch_are_not_accepted_as_repair():
    target = aggregator()
    right = event(3, 120_000)
    target.accept(right, generation=1)
    with pytest.raises(ProtocolError, match="conflicts"):
        finalize(
            target,
            coverage=TradeFlowCoverage(event(1, 59_999), replace(right, price=Decimal(1)), 1),
        )
    with pytest.raises(ValidationError, match="dataset"):
        target.accept(replace(right, aggregate_id=4, source="other_dataset"), generation=1)
    assert len(target._events) == 1


def test_capacity_window_and_generation_fences_never_discard_existing_observations():
    target = aggregator(max_pending_events=1, max_minutes=2)
    item = event(2, 60_000)
    target.accept(item, generation=1)
    for candidate in (event(3, 70_000), event(4, 180_000)):
        with pytest.raises(ProtocolError):
            target.accept(candidate, generation=1)
    with pytest.raises(ProtocolError):
        target.accept(item, generation=0)
    with pytest.raises(ProtocolError):
        target.finalize(60_000, as_of_ms=120_001, generation=0)
    assert target._events == {2: item} and target.next_open_ms == 60_000


def test_reset_reports_discard_and_requires_a_new_generation_and_new_coverage():
    target = aggregator()
    target.accept(event(2, 60_000), generation=1)
    assert target.reset(120_000, generation=2) == {
        "previous_generation": 1,
        "discarded_events": 1,
        "discarded_minutes": 1,
    }
    with pytest.raises(ValidationError):
        target.reset(120_000, generation=2)
    with pytest.raises(ProtocolError):
        target.finalize(
            120_000,
            as_of_ms=180_001,
            generation=2,
            coverage=TradeFlowCoverage(event(1, 119_999), event(2, 180_000), 1),
        )
    result = target.finalize(120_000, as_of_ms=180_001, generation=2)
    assert result.quality == "missing"


def test_exact_notional_and_sum_do_not_inherit_small_process_decimal_precision():
    target = aggregator()
    item = event(
        2,
        60_000,
        price=Decimal("12345678901234567890.123456789"),
        quantity=Decimal(".1234567890123456789"),
    )
    target.accept(item, generation=1)
    with localcontext() as context:
        context.prec = 80
        expected = item.price * item.quantity
    with localcontext() as context:
        context.prec = 6
        frame = finalize(target)
    assert frame.mapping()["AGGTRADE_NOTIONALBUY"] == expected


def test_extreme_arithmetic_is_rejected_with_no_partial_commit():
    target = aggregator()
    item = event(2, 60_000, price=Decimal("1e5000"))
    target.accept(item, generation=1)
    with pytest.raises(ValidationError, match="arithmetic"):
        finalize(target)
    assert target._events == {2: item} and target.next_open_ms == 60_000


def test_aggregator_auxiliary_frame_drives_two_minute_nes_seed_without_field_translation():
    target = aggregator()
    buy, sell, after = (
        event(2, 60_000),
        event(3, 120_000, quantity=Decimal(".01"), buyer_is_maker=True),
        event(4, 180_000),
    )
    for item in (buy, sell, after):
        target.accept(item, generation=1)
    first = finalize(target, coverage=TradeFlowCoverage(event(1, 59_999), sell, 1))
    second = target.finalize(
        120_000, as_of_ms=180_001, generation=1, coverage=TradeFlowCoverage(buy, after, 1)
    )
    features = LegacyOrderFlowFeatures(INSTRUMENT, [OrderFlowConfiguration(0, "NES", 2)])
    outputs = []
    for observation in (first, second):
        candle = Candle(
            INSTRUMENT,
            observation.window_start_ms,
            observation.window_end_ms,
            *(Decimal(value) for value in ("100", "101", "99", "100", "1")),
        )
        frame = MarketFeatureFrame(candle, observation.available_at_ms, (observation,))
        outputs.append(features.update_frame(frame))
    assert outputs[0]["0_NES_0_NES"] is None
    assert outputs[1]["0_NES_0_NES"] == 1.0001  # Mean of +3.0003 and -1.0001.
    assert features.input_ready and features.input_quality["0_NES_0"]["ready"]


@pytest.mark.parametrize("generation", [-1, True, 2**63])
def test_generation_is_a_bounded_nonnegative_integer(generation):
    with pytest.raises(ValidationError):
        TradeFlowAggregator(INSTRUMENT, 60_000, source=SOURCE, generation=generation)


@pytest.mark.parametrize(
    "changes",
    [
        {"instrument": "ETHUSDT"},
        {"aggregate_id": True},
        {"aggregate_id": -1},
        {"first_trade_id": 100},
        {"buyer_is_maker": 1},
        {"price": 1.5},
        {"price": Decimal("NaN")},
        {"quantity": Decimal(0)},
        {"trade_time_ms": True},
        {"available_at_ms": 253_402_300_799_001},
        {"available_at_ms": 59_999},
        {"source": ""},
    ],
)
def test_normalized_event_contract_rejects_invalid_inputs(changes):
    with pytest.raises(ValidationError):
        event(2, 60_000, **changes)
