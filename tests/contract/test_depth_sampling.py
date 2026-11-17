"""Synthetic synchronized books; expected bins derive from actual price ranges."""

from dataclasses import replace
from decimal import Decimal as D
from decimal import localcontext

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.depth_book import BookView
from omega.infrastructure.binance.depth_sampling import DEPTH_FIELDS, DepthMinuteSampler, depth_bins

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
REFS = ("a" * 64, "b" * 64)


def view(quantity="1", *, floor="95", ceiling="105"):
    return BookView(
        12,
        tuple((D(price), D(quantity)) for price in ("99.9", "99.8", "99", "98", "97", "96", "95")),
        tuple(
            (D(price), D(quantity))
            for price in ("100.1", "100.2", "101", "102", "103", "104", "105")
        ),
        D(floor),
        D(ceiling),
    )


def sampler(**kwargs):
    return DepthMinuteSampler(INSTRUMENT, price_precision=2, quote_precision=2, **kwargs)


def observe(target, when, book=None, **changes):
    args = dict(
        event_time_ms=when, received_at_ms=when, accepted_at_ms=when, generation=1, raw_refs=REFS
    )
    args.update(changes)
    return target.observe(book or view(), **args)


def test_exact_eta_spatial_boundaries_and_twelve_notional_names():
    fields, coverage, center = depth_bins(view(), price_precision=2, quote_precision=2)
    values = dict(fields)
    assert set(values) == set(DEPTH_FIELDS) and coverage == 1 and center == 100
    assert values["DEPTH_BIDS0"] == D("199.70")
    assert values["DEPTH_ASKS0"] == D("200.30")
    assert [values[f"DEPTH_BIDS{i}"] for i in range(1, 6)] == list(
        map(D, ("99", "98", "97", "96", "95"))
    )
    assert [values[f"DEPTH_ASKS{i}"] for i in range(1, 6)] == list(
        map(D, ("101", "102", "103", "104", "105"))
    )


def test_unknown_outer_range_is_omitted_not_zero_and_precision_context_is_independent():
    with localcontext() as context:
        context.prec = 3
        values, coverage, center = depth_bins(
            view(floor="99", ceiling="101"), price_precision=2, quote_precision=2
        )
    assert 0 < coverage < 1 and center == 100
    assert set(dict(values)) == {"DEPTH_BIDS0", "DEPTH_BIDS1", "DEPTH_ASKS0", "DEPTH_ASKS1"}
    assert "DEPTH_BIDS5" not in dict(values)


def test_known_empty_levels_are_actual_zero_and_rounded_center_controls_coverage():
    book = BookView(0, ((D("99.9"), D(1)),), ((D("100.1"), D(1)),), D(95), D(105))
    values, coverage, _ = depth_bins(book, price_precision=2, quote_precision=2)
    assert coverage == 1 and dict(values)["DEPTH_BIDS5"] == 0
    # Unrounded midpoint100.005 permits floor95.004; rounded100.00 needs95.000.
    edge = BookView(1, ((D(100), D(1)),), ((D("100.01"), D(1)),), D("95.004"), D("105.006"))
    assert edge.covers()
    values, coverage, _ = depth_bins(edge, price_precision=2, quote_precision=2)
    assert coverage < 1 and "DEPTH_BIDS5" not in dict(values)


def test_latest_minute_snapshot_and_actual_availability_not_average_or_close_clamp():
    target = sampler()
    target.begin(1, started_at_ms=0)
    observe(target, 1000)
    observe(target, 59999, view("2"), received_at_ms=60001, accepted_at_ms=60002)
    result = target.finalize(0, as_of_ms=60003)
    assert result.observation.quality == "observed" and result.observation.coverage == 1
    assert result.observation.mapping()["DEPTH_BIDS0"] == D("399.40")
    assert result.observation.event_time_ms == 59999 and result.observation.available_at_ms == 60003
    assert result.observation.mapping()["DEPTH_RECEIVED_AT_MS"] == 60001
    assert result.observation.source == INSTRUMENT.key + ":binance_depth_ws_v1"
    assert result.raw_refs == REFS


def test_start_prefix_gap_stale_and_absence_are_separate_from_spatial_coverage():
    target = sampler()
    target.begin(1, started_at_ms=1)
    observe(target, 59999)
    assert target.finalize(0, as_of_ms=60000).observation.quality == "partial"
    observe(target, 119999)
    assert target.finalize(60000, as_of_ms=120000).observation.quality == "observed"
    missing = target.finalize(120000, as_of_ms=180000).observation
    assert missing.quality == "missing" and not missing.values and missing.coverage == 0
    stale = sampler()
    stale.begin(1, started_at_ms=0)
    observe(stale, 1000)
    assert dict(stale.finalize(0, as_of_ms=60000).diagnostics)["reason"] == "stale_snapshot"
    disconnected = sampler()
    disconnected.begin(1, started_at_ms=0)
    observe(disconnected, 59999)
    disconnected.break_stream("sequence_gap")
    result = disconnected.finalize(0, as_of_ms=60000)
    assert (
        result.observation.quality == "partial"
        and dict(result.diagnostics)["reason"] == "sequence_gap"
    )


def test_new_generation_starts_clean_and_closed_minute_cannot_be_revised():
    target = sampler()
    target.begin(1, started_at_ms=0)
    observe(target, 59999)
    target.finalize(0, as_of_ms=60000)
    assert observe(target, 59999, view("2")) is False
    target.break_stream()
    target.begin(2, started_at_ms=60001)
    with pytest.raises(ProtocolError, match="generation"):
        observe(target, 119999)
    observe(target, 119999, generation=2)
    assert target.finalize(60000, as_of_ms=120000).observation.quality == "partial"
    observe(target, 179999, generation=2)
    assert target.finalize(120000, as_of_ms=180000).observation.quality == "observed"
    with pytest.raises(ValidationError):
        target.finalize(120000, as_of_ms=180001)


@pytest.mark.parametrize(
    "change",
    [
        {"event_time_ms": 1001},
        {"generation": True},
        {"raw_refs": ()},
        {"raw_refs": ("a" * 64,) * 2},
    ],
)
def test_future_unowned_or_unarchived_observation_is_rejected(change):
    target = sampler()
    target.begin(1, started_at_ms=0)
    with pytest.raises((ValidationError, ProtocolError)):
        observe(target, 1000, **change)


@pytest.mark.parametrize("changes", [{"price_precision": True}, {"quote_precision": 19}])
def test_precisions_are_native_and_bounded(changes):
    kwargs = {"price_precision": 2, "quote_precision": 2, **changes}
    with pytest.raises(ValidationError):
        depth_bins(view(), **kwargs)


def test_invalid_book_types_and_bounded_minute_buffer_are_rejected():
    with pytest.raises(ValidationError):
        depth_bins(replace(view(), bids=((100.0, D(1)),)), price_precision=2, quote_precision=2)
    target = sampler()
    target.begin(1, started_at_ms=0)
    observe(target, 1000)
    with pytest.raises(ProtocolError):
        observe(target, 180000)
