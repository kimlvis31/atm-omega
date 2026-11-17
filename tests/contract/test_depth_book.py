"""Synthetic snapshot/diff sequences; not an exchange connectivity validation."""

from decimal import Decimal

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.depth_book import DepthBook


def book(market=Market.USDM, **options):
    return DepthBook(Instrument("binance", market, "ETHUSDT"), **options)


def snapshot(identifier=10, **changes):
    return {
        "lastUpdateId": identifier,
        "bids": [["99", "2"], ["90", "1"]],
        "asks": [["101", "3"], ["110", "1"]],
        **changes,
    }


def event(first=10, final=11, previous=9, **changes):
    return {
        "e": "depthUpdate",
        "s": "ETHUSDT",
        "E": 60_000,
        "U": first,
        "u": final,
        "pu": previous,
        "b": [],
        "a": [],
        **changes,
    }


def sync(target):
    generation = target.generation
    target.receive(event(first=9), generation=generation)
    assert target.snapshot(snapshot(), generation=generation)
    return generation


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_buffer_snapshot_bridge_absolute_updates_and_unknown_zero_deletion(market):
    target = book(market)
    generation = target.generation
    target.receive(event(9, 11, b=[["99", "7"], ["80", "0"]]), generation=generation)
    assert target.snapshot(snapshot(), generation=generation)
    assert target.view().bids[0] == (Decimal(99), Decimal(7))
    target.receive(event(12, 13, 11, b=[["99", "4"]]), generation=generation)
    assert target.view().bids[0][1] == 4  # Absolute replacement, not +4.
    assert target.view().covers()


def test_usdm_snapshot_equal_final_id_zero_is_a_valid_first_bridge():
    target = book()
    g = target.generation
    zero = event(0, 0, 0)
    target.receive(zero, generation=g)
    assert target.snapshot(snapshot(0), generation=g)
    assert target.view().update_id == 0
    assert not target.receive(zero, generation=g)
    assert target.receive(event(1, 1, 0), generation=g)


def test_spot_discards_equal_snapshot_events_and_accepts_next_expected_id():
    target = book(Market.SPOT)
    g = target.generation
    target.receive(event(9, 10), generation=g)
    assert not target.snapshot(snapshot(10), generation=g)
    with pytest.raises(ProtocolError, match="not synchronized"):
        target.view()
    assert target.receive(event(11, 11), generation=g)
    assert target.view().update_id == 11


def test_spot_snapshot_older_than_first_buffered_start_requires_refetch():
    target = book(Market.SPOT)
    g = target.generation
    target.receive(event(11, 12), generation=g)
    with pytest.raises(ProtocolError, match="stale"):
        target.snapshot(snapshot(10), generation=g)
    assert target.state == "resync_required"


@pytest.mark.parametrize("first,final", [(8, 9), (9, 10), (10, 12)])
def test_usdm_first_bridge_lower_equal_upper_boundaries(first, final):
    target = book()
    g = target.generation
    target.receive(event(first, final), generation=g)
    ready = target.snapshot(snapshot(10), generation=g)
    assert ready is (final >= 10)


@pytest.mark.parametrize("market", [Market.SPOT, Market.USDM])
def test_continuity_gap_invalidates_book_and_requires_new_generation(market):
    target = book(market)
    g = sync(target)
    with pytest.raises(ProtocolError):
        target.receive(event(14, 15, 13), generation=g)
    assert target.state == "resync_required"
    with pytest.raises(ProtocolError):
        target.view()
    with pytest.raises(ProtocolError):
        target.snapshot(snapshot(), generation=g)
    assert target.reset() != g


def test_duplicates_old_events_and_noop_are_safe_but_conflict_is_not():
    target = book()
    g = sync(target)
    change = event(12, 12, 11)
    assert target.receive(change, generation=g)
    assert not target.receive(change, generation=g)
    assert not target.receive(event(1, 2, 0), generation=g)
    with pytest.raises(ProtocolError):
        target.receive(event(12, 12, 11, b=[["99", "9"]]), generation=g)


def test_reset_and_close_fence_late_snapshot_and_diff_without_poisoning_current_owner():
    target = book()
    old = sync(target)
    current = target.reset()
    with pytest.raises(ProtocolError, match="stale"):
        target.snapshot(snapshot(), generation=old)
    assert target.state == "waiting_snapshot"
    sync(target)
    with pytest.raises(ProtocolError, match="stale"):
        target.receive(event(), generation=old)
    assert target.state == "synced" and target.generation == current
    target.close()
    with pytest.raises(ProtocolError, match="closed"):
        target.receive(event(), generation=current)


def test_limited_snapshot_coverage_is_not_expanded_by_new_outside_levels():
    target = book()
    g = target.generation
    target.receive(event(9, 11), generation=g)
    target.snapshot(snapshot(bids=[["99", "2"]], asks=[["101", "2"]]), generation=g)
    assert not target.view().covers()
    target.receive(event(12, 12, 11, b=[["80", "2"]], a=[["120", "2"]]), generation=g)
    assert not target.view().covers()
    assert target.view().bid_floor == 99 and target.view().ask_ceiling == 101


@pytest.mark.parametrize(
    "changes",
    [
        {"b": [["102", "1"]]},
        {"b": [["99", "-1"]]},
        {"b": [["0", "1"]]},
        {"b": [["99", "1"], ["99.0", "2"]]},
        {"a": "bad"},
        {"pu": True},
        {"U": 15, "u": 14},
        {"E": "60000"},
        {"s": "BTCUSDT"},
        {"e": "bookTicker"},
    ],
)
def test_invalid_diff_cannot_remain_synced(changes):
    target = book()
    g = sync(target)
    with pytest.raises(ProtocolError):
        target.receive(event(12, 13, 11, **changes), generation=g)
    assert target.state == "resync_required"


def test_pending_and_total_level_capacity_are_fail_closed():
    target = book(max_pending=1)
    g = target.generation
    target.receive(event(), generation=g)
    with pytest.raises(ProtocolError):
        target.receive(event(12, 13, 11), generation=g)
    target = book(max_levels=2)
    g = sync(target)
    with pytest.raises(ProtocolError):
        target.receive(event(12, 13, 11, b=[["98", "1"]]), generation=g)


def test_pending_storage_is_bounded_by_total_levels_not_only_event_count():
    target = book(max_levels=2)
    g = target.generation
    target.receive(
        event(b=[["99", "1"], ["98", "1"]], a=[["101", "1"], ["102", "1"]]), generation=g
    )
    with pytest.raises(ProtocolError):
        target.receive(event(12, 13, 11, b=[["97", "1"]]), generation=g)
    assert not target.pending and target.state == "resync_required"


@pytest.mark.parametrize(
    "changes",
    [
        {"bids": []},
        {"asks": []},
        {"lastUpdateId": True},
        {"bids": [["102", "1"]]},
        {"bids": [["99", "0"]]},
        {"asks": [["101", "NaN"]]},
    ],
)
def test_invalid_snapshot_is_never_published(changes):
    target = book()
    with pytest.raises(ProtocolError):
        target.snapshot(snapshot(**changes), generation=target.generation)
    assert target.state == "resync_required"


def test_capacity_and_coverage_ratio_inputs_are_strict():
    with pytest.raises(ValidationError):
        book(max_pending=True)
    target = book()
    sync(target)
    with pytest.raises(ValidationError):
        target.view().covers(0.05)
