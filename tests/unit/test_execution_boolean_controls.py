"""Native boolean boundaries before order identity, journal and broker effects."""

from dataclasses import replace
from decimal import Decimal

import pytest
from test_execution_safety_audit import ready_owner, request

from omega.domain.errors import TradingBlocked, ValidationError
from omega.domain.safety import BlockReason

INVALID_BOOLEANS = (0, 1, "false", "true", None, 0.0, 1.0, [], {})


@pytest.mark.parametrize("value", INVALID_BOOLEANS)
def test_request_reduce_only_rejects_truthiness_instead_of_coercing(value):
    with pytest.raises(ValidationError, match="native boolean"):
        replace(request(), reduce_only=value)


@pytest.mark.parametrize("value", INVALID_BOOLEANS)
@pytest.mark.parametrize("control", ["state_certain", "reducing"])
@pytest.mark.parametrize("previously_accepted", [False, True])
def test_invalid_submit_controls_reject_before_deduplication_or_any_effect(
    value, control, previously_accepted
):
    owner, broker, gate = ready_owner()
    order = request()
    accepted = None
    if previously_accepted:
        accepted = owner.submit(order, position=Decimal(0), state_certain=True, reducing=False)
    before_events = tuple(dict(event) for event in owner.sink.events)
    before_calls, before_reasons = broker.calls, gate.reasons
    options = {"state_certain": True, "reducing": False, control: value}
    with pytest.raises(ValidationError, match="native booleans"):
        owner.submit(order, position=Decimal(0), **options)
    assert tuple(owner.sink.events) == before_events
    assert broker.calls == before_calls and gate.reasons == before_reasons
    if accepted is None:
        assert not owner._operations and not owner._historical_ids
    else:
        assert owner._operations[order.client_order_id] is accepted
        assert accepted.state == "OBSERVED"


def test_native_false_certain_state_blocks_new_submission_without_effects():
    owner, broker, _gate = ready_owner()
    with pytest.raises(TradingBlocked, match="uncertain"):
        owner.submit(request(), position=Decimal(0), state_certain=False, reducing=False)
    assert not owner.sink.events and broker.calls == 0
    assert not owner._operations and not owner._historical_ids


def test_native_booleans_preserve_original_accepted_identity_even_if_now_uncertain():
    owner, broker, gate = ready_owner()
    order = request()
    assert order.reduce_only is False
    accepted = owner.submit(order, position=Decimal(0), state_certain=True, reducing=False)
    events = tuple(owner.sink.events)
    gate.block(BlockReason.PAUSE)
    duplicate = owner.submit(order, position=Decimal(0), state_certain=False, reducing=False)
    assert duplicate is accepted and broker.calls == 1
    assert tuple(owner.sink.events) == events


def test_native_true_reduction_keeps_protection_available_while_entries_are_paused():
    owner, broker, gate = ready_owner()
    gate.block(BlockReason.PAUSE)
    order = request(reducing=True, quantity="1")
    assert order.reduce_only is True
    operation = owner.submit(order, position=Decimal(1), state_certain=True, reducing=True)
    assert operation.state == "OBSERVED" and broker.calls == 1
    assert owner.sink.events[0]["reducing"] is True
    assert owner.sink.events[0]["reduce_only"] is True
    assert not gate.allows_new_risk
