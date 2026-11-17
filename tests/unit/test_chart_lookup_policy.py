"""Identity-scoped bounded failures without clocks, network, Qt or job mutation."""

import pytest

from omega.gui.chart_lookup_policy import ChartLookupPolicy


@pytest.mark.parametrize("limit", [False, True, None, 0, -1, 33, 8.0, "8"])
def test_invalid_lookup_limits_are_rejected(limit):
    with pytest.raises(ValueError):
        ChartLookupPolicy(limit)


@pytest.mark.parametrize("identity", [None, False, 1, ""])
def test_binding_requires_nonempty_native_identity(identity):
    with pytest.raises(ValueError):
        ChartLookupPolicy().bind(identity)


@pytest.mark.parametrize("limit", [1, 8, 32])
def test_same_identity_cannot_refill_saturated_budget(limit):
    policy = ChartLookupPolicy(limit)
    assert not policy.permits_auto("queued")
    policy.bind("job-a")
    for count in range(1, limit + 1):
        policy.bind("job-a")
        assert policy.record_failure() is (count == limit)
        assert policy.permits_auto("unknown") is (count < limit)
    for _ in range(100):
        assert policy.record_failure() is True
        policy.bind("job-a")
        assert not policy.permits_auto("unknown")
    policy.bind("job-b")
    assert policy.permits_auto("unknown")


def test_only_accepted_response_restores_same_identity_after_manual_lookup():
    policy = ChartLookupPolicy()
    policy.bind("original-id")
    for _ in range(8):
        policy.record_failure()
    # Manual same-ID dispatch binds again, without restoring automatic work.
    policy.bind("original-id")
    assert not policy.permits_auto("unknown")
    assert policy.record_failure() is True
    policy.accept_response()
    assert policy.permits_auto("running")
    for _ in range(7):
        assert policy.record_failure() is False
    assert policy.record_failure() is True


@pytest.mark.parametrize(
    "state",
    [
        "off",
        "complete",
        "failed",
        "cancelled",
        "expired",
        "disconnected",
        "lookup_paused",
        "source_unverifiable",
        "select_bar",
        [],
        None,
    ],
)
def test_only_pending_unknown_states_can_automatically_poll(state):
    policy = ChartLookupPolicy()
    policy.bind("job")
    assert not policy.permits_auto(state)
    assert all(policy.permits_auto(active) for active in ("queued", "running", "unknown"))
