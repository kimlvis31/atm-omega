"""Auxiliary request counts cannot bypass existing weighted/cooldown admission."""

import json

import pytest

from omega.domain.errors import RateLimited
from omega.infrastructure.binance.auxiliary_budget import AuxiliaryPublicBudget
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.shared_budget import SharedRequestBudget


def test_auxiliary_admission_preserves_existing_primary_state_policy_and_counts(tmp_path):
    legacy = SharedRequestBudget(tmp_path / "binance-public-budget.json", clock=lambda: 1000)
    legacy.reserve({"binance-ip": 5})
    before = json.loads(legacy.path.read_text())
    auxiliary = AuxiliaryPublicBudget(tmp_path, clock=lambda: 1000)
    auxiliary.reserve({"binance-ip": 20})
    after = json.loads(legacy.path.read_text())
    assert after["policy"] == before["policy"] == [["binance-ip", 600, 60]]
    assert after["used"][0][-1] == 25
    count = json.loads(auxiliary.requests.path.read_text())
    assert count["policy"] == [["binance-ip", 1000, 300]]
    assert count["used"][0][-1] == 1
    legacy.reserve({"binance-ip": 575})
    with pytest.raises(RateLimited):
        auxiliary.reserve({"binance-ip": 1})
    assert json.loads(auxiliary.requests.path.read_text())["used"][0][-1] == 1


def test_request_count_rejects_even_when_weighted_window_allows_more(tmp_path):
    auxiliary = AuxiliaryPublicBudget(tmp_path, clock=lambda: 1000)
    auxiliary.requests = RequestBudget((Window("binance-ip", 2, 300),), clock=lambda: 1000)
    auxiliary.reserve({"binance-ip": 1})
    auxiliary.reserve({"binance-ip": 1})
    with pytest.raises(RateLimited):
        auxiliary.reserve({"binance-ip": 1})
    # A failed second reservation doesn't refund already spent admission.
    assert json.loads(auxiliary.primary.path.read_text())["used"][0][-1] == 3


def test_provider_cooldown_is_shared_with_existing_candle_collectors(tmp_path):
    auxiliary = AuxiliaryPublicBudget(tmp_path, clock=lambda: 1000)
    auxiliary.cooldown(7)
    legacy = SharedRequestBudget(tmp_path / "binance-public-budget.json", clock=lambda: 1000)
    with pytest.raises(RateLimited) as error:
        legacy.reserve({"binance-ip": 1})
    assert error.value.retry_after == 7
    with pytest.raises(RateLimited):
        auxiliary.requests.reserve({"binance-ip": 1})
