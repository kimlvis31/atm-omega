"""Registered raw configuration provenance, cache-only and fail-closed reads."""

import builtins
import json
import threading
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

import pytest
from test_margin_session import BTC, configuration
from test_margin_strategy_costs import policy
from test_margin_trading import Session, Source, await_value

from omega.application.margin_funding import FundingPoint, FundingSchedule
from omega.domain.errors import ValidationError
from omega.domain.external_features import ExternalFeatureBinding
from omega.runtime.margin_configuration import configuration_parameters
from omega.runtime.margin_session import prepare_margin_configuration
from omega.runtime.margin_trading import MarginTradingService


def parameters():
    account, strategies = configuration()
    return {
        "account": account,
        "strategies": strategies,
        "start_ms": 60000,
        "fee_rate": "0.00100",
        "brackets": [
            {
                "instrument": BTC.key,
                "observed_at_ms": 0,
                "available_at_ms": 0,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": "1000.00",
                        "maintenance_rate": "0.01",
                        "maintenance_amount": "0",
                        "max_leverage": 20,
                    },
                    {
                        "notional_floor": "1000.00",
                        "notional_cap": "1000000",
                        "maintenance_rate": "0.02",
                        "maintenance_amount": "10",
                        "max_leverage": 10,
                    },
                ],
            }
        ],
    }


def cached(tmp_path):
    owner = MarginTradingService(tmp_path, None, lambda: None, core_epoch="configuration")
    owner.ready = True
    owner.data = {
        "accounts": {"session-test": {"parameters": parameters(), "creation_state": "created"}}
    }
    owner.views = {"session-test": {"account_id": "session-test", "state": "ready"}}
    return owner


def test_canonical_raw_provenance_preserves_numeric_strings_all_tiers_and_optional_inputs():
    original = parameters()
    original["cost_policy"] = policy().document()
    original["cost_policy"]["slippage_rate"] = "0.00100"
    original["funding"] = FundingSchedule(
        (FundingPoint(BTC, 60000, 60010, 60000, Decimal(".001"), Decimal(100)),)
    ).document()
    original["external_feature_wait_ms"] = 15000
    binding = ExternalFeatureBinding(
        "causal", "model", "stream", BTC, "a" * 64, "b" * 64, ("NN.x",)
    )
    original["strategies"][0]["external_features"] = [binding.document()]
    original["strategies"][0]["configuration"]["future_data"] = {"label": "한글", "v": [None, 1.5]}
    before = deepcopy(original)
    copied, digest = configuration_parameters(original, account_id="session-test")
    assert copied == original == before and copied is not original
    assert copied["brackets"][0]["rows"][1]["notional_floor"] == "1000.00"
    assert copied["cost_policy"]["slippage_rate"] == "0.00100"
    assert copied["strategies"][0]["external_features"] == [binding.document()]
    assert (
        digest
        == sha256(
            json.dumps(original, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
    )
    copied["strategies"][0]["configuration"]["quantity"] = "changed"
    assert original == before
    changed = deepcopy(original)
    changed["fee_rate"] = "0.001"
    assert configuration_parameters(changed)[1] != digest
    assert configuration_parameters(dict(reversed(list(original.items()))))[1] == digest


@pytest.mark.parametrize(
    "bad", [float("nan"), float("inf"), Decimal("1"), (1,), {1: "bad"}, {True: "bad"}, object()]
)
def test_non_native_or_nonfinite_cached_values_are_not_coerced(bad):
    value = parameters()
    value["strategies"][0]["configuration"]["future_data"] = bad
    with pytest.raises(ValidationError):
        configuration_parameters(value)


@pytest.mark.parametrize("fault", ["bytes", "escaped_bytes", "depth", "nodes", "cycle"])
def test_structural_and_encoded_capacities_are_checked(fault):
    value = parameters()
    if fault in {"bytes", "escaped_bytes"}:
        extra = ("x" * 131073) if fault == "bytes" else ("한" * 24000)
    elif fault == "nodes":
        extra = [None] * 20001
    else:
        extra = []
        if fault == "cycle":
            extra.append(extra)
        else:
            for _ in range(33):
                extra = [extra]
    value["strategies"][0]["configuration"]["extra"] = extra
    with pytest.raises(ValidationError):
        configuration_parameters(value)


@pytest.mark.parametrize("wrapper", ["top", "account", "position", "strategy"])
def test_runtime_or_private_state_cannot_be_added_to_registered_wrappers(wrapper):
    value = parameters()
    target = {
        "top": value,
        "account": value["account"],
        "position": value["account"]["positions"][0],
        "strategy": value["strategies"][0],
    }[wrapper]
    target["private_runtime_path"] = "SECRET"
    with pytest.raises(ValidationError):
        configuration_parameters(value)
    with pytest.raises(ValidationError):
        configuration_parameters(parameters(), account_id="other")


@pytest.mark.parametrize("where", ["parameters", "creation_state", "view_state", "view_identity"])
def test_native_copy_does_not_execute_custom_hooks(tmp_path, where):
    calls = []

    class Hostile(dict):
        def __deepcopy__(self, memo):
            calls.append("deepcopy")
            raise AssertionError("must not execute")

        def __repr__(self):
            calls.append("repr")
            raise AssertionError("must not execute")

        def items(self):
            calls.append("items")
            raise AssertionError("must not execute")

        def __eq__(self, other):
            calls.append("equality")
            raise AssertionError("must not execute")

        def __ne__(self, other):
            calls.append("inequality")
            raise AssertionError("must not execute")

    owner = cached(tmp_path)
    slot = owner.data["accounts"]["session-test"]
    if where == "parameters":
        slot["parameters"]["strategies"][0]["configuration"] = Hostile()
    elif where == "creation_state":
        slot["creation_state"] = Hostile()
    else:
        owner.views["session-test"]["state" if where == "view_state" else "account_id"] = Hostile()
    result = owner.read_configuration("session-test")
    assert result["state"] == "unavailable"
    assert result["reason_code"] == (
        "configuration_invalid" if where == "parameters" else "configuration_unconfirmed"
    )
    assert not calls


@pytest.mark.parametrize(
    "fault",
    [
        "starting",
        "closed",
        "error",
        "missing",
        "creating",
        "unknown",
        "blocked",
        "view",
        "view_error",
        "identity",
        "invalid",
    ],
)
def test_unconfirmed_cache_never_returns_an_empty_or_ready_configuration(tmp_path, fault):
    owner = cached(tmp_path)
    slot = owner.data["accounts"]["session-test"]
    if fault == "starting":
        owner.ready = False
    elif fault == "closed":
        owner.closed = True
    elif fault == "error":
        owner.error = "SECRET"
    elif fault == "missing":
        owner.data["accounts"].clear()
    elif fault in {"creating", "unknown"}:
        slot["creation_state"] = fault
    elif fault == "blocked":
        owner.blocked_accounts.add("session-test")
    elif fault == "view":
        owner.views["session-test"]["state"] = "unknown"
    elif fault == "view_error":
        owner.views["session-test"]["error"] = "SECRET"
    elif fault == "identity":
        owner.views["session-test"]["account_id"] = "other"
    else:
        slot["parameters"]["account"]["ledger"] = "SECRET"
    result = owner.read_configuration("session-test")
    assert set(result) == {
        "schema_version",
        "scope",
        "state",
        "account_id",
        "configuration_sha256",
        "parameters",
        "reason_code",
    }
    assert result["schema_version"] == 1 and result["scope"] == "virtual_margin_configuration"
    assert result["state"] == "unavailable" and result["account_id"] == "session-test"
    assert result["parameters"] is None and result["configuration_sha256"] is None
    assert result["reason_code"] and "SECRET" not in json.dumps(result)


@pytest.mark.parametrize("bad", ["../bad", "", True, None, 1, "a" * 129])
def test_read_identity_is_validated_before_cache_selection(tmp_path, bad):
    with pytest.raises(ValidationError):
        cached(tmp_path).read_configuration(bad)


def test_get_uses_only_bounded_cache_and_does_not_wait_for_financial_fence(tmp_path, monkeypatch):
    owner = cached(tmp_path)
    original = deepcopy(owner.data)

    def forbidden(*args, **kwargs):
        raise AssertionError("GET cannot invoke IO, financial/session work or clocks")

    class Private:
        def __deepcopy__(self, memo):
            forbidden()

    owner.views["session-test"]["account"] = Private()
    owner.sessions = Private()
    for name in (
        "_guard",
        "_read",
        "_write",
        "_open",
        "_refresh",
        "clock_ms",
        "entry_gate",
        "source_factory",
    ):
        monkeypatch.setattr(owner, name, forbidden)
    result = []
    with monkeypatch.context() as patch:
        patch.setattr(builtins, "open", forbidden)
        patch.setattr(Path, "open", forbidden)
        with owner.mutation_lock:
            thread = threading.Thread(
                target=lambda: result.append(owner.read_configuration("session-test"))
            )
            thread.start()
            thread.join(1)
            assert not thread.is_alive()
    assert result[0]["state"] == "ready" and result[0]["parameters"] == parameters()
    result[0]["parameters"]["account"]["balance"] = "0"
    assert owner.data == original and not owner.operations and not owner.queue


def test_concurrent_cache_replacement_produces_one_exact_provenance(tmp_path):
    owner = cached(tmp_path)
    first, second = parameters(), parameters()
    second["account"]["balance"] = "2000.00"
    allowed = {configuration_parameters(value)[1] for value in (first, second)}
    errors = []

    def writer():
        for index in range(100):
            with owner.condition:
                owner.data["accounts"]["session-test"]["parameters"] = (
                    first if index % 2 else second
                )

    thread = threading.Thread(target=writer)
    thread.start()
    for _ in range(100):
        result = owner.read_configuration("session-test")
        if (
            result["configuration_sha256"] not in allowed
            or configuration_parameters(result["parameters"])[1] != result["configuration_sha256"]
        ):
            errors.append(result)
    thread.join(1)
    assert not thread.is_alive() and not errors


def test_actual_registry_creation_restart_reads_cache_without_session_or_files(
    tmp_path, monkeypatch
):
    class ValidatedSession(Session):
        prepare_configuration = staticmethod(prepare_margin_configuration)

    def start():
        owner = MarginTradingService(
            tmp_path,
            None,
            lambda: Source(),
            core_epoch="configuration",
            session_factory=ValidatedSession,
        )
        owner.start()
        await_value(owner.snapshot, lambda value: value["state"] == "ready")
        return owner

    owner = start()
    try:
        owner.submit("create", "configure", parameters())
        await_value(lambda: owner.status("create"), lambda value: value["state"] == "completed")
        first = owner.read_configuration("session-test")
        assert first["state"] == "ready" and first["parameters"] == parameters()
    finally:
        owner.close()
    owner = start()
    try:
        before = deepcopy(owner.operations)
        with monkeypatch.context() as patch:

            def forbidden(*args, **kwargs):
                raise AssertionError("cached GET must not invoke a session")

            patch.setattr(owner.sessions["session-test"], "snapshot", forbidden)
            patch.setattr(owner, "clock_ms", forbidden)
            patch.setattr(builtins, "open", forbidden)
            patch.setattr(Path, "open", forbidden)
            assert owner.read_configuration("session-test") == first
        assert owner.operations == before
    finally:
        owner.close()
