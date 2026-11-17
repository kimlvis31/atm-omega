"""Execution policy is explicit; full saved analysis remains registered data."""

import json
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256

import pytest
from test_margin_session import BTC, configuration
from test_margin_session_features import NNRegistry, Provider, external_configuration, external_run

from omega.domain.errors import ValidationError
from omega.domain.margin import FillEvent
from omega.runtime.margin_configuration import configuration_parameters
from omega.runtime.margin_session import MarginTradingSession, prepare_margin_configuration
from omega.runtime.margin_trading import MarginTradingService

POLICY = {"version": 1, "policy": "required_fields"}


def documents():
    account, strategies = configuration(tef=True)
    account["positions"] = account["positions"][:1]
    strategies = strategies[:1]
    strategies[0]["analysis"] = {
        "moving_averages": [
            {"interval_id": 0, "ema_period": 1000, "mmacd_periods": [2, 3], "signal_period": 2}
        ],
        "market_sentiment": [{"interval_id": 0, "nSamples": 2, "maximum_metric_age_ms": 330000}],
        "order_flow": [{"interval_id": 0, "analysis_code": "WOI", "nSamples": 2}],
    }
    return account, strategies


def test_registration_copy_preserves_full_profile_policy_and_exact_canonical_identity():
    account, strategies = documents()
    strategies[0]["analysis_execution"] = deepcopy(POLICY)
    original = {"account": account, "strategies": strategies, "start_ms": 0, "brackets": []}
    before = deepcopy(original)
    copied, digest = configuration_parameters(original)
    assert copied == before == original
    assert copied is not original and copied["strategies"] is not strategies
    assert copied["strategies"][0]["analysis"]["moving_averages"][0]["ema_period"] == 1000
    assert (
        digest
        == sha256(
            json.dumps(before, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest()
    )


def test_only_explicit_policy_changes_prepared_dependencies_and_keeps_original_document():
    account, strategies = documents()
    before = deepcopy(strategies)
    original = prepare_margin_configuration(account, strategies, 0)
    assert original[2] == ({"id": "btc-ts", "instrument": BTC},)
    assert original[4]["btc-ts"].descriptor.required_fields == frozenset(
        {"OHLCV", "trade_flow", "depth", "metric"}
    )
    strategies[0]["analysis_execution"] = deepcopy(POLICY)
    prepared = prepare_margin_configuration(account, strategies, 0)
    plan = prepared[2][0]["analysis_execution_plan"]
    assert prepared[4]["btc-ts"].descriptor.required_fields == frozenset({"OHLCV"})
    assert plan["required_fields"] == ["0_MMACD_MSDELTAABSMAREL"]
    assert not plan["selected_configuration"]["market_sentiment"]
    assert not plan["selected_configuration"]["order_flow"]
    assert plan["selected_configuration"]["moving_averages"][0]["ema_period"] is None
    assert strategies[0]["analysis"] == before[0]["analysis"]
    assert {
        key: value for key, value in strategies[0].items() if key != "analysis_execution"
    } == before[0]


@pytest.mark.parametrize(
    "policy",
    [
        None,
        False,
        {},
        {"version": True, "policy": "required_fields"},
        {"version": 1.0, "policy": "required_fields"},
        {"version": 2, "policy": "required_fields"},
        {"version": 1, "policy": "all"},
        {"version": 1, "policy": "required_fields", "extra": 1},
    ],
)
def test_invalid_explicit_policy_never_creates_a_financial_namespace(tmp_path, policy):
    account, strategies = documents()
    strategies[0]["analysis_execution"] = policy
    root = tmp_path / "invalid"
    with pytest.raises(ValidationError):
        MarginTradingSession(root, account, strategies, 0, create=True)
    assert not root.exists()


def test_unused_invalid_definition_is_not_excused_by_execution_projection(tmp_path):
    account, strategies = documents()
    strategies[0]["analysis_execution"] = deepcopy(POLICY)
    strategies[0]["analysis"]["market_sentiment"][0]["nSamples"] = True
    root = tmp_path / "invalid-unused"
    with pytest.raises(ValidationError):
        MarginTradingSession(root, account, strategies, 0, create=True)
    assert not root.exists()


def test_shorthand_profile_is_not_silently_converted_by_the_explicit_saved_ca_policy(tmp_path):
    account, strategies = documents()
    strategies[0].update(analysis={}, analysis_execution=deepcopy(POLICY))
    root = tmp_path / "shorthand"
    with pytest.raises(ValidationError):
        MarginTradingSession(root, account, strategies, 0, create=True)
    assert not root.exists()


def test_unaware_injected_session_factory_cannot_silently_ignore_an_explicit_policy(tmp_path):
    calls = []

    def unaware(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("unsupported factory must not be constructed")

    account, rows = documents()
    rows[0]["analysis_execution"] = deepcopy(POLICY)
    owner = MarginTradingService(
        tmp_path, None, lambda: None, core_epoch="projection", session_factory=unaware
    )
    try:
        assert owner.analysis_execution_scope is None
        with pytest.raises(ValidationError, match="does not support"):
            owner._open({"account": account, "strategies": rows, "start_ms": 0}, create=True)
        assert not calls
        assert not (tmp_path / "state" / "margin-sessions").exists()
    finally:
        owner.close()


@pytest.mark.parametrize("mixed,empty", [(False, True), (False, False), (True, False)])
def test_registered_nn_only_and_mixed_projection_keep_binding_authority_and_actual_fills(
    tmp_path, mixed, empty
):
    # The declared cached authority port is synthetic; all session/checkpoint/
    # financial writes and provider pin/guard interfaces are the real path.
    root = tmp_path / "external-projection"
    provider = Provider(root)
    account, rows = external_configuration(provider, mixed=mixed)
    full = {"moving_averages": [], "momentum": []} if empty else documents()[1][0]["analysis"]
    if mixed:
        full["moving_averages"][0]["ema_period"] = 2
    rows[0].update(analysis=deepcopy(full), analysis_execution=deepcopy(POLICY))
    original = deepcopy(rows)
    session = MarginTradingSession(
        root,
        account,
        rows,
        0,
        create=True,
        registry=NNRegistry(mixed=mixed),
        external_feature_provider=provider,
        clock_ms=lambda: provider.clock,
    )
    try:
        frozen = session._meta["configuration"]["analysis_execution"]["btc-ts"]
        assert frozen["plan"]["required_fields"] == (["0_EMA_0_EMA"] if mixed else [])
        assert frozen["module_descriptor"]["required_fields"] == (
            ["0_EMA_0_EMA", "NN.score"] if mixed else ["NN.score"]
        )
        assert session.analysis_input_channels == {}
        assert session._schema_version == 2
        session.resume("explicit-authorized-start")
        # Cached deliveries arrive10ms after close; the mixed EMA also needs
        # its second sample. Preserve original next-minute ceiling eligibility.
        for index in range(4):
            external_run(session, provider, index)
        assert any(
            isinstance(row.fact, FillEvent) for row in session.execution.snapshot().account.ledger
        )
        assert "NN.score" not in session._meta["last_frame"]["features"]["btc-ts"]
        assert session._meta["configuration"]["strategies"] == original == rows
        assert provider.lookups > 0 and provider.guard_calls > 0
    finally:
        session.close()


@pytest.mark.parametrize("damage", ["missing", "foreign", "duplicate", "unregistered"])
def test_projection_cannot_discard_invalid_external_binding_authority_before_financial_creation(
    tmp_path, damage
):
    root = tmp_path / "invalid-external"
    provider = Provider(root)
    account, rows = external_configuration(provider)
    rows[0].update(
        analysis={"moving_averages": [], "momentum": []},
        analysis_execution=deepcopy(POLICY),
    )
    if damage == "missing":
        del rows[0]["external_features"]
    elif damage == "foreign":
        rows[0]["external_features"] = [replace(provider.binding, instance_id="foreign").document()]
    elif damage == "duplicate":
        rows[0]["external_features"] *= 2
    else:
        provider.binding = None
    with pytest.raises(ValidationError):
        MarginTradingSession(
            root,
            account,
            rows,
            0,
            create=True,
            registry=NNRegistry(),
            external_feature_provider=provider,
            clock_ms=lambda: provider.clock,
        )
    assert provider.lookups == 0
    assert not root.exists()
