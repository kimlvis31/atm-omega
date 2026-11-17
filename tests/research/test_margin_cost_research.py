"""Shared cost policy identity and actual owner/replay parity, never wallet adjustments."""

import json
from copy import deepcopy
from decimal import Decimal as D
from hashlib import sha256
from threading import Event

import pytest

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.research.artifacts import Cancelled, bounded_artifact_hash, read_status_json
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.research.margin_simulation import (
    MODEL,
    _document,
    _verify_cost_policy_view,
    simulate_margin,
)
from omega.research.result_summary import summarize_simulation
from omega.runtime import margin_research
from omega.runtime.margin_research import _snapshot_cost_policy, run_margin_simulation_job
from omega.runtime.margin_session import MarginTradingSession

BTC, ETH = (Instrument("binance", Market.USDM, name) for name in ("BTCUSDT", "ETHUSDT"))


def policy(**changes):
    return {
        "schema_version": 1,
        "model": "linear_usdt_ohlcv_cost_v1",
        "slippage_rate": "0.0100",
        "execution_price_bound_rate": "0.1000",
        "mark_drift_bound_rate": "0.1000",
        "entry_delay_ms": 1,
        "reduction_delay_ms": 0,
        "limit_price_policy": "adverse_then_cap_v1",
    } | changes


def configuration():
    account = {"account_id": "cost-replay", "balance": "1000", "positions": []}
    strategies = []
    for instrument, owner, quantity, mode in (
        (BTC, "long", "1", "cross"),
        (ETH, "short", "-1", "isolated"),
    ):
        position = {
            "instrument": instrument.key,
            "owner_id": owner,
            "margin_mode": mode,
            "leverage": 10,
            "budget_margin": "100",
        }
        if mode == "isolated":
            position["isolated_collateral"] = "100"
        account["positions"].append(position)
        strategies.append(
            {
                "id": owner,
                "instrument": instrument.key,
                "module": "target_quantity",
                "configuration": {"quantity": quantity},
                "analysis": {},
                "constraints": {"leverage": "10", "can_short": True},
            }
        )
    return account, strategies


def rows(count=4):
    return [
        MarketRow(item.key, i * 60000, D(100), D(103), D(97), D(100), D(10))
        for i in range(count)
        for item in (BTC, ETH)
    ]


def tables():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "cost_research_fixture",
            "a" * 64,
        )
        for item in (BTC, ETH)
    )


def funding():
    return {
        "schema_version": 1,
        "model": "linear_usdt_scenario_rate_v1",
        "points": [
            {
                "instrument": item.key,
                "event_time_ms": 180000,
                "source_available_at_ms": 181000,
                "reference_time_ms": 180000,
                "rate": ".0100",
                "reference_price": "100",
            }
            for item in (BTC, ETH)
        ],
    }


def replay(root, *, count=4, **changes):
    account, strategies = configuration()
    parameters = dict(
        root=root,
        account=account,
        strategies=strategies,
        start_ms=0,
        brackets=tables(),
        session_factory=MarginTradingSession,
    )
    return simulate_margin(rows(count), **(parameters | changes))


def job_parameters(tmp_path, **changes):
    source = tmp_path / "source.csv"
    write_rows(source, rows())
    dataset = tmp_path / "dataset"
    export_dataset(source, dataset, {"split_ratios": [0.5, 0.25, 0.25]})
    account, strategies = configuration()
    return {
        "dataset": str(dataset),
        "account": account,
        "strategies": strategies,
        "start_ms": 0,
        "brackets": [
            {
                k: v
                for k, v in table.items()
                if k in {"instrument", "observed_at_ms", "available_at_ms", "rows"}
            }
            for table in _document(tables())
        ],
        "cost_policy": policy(),
    } | changes


def recorded_result():
    from omega.application.margin_cost import MarginCostPolicy

    raw = policy()
    parsed = MarginCostPolicy.parse(raw)
    return {
        "schema_version": 1,
        "state": "complete",
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "quote_asset": "USDT",
        "initial_wallet": "100",
        "wallet": "99.9",
        "fills": [
            {
                "timestamp_ms": 60000,
                "instrument": BTC.key,
                "quantity": "1",
                "price": "101",
                "fee": "0.1",
                "realized_pnl": "0",
            }
        ],
        "events": [],
        "equity": [{"timestamp_ms": 120000, "wallet": "99.9", "equity": "99.9"}],
        "cost_policy": {
            "schema_version": 1,
            "model": parsed.document()["model"],
            "raw_document": raw,
            "canonical_document": parsed.document(),
            "policy_sha256": parsed.sha256,
            "request_sha256": sha256(
                json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        },
    }


def test_none_policy_preserves_complete_legacy_and_funding_results_and_kwargs(tmp_path):
    options = []

    def factory(*args, **kwargs):
        options.append(deepcopy(kwargs))
        return MarginTradingSession(*args, **kwargs)

    for index, extra in enumerate(({}, {"funding": funding()})):
        before = replay(tmp_path / f"before{index}", session_factory=factory, **extra)
        after = replay(
            tmp_path / f"after{index}", session_factory=factory, cost_policy=None, **extra
        )
        assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)
        assert "cost_policy" not in after
        assert "cost_policy" not in summarize_simulation(after)
        assert not any("cost_policy" in key for key in after["lineage"])
    assert all("cost_policy" not in value for value in options)


def test_policy_metadata_is_explanatory_not_a_second_wallet_debit():
    record = recorded_result()
    summary = summarize_simulation(record)
    legacy = deepcopy(record)
    del legacy["cost_policy"]
    before = summarize_simulation(legacy)
    for field in ("costs", "reconciliation", "metrics", "curve"):
        assert summary[field] == before[field]
    assert summary["reconciliation"]["matches"] is True
    assert summary["cost_policy"] == record["cost_policy"]
    assert "no additional slippage wallet debit" in " ".join(summary["limitations"])


def test_policy_status_requires_exact_scope_hash_canonical_document_and_native_false():
    from omega.application.margin_cost import MarginCostPolicy

    costs = MarginCostPolicy.parse(policy())
    status = {
        "scope": "fixed_adverse_full_fill_delay_v1",
        "policy": costs.document(),
        "policy_sha256": costs.sha256,
        "venue_execution": False,
    }
    _verify_cost_policy_view({"cost_policy": status}, costs)
    for key, value in (
        ("scope", "legacy"),
        ("policy_sha256", "a" * 64),
        ("policy", policy()),
        ("venue_execution", 0),
        ("venue_execution", True),
    ):
        with pytest.raises(ProtocolError, match="confirm its original cost policy"):
            _verify_cost_policy_view({"cost_policy": status | {key: value}}, costs)
    with pytest.raises(ProtocolError, match="confirm its original cost policy"):
        _verify_cost_policy_view({}, costs)


def test_custom_factory_cannot_silently_ignore_cost_policy(tmp_path):
    calls = []

    def ignored(*args, **kwargs):
        calls.append(kwargs.pop("cost_policy"))
        return MarginTradingSession(*args, **kwargs)

    with pytest.raises(ProtocolError, match="confirm its original cost policy"):
        replay(tmp_path, count=1, cost_policy=policy(), session_factory=ignored)
    assert calls == [policy()]


def test_policy_snapshot_preserves_raw_strings_and_distinguishes_canonical_hash(tmp_path):
    from omega.application.margin_cost import MarginCostPolicy

    raw = policy()
    frozen, meta = _snapshot_cost_policy(raw, tmp_path / "input-cost-policy")
    raw["slippage_rate"] = "0.2"
    assert frozen["slippage_rate"] == "0.0100"
    assert read_status_json(tmp_path / meta["source_file"]) == frozen
    canonical = read_status_json(tmp_path / meta["normalized_file"])
    assert canonical == MarginCostPolicy.parse(frozen).document()
    assert meta["policy_sha256"] == MarginCostPolicy.parse(frozen).sha256
    assert (
        meta["request_sha256"]
        == sha256(json.dumps(frozen, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    )
    assert meta["scope"] == "request_json_snapshot"
    assert meta["source_file_sha256"] == bounded_artifact_hash(
        tmp_path / meta["source_file"], max_bytes=2097152
    )


@pytest.mark.parametrize(
    "change",
    [
        {"entry_delay_ms": True},
        {"slippage_rate": 0.01},
        {"unknown": "field"},
        {"entry_delay_ms": 86400001},
    ],
)
def test_invalid_policy_is_rejected_before_financial_namespace_or_factory(tmp_path, change):
    calls = []
    root = tmp_path / "no-owner"
    with pytest.raises(ValidationError):
        replay(root, cost_policy=policy(**change), session_factory=lambda *a, **k: calls.append(k))
    assert not calls and not root.exists()


def test_actual_cost_and_funding_session_parity_final_facts_and_no_double_cost(tmp_path):
    source = policy()
    requests = []

    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        original = session.cost_quote_summaries

        def quotes(*, limit):
            requests.append(limit)
            return original(limit=limit)

        session.cost_quote_summaries = quotes
        return session

    result = replay(
        tmp_path / "research", cost_policy=source, funding=funding(), session_factory=factory
    )
    assert requests == [10000]
    account, strategies = configuration()
    session = MarginTradingSession(
        tmp_path / "virtual",
        account,
        strategies,
        0,
        create=True,
        cost_policy=source,
        funding=funding(),
    )
    try:
        session.resume("human-start")
        for index in range(4):
            frame = tuple(
                Candle(
                    item, index * 60000, (index + 1) * 60000, D(100), D(103), D(97), D(100), D(10)
                )
                for item in (BTC, ETH)
            )
            session.run_once(frame, as_of_ms=(index + 1) * 60000, brackets=tables())
        final = session.execution.snapshot()
        assert result["account"] == _document(final.account)
        assert result["pending"] == _document(final.pending)
        assert result["cost_quotes"] == session.cost_quote_summaries()
    finally:
        session.close()
    assert result["model"] == MODEL
    assert result["cost_policy"]["raw_document"] == source
    assert result["cost_policy"]["model"] == source["model"]
    assert len(result["fills"]) == 2 and len(result["funding_history"]) == 2
    assert {D(row["price"]) for row in result["fills"]} == {D(101), D(99)}
    assert {row["event_time_ms"] for row in result["fills"]} == {120000}
    assert sum((D(row["fee"]) for row in result["fills"]), D(0)) == D(".2")
    assert D(result["wallet"]) == D("999.8")
    assert result["cost_quotes"]["total"] == 2
    assert {D(row["raw_price"]) for row in result["cost_quotes"]["rows"]} == {D(100)}
    assert {D(row["explanatory_slippage_amount"]) for row in result["cost_quotes"]["rows"]} == {
        D(1)
    }
    summary = summarize_simulation(result)
    assert D(summary["costs"][0]["fee"]) == D(".2")
    assert D(summary["costs"][0]["funding_net"]) == 0
    assert "slippage" not in summary["costs"][0]
    assert summary["reconciliation"]["matches"] is True
    assert "no second slippage cash debit" in " ".join(result["assumptions"])


def test_zero_policy_is_explicit_and_delayed_eod_orders_are_not_completed(tmp_path):
    zero = policy(
        slippage_rate="0",
        execution_price_bound_rate="0",
        mark_drift_bound_rate="0",
        entry_delay_ms=0,
    )
    result = replay(tmp_path / "zero", count=1, cost_policy=zero)
    assert result["cost_policy"]["raw_document"] == zero
    assert result["lineage"]["cost_policy_sha256"] == result["cost_policy"]["policy_sha256"]
    delayed = replay(
        tmp_path / "delayed",
        count=1,
        cost_policy=policy(entry_delay_ms=86400000, reduction_delay_ms=60001),
    )
    assert delayed["fills"] == [] and delayed["wallet"] == "1000"
    assert delayed["pending_orders"] == len(delayed["pending"]) == 2
    assert all(row["eligible_at_ms"] > 60000 for row in delayed["pending"])
    assert "reduction delay also delays protective" in " ".join(delayed["assumptions"])
    assert delayed["cost_quotes"]["state"] == "ready" and delayed["cost_quotes"]["rows"] == []


def test_resume_refuses_changed_raw_identity_even_with_equivalent_canonical_policy(tmp_path):
    from omega.application.margin_cost import MarginCostPolicy

    original = policy()
    replay(tmp_path, count=1, cost_policy=original)
    changed = original | {"slippage_rate": "0.01"}
    assert MarginCostPolicy.parse(changed).sha256 == MarginCostPolicy.parse(original).sha256
    calls = []
    with pytest.raises(ProtocolError, match="changed"):
        replay(
            tmp_path,
            count=1,
            cost_policy=changed,
            resume=True,
            session_factory=lambda *a, **k: calls.append(k),
        )
    assert calls == []


def test_cancel_resume_retains_actual_cost_facts_and_policy_identity(tmp_path):
    full = replay(tmp_path / "full", cost_policy=policy(), funding=funding())
    cancel = Event()

    def progress(value):
        if value["completed_frames"] == 3:
            cancel.set()

    with pytest.raises(Cancelled):
        replay(
            tmp_path / "restart",
            cost_policy=policy(),
            funding=funding(),
            cancel=cancel,
            progress=progress,
        )
    restored = replay(tmp_path / "restart", cost_policy=policy(), funding=funding(), resume=True)
    for key in (
        "account",
        "fills",
        "equity",
        "funding_history",
        "pending",
        "cost_policy",
        "cost_quotes",
        "wallet",
    ):
        assert restored[key] == full[key], key


def test_real_job_snapshots_policy_common_sources_and_final_account(tmp_path):
    parameters = job_parameters(tmp_path)
    partial = tmp_path / "partial"
    partial.mkdir()
    response = run_margin_simulation_job(partial, tmp_path / "final", parameters, None)
    result = read_status_json(partial / "simulation.json")
    record = result["cost_policy"]
    assert result["configuration"]["cost_policy"] == parameters["cost_policy"]
    assert record["source_snapshot"]["scope"] == "request_json_snapshot"
    assert response["summary"]["cost_policy_sha256"] == record["policy_sha256"]
    names = {row["file"] for row in result["implementation"]["files"]}
    assert {
        "code-snapshot/application/margin_cost.py",
        "code-snapshot/runtime/margin_session_costs.py",
        "code-snapshot/strategies/neural_signal/module.py",
    } <= names
    assert summarize_simulation(result)["reconciliation"]["matches"] is True


def test_changed_policy_snapshot_blocks_result_publication(tmp_path, monkeypatch):
    parameters = job_parameters(tmp_path)
    partial = tmp_path / "partial"
    partial.mkdir()
    original = margin_research.simulate_margin

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        (partial / "input-cost-policy/source.json").write_text("{}", encoding="utf-8")
        return result

    monkeypatch.setattr(margin_research, "simulate_margin", changed)
    with pytest.raises(ValidationError, match="snapshot changed"):
        run_margin_simulation_job(partial, tmp_path / "final", parameters, None)
    assert not (partial / "simulation.json").exists()


@pytest.mark.parametrize("field", ["policy_sha256", "request_sha256", "canonical_document"])
def test_result_reader_rejects_cost_policy_identity_tampering_without_repricing(field):
    result = recorded_result()
    changed = deepcopy(result)
    changed["cost_policy"][field] = {} if field == "canonical_document" else "f" * 64
    with pytest.raises(ValueError, match="cost policy"):
        summarize_simulation(changed)


@pytest.mark.parametrize(
    "change", ["unavailable", "truncated", "overflow", "wrong_price", "wrong_side"]
)
def test_quote_reader_rejects_incomplete_or_different_applied_facts(change):
    result = recorded_result()
    fill = result["fills"][0]
    fill.update(fill_id="fill", order_id="order", event_time_ms=60000, available_at_ms=120000)
    row = {
        "fill_id": "fill",
        "order_id": "order",
        "instrument": BTC.key,
        "side": "BUY",
        "quantity": "1",
        "raw_price": "100",
        "final_price": "101",
        "fee": "0.1",
        "explanatory_slippage_amount": "1",
        "policy_sha256": result["cost_policy"]["policy_sha256"],
        "issued_at_ms": 60000,
        "eligible_at_ms": 60000,
        "event_time_ms": 60000,
        "available_at_ms": 120000,
    }
    result["cost_quotes"] = {
        "schema_version": 1,
        "model": policy()["model"],
        "state": "ready",
        "total": 1,
        "truncated": False,
        "rows": [row],
    }
    summary = summarize_simulation(result)
    assert summary["cost_quotes"] == result["cost_quotes"]
    assert summary["reconciliation"]["matches"] is True
    if change == "unavailable":
        result["cost_quotes"]["state"] = "unavailable"
    elif change == "truncated":
        result["cost_quotes"]["truncated"] = True
    elif change == "overflow":
        result["cost_quotes"]["total"] = 10001
    elif change == "wrong_price":
        row["final_price"] = "102"
    else:
        row["side"] = "SELL"
    with pytest.raises(ValueError, match="cost quote"):
        summarize_simulation(result)


def test_unavailable_final_accessor_cannot_publish_cost_explanations(tmp_path):
    def factory(*args, **kwargs):
        session = MarginTradingSession(*args, **kwargs)
        original = session.cost_quote_summaries

        def unavailable(**options):
            return original(**options) | {"state": "unavailable"}

        session.cost_quote_summaries = unavailable
        return session

    with pytest.raises(ProtocolError, match="quote explanations are unavailable"):
        replay(tmp_path, count=1, cost_policy=policy(), session_factory=factory)
