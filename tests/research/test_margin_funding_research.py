"""Actual shared funding ledger/replay, request snapshots and signed display totals."""

import json
from copy import deepcopy
from decimal import Decimal as D
from threading import Event

import pytest

from omega.application.margin_funding import FundingSchedule
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.margin import MarginBracket, MarginBracketSnapshot
from omega.domain.models import Candle, Instrument, Market
from omega.research.artifacts import Cancelled, bounded_artifact_hash, read_status_json
from omega.research.datasets import MarketRow, export_dataset, write_rows
from omega.research.margin_simulation import _document, simulate_margin
from omega.research.result_summary import summarize_simulation
from omega.runtime import margin_research
from omega.runtime.margin_research import _snapshot_funding, run_margin_simulation_job
from omega.runtime.margin_session import MarginTradingSession

BTC, ETH = (Instrument("binance", Market.USDM, name) for name in ("BTCUSDT", "ETHUSDT"))


def configuration():
    account = {"account_id": "funding-replay", "balance": "1000", "positions": []}
    strategies = []
    for instrument, owner, quantity, mode in (
        (BTC, "btc", "1", "cross"),
        (ETH, "eth", "-1", "isolated"),
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


def rows():
    return [
        MarketRow(item.key, index * 60000, D(100), D(103), D(97), D(100), D(10))
        for index in range(4)
        for item in (BTC, ETH)
    ]


def tables():
    return tuple(
        MarginBracketSnapshot(
            item,
            (MarginBracket(D(0), D(1000000), D(".01"), D(0), 20),),
            0,
            0,
            "funding_research_fixture",
            "a" * 64,
        )
        for item in (BTC, ETH)
    )


def schedule():
    points = []
    for timestamp, instrument, rate in (
        (0, BTC, "0"),
        (120000, BTC, ".0100"),
        (120000, ETH, ".01"),
        (180000, BTC, "-.02"),
        (180000, ETH, ".01"),
    ):
        points.append(
            {
                "instrument": instrument.key,
                "event_time_ms": timestamp,
                "source_available_at_ms": timestamp + 1000,
                "reference_time_ms": timestamp,
                "rate": rate,
                "reference_price": "+100.00",
            }
        )
    return {"schema_version": 1, "model": "linear_usdt_scenario_rate_v1", "points": points}


def replay(root, **changes):
    account, strategies = configuration()
    parameters = dict(
        root=root,
        account=account,
        strategies=strategies,
        start_ms=0,
        brackets=tables(),
        session_factory=MarginTradingSession,
    )
    parameters.update(changes)
    return simulate_margin(rows(), **parameters)


def test_optional_none_preserves_complete_legacy_result_and_factory_call(tmp_path):
    options = []

    def factory(*args, **kwargs):
        options.append(deepcopy(kwargs))
        return MarginTradingSession(*args, **kwargs)

    before = replay(tmp_path / "legacy", session_factory=factory)
    after = replay(tmp_path / "none", session_factory=factory, funding=None)
    assert json.dumps(before, sort_keys=True) == json.dumps(after, sort_keys=True)
    assert all("funding" not in value for value in options)
    assert "funding" not in after and "funding_history" not in after
    assert "funding_net" not in summarize_simulation(after)["costs"][0]


def test_actual_cross_long_isolated_short_cashflows_curve_and_shared_session_parity(tmp_path):
    source = schedule()
    result = replay(tmp_path / "research", funding=source)
    account, strategies = configuration()
    session = MarginTradingSession(
        tmp_path / "virtual", account, strategies, 0, create=True, funding=source
    )
    try:
        session.resume("explicit-human-start")
        for index in range(4):
            frame = tuple(
                Candle(
                    item, index * 60000, (index + 1) * 60000, D(100), D(103), D(97), D(100), D(10)
                )
                for item in (BTC, ETH)
            )
            session.run_once(frame, as_of_ms=(index + 1) * 60000, brackets=tables())
        final = session.execution.snapshot().account
        assert result["account"] == _document(final)
    finally:
        session.close()
    assert len(result["fills"]) == 2 and len(result["funding_history"]) == 5
    assert D(result["funding_history"][0]["amount"]) == 0
    assert [D(item["amount"]) for item in result["funding_history"]] == [0, -1, 1, 2, 1]
    assert {item["source_kind"] for item in result["funding_history"]} == {"scenario_rate"}
    assert D(result["wallet"]) == D("1002.8")
    assert D(result["equity"][0]["wallet"]) == 1000
    assert D(result["equity"][-1]["wallet"]) == D("1002.8")
    assert D(result["account"]["cross_wallet"]) == D("900.9")
    assert D(result["account"]["positions"][1]["isolated_wallet"]) == D("101.9")
    assert all(D(item["realized_pnl"]) == 0 for item in result["account"]["positions"])
    summary = summarize_simulation(result)
    assert D(summary["costs"][0]["fee"]) == D(".2")
    assert summary["costs"][0]["funding"] == "0"
    assert D(summary["costs"][0]["funding_net"]) == 3
    assert D(summary["costs"][0]["realized_pnl"]) == 0
    assert summary["reconciliation"]["matches"] is True
    assert "venue account payments" in " ".join(result["assumptions"])


def test_funding_cancel_restart_reconstructs_original_cashflows_without_duplicate_settlement(
    tmp_path,
):
    complete = replay(tmp_path / "full", funding=schedule())
    cancel = Event()

    def progress(value):
        if value["completed_frames"] == 3:
            cancel.set()

    with pytest.raises(Cancelled):
        replay(tmp_path / "restart", funding=schedule(), cancel=cancel, progress=progress)
    restored = replay(tmp_path / "restart", funding=schedule(), resume=True)
    for field in ("account", "events", "funding_history", "funding", "equity", "wallet", "fills"):
        assert restored[field] == complete[field], field
    again = replay(tmp_path / "restart", funding=schedule(), resume=True)
    assert again["account"] == restored["account"]
    assert len({item["funding_id"] for item in again["funding_history"]}) == 5


def test_changed_funding_identity_refuses_resume_even_with_same_ohlcv(tmp_path):
    replay(tmp_path, funding=schedule())
    changed = schedule()
    changed["points"][1]["rate"] = "0.02"
    with pytest.raises(ProtocolError, match="changed"):
        replay(tmp_path, funding=changed, resume=True)


def test_out_of_dataset_settlement_fails_before_any_financial_namespace(tmp_path):
    source = schedule()
    source["points"] = [
        source["points"][-1]
        | {"event_time_ms": 240000, "reference_time_ms": 240000, "source_available_at_ms": 240000}
    ]
    root = tmp_path / "must-not-exist"
    with pytest.raises(ValidationError, match="outside"):
        replay(root, funding=source)
    assert not root.exists()


def test_request_snapshot_preserves_decimal_strings_and_records_distinct_normalized_digest(
    tmp_path,
):
    raw = schedule()
    frozen, meta = _snapshot_funding(raw, tmp_path / "input-funding")
    raw["points"][1]["rate"] = "0.5"
    assert frozen["points"][1]["rate"] == ".0100"
    assert read_status_json(tmp_path / meta["source_file"]) == frozen
    normalized = read_status_json(tmp_path / meta["normalized_file"])
    assert normalized["points"][1]["rate"] == "0.0100"
    assert normalized["points"][1]["reference_price"] == "100.00"
    assert meta["scope"] == "request_json_snapshot"
    assert FundingSchedule.parse(normalized).sha256 == meta["schedule_sha256"]
    assert (
        bounded_artifact_hash(tmp_path / meta["source_file"], max_bytes=2097152)
        == meta["source_file_sha256"]
    )


def test_real_job_snapshots_all_builtins_new_funding_host_sources_and_signed_summary(tmp_path):
    source = tmp_path / "source.csv"
    write_rows(source, rows())
    dataset = tmp_path / "dataset"
    export_dataset(source, dataset, {"split_ratios": [0.5, 0.25, 0.25]})
    account, strategies = configuration()
    params = {
        "dataset": str(dataset),
        "account": account,
        "strategies": strategies,
        "start_ms": 0,
        "brackets": [
            {
                key: value
                for key, value in table.items()
                if key in {"instrument", "observed_at_ms", "available_at_ms", "rows"}
            }
            for table in _document(tables())
        ],
        "funding": schedule(),
    }
    partial = tmp_path / "partial"
    partial.mkdir()
    response = run_margin_simulation_job(partial, tmp_path / "final", params, None)
    result = read_status_json(partial / "simulation.json")
    assert D(response["summary"]["funding_net"]) == 3
    assert response["summary"]["funding_settlements"] == 5
    assert result["configuration"]["funding"]["points"][1]["rate"] == ".0100"
    names = {record["file"] for record in result["implementation"]["files"]}
    assert {
        "code-snapshot/application/margin_funding.py",
        "code-snapshot/runtime/margin_session_funding.py",
        "code-snapshot/strategies/neural_signal/module.py",
        "code-snapshot/strategies/target_quantity/module.py",
    } <= names
    assert summarize_simulation(result)["reconciliation"]["matches"] is True


def test_signed_cashflow_does_not_flip_existing_positive_cost_semantics():
    result = {
        "schema_version": 1,
        "state": "complete",
        "scope": "virtual_margin_v2_ohlcv_proxy",
        "quote_asset": "USDT",
        "initial_wallet": "100",
        "wallet": "95",
        "fills": [],
        "events": [
            {"kind": "funding", "timestamp_ms": 60000, "cost": "3"},
            {
                "kind": "funding_cashflow",
                "timestamp_ms": 120000,
                "amount": "-2",
                "settlement_asset": "USDT",
                "source_kind": "scenario_rate",
            },
        ],
        "equity": [{"timestamp_ms": 180000, "wallet": "95", "equity": "95"}],
    }
    summary = summarize_simulation(result)
    assert summary["costs"][0]["funding"] == "3"
    assert summary["costs"][0]["funding_net"] == "-2"
    assert summary["reconciliation"] == {
        "expected_wallet": "95",
        "difference": "0",
        "matches": True,
    }


def test_job_refuses_mutated_funding_snapshot_before_publishing_result(tmp_path, monkeypatch):
    source = tmp_path / "rows.csv"
    write_rows(source, rows())
    dataset = tmp_path / "dataset"
    export_dataset(source, dataset, {"split_ratios": [0.5, 0.25, 0.25]})
    partial = tmp_path / "partial"
    partial.mkdir()
    account, strategies = configuration()
    params = {
        "dataset": str(dataset),
        "account": account,
        "strategies": strategies,
        "start_ms": 0,
        "brackets": [
            {
                key: value
                for key, value in table.items()
                if key in {"instrument", "observed_at_ms", "available_at_ms", "rows"}
            }
            for table in _document(tables())
        ],
        "funding": schedule(),
    }
    original = margin_research.simulate_margin

    def changed(*args, **kwargs):
        result = original(*args, **kwargs)
        (partial / "input-funding/source.json").write_text("{}", encoding="utf-8")
        return result

    monkeypatch.setattr(margin_research, "simulate_margin", changed)
    with pytest.raises(ValidationError, match="snapshot changed"):
        run_margin_simulation_job(partial, tmp_path / "final", params, None)
    assert not (partial / "simulation.json").exists()
