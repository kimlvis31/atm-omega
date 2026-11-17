"""Recorded simulation accounting and inert file-reader boundaries."""

import hashlib
import json
import os
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace

import pytest

from omega.research.result_summary import (
    MAX_RESULT_BYTES,
    read_simulation_result,
    summarize_simulation,
)


def result_record():
    return {
        "schema_version": 1,
        "state": "complete",
        "quote_asset": "USDT",
        "quote_assets": {"A": "USDT"},
        "model": "linear-pnl-unleveraged-reference-v1",
        "spec": {"initial_wallet": "100", "latency_ms": 25, "slippage_rate": "0.01"},
        "wallet": "105",
        "positions": {"A": "1"},
        "pending_orders": 1,
        "equity": [
            {"timestamp_ms": 60000, "wallet": "99", "equity": "90"},
            {"timestamp_ms": 180000, "wallet": "105", "equity": "110"},
        ],
        "fills": [
            {
                "timestamp_ms": "60001.25",
                "instrument": "A",
                "quantity": "1",
                "price": "10",
                "fee": "1",
                "realized_pnl": "0",
                "tag": "ENTRY",
            },
            {
                "timestamp_ms": "120001",
                "instrument": "A",
                "quantity": "-1",
                "price": "20",
                "fee": "1",
                "realized_pnl": "10",
                "tag": "EXIT",
            },
        ],
        "events": [{"kind": "funding", "timestamp_ms": 120000, "instrument": "A", "cost": "3"}],
        "input": {"rows_sha256": "fixture", "strategy": "demo", "strategy_config": {}},
        "assumptions": ["synthetic path; not exchange trade history"],
        "backend": {"requested": "gpu", "selected": "cpu", "reason": "not validated"},
    }


def test_recorded_accounting_initial_baseline_and_unrealized_are_separate():
    original = result_record()
    untouched = deepcopy(original)
    summary = summarize_simulation(original)
    metrics = summary["metrics"]
    assert Decimal(metrics["max_drawdown_pct"]) == 10
    assert Decimal(summary["curve"][0]["drawdown_pct"]) == 10
    assert Decimal(metrics["net_wallet_change"]) == 5
    assert Decimal(metrics["unrealized_pnl"]) == 5
    assert Decimal(metrics["equity_return_pct"]) == 10
    assert summary["costs"] == [
        {"currency": "USDT", "fee": "2", "funding": "3", "realized_pnl": "10"}
    ]
    assert summary["reconciliation"] == {
        "expected_wallet": "105",
        "difference": "0",
        "matches": True,
    }
    assert summary["fills_count"] == 2
    assert "win_rate" not in summary and "slippage_cost" not in summary
    assert summary["spec"]["latency_ms"] == 25
    assert summary["positions"] == {"A": "1"} and summary["pending_orders"] == 1
    assert original == untouched


def test_accounting_discrepancy_is_exposed_and_not_corrected():
    result = result_record()
    result["wallet"] = "106"
    summary = summarize_simulation(result)
    assert summary["reconciliation"]["difference"] == "1"
    assert summary["reconciliation"]["matches"] is False
    assert summary["metrics"]["final_wallet"] == "106"
    assert any("differs" in note for note in summary["limitations"])


def test_missing_fields_are_unavailable_but_explicit_empty_lists_have_zero_counts():
    result = {"schema_version": 1, "state": "complete", "quote_asset": "USDT"}
    summary = summarize_simulation(result)
    assert summary["fills_count"] is None and summary["events_count"] is None
    assert all(value is None for value in summary["metrics"].values())
    assert summary["costs"][0]["fee"] is None
    result.update(fills=[], events=[], equity=[])
    summary = summarize_simulation(result)
    assert summary["fills_count"] == 0 and summary["events_count"] == 0
    assert summary["costs"][0]["fee"] == "0"
    assert summary["costs"][0]["funding"] == "0"
    assert summary["metrics"]["max_drawdown_pct"] is None


def test_partially_missing_fee_blocks_total_and_reconciliation():
    result = result_record()
    del result["fills"][1]["fee"]
    summary = summarize_simulation(result)
    assert summary["costs"][0]["fee"] is None
    assert summary["reconciliation"]["matches"] is None


def test_foreign_fee_currency_is_not_added_to_quote_fees():
    result = result_record()
    result["fills"][0].update(fee_asset="BNB", fee="0.002")
    summary = summarize_simulation(result)
    costs = {row["currency"]: row for row in summary["costs"]}
    assert costs["USDT"]["fee"] == "1" and costs["BNB"]["fee"] == "0.002"
    assert summary["reconciliation"]["matches"] is None


def test_mixed_quote_records_do_not_produce_global_wallet_totals_or_curve():
    result = result_record()
    result["quote_assets"]["A"] = "BTC"
    summary = summarize_simulation(result)
    assert summary["wallet_currency"] is None
    assert all(value is None for value in summary["metrics"].values())
    assert all(row["equity"] is None for row in summary["curve"])
    assert summary["reconciliation"]["matches"] is None
    assert {row["currency"] for row in summary["costs"]} == {"BTC", "USDT"}


def test_unidentified_units_are_not_summed_into_unnamed_money():
    result = result_record()
    result.pop("quote_asset")
    result.pop("quote_assets")
    summary = summarize_simulation(result)
    assert summary["wallet_currency"] is None
    assert summary["costs"] == [
        {"currency": "UNAVAILABLE", "fee": None, "funding": None, "realized_pnl": None}
    ]


@pytest.mark.parametrize(
    "field,value", [("fee", "NaN"), ("price", "Infinity"), ("quantity", True), ("fee", "1e9999")]
)
def test_invalid_recorded_numbers_are_rejected(field, value):
    result = result_record()
    result["fills"][0][field] = value
    with pytest.raises(ValueError):
        summarize_simulation(result)


@pytest.mark.parametrize("mutation", ["duplicate", "unsorted", "fractional", "negative"])
def test_invalid_equity_timestamps_are_rejected(mutation):
    result = result_record()
    if mutation == "duplicate":
        result["equity"][1]["timestamp_ms"] = 60000
    elif mutation == "unsorted":
        result["equity"].reverse()
    elif mutation == "fractional":
        result["equity"][0]["timestamp_ms"] = "60000.5"
    else:
        result["equity"][0]["timestamp_ms"] = -1
    with pytest.raises(ValueError):
        summarize_simulation(result)


def test_missing_equity_samples_do_not_invent_zero_drawdown():
    result = result_record()
    for row in result["equity"]:
        row.pop("equity")
    assert summarize_simulation(result)["metrics"]["max_drawdown_pct"] is None


def test_reader_preserves_bytes_and_hash_and_rejects_path_escape(tmp_path):
    root = tmp_path / "artifacts"
    root.mkdir()
    path = root / "simulation.json"
    raw = json.dumps(result_record()).encode("utf-8")
    path.write_bytes(raw)
    packet = read_simulation_result(path, root)
    assert packet["sha256"] == hashlib.sha256(raw).hexdigest()
    assert path.read_bytes() == raw
    with pytest.raises(ValueError, match="below"):
        read_simulation_result(path, tmp_path / "different")


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b'{"schema_version":NaN}',
        b'{"schema_version":Infinity}',
        b'{"x":1e999}',
        b"[]",
        b"{",
    ],
)
def test_reader_rejects_invalid_utf8_json_and_nonfinite(tmp_path, raw):
    path = tmp_path / "simulation.json"
    path.write_bytes(raw)
    with pytest.raises((ValueError, UnicodeError)):
        read_simulation_result(path)


def test_reader_rejects_partial_wrong_schema_suffix_and_oversize(tmp_path):
    path = tmp_path / "simulation.json"
    record = result_record()
    record["state"] = "running"
    path.write_text(json.dumps(record), encoding="utf-8")
    with pytest.raises(ValueError, match="completed"):
        read_simulation_result(path)
    wrong = tmp_path / "module.py"
    wrong.write_text("raise AssertionError('must not execute')", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON only"):
        read_simulation_result(wrong)
    with path.open("wb") as stream:
        stream.truncate(MAX_RESULT_BYTES + 1)
    with pytest.raises(ValueError, match="16 MiB"):
        read_simulation_result(path)


def test_reader_detects_file_change_during_read(tmp_path, monkeypatch):
    path = tmp_path / "simulation.json"
    path.write_text(json.dumps(result_record()), encoding="utf-8")
    original = os.fstat
    calls = []

    def changed(descriptor):
        value = original(descriptor)
        calls.append(1)
        return SimpleNamespace(
            st_dev=value.st_dev,
            st_ino=value.st_ino,
            st_mode=value.st_mode,
            st_size=value.st_size,
            st_mtime_ns=value.st_mtime_ns + (len(calls) > 1),
        )

    monkeypatch.setattr(os, "fstat", changed)
    with pytest.raises(ValueError, match="changed"):
        read_simulation_result(path)


def test_actual_cpu_result_can_be_summarized():
    from omega.research.datasets import MarketRow
    from omega.research.simulation import SimulationSpec, simulate

    row = MarketRow("A", 0, *(Decimal(value) for value in (10, 11, 9, 10, 1)))
    result = simulate([row], lambda _view: [], SimulationSpec())
    summary = summarize_simulation(result)
    assert summary["reconciliation"]["matches"] is True
    assert summary["fills_count"] == 0
    assert summary["backend"]["selected"] == "cpu"


def test_duplicate_json_keys_and_invalid_pending_count_are_rejected(tmp_path):
    path = tmp_path / "simulation.json"
    path.write_text('{"schema_version":1,"schema_version":1,"state":"complete"}', encoding="utf-8")
    with pytest.raises(ValueError, match="duplicate"):
        read_simulation_result(path)
    result = result_record()
    result["pending_orders"] = True
    with pytest.raises(ValueError, match="nonnegative integer"):
        summarize_simulation(result)
