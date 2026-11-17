"""The root checker reports incomplete family validation honestly."""

import json

import pytest

from omega.application.conformance import run_conformance
from omega.runtime.ts_cli import main
from omega.strategies.registry import StrategyRegistry


@pytest.mark.parametrize("identity", ["tef", "target_quantity", "neural_signal"])
def test_builtin_common_and_family_edge_cases_pass(identity):
    report = run_conformance(StrategyRegistry(), identity)
    assert report["status"] == "passed", report
    assert report["failed"] == 0
    assert report["family_validation"] == "provided"
    assert {case["scope"] for case in report["cases"]} >= {
        "common_executor",
        "selected_module",
        "family_specific",
    }


def test_interactive_selection_and_json_list(capsys):
    assert main([], input_fn=lambda _prompt: "target_quantity") == 0
    assert "Trade Strategy" in capsys.readouterr().out
    assert main(["--list", "--json"]) == 0
    listed = json.loads(capsys.readouterr().out)
    assert {module["id"] for module in listed["modules"]} == {
        "tef",
        "target_quantity",
        "neural_signal",
    }
    assert main(["tef", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["module_id"] == "tef"


def test_unknown_selection_fails_with_machine_readable_error(capsys):
    assert main(["missing", "--json"]) == 1
    assert json.loads(capsys.readouterr().out)["status"] == "failed"


def test_missing_family_checks_are_partial_not_passed():
    registry = StrategyRegistry()
    registry.validator = lambda _identity: None
    report = run_conformance(registry, "target_quantity")
    assert report["status"] == "partial"
    assert report["family_validation"] == "not_provided"


def test_malformed_family_results_are_failed():
    registry = StrategyRegistry()
    registry.validator = lambda _identity: lambda *_: []
    report = run_conformance(registry, "target_quantity")
    assert report["status"] == "failed"
    assert report["failed"] == 1


def test_skipped_family_cases_are_partial():
    registry = StrategyRegistry()
    registry.validator = lambda _identity: (
        lambda *_: [{"id": "not_implemented", "status": "skipped"}]
    )
    report = run_conformance(registry, "target_quantity")
    assert report["status"] == "partial"
    assert report["skipped"] == 1
