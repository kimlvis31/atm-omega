"""Preview is bounded declarative data, never loading checkpoint/model code."""

import json
import sys

import pytest

from omega import __version__
from omega.research.training_view import training_view

JOB = "a" * 32


def source(tmp_path, *, module="external.example", features=None):
    row = {
        "state": "complete",
        "cleanup_completed": True,
        "module": {
            "module_id": module,
            "core_version": __version__,
            "api_version": 1,
            "input": {"schema_id": "features.v1", "features": features or ["x"]},
        },
        "config": {},
        "model": {"file": "checkpoints/model.bin"},
        "artifacts": [{"kind": "checkpoint", "file": "checkpoints/model.bin", "sha256": "b" * 64}],
        "device": {"used": "cpu"},
    }
    (tmp_path / "run.json").write_text(json.dumps(row), encoding="utf-8")
    # These deliberately invalid bytes must not be imported or deserialized.
    (tmp_path / "instance_module.py").write_text('raise RuntimeError("must not execute")')
    return row


def write(tmp_path, row):
    (tmp_path / "run.json").write_text(json.dumps(row), encoding="utf-8")


def test_external_unknown_shape_stays_explicit_without_model_import(tmp_path):
    source(tmp_path)
    before = set(sys.modules)
    preview = training_view(tmp_path, JOB)
    assert preview["shape_confirmation_required"] and preview["contract_defaults"] is None
    assert preview["checkpoint"]["sha256"] == "b" * 64
    assert not {"torch", "numpy", "PySide6"} & (set(sys.modules) - before)
    assert not (tmp_path / "checkpoints/model.bin").exists()


def test_builtin_actual_feature_vocabulary_is_not_rewritten(tmp_path):
    row = source(
        tmp_path, module="omega.example.pytorch_return", features=["close/open-1", "log_volume"]
    )
    row["config"] = {"architecture": "mlp", "lookback": 3}
    write(tmp_path, row)
    result = training_view(tmp_path, JOB)
    assert result["contract_defaults"]["features"] == ["close/open-1", "log_volume"]
    assert result["contract_defaults"]["input_shape"] == [None, 3, 2]


def test_recurrent_unknown_hidden_shape_requires_confirmation(tmp_path):
    row = source(tmp_path, module="omega.example.pytorch_return")
    row["config"] = {"architecture": "gru", "lookback": 2}
    write(tmp_path, row)
    assert training_view(tmp_path, JOB)["shape_confirmation_required"]


@pytest.mark.parametrize(
    "case", ["incomplete", "unclean", "old_core", "duplicate", "traversal", "bad_hash"]
)
def test_unsafe_or_incompatible_metadata_cannot_prefill_candidate(tmp_path, case):
    row = source(tmp_path)
    if case == "incomplete":
        row["state"] = "running"
    elif case == "unclean":
        row["cleanup_completed"] = False
    elif case == "old_core":
        row["module"]["core_version"] = "other"
    elif case == "duplicate":
        row["artifacts"].append(dict(row["artifacts"][0]))
    elif case == "traversal":
        row["model"]["file"] = row["artifacts"][0]["file"] = "../escape.bin"
    elif case == "bad_hash":
        row["artifacts"][0]["sha256"] = "unverified"
    write(tmp_path, row)
    with pytest.raises(ValueError):
        training_view(tmp_path, JOB)


def test_explicit_machine_contract_cannot_lie_about_frozen_feature_order(tmp_path):
    row = source(tmp_path, features=["a", "b"])
    row["module"]["inference"] = {
        "contract": {
            "schema_id": "features.v1",
            "features": ["b", "a"],
            "lookback": 1,
            "input_shape": [None, 2],
            "output_shape": [None],
            "state_shape": None,
            "state_version": None,
            "allow_no_signal": False,
        }
    }
    write(tmp_path, row)
    with pytest.raises(ValueError, match="actual training schema"):
        training_view(tmp_path, JOB)
