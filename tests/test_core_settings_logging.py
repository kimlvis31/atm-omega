"""Preferences reject ambiguous updates and diagnostic cleanup owns only its files."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from omega.domain.errors import ValidationError
from omega.infrastructure.logging import CategoryLogger
from omega.runtime.settings import SettingsStore


def test_exact_legacy_settings_gain_execution_and_analysis_defaults_without_rewriting_file(
    tmp_path,
):
    import json

    value = SettingsStore(tmp_path / "settings.json").snapshot()
    value.pop("execution")
    value.pop("analysis")
    value["revision"] = 7
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    before = path.read_bytes()
    restored = SettingsStore(path)
    assert restored.snapshot()["revision"] == 7 and path.read_bytes() == before
    assert restored.snapshot()["execution"] == {
        "mode": "static",
        "max_age_ms": 60000,
        "apply_to_manual": False,
    }
    assert restored.update({"analysis": {"input_storage_mb": 32}}, revision=7)["revision"] == 8
    assert SettingsStore(path).snapshot()["analysis"]["input_storage_mb"] == 32


def test_partial_new_settings_schema_is_not_treated_as_legacy(tmp_path):
    import json

    value = SettingsStore(tmp_path / "settings.json").snapshot()
    value.pop("analysis")
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValidationError, match="schema"):
        SettingsStore(path)


@pytest.mark.parametrize(
    "changes",
    [
        {"execution": {"mode": "live"}},
        {"execution": {"mode": []}},
        {"execution": {"max_age_ms": True}},
        {"execution": {"max_age_ms": 0}},
        {"execution": {"apply_to_manual": 1}},
        {"analysis": {"input_storage_mb": True}},
        {"analysis": {"input_storage_mb": 15}},
        {"analysis": {"input_storage_mb": 16385}},
    ],
)
def test_execution_and_analysis_policy_invalid_changes_are_atomic(tmp_path, changes):
    settings = SettingsStore(tmp_path / "settings.json")
    before = settings.snapshot()
    with pytest.raises(ValidationError):
        settings.update(changes, revision=0)
    assert settings.snapshot() == before and not settings.path.exists()


def test_settings_roundtrip_revision_and_copy_isolation(tmp_path):
    store = SettingsStore(tmp_path / "settings.json")
    snapshot = store.snapshot()
    snapshot["logging"]["retention_days"] = 999
    assert store.snapshot()["logging"]["retention_days"] == 30
    result = store.update({"logging": {"retention_days": 7}}, revision=0)
    assert result["revision"] == 1
    assert SettingsStore(store.path).snapshot() == result
    with pytest.raises(ValidationError, match="revision"):
        store.update({}, revision=0)


@pytest.mark.parametrize(
    "changes",
    [
        {"unknown": {}},
        {"logging": {"token": "x"}},
        {"logging": {"retention_days": 0}},
        {"logging": {"max_file_bytes": True}},
        {"training": {"recording": "anything"}},
        {"simulation": {"seed": -1}},
    ],
)
def test_invalid_settings_never_persist(tmp_path, changes):
    store = SettingsStore(tmp_path / "settings.json")
    with pytest.raises(ValidationError):
        store.update(changes, revision=0)
    assert store.snapshot()["revision"] == 0
    assert not store.path.exists()


def test_log_partition_rotation_redaction_and_retention(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    settings.update({"logging": {"retention_days": 2, "max_file_bytes": 4096}}, revision=0)
    now = [datetime(2026, 10, 24, tzinfo=UTC)]
    logger = CategoryLogger(tmp_path / "logs", settings.snapshot, clock=lambda: now[0])
    logger.write(
        "control", "INFO", "TEST", api_key="sensitive", nested={"token": "hidden"}, safe="x" * 2000
    )
    logger.write("control", "INFO", "TEST", safe="x" * 2000)
    files = sorted((tmp_path / "logs" / "control").glob("*.jsonl"))
    assert len(files) == 2
    record = json.loads(files[0].read_text())
    assert record["parameters"]["api_key"] == "[REDACTED]"
    assert record["parameters"]["nested"]["token"] == "[REDACTED]"
    unrelated = files[0].parent / "keep.txt"
    unrelated.write_text("preserve")
    now[0] += timedelta(days=4)
    logger.write("system", "WARNING", "LATER")
    assert not any(path.exists() for path in files)
    assert unrelated.exists()


def test_log_level_and_unsafe_category(tmp_path):
    settings = SettingsStore(tmp_path / "settings.json")
    logger = CategoryLogger(tmp_path / "logs", settings.snapshot)
    logger.write("system", "DEBUG", "HIDDEN")
    assert not logger.root.exists()
    with pytest.raises(ValueError):
        logger.write("../escape", "INFO", "BAD")
