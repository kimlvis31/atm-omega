from pathlib import Path

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.config import load_master


def test_default_is_headless_and_loading_creates_nothing(tmp_path):
    config = load_master(tmp_path)
    assert not config.gui_autostart
    assert config.paths["data_dir"] == tmp_path / "data"
    assert not list(tmp_path.iterdir())


def test_documented_master_example_and_root_relative_paths(tmp_path):
    source = Path("config/master.config.example.toml").resolve()
    config = load_master(tmp_path, source)
    assert config.instance_id == "omega-main"
    assert config.paths["runtime_settings_store"] == tmp_path / "state/settings.json"
    assert config.ready_timeout_s == 30


@pytest.mark.parametrize(
    "content",
    [
        "schema_version=true",
        'instance_id="../bad"',
        '[gui]\nautostart="false"',
        "[gui]\nautosart=false",
        '[runtime]\nstart_method="fork"',
        "[runtime]\nready_timeout_s=nan",
        '[safety]\nafter_core_restart="auto_resume"',
    ],
)
def test_invalid_configuration_is_not_silently_coerced(tmp_path, content):
    path = tmp_path / "invalid.toml"
    path.write_text(content)
    with pytest.raises(ValidationError):
        load_master(tmp_path, path)


def test_neural_evidence_defaults_partial_override_and_no_side_effect(tmp_path):
    baseline = load_master(tmp_path)
    assert baseline.neural_evidence.max_bytes == 536870912
    path = tmp_path / "master.config.toml"
    path.write_text("[neural_evidence]\nwarning_percent=70\nmax_pins=100\n")
    changed = load_master(tmp_path)
    assert changed.neural_evidence.warning_percent == 70
    assert changed.neural_evidence.max_pins == 100
    assert changed.neural_evidence.max_bytes == baseline.neural_evidence.max_bytes
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "setting",
    [
        "max_bytes=true",
        "max_bytes=4194303",
        "max_bytes=8589934593",
        "max_records=15",
        "max_records=1000001",
        "max_pins=0",
        "max_pins=65537",
        "warning_percent=100",
        "warning_percent=1.0",
        "warning_percent=0",
        "retention_days=7",
    ],
)
def test_invalid_evidence_budget_is_not_silently_coerced(tmp_path, setting):
    (tmp_path / "master.config.toml").write_text("[neural_evidence]\n" + setting)
    with pytest.raises(ValidationError):
        load_master(tmp_path)
