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
    source = Path("docs/examples/master.config.example.toml").resolve()
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
