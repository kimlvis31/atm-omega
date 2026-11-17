"""Local DB setup must preserve secrets and keep credentials out of diagnostics."""

from pathlib import Path

import pytest

from omega.domain.errors import ValidationError
from omega.infrastructure.postgres.config import configure_local_database, load_database_settings


@pytest.fixture
def project(tmp_path):
    (tmp_path / "config").mkdir()
    source = Path(__file__).resolve().parents[2] / "config/database.config.example.toml"
    (tmp_path / "config/database.config.example.toml").write_bytes(source.read_bytes())
    return tmp_path


def test_new_configuration_uses_distinct_secrets_and_is_idempotent(project):
    config = configure_local_database(project)
    before = {path: path.read_bytes() for path in project.rglob("*") if path.is_file()}
    assert configure_local_database(project) == config
    assert before == {path: path.read_bytes() for path in project.rglob("*") if path.is_file()}
    settings = load_database_settings(config)
    assert settings.user == "omega_app" and settings.port == 55432
    assert settings.password != (project / "secrets/db_admin_password").read_text().strip()
    assert settings.password not in repr(settings)
    assert settings.password not in config.read_text()
    assert b"\r" not in (project / "secrets/db_app_password").read_bytes()
    assert b"\r" not in (project / "secrets/db_admin_password").read_bytes()


def test_partial_credentials_are_never_overwritten(project):
    (project / "secrets").mkdir()
    password = project / "secrets/db_app_password"
    password.write_text("keep-this-secret")
    with pytest.raises(ValidationError, match="refusing to overwrite"):
        configure_local_database(project)
    assert password.read_text() == "keep-this-secret"
    assert not (project / "database.config.toml").exists()


def test_wrong_project_root_creates_no_partial_credentials(tmp_path):
    with pytest.raises(ValidationError, match="complete Omega project root"):
        configure_local_database(tmp_path)
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("port", ["0", "65536", "true", '"55432"'])
def test_invalid_database_ports_rejected_without_secret_in_error(project, port):
    config = configure_local_database(project)
    password = load_database_settings(config).password
    config.write_text(config.read_text().replace("port = 55432", f"port = {port}"))
    with pytest.raises(ValidationError) as raised:
        load_database_settings(config)
    assert password not in str(raised.value)


def test_password_file_path_is_relative_to_config_not_working_directory(project, monkeypatch):
    config = configure_local_database(project)
    expected = load_database_settings(config)
    monkeypatch.chdir(project / "config")
    assert load_database_settings(config) == expected
