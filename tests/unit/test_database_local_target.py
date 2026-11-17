"""Local configuration selects the same explicit target used by Compose init."""

import sys
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.domain.errors import ValidationError
from omega.infrastructure.postgres.config import configure_local_database, load_database_settings
from omega.runtime import database_cli


@pytest.fixture
def project(tmp_path):
    (tmp_path / "config").mkdir()
    template = Path(__file__).resolve().parents[2] / "config/database.config.example.toml"
    (tmp_path / "config/database.config.example.toml").write_bytes(template.read_bytes())
    return tmp_path


def files(root):
    return {
        str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()
    }


def invoke(monkeypatch, root, command, *options):
    monkeypatch.setattr(sys, "argv", ["omega-db", "--root", str(root), command, *options])
    database_cli.main()


@pytest.fixture
def driver(monkeypatch):
    calls = {"compose": [], "connections": [], "migrate": 0, "verify": 0}

    class DatabaseError(Exception):
        pass

    monkeypatch.setitem(sys.modules, "psycopg", SimpleNamespace(Error=DatabaseError))

    class Connection:
        def execute(self, query):
            assert query == "SELECT extversion FROM pg_extension WHERE extname='timescaledb'"
            return SimpleNamespace(fetchone=lambda: ("test-extension",))

    @contextmanager
    def connect(settings):
        calls["connections"].append(settings)
        yield Connection()

    def compose(command, **kwargs):
        calls["compose"].append((command, kwargs))

    def migrate(_connection):
        calls["migrate"] += 1

    def verify(_connection):
        calls["verify"] += 1

    monkeypatch.setattr(database_cli, "connect_database", connect)
    monkeypatch.setattr(database_cli.subprocess, "run", compose)
    monkeypatch.setattr(database_cli, "migrate", migrate)
    monkeypatch.setattr(database_cli, "verify_schema", verify)
    return calls


@pytest.mark.parametrize("port", [False, True, 0, -1, 65536, 54331.0, "54331", []])
def test_invalid_native_port_does_not_create_configuration_or_secrets(project, port):
    before = files(project)
    with pytest.raises(ValidationError, match="port must be an integer"):
        configure_local_database(project, port=port)
    assert files(project) == before


@pytest.mark.parametrize("port", [1, 54331, 65535])
def test_explicit_new_port_preserves_other_template_fields_and_credentials(project, port):
    template = (project / "config/database.config.example.toml").read_text(encoding="utf-8")
    config = configure_local_database(project, port=port)
    assert config.read_text(encoding="utf-8") == template.replace("port = 55432", f"port = {port}")
    settings = load_database_settings(config)
    assert (settings.host, settings.port, settings.dbname, settings.user) == (
        "127.0.0.1",
        port,
        "omega",
        "omega_app",
    )
    before = files(project)
    assert configure_local_database(project, port=port) == config
    assert configure_local_database(project) == config
    assert files(project) == before
    assert settings.password not in config.read_text(encoding="utf-8")
    assert settings.password != (project / "secrets/db_admin_password").read_text().strip()


def test_different_requested_port_never_overwrites_existing_target_or_secrets(project):
    configure_local_database(project, port=54331)
    before = files(project)
    with pytest.raises(ValidationError, match="refusing to overwrite configuration"):
        configure_local_database(project, port=54332)
    assert files(project) == before


def test_partial_credentials_remain_untouched_with_explicit_port(project):
    (project / "secrets").mkdir()
    (project / "secrets/db_app_password").write_text("retain-existing-private-value")
    before = files(project)
    with pytest.raises(ValidationError, match="partial database configuration"):
        configure_local_database(project, port=54331)
    assert files(project) == before


def test_ambiguous_explicit_template_port_fails_before_creating_secrets(project):
    template = project / "config/database.config.example.toml"
    template.write_text(template.read_text().replace("port = 55432\n", ""))
    before = files(project)
    with pytest.raises(ValidationError, match="template port cannot be configured"):
        configure_local_database(project, port=54331)
    assert files(project) == before


@pytest.mark.parametrize("explicit", [False, True])
def test_init_compose_and_connection_use_existing_toml_target_over_inherited_env(
    project, monkeypatch, driver, explicit
):
    config = configure_local_database(project, port=54331)
    config.write_text(config.read_text().replace('dbname = "omega"', 'dbname = "omega_chosen"'))
    (project / ".env").write_text("OMEGA_DB_PORT=60000\nOMEGA_DB_NAME=wrong_dotenv\n")
    before = files(project)
    monkeypatch.setenv("OMEGA_DB_PORT", "61000")
    monkeypatch.setenv("OMEGA_DB_NAME", "wrong_inherited")
    monkeypatch.setenv("OMEGA_TARGET_TEST_UNRELATED", "retained")
    options = ("--port", "54331") if explicit else ()
    invoke(monkeypatch, project, "init", *options)
    assert len(driver["compose"]) == len(driver["connections"]) == 1
    command, kwargs = driver["compose"][0]
    assert command == [
        "docker",
        "compose",
        "--project-directory",
        str(project),
        "-f",
        str(project / "compose.yaml"),
        "up",
        "--detach",
        "--wait",
        "--wait-timeout",
        "120",
        "postgres",
    ]
    assert kwargs["cwd"] == project and kwargs["check"] is True and kwargs["timeout"] == 150
    assert kwargs["env"]["OMEGA_DB_PORT"] == "54331"
    assert kwargs["env"]["OMEGA_DB_NAME"] == "omega_chosen"
    assert kwargs["env"]["OMEGA_TARGET_TEST_UNRELATED"] == "retained"
    assert database_cli.os.environ["OMEGA_DB_PORT"] == "61000"
    assert database_cli.os.environ["OMEGA_DB_NAME"] == "wrong_inherited"
    assert driver["connections"] == [load_database_settings(config)]
    assert driver["migrate"] == driver["verify"] == 1
    assert files(project) == before


def test_configure_port_only_creates_local_files_without_compose_or_connection(
    project, monkeypatch, driver
):
    invoke(monkeypatch, project, "configure", "--port", "54331")
    assert load_database_settings(project / "database.config.toml").port == 54331
    assert driver["compose"] == driver["connections"] == []
    assert driver["migrate"] == driver["verify"] == 0


@pytest.mark.parametrize("command", ["stop", "migrate", "status"])
def test_unused_port_option_rejected_before_configuration_compose_or_connection(
    tmp_path, monkeypatch, driver, command
):
    before = files(tmp_path)

    def forbidden(*_args, **_kwargs):
        raise AssertionError("unused option reached filesystem/configuration operation")

    monkeypatch.setattr(database_cli, "configure_local_database", forbidden)
    monkeypatch.setattr(database_cli, "load_database_settings", forbidden)
    with pytest.raises(SystemExit) as raised:
        invoke(monkeypatch, tmp_path, command, "--port", "54331")
    assert raised.value.code == 2
    assert driver["compose"] == driver["connections"] == [] and files(tmp_path) == before


@pytest.mark.parametrize("port", ["0", "-1", "65536", "not-a-number"])
def test_invalid_cli_port_creates_no_files(tmp_path, monkeypatch, driver, port):
    with pytest.raises(SystemExit) as raised:
        invoke(monkeypatch, tmp_path, "init", "--port", port)
    assert raised.value.code == 2
    assert files(tmp_path) == {} and driver["compose"] == driver["connections"] == []


@pytest.mark.parametrize("field,value", [("host", "192.0.2.15"), ("user", "other_role")])
def test_init_rejects_nonlocal_or_other_role_target_without_docker_or_connection(
    project, monkeypatch, driver, field, value
):
    config = configure_local_database(project, port=54331)
    original = 'host = "127.0.0.1"' if field == "host" else 'user = "omega_app"'
    config.write_text(config.read_text().replace(original, f'{field} = "{value}"'))
    before = files(project)
    with pytest.raises(SystemExit) as raised:
        invoke(monkeypatch, project, "init")
    assert raised.value.code == 1
    assert driver["compose"] == driver["connections"] == [] and files(project) == before


@pytest.mark.parametrize("command", ["migrate", "status"])
def test_existing_explicit_external_migrate_and_status_contract_unchanged(
    project, monkeypatch, driver, command
):
    config = configure_local_database(project, port=54331)
    config.write_text(config.read_text().replace('host = "127.0.0.1"', 'host = "192.0.2.15"'))
    before = files(project)
    invoke(monkeypatch, project, command)
    assert driver["compose"] == []
    assert driver["connections"] == [load_database_settings(config)]
    assert driver["verify"] == 1 and driver["migrate"] == (command == "migrate")
    assert files(project) == before


def test_stop_keeps_original_compose_command_without_target_or_env_rewrite(
    tmp_path, monkeypatch, driver
):
    invoke(monkeypatch, tmp_path, "stop")
    assert len(driver["compose"]) == 1
    command, kwargs = driver["compose"][0]
    assert command == [
        "docker",
        "compose",
        "--project-directory",
        str(tmp_path),
        "-f",
        str(tmp_path / "compose.yaml"),
        "stop",
        "postgres",
    ]
    assert kwargs == {"cwd": tmp_path, "check": True, "timeout": 150}
    assert driver["connections"] == [] and files(tmp_path) == {}
