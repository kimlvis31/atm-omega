"""Explicit database credentials; never discovers or rewrites legacy settings."""

# Imports
import json
import os
import re
import secrets
import tomllib
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ValidationError


@dataclass(frozen=True)
class DatabaseSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str = field(repr=False)
    connect_timeout_s: int = 5


def database_destination_id(settings: DatabaseSettings) -> str:
    """Opaque public namespace identity; credentials and secret paths are excluded."""
    identity = [settings.host, settings.port, settings.dbname, settings.user]
    return sha256(
        json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def load_database_settings(path: Path) -> DatabaseSettings:
    try:
        with path.open("rb") as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError("oversized configuration")
        data = tomllib.loads(raw.decode("utf-8"))
        required = {"host", "port", "dbname", "user", "password_file"}
        if not required <= data.keys() or data.keys() - required - {"connect_timeout_s"}:
            raise ValueError("invalid keys")
        for key in ("host", "dbname", "user", "password_file"):
            if not isinstance(data[key], str) or not data[key] or "\0" in data[key]:
                raise ValueError("invalid value")
        if type(data["port"]) is not int or not 1 <= data["port"] <= 65535:
            raise ValueError("invalid port")
        timeout = data.get("connect_timeout_s", 5)
        if type(timeout) is not int or not 1 <= timeout <= 60:
            raise ValueError("invalid timeout")
        password_path = Path(data["password_file"])
        if not password_path.is_absolute():
            password_path = path.resolve().parent / password_path
        with password_path.open(encoding="utf-8") as stream:
            password = stream.read(4097).rstrip("\r\n")
        if not password or len(password) > 4096 or any(c in password for c in ("\0", "\r", "\n")):
            raise ValueError("invalid password file")
        return DatabaseSettings(
            data["host"], data["port"], data["dbname"], data["user"], password, timeout
        )
    except (OSError, ValueError, UnicodeError):
        raise ValidationError("database configuration or password file cannot be loaded") from None


def configure_local_database(root: Path, *, port: int | None = None) -> Path:
    if port is not None and (type(port) is not int or not 1 <= port <= 65535):
        raise ValidationError("local database port must be an integer from 1 to 65535")
    # [1]: Preserve Existing Configuration & Credentials
    config = root / "database.config.toml"
    admin_password = root / "secrets/db_admin_password"
    app_password = root / "secrets/db_app_password"
    paths = (config, admin_password, app_password)
    if all(path.is_file() for path in paths):
        settings = load_database_settings(config)
        if port is not None and settings.port != port:
            raise ValidationError(
                "existing database port differs; refusing to overwrite configuration"
            )
        return config
    if any(path.exists() for path in paths):
        raise ValidationError(
            "partial database configuration exists; refusing to overwrite secrets"
        )
    try:
        template = (root / "config/database.config.example.toml").read_text(encoding="utf-8")
    except OSError:
        raise ValidationError("run database setup from a complete Omega project root") from None
    if port is not None:
        try:
            original = tomllib.loads(template)
            template, changes = re.subn(r"(?m)^port[ \t]*=[^\r\n]*", f"port = {port}", template)
            updated = tomllib.loads(template)
            if changes != 1 or updated != original | {"port": port}:
                raise ValueError("template port cannot be selected unambiguously")
        except ValueError:
            raise ValidationError("local database template port cannot be configured") from None

    # [2]: New Project-Local Secrets & Non-Secret Configuration
    admin_password.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    for path in (admin_password, app_password):
        with open(
            path,
            "x",
            encoding="utf-8",
            newline="\n",
            opener=lambda p, flags: os.open(p, flags, 0o600),
        ) as stream:
            stream.write(secrets.token_urlsafe(36) + "\n")
    with config.open("x", encoding="utf-8") as stream:
        stream.write(template)
    return config
