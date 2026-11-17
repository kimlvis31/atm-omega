"""Structured category/date diagnostic logs; never an order durability journal."""

import json
import re
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import RLock

LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}
PRIVATE_KEYS = {
    "token",
    "authorization",
    "api_key",
    "api_secret",
    "password",
    "secret",
    "signature",
}


def redact(value):
    if isinstance(value, dict):
        return {
            key: "[REDACTED]"
            if any(word in str(key).lower() for word in PRIVATE_KEYS)
            else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact(item) for item in value]
    return value


class CategoryLogger:
    def __init__(self, root: Path, settings, *, clock=None):
        self.root, self.settings = Path(root).resolve(), settings
        self.clock = clock or (lambda: datetime.now(UTC))
        self.lock = RLock()
        self.last_cleanup = None

    def write(self, category: str, level: str, code: str, **parameters) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", category) or level not in LEVELS:
            raise ValueError("invalid log category or level")
        configuration = self.settings()["logging"]
        if LEVELS[level] < LEVELS[configuration["level"]]:
            return
        now = self.clock().astimezone(UTC)
        record = {
            "schema_version": 1,
            "timestamp": now.isoformat(),
            "category": category,
            "level": level,
            "code": code,
            "parameters": redact(parameters),
        }
        payload = (json.dumps(record, ensure_ascii=False, allow_nan=False) + "\n").encode("utf-8")
        if len(payload) > 65_536:
            raise ValueError("log record exceeds limit")
        with self.lock:
            folder = self.root / category
            if (
                folder.is_symlink()
                or folder.is_junction()
                or not folder.resolve().is_relative_to(self.root)
            ):
                raise ValueError("log category cannot be a symlink")
            folder.mkdir(parents=True, exist_ok=True)
            index = 0
            while True:
                path = folder / f"{now:%Y-%m-%d}.{index:04d}.jsonl"
                if path.is_symlink():
                    raise ValueError("log segment cannot be a symlink")
                if (
                    not path.exists()
                    or path.stat().st_size + len(payload) <= configuration["max_file_bytes"]
                ):
                    break
                index += 1
            with path.open("ab") as stream:
                stream.write(payload)
            if self.last_cleanup != now.date():
                self.cleanup(now, configuration["retention_days"])
                self.last_cleanup = now.date()

    def cleanup(self, now: datetime, retention_days: int) -> None:
        cutoff = (now.astimezone(UTC) - timedelta(days=retention_days)).date()
        for folder in self.root.iterdir() if self.root.exists() else ():
            if (
                not folder.is_dir()
                or folder.is_symlink()
                or folder.is_junction()
                or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", folder.name)
            ):
                continue
            for path in folder.iterdir():
                if (
                    path.is_symlink()
                    or not path.is_file()
                    or not re.fullmatch(r"\d{4}-\d{2}-\d{2}\.\d{4,}\.jsonl", path.name)
                ):
                    continue
                try:
                    date = datetime.strptime(path.name[:10], "%Y-%m-%d").date()
                except ValueError:
                    continue
                if date < cutoff and path.resolve().is_relative_to(self.root):
                    path.unlink()
