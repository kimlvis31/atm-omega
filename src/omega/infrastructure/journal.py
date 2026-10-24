"""Synchronous single-writer JSONL durability. No credentials/pickle/module loading."""

import hashlib
import json
import os
from collections.abc import Callable
from pathlib import Path

from omega.domain.errors import JournalError
from omega.ports.journal import Durability, Journal


def _canonical(record: dict) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class JsonlJournal:
    """Must have exactly one writer. Torn/corrupt tails require explicit repair."""

    def __init__(self, path: Path, *, max_record_bytes: int = 65_536):
        self.path = Path(path)
        self.max_bytes = max_record_bytes
        if max_record_bytes < 128:
            raise ValueError("record limit too small")

    def append(self, record: dict) -> None:
        try:
            payload = _canonical(record)
            wrapped = {"event": record, "sha256": hashlib.sha256(payload).hexdigest()}
            line = _canonical(wrapped) + b"\n"
            if len(line) > self.max_bytes:
                raise JournalError("journal event exceeds record limit")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # Validate the existing tail before extending it. The owner is single-writer.
            if self.path.exists() and self.path.stat().st_size:
                with self.path.open("rb") as check:
                    check.seek(-1, os.SEEK_END)
                    if check.read(1) != b"\n":
                        raise JournalError("torn journal tail; repair before append")
            with self.path.open("ab", buffering=0) as stream:
                remaining = memoryview(line)
                while remaining:
                    written = stream.write(remaining)
                    if not written:
                        raise JournalError("incomplete journal write")
                    remaining = remaining[written:]
                os.fsync(stream.fileno())
            if os.name != "nt":
                fd = os.open(self.path.parent, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        except (OSError, ValueError, TypeError):
            raise JournalError("journal persistence failed") from None

    def records(self) -> tuple[dict, ...]:
        if not self.path.exists():
            return ()
        records = []
        try:
            with self.path.open("rb") as stream:
                while line := stream.readline(self.max_bytes + 1):
                    if len(line) > self.max_bytes or not line.endswith(b"\n"):
                        raise JournalError("oversized or torn journal event")
                    wrapped = json.loads(line)
                    record = wrapped["event"]
                    if not isinstance(record, dict):
                        raise JournalError("journal event must be an object")
                    if hashlib.sha256(_canonical(record)).hexdigest() != wrapped["sha256"]:
                        raise JournalError("journal event checksum mismatch")
                    records.append(record)
        except (OSError, ValueError, TypeError, KeyError):
            raise JournalError("journal replay failed") from None
        return tuple(records)


class FailoverJournal:
    def __init__(self, primary: Journal, fallback: Journal, critical: Callable[[str], None]):
        self.primary, self.fallback, self.critical = primary, fallback, critical
        self.last_incident: str | None = None
        self.notification_failed = False

    def record(self, event: dict) -> Durability:
        try:
            self.primary.append(event)
            return Durability.PRIMARY
        except (JournalError, OSError):
            pass
        try:
            self.fallback.append(event)
            return Durability.FALLBACK
        except (JournalError, OSError):
            # Alarm delivery is attempted without a successful disk write.
            self.last_incident = "ALL_RECORDING_UNAVAILABLE"
            try:
                self.critical(self.last_incident)
            except Exception:
                # An unavailable audio/GUI agent cannot stop emergency risk reduction.
                self.notification_failed = True
            return Durability.VOLATILE

    def records(self) -> tuple[dict, ...]:
        # Corrupt/unavailable history is not silently replaced with an empty history.
        return self.primary.records() + self.fallback.records()
