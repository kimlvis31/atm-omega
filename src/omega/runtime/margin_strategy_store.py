"""Exclusive CAS checkpoints for the TS adapter, separate from financial facts."""

import json
import os
import re
import stat
from hashlib import sha256
from pathlib import Path
from threading import RLock

from omega.application.margin_strategy import MAX_BYTES, MarginStrategyState
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MAX_FILE_BYTES = MAX_BYTES * 2


def _json(value):
    return json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":")).encode()


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate checkpoint field")
        result[key] = value
    return result


class FileMarginStrategyStore:
    def __init__(self, root, strategy_id):
        if (
            type(strategy_id) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", strategy_id) is None
        ):
            raise ValidationError("invalid margin strategy checkpoint identity")
        self.root = Path(root).absolute()
        self.strategy_id, self.closed = strategy_id, False
        self.directory = self.root / "state/margin-strategies"
        stem = sha256(strategy_id.encode()).hexdigest()
        self.path, self.lock_path = (
            self.directory / (stem + ".json"),
            self.directory / (stem + ".lock"),
        )
        self.lock = RLock()
        self._check()
        self.owner = InstanceLock(self.lock_path)

    def _check(self):
        if self.closed:
            raise StorageError("margin strategy store is closed")
        for path in (self.path, self.lock_path, *self.directory.parents, self.directory):
            if path.is_symlink() or path.is_junction():
                raise StorageError("margin strategy checkpoint cannot use linked paths")
        for path in (self.root, self.root / "state", self.directory):
            if path.exists() and not path.is_dir():
                raise StorageError("margin strategy checkpoint parent is not a directory")

    def load(self, strategy_id):
        if strategy_id != self.strategy_id:
            raise ValidationError("checkpoint belongs to another strategy")
        with self.lock:
            self._check()
            descriptor = None
            try:
                descriptor = os.open(
                    self.path,
                    os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0),
                )
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(details.st_mode)
                    or details.st_size > MAX_FILE_BYTES
                    or getattr(details, "st_file_attributes", 0) & 0x400
                ):
                    raise StorageError("margin strategy checkpoint must be a bounded regular file")
                stream = os.fdopen(descriptor, "rb")
                descriptor = None
                with stream:
                    raw = stream.read(MAX_FILE_BYTES + 1)
            except FileNotFoundError:
                return None
            except OSError:
                raise StorageError("margin strategy checkpoint cannot be read") from None
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            try:
                if len(raw) > MAX_FILE_BYTES:
                    raise ValueError
                envelope = json.loads(raw, object_pairs_hook=_unique)
                if (
                    type(envelope) is not dict
                    or set(envelope) != {"data", "sha256"}
                    or envelope["sha256"] != sha256(_json(envelope["data"])).hexdigest()
                ):
                    raise ValueError
                state = MarginStrategyState.parse(envelope["data"])
                if state.strategy_id != strategy_id:
                    raise ValueError
                return state
            except (ValueError, TypeError, KeyError, RecursionError, ValidationError):
                raise StorageError(
                    "invalid margin strategy checkpoint; original preserved"
                ) from None

    def save(self, state, *, expected_revision):
        if (
            not isinstance(state, MarginStrategyState)
            or expected_revision is not None
            and (type(expected_revision) is not int or expected_revision < 0)
        ):
            raise ValidationError("typed checkpoint and expected revision required")
        with self.lock:
            current = self.load(state.strategy_id)
            if current == state:
                return  # Exact ACK-loss retry, never a new revision or evaluation.
            if (
                (current is None) != (expected_revision is None)
                or current is not None
                and current.revision != expected_revision
            ):
                raise ProtocolError("margin strategy checkpoint revision conflict")
            if state.revision != (0 if current is None else current.revision + 1):
                raise ProtocolError("margin strategy checkpoint must advance exactly one revision")
            if current is not None and any(
                getattr(state, key) != getattr(current, key)
                for key in (
                    "account_id",
                    "strategy_id",
                    "instrument",
                    "owner_id",
                    "module_id",
                    "api_version",
                    "bootstrap_digest",
                )
            ):
                raise ProtocolError("margin strategy checkpoint identity cannot change")
            document = state.document()
            atomic_json(
                self.path, {"data": document, "sha256": sha256(_json(document)).hexdigest()}
            )

    @property
    def released(self):
        return self.owner.released

    def close(self):
        with self.lock:
            self.closed = True
            if not self.released:
                self.owner.close()
            if not self.released:
                raise StorageError("margin strategy lease release is not confirmed")
