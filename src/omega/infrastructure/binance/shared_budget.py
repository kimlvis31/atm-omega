"""Project-owned public request admission shared by collector processes.

This conservative limit is shared across Spot and USD-M. It is not a discovery
of the exchange quota, nor an admission service for other PCs/projects on the IP.
Corrupt state is deliberately retained and blocks requests until repaired.
"""

import errno
import json
import math
import os
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from threading import Lock

from omega.domain.errors import RateLimited, StorageError
from omega.infrastructure.atomic_files import replace_atomic
from omega.infrastructure.binance.budget import RequestBudget, Window

PUBLIC_WINDOWS = (Window("binance-ip", 600, 60),)


class SharedRequestBudget(RequestBudget):
    """OS locks serialize admission; no lock is held during a network request.

    A crash releases the OS lock while retaining spent reservations. Backward
    wall-clock jumps retain the greatest observed time rather than reopening an
    earlier window. A large forward jump can temporarily delay future requests;
    state is never silently reset to recover from clock or storage failures.
    """

    def __init__(self, path: Path, windows=PUBLIC_WINDOWS, *, clock=time.time):
        self.path = Path(path)
        self.lock_path = self.path.with_suffix(self.path.suffix + ".lock")
        self.wall_clock = clock
        self._thread_lock = Lock()
        self._active_now = 0.0
        super().__init__(windows, clock=lambda: self._active_now)

    @contextmanager
    def _locked(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.is_symlink() or self.lock_path.is_symlink():
                raise StorageError("public request budget path must not be a symlink")
            stream = self.lock_path.open("a+b", buffering=0)
        except OSError:
            raise StorageError("public request budget lock unavailable") from None
        locked = False
        try:
            # A permanent lock inode is separate from the atomically replaced JSON.
            if os.name == "nt":
                import msvcrt

                if stream.seek(0, os.SEEK_END) == 0:
                    try:
                        stream.write(b"\0")
                        stream.flush()
                    except PermissionError:
                        # Another process may have initialized and locked byte
                        # zero after our empty-file check.
                        raise RateLimited(0.05) from None
                stream.seek(0)
                try:
                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                except OSError as exc:
                    if exc.errno in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                        raise RateLimited(0.05) from None
                    raise
            else:
                import fcntl

                try:
                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                except BlockingIOError:
                    raise RateLimited(0.05) from None
            locked = True
            yield
        except OSError:
            raise StorageError("public request budget storage unavailable") from None
        finally:
            try:
                if locked:
                    if os.name == "nt":
                        stream.seek(0)
                        msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
                    else:
                        fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            finally:
                stream.close()

    def _load(self):
        policy = [[w.scope, w.limit, w.seconds] for w in self._windows]
        if not self.path.exists():
            state = {
                "schema_version": 1,
                "policy": policy,
                "last_time": 0,
                "blocked_until": 0,
                "used": [],
            }
        else:
            try:
                if self.path.stat().st_size > 65_536:
                    raise ValueError
                state = json.loads(self.path.read_text(encoding="utf-8"))
                if (
                    not isinstance(state, dict)
                    or set(state)
                    != {"schema_version", "policy", "last_time", "blocked_until", "used"}
                    or type(state["schema_version"]) is not int
                    or state["schema_version"] != 1
                    or state["policy"] != policy
                    or not isinstance(state["used"], list)
                    or len(state["used"]) > len(self._windows)
                ):
                    raise ValueError
                for name in ("last_time", "blocked_until"):
                    value = state[name]
                    if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                        raise ValueError
                keys = set()
                for row in state["used"]:
                    if (
                        not isinstance(row, list)
                        or len(row) != 4
                        or not isinstance(row[0], str)
                        or any(type(v) is not int or v < 0 for v in row[1:])
                        or (row[0], row[1]) not in {(w.scope, w.seconds) for w in self._windows}
                        or tuple(row[:3]) in keys
                    ):
                        raise ValueError
                    keys.add(tuple(row[:3]))
            except (OSError, TypeError, ValueError, OverflowError):
                raise StorageError(
                    "public request budget state is invalid; requests blocked"
                ) from None
        now = self.wall_clock()
        if type(now) not in {int, float} or not math.isfinite(now) or now < 0:
            raise StorageError("public request budget clock unavailable")
        self._active_now = max(now, state["last_time"])
        self._blocked_until = state["blocked_until"]
        self._used = {tuple(row[:3]): row[3] for row in state["used"]}
        return now

    def _save(self):
        state = {
            "schema_version": 1,
            "policy": [[w.scope, w.limit, w.seconds] for w in self._windows],
            "last_time": self._active_now,
            "blocked_until": self._blocked_until,
            "used": [[*key, value] for key, value in sorted(self._used.items())],
        }
        temporary = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w", encoding="utf-8", dir=self.path.parent, prefix=".budget-", delete=False
            ) as stream:
                temporary = Path(stream.name)
                json.dump(state, stream, separators=(",", ":"), allow_nan=False)
                stream.flush()
                os.fsync(stream.fileno())
            replace_atomic(temporary, self.path)
        except (OSError, TypeError, ValueError):
            raise StorageError("public request budget recording unavailable") from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _apply(self, method, *args):
        with self._thread_lock, self._locked():
            real_now = self._load()
            try:
                result = method(*args)
            except RateLimited as exc:
                self._save()
                # Include clock rollback in the wait instead of retrying a frozen
                # window every second while the wall clock catches up.
                raise RateLimited((exc.retry_after or 0) + self._active_now - real_now) from None
            self._save()
            return result

    def reserve(self, costs):
        return self._apply(super().reserve, costs)

    def cooldown(self, seconds):
        return self._apply(super().cooldown, seconds)

    def observe_usage(self, scope, seconds, used):
        return self._apply(super().observe_usage, scope, seconds, used)
