"""Shared admission across accounts/clients, using caller-supplied provider scopes."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from omega.domain.errors import RateLimited, ValidationError


@dataclass(frozen=True)
class Window:
    scope: str
    limit: int
    seconds: int

    def __post_init__(self):
        if not self.scope or self.limit <= 0 or self.seconds <= 0:
            raise ValidationError("invalid rate window")


class RequestBudget:
    """Fixed-window counters plus provider cooldown. No sleeps or busy polling.

    A gateway owner must share this instance for identical IP/account scopes.
    This is not a cross-process distributed quota service.
    """

    def __init__(self, windows: tuple[Window, ...], *, clock: Callable[[], float] = time.time):
        if not windows or len({(w.scope, w.seconds) for w in windows}) != len(windows):
            raise ValidationError("unique rate windows required")
        self._windows = windows
        self._clock = clock
        self._used: dict[tuple[str, int, int], int] = {}
        self._blocked_until = 0.0
        self._lock = threading.Lock()

    def reserve(self, costs: dict[str, int]) -> None:
        with self._lock:
            now = self._clock()
            if now < self._blocked_until:
                raise RateLimited(self._blocked_until - now)
            if any(v < 0 for v in costs.values()):
                raise ValidationError("negative request weight")
            if set(costs) - {w.scope for w in self._windows}:
                raise ValidationError("request rate scope has no configured provider limit")
            proposed = []
            for window in self._windows:
                slot = int(now // window.seconds)
                key = window.scope, window.seconds, slot
                used = self._used.get(key, 0) + costs.get(window.scope, 0)
                if used > window.limit:
                    raise RateLimited((slot + 1) * window.seconds - now)
                proposed.append((key, used))
            # Remove old slots so a long-running process has bounded state.
            self._used = dict(proposed)

    def cooldown(self, seconds: float) -> None:
        with self._lock:
            self._blocked_until = max(self._blocked_until, self._clock() + max(0, seconds))

    def observe_usage(self, scope: str, seconds: int, used: int) -> None:
        """Raise local usage from provider headers; a late header cannot lower it."""
        if used < 0:
            return
        with self._lock:
            if not any(w.scope == scope and w.seconds == seconds for w in self._windows):
                return
            key = scope, seconds, int(self._clock() // seconds)
            self._used[key] = max(self._used.get(key, 0), used)
