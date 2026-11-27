"""Immutable UTC bar alignment and finite session calendars."""

from bisect import bisect_right
from dataclasses import dataclass
from operator import itemgetter

from omega.domain.errors import ValidationError


@dataclass(frozen=True, slots=True)
class BarSchedule:
    """Bar interval and optional resolved UTC sessions, represented in epoch ms.

    Sessions are half-open ``(open_ms, close_ms)`` intervals. ``None`` means
    continuous coverage, an empty tuple means unresolved coverage, and a
    non-empty tuple is finite and never extrapolated past its supplied close.
    """

    interval_ms: int = 60_000
    sessions: tuple[tuple[int, int], ...] | None = None
    receipt_grace_ms: int = 0

    def __post_init__(self):
        if type(self.interval_ms) is not int or self.interval_ms <= 0 or self.interval_ms % 60_000:
            raise ValidationError("bar interval must be a positive whole-minute duration")
        if type(self.receipt_grace_ms) is not int or self.receipt_grace_ms < 0:
            raise ValidationError("receipt grace must be a non-negative duration")
        if self.sessions is not None and type(self.sessions) is not tuple:
            raise ValidationError("sessions must be an immutable tuple or None")
        previous_close = -1
        for session in self.sessions or ():
            if (
                type(session) is not tuple
                or len(session) != 2
                or any(type(value) is not int or value < 0 for value in session)
            ):
                raise ValidationError("session bounds must be non-negative UTC milliseconds")
            start, close = session
            if (
                start % 60_000
                or close % 60_000
                or close <= start
                or (close - start) % self.interval_ms
            ):
                raise ValidationError("session bounds must align to whole bars")
            if start < previous_close:
                raise ValidationError("sessions must be ordered and non-overlapping")
            previous_close = close

    def _session_index(self, timestamp_ms: int) -> int | None:
        sessions = self.sessions or ()
        index = bisect_right(sessions, timestamp_ms, key=itemgetter(0)) - 1
        if index >= 0 and timestamp_ms < sessions[index][1]:
            return index
        return None

    def aligned_start(self, timestamp_ms: int) -> int | None:
        if type(timestamp_ms) is not int or timestamp_ms < 0:
            raise ValidationError("UTC timestamp must be non-negative milliseconds")
        if self.sessions is None:
            return timestamp_ms // self.interval_ms * self.interval_ms
        index = self._session_index(timestamp_ms)
        if index is None:
            return None
        start, _close = self.sessions[index]
        return start + (timestamp_ms - start) // self.interval_ms * self.interval_ms

    def coverage_known(self, timestamp_ms: int) -> bool:
        """Whether timestamp lies inside the resolved calendar horizon, including closures."""
        if type(timestamp_ms) is not int or timestamp_ms < 0:
            raise ValidationError("UTC timestamp must be non-negative milliseconds")
        if self.sessions is None:
            return True
        return bool(self.sessions) and self.sessions[0][0] <= timestamp_ms <= self.sessions[-1][1]

    def contains_open(self, open_ms: int) -> bool:
        if type(open_ms) is not int or open_ms < 0:
            raise ValidationError("aligned bar start required")
        if self.sessions is None:
            return open_ms % self.interval_ms == 0
        index = self._session_index(open_ms)
        if index is None:
            return False
        start, close = self.sessions[index]
        return open_ms + self.interval_ms <= close and (open_ms - start) % self.interval_ms == 0

    def is_terminal_cursor(self, cursor_ms: int) -> bool:
        """Whether cursor is exactly the close of this resolved calendar horizon."""
        if type(cursor_ms) is not int or cursor_ms < 0:
            raise ValidationError("aligned cursor required")
        return bool(self.sessions) and cursor_ms == self.sessions[-1][1]

    def next_expected(self, open_ms: int) -> int | None:
        if type(open_ms) is not int or open_ms < 0:
            raise ValidationError("aligned bar start required")
        if self.sessions is None:
            if open_ms % self.interval_ms:
                raise ValidationError("aligned bar start required")
            return open_ms + self.interval_ms
        index = self._session_index(open_ms)
        if index is None:
            return None
        start, close = self.sessions[index]
        if (open_ms - start) % self.interval_ms:
            raise ValidationError("aligned bar start required")
        following = open_ms + self.interval_ms
        if following < close:
            return following
        return self.sessions[index + 1][0] if index + 1 < len(self.sessions) else None

    def contiguous_missing_range(
        self, start_ms: int, now_ms: int, *, chunk: int = 500
    ) -> tuple[int, int] | None:
        if (
            type(start_ms) is not int
            or start_ms < 0
            or self.sessions is None
            and start_ms % self.interval_ms
            or type(now_ms) is not int
            or now_ms < 0
            or type(chunk) is not int
            or not 1 <= chunk <= 20_000
        ):
            raise ValidationError("aligned cursor, UTC time, and bounded chunk required")
        deadline_ms = max(0, now_ms - self.receipt_grace_ms)
        if self.sessions is None:
            end = min(
                deadline_ms // self.interval_ms * self.interval_ms,
                start_ms + chunk * self.interval_ms,
            )
            return (start_ms, end) if start_ms < end else None
        index = self._session_index(start_ms)
        if index is None:
            return None
        first_start, first_close = self.sessions[index]
        if (start_ms - first_start) % self.interval_ms:
            return None
        elapsed_intervals = max(0, (deadline_ms - first_start) // self.interval_ms)
        matured_end = first_start + elapsed_intervals * self.interval_ms
        end = min(first_close, start_ms + chunk * self.interval_ms, matured_end)
        return (start_ms, end) if start_ms < end else None
