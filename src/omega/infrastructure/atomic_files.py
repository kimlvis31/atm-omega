"""Bounded retry of a completed atomic replacement on Windows sharing errors."""

# Imports
from os import PathLike
from os import name as _platform
from os import replace as _replace
from time import monotonic as _clock
from time import sleep as _sleep

# Constants
_RETRY_WINERRORS = {5, 32, 33}
_RETRY_SECONDS = 0.250
_INITIAL_DELAY = 0.010
_MAX_DELAY = 0.050


def replace_atomic(source: str | PathLike[str], target: str | PathLike[str]) -> None:
    """Retry only the same replace operation; never rewrite or remove either file.

    The deadline bounds retry scheduling, not the duration of an OS filesystem
    call or an overscheduled sleep. Permanent errors still propagate unchanged.
    """
    deadline, delay = _clock() + _RETRY_SECONDS, _INITIAL_DELAY
    while True:
        try:
            _replace(source, target)
            return
        except OSError as exc:
            if _platform != "nt" or getattr(exc, "winerror", None) not in _RETRY_WINERRORS:
                raise
            remaining = deadline - _clock()
            if remaining <= 0:
                raise
            _sleep(min(delay, remaining))
            if _clock() >= deadline:
                raise
            delay = min(delay * 2, _MAX_DELAY)
