"""Bound consecutive uncertain read-only job lookups without changing job identity."""


class ChartLookupPolicy:
    def __init__(self, limit=8):
        if type(limit) is not int or not 1 <= limit <= 32:
            raise ValueError("chart lookup failure limit must be an integer from1to32")
        self._limit = limit
        self._identity = None
        self._failures = 0
        self._paused = False

    def bind(self, identity):
        if type(identity) is not str or not identity:
            raise ValueError("chart lookup requires a nonempty job identity")
        if identity != self._identity:
            self._identity = identity
            self._failures, self._paused = 0, False

    def record_failure(self):
        self._failures = min(self._limit, self._failures + 1)
        self._paused = self._failures >= self._limit
        return self._paused

    def accept_response(self):
        self._failures, self._paused = 0, False

    def permits_auto(self, state):
        return (
            type(state) is str
            and self._identity is not None
            and not self._paused
            and state in {"queued", "running", "unknown"}
        )
