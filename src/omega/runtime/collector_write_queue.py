"""One durable write per turn across the collector's bounded request queues."""

from collections import deque
from concurrent.futures import Future
from threading import Condition, Thread

from omega.infrastructure.candles import DatabaseUnavailable


def run_write(_kind, function, /, *args, **kwargs):
    return function(*args, **kwargs)


class CollectorWriteQueue:
    def __init__(self):
        self.lanes = {
            name: deque()
            for name in ("cursor", "stream_bars", "stream_auxiliary", "repair", "history")
        }
        self.turns = deque(self.lanes.values())
        self.changed = Condition()
        self.closed = False
        self.pending = 0
        self.worker = Thread(target=self._run, name="collector-write")
        self.worker.start()

    def run(self, kind, function, /, *args, **kwargs):
        with self.changed:
            self.changed.wait_for(lambda: self.pending < 8 or self.closed)
            if self.closed:
                raise DatabaseUnavailable("collector writer has closed")
            result = Future()
            self.lanes[kind].append((result, function, args, kwargs))
            self.pending += 1
            self.changed.notify_all()
        return result.result()

    def _run(self):
        while True:
            with self.changed:
                self.changed.wait_for(lambda: self.pending or self.closed)
                if not self.pending:
                    return
                while not self.turns[0]:
                    self.turns.rotate(-1)
                result, function, args, kwargs = self.turns[0].popleft()
                self.turns.rotate(-1)
                self.pending -= 1
                self.changed.notify_all()
            try:
                result.set_result(function(*args, **kwargs))
            except BaseException as error:
                result.set_exception(error)

    def close(self):
        with self.changed:
            self.closed = True
            self.changed.notify_all()
        self.worker.join()
