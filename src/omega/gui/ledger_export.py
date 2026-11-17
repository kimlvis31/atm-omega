"""Pure Python export worker, observed by an active-only GUI-thread Qt bridge."""

import threading
import time
from pathlib import Path
from uuid import uuid4

from PySide6.QtCore import QObject, Qt, QTimer, Signal


class _ExportState:
    """No QObject, GUI callback, bound task or signal may enter worker state."""

    def __init__(self, staging):
        self.staging = staging
        self.condition = threading.Condition()
        self.cancelled, self.ticket, self.finished = False, None, False
        self.result, self.error, self.cleanup_error = None, "", None

    def cancel(self):
        with self.condition:
            self.cancelled = True
            self.condition.notify_all()
        self.discard_safely()

    def discard(self):
        # Only this task's unique staging ZIP; the user's prior export is untouched.
        self.staging.unlink(missing_ok=True)

    def discard_safely(self):
        try:
            self.discard()
        except OSError as error:
            self.cleanup_error = str(error)[:300]

    def deliver(self, ticket, data=None, error=None):
        with self.condition:
            if self.ticket is ticket and not self.cancelled:
                ticket.update(done=True, data=data, error=error)
                self.condition.notify_all()

    def read_page(self, account_id, query):
        with self.condition:
            if self.cancelled:
                raise ValueError("Ledger export cancelled")
            ticket = {"account_id": account_id, "query": dict(query), "done": False, "sent": False}
            self.ticket = ticket
            deadline = time.monotonic() + 6
            while not ticket["done"] and not self.cancelled:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("Ledger page timed out")
                self.condition.wait(remaining)
            self.ticket = None
            if self.cancelled:
                raise ValueError("Ledger export cancelled")
            if ticket.get("error"):
                raise ValueError(ticket["error"])
            return ticket["data"]


def _run_export(state, account_id, filters):
    """Thread target and its arguments are Python-only; it never touches Qt."""
    result, error = None, ""
    try:
        from omega.runtime.ledger_client import export_margin_ledger

        result = export_margin_ledger(
            state.read_page,
            state.staging,
            account_id=account_id,
            page_size=128,
            **filters,
        )
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:300]}"
    finally:
        if state.cancelled or error:
            state.discard_safely()
        with state.condition:
            state.result, state.error, state.finished = result, error, True


class LedgerExportTask(QObject):
    readRequested = Signal(object)
    completed = Signal(object, object, str)

    def __init__(self, destination, *, account_id, filters, context, parent=None):
        super().__init__(parent)
        self.destination = Path(destination)
        self.staging = self.destination.with_name(f".{self.destination.name}.{uuid4().hex}.zip")
        self.context = context
        self.state = _ExportState(self.staging)
        self.thread = threading.Thread(
            target=_run_export,
            args=(self.state, account_id, dict(filters)),
            name="omega-ledger-export",
            daemon=False,
        )
        self.timer = QTimer(self)
        self.timer.setInterval(20)
        self.timer.timeout.connect(self._poll)
        self._parent_connection = None
        state = self.state
        self.destroyed.connect(lambda *_args: state.cancel(), Qt.ConnectionType.DirectConnection)
        if parent is not None:
            # The callback retains only ordinary Python state, never an invalid
            # task wrapper. Destroying Qt cannot invalidate the running worker.
            self._parent_connection = parent.destroyed.connect(
                lambda *_args: state.cancel(), Qt.ConnectionType.DirectConnection
            )

    @property
    def cancelled(self):
        return self.state.cancelled

    @property
    def cleanup_error(self):
        return self.state.cleanup_error

    def start(self):
        self.thread.start()
        self.timer.start()

    def cancel(self):
        self.state.cancel()

    def deliver(self, ticket, data=None, error=None):
        self.state.deliver(ticket, data, error)

    def discard(self):
        self.state.discard()

    def _poll(self):
        # This slot and both Qt signals execute only on the GUI thread. The
        # completion receiver may delete the task: the worker has already exited.
        state = self.state
        with state.condition:
            ticket = state.ticket
            send = ticket is not None and not ticket["sent"] and not state.cancelled
            if send:
                ticket["sent"] = True
        if send:
            self.readRequested.emit(ticket)
        if state.finished and not self.thread.is_alive():
            self.timer.stop()
            if self._parent_connection is not None:
                QObject.disconnect(self._parent_connection)
                self._parent_connection = None
            self.completed.emit(self, state.result, state.error)
