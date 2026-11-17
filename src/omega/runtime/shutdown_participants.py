"""Owner adapters for shutdown; never calculate an execution price."""

from contextlib import contextmanager

from omega.domain.errors import ValidationError


def empty_view(owner, *, ready=True):
    return {
        "owner": owner,
        "ready": ready,
        "durable": ready,
        "supports_flatten": ready,
        "positions": [],
        "pending_orders": [],
        "unknown": [] if ready else ["owner_unavailable"],
        "notes": [],
    }


class PaperShutdownParticipant:
    def __init__(self, core):
        self.core = core

    @contextmanager
    def _owner(self):
        while not self.core._closing:
            if self.core.owner_mutation_lock.acquire(timeout=0.1):
                try:
                    if self.core._closing:
                        raise ValidationError("core is closing")
                    yield
                    return
                finally:
                    self.core.owner_mutation_lock.release()
        raise ValidationError("core is closing")

    def shutdown_snapshot(self):
        with self._owner():
            return (
                self.core.paper.shutdown_snapshot() if self.core.paper else empty_view("paper_v1")
            )

    def shutdown_drain(self, operation_id, issued_at_ms):
        with self._owner():
            return (
                self.core.paper.shutdown_drain(operation_id, issued_at_ms)
                if self.core.paper
                else {}
            )

    def shutdown_cancel(self, operation_id):
        with self._owner():
            return self.core.paper.shutdown_cancel(operation_id) if self.core.paper else {}


class MarginShutdownParticipant:
    def __init__(self, core):
        self.core = core

    def shutdown_snapshot(self):
        # A rendezvous must not hold the core mutation lock needed by this actor.
        if self.core._closing:
            return empty_view("margin_v2", ready=False)
        owner = self.core.margin_trading
        return owner.inspect_shutdown(timeout=5) if owner else empty_view("margin_v2")

    def shutdown_drain(self, operation_id, issued_at_ms):
        owner = self.core.margin_trading
        if self.core._closing:
            raise ValidationError("core is closing")
        return owner.shutdown_drain(operation_id, issued_at_ms) if owner else {"flat": True}

    def shutdown_cancel(self, operation_id):
        owner = self.core.margin_trading
        return owner.shutdown_cancel(operation_id) if owner else {"pending_preserved": True}
