"""Bounded shutdown mutations marshaled to the existing margin financial owner."""

import math
import threading
from collections import deque
from copy import deepcopy
from dataclasses import dataclass, field

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.runtime.margin_console import scenario_brackets


@dataclass(eq=False, slots=True)
class _Mutation:
    kind: str
    operation_id: str
    issued_at_ms: int | None
    event: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None
    error: Exception | None = None
    started: bool = False
    expired: bool = False


class MarginShutdownBridge:
    def __init__(self, owner):
        self.owner = owner
        self.queue, self.waiters = deque(), set()

    def request(self, kind, operation_id, issued_at_ms=None, *, timeout=5):
        from omega.runtime.margin_trading import _identity

        _identity(operation_id)
        if kind not in {"drain", "cancel"}:
            raise ValidationError("invalid margin shutdown mutation")
        if kind == "drain" and (
            type(issued_at_ms) is not int or not 0 <= issued_at_ms <= 253_402_300_799_000
        ):
            raise ValidationError("invalid margin shutdown issue time")
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 30:
            raise ValidationError("shutdown mutation timeout must be within (0,30]")
        owner, ticket = self.owner, _Mutation(kind, operation_id, issued_at_ms)
        with owner.condition:
            if (
                owner.closed
                or not owner.ready
                or owner.error
                or owner.thread is None
                or not owner.thread.is_alive()
                or threading.current_thread() is owner.thread
            ):
                raise StorageError("margin shutdown owner is unavailable")
            if len(self.waiters) >= 8:
                raise ValidationError("margin shutdown mutation queue is full")
            self.waiters.add(ticket)
            self.queue.append(ticket)
            owner.condition.notify_all()
        ticket.event.wait(timeout)
        with owner.condition:
            if not ticket.event.is_set():
                ticket.expired = True
                self.waiters.discard(ticket)
                if not ticket.started:
                    try:
                        self.queue.remove(ticket)
                    except ValueError:
                        pass
                raise StorageError(
                    "margin shutdown outcome is unconfirmed; inspect the original ID"
                )
            if ticket.error is not None:
                raise ticket.error
            return deepcopy(ticket.result)

    def fail(self):
        # Called while holding the owner's condition; never confirms a mutation.
        for ticket in self.waiters:
            ticket.error = StorageError("margin shutdown owner retired; outcome unconfirmed")
            ticket.event.set()
        self.waiters.clear()
        self.queue.clear()

    def serve(self):
        owner = self.owner
        with owner.condition:
            tickets = tuple(self.queue)
            self.queue.clear()
        for ticket in tickets:
            with owner.condition:
                if ticket.expired or owner.closed:
                    continue
                ticket.started = True
            try:
                with owner._mutation():
                    result = self._execute(ticket)
            except Exception as error:
                ticket.error = error
            else:
                ticket.result = result
            finally:
                with owner.condition:
                    self.waiters.discard(ticket)
                    ticket.event.set()

    def _execute(self, ticket):
        owner = self.owner
        retirement = getattr(owner, "retirement", None)
        if retirement is not None and any(
            retirement.gated(identity) for identity in owner.data["accounts"]
        ):
            raise TradingBlocked("account retirement must finish before financial shutdown")
        owner._guard()
        if owner.error or not owner.ready:
            raise StorageError("margin shutdown owner is unavailable")
        if ticket.kind == "cancel":
            for account_id, session in owner.sessions.items():
                status = session.shutdown_status()
                if status["operation_id"] == ticket.operation_id:
                    session.shutdown_cancel(ticket.operation_id)
                    owner._refresh(account_id, feed_state="shutdown_cancelled")
            return {"pending_preserved": True}
        if owner.blocked_accounts or any(
            row["state"] in {"queued", "running"} for row in owner.operations.values()
        ):
            raise TradingBlocked("margin financial operations need confirmation first")
        accounts = owner.data["accounts"]
        if set(accounts) != set(owner.sessions):
            raise TradingBlocked("margin shutdown account registry is incomplete")
        plans = []
        for account_id, slot in sorted(accounts.items()):
            if slot["creation_state"] != "created":
                raise TradingBlocked("margin account creation is unconfirmed")
            session = owner.sessions[account_id]
            brackets = scenario_brackets(slot["parameters"]["brackets"])
            plan = session.shutdown_preflight(
                ticket.operation_id, ticket.issued_at_ms, brackets=brackets
            )
            if type(plan) is not dict or plan.get("ready") is not True:
                raise TradingBlocked("margin shutdown preflight failed: " + str(plan.get("reason")))
            plans.append((account_id, session, brackets, plan))
        candidate = deepcopy(owner.data)
        for account_id, _, _, plan in plans:
            candidate["accounts"][account_id]["shutdown_control"] = {
                "operation_id": ticket.operation_id,
                "issued_at_ms": ticket.issued_at_ms,
            }
            candidate["accounts"][account_id]["feed_enabled"] = bool(
                plan["positions"] or plan["pending"]
            )
        try:
            owner._write(candidate)  # Persist the TS fence before a financial mutation.
        except Exception:
            owner.blocked_accounts.update(accounts)
            if owner.on_incident is not None:
                owner.on_incident("margin_shutdown_recording_unavailable", "registry")
            raise
        for account_id, session, brackets, _ in plans:
            try:
                session.shutdown_drain(ticket.operation_id, ticket.issued_at_ms, brackets=brackets)
            except Exception:
                owner.blocked_accounts.add(account_id)
                owner._refresh(account_id, feed_state="blocked", manual_recovery_required=True)
                if owner.on_incident is not None:
                    owner.on_incident("margin_shutdown_recording_unavailable", account_id)
                raise
            owner._refresh(account_id, feed_state="draining", feed_reason=None)
        return {"accepted": True, "accounts": [row[0] for row in plans]}
