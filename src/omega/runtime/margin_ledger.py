"""Stable owner-ledger pages and bounded observational worker tickets, with no IO."""

import base64
import json
import math
import re
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from hashlib import sha256

from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import MAX_EVENTS, MAX_TIME, MarginAccount
from omega.runtime.margin_store import ledger_entry_document

SCOPE = "virtual_margin_v2_ledger"
MAX_READS = 8
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_SEED = sha256(b"omega.margin-ledger-prefix.v1").hexdigest()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _snapshot(data):
    return base64.urlsafe_b64encode(data).decode().rstrip("=")


def _parse_snapshot(value):
    if type(value) is not str or not 1 <= len(value) <= 256:
        raise ValidationError("ledger snapshot must be a bounded opaque string")
    try:
        raw = base64.b64decode(value + "=" * (-len(value) % 4), altchars=b"-_", validate=True)
    except (ValueError, TypeError):
        raise ValidationError("invalid ledger snapshot encoding") from None
    if len(raw) != 133 or raw[0] != 1 or _snapshot(raw) != value:
        raise ValidationError("invalid ledger snapshot version or shape")
    return (
        int.from_bytes(raw[1:5], "big"),
        *(raw[start : start + 32].hex() for start in (5, 37, 69, 101)),
    )


@dataclass(frozen=True, slots=True)
class LedgerQuery:
    offset: int = 0
    limit: int = 128
    snapshot: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None

    def __post_init__(self):
        if type(self.offset) is not int or not 0 <= self.offset <= MAX_EVENTS:
            raise ValidationError("ledger offset must be a bounded native integer")
        if type(self.limit) is not int or not 1 <= self.limit <= 1000:
            raise ValidationError("ledger limit must be a native integer within1..1000")
        for value in (self.start_ms, self.end_ms):
            if value is not None and (type(value) is not int or not 0 <= value <= MAX_TIME):
                raise ValidationError("ledger filters require bounded native UTC milliseconds")
        if self.start_ms is not None and self.end_ms is not None and self.start_ms >= self.end_ms:
            raise ValidationError("ledger start must precede its exclusive end")
        if self.snapshot is not None:
            count, *_ = _parse_snapshot(self.snapshot)
            if count > MAX_EVENTS:
                raise ValidationError("ledger snapshot prefix exceeds capacity")

    def arguments(self):
        return {
            "offset": self.offset,
            "limit": self.limit,
            "snapshot": self.snapshot,
            "start_ms": self.start_ms,
            "end_ms": self.end_ms,
        }


def unavailable_ledger(account_id, query, reason):
    return {
        "schema_version": 1,
        "scope": SCOPE,
        "state": "unavailable",
        "account_id": account_id,
        "snapshot": None,
        "ledger_total": None,
        "ledger_capacity": None,
        "total": None,
        "offset": query.offset,
        "limit": query.limit,
        "next_offset": None,
        "rows": [],
        "reason_code": reason,
    }


class LedgerPages:
    """Owner-local append cache: serialize old facts once, page selected rows only."""

    def __init__(self, namespace):
        if type(namespace) is not str or _SHA.fullmatch(namespace) is None:
            raise ValidationError("ledger namespace needs an immutable hash")
        self.namespace = namespace
        self._ledger = None
        self._prefixes, self._tails = [_SEED], [_SEED]

    def _index(self, ledger):
        if ledger is self._ledger:
            return
        previous = self._ledger
        if (
            previous is None
            or len(previous) > len(ledger)
            or any(old is not ledger[index] for index, old in enumerate(previous))
        ):
            prefixes, tails = [_SEED], [_SEED]
            start = 0
        else:
            start = len(previous)
            prefixes, tails = list(self._prefixes), list(self._tails)
        for entry in ledger[start:]:
            raw = _json(ledger_entry_document(entry))
            tails.append(sha256(raw).hexdigest())
            prefixes.append(sha256(bytes.fromhex(prefixes[-1]) + raw).hexdigest())
        self._ledger, self._prefixes, self._tails = ledger, prefixes, tails

    def page(self, source_anchor, account, query):
        if (
            type(source_anchor) is not str
            or _SHA.fullmatch(source_anchor) is None
            or type(account) is not MarginAccount
            or type(query) is not LedgerQuery
        ):
            raise ProtocolError("confirmed immutable ledger observation required")
        self._index(account.ledger)
        anchor = sha256(_json((self.namespace, source_anchor, account.account_id))).hexdigest()
        selection = sha256(_json((account.account_id, query.start_ms, query.end_ms))).hexdigest()
        count = len(account.ledger)
        if query.snapshot is not None:
            count, original_anchor, tail, prefix, original_selection = _parse_snapshot(
                query.snapshot
            )
            if (
                count > len(account.ledger)
                or original_anchor != anchor
                or selection != original_selection
                or self._tails[count] != tail
                or self._prefixes[count] != prefix
            ):
                raise ProtocolError("ledger snapshot prefix, lineage or selection changed")
        token = _snapshot(
            b"\x01"
            + count.to_bytes(4, "big")
            + bytes.fromhex(anchor)
            + bytes.fromhex(self._tails[count])
            + bytes.fromhex(self._prefixes[count])
            + bytes.fromhex(selection)
        )
        selected = [
            index
            for index in range(count)
            if (
                query.start_ms is None or account.ledger[index].fact.event_time_ms >= query.start_ms
            )
            and (query.end_ms is None or account.ledger[index].fact.event_time_ms < query.end_ms)
        ]
        total = len(selected)
        if query.offset > total:
            raise ValidationError("ledger offset exceeds its frozen selection")
        indexes = selected[query.offset : query.offset + query.limit]
        ending = query.offset + len(indexes)
        return {
            "schema_version": 1,
            "scope": SCOPE,
            "state": "ready",
            "account_id": account.account_id,
            "snapshot": token,
            "ledger_total": count,
            "ledger_capacity": account.event_capacity,
            "total": total,
            "offset": query.offset,
            "limit": query.limit,
            "next_offset": ending if ending < total else None,
            "rows": [ledger_entry_document(account.ledger[index]) for index in indexes],
            "reason_code": None,
        }


@dataclass(eq=False, slots=True)
class _LedgerRead:
    account_id: str
    query: LedgerQuery
    deadline: float
    event: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None
    expired: bool = False


class MarginLedgerReads:
    """At most8 observations; financial/shutdown scheduling stays with the worker."""

    def __init__(self, owner):
        self.owner = owner
        self.queue, self.waiters = deque(), set()

    def request(self, account_id, *, timeout=5, **arguments):
        if type(account_id) is not str or _ID.fullmatch(account_id) is None:
            raise ValidationError("invalid ledger account identity")
        query = LedgerQuery(**arguments)
        if type(timeout) not in {int, float} or not math.isfinite(timeout) or not 0 < timeout <= 5:
            raise ValidationError("ledger timeout must be within(0,5]seconds")
        ticket = _LedgerRead(account_id, query, time.monotonic() + timeout)
        owner = self.owner
        with owner.condition:
            if (
                owner.closed
                or not owner.ready
                or owner.error
                or owner.thread is None
                or not owner.thread.is_alive()
            ):
                return unavailable_ledger(account_id, query, "owner_unavailable")
            if threading.current_thread() is owner.thread:
                return unavailable_ledger(account_id, query, "owner_reentrant_read")
            if len(self.waiters) >= MAX_READS:
                return unavailable_ledger(account_id, query, "read_queue_full")
            self.waiters.add(ticket)
            self.queue.append(ticket)
            owner.condition.notify_all()
        ticket.event.wait(timeout)
        with owner.condition:
            if ticket.result is None:
                ticket.expired = True
                self.waiters.discard(ticket)
                try:
                    self.queue.remove(ticket)
                except ValueError:
                    pass
                return unavailable_ledger(account_id, query, "read_timeout")
            return ticket.result

    def serve_one(self):
        owner, ticket = self.owner, None
        with owner.condition:
            while self.queue:
                candidate = self.queue.popleft()
                if not candidate.expired and candidate.deadline > time.monotonic():
                    ticket = candidate
                    break
                self.waiters.discard(candidate)
            if ticket is None:
                return
            session = owner.sessions.get(ticket.account_id)
            accounts = owner.data.get("accounts") if type(owner.data) is dict else None
            slot = accounts.get(ticket.account_id) if type(accounts) is dict else None
            unavailable = (
                owner.closed
                or not owner.ready
                or owner.error
                or ticket.account_id in owner.blocked_accounts
                or session is None
                or type(slot) is not dict
                or slot.get("creation_state") != "created"
            )
        if unavailable:
            result = unavailable_ledger(ticket.account_id, ticket.query, "account_unavailable")
        else:
            try:
                result = session.ledger_page(**ticket.query.arguments())
                expected = set(unavailable_ledger(ticket.account_id, ticket.query, ""))
                if (
                    type(result) is not dict
                    or set(result) != expected
                    or type(result.get("schema_version")) is not int
                    or result["schema_version"] != 1
                    or result.get("scope") != SCOPE
                    or result.get("account_id") != ticket.account_id
                    or result.get("state") not in {"ready", "unavailable"}
                ):
                    raise ProtocolError("ledger result has another identity or invalid schema")
            except ProtocolError:
                result = unavailable_ledger(ticket.account_id, ticket.query, "snapshot_conflict")
            except ValidationError:
                result = unavailable_ledger(ticket.account_id, ticket.query, "query_rejected")
            except (StorageError, TradingBlocked):
                result = unavailable_ledger(
                    ticket.account_id, ticket.query, "financial_unavailable"
                )
            except Exception:
                result = unavailable_ledger(ticket.account_id, ticket.query, "ledger_unavailable")
        with owner.condition:
            self.waiters.discard(ticket)
            if not ticket.expired and ticket.deadline > time.monotonic():
                ticket.result = (
                    result
                    if not owner.closed
                    else unavailable_ledger(ticket.account_id, ticket.query, "owner_closed")
                )
                ticket.event.set()

    def fail(self, reason):
        # The caller holds the owning condition during retirement/failure.
        for ticket in self.waiters:
            ticket.result = unavailable_ledger(ticket.account_id, ticket.query, reason)
            ticket.event.set()
        self.waiters.clear()
        self.queue.clear()
