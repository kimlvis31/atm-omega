"""Query-only durable order observations; never dispatch, fill settlement or resume.

All original requests are restored first and their UNKNOWN reservations stay
untouched. Broker.query is used deliberately instead of mutating reconcile().
Historical records are facts, not fresh primary receipts or current account truth.
The caller owns serialization with every other account operation.
"""

# Imports
import json
from copy import deepcopy
from dataclasses import dataclass
from hashlib import sha256
from threading import RLock

from omega.application.execution_bindings import _number, _parse_number, _read_document
from omega.application.orders import _TERMINAL, OrderCoordinator, _validate_observation
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.models import OrderObservation, OrderRequest, OrderStatus
from omega.domain.safety import BlockReason
from omega.ports.journal import Durability, RecordSink

# Constants
KIND = "ExecutionRecoveryObserved"
MAX_RECORD_BYTES = 61_440


def _encoded(event):
    raw = json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_RECORD_BYTES:
        raise ValidationError("execution recovery record exceeds capacity")
    return raw


def _fingerprint(event):
    return sha256(_encoded(event)).hexdigest()


def _request(request):
    return {
        "instrument": request.instrument.key,
        "client_order_id": request.client_order_id,
        "side": request.side.value,
        "order_kind": request.kind.value,
        "quantity": _number(request.quantity),
        "price": None if request.price is None else _number(request.price),
        "reduce_only": request.reduce_only,
    }


def _facts(observation):
    return {
        "provider_order_id": observation.provider_order_id,
        "status": observation.status.value,
        "executed_quantity": _number(observation.executed_quantity),
        "cumulative_quote": _number(observation.cumulative_quote),
    }


@dataclass(frozen=True, slots=True)
class RecoveryObservation:
    request: OrderRequest
    observation: OrderObservation
    revision: int
    record_sha256: str
    state: str  # historical or primary_acknowledged; neither is account settlement.
    source_bound: bool


class ExecutionObservationRecovery:
    """One existing coordinator/sink owner, with an explicit query-only API.

    snapshot() never exposes a pending write. Retrying recover() commits its exact
    saved candidate with a fresh PRIMARY ACK before another query for that order.
    Restart views remain historical until queried and freshly recorded. No gate
    is cleared and no restored Operation is mutated by this owner.
    """

    def __init__(self, account_id: str, coordinator: OrderCoordinator, sink: RecordSink):
        if (
            type(coordinator) is not OrderCoordinator
            or type(account_id) is not str
            or coordinator.account_id != account_id
            or coordinator.sink is not sink
        ):
            raise ValidationError("recovery requires the original account coordinator and sink")
        self.account_id, self.coordinator, self.sink = account_id, coordinator, sink
        self._lock = RLock()
        self._pending, self._known, self._views = {}, set(), {}
        self._faulted = False
        operations = coordinator.restore_history()  # ALL originals, before any query.
        if any(
            operation.state != "UNKNOWN"
            or operation.observation is not None
            or operation.terminal_review_required
            for operation in operations
        ):
            raise TradingBlocked("query recovery requires a fresh restored coordinator")
        self._requests = {
            operation.request.client_order_id: operation.request for operation in operations
        }
        for request in self._requests.values():
            _request(request)  # Reject amounts outside the bounded record contract before queries.
        rows = self._rows()
        self._originals = self._intent_rows(rows)
        self._sources = self._source_bindings(rows)
        self._legacy_bounds = self._legacy_rows(rows)
        self._known, self._views = self._replay(rows)

    def _rows(self):
        rows = self.sink.records()
        if type(rows) is not tuple or any(type(row) is not dict for row in rows):
            raise JournalError("recovery history must contain regular journal events")
        return rows

    def _intent_rows(self, rows):
        return frozenset(
            _encoded(row)
            for row in rows
            if (row.get("kind") == "OrderIntent" and row.get("account_id") == self.account_id)
        )

    def _source_bindings(self, rows):
        sources = {identity: None for identity in self._requests}
        reductions = {}
        for row in rows:
            if row.get("kind") == "OrderIntent" and row.get("account_id") == self.account_id:
                reductions.setdefault(row["client_order_id"], set()).add(row["reducing"])
        seen = {}
        for row in rows:
            if row.get("kind") != "ExecutionDecisionBound":
                continue
            try:
                decision, state_digest = _read_document(row)
            except (ValidationError, ValueError, TypeError, KeyError, ArithmeticError):
                raise JournalError("execution source binding history is malformed") from None
            cycle = decision.cycle
            key = cycle.account_id, cycle.strategy_id, cycle.cycle_id
            encoded = _encoded(row)
            if key in seen and seen[key] != encoded:
                raise ValidationError("execution cycle has conflicting durable source bindings")
            seen[key] = encoded
            if decision.cycle.account_id != self.account_id:
                continue
            for intent in decision.intents:
                identity = intent.request.client_order_id
                if identity not in self._requests:
                    continue
                if intent.request != self._requests[identity]:
                    raise ValidationError(
                        "source binding conflicts with the original order request"
                    )
                if reductions[identity] != {intent.reducing}:
                    raise ValidationError(
                        "source binding conflicts with original reduction controls"
                    )
                reference = {
                    "cycle": deepcopy(row["cycle"]),
                    "account_state_digest": state_digest,
                    "binding_sha256": _fingerprint(row),
                }
                if sources[identity] is not None and sources[identity] != reference:
                    raise ValidationError("order identity belongs to conflicting source bindings")
                sources[identity] = reference
        return sources

    def _legacy_rows(self, rows):
        result = {}
        for row in rows:
            if row.get("kind") != "OrderObserved" or row.get("account_id") != self.account_id:
                continue
            identity = row.get("client_order_id")
            if identity not in self._requests:
                raise JournalError("legacy observation has no original account intent")
            try:
                status = OrderStatus(row["status"])
                provider = row["provider_order_id"]
                if type(provider) is not str or not provider:
                    raise ValueError("invalid provider identity")
            except (KeyError, TypeError, ValueError):
                raise JournalError("legacy order observation is malformed") from None
            result.setdefault(identity, set()).add((provider, status))
        for values in result.values():
            if len({provider for provider, _ in values}) > 1:
                raise ValidationError("legacy provider order identity conflicts")
            if len({status for _, status in values if status in _TERMINAL}) > 1:
                raise TradingBlocked("legacy terminal snapshots conflict")
        return result

    def _legacy(self, request, observation):
        for provider, status in self._legacy_bounds.get(request.client_order_id, ()):
            if provider != observation.provider_order_id:
                raise ValidationError("recovery changed the legacy provider order identity")
            if status in _TERMINAL and status != observation.status:
                raise TradingBlocked("legacy terminal status changed; reconcile account and fills")
            if status == OrderStatus.PARTIALLY_FILLED and observation.status == OrderStatus.NEW:
                raise ValidationError("recovery regressed a legacy partial order")

    def _event(self, request, observation, revision, previous):
        event = {
            "kind": KIND,
            "schema_version": 1,
            "account_id": self.account_id,
            "request": _request(request),
            "observation": _facts(observation),
            "revision": revision,
            "previous_sha256": previous,
            "source_binding": self._sources[request.client_order_id],
        }
        _encoded(event)
        return event

    def _read_event(self, event):
        if (
            type(event) is not dict
            or set(event)
            != {
                "kind",
                "schema_version",
                "account_id",
                "request",
                "observation",
                "revision",
                "previous_sha256",
                "source_binding",
            }
            or type(event["schema_version"]) is not int
            or event["schema_version"] != 1
        ):
            raise JournalError("unsupported execution recovery record")
        try:
            request = self._requests[event["request"]["client_order_id"]]
            body = event["observation"]
            if (
                type(body["provider_order_id"]) is not str
                or not 1 <= len(body["provider_order_id"]) <= 128
            ):
                raise ValidationError("invalid bounded provider order identity")
            observation = OrderObservation(
                request.instrument,
                request.client_order_id,
                body["provider_order_id"],
                OrderStatus(body["status"]),
                _parse_number(body["executed_quantity"]),
                _parse_number(body["cumulative_quote"]),
            )
            revision = event["revision"]
            if type(revision) is not int or not 1 <= revision < 2**63:
                raise ValidationError("invalid recovery revision")
            if _encoded(
                self._event(request, observation, revision, event["previous_sha256"])
            ) != _encoded(event):
                raise ValidationError("recovery record differs from its original request or source")
            _validate_observation(request, observation)
        except (KeyError, TypeError, ValueError, ArithmeticError, ValidationError):
            raise JournalError("execution recovery record is malformed") from None
        return request, observation

    def _replay(self, rows):
        chains, records, views = {}, set(), {}
        for event in rows:
            if event.get("kind") != KIND or event.get("account_id") != self.account_id:
                continue
            request, observation = self._read_event(event)
            identity, revision = request.client_order_id, event["revision"]
            raw = _encoded(event)
            versions = chains.setdefault(identity, {})
            if revision in versions and versions[revision][0] != raw:
                raise JournalError("conflicting duplicate recovery revision")
            versions[revision] = raw, event, observation
            records.add(raw)
        for identity, versions in chains.items():
            previous, digest = None, None
            for expected_revision, revision in enumerate(sorted(versions), start=1):
                raw, event, observation = versions[revision]
                if revision != expected_revision or event["previous_sha256"] != digest:
                    raise JournalError("recovery revision chain is incomplete or changed")
                _validate_observation(self._requests[identity], observation, previous)
                self._legacy(self._requests[identity], observation)
                digest = sha256(raw).hexdigest()
                previous = observation
            views[identity] = RecoveryObservation(
                self._requests[identity],
                previous,
                len(versions),
                digest,
                "historical",
                self._sources[identity] is not None,
            )
        return records, views

    def _check_history(self):
        rows = self._rows()
        if (
            self._intent_rows(rows) != self._originals
            or self._source_bindings(rows) != self._sources
            or self._legacy_rows(rows) != self._legacy_bounds
        ):
            raise JournalError("original recovery requests or source bindings changed")
        records, _ = self._replay(rows)
        pending = {_encoded(event) for event in self._pending.values()}
        if not self._known <= records or records - self._known - pending:
            raise JournalError("recovery history changed outside its original owner")

    def _commit(self, identity):
        event = self._pending[identity]
        try:
            durability = self.sink.record(deepcopy(event))
        except (JournalError, OSError, TimeoutError):
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked("recovery primary acknowledgement is unavailable") from None
        if durability is not Durability.PRIMARY:
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked("recovery requires a fresh primary acknowledgement")
        request, observation = self._read_event(event)
        self._known.add(_encoded(event))
        self._views[identity] = RecoveryObservation(
            request,
            observation,
            event["revision"],
            _fingerprint(event),
            "primary_acknowledged",
            self._sources[identity] is not None,
        )
        del self._pending[identity]

    def snapshot(self) -> tuple[RecoveryObservation, ...]:
        with self._lock:
            self._check_history()
            return tuple(self._views[key] for key in sorted(self._views))

    def recover(self) -> tuple[RecoveryObservation, ...]:
        with self._lock:
            self.coordinator.gate.block(BlockReason.EXECUTION)
            if self._faulted:
                raise TradingBlocked("order recovery conflict requires explicit account review")
            self._check_history()
            for identity, request in sorted(self._requests.items()):
                if identity in self._pending:
                    self._commit(identity)  # No requery/replacement after an uncertain write.
                    continue
                previous = self._views.get(identity)
                try:
                    observation = self.coordinator.broker.query(request.instrument, identity)
                    _validate_observation(
                        request, observation, None if previous is None else previous.observation
                    )
                    self._legacy(request, observation)
                    event = self._event(
                        request,
                        observation,
                        1 if previous is None else previous.revision + 1,
                        None if previous is None else previous.record_sha256,
                    )
                    self._read_event(event)
                except (ValidationError, TradingBlocked, JournalError):
                    self._faulted = True
                    raise
                self._pending[identity] = event
                self._commit(identity)
            return tuple(self._views[key] for key in sorted(self._views))
