"""Single-writer durable decision/source binding, without dispatch or recovery authority.

A receipt describes this owner's observed primary ACK. It is not broker permission.
History, including an exact primary/fallback replay, always starts recovery-required.
The caller still owns account reconciliation, manual resume and OrderCoordinator.
"""

# Imports
import json
import re
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from threading import RLock

from omega.application.execution_intents import (
    ExecutionCycle,
    ExecutionDecision,
    ExecutionIntent,
)
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.execution_rules import bounded_decimal
from omega.domain.models import Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal
from omega.ports.journal import Durability, RecordSink

# Constants
KIND = "ExecutionDecisionBound"
MAX_BINDING_BYTES = 61_440  # Leave room for the default 65,536-byte journal envelope.


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise ValidationError("binding account state must be a canonical SHA256")
    return value


def _cycle(value):
    if type(value) is not ExecutionCycle:
        raise ValidationError("binding requires a typed execution cycle")
    return replace(value)


def _key(cycle):
    return cycle.account_id, cycle.strategy_id, cycle.cycle_id


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise ValidationError("invalid execution binding document fields")


def _number(value):
    bounded_decimal(value)
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _parse_number(value):
    if type(value) is not str or not 1 <= len(value) <= 160:
        raise ValidationError("binding amount requires a bounded canonical decimal string")
    amount = Decimal(value)
    if _number(amount) != value:
        raise ValidationError("binding amount is not canonical")
    return amount


def _cycle_document(value):
    value = _cycle(value)
    return {
        name: getattr(value, name)
        for name in ("account_id", "strategy_id", "cycle_id", "source_digest")
    }


def _read_cycle(value):
    _keys(value, ("account_id", "strategy_id", "cycle_id", "source_digest"))
    return ExecutionCycle(**value)


def _intent_document(value):
    if type(value) is not ExecutionIntent:
        raise ValidationError("binding requires the original typed execution intent")
    request = value.request
    proposal = value.proposal
    return {
        "cycle": _cycle_document(value.cycle),
        "instrument": {
            "provider": request.instrument.provider,
            "market": request.instrument.market.value,
            "symbol": request.instrument.symbol,
        },
        "ordinal": value.ordinal,
        "proposal": {
            "side": proposal.side.value,
            "kind": proposal.kind.value,
            "quantity": _number(proposal.quantity),
            "price": None if proposal.price is None else _number(proposal.price),
            "reducing": proposal.reducing,
            "reason": proposal.reason,
        },
        "client_order_id": request.client_order_id,
        "reduce_only": request.reduce_only,
    }


def _read_intent(value):
    _keys(
        value,
        (
            "cycle",
            "instrument",
            "ordinal",
            "proposal",
            "client_order_id",
            "reduce_only",
        ),
    )
    instrument, proposal = value["instrument"], value["proposal"]
    _keys(instrument, ("provider", "market", "symbol"))
    _keys(proposal, ("side", "kind", "quantity", "price", "reducing", "reason"))
    if any(type(proposal[name]) is not str for name in ("side", "kind", "reason")):
        raise ValidationError("binding proposal text must be native strings")
    if type(instrument["market"]) is not str:
        raise ValidationError("binding market must be a native string")
    intent = ExecutionIntent(
        _read_cycle(value["cycle"]),
        Instrument(instrument["provider"], Market(instrument["market"]), instrument["symbol"]),
        value["ordinal"],
        OrderProposal(
            Side(proposal["side"]),
            _parse_number(proposal["quantity"]),
            OrderKind(proposal["kind"]),
            None if proposal["price"] is None else _parse_number(proposal["price"]),
            proposal["reducing"],
            proposal["reason"],
        ),
    )
    if type(value["reduce_only"]) is not bool or _intent_document(intent) != value:
        raise ValidationError("binding request differs from its original execution intent")
    return intent


def _document(decision, account_state_digest):
    if type(decision) is not ExecutionDecision:
        raise ValidationError("binding requires the complete typed execution decision")
    decision = replace(decision)
    event = {
        "kind": KIND,
        "schema_version": 1,
        "cycle": _cycle_document(decision.cycle),
        "account_state_digest": _digest(account_state_digest),
        "decision": {
            "reason": decision.reason,
            "intents": [_intent_document(value) for value in decision.intents],
            "cancel_intent": None
            if decision.cancel_intent is None
            else _intent_document(decision.cancel_intent),
        },
    }
    if (
        len(json.dumps(event, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        > MAX_BINDING_BYTES
    ):
        raise ValidationError("execution binding exceeds journal record capacity")
    return event


def _read_document(event):
    _keys(event, ("kind", "schema_version", "cycle", "account_state_digest", "decision"))
    if (
        event["kind"] != KIND
        or type(event["schema_version"]) is not int
        or event["schema_version"] != 1
    ):
        raise ValidationError("unsupported execution binding schema")
    body = event["decision"]
    _keys(body, ("reason", "intents", "cancel_intent"))
    if type(body["intents"]) is not list or len(body["intents"]) > 1:
        raise ValidationError("execution binding requires a serial decision")
    decision = ExecutionDecision(
        _read_cycle(event["cycle"]),
        tuple(_read_intent(value) for value in body["intents"]),
        None if body["cancel_intent"] is None else _read_intent(body["cancel_intent"]),
        body["reason"],
    )
    state_digest = _digest(event["account_state_digest"])
    if _document(decision, state_digest) != event:
        raise ValidationError("execution binding is not canonical")
    return decision, state_digest


@dataclass(frozen=True, slots=True)
class ExecutionBinding:
    decision: ExecutionDecision
    account_state_digest: str
    state: str

    def __post_init__(self):
        _document(self.decision, self.account_state_digest)
        if type(self.state) is not str or self.state not in {
            "pending",
            "admitted",
            "recovery_required",
        }:
            raise ValidationError("invalid execution binding receipt state")

    @property
    def admitted(self) -> bool:
        return self.state == "admitted"

    @property
    def recovery_required(self) -> bool:
        return self.state == "recovery_required"


class ExecutionDecisionBindings:
    """One account/sink owner; no broker, clock, journal repair or automatic resume.

    A failed write retains the exact candidate. Retrying bind must obtain a fresh
    PRIMARY ACK; finding its bytes in records() alone is insufficient. Restarted
    bindings remain recovery-required even when bind() is called again.
    """

    def __init__(self, account_id: str, sink: RecordSink):
        if (
            type(account_id) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", account_id) is None
        ):
            raise ValidationError("binding owner requires a bounded account identity")
        self.account_id, self.sink = account_id, sink
        self._bindings = {}
        self._lock = RLock()
        self._refresh_history()

    def _refresh_history(self):
        found = {}
        for event in self.sink.records():
            if type(event) is not dict:
                raise JournalError("execution binding history contains an invalid event")
            if event.get("kind") != KIND:
                continue
            try:
                decision, state_digest = _read_document(event)
            except (ValidationError, ValueError, TypeError, KeyError, ArithmeticError):
                raise JournalError("execution binding history is malformed") from None
            if decision.cycle.account_id != self.account_id:
                continue
            identity = _key(decision.cycle)
            binding = ExecutionBinding(decision, state_digest, "recovery_required")
            previous = found.get(identity) or self._bindings.get(identity)
            if previous is not None and (
                previous.decision != decision or previous.account_state_digest != state_digest
            ):
                raise ValidationError(
                    "execution cycle already belongs to another source or decision"
                )
            found[identity] = binding
        if any(
            value.admitted and identity not in found for identity, value in self._bindings.items()
        ):
            raise JournalError("previously acknowledged execution binding is missing")
        for identity, binding in found.items():
            self._bindings.setdefault(identity, binding)

    def lookup(self, cycle: ExecutionCycle) -> ExecutionBinding | None:
        cycle = _cycle(cycle)
        if cycle.account_id != self.account_id:
            raise ValidationError("execution cycle belongs to another account")
        with self._lock:
            self._refresh_history()
            binding = self._bindings.get(_key(cycle))
            if binding is not None and binding.decision.cycle != cycle:
                raise ValidationError(
                    "execution cycle already belongs to another source or decision"
                )
            return binding

    def bind(self, decision: ExecutionDecision, *, account_state_digest: str) -> ExecutionBinding:
        event = _document(decision, account_state_digest)
        # Round-trip the bounded canonical record, retaining no caller-owned DTO.
        decision, account_state_digest = _read_document(event)
        if decision.cycle.account_id != self.account_id:
            raise ValidationError("execution decision belongs to another account")
        identity = _key(decision.cycle)
        with self._lock:
            self._refresh_history()
            previous = self._bindings.get(identity)
            if previous is not None:
                if (
                    previous.decision != decision
                    or previous.account_state_digest != account_state_digest
                ):
                    raise ValidationError(
                        "execution cycle already belongs to another source or decision"
                    )
                if previous.state != "pending":
                    return previous
            pending = ExecutionBinding(decision, account_state_digest, "pending")
            self._bindings[identity] = pending  # Freeze before the first uncertain write.
            try:
                durability = self.sink.record(deepcopy(event))
            except (JournalError, OSError, TimeoutError):
                raise TradingBlocked(
                    "execution binding primary acknowledgement is unavailable"
                ) from None
            if durability is not Durability.PRIMARY:
                raise TradingBlocked("execution binding requires a fresh primary acknowledgement")
            admitted = replace(pending, state="admitted")
            self._bindings[identity] = admitted
            return admitted
