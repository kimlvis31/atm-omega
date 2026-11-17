"""Durable TS-to-margin intent adapter, independent of source IO and fill pricing.

The host commits its analysis checkpoint/input pins before calling this adapter,
and drives existing VirtualMarginEngine open/close batches separately. This is a
full-fill OHLCV-proxy integration, not funding, liquidation or a broker adapter.
"""

import json
import re
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import dataclass, field, fields, is_dataclass, replace
from decimal import Context, Decimal, localcontext
from enum import Enum
from hashlib import sha256
from threading import RLock
from typing import Protocol

from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import MarginReservation
from omega.application.trading import TradingEngine
from omega.application.virtual_margin import VirtualMarginEngine
from omega.domain.errors import ProtocolError, StorageError, TradingBlocked, ValidationError
from omega.domain.margin import (
    MAX_TIME,
    FillEvent,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarkSnapshot,
    evaluate_margin,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side, require_decimal
from omega.domain.protection import ProtectionObservation
from omega.domain.trading import (
    BudgetView,
    ExecutionConstraints,
    OrderProposal,
    PositionView,
    TradeContext,
    TradeDecision,
)

MAX_BYTES = 2_097_152
_MATH = Context(prec=256)


def _identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValidationError("invalid margin strategy identity")


def _hash(value):
    if type(value) is not str or re.fullmatch("[a-f0-9]{64}", value) is None:
        raise ValidationError("invalid margin strategy checksum")


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid margin strategy time")


def _json(value):
    result = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))
    if len(result.encode("utf8")) > MAX_BYTES:
        raise ValidationError("margin strategy checkpoint exceeds capacity")
    return result


def _checkpoint(module):
    return _json(module.checkpoint())


def _types():
    # Static data-only allowlist; no import, eval, pickle or arbitrary class lookup.
    from omega.application.margin_cost import (
        CostEnvelope,
        CostMarginReservation,
        FrozenCostPendingOrder,
        MarginCostPolicy,
    )
    from omega.application.margin_strategy_costs import StagedCostMarginDecision

    return {
        kind.__name__: kind
        for kind in (
            Instrument,
            OrderProposal,
            TradeDecision,
            MarginReservation,
            FrozenPendingOrder,
            MarkSnapshot,
            MarginBracket,
            MarginBracketSnapshot,
            StagedMarginDecision,
            StagedProtectionDecision,
            ProtectionObservation,
            RejectedMarginIntent,
            MarginStrategyState,
            Candle,
            ExecutionConstraints,
            MarginCostPolicy,
            CostEnvelope,
            CostMarginReservation,
            FrozenCostPendingOrder,
            StagedCostMarginDecision,
        )
    }


def _encode(value, depth=0):
    if depth > 24:
        raise ValidationError("margin strategy document nesting exceeds capacity")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValidationError("nonfinite margin strategy Decimal")
        return {"decimal": str(value)}
    if isinstance(value, Enum):
        return {"enum": type(value).__name__, "value": value.value}
    if is_dataclass(value) and _types().get(type(value).__name__) is type(value):
        return {
            "record": type(value).__name__,
            "fields": {
                field.name: _encode(getattr(value, field.name), depth + 1)
                for field in fields(value)
                if not (
                    type(value) is MarginStrategyState
                    and field.name == "last_protection"
                    and value.last_protection is None
                    or type(value) is ProtectionObservation
                    and field.name == "raw_received_at_ms"
                    and value.raw_received_at_ms is None
                )
            },
        }
    if type(value) is tuple:
        return {"tuple": [_encode(item, depth + 1) for item in value]}
    if type(value) is dict and all(type(key) is str for key in value):
        return {"mapping": {key: _encode(item, depth + 1) for key, item in value.items()}}
    if type(value) is list:
        return {"list": [_encode(item, depth + 1) for item in value]}
    if value is None or type(value) in {str, int, bool, float}:
        return value
    raise ValidationError("unsupported margin strategy document value")


def _decode(value, depth=0):
    if depth > 24:
        raise ValidationError("margin strategy document nesting exceeds capacity")
    if value is None or type(value) in {str, int, bool, float}:
        return value
    if type(value) is not dict:
        raise ValidationError("invalid margin strategy typed document")
    if set(value) == {"decimal"} and type(value["decimal"]) is str:
        parsed = Decimal(value["decimal"])
        if not parsed.is_finite() or len(value["decimal"]) > 512:
            raise ValidationError("invalid checkpoint Decimal")
        return parsed
    if set(value) == {"enum", "value"}:
        enum = {kind.__name__: kind for kind in (Market, Side, OrderKind, MarginMode)}.get(
            value["enum"]
        )
        if enum is None or type(value["value"]) is not str:
            raise ValidationError("invalid checkpoint enum")
        return enum(value["value"])
    if set(value) in ({"tuple"}, {"list"}):
        key = next(iter(value))
        if type(value[key]) is not list or len(value[key]) > 10_000:
            raise ValidationError("invalid checkpoint collection")
        parsed = [_decode(item, depth + 1) for item in value[key]]
        return tuple(parsed) if key == "tuple" else parsed
    if set(value) == {"mapping"} and type(value["mapping"]) is dict:
        return {key: _decode(item, depth + 1) for key, item in value["mapping"].items()}
    if set(value) == {"record", "fields"}:
        kind = _types().get(value["record"])
        if (
            kind is None
            or type(value["fields"]) is not dict
            or set(value["fields"]) != {field.name for field in fields(kind)}
            and not (
                kind is MarginStrategyState
                and set(value["fields"])
                == {field.name for field in fields(kind)} - {"last_protection"}
            )
            and not (
                kind is ProtectionObservation
                and set(value["fields"])
                == {field.name for field in fields(kind)} - {"raw_received_at_ms"}
            )
        ):
            raise ValidationError("invalid checkpoint record schema")
        return kind(**{key: _decode(item, depth + 1) for key, item in value["fields"].items()})
    raise ValidationError("unknown margin strategy document tag")


def _digest(value):
    return sha256(_json(_encode(value)).encode()).hexdigest()


def margin_bar_digest(candle, features, constraints, input_reference=None):
    """Public pure identity for an exact closed-bar/feature/config selection."""
    if not isinstance(candle, Candle) or not isinstance(constraints, ExecutionConstraints):
        raise ValidationError("typed candle and execution constraints required")
    _time(candle.open_time_ms)
    _time(candle.close_time_ms)
    if (
        candle.open_time_ms % 60000
        or candle.close_time_ms != candle.open_time_ms + 60000
        or type(features) is not dict
        or len(features) > 2048
        or any(type(key) is not str or not 1 <= len(key) <= 128 for key in features)
    ):
        raise ValidationError("invalid bounded margin bar features")
    if input_reference is not None:
        _hash(input_reference)
    return _digest((candle, features, constraints, input_reference))


@dataclass(frozen=True, slots=True)
class StagedMarginDecision:
    open_time_ms: int
    as_of_ms: int
    cycle: int
    input_digest: str
    bar_digest: str
    context_digest: str
    input_reference: str | None
    decision: TradeDecision
    module_checkpoint: str
    order: FrozenPendingOrder | None
    cancellations: tuple[tuple[str, str], ...]
    marks: tuple[MarkSnapshot, ...]
    brackets: tuple[MarginBracketSnapshot, ...]
    max_mark_age_ms: int
    max_bracket_age_ms: int

    def __post_init__(self):
        self._validate_structure()

    def _validate_structure(self, *, costed=False, protection=False):
        _time(self.open_time_ms)
        _time(self.as_of_ms)
        for value in (self.input_digest, self.bar_digest, self.context_digest):
            _hash(value)
        if self.input_reference is not None:
            _hash(self.input_reference)
        if (
            self.open_time_ms % 60000
            or self.as_of_ms < self.open_time_ms + (0 if protection else 60000)
            or type(self.cycle) is not int
            or not 0 <= self.cycle <= 31
            or not isinstance(self.decision, TradeDecision)
            or type(self.module_checkpoint) is not str
            or _json(json.loads(self.module_checkpoint)) != self.module_checkpoint
            or self.order is not None
            and not isinstance(self.order, FrozenPendingOrder)
            or type(self.cancellations) is not tuple
            or len(self.cancellations) > 1
            or type(self.marks) is not tuple
            or len(self.marks) > 100
            or any(not isinstance(item, MarkSnapshot) for item in self.marks)
            or type(self.brackets) is not tuple
            or len(self.brackets) > 100
            or any(not isinstance(item, MarginBracketSnapshot) for item in self.brackets)
        ):
            raise ValidationError("invalid staged margin strategy decision")
        for age in (self.max_mark_age_ms, self.max_bracket_age_ms):
            if type(age) is not int or not 0 <= age <= 30 * 86400000:
                raise ValidationError("invalid margin strategy freshness policy")
        for row in self.cancellations:
            if type(row) is not tuple or len(row) != 2:
                raise ValidationError("invalid staged cancellation")
            for identity in row:
                _identity(identity)
        if (
            self.decision.cancel_pending != bool(self.cancellations)
            or bool(self.decision.orders) != (self.order is not None)
            or self.order is not None
            and (
                self.decision.orders != (self.order.original.proposal,)
                or self.order.original.as_of_ms != self.as_of_ms
                or not costed
                and (
                    type(self.order) is not FrozenPendingOrder
                    or type(self.order.original) is not MarginReservation
                    or self.order.eligible_at_ms != ((self.as_of_ms + 59999) // 60000) * 60000
                )
            )
        ):
            raise ValidationError("staged decision and original action disagree")


@dataclass(frozen=True, slots=True)
class StagedProtectionDecision(StagedMarginDecision):
    observation: ProtectionObservation = field(kw_only=True)
    constraints: ExecutionConstraints = field(kw_only=True)
    fee_rate: Decimal = field(kw_only=True)
    cost_policy_sha256: str | None = field(default=None, kw_only=True)

    def __post_init__(self):
        self._validate_structure(costed=self.cost_policy_sha256 is not None, protection=True)
        if (
            type(self.observation) is not ProtectionObservation
            or self.open_time_ms != self.observation.open_time_ms
            or self.as_of_ms < self.observation.received_at_ms
            or self.cycle != 0
            or self.input_reference is not None
            or any(not order.reducing for order in self.decision.orders)
            or self.order is not None
            and self.order.original.instrument != self.observation.instrument
        ):
            raise ValidationError("invalid protection-only staged action")
        require_decimal(self.fee_rate)
        if (
            not isinstance(self.constraints, ExecutionConstraints)
            or not 0 <= self.fee_rate <= 1
            or len(self.fee_rate.as_tuple().digits) > 18
            or not -18 <= self.fee_rate.as_tuple().exponent <= 18
            or self.bar_digest != _digest(self.observation)
            or self.input_digest
            != _digest(
                (
                    self.observation,
                    self.as_of_ms,
                    self.context_digest,
                    self.constraints,
                    self.marks,
                    self.brackets,
                    self.fee_rate,
                )
            )
        ):
            raise ValidationError("protection original input digest changed")
        self.observation.current(self.as_of_ms)
        own_marks = tuple(
            item for item in self.marks if item.instrument == self.observation.instrument
        )
        if len(own_marks) != 1 or own_marks[0] != MarkSnapshot(
            self.observation.instrument,
            self.observation.last,
            self.observation.event_time_ms,
            self.observation.received_at_ms,
            self.observation.source,
        ):
            raise ValidationError("protection mark differs from its original observation")
        if self.cost_policy_sha256 is not None:
            from omega.application.margin_cost import FrozenCostPendingOrder, freeze_cost_pending

            _hash(self.cost_policy_sha256)
            if self.order is not None and (
                type(self.order) is not FrozenCostPendingOrder
                or self.order.original.envelope.policy.sha256 != self.cost_policy_sha256
                or freeze_cost_pending(self.order.original) != self.order
            ):
                raise ValidationError("protection cost policy/eligibility changed")


@dataclass(frozen=True, slots=True)
class RejectedMarginIntent:
    action: StagedMarginDecision
    reason: str

    def __post_init__(self):
        if (
            not isinstance(self.action, StagedMarginDecision)
            or self.action.order is None
            or self.action.order.original.proposal.reducing
            or self.reason not in {"paused_before_intent", "external_input_unavailable"}
        ):
            raise ValidationError("invalid explicit unsubmitted-entry disposition")


@dataclass(frozen=True, slots=True)
class MarginStrategyState:
    account_id: str
    strategy_id: str
    instrument: Instrument
    owner_id: str
    module_id: str
    api_version: str
    bootstrap_digest: str
    revision: int
    module_checkpoint: str
    last_open_time_ms: int | None = None
    last_as_of_ms: int | None = None
    last_cycle: int | None = None
    last_input_digest: str | None = None
    last_bar_digest: str | None = None
    last_context_digest: str | None = None
    last_decision: TradeDecision | None = None
    pending: StagedMarginDecision | None = None
    rejected_intents: tuple[RejectedMarginIntent, ...] = ()
    last_protection: StagedProtectionDecision | None = None

    def __post_init__(self):
        for value in (
            self.account_id,
            self.strategy_id,
            self.owner_id,
            self.module_id,
            self.api_version,
        ):
            _identity(value)
        _hash(self.bootstrap_digest)
        if (
            not isinstance(self.instrument, Instrument)
            or self.instrument.market != Market.USDM
            or self.instrument.provider != "binance"
            or not self.instrument.symbol.endswith("USDT")
            or type(self.revision) is not int
            or not 0 <= self.revision <= 1_000_000
            or type(self.module_checkpoint) is not str
            or _json(json.loads(self.module_checkpoint)) != self.module_checkpoint
        ):
            raise ValidationError("invalid margin strategy checkpoint identity")
        values = (
            self.last_open_time_ms,
            self.last_as_of_ms,
            self.last_cycle,
            self.last_input_digest,
            self.last_bar_digest,
            self.last_context_digest,
            self.last_decision,
        )
        if any(item is None for item in values) != all(item is None for item in values):
            raise ValidationError("incomplete margin strategy cursor")
        if self.last_open_time_ms is not None:
            _time(self.last_open_time_ms)
            _time(self.last_as_of_ms)
            for item in (self.last_input_digest, self.last_bar_digest, self.last_context_digest):
                _hash(item)
            if (
                self.last_open_time_ms % 60000
                or self.last_as_of_ms < self.last_open_time_ms + 60000
                or type(self.last_cycle) is not int
                or not 0 <= self.last_cycle <= 31
                or not isinstance(self.last_decision, TradeDecision)
            ):
                raise ValidationError("invalid margin strategy cursor")
        if self.pending is not None:
            from omega.application.margin_strategy_costs import StagedCostMarginDecision

            if type(self.pending) not in {
                StagedMarginDecision,
                StagedCostMarginDecision,
                StagedProtectionDecision,
            }:
                raise ValidationError("invalid pending strategy checkpoint")
            action = self.pending
            if type(action) is StagedProtectionDecision:
                valid = (
                    action.observation.instrument == self.instrument
                    and (self.last_as_of_ms is None or action.as_of_ms >= self.last_as_of_ms)
                    and (
                        self.last_open_time_ms is None
                        or action.observation.open_time_ms >= self.last_open_time_ms + 60000
                    )
                )
            elif self.last_open_time_ms is None:
                valid = action.cycle == 0
            elif action.open_time_ms == self.last_open_time_ms:
                valid = (
                    action.cycle == self.last_cycle + 1
                    and action.bar_digest == self.last_bar_digest
                    and action.context_digest != self.last_context_digest
                    and action.as_of_ms >= self.last_as_of_ms
                )
            else:
                valid = (
                    action.open_time_ms == self.last_open_time_ms + 60000
                    and action.cycle == 0
                    and action.as_of_ms >= self.last_as_of_ms
                )
            if (
                not valid
                or action.order is not None
                and (
                    action.order.original.account_id != self.account_id
                    or action.order.original.instrument != self.instrument
                    or action.order.original.owner_id != self.owner_id
                )
            ):
                raise ValidationError("pending action conflicts with checkpoint cursor/owner")
            expected_order = (
                _digest(
                    (
                        self.strategy_id,
                        self.instrument.key,
                        action.observation.identity,
                        "protection",
                    )
                )
                if type(action) is StagedProtectionDecision
                else _digest(
                    (
                        self.strategy_id,
                        self.instrument.key,
                        action.open_time_ms,
                        action.cycle,
                        "order",
                    )
                )
            )
            if action.order is not None and action.order.order_id != expected_order:
                raise ValidationError("staged strategy order identity changed")
            if action.cancellations and action.cancellations[0][1] != _digest(
                (self.strategy_id, action.input_digest, "cancel")
            ):
                raise ValidationError("staged strategy cancellation identity changed")
        if self.last_protection is not None:
            action = self.last_protection
            if (
                type(action) is not StagedProtectionDecision
                or action.observation.instrument != self.instrument
                or action.order is not None
                and (
                    action.order.original.account_id != self.account_id
                    or action.order.original.owner_id != self.owner_id
                    or action.order.order_id
                    != _digest(
                        (
                            self.strategy_id,
                            self.instrument.key,
                            action.observation.identity,
                            "protection",
                        )
                    )
                )
                or action.cancellations
                and action.cancellations[0][1]
                != _digest((self.strategy_id, action.input_digest, "cancel"))
            ):
                raise ValidationError("protection checkpoint owner/identity changed")
            if type(self.pending) is StagedProtectionDecision:
                previous, current = action.observation, self.pending.observation
                if (
                    current.identity == previous.identity
                    or current.event_time_ms < previous.event_time_ms
                    or current.received_at_ms < previous.received_at_ms
                    or self.pending.as_of_ms < action.as_of_ms
                    or current.generation == previous.generation
                    and (current.connection, current.sequence)
                    <= (previous.connection, previous.sequence)
                ):
                    raise ValidationError("pending protection reuses older evidence")
        if (
            type(self.rejected_intents) is not tuple
            or len(self.rejected_intents) > 32
            or any(not isinstance(item, RejectedMarginIntent) for item in self.rejected_intents)
            or len({item.action.order.order_id for item in self.rejected_intents})
            != len(self.rejected_intents)
        ):
            raise ValidationError("invalid bounded rejected margin intent history")
        for item in self.rejected_intents:
            original = item.action.order.original
            if (
                original.account_id != self.account_id
                or original.instrument != self.instrument
                or original.owner_id != self.owner_id
                or self.last_open_time_ms is None
                or item.action.open_time_ms > self.last_open_time_ms
                or item.action.as_of_ms > self.last_as_of_ms
                or item.action.order.order_id
                != _digest(
                    (
                        self.strategy_id,
                        self.instrument.key,
                        item.action.open_time_ms,
                        item.action.cycle,
                        "order",
                    )
                )
            ):
                raise ValidationError("rejected intent conflicts with strategy identity/cursor")

    def document(self):
        result = {
            "schema_version": 3
            if self.last_protection is not None or type(self.pending) is StagedProtectionDecision
            else 2,
            "state": _encode(self),
        }
        _json(result)
        return result

    @classmethod
    def parse(cls, document):
        try:
            _json(document)
            if (
                type(document) is not dict
                or set(document) != {"schema_version", "state"}
                or type(document["schema_version"]) is not int
                or document["schema_version"] not in {1, 2, 3}
            ):
                raise ValidationError("invalid margin strategy document schema")
            document = deepcopy(document)
            raw = document["state"]
            legacy = (
                type(raw) is dict
                and set(raw) == {"record", "fields"}
                and raw["record"] == "MarginStrategyState"
                and type(raw["fields"]) is dict
                and set(raw["fields"])
                == {field.name for field in fields(cls)} - {"rejected_intents", "last_protection"}
            )
            if document["schema_version"] == 1:
                if not legacy:
                    raise ValidationError("v1 checkpoint cannot contain disposition fields")
                # Only the exact pre-disposition schema gets the additive empty history.
                raw["fields"]["rejected_intents"] = {"tuple": []}
                document["schema_version"] = 2
            result = _decode(document["state"])
            if type(result) is not cls or result.document() != document:
                raise ValidationError("noncanonical margin strategy document")
            return result
        except (ValueError, TypeError, KeyError, ArithmeticError, RecursionError) as error:
            raise ValidationError("malformed margin strategy document") from error


class MarginStrategyStore(Protocol):
    def load(self, strategy_id: str) -> MarginStrategyState | None: ...

    def save(self, state: MarginStrategyState, *, expected_revision: int | None) -> None:
        """Atomic CAS, exact retry dedup. Raise on unknown durability/corruption."""
        ...


@dataclass(frozen=True, slots=True)
class MarginStrategySnapshot:
    state: str
    error: str | None
    checkpoint: MarginStrategyState | None


class MarginStrategyAdapter:
    def __init__(
        self,
        strategy_id,
        instrument,
        module,
        execution,
        store,
        *,
        owner_id,
        create_if_missing=False,
        entry_authorizer=None,
        entry_guard=None,
        cost_policy=None,
    ):
        _identity(strategy_id)
        _identity(owner_id)
        if (
            type(create_if_missing) is not bool
            or entry_authorizer is not None
            and not callable(entry_authorizer)
            or entry_guard is not None
            and not callable(entry_guard)
            or not isinstance(execution, VirtualMarginEngine)
            or any(
                not callable(getattr(module, method, None))
                for method in ("evaluate", "checkpoint", "restore", "resume")
            )
            or not callable(getattr(store, "load", None))
            or not callable(getattr(store, "save", None))
        ):
            raise ValidationError("durable margin strategy needs explicit checkpoint/restore ports")
        self._lock, self.execution, self.store = RLock(), execution, store
        if cost_policy is not None:
            from omega.application.margin_cost import MarginCostPolicy

            if type(cost_policy) is not MarginCostPolicy:
                raise ValidationError("strategy cost policy must be a parsed immutable policy")
        self.cost_policy = cost_policy
        self.entry_authorizer = entry_authorizer
        self.entry_guard = entry_guard
        self.module = deepcopy(module)
        self._confirmed, self._write_pending = None, None
        self._state, self._error = "unavailable", None
        snapshot = execution.snapshot()
        account = snapshot.account
        if account is None:
            raise TradingBlocked("margin account is not confirmed")
        position = next((item for item in account.positions if item.instrument == instrument), None)
        if position is None or position.owner_id != owner_id:
            raise ValidationError("strategy does not own this one-way margin instrument")
        checkpoint = _checkpoint(self.module)
        descriptor = self.module.descriptor
        bootstrap = (descriptor.module_id, descriptor.api_version, checkpoint)
        if self.cost_policy is not None:
            bootstrap += (("cost_policy", self.cost_policy.sha256),)
        self._initial = MarginStrategyState(
            account.account_id,
            strategy_id,
            instrument,
            owner_id,
            descriptor.module_id,
            descriptor.api_version,
            _digest(bootstrap),
            0,
            checkpoint,
        )
        try:
            loaded = store.load(strategy_id)
            if loaded is None:
                if not create_if_missing:
                    raise StorageError("previous margin strategy checkpoint is missing")
                self._save(self._initial)
            else:
                self._adopt(loaded)
        except Exception as error:
            self._fail(error)

    def _fail(self, error):
        self._state = (
            "unknown"
            if self._write_pending
            else "conflict"
            if isinstance(error, ProtocolError)
            else "unavailable"
        )
        self._error = type(error).__name__

    def _adopt(self, state):
        if not isinstance(state, MarginStrategyState):
            raise StorageError("typed margin strategy checkpoint is missing/corrupt")
        identity = (
            "account_id",
            "strategy_id",
            "instrument",
            "owner_id",
            "module_id",
            "api_version",
            "bootstrap_digest",
        )
        if any(getattr(state, key) != getattr(self._initial, key) for key in identity):
            raise ProtocolError("margin strategy bootstrap identity changed")
        candidate = deepcopy(self.module)
        candidate.restore(json.loads(state.module_checkpoint))
        if _checkpoint(candidate) != state.module_checkpoint:
            raise ProtocolError("strategy checkpoint did not restore exactly")
        cursor = getattr(candidate, "last_open_time_ms", state.last_open_time_ms)
        if cursor != state.last_open_time_ms:
            raise ProtocolError("strategy module and adapter cursor disagree")
        if state.pending is not None:
            from omega.application.margin_strategy_costs import validate_cost_action

            if type(state.pending) is StagedProtectionDecision:
                expected = None if self.cost_policy is None else self.cost_policy.sha256
                if state.pending.cost_policy_sha256 != expected:
                    raise ProtocolError("protection action changed cost policy")
            else:
                validate_cost_action(state.pending, self.cost_policy)
            staged = deepcopy(candidate)
            staged.restore(json.loads(state.pending.module_checkpoint))
            if _checkpoint(staged) != state.pending.module_checkpoint or getattr(
                staged,
                "last_open_time_ms",
                state.last_open_time_ms
                if type(state.pending) is StagedProtectionDecision
                else state.pending.open_time_ms,
            ) != (
                state.last_open_time_ms
                if type(state.pending) is StagedProtectionDecision
                else state.pending.open_time_ms
            ):
                raise ProtocolError("staged module checkpoint/cursor is inconsistent")
        from omega.application.margin_strategy_costs import validate_cost_action

        for item in state.rejected_intents:
            validate_cost_action(item.action, self.cost_policy)
        if state.last_protection is not None:
            expected = None if self.cost_policy is None else self.cost_policy.sha256
            if state.last_protection.cost_policy_sha256 != expected:
                raise ProtocolError("consumed protection action changed cost policy")
        self._confirmed, self.module = state, candidate
        self._state, self._error = "pending" if state.pending else "ready", None

    def _save(self, candidate):
        previous = self._confirmed
        self._write_pending = (previous, candidate)
        try:
            self.store.save(
                candidate, expected_revision=None if previous is None else previous.revision
            )
            self._adopt(candidate)
            self._write_pending = None
        except Exception as error:
            self._fail(error)
            raise

    def snapshot(self):
        with self._lock:
            return MarginStrategySnapshot(self._state, self._error, self._confirmed)

    def resume_module(self):
        """Commit an explicit module-local resume without enabling financial risk.

        The composition owner first records an account entry pause and a durable
        control intent. It owns command-id deduplication; an uncertain checkpoint
        is reconciled by its original CAS candidate, never by recalling resume.
        Already accepted financial orders are independent of this TS state.
        """
        with self._lock:
            state = self._confirmed
            if self._state != "ready" or self._write_pending is not None or state is None:
                raise TradingBlocked("module resume needs a reconciled strategy checkpoint")
            candidate = deepcopy(self.module)
            candidate.resume()
            checkpoint = _checkpoint(candidate)
            if (
                getattr(candidate, "last_open_time_ms", state.last_open_time_ms)
                != state.last_open_time_ms
            ):
                raise ProtocolError("module resume cannot change its consumed candle cursor")
            restored = deepcopy(self.module)
            restored.restore(json.loads(checkpoint))
            if _checkpoint(restored) != checkpoint:
                raise ProtocolError("resumed module checkpoint does not restore exactly")
            if checkpoint != state.module_checkpoint:
                self._save(
                    replace(state, revision=state.revision + 1, module_checkpoint=checkpoint)
                )
            return self.snapshot()

    def _finish(self, *, reject_unsubmitted_entries=False):
        state = self._confirmed
        action = state.pending
        if action is None:
            return
        decision, checkpoint, rejected = (
            action.decision,
            action.module_checkpoint,
            state.rejected_intents,
        )
        for order_id, request_id in action.cancellations:
            current = next(
                (item for item in self.execution.snapshot().pending if item.order_id == order_id),
                None,
            )
            if current is not None and (
                current.original.instrument != state.instrument
                or current.original.owner_id != state.owner_id
            ):
                raise ProtocolError("staged strategy cancellation targets another owner")
            self.execution.cancel_order(order_id, request_id)
        with ExitStack() as scopes:
            reject, reason = False, None
            if action.order is not None and not action.order.original.proposal.reducing:
                accepted = self.execution.original_intent(action.order.order_id)
                if accepted is not None and accepted != action.order:
                    raise ProtocolError("accepted original differs from staged strategy intent")
                # Already accepted original intents retain their execution authority.
                # Fresh external-input authorization is checked only at first dispatch,
                # after cancellations, including when recovering a staged decision.
                if accepted is None:
                    if reject_unsubmitted_entries:
                        if not self.execution.snapshot().paused:
                            raise ProtocolError(
                                "unsubmitted-entry disposition requires a confirmed pause"
                            )
                        reason = "paused_before_intent"
                    else:
                        if self.entry_guard is not None:
                            try:
                                authorized = scopes.enter_context(self.entry_guard(action))
                            except Exception:
                                authorized = False
                            if authorized is not True:
                                reason = "external_input_unavailable"
                        if reason is None and self.entry_authorizer is not None:
                            try:
                                authorized = self.entry_authorizer(action)
                            except Exception:
                                authorized = False
                            if authorized is not True:
                                reason = "external_input_unavailable"
                    reject = reason is not None
                if reject:
                    if len(rejected) >= 32:
                        raise StorageError(
                            "rejected-intent history is full; "
                            "preserve and archive before continuing"
                        )
                    candidate = deepcopy(self.module)
                    candidate.restore(json.loads(action.module_checkpoint))
                    callback = getattr(candidate, "on_order_rejected", None)
                    if callable(callback):
                        callback(action.decision)
                    checkpoint = _checkpoint(candidate)
                    decision = TradeDecision(reason=f"blocked: {reason}")
                    rejected = rejected + (RejectedMarginIntent(action, reason),)
            if action.order is not None and not reject:
                self.execution.submit_intent(
                    action.order,
                    marks=action.marks,
                    brackets=action.brackets,
                    max_mark_age_ms=action.max_mark_age_ms,
                    max_bracket_age_ms=action.max_bracket_age_ms,
                )
        if type(action) is StagedProtectionDecision:
            final = replace(
                state,
                revision=state.revision + 1,
                module_checkpoint=checkpoint,
                pending=None,
                last_protection=action,
            )
        else:
            final = replace(
                state,
                revision=state.revision + 1,
                module_checkpoint=checkpoint,
                last_open_time_ms=action.open_time_ms,
                last_as_of_ms=action.as_of_ms,
                last_cycle=action.cycle,
                last_input_digest=action.input_digest,
                last_bar_digest=action.bar_digest,
                last_context_digest=action.context_digest,
                last_decision=decision,
                pending=None,
                rejected_intents=rejected,
            )
        self._save(final)

    def reconcile(self, *, reject_unsubmitted_entries=False):
        with self._lock:
            if type(reject_unsubmitted_entries) is not bool:
                raise ValidationError("explicit native disposition mode required")
            try:
                if self.execution.snapshot().state != "ready":
                    self.execution.reconcile()
                loaded = self.store.load(self._initial.strategy_id)
                if self._write_pending is not None:
                    previous, candidate = self._write_pending
                    if loaded == candidate:
                        self._adopt(candidate)
                        self._write_pending = None
                    elif loaded == previous:
                        self._save(candidate)
                    else:
                        raise ProtocolError("strategy CAS conflict; automatic rebase is forbidden")
                else:
                    if self._confirmed is not None and loaded != self._confirmed:
                        raise ProtocolError("strategy checkpoint changed outside this owner")
                    self._adopt(loaded)
                self._finish(reject_unsubmitted_entries=reject_unsubmitted_entries)
                return self.snapshot()
            except Exception as error:
                self._fail(error)
                raise

    def _context(
        self,
        candle,
        features,
        constraints,
        *,
        as_of_ms,
        marks,
        brackets,
        entries_allowed,
        max_mark_age_ms,
        max_bracket_age_ms,
        protection_observation=None,
    ):
        """One factual context for both committed decisions and detached protection probes."""
        observation = protection_observation
        instrument = candle.instrument if observation is None else observation.instrument
        price = candle.close if observation is None else observation.last
        snapshot = self.execution.snapshot()
        if snapshot.state != "ready" or snapshot.account is None:
            raise TradingBlocked("margin execution is not reconciled")
        account = snapshot.account
        position = next(item for item in account.positions if item.instrument == instrument)
        if position.owner_id != self._confirmed.owner_id or constraints.leverage != Decimal(
            position.leverage
        ):
            raise ProtocolError("strategy owner/leverage differs from margin account")
        if any(entry.fact.available_at_ms > as_of_ms for entry in account.ledger):
            raise ProtocolError("future account facts cannot enter a past strategy context")
        own_fills = [
            entry
            for entry in account.ledger
            if isinstance(entry.fact, FillEvent) and entry.fact.instrument == instrument
        ]
        context_digest = _digest(
            (
                position.quantity,
                position.entry_price,
                tuple(entry.fact.fill_id for entry in own_fills),
                tuple(
                    (item.order_id, item.original.request_fingerprint)
                    for item in snapshot.pending
                    if item.original.instrument == instrument
                ),
            )
        )
        evaluation = evaluate_margin(
            account,
            marks,
            brackets,
            as_of_ms=as_of_ms,
            max_mark_age_ms=max_mark_age_ms,
            max_bracket_age_ms=max_bracket_age_ms,
        )
        current = next(item for item in evaluation.positions if item.instrument == instrument)
        with localcontext(_MATH):
            reserved = sum(
                (
                    item.original.total_reserved
                    for item in snapshot.pending
                    if item.original.margin_mode == position.margin_mode
                    and (
                        position.margin_mode == MarginMode.CROSS
                        or item.original.instrument == position.instrument
                    )
                ),
                Decimal(0),
            )
            own_reserved = sum(
                (
                    item.original.total_reserved
                    for item in snapshot.pending
                    if item.original.instrument == position.instrument
                ),
                Decimal(0),
            )
            free = (
                evaluation.cross_free_collateral
                if position.margin_mode == MarginMode.CROSS
                else current.free_collateral
            )
            available = (
                max(
                    Decimal(0),
                    min(
                        (free or Decimal(0)) - reserved,
                        (current.budget_remaining or Decimal(0)) - own_reserved,
                    ),
                )
                if evaluation.ready
                else Decimal(0)
            )
        view = PositionView(
            position.quantity,
            position.entry_price,
            len(own_fills),
            own_fills[-1].fact.event_time_ms if own_fills else 0,
        )
        context = TradeContext(
            instrument,
            price,
            as_of_ms,
            view,
            BudgetView(position.budget_margin, available),
            constraints,
            candle,
            features,
            entries_allowed and not snapshot.paused and evaluation.ready,
            True,
            True,
            any(item.original.instrument == instrument for item in snapshot.pending),
            observation,
        )
        return snapshot, account, context, context_digest

    def evaluate_observation(
        self,
        observation,
        constraints,
        *,
        as_of_ms,
        marks,
        brackets,
        fee_rate,
        max_mark_age_ms=120000,
        max_bracket_age_ms=86400000,
    ):
        """Commit only independent protection; confirmed-minute cursor is untouched."""
        if (
            type(observation) is not ProtectionObservation
            or observation.instrument != self._initial.instrument
        ):
            raise ValidationError("foreign protection observation")
        with self._lock:
            prior = self._confirmed
            if prior is None:
                raise TradingBlocked("protection needs a confirmed strategy")
            if type(prior.pending) is StagedProtectionDecision:
                if prior.pending.observation.document() != observation.document():
                    raise ProtocolError("pending protection original cannot be replaced")
                self.reconcile()
                return self._confirmed.last_protection.decision
            if self._state != "ready" or self._write_pending is not None:
                raise TradingBlocked("protection needs explicit strategy reconciliation")
            previous = prior.last_protection
            if previous is not None:
                original = previous.observation
                if observation.identity == original.identity:
                    if observation.document() != original.document():
                        raise ProtocolError("same protection identity changed payload")
                    return previous.decision
                if (
                    observation.event_time_ms < original.event_time_ms
                    or observation.received_at_ms < original.received_at_ms
                    or as_of_ms < previous.as_of_ms
                    or observation.generation == original.generation
                    and (observation.connection, observation.sequence)
                    <= (original.connection, original.sequence)
                ):
                    raise ProtocolError("protection observation moved backwards")
            _time(as_of_ms)
            require_decimal(fee_rate)
            if (
                not 0 <= fee_rate <= 1
                or len(fee_rate.as_tuple().digits) > 18
                or not -18 <= fee_rate.as_tuple().exponent <= 18
            ):
                raise ValidationError("invalid bounded protection fee rate")
            observation.current(as_of_ms)
            if (
                prior.last_open_time_ms is not None
                and observation.open_time_ms <= prior.last_open_time_ms
            ):
                raise ProtocolError("partial protection cannot reuse a consumed minute")
            if (
                not isinstance(constraints, ExecutionConstraints)
                or prior.last_as_of_ms is not None
                and as_of_ms < prior.last_as_of_ms
            ):
                raise ValidationError("invalid protection rules/clock")
            financial = self.execution.snapshot()
            if financial.state != "ready" or financial.account is None:
                raise TradingBlocked("partial protection needs reconciled financial ownership")
            if any(item.eligible_at_ms <= observation.open_time_ms for item in financial.pending):
                # The current native partial has no proven bar-open price. Its book
                # must first settle that original open; never retroactively cancel it.
                return TradeDecision(reason="await_eligible_pending_settlement")
            if previous is not None and previous.order is not None:
                original_pending = next(
                    (
                        item
                        for item in self.execution.snapshot().pending
                        if item.order_id == previous.order.order_id
                    ),
                    None,
                )
                if original_pending is not None:
                    if original_pending != previous.order:
                        raise ProtocolError("original pending protection intent changed")
                    return (
                        previous.decision
                    )  # Do not replace its source/checkpoint while unsettled.
            if type(marks) is not tuple or tuple(
                item
                for item in marks
                if isinstance(item, MarkSnapshot) and item.instrument == observation.instrument
            ) != (
                MarkSnapshot(
                    observation.instrument,
                    observation.last,
                    observation.event_time_ms,
                    observation.received_at_ms,
                    observation.source,
                ),
            ):
                raise ValidationError("protection mark differs from source-qualified price")
            snapshot, account, context, context_digest = self._context(
                None,
                {},
                constraints,
                as_of_ms=as_of_ms,
                marks=marks,
                brackets=brackets,
                entries_allowed=False,
                max_mark_age_ms=max_mark_age_ms,
                max_bracket_age_ms=max_bracket_age_ms,
                protection_observation=observation,
            )
            engine = TradingEngine(deepcopy(self.module))
            with localcontext(_MATH):
                decision = engine.evaluate(context, protection_only=True)
            checkpoint = _checkpoint(engine.module)
            if (
                getattr(engine.module, "last_open_time_ms", prior.last_open_time_ms)
                != prior.last_open_time_ms
            ):
                raise ProtocolError("partial protection advanced the signal cursor")
            input_digest = _digest(
                (observation, as_of_ms, context_digest, constraints, marks, brackets, fee_rate)
            )
            order, cancellations = None, ()
            if decision.orders:
                from omega.application.margin_strategy_costs import prepare_strategy_order

                order_id = _digest(
                    (
                        prior.strategy_id,
                        observation.instrument.key,
                        observation.identity,
                        "protection",
                    )
                )
                try:
                    order = prepare_strategy_order(
                        account,
                        order_id,
                        decision.orders[0],
                        instrument=observation.instrument,
                        owner_id=prior.owner_id,
                        as_of_ms=as_of_ms,
                        marks=marks,
                        brackets=brackets,
                        worst_execution_price=observation.last,
                        fee_rate=fee_rate,
                        reservations=tuple(item.original for item in snapshot.pending),
                        max_mark_age_ms=max_mark_age_ms,
                        max_bracket_age_ms=max_bracket_age_ms,
                        policy=self.cost_policy,
                    )
                except TradingBlocked as error:
                    decision = engine.reject_execution(decision, str(error)[:128])
                    checkpoint = _checkpoint(engine.module)
            if decision.cancel_pending:
                own = tuple(
                    item
                    for item in snapshot.pending
                    if item.original.instrument == observation.instrument
                )
                if len(own) != 1:
                    raise ProtocolError("protection cancellation needs one original owned order")
                cancellations = (
                    (own[0].order_id, _digest((prior.strategy_id, input_digest, "cancel"))),
                )
            action = StagedProtectionDecision(
                observation.open_time_ms,
                as_of_ms,
                0,
                input_digest,
                _digest(observation),
                context_digest,
                None,
                decision,
                checkpoint,
                order,
                cancellations,
                marks,
                brackets,
                max_mark_age_ms,
                max_bracket_age_ms,
                observation=observation,
                constraints=constraints,
                fee_rate=fee_rate,
                cost_policy_sha256=None if self.cost_policy is None else self.cost_policy.sha256,
            )
            try:
                self._save(replace(prior, revision=prior.revision + 1, pending=action))
                self._finish()
            except Exception as error:
                self._fail(error)
                raise
            return self._confirmed.last_protection.decision

    def preview_protection(
        self,
        candle,
        constraints,
        *,
        as_of_ms,
        marks,
        brackets,
        max_mark_age_ms=120000,
        max_bracket_age_ms=86400000,
    ):
        """Evaluate only a detached module; no graph/cursor/checkpoint/financial writes.

        A scheduler can wait for external inputs only while this finds no urgent
        reduction/cancellation. Existing financial settlement is checked separately
        by the host and never delayed by a signal wait.
        """
        with self._lock:
            if self._state != "ready" or self._write_pending is not None:
                raise TradingBlocked("protection preview requires a reconciled strategy")
            _time(as_of_ms)
            if (
                not isinstance(candle, Candle)
                or candle.instrument != self._initial.instrument
                or candle.open_time_ms % 60000
                or candle.close_time_ms != candle.open_time_ms + 60000
                or candle.close_time_ms > as_of_ms
                or not isinstance(constraints, ExecutionConstraints)
            ):
                raise ValidationError("protection preview requires an exact closed minute")
            _, _, context, _ = self._context(
                candle,
                {},
                constraints,
                as_of_ms=as_of_ms,
                marks=marks,
                brackets=brackets,
                entries_allowed=False,
                max_mark_age_ms=max_mark_age_ms,
                max_bracket_age_ms=max_bracket_age_ms,
            )
            with localcontext(_MATH):
                return TradingEngine(self.module).peek_protection(context)

    def evaluate_closed(
        self,
        candle,
        features,
        constraints,
        *,
        as_of_ms,
        marks,
        brackets,
        worst_execution_price,
        fee_rate,
        entries_allowed=True,
        input_reference=None,
        max_mark_age_ms=120000,
        max_bracket_age_ms=86400000,
        decision_cycle=0,
        protection_only=False,
    ):
        with self._lock:
            if self._state != "ready" or self._write_pending is not None:
                raise TradingBlocked("margin strategy needs explicit reconciliation")
            _time(as_of_ms)
            for value in (worst_execution_price, fee_rate):
                require_decimal(value)
                if len(value.as_tuple().digits) > 18 or not -18 <= value.as_tuple().exponent <= 18:
                    raise ValidationError("margin strategy price/fee exceeds its numeric bound")
            if worst_execution_price <= 0 or not 0 <= fee_rate <= 1:
                raise ValidationError("invalid margin strategy worst price/fee")
            if (
                not isinstance(candle, Candle)
                or candle.instrument != self._initial.instrument
                or candle.open_time_ms % 60000
                or candle.close_time_ms != candle.open_time_ms + 60000
                or candle.close_time_ms > as_of_ms
                or type(features) is not dict
                or not isinstance(constraints, ExecutionConstraints)
                or type(entries_allowed) is not bool
                or type(protection_only) is not bool
                or type(decision_cycle) is not int
                or not 0 <= decision_cycle <= 31
            ):
                raise ValidationError("invalid closed-minute margin strategy inputs")
            if input_reference is not None:
                _hash(input_reference)
            bar_digest = margin_bar_digest(candle, features, constraints, input_reference)
            input_digest = _digest(
                (
                    bar_digest,
                    as_of_ms,
                    decision_cycle,
                    marks,
                    brackets,
                    worst_execution_price,
                    fee_rate,
                    entries_allowed,
                    max_mark_age_ms,
                    max_bracket_age_ms,
                )
                + (("protection_only",) if protection_only else ())
                + (("cost_policy", self.cost_policy.sha256) if self.cost_policy is not None else ())
            )
            prior = self._confirmed
            if prior.last_open_time_ms is not None:
                if (
                    candle.open_time_ms == prior.last_open_time_ms
                    and decision_cycle == prior.last_cycle
                ):
                    if input_digest != prior.last_input_digest:
                        raise ProtocolError("consumed strategy minute/cycle inputs changed")
                    return prior.last_decision
                if (
                    as_of_ms < prior.last_as_of_ms
                    or candle.open_time_ms < prior.last_open_time_ms
                    or candle.open_time_ms == prior.last_open_time_ms
                    and (
                        decision_cycle != prior.last_cycle + 1
                        or bar_digest != prior.last_bar_digest
                    )
                    or candle.open_time_ms > prior.last_open_time_ms
                    and (
                        candle.open_time_ms != prior.last_open_time_ms + 60000
                        or decision_cycle != 0
                    )
                ):
                    raise ProtocolError("margin strategy cursor/cycle is not causal")
            elif decision_cycle != 0:
                raise ProtocolError("first margin strategy cycle must be zero")
            snapshot, account, context, context_digest = self._context(
                candle,
                features,
                constraints,
                as_of_ms=as_of_ms,
                marks=marks,
                brackets=brackets,
                entries_allowed=entries_allowed,
                max_mark_age_ms=max_mark_age_ms,
                max_bracket_age_ms=max_bracket_age_ms,
            )
            if (
                candle.open_time_ms == prior.last_open_time_ms
                and context_digest == prior.last_context_digest
            ):
                raise ProtocolError("same-minute cycle requires a concrete position/pending change")
            engine = TradingEngine(deepcopy(self.module))
            with localcontext(_MATH):
                decision = engine.evaluate(context, protection_only=protection_only)
            order, cancellations = None, ()
            if decision.orders:
                order_id = _digest(
                    (
                        prior.strategy_id,
                        candle.instrument.key,
                        candle.open_time_ms,
                        decision_cycle,
                        "order",
                    )
                )
                try:
                    from omega.application.margin_strategy_costs import prepare_strategy_order

                    order = prepare_strategy_order(
                        account,
                        order_id,
                        decision.orders[0],
                        instrument=candle.instrument,
                        owner_id=prior.owner_id,
                        as_of_ms=as_of_ms,
                        marks=marks,
                        brackets=brackets,
                        worst_execution_price=worst_execution_price,
                        fee_rate=fee_rate,
                        reservations=tuple(item.original for item in snapshot.pending),
                        max_mark_age_ms=max_mark_age_ms,
                        max_bracket_age_ms=max_bracket_age_ms,
                        policy=self.cost_policy,
                    )
                except TradingBlocked as error:
                    decision = engine.reject_execution(decision, str(error)[:128])
            if decision.cancel_pending:
                own = tuple(
                    item
                    for item in snapshot.pending
                    if item.original.instrument == candle.instrument
                )
                if len(own) != 1:
                    raise ProtocolError("strategy cancellation needs one owned pending order")
                cancellations = (
                    (own[0].order_id, _digest((prior.strategy_id, input_digest, "cancel"))),
                )
            from omega.application.margin_strategy_costs import staged_decision

            action = staged_decision(
                candle.open_time_ms,
                as_of_ms,
                decision_cycle,
                input_digest,
                bar_digest,
                context_digest,
                input_reference,
                decision,
                _checkpoint(engine.module),
                order,
                cancellations,
                marks,
                brackets,
                max_mark_age_ms,
                max_bracket_age_ms,
                policy=self.cost_policy,
            )
            try:
                self._save(replace(prior, revision=prior.revision + 1, pending=action))
                self._finish()
            except Exception as error:
                self._fail(error)
                raise
            return self._confirmed.last_decision
