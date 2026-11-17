"""Single account owner for identity, durable intent and uncertain submissions.

This coordinator is not a futures margin engine or a complete strategy runner.
Its caller must supply reconciled account state and account-wide risk allocation.
"""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import OrderOutcomeUnknown, TradingBlocked, ValidationError
from omega.domain.models import (
    Instrument,
    Market,
    OrderKind,
    OrderObservation,
    OrderRequest,
    OrderStatus,
    Side,
    decimal,
)
from omega.domain.safety import BlockReason, EntryGate
from omega.ports.brokers import Execution
from omega.ports.journal import Durability, RecordSink

_TERMINAL = {
    OrderStatus.FILLED,
    OrderStatus.CANCELED,
    OrderStatus.REJECTED,
    OrderStatus.EXPIRED,
    OrderStatus.EXPIRED_IN_MATCH,
}


def _validate_observation(request, observation, previous=None):
    """Validate transport identity and monotonic cumulative facts before publication.

    A later correction after a terminal snapshot may be real. It requires an
    account/fill reconciliation, rather than silently replacing settled facts.
    """
    if (
        not isinstance(observation, OrderObservation)
        or observation.instrument != request.instrument
        or observation.client_order_id != request.client_order_id
        or type(observation.provider_order_id) is not str
        or not observation.provider_order_id
        or observation.executed_quantity > request.quantity
        or (observation.executed_quantity == 0) != (observation.cumulative_quote == 0)
        or observation.status == OrderStatus.FILLED
        and observation.executed_quantity != request.quantity
        or observation.status == OrderStatus.NEW
        and observation.executed_quantity != 0
        or observation.status == OrderStatus.PARTIALLY_FILLED
        and not 0 < observation.executed_quantity < request.quantity
    ):
        raise ValidationError("broker order observation conflicts with its original intent")
    if previous is None:
        return
    if (
        observation.provider_order_id != previous.provider_order_id
        or observation.executed_quantity < previous.executed_quantity
        or observation.cumulative_quote < previous.cumulative_quote
        or observation.executed_quantity == previous.executed_quantity
        and observation.cumulative_quote != previous.cumulative_quote
        or observation.executed_quantity > previous.executed_quantity
        and observation.cumulative_quote <= previous.cumulative_quote
    ):
        raise ValidationError("broker cumulative observation regressed or changed identity")
    if previous.status in _TERMINAL and observation != previous:
        raise TradingBlocked("terminal order changed; reconcile account and actual fills")
    if previous.status == OrderStatus.PARTIALLY_FILLED and observation.status == OrderStatus.NEW:
        raise ValidationError("broker order progress regressed")


@dataclass
class Operation:
    request: OrderRequest
    state: str = "PENDING"
    observation: OrderObservation | None = None
    terminal_review_required: bool = False


class OrderCoordinator:
    def __init__(self, account_id: str, broker: Execution, sink: RecordSink, gate: EntryGate):
        if not account_id or broker.account_id != account_id:
            raise ValidationError("broker and account owner identity must match")
        self.account_id, self.broker, self.sink, self.gate = (
            account_id,
            broker,
            sink,
            gate,
        )
        self._operations: dict[str, Operation] = {}
        self._account_recovery_owner = self._account_recovery_validator = None
        self._account_recovery_binding = None
        self._historical_ids: set[str] = {
            event["client_order_id"]
            for event in sink.records()
            if event.get("kind") == "OrderIntent" and event.get("account_id") == account_id
        }

    def restore_history(self) -> tuple[Operation, ...]:
        """Restore ALL account intent reservations without contacting the broker.

        The next recovery owner must call this before handling a restarted
        account. Historical snapshots do not prove the current order is terminal:
        every restored request remains UNKNOWN at its full quantity until queried.
        This does not reconcile positions/fills or authorize dispatch/resume.
        """
        self.gate.block(BlockReason.EXECUTION)
        originals = {}
        # Validate the entire replay before publishing any restored operation.
        for event in self.sink.records():
            if event.get("kind") != "OrderIntent" or event.get("account_id") != self.account_id:
                continue
            try:
                if (
                    type(event["instrument"]) is not str
                    or type(event["quantity"]) is not str
                    or event["price"] is not None
                    and type(event["price"]) is not str
                    or type(event["reducing"]) is not bool
                    or type(event["reduce_only"]) is not bool
                ):
                    raise ValidationError("invalid historical intent field types")
                provider, market, symbol = event["instrument"].split(":")
                request = OrderRequest(
                    Instrument(provider, Market(market), symbol),
                    event["client_order_id"],
                    Side(event["side"]),
                    OrderKind(event["order_kind"]),
                    decimal(event["quantity"]),
                    None if event["price"] is None else decimal(event["price"]),
                    event["reduce_only"],
                )
                reducing = event["reducing"]
                if (
                    request.reduce_only
                    and not reducing
                    or reducing
                    and request.instrument.market == Market.USDM
                    and not request.reduce_only
                    or request.instrument.market == Market.SPOT
                    and request.side == Side.SELL
                    and not reducing
                ):
                    raise ValidationError("historical reduction conflicts with original controls")
            except (KeyError, TypeError, ValueError) as error:
                raise ValidationError("invalid historical order intent") from error
            identity = request.client_order_id
            original = (request, reducing)
            if identity in originals and originals[identity] != original:
                raise ValidationError("conflicting duplicate historical order intent")
            operation = self._operations.get(identity)
            if operation is not None and operation.request != request:
                raise ValidationError(
                    "historical order conflicts with the current immutable intent"
                )
            originals[identity] = original
        for identity, (request, _) in originals.items():
            if identity not in self._operations:
                self._operations[identity] = Operation(request, state="UNKNOWN")
            self._historical_ids.add(identity)
        return tuple(self._operations[identity] for identity in sorted(originals))

    def submit(
        self,
        request: OrderRequest,
        *,
        position: Decimal,
        state_certain: bool,
        reducing: bool = False,
    ) -> Operation:
        if type(state_certain) is not bool or type(reducing) is not bool:
            raise ValidationError("order certainty and reduction controls must be native booleans")
        previous = self._operations.get(request.client_order_id)
        if previous:
            if previous.request != request:
                raise ValidationError("client order ID already belongs to another intent")
            return previous
        if request.client_order_id in self._historical_ids:
            raise TradingBlocked("intent exists in history; reconcile original ID before acting")
        if BlockReason.EXTERNAL in self.gate.reasons or not state_certain:
            raise TradingBlocked("position control or account state is uncertain")
        position = decimal(position)
        if request.instrument.market == Market.SPOT:
            if position < 0:
                raise TradingBlocked("spot cash position is inconsistent; reconcile inventory")
            if request.side == Side.SELL and not reducing:
                raise TradingBlocked("spot sells require a validated inventory reduction")
        opposite = (position > 0 and request.side == Side.SELL) or (
            position < 0 and request.side == Side.BUY
        )
        if reducing:
            if request.instrument.market == Market.USDM and not request.reduce_only:
                raise TradingBlocked("futures reduction must be exchange reduce-only")
            pending = sum(
                self._remaining(op)
                for op in self._operations.values()
                if op.state in {"PENDING", "UNKNOWN", "OBSERVED"}
                and op.request.instrument == request.instrument
                and op.request.side == request.side
            )
            if not opposite or request.quantity + pending > abs(position):
                raise TradingBlocked("reduction exceeds certain unreserved position")
        elif not self.gate.allows_new_risk or opposite:
            raise TradingBlocked("new risk blocked or reversal needs explicit split planning")
        if request.reduce_only and not reducing:
            raise TradingBlocked("reduce-only request is not a validated reduction")
        operation = Operation(request)
        event = {
            "kind": "OrderIntent",
            "account_id": self.account_id,
            "client_order_id": request.client_order_id,
            "instrument": request.instrument.key,
            "side": request.side.value,
            "order_kind": request.kind.value,
            "quantity": str(request.quantity),
            "price": str(request.price) if request.price is not None else None,
            "reducing": reducing,
            "reduce_only": request.reduce_only,
        }
        # Record before calling the gateway. Retain identity even if no durable write is possible.
        self._operations[request.client_order_id] = operation
        self._historical_ids.add(request.client_order_id)
        durability = self.sink.record(event)
        if durability != Durability.PRIMARY:
            self.gate.block(BlockReason.STORAGE)
            if not reducing:
                operation.state = "BLOCKED"
                raise TradingBlocked("intent not committed to primary storage; new risk blocked")
        try:
            observation = self.broker.submit(request)
            _validate_observation(request, observation)
            operation.observation = observation
        except OrderOutcomeUnknown:
            operation.state = "UNKNOWN"
            self.gate.block(BlockReason.EXECUTION)
            raise
        except Exception:
            # Adapter failures and malformed replies do not prove submission failed.
            # Keep this identity's uncertain quantity reserved until reconciliation.
            operation.state = "UNKNOWN"
            self.gate.block(BlockReason.EXECUTION)
            raise
        operation.state = "OBSERVED"
        result_durability = self.sink.record(
            {
                "kind": "OrderObserved",
                "account_id": self.account_id,
                "client_order_id": request.client_order_id,
                "provider_order_id": operation.observation.provider_order_id,
                "status": operation.observation.status.value,
            }
        )
        if result_durability != Durability.PRIMARY:
            self.gate.block(BlockReason.STORAGE)
        return operation

    def reconcile(self, request: OrderRequest) -> OrderObservation:
        operation = self._operations.get(request.client_order_id)
        if operation is not None and operation.request != request:
            raise ValidationError("reconciliation must use the original immutable intent")
        if operation is not None and operation.terminal_review_required:
            raise TradingBlocked(
                "terminal discrepancy needs account and actual-fill reconciliation"
            )
        try:
            observation = self.broker.query(request.instrument, request.client_order_id)
            _validate_observation(
                request,
                observation,
                operation.observation if operation is not None else None,
            )
        except Exception:
            self.gate.block(BlockReason.EXECUTION)
            if operation is not None:
                operation.state = "UNKNOWN"
                if operation.observation is not None and operation.observation.status in _TERMINAL:
                    operation.terminal_review_required = True
            raise
        if operation:
            operation.observation = observation
            operation.state = "OBSERVED"
        # Account-wide consistency and manual resume remain separate decisions.
        return observation

    def _bind_account_recovery(self, owner, validator):
        """Bind one trusted composition owner; no serialized receipt grants authority."""
        if (
            self._account_recovery_owner is not None
            or getattr(owner, "coordinator", None) is not self
            or getattr(owner, "sink", None) is not self.sink
            or getattr(owner, "account_id", None) != self.account_id
            or not callable(validator)
            or getattr(validator, "__self__", None) is not owner
        ):
            raise TradingBlocked("account recovery owner is foreign or already registered")
        self._account_recovery_owner, self._account_recovery_validator = owner, validator
        self._account_recovery_binding = (self.account_id, self.sink, self.broker, self.gate)

    def _adopt_account_recovery(self, owner):
        """Publish all validated originals atomically; never clear trading gates."""
        if owner is not self._account_recovery_owner or owner is None:
            raise TradingBlocked("only the registered account recovery owner may adopt")
        account, sink, broker, gate = self._account_recovery_binding
        if (
            self.account_id != account
            or self.sink is not sink
            or self.broker is not broker
            or self.gate is not gate
            or getattr(owner, "coordinator", None) is not self
            or getattr(owner, "sink", None) is not sink
            or getattr(owner, "account_id", None) != account
            or getattr(self._account_recovery_validator, "__self__", None) is not owner
        ):
            raise TradingBlocked("registered account recovery composition changed")
        if BlockReason.EXTERNAL in self.gate.reasons:
            raise TradingBlocked("external position control must remain held")
        before = {
            key: (op.request, op.state, op.observation, op.terminal_review_required)
            for key, op in self._operations.items()
        }
        candidates = self._account_recovery_validator()
        if type(candidates) is not tuple:
            raise ValidationError("account recovery candidates must be immutable")
        prepared = {}
        for request, observation in candidates:
            identity = request.client_order_id
            if identity in prepared or identity not in before:
                raise TradingBlocked("account recovery original set differs")
            original, state, previous, review = before[identity]
            if original != request or review or state not in {"UNKNOWN", "OBSERVED"}:
                raise TradingBlocked("account recovery original state differs")
            _validate_observation(request, observation, previous)
            if observation.status not in _TERMINAL:
                raise TradingBlocked("partial or unresolved originals remain reserved")
            prepared[identity] = Operation(request, "OBSERVED", observation)
        if set(prepared) != set(before) or not prepared:
            raise TradingBlocked("account recovery must cover every original")
        current = {
            key: (op.request, op.state, op.observation, op.terminal_review_required)
            for key, op in self._operations.items()
        }
        if current != before or BlockReason.EXTERNAL in self.gate.reasons:
            raise TradingBlocked("account operations changed during recovery adoption")
        self._operations = prepared
        return tuple(prepared[key] for key in sorted(prepared))

    @staticmethod
    def _remaining(operation: Operation) -> Decimal:
        observed = operation.observation
        if observed is None:
            return operation.request.quantity
        if observed.status in _TERMINAL and operation.state != "UNKNOWN":
            return Decimal(0)
        return max(Decimal(0), operation.request.quantity - observed.executed_quantity)
