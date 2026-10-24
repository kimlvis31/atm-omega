"""Single account owner for identity, durable intent and uncertain submissions.

This coordinator is not a futures margin engine or a complete strategy runner.
Its caller must supply reconciled account state and account-wide risk allocation.
"""

from dataclasses import dataclass
from decimal import Decimal

from omega.domain.errors import OrderOutcomeUnknown, TradingBlocked, ValidationError
from omega.domain.models import Market, OrderObservation, OrderRequest, OrderStatus, Side, decimal
from omega.domain.safety import BlockReason, EntryGate
from omega.ports.brokers import Execution
from omega.ports.journal import Durability, RecordSink


@dataclass
class Operation:
    request: OrderRequest
    state: str = "PENDING"
    observation: OrderObservation | None = None


class OrderCoordinator:
    def __init__(self, account_id: str, broker: Execution, sink: RecordSink, gate: EntryGate):
        if not account_id or broker.account_id != account_id:
            raise ValidationError("broker and account owner identity must match")
        self.account_id, self.broker, self.sink, self.gate = account_id, broker, sink, gate
        self._operations: dict[str, Operation] = {}
        self._historical_ids: set[str] = {
            event["client_order_id"]
            for event in sink.records()
            if event.get("kind") == "OrderIntent" and event.get("account_id") == account_id
        }

    def submit(
        self,
        request: OrderRequest,
        *,
        position: Decimal,
        state_certain: bool,
        reducing: bool = False,
    ) -> Operation:
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
            operation.observation = self.broker.submit(request)
        except OrderOutcomeUnknown:
            operation.state = "UNKNOWN"
            self.gate.block(BlockReason.EXECUTION)
            raise
        except Exception:
            # A local failure must never trigger implicit retransmission by this owner.
            operation.state = "FAILED_REQUIRES_REVIEW"
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
        observation = self.broker.query(request.instrument, request.client_order_id)
        if observation.client_order_id != request.client_order_id:
            raise ValidationError("query returned another client order ID")
        operation = self._operations.get(request.client_order_id)
        if operation:
            operation.observation = observation
            operation.state = "OBSERVED"
        # Account-wide consistency and manual resume remain separate decisions.
        return observation

    @staticmethod
    def _remaining(operation: Operation) -> Decimal:
        observed = operation.observation
        if observed is None:
            return operation.request.quantity
        if observed.status in {
            OrderStatus.FILLED,
            OrderStatus.CANCELED,
            OrderStatus.REJECTED,
            OrderStatus.EXPIRED,
            OrderStatus.EXPIRED_IN_MATCH,
        }:
            return Decimal(0)
        return max(Decimal(0), operation.request.quantity - observed.executed_quantity)
