"""Explicit costed stage/intent dispatch, with no pricing or risk arithmetic."""

from dataclasses import dataclass, field

from omega.application.margin_cost import (
    CostMarginReservation,
    FrozenCostPendingOrder,
    approve_cost_order,
    freeze_cost_pending,
)
from omega.application.margin_execution import FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.application.margin_strategy import StagedMarginDecision, _hash
from omega.domain.errors import ProtocolError, ValidationError


@dataclass(frozen=True, slots=True)
class StagedCostMarginDecision(StagedMarginDecision):
    order: FrozenCostPendingOrder | None
    cost_policy_sha256: str = field(kw_only=True)

    def __post_init__(self):
        self._validate_structure(costed=True)
        _hash(self.cost_policy_sha256)
        if self.order is not None and (
            type(self.order) is not FrozenCostPendingOrder
            or type(self.order.original) is not CostMarginReservation
            or self.order.original.envelope.policy.sha256 != self.cost_policy_sha256
            or freeze_cost_pending(self.order.original) != self.order
        ):
            raise ValidationError("costed stage differs from its original policy/eligibility")


def validate_cost_action(action, policy):
    if policy is None:
        if type(action) is not StagedMarginDecision:
            raise ProtocolError("ordinary strategy cannot adopt a costed action")
    elif type(action) is not StagedCostMarginDecision or action.cost_policy_sha256 != policy.sha256:
        raise ProtocolError("costed strategy action differs from its configured policy")


def prepare_strategy_order(account, order_id, proposal, *, policy, **references):
    if policy is not None:
        return freeze_cost_pending(
            approve_cost_order(account, order_id, proposal, policy=policy, **references)
        )
    reservation = approve_order(account, order_id, proposal, **references)
    return FrozenPendingOrder(reservation, ((reservation.as_of_ms + 59999) // 60000) * 60000)


def staged_decision(*args, policy):
    if policy is None:
        return StagedMarginDecision(*args)
    return StagedCostMarginDecision(*args, cost_policy_sha256=policy.sha256)
