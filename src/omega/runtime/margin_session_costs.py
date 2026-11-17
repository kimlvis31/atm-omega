"""Optional cost policy wiring; financial arithmetic stays in application."""

from copy import deepcopy

from omega.domain.errors import ProtocolError

LIMITATION = (
    "OHLCV proxy; full fills; fixed adverse slippage and minute-rounded delay assumptions; "
    "no actual venue execution/partial fills/liquidation"
)


def prepare_session_costs(value):
    if value is None:
        return None
    from omega.application.margin_cost import MarginCostPolicy

    return value if type(value) is MarginCostPolicy else MarginCostPolicy.parse(value)


class MarginSessionCosts:
    def __init__(self, value):
        self.policy = prepare_session_costs(value)
        self.enabled = self.policy is not None
        self.configuration = (
            None
            if not self.enabled
            else deepcopy(value)
            if type(value) is dict
            else self.policy.document()
        )

    def adapter_options(self):
        return {"cost_policy": self.policy} if self.enabled else {}

    def prepare_order(self, account, order_id, proposal, **references):
        from omega.application.margin_cost import approve_cost_order, freeze_cost_pending

        if not self.enabled:
            raise ProtocolError("ordinary session cannot prepare a costed order")
        return freeze_cost_pending(
            approve_cost_order(account, order_id, proposal, policy=self.policy, **references)
        )

    def validate_order(self, order, *, issued_at_ms):
        from omega.application.margin_cost import FrozenCostPendingOrder, freeze_cost_pending

        if (
            not self.enabled
            or type(order) is not FrozenCostPendingOrder
            or order.original.as_of_ms > issued_at_ms
            or order.original.envelope.policy.sha256 != self.policy.sha256
            or freeze_cost_pending(order.original) != order
        ):
            raise ProtocolError("shutdown costed order differs from its original authority")

    def frame_identity(self):
        if not self.enabled:
            raise ProtocolError("ordinary session has no cost policy identity")
        return {
            "schema_version": 1,
            "model": self.policy.document()["model"],
            "policy_sha256": self.policy.sha256,
        }

    def validate_frame(self, value):
        if type(value) is not dict or value != self.frame_identity():
            raise ProtocolError("frozen cost policy differs from its original session")
        if type(value.get("schema_version")) is not int:
            raise ProtocolError("cost policy schema must be a native integer")

    def status(self):
        return {
            "scope": "fixed_adverse_full_fill_delay_v1",
            "policy": self.policy.document(),
            "policy_sha256": self.policy.sha256,
            "venue_execution": False,
        }
