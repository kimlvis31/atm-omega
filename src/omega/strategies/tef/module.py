"""TEF is one complete TS family: model + margin targeting + local stop/reentry policy."""

# Imports
import json
from copy import deepcopy
from decimal import ROUND_HALF_EVEN, Decimal, localcontext
from fractions import Fraction
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.execution_rules import bounded_decimal
from omega.domain.models import Candle, Instrument, OrderKind, Side, decimal
from omega.domain.trading import (
    OrderProposal,
    TradeContext,
    TradeDecision,
)
from omega.ports.modules import ModuleDescriptor
from omega.strategies.tef.strategy import TefModel


class TefModule:
    def __init__(self, instrument: Instrument, configuration: dict | None = None):
        config = dict(configuration or {})
        allowed = {
            "model",
            "parameters",
            "direction",
            "order_type",
            "order_offset",
            "fullStopLossImmediate",
            "fullStopLossClose",
            "postStopLossReentry",
            "stopTradeOnFSL",
            "max_signal_age_ms",
            "max_analysis_deviation",
        }
        if config.keys() - allowed:
            raise ValidationError("unknown TEF TS module configuration")
        self.instrument = instrument
        self.model = TefModel(
            config.get("model", "MMACDDEFAULT"), instrument, config.get("parameters")
        )
        self.descriptor = ModuleDescriptor("tef", "1", self.model.descriptor.required_fields, True)
        self.config = deepcopy(config)
        self.direction = config.get("direction", "BOTH")
        self.order_type = config.get("order_type", "MARKET")
        self.order_offset = decimal(config.get("order_offset", "0"))
        self.deviation = decimal(config.get("max_analysis_deviation", "0.005"))
        self.max_age = config.get("max_signal_age_ms", 120000)
        self.fsl_immediate = self._ratio(config.get("fullStopLossImmediate"))
        self.fsl_close = self._ratio(config.get("fullStopLossClose"))
        self.reenter = config.get("postStopLossReentry", False)
        self.stop_after_fsl = config.get("stopTradeOnFSL", True)
        if (
            self.direction not in {"BOTH", "LONG", "SHORT"}
            or self.order_type not in {"MARKET", "LIMIT", "ADAPTIVE"}
            or not 0 <= self.order_offset < 1
            or not 0 < self.deviation <= 1
            or type(self.max_age) is not int
            or not 0 <= self.max_age <= 3600000
            or type(self.reenter) is not bool
            or type(self.stop_after_fsl) is not bool
        ):
            raise ValidationError("invalid TEF TS settings")
        self._pending_stop = None
        self._sl_exited = None
        self._halted = False
        self._stop_started_this_decision = False
        self._model_failure = None
        self._protection_open_time_ms = None

    @staticmethod
    def _ratio(value):
        if value is None:
            return None
        ratio = decimal(value)
        if not 0 < ratio < 1:
            raise ValidationError("local stop ratio must be between zero and one")
        return ratio

    def snapshot(self) -> dict:
        return {
            "module": "tef",
            "configuration": deepcopy(self.config),
            "model_state": self.model.state,
            "pending_stop": self._pending_stop,
            "sl_exited": self._sl_exited,
            "halted": self._halted,
            "model_failure": deepcopy(self._model_failure),
            "model_last_open_time_ms": self.model.last_open_time_ms,
            "protection_open_time_ms": self._protection_open_time_ms,
            "history_priming": {
                "ready": self.history_priming_ready,
                "pending_closed_intervals": self.model.pending_closed_intervals,
                "reason": "model_failure_requires_recovery"
                if self._model_failure is not None
                else "ready"
                if self.history_priming_ready
                else "confirmed_anchors_missing"
                if self.model.pending_closed_intervals
                else "inputs_unavailable",
            },
        }

    def resume(self) -> None:
        self._halted = False
        self._sl_exited = None
        self._pending_stop = None

    @property
    def last_open_time_ms(self) -> int | None:
        # A financial protection decision can consume a minute without a successful
        # calculator update. Keep that wrapper cursor distinct from the model's.
        times = (self.model.last_open_time_ms, self._protection_open_time_ms)
        return max((value for value in times if value is not None), default=None)

    @property
    def history_priming_ready(self) -> bool:
        return self._model_failure is None and self.model.history_priming_ready

    def can_prime_history(self) -> bool:
        return (
            self.last_open_time_ms is None
            and self._pending_stop is None
            and self._sl_exited is None
            and not self._halted
            and self._model_failure is None
        )

    def prime_history(self, candle: Candle, features: dict) -> None:
        """Advance a caller-owned fresh candidate without orders or protection state."""
        if (
            self._pending_stop is not None
            or self._sl_exited is not None
            or self._halted
            or self._model_failure is not None
        ):
            raise ValidationError("historical priming cannot alter active local protection")
        self.model.evaluate(candle, dict(features))

    def on_order_rejected(self, decision: TradeDecision) -> None:
        # A locally blocked proposal is not evidence of a submitted stop.
        if (
            self._stop_started_this_decision
            and decision.orders
            and decision.reason in {"FSLIMMED", "FSLCLOSE"}
        ):
            self._pending_stop = None

    def checkpoint(self) -> dict:
        payload = {
            "configuration": deepcopy(self.config),
            "instrument": self.instrument.key,
            "model": self.model.checkpoint(),
            "pending_stop": self._pending_stop,
            "sl_exited": self._sl_exited,
            "halted": self._halted,
        }
        # Healthy v1 checkpoints retain their original exact payload and hash.
        if self._model_failure is not None:
            payload.update(
                model_failure=deepcopy(self._model_failure),
                protection_open_time_ms=self._protection_open_time_ms,
            )
        digest = sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
        return {"version": 1, "payload": payload, "sha256": digest}

    def restore(self, state: dict) -> None:
        try:
            payload = state["payload"]
            if (
                set(state) != {"version", "payload", "sha256"}
                or type(state["version"]) is not int
                or state["version"] != 1
                or set(payload)
                not in (
                    {"configuration", "instrument", "model", "pending_stop", "sl_exited", "halted"},
                    {
                        "configuration",
                        "instrument",
                        "model",
                        "pending_stop",
                        "sl_exited",
                        "halted",
                        "model_failure",
                        "protection_open_time_ms",
                    },
                )
                or payload["configuration"] != self.config
                or payload["instrument"] != self.instrument.key
                or type(payload["halted"]) is not bool
                or sha256(json.dumps(payload, sort_keys=True, allow_nan=False).encode()).hexdigest()
                != state["sha256"]
            ):
                raise ValidationError("TEF checkpoint checksum/configuration mismatch")
            for key, n in (("pending_stop", 4), ("sl_exited", 2)):
                row = payload[key]
                if row is not None and (
                    not isinstance(row, (list, tuple))
                    or len(row) != n
                    or row[0] not in {"LONG", "SHORT"}
                    or any(type(v) is not int or v < 0 for v in row[1 : 3 if n == 4 else 2])
                    or (n == 4 and row[3] not in {"FSLIMMED", "FSLCLOSE"})
                ):
                    raise ValidationError("invalid TEF stop checkpoint")
            candidate = deepcopy(self.model)
            candidate.restore(payload["model"])
            failure = payload.get("model_failure")
            protection_time = payload.get("protection_open_time_ms")
            if "model_failure" in payload:
                if (
                    type(failure) is not dict
                    or set(failure) != {"error_type", "open_time_ms"}
                    or type(failure["error_type"]) is not str
                    or not 1 <= len(failure["error_type"]) <= 128
                    or not failure["error_type"].isascii()
                    or not failure["error_type"].isidentifier()
                    or type(failure["open_time_ms"]) is not int
                    or not 0 <= failure["open_time_ms"] <= 253_402_300_799_000
                    or failure["open_time_ms"] % 60000
                    or type(protection_time) is not int
                    or not 0 <= protection_time <= 253_402_300_799_000
                    or protection_time % 60000
                    or protection_time < failure["open_time_ms"]
                    or candidate.last_open_time_ms is not None
                    and protection_time < candidate.last_open_time_ms
                ):
                    raise ValidationError("invalid TEF failed-model protection checkpoint")
            self.model = candidate
            self._pending_stop = tuple(payload["pending_stop"]) if payload["pending_stop"] else None
            self._sl_exited = tuple(payload["sl_exited"]) if payload["sl_exited"] else None
            self._halted = payload["halted"]
            self._model_failure = deepcopy(failure)
            self._protection_open_time_ms = protection_time
        except (TypeError, ValueError, KeyError, AttributeError, RecursionError) as exc:
            raise ValidationError("malformed TEF checkpoint") from exc

    @staticmethod
    def _exact_sizing(context, target, kind, *, reducing):
        # Only internally calculated, over-bound quantities use this path. Original
        # inputs retain the public execution bound before any rational allocation.
        originals = (
            context.budget.allocated,
            context.budget.available,
            target,
            context.position.quantity,
            context.position.entry_price,
            context.constraints.leverage,
            context.price,
        )
        for value in originals:
            bounded_decimal(value)
        allocated, available, exposure, signed, entry, leverage, mark = map(Fraction, originals)
        held = abs(signed)
        if (
            allocated < 0
            or available < 0
            or abs(exposure) > 1
            or leverage < 1
            or mark <= 0
            or entry < 0
            or held
            and entry <= 0
        ):
            raise ValidationError("invalid original TEF sizing inputs")
        difference = allocated * abs(exposure) - held * entry / leverage
        if reducing:
            if not held or difference >= 0:
                return Decimal(0)
            quantity = min(held, -difference * leverage / entry)
        else:
            if difference <= 0:
                return Decimal(0)
            quantity = min(difference, available) * leverage / mark
        step, origin, minimum = context.constraints.quantity_grid(kind)
        for value in (step, origin, minimum):
            bounded_decimal(value)
        unit, start = Fraction(step), Fraction(origin)
        if unit <= 0 or start < 0 or minimum < 0:
            raise ValidationError("invalid TEF quantity grid")
        result = start + ((quantity - start) // unit) * unit
        if result <= 0 or result < Fraction(minimum):
            return Decimal(0)
        # A grid point is finite at its native grid exponent. Decimal tuple
        # construction neither divides nor rounds in the ambient context.
        scale = min(step.as_tuple().exponent, origin.as_tuple().exponent)
        coefficient = result * Fraction(10) ** -scale
        if coefficient.denominator != 1:
            raise ValidationError("TEF grid did not produce a finite quantity")
        result = Decimal((0, tuple(map(int, str(coefficient.numerator))), scale))
        bounded_decimal(result)
        return result

    def _order(self, context, side, quantity, *, reducing, reason, sizing_target=None):
        # Local protection uses a market proposal; it is never a broker-side conditional order.
        kind = self.order_type
        if reason in {"FSLIMMED", "FSLCLOSE"} or (kind == "ADAPTIVE" and reason == "CLEAR"):
            kind = "MARKET"
        elif kind == "ADAPTIVE":
            kind = "LIMIT"
        if sizing_target is not None:
            try:
                bounded_decimal(quantity)
            except ValidationError:
                if type(quantity) is not Decimal or not quantity.is_finite():
                    raise
                quantity = self._exact_sizing(
                    context, sizing_target, OrderKind(kind), reducing=reducing
                )
        quantity = context.constraints.floor_quantity(quantity, OrderKind(kind))
        if quantity <= 0:
            return TradeDecision(reason="below_quantity_step")
        price = None
        if kind == "LIMIT":
            with localcontext() as calculation:
                calculation.prec = 256
                offset = -self.order_offset if side == Side.BUY else self.order_offset
                price = context.constraints.limit_price(context.price * (1 + offset), side)
        proposal = OrderProposal(side, quantity, OrderKind(kind), price, reducing, reason)
        return TradeDecision((proposal,), reason)

    @staticmethod
    def _stop_reached(context, side, ratio, price):
        with localcontext() as calculation:
            calculation.prec = 256
            offset = ratio if side == "SHORT" else -ratio
            level = context.constraints.round_price(
                context.position.entry_price * (1 + offset), ROUND_HALF_EVEN
            )
        return price >= level if side == "SHORT" else price <= level

    def _protection(self, context: TradeContext) -> TradeDecision | None:
        if not context.state_certain or not context.control_owned:
            return TradeDecision(reason="position_or_control_uncertain")
        position = context.position
        if (
            self._pending_stop is not None
            and position.quantity == 0
            and position.revision > self._pending_stop[1]
        ):
            self._sl_exited = (self._pending_stop[0], self._pending_stop[2])
            self._pending_stop = None
            self._halted = self.stop_after_fsl
        if self._pending_stop is not None and position.quantity == 0:
            return TradeDecision(reason="await_local_stop_confirmation")
        if self._pending_stop is not None and context.has_pending_order:
            return TradeDecision(reason="await_pending_protection_order")

        # [1]: Local Position Protection Is Independent Of Signal Warmup/Entry Pause
        if position.quantity:
            side = "LONG" if position.quantity > 0 else "SHORT"
            reason = None
            # Closed-minute replay may observe a touched stop after price recovered.
            # Only a position present before the minute may use its full wick;
            # otherwise the current observed price is the only causal evidence.
            extreme = context.price
            if (
                context.closed_candle is not None
                and position.last_changed_ms <= context.closed_candle.open_time_ms
            ):
                wick = context.closed_candle.low if side == "LONG" else context.closed_candle.high
                extreme = min(extreme, wick) if side == "LONG" else max(extreme, wick)
            observation = context.protection_observation
            if observation is not None and position.last_changed_ms <= observation.open_time_ms:
                wick = observation.low if side == "LONG" else observation.high
                extreme = min(extreme, wick) if side == "LONG" else max(extreme, wick)
            if self.fsl_immediate is not None and self._stop_reached(
                context, side, self.fsl_immediate, extreme
            ):
                reason = "FSLIMMED"
            elif (
                self.fsl_close is not None
                and context.closed_candle is not None
                and position.last_changed_ms <= context.closed_candle.close_time_ms
            ):
                # Unlike a full wick, this close is observed after confirmed fills.
                if self._stop_reached(context, side, self.fsl_close, context.closed_candle.close):
                    reason = "FSLCLOSE"
            # A partially filled/cancelled protection order does not authorize
            # re-expansion merely because the latest price has recovered.
            if self._pending_stop is not None:
                if side != self._pending_stop[0]:
                    raise ValidationError("position direction changed during local stop")
                reason = self._pending_stop[3]
            if reason is not None:
                if context.has_pending_order:
                    if context.protection_observation is not None and self._pending_stop is None:
                        self._pending_stop = (
                            side,
                            position.revision,
                            context.observed_at_ms,
                            reason,
                        )
                    return TradeDecision(reason=reason, cancel_pending=True)
                stamp = (
                    context.closed_candle.open_time_ms
                    if context.closed_candle
                    else context.observed_at_ms
                )
                decision = self._order(
                    context,
                    Side.SELL if side == "LONG" else Side.BUY,
                    abs(position.quantity),
                    reducing=True,
                    reason=reason,
                )
                if decision.orders and self._pending_stop is None:
                    self._pending_stop = (side, position.revision, stamp, reason)
                    self._stop_started_this_decision = True
                return decision
        if self._model_failure is not None:
            return TradeDecision(reason="model_failure_requires_recovery")
        if context.has_pending_order:
            return TradeDecision(reason="await_pending_order")
        if self._halted:
            return TradeDecision(reason="stopped_after_local_stop")
        if context.closed_candle is None:
            return TradeDecision(reason="await_closed_minute")

        return None

    def preview_protection(self, context: TradeContext) -> TradeDecision:
        """Inspect local protection on the caller's detached clone only.

        This does not advance the private model or implement the durable
        evaluate_protection cursor contract. TradingEngine.peek_protection
        discards every local stop/confirmation change made by this preview.
        """
        self._stop_started_this_decision = False
        if context.instrument != self.instrument:
            raise ValidationError("TEF module instrument mismatch")
        decision = self._protection(context)
        return decision if decision is not None else TradeDecision(reason="tef_protection_preview")

    def evaluate_protection(self, context: TradeContext) -> TradeDecision:
        """Durably consume unavailable inputs and run only existing local protection."""
        if context.instrument != self.instrument:
            raise ValidationError("TEF module instrument mismatch")
        self._stop_started_this_decision = False
        if context.closed_candle is not None:
            if self._model_failure is None:
                candidate = deepcopy(self.model)
                candidate.advance_unavailable(context.closed_candle)
                self.model = candidate
            else:
                # A failed calculator stays frozen; only the existing protection
                # clock moves, exactly as on the ordinary sticky-failure path.
                self._protection_open_time_ms = max(
                    context.closed_candle.open_time_ms,
                    self.last_open_time_ms or 0,
                )
        decision = self._protection(context)
        return decision if decision is not None else TradeDecision(reason="tef_inputs_unavailable")

    def evaluate(self, context: TradeContext) -> TradeDecision:
        self._stop_started_this_decision = False
        if context.instrument != self.instrument:
            raise ValidationError("TEF module instrument mismatch")
        # Advance the private model even while entries are paused, an order is pending,
        # or local protection takes priority. Otherwise a later minute becomes a gap.
        signal = None
        if context.closed_candle is not None:
            if self._model_failure is None:
                try:
                    candidate = deepcopy(self.model)
                    signal = candidate.evaluate(context.closed_candle, dict(context.features))
                except Exception as error:
                    # Only the calculator is isolated here. Financial/context/order
                    # validation below still raises normally; control signals escape.
                    name = type(error).__name__
                    self._model_failure = {
                        "error_type": name
                        if name.isascii() and name.isidentifier() and len(name) <= 128
                        else "ModelError",
                        "open_time_ms": context.closed_candle.open_time_ms,
                    }
                else:
                    self.model = candidate
            if self._model_failure is not None:
                self._protection_open_time_ms = max(
                    context.closed_candle.open_time_ms,
                    self.last_open_time_ms or 0,
                )
        protection = self._protection(context)
        if protection is not None:
            return protection
        position = context.position

        # [2]: TEF Model & Signal Eligibility
        candle = context.closed_candle
        if signal is None or not self.history_priming_ready:
            return TradeDecision(reason="indicator_warmup")
        if (
            context.observed_at_ms - candle.close_time_ms > self.max_age
            or abs(context.price / candle.close - 1) >= self.deviation
        ):
            return TradeDecision(reason="expired_or_price_diverged_signal")
        direction = self.model.last_direction
        if (
            self._sl_exited is not None
            and candle.open_time_ms > self._sl_exited[1]
            and direction != self._sl_exited[0]
        ):
            self._sl_exited = None
        opposite = (position.quantity > 0 and direction != "LONG") or (
            position.quantity < 0 and direction != "SHORT"
        )
        if opposite:
            return self._order(
                context,
                Side.SELL if position.quantity > 0 else Side.BUY,
                abs(position.quantity),
                reducing=True,
                reason="CLEAR",
            )

        # [3]: TEF-Specific Entry-Price Margin Commitment & Incremental Resizing
        leverage = context.constraints.leverage
        committed = abs(position.quantity) * position.entry_price / leverage
        desired = context.budget.allocated * abs(signal.target_exposure)
        difference = desired - committed
        if difference < 0 and position.quantity:
            quantity = (
                abs(position.quantity)
                if desired == 0
                else min(abs(position.quantity), -difference * leverage / position.entry_price)
            )
            return self._order(
                context,
                Side.SELL if position.quantity > 0 else Side.BUY,
                quantity,
                reducing=True,
                reason="EXIT",
                sizing_target=signal.target_exposure,
            )
        if difference <= 0:
            return TradeDecision(reason="target_reached")
        if not context.entries_allowed:
            return TradeDecision(reason="new_entries_paused")
        if self._sl_exited is not None and not self.reenter:
            return TradeDecision(reason="post_stop_reentry_blocked")
        if direction not in {"LONG", "SHORT"} or self.direction not in {"BOTH", direction}:
            return TradeDecision(reason="direction_disabled")
        if direction == "SHORT" and not context.constraints.can_short:
            return TradeDecision(reason="short_not_supported")
        quantity = min(difference, context.budget.available) * leverage / context.price
        return self._order(
            context,
            Side.BUY if direction == "LONG" else Side.SELL,
            quantity,
            reducing=False,
            reason="ENTRY",
            sizing_target=signal.target_exposure,
        )


def create(instrument: Instrument, configuration: dict | None = None) -> TefModule:
    return TefModule(instrument, configuration)
