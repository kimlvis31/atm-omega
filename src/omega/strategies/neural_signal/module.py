"""Fixed signed-quantity NN demo with independent closed-minute local protection.

The external model owns NN structure/preprocessing; the host owns feature truth,
availability and pinning. A threshold deadband holds the observed position,
never implying a flat target. This module imports no model, source or broker.
"""

# Imports
import json
import re
from copy import deepcopy
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
from hashlib import sha256

from omega.domain.errors import ValidationError
from omega.domain.execution_rules import bounded_decimal
from omega.domain.models import Instrument, decimal
from omega.domain.trading import TradeContext, TradeDecision, close_position
from omega.ports.modules import ModuleDescriptor
from omega.strategies.target_quantity.module import TargetQuantityModule

# Constants
MAX_TIME = 253_402_300_799_000
CALCULATION = Context(prec=256, rounding=ROUND_HALF_EVEN)


def _document(value):
    result = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(result.encode()) > 16384:
        raise ValidationError("neural signal checkpoint exceeds capacity")
    return result


def _signal(value):
    if type(value) is not Decimal or not value.is_finite() or len(str(value)) > 4096:
        raise ValidationError("neural signal requires a bounded finite Decimal feature")
    return value


class NeuralSignalModule:
    def __init__(self, instrument: Instrument, configuration: dict | None = None):
        config = {} if configuration is None else configuration
        if type(config) is not dict or config.keys() - {
            "signal_field",
            "threshold",
            "quantity",
            "local_stop_rate",
        }:
            raise ValidationError("unknown neural signal configuration")
        if not isinstance(instrument, Instrument):
            raise ValidationError("typed neural signal instrument required")
        self.instrument = instrument
        self.signal_field = config.get("signal_field", "NN.score")
        if (
            type(self.signal_field) is not str
            or re.fullmatch(r"NN\.[A-Za-z][A-Za-z0-9_.]{0,123}", self.signal_field) is None
        ):
            raise ValidationError("neural signal field must use the NN namespace")
        self.threshold = decimal(config.get("threshold", "0"))
        self.quantity = decimal(config.get("quantity", "1"))
        self.local_stop_rate = decimal(config.get("local_stop_rate", "0.05"))
        for value in (self.threshold, self.quantity, self.local_stop_rate):
            bounded_decimal(value)
        if self.threshold < 0 or self.quantity <= 0 or not 0 < self.local_stop_rate < 1:
            raise ValidationError("invalid neural threshold, quantity or local stop rate")
        self.configuration = {
            "signal_field": self.signal_field,
            "threshold": str(self.threshold),
            "quantity": str(self.quantity),
            "local_stop_rate": str(self.local_stop_rate),
        }
        self.descriptor = ModuleDescriptor(
            "neural_signal", "1", frozenset({self.signal_field}), True
        )
        self._long = TargetQuantityModule(instrument, {"quantity": str(self.quantity)})
        self._short = TargetQuantityModule(
            instrument, {"quantity": str(self.quantity.copy_negate())}
        )
        self._halted, self._pending_stop = False, None
        self._last_open_time_ms, self._candle_sha256, self._last_signal = None, None, None

    @property
    def last_open_time_ms(self):
        return self._last_open_time_ms

    @property
    def history_priming_ready(self):
        # Model readiness belongs to the external authority, not synthetic priming.
        return True

    def validation_features(self):
        """Explicit synthetic conformance input; never a production NN fallback."""
        return {self.signal_field: Decimal(1)}

    def snapshot(self):
        return {
            "module": "neural_signal",
            "configuration": deepcopy(self.configuration),
            "halted": self._halted,
            "pending_stop": deepcopy(self._pending_stop),
            "last_open_time_ms": self._last_open_time_ms,
            "candle_sha256": self._candle_sha256,
            "last_signal": self._last_signal,
        }

    def checkpoint(self):
        payload = self.snapshot() | {"instrument": self.instrument.key}
        return {
            "version": 1,
            "payload": payload,
            "sha256": sha256(_document(payload).encode()).hexdigest(),
        }

    def restore(self, state):
        try:
            if type(state) is not dict or set(state) != {"version", "payload", "sha256"}:
                raise ValidationError("invalid neural signal checkpoint envelope")
            payload = state["payload"]
            if (
                type(state["version"]) is not int
                or state["version"] != 1
                or type(payload) is not dict
                or set(payload) != set(self.snapshot()) | {"instrument"}
                or payload["module"] != "neural_signal"
                or payload["configuration"] != self.configuration
                or payload["instrument"] != self.instrument.key
                or type(payload["halted"]) is not bool
                or sha256(_document(payload).encode()).hexdigest() != state["sha256"]
            ):
                raise ValidationError("neural signal checkpoint checksum/configuration mismatch")
            stamp, candle_hash = payload["last_open_time_ms"], payload["candle_sha256"]
            if (
                (stamp is None) != (candle_hash is None)
                or stamp is not None
                and (
                    type(stamp) is not int
                    or not 0 <= stamp <= MAX_TIME - 60000
                    or stamp % 60000
                    or type(candle_hash) is not str
                    or re.fullmatch(r"[a-f0-9]{64}", candle_hash) is None
                )
            ):
                raise ValidationError("invalid neural signal cursor")
            signal = payload["last_signal"]
            if signal is not None:
                if stamp is None or type(signal) is not str or len(signal) > 4096:
                    raise ValidationError("invalid neural signal cursor feature")
                _signal(decimal(signal))
            pending = payload["pending_stop"]
            if pending is not None and (
                type(pending) is not dict
                or set(pending) != {"side", "position_revision", "open_time_ms", "stage"}
                or pending["side"] not in {"LONG", "SHORT"}
                or type(pending["position_revision"]) is not int
                or pending["position_revision"] < 0
                or type(pending["open_time_ms"]) is not int
                or stamp is None
                or not 0 <= pending["open_time_ms"] <= stamp
                or pending["open_time_ms"] % 60000
                or pending["stage"] not in {"ready", "cancelling", "closing"}
            ):
                raise ValidationError("invalid neural local stop progress")
            self._halted, self._pending_stop = payload["halted"], deepcopy(pending)
            self._last_open_time_ms, self._candle_sha256, self._last_signal = (
                stamp,
                candle_hash,
                signal,
            )
        except (ValueError, TypeError, KeyError, RecursionError) as error:
            raise ValidationError("malformed neural signal checkpoint") from error

    def resume(self):
        # Resume releases the latch, never an already-triggered protective intent.
        self._halted = False

    def on_order_rejected(self, decision):
        if (
            self._pending_stop is not None
            and decision.reason == "neural_local_stop"
            and (decision.orders or decision.cancel_pending)
        ):
            self._pending_stop["stage"] = "ready"

    def _observation(self, context):
        if context.instrument != self.instrument:
            raise ValidationError("neural signal instrument mismatch")
        candle = context.closed_candle
        if candle is None:
            return None
        if type(candle.open_time_ms) is not int or not 0 <= candle.open_time_ms <= MAX_TIME - 60000:
            raise ValidationError("invalid neural signal minute")
        values = [self.instrument.key, candle.open_time_ms, candle.close_time_ms]
        for name in ("open", "high", "low", "close", "volume"):
            value = str(getattr(candle, name))
            if len(value) > 4096:
                raise ValidationError("neural signal candle exceeds capacity")
            values.append(value)
        candle_hash = sha256(json.dumps(values, separators=(",", ":")).encode()).hexdigest()
        if self._last_open_time_ms is not None and (
            candle.open_time_ms < self._last_open_time_ms
            or candle.open_time_ms == self._last_open_time_ms
            and candle_hash != self._candle_sha256
        ):
            raise ValidationError("late or conflicting neural signal candle")
        return candle.open_time_ms, candle_hash

    def _commit_observation(self, observation):
        if observation is not None:
            if observation[0] != self._last_open_time_ms:
                self._last_signal = None
            self._last_open_time_ms, self._candle_sha256 = observation

    def _protection(self, context):
        if not context.state_certain or not context.control_owned:
            return TradeDecision(reason="position_or_control_uncertain")
        position = context.position
        if self._pending_stop is not None and position.quantity == 0:
            if position.revision <= self._pending_stop["position_revision"]:
                return TradeDecision(reason="await_local_stop_confirmation")
            if context.has_pending_order:
                if self._pending_stop["stage"] == "cancelling":
                    return TradeDecision(reason="await_pending_cancellation")
                self._pending_stop["stage"] = "cancelling"
                return TradeDecision(reason="neural_local_stop", cancel_pending=True)
            self._pending_stop = None
        if position.quantity:
            side = "LONG" if position.quantity > 0 else "SHORT"
            if self._pending_stop is not None and side != self._pending_stop["side"]:
                raise ValidationError("position direction changed during neural local stop")
            candle = context.closed_candle
            if (
                self._pending_stop is None
                and candle is not None
                and position.last_changed_ms <= candle.close_time_ms
            ):
                bounded_decimal(position.entry_price)
                bounded_decimal(candle.close)
                with localcontext(CALCULATION):
                    level = position.entry_price * (
                        1 + self.local_stop_rate if side == "SHORT" else 1 - self.local_stop_rate
                    )
                reached = candle.close >= level if side == "SHORT" else candle.close <= level
                if reached:
                    self._halted = True
                    self._pending_stop = {
                        "side": side,
                        "position_revision": position.revision,
                        "open_time_ms": candle.open_time_ms,
                        "stage": "ready",
                    }
            if self._pending_stop is not None:
                if context.has_pending_order:
                    if self._pending_stop["stage"] in {"closing", "cancelling"}:
                        return TradeDecision(reason="await_pending_protection")
                    self._pending_stop["stage"] = "cancelling"
                    return TradeDecision(reason="neural_local_stop", cancel_pending=True)
                with localcontext(CALCULATION):
                    decision = close_position(context, "neural_local_stop")
                self._pending_stop["stage"] = "closing" if decision.orders else "ready"
                return decision
        if self._halted:
            return TradeDecision(reason="stopped_after_local_stop")
        if context.has_pending_order:
            return TradeDecision(reason="await_pending_order")
        if context.closed_candle is None:
            return TradeDecision(reason="await_closed_minute")
        return None

    def evaluate_protection(self, context: TradeContext) -> TradeDecision:
        observation = self._observation(context)
        decision = self._protection(context)
        self._commit_observation(observation)
        return decision if decision is not None else TradeDecision(reason="neural_protection_only")

    def evaluate(self, context: TradeContext) -> TradeDecision:
        observation = self._observation(context)
        protection = self._protection(context)
        if protection is not None:
            self._commit_observation(observation)
            return protection
        signal = _signal(context.features.get(self.signal_field))
        if (
            self._last_open_time_ms == observation[0]
            and self._last_signal is not None
            and decimal(self._last_signal) != signal
        ):
            raise ValidationError("conflicting neural feature within one closed minute")
        if signal > self.threshold:
            target = self._long
        elif signal < self.threshold.copy_negate():
            target = self._short
        else:
            target = None
        with localcontext(CALCULATION):
            decision = (
                target.evaluate(context) if target else TradeDecision(reason="neural_deadband_hold")
            )
        self._commit_observation(observation)
        if self._last_signal is None:
            self._last_signal = str(signal)
        return decision


def create(instrument: Instrument, configuration: dict | None = None) -> NeuralSignalModule:
    return NeuralSignalModule(instrument, configuration)
