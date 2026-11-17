"""Validated, per-instance legacy TEF state. No order sizing or broker access."""

import json
import math
from copy import deepcopy
from hashlib import sha256
from types import MappingProxyType

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, decimal
from omega.ports.modules import ModuleDescriptor
from omega.strategies.tef.models import (
    bscalpha,
    bscbeta,
    cgalpha,
    mmacddefault,
    pathfinder,
    spddefault,
)
from omega.strategies.tef.signal import TefSignal

MODELS = MappingProxyType(
    {
        "BSCALPHA": bscalpha,
        "BSCBETA": bscbeta,
        "CGALPHA": cgalpha,
        "MMACDDEFAULT": mmacddefault,
        "PATHFINDER": pathfinder,
        "SPDDEFAULT": spddefault,
    }
)
REQUIRED = {
    "BSCALPHA": {"CLOSED_11", "CLOSED_5", "11_EMA_0_EMA", "5_MMACD_MSDELTAABSMAREL"},
    "BSCBETA": {
        "CLOSED_5",
        "CLOSED_3",
        "CLOSED_2",
        "5_EMA_0_EMA",
        "3_MMACD_MSDELTAABSMAREL",
        "2_DMIxADX_0_DMIxADXABSMAREL",
        "2_MFI_0_MFIDEVABSMAREL",
    },
    "CGALPHA": {
        "CLOSED_7",
        "7_MMACD_MSDELTAABSMAREL",
        "7_DMIxADX_0_DMIxADXABSMAREL",
        "5_MFI_0_MFIDEVABSMAREL",
        "5_PSAR_0_PSAR",
        "5_PSAR_0_DCC",
        "0_NES_0_NESABSMAREL",
        "0_WOI_0_WOIABSMAREL",
    },
    "MMACDDEFAULT": {"0_MMACD_MSDELTAABSMAREL"},
    "PATHFINDER": {"CLOSED_5", "CLOSED_0", "5_MST_0_MST", "0_STS_S"},
    "SPDDEFAULT": {"0_SWING_0_LSPRICE", "0_SWING_0_LSTYPE"},
}


class TefModel:
    def __init__(self, model: str, instrument: Instrument, parameters: dict | None = None):
        if model not in MODELS:
            raise ValidationError("unknown or unfinished TEF model")
        self.model = model
        self.instrument = instrument
        formula = MODELS[model]
        self._calculate = formula.calculate
        self._cached_inputs = getattr(formula, "CACHED_INPUTS", ())
        supplied = parameters or {}
        specifications = formula.PARAMETERS
        if supplied.keys() - {p[0] for p in specifications}:
            raise ValidationError("unknown TEF parameter")
        values = []
        for name, default, low, high in specifications:
            value = supplied.get(name, default)
            if (
                isinstance(value, bool)
                or not isinstance(value, (float, int))
                or not math.isfinite(value)
                or not low <= value <= high
            ):
                raise ValidationError(f"invalid TEF parameter: {name}")
            if model == "BSCBETA" and name.startswith("MTF_Alpha_") and value == 0:
                raise ValidationError("BSCBETA alpha must be positive (legacy divide-by-zero)")
            values.append(float(value))
        self.parameters = tuple(values)
        self.descriptor = ModuleDescriptor(
            f"tef.{model.lower()}", "1", frozenset(REQUIRED[model]), stateful=True
        )
        self._tracker: dict = {}
        self._last_candle: Candle | None = None
        self._last_features: str | None = None
        self._last_signal: TefSignal | None = None
        self._closed_intervals: set[str] = set()
        self.last_direction: str | None = None

    @property
    def last_open_time_ms(self) -> int | None:
        return None if self._last_candle is None else self._last_candle.open_time_ms

    @property
    def history_priming_ready(self) -> bool:
        # A preview value alone cannot initialize a model's last CLOSED anchor.
        required = {key for key in self.descriptor.required_fields if key.startswith("CLOSED_")}
        return self._last_signal is not None and required <= self._closed_intervals

    @property
    def pending_closed_intervals(self) -> list[str]:
        return sorted(
            key
            for key in self.descriptor.required_fields
            if key.startswith("CLOSED_") and key not in self._closed_intervals
        )

    @property
    def state(self) -> dict:
        """A detached inspection snapshot; callers cannot mutate live state."""
        return deepcopy(self._tracker)

    def checkpoint(self) -> dict:
        candle = self._last_candle
        payload = {
            "version": 2,
            "instrument": self.instrument.key,
            "model": self.model,
            "parameters": list(self.parameters),
            "tracker": deepcopy(self._tracker),
            "last_direction": self.last_direction,
            "features_digest": self._last_features,
            "candle": None
            if candle is None
            else {
                "open_time_ms": candle.open_time_ms,
                "values": [
                    str(getattr(candle, k)) for k in ("open", "high", "low", "close", "volume")
                ],
            },
            "signal": None if self._last_signal is None else str(self._last_signal.target_exposure),
            "closed_intervals": sorted(self._closed_intervals),
        }
        return payload

    def restore(self, state: dict) -> None:
        """Restore calculation state only; account ownership/resume stays outside TEF."""
        try:
            if (
                not isinstance(state, dict)
                or set(state)
                != set(self.checkpoint())
                - ({"closed_intervals"} if state.get("version") == 1 else set())
                or type(state["version"]) is not int
                or state["version"] not in {1, 2}
                or state["instrument"] != self.instrument.key
                or state["model"] != self.model
                or state["parameters"] != list(self.parameters)
                or state["last_direction"] not in {None, "LONG", "SHORT"}
                or not isinstance(state["tracker"], dict)
                or len(state["tracker"]) > 128
            ):
                raise ValidationError("TEF checkpoint configuration mismatch")
            anchors = state.get("closed_intervals", [])
            required = {key for key in self.descriptor.required_fields if key.startswith("CLOSED_")}
            if (
                not isinstance(anchors, list)
                or any(type(key) is not str for key in anchors)
                or len(anchors) != len(set(anchors))
                or not set(anchors) <= required
            ):
                raise ValidationError("invalid TEF closed-interval checkpoint")
            for key, value in state["tracker"].items():
                if (
                    not isinstance(key, str)
                    or (value is not None and type(value) not in {bool, int, float, str})
                    or (type(value) in {int, float} and not math.isfinite(value))
                ):
                    raise ValidationError("invalid TEF tracker checkpoint")
            candle, signal = None, None
            if state["candle"] is not None:
                row = state["candle"]
                if set(row) != {"open_time_ms", "values"} or len(row["values"]) != 5:
                    raise ValidationError("invalid TEF checkpoint candle")
                candle = Candle(
                    self.instrument,
                    row["open_time_ms"],
                    row["open_time_ms"] + 60000,
                    *(decimal(v) for v in row["values"]),
                )
                digest = state["features_digest"]
                if (
                    not isinstance(digest, str)
                    or len(digest) != 64
                    or any(char not in "0123456789abcdef" for char in digest)
                ):
                    raise ValidationError("invalid TEF checkpoint input digest")
                if state["signal"] is not None:
                    target = decimal(state["signal"])
                    if abs(target) > 1 or state["last_direction"] is None:
                        raise ValidationError("invalid TEF checkpoint signal")
                    signal = TefSignal(self.instrument, target, candle.close_time_ms)
            elif (
                any(state[k] is not None for k in ("features_digest", "signal", "last_direction"))
                or state["tracker"]
                or anchors
            ):
                raise ValidationError("uninitialized TEF checkpoint contains active state")
            self._tracker, self.last_direction = deepcopy(state["tracker"]), state["last_direction"]
            self._last_candle, self._last_features, self._last_signal = (
                candle,
                state["features_digest"],
                signal,
            )
            self._closed_intervals = set(anchors)
        except (TypeError, ValueError, KeyError, ArithmeticError) as exc:
            raise ValidationError("malformed TEF model checkpoint") from exc

    def advance_unavailable(self, closed_candle: Candle) -> None:
        """Consume a missing-input minute without running or priming a calculator.

        An already consumed identical bar keeps its original features/signal.
        A newly missing bar freezes an unavailable identity: late features cannot
        replace that decision, while a healthy next minute remains contiguous.
        """
        if (
            type(closed_candle) is not Candle
            or type(closed_candle.instrument) is not Instrument
            or closed_candle.instrument != self.instrument
            or type(closed_candle.open_time_ms) is not int
            or type(closed_candle.close_time_ms) is not int
            or not 0
            <= closed_candle.open_time_ms
            < closed_candle.close_time_ms
            <= 253_402_300_799_000
        ):
            raise ValidationError("unavailable TEF input requires its original native candle")
        candle = Candle(
            self.instrument,
            closed_candle.open_time_ms,
            closed_candle.close_time_ms,
            *(getattr(closed_candle, key) for key in ("open", "high", "low", "close", "volume")),
        )
        if self._last_candle is not None:
            if candle.open_time_ms < self._last_candle.open_time_ms:
                raise ValidationError("strategy cannot consume late candles without replay")
            if candle.open_time_ms == self._last_candle.open_time_ms:
                if candle != self._last_candle:
                    raise ValidationError("conflicting strategy candle at the same minute")
                return
            if candle.open_time_ms != self._last_candle.close_time_ms:
                raise ValidationError("strategy requires contiguous minutes; replay after a gap")
        # A sequence cannot equal the normal feature-object digest domain.
        digest = sha256(
            json.dumps(
                [
                    "tef.unavailable-input.v1",
                    self.instrument.key,
                    candle.open_time_ms,
                    [
                        str(getattr(candle, key))
                        for key in ("open", "high", "low", "close", "volume")
                    ],
                ],
                sort_keys=True,
                allow_nan=False,
            ).encode()
        ).hexdigest()
        self._last_candle, self._last_features, self._last_signal = candle, digest, None

    def evaluate(self, closed_candle: Candle, features: dict) -> TefSignal | None:
        if closed_candle.instrument != self.instrument:
            raise ValidationError("strategy instrument mismatch")
        fields = {**features, "KLINE_CLOSEPRICE": float(closed_candle.close)}
        for key, value in fields.items():
            if key.startswith("CLOSED_"):
                if type(value) is not bool:
                    raise ValidationError("interval closure must be boolean")
            elif value is not None and (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValidationError("TEF features must be finite numbers or unavailable")
            if value is not None and (
                key == "KLINE_CLOSEPRICE" or key.endswith(("_LSPRICE", "_EMA", "_PSAR"))
            ):
                if value <= 0:
                    raise ValidationError("TEF price reference must be positive")
        digest = sha256(json.dumps(fields, sort_keys=True, allow_nan=False).encode()).hexdigest()
        if self._last_candle is not None:
            if closed_candle.open_time_ms < self._last_candle.open_time_ms:
                raise ValidationError("strategy cannot consume late candles without replay")
            if closed_candle.open_time_ms == self._last_candle.open_time_ms:
                if closed_candle != self._last_candle or digest != self._last_features:
                    raise ValidationError("conflicting strategy input at the same minute")
                return self._last_signal
            if closed_candle.open_time_ms != self._last_candle.close_time_ms:
                raise ValidationError("strategy requires contiguous minutes; replay after a gap")
        # Warm-up is absence of a decision, never an implicit zero-exposure exit.
        # PATH caches each valid closed input independently before its missing-input return.
        missing = any(fields.get(key) is None for key in self.descriptor.required_fields)
        consumed_anchors = set()
        if missing and not self._cached_inputs:
            signal, direction, tracker = None, self.last_direction, self._tracker
        else:
            tracker = deepcopy(self._tracker)
            try:
                direction, raw = self._calculate(self.parameters, fields, tracker)
            except (ArithmeticError, ValueError, TypeError, KeyError):
                raise ValidationError("TEF calculation failed; state was not committed") from None
            if direction not in {"LONG", "SHORT"} or not math.isfinite(raw) or abs(raw) > 1:
                raise ValidationError("invalid TEF decision; state was not committed")
            # Legacy account.py sizes by abs(raw) and separately uses direction.
            # Preserve that meaning, including BSCALPHA's held opposite-signed raw value.
            target = abs(raw) * (-1 if direction == "SHORT" else 1)
            complete = not self._cached_inputs or all(
                tracker.get(cache) is not None for _flag, _field, cache in self._cached_inputs
            )
            signal = (
                TefSignal(self.instrument, decimal(str(target)), closed_candle.close_time_ms)
                if complete
                else None
            )
            if self._cached_inputs:
                consumed_anchors = {
                    flag
                    for flag, field, _cache in self._cached_inputs
                    if fields.get(flag) is True and fields.get(field) is not None
                }
            elif signal is not None:
                consumed_anchors = {
                    key
                    for key in self.descriptor.required_fields
                    if key.startswith("CLOSED_") and fields[key]
                }
        self._tracker = tracker
        self.last_direction = direction
        self._last_candle, self._last_features = closed_candle, digest
        self._last_signal = signal
        self._closed_intervals.update(consumed_anchors)
        return signal
