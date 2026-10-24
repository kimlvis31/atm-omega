"""Validated, per-instance legacy TEF state. No order sizing or broker access."""

import json
import math
from copy import deepcopy
from hashlib import sha256
from types import MappingProxyType

from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, decimal
from omega.ports.modules import ModuleDescriptor, StrategySignal
from omega.strategies.tef.models import (
    bscalpha,
    bscbeta,
    cgalpha,
    mmacddefault,
    pathfinder,
    spddefault,
)

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


class TefStrategy:
    def __init__(self, model: str, instrument: Instrument, parameters: dict | None = None):
        if model not in MODELS:
            raise ValidationError("unknown or unfinished TEF model")
        self.model = model
        self.instrument = instrument
        self._formula = MODELS[model]
        supplied = parameters or {}
        specifications = self._formula.PARAMETERS
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
        self._last_signal: StrategySignal | None = None
        self.last_direction: str | None = None

    @property
    def state(self) -> dict:
        """A detached inspection snapshot; callers cannot mutate live state."""
        return deepcopy(self._tracker)

    def evaluate(self, closed_candle: Candle, features: dict) -> StrategySignal | None:
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
            if value is not None and ("PRICE" in key or key.endswith(("_EMA", "_PSAR"))):
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
        if any(fields.get(key) is None for key in self.descriptor.required_fields):
            signal, direction, tracker = None, self.last_direction, self._tracker
        else:
            tracker = deepcopy(self._tracker)
            try:
                direction, raw = self._formula.calculate(self.parameters, fields, tracker)
            except (ArithmeticError, ValueError, TypeError, KeyError):
                raise ValidationError("TEF calculation failed; state was not committed") from None
            if direction not in {"LONG", "SHORT"} or not math.isfinite(raw) or abs(raw) > 1:
                raise ValidationError("invalid TEF decision; state was not committed")
            # Legacy account.py sizes by abs(raw) and separately uses direction.
            # Preserve that meaning, including BSCALPHA's held opposite-signed raw value.
            target = abs(raw) * (-1 if direction == "SHORT" else 1)
            signal = StrategySignal(
                self.instrument, decimal(str(target)), closed_candle.close_time_ms
            )
        self._tracker = tracker
        self.last_direction = direction
        self._last_candle, self._last_features = closed_candle, digest
        self._last_signal = signal
        return signal
