"""Eta-compatible MFI and DMI/ADX; shared frames, bounded state, causal previews."""

# Imports
from collections import deque
from dataclasses import dataclass
from itertools import pairwise

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    AggregatedBar,
    CandleAggregation,
    bars_from_dependencies,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument
from omega.ports.modules import ModuleDescriptor


@dataclass(frozen=True)
class MomentumConfiguration:
    interval_id: int
    analysis_code: str
    nSamples: int = 14
    line_index: int = 0

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or self.analysis_code not in {"MFI", "DMIxADX"}
            or type(self.nSamples) is not int
            or not 2 <= self.nSamples <= 100000
            or type(self.line_index) is not int
            or not 0 <= self.line_index <= 9
        ):
            raise ValidationError("invalid momentum indicator configuration")


class _MomentumKernel:
    def __init__(self, configuration: MomentumConfiguration):
        self.config = configuration
        self.history: deque = deque(maxlen=configuration.nSamples)
        self.previous_bar: AggregatedBar | None = None
        self.analysisCount = 0

    def preview(self, bar: AggregatedBar) -> dict:
        if self.config.analysis_code == "MFI":
            return self._mfi(bar)
        return self._dmixadx(bar)

    def _mfi(self, bar: AggregatedBar) -> dict:
        # [1]: Typical Price & Directional Money Flow
        nSamples = self.config.nSamples
        tp = (float(bar.high) + float(bar.low) + float(bar.close)) / 3
        mf = tp * float(bar.volume)
        mfi = None
        if self.analysisCount >= nSamples:
            records = [*self.history, {"TP": tp, "MF": mf}]
            mfPlusSum = mfMinusSum = 0.0
            for previous, current in pairwise(records):
                tpDelta = current["TP"] - previous["TP"]
                if tpDelta < 0:
                    mfMinusSum += current["MF"]
                elif tpDelta > 0:
                    mfPlusSum += current["MF"]
            # Compatibility: Eta returns 1.0 when negative flow is zero, including flat input.
            mfr = float("inf") if mfMinusSum == 0 else mfPlusSum / mfMinusSum
            mfi = 1.0 - 1.0 / (1.0 + mfr)
            mfi = 0.5 + ((mfi - 0.5) * 1.0)  # Eta confidence path for complete input.

        # [2]: Deviation Smoothing Against The Last CLOSED Bar
        absolute = None
        if mfi is not None and self.history and self.history[-1]["MFI"] is not None:
            previous = self.history[-1]
            prior_absolute = previous["MFI_DEVABSMA"]
            if prior_absolute is None:
                prior_absolute = abs(previous["MFI"] - 0.5)
            k = 2 / (nSamples * 10 + 1)
            absolute = abs(mfi - 0.5) * k + prior_absolute * (1 - k)
        relative = (
            None if absolute is None else round((mfi - 0.5) / absolute, 5) if absolute else 0.0
        )
        return {
            "TP": tp,
            "MF": mf,
            "MFI": mfi,
            "MFI_DEVABSMA": absolute,
            "MFI_DEVABSMAREL": relative,
        }

    def _dmixadx(self, bar: AggregatedBar) -> dict:
        # [1]: Directional Movement & True Range
        nSamples = self.config.nSamples
        dmPlus = dmMinus = tr = None
        if self.previous_bar is not None:
            previous = self.previous_bar
            high, low = float(bar.high), float(bar.low)
            previous_high, previous_low = float(previous.high), float(previous.low)
            previous_close = float(previous.close)
            move_up = high - previous_high
            move_down = previous_low - low
            dmPlus = move_up if move_up > move_down and move_up > 0 else 0.0
            dmMinus = move_down if move_down > move_up and move_down > 0 else 0.0
            tr = max(high - low, abs(high - previous_close), abs(low - previous_close))

        # [2]: Wilder Smoothing, DI & DX
        dmPlusSum = dmMinusSum = trSum = diPlus = diMinus = dx = None
        if self.analysisCount == nSamples:
            records = [
                {"DM+": dmPlus, "DM-": dmMinus, "TR": tr},
                *list(reversed(self.history))[: nSamples - 1],
            ]
            dmPlusSum = sum(row["DM+"] for row in records)
            dmMinusSum = sum(row["DM-"] for row in records)
            trSum = sum(row["TR"] for row in records)
        elif self.analysisCount > nSamples:
            previous = self.history[-1]
            dmPlusSum = previous["DM+Sum"] - previous["DM+Sum"] / nSamples + dmPlus
            dmMinusSum = previous["DM-Sum"] - previous["DM-Sum"] / nSamples + dmMinus
            trSum = previous["TRSum"] - previous["TRSum"] / nSamples + tr
        if trSum is not None:
            diPlus = dmPlusSum / trSum if trSum else 0.0
            diMinus = dmMinusSum / trSum if trSum else 0.0
            dx = abs(diPlus - diMinus) / (diPlus + diMinus) if diPlus + diMinus else 0.0

        # [3]: ADX, DMIxADX & Absolute Moving Average
        adx = None
        if self.analysisCount == nSamples * 2 - 1:
            dxs = [row["DX"] for row in list(reversed(self.history))[: nSamples - 1]]
            adx = (dx + sum(dxs)) / nSamples
        elif self.analysisCount > nSamples * 2 - 1:
            adx = (self.history[-1]["ADX"] * (nSamples - 1) + dx) / nSamples
        dmixadx = None if adx is None else (diPlus - diMinus) * adx
        absolute = None
        if dmixadx is not None and self.history[-1]["DMIxADX"] is not None:
            previous = self.history[-1]
            prior_absolute = previous["DMIxADX_ABSMA"]
            if prior_absolute is None:
                prior_absolute = abs(previous["DMIxADX"])
            k = 2 / (nSamples * 10 + 1)
            absolute = abs(dmixadx) * k + prior_absolute * (1 - k)
        relative = None if absolute is None else round(dmixadx / absolute, 5) if absolute else 0.0
        return {
            "DM+": dmPlus,
            "DM-": dmMinus,
            "TR": tr,
            "DM+Sum": dmPlusSum,
            "DM-Sum": dmMinusSum,
            "TRSum": trSum,
            "DI+": diPlus,
            "DI-": diMinus,
            "DX": dx,
            "ADX": adx,
            "DMIxADX": dmixadx,
            "DMIxADX_ABSMA": absolute,
            "DMIxADX_ABSMAREL": relative,
        }

    def commit(self, bar: AggregatedBar, result: dict) -> None:
        self.history.append(result)
        self.previous_bar = bar
        self.analysisCount += 1


class LegacyMomentumFeatures:
    descriptor = ModuleDescriptor("legacy.mfi_dmixadx", "1", frozenset({"OHLCV"}), True)

    def __init__(self, instrument: Instrument, configurations: tuple[MomentumConfiguration, ...]):
        keys = [(c.interval_id, c.analysis_code, c.line_index) for c in configurations]
        if not keys or len(set(keys)) != len(keys):
            raise ValidationError("unique momentum configurations are required")
        self.instrument = instrument
        self._kernels = {
            key: _MomentumKernel(c) for key, c in zip(keys, configurations, strict=True)
        }
        self._intervals = tuple(dict.fromkeys(key[0] for key in keys))
        self._aggregation = CandleAggregation(instrument, self._intervals)
        self._last: Candle | None = None
        self._result: dict = {}

    def update(self, candle: Candle) -> dict:
        return self._update(candle, self._aggregation.update(candle), include_closure=True)

    def update_with(self, candle: Candle, inputs: dict) -> dict:
        bars = bars_from_dependencies(inputs, candle, self._intervals)
        return self._update(
            candle, {f"BAR_{iid}": bar for iid, bar in bars.items()}, include_closure=False
        )

    def _update(self, candle: Candle, frames: dict, *, include_closure: bool) -> dict:
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        result = {}
        for (iid, code, line), kernel in self._kernels.items():
            frame = frames[f"BAR_{iid}"]
            values = kernel.preview(frame)
            if include_closure:
                result[f"CLOSED_{iid}"] = frame.complete
            fields = (
                {"MFI": "MFI", "MFIDEVABSMA": "MFI_DEVABSMA", "MFIDEVABSMAREL": "MFI_DEVABSMAREL"}
                if code == "MFI"
                else {
                    "DMIxADX": "DMIxADX",
                    "DMIxADXABSMA": "DMIxADX_ABSMA",
                    "DMIxADXABSMAREL": "DMIxADX_ABSMAREL",
                }
            )
            for suffix, key in fields.items():
                result[f"{iid}_{code}_{line}_{suffix}"] = (
                    values[key] if frame.valid_prefix else None
                )
            if frame.complete:
                kernel.commit(frame, values)
        self._last, self._result = candle, result
        return dict(result)
