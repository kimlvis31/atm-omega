"""Eta STS composition over shared arithmetic kernels and explicit depth frames."""

# Imports
import math
from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass
from decimal import Decimal

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    AggregatedBar,
    CandleAggregation,
    validate_minute,
)
from omega.analysis.momentum import MomentumConfiguration, _MomentumKernel
from omega.analysis.moving_averages import MaConfiguration, _Kernel
from omega.analysis.order_flow import (
    DEPTH_FIELDS,
    LegacyOrderFlowFeatures,
    OrderFlowConfiguration,
    _OrderFlowBucket,
    _OrderFlowKernel,
    _scalar,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Instrument
from omega.ports.modules import ModuleDescriptor

COMPONENTS = ("MMACD", "MFI", "DMI", "WOI")
PUBLIC_FIELDS = ("S", "M", "F", "D", "W")
RAW_FIELDS = frozenset(
    "analysisCount MMACD_MAFAST MMACD_MASLOW MMACD_MAFAST_MODE MMACD_MASLOW_MODE "
    "MMACD MMACD_SIGNAL MMACD_MSDELTA MMACD_MSDELTA_ABSMA MMACD_REL "
    "MFI_TP MFI_MF MFI MFI_DEVABSMA MFI_REL DMI_DM+ DMI_DM- DMI_TR DMI_DM+Sum "
    "DMI_DM-Sum DMI_TRSum DMI_DX DMI_ADX DMI_DMIxADX DMI_DMIxADX_ABSMA DMI_REL "
    "WOI_IMBALANCE WOI WOI_ABSMA WOI_REL WOI_MODE WOI_BASE WOI_DEV S M F D W".split()
)


@dataclass(frozen=True)
class ScalarConfiguration:
    interval_id: int
    MMACDFast: int = 10
    MMACDSlow: int = 20
    MMACDSignal: int = 9
    DMIxADXNSamples: int = 20
    MFINSamples: int = 20
    WOINSamples: int = 5
    WOIBaseNSamples: int = 240
    OSCScale: float = 1.0
    WMMACD: float = 1.0
    WMFI: float = 1.0
    WDMIxADX: float = 0.5
    WWOI: float = 0.5
    price_precision: int = 8
    quote_precision: int = 8

    def __post_init__(self):
        periods = (
            self.MMACDFast,
            self.MMACDSlow,
            self.MMACDSignal,
            self.DMIxADXNSamples,
            self.MFINSamples,
            self.WOINSamples,
            self.WOIBaseNSamples,
        )
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or any(type(value) is not int or not 2 <= value <= 20000 for value in periods)
            or self.MMACDFast >= self.MMACDSlow
            or any(
                type(value) is not int or not 0 <= value <= 16
                for value in (self.price_precision, self.quote_precision)
            )
        ):
            raise ValidationError("invalid bounded STS period, interval or precision")
        for name in ("OSCScale", "WMMACD", "WMFI", "WDMIxADX", "WWOI"):
            value = getattr(self, name)
            if type(value) not in {int, float} or not math.isfinite(value) or value < 0:
                raise ValidationError("STS scale and weights must be finite nonnegative numbers")
            object.__setattr__(self, name, float(value))
        weight = self.WMMACD + self.WMFI + self.WDMIxADX + self.WWOI
        if self.OSCScale <= 0 or not math.isfinite(weight) or weight <= 0:
            raise ValidationError("STS needs a positive scale and at least one positive weight")


def _finite_data(value):
    if value is None or type(value) is int:
        return
    if type(value) is float:
        if not math.isfinite(value):
            raise ValidationError("nonfinite STS arithmetic or checkpoint")
        return
    if isinstance(value, dict):
        for item in value.values():
            _finite_data(item)
        return
    if isinstance(value, (tuple, list)):
        for item in value:
            _finite_data(item)
        return
    raise ValidationError("STS numeric state contains unsupported data")


def _numeric_record(value, fields):
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or any(item is not None and type(item) not in {float, int} for item in value.values())
    ):
        raise ValidationError("invalid STS numeric record schema")
    _finite_data(value)


def _valid_bar_values(bar):
    values = (bar.open, bar.high, bar.low, bar.close, bar.volume)
    if (
        any(type(value) is not Decimal or not value.is_finite() for value in values)
        or min(values[:4]) <= 0
        or bar.volume < 0
        or bar.high < max(bar.open, bar.close)
        or bar.low > min(bar.open, bar.close)
        or bar.high < bar.low
        or any(not math.isfinite(float(value)) for value in values)
    ):
        raise ValidationError("invalid STS checkpoint aggregate values")


class LegacyShortTermScalarFeatures:
    descriptor = ModuleDescriptor("legacy.sts", "1", frozenset({"OHLCV", "depth"}), True)

    @staticmethod
    def validate_configurations(configurations):
        """Check all STS component history budgets without allocating kernels."""
        configurations = tuple(configurations)
        if (
            not 1 <= len(configurations) <= 12
            or any(not isinstance(value, ScalarConfiguration) for value in configurations)
            or len({value.interval_id for value in configurations}) != len(configurations)
            or sum(
                value.MMACDSlow + value.MFINSamples + value.DMIxADXNSamples + value.WOINSamples
                for value in configurations
            )
            > 100000
        ):
            raise ValidationError("STS needs unique bounded configurations per interval")
        return configurations

    def __init__(self, instrument, configurations):
        if not isinstance(instrument, Instrument):
            raise ValidationError("STS requires a typed instrument")
        configurations = self.validate_configurations(configurations)
        self.instrument = instrument
        self.configurations = configurations
        self._kernels = {}
        for config in configurations:
            iid = config.interval_id
            self._kernels[iid, "MMACD"] = _Kernel(
                MaConfiguration(
                    iid,
                    mmacd_periods=(config.MMACDFast, config.MMACDSlow),
                    signal_period=config.MMACDSignal,
                    price_precision=config.price_precision,
                )
            )
            self._kernels[iid, "MFI"] = _MomentumKernel(
                MomentumConfiguration(iid, "MFI", config.MFINSamples)
            )
            self._kernels[iid, "DMI"] = _MomentumKernel(
                MomentumConfiguration(iid, "DMIxADX", config.DMIxADXNSamples)
            )
            self._kernels[iid, "WOI"] = _OrderFlowKernel(
                OrderFlowConfiguration(
                    iid, "WOI", config.WOINSamples, quote_precision=config.quote_precision
                )
            )
        self._aggregation = CandleAggregation(
            instrument, tuple(c.interval_id for c in configurations)
        )
        self._buckets, self._baselines = {}, {c.interval_id: None for c in configurations}
        self._last_frame = None
        self._result, self._input_quality, self._raw = {}, {}, {}

    @property
    def warmup_alignment_minutes(self):
        return self._aggregation.warmup_alignment_minutes

    @property
    def input_ready(self):
        return bool(self._input_quality) and all(
            row["ready"] for row in self._input_quality.values()
        )

    @property
    def input_quality(self):
        return deepcopy(self._input_quality)

    @property
    def state(self):
        return deepcopy(
            {
                "kernels": {
                    key: {k: v for k, v in vars(kernel).items() if k != "config"}
                    for key, kernel in self._kernels.items()
                },
                "buckets": {key: asdict(bucket) for key, bucket in self._buckets.items()},
                "baselines": self._baselines,
                "raw": self._raw,
                "input_quality": self._input_quality,
                "last_frame": self._last_frame,
            }
        )

    @staticmethod
    def _raw_values(config, kernels, bar, imbalance, baseline):
        iid = config.interval_id
        ma = kernels[iid, "MMACD"]
        mmacd, mfi, dmi, woi = (
            ma.preview(float(bar.close)),
            kernels[iid, "MFI"].preview(bar),
            kernels[iid, "DMI"].preview(bar),
            kernels[iid, "WOI"].preview(imbalance),
        )
        fast, slow = mmacd["mas"][config.MMACDFast], mmacd["mas"][config.MMACDSlow]
        raw = {
            "analysisCount": ma.count,
            "MMACD_MAFAST": fast,
            "MMACD_MASLOW": slow,
            "MMACD_MAFAST_MODE": int(fast is not None),
            "MMACD_MASLOW_MODE": int(slow is not None),
            "MMACD": None
            if ma.count < config.MMACDSlow - 1 or fast is None or slow is None
            else fast - slow,
            "MMACD_SIGNAL": mmacd["signal"],
            "MMACD_MSDELTA": mmacd["delta"],
            "MMACD_MSDELTA_ABSMA": mmacd["absolute"],
            "MMACD_REL": mmacd["relative"],
            "MFI_TP": mfi["TP"],
            "MFI_MF": mfi["MF"],
            "MFI": mfi["MFI"],
            "MFI_DEVABSMA": mfi["MFI_DEVABSMA"],
            "MFI_REL": mfi["MFI_DEVABSMAREL"],
            **{
                f"DMI_{key}": dmi[key]
                for key in (
                    "DM+",
                    "DM-",
                    "TR",
                    "DM+Sum",
                    "DM-Sum",
                    "TRSum",
                    "DX",
                    "ADX",
                    "DMIxADX",
                    "DMIxADX_ABSMA",
                )
            },
            "DMI_REL": dmi["DMIxADX_ABSMAREL"],
            "WOI_IMBALANCE": imbalance,
            "WOI": woi["WOI"],
            "WOI_ABSMA": woi["WOI_ABSMA"],
            "WOI_REL": woi["WOI_ABSMAREL"],
            "WOI_MODE": woi["mode"],
        }
        relative = raw["WOI_REL"]
        next_base = (
            baseline
            if relative is None
            else relative
            if baseline is None
            else relative * (2 / (config.WOIBaseNSamples + 1))
            + baseline * (1 - 2 / (config.WOIBaseNSamples + 1))
        )
        raw["WOI_BASE"] = next_base
        raw["WOI_DEV"] = (
            None if relative is None or next_base is None else round(relative - next_base, 5)
        )
        for target, field in (
            ("M", "MMACD_REL"),
            ("F", "MFI_REL"),
            ("D", "DMI_REL"),
            ("W", "WOI_DEV"),
        ):
            raw[target] = (
                None if raw[field] is None else round(math.tanh(raw[field] / config.OSCScale), 5)
            )
        strengths = [raw[field] for field in ("M", "F", "D", "W")]
        raw["S"] = (
            None
            if any(value is None for value in strengths)
            else round(
                (
                    config.WMMACD * raw["M"]
                    + config.WMFI * raw["F"]
                    + config.WDMIxADX * raw["D"]
                    - config.WWOI * raw["W"]
                )
                / (config.WMMACD + config.WMFI + config.WDMIxADX + config.WWOI),
                5,
            )
        )
        _finite_data(raw)
        return raw, (mmacd, mfi, dmi, woi), next_base

    def update(self, candle):
        return self.update_frame(MarketFeatureFrame(candle, candle.close_time_ms))

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("STS requires an explicit minute market frame")
        candle, previous = frame.candle, self._last_frame
        if type(candle.open_time_ms) is not int or type(candle.close_time_ms) is not int:
            raise ValidationError("STS requires integer minute boundaries")
        validate_minute(self.instrument, previous.candle if previous else None, candle)
        if previous is not None and candle == previous.candle:
            if (
                frame.inputs != previous.inputs
                or frame.reference_time_ms != previous.reference_time_ms
                or frame.as_of_ms < previous.as_of_ms
            ):
                raise ValidationError("STS needs immutable observations and a stable replay clock")
            return dict(self._result)
        kernels, aggregation, buckets = (
            deepcopy(self._kernels),
            deepcopy(self._aggregation),
            deepcopy(self._buckets),
        )
        baselines, raw_results, result, quality = dict(self._baselines), {}, {}, {}
        bars = aggregation.update(candle)
        values, reason = LegacyOrderFlowFeatures._observation(frame, "depth", DEPTH_FIELDS)
        for config in self.configurations:
            iid = config.interval_id
            bar = bars[f"BAR_{iid}"]
            bucket = buckets.get(iid)
            if bucket is None or bucket.open_time_ms != bar.open_time_ms:
                bucket = _OrderFlowBucket(bar.open_time_ms, bar.valid_prefix)
                buckets[iid] = bucket
            bucket.ready = values is not None
            if values is not None:
                bucket.values = values
            raw = None
            if bar.valid_prefix:
                imbalance = _scalar("WOI", bucket.values)
                raw, components, next_base = self._raw_values(
                    config, kernels, bar, imbalance, baselines[iid]
                )
                if bar.complete:
                    for code, component in zip(COMPONENTS, components, strict=True):
                        kernel = kernels[iid, code]
                        kernel.commit(
                            float(bar.close)
                            if code == "MMACD"
                            else imbalance
                            if code == "WOI"
                            else bar,
                            component,
                        )
                    baselines[iid] = next_base
            raw_results[iid] = raw
            for field in PUBLIC_FIELDS:
                result[f"{iid}_STS_{field}"] = None if raw is None else raw[field]
            result[f"CLOSED_{iid}"] = bar.complete
            quality[f"{iid}_STS"] = {
                "ready": bar.valid_prefix and bucket.ready,
                "reason": reason if bar.valid_prefix else "partial_prefix",
                "channel": "depth",
            }
        self._kernels, self._aggregation, self._buckets = kernels, aggregation, buckets
        self._baselines, self._last_frame = baselines, frame
        self._result, self._input_quality, self._raw = result, quality, raw_results
        return dict(result)

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, value, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, value, instrument)

    def validate_checkpoint(self):
        expected = {(c.interval_id, code) for c in self.configurations for code in COMPONENTS}
        if not isinstance(self._kernels, dict) or set(self._kernels) != expected:
            raise ValidationError("STS checkpoint kernel configuration mismatch")
        for config in self.configurations:
            iid = config.interval_id
            ma, mfi, dmi, woi = (self._kernels[iid, code] for code in COMPONENTS)
            if (
                not isinstance(ma, _Kernel)
                or not all(isinstance(value, _MomentumKernel) for value in (mfi, dmi))
                or not isinstance(woi, _OrderFlowKernel)
            ):
                raise ValidationError("STS checkpoint kernel type mismatch")
            woi.validate_checkpoint()
            if (
                type(ma.count) is not int
                or not 0 <= ma.count <= 2**63 - 1
                or type(mfi.analysisCount) is not int
                or type(dmi.analysisCount) is not int
                or mfi.analysisCount != ma.count
                or dmi.analysisCount != ma.count
            ):
                raise ValidationError("STS checkpoint component counters disagree")
            for kernel, nSamples in ((mfi, config.MFINSamples), (dmi, config.DMIxADXNSamples)):
                if (
                    not isinstance(kernel.history, deque)
                    or kernel.history.maxlen != nSamples
                    or len(kernel.history) != min(nSamples, ma.count)
                    or (kernel.previous_bar is None) != (ma.count == 0)
                ):
                    raise ValidationError("invalid STS momentum history or cursor")
                _finite_data(list(kernel.history))
                fields = (
                    {"TP", "MF", "MFI", "MFI_DEVABSMA", "MFI_DEVABSMAREL"}
                    if kernel.config.analysis_code == "MFI"
                    else {
                        "DM+",
                        "DM-",
                        "TR",
                        "DM+Sum",
                        "DM-Sum",
                        "TRSum",
                        "DI+",
                        "DI-",
                        "DX",
                        "ADX",
                        "DMIxADX",
                        "DMIxADX_ABSMA",
                        "DMIxADX_ABSMAREL",
                    }
                )
                for row in kernel.history:
                    _numeric_record(row, fields)
            if (
                not isinstance(ma.closes, deque)
                or ma.closes.maxlen != config.MMACDSlow
                or len(ma.closes) != min(ma.count, config.MMACDSlow)
                or len(woi.history) != min(ma.count, config.WOINSamples - 1)
            ):
                raise ValidationError("invalid STS smoothing history lengths")
            _finite_data([*ma.closes, ma.mas, ma.signal, ma.delta, ma.absolute])
            if (
                not isinstance(ma.mas, dict)
                or set(ma.mas) != ({config.MMACDFast, config.MMACDSlow} if ma.count else set())
                or any(type(value) is not float or value <= 0 for value in ma.closes)
                or any(
                    (value is None) != (ma.count < period)
                    or value is not None
                    and (type(value) is not float or value < 0)
                    for period, value in ma.mas.items()
                )
                or (ma.signal is None) != (ma.count < config.MMACDSlow + config.MMACDSignal)
                or (ma.delta is None) != (ma.signal is None)
                or (ma.absolute is None) != (ma.count < config.MMACDSlow + config.MMACDSignal + 1)
                or ma.absolute is not None
                and ma.absolute < 0
            ):
                raise ValidationError("invalid STS shared MA seed or smoothing state")
        keys = {c.interval_id for c in self.configurations}
        if not isinstance(self._baselines, dict) or set(self._baselines) != keys:
            raise ValidationError("invalid STS baseline configuration")
        _finite_data(self._baselines)
        if any(
            value is not None and type(value) is not float for value in self._baselines.values()
        ):
            raise ValidationError("invalid STS baseline scalar type")
        if not all(
            isinstance(value, dict)
            for value in (self._buckets, self._raw, self._input_quality, self._result)
        ):
            raise ValidationError("invalid STS checkpoint mappings")
        if self._last_frame is None:
            if (
                self._buckets
                or self._raw
                or self._result
                or self._input_quality
                or self._aggregation._last is not None
                or any(kernel.count for key, kernel in self._kernels.items() if key[1] == "MMACD")
                or any(value is not None for value in self._baselines.values())
            ):
                raise ValidationError("inconsistent empty STS checkpoint")
            return
        if (
            not isinstance(self._last_frame, MarketFeatureFrame)
            or self._last_frame.candle.instrument != self.instrument
            or self._aggregation._last != self._last_frame.candle
            or set(self._aggregation._buckets) != keys
        ):
            raise ValidationError("invalid STS checkpoint frame cursor")
        if (
            set(self._buckets) != keys
            or set(self._raw) != keys
            or set(self._input_quality) != {f"{iid}_STS" for iid in keys}
            or set(self._result)
            != {
                name
                for iid in keys
                for name in (f"CLOSED_{iid}", *(f"{iid}_STS_{field}" for field in PUBLIC_FIELDS))
            }
        ):
            raise ValidationError("invalid STS checkpoint public schema")
        for config in self.configurations:
            iid, last = config.interval_id, self._last_frame.candle
            bucket = self._buckets[iid]
            duration = INTERVAL_MINUTES[iid] * 60000
            bar = self._aggregation._buckets[iid]
            if (
                not isinstance(bucket, _OrderFlowBucket)
                or bucket.open_time_ms != last.open_time_ms // duration * duration
                or type(bucket.valid_prefix) is not bool
                or type(bucket.ready) is not bool
            ):
                raise ValidationError("invalid STS depth bucket cursor")
            if (
                not isinstance(bar, AggregatedBar)
                or bar.instrument != self.instrument
                or bar.interval_id != iid
                or bar.open_time_ms != bucket.open_time_ms
                or bar.close_time_ms != last.close_time_ms
                or bar.valid_prefix is not bucket.valid_prefix
                or bar.complete
                is not (
                    bucket.valid_prefix and last.close_time_ms == bucket.open_time_ms + duration
                )
                or self._aggregation._result.get(f"BAR_{iid}") != bar
            ):
                raise ValidationError("invalid STS aggregated OHLCV cursor")
            _valid_bar_values(bar)
            if bucket.values is not None and (
                type(bucket.values) is not tuple
                or len(bucket.values) != 12
                or any(
                    type(value) is not float or not math.isfinite(value) or value < 0
                    for value in bucket.values
                )
            ):
                raise ValidationError("invalid STS depth snapshot")
            quality = self._input_quality[f"{iid}_STS"]
            observed, reason = LegacyOrderFlowFeatures._observation(
                self._last_frame, "depth", DEPTH_FIELDS
            )
            if (
                not isinstance(quality, dict)
                or set(quality) != {"ready", "reason", "channel"}
                or type(quality["ready"]) is not bool
                or quality["ready"] != (bucket.valid_prefix and bucket.ready)
                or quality["channel"] != "depth"
                or quality["reason"] != (reason if bucket.valid_prefix else "partial_prefix")
                or bucket.ready != (observed is not None)
                or observed is not None
                and bucket.values != observed
            ):
                raise ValidationError("invalid STS quality checkpoint")
            closed = bucket.valid_prefix and last.close_time_ms == bucket.open_time_ms + duration
            ma = self._kernels[iid, "MMACD"]
            for code in ("MFI", "DMI"):
                previous_bar = self._kernels[iid, code].previous_bar
                expected_end = last.close_time_ms if closed else bucket.open_time_ms
                if ma.count and (
                    not isinstance(previous_bar, AggregatedBar)
                    or previous_bar.instrument != self.instrument
                    or previous_bar.interval_id != iid
                    or previous_bar.close_time_ms != expected_end
                    or previous_bar.open_time_ms != expected_end - duration
                    or not previous_bar.valid_prefix
                    or not previous_bar.complete
                    or float(previous_bar.close) != ma.closes[-1]
                ):
                    raise ValidationError("invalid STS closed component cursor")
                if ma.count:
                    _valid_bar_values(previous_bar)
            if (
                type(self._result[f"CLOSED_{iid}"]) is not bool
                or self._result[f"CLOSED_{iid}"] != closed
            ):
                raise ValidationError("invalid STS interval closure checkpoint")
            raw = self._raw[iid]
            if (raw is None) != (not bucket.valid_prefix):
                raise ValidationError("invalid STS partial prefix raw cache")
            if raw is not None:
                _numeric_record(raw, RAW_FIELDS)
                if type(raw.get("analysisCount")) is not int or raw[
                    "analysisCount"
                ] != self._kernels[iid, "MMACD"].count - int(closed):
                    raise ValidationError("invalid STS cached analysis counter")
                if closed and raw["WOI_BASE"] != self._baselines[iid]:
                    raise ValidationError("invalid STS closed WOI baseline cache")
                for mode, field in (
                    ("MMACD_MAFAST_MODE", "MMACD_MAFAST"),
                    ("MMACD_MASLOW_MODE", "MMACD_MASLOW"),
                    ("WOI_MODE", "WOI"),
                ):
                    if (
                        type(raw[mode]) is not int
                        or raw[mode] not in {0, 1}
                        or (raw[field] is None) != (raw[mode] == 0)
                    ):
                        raise ValidationError("invalid STS cached component mode")
            for field in PUBLIC_FIELDS:
                value = self._result[f"{iid}_STS_{field}"]
                if (
                    value != (None if raw is None else raw.get(field))
                    or value is not None
                    and (
                        type(value) is not float or not math.isfinite(value) or not -1 <= value <= 1
                    )
                ):
                    raise ValidationError("invalid STS cached public scalar")
