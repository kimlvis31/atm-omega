"""Eta interpreted OHLCV volume profile, distinct from actual trade-price volume.

NumPy/SciPy are optional and imported only when a seeded profile is evaluated.
The historical profile decays and rebins; it is never rebuilt as a sliding window.
"""

import math
from collections import deque
from copy import deepcopy
from dataclasses import dataclass

from omega.analysis.aggregation import (
    INTERVAL_MINUTES,
    AggregatedBar,
    CandleAggregation,
    validate_minute,
)
from omega.domain.errors import ValidationError
from omega.domain.market_inputs import MarketFeatureFrame
from omega.domain.models import Instrument
from omega.ports.modules import ModuleDescriptor

PROFILE_FIELDS = (
    "lastOpenPrice",
    "lastClosePrice",
    "priceMax",
    "gammaFactor",
    "betaFactor",
    "divisionHeight",
    "volumePriceLevelProfile",
    "analysisCount",
)
RAW_FIELDS = frozenset(
    (
        *PROFILE_FIELDS,
        "volumePriceLevelProfile_Filtered",
        "volumePriceLevelProfile_Filtered_Max",
        "volumePriceLevelProfile_Boundaries",
        "volumePriceLevelProfile_NearBoundaries",
    )
)


def _libraries():
    try:
        import numpy
        from scipy import ndimage, signal
    except ImportError:
        raise ValidationError(
            "IVP requires the optional analysis dependencies (NumPy and SciPy)"
        ) from None
    return numpy, ndimage, signal


@dataclass(frozen=True)
class ProfileConfiguration:
    interval_id: int
    nSamples: int = 500
    gammaFactor: float = 0.01
    deltaFactor: float = 1.0
    prominence: float = 0.1
    distance: int = 5
    height: float = 0.5
    price_precision: int = 8
    max_divisions: int = 8192

    def __post_init__(self):
        if (
            type(self.interval_id) is not int
            or self.interval_id not in INTERVAL_MINUTES
            or type(self.nSamples) is not int
            or not 2 <= self.nSamples <= 20000
            or type(self.distance) is not int
            or not 1 <= self.distance <= 100
            or type(self.price_precision) is not int
            or not 0 <= self.price_precision <= 16
            or type(self.max_divisions) is not int
            or not 2 <= self.max_divisions <= 32768
        ):
            raise ValidationError(
                "invalid bounded IVP interval, period, distance or profile budget"
            )
        for name, lower, upper in (
            ("gammaFactor", 0.005, 0.1),
            ("deltaFactor", 0.1, 10),
            ("prominence", 0.01, 1),
            ("height", 0, 1),
        ):
            value = getattr(self, name)
            if (
                type(value) not in {int, float}
                or not math.isfinite(value)
                or not lower <= value <= upper
            ):
                raise ValidationError("invalid IVP smoothing, prominence or height")
            object.__setattr__(self, name, float(value))


def _add_profile(weight, low, high, profile, divisionHeight, precision):
    """Distribute a bar/rebin cell's weight using Eta's price-precision arithmetic."""
    floor, ceiling = int(low / divisionHeight), int(high / divisionHeight)
    count = len(profile)
    if floor >= count:
        return
    if floor == ceiling:
        profile[floor] += weight
        if profile[floor] < 0:
            profile[floor] = 0
    elif ceiling == floor + 1:
        density = weight / (high - low)
        center = round(ceiling * divisionHeight, precision)
        profile[floor] += density * (center - low)
        if profile[floor] < 0:
            profile[floor] = 0
        if ceiling < count:
            profile[ceiling] += density * (high - center)
            if profile[ceiling] < 0:
                profile[ceiling] = 0
    else:
        density = weight / (high - low)
        position = round((floor + 1) * divisionHeight, precision)
        profile[floor] += density * (position - low)
        if profile[floor] < 0:
            profile[floor] = 0
        volume = density * divisionHeight
        for index in range(floor + 1, min(ceiling, count)):
            profile[index] += volume
            if profile[index] < 0:
                profile[index] = 0
        if ceiling < count:
            position = round(ceiling * divisionHeight, precision)
            profile[ceiling] += density * (high - position)
            if profile[ceiling] < 0:
                profile[ceiling] = 0


def _scale(price_max, close, configuration):
    precision = configuration.price_precision
    beta = round(close * configuration.gammaFactor, precision)
    if not math.isfinite(beta) or beta <= 0:
        raise ValidationError("IVP gamma and price precision produce a zero/invalid profile scale")
    order = math.floor(math.log(price_max, 10))
    digit = int(price_max / pow(10, order))
    if digit == 10:
        digit, order = 1, order + 1
    ceiling_digit = int(digit / 1) + 1
    if ceiling_digit == 10:
        ceiling_digit, order = 1, order + 1
    ceiling = ceiling_digit * pow(10, order)
    minimum = beta / 10
    order = math.floor(math.log(minimum, 10))
    digit = int(minimum / pow(10, order))
    if digit == 10:
        digit, order = 1, order + 1
    raw_height = (int(digit) or 1) * pow(10, order)
    unit = pow(10, -precision)
    units = int(raw_height / unit)
    height = round(unit if units == 0 else unit * units, precision)
    if not math.isfinite(height) or height <= 0 or not math.isfinite(ceiling):
        raise ValidationError("IVP profile scale is outside finite arithmetic")
    divisions = math.ceil(ceiling / height)
    if not 1 <= divisions <= configuration.max_divisions:
        raise ValidationError("IVP division budget exceeded; previous state retained")
    return beta, height, divisions


class _ProfileKernel:
    def __init__(self, configuration):
        self.config = configuration
        self.history = deque(maxlen=configuration.nSamples - 1)
        self.previous = {}

    def preview(self, bar):
        config, previous = self.config, self.previous
        opened, high, low, close, volume = map(
            float, (bar.open, bar.high, bar.low, bar.close, bar.volume)
        )
        if (
            any(not math.isfinite(value) for value in (opened, high, low, close, volume))
            or min(opened, high, low, close) <= 0
        ):
            raise ValidationError("IVP OHLCV is outside finite legacy arithmetic")
        count = previous.get("analysisCount", -1) + 1
        price_max = max(previous.get("priceMax", high), high)
        beta = division = profile = filtered = maximum = boundaries = None
        near = [None] * 10
        if count >= config.nSamples - 1:
            beta, division, divisions = _scale(price_max, close, config)
            numpy, ndimage, signal = _libraries()
            prior = previous.get("volumePriceLevelProfile")
            if prior is None:
                profile = numpy.zeros(divisions)
                for weight, lower, upper in ((volume, low, high), *reversed(self.history)):
                    _add_profile(weight, lower, upper, profile, division, config.price_precision)
            else:
                previous_height = previous["divisionHeight"]
                if previous_height == division and len(prior) == divisions:
                    profile = numpy.array(prior, dtype=float, copy=True)
                else:
                    profile = numpy.zeros(divisions)
                    for index, weight in enumerate(prior):
                        _add_profile(
                            weight,
                            round(previous_height * index, config.price_precision),
                            round(previous_height * (index + 1), config.price_precision),
                            profile,
                            division,
                            config.price_precision,
                        )
                profile = profile * (1 - 1 / config.nSamples)
                _add_profile(volume, low, high, profile, division, config.price_precision)
            if not numpy.isfinite(profile).all() or (profile < 0).any():
                raise ValidationError("IVP volume arithmetic overflow")
            if numpy.sum(profile) != 0:
                filtered = ndimage.gaussian_filter1d(input=profile, sigma=config.deltaFactor)
                maximum = numpy.max(filtered)
                if not numpy.isfinite(filtered).all() or not math.isfinite(float(maximum)):
                    raise ValidationError("IVP smoothing arithmetic overflow")
                peaks, _ = signal.find_peaks(
                    filtered,
                    prominence=maximum * config.prominence,
                    distance=config.distance,
                    height=maximum * config.height,
                )
                valleys, _ = signal.find_peaks(
                    -filtered,
                    prominence=maximum * config.prominence * 0.5,
                    distance=config.distance,
                    height=-maximum * (1 - config.height),
                )
                boundaries = sorted(peaks.tolist() + valleys.tolist())
                opening_index = opened // division
                nearest = next(
                    (i for i, position in enumerate(boundaries) if opening_index <= position), None
                )
                above = len(boundaries) if nearest is None else nearest
                for i in range(5):
                    for target, source in ((i, above - 5 + i), (i + 5, above + i)):
                        if 0 <= source < len(boundaries):
                            near[target] = round(
                                (boundaries[source] + 0.5) * division, config.price_precision
                            )
        return {
            "lastOpenPrice": opened,
            "lastClosePrice": close,
            "priceMax": price_max,
            "gammaFactor": config.gammaFactor,
            "betaFactor": beta,
            "divisionHeight": division,
            "volumePriceLevelProfile": None if profile is None else profile.tolist(),
            "volumePriceLevelProfile_Filtered": None if filtered is None else filtered.tolist(),
            "volumePriceLevelProfile_Filtered_Max": None if maximum is None else float(maximum),
            "volumePriceLevelProfile_Boundaries": boundaries,
            "volumePriceLevelProfile_NearBoundaries": tuple(near)
            if boundaries is not None
            else near,
            "analysisCount": count,
        }

    def commit(self, bar, result):
        self.previous = {key: deepcopy(result[key]) for key in PROFILE_FIELDS}
        self.previous["closed_time_ms"] = bar.close_time_ms
        if result["volumePriceLevelProfile"] is None:
            self.history.append((float(bar.volume), float(bar.low), float(bar.high)))
        else:
            self.history.clear()

    def validate_checkpoint(self):
        config = self.config
        if (
            not isinstance(self.history, deque)
            or self.history.maxlen != config.nSamples - 1
            or not isinstance(self.previous, dict)
        ):
            raise ValidationError("invalid IVP seed history")
        if self.previous == {}:
            if self.history:
                raise ValidationError("IVP seed history lacks its cursor")
            return
        if (
            set(self.previous) != {*PROFILE_FIELDS, "closed_time_ms"}
            or type(self.previous["analysisCount"]) is not int
            or not 0 <= self.previous["analysisCount"] <= 2**63 - 1
        ):
            raise ValidationError("invalid IVP kernel state schema")
        if (
            type(self.previous["closed_time_ms"]) is not int
            or self.previous["closed_time_ms"] <= 0
            or self.previous["closed_time_ms"] % (INTERVAL_MINUTES[config.interval_id] * 60000)
        ):
            raise ValidationError("invalid IVP closed profile cursor")
        count = self.previous["analysisCount"]
        profile = self.previous["volumePriceLevelProfile"]
        if (profile is None) != (count < config.nSamples - 1) or len(self.history) != (
            count + 1 if profile is None else 0
        ):
            raise ValidationError("inconsistent IVP seeding state")
        for row in self.history:
            if (
                type(row) is not tuple
                or len(row) != 3
                or any(type(value) is not float or not math.isfinite(value) for value in row)
                or row[0] < 0
                or not 0 < row[1] <= row[2]
            ):
                raise ValidationError("invalid IVP seed observation")
        for field in ("lastOpenPrice", "lastClosePrice", "priceMax", "gammaFactor"):
            value = self.previous[field]
            if type(value) is not float or not math.isfinite(value) or value <= 0:
                raise ValidationError("invalid IVP price/scale state")
        if self.previous["gammaFactor"] != config.gammaFactor or self.previous["priceMax"] < max(
            self.previous["lastOpenPrice"], self.previous["lastClosePrice"]
        ):
            raise ValidationError("inconsistent IVP price maximum")
        if profile is not None:
            beta, height, count = _scale(
                self.previous["priceMax"], self.previous["lastClosePrice"], config
            )
            if (
                type(profile) is not list
                or len(profile) != count
                or self.previous["divisionHeight"] != height
                or self.previous["betaFactor"] != beta
                or any(
                    type(value) is not float or not math.isfinite(value) or value < 0
                    for value in profile
                )
            ):
                raise ValidationError("invalid IVP retained profile")
        elif self.previous["betaFactor"] is not None or self.previous["divisionHeight"] is not None:
            raise ValidationError("unseeded IVP cannot contain a profile scale")


def _validate_raw(raw, kernel, bar):
    config = kernel.config
    if (
        not isinstance(raw, dict)
        or set(raw) != RAW_FIELDS
        or type(raw["analysisCount"]) is not int
        or raw["analysisCount"] < 0
    ):
        raise ValidationError("invalid IVP raw cache schema")
    for field in ("lastOpenPrice", "lastClosePrice", "priceMax", "gammaFactor"):
        if type(raw[field]) is not float or not math.isfinite(raw[field]) or raw[field] <= 0:
            raise ValidationError("invalid IVP cached price or scale")
    if (
        raw["lastOpenPrice"] != float(bar.open)
        or raw["lastClosePrice"] != float(bar.close)
        or raw["priceMax"] < float(bar.high)
        or raw["gammaFactor"] != config.gammaFactor
    ):
        raise ValidationError("inconsistent IVP cached OHLCV")
    profile = raw["volumePriceLevelProfile"]
    filtered = raw["volumePriceLevelProfile_Filtered"]
    boundaries = raw["volumePriceLevelProfile_Boundaries"]
    maximum = raw["volumePriceLevelProfile_Filtered_Max"]
    if (profile is None) != (raw["analysisCount"] < config.nSamples - 1):
        raise ValidationError("invalid IVP cached seed phase")
    if profile is None:
        if raw["betaFactor"] is not None or raw["divisionHeight"] is not None:
            raise ValidationError("invalid IVP unseeded scale cache")
    else:
        beta, division, count = _scale(raw["priceMax"], raw["lastClosePrice"], config)
        if (
            type(profile) is not list
            or len(profile) != count
            or raw["divisionHeight"] != division
            or raw["betaFactor"] != beta
            or any(type(v) is not float or not math.isfinite(v) or v < 0 for v in profile)
        ):
            raise ValidationError("invalid IVP profile cache")
    empty = profile is None or not any(profile)
    near = raw["volumePriceLevelProfile_NearBoundaries"]
    if not isinstance(near, (list, tuple)) or len(near) != 10:
        raise ValidationError("invalid IVP near-boundary cache")
    if empty:
        if (
            filtered is not None
            or maximum is not None
            or boundaries is not None
            or list(near) != [None] * 10
        ):
            raise ValidationError("empty IVP profile cannot contain detected boundaries")
    else:
        if (
            type(filtered) is not list
            or len(filtered) != len(profile)
            or any(type(v) is not float or not math.isfinite(v) or v < 0 for v in filtered)
            or type(maximum) is not float
            or maximum != max(filtered)
            or maximum <= 0
        ):
            raise ValidationError("invalid IVP filtered profile cache")
        if (
            type(boundaries) is not list
            or any(type(v) is not int or not 0 <= v < len(profile) for v in boundaries)
            or boundaries != sorted(set(boundaries))
        ):
            raise ValidationError("invalid IVP detected boundary indices")
        opening = raw["lastOpenPrice"] // raw["divisionHeight"]
        above = next((i for i, value in enumerate(boundaries) if opening <= value), len(boundaries))
        expected = [None] * 10
        for i in range(5):
            for target, source in ((i, above - 5 + i), (i + 5, above + i)):
                if 0 <= source < len(boundaries):
                    expected[target] = round(
                        (boundaries[source] + 0.5) * raw["divisionHeight"], config.price_precision
                    )
        if list(near) != expected:
            raise ValidationError("inconsistent IVP near-boundary selection")


class LegacyVolumeProfileFeatures:
    descriptor = ModuleDescriptor("legacy.ivp", "1", frozenset({"OHLCV"}), True)
    optional_dependencies = ("numpy", "scipy")

    @staticmethod
    def validate_configurations(configurations):
        """Check total seed/profile budgets without optional libraries or kernels."""
        configurations = tuple(configurations)
        if (
            not 1 <= len(configurations) <= 12
            or any(not isinstance(c, ProfileConfiguration) for c in configurations)
            or len({c.interval_id for c in configurations}) != len(configurations)
            or sum(c.nSamples for c in configurations) > 20000
            or sum(c.max_divisions for c in configurations) > 32768
        ):
            raise ValidationError("IVP requires unique intervals within seed/profile budgets")
        return configurations

    def __init__(self, instrument, configurations):
        if not isinstance(instrument, Instrument):
            raise ValidationError("IVP requires a typed instrument")
        configurations = self.validate_configurations(configurations)
        self.instrument = instrument
        self._kernels = {c.interval_id: _ProfileKernel(c) for c in configurations}
        self._aggregation = CandleAggregation(instrument, tuple(self._kernels))
        self._last = None
        self._result, self._raw = {}, {}

    @property
    def warmup_alignment_minutes(self):
        return self._aggregation.warmup_alignment_minutes

    @property
    def input_ready(self):
        return bool(self._raw) and all(value is not None for value in self._raw.values())

    @property
    def calculation_ready(self):
        return self.input_ready and all(
            value["volumePriceLevelProfile"] is not None for value in self._raw.values()
        )

    @property
    def nullable_fields(self):
        return frozenset(f"{iid}_IVP_NB{index}" for iid in self._kernels for index in range(10))

    @property
    def input_quality(self):
        return {
            f"{iid}_IVP": {
                "ready": raw is not None,
                "reason": "observed" if raw is not None else "partial_prefix",
                "channel": "OHLCV",
            }
            for iid, raw in self._raw.items()
        }

    @property
    def state(self):
        return deepcopy(
            {
                "kernels": {
                    iid: {"history": tuple(kernel.history), "previous": kernel.previous}
                    for iid, kernel in self._kernels.items()
                },
                "raw": self._raw,
                "last": self._last,
            }
        )

    def update_frame(self, frame):
        if not isinstance(frame, MarketFeatureFrame):
            raise ValidationError("IVP requires a market frame")
        return self.update(frame.candle)

    def update(self, candle):
        if type(candle.open_time_ms) is not int or type(candle.close_time_ms) is not int:
            raise ValidationError("IVP requires exact minute timestamps")
        validate_minute(self.instrument, self._last, candle)
        if candle == self._last:
            return dict(self._result)
        kernels, aggregation = deepcopy(self._kernels), deepcopy(self._aggregation)
        frames, result, raw = aggregation.update(candle), {}, {}
        for iid, kernel in kernels.items():
            bar = frames[f"BAR_{iid}"]
            value = kernel.preview(bar) if bar.valid_prefix else None
            raw[iid] = value
            for index in range(10):
                result[f"{iid}_IVP_NB{index}"] = (
                    None
                    if value is None
                    else value["volumePriceLevelProfile_NearBoundaries"][index]
                )
            result[f"CLOSED_{iid}"] = bar.complete
            if bar.complete:
                kernel.commit(bar, value)
        self._kernels, self._aggregation = kernels, aggregation
        self._last, self._result, self._raw = candle, result, raw
        return dict(result)

    def checkpoint(self):
        from omega.analysis.checkpoint import checkpoint

        return checkpoint(self)

    def restored(self, value, instrument):
        from omega.analysis.checkpoint import restore

        return restore(self, value, instrument)

    def validate_checkpoint(self):
        if (
            not isinstance(self._kernels, dict)
            or tuple(self._kernels) != self._aggregation.intervals
            or not all(isinstance(value, dict) for value in (self._raw, self._result))
        ):
            raise ValidationError("invalid IVP provider mapping schema")
        for kernel in self._kernels.values():
            if not isinstance(kernel, _ProfileKernel):
                raise ValidationError("invalid IVP kernel type")
            kernel.validate_checkpoint()
        if self._last is None:
            if (
                self._raw
                or self._result
                or self._aggregation._last is not None
                or any(kernel.previous for kernel in self._kernels.values())
            ):
                raise ValidationError("inconsistent empty IVP provider state")
            return
        if (
            self._last.instrument != self.instrument
            or self._aggregation._last != self._last
            or set(self._raw) != set(self._kernels)
            or set(self._aggregation._buckets) != set(self._kernels)
        ):
            raise ValidationError("invalid IVP provider cursor")
        public = set()
        for iid, kernel in self._kernels.items():
            bar = self._aggregation._buckets[iid]
            raw = self._raw[iid]
            duration = INTERVAL_MINUTES[iid] * 60000
            if (
                not isinstance(bar, AggregatedBar)
                or bar.instrument != self.instrument
                or bar.interval_id != iid
                or bar.open_time_ms != self._last.open_time_ms // duration * duration
                or bar.close_time_ms != self._last.close_time_ms
                or (raw is None) != (not bar.valid_prefix)
                or type(bar.valid_prefix) is not bool
                or type(bar.complete) is not bool
                or bar.complete
                != (bar.valid_prefix and self._last.close_time_ms == bar.open_time_ms + duration)
                or self._aggregation._result.get(f"BAR_{iid}") != bar
            ):
                raise ValidationError("invalid IVP current aggregate")
            if raw is not None:
                _validate_raw(raw, kernel, bar)
                if (
                    not isinstance(raw, dict)
                    or set(raw) != RAW_FIELDS
                    or raw["analysisCount"]
                    != kernel.previous.get("analysisCount", -1) + (0 if bar.complete else 1)
                ):
                    raise ValidationError("invalid IVP preview cache")
                expected_end = self._last.close_time_ms if bar.complete else bar.open_time_ms
                if kernel.previous and kernel.previous["closed_time_ms"] != expected_end:
                    raise ValidationError("invalid IVP retained profile cursor")
                if bar.complete and any(
                    kernel.previous[field] != raw[field] for field in PROFILE_FIELDS
                ):
                    raise ValidationError("IVP closed raw cache differs from its retained profile")
            elif kernel.previous:
                raise ValidationError("partial-prefix IVP cannot have a seeded past")
            for index in range(10):
                field = f"{iid}_IVP_NB{index}"
                public.add(field)
                if self._result.get(field) != (
                    None if raw is None else raw["volumePriceLevelProfile_NearBoundaries"][index]
                ):
                    raise ValidationError("invalid IVP boundary cache")
            public.add(f"CLOSED_{iid}")
            if (
                type(self._result.get(f"CLOSED_{iid}")) is not bool
                or self._result[f"CLOSED_{iid}"] != bar.complete
            ):
                raise ValidationError("invalid IVP interval closure")
        if set(self._result) != public:
            raise ValidationError("invalid IVP public schema")
