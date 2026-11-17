"""Select only requested, causally available auxiliary observations for a candle.

Native metric samples stay immutable in storage. The combined ``metric`` input
is a derived minute view, with the actual source revisions retained in ``pins``.
Storage failures propagate; absence is never used to hide an unknown DB outcome.
"""

# Imports
import re
from decimal import Decimal
from hashlib import sha256

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput, MarketFeatureFrame
from omega.domain.models import Candle, Instrument, Market
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.ports.auxiliary import AuxiliaryStore, StoredAuxiliary

# Constants
MINUTE = 60_000
MAX_TIME = 253_402_300_799_000
CHANNELS = frozenset({"trade_flow", "metric", "depth"})
DEPTH_SOURCE_SUFFIX = "binance_depth_ws_v1"


class MarketFrameBuilder:
    def __init__(
        self,
        store: AuxiliaryStore,
        *,
        maximum_metric_age_ms: int = 330_000,
        open_interest_source_suffix: str = "open_interest_history",
        long_short_source_suffix: str = "long_short_ratio",
    ):
        if (
            type(maximum_metric_age_ms) is not int
            or not 0 <= maximum_metric_age_ms <= 86_400_000
            or any(
                not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value)
                for value in (open_interest_source_suffix, long_short_source_suffix)
            )
        ):
            raise ValidationError("invalid metric freshness or dataset suffix")
        self.store = store
        self.maximum_metric_age_ms = maximum_metric_age_ms
        self.open_interest_source_suffix = open_interest_source_suffix
        self.long_short_source_suffix = long_short_source_suffix

    @staticmethod
    def _source(candle, suffix):
        value = f"{candle.instrument.key}:{suffix}"
        if len(value) > 96:
            raise ValidationError("auxiliary dataset identity exceeds its source bound")
        return value

    @staticmethod
    def _empty(candle, as_of_ms, channel, source, quality="missing"):
        return AuxiliaryInput(
            channel=channel,
            values=(),
            window_start_ms=candle.open_time_ms,
            window_end_ms=candle.close_time_ms,
            event_time_ms=candle.close_time_ms,
            available_at_ms=as_of_ms,
            quality=quality,
            coverage=Decimal(0),
            source=source,
        )

    @staticmethod
    def _checked(value, candle, channel, source, as_of_ms):
        if value is None:
            return None
        if (
            not isinstance(value, StoredAuxiliary)
            or value.instrument != candle.instrument
            or value.observation.channel != channel
            or value.observation.source != source
            or value.observation.available_at_ms > as_of_ms
        ):
            raise ProtocolError("auxiliary read conflicts with requested identity or availability")
        return value

    def _flow(self, candle, as_of_ms):
        source = self._source(candle, "aggTrade")
        value = self._checked(
            self.store.get(
                candle.instrument, "trade_flow", source, candle.open_time_ms, as_of_ms=as_of_ms
            ),
            candle,
            "trade_flow",
            source,
            as_of_ms,
        )
        if value is None:
            return self._empty(candle, as_of_ms, "trade_flow", source), ()
        observation = value.observation
        if (
            observation.window_start_ms != candle.open_time_ms
            or observation.window_end_ms != candle.close_time_ms
            or observation.event_time_ms != candle.close_time_ms
        ):
            raise ProtocolError("trade-flow read is not the requested closed minute")
        return observation, (value,)

    def _sample(self, candle, as_of_ms, channel, suffix):
        # Native events must precede close strictly. A zero-age policy has no
        # eligible sample; don't send an invalid zero horizon to the store.
        if self.maximum_metric_age_ms == 0:
            return None
        source = self._source(candle, suffix)
        value = self._checked(
            self.store.latest_sample(
                candle.instrument,
                channel,
                source,
                event_before_ms=candle.close_time_ms,
                as_of_ms=as_of_ms,
                max_age_ms=self.maximum_metric_age_ms,
            ),
            candle,
            channel,
            source,
            as_of_ms,
        )
        if value is not None:
            observation = value.observation
            if (
                observation.event_time_ms >= candle.close_time_ms
                or observation.window_start_ms != observation.event_time_ms // MINUTE * MINUTE
            ):
                raise ProtocolError("native metric read violates its sample time or minute")
        return value

    def _depth(self, candle, as_of_ms):
        source = self._source(candle, DEPTH_SOURCE_SUFFIX)
        value = self._checked(
            self.store.get(
                candle.instrument, "depth", source, candle.open_time_ms, as_of_ms=as_of_ms
            ),
            candle,
            "depth",
            source,
            as_of_ms,
        )
        if value is None:
            return self._empty(candle, as_of_ms, "depth", source), ()
        observation = value.observation
        if (
            observation.window_start_ms != candle.open_time_ms
            or observation.window_end_ms != candle.close_time_ms
            or not (
                candle.open_time_ms <= observation.event_time_ms < candle.close_time_ms
                or observation.quality in {"missing", "unsupported"}
                and not observation.values
                and observation.coverage == Decimal(0)
                and observation.event_time_ms == candle.close_time_ms
            )
            or observation.available_at_ms < candle.close_time_ms
        ):
            raise ProtocolError("depth is not the requested finalized minute snapshot")
        # Partial range/stream coverage is retained, never promoted to observed.
        return observation, (value,)

    def _metric(self, candle, as_of_ms, feature_time_ms):
        oi_source = self._source(candle, self.open_interest_source_suffix)
        ls_source = self._source(candle, self.long_short_source_suffix)
        # Stable label identifies this derived view, not a provider response/dataset.
        digest = sha256((oi_source + "\0" + ls_source).encode()).hexdigest()
        view_source = "metric_view:" + digest
        if candle.instrument.market == Market.SPOT:
            return self._empty(candle, as_of_ms, "metric", view_source, "unsupported"), ()
        oi = self._sample(candle, as_of_ms, "open_interest", self.open_interest_source_suffix)
        ls = self._sample(candle, as_of_ms, "long_short_ratio", self.long_short_source_suffix)
        pins = tuple(value for value in (oi, ls) if value is not None)
        if oi is None or ls is None:
            return self._empty(candle, as_of_ms, "metric", view_source), pins
        observations = (oi.observation, ls.observation)
        # DB lookup's horizon is relative to bar close; latency can make an otherwise
        # eligible source sample stale at this actual analysis availability cutoff.
        if any(
            observation.quality != "observed"
            or observation.coverage != 1
            or not 0 <= feature_time_ms - observation.event_time_ms <= self.maximum_metric_age_ms
            for observation in observations
        ):
            return self._empty(candle, as_of_ms, "metric", view_source), pins
        values = []
        for label, observation in zip(
            ("OPENINTEREST", "LONGSHORTRATIO"), observations, strict=True
        ):
            field = "METRIC_" + label
            number = observation.mapping().get(field)
            if type(number) is not Decimal or number < 0:
                raise ProtocolError("native metric sample lacks its nonnegative Decimal value")
            values.extend(
                (
                    (field, number),
                    (f"SOURCE_{label}_TIME_MS", observation.event_time_ms),
                    (f"SOURCE_{label}_AVAILABLE_MS", observation.available_at_ms),
                )
            )
        event = min(value.event_time_ms for value in observations)
        return (
            AuxiliaryInput(
                channel="metric",
                values=tuple(values),
                window_start_ms=candle.open_time_ms,
                window_end_ms=candle.close_time_ms,
                event_time_ms=event,
                available_at_ms=max(value.available_at_ms for value in observations),
                quality="carried" if event < candle.open_time_ms else "observed",
                coverage=Decimal(1),
                source=view_source,
            ),
            pins,
        )

    def prepare(
        self, candle: Candle, *, as_of_ms: int, channels=(), retrospective=False
    ) -> PreparedMarketFrame:
        """Prepare live inputs, or explicitly replay with bar-close feature freshness.

        Retrospective mode retains the actual knowledge/availability cutoff. It
        does not claim the downloaded observations were known at historical close.
        """
        if (
            not isinstance(candle, Candle)
            or not isinstance(candle.instrument, Instrument)
            or type(candle.open_time_ms) is not int
            or type(candle.close_time_ms) is not int
            or not 0 <= candle.open_time_ms < candle.close_time_ms <= MAX_TIME
            or type(as_of_ms) is not int
            or not candle.close_time_ms <= as_of_ms <= MAX_TIME
            or not isinstance(channels, (tuple, list, set, frozenset))
            or len(channels) > len(CHANNELS)
            or any(type(value) is not str or value not in CHANNELS for value in channels)
            or len(channels) != len(set(channels))
            or type(retrospective) is not bool
        ):
            raise ValidationError("invalid closed candle, cutoff or requested auxiliary channels")
        inputs, pins = [], []
        reference_time = candle.close_time_ms if retrospective else None
        for channel in sorted(channels):
            if channel == "depth":
                value, selected = self._depth(candle, as_of_ms)
            elif channel == "trade_flow":
                value, selected = self._flow(candle, as_of_ms)
            else:
                value, selected = self._metric(
                    candle, as_of_ms, reference_time if retrospective else as_of_ms
                )
            inputs.append(value)
            pins.extend(selected)
        return PreparedMarketFrame(
            MarketFeatureFrame(
                candle=candle,
                as_of_ms=as_of_ms,
                inputs=tuple(inputs),
                reference_time_ms=reference_time,
            ),
            tuple(pins),
        )
