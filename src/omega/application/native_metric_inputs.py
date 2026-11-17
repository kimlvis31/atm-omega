"""Fixed native metric policy and exact selected-pin replay; no source or financial IO."""

from dataclasses import dataclass
from decimal import Decimal

from omega.application.auxiliary_codec import (
    decode_observation,
    encode_observation,
    observation_checksum,
)
from omega.application.market_frame_builder import MAX_TIME, MarketFrameBuilder
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Candle, Instrument, Market
from omega.ports.auxiliary import StoredAuxiliary

_NATIVE = (
    ("open_interest", "open_interest_history", "METRIC_OPENINTEREST"),
    ("long_short_ratio", "long_short_ratio", "METRIC_LONGSHORTRATIO"),
)


@dataclass(frozen=True, slots=True)
class MetricInputPolicy:
    maximum_metric_age_ms: int

    def __post_init__(self):
        if (
            type(self.maximum_metric_age_ms) is not int
            or not 0 <= self.maximum_metric_age_ms <= 86400000
        ):
            raise ValidationError("metric selection age must be an integer within0..86400000")

    def document(self):
        self.__post_init__()
        return {
            "version": 1,
            "maximum_metric_age_ms": self.maximum_metric_age_ms,
            "open_interest_source_suffix": "open_interest_history",
            "long_short_source_suffix": "long_short_ratio",
        }

    @classmethod
    def from_document(cls, value):
        if (
            type(value) is not dict
            or set(value)
            != {
                "version",
                "maximum_metric_age_ms",
                "open_interest_source_suffix",
                "long_short_source_suffix",
            }
            or type(value["version"]) is not int
            or value["version"] != 1
            or type(value["open_interest_source_suffix"]) is not str
            or value["open_interest_source_suffix"] != "open_interest_history"
            or type(value["long_short_source_suffix"]) is not str
            or value["long_short_source_suffix"] != "long_short_ratio"
        ):
            raise ValidationError("invalid fixed native metric policy")
        return cls(value["maximum_metric_age_ms"])


def _selection(candle, as_of_ms, policy):
    if type(policy) is not MetricInputPolicy:
        raise ValidationError("metric selection needs an explicit native policy")
    policy = MetricInputPolicy(policy.maximum_metric_age_ms)
    if (
        type(candle) is not Candle
        or type(candle.instrument) is not Instrument
        or candle.instrument.provider != "binance"
        or candle.instrument.market not in {Market.SPOT, Market.USDM}
        or type(candle.open_time_ms) is not int
        or type(candle.close_time_ms) is not int
        or not 0 <= candle.open_time_ms < candle.close_time_ms <= MAX_TIME
        or type(as_of_ms) is not int
        or not candle.close_time_ms <= as_of_ms <= MAX_TIME
    ):
        raise ProtocolError("metric selection requires its native closed candle and actual cutoff")
    instrument = Instrument(
        candle.instrument.provider, candle.instrument.market, candle.instrument.symbol
    )
    checked = Candle(
        instrument,
        candle.open_time_ms,
        candle.close_time_ms,
        *(getattr(candle, key) for key in ("open", "high", "low", "close", "volume")),
    )
    return checked, policy


def _pins(candle, as_of_ms, policy, pins):
    if type(pins) is not tuple or len(pins) > len(_NATIVE):
        raise ProtocolError("metric selection needs at most two immutable native pins")
    if pins and (candle.instrument.market is Market.SPOT or policy.maximum_metric_age_ms == 0):
        raise ProtocolError("unsupported or disabled metric policy cannot acquire native pins")
    selected = {}
    contracts = {channel: (suffix, field) for channel, suffix, field in _NATIVE}
    for pin in pins:
        if type(pin) is not StoredAuxiliary or type(pin.observation) is not AuxiliaryInput:
            raise ProtocolError("metric selection requires typed original native pins")
        # Reconstruct original codecs/types rather than trusting a mutable dataclass shell.
        payload = encode_observation(pin.instrument, pin.observation)
        instrument, observation = decode_observation(payload)
        native = StoredAuxiliary(
            instrument, observation, pin.revision, pin.checksum, pin.previous_checksum
        )
        if type(pin.instrument) is not Instrument or native.instrument != candle.instrument:
            raise ProtocolError("metric native pin instrument differs")
        channel = observation.channel
        if channel not in contracts or channel in selected:
            raise ProtocolError("foreign or duplicate metric native channel")
        suffix, field = contracts[channel]
        number = observation.mapping().get(field)
        if (
            observation.source != candle.instrument.key + ":" + suffix
            or observation.quality != "observed"
            or observation.window_start_ms != observation.event_time_ms // 60000 * 60000
            or not max(0, candle.close_time_ms - policy.maximum_metric_age_ms)
            <= observation.event_time_ms
            < candle.close_time_ms
            or observation.available_at_ms > as_of_ms
            or type(number) is not Decimal
            or not number.is_finite()
            or number < 0
            or native.checksum != observation_checksum(payload)
        ):
            raise ProtocolError("metric native source, eligibility, field or checksum differs")
        selected[channel] = native
    return tuple(selected[channel] for channel, _suffix, _field in _NATIVE if channel in selected)


class _PinnedMetrics:
    def __init__(self, pins):
        self.pins = pins

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        return next(
            (
                pin
                for pin in self.pins
                if pin.instrument == instrument
                and pin.observation.channel == channel
                and pin.observation.source == source
                and event_before_ms - max_age_ms <= pin.observation.event_time_ms < event_before_ms
                and pin.observation.available_at_ms <= as_of_ms
            ),
            None,
        )


def rebuild_metric(candle, *, as_of_ms, policy, pins):
    """Rebuild only recorded selection; not proof of complete provider/DB history.

    The builder retains sparse source clocks and applies freshness at actual
    as_of_ms. In particular, eligible pins can produce a missing derived view
    when decision latency makes them stale. No latest-source query is performed.
    """
    candle, policy = _selection(candle, as_of_ms, policy)
    selected = _pins(candle, as_of_ms, policy, pins)
    builder = MarketFrameBuilder(
        _PinnedMetrics(selected),
        maximum_metric_age_ms=policy.maximum_metric_age_ms,
        open_interest_source_suffix="open_interest_history",
        long_short_source_suffix="long_short_ratio",
    )
    prepared = builder.prepare(candle, as_of_ms=as_of_ms, channels=("metric",))
    if prepared.pins != selected:
        raise ProtocolError("metric rebuild did not retain its exact selected native pins")
    return prepared


def validate_metric(candle, observation, *, as_of_ms, policy, pins):
    """Reject a changed derived view or noncanonical original native pin order."""
    if type(observation) is not AuxiliaryInput or observation.channel != "metric":
        raise ProtocolError("metric validation requires its original derived observation")
    prepared = rebuild_metric(candle, as_of_ms=as_of_ms, policy=policy, pins=pins)
    if prepared.pins != pins or encode_observation(
        candle.instrument, observation
    ) != encode_observation(candle.instrument, prepared.frame.input("metric")):
        raise ProtocolError("derived metric observation or selected pin order changed")
    return prepared
