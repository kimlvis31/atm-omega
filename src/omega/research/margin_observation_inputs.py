"""Native selected-observation inputs for isolated shared-margin research replay.

The job must first verify/snapshot the bundle with load_auxiliary_dataset. This
reader has no file/DB/network access: it rechecks the selected canonical record
identity, retains actual availability and never reconstructs absent revisions.
"""

# Imports
import json
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from types import MappingProxyType

from omega.analysis.order_flow import FLOW_FIELDS
from omega.application.market_frame_builder import MAX_TIME, MarketFrameBuilder
from omega.application.native_metric_inputs import MetricInputPolicy, validate_metric
from omega.domain.errors import ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.research.auxiliary_datasets import SCHEMA_ID_V2, freeze_selected_observation_snapshot


def _instrument(key):
    if type(key) is not str:
        raise ValidationError("native replay requirements need canonical instrument keys")
    try:
        provider, market, symbol = key.split(":")
        value = Instrument(provider, Market(market), symbol)
    except (ValueError, TypeError):
        raise ValidationError("invalid native replay instrument key") from None
    if value.key != key:
        raise ValidationError("native replay instrument key changed during validation")
    return value


class NativeObservationReplay:
    def __init__(self, manifest, observations, *, decision_latency_ms=0):
        if type(decision_latency_ms) is not int or not 0 <= decision_latency_ms <= 60_000:
            raise ValidationError(
                "native replay decision latency must be an integer within0..60000"
            )
        if type(manifest) is dict and manifest.get("schema_id") == SCHEMA_ID_V2:
            self._v2 = _RetainedNativeReplay(manifest, observations, decision_latency_ms)
            self._latency = decision_latency_ms
            return
        header, selected = freeze_selected_observation_snapshot(manifest, observations)
        entries = {}
        for native in selected:
            instrument, observation = native.instrument, native.observation
            if observation.source.startswith("research_mc:"):
                raise ValidationError("generated observations are not native replay inputs")
            if observation.channel == "trade_flow":
                if (
                    observation.source != instrument.key + ":aggTrade"
                    or observation.event_time_ms != observation.window_end_ms
                    or observation.quality == "carried"
                ):
                    raise ValidationError("native replay trade-flow source/window policy differs")
                values = observation.mapping()
                if observation.quality in {"observed", "partial"} and any(
                    type(values.get(name)) is not Decimal
                    or not values[name].is_finite()
                    or values[name] < 0
                    for name in FLOW_FIELDS
                ):
                    raise ValidationError("native replay trade flow requires nonnegative notionals")
            key = (
                instrument.key,
                observation.window_start_ms,
                observation.channel,
                observation.source,
            )
            entries[key] = native
        provenance = header.get("export_provenance")
        if provenance is not None:
            declarations = provenance["data"]["selections"]
            declared = {
                row["instrument"]
                for row in declarations
                if row["channel"] == "trade_flow"
                and row["source"] == row["instrument"] + ":aggTrade"
            }
        else:
            declared = {
                source.removesuffix(":aggTrade")
                for source in header["channels"].get("trade_flow", {}).get("sources", [])
                if source.endswith(":aggTrade")
            }
        for key in declared:
            _instrument(key)
        self._entries = MappingProxyType(entries)
        self._declared = frozenset(declared)
        self._latency = decision_latency_ms
        lineage = {
            "schema_id": "omega.native-margin-observation-replay.v1",
            "scope": "native_observation_availability",
            "manifest_sha256": sha256(self._json(header)).hexdigest(),
            "candle_manifest_sha256": header["candles"]["sha256"],
            "candle_rows_sha256": header["candles"]["rows_sha256"],
            "observations_sha256": header["observations"]["sha256"],
            "observations_count": len(selected),
            "selection_policy": "same_minute_selected_revision_no_substitution",
            "declared_trade_flow_sources": [key + ":aggTrade" for key in sorted(declared)],
            "decision_latency_ms": decision_latency_ms,
        }
        self._lineage = self._json(lineage)
        self._destination = sha256(self._lineage).hexdigest()
        self._builder = MarketFrameBuilder(self, maximum_metric_age_ms=0)

    @staticmethod
    def _json(value):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()

    @property
    def lineage(self):
        if self.version == 2:
            return self._v2.lineage
        return json.loads(self._lineage)

    @property
    def destination_id(self):
        if self.version == 2:
            return self._v2.destination_id
        return self._destination

    @property
    def version(self):
        return 2 if hasattr(self, "_v2") else 1

    def bind_configuration(self, required_channels, metric_policies, *, start_ms, end_ms):
        """Freeze actual graph policies before creating any financial namespace."""
        if self.version != 2:
            raise ValidationError("version1 replay keeps its original selection contract")
        self._v2.bind_configuration(required_channels, metric_policies, start_ms, end_ms)

    def validate_configuration(self, required_channels, metric_policies):
        if self.version != 2:
            raise ValidationError("version1 replay keeps its original selection contract")
        self._v2.validate_configuration(required_channels, metric_policies)

    def _candles(self, candles):
        if type(candles) is not tuple or not 1 <= len(candles) <= 100:
            raise ValidationError("native replay requires one bounded common candle tuple")
        copied = []
        for candle in candles:
            if (
                type(candle) is not Candle
                or type(candle.instrument) is not Instrument
                or type(candle.open_time_ms) is not int
                or type(candle.close_time_ms) is not int
                or not 0 <= candle.open_time_ms < candle.close_time_ms <= MAX_TIME
            ):
                raise ValidationError("native replay requires native aligned candle clocks")
            copied.append(replace(candle, instrument=replace(candle.instrument)))
        if len({item.open_time_ms for item in copied}) != 1 or len(
            {item.instrument for item in copied}
        ) != len(copied):
            raise ValidationError("native replay candles must share one unique instrument minute")
        if copied[0].close_time_ms + self._latency > MAX_TIME:
            raise ValidationError("native replay decision cutoff overflows its time bound")
        return tuple(copied)

    def as_of_ms(self, candles):
        bars = self._candles(candles)
        return bars[0].close_time_ms + self._latency

    def _requirements(self, requirements):
        if type(requirements) is not dict or len(requirements) > 100:
            raise ValidationError("native replay requirements must be a bounded dictionary")
        result = {}
        for key, channels in requirements.items():
            _instrument(key)
            if type(channels) not in {list, tuple} or tuple(channels) != ("trade_flow",):
                raise ValidationError(
                    "native replay supports only declared trade_flow requirements"
                )
            if key not in self._declared:
                raise ValidationError(
                    "required native trade-flow source is absent from the dataset"
                )
            result[key] = ("trade_flow",)
        return result

    def validate_requirements(self, requirements):
        if self.version == 2:
            self._v2.validate_requirements(requirements)
            return
        self._requirements(requirements)

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        """Readonly AuxiliaryStore.get seam used only by MarketFrameBuilder."""
        if self.version == 2:
            return self._v2.get(instrument, channel, source, open_time_ms, as_of_ms=as_of_ms)
        if (
            type(instrument) is not Instrument
            or instrument.key not in self._declared
            or channel != "trade_flow"
            or source != instrument.key + ":aggTrade"
            or type(open_time_ms) is not int
            or type(as_of_ms) is not int
            or open_time_ms % 60000
            or not 0 <= open_time_ms < as_of_ms <= MAX_TIME
            or as_of_ms != open_time_ms + 60000 + self._latency
        ):
            raise ValidationError("foreign or invalid native replay source request")
        selected = self._entries.get((instrument.key, open_time_ms, channel, source))
        if selected is None or selected.observation.available_at_ms > as_of_ms:
            return None
        return replace(
            selected,
            instrument=replace(selected.instrument),
            observation=replace(selected.observation),
        )

    def latest_sample(
        self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms=330000
    ):
        """Readonly native point port, available only under the bound V2 policy."""
        if self.version != 2:
            raise ValidationError("version1 replay does not support native metric samples")
        return self._v2.latest_sample(
            instrument,
            channel,
            source,
            event_before_ms=event_before_ms,
            as_of_ms=as_of_ms,
            max_age_ms=max_age_ms,
        )

    def prepare(self, candles, *, as_of_ms, requirements):
        bars = self._candles(candles)
        if type(as_of_ms) is not int or as_of_ms != bars[0].close_time_ms + self._latency:
            raise ValidationError("native replay cutoff differs from its declared latency")
        if self.version == 2:
            return self._v2.prepare(bars, as_of_ms, requirements)
        needed = self._requirements(requirements)
        by_key = {bar.instrument.key: bar for bar in bars}
        if not needed.keys() <= by_key.keys():
            raise ValidationError("native replay requirements reference a foreign candle")
        return tuple(
            self._builder.prepare(by_key[key], as_of_ms=as_of_ms, channels=needed[key])
            for key in sorted(needed)
        )


class _RetainedNativeReplay:
    """Version2 readonly revision index; no clock substitution, IO or mutable policy."""

    _SOURCES = {
        "trade_flow": "aggTrade",
        "depth": "binance_depth_ws_v1",
        "open_interest": "open_interest_history",
        "long_short_ratio": "long_short_ratio",
    }

    def __init__(self, manifest, observations, latency):
        self.header, records = freeze_selected_observation_snapshot(manifest, observations)
        self.latency = latency
        self.ranges = {
            (row["instrument"], row["channel"], row["source"]): (row["start_ms"], row["end_ms"])
            for row in self.header["selection"]["ranges"]
        }
        entries, samples = {}, {}
        for pin in records:
            observation = pin.observation
            identity = pin.instrument.key, observation.channel, observation.source
            entries.setdefault((*identity, observation.window_start_ms), []).append(pin)
            if observation.channel in {"open_interest", "long_short_ratio"}:
                samples.setdefault(identity, []).append(pin)
        self.entries = MappingProxyType({key: tuple(value) for key, value in entries.items()})
        self.samples = MappingProxyType({key: tuple(value) for key, value in samples.items()})
        self.binding = None
        self.builders = {}

    @staticmethod
    def _channels(requirements):
        if type(requirements) is not dict or len(requirements) > 100:
            raise ValidationError("native V2 replay needs bounded graph requirements")
        result = {}
        for key, channels in requirements.items():
            _instrument(key)
            if (
                type(channels) not in {list, tuple}
                or not 1 <= len(channels) <= 3
                or any(
                    type(item) is not str or item not in {"trade_flow", "depth", "metric"}
                    for item in channels
                )
                or len(set(channels)) != len(channels)
            ):
                raise ValidationError("native V2 graph channels are unsupported")
            result[key] = tuple(sorted(channels))
        return result

    def _configuration(self, requirements, metric_policies):
        required = self._channels(requirements)
        metric_keys = {key for key, channels in required.items() if "metric" in channels}
        if type(metric_policies) is not dict or metric_policies.keys() != metric_keys:
            raise ValidationError("native V2 replay requires exact actual graph metric policies")
        policies = {
            key: MetricInputPolicy.from_document(value) for key, value in metric_policies.items()
        }
        return required, policies

    def bind_configuration(self, requirements, metric_policies, start_ms, end_ms):
        required, policies = self._configuration(requirements, metric_policies)
        if (
            type(start_ms) is not int
            or type(end_ms) is not int
            or not 0 <= start_ms < end_ms <= MAX_TIME
            or start_ms % 60000
            or end_ms % 60000
        ):
            raise ValidationError("native V2 replay needs its exact aligned candle range")
        candle_ranges = {
            row["instrument"]: row for row in self.header["selection"]["candle_ranges"]
        }
        for key, channels in required.items():
            candle_range = candle_ranges.get(key)
            if (
                candle_range is None
                or not candle_range["start_ms"] <= start_ms < end_ms <= candle_range["end_ms"]
            ):
                raise ValidationError("native V2 replay exceeds its retained candle range")
            for channel in channels:
                native_channels = (
                    ("open_interest", "long_short_ratio") if channel == "metric" else (channel,)
                )
                earliest = start_ms
                if channel == "metric":
                    age = policies[key].maximum_metric_age_ms
                    coverage = self.header["selection"]["metric_lookback_ms"]
                    if type(coverage) is not int or age > coverage:
                        raise ValidationError(
                            "actual MST horizon exceeds declared native seed coverage"
                        )
                    earliest = max(0, start_ms - age) // 60000 * 60000
                for native in native_channels:
                    bounds = self.ranges.get((key, native, key + ":" + self._SOURCES[native]))
                    if bounds is None or not bounds[0] <= earliest < end_ms <= bounds[1]:
                        raise ValidationError(
                            "native V2 source range does not cover actual graph policy"
                        )
        binding = {
            "required_channels": {key: list(value) for key, value in sorted(required.items())},
            "metric_policies": {key: value.document() for key, value in sorted(policies.items())},
            "start_ms": start_ms,
            "end_ms": end_ms,
        }
        raw = NativeObservationReplay._json(binding)
        if self.binding is not None:
            if self.binding != raw:
                raise ValidationError("native V2 replay graph policy changed after binding")
            return
        lineage = {
            "schema_id": "omega.native-margin-observation-replay.v2",
            "scope": "native_observation_availability",
            "manifest_sha256": sha256(NativeObservationReplay._json(self.header)).hexdigest(),
            "candle_manifest_sha256": self.header["candles"]["sha256"],
            "candle_rows_sha256": self.header["candles"]["rows_sha256"],
            "observations_sha256": self.header["observations"]["sha256"],
            "observations_count": self.header["observations"]["count"],
            "selection_policy": "retained_native_event_revision_available_cutoff_v2",
            "selection": self.header["selection"],
            "decision_latency_ms": self.latency,
            "graph_configuration": binding,
        }
        self._lineage = NativeObservationReplay._json(lineage)
        self._destination = sha256(self._lineage).hexdigest()
        self.binding, self.required, self.policies = raw, required, policies
        self.start_ms, self.end_ms = start_ms, end_ms
        self.builders = {
            key: MarketFrameBuilder(
                self,
                maximum_metric_age_ms=policies[key].maximum_metric_age_ms if key in policies else 0,
            )
            for key in required
        }

    def _bound(self):
        if self.binding is None:
            raise ValidationError("native V2 graph configuration must be bound before lineage")

    @property
    def lineage(self):
        self._bound()
        return json.loads(self._lineage)

    @property
    def destination_id(self):
        self._bound()
        return self._destination

    def validate_configuration(self, requirements, metric_policies):
        self._bound()
        required, policies = self._configuration(requirements, metric_policies)
        if required != self.required or policies != self.policies:
            raise ValidationError("financial session graph differs from frozen native V2 policies")

    def validate_requirements(self, requirements):
        self._bound()
        if self._channels(requirements) != self.required:
            raise ValidationError("native V2 graph requirements changed after binding")

    @staticmethod
    def _copy(pin):
        return (
            None
            if pin is None
            else replace(
                pin, instrument=replace(pin.instrument), observation=replace(pin.observation)
            )
        )

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        self._bound()
        if (
            type(instrument) is not Instrument
            or type(channel) is not str
            or channel not in {"trade_flow", "depth"}
            or instrument.key not in self.required
            or channel not in self.required[instrument.key]
            or source != instrument.key + ":" + self._SOURCES[channel]
            or type(open_time_ms) is not int
            or open_time_ms % 60000
            or not self.start_ms <= open_time_ms < self.end_ms
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= MAX_TIME
            or as_of_ms != open_time_ms + 60000 + self.latency
        ):
            raise ValidationError("foreign native V2 exact-minute source request")
        candidates = self.entries.get((instrument.key, channel, source, open_time_ms), ())
        return self._copy(
            max(
                (pin for pin in candidates if pin.observation.available_at_ms <= as_of_ms),
                key=lambda pin: pin.revision,
                default=None,
            )
        )

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        self._bound()
        policy = self.policies.get(instrument.key) if type(instrument) is Instrument else None
        if (
            policy is None
            or type(channel) is not str
            or channel not in {"open_interest", "long_short_ratio"}
            or source != instrument.key + ":" + self._SOURCES[channel]
            or type(event_before_ms) is not int
            or event_before_ms % 60000
            or not self.start_ms < event_before_ms <= self.end_ms
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= MAX_TIME
            or as_of_ms != event_before_ms + self.latency
            or type(max_age_ms) is not int
            or not 1 <= max_age_ms <= 86400000
            or max_age_ms != policy.maximum_metric_age_ms
        ):
            raise ValidationError("foreign native V2 metric sample request")
        candidates = self.samples.get((instrument.key, channel, source), ())
        return self._copy(
            max(
                (
                    pin
                    for pin in candidates
                    if max(0, event_before_ms - max_age_ms)
                    <= pin.observation.event_time_ms
                    < event_before_ms
                    and pin.observation.available_at_ms <= as_of_ms
                ),
                key=lambda pin: (pin.observation.event_time_ms, pin.revision),
                default=None,
            )
        )

    def prepare(self, bars, as_of_ms, requirements):
        self.validate_requirements(requirements)
        by_key = {bar.instrument.key: bar for bar in bars}
        if not self.required.keys() <= by_key.keys():
            raise ValidationError("native V2 requirements reference a foreign candle")
        prepared = []
        for key in sorted(self.required):
            bar = by_key[key]
            if not self.start_ms <= bar.open_time_ms < self.end_ms:
                raise ValidationError("native V2 candle exceeds its frozen range")
            value = self.builders[key].prepare(bar, as_of_ms=as_of_ms, channels=self.required[key])
            if key in self.policies:
                observation = next(item for item in value.frame.inputs if item.channel == "metric")
                pins = tuple(
                    pin
                    for pin in value.pins
                    if pin.observation.channel in {"open_interest", "long_short_ratio"}
                )
                validate_metric(
                    bar, observation, as_of_ms=as_of_ms, policy=self.policies[key], pins=pins
                )
            prepared.append(value)
        return tuple(prepared)
