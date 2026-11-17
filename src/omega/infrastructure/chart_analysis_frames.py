"""Chart-only bounded native revision batches supplied to the existing frame builder."""

from bisect import bisect_left
from hashlib import sha256

from omega.application.market_frame_builder import DEPTH_SOURCE_SUFFIX, MarketFrameBuilder
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Market
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.ports.auxiliary import AuxiliaryReadRange, StoredAuxiliary

MINUTE = 60000
MAX_RECORDS = 10000
MAX_BYTES = 32 * 1024 * 1024
_SOURCES = {
    "trade_flow": "aggTrade",
    "depth": DEPTH_SOURCE_SUFFIX,
    "open_interest": "open_interest_history",
    "long_short_ratio": "long_short_ratio",
}


class _RevisionIndex:
    """One immutable chunk, selecting exactly the existing native point-port policy."""

    def __init__(self, rows, ranges, as_of_ms, check_stop):
        if type(rows) is not tuple or len(rows) > MAX_RECORDS:
            raise ProtocolError("chart native batch exceeds its record capacity")
        self.as_of_ms, self.check_stop = as_of_ms, check_stop
        entries, samples, seen, size = {}, {}, {}, 0
        for pin in rows:
            check_stop()
            if not isinstance(pin, StoredAuxiliary):
                raise ProtocolError("chart native batch requires typed revision pins")
            observation = pin.observation
            matches = [
                row
                for row in ranges
                if row.instrument == pin.instrument
                and row.channel == observation.channel
                and row.source == observation.source
                and row.start_ms <= observation.window_start_ms < row.end_ms
            ]
            identity = (
                pin.instrument,
                observation.channel,
                observation.source,
                observation.window_start_ms,
            )
            native_key = (*identity, pin.revision)
            payload = encode_observation(pin.instrument, observation)
            size += len(payload)
            if size > MAX_BYTES:
                raise StorageError("chart native batch exceeds its 32MiB capacity")
            if (
                len(matches) != 1
                or native_key in seen
                or observation.available_at_ms > as_of_ms
                or sha256(payload).hexdigest() != pin.checksum
                or observation.channel in {"open_interest", "long_short_ratio"}
                and observation.window_start_ms != observation.event_time_ms // MINUTE * MINUTE
            ):
                raise ProtocolError("chart native revision identity, checksum or cutoff conflicts")
            seen[native_key] = pin
            prior = entries.get(identity)
            if prior is None or pin.revision > prior.revision:
                entries[identity] = pin
            if observation.channel in {"open_interest", "long_short_ratio"}:
                samples.setdefault(identity[:3], []).append(pin)
        self.entries, self.samples = entries, {}
        for native_key, pin in seen.items():
            check_stop()
            predecessor = seen.get((*native_key[:-1], pin.revision - 1))
            if pin.revision > 1 and (
                predecessor is None
                or predecessor.checksum != pin.previous_checksum
                or predecessor.observation.available_at_ms >= pin.observation.available_at_ms
            ):
                raise ProtocolError("chart native batch has a conflicting predecessor edge")
        for identity, pins in samples.items():
            check_stop()
            ordered = tuple(
                sorted(pins, key=lambda pin: (pin.observation.event_time_ms, pin.revision))
            )
            self.samples[identity] = (
                tuple((pin.observation.event_time_ms, pin.revision) for pin in ordered),
                ordered,
            )

    def _cutoff(self, as_of_ms):
        self.check_stop()
        if as_of_ms != self.as_of_ms:
            raise ValidationError("chart chunk selection cutoff changed")

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        self._cutoff(as_of_ms)
        return self.entries.get((instrument, channel, source, open_time_ms))

    def latest_sample(self, instrument, channel, source, *, event_before_ms, as_of_ms, max_age_ms):
        self._cutoff(as_of_ms)
        keys, pins = self.samples.get((instrument, channel, source), ((), ()))
        index = bisect_left(keys, (event_before_ms, 0)) - 1
        if index < 0 or pins[index].observation.event_time_ms < max(
            0, event_before_ms - max_age_ms
        ):
            return None
        selected = pins[index]
        if selected.observation.quality != "observed":
            raise StorageError("stored auxiliary native sample failed integrity checks")
        return selected


class ChartAnalysisFrameSource:
    """Own a source's public candle/batch ports; never use per-minute DB queries."""

    def __init__(self, source, *, check_stop):
        if not callable(check_stop):
            raise ValidationError("chart analysis needs a cooperative stop check")
        self.source, self.check_stop = source, check_stop

    def read_frames(
        self,
        instrument,
        start_ms,
        end_ms,
        *,
        as_of_ms,
        channels,
        maximum_metric_age_ms,
        retrospective_until_ms,
        limit=512,
    ):
        self.check_stop()
        if (
            type(limit) is not int
            or not 1 <= limit <= 512
            or type(start_ms) is not int
            or type(end_ms) is not int
            or not 0 <= start_ms < end_ms <= as_of_ms
            or start_ms % MINUTE
            or end_ms % MINUTE
            or (end_ms - start_ms) // MINUTE > limit
            or type(channels) is not tuple
            or len(set(channels)) != len(channels)
            or any(channel not in {"depth", "metric", "trade_flow"} for channel in channels)
            or type(maximum_metric_age_ms) is not int
            or not 0 <= maximum_metric_age_ms <= 86400000
            or type(retrospective_until_ms) is not int
            or not end_ms <= retrospective_until_ms <= as_of_ms
        ):
            raise ValidationError("invalid bounded chart native frame chunk")
        if not callable(getattr(self.source, "read_revisions", None)):
            raise ProtocolError("chart analysis source lacks bounded native revision reads")
        candles = self.source.read(
            (instrument.key,), start_ms, end_ms, limit=limit, as_of_ms=as_of_ms
        )
        self.check_stop()
        if type(candles) is not tuple or len(candles) > limit:
            raise ProtocolError("chart source exceeds its bounded candle chunk")
        previous = None
        for candle in candles:
            self.check_stop()
            if (
                not isinstance(candle, Candle)
                or candle.instrument != instrument
                or not start_ms <= candle.open_time_ms < end_ms
                or candle.close_time_ms > as_of_ms
                or previous is not None
                and candle.open_time_ms <= previous
            ):
                raise ProtocolError("chart source returned conflicting candle identity or order")
            previous = candle.open_time_ms
        if not candles:
            return ()
        ranges = []
        for channel in sorted(channels):
            if channel == "metric":
                if instrument.market == Market.SPOT or maximum_metric_age_ms == 0:
                    continue
                earliest = max(0, start_ms - maximum_metric_age_ms) // MINUTE * MINUTE
                native_channels = ("open_interest", "long_short_ratio")
            else:
                earliest, native_channels = start_ms, (channel,)
            ranges.extend(
                AuxiliaryReadRange(
                    instrument, native, instrument.key + ":" + _SOURCES[native], earliest, end_ms
                )
                for native in native_channels
            )
        self.check_stop()
        rows = (
            self.source.read_revisions(
                tuple(ranges), as_of_ms=as_of_ms, limit=MAX_RECORDS, check_stop=self.check_stop
            )
            if ranges
            else ()
        )
        self.check_stop()
        index = _RevisionIndex(rows, ranges, as_of_ms, self.check_stop)
        builder = MarketFrameBuilder(index, maximum_metric_age_ms=maximum_metric_age_ms)
        result = []
        for candle in candles:
            self.check_stop()
            result.append(
                builder.prepare(candle, as_of_ms=as_of_ms, channels=channels, retrospective=True)
            )
        self.check_stop()
        return tuple(result)

    def close(self):
        self.source.close()
