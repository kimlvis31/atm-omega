"""Binance history collection and normalization owned by the market data process."""

from decimal import Decimal
from threading import BoundedSemaphore, Event, local

from omega.domain.errors import ProtocolError, TransportError, ValidationError
from omega.domain.models import Market
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.binance.vision import DAY, VisionArchive
from omega.infrastructure.binance.vision_market import VisionMarketData
from omega.infrastructure.postgres.batching import COMMIT_BATCH_ROWS
from omega.infrastructure.raw_archive import FileRawArchive
from omega.runtime.collector_aux_history import HistoricalAuxiliaryFetch
from omega.runtime.collector_history import CHANNELS, MINUTE, clock_ms, discover, log
from omega.runtime.market_ipc import BackfillPaused, MarketSocket
from omega.runtime.market_trade_repair import TradeRepair
from omega.runtime.market_wire import (
    auxiliary_document,
    auxiliary_from_document,
    instrument_key,
    minute_document,
    minute_from_document,
)

AUXILIARY_KINDS = {
    "agg_trade": {"trade_flow"},
    "depth": {"depth"},
    "metrics": {"open_interest", "long_short_ratio"},
}


def _verified_windows(channel, rows):
    observed = {name: set() for name in AUXILIARY_KINDS[channel]}
    for row in rows:
        if row.quality == "observed":
            observed[row.channel].add(row.window_start_ms)
    return len(set.intersection(*observed.values()))


class MarketHistoryFetch:
    def __init__(self, root, inventory, stop, *, workers=12):
        self.root, self.inventory, self.stop = root, set(inventory), stop
        self.budget = SharedRequestBudget(root / "binance-public-budget.json")
        self.repair = TradeRepair(
            root,
            FileRawArchive(root / "history-repair-raw"),
            RequestBudget((Window("repair", 120, 60),)),
            clock_ms=clock_ms,
            stop_requested=stop.is_set,
        )
        self.workers = local()
        self.downloads = BoundedSemaphore(workers)
        self.backfill_paused = Event()
        self.admission = VisionArchive(root / "vision-history")._budget

    def _worker(self):
        if not hasattr(self.workers, "state"):
            rest, auxiliary = {}, {}
            for market in {instrument.market for instrument in self.inventory}:
                from omega.runtime.collect_cli import _CollectionTransport

                client = BinanceClient(
                    "https://api.binance.com"
                    if market == Market.SPOT
                    else "https://fapi.binance.com",
                    _CollectionTransport(UrllibTransport(), market),
                    budget=self.budget,
                )
                rest[market] = BinanceMarketData(market, client)
                auxiliary[market] = BinanceAuxiliaryClient(market, client)
            vision = VisionArchive(
                self.root / "vision-history",
                stop_requested=self.stop.is_set,
                shared_admission=self.admission,
            )
            self.workers.state = (
                rest,
                auxiliary,
                vision,
                HistoricalAuxiliaryFetch(vision, auxiliary, clock_ms=clock_ms, stop=self.stop),
            )
        return self.workers.state

    @staticmethod
    def _unaligned_bar(instrument, opening, closing, source_row):
        log(
            "kline_archive_noncanonical",
            instrument=instrument.key,
            open_ms=opening,
            close_ms=closing,
            source_row=source_row,
        )

    def dispatch(self, operation, args):
        if operation == "backfill_pause":
            paused = args["paused"]
            if type(paused) is not bool:
                raise ValidationError("invalid market backfill pause state")
            if paused:
                self.backfill_paused.set()
            else:
                self.backfill_paused.clear()
            return paused
        if operation == "repair_trade":
            return self._fetch(operation, args)
        with self.downloads:
            if operation in {"klines", "aux_day"} and self.backfill_paused.is_set():
                raise BackfillPaused()
            return self._fetch(operation, args)

    def _fetch(self, operation, args):
        if self.stop.is_set():
            raise TransportError("market collector is stopping")
        requested = instrument_key(args["instrument"])
        if requested not in self.inventory:
            raise ValidationError("market request is outside the active inventory")
        rest, auxiliary, vision, fetch = self._worker()
        if operation == "origin":
            if args["channel"] not in CHANNELS:
                raise ValidationError("invalid market channel")
            return discover(requested, args["channel"], rest[requested.market], vision)

        start, end = args["start"], args["end"]
        if (
            type(start) is not int
            or type(end) is not int
            or not 0 <= start < end <= clock_ms()
            or end - start > DAY
        ):
            raise ValidationError("market fetch requires a bounded closed window")
        if operation == "klines":
            provider = VisionMarketData(
                rest[requested.market],
                vision,
                on_unaligned_bar=self._unaligned_bar,
            )
            windows = args["windows"]
            previous = start
            for low, high in windows:
                if (
                    type(low) is not int
                    or type(high) is not int
                    or not previous <= low < high <= end
                    or low % MINUTE
                    or high % MINUTE
                ):
                    raise ValidationError("invalid requested candle intervals")
                previous = high
            archive = bool(vision.files(requested, "kline", start, end))
            if archive:
                # A daily ZIP is atomic; send only the requested gaps to the data manager.
                missing = {stamp for low, high in windows for stamp in range(low, high, MINUTE)}
                rows = provider.closed_observations(requested, start, end, now_ms=clock_ms())
                return [
                    minute_document(row) for row in rows if row.candle.open_time_ms in missing
                ], archive
            rows = []
            for low, high in windows:
                values = rest[requested.market].closed_minutes(
                    requested, low, high, now_ms=clock_ms()
                )
                from omega.ports.collection import MinuteObservation

                rows.extend(
                    minute_document(MinuteObservation(row, "rest", clock_ms())) for row in values
                )
            return rows, archive
        if operation == "repair_trade":
            if end - start != MINUTE or start % MINUTE:
                raise ValidationError("invalid trade repair window")
            return self.repair.collect(requested, start, auxiliary[requested.market])

        if operation != "aux_day":
            raise ProtocolError("unknown market fetch operation")
        channel = args["channel"]
        step = 300000 if channel == "metrics" else MINUTE
        if channel not in {"agg_trade", "depth", "metrics"} or type(args["allow_rest"]) is not bool:
            raise ValidationError("invalid auxiliary fetch channel")
        known = set(args["known"])
        if not known.issubset(range(start, end, step)):
            raise ValidationError("invalid known auxiliary windows")
        archive = bool(vision.files(requested, channel, start, end))
        from_rest = not archive and channel == "metrics" and args["allow_rest"]
        if not archive and not from_rest:
            return archive, from_rest, [], 0
        if channel == "agg_trade":
            volumes = {}
            for stamp, volume in args["volumes"]:
                if type(stamp) is not int or stamp % MINUTE or not start <= stamp < end:
                    raise ValidationError("invalid trade reference minute")
                amount = Decimal(volume)
                if not amount.is_finite() or amount < 0 or stamp in volumes:
                    raise ValidationError("invalid trade reference volume")
                volumes[stamp] = amount
            if len(volumes) > COMMIT_BATCH_ROWS:
                raise ValidationError("trade reference window overflow")
            rows, _ = fetch.trade_day(requested, start, end, volumes)
        elif channel == "depth":
            rows, _ = fetch.depth_day(requested, start, end)
        else:
            rows, _ = fetch.metric_day(requested, start, end, archive)
        unsaved = [row for row in rows if row.window_start_ms not in known]
        if len(unsaved) > COMMIT_BATCH_ROWS:
            raise ProtocolError("backfill reply exceeds the reserved rows")
        verified = _verified_windows(channel, unsaved)
        return archive, from_rest, [auxiliary_document(requested, row) for row in unsaved], verified


class RemoteHistoryFetch:
    def __init__(self, path):
        self.connection = MarketSocket(path, response_timeout=600)
        self.control = MarketSocket(path, response_timeout=3)

    def set_backfill_paused(self, paused):
        if self.control.call("backfill_pause", paused=paused) is not paused:
            raise ProtocolError("market backfill pause acknowledgement disagrees")

    def origin(self, instrument, channel):
        return self.connection.call("origin", instrument=instrument.key, channel=channel)

    def klines(self, instrument, start, end, windows):
        if not windows:
            return (), None
        documents, archive = self.connection.call(
            "klines", instrument=instrument.key, start=start, end=end, windows=windows
        )
        if (
            type(archive) is not bool
            or not isinstance(documents, list)
            or len(documents) > COMMIT_BATCH_ROWS
        ):
            raise ProtocolError("invalid backfill bar reply")
        rows = tuple(minute_from_document(row) for row in documents)
        allowed = {stamp for low, high in windows for stamp in range(low, high, MINUTE)}
        if any(
            row.candle.instrument != instrument or row.candle.open_time_ms not in allowed
            for row in rows
        ):
            raise ProtocolError("candle reply is outside the requested gaps")
        if any(
            left.candle.open_time_ms >= right.candle.open_time_ms
            for left, right in zip(rows, rows[1:], strict=False)
        ):
            raise ProtocolError("backfill bar reply must have unique ordered times")
        return rows, archive

    def day(self, instrument, channel, start, end, allow_rest, volumes, known):
        archive, from_rest, documents, verified = self.connection.call(
            "aux_day",
            instrument=instrument.key,
            channel=channel,
            start=start,
            end=end,
            volumes=[(key, str(value)) for key, value in (volumes or {}).items()],
            known=sorted(known),
            allow_rest=allow_rest,
        )
        if (
            type(archive) is not bool
            or type(from_rest) is not bool
            or (from_rest and (archive or channel != "metrics" or not allow_rest))
            or not isinstance(documents, list)
            or len(documents) > COMMIT_BATCH_ROWS
            or (documents and not (archive or from_rest))
            or type(verified) is not int
        ):
            raise ProtocolError("invalid backfill auxiliary reply")
        rows = []
        for value in documents:
            owner, observation = auxiliary_from_document(value)
            opening = observation.window_start_ms
            interval = 300_000 if channel == "metrics" else MINUTE
            if (
                owner != instrument
                or observation.channel not in AUXILIARY_KINDS[channel]
                or not start <= opening < end
                or opening % interval
                or opening in known
            ):
                raise ProtocolError("market response is outside the requested window")
            rows.append(observation)
        if verified != _verified_windows(channel, rows):
            raise ProtocolError("backfill auxiliary coverage disagrees with the received rows")
        return archive, from_rest, rows, verified

    def repair_trade(self, instrument, opening):
        result = self.connection.call(
            "repair_trade", instrument=instrument.key, start=opening, end=opening + MINUTE
        )
        if result is None:
            return None
        values, events, archive_ids = result
        return (
            {
                key: Decimal(value) if isinstance(value, str) else value
                for key, value in values.items()
            },
            events,
            archive_ids,
        )
