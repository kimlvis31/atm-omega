"""Binance history collection and normalization owned by the market data process."""

from decimal import Decimal
from threading import local

from omega.application.trade_flow import trade_flow_values
from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Market
from omega.infrastructure.binance.auxiliary_client import BinanceAuxiliaryClient
from omega.infrastructure.binance.budget import RequestBudget, Window
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.market import BinanceMarketData
from omega.infrastructure.binance.shared_budget import SharedRequestBudget
from omega.infrastructure.binance.transport import UrllibTransport
from omega.infrastructure.binance.vision import DAY, VisionArchive
from omega.infrastructure.binance.vision_market import VisionMarketData
from omega.infrastructure.raw_archive import FileRawArchive
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.collector_aux_history import HistoricalAuxiliaryFetch
from omega.runtime.collector_history import CHANNELS, MINUTE, clock_ms, discover, log
from omega.runtime.market_ipc import MarketSocket
from omega.runtime.market_wire import (
    auxiliary_document,
    auxiliary_from_document,
    instrument_key,
    minute_document,
    minute_from_document,
)


class MarketHistoryFetch:
    def __init__(self, root, inventory, stop):
        self.root, self.inventory, self.stop = root, set(inventory), stop
        self.budget = SharedRequestBudget(root / "binance-public-budget.json")
        self.repair_budget = RequestBudget((Window("repair", 120, 60),))
        self.raw = FileRawArchive(root / "history-repair-raw")
        self.workers = local()
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
            vision = VisionArchive(self.root / "vision-history", stop_requested=self.stop.is_set)
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

    def _repair(self, instrument, opening, client):
        end, pending = opening + MINUTE, []
        archive_ids = []
        for _ in range(16):
            parameters = (
                {"from_id": pending[-1].aggregate_id + 1}
                if pending
                else {
                    "start_ms": opening,
                    "end_ms": end,
                }
            )
            self.repair_budget.reserve({"repair": 4 if instrument.market == Market.SPOT else 20})
            batch = client.agg_trades(instrument, **parameters, limit=1000)
            receipt = self.raw.put(
                PublicRawResponse(
                    instrument,
                    "agg_trades",
                    batch.request_parameters,
                    batch.received_at_ms,
                    batch.body,
                    batch.headers,
                )
            )
            archive_ids.append(receipt.archive_id)
            pending.extend(
                item.as_flow_event() for item in batch.events if item.trade_time_ms < end
            )
            if len(batch.events) < 1000 or batch.events[-1].trade_time_ms >= end:
                break
        else:
            return None
        if not pending or any(
            left.aggregate_id + 1 != right.aggregate_id
            for left, right in zip(pending, pending[1:], strict=False)
        ):
            return None
        values = dict(trade_flow_values(pending))
        return (
            {
                name: str(value) if isinstance(value, Decimal) else value
                for name, value in values.items()
            },
            len(pending),
            archive_ids,
        )

    def dispatch(self, operation, args):
        requested = instrument_key(args["instrument"])
        if requested not in self.inventory or self.stop.is_set():
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
            return self._repair(requested, start, auxiliary[requested.market])

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
            if len(volumes) > 1440:
                raise ValidationError("trade reference window overflow")
            rows, _ = fetch.trade_day(requested, start, end, volumes)
        elif channel == "depth":
            rows, _ = fetch.depth_day(requested, start, end)
        else:
            rows, _ = fetch.metric_day(requested, start, end, archive)
        unsaved = [row for row in rows if row.window_start_ms not in known]
        if channel == "metrics":
            coverage = [
                {
                    row.window_start_ms
                    for row in unsaved
                    if row.channel == name and row.quality == "observed"
                }
                for name in ("open_interest", "long_short_ratio")
            ]
            verified = len(coverage[0] & coverage[1])
        else:
            verified = sum(row.quality == "observed" for row in unsaved)
        return archive, from_rest, [auxiliary_document(requested, row) for row in unsaved], verified


class RemoteHistoryFetch:
    def __init__(self, path):
        self.connection = MarketSocket(path, response_timeout=600)

    def origin(self, instrument, channel):
        return self.connection.call("origin", instrument=instrument.key, channel=channel)

    def klines(self, instrument, start, end, windows):
        if not windows:
            return (), None
        documents, archive = self.connection.call(
            "klines", instrument=instrument.key, start=start, end=end, windows=windows
        )
        rows = tuple(minute_from_document(row) for row in documents)
        allowed = {stamp for low, high in windows for stamp in range(low, high, MINUTE)}
        if any(
            row.candle.instrument != instrument or row.candle.open_time_ms not in allowed
            for row in rows
        ):
            raise ProtocolError("candle reply is outside the requested gaps")
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
        rows = []
        for value in documents:
            owner, observation = auxiliary_from_document(value)
            if owner != instrument or not start <= observation.window_start_ms < end:
                raise ProtocolError("market response is outside the requested window")
            rows.append(observation)
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
