"""Resumable REST repair using only previously verified raw response receipts."""

import json
import re
from decimal import Decimal
from hashlib import sha256
from threading import Lock

from omega.application.trade_flow import trade_flow_values
from omega.domain.errors import ProtocolError, StorageError, TransportError
from omega.domain.models import Market
from omega.infrastructure.binance.auxiliary import _unique_object, agg_trade
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.settings import atomic_json

MINUTE = 60_000
PAGE_SIZE = 1000
MAX_PAGES = 16


class TradeRepair:
    def __init__(self, root, archive, budget, *, clock_ms, stop_requested=lambda: False):
        self.root = root / "history-repair-pending"
        self.archive, self.budget, self.clock_ms = archive, budget, clock_ms
        self.stop_requested = stop_requested
        self.lock = Lock()
        self.next_cleanup = 0

    def _path(self, instrument, opening):
        key = sha256(instrument.key.encode()).hexdigest()
        return self.root / f"{opening}-{key}.json"

    def _prune(self):
        now = self.clock_ms()
        if now < self.next_cleanup:
            return
        for file in self.root.glob("*.json"):
            name = re.fullmatch(r"([0-9]{1,15})-[a-f0-9]{64}\.json", file.name)
            if name and int(name[1]) < now - 86_400_000:
                file.unlink(missing_ok=True)
        self.next_cleanup = now + 60 * MINUTE

    @staticmethod
    def _params(opening, pending):
        if pending:
            return {"from_id": pending[-1].aggregate_id + 1}
        return {"start_ms": opening, "end_ms": opening + MINUTE}

    @staticmethod
    def _wire_params(instrument, params):
        fields = {"symbol": instrument.symbol, "limit": str(PAGE_SIZE)}
        if "from_id" in params:
            fields["fromId"] = str(params["from_id"])
        else:
            fields["startTime"] = str(params["start_ms"])
            fields["endTime"] = str(params["end_ms"] - 1)
        return tuple(sorted(fields.items()))

    @staticmethod
    def _append(events, pending, opening):
        end = opening + MINUTE
        prior = pending[-1] if pending else None
        if len(events) > PAGE_SIZE:
            raise ProtocolError("trade repair page exceeds its limit")
        for event in events:
            if (
                event.trade_time_ms < opening
                or prior is not None
                and (
                    event.aggregate_id != prior.aggregate_id + 1
                    or event.trade_time_ms < prior.trade_time_ms
                )
            ):
                raise ProtocolError("trade repair page is not continuous")
            if event.trade_time_ms >= end:
                return True
            pending.append(event.as_flow_event())
            prior = event
        return len(events) < PAGE_SIZE

    def _restore(self, path, instrument, opening):
        pending = []
        try:
            with path.open("rb") as stream:
                blob = stream.read(4097)
            if len(blob) > 4096:
                raise ValueError
            manifest = json.loads(blob, object_pairs_hook=_unique_object)
            ids = manifest["raw_responses"]
            if (
                manifest["instrument"] != instrument.key
                or manifest["open_ms"] != opening
                or not isinstance(ids, list)
                or not 1 <= len(ids) <= MAX_PAGES
                or len(set(ids)) != len(ids)
            ):
                raise ValueError
            for archive_id in ids:
                record = self.archive.get(archive_id).record
                params = self._wire_params(instrument, self._params(opening, pending))
                if (record.instrument, record.channel, record.request_parameters) != (
                    instrument,
                    "agg_trades",
                    params,
                ):
                    raise ValueError
                rows = json.loads(
                    record.body, parse_float=Decimal, object_pairs_hook=_unique_object
                )
                if not isinstance(rows, list) or len(rows) != PAGE_SIZE:
                    raise ValueError
                events = tuple(
                    agg_trade(instrument, row, available_at_ms=record.received_at_ms)
                    for row in rows
                )
                if self._append(events, pending, opening):
                    raise ValueError
        except FileNotFoundError:
            return [], []
        except (KeyError, TypeError, ValueError, RecursionError, ProtocolError) as error:
            raise StorageError("trade repair checkpoint failed integrity checks") from error
        return pending, ids

    def collect(self, instrument, opening, client):
        with self.lock:
            self._prune()
            path = self._path(instrument, opening)
            pending, ids = self._restore(path, instrument, opening)
            for _ in range(len(ids), MAX_PAGES):
                if self.stop_requested():
                    raise TransportError("trade repair is stopping")
                params = self._params(opening, pending)
                self.budget.reserve({"repair": 4 if instrument.market == Market.SPOT else 20})
                page = client.agg_trades(instrument, **params, limit=PAGE_SIZE)
                if (page.instrument, page.channel, page.request_parameters) != (
                    instrument,
                    "agg_trades",
                    self._wire_params(instrument, params),
                ):
                    raise ProtocolError("trade repair page identity disagrees")
                receipt = self.archive.put(
                    PublicRawResponse(
                        instrument,
                        "agg_trades",
                        page.request_parameters,
                        page.received_at_ms,
                        page.body,
                        page.headers,
                    )
                )
                complete = self._append(page.events, pending, opening)
                ids.append(receipt.archive_id)
                if complete:
                    values = trade_flow_values(pending) if pending else ()
                    path.unlink(missing_ok=True)
                    if not values:
                        return None
                    return (
                        {
                            key: str(value) if isinstance(value, Decimal) else value
                            for key, value in values
                        },
                        len(pending),
                        ids,
                    )
                atomic_json(
                    path,
                    {
                        "instrument": instrument.key,
                        "open_ms": opening,
                        "raw_responses": ids,
                    },
                )
            return None
