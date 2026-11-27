import json
import os
from types import SimpleNamespace

import pytest

from omega.domain.errors import RateLimited, StorageError
from omega.domain.models import Instrument, Market
from omega.infrastructure.binance.auxiliary import agg_trade
from omega.infrastructure.raw_archive import FileRawArchive
from omega.runtime.market_trade_repair import TradeRepair

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
OPENING = 60_000


class Budget:
    def __init__(self, allowed):
        self.allowed = allowed
        self.reservations = 0

    def reserve(self, _weights):
        self.reservations += 1
        if self.reservations > self.allowed:
            raise RateLimited(1)


class SyntheticClient:
    def __init__(self, *, remaining=6001):
        self.remaining = remaining
        self.calls = []

    def agg_trades(self, instrument, *, from_id=None, start_ms=None, end_ms=None, limit):
        self.calls.append(
            {
                "from_id": from_id,
                "start_ms": start_ms,
                "end_ms": end_ms,
                "limit": limit,
            }
        )
        first_id = 0 if from_id is None else from_id
        count = min(limit, self.remaining)
        rows = [
            {
                "a": event_id,
                "p": "100.25",
                "q": "1",
                "f": event_id,
                "l": event_id,
                "T": OPENING + 1,
                "m": False,
            }
            for event_id in range(first_id, first_id + count)
        ]
        self.remaining -= count
        params = {"symbol": instrument.symbol, "limit": str(limit)}
        if from_id is not None:
            params["fromId"] = str(from_id)
        else:
            params.update(startTime=str(start_ms), endTime=str(end_ms - 1))
        received = OPENING + 2
        return SimpleNamespace(
            instrument=instrument,
            channel="agg_trades",
            body=json.dumps(rows, separators=(",", ":")).encode(),
            events=tuple(agg_trade(instrument, row, available_at_ms=received) for row in rows),
            request_parameters=tuple(sorted(params.items())),
            received_at_ms=received,
            headers=(("content-type", "application/json"),),
        )


def setup_repair(tmp_path, budget, monkeypatch):
    # This Windows sandbox blocks hard links; same-directory rename still
    # exercises FileRawArchive's atomic publication and receipt validation.
    monkeypatch.setattr("omega.infrastructure.raw_archive.os.link", os.rename)
    root = tmp_path / "repair-state"
    archive = FileRawArchive(root / "raw")
    repair = TradeRepair(root, archive, budget, clock_ms=lambda: OPENING + 1_000)
    return root, archive, repair


def test_trade_repair_resumes_after_rate_limit_and_restart(tmp_path, monkeypatch):
    root, archive, repair = setup_repair(tmp_path, Budget(allowed=6), monkeypatch)
    client = SyntheticClient()

    with pytest.raises(RateLimited):
        repair.collect(INSTRUMENT, OPENING, client)

    assert len(client.calls) == 6
    manifest = next((root / "history-repair-pending").glob("*.json"))
    checkpoint = json.loads(manifest.read_text(encoding="utf-8"))
    assert len(checkpoint["raw_responses"]) == 6
    assert all(len(archive.get(receipt).record.body) > 0 for receipt in checkpoint["raw_responses"])

    resumed = TradeRepair(root, archive, Budget(allowed=1), clock_ms=lambda: OPENING + 1_000)
    result = resumed.collect(INSTRUMENT, OPENING, client)

    assert len(client.calls) == 7
    assert client.calls[-1]["from_id"] == 6000
    assert client.calls[-1]["start_ms"] is None
    assert result[1] == 6001
    assert dict(result[0])["AGGTRADE_EVENTCOUNT"] == 6001
    assert not manifest.exists()


def test_trade_repair_rejects_corrupt_manifest_without_refetching(tmp_path, monkeypatch):
    root, archive, repair = setup_repair(tmp_path, Budget(allowed=2), monkeypatch)
    client = SyntheticClient()
    with pytest.raises(RateLimited):
        repair.collect(INSTRUMENT, OPENING, client)
    manifest = next((root / "history-repair-pending").glob("*.json"))
    manifest.write_text("{not-json", encoding="utf-8")

    fresh = TradeRepair(root, archive, Budget(allowed=10), clock_ms=lambda: OPENING + 1_000)
    calls_before = len(client.calls)
    with pytest.raises(StorageError, match="checkpoint"):
        fresh.collect(INSTRUMENT, OPENING, client)

    assert len(client.calls) == calls_before
