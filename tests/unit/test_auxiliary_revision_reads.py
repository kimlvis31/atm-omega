"""Retained-revision SQL shape and bounds, without connecting to a database."""

from contextlib import contextmanager
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal as D

import pytest

from omega.application.auxiliary_codec import encode_observation, observation_checksum
from omega.domain.errors import StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_store import EPOCH, PostgresAuxiliaryStore
from omega.ports.auxiliary import AuxiliaryReadRange

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
SPAN = AuxiliaryReadRange(BTC, "open_interest", BTC.key + ":open_interest_history", 0, 60000)


def payload(number, available):
    row = AuxiliaryInput(
        "open_interest",
        (("METRIC_OPENINTEREST", D(number)),),
        0,
        60000,
        0,
        available,
        source=SPAN.source,
    )
    raw = encode_observation(BTC, row)
    return raw, observation_checksum(raw)


class Connection:
    def __init__(self, rows):
        self.rows, self.calls = rows, []

    @contextmanager
    def transaction(self):
        self.calls.append(("transaction",))
        yield

    def execute(self, *args):
        self.calls.append(args)

    @contextmanager
    def cursor(self, *, name):
        self.calls.append(("cursor", name))
        yield self

    def fetchmany(self, size):
        self.calls.append(("fetchmany", size))
        result, self.rows = self.rows, []
        return result


def store(rows):
    value = object.__new__(PostgresAuxiliaryStore)
    value.connection = Connection(rows)
    value._database_error = RuntimeError
    return value


def test_read_revisions_keeps_both_known_versions_and_checks_edges_in_one_readonly_snapshot():
    first, sha1 = payload(100, 1)
    second, sha2 = payload(200, 2)
    owner = store(
        [
            (EPOCH, 1, sha1, None, first, 1, 0, None, None),
            (EPOCH, 2, sha2, sha1, second, 2, 0, sha1, 1),
        ]
    )
    records = owner.read_revisions((SPAN,), as_of_ms=10)
    assert [row.revision for row in records] == [1, 2]
    assert records[1].previous_checksum == sha1
    assert [row.observation.mapping()["METRIC_OPENINTEREST"] for row in records] == [100, 200]
    calls = owner.connection.calls
    assert calls[0] == ("transaction",)
    assert ("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY",) in calls
    query = next(
        args for args in calls if type(args[0]) is str and args[0].startswith("WITH selected")
    )
    assert "DISTINCT ON" not in query[0] and "ORDER BY s.open_time,s.revision" in query[0]
    assert query[1][4] == EPOCH + timedelta(milliseconds=60000) and query[1][5] == 10
    assert sum(args[0] == "transaction" for args in calls) == 1


@pytest.mark.parametrize("fault", ["overlap", "count", "bytes", "edge"])
def test_retained_read_rejects_overlapping_or_excess_or_broken_original_evidence(fault):
    raw, checksum = payload(100, 1)
    row = (EPOCH, 1, checksum, None, raw, 1, 0, None, None)
    owner = store([row, row] if fault == "count" else [row])
    specs = (SPAN, SPAN) if fault == "overlap" else (SPAN,)
    if fault == "bytes":
        owner.connection.rows = [(*row[:4], b"x" * (32 * 1024 * 1024 + 1), *row[5:])]
    elif fault == "edge":
        broken, broken_checksum = payload(200, 2)
        owner.connection.rows = [(EPOCH, 2, broken_checksum, "0" * 64, broken, 2, 0, "1" * 64, 1)]
    with pytest.raises((ValidationError, StorageError)):
        owner.read_revisions(specs, as_of_ms=10, limit=1 if fault == "count" else 10000)
    if fault == "overlap":
        assert owner.connection.calls == []


def test_retained_cutoff_and_native_window_bounds_reject_before_connection():
    for specs, cutoff in (((SPAN,), True), ((replace(SPAN, end_ms=60000 * 10001),), 10)):
        owner = store([])
        with pytest.raises(ValidationError):
            owner.read_revisions(specs, as_of_ms=cutoff)
        assert owner.connection.calls == []
