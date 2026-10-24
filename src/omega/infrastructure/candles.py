"""New Omega SQLite storage. Immutable observed OHLCV, original final WS payloads."""

import sqlite3
from pathlib import Path

from omega.domain.errors import ProtocolError, ValidationError
from omega.domain.models import Candle, Instrument, decimal
from omega.ports.collection import MinuteObservation


class SqliteCandleStore:
    def __init__(self, path: Path):
        # Explicit caller-selected database; never discovers or opens a legacy DB.
        self.connection = sqlite3.connect(path, timeout=5)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS minutes (instrument TEXT NOT NULL, "
            "open_ms INTEGER NOT NULL, close_ms INTEGER NOT NULL, "
            "o TEXT NOT NULL, h TEXT NOT NULL, l TEXT NOT NULL, c TEXT NOT NULL, v TEXT NOT NULL, "
            "source TEXT NOT NULL, received_ms INTEGER NOT NULL, raw TEXT, "
            "PRIMARY KEY (instrument, open_ms)) WITHOUT ROWID"
        )
        self.connection.commit()

    def get(self, instrument: Instrument, open_time_ms: int) -> Candle | None:
        row = self.connection.execute(
            "SELECT open_ms,close_ms,o,h,l,c,v FROM minutes WHERE instrument=? AND open_ms=?",
            (instrument.key, open_time_ms),
        ).fetchone()
        return (
            None
            if row is None
            else Candle(instrument, row[0], row[1], *(decimal(v) for v in row[2:]))
        )

    def put(self, observation: MinuteObservation) -> bool:
        c = observation.candle
        if observation.raw is not None and len(observation.raw.encode()) > 65_536:
            raise ValidationError("raw candle payload exceeds limit")
        with self.connection:
            prior = self.get(c.instrument, c.open_time_ms)
            if prior is not None:
                if prior != c:
                    raise ProtocolError("conflicting closed minute; original data retained")
                # REST repair may arrive first. Preserve the later original WS payload too.
                if observation.raw is not None:
                    self.connection.execute(
                        "UPDATE minutes SET raw=COALESCE(raw,?) WHERE instrument=? AND open_ms=?",
                        (observation.raw, c.instrument.key, c.open_time_ms),
                    )
                return False
            self.connection.execute(
                "INSERT INTO minutes VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (
                    c.instrument.key,
                    c.open_time_ms,
                    c.close_time_ms,
                    *(str(x) for x in (c.open, c.high, c.low, c.close, c.volume)),
                    observation.source,
                    observation.received_at_ms,
                    observation.raw,
                ),
            )
        return True

    def close(self) -> None:
        self.connection.close()
