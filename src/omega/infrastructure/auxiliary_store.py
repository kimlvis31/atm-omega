"""Append-only derived observations with explicit repair and availability cutoff.

One owner per connection. Use independent instances for concurrent workers.
Provider wire bytes are not these derived JSON records and need a separate archive.
"""

# Imports
import json
import re
from datetime import timedelta
from hashlib import sha256

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.infrastructure.candles import EPOCH, connect_database
from omega.infrastructure.postgres.batching import COMMIT_BATCH_ROWS
from omega.infrastructure.postgres.config import database_destination_id
from omega.infrastructure.postgres.schema import verify_schema
from omega.ports.auxiliary import (
    AuxiliaryReadRange,
    AuxiliaryWriteReceipt,
    StoredAuxiliary,
)

# Constants
AUXILIARY_BATCH_SIZE = COMMIT_BATCH_ROWS
_COLUMNS = "revision,checksum,previous_checksum,payload,available_ms,event_ms"
_KEY = "instrument=%s AND channel=%s AND dataset=%s AND open_time=%s"
_MAX_TIME = 253_402_300_799_000


def _checksum(value):
    return isinstance(value, str) and re.fullmatch("[a-f0-9]{64}", value) is not None


def _identity(instrument, channel, source, open_time_ms):
    if (
        not isinstance(instrument, Instrument)
        or not isinstance(channel, str)
        or not re.fullmatch("[a-z][a-z0-9_]{0,31}", channel)
        or not isinstance(source, str)
        or not 1 <= len(source) <= 96
        or not source.isprintable()
        or type(open_time_ms) is not int
        or not 0 <= open_time_ms <= _MAX_TIME - 60000
        or open_time_ms % 60000
    ):
        raise ValidationError("invalid auxiliary storage identity")
    return instrument.key, channel, source, EPOCH + timedelta(milliseconds=open_time_ms)


def _same(left, right):
    # Decimal representation scale does not change a value; integer dtype does.
    return left == right and all(
        type(a[1]) is type(b[1]) for a, b in zip(left.values, right.values, strict=True)
    )


def _lock_id(instrument, observation):
    # Shared by live writes and backfill, including when a key has no rows yet.
    lock_bytes = json.dumps(
        (instrument.key, observation.channel, observation.source, observation.window_start_ms),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return int.from_bytes(sha256(lock_bytes).digest()[:8], "big", signed=True)


class PostgresAuxiliaryStore:
    def __init__(self, settings, *, on_commit=None):
        self.on_commit = on_commit
        self._destination_id = database_destination_id(settings)
        self.connection = connect_database(settings)
        import psycopg

        self._database_error = psycopg.Error
        try:
            verify_schema(self.connection)
        except (psycopg.Error, StorageError):
            self.connection.close()
            raise StorageError("auxiliary schema is not ready; run omega-db migrate") from None

    @property
    def destination_id(self):
        return self._destination_id

    def _stored(self, instrument, channel, source, opening, row, *, previous_edge=...):
        try:
            revision, checksum, previous, payload, available, event = row
            if (
                type(revision) is not int
                or not 1 <= revision <= 2**63 - 1
                or not _checksum(checksum)
                or (previous is not None and not _checksum(previous))
                or (revision == 1) != (previous is None)
                or not isinstance(payload, (bytes, memoryview))
            ):
                raise ValueError("invalid stored metadata")
            payload = bytes(payload)
            if sha256(payload).hexdigest() != checksum:
                raise ValueError("payload checksum mismatch")
            decoded_instrument, observation = decode_observation(payload)
            if (
                decoded_instrument != instrument
                or observation.channel != channel
                or observation.source != source
                or observation.window_start_ms != opening
                or type(available) is not int
                or type(event) is not int
                or observation.available_at_ms != available
                or observation.event_time_ms != event
            ):
                raise ValueError("payload identity mismatch")
            if revision > 1:
                key = _identity(instrument, channel, source, opening)
                prior = (
                    self.connection.execute(
                        "SELECT checksum,available_ms FROM market.auxiliary_minutes WHERE "
                        + _KEY
                        + " AND revision=%s",
                        (*key, revision - 1),
                    ).fetchone()
                    if previous_edge is ...
                    else previous_edge
                )
                if prior is None or prior[0] != previous or prior[1] >= available:
                    raise ValueError("broken auxiliary revision edge")
            return StoredAuxiliary(instrument, observation, revision, checksum, previous)
        except (ValueError, TypeError, ValidationError, OverflowError):
            raise StorageError("stored auxiliary observation failed integrity checks") from None

    def put(self, instrument, observation, *, expected_previous=None):
        if not isinstance(observation, AuxiliaryInput):
            raise ValidationError("typed auxiliary observation required")
        key = _identity(
            instrument,
            observation.channel,
            observation.source,
            observation.window_start_ms,
        )
        if expected_previous is not None and not _checksum(expected_previous):
            raise ValidationError("expected auxiliary checksum must be lowercase SHA256")
        payload = encode_observation(instrument, observation)
        digest = sha256(payload).hexdigest()
        lock_id = _lock_id(instrument, observation)
        written_bytes = None
        try:
            with self.connection.transaction():
                # The identity lock also serializes writes to compressed revisions.
                self.connection.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
                row = self.connection.execute(
                    "SELECT "
                    + _COLUMNS
                    + " FROM market.auxiliary_minutes WHERE "
                    + _KEY
                    + " ORDER BY revision DESC LIMIT 1",
                    key,
                ).fetchone()
                previous, revision = None, 1
                if row is not None:
                    latest = self._stored(
                        instrument,
                        observation.channel,
                        observation.source,
                        observation.window_start_ms,
                        row,
                    )
                    if _same(latest.observation, observation):
                        receipt = AuxiliaryWriteReceipt(latest.revision, latest.checksum, False)
                    else:
                        if expected_previous != latest.checksum:
                            raise ProtocolError(
                                "auxiliary revision conflict; original history retained"
                            )
                        if observation.available_at_ms <= latest.observation.available_at_ms:
                            raise ProtocolError(
                                "auxiliary repair requires strictly later actual availability"
                            )
                        previous, revision = latest.checksum, latest.revision + 1
                        receipt = None
                else:
                    if expected_previous is not None:
                        raise ProtocolError("auxiliary revision anchor does not exist")
                    receipt = None
                if receipt is None:
                    if revision > 2**63 - 1:
                        raise ProtocolError("auxiliary revision capacity exceeded")
                    size_column = (
                        " RETURNING pg_column_size(auxiliary_minutes)"
                        if self.on_commit is not None
                        else ""
                    )
                    inserted = self.connection.execute(
                        "INSERT INTO market.auxiliary_minutes "
                        "(instrument,channel,dataset,open_time,revision,available_ms,event_ms,"
                        "payload,checksum,previous_checksum) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)" + size_column,
                        (
                            *key,
                            revision,
                            observation.available_at_ms,
                            observation.event_time_ms,
                            payload,
                            digest,
                            previous,
                        ),
                    )
                    if size_column:
                        written_bytes = inserted.fetchone()[0]
                    receipt = AuxiliaryWriteReceipt(revision, digest, True)
            # A raised/unknown commit never publishes a successful receipt.
            if written_bytes is not None:
                self.on_commit("repair", observation.channel, 1, written_bytes)
            return receipt
        except self._database_error:
            raise StorageError(
                "auxiliary commit failed; reconcile before retrying repair"
            ) from None

    def put_history_batch(self, instrument, observations):
        """Append archive evidence or upgrade older incomplete observations atomically."""
        return self._put_ingested_batch([(instrument, row) for row in observations], history=True)

    def put_live_batch(self, observations):
        """Commit closed stream observations together, retaining the first live receipt."""
        return self._put_ingested_batch(observations, history=False)

    def _put_ingested_batch(self, observations, *, history):
        if len(observations) > AUXILIARY_BATCH_SIZE:
            raise ValidationError("too many auxiliary observations in one batch")
        if not observations:
            return 0

        by_key = {}
        for instrument, observation in observations:
            if not isinstance(observation, AuxiliaryInput):
                raise ValidationError("typed auxiliary observation required")
            key = _identity(
                instrument, observation.channel, observation.source, observation.window_start_ms
            )
            if key in by_key:
                raise ValidationError("duplicate identity in auxiliary batch")
            by_key[key] = instrument, observation

        lock_ids = sorted({_lock_id(instrument, row) for instrument, row in observations})
        keys = tuple(map(list, zip(*by_key, strict=True)))
        measured = ()
        try:
            with self.connection.transaction():
                self.connection.execute(
                    "SELECT pg_advisory_xact_lock(lock_id) "
                    "FROM unnest(%s::bigint[]) AS locks(lock_id) ORDER BY lock_id",
                    (lock_ids,),
                )
                rows = self.connection.execute(
                    """
                    SELECT requested.*, latest.revision, latest.checksum, latest.previous_checksum,
                           latest.payload, latest.available_ms, latest.event_ms,
                           prior.checksum, prior.available_ms
                    FROM unnest(%s::text[], %s::text[], %s::text[], %s::timestamptz[])
                        AS requested(instrument,channel,dataset,open_time)
                    CROSS JOIN LATERAL (
                        SELECT revision,checksum,previous_checksum,payload,available_ms,event_ms
                        FROM market.auxiliary_minutes
                        WHERE instrument=requested.instrument AND channel=requested.channel
                          AND dataset=requested.dataset AND open_time=requested.open_time
                        ORDER BY revision DESC LIMIT 1
                    ) AS latest
                    LEFT JOIN LATERAL (
                        SELECT checksum,available_ms
                        FROM market.auxiliary_minutes
                        WHERE instrument=requested.instrument AND channel=requested.channel
                          AND dataset=requested.dataset AND open_time=requested.open_time
                          AND revision=latest.revision-1
                        LIMIT 1
                    ) AS prior ON latest.revision>1
                    """,
                    keys,
                ).fetchall()
                latest_by_key = {row[:4]: row[4:] for row in rows}
                inserts = []
                for key, (instrument, observation) in by_key.items():
                    row = latest_by_key.get(key)
                    previous, revision = None, 1
                    if row is not None:
                        latest = self._stored(
                            instrument,
                            observation.channel,
                            observation.source,
                            observation.window_start_ms,
                            row[:6],
                            previous_edge=row[6:],
                        )
                        previous_observation = latest.observation
                        if observation.available_at_ms <= previous_observation.available_at_ms:
                            continue
                        if history:
                            if (
                                previous_observation.quality == "observed"
                                or observation.quality != "observed"
                            ):
                                continue
                        elif (
                            observation.channel not in {"open_interest", "long_short_ratio"}
                            or previous_observation.values == observation.values
                        ):
                            continue
                        previous, revision = latest.checksum, latest.revision + 1
                        if revision > 2**63 - 1:
                            raise ProtocolError("auxiliary revision capacity exceeded")
                    payload = encode_observation(instrument, observation)
                    inserts.append(
                        (
                            *key,
                            revision,
                            observation.available_at_ms,
                            observation.event_time_ms,
                            payload,
                            sha256(payload).hexdigest(),
                            previous,
                        )
                    )
                if inserts:
                    size_column = (
                        " RETURNING channel,pg_column_size(auxiliary_minutes)"
                        if self.on_commit is not None
                        else ""
                    )
                    query = (
                        """
                        INSERT INTO market.auxiliary_minutes
                            (instrument,channel,dataset,open_time,revision,available_ms,event_ms,
                             payload,checksum,previous_checksum)
                        SELECT * FROM unnest(
                            %s::text[], %s::text[], %s::text[], %s::timestamptz[], %s::bigint[],
                            %s::bigint[], %s::bigint[], %s::bytea[], %s::text[], %s::text[]
                        )
                    """
                        + size_column
                    )
                    inserted = self.connection.execute(
                        query, tuple(map(list, zip(*inserts, strict=True)))
                    )
                    if size_column:
                        measured = inserted.fetchall()
            totals = {}
            for channel, size in measured:
                rows, bytes_saved = totals.get(channel, (0, 0))
                totals[channel] = (rows + 1, bytes_saved + size)
            for channel, (rows, bytes_saved) in totals.items():
                self.on_commit("backfill" if history else "live", channel, rows, bytes_saved)
            return len(inserts)
        except self._database_error:
            raise StorageError("auxiliary batch commit failed") from None

    def get(self, instrument, channel, source, open_time_ms, *, as_of_ms):
        key = _identity(instrument, channel, source, open_time_ms)
        if type(as_of_ms) is not int or not 0 <= as_of_ms <= _MAX_TIME:
            raise ValidationError("invalid auxiliary availability cutoff")
        try:
            with self.connection.transaction():
                self.connection.execute("SET TRANSACTION READ ONLY")
                row = self.connection.execute(
                    "SELECT "
                    + _COLUMNS
                    + " FROM market.auxiliary_minutes WHERE "
                    + _KEY
                    + " AND available_ms<=%s ORDER BY revision DESC LIMIT 1",
                    (*key, as_of_ms),
                ).fetchone()
                result = (
                    None
                    if row is None
                    else self._stored(instrument, channel, source, open_time_ms, row)
                )
            return result
        except self._database_error:
            raise StorageError("auxiliary query failed") from None

    def close(self):
        self.connection.close()

    def read_eta_metric_minutes(self, instrument, start_ms, end_ms, *, as_of_ms):
        from psycopg.rows import dict_row

        try:
            with self.connection.transaction():
                self.connection.execute("SET TRANSACTION READ ONLY")
                with self.connection.cursor(row_factory=dict_row) as cursor:
                    cursor.execute(
                        "SELECT * FROM market.eta_metric_minutes(%s, %s, %s, %s)",
                        (instrument, start_ms, end_ms, as_of_ms),
                    )
                    return tuple(cursor.fetchall())
        except self._database_error:
            raise StorageError("Eta metric minute query failed") from None

    def read_ranges(self, ranges, *, as_of_ms, limit=10000):
        if (
            type(ranges) is not tuple
            or not 1 <= len(ranges) <= 64
            or any(not isinstance(row, AuxiliaryReadRange) for row in ranges)
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= _MAX_TIME
            or type(limit) is not int
            or not 1 <= limit <= 10000
            or sum((row.end_ms - row.start_ms) // 60000 for row in ranges) > limit
        ):
            raise ValidationError("auxiliary snapshot needs at most10000 requested minute slots")
        spans = {}
        for row in ranges:
            key = row.instrument, row.channel, row.source
            existing = spans.setdefault(key, [])
            if any(row.start_ms < end and start < row.end_ms for start, end in existing):
                raise ValidationError("auxiliary snapshot ranges overlap")
            existing.append((row.start_ms, row.end_ms))
        result, total_bytes = [], 0
        try:
            with self.connection.transaction():
                self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for index, spec in enumerate(ranges):
                    instrument, channel, source, start = _identity(
                        spec.instrument, spec.channel, spec.source, spec.start_ms
                    )
                    query = (
                        "WITH selected AS (SELECT DISTINCT ON (open_time) open_time,"
                        + _COLUMNS
                        + " FROM market.auxiliary_minutes WHERE instrument=%s AND channel=%s "
                        "AND dataset=%s AND open_time>=%s AND open_time<%s AND available_ms<=%s "
                        "ORDER BY open_time,revision DESC) "
                        "SELECT s.open_time,s.revision,s.checksum,s.previous_checksum,s.payload,"
                        "s.available_ms,s.event_ms,p.checksum,p.available_ms FROM selected s "
                        "LEFT JOIN market.auxiliary_minutes p ON p.instrument=%s AND p.channel=%s "
                        "AND p.dataset=%s AND p.open_time=s.open_time AND p.revision=s.revision-1 "
                        "ORDER BY s.open_time"
                    )
                    with self.connection.cursor(name="omega_aux_range_" + str(index)) as cursor:
                        cursor.execute(
                            query,
                            (
                                instrument,
                                channel,
                                source,
                                start,
                                EPOCH + timedelta(milliseconds=spec.end_ms),
                                as_of_ms,
                                instrument,
                                channel,
                                source,
                            ),
                        )
                        while batch := cursor.fetchmany(64):
                            for row in batch:
                                total_bytes += len(row[4])
                                if total_bytes > 32 * 1024 * 1024 or len(result) >= limit:
                                    raise StorageError(
                                        "auxiliary snapshot exceeds its memory capacity"
                                    )
                                delta = row[0] - EPOCH
                                opening = (
                                    delta.days * 86400000
                                    + delta.seconds * 1000
                                    + delta.microseconds // 1000
                                )
                                if delta.microseconds % 1000 or opening % 60000:
                                    raise StorageError(
                                        "auxiliary snapshot contains an unaligned window"
                                    )
                                result.append(
                                    self._stored(
                                        spec.instrument,
                                        channel,
                                        source,
                                        opening,
                                        row[1:7],
                                        previous_edge=None if row[7] is None else row[7:9],
                                    )
                                )
            return tuple(result)
        except self._database_error:
            raise StorageError("auxiliary snapshot query failed") from None

    def read_revisions(self, ranges, *, as_of_ms, limit=10000, check_stop=None):
        if check_stop is not None and not callable(check_stop):
            raise ValidationError("retained revision stop check must be callable")

        def stopped():
            if check_stop is not None:
                check_stop()

        stopped()
        if (
            type(ranges) is not tuple
            or not 1 <= len(ranges) <= 64
            or any(not isinstance(row, AuxiliaryReadRange) for row in ranges)
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= _MAX_TIME
            or type(limit) is not int
            or not 1 <= limit <= 10000
            or sum((row.end_ms - row.start_ms) // 60000 for row in ranges) > limit
        ):
            raise ValidationError(
                "auxiliary retained revision snapshot needs at most10000 requested minute slots"
            )
        spans = {}
        for row in ranges:
            key = row.instrument, row.channel, row.source
            existing = spans.setdefault(key, [])
            if any(row.start_ms < end and start < row.end_ms for start, end in existing):
                raise ValidationError("auxiliary retained revision snapshot ranges overlap")
            existing.append((row.start_ms, row.end_ms))
        result, total_bytes = [], 0
        try:
            stopped()
            with self.connection.transaction():
                self.connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                for index, spec in enumerate(ranges):
                    stopped()
                    instrument, channel, source, start = _identity(
                        spec.instrument, spec.channel, spec.source, spec.start_ms
                    )
                    query = (
                        "WITH selected AS (SELECT open_time,"
                        + _COLUMNS
                        + " FROM market.auxiliary_minutes WHERE instrument=%s AND channel=%s "
                        "AND dataset=%s AND open_time>=%s AND open_time<%s AND available_ms<=%s "
                        "ORDER BY open_time,revision) "
                        "SELECT s.open_time,s.revision,s.checksum,s.previous_checksum,s.payload,"
                        "s.available_ms,s.event_ms,p.checksum,p.available_ms FROM selected s "
                        "LEFT JOIN market.auxiliary_minutes p ON p.instrument=%s AND p.channel=%s "
                        "AND p.dataset=%s AND p.open_time=s.open_time AND p.revision=s.revision-1 "
                        "ORDER BY s.open_time,s.revision"
                    )
                    with self.connection.cursor(name="omega_aux_revisions_" + str(index)) as cursor:
                        cursor.execute(
                            query,
                            (
                                instrument,
                                channel,
                                source,
                                start,
                                EPOCH + timedelta(milliseconds=spec.end_ms),
                                as_of_ms,
                                instrument,
                                channel,
                                source,
                            ),
                        )
                        stopped()
                        while True:
                            stopped()
                            batch = cursor.fetchmany(64)
                            stopped()
                            if not batch:
                                break
                            for row in batch:
                                stopped()
                                total_bytes += len(row[4])
                                if total_bytes > 32 * 1024 * 1024 or len(result) >= limit:
                                    raise StorageError(
                                        "retained revision snapshot exceeds memory capacity"
                                    )
                                delta = row[0] - EPOCH
                                opening = (
                                    delta.days * 86400000
                                    + delta.seconds * 1000
                                    + delta.microseconds // 1000
                                )
                                if delta.microseconds % 1000 or opening % 60000:
                                    raise StorageError(
                                        "retained revision snapshot contains an unaligned window"
                                    )
                                result.append(
                                    self._stored(
                                        spec.instrument,
                                        channel,
                                        source,
                                        opening,
                                        row[1:7],
                                        previous_edge=None if row[7] is None else row[7:9],
                                    )
                                )
            stopped()
            return tuple(result)
        except self._database_error:
            raise StorageError("auxiliary retained revision snapshot query failed") from None

    def latest_sample(
        self,
        instrument,
        channel,
        source,
        *,
        event_before_ms,
        as_of_ms,
        max_age_ms=330000,
    ):
        _identity(instrument, channel, source, 0)
        if (
            type(event_before_ms) is not int
            or not 0 <= event_before_ms <= _MAX_TIME
            or type(as_of_ms) is not int
            or not 0 <= as_of_ms <= _MAX_TIME
            or type(max_age_ms) is not int
            or not 1 <= max_age_ms <= 86400000
        ):
            raise ValidationError("invalid auxiliary native-sample range")
        if event_before_ms == 0:
            return None
        earliest = max(0, event_before_ms - max_age_ms)
        start = EPOCH + timedelta(milliseconds=earliest // 60000 * 60000)
        end = EPOCH + timedelta(milliseconds=event_before_ms)
        try:
            with self.connection.transaction():
                self.connection.execute("SET TRANSACTION READ ONLY")
                row = self.connection.execute(
                    "SELECT open_time," + _COLUMNS + " FROM market.auxiliary_minutes "
                    "WHERE instrument=%s AND channel=%s AND dataset=%s "
                    "AND open_time>=%s AND open_time<%s "
                    "AND event_ms>=%s AND event_ms<%s AND available_ms<=%s "
                    "ORDER BY event_ms DESC,revision DESC LIMIT 1",
                    (
                        instrument.key,
                        channel,
                        source,
                        start,
                        end,
                        earliest,
                        event_before_ms,
                        as_of_ms,
                    ),
                ).fetchone()
                if row is None:
                    return None
                try:
                    delta = row[0] - EPOCH
                    opening = (
                        delta.days * 86400 + delta.seconds
                    ) * 1000 + delta.microseconds // 1000
                    if delta.microseconds % 1000 or opening % 60000:
                        raise ValueError("unaligned native point window")
                    stored = self._stored(instrument, channel, source, opening, row[1:])
                    sample = stored.observation
                    if (
                        sample.window_start_ms != sample.event_time_ms // 60000 * 60000
                        or sample.quality != "observed"
                    ):
                        raise ValueError(
                            "carried view cannot substitute for an actual source point"
                        )
                except (TypeError, ValueError, OverflowError, AttributeError):
                    raise StorageError(
                        "stored auxiliary native sample failed integrity checks"
                    ) from None
            return stored
        except self._database_error:
            raise StorageError("auxiliary native sample query failed") from None
