"""Archive-first point statistics with frozen normalization, CAS and durable pins.

Only actual provider samples are stored. Five-minute samples are not expanded
into synthetic one-minute carry observations, and no account state is loaded.
"""

import json
import re
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.market_inputs import AuxiliaryInput
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.infrastructure.binance.auxiliary import (
    _unique_object,
    long_short_ratio,
    open_interest,
    open_interest_history,
)
from omega.ports.auxiliary import AuxiliaryWriteReceipt, StoredAuxiliary
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MAX_BYTES = 2_000_000
_CHANNELS = {"open_interest", "open_interest_history", "long_short_ratio"}
_STATE_KEYS = {
    "schema_version",
    "archive_id",
    "instrument",
    "batch_channel",
    "status",
    "rows",
    "deferred_until_ms",
    "reason",
    "destination_id",
}
_ROW_KEYS = {
    "status",
    "payload",
    "checksum",
    "expected_previous",
    "receipt",
    "pinned_payload",
    "committed_previous",
}


def _encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _hash(value):
    return type(value) is str and re.fullmatch("[a-f0-9]{64}", value) is not None


def _same(left, right):
    return (
        left == right
        and len(left.values) == len(right.values)
        and all(type(a[1]) is type(b[1]) for a, b in zip(left.values, right.values, strict=True))
    )


def _instrument(key):
    try:
        provider, market, symbol = key.split(":")
        result = Instrument(provider, Market(market), symbol)
        if result.provider != "binance" or result.market != Market.USDM:
            raise ValueError
        return result
    except (AttributeError, TypeError, ValueError):
        raise ValidationError("statistics require a Binance USD-M instrument") from None


class AuxiliaryStatisticsCollector:
    def __init__(self, root, client, archive, store, clock_ms, *, destination_id=None):
        if not callable(clock_ms):
            raise ValidationError("statistics require an actual local clock")
        self.root = Path(root)
        store_id = getattr(store, "destination_id", None)
        selected = store_id if destination_id is None else destination_id
        if not _hash(selected) or (store_id is not None and selected != store_id):
            raise ValidationError("statistics collector requires its bound database destination")
        self.destination_id = selected
        self.client, self.archive, self.store, self.clock_ms = client, archive, store, clock_ms
        self.directory = self.root / "state/auxiliary-statistics"
        self._bind_destination(allow_create=True)

    def _bind_destination(self, *, allow_create=False):
        """A trusted store ID owns this namespace; never silently retarget its journals."""
        store_id = getattr(self.store, "destination_id", None)
        if store_id is not None and store_id != self.destination_id:
            raise ValidationError("statistics store destination changed; use a new root")
        owner = InstanceLock(self.root / "state/locks/auxiliary-statistics-destination.lock")
        path = self.directory / ".destination.json"
        try:
            if (
                path.is_symlink()
                or self.directory.is_symlink()
                or getattr(self.directory, "is_junction", lambda: False)()
            ):
                raise StorageError("statistics destination guard cannot use links")
            if path.exists():
                try:
                    with path.open("rb") as stream:
                        raw = stream.read(1025)
                    guard = json.loads(raw, object_pairs_hook=_unique_object)
                    if (
                        len(raw) > 1024
                        or type(guard) is not dict
                        or set(guard) != {"schema_version", "destination_id"}
                        or type(guard["schema_version"]) is not int
                        or guard["schema_version"] != 1
                        or not _hash(guard["destination_id"])
                    ):
                        raise ValueError
                except (
                    OSError,
                    ValueError,
                    TypeError,
                    ProtocolError,
                    UnicodeError,
                    RecursionError,
                ):
                    raise StorageError(
                        "statistics destination guard failed validation; use a new root"
                    ) from None
                if guard["destination_id"] != self.destination_id:
                    raise ValidationError(
                        "statistics root belongs to another database; use a new root"
                    )
            else:
                if not allow_create or any(self.directory.glob("*.json")):
                    raise StorageError("statistics destination guard is missing; use a new root")
                try:
                    atomic_json(path, {"schema_version": 1, "destination_id": self.destination_id})
                except OSError:
                    raise StorageError(
                        "statistics destination guard could not be written"
                    ) from None
        finally:
            owner.close()

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= 253_402_300_799_000:
            raise ValidationError("invalid actual statistics normalization clock")
        return value

    def _path(self, archive_id):
        if not _hash(archive_id):
            raise ValidationError("statistics resume requires a lowercase archive SHA256 ID")
        return self.directory / (archive_id + ".json")

    def _lock(self, instrument, channel):
        if not isinstance(instrument, Instrument):
            raise ValidationError("statistics require a typed instrument")
        _instrument(instrument.key)
        if channel not in _CHANNELS:
            raise ValidationError("unsupported statistics channel")
        identity = f"{instrument.key}\n{channel}\n{instrument.key}:{channel}"
        return InstanceLock(
            self.root / "state/locks" / (sha256(identity.encode()).hexdigest() + ".lock")
        )

    def _validate(self, state):
        if (
            type(state) is not dict
            or set(state) != _STATE_KEYS
            or type(state["schema_version"]) is not int
            or state["schema_version"] != 1
            or not _hash(state["archive_id"])
            or state["destination_id"] != self.destination_id
            or type(state["batch_channel"]) is not str
            or state["batch_channel"] not in _CHANNELS
            or state["status"] not in {"archived", "pending", "committed"}
            or type(state["rows"]) is not list
            or len(state["rows"]) > 500
            or type(state["reason"]) is not str
            or len(state["reason"]) > 160
        ):
            raise ValidationError("invalid statistics journal identity or bounds")
        instrument = _instrument(state["instrument"])
        deferred = state["deferred_until_ms"]
        if deferred is not None and (
            type(deferred) is not int or not 0 <= deferred <= 253_402_300_799_000
        ):
            raise ValidationError("invalid deferred statistics clock boundary")
        seen = set()
        for row in state["rows"]:
            if (
                type(row) is not dict
                or set(row) != _ROW_KEYS
                or row["status"] not in {"pending", "committed"}
                or type(row["payload"]) is not str
                or not _hash(row["checksum"])
                or row["expected_previous"] is not None
                and not _hash(row["expected_previous"])
            ):
                raise ValidationError("invalid frozen statistics row")
            payload = row["payload"].encode("utf-8")
            identity, observation = decode_observation(payload)
            expected_channel = (
                "long_short_ratio"
                if state["batch_channel"] == "long_short_ratio"
                else "open_interest"
            )
            if (
                sha256(payload).hexdigest() != row["checksum"]
                or identity != instrument
                or observation.source != f"{instrument.key}:{state['batch_channel']}"
                or observation.channel != expected_channel
                or observation.window_start_ms != observation.event_time_ms // 60000 * 60000
                or observation.window_start_ms in seen
            ):
                raise ValidationError("statistics payload conflicts with its frozen journal")
            seen.add(observation.window_start_ms)
            receipt = row["receipt"]
            if receipt is not None:
                if type(receipt) is not dict or set(receipt) != {
                    "revision",
                    "checksum",
                    "inserted",
                }:
                    raise ValidationError("invalid statistics commit receipt")
                AuxiliaryWriteReceipt(**receipt)
            previous = row["committed_previous"]
            if previous is not None and not _hash(previous):
                raise ValidationError("invalid statistics predecessor pin")
            if row["status"] == "committed":
                if receipt is None or type(row["pinned_payload"]) is not str:
                    raise ValidationError("statistics row has no canonical commit pin")
                pinned = row["pinned_payload"].encode("utf-8")
                pinned_instrument, canonical = decode_observation(pinned)
                if (
                    pinned_instrument != identity
                    or not _same(canonical, observation)
                    or sha256(pinned).hexdigest() != receipt["checksum"]
                    or (receipt["revision"] == 1) != (previous is None)
                ):
                    raise ValidationError("invalid canonical statistics pin")
            elif row["pinned_payload"] is not None:
                raise ValidationError("pending statistics cannot expose a completed pin")
        if (
            state["status"] == "archived"
            and state["rows"]
            or state["status"] == "committed"
            and any(row["status"] != "committed" for row in state["rows"])
        ):
            raise ValidationError("inconsistent statistics batch progress")

    def _save(self, state):
        self._validate(state)
        envelope = {"data": state, "sha256": sha256(_encoded(state)).hexdigest()}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise ValidationError("statistics journal exceeds its size bound")
        path = self._path(state["archive_id"])
        try:
            if path.is_symlink():
                raise StorageError("statistics journal cannot be a symlink")
            atomic_json(path, envelope)
        except OSError:
            raise StorageError("statistics journal could not be written") from None

    def _read(self, archive_id):
        path = self._path(archive_id)
        try:
            if path.is_symlink():
                raise ValueError
            with path.open("rb") as stream:
                raw = stream.read(MAX_BYTES + 1)
            if len(raw) > MAX_BYTES:
                raise ValueError
            envelope = json.loads(raw, object_pairs_hook=_unique_object)
            if (
                type(envelope) is not dict
                or set(envelope) != {"data", "sha256"}
                or envelope["sha256"] != sha256(_encoded(envelope["data"])).hexdigest()
                or envelope["data"]["archive_id"] != archive_id
            ):
                raise ValueError
            self._validate(envelope["data"])
            return envelope["data"]
        except (
            OSError,
            TypeError,
            ValueError,
            KeyError,
            UnicodeError,
            RecursionError,
            ValidationError,
            ProtocolError,
        ):
            raise StorageError(
                "statistics journal is unavailable or failed integrity checks"
            ) from None

    @staticmethod
    def _summary(state):
        return {
            "archive_id": state["archive_id"],
            "status": state["status"],
            "reason": state["reason"],
            "rows": [
                {
                    "status": row["status"],
                    "revision": row["receipt"]["revision"] if row["receipt"] else None,
                    "checksum": row["receipt"]["checksum"] if row["receipt"] else None,
                }
                for row in state["rows"]
            ],
            "deferred_until_ms": state["deferred_until_ms"],
            "received_rows": len(state["rows"]),
            "coverage_scope": "point_observation"
            if state["batch_channel"] == "open_interest"
            else "received_native_batch",
            "full_requested_range_verified": False,
        }

    def _samples(self, record):
        try:
            data = json.loads(record.body, parse_float=Decimal, object_pairs_hook=_unique_object)
            if record.channel == "open_interest":
                rows, parser = [data], open_interest
            else:
                rows = data
                parser = (
                    long_short_ratio
                    if record.channel == "long_short_ratio"
                    else open_interest_history
                )
            if type(rows) is not list or len(rows) > 500:
                raise ProtocolError("invalid archived statistics batch")
            options = {} if record.channel == "open_interest" else {"period_ms": 300000}
            return tuple(
                parser(record.instrument, row, available_at_ms=record.received_at_ms, **options)
                for row in rows
            )
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise ProtocolError("invalid archived statistics JSON") from None

    @staticmethod
    def _observation(sample, instrument, batch_channel, now):
        values = {}
        if batch_channel == "long_short_ratio":
            for name, value in zip(
                ("METRIC_LONGSHORTRATIO", "METRIC_LONGACCOUNT", "METRIC_SHORTACCOUNT"),
                sample.values,
                strict=True,
            ):
                values[name] = value
            channel = "long_short_ratio"
        else:
            values["METRIC_OPENINTEREST"] = sample.values[0]
            if batch_channel == "open_interest_history":
                values["METRIC_OPENINTERESTVALUE"] = sample.values[1]
            channel = "open_interest"
        if sample.source_period_ms is not None:
            values["SOURCE_PERIOD_MS"] = sample.source_period_ms
        opening = sample.event_time_ms // 60000 * 60000
        return AuxiliaryInput(
            channel,
            tuple(values.items()),
            opening,
            opening + 60000,
            sample.event_time_ms,
            now,
            source=f"{instrument.key}:{batch_channel}",
        )

    def _stored(self, instrument, observation, as_of):
        stored = self.store.get(
            instrument,
            observation.channel,
            observation.source,
            observation.window_start_ms,
            as_of_ms=as_of,
        )
        if stored is not None and (
            not isinstance(stored, StoredAuxiliary)
            or stored.instrument != instrument
            or stored.observation.channel != observation.channel
            or stored.observation.source != observation.source
            or stored.observation.window_start_ms != observation.window_start_ms
            or stored.observation.available_at_ms > as_of
            or sha256(encode_observation(instrument, stored.observation)).hexdigest()
            != stored.checksum
        ):
            raise ProtocolError("statistics store returned an inconsistent observation")
        return stored

    def _prepare(self, state, record):
        samples = self._samples(record)
        now = self._now()
        threshold = max((item.event_time_ms for item in samples), default=0)
        if threshold > now:
            state.update(reason="source_time_not_yet_available", deferred_until_ms=threshold)
            self._save(state)
            return
        prepared = []
        for sample in samples:
            observation = self._observation(sample, record.instrument, record.channel, now)
            prior = self._stored(record.instrument, observation, now)
            same = prior is not None and _same(
                replace(prior.observation, available_at_ms=now), observation
            )
            prepared.append((observation, prior, same))
        completed = self._now()
        if completed < now:
            state.update(reason="normalization_clock_moved_backward", deferred_until_ms=now)
            self._save(state)
            return
        if any(
            prior is not None and not same and completed <= prior.observation.available_at_ms
            for _, prior, same in prepared
        ):
            state.update(
                reason="normalization_requires_strictly_later_clock", deferred_until_ms=completed
            )
            self._save(state)
            return
        rows = []
        for observation, prior, same in prepared:
            observation = (
                prior.observation if same else replace(observation, available_at_ms=completed)
            )
            payload = encode_observation(record.instrument, observation)
            rows.append(
                {
                    "status": "pending",
                    "payload": payload.decode(),
                    "checksum": sha256(payload).hexdigest(),
                    "expected_previous": prior.checksum if prior is not None else None,
                    "receipt": {
                        "revision": prior.revision,
                        "checksum": prior.checksum,
                        "inserted": False,
                    }
                    if same
                    else None,
                    "pinned_payload": None,
                    "committed_previous": prior.previous_checksum if same else None,
                }
            )
        state.update(
            status="pending", rows=rows, reason="normalization_frozen", deferred_until_ms=None
        )
        self._save(state)

    def _flush(self, state):
        instrument = _instrument(state["instrument"])
        for row in state["rows"]:
            if row["status"] == "committed":
                continue
            _, observation = decode_observation(row["payload"].encode())
            if row["receipt"] is None:
                receipt = self.store.put(
                    instrument, observation, expected_previous=row["expected_previous"]
                )
                if not isinstance(receipt, AuxiliaryWriteReceipt):
                    raise ProtocolError("statistics store returned no commit receipt")
                row["receipt"] = {
                    "revision": receipt.revision,
                    "checksum": receipt.checksum,
                    "inserted": receipt.inserted,
                }
                self._save(state)  # Receipt survives a later read-back failure.
            receipt = row["receipt"]
            stored = self._stored(instrument, observation, observation.available_at_ms)
            if (
                stored is None
                or stored.revision != receipt["revision"]
                or stored.checksum != receipt["checksum"]
                or not _same(stored.observation, observation)
                or receipt["inserted"]
                and stored.previous_checksum != row["expected_previous"]
            ):
                raise ProtocolError("statistics commit has no matching canonical read-back")
            row.update(
                status="committed",
                pinned_payload=encode_observation(instrument, stored.observation).decode(),
                committed_previous=stored.previous_checksum,
            )
            self._save(state)
        state.update(status="committed", reason="all_statistics_rows_pinned")
        self._save(state)

    def _advance(self, state):
        if state["status"] == "committed":
            return self._summary(state)
        if state["status"] == "archived":
            record = self.archive.get(state["archive_id"]).record
            if (
                record.instrument.key != state["instrument"]
                or record.channel != state["batch_channel"]
            ):
                raise ProtocolError("statistics archive identity does not match journal")
            self._prepare(state, record)
        if state["status"] == "pending":
            self._flush(state)
        return self._summary(state)

    def _collect(self, instrument, channel, request):
        self._bind_destination()
        owner = self._lock(instrument, channel)
        try:
            batch = request()
            if batch.instrument != instrument or batch.channel != channel:
                raise ProtocolError("public statistics batch identity mismatch")
            receipt = self.archive.put(
                PublicRawResponse(
                    instrument,
                    channel,
                    batch.request_parameters,
                    batch.received_at_ms,
                    batch.body,
                    batch.headers,
                )
            )
            path = self._path(receipt.archive_id)
            if path.exists():
                state = self._read(receipt.archive_id)
            else:
                state = {
                    "schema_version": 1,
                    "archive_id": receipt.archive_id,
                    "instrument": instrument.key,
                    "batch_channel": channel,
                    "status": "archived",
                    "rows": [],
                    "deferred_until_ms": None,
                    "reason": "original_response_archived",
                    "destination_id": self.destination_id,
                }
                self._save(state)
            return self._advance(state)
        finally:
            owner.close()

    def collect_current(self, instrument):
        return self._collect(
            instrument, "open_interest", lambda: self.client.current_open_interest(instrument)
        )

    def collect_history(self, instrument, start_ms, end_ms, limit=500):
        return self._collect(
            instrument,
            "open_interest_history",
            lambda: self.client.open_interest_history(
                instrument, start_ms=start_ms, end_ms=end_ms, limit=limit
            ),
        )

    def collect_ratios(self, instrument, start_ms, end_ms, limit=500):
        return self._collect(
            instrument,
            "long_short_ratio",
            lambda: self.client.long_short_ratio(
                instrument, start_ms=start_ms, end_ms=end_ms, limit=limit
            ),
        )

    def resume(self, archive_id):
        self._bind_destination()
        state = self._read(archive_id)
        owner = self._lock(_instrument(state["instrument"]), state["batch_channel"])
        try:
            return self._advance(self._read(archive_id))
        finally:
            owner.close()

    def recover_archived(self, archive_id, *, instrument, channel, request_parameters):
        """Explicit manual recovery of raw bytes whose first journal was never saved.

        An existing journal always takes the exact resume path, including failure
        on corruption. Missing state is newly normalized at the actual current
        clock; it is never claimed to reproduce a lost historical knowledge time.
        """
        self._bind_destination()
        path = self._path(archive_id)
        if path.exists() or path.is_symlink():
            return self.resume(archive_id)
        owner = self._lock(instrument, channel)
        try:
            if path.exists() or path.is_symlink():
                return self._advance(self._read(archive_id))
            entry = self.archive.get(archive_id)
            record = entry.record
            if (
                entry.receipt.archive_id != archive_id
                or record.instrument != instrument
                or record.channel != channel
                or type(request_parameters) is not tuple
                or tuple(sorted(record.request_parameters)) != tuple(sorted(request_parameters))
            ):
                raise ValidationError("archived statistics do not match the fixed recovery request")
            parameters = dict(record.request_parameters)
            if (
                len(parameters) != len(record.request_parameters)
                or parameters.get("symbol") != instrument.symbol
            ):
                raise ValidationError("invalid archived statistics request identity")
            samples = self._samples(record)
            if channel == "open_interest":
                if set(parameters) != {"symbol"}:
                    raise ValidationError("invalid current OI recovery request")
            else:
                if (
                    set(parameters) != {"symbol", "period", "startTime", "endTime", "limit"}
                    or parameters["period"] != "5m"
                ):
                    raise ValidationError("invalid native statistics recovery request")
                try:
                    begin, end, limit = (
                        int(parameters[key]) for key in ("startTime", "endTime", "limit")
                    )
                except (ValueError, TypeError):
                    raise ValidationError("invalid archived statistics range") from None
                if (
                    not 0 <= begin <= end <= 253_402_300_799_000
                    or end - begin >= 28 * 86400000
                    or not 1 <= limit <= 500
                    or len(samples) > limit
                    or any(not begin <= sample.event_time_ms <= end for sample in samples)
                    or any(
                        a.event_time_ms >= b.event_time_ms
                        for a, b in zip(samples, samples[1:], strict=False)
                    )
                ):
                    raise ValidationError("archived statistics conflict with their native range")
            state = {
                "schema_version": 1,
                "archive_id": archive_id,
                "instrument": instrument.key,
                "batch_channel": channel,
                "status": "archived",
                "rows": [],
                "deferred_until_ms": None,
                "reason": "original_response_archived",
                "destination_id": self.destination_id,
            }
            self._save(state)
            return {**self._advance(state), "recovered_missing_journal": True}
        finally:
            owner.close()
