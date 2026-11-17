"""Explicit, bounded public depth sessions; no boot connection, orders or repair.

One worker owns HTTP, websocket contexts, raw archive and normalized store.
Snapshot/diff originals precede minute publication. Unknown persistence retires
that instrument until explicit resume; a new connection never revives old book.
"""

import asyncio
import json
import os
import re
import stat
import threading
import time
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

from omega.domain.errors import (
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.domain.models import Instrument, Market
from omega.infrastructure.auxiliary_codec import decode_observation, encode_observation
from omega.infrastructure.binance.auxiliary import _payload
from omega.infrastructure.binance.client import BinanceClient
from omega.infrastructure.binance.depth_book import DepthBook
from omega.infrastructure.binance.depth_sampling import MINUTE, SOURCE_SUFFIX, DepthMinuteSampler
from omega.ports.raw_data import PublicRawResponse
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MAX_STATE_BYTES = 2_000_000
MAX_TIME = 253_402_300_799_000
_FIELDS = {
    "schema_version",
    "instrument",
    "source",
    "destination_id",
    "price_precision",
    "quote_precision",
    "generation",
    "cursor_ms",
    "raw_bytes",
    "records",
    "pending",
    "last",
}


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(rows):
    result = {}
    for key, value in rows:
        if key in result:
            raise ValidationError("duplicate depth collection state field")
        result[key] = value
    return result


def _hash(value):
    return type(value) is str and re.fullmatch("[a-f0-9]{64}", value) is not None


def _configs(values, maximum):
    if type(values) not in (tuple, list) or not 1 <= len(values) <= maximum:
        raise ValidationError("depth start requires 1..8 instrument configurations")
    result = []
    for value in values:
        if type(value) is not dict or set(value) != {
            "instrument",
            "price_precision",
            "quote_precision",
        }:
            raise ValidationError("depth configuration requires instrument and explicit precisions")
        instrument = value["instrument"]
        if type(instrument) is str:
            try:
                provider, market, symbol = instrument.split(":")
                instrument = Instrument(provider, Market(market), symbol)
            except (TypeError, ValueError):
                raise ValidationError("depth instrument must be a canonical key") from None
        if not isinstance(instrument, Instrument):
            raise ValidationError("depth start requires typed instruments")
        DepthMinuteSampler(
            instrument,
            price_precision=value["price_precision"],
            quote_precision=value["quote_precision"],
        )
        result.append({**value, "instrument": instrument})
    if len({row["instrument"] for row in result}) != len(result):
        raise ValidationError("duplicate depth subscription")
    return tuple(result)


def depth_url(instrument):
    root = (
        "wss://stream.binance.com:443/stream"
        if instrument.market is Market.SPOT
        else "wss://fstream.binance.com/public/stream"
    )
    return root + "?streams=" + instrument.symbol.lower() + "@depth"


def connect_depth(instrument):
    from websockets.asyncio.client import connect

    return connect(
        depth_url(instrument),
        proxy=None,
        compression=None,
        ping_interval=None,
        open_timeout=10,
        close_timeout=5,
        max_size=1_048_576,
        max_queue=32,
    )


class DepthCollection:
    def __init__(
        self,
        root,
        resource_factory,
        *,
        clock_ms=None,
        connect=None,
        max_symbols=8,
        max_raw_bytes=64 * 1024 * 1024,
        max_raw_refs=8192,
        max_minutes=10000,
        snapshot_weight_usdm=20,
        retry_base_s=5,
        max_retries=6,
    ):
        if (
            not callable(resource_factory)
            or clock_ms is not None
            and not callable(clock_ms)
            or connect is not None
            and not callable(connect)
        ):
            raise ValidationError("depth factories and clock must be callable")
        if (
            any(
                type(value) is not int or value <= 0
                for value in (
                    max_symbols,
                    max_raw_bytes,
                    max_raw_refs,
                    max_minutes,
                    snapshot_weight_usdm,
                    retry_base_s,
                    max_retries,
                )
            )
            or max_symbols > 8
            or not 2 <= max_raw_refs <= 8192
            or max_minutes > 10000
            or max_raw_bytes > 1024 * 1024 * 1024
            or snapshot_weight_usdm < 20
            or retry_base_s > 300
            or max_retries > 12
        ):
            raise ValidationError("invalid bounded depth collection policy")
        self.root, self.resource_factory = Path(os.path.abspath(root)), resource_factory
        self.clock_ms, self.connect = (
            clock_ms or (lambda: time.time_ns() // 1_000_000),
            connect or connect_depth,
        )
        self.max_symbols, self.max_raw_bytes, self.max_raw_refs, self.max_minutes = (
            max_symbols,
            max_raw_bytes,
            max_raw_refs,
            max_minutes,
        )
        self.snapshot_weight_usdm, self.retry_base_s, self.max_retries = (
            snapshot_weight_usdm,
            retry_base_s,
            max_retries,
        )
        self.lock, self.stop_event = threading.RLock(), threading.Event()
        self.worker = self.loop = self.task = None
        self.generation, self.closed = 0, False
        self.rows, self.close_errors = {}, []

    def _clock(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= MAX_TIME:
            raise ProtocolError("invalid actual depth worker clock")
        return value

    def _active(self, token):
        with self.lock:
            if self.closed or self.stop_event.is_set() or token != self.generation:
                raise asyncio.CancelledError

    def _row(self, instrument, token, **changes):
        with self.lock:
            self._active(token)
            self.rows[instrument.key].update(changes)

    def start(self, configurations, *, resume=False):
        values = _configs(configurations, self.max_symbols)
        if type(resume) is not bool:
            raise ValidationError("depth resume flag must be boolean")
        with self.lock:
            if self.closed or self.worker is not None and self.worker.is_alive():
                raise ValidationError("depth owner is closed or already running")
            self.generation += 1
            token = self.generation
            self.stop_event.clear()
            self.close_errors = []
            self.rows = {
                row["instrument"].key: {
                    "instrument": row["instrument"].key,
                    "state": "queued",
                    "source": row["instrument"].key + ":" + SOURCE_SUFFIX,
                    "cursor_ms": None,
                    "quality": None,
                    "reason": None,
                }
                for row in values
            }
            self.worker = threading.Thread(
                target=self._worker, args=(values, token, resume), name="omega-depth", daemon=True
            )
            self.worker.start()
        return self.snapshot()

    def snapshot(self):
        with self.lock:
            return {
                "generation": self.generation,
                "running": self.worker is not None and self.worker.is_alive(),
                "closed": self.closed,
                "instruments": deepcopy(list(self.rows.values())),
                "close_errors": list(self.close_errors),
                "limitation": (
                    "observed live snapshots only; no historical depth or automatic repair"
                ),
            }

    def request_stop(self):
        with self.lock:
            already_requested = self.stop_event.is_set()
            self.stop_event.set()
            loop, task = self.loop, self.task
        if not already_requested and loop is not None and task is not None:
            try:
                loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                pass
        return self.snapshot()

    def stop(self):
        self.request_stop()
        worker = self.worker
        if worker is not None and worker is not threading.current_thread():
            worker.join(timeout=15)
            if worker.is_alive():
                raise StorageError("depth worker shutdown is not confirmed; owner remains held")
        return self.snapshot()

    def close(self):
        self.stop()
        with self.lock:
            self.closed = True
            if self.close_errors:
                raise StorageError("depth worker resources could not all be closed")

    def _worker(self, configurations, token, resume):
        owner = None
        try:
            lock_path = self.root / "state/locks/depth-collection.lock"
            self._guard(lock_path)
            owner = InstanceLock(lock_path)
            asyncio.run(self._run(configurations, token, resume))
        except asyncio.CancelledError:
            pass
        except Exception as error:
            with self.lock:
                if token == self.generation:
                    for row in self.rows.values():
                        row.update(state="blocked", reason=type(error).__name__)
        finally:
            if owner is not None:
                owner.close()
            with self.lock:
                self.loop = self.task = None
                for row in self.rows.values():
                    if row["state"] == "stopping_http":
                        row.update(state="stopped", reason="stopped_after_public_get")
                    if row["state"] in {"queued", "connecting", "synced", "backoff"}:
                        row["state"] = "stopped"

    async def _run(self, configurations, token, resume):
        with self.lock:
            self.loop, self.task = asyncio.get_running_loop(), asyncio.current_task()
        await asyncio.gather(*(self._instrument(row, token, resume) for row in configurations))

    def _public(self, client, instrument):
        expected = (
            "https://api.binance.com"
            if instrument.market is Market.SPOT
            else "https://fapi.binance.com"
        )
        if (
            not isinstance(client, BinanceClient)
            or client.base_url != expected
            or client.credentials is not None
            or client.enable_trading is not False
            or client.budget is None
            or client.ip_scope != "binance-ip"
        ):
            raise ValidationError("depth requires a public matching-origin shared-budget client")

    async def _snapshot_request(self, client, instrument, token):
        self._active(token)
        self._public(client, instrument)
        path = "/api/v3/depth" if instrument.market is Market.SPOT else "/fapi/v1/depth"
        parameters = {"symbol": instrument.symbol, "limit": 1000}
        weight = 50 if instrument.market is Market.SPOT else self.snapshot_weight_usdm

        def request():
            # Only the public GET and its shared HTTP budget leave the owner
            # loop. Archive/store/state remain on the single worker thread.
            reply = client.request_with_response(
                "GET", path, parameters, signed=False, order=False, weight=weight
            )
            return reply, self._clock()

        task = asyncio.create_task(asyncio.to_thread(request))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            with self.lock:
                if token == self.generation:
                    self.rows[instrument.key].update(
                        state="stopping_http", reason="public_get_in_flight"
                    )
            try:
                # Cancellation cannot terminate a running HTTP thread. Confirm
                # completion before resource close; close() has a bounded join
                # and reports an unconfirmed owner if the transport is stuck.
                await asyncio.shield(task)
            except Exception:
                pass
            raise

    def _path(self, instrument):
        path = (
            self.root
            / "state/depth-collection"
            / (sha256(instrument.key.encode()).hexdigest() + ".json")
        )
        self._guard(path)
        return path

    @staticmethod
    def _guard(path):
        for candidate in (path, *path.parents):
            try:
                details = candidate.lstat()
            except FileNotFoundError:
                continue
            if stat.S_ISLNK(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
                raise StorageError("depth collection state cannot use linked paths")
            if candidate != path and not stat.S_ISDIR(details.st_mode):
                raise StorageError("depth collection state parent must be a directory")

    def _load(self, config, store):
        instrument = config["instrument"]
        destination = getattr(store, "destination_id", None)
        if not _hash(destination):
            raise ValidationError("depth store requires an opaque destination identity")
        empty = {
            "schema_version": 1,
            "instrument": instrument.key,
            "source": instrument.key + ":" + SOURCE_SUFFIX,
            "destination_id": destination,
            "price_precision": config["price_precision"],
            "quote_precision": config["quote_precision"],
            "generation": 0,
            "cursor_ms": None,
            "raw_bytes": 0,
            "records": 0,
            "pending": None,
            "last": None,
        }
        path = self._path(instrument)
        try:
            descriptor = os.open(
                path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
            )
            try:
                details = os.fstat(descriptor)
                if (
                    not stat.S_ISREG(details.st_mode)
                    or details.st_size > MAX_STATE_BYTES
                    or getattr(details, "st_file_attributes", 0) & 0x400
                ):
                    raise StorageError("depth collection state must be a bounded regular file")
                stream = os.fdopen(descriptor, "rb")
                descriptor = None
                with stream:
                    raw = stream.read(MAX_STATE_BYTES + 1)
            finally:
                if descriptor is not None:
                    os.close(descriptor)
        except FileNotFoundError:
            return empty
        if len(raw) > MAX_STATE_BYTES:
            raise StorageError("depth state exceeds its byte limit")
        try:
            envelope = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique)
            if (
                type(envelope) is not dict
                or set(envelope) != {"data", "sha256"}
                or envelope["sha256"] != sha256(_json(envelope["data"])).hexdigest()
            ):
                raise ValueError
            data = envelope["data"]
            if (
                type(data) is not dict
                or set(data) != _FIELDS
                or any(
                    data[key] != empty[key] or type(data[key]) is not type(empty[key])
                    for key in (
                        "schema_version",
                        "instrument",
                        "source",
                        "destination_id",
                        "price_precision",
                        "quote_precision",
                    )
                )
            ):
                raise ValueError
            for key, maximum in (
                ("generation", 2**63 - 1),
                ("raw_bytes", self.max_raw_bytes),
                ("records", self.max_minutes),
            ):
                if type(data[key]) is not int or not 0 <= data[key] <= maximum:
                    raise ValueError
            if data["cursor_ms"] is not None and (
                type(data["cursor_ms"]) is not int
                or not 0 <= data["cursor_ms"] <= MAX_TIME
                or data["cursor_ms"] % MINUTE
            ):
                raise ValueError
            if data["last"] is not None:
                self._last(data["last"], data["cursor_ms"])
            if (
                data["records"] == 0
                and (data["last"] is not None or data["cursor_ms"] is not None)
                or data["records"] > 0
                and (data["last"] is None or data["cursor_ms"] is None)
            ):
                raise ValueError
            if data["pending"] is not None:
                self._pending(data, instrument)
            return data
        except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, ValidationError):
            raise StorageError(
                "depth state is corrupt or bound to another plan; original preserved"
            ) from None

    @staticmethod
    def _last(value, cursor):
        if (
            type(value) is not dict
            or set(value) != {"open_ms", "available_at_ms", "revision", "checksum"}
            or any(
                type(value[key]) is not int for key in ("open_ms", "available_at_ms", "revision")
            )
            or value["open_ms"] + MINUTE != cursor
            or not 0 <= value["open_ms"] < cursor <= value["available_at_ms"] <= MAX_TIME
            or not 1 <= value["revision"] <= 2**63 - 1
            or not _hash(value["checksum"])
        ):
            raise ValidationError("invalid depth committed cursor receipt")

    def _pending(self, state, instrument):
        pending = state["pending"]
        if (
            type(pending) is not dict
            or set(pending) != {"payload", "checksum", "raw_refs"}
            or type(pending["payload"]) is not str
            or len(pending["payload"]) > 524288
            or not _hash(pending["checksum"])
            or type(pending["raw_refs"]) is not list
            or len(pending["raw_refs"]) > self.max_raw_refs
            or len(set(pending["raw_refs"])) != len(pending["raw_refs"])
            or any(not _hash(item) for item in pending["raw_refs"])
        ):
            raise ValidationError("invalid fixed depth pending observation")
        payload = bytes.fromhex(pending["payload"])
        if sha256(payload).hexdigest() != pending["checksum"]:
            raise ValidationError("depth pending checksum mismatch")
        found, observation = decode_observation(payload)
        if (
            found != instrument
            or observation.channel != "depth"
            or observation.source != state["source"]
            or observation.available_at_ms < observation.window_end_ms
            or observation.values
            and not pending["raw_refs"]
            or state["cursor_ms"] is not None
            and observation.window_start_ms != state["cursor_ms"]
        ):
            raise ValidationError("depth pending observation does not follow its cursor")
        return observation

    def _save(self, state, instrument, token):
        self._active(token)
        document = {"data": state, "sha256": sha256(_json(state)).hexdigest()}
        if len(_json(document)) > MAX_STATE_BYTES:
            raise StorageError("depth state byte capacity reached")
        try:
            atomic_json(self._path(instrument), document)
        except OSError:
            raise StorageError("depth state commit is unknown; explicit resume required") from None
        self._active(token)

    def _archive(
        self,
        body,
        instrument,
        archive,
        state,
        token,
        *,
        channel,
        parameters,
        headers=(),
        received_at_ms=None,
    ):
        self._active(token)
        if (
            type(body) is not bytes
            or not 0 < len(body) <= 1_048_576
            or state["raw_bytes"] + len(body) > self.max_raw_bytes
        ):
            raise StorageError("depth original-byte quota reached")
        received = self._clock() if received_at_ms is None else received_at_ms
        if type(received) is not int or not 0 <= received <= self._clock():
            raise ProtocolError("invalid original depth receipt clock")
        # Reserve durably before publication so process crashes cannot reset the
        # disk budget. Failed/archive-orphan attempts conservatively consume it.
        state["raw_bytes"] += len(body)
        self._save(state, instrument, token)
        try:
            receipt = archive.put(
                PublicRawResponse(
                    instrument, channel, tuple(parameters), received, body, tuple(headers)
                )
            )
        except Exception:
            raise StorageError(
                "depth archive commit is unknown; original bytes must be inspected"
            ) from None
        self._active(token)
        if not _hash(receipt.archive_id):
            raise StorageError("depth archive returned an invalid identity")
        return receipt.archive_id, received

    def _deliver(self, state, instrument, archive, store, token):
        try:
            self._deliver_pending(state, instrument, archive, store, token)
        except Exception:
            raise StorageError(
                "depth normalization commit/readback is unknown; explicit resume required"
            ) from None

    def _deliver_pending(self, state, instrument, archive, store, token):
        observation = self._pending(state, instrument)
        for identity in state["pending"]["raw_refs"]:
            self._active(token)
            raw = archive.get(identity)
            if (
                raw.record.instrument != instrument
                or raw.record.channel not in {"depth_snapshot", "depth_diff"}
                or raw.receipt.archive_id != identity
                or type(raw.record.body) is not bytes
                or not 0 < len(raw.record.body) <= 1_048_576
                or sha256(raw.record.body).hexdigest() != raw.receipt.body_sha256
                or raw.receipt.received_at_ms != raw.record.received_at_ms
            ):
                raise ProtocolError("depth pending raw identity conflicts with its instrument")
            parameters = dict(raw.record.request_parameters)
            if raw.record.channel == "depth_snapshot":
                if parameters != {"symbol": instrument.symbol, "limit": "1000"}:
                    raise ProtocolError("depth pending snapshot request identity changed")
            elif parameters != {
                "stream": instrument.symbol.lower() + "@depth",
                "generation": str(observation.mapping().get("DEPTH_GENERATION")),
            }:
                raise ProtocolError("depth pending diff generation or stream identity changed")
        existing = store.get(
            instrument, "depth", state["source"], observation.window_start_ms, as_of_ms=MAX_TIME
        )
        self._active(token)
        if existing is None:
            receipt = store.put(instrument, observation, expected_previous=None)
        else:
            receipt = existing
        self._active(token)
        actual = store.get(
            instrument,
            "depth",
            state["source"],
            observation.window_start_ms,
            as_of_ms=observation.available_at_ms,
        )
        if (
            actual is None
            or actual.revision != receipt.revision
            or actual.checksum != receipt.checksum
            or actual.instrument != instrument
            or actual.observation != observation
            or tuple(type(value) for _, value in actual.observation.values)
            != tuple(type(value) for _, value in observation.values)
            or sha256(encode_observation(instrument, actual.observation)).hexdigest()
            != actual.checksum
        ):
            raise ProtocolError("depth commit readback differs from its frozen observation")
        state["cursor_ms"] = observation.window_end_ms
        state["records"] += 1
        state["last"] = {
            "open_ms": observation.window_start_ms,
            "available_at_ms": actual.observation.available_at_ms,
            "revision": actual.revision,
            "checksum": actual.checksum,
        }
        state["pending"] = None
        self._save(state, instrument, token)
        self._row(
            instrument,
            token,
            cursor_ms=state["cursor_ms"],
            quality=observation.quality,
            reason=None if observation.quality == "observed" else observation.quality,
            raw_bytes=state["raw_bytes"],
        )

    def _stage(self, minute, state, instrument, archive, store, token):
        if state["records"] >= self.max_minutes:
            raise StorageError("depth minute journal capacity reached")
        payload = encode_observation(instrument, minute.observation)
        state["pending"] = {
            "payload": payload.hex(),
            "checksum": sha256(payload).hexdigest(),
            "raw_refs": list(minute.raw_refs),
        }
        self._save(state, instrument, token)
        self._deliver(state, instrument, archive, store, token)

    async def _instrument(self, config, token, resume):
        instrument, resources = config["instrument"], None
        try:
            self._active(token)
            resources = self.resource_factory(instrument)
            client, archive, store = resources
            self._public(client, instrument)
            state = self._load(config, store)
            if state["pending"] is not None:
                if not resume:
                    raise StorageError(
                        "fixed depth pending requires explicit resume before new GET"
                    )
                self._deliver(state, instrument, archive, store, token)
            if state["last"] is not None:
                last = state["last"]
                actual = store.get(
                    instrument,
                    "depth",
                    state["source"],
                    last["open_ms"],
                    as_of_ms=last["available_at_ms"],
                )
                if (
                    actual is None
                    or actual.revision != last["revision"]
                    or actual.checksum != last["checksum"]
                ):
                    raise ProtocolError("depth last committed cursor anchor changed")
            sampler = DepthMinuteSampler(
                instrument,
                price_precision=config["price_precision"],
                quote_precision=config["quote_precision"],
                max_raw_refs=self.max_raw_refs,
            )
            opening = (
                state["cursor_ms"]
                if state["cursor_ms"] is not None
                else self._clock() // MINUTE * MINUTE
            )
            if self._clock() - opening > 10 * MINUTE:
                raise StorageError("depth restart gap exceeds the bounded ten-minute window")
            retries = 0
            while True:
                self._active(token)
                try:
                    state["generation"] += 1
                    if state["generation"] > 2**63 - 1:
                        raise StorageError("depth generation capacity reached")
                    self._save(state, instrument, token)
                    self._row(instrument, token, state="connecting")
                    async with self.connect(instrument) as socket:
                        sampler.begin(state["generation"], started_at_ms=self._clock())
                        book, refs, ref_set, last_sample = DepthBook(instrument), [], set(), None
                        snapshot_done = False
                        snapshot_event = None
                        while True:
                            self._active(token)
                            now = self._clock()
                            while opening + MINUTE <= now:
                                minute = sampler.finalize(opening, as_of_ms=now)
                                self._stage(minute, state, instrument, archive, store, token)
                                opening += MINUTE
                                if minute.observation.quality == "observed":
                                    retries = 0
                            try:
                                raw = await asyncio.wait_for(socket.recv(decode=False), timeout=1)
                            except TimeoutError:
                                continue
                            if type(raw) is not bytes:
                                raise ProtocolError(
                                    "depth socket must return original message bytes"
                                )
                            socket_received = self._clock()
                            row, _ = _payload(raw)
                            payload = row.get("data", row)
                            if payload.get("e") == "serverShutdown":
                                raise TransportError("public depth server requested reconnect")
                            name = row.get("stream", instrument.symbol.lower() + "@depth")
                            if name != instrument.symbol.lower() + "@depth":
                                raise ProtocolError("depth stream symbol binding mismatch")
                            identity, received = self._archive(
                                raw,
                                instrument,
                                archive,
                                state,
                                token,
                                channel="depth_diff",
                                parameters=(
                                    ("stream", name),
                                    ("generation", str(state["generation"])),
                                ),
                                received_at_ms=socket_received,
                            )
                            if identity not in ref_set:
                                refs.append(identity)
                                ref_set.add(identity)
                            if len(refs) > self.max_raw_refs:
                                raise StorageError("depth original-reference quota reached")
                            book.receive(raw, generation=book.generation)
                            if not snapshot_done:
                                reply, snapshot_received = await self._snapshot_request(
                                    client, instrument, token
                                )
                                headers = tuple(
                                    (key.lower(), value)
                                    for key, value in reply.response.headers.items()
                                    if key.lower() in {"date", "content-type", "retry-after"}
                                    or re.fullmatch(
                                        r"x-mbx-used-weight(?:-\d+[smhd])?", key.lower()
                                    )
                                )
                                identity, _ = self._archive(
                                    reply.response.body,
                                    instrument,
                                    archive,
                                    state,
                                    token,
                                    channel="depth_snapshot",
                                    parameters=(("symbol", instrument.symbol), ("limit", "1000")),
                                    headers=headers,
                                    received_at_ms=snapshot_received,
                                )
                                if identity not in ref_set:
                                    refs.append(identity)
                                    ref_set.add(identity)
                                book.snapshot(reply.response.body, generation=book.generation)
                                snapshot_data, _ = _payload(reply.response.body)
                                for field in ("E", "T"):
                                    stamp = snapshot_data.get(field)
                                    if stamp is not None and (
                                        type(stamp) is not int or not 0 <= stamp <= MAX_TIME
                                    ):
                                        raise ProtocolError(
                                            "depth snapshot native clock is invalid"
                                        )
                                snapshot_event = snapshot_data.get("T", snapshot_data.get("E"))
                                snapshot_done = True
                            if book.state != "synced":
                                continue
                            event = (
                                payload.get("T")
                                if instrument.market is Market.USDM
                                else payload.get("E")
                            )
                            if type(event) is not int or not 0 <= event <= MAX_TIME:
                                raise ProtocolError("depth event lacks its native source clock")
                            if snapshot_event is not None and event < snapshot_event:
                                continue  # Await a real event; don't relabel a buffered old minute.
                            if event - received > 2000:
                                raise ProtocolError(
                                    "depth source event exceeds the bounded clock tolerance"
                                )
                            deadline = time.monotonic() + 2
                            while event > self._clock():
                                self._active(token)
                                if event - received > 2000 or time.monotonic() >= deadline:
                                    raise ProtocolError("depth source clock remains in the future")
                                await asyncio.sleep(0.02)
                            accepted = self._clock()
                            if (
                                last_sample is None
                                or event // MINUTE != last_sample // MINUTE
                                or event - last_sample >= 1000
                            ):
                                sampler.observe(
                                    book.view(),
                                    event_time_ms=event,
                                    received_at_ms=received,
                                    accepted_at_ms=accepted,
                                    generation=state["generation"],
                                    raw_refs=tuple(refs),
                                )
                                last_sample = event
                            self._row(
                                instrument, token, state="synced", generation=state["generation"]
                            )
                except (ProtocolError, TransportError, RateLimited, OSError) as error:
                    sampler.break_stream("sequence_or_transport_gap")
                    retries += 1
                    if retries > self.max_retries:
                        raise StorageError("depth reconnect retry budget exhausted") from None
                    wait = min(300, self.retry_base_s * 2 ** (retries - 1))
                    if isinstance(error, RateLimited) and error.retry_after is not None:
                        wait = max(wait, error.retry_after)
                    self._row(
                        instrument,
                        token,
                        state="backoff",
                        reason=type(error).__name__,
                        retry_seconds=wait,
                    )
                    await asyncio.sleep(wait)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._row(instrument, token, state="unknown", reason=type(error).__name__)
        finally:
            if resources is not None:
                client, archive, store = resources
                for value in (store, archive, getattr(client, "transport", None)):
                    try:
                        if callable(getattr(value, "close", None)):
                            value.close()
                    except Exception:
                        with self.lock:
                            self.close_errors.append("resource_close_failed")
