"""Independent, bounded OHLCV CA execution with frozen plans and exact checkpoints."""

import json
import os
import re
import tempfile
import threading
import time
from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.analysis import catalog
from omega.analysis.warmup import CHUNK_MINUTES, MAX_WARMUP, MINUTE, aligned_window
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.models import Candle, Instrument, Market
from omega.runtime.analysis_manifest import AnalysisInputManifest, InputPolicy, frame_digest
from omega.runtime.analysis_replay import CHUNK_KEYS, FRAME_KEYS, replay, validate_head
from omega.runtime.settings import atomic_json

MAX_BYTES = 8_000_000
_SNAPSHOT_KEYS = {
    "analysis_id",
    "analysis_revision",
    "template_id",
    "template_revision",
    "instrument",
    "configuration",
    "configuration_hash",
}
_ACTIVE = {"queued", "preparing", "running", "blocked"}
_STATES = _ACTIVE | {"stopped", "error"}


class _InputPersistenceError(StorageError):
    pass


class _InputsNotReady(ProtocolError):
    pass


def _encoded(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")


def _digest(value):
    return sha256(_encoded(value)).hexdigest()


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate analysis checkpoint field")
        result[key] = value
    return result


def _identity(value):
    if type(value) is not str or re.fullmatch("[a-f0-9]{32}", value) is None:
        raise ValidationError("invalid analysis execution identity")
    return value


def _instrument(value):
    try:
        provider, market, symbol = value.split(":")
        result = Instrument(provider, Market(market), symbol)
        if result.provider != "binance" or result.market not in {Market.SPOT, Market.USDM}:
            raise ValueError
        return result
    except (ValueError, AttributeError, TypeError):
        raise ValidationError("unsupported analysis instrument") from None


def _snapshot(value):
    if type(value) is not dict or set(value) != _SNAPSHOT_KEYS:
        raise ValidationError("a frozen library analysis snapshot is required")
    _identity(value["analysis_id"])
    _identity(value["template_id"])
    _instrument(value["instrument"])
    if (
        any(
            type(value[key]) is not int or value[key] < 1
            for key in ("analysis_revision", "template_revision")
        )
        or catalog.validate_configuration(value["configuration"]) != value["configuration"]
        or _digest(value["configuration"]) != value["configuration_hash"]
    ):
        raise ValidationError("analysis configuration snapshot changed or is invalid")
    return deepcopy(value)


def _candle(value):
    return {
        "open_ms": value.open_time_ms,
        **{key: str(getattr(value, key)) for key in ("open", "high", "low", "close", "volume")},
    }


def _decode_candle(value, instrument):
    if type(value) is not dict or set(value) != {
        "open_ms",
        "open",
        "high",
        "low",
        "close",
        "volume",
    }:
        raise ValidationError("invalid analysis execution last candle")
    if (
        type(value["open_ms"]) is not int
        or not 0 <= value["open_ms"] <= 253402300739000
        or value["open_ms"] % MINUTE
    ):
        raise ValidationError("invalid analysis execution candle timestamp")
    return Candle(
        instrument,
        value["open_ms"],
        value["open_ms"] + MINUTE,
        *(Decimal(value[key]) for key in ("open", "high", "low", "close", "volume")),
    )


def _outputs(values):
    if any(isinstance(value, Decimal) and not value.is_finite() for value in values.values()):
        raise ValidationError("analysis produced a non-finite Decimal output")
    result = {
        key: str(value) if isinstance(value, Decimal) else value for key, value in values.items()
    }
    _encoded(result)
    return result


class AnalysisExecution:
    def __init__(
        self,
        root,
        source_factory,
        *,
        clock_ms=None,
        feature_factory=None,
        background=True,
        monotonic=None,
        max_instances=100,
        input_policy_factory=None,
        manifest_factory=None,
    ):
        if (
            not callable(source_factory)
            or type(background) is not bool
            or type(max_instances) is not int
            or not 1 <= max_instances <= 512
            or input_policy_factory is not None
            and not callable(input_policy_factory)
            or manifest_factory is not None
            and not callable(manifest_factory)
        ):
            raise ValidationError("invalid analysis execution owner settings")
        self.root = Path(root) / "state/analysis-executions"
        self.input_policy_factory = input_policy_factory
        self.manifest_factory = manifest_factory or (lambda: AnalysisInputManifest(root))
        self.manifest, self.manifest_thread = None, None
        self.source_factory = source_factory
        self.clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self.monotonic = monotonic or time.monotonic
        self.feature_factory = feature_factory or catalog.create_features
        self.background, self.max_instances = background, max_instances
        self.lock, self.tick_lock, self.thread_lock = (
            threading.RLock(),
            threading.Lock(),
            threading.Lock(),
        )
        self.event, self.stop_event = threading.Event(), threading.Event()
        self.thread, self.source, self.source_thread = None, None, None
        self.closed = False
        self.worker_error = None
        self.jobs, self.features, self.results, self.load_errors = {}, {}, {}, {}
        self.due, self.failures, self.order = {}, {}, []
        self.needs_anchor = {}
        self._load()

    def _now(self):
        value = self.clock_ms()
        if type(value) is not int or not 0 <= value <= 253_402_300_799_000:
            raise ValidationError("invalid actual analysis clock")
        return value

    def _graph(self, snapshot):
        fields = catalog.configured_output_fields(snapshot["configuration"])
        # Saved configurations compose every requested producer; a CA doesn't
        # need to consume the TS family's bounded required-field budget itself.
        fields = frozenset(field for field in fields if field.startswith("CLOSED_"))
        return self.feature_factory(
            _instrument(snapshot["instrument"]), fields, snapshot["configuration"]
        )

    def _path(self, identity):
        return self.root / (_identity(identity) + ".json")

    def _persist(self, job):
        envelope = {"data": job, "sha256": _digest(job)}
        if len(_encoded(envelope)) > MAX_BYTES:
            raise StorageError("analysis checkpoint exceeds its bounded size")
        path = self._path(job["snapshot"]["analysis_id"])
        if path.is_symlink():
            raise StorageError("analysis checkpoint cannot be a symlink")
        try:
            atomic_json(path, envelope)
        except OSError:
            raise StorageError("analysis checkpoint could not be saved") from None

    def _restore(self, job, identity):
        fields = {
            "schema_version",
            "snapshot",
            "execution_id",
            "generation",
            "state",
            "reason",
            "start_ms",
            "target_ms",
            "next_ms",
            "alignment_minutes",
            "checkpoint",
            "last_candle",
        }
        schema = job.get("schema_version") if type(job) is dict else None
        if schema == 2:
            fields |= {"input_policy", "manifest_head", "pending_input"}
        if (
            type(job) is not dict
            or set(job) != fields
            or type(job["schema_version"]) is not int
            or job["schema_version"] not in {1, 2}
        ):
            raise ValidationError("invalid analysis execution checkpoint fields")
        frozen = _snapshot(job["snapshot"])
        _identity(job["execution_id"])
        if (
            frozen["analysis_id"] != identity
            or type(job["generation"]) is not int
            or not 0 <= job["generation"] < 2**63
            or job["state"] not in _STATES
            or type(job["reason"]) is not str
            or len(job["reason"]) > 160
            or any(
                type(job[key]) is not int or job[key] < 0 or job[key] % MINUTE
                for key in ("start_ms", "target_ms", "next_ms")
            )
            or not job["start_ms"] < job["target_ms"] <= 253_402_300_799_000
            or not job["start_ms"] <= job["next_ms"] <= 253_402_300_799_000
            or (job["target_ms"] - job["start_ms"]) // MINUTE > MAX_WARMUP
        ):
            raise ValidationError("inconsistent analysis execution checkpoint")
        graph = self._graph(frozen).restored(job["checkpoint"], _instrument(frozen["instrument"]))
        if graph.warmup_alignment_minutes != job["alignment_minutes"] or job["start_ms"] % (
            job["alignment_minutes"] * MINUTE
        ):
            raise ValidationError("analysis warmup alignment changed")
        expected = None if job["next_ms"] == job["start_ms"] else job["next_ms"] - MINUTE
        if graph.last_open_time_ms != expected or (job["last_candle"] is None) != (
            expected is None
        ):
            raise ValidationError("analysis graph and delivery cursor disagree")
        result = {}
        if expected is not None:
            last = _decode_candle(job["last_candle"], _instrument(frozen["instrument"]))
            if last.open_time_ms != expected:
                raise ValidationError("analysis last candle does not match its cursor")
            if graph.last_candle != last:
                raise ValidationError("analysis checkpoint last candle changed")
            result = _outputs(graph.output_values)
        if schema == 2:
            self._validate_inputs(job, graph)
        return graph, result

    def _validate_inputs(self, job, graph):
        policy = InputPolicy.from_document(job["input_policy"])
        reader = self.manifest_factory()
        try:
            identity = job["snapshot"]["analysis_id"]
            head = job["manifest_head"]
            if (head is None) != (job["next_ms"] == job["start_ms"]):
                raise ValidationError("analysis input head and cursor disagree")
            if head is not None:
                selected = validate_head(
                    reader, head, expected_analysis_id=identity, expected_policy=policy
                )
                self._input_attempt(job, selected)
                if selected["end_ms"] != job["next_ms"]:
                    raise ValidationError("analysis consumed manifest cursor changed")
                chunk = self._input_header(reader, selected, policy, job["snapshot"]["instrument"])
                if chunk["frames"][-1]["candle"] != {
                    "instrument": job["snapshot"]["instrument"],
                    **job["last_candle"],
                }:
                    raise ValidationError("analysis manifest last candle differs from checkpoint")
                last_frame = getattr(graph, "last_input_frame", None)
                if (
                    last_frame is not None
                    and frame_digest(last_frame) != chunk["frames"][-1]["frame_digest"]
                ):
                    raise ValidationError("analysis graph frame differs from its pinned inputs")
            pending = job["pending_input"]
            if pending is not None:
                selected = validate_head(
                    reader, pending, expected_analysis_id=identity, expected_policy=policy
                )
                self._input_attempt(job, selected)
                self._input_header(reader, selected, policy, job["snapshot"]["instrument"])
                if selected["start_ms"] != job["next_ms"] or selected["previous"] != head:
                    raise ValidationError("analysis staged input range or predecessor changed")
        finally:
            reader.close()

    @staticmethod
    def _input_header(reader, head, policy, instrument_key):
        chunk = reader.read(head["source"])
        if (
            type(chunk) is not dict
            or set(chunk) != CHUNK_KEYS
            or type(chunk["schema_version"]) is not int
            or chunk["schema_version"] != 1
            or chunk["kind"] != "source_chunk"
            or chunk["instrument"] != instrument_key
            or InputPolicy.from_document(chunk["policy"]) != policy
            or type(chunk["pins"]) is not list
            or len(chunk["pins"]) > 4096
            or type(chunk["frames"]) is not list
            or len(chunk["frames"]) != (head["end_ms"] - head["start_ms"]) // MINUTE
        ):
            raise ValidationError("analysis input source header changed")
        instrument = _instrument(instrument_key)
        for index, row in enumerate(chunk["frames"]):
            if (
                type(row) is not dict
                or set(row) != FRAME_KEYS
                or type(row["frame_digest"]) is not str
                or re.fullmatch("[a-f0-9]{64}", row["frame_digest"]) is None
                or type(row["candle"]) is not dict
                or row["candle"].get("instrument") != instrument_key
                or type(row["pins"]) is not list
                or len(row["pins"]) > 8
                or any(
                    type(index) is not int or not 0 <= index < len(chunk["pins"])
                    for index in row["pins"]
                )
                or len(set(row["pins"])) != len(row["pins"])
            ):
                raise ValidationError("invalid analysis input frame header")
            value = {key: item for key, item in row["candle"].items() if key != "instrument"}
            if (
                type(value.get("open_ms")) is not int
                or value["open_ms"] != head["start_ms"] + index * MINUTE
                or any(
                    type(value.get(key)) is not str or len(value[key]) > 512
                    for key in ("open", "high", "low", "close", "volume")
                )
            ):
                raise ValidationError("invalid analysis manifest candle header")
            candle = _decode_candle(value, instrument)
            if (
                type(row["as_of_ms"]) is not int
                or not candle.close_time_ms <= row["as_of_ms"] <= 253402300799000
                or row["reference_time_ms"] is not None
                and (
                    type(row["reference_time_ms"]) is not int
                    or row["reference_time_ms"] != candle.close_time_ms
                )
            ):
                raise ValidationError("invalid analysis manifest frame clocks")
        return chunk

    @staticmethod
    def _input_attempt(job, head):
        if head["generation"] > job["generation"] or (
            head["generation"] == job["generation"] and head["execution_id"] != job["execution_id"]
        ):
            raise ValidationError("analysis input belongs to a different staging attempt")

    def _input_policy(self, frozen):
        policy = self.input_policy_factory(deepcopy(frozen)) if self.input_policy_factory else None
        if policy is not None and not isinstance(policy, InputPolicy):
            raise ValidationError("analysis input policy factory returned an invalid policy")
        if policy is not None and (
            policy.open_interest_source_suffix != "open_interest_history"
            or policy.long_short_source_suffix != "long_short_ratio"
        ):
            raise ValidationError("analysis execution does not support custom metric datasets yet")
        return policy

    def _archive_checkpoint(self, identity):
        source = self._path(identity)
        directory = self.root.parent / "analysis-execution-history"
        temporary = None
        try:
            if (
                source.is_symlink()
                or directory.is_symlink()
                or getattr(directory, "is_junction", lambda: False)()
            ):
                raise OSError("linked analysis execution history")
            raw = source.read_bytes()
            if len(raw) > MAX_BYTES:
                raise OSError("oversize analysis checkpoint")
            envelope = json.loads(raw, object_pairs_hook=_unique)
            if (
                type(envelope) is not dict
                or set(envelope) != {"data", "sha256"}
                or _digest(envelope["data"]) != envelope["sha256"]
            ):
                raise OSError("invalid original analysis checkpoint")
            self._restore(envelope["data"], identity)
            directory.mkdir(parents=True, exist_ok=True)
            target = directory / (identity + "-" + uuid4().hex + ".json")
            with tempfile.NamedTemporaryFile(dir=directory, delete=False) as stream:
                temporary = Path(stream.name)
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, target)
            temporary = None
            if os.name != "nt":
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        except (OSError, ValueError, TypeError, KeyError, ValidationError, StorageError):
            raise StorageError(
                "previous analysis checkpoint could not be archived; restart aborted"
            ) from None
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def _load(self):
        for index, path in enumerate(sorted(self.root.glob("*.json"))):
            identity = path.stem
            try:
                _identity(identity)
                if index >= self.max_instances or path.is_symlink():
                    raise ValueError
                with path.open("rb") as stream:
                    raw = stream.read(MAX_BYTES + 1)
                if len(raw) > MAX_BYTES:
                    raise ValueError
                envelope = json.loads(raw, object_pairs_hook=_unique)
                if (
                    type(envelope) is not dict
                    or set(envelope) != {"data", "sha256"}
                    or _digest(envelope["data"]) != envelope["sha256"]
                ):
                    raise ValueError
                job = envelope["data"]
                graph, result = self._restore(job, identity)
                job.update(state="stopped", reason="restart_requires_manual_start")
                self.jobs[identity], self.features[identity], self.results[identity] = (
                    job,
                    graph,
                    result,
                )
                self.order.append(identity)
            except Exception:
                self.load_errors[identity] = "invalid_execution_checkpoint_preserved"

    def _public(self, identity):
        job = self.jobs[identity]
        frozen = job["snapshot"]
        fields = catalog.configured_output_fields(frozen["configuration"])
        values = self.results.get(identity, {})
        graph = self.features[identity]
        nullable = graph.nullable_fields
        calculation_ready = graph.calculation_ready
        input_ready = bool(getattr(graph, "input_ready", True))
        missing = sorted(
            field
            for field in fields
            if not field.startswith("CLOSED_")
            and field not in nullable
            and values.get(field) is None
        )
        last_closed = job["next_ms"] if job["last_candle"] is not None else None
        try:
            now = self._now()
            fresh = last_closed is not None and last_closed <= now and now - last_closed <= 120_000
        except ValidationError:
            fresh = False
        prepared = job["next_ms"] >= job["target_ms"]
        ready = (
            not self.closed
            and job["state"] == "running"
            and prepared
            and not missing
            and calculation_ready
            and fresh
            and job.get("pending_input") is None
        )
        return {
            "analysis_id": identity,
            "instrument": frozen["instrument"],
            "execution_id": job["execution_id"],
            "generation": job["generation"],
            "state": job["state"],
            "analysis_revision": frozen["analysis_revision"],
            "configuration_hash": frozen["configuration_hash"],
            "ready": ready,
            "last_closed_ms": last_closed,
            "reason": job["reason"]
            if not prepared or job["state"] != "running"
            else "inputs_not_ready"
            if not input_ready
            else "warming_indicators"
            if not calculation_ready
            else "outputs_not_ready"
            if missing
            else "stale"
            if not fresh
            else "ready",
            "outputs": deepcopy(values),
            "warmup": {
                "start_ms": job["start_ms"],
                "target_ms": job["target_ms"],
                "cursor_ms": min(job["next_ms"], job["target_ms"]),
                "processed_minutes": (min(job["next_ms"], job["target_ms"]) - job["start_ms"])
                // MINUTE,
                "total_minutes": (job["target_ms"] - job["start_ms"]) // MINUTE,
                "alignment_minutes": job["alignment_minutes"],
                "chunk_minutes": CHUNK_MINUTES,
            },
            "input_quality": {
                "missing_outputs": missing,
                "nullable_outputs": sorted(
                    field for field in nullable if values.get(field) is None
                ),
                "calculation_ready": calculation_ready,
                "input_ready": input_ready,
                "fresh": fresh,
                "contiguous": job["state"] != "blocked",
                "source": "committed_frames" if job["schema_version"] == 2 else "committed_ohlcv",
                "auxiliary_subscription": job["schema_version"] == 2,
                **(
                    {
                        "knowledge_mode": job["input_policy"]["knowledge_mode"],
                        "manifest_head": job["manifest_head"] is not None,
                        "pending_input": job["pending_input"] is not None,
                    }
                    if job["schema_version"] == 2
                    else {}
                ),
            },
        }

    def snapshot(self, identity=None):
        with self.lock:
            if identity is not None and identity not in self.jobs:
                raise ValidationError("analysis execution not found")
            return {
                "schema_version": 1,
                "instances": [self._public(key) for key in ([identity] if identity else self.jobs)],
                "load_errors": [
                    {"analysis_id": key, "reason": reason}
                    for key, reason in self.load_errors.items()
                ],
                "worker_alive": bool(self.thread and self.thread.is_alive()),
                "worker_error": self.worker_error,
            }

    def start(self, snapshot, warmup_minutes=240, restart=False):
        frozen = _snapshot(snapshot)
        if (
            type(restart) is not bool
            or type(warmup_minutes) is not int
            or not 1 <= warmup_minutes <= MAX_WARMUP
        ):
            raise ValidationError("invalid analysis preparation settings")
        identity = frozen["analysis_id"]
        policy = self._input_policy(frozen)
        with self.lock:
            if self.closed:
                raise ValidationError("analysis execution owner is closed")
            if identity in self.load_errors:
                raise ValidationError(
                    "preserve and recover the invalid checkpoint before starting this CA"
                )
            previous = self.jobs.get(identity)
            if (
                previous is not None
                and previous.get("input_policy") != (policy.document() if policy else None)
                and not restart
            ):
                raise ValidationError("changed analysis input policy requires explicit restart")
            if previous is not None and previous["snapshot"] != frozen and not restart:
                raise ValidationError("changed CA snapshot requires explicit restart")
            if previous is not None and previous["state"] in _ACTIVE and not restart:
                return self._public(identity)
            if previous is None and len(self.jobs) >= self.max_instances:
                raise ValidationError("analysis execution capacity reached")
            if previous is not None and previous["generation"] >= 2**63 - 1:
                raise ValidationError("analysis execution generation capacity reached")
            if previous is not None and not restart:
                job, graph, result = (
                    deepcopy(previous),
                    self.features[identity],
                    self.results[identity],
                )
            else:
                graph = self._graph(frozen)
                target = self._now() // MINUTE * MINUTE
                start, target = aligned_window(
                    target, warmup_minutes, graph.warmup_alignment_minutes
                )
                job = {
                    "schema_version": 1,
                    "snapshot": frozen,
                    "execution_id": uuid4().hex,
                    "generation": 0,
                    "state": "queued",
                    "reason": "waiting_for_worker",
                    "start_ms": start,
                    "target_ms": target,
                    "next_ms": start,
                    "alignment_minutes": graph.warmup_alignment_minutes,
                    "checkpoint": graph.checkpoint(),
                    "last_candle": None,
                }
                result = {}
                if policy is not None:
                    job.update(
                        schema_version=2,
                        input_policy=policy.document(),
                        manifest_head=None,
                        pending_input=None,
                    )
            job.update(
                execution_id=uuid4().hex,
                generation=0 if previous is None else previous["generation"] + 1,
                state="queued",
                reason="waiting_for_worker",
            )
            if previous is not None and restart:
                self._archive_checkpoint(identity)
            self._persist(job)
            self.jobs[identity], self.features[identity], self.results[identity] = (
                job,
                graph,
                deepcopy(result),
            )
            if identity not in self.order:
                self.order.append(identity)
            self.due[identity], self.failures[identity] = 0, 0
            self.needs_anchor[identity] = job["last_candle"] is not None
            response = self._public(identity)
        self.notify()
        return response

    def stop(self, identity, expected_execution_id=None):
        with self.lock:
            if self.closed:
                raise ValidationError("analysis execution owner is closed")
            if identity not in self.jobs:
                raise ValidationError("analysis execution not found")
            previous = self.jobs[identity]
            if (
                expected_execution_id is not None
                and expected_execution_id != previous["execution_id"]
            ):
                raise ValidationError("stale analysis execution stop request")
            if previous["state"] == "stopped":
                return self._public(identity)
            if previous["generation"] >= 2**63 - 1:
                raise ValidationError("analysis execution generation capacity reached")
            candidate = deepcopy(previous)
            candidate.update(
                state="stopped", reason="user_stopped", generation=candidate["generation"] + 1
            )
            # Fence late reads before touching persistence; a save failure never keeps it active.
            self.jobs[identity] = candidate
            try:
                self._persist(candidate)
            except StorageError:
                self.jobs[identity].update(
                    state="error", reason="checkpoint_persistence_unavailable"
                )
                raise
            return self._public(identity)

    def notify(self):
        with self.lock:
            if self.closed:
                return
            self.due = {key: 0 for key in self.due}
        self.event.set()
        if self.background:
            with self.thread_lock:
                if self.thread is None or not self.thread.is_alive():
                    self.thread = threading.Thread(
                        target=self._worker, name="Omega-AnalysisExecution", daemon=True
                    )
                    self.thread.start()

    def _source(self):
        if self.source is None:
            self.source = self.source_factory()
            self.source_thread = threading.get_ident()
        elif self.source_thread != threading.get_ident():
            raise ValidationError("analysis source belongs to another worker thread")
        return self.source

    def _manifest(self):
        if self.manifest is None:
            self.manifest = self.manifest_factory()
            self.manifest_thread = threading.get_ident()
        elif self.manifest_thread != threading.get_ident():
            raise ValidationError("analysis manifest belongs to another worker thread")
        return self.manifest

    def _current(self, identity, job):
        current = self.jobs[identity]
        return (
            not self.closed
            and current["execution_id"] == job["execution_id"]
            and current["generation"] == job["generation"]
            and current["state"] in _ACTIVE
        )

    def _persist_input_job(self, job):
        try:
            self._persist(job)
        except StorageError:
            raise _InputPersistenceError("analysis input checkpoint could not be saved") from None

    @staticmethod
    def _input_source_call(operation, *args, **kwargs):
        try:
            return operation(*args, **kwargs)
        except ValidationError:
            raise ProtocolError("analysis input source binding or policy is unavailable") from None

    def _run_inputs(self, identity, job, graph, now, ending):
        instrument = _instrument(job["snapshot"]["instrument"])
        policy = InputPolicy.from_document(job["input_policy"])
        source = self._source()
        self._input_source_call(source.check_destination, policy.destination_id)
        opening = job["next_ms"]
        if job["last_candle"] is not None:
            rows = self._input_source_call(
                source.read, (instrument.key,), opening - MINUTE, opening, limit=1, as_of_ms=now
            )
            if (
                len(rows) != 1
                or _candle(rows[0]) != job["last_candle"]
                or rows[0].close_time_ms > now
            ):
                raise ProtocolError("analysis durable source anchor changed")
        if self.needs_anchor.get(identity, False) and job["manifest_head"] is not None:
            restored_frames = replay(
                self._manifest(),
                job["manifest_head"],
                source,
                expected_analysis_id=identity,
                expected_policy=policy,
            )
            if _candle(restored_frames[-1].frame.candle) != job["last_candle"]:
                raise ProtocolError("analysis pinned head differs from its graph anchor")
            last_frame = getattr(graph, "last_input_frame", None)
            if last_frame is not None and last_frame != restored_frames[-1].frame:
                raise ProtocolError("analysis graph and pinned frame inputs differ")
        pending = job["pending_input"]
        if pending is not None:
            head = validate_head(
                self._manifest(), pending, expected_analysis_id=identity, expected_policy=policy
            )
            self._input_attempt(job, head)
            if head["start_ms"] != opening or head["previous"] != job["manifest_head"]:
                raise ProtocolError("analysis pending input cursor changed")
            frames = replay(
                self._manifest(),
                pending,
                source,
                expected_analysis_id=identity,
                expected_policy=policy,
            )
            ending = head["end_ms"]
        elif ending > opening:
            frames = tuple(
                self._input_source_call(
                    source.read_frames,
                    instrument,
                    opening,
                    ending,
                    as_of_ms=now,
                    channels=policy.channels,
                    maximum_metric_age_ms=policy.maximum_metric_age_ms,
                    retrospective_until_ms=job["target_ms"],
                    limit=CHUNK_MINUTES,
                )
            )
        else:
            frames = ()
        with self.lock:
            if not self._current(identity, job):
                return False
        if len(frames) != (ending - opening) // MINUTE:
            raise ProtocolError("analysis frame source minute gap")
        candidate = graph.restored(job["checkpoint"], instrument)
        output = self.results.get(identity, {})
        for index, prepared in enumerate(frames):
            frame = prepared.frame
            if (
                frame.candle.instrument != instrument
                or frame.candle.open_time_ms != opening + index * MINUTE
                or frame.candle.close_time_ms > now
                or frame.as_of_ms > now
            ):
                raise ProtocolError("analysis input frames are not contiguous known closed minutes")
            operation = getattr(candidate, "update_frame", None)
            output = _outputs(
                operation(frame) if callable(operation) else candidate.update(frame.candle)
            )
            if not getattr(candidate, "input_ready", True):
                raise _InputsNotReady("required auxiliary observations are unavailable")
        if frames and pending is None:
            with self.lock:
                if not self._current(identity, job):
                    return False
            reference = self._manifest().stage(
                identity,
                job["execution_id"],
                job["generation"],
                frames,
                policy,
                previous=job["manifest_head"],
            )
            pending = reference.document()
            staged = deepcopy(job)
            staged["pending_input"] = pending
            with self.lock:
                if not self._current(identity, job):
                    return False  # An orphan source record never consumes an input minute.
                self._persist_input_job(staged)
                self.jobs[identity] = staged
        completed = deepcopy(job)
        completed.update(
            next_ms=ending,
            checkpoint=candidate.checkpoint(),
            last_candle=_candle(frames[-1].frame.candle) if frames else job["last_candle"],
            manifest_head=pending if frames else job["manifest_head"],
            pending_input=None,
            state="preparing" if ending < job["target_ms"] else "running",
            reason="preparing_snapshot" if ending < job["target_ms"] else "running",
        )
        with self.lock:
            if not self._current(identity, job):
                return False
            self._persist_input_job(completed)
            self.jobs[identity], self.features[identity], self.results[identity] = (
                completed,
                candidate,
                output,
            )
            self.due[identity], self.failures[identity] = 0, 0
            self.needs_anchor[identity] = False
        return True

    def run_once(self):
        if not self.tick_lock.acquire(blocking=False):
            return False
        try:
            with self.lock:
                now, selected = self._now(), None
                for identity in self.order:
                    job = self.jobs[identity]
                    if (
                        job["state"] in _ACTIVE
                        and self.due.get(identity, 0) <= self.monotonic()
                        and (
                            self.needs_anchor.get(identity, False)
                            or job.get("pending_input") is not None
                            or job["next_ms"] < now // MINUTE * MINUTE
                        )
                    ):
                        selected = identity
                        break
                if selected is None or self.closed:
                    return False
                self.order.remove(selected)
                self.order.append(selected)
                job = deepcopy(self.jobs[selected])
                graph = self.features[selected]
            opening = job["next_ms"]
            preparing = opening < job["target_ms"]
            ending = min(
                job["target_ms"] if preparing else now // MINUTE * MINUTE,
                opening + (CHUNK_MINUTES if preparing else 16) * MINUTE,
            )
            ending = max(opening, ending)
            overlap = MINUTE if job["last_candle"] is not None else 0
            persisting = False
            try:
                if job["schema_version"] == 2:
                    return self._run_inputs(selected, job, graph, now, ending)
                instrument = _instrument(job["snapshot"]["instrument"])
                rows = tuple(
                    self._source().read((instrument.key,), opening - overlap, ending, limit=4096)
                )
                with self.lock:
                    current = self.jobs[selected]
                    if (
                        self.closed
                        or current["execution_id"] != job["execution_id"]
                        or current["generation"] != job["generation"]
                        or current["state"] not in _ACTIVE
                    ):
                        return False
                if overlap:
                    if not rows or _candle(rows[0]) != job["last_candle"]:
                        raise ProtocolError("analysis durable source anchor changed")
                    anchor = rows[0]
                    rows = rows[1:]
                if len(rows) != (ending - opening) // MINUTE:
                    raise ProtocolError("analysis source minute gap")
                accepted = self._now()
                if overlap and anchor.close_time_ms > accepted:
                    raise ProtocolError("analysis checkpoint anchor is ahead of the actual clock")
                candidate = graph.restored(job["checkpoint"], instrument)
                output = self.results.get(selected, {})
                for index, candle in enumerate(rows):
                    if (
                        not isinstance(candle, Candle)
                        or candle.instrument != instrument
                        or candle.open_time_ms != opening + index * MINUTE
                        or candle.close_time_ms > accepted
                    ):
                        raise ProtocolError("analysis requires contiguous closed observed minutes")
                    output = _outputs(candidate.update(candle))
                job.update(
                    next_ms=ending,
                    checkpoint=candidate.checkpoint(),
                    last_candle=_candle(rows[-1]) if rows else job["last_candle"],
                    state="preparing" if ending < job["target_ms"] else "running",
                    reason="preparing_snapshot" if ending < job["target_ms"] else "running",
                )
                with self.lock:
                    current = self.jobs[selected]
                    if (
                        self.closed
                        or current["execution_id"] != job["execution_id"]
                        or current["generation"] != job["generation"]
                        or current["state"] not in _ACTIVE
                    ):
                        return False
                    persisting = True
                    self._persist(job)
                    self.jobs[selected], self.features[selected], self.results[selected] = (
                        job,
                        candidate,
                        output,
                    )
                    self.due[selected], self.failures[selected] = 0, 0
                    self.needs_anchor[selected] = False
                return True
            except Exception as exc:
                with self.lock:
                    current = self.jobs[selected]
                    if (
                        not self.closed
                        and current["execution_id"] == job["execution_id"]
                        and current["generation"] == job["generation"]
                        and current["state"] in _ACTIVE
                    ):
                        persisting = persisting or isinstance(exc, _InputPersistenceError)
                        blocked = not persisting and isinstance(exc, (ProtocolError, StorageError))
                        current.update(
                            state="blocked" if blocked else "error",
                            reason="checkpoint_persistence_unavailable"
                            if persisting
                            else "auxiliary_inputs_not_ready"
                            if isinstance(exc, _InputsNotReady)
                            else "source_gap_or_unavailable"
                            if blocked
                            else "calculation_failed",
                        )
                        self.failures[selected] = self.failures.get(selected, 0) + 1
                        self.due[selected] = self.monotonic() + min(
                            30, 2 ** min(5, self.failures[selected] - 1)
                        )
                return False
        finally:
            if self.closed and self.source_thread == threading.get_ident():
                self._close_source()
            self.tick_lock.release()

    def _close_source(self):
        source, self.source = self.source, None
        manifest, self.manifest = self.manifest, None
        try:
            if source is not None:
                source.close()
        except Exception:
            self.worker_error = "analysis_source_close_failed"
        finally:
            if manifest is not None:
                try:
                    manifest.close()
                except Exception:
                    self.worker_error = "analysis_manifest_close_failed"

    def _worker(self):
        try:
            while not self.stop_event.is_set():
                self.event.clear()
                progress = self.run_once()
                with self.lock:
                    closed = self._now() // MINUTE * MINUTE
                    due_work = any(
                        job["state"] in _ACTIVE
                        and self.due.get(identity, 0) <= self.monotonic()
                        and (
                            self.needs_anchor.get(identity, False)
                            or job.get("pending_input") is not None
                            or job["next_ms"] < closed
                        )
                        for identity, job in self.jobs.items()
                    )
                if progress or due_work:
                    self.stop_event.wait(0.005)
                else:
                    self.event.wait(1)
        except Exception:
            with self.lock:
                self.worker_error = "analysis_worker_failed"
                for job in self.jobs.values():
                    if job["state"] in _ACTIVE:
                        job.update(state="error", reason="analysis_worker_failed")
        finally:
            self._close_source()

    def close(self):
        with self.lock:
            self.closed = True
        self.stop_event.set()
        self.event.set()
        if self.thread is not None and self.thread is not threading.current_thread():
            self.thread.join(timeout=12)
        if (
            self.source_thread == threading.get_ident()
            or self.manifest_thread == threading.get_ident()
        ):
            self._close_source()
        return {"closed": True, "worker_alive": bool(self.thread and self.thread.is_alive())}
