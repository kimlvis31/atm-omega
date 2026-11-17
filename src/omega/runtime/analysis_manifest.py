"""Bounded shared input segments; only a committed CA checkpoint consumes a record."""

import json
import os
import re
import threading
from collections import OrderedDict
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.domain.errors import StorageError, ValidationError
from omega.infrastructure.auxiliary_codec import encode_observation
from omega.ports.analysis_inputs import PreparedMarketFrame
from omega.runtime.control import InstanceLock

MAX_RECORD = 2_000_000
MAX_SEGMENT = 16_000_000
MAX_TOTAL = 1_024_000_000


def _hex(value, length=64):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{length}}}", value) is not None


def _encoded(value):
    try:
        return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError, RecursionError):
        raise ValidationError("analysis manifest requires bounded finite JSON") from None


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate manifest field")
        result[key] = value
    return result


def _sync_directory(path):
    if os.name != "nt":
        descriptor = os.open(path, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


@dataclass(frozen=True, slots=True)
class InputPolicy:
    destination_id: str
    channels: tuple[str, ...]
    maximum_metric_age_ms: int = 330000
    open_interest_source_suffix: str = "open_interest_history"
    long_short_source_suffix: str = "long_short_ratio"
    schema_version: int = 1
    knowledge_mode: str = "warmup_retrospective_then_live"

    def __post_init__(self):
        if (
            not _hex(self.destination_id)
            or type(self.channels) is not tuple
            or any(type(value) is not str for value in self.channels)
            or tuple(sorted(set(self.channels))) != self.channels
            or set(self.channels) - {"trade_flow", "metric", "depth"}
            or type(self.maximum_metric_age_ms) is not int
            or not 0 <= self.maximum_metric_age_ms <= 86400000
            or type(self.schema_version) is not int
            or self.schema_version != 1
            or self.knowledge_mode != "warmup_retrospective_then_live"
            or any(
                type(value) is not str or re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value) is None
                for value in (self.open_interest_source_suffix, self.long_short_source_suffix)
            )
        ):
            raise ValidationError("invalid fixed analysis input policy")

    def document(self):
        return {**asdict(self), "channels": list(self.channels)}

    @property
    def checksum(self):
        return sha256(_encoded(self.document())).hexdigest()

    @classmethod
    def from_document(cls, value):
        keys = {field for field in cls.__dataclass_fields__}
        if type(value) is not dict or set(value) != keys or type(value["channels"]) is not list:
            raise ValidationError("invalid analysis input policy document")
        return cls(**(value | {"channels": tuple(value["channels"])}))


@dataclass(frozen=True, slots=True)
class ManifestReference:
    segment: str
    offset: int
    length: int
    checksum: str

    def __post_init__(self):
        if (
            not _hex(self.segment, 32)
            or not _hex(self.checksum)
            or type(self.offset) is not int
            or type(self.length) is not int
            or not 0 <= self.offset < MAX_SEGMENT
            or not 2 <= self.length <= MAX_RECORD
            or self.offset + self.length > MAX_SEGMENT
        ):
            raise ValidationError("invalid analysis manifest reference")

    def document(self):
        return asdict(self)

    @classmethod
    def from_document(cls, value):
        if type(value) is not dict or set(value) != {"segment", "offset", "length", "checksum"}:
            raise ValidationError("invalid analysis manifest reference document")
        return cls(**value)


def candle_values(candle):
    return {
        "instrument": candle.instrument.key,
        "open_ms": candle.open_time_ms,
        **{name: str(getattr(candle, name)) for name in ("open", "high", "low", "close", "volume")},
    }


def frame_digest(frame):
    data = {
        "candle": candle_values(frame.candle),
        "as_of_ms": frame.as_of_ms,
        "reference_time_ms": frame.reference_time_ms,
        "inputs": [
            encode_observation(frame.candle.instrument, value).decode() for value in frame.inputs
        ],
    }
    return sha256(_encoded(data)).hexdigest()


def pin_values(pin):
    observation = pin.observation
    return {
        "instrument": pin.instrument.key,
        "channel": observation.channel,
        "source": observation.source,
        "open_ms": observation.window_start_ms,
        "revision": pin.revision,
        "checksum": pin.checksum,
        "previous_checksum": pin.previous_checksum,
        "available_at_ms": observation.available_at_ms,
        "event_time_ms": observation.event_time_ms,
    }


class AnalysisInputManifest:
    """One worker writer; restart uses a new segment and preserves any torn tail.

    Segment data may be orphaned after a CA checkpoint failure. No record alone
    advances a calculation. Automatic deletion is intentionally not performed.
    """

    def __init__(
        self,
        root,
        *,
        maximum_bytes=MAX_TOTAL,
        quota_provider=None,
        namespace="analysis-inputs",
        consumer_kind="ca_input",
    ):
        if type(maximum_bytes) is not int or not MAX_RECORD <= maximum_bytes <= 16 * MAX_TOTAL:
            raise ValidationError("invalid analysis input storage quota")
        if quota_provider is not None and not callable(quota_provider):
            raise ValidationError("analysis input quota provider must be callable")
        if (namespace, consumer_kind) not in {
            ("analysis-inputs", "ca_input"),
            ("paper-inputs", "paper_input"),
        }:
            raise ValidationError("unsupported analysis input consumer namespace")
        self.root = Path(root).resolve() / "state" / namespace
        self.consumer_kind = consumer_kind
        self.maximum_bytes = maximum_bytes
        self.quota_provider = quota_provider
        self.owner, self.owner_thread, self.stream = None, None, None
        self.segment, self.size, self.total = None, 0, 0
        self.closed = False
        self.cache = OrderedDict()

    def _directory(self):
        directory = self.root / "segments"
        for path in (self.root.parent, self.root, directory):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise StorageError("analysis input storage cannot use linked directories")
        return directory

    def _writer(self):
        if self.closed:
            raise StorageError("analysis input writer is closed")
        if self.owner_thread is not None and self.owner_thread != threading.get_ident():
            raise StorageError("analysis input writer belongs to another thread")
        if self.owner is None:
            directory = self._directory()
            try:
                directory.mkdir(parents=True, exist_ok=True)
                owner = InstanceLock(self.root / "writer.lock")
            except (OSError, RuntimeError):
                raise StorageError("analysis input writer is busy or unavailable") from None
            try:
                for path in (directory, self.root, self.root.parent, self.root.parent.parent):
                    _sync_directory(path)
                count = total = 0
                for path in directory.glob("*.jsonl"):
                    count += 1
                    if count > 4096 or path.is_symlink() or not _hex(path.stem, 32):
                        raise StorageError("analysis input segment directory is invalid")
                    total += path.stat().st_size
                self.total = total
            except OSError:
                owner.close()
                raise StorageError("analysis input segment directory is unavailable") from None
            except Exception:
                owner.close()
                raise
            self.owner, self.owner_thread = owner, threading.get_ident()

    def _append(self, data):
        line = _encoded(data) + b"\n"
        if len(line) > MAX_RECORD:
            raise StorageError("analysis input chunk exceeds bounded record size")
        self._writer()
        quota = self.maximum_bytes
        if self.quota_provider is not None:
            try:
                quota = self.quota_provider()
                if type(quota) is not int or not MAX_RECORD <= quota <= 16 * MAX_TOTAL:
                    raise ValueError
            except Exception:
                raise StorageError("analysis input storage quota is unavailable") from None
        if self.total + len(line) > quota:
            raise StorageError(
                "analysis input storage quota reached; referenced records are protected"
            )
        try:
            if self.stream is None or self.size + len(line) > MAX_SEGMENT:
                if self.stream is not None:
                    self.stream.close()
                self.segment, self.size = uuid4().hex, 0
                self.stream = (self._directory() / (self.segment + ".jsonl")).open(
                    "xb", buffering=0
                )
                _sync_directory(self._directory())
            offset = self.size
            remaining = memoryview(line)
            while remaining:
                count = self.stream.write(remaining)
                if not count:
                    raise OSError("incomplete analysis input append")
                self.total += count
                remaining = remaining[count:]
            os.fsync(self.stream.fileno())
            self.size += len(line)
            return ManifestReference(self.segment, offset, len(line), sha256(line).hexdigest())
        except OSError:
            try:
                if self.stream is not None:
                    self.stream.close()
            except OSError:
                pass
            finally:
                self.stream = None  # Keep the failed tail; next append starts a fresh file.
            raise StorageError("analysis input append could not be confirmed") from None

    def read(self, reference):
        if not isinstance(reference, ManifestReference):
            reference = ManifestReference.from_document(reference)
        path = self._directory() / (reference.segment + ".jsonl")
        try:
            if path.is_symlink() or path.stat().st_size > MAX_SEGMENT:
                raise ValueError
            with path.open("rb") as stream:
                if reference.offset:
                    stream.seek(reference.offset - 1)
                    if stream.read(1) != b"\n":
                        raise ValueError
                stream.seek(reference.offset)
                raw = stream.read(reference.length)
            if (
                len(raw) != reference.length
                or not raw.endswith(b"\n")
                or sha256(raw).hexdigest() != reference.checksum
            ):
                raise ValueError
            value = json.loads(
                raw,
                object_pairs_hook=_unique,
                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)),
            )
            if (
                type(value) is not dict
                or type(value.get("schema_version")) is not int
                or value["schema_version"] != 1
            ):
                raise ValueError
            return value
        except (OSError, ValueError, TypeError, RecursionError, UnicodeError):
            raise StorageError("referenced analysis input record is missing or corrupt") from None

    def stage(self, analysis_id, execution_id, generation, frames, policy, *, previous=None):
        if (
            not _hex(analysis_id, 32)
            or not _hex(execution_id, 32)
            or type(generation) is not int
            or not 0 <= generation < 2**63
            or not isinstance(policy, InputPolicy)
            or type(frames) is not tuple
            or not 1 <= len(frames) <= 512
            or any(not isinstance(value, PreparedMarketFrame) for value in frames)
        ):
            raise ValidationError("invalid selected analysis input chunk")
        if previous is not None:
            previous = (
                ManifestReference.from_document(previous) if type(previous) is dict else previous
            )
            prior = self.read(previous)
            if (
                prior.get("kind") != self.consumer_kind
                or prior.get("analysis_id") != analysis_id
                or prior.get("policy_hash") != policy.checksum
                or prior.get("end_ms") != frames[0].frame.candle.open_time_ms
                or prior.get("generation", -1) > generation
            ):
                raise ValidationError("analysis input chain identity changed")
        pins, indices, rows = [], {}, []
        instrument = frames[0].frame.candle.instrument
        expected = frames[0].frame.candle.open_time_ms
        for prepared in frames:
            frame = prepared.frame
            if frame.candle.instrument != instrument or frame.candle.open_time_ms != expected:
                raise ValidationError(
                    "analysis input chunk must contain contiguous instrument minutes"
                )
            if len(prepared.pins) > 8 or {value.channel for value in frame.inputs} != set(
                policy.channels
            ):
                raise ValidationError("analysis input chunk does not match its requested channels")
            selected = []
            for pin in prepared.pins:
                if pin.instrument != instrument or pin.observation.available_at_ms > frame.as_of_ms:
                    raise ValidationError("analysis input pin identity or availability changed")
                value = pin_values(pin)
                key = sha256(_encoded(value)).hexdigest()
                if key not in indices:
                    indices[key] = len(pins)
                    pins.append(value)
                selected.append(indices[key])
            rows.append(
                {
                    "candle": candle_values(frame.candle),
                    "as_of_ms": frame.as_of_ms,
                    "reference_time_ms": frame.reference_time_ms,
                    "pins": selected,
                    "frame_digest": frame_digest(frame),
                }
            )
            expected = frame.candle.close_time_ms
        chunk = {
            "schema_version": 1,
            "kind": "source_chunk",
            "policy": policy.document(),
            "instrument": instrument.key,
            "pins": pins,
            "frames": rows,
        }
        key = sha256(_encoded(chunk)).hexdigest()
        source_ref = self.cache.get(key)
        if source_ref is None:
            source_ref = self._append(chunk)
            self.cache[key] = source_ref
            while len(self.cache) > 8:
                self.cache.popitem(last=False)
        else:
            self.read(source_ref)
            self.cache.move_to_end(key)
        return self._append(
            {
                "schema_version": 1,
                "kind": self.consumer_kind,
                "analysis_id": analysis_id,
                "execution_id": execution_id,
                "generation": generation,
                "policy_hash": policy.checksum,
                "start_ms": frames[0].frame.candle.open_time_ms,
                "end_ms": expected,
                "source": source_ref.document(),
                "previous": previous.document() if previous else None,
            }
        )

    def close(self):
        if self.owner_thread is not None and self.owner_thread != threading.get_ident():
            raise StorageError("analysis input writer must close on its owner thread")
        self.closed = True
        try:
            if self.stream is not None:
                self.stream.close()
        finally:
            self.stream = None
            if self.owner is not None:
                self.owner.close()
                self.owner = None
