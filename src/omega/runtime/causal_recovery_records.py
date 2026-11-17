"""Bounded recovery evidence and one original coordinator-owned journal.

Typed records identify bytes; they grant no actor, recovery or financial authority.
Immutable evidence is never deleted. Unconfirmed IO remains sticky on this owner,
and an uncheckpointed record prevents a later owner from assuming nonexecution.
Confirmation files distinguish a missing original positive file-store ACK; their
own write can also persist before an unknown return. A later owner validates
bytes only and must enter the host's manual HELD startup policy. This adapter
provides no recursive ACK-of-ACK, process-crash success or actor-install authority.
"""

# Imports
import os
import stat
import threading
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Protocol
from uuid import uuid4

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.inference import (
    CAUSAL_SCOPE,
    InferenceCommitAck,
    checksum,
    decoded,
    digest,
    encoded,
    identity,
)
from omega.research.artifacts import read_status_json, validate_artifact_path
from omega.runtime.causal_capacity import EvidenceBudgetLimits
from omega.runtime.causal_nn import FileInferenceStateStore
from omega.runtime.causal_store_leases import InferenceStoreLeases

# Constants
RECOVERY_KIND = "causal_generation_recovery"
MAX_HOT_MUTATIONS = 128
RELEASE_RESERVED = 32
MAX_RECORD_BYTES = 262_144
MAX_HOST_BYTES = 65_536


def _integer(value):
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ValidationError("bounded native recovery integer required")
    return value


def _document(raw, maximum=MAX_RECORD_BYTES):
    value = decoded(raw, maximum=maximum)
    if type(value) is not dict:
        raise ValidationError("recovery document must be an object")

    def validate(item, depth=0):
        if depth > 24:
            raise ValidationError("recovery document nesting exceeds capacity")
        if item is None or type(item) is bool:
            return
        if type(item) is int and -(2**63) <= item <= 2**63 - 1:
            return
        if type(item) is str and len(item.encode("utf-8")) <= maximum:
            return
        if type(item) is list and len(item) <= 4096:
            for child in item:
                validate(child, depth + 1)
            return
        if type(item) is dict and len(item) <= 4096:
            for key, child in item.items():
                if type(key) is not str or len(key) > 128:
                    raise ValidationError("bounded native recovery keys required")
                validate(child, depth + 1)
            return
        raise ValidationError("recovery document needs bounded native JSON values")

    validate(value)
    return value


def canonical_recovery_document(raw, *, maximum=MAX_RECORD_BYTES):
    """Return bounded native JSON bytes as canonical text, without interpreting policy."""
    if type(maximum) is not int or not 1 <= maximum <= MAX_RECORD_BYTES:
        raise ValidationError("invalid recovery document capacity")
    return encoded(_document(raw, maximum), maximum=maximum)


@dataclass(frozen=True, slots=True)
class FeatureMutationRef:
    core_epoch: str
    generation: int
    actor_token: str
    mutation_token: str
    kind: str
    pin_id: str
    arguments_sha256: str

    def __post_init__(self):
        for value in (self.core_epoch, self.actor_token, self.mutation_token, self.pin_id):
            identity(value)
        _integer(self.generation)
        if type(self.kind) is not str or self.kind not in {"pin", "release"}:
            raise ValidationError("exact feature mutation kind required")
        checksum(self.arguments_sha256)

    def document(self):
        return asdict(self)

    @property
    def sha256(self):
        return digest(self.document())


@dataclass(frozen=True, slots=True)
class MutationJournalAck:
    ref_sha256: str
    state: str
    revision: int
    snapshot_sha256: str
    record_sha256: str
    durable: bool
    result_json: str | None = None
    code: str | None = None

    def __post_init__(self):
        for value in (self.ref_sha256, self.snapshot_sha256, self.record_sha256):
            checksum(value)
        _integer(self.revision)
        if (
            type(self.state) is not str
            or self.state not in {"prepared", "terminal", "unknown"}
            or type(self.durable) is not bool
        ):
            raise ValidationError("invalid recovery mutation acknowledgement")
        if self.result_json is not None:
            _document(self.result_json)
        if (self.state == "terminal") != (self.result_json is not None):
            raise ValidationError("terminal mutation requires its exact result")
        if self.code is not None:
            identity(self.code)
        if (self.state == "unknown") != (self.code is not None):
            raise ValidationError("unknown mutation requires its bounded code")


@dataclass(frozen=True, slots=True)
class MutationRecord:
    ref: FeatureMutationRef
    arguments_json: str
    ack: MutationJournalAck

    def __post_init__(self):
        if type(self.ref) is not FeatureMutationRef or type(self.ack) is not MutationJournalAck:
            raise ValidationError("exact immutable mutation record required")
        if digest(_document(self.arguments_json)) != self.ref.arguments_sha256:
            raise ValidationError("mutation arguments differ from original reference")
        if self.ref.sha256 != self.ack.ref_sha256:
            raise ValidationError("mutation acknowledgement reference mismatch")


class FeatureMutationBroker(Protocol):
    def prepare_mutation(self, ref: FeatureMutationRef, args_json: str) -> MutationJournalAck: ...

    def finish_mutation(self, ref: FeatureMutationRef, result_json: str) -> MutationJournalAck: ...

    def mark_unknown(self, ref: FeatureMutationRef, code: str) -> MutationJournalAck: ...


class FeatureMutationUnknown(StorageError):
    def __init__(self, ref, phase, recording_unknown):
        if type(ref) is not FeatureMutationRef:
            raise ValidationError("exact original mutation reference required")
        if type(phase) is not str or phase not in {"prepare", "dispatch", "terminal", "unknown"}:
            raise ValidationError("exact mutation uncertainty phase required")
        if type(recording_unknown) is not bool:
            raise ValidationError("native mutation recording flag required")
        super().__init__("feature mutation outcome remains unknown")
        self.ref, self.phase, self.recording_unknown = ref, phase, recording_unknown


class FeatureMutationRejected(ValidationError):
    recording_unknown = False
    dispatched = False

    def __init__(self, code="mutation_capacity_unavailable"):
        identity(code)
        super().__init__(code)
        self.code = code


@dataclass(frozen=True, slots=True)
class _MutationAckOrigin:
    pid: int
    thread: object
    issuer_token: object
    lock: object
    issued: dict


class MutationAckAuthority:
    """Read-only cross-thread witness for this issuer's actual terminal returns.

    An equal DTO is not an issued object. Construction, file readback and cold
    restart alone do not register a witness. Only the captured journal owner can
    issue or retire entries, using the original private capability. No filesystem
    or actor/provider callback is reachable through ``verify``.
    """

    def __init__(self, issuer_token):
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._issuer_token = issuer_token
        self._lock = threading.Lock()
        self._issued = {}
        self._origin = _MutationAckOrigin(
            self._pid, self._thread, issuer_token, self._lock, self._issued
        )
        self._identity_failed = False

    def _intact(self):
        try:
            origin = self._origin
            intact = (
                type(origin) is _MutationAckOrigin
                and self._pid == origin.pid
                and self._thread is origin.thread
                and self._issuer_token is origin.issuer_token
                and self._lock is origin.lock
                and self._issued is origin.issued
            )
        except Exception:
            intact = False
        if not intact:
            self._identity_failed = True
        return intact and not self._identity_failed

    def _owner(self, token):
        if (
            not self._intact()
            or os.getpid() != self._origin.pid
            or threading.current_thread() is not self._origin.thread
            or token is not self._origin.issuer_token
        ):
            raise StorageError("mutation ACK authority requires its original issuing owner")

    def _issue(self, token, ref, ack):
        self._owner(token)
        if (
            type(ref) is not FeatureMutationRef
            or type(ack) is not MutationJournalAck
            or ack.ref_sha256 != ref.sha256
            or ack.state != "terminal"
            or ack.durable is not True
        ):
            raise ValidationError("exact durable terminal mutation ACK required")
        origin = self._origin
        with origin.lock:
            previous = origin.issued.get(ref.sha256)
            if previous is not None:
                if previous[0] != ref or previous[1].result_json != ack.result_json:
                    raise ProtocolError("original issued mutation ACK conflicts")
                return previous[1]
            if len(origin.issued) >= MAX_HOT_MUTATIONS:
                raise FeatureMutationUnknown(ref, "terminal", False)
            origin.issued[ref.sha256] = (ref, ack)
            return ack

    def _lookup(self, token, ref):
        self._owner(token)
        origin = self._origin
        with origin.lock:
            row = origin.issued.get(ref.sha256)
            return None if row is None else row[1]

    def _count(self, token):
        self._owner(token)
        origin = self._origin
        with origin.lock:
            return len(origin.issued)

    def _retire(self, token, ref, ack):
        self._owner(token)
        origin = self._origin
        with origin.lock:
            row = origin.issued.get(ref.sha256)
            if row is None or row[0] != ref or row[1] is not ack:
                return False
            del origin.issued[ref.sha256]
            return True

    def verify(self, ref, ack, result_json):
        if (
            not self._intact()
            or os.getpid() != self._origin.pid
            or type(ref) is not FeatureMutationRef
            or type(ack) is not MutationJournalAck
            or type(result_json) is not str
        ):
            return False
        origin = self._origin
        with origin.lock:
            row = origin.issued.get(ref.sha256)
            return (
                row is not None
                and row[0] == ref
                and row[1] is ack
                and ack.result_json == result_json
                and ack.state == "terminal"
                and ack.durable is True
            )


class RecoveryJournal:
    """One file-store lease; bounded hot checkpoint and immutable chained records.

    The coordinator must construct, call and close this object on its own Thread.
    The optional factory is a fault-testing seam, never a public API parameter.
    """

    def __init__(
        self, root, *, generation=None, limits=None, store_factory=FileInferenceStateStore
    ):
        self._pid, self._thread = os.getpid(), threading.current_thread()
        self._leases = InferenceStoreLeases(
            "recovery-" + uuid4().hex if generation is None else generation, capacity=1
        )
        self._store = None
        self._failed, self._closed = False, False
        self._last_ack = None
        self._issuer_token = object()
        self._ack_authority = MutationAckAuthority(self._issuer_token)
        self._limits = EvidenceBudgetLimits() if limits is None else limits
        if type(self._limits) is not EvidenceBudgetLimits:
            raise ValidationError("parsed immutable recovery evidence limits required")
        self._snapshot = {
            "scope": CAUSAL_SCOPE,
            "kind": RECOVERY_KIND,
            "schema_version": 1,
            "revision": 0,
            "host": {},
            "hot": {},
            "record_count": 0,
            "record_bytes": 0,
            "record_head": None,
        }
        try:
            supplied = Path(root)
            if ".." in supplied.parts:
                raise ValidationError("recovery root cannot contain parent traversal")
            self._root = validate_artifact_path(supplied, allow_missing=True)
            state = validate_artifact_path(self._root / "state", allow_missing=True)
            state.mkdir(parents=True, exist_ok=True)
            self._history = validate_artifact_path(
                state / "causal-recovery-records", allow_missing=True
            )
            self._history.mkdir(exist_ok=True)
            self._store = store_factory(
                state / "causal-recovery.json", leases=self._leases, role="journal"
            )
            if type(self._store) is not FileInferenceStateStore:
                raise ValidationError("exact original recovery file store required")
            raw = self._store.read()
            if raw is not None:
                self._snapshot = self._validate_snapshot(_document(raw, 1_048_576))
                self._confirm_checkpoint(self._snapshot, create=False)
            self._load_records()
            if raw is None:
                self._commit(dict(self._snapshot), expected_revision=None)
            else:
                self._last_ack = InferenceCommitAck(
                    self._snapshot["revision"], digest(self._snapshot), True
                )
        except BaseException as failure:
            self._failed = True
            # Caller retains the original scope even when construction never returns.
            try:
                failure.lease_scope = self._leases
            except BaseException:
                pass
            raise

    @property
    def lease_scope(self):
        self._owner()
        return self._leases

    @property
    def recording_unknown(self):
        self._owner()
        return self._failed or (self._store is not None and self._store.failed)

    @property
    def last_ack(self):
        self._owner()
        return self._last_ack

    def mutation_ack_authority(self):
        self._owner()
        return self._ack_authority

    def retire_mutation_ack(self, ref, ack):
        self._owner()
        self._ref(ref)
        return self._ack_authority._retire(self._issuer_token, ref, ack)

    def _owner(self):
        if os.getpid() != self._pid or threading.current_thread() is not self._thread:
            raise StorageError("recovery journal requires its original PID and Thread object")

    def _guard(self):
        self._owner()
        if self._closed or self.recording_unknown:
            raise StorageError("recovery journal is closed or recording outcome is unknown")
        try:
            validate_artifact_path(self._history)
            if self._store.read() != encoded(self._snapshot):
                raise StorageError("recovery journal changed outside its sole writer")
        except BaseException:
            self._failed = True
            raise

    def _validate_snapshot(self, value):
        if (
            set(value) != set(self._snapshot)
            or value["scope"] != CAUSAL_SCOPE
            or value["kind"] != RECOVERY_KIND
            or type(value["schema_version"]) is not int
            or value["schema_version"] != 1
            or type(value["hot"]) is not dict
            or len(value["hot"]) > MAX_HOT_MUTATIONS
        ):
            raise StorageError("invalid recovery journal schema")
        _integer(value["revision"])
        _integer(value["record_count"])
        _integer(value["record_bytes"])
        if (
            value["record_count"] > self._limits.max_records + RELEASE_RESERVED * 2
            or value["record_bytes"]
            > self._limits.max_bytes + RELEASE_RESERVED * (MAX_RECORD_BYTES + 2048) * 2
        ):
            raise StorageError("recovery evidence exceeds capacity")
        _document(encoded(value["host"]), MAX_HOST_BYTES)
        if value["record_count"] == 0:
            if value["record_head"] is not None:
                raise StorageError("empty recovery evidence has a head")
        else:
            checksum(value["record_head"])
        for key, item in value["hot"].items():
            checksum(key)
            if type(item) is not dict or set(item) != {
                "state",
                "record_sha256",
                "sequence",
                "prepared_sequence",
            }:
                raise StorageError("invalid hot mutation head")
            checksum(item["record_sha256"])
            for name in ("sequence", "prepared_sequence"):
                if not 1 <= _integer(item[name]) <= value["record_count"]:
                    raise StorageError("invalid hot mutation sequence")
            if item["state"] not in {"prepared", "unknown"}:
                raise StorageError("invalid hot mutation state")
        return value

    def _event_path(self, sequence):
        return self._history / f"{sequence:08d}.json"

    def _read_event(self, sequence):
        path = validate_artifact_path(self._event_path(sequence))
        if path.stat().st_size > MAX_RECORD_BYTES:
            raise StorageError("recovery evidence file exceeds capacity")
        value = read_status_json(path)
        if (
            type(value) is not dict
            or set(value) != {"data", "sha256"}
            or type(value["data"]) is not dict
            or value["sha256"] != digest(value["data"])
        ):
            raise StorageError("recovery evidence integrity unavailable")
        _document(encoded(value["data"]))
        item = value["data"]
        if (
            set(item) != {"sequence", "previous_sha256", "ref", "state", "payload", "revision"}
            or type(item["sequence"]) is not int
            or item["sequence"] != sequence
            or not 1 <= _integer(item["revision"]) <= self._snapshot["revision"] + 1
        ):
            raise StorageError("invalid recovery evidence identity")
        if sequence == 1:
            if item["previous_sha256"] is not None:
                raise StorageError("invalid initial recovery evidence head")
        else:
            checksum(item["previous_sha256"])
        return value

    def _load_records(self):
        count = self._snapshot["record_count"]
        # The original owner stops on its first unconfirmed write, so precisely
        # the next sequence detects an abandoned write without scanning lifetime history.
        next_event = validate_artifact_path(self._event_path(count + 1), allow_missing=True)
        if next_event.exists():
            raise StorageError("uncheckpointed recovery evidence retained")
        if count and self._read_event(count)["sha256"] != self._snapshot["record_head"]:
            raise StorageError("recovery checkpoint evidence head differs")
        pins = 0
        for key, head in self._snapshot["hot"].items():
            event = self._read_event(head["sequence"])
            ref = FeatureMutationRef(**event["data"]["ref"])
            record = self._lookup(ref)
            if ref.sha256 != key or record is None or record.ack.state != head["state"]:
                raise StorageError("recovery hot membership differs from actual evidence")
            pins += ref.kind == "pin"
        if pins > MAX_HOT_MUTATIONS - RELEASE_RESERVED:
            raise StorageError("recovery pin reservation exceeds capacity")

    def _token(self, ref):
        return digest([ref.core_epoch, ref.generation, ref.actor_token, ref.mutation_token])

    def _ref(self, ref):
        if type(ref) is not FeatureMutationRef:
            raise ValidationError("exact immutable feature mutation reference required")

    def _index_path(self, ref, state):
        return self._history / f"{state}-{self._token(ref)}.json"

    def _read_index(self, ref, state):
        path = validate_artifact_path(self._index_path(ref, state), allow_missing=True)
        if not path.exists():
            return None
        if path.stat().st_size > 1024:
            raise StorageError("recovery evidence index exceeds capacity")
        item = read_status_json(path)
        if type(item) is not dict or set(item) != {"sequence", "record_sha256"}:
            raise StorageError("invalid recovery evidence index")
        if not 1 <= _integer(item["sequence"]) <= self._snapshot["record_count"]:
            raise StorageError("recovery evidence index is uncheckpointed")
        checksum(item["record_sha256"])
        event = self._read_event(item["sequence"])
        if event["sha256"] != item["record_sha256"] or event["data"]["state"] != state:
            raise StorageError("recovery evidence index differs from actual record")
        return event

    def _lookup(self, ref):
        try:
            return self._lookup_unchecked(ref)
        except ProtocolError:
            raise
        except BaseException:
            self._failed = True
            raise FeatureMutationUnknown(ref, "unknown", True) from None

    def _lookup_unchecked(self, ref):
        prepared = self._read_index(ref, "prepared")
        if prepared is None:
            if ref.sha256 in self._snapshot["hot"] or self._index_path(ref, "terminal").exists():
                raise StorageError("original recovery preparation is missing")
            return None
        if prepared["data"]["ref"] != ref.document():
            raise ProtocolError("mutation token identifies different original arguments")
        args_json = encoded(prepared["data"]["payload"])
        terminal = self._read_index(ref, "terminal")
        head = self._snapshot["hot"].get(ref.sha256)
        if terminal is not None:
            if head is not None:
                raise StorageError("terminal evidence is not durably settled")
            event = terminal
        elif head is not None:
            event = self._read_event(head["sequence"])
            if (
                event["sha256"] != head["record_sha256"]
                or prepared["data"]["sequence"] != head["prepared_sequence"]
            ):
                raise StorageError("hot recovery evidence changed")
        else:
            raise StorageError("mutation has neither hot membership nor terminal evidence")
        item = event["data"]
        if item["ref"] != ref.document() or item["revision"] > self._snapshot["revision"]:
            raise StorageError("recovery outcome differs from its original reference")
        state = item["state"]
        code = None
        if state == "unknown":
            if type(item["payload"]) is not dict or set(item["payload"]) != {"code"}:
                raise StorageError("unknown recovery evidence has invalid classification")
            code = item["payload"]["code"]
        ack = MutationJournalAck(
            ref.sha256,
            state,
            self._snapshot["revision"],
            digest(self._snapshot),
            event["sha256"],
            True,
            encoded(item["payload"]) if state == "terminal" else None,
            code,
        )
        return MutationRecord(ref, args_json, ack)

    def _commit(self, candidate, *, expected_revision):
        try:
            raw = encoded(candidate)
            ack = self._store.save_snapshot(raw, expected_revision=expected_revision)
            if (
                type(ack) is not InferenceCommitAck
                or ack.durable is not True
                or ack.revision != candidate["revision"]
                or ack.snapshot_sha256 != digest(candidate)
                or self._store.failed
                or self._store.read() != raw
            ):
                raise StorageError("recovery checkpoint acknowledgement is unconfirmed")
            # An actual committed file alone cannot stand in for an original
            # positive callback ACK after a duck/unknown return or a crash.
            self._confirm_checkpoint(candidate, create=True)
        except BaseException:
            self._failed = True
            raise
        self._snapshot, self._last_ack = candidate, ack
        return ack

    def _confirm_checkpoint(self, candidate, *, create):
        try:
            path = validate_artifact_path(
                self._history / f"checkpoint-{candidate['revision']:016d}.json",
                allow_missing=create,
            )
        except FileNotFoundError:
            raise StorageError("original recovery checkpoint acknowledgement is unknown") from None
        value = {
            "revision": candidate["revision"],
            "snapshot_sha256": digest(candidate),
            "exact_file_ack": True,
        }
        if create and not path.exists():
            with path.open("xb") as stream:
                stream.write(encoded(value).encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
        if path.stat().st_size > 1024 or read_status_json(path) != value:
            raise StorageError("original recovery checkpoint confirmation is unavailable")

    def _append(self, ref, state, payload):
        count = self._snapshot["record_count"]
        old = self._lookup(ref)
        sequence, revision = count + 1, self._snapshot["revision"] + 1
        data = {
            "sequence": sequence,
            "previous_sha256": self._snapshot["record_head"],
            "ref": ref.document(),
            "state": state,
            "payload": payload,
            "revision": revision,
        }
        record_sha = digest(data)
        raw = encoded({"data": data, "sha256": record_sha}, maximum=MAX_RECORD_BYTES)
        reserve = ref.kind == "release" or state != "prepared"
        extra_records = RELEASE_RESERVED * 2 if reserve else 0
        extra_bytes = RELEASE_RESERVED * (MAX_RECORD_BYTES + 2048) * 2 if reserve else 0
        # Charge actual record bytes plus the small immutable index. Preserve a
        # worst-case terminal record for each admitted hot ref before new work.
        charged = (
            len(raw.encode("utf-8")) + 1024 + (1024 if state in {"prepared", "terminal"} else 0)
        )
        remaining_hot = len(self._snapshot["hot"]) + (state == "prepared") - (state == "terminal")
        if (
            count + 1 + remaining_hot > self._limits.max_records + extra_records
            or self._snapshot["record_bytes"] + charged + remaining_hot * (MAX_RECORD_BYTES + 2048)
            > self._limits.max_bytes + extra_bytes
        ):
            if state == "prepared":
                raise FeatureMutationRejected()
            raise FeatureMutationUnknown(
                ref, "terminal" if state == "terminal" else "unknown", False
            )
        try:
            path = validate_artifact_path(self._event_path(sequence), allow_missing=True)
            with path.open("xb") as stream:
                details = os.fstat(stream.fileno())
                if (
                    not stat.S_ISREG(details.st_mode)
                    or getattr(details, "st_file_attributes", 0) & 0x400
                ):
                    raise StorageError("recovery evidence is not a regular file")
                stream.write(raw.encode("utf-8"))
                stream.flush()
                os.fsync(stream.fileno())
            if self._read_event(sequence) != {"data": data, "sha256": record_sha}:
                raise StorageError("recovery evidence readback differs")
            if state in {"prepared", "terminal"}:
                index = {"sequence": sequence, "record_sha256": record_sha}
                index_path = validate_artifact_path(
                    self._index_path(ref, state), allow_missing=True
                )
                with index_path.open("xb") as stream:
                    stream.write(encoded(index).encode("utf-8"))
                    stream.flush()
                    os.fsync(stream.fileno())
                if read_status_json(validate_artifact_path(index_path)) != index:
                    raise StorageError("recovery evidence index readback differs")
            candidate = dict(self._snapshot)
            hot = dict(candidate["hot"])
            if state == "terminal":
                hot.pop(ref.sha256)
            else:
                hot[ref.sha256] = {
                    "state": state,
                    "record_sha256": record_sha,
                    "sequence": sequence,
                    "prepared_sequence": sequence
                    if old is None
                    else hot[ref.sha256]["prepared_sequence"],
                }
            candidate.update(
                revision=revision,
                hot=hot,
                record_count=sequence,
                record_head=record_sha,
                record_bytes=candidate["record_bytes"] + charged,
            )
            commit = self._commit(candidate, expected_revision=self._snapshot["revision"])
        except BaseException:
            self._failed = True
            phase = {"prepared": "prepare", "terminal": "terminal", "unknown": "unknown"}[state]
            raise FeatureMutationUnknown(ref, phase, True) from None
        ack = MutationJournalAck(
            ref.sha256,
            state,
            commit.revision,
            commit.snapshot_sha256,
            record_sha,
            True,
            encoded(payload) if state == "terminal" else None,
            payload["code"] if state == "unknown" else None,
        )
        return (
            self._ack_authority._issue(self._issuer_token, ref, ack) if state == "terminal" else ack
        )

    def prepare_mutation(self, ref, args_json):
        self._guard()
        self._ref(ref)
        args = _document(args_json)
        if digest(args) != ref.arguments_sha256:
            raise ProtocolError("prepared arguments differ from original mutation")
        old = self._lookup(ref)
        if old is not None:
            if old.ack.state == "terminal":
                return self.finish_mutation(ref, old.ack.result_json)
            return old.ack
        hot = self._snapshot["hot"]
        occupied = len(hot) + self._ack_authority._count(self._issuer_token)
        if occupied >= MAX_HOT_MUTATIONS or ref.kind == "pin" and occupied >= 96:
            raise FeatureMutationRejected()
        return self._append(ref, "prepared", args)

    def finish_mutation(self, ref, result_json):
        self._guard()
        self._ref(ref)
        result = _document(result_json)
        old = self._lookup(ref)
        if old is None:
            raise ProtocolError("mutation was not durably prepared")
        if old.ack.state == "terminal":
            if old.ack.result_json != encoded(result):
                raise ProtocolError("terminal mutation result conflicts with original")
            issued = self._ack_authority._lookup(self._issuer_token, ref)
            if issued is not None:
                return issued
            if (
                len(self._snapshot["hot"]) + self._ack_authority._count(self._issuer_token)
                >= MAX_HOT_MUTATIONS
            ):
                raise FeatureMutationUnknown(ref, "terminal", False)
            # Reissue only after another actual exact file-store ACK/readback;
            # get_mutation and cold construction never populate the authority.
            try:
                commit = self._commit(
                    dict(self._snapshot), expected_revision=self._snapshot["revision"]
                )
            except BaseException:
                raise FeatureMutationUnknown(ref, "terminal", True) from None
            ack = MutationJournalAck(
                ref.sha256,
                "terminal",
                commit.revision,
                commit.snapshot_sha256,
                old.ack.record_sha256,
                True,
                old.ack.result_json,
            )
            return self._ack_authority._issue(self._issuer_token, ref, ack)
        return self._append(ref, "terminal", result)

    def mark_unknown(self, ref, code):
        self._guard()
        self._ref(ref)
        identity(code)
        old = self._lookup(ref)
        if old is None:
            raise ProtocolError("unknown mutation lacks original preparation")
        if old.ack.state == "terminal":
            return self.finish_mutation(ref, old.ack.result_json)
        if old.ack.code == code:
            return old.ack
        return self._append(ref, "unknown", {"code": code})

    def get_mutation(self, ref):
        self._guard()
        self._ref(ref)
        record = self._lookup(ref)
        issued = self._ack_authority._lookup(self._issuer_token, ref)
        if record is not None and issued is not None:
            if record.ack.state != "terminal" or record.ack.result_json != issued.result_json:
                self._failed = True
                raise FeatureMutationUnknown(ref, "unknown", True)
            return MutationRecord(ref, record.arguments_json, issued)
        return record

    def has_unsettled_mutations(self):
        """Original coordinator observation, including an uncheckpointed next tail."""
        self._guard()
        if type(self._snapshot["hot"]) is not dict:
            raise StorageError("recovery mutation membership is unavailable")
        if self._snapshot["hot"]:
            return True
        try:
            self._event_path(self._snapshot["record_count"] + 1).lstat()
        except FileNotFoundError:
            return False
        except OSError:
            raise StorageError("recovery event tail observation is unavailable") from None
        return True

    def read_host(self):
        self._guard()
        return self._snapshot["revision"], encoded(self._snapshot["host"])

    def save_host(self, host_json, *, expected_revision):
        self._guard()
        _integer(expected_revision)
        host = _document(host_json, MAX_HOST_BYTES)
        if expected_revision != self._snapshot["revision"]:
            raise ProtocolError("recovery host checkpoint revision changed")
        candidate = dict(self._snapshot)
        candidate.update(host=host, revision=expected_revision + 1)
        return self._commit(candidate, expected_revision=expected_revision)

    def retirement_status(self):
        self._owner()
        return self._leases.snapshot()

    def close(self):
        self._owner()
        self._closed = True
        return self._leases.close_all()
