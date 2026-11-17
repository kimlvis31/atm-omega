"""Immutable local virtual-margin facts, separate from account snapshots and SQL.

One project/account owns both namespaces. A custom fallback must not be shared
with another project/core. Recovery checks both sources; it never chooses a
healthy copy to hide corruption. The application owns causal-chain validation.
"""

# Imports
import json
import os
import re
import stat
import types
from dataclasses import fields
from decimal import Decimal
from functools import lru_cache
from hashlib import sha256
from itertools import islice
from pathlib import Path
from threading import RLock
from typing import get_args, get_origin, get_type_hints
from uuid import uuid4

from omega.application.margin_cost import (
    COST_MODEL,
    CostBarFillQuote,
    CostEnvelope,
    CostMarginReservation,
    CostPendingFillAdmission,
    FrozenCostPendingOrder,
    MarginCostPolicy,
    PreparedCostMarginBatch,
)
from omega.application.margin_execution import (
    FEE_QUANTUM,
    MAX_TIME,
    MODEL,
    FrozenPendingOrder,
    PendingFillAdmission,
    PreparedMarginBatch,
)
from omega.application.margin_funding import (
    PreparedFundingBatch,
    frozen_funding_document,
    restore_funding_batch,
)
from omega.application.margin_risk import MarginReservation
from omega.application.paper_fill import BarFillQuote
from omega.application.virtual_margin import (
    MAX_RECORDS,
    AccountAnchor,
    CancelIntent,
    EntryControl,
    JournalReceipt,
    VirtualMarginRecord,
    _validate_batch,
)
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import (
    FillEvent,
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarkSnapshot,
    apply_fill,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal
from omega.ports.journal import Durability
from omega.runtime.control import InstanceLock
from omega.runtime.margin_store import account_document, account_from_document
from omega.runtime.settings import atomic_json

MAX_RECORD_BYTES = 16_000_000
MAX_NAMESPACE_BYTES = 128_000_000  # Two independently bounded sources: 256 MB total.
_HASH = re.compile(r"[a-f0-9]{64}\Z")
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_CLASSES = {
    item.__name__: item
    for item in (
        AccountAnchor,
        CancelIntent,
        EntryControl,
        FrozenPendingOrder,
        FrozenCostPendingOrder,
        MarginReservation,
        CostMarginReservation,
        MarginCostPolicy,
        CostEnvelope,
        OrderProposal,
        PendingFillAdmission,
        CostPendingFillAdmission,
        BarFillQuote,
        CostBarFillQuote,
        Candle,
        Instrument,
        FillEvent,
        MarginBracketSnapshot,
        MarginBracket,
        MarkSnapshot,
        VirtualMarginRecord,
    )
}
_ENUMS = {item.__name__: item for item in (Market, Side, OrderKind, MarginMode)}
_BATCHES = {item.__name__: item for item in (PreparedMarginBatch, PreparedCostMarginBatch)}


class _Capacity(StorageError):
    """A full primary may use the independently bounded fallback."""


def _json(value):
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValidationError("duplicate virtual margin journal field")
        result[key] = value
    return result


def _object(value, keys):
    if type(value) is not dict or set(value) != set(keys):
        raise ValidationError("invalid virtual margin journal fields")
    return value


def _hash(value):
    if type(value) is not str or _HASH.fullmatch(value) is None:
        raise ValidationError("invalid virtual margin journal checksum")


def _identity(value):
    if type(value) is not str or _ID.fullmatch(value) is None:
        raise ValidationError("invalid virtual margin journal identity")


@lru_cache(maxsize=32)
def _annotations(kind):
    return get_type_hints(kind)


def _matches(value, annotation):
    origin, args = get_origin(annotation), get_args(annotation)
    if origin is types.UnionType:
        return any(_matches(value, item) for item in args)
    if origin is tuple:
        if type(value) is not tuple:
            return False
        if len(args) == 2 and args[1] is Ellipsis:
            return all(_matches(item, args[0]) for item in value)
        return len(value) == len(args) and all(
            _matches(item, kind) for item, kind in zip(value, args, strict=True)
        )
    return type(value) is annotation


def _typed(value):
    for name, annotation in _annotations(type(value)).items():
        if not _matches(getattr(value, name), annotation):
            raise ValidationError("virtual margin journal field has the wrong native type")
    if isinstance(value, AccountAnchor):
        if value.account.ledger:
            raise ValidationError("virtual margin anchor must precede all factual entries")
    elif isinstance(value, CancelIntent):
        _identity(value.order_id)
        _hash(value.original_request_fingerprint)
    elif isinstance(value, EntryControl):
        _identity(value.request_id)
    elif isinstance(value, PendingFillAdmission):
        _identity(value.order_id)
        _hash(value.original_request_fingerprint)
        allowed = {
            "already_settled",
            "waiting_eligible",
            "await_close",
            "not_touched",
            "deferred",
            "rejected",
            "fill",
        }
        if type(value) is CostPendingFillAdmission:
            allowed.add("open_phase_required")
        if value.outcome not in allowed:
            raise ValidationError("invalid frozen admission outcome")
        if value.reason is not None and re.fullmatch(r"[a-z_]{1,128}", value.reason) is None:
            raise ValidationError("invalid frozen admission reason")
        if not value.fee_adjustment.is_finite() or value.fee_adjustment < 0:
            raise ValidationError("invalid frozen admission fee adjustment")


def _prefixes(baseline, facts):
    result = [baseline]
    for fact in facts:
        transition = apply_fill(result[-1], fact)
        if transition.entry is None or len(transition.account.ledger) != len(result[-1].ledger) + 1:
            raise ValidationError("frozen batch must append each factual fill exactly once")
        result.append(transition.account)
    return tuple(result)


def _batch(value):
    _typed(value)
    for digest in (value.batch_id, value.checksum, *value.prefix_checksums):
        _hash(digest)
    if (
        value.model != (COST_MODEL if type(value) is PreparedCostMarginBatch else MODEL)
        or value.fee_quantum != FEE_QUANTUM
        or value.phase not in {"open", "close"}
        or not 0 <= value.as_of_ms <= MAX_TIME
        or not 1 <= len(value.candles) <= 100
        or any(
            len(getattr(value, name)) > 100
            for name in (
                "brackets",
                "original_pending",
                "admissions",
                "facts",
                "remaining",
                "rejections",
            )
        )
        or len(value.prefix_accounts) != len(value.facts) + 1
        or len(value.prefix_checksums) != len(value.prefix_accounts)
    ):
        raise ValidationError("invalid frozen virtual margin batch bounds")
    for identity, reason in value.rejections:
        _identity(identity)
        if re.fullmatch(r"[a-z_]{1,128}", reason) is None:
            raise ValidationError("invalid frozen batch rejection")
    if _prefixes(value.prefix_accounts[0], value.facts) != value.prefix_accounts:
        raise ValidationError("frozen batch account prefixes differ from its facts")
    if type(value) is PreparedCostMarginBatch:
        try:
            _validate_batch(
                value,
                value.prefix_accounts[0],
                {order.order_id: order for order in value.original_pending},
            )
        except ProtocolError:
            raise ValidationError(
                "frozen cost quote/admission differs from its original inputs"
            ) from None


def _encode(value, depth=0):
    if depth > 48:
        raise ValidationError("virtual margin journal nesting exceeds limit")
    if value is None or type(value) in (bool, int, str):
        if type(value) is str and len(value) > 1024 or type(value) is int and abs(value) > MAX_TIME:
            raise ValidationError("virtual margin journal scalar exceeds limit")
        return value
    if type(value) is Decimal:
        if (
            not value.is_finite()
            or len(value.as_tuple().digits) > 256
            or abs(value.as_tuple().exponent) > 256
        ):
            raise ValidationError("invalid bounded journal decimal")
        return {"type": "decimal", "value": str(value)}
    if type(value) in _ENUMS.values():
        return {"type": type(value).__name__, "value": value.value}
    if type(value) is tuple:
        if len(value) > 256:
            raise ValidationError("virtual margin journal tuple exceeds limit")
        return {"type": "tuple", "items": [_encode(item, depth + 1) for item in value]}
    if type(value) is MarginAccount:
        return {"type": "MarginAccount", "value": account_document(value)}
    if type(value) is PreparedFundingBatch:
        # Store the original baseline once. The strict funding decoder rebuilds
        # every exact account prefix from the immutable scenario facts, never
        # by consulting new source data or current account state.
        try:
            document = frozen_funding_document(value)
        except ProtocolError:
            raise ValidationError(
                "frozen funding prefixes differ from their original facts"
            ) from None
        return {
            "type": "PreparedFundingBatch",
            "baseline": _encode(value.prefix_accounts[0], depth + 1),
            "document": document,
        }
    if type(value) in _BATCHES.values():
        _batch(value)
        data = {
            field.name: _encode(getattr(value, field.name), depth + 1)
            for field in fields(value)
            if field.name != "prefix_accounts"
        }
        data["baseline"] = _encode(value.prefix_accounts[0], depth + 1)
        return {"type": type(value).__name__, "fields": data}
    if type(value) in _CLASSES.values():
        _typed(value)
        return {
            "type": type(value).__name__,
            "fields": {
                field.name: _encode(getattr(value, field.name), depth + 1)
                for field in fields(value)
            },
        }
    raise ValidationError("unknown virtual margin journal value type")


def _decode(value, depth=0):
    if depth > 48:
        raise ValidationError("virtual margin journal nesting exceeds limit")
    if value is None or type(value) in (bool, int, str):
        _encode(value, depth)
        return value
    if type(value) is not dict or type(value.get("type")) is not str:
        raise ValidationError("invalid typed virtual margin journal value")
    kind = value["type"]
    if kind == "decimal":
        _object(value, ("type", "value"))
        if (
            type(value["value"]) is not str
            or len(value["value"]) > 1024
            or re.fullmatch(r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:E[+-]?[0-9]+)?", value["value"])
            is None
        ):
            raise ValidationError("journal decimal must be a bounded string")
        result = Decimal(value["value"])
        _encode(result)
        return result
    if kind in _ENUMS:
        _object(value, ("type", "value"))
        if type(value["value"]) is not str:
            raise ValidationError("invalid journal enumeration")
        return _ENUMS[kind](value["value"])
    if kind == "tuple":
        _object(value, ("type", "items"))
        if type(value["items"]) is not list or len(value["items"]) > 256:
            raise ValidationError("invalid journal tuple")
        return tuple(_decode(item, depth + 1) for item in value["items"])
    if kind == "MarginAccount":
        _object(value, ("type", "value"))
        return account_from_document(value["value"])
    if kind == "PreparedFundingBatch":
        _object(value, ("type", "baseline", "document"))
        baseline = _decode(value["baseline"], depth + 1)
        if type(baseline) is not MarginAccount:
            raise ValidationError("funding journal requires its exact original account baseline")
        try:
            return restore_funding_batch(value["document"], baseline)
        except ProtocolError:
            raise ValidationError(
                "frozen funding source/prefix differs from its baseline"
            ) from None
    if kind not in _CLASSES and kind not in _BATCHES:
        raise ValidationError("unknown virtual margin journal type")
    _object(value, ("type", "fields"))
    constructor = _BATCHES[kind] if kind in _BATCHES else _CLASSES[kind]
    names = {field.name for field in fields(constructor)}
    if constructor in _BATCHES.values():
        names = names - {"prefix_accounts"} | {"baseline"}
    raw = _object(value["fields"], names)
    data = {name: _decode(member, depth + 1) for name, member in raw.items()}
    if constructor in _BATCHES.values():
        baseline = data.pop("baseline")
        if (
            type(baseline) is not MarginAccount
            or type(data["facts"]) is not tuple
            or any(type(item) is not FillEvent for item in data["facts"])
            or len(data["facts"]) > 100
        ):
            raise ValidationError("invalid frozen batch factual prefix")
        data["prefix_accounts"] = _prefixes(baseline, data["facts"])
    # Native-type validation must happen before constructor validators/coercions.
    for name, annotation in _annotations(constructor).items():
        if not _matches(data[name], annotation):
            raise ValidationError("virtual margin journal field has the wrong native type")
    result = constructor(**data)
    _batch(result) if constructor in _BATCHES.values() else _typed(result)
    return result


def _binding(record):
    if type(record) is not VirtualMarginRecord:
        raise ValidationError("typed virtual margin record required")
    payload = record.payload
    accounts = (
        (payload.account.account_id,)
        if isinstance(payload, AccountAnchor)
        else (payload.original.account_id,)
        if isinstance(payload, FrozenPendingOrder)
        else tuple(account.account_id for account in payload.prefix_accounts)
        + tuple(
            order.original.account_id for order in (*payload.original_pending, *payload.remaining)
        )
        + tuple(
            admission.reservation.account_id
            for admission in payload.admissions
            if admission.reservation is not None
        )
        if isinstance(payload, PreparedMarginBatch)
        else tuple(account.account_id for account in payload.prefix_accounts)
        if isinstance(payload, PreparedFundingBatch)
        else ()
    )
    if any(account_id != record.account_id for account_id in accounts):
        raise ValidationError("virtual margin record belongs to another account")


def encode_record(record: VirtualMarginRecord) -> bytes:
    """Versioned data-only JSON; Decimal scale is preserved, never normalized."""
    _binding(record)
    data = _encode(record)
    # Match atomic_json's exact on-disk formatting for the physical byte budget.
    result = json.dumps(
        {
            "schema_version": 1,
            "data": data,
            "record_checksum": record.checksum,
            "sha256": sha256(_json(data)).hexdigest(),
        },
        ensure_ascii=False,
        sort_keys=True,
        allow_nan=False,
    ).encode("utf-8")
    if len(result) > MAX_RECORD_BYTES:
        raise ValidationError("virtual margin journal record exceeds capacity")
    return result


def decode_record(payload: bytes) -> VirtualMarginRecord:
    """Reject corruption rather than reinterpret it as an absent journal."""
    if type(payload) is not bytes or not 1 <= len(payload) <= MAX_RECORD_BYTES:
        raise ValidationError("invalid virtual margin journal payload size")
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_unique,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("nonfinite JSON")),
        )
        _object(value, ("schema_version", "data", "record_checksum", "sha256"))
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValidationError("unsupported virtual margin journal version")
        _hash(value["sha256"])
        _hash(value["record_checksum"])
        if sha256(_json(value["data"])).hexdigest() != value["sha256"]:
            raise ValidationError("virtual margin journal content checksum mismatch")
        result = _decode(value["data"])
        _binding(result)
        if result.checksum != value["record_checksum"]:
            raise ValidationError("virtual margin journal record checksum mismatch")
        return result
    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError, RecursionError):
        raise ValidationError("invalid virtual margin journal encoding") from None


def _guard(path):
    for current in (path, *path.parents):
        try:
            details = current.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(details.st_mode) or getattr(details, "st_file_attributes", 0) & 0x400:
            raise StorageError("virtual margin journal cannot use linked paths")
        if current != path and not stat.S_ISDIR(details.st_mode):
            raise StorageError("virtual margin journal parent is not a directory")


def _descriptor(path):
    if os.name != "nt":
        return os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    # No-follow final component, DELETE sharing for short atomic-file reads.
    import ctypes
    import msvcrt
    from ctypes import wintypes

    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    create = kernel.CreateFileW
    create.argtypes = (
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    )
    create.restype = wintypes.HANDLE
    close = kernel.CloseHandle
    close.argtypes, close.restype = (wintypes.HANDLE,), wintypes.BOOL
    handle = create(str(path), 0x80000000, 7, None, 3, 0x00200000, None)
    if handle == wintypes.HANDLE(-1).value:
        raise ctypes.WinError(ctypes.get_last_error())
    try:
        return msvcrt.open_osfhandle(handle, os.O_RDONLY | os.O_BINARY)
    except BaseException:
        close(handle)
        raise


def _read(path):
    _guard(path)
    descriptor = _descriptor(path)
    try:
        details = os.fstat(descriptor)
        if (
            not stat.S_ISREG(details.st_mode)
            or getattr(details, "st_file_attributes", 0) & 0x400
            or details.st_size > MAX_RECORD_BYTES
        ):
            raise StorageError("invalid virtual margin journal file")
        stream = os.fdopen(descriptor, "rb")
        descriptor = None
        with stream:
            raw = stream.read(MAX_RECORD_BYTES + 1)
        try:
            return decode_record(raw)
        except ValidationError:
            raise StorageError(
                "virtual margin journal record is corrupt; original preserved"
            ) from None
    finally:
        if descriptor is not None:
            os.close(descriptor)


class FileVirtualMarginJournal:
    """Exclusive project/account writer with an independent local fallback.

    A custom fallback is account-private and cannot be shared across projects.
    These local checksums detect corruption, not malicious history replacement.
    Both namespaces retain their originals; there is no automatic pruning.
    """

    def __init__(self, root, account_id, *, fallback_root=None):
        _identity(account_id)
        self.root = Path(os.path.abspath(root))
        self.account_id = account_id
        digest = sha256(account_id.encode("utf-8")).hexdigest()
        primary = self.root / "state/margin-v2-journal"
        fallback = (
            Path(os.path.abspath(fallback_root))
            if fallback_root is not None
            else self.root / "state/margin-v2-fallback"
        )
        if primary.is_relative_to(fallback) or fallback.is_relative_to(primary):
            raise ValidationError("virtual margin journal namespaces must not overlap")
        self.primary, self.fallback = primary / digest, fallback / digest
        self.lock_path = self.root / "state/locks" / ("margin-v2-journal-" + digest + ".lock")
        self.lock, self.closed = RLock(), False
        _guard(self.primary)
        _guard(self.lock_path)
        self.owner = InstanceLock(self.lock_path)

    def _open(self, account_id):
        if self.closed:
            raise StorageError("virtual margin journal owner is closed")
        if account_id != self.account_id:
            raise ValidationError("virtual margin journal belongs to another account")
        _guard(self.lock_path)

    def _inventory(self, directory):
        _guard(directory)
        try:
            # Cap even orphan files before allocating a directory-wide list.
            entries = list(islice(directory.iterdir(), MAX_RECORDS + 1))
        except FileNotFoundError:
            return (), 0
        if len(entries) > MAX_RECORDS:
            raise _Capacity("virtual margin journal file capacity reached")
        records, total = [], 0
        for path in entries:
            _guard(path)
            details = path.lstat()
            if not stat.S_ISREG(details.st_mode):
                raise StorageError("unexpected virtual margin journal entry")
            total += details.st_size
            if path.name.startswith(".stage-"):
                continue  # A crash orphan is retained and counted, never replayed.
            if path.suffix != ".json" or _HASH.fullmatch(path.stem) is None:
                raise StorageError("unexpected virtual margin journal filename")
            records.append(path)
            if len(records) > MAX_RECORDS:
                raise _Capacity("virtual margin journal record capacity reached")
        if total > MAX_NAMESPACE_BYTES:
            raise _Capacity("virtual margin journal byte capacity reached")
        return tuple(sorted(records)), total

    def read(self, account_id):
        with self.lock:
            self._open(account_id)
            result = []
            try:
                for directory, durability in (
                    (self.primary, Durability.PRIMARY),
                    (self.fallback, Durability.FALLBACK),
                ):
                    paths, _ = self._inventory(directory)
                    for path in paths:
                        record = _read(path)
                        if record.account_id != account_id or record.record_id != path.stem:
                            raise StorageError("virtual margin journal file identity mismatch")
                        result.append(JournalReceipt(record, durability))
            except (OSError, ValidationError):
                raise StorageError(
                    "virtual margin journal cannot be verified; originals preserved"
                ) from None
            return tuple(result) if result else None

    def _publish(self, directory, record, payload):
        _guard(directory)
        paths, total = self._inventory(directory)
        target = directory / (record.record_id + ".json")
        if target in paths:
            existing = _read(target)
            if existing != record or encode_record(existing) != payload:
                raise ProtocolError("virtual margin journal record identity conflict")
            return
        if len(paths) >= MAX_RECORDS or total + len(payload) > MAX_NAMESPACE_BYTES:
            raise _Capacity("virtual margin journal capacity reached; originals preserved")
        missing = []
        parent = directory
        while not parent.exists():
            missing.append(parent)
            parent = parent.parent
        for child in reversed(missing):
            _guard(child)
            child.mkdir(exist_ok=True)
            if os.name != "nt":
                descriptor = os.open(child.parent, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        _guard(directory)
        temporary = directory / (".stage-" + uuid4().hex + ".json")
        try:
            atomic_json(temporary, json.loads(payload))
            _guard(target)
            try:
                os.link(temporary, target)
            except FileExistsError:
                if encode_record(_read(target)) != payload:
                    raise ProtocolError("virtual margin journal record identity conflict") from None
            if os.name != "nt":
                descriptor = os.open(directory, os.O_RDONLY)
                try:
                    os.fsync(descriptor)
                finally:
                    os.close(descriptor)
        finally:
            temporary.unlink(missing_ok=True)

    def append(self, record):
        payload = encode_record(record)
        with self.lock:
            self._open(record.account_id)
            for directory, durability in (
                (self.primary, Durability.PRIMARY),
                (self.fallback, Durability.FALLBACK),
            ):
                try:
                    self._publish(directory, record, payload)
                    return durability
                except (OSError, _Capacity):
                    continue
            return Durability.VOLATILE

    @property
    def released(self):
        return self.owner.released

    def close(self):
        with self.lock:
            self.closed = True
            if not self.released:
                self.owner.close()
            if not self.released:
                raise StorageError("margin journal lease release is not confirmed")
