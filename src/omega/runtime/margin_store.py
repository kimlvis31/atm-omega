"""Versioned local margin snapshots, separate from v1 and market PostgreSQL data."""

import json
import os
import re
import stat
from dataclasses import fields
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from threading import RLock

from omega.application.margin_funding import funding_fact_from_document
from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.domain.margin import (
    MAX_EVENTS,
    MAX_POSITIONS,
    CollateralTransfer,
    FillEvent,
    FundingCashflow,
    LedgerEntry,
    MarginAccount,
    MarginMode,
    MarginPosition,
    apply_fill,
    apply_funding,
    apply_transfer,
)
from omega.domain.models import Instrument, Market, decimal
from omega.runtime.control import InstanceLock
from omega.runtime.settings import atomic_json

MAX_BYTES = 16_000_000


def _encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValueError("duplicate margin state field")
        result[key] = value
    return result


def _object(value, kind, *, extra=()):
    if type(value) is not dict or set(value) != {field.name for field in fields(kind)} | set(extra):
        raise ValidationError("invalid margin snapshot fields")
    return dict(value)


def _money(value):
    if type(value) is not str or len(value) > 1024:
        raise ValidationError("margin state amounts must be bounded decimal strings")
    return decimal(value)


def _instrument(value):
    if type(value) is not str:
        raise ValidationError("invalid margin snapshot instrument")
    provider, market, symbol = value.split(":")
    return Instrument(provider, Market(market), symbol)


def _position_identity(account):
    return tuple(
        sorted(
            (
                position.instrument.key,
                position.owner_id,
                position.margin_mode,
                position.leverage,
                position.budget_margin,
            )
            for position in account.positions
        )
    )


def _item_document(value):
    result = {}
    for field in fields(value):
        member = getattr(value, field.name)
        result[field.name] = (
            str(member)
            if isinstance(member, Decimal)
            else member.key
            if isinstance(member, Instrument)
            else member
        )
    return result


def _entry_document(entry):
    if isinstance(entry.fact, FillEvent):
        kind = "fill"
    elif isinstance(entry.fact, CollateralTransfer):
        kind = "transfer"
    elif isinstance(entry.fact, FundingCashflow):
        kind = "funding"
    else:
        raise ValidationError("unknown typed margin ledger fact")
    return {**_item_document(entry), "fact": {"kind": kind, **_item_document(entry.fact)}}


def ledger_entry_document(entry):
    """Exact existing public row, without copying a whole financial account."""
    if not isinstance(entry, LedgerEntry):
        raise ValidationError("typed margin ledger entry required")
    return _entry_document(entry)


def ledger_entry_from_document(value):
    """Pure strict row decoder; account/source authority is checked separately."""
    entry = _object(value, LedgerEntry)
    raw = entry["fact"]
    if (
        type(raw) is not dict
        or type(raw.get("kind")) is not str
        or raw["kind"] not in {"fill", "transfer", "funding"}
    ):
        raise ValidationError("unknown margin ledger fact")
    if raw["kind"] == "funding":
        entry["fact"] = funding_fact_from_document(
            {key: member for key, member in raw.items() if key != "kind"}
        )
    else:
        constructor = {"fill": FillEvent, "transfer": CollateralTransfer}[raw["kind"]]
        fact = _object(raw, constructor, extra=("kind",))
        del fact["kind"]
        fact["instrument"] = _instrument(fact["instrument"])
        for key in ("delta_qty", "price", "fee") if constructor is FillEvent else ("amount",):
            fact[key] = _money(fact[key])
        entry["fact"] = constructor(**fact)
    for key in ("realized_pnl", "fee", "cross_delta", "isolated_delta"):
        entry[key] = _money(entry[key])
    return LedgerEntry(**entry)


def account_document(account, *, ledger_limit=None):
    if not isinstance(account, MarginAccount):
        raise ValidationError("typed margin account required")
    if ledger_limit is not None and (type(ledger_limit) is not int or not 1 <= ledger_limit <= 128):
        raise ValidationError("account summary ledger limit must be within1..128")

    data = _item_document(account)
    data["positions"] = [_item_document(position) for position in account.positions]
    data["ledger"] = []
    for entry in account.ledger if ledger_limit is None else account.ledger[-ledger_limit:]:
        data["ledger"].append(_entry_document(entry))
    version = 2 if any(isinstance(entry.fact, FundingCashflow) for entry in account.ledger) else 1
    result = {"schema_version": version, "account": data}
    if ledger_limit is not None:
        # An explicitly partial display document is never accepted by the
        # strict financial snapshot decoder. Durable facts remain unchanged.
        result["summary"] = {
            "ledger_total": len(account.ledger),
            "ledger_limit": ledger_limit,
            "ledger_truncated": len(account.ledger) > ledger_limit,
        }
    return result


def account_from_document(value):
    if (
        type(value) is not dict
        or set(value) != {"schema_version", "account"}
        or type(value["schema_version"]) is not int
        or value["schema_version"] not in {1, 2}
    ):
        raise ValidationError("invalid margin snapshot version")
    data = _object(value["account"], MarginAccount)
    if (
        type(data["positions"]) is not list
        or len(data["positions"]) > MAX_POSITIONS
        or type(data["ledger"]) is not list
        or len(data["ledger"]) > MAX_EVENTS
    ):
        raise ValidationError("invalid margin state tables")
    positions, ledger = [], []
    for row in data["positions"]:
        position = _object(row, MarginPosition)
        position["instrument"] = _instrument(position["instrument"])
        position["margin_mode"] = MarginMode(position["margin_mode"])
        for key in ("budget_margin", "quantity", "entry_price", "isolated_wallet", "realized_pnl"):
            position[key] = _money(position[key])
        positions.append(MarginPosition(**position))
    for row in data["ledger"]:
        entry = ledger_entry_from_document(row)
        if isinstance(entry.fact, FundingCashflow) and value["schema_version"] != 2:
            raise ValidationError("funding facts require the explicit funding snapshot version")
        ledger.append(entry)
    data.update(
        positions=tuple(positions), ledger=tuple(ledger), cross_wallet=_money(data["cross_wallet"])
    )
    if data["opening_balance"] is not None:
        data["opening_balance"] = _money(data["opening_balance"])
    account = MarginAccount(**data)
    if value["schema_version"] == 2 and not any(
        isinstance(entry.fact, FundingCashflow) for entry in account.ledger
    ):
        raise ValidationError("funding snapshot version requires a funding fact")
    return account


class FileMarginStateStore:
    """One exclusive account owner. A failed save never rewrites previous data.

    Save failure may occur after replacement; callers must reconcile the exact
    candidate rather than assume no commit. This is not a market-data store.
    """

    def __init__(self, root, account_id):
        if (
            type(account_id) is not str
            or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", account_id) is None
        ):
            raise ValidationError("invalid margin account identity")
        self.root, self.account_id = Path(root).resolve(), account_id
        self.directory = self.root / "state/margin-v2"
        name = sha256(account_id.encode()).hexdigest()
        self.path = self.directory / (name + ".json")
        self.lock_path = self.directory / (name + ".lock")
        self.lock, self.closed = RLock(), False
        self._check_paths()
        self.owner = InstanceLock(self.lock_path)

    def _check_paths(self):
        for path in (self.root / "state", self.directory, self.path, self.lock_path):
            if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
                raise StorageError("margin snapshots cannot use linked paths")
        for path in (self.root / "state", self.directory):
            try:
                details = path.stat()
            except FileNotFoundError:
                continue
            if not stat.S_ISDIR(details.st_mode):
                raise StorageError("margin snapshot parent is not a directory")

    def _require_open(self, account_id):
        if self.closed:
            raise StorageError("margin state owner is closed")
        if account_id != self.account_id:
            raise ValidationError("margin snapshot belongs to another account")
        self._check_paths()

    def load(self, account_id):
        with self.lock:
            self._require_open(account_id)
            descriptor = None
            try:
                descriptor = os.open(
                    self.path,
                    os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0),
                )
                details = os.fstat(descriptor)
                if not stat.S_ISREG(details.st_mode) or details.st_size > MAX_BYTES:
                    raise StorageError("margin snapshot must be a bounded regular file")
                stream = os.fdopen(descriptor, "rb")
                descriptor = None
                with stream:
                    raw = stream.read(MAX_BYTES + 1)
            except FileNotFoundError:
                return None
            except OSError:
                raise StorageError("margin snapshot cannot be read") from None
            finally:
                if descriptor is not None:
                    os.close(descriptor)
            try:
                if len(raw) > MAX_BYTES:
                    raise ValueError
                envelope = json.loads(raw, object_pairs_hook=_unique)
                if (
                    type(envelope) is not dict
                    or set(envelope) != {"data", "sha256"}
                    or envelope["sha256"] != sha256(_encoded(envelope["data"])).hexdigest()
                ):
                    raise ValueError
                account = account_from_document(envelope["data"])
                if account.account_id != self.account_id:
                    raise ValueError
                return account
            except (ValueError, TypeError, KeyError, RecursionError, ValidationError):
                raise StorageError("margin snapshot is invalid; original preserved") from None

    def save(self, account, *, expected_sequence):
        if (
            not isinstance(account, MarginAccount)
            or expected_sequence is not None
            and (type(expected_sequence) is not int or expected_sequence < 0)
        ):
            raise ValidationError("invalid margin save candidate or sequence")
        with self.lock:
            self._require_open(account.account_id)
            current = self.load(account.account_id)
            if (
                (current is None) != (expected_sequence is None)
                or current is not None
                and len(current.ledger) != expected_sequence
            ):
                raise ProtocolError("margin state sequence conflict; no automatic rebase")
            if current is not None and (
                len(account.ledger) != len(current.ledger) + 1
                or account.ledger[: len(current.ledger)] != current.ledger
                or account.settlement_asset != current.settlement_asset
                or account.event_capacity != current.event_capacity
                or account.opening_balance != current.opening_balance
                or _position_identity(account) != _position_identity(current)
            ):
                raise ProtocolError("margin save must append one fact to its unchanged history")
            if current is not None:
                fact = account.ledger[-1].fact
                if isinstance(fact, FillEvent):
                    transition = apply_fill(current, fact)
                elif isinstance(fact, CollateralTransfer):
                    transition = apply_transfer(current, fact)
                elif isinstance(fact, FundingCashflow):
                    transition = apply_funding(current, fact)
                else:
                    raise ProtocolError("unknown typed appended margin fact")
                if transition.account != account:
                    raise ProtocolError("margin candidate differs from its appended fact")
            data = account_document(account)
            envelope = {"data": data, "sha256": sha256(_encoded(data)).hexdigest()}
            if len(_encoded(envelope)) > MAX_BYTES:
                raise StorageError("margin snapshot capacity reached")
            try:
                atomic_json(self.path, envelope)
            except OSError:
                raise StorageError("margin snapshot commit outcome is unknown") from None

    @property
    def released(self):
        return self.owner.released

    def close(self):
        with self.lock:
            self.closed = True  # Block IO even if the lease still needs cleanup.
            if not self.released:
                self.owner.close()
            if not self.released:
                raise StorageError("margin state lease release is not confirmed")
