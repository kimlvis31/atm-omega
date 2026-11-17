"""Experimental local USDM full-fill scenarios; no broker, feed, HTTP or SQL.

All marks, brackets and candles are explicit manual_scenario inputs. This does
not enable v1 leverage or claim exchange mark/funding/liquidation simulation.
"""

# Imports
import argparse
import json
import os
import re
import stat
from decimal import Context, Decimal, localcontext
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

from omega.application.margin_account import MarginAccountOwner
from omega.application.margin_execution import MAX_TIME, FrozenPendingOrder
from omega.application.margin_risk import approve_order
from omega.application.virtual_margin import VirtualMarginEngine
from omega.domain.errors import OmegaError, StorageError, ValidationError
from omega.domain.margin import (
    MarginAccount,
    MarginBracket,
    MarginBracketSnapshot,
    MarginMode,
    MarginPosition,
    MarkSnapshot,
)
from omega.domain.models import Candle, Instrument, Market, OrderKind, Side
from omega.domain.trading import OrderProposal
from omega.runtime.margin_store import FileMarginStateStore, account_document
from omega.runtime.virtual_margin_journal import FileVirtualMarginJournal

MAX_INPUT_BYTES = 1_048_576
_MATH = Context(prec=256)
_LIMITATION = (
    "experimental manual full-fill USDM cross/isolated; no automatic TS, live orders, "
    "venue-rule validation, exchange mark, funding, liquidation or partial-fill generator"
)
_HELP = {
    "commands": [
        "help",
        "status",
        "resume",
        "pause",
        "reconcile",
        "cancel ID",
        "submit FILE",
        "step FILE",
        "exit",
    ],
    "source": "manual_scenario",
    "limitation": _LIMITATION,
    "pause": "blocks new entries; existing pending orders remain eligible",
    "recovery": (
        "restart pauses entries; use reconcile after an uncertain commit, then explicit resume"
    ),
}


def _unique(items):
    result = {}
    for key, value in items:
        if key in result:
            raise ValidationError("duplicate scenario field")
        result[key] = value
    return result


def _reject_number(_):
    raise ValidationError(
        "scenario amounts require decimal strings; JSON floats/nonfinite are forbidden"
    )


def _object(value, required, optional=()):
    if type(value) is not dict or not set(required) <= set(value) <= set(required) | set(optional):
        raise ValidationError("invalid scenario fields")
    return value


def _rows(value):
    if type(value) is not list or len(value) > 100:
        raise ValidationError("scenario rows must be a list of at most 100 items")
    return value


def _time(value):
    if type(value) is not int or not 0 <= value <= MAX_TIME:
        raise ValidationError("invalid native scenario timestamp")
    return value


def _money(value):
    if (
        type(value) is not str
        or len(value) > 128
        or re.fullmatch(r"-?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?", value) is None
    ):
        raise ValidationError("scenario amount requires a bounded decimal string")
    result = Decimal(value)
    if (
        not result.is_finite()
        or len(result.as_tuple().digits) > 18
        or abs(result.as_tuple().exponent) > 18
    ):
        raise ValidationError("scenario amount exceeds supported precision")
    return result


def _instrument(value):
    if type(value) is not str or value.count(":") != 2:
        raise ValidationError("scenario instrument must be a canonical key")
    provider, market, symbol = value.split(":")
    result = Instrument(provider, Market(market), symbol)
    if (
        result.provider != "binance"
        or result.market is not Market.USDM
        or not result.symbol.endswith("USDT")
    ):
        raise ValidationError("manual margin scenario only supports Binance linear USDT USDM")
    return result


def read_scenario(path):
    """Bounded strict user-selected JSON; never executes a Python input file."""
    path = Path(path)
    try:
        if path.is_symlink() or getattr(path, "is_junction", lambda: False)():
            raise ValidationError("scenario file cannot be a link")
        descriptor = os.open(
            path, os.O_RDONLY | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_NOFOLLOW", 0)
        )
        try:
            details = os.fstat(descriptor)
            if (
                not stat.S_ISREG(details.st_mode)
                or getattr(details, "st_file_attributes", 0) & 0x400
                or details.st_size > MAX_INPUT_BYTES
            ):
                raise ValidationError("scenario file must be a bounded regular file")
            stream = os.fdopen(descriptor, "rb")
            descriptor = None
            with stream:
                raw = stream.read(MAX_INPUT_BYTES + 1)
        finally:
            if descriptor is not None:
                os.close(descriptor)
        if len(raw) > MAX_INPUT_BYTES:
            raise ValidationError("scenario file exceeds capacity")
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_unique,
            parse_float=_reject_number,
            parse_constant=_reject_number,
        )
        if type(value) is not dict:
            raise ValidationError("scenario must be a JSON object")
        return value
    except (OSError, UnicodeError, ValueError, RecursionError):
        raise ValidationError("scenario file cannot be read as bounded strict JSON") from None


def initial_account(document):
    data = _object(document, ("account_id", "balance", "positions"))
    balance = _money(data["balance"])
    positions = []
    with localcontext(_MATH):
        collateral, allocated = Decimal(0), Decimal(0)
        for raw in _rows(data["positions"]):
            row = _object(
                raw,
                ("instrument", "owner_id", "margin_mode", "leverage", "budget_margin"),
                ("isolated_collateral",),
            )
            budget = _money(row["budget_margin"])
            isolated = _money(row.get("isolated_collateral", "0"))
            if budget < 0 or isolated < 0:
                raise ValidationError("initial budget and collateral cannot be negative")
            positions.append(
                MarginPosition(
                    _instrument(row["instrument"]),
                    row["owner_id"],
                    MarginMode(row["margin_mode"]),
                    row["leverage"],
                    budget,
                    isolated_wallet=isolated,
                )
            )
            collateral += isolated
            allocated += budget
        if balance < 0 or collateral > balance or allocated > balance:
            raise ValidationError("initial collateral or allocated budgets exceed balance")
        return MarginAccount(data["account_id"], "USDT", balance - collateral, tuple(positions))


def scenario_marks(rows):
    result = []
    for raw in _rows(rows):
        row = _object(raw, ("instrument", "price", "event_time_ms", "available_at_ms"))
        result.append(
            MarkSnapshot(
                _instrument(row["instrument"]),
                _money(row["price"]),
                _time(row["event_time_ms"]),
                _time(row["available_at_ms"]),
                "manual_scenario",
            )
        )
    return tuple(result)


def scenario_brackets(rows):
    result = []
    for raw in _rows(rows):
        data = _object(raw, ("instrument", "observed_at_ms", "available_at_ms", "rows"))
        brackets = []
        for raw_row in _rows(data["rows"]):
            row = _object(
                raw_row,
                (
                    "notional_floor",
                    "notional_cap",
                    "maintenance_rate",
                    "maintenance_amount",
                    "max_leverage",
                ),
            )
            brackets.append(
                MarginBracket(
                    *(
                        _money(row[key])
                        for key in (
                            "notional_floor",
                            "notional_cap",
                            "maintenance_rate",
                            "maintenance_amount",
                        )
                    ),
                    row["max_leverage"],
                )
            )
        checksum = sha256(
            json.dumps(
                data, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False
            ).encode()
        ).hexdigest()
        result.append(
            MarginBracketSnapshot(
                _instrument(data["instrument"]),
                tuple(brackets),
                _time(data["observed_at_ms"]),
                _time(data["available_at_ms"]),
                "manual_scenario",
                checksum,
            )
        )
    return tuple(result)


class MarginConsole:
    """One account and synchronous explicit scenarios, with no background worker."""

    def __init__(self, root, *, account_id=None, create=None):
        if (account_id is None) == (create is None):
            raise ValidationError("choose existing account or explicit creation")
        self.store = self.journal = None
        self.owner = self.engine = None
        self.create_authorized = create is not None
        try:
            initial = initial_account(read_scenario(create)) if create is not None else None
            identity = initial.account_id if initial is not None else account_id
            # The journal validates linked root/ancestor paths before the
            # snapshot store resolves its root or creates its lock directory.
            self.journal = FileVirtualMarginJournal(root, identity)
            self.store = FileMarginStateStore(root, identity)
            current = self.store.load(identity)
            if initial is None:
                if current is None:
                    raise StorageError(
                        "existing virtual wallet is missing; no new wallet was created"
                    )
                initial = current
            elif current is not None and (
                current.ledger or account_document(current) != account_document(initial)
            ):
                raise ValidationError("creation conflicts with the existing initial wallet")
            self.owner = MarginAccountOwner(
                initial, self.store, create_if_missing=self.create_authorized
            )
            self.engine = VirtualMarginEngine(
                self.owner, self.journal, create_if_missing=self.create_authorized
            )
        except BaseException:
            self.close()
            raise

    def status(self):
        snapshot = self.engine.snapshot()
        account = snapshot.account
        return {
            "source": "manual_scenario",
            "limitation": _LIMITATION,
            "account_id": account.account_id if account is not None else self.store.account_id,
            "state": snapshot.state,
            "paused": snapshot.paused,
            "error": snapshot.error,
            "owner": self.owner.status(),
            "sequence": len(account.ledger) if account is not None else None,
            "wallet": {
                "cross": str(account.cross_wallet),
                "opening_balance": str(account.opening_balance),
            }
            if account is not None
            else None,
            "positions": [
                {
                    "instrument": item.instrument.key,
                    "owner_id": item.owner_id,
                    "margin_mode": item.margin_mode.value,
                    "leverage": item.leverage,
                    "budget_margin": str(item.budget_margin),
                    "quantity": str(item.quantity),
                    "entry_price": str(item.entry_price),
                    "isolated_collateral": str(item.isolated_wallet),
                    "realized_pnl": str(item.realized_pnl),
                }
                for item in account.positions
            ]
            if account is not None
            else [],
            "pending": [
                {
                    "order_id": item.order_id,
                    "instrument": item.original.instrument.key,
                    "side": item.original.proposal.side.value,
                    "quantity": str(item.original.proposal.quantity),
                    "reducing": item.original.proposal.reducing,
                    "eligible_at_ms": item.eligible_at_ms,
                }
                for item in snapshot.pending
            ],
            "pending_record_id": snapshot.pending_record_id,
        }

    def submit(self, data):
        row = _object(
            data,
            (
                "order_id",
                "instrument",
                "side",
                "quantity",
                "kind",
                "price",
                "reducing",
                "reason",
                "as_of_ms",
                "worst_execution_price",
                "fee_rate",
                "marks",
                "brackets",
            ),
        )
        instrument, as_of = _instrument(row["instrument"]), _time(row["as_of_ms"])
        marks, brackets = scenario_marks(row["marks"]), scenario_brackets(row["brackets"])
        snapshot = self.engine.snapshot()
        if snapshot.account is None:
            raise StorageError("virtual account is not confirmed")
        known_at = max((item.fact.available_at_ms for item in snapshot.account.ledger), default=0)
        known_at = max(
            known_at, max((item.original.as_of_ms for item in snapshot.pending), default=0)
        )
        if as_of < known_at:
            raise ValidationError("scenario creation precedes its known account or pending facts")
        position = next(
            (item for item in snapshot.account.positions if item.instrument == instrument), None
        )
        if position is None:
            raise ValidationError("scenario instrument has no registered owner")
        proposal = OrderProposal(
            Side(row["side"]),
            _money(row["quantity"]),
            OrderKind(row["kind"]),
            None if row["price"] is None else _money(row["price"]),
            row["reducing"],
            row["reason"],
        )
        original = approve_order(
            snapshot.account,
            row["order_id"],
            proposal,
            instrument=instrument,
            owner_id=position.owner_id,
            as_of_ms=as_of,
            marks=marks,
            brackets=brackets,
            worst_execution_price=_money(row["worst_execution_price"]),
            fee_rate=_money(row["fee_rate"]),
            reservations=tuple(item.original for item in snapshot.pending),
        )
        eligible = ((as_of + 59999) // 60000) * 60000
        if eligible > MAX_TIME:
            raise ValidationError("next scenario entry minute exceeds timestamp range")
        self.engine.submit_intent(
            FrozenPendingOrder(original, eligible), marks=marks, brackets=brackets
        )
        return self.status()

    def step(self, data):
        row = _object(data, ("candles", "as_of_ms", "phase", "brackets"))
        candles = []
        for raw in _rows(row["candles"]):
            item = _object(
                raw, ("instrument", "open_time_ms", "open", "high", "low", "close", "volume")
            )
            opening = _time(item["open_time_ms"])
            candles.append(
                Candle(
                    _instrument(item["instrument"]),
                    opening,
                    _time(opening + 60000),
                    *(_money(item[key]) for key in ("open", "high", "low", "close", "volume")),
                )
            )
        plan = self.engine.prepare_batch(
            tuple(candles),
            as_of_ms=_time(row["as_of_ms"]),
            phase=row["phase"],
            brackets=scenario_brackets(row["brackets"]),
        )
        self.engine.commit_batch(plan)
        return {
            **self.status(),
            "batch": {
                "id": plan.batch_id,
                "phase": plan.phase,
                "fills": len(plan.facts),
                "admissions": [
                    {"order_id": item.order_id, "outcome": item.outcome, "reason": item.reason}
                    for item in plan.admissions
                ],
            },
        }

    def execute(self, command):
        if type(command) is not str or len(command) > 4096:
            raise ValidationError("scenario command exceeds limit")
        parts = command.strip().split(maxsplit=1)
        if not parts:
            return None
        name, argument = parts[0], parts[1] if len(parts) == 2 else None
        if name in {"help", "status", "resume", "pause", "reconcile", "exit"}:
            if argument is not None:
                raise ValidationError("command does not accept arguments")
            if name == "help":
                return dict(_HELP)
            if name == "exit":
                return {"exit": True}
            if name == "resume":
                self.engine.manual_resume(uuid4().hex)
            elif name == "pause":
                self.engine.pause_entries(uuid4().hex)
            elif name == "reconcile":
                if self.engine.snapshot().account is None and self.create_authorized:
                    # Only an explicitly authorized initial registration can
                    # precede its anchor. Existing candidate fills must instead
                    # be authorized by the wrapper's durable frozen history.
                    self.owner.reconcile()
                    self.engine = VirtualMarginEngine(
                        self.owner, self.journal, create_if_missing=True
                    )
                else:
                    self.engine.reconcile()
            return self.status()
        if name == "cancel" and argument:
            self.engine.cancel_order(argument, uuid4().hex)
            return self.status()
        if name in {"submit", "step"} and argument:
            if len(argument) >= 2 and argument[0] == argument[-1] and argument[0] in {'"', "'"}:
                argument = argument[1:-1]
            data = read_scenario(argument)
            return self.submit(data) if name == "submit" else self.step(data)
        raise ValidationError("unknown scenario command; use help")

    def close(self):
        try:
            if self.journal is not None:
                self.journal.close()
        finally:
            if self.store is not None:
                self.store.close()


def parser():
    result = argparse.ArgumentParser(
        description=(
            "Experimental manual USDM cross/isolated full-fill scenarios. "
            "No real orders or network."
        )
    )
    result.add_argument("--root", type=Path, default=Path.cwd(), help="Local Omega state root")
    selection = result.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--account", help="Open existing virtual account; never create a missing wallet"
    )
    selection.add_argument("--create", type=Path, help="Explicit flat account JSON registration")
    result.add_argument(
        "--execute",
        action="append",
        default=[],
        metavar="TEXT",
        help="Run a command, repeat for a bounded batch, then exit",
    )
    return result


def main(argv=None, *, input_fn=input, output=print):
    args = parser().parse_args(argv)
    console, failed = None, False

    def emit(value):
        output(json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False))

    try:
        if len(args.execute) > 128:
            raise ValidationError("scenario batch exceeds 128 commands")
        console = MarginConsole(args.root, account_id=args.account, create=args.create)
        initial = console.status()
        failed = initial["state"] != "ready"
        emit(initial)
        commands = iter(args.execute) if args.execute else None
        while True:
            try:
                command = next(commands) if commands is not None else input_fn("margin-v2> ")
            except (EOFError, StopIteration):
                break
            try:
                result = console.execute(command)
                if result is not None:
                    emit(result)
                if result is not None and result.get("exit"):
                    break
            except (OmegaError, OSError, ValueError, TypeError) as error:
                failed = True
                emit(
                    {
                        "error": type(error).__name__,
                        "message": str(error)
                        if isinstance(error, OmegaError)
                        else "scenario operation failed",
                        "source": "manual_scenario",
                        "next": "status or explicit reconcile; no automatic retry",
                    }
                )
        return 2 if failed else 0
    except KeyboardInterrupt:
        return 130
    except (OmegaError, OSError, ValueError, TypeError, RuntimeError) as error:
        emit(
            {
                "error": type(error).__name__,
                "message": str(error)
                if isinstance(error, OmegaError)
                else "scenario startup failed",
                "source": "manual_scenario",
            }
        )
        return 2
    finally:
        if console is not None:
            try:
                console.close()
            except (OmegaError, OSError, RuntimeError) as error:
                emit(
                    {
                        "error": type(error).__name__,
                        "message": "scenario resources could not be closed",
                        "source": "manual_scenario",
                    }
                )
                return 2


if __name__ == "__main__":
    raise SystemExit(main())
