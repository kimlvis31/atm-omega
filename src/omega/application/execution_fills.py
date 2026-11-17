"""One bounded actual-fill page; recorded history is never account authority.

No order mutations or financial engine updates. The caller supplies immutable
owned order observations, chooses the initial scan boundary and reconciles the
account separately. A short/empty page does not prove universal history coverage.
"""

import json
import re
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from fractions import Fraction
from hashlib import sha256

from omega.application.orders import _validate_observation
from omega.domain.errors import JournalError, ProtocolError, TradingBlocked, ValidationError
from omega.domain.execution_rules import MAX_TIME, bounded_decimal
from omega.domain.models import Fill, Instrument, OrderObservation, OrderRequest, Side
from omega.domain.safety import BlockReason
from omega.ports.journal import Durability

FACT = "ExecutionFillFact"
PAGE = "ExecutionFillPage"
MAX_PAGE = 1000
REPORTED_FIELDS = ("quote_quantity", "realized_pnl", "realized_pnl_asset")


def _format(value):
    if type(value) is not int or value not in {1, 2}:
        raise ValidationError("fill fact format must be native integer 1 or 2")
    return value


def _hash(value):
    return sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _id(value):
    if type(value) is not str or re.fullmatch(r"0|[1-9][0-9]{0,19}", value) is None:
        raise ValidationError("fill IDs require canonical numeric strings of at most 20 digits")
    return value


def _number(value):
    if type(value) is not Decimal:
        raise ValidationError("fill amounts require native Decimals")
    bounded_decimal(value)
    if value == 0:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _text(value):
    if type(value) is not str or not 1 <= len(value) <= 128 or not value.isprintable():
        raise ValidationError("fill identities require bounded native text")
    return value


def _checked(fill, account_id, instrument):
    if type(fill) is not Fill or type(fill.instrument) is not Instrument:
        raise ValidationError("actual fill page requires native Fill facts")
    if fill.account_id != account_id or fill.instrument != instrument:
        raise ProtocolError("actual fill belongs to another account or instrument")
    _id(fill.trade_id)
    _text(fill.account_id)
    _text(fill.provider_order_id)
    if (
        type(fill.side) is not Side
        or type(fill.timestamp_ms) is not int
        or not (0 <= fill.timestamp_ms <= MAX_TIME)
    ):
        raise ValidationError("fill side or actual timestamp is invalid")
    if (
        type(fill.fee_asset) is not str
        or len(fill.fee_asset) > 40
        or (
            fill.fee_asset
            and (not fill.fee_asset.isprintable() or fill.fee_asset.strip() != fill.fee_asset)
        )
    ):
        raise ValidationError("fill commission asset must be bounded native text")
    for value in (fill.quantity, fill.price, fill.fee):
        _number(value)
    return replace(fill, instrument=replace(fill.instrument))


def _document(fill, *, format_version=1):
    _format(format_version)
    document = {
        "account_id": fill.account_id,
        "instrument": fill.instrument.key,
        "trade_id": fill.trade_id,
        "provider_order_id": fill.provider_order_id,
        "side": fill.side.value,
        "quantity": _number(fill.quantity),
        "price": _number(fill.price),
        "fee": _number(fill.fee),
        "fee_asset": fill.fee_asset,
        "timestamp_ms": fill.timestamp_ms,
    }
    if format_version == 1:
        if any(getattr(fill, name) is not None for name in REPORTED_FIELDS):
            raise ValidationError("reported fill metadata requires explicit fact format 2")
    else:
        document.update(
            {
                name: None if getattr(fill, name) is None else _number(getattr(fill, name))
                for name in REPORTED_FIELDS[:2]
            }
        )
        document["realized_pnl_asset"] = fill.realized_pnl_asset
    return document


def _keys(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise JournalError("invalid recorded fill recovery fields")


@dataclass(frozen=True, slots=True)
class RecordedFillHistory:
    facts: tuple[Fill, ...]
    cursor: int
    revision: int
    page_sha256: str | None
    orphan_trade_ids: tuple[str, ...]
    recovery_required: bool = True
    format_version: int = 1


@dataclass(frozen=True, slots=True)
class FillPageReceipt:
    facts: tuple[Fill, ...]
    new_facts: tuple[Fill, ...]
    from_id: int
    through_id: int
    revision: int
    page_sha256: str
    needs_more: bool
    primary_acknowledged: bool = True
    recovery_required: bool = True
    history_complete: bool = False
    format_version: int = 1


class ExecutionFillRecovery:
    """Single writer, one inclusive page, exact pending-write retry.

    Format1 remains the original immutable namespace and rejects reported extra
    facts rather than dropping them. Explicit format_version=2 uses a separate
    scan namespace for nullable provider quote/PnL fields. It recaptures from the
    caller's explicit initial boundary; it never enriches/migrates/reapplies v1
    history or authorizes account settlement and entry resume.
    """

    def __init__(
        self,
        account_id,
        instrument,
        broker,
        sink,
        gate,
        *,
        initial_from_id,
        owned,
        format_version=1,
    ):
        self._format_version = _format(format_version)
        _text(account_id)
        if type(instrument) is not Instrument or broker.account_id != account_id:
            raise ValidationError("fill recovery broker/account/instrument identity differs")
        if type(initial_from_id) is not int or not 0 <= initial_from_id < 10**20:
            raise ValidationError("fill scan needs an explicit bounded initial numeric cursor")
        if type(owned) is not tuple or not 1 <= len(owned) <= MAX_PAGE:
            raise ValidationError("fill scan needs bounded immutable owned order pairs")
        self.account_id, self.instrument = account_id, replace(instrument)
        self.broker, self.sink, self.gate = broker, sink, gate
        self._scan = {
            "account_id": account_id,
            "instrument": instrument.key,
            "initial_from_id": str(initial_from_id),
        }
        if self._format_version == 2:
            self._scan["format_version"] = 2
        self._owned = {}
        for pair in owned:
            if type(pair) is not tuple or len(pair) != 2:
                raise ValidationError("fill ownership requires request/observation pairs")
            request, observation = pair
            if type(request) is not OrderRequest or type(observation) is not OrderObservation:
                raise ValidationError("fill ownership requires native order records")
            request, observation = replace(request), replace(observation)
            if request.instrument != self.instrument:
                raise ValidationError("owned order belongs to another instrument")
            _validate_observation(request, observation)
            _text(observation.provider_order_id)
            for value in (
                request.quantity,
                observation.executed_quantity,
                observation.cumulative_quote,
            ):
                _number(value)
            key = observation.provider_order_id
            if key in self._owned and self._owned[key] != (request, observation):
                raise ValidationError("provider order identity has conflicting ownership")
            self._owned[key] = request, observation
        self._pending = None
        self.gate.block(BlockReason.EXECUTION)
        try:
            self._facts, self._pages, self._committed = self._read()
        except (JournalError, OSError, TimeoutError):
            self.gate.block(BlockReason.STORAGE)
            raise

    def _ownership(self, facts):
        quantities = {}
        for fill in facts.values():
            pair = self._owned.get(fill.provider_order_id)
            if pair is None or fill.side != pair[0].side:
                self.gate.block(BlockReason.EXTERNAL)
                raise TradingBlocked("unowned actual fill; external intervention requires review")
            quantities[fill.provider_order_id] = quantities.get(
                fill.provider_order_id, Fraction()
            ) + (Fraction(fill.quantity))
        for identity, quantity in quantities.items():
            request, observation = self._owned[identity]
            if quantity > Fraction(request.quantity):
                self.gate.block(BlockReason.EXTERNAL)
                raise ProtocolError("actual fills exceed the immutable original order quantity")
            if quantity > Fraction(observation.executed_quantity):
                self.gate.block(BlockReason.EXECUTION)
                raise TradingBlocked("fill observations are stale; refresh original order query")

    def _fact_event(self, fill):
        event = {
            "kind": FACT,
            "schema_version": self._format_version,
            "scan": dict(self._scan),
            "fill": _document(fill, format_version=self._format_version),
        }
        return event | {"sha256": _hash(event)}

    def _decode_fact(self, event):
        _keys(event, ("kind", "schema_version", "scan", "fill", "sha256"))
        row = event["fill"]
        keys = (
            "account_id",
            "instrument",
            "trade_id",
            "provider_order_id",
            "side",
            "quantity",
            "price",
            "fee",
            "fee_asset",
            "timestamp_ms",
        )
        _keys(row, keys if self._format_version == 1 else keys + REPORTED_FIELDS)
        metadata = {}
        if self._format_version == 2:
            for name in REPORTED_FIELDS[:2]:
                value = row[name]
                if value is not None and type(value) is not str:
                    raise JournalError("recorded reported amounts require nullable decimal text")
                try:
                    metadata[name] = None if value is None else Decimal(value)
                except ArithmeticError:
                    raise JournalError("invalid recorded reported amount") from None
            metadata["realized_pnl_asset"] = row["realized_pnl_asset"]
        if row["instrument"] != self.instrument.key or any(
            type(row[name]) is not str for name in ("quantity", "price", "fee")
        ):
            raise JournalError("recorded actual fill identity or amounts changed")
        fill = _checked(
            Fill(
                row["account_id"],
                self.instrument,
                row["trade_id"],
                row["provider_order_id"],
                Side(row["side"]),
                Decimal(row["quantity"]),
                Decimal(row["price"]),
                Decimal(row["fee"]),
                row["fee_asset"],
                row["timestamp_ms"],
                **metadata,
            ),
            self.account_id,
            self.instrument,
        )
        if event != self._fact_event(fill):
            raise JournalError("recorded actual fill checksum or canonical content changed")
        return fill

    def _page_event(self, facts, returned_count, from_id, revision, previous):
        ids = sorted(facts, key=int)
        event = {
            "kind": PAGE,
            "schema_version": self._format_version,
            "scan": dict(self._scan),
            "revision": revision,
            "previous_sha256": previous,
            "from_id": str(from_id),
            "through_id": ids[-1] if ids else str(from_id),
            "trade_ids": ids,
            "returned_count": returned_count,
            "page_hash": _hash([(key, self._fact_event(facts[key])["sha256"]) for key in ids]),
        }
        return event | {"sha256": _hash(event)}

    def _read(self):
        facts, pages = {}, {}
        for event in self.sink.records():
            if type(event) is not dict:
                raise JournalError("fill recovery history requires typed events")
            if event.get("scan") != self._scan:
                continue
            if type(event["scan"]) is not dict or any(
                type(event["scan"][name]) is not type(value) for name, value in self._scan.items()
            ):
                raise JournalError("recorded fill scan requires native namespace fields")
            if event.get("kind") not in {FACT, PAGE}:
                raise JournalError("unexpected event in the owned fill scan")
            if (
                type(event.get("schema_version")) is not int
                or event["schema_version"] != self._format_version
            ):
                raise JournalError("unsupported fill recovery schema")
            if event["kind"] == FACT:
                fill = self._decode_fact(event)
                if int(fill.trade_id) < int(self._scan["initial_from_id"]):
                    raise JournalError("recorded fill precedes explicit scan boundary")
                if fill.trade_id in facts and facts[fill.trade_id] != fill:
                    raise ProtocolError("same actual fill key has conflicting financial contents")
                facts[fill.trade_id] = fill
            else:
                _keys(
                    event,
                    (
                        "kind",
                        "schema_version",
                        "scan",
                        "revision",
                        "previous_sha256",
                        "from_id",
                        "through_id",
                        "trade_ids",
                        "returned_count",
                        "page_hash",
                        "sha256",
                    ),
                )
                revision = event["revision"]
                if type(revision) is not int or not 1 <= revision <= 2**63 - 1:
                    raise JournalError("invalid fill page revision")
                if revision in pages and pages[revision] != event:
                    raise ProtocolError("same fill page revision has conflicting contents")
                pages[revision] = deepcopy(event)
        committed, cursor, previous = set(), int(self._scan["initial_from_id"]), None
        for expected_revision, revision in enumerate(sorted(pages), 1):
            event = pages[revision]
            ids, count = event["trade_ids"], event["returned_count"]
            if (
                revision != expected_revision
                or type(ids) is not list
                or not len(ids) <= MAX_PAGE
                or type(count) is not int
                or not len(ids) <= count <= MAX_PAGE
                or bool(ids) != bool(count)
            ):
                raise JournalError("invalid bounded fill page chain")
            for value in ids:
                _id(value)
            if ids != sorted(set(ids), key=int) or any(
                key not in facts or int(key) < cursor for key in ids
            ):
                raise JournalError("fill page lacks its complete original facts")
            selected = {key: facts[key] for key in ids}
            if event != self._page_event(selected, count, cursor, revision, previous):
                raise JournalError("fill page chain, checksum or original cursor changed")
            committed.update(ids)
            cursor, previous = int(event["through_id"]), event["sha256"]
        if any(int(key) < cursor for key in set(facts) - committed):
            raise JournalError("recorded page cursor skipped an orphan fill fact")
        self._ownership(facts)
        return facts, pages, committed

    @property
    def history(self):
        latest = self._pages[max(self._pages)] if self._pages else None
        return RecordedFillHistory(
            tuple(self._facts[key] for key in sorted(self._facts, key=int)),
            int(latest["through_id"]) if latest else int(self._scan["initial_from_id"]),
            len(self._pages),
            latest["sha256"] if latest else None,
            tuple(sorted(set(self._facts) - self._committed, key=int)),
            format_version=self._format_version,
        )

    def recover_page(self):
        self.gate.block(BlockReason.EXECUTION)
        try:
            loaded = self._read()
        except (JournalError, OSError, TimeoutError):
            self.gate.block(BlockReason.STORAGE)
            raise
        if self._pending is None:
            if loaded != (self._facts, self._pages, self._committed):
                raise ProtocolError("fill recovery history changed outside its single writer")
            history = self.history
            rows = self.broker.trades(self.instrument, from_id=history.cursor)
            if type(rows) is not tuple or len(rows) > MAX_PAGE:
                raise ValidationError("broker fill page must contain at most 1000 native facts")
            selected = {}
            for raw in rows:
                if type(raw) is Fill and (
                    raw.account_id != self.account_id or raw.instrument != self.instrument
                ):
                    self.gate.block(BlockReason.EXTERNAL)
                fill = _checked(raw, self.account_id, self.instrument)
                if int(fill.trade_id) < history.cursor:
                    raise ProtocolError("broker returned a fill before the inclusive page cursor")
                previous = selected.get(fill.trade_id) or self._facts.get(fill.trade_id)
                if previous is not None and previous != fill:
                    raise ProtocolError("same actual fill key has conflicting financial contents")
                selected[fill.trade_id] = fill
            self._ownership(self._facts | selected)
            page = self._page_event(
                selected, len(rows), history.cursor, history.revision + 1, history.page_sha256
            )
            if any(
                key not in selected and int(key) <= int(page["through_id"])
                for key in set(self._facts) - self._committed
            ):
                raise ProtocolError(
                    "recorded orphan fill is absent; reconcile before cursor advancement"
                )
            self._pending = selected, page
        selected, page = self._pending
        loaded_facts, loaded_pages, _loaded_committed = loaded
        allowed_facts = self._facts | selected
        allowed_pages = self._pages | {page["revision"]: page}
        if (
            any(loaded_facts.get(key) != value for key, value in self._facts.items())
            or any(loaded_pages.get(key) != value for key, value in self._pages.items())
            or any(allowed_facts.get(key) != value for key, value in loaded_facts.items())
            or any(allowed_pages.get(key) != value for key, value in loaded_pages.items())
        ):
            raise ProtocolError(
                "pending fill recovery history conflicts with original frozen facts"
            )
        try:
            for fill in selected.values():
                if self.sink.record(deepcopy(self._fact_event(fill))) is not Durability.PRIMARY:
                    raise TradingBlocked("fill fact requires a fresh primary acknowledgement")
            if self.sink.record(deepcopy(page)) is not Durability.PRIMARY:
                raise TradingBlocked("fill page requires a fresh primary acknowledgement")
        except (JournalError, OSError, TimeoutError):
            self.gate.block(BlockReason.STORAGE)
            raise TradingBlocked(
                "fill page primary acknowledgement is unavailable; retry original page"
            ) from None
        except Exception:
            self.gate.block(BlockReason.STORAGE)
            raise
        new = tuple(
            selected[key] for key in sorted(selected, key=int) if key not in self._committed
        )
        self._facts.update(selected)
        self._pages[page["revision"]] = deepcopy(page)
        self._committed.update(selected)
        self._pending = None
        return FillPageReceipt(
            tuple(selected[key] for key in sorted(selected, key=int)),
            new,
            int(page["from_id"]),
            int(page["through_id"]),
            page["revision"],
            page["sha256"],
            page["returned_count"] == MAX_PAGE,
            format_version=self._format_version,
        )
