"""Financial recovery and explicit known-terminal adoption; no dispatch or resume.

A matched book is not complete venue history, collateral approval, reservation
release or manual readiness. Adoption requires additional exact coverage checks.
The caller serializes this owner with the account.
"""

import json
from copy import deepcopy
from dataclasses import dataclass, replace
from decimal import Decimal
from fractions import Fraction
from hashlib import sha256
from threading import RLock

from omega.application.execution_bindings import ExecutionDecisionBindings, _read_document
from omega.application.execution_fills import ExecutionFillRecovery
from omega.application.execution_fills import _document as fill_document
from omega.application.execution_recovery import (
    ExecutionObservationRecovery,
)
from omega.application.execution_recovery import (
    _request as request_document,
)
from omega.application.orders import _TERMINAL, OrderCoordinator, _validate_observation
from omega.domain.errors import JournalError, TradingBlocked, ValidationError
from omega.domain.execution_account import (
    ExecutionAccountBook,
    ExecutionAccountOpening,
    apply_execution_fills,
    check_execution_account_snapshot,
    initial_execution_book,
)
from omega.domain.execution_rules import MAX_TIME
from omega.domain.models import (
    AccountSnapshot,
    Balance,
    Instrument,
    Market,
    OrderObservation,
    OrderStatus,
    PositionSnapshot,
)
from omega.domain.safety import BlockReason
from omega.ports.journal import Durability

KIND = "ExecutionAccountApplied"
MAX_RECORD_BYTES = 61_440
MAX_ACCOUNT_RECORDS = 128
MAX_SOURCE_RECORDS = 512


def _encoded(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    if len(raw) > MAX_RECORD_BYTES:
        raise ValidationError("execution account record exceeds capacity")
    return raw


def _hash(value):
    return sha256(_encoded(value)).hexdigest()


def _number(value):
    if (
        type(value) is not Decimal
        or not value.is_finite()
        or len(value.as_tuple().digits) > 512
        or not -512 <= value.as_tuple().exponent <= 512
    ):
        raise ValidationError("account document requires finite native Decimal")
    if value == 0:
        return "0"
    text = format(value, "f")
    return text.rstrip("0").rstrip(".") if "." in text else text


def _parse(value):
    if type(value) is not str or len(value) > 1100:
        raise ValidationError("account document amount is invalid")
    number = Decimal(value)
    if _number(number) != value:
        raise ValidationError("account document amount is noncanonical")
    return number


def _account_document(account):
    return {
        "account_id": account.account_id,
        "market": account.market.value,
        "can_trade": account.can_trade,
        "observed_at_ms": account.observed_at_ms,
        "balances": [
            {
                "asset": row.asset,
                "wallet": _number(row.wallet),
                "available": _number(row.available),
                "locked": None if row.locked is None else _number(row.locked),
                "margin_balance": None
                if row.margin_balance is None
                else _number(row.margin_balance),
            }
            for row in sorted(account.balances, key=lambda row: row.asset)
        ],
        "positions": [
            {
                "instrument": row.instrument.key,
                "signed_quantity": _number(row.signed_quantity),
                "entry_price": _number(row.entry_price),
                "unrealized_pnl": _number(row.unrealized_pnl),
            }
            for row in sorted(account.positions, key=lambda row: row.instrument.key)
        ],
    }


def _read_account(row):
    def instrument(key):
        provider, market, symbol = key.split(":")
        return Instrument(provider, Market(market), symbol)

    account = AccountSnapshot(
        row["account_id"],
        Market(row["market"]),
        row["can_trade"],
        row["observed_at_ms"],
        tuple(
            Balance(
                item["asset"],
                _parse(item["wallet"]),
                _parse(item["available"]),
                None if item["locked"] is None else _parse(item["locked"]),
                None if item["margin_balance"] is None else _parse(item["margin_balance"]),
            )
            for item in row["balances"]
        ),
        tuple(
            PositionSnapshot(
                instrument(item["instrument"]),
                _parse(item["signed_quantity"]),
                _parse(item["entry_price"]),
                _parse(item["unrealized_pnl"]),
            )
            for item in row["positions"]
        ),
    )
    if _encoded(_account_document(account)) != _encoded(row):
        raise ValidationError("account snapshot document is not canonical")
    return account


def _opening_document(opening):
    value = replace(opening)
    return {
        "account": _account_document(value.account),
        "instrument": value.instrument.key,
        "base_asset": value.base_asset,
        "quote_asset": value.quote_asset,
        "settlement_asset": value.settlement_asset,
        "initial_from_id": str(value.initial_from_id),
    }


def _book_document(book):
    return {
        "account_id": book.account_id,
        "instrument": book.instrument.key,
        "revision": book.revision,
        "wallets": [[asset, _number(amount)] for asset, amount in book.wallets],
        "quantity": _number(book.quantity),
        "book_cost": None if book.book_cost is None else _number(book.book_cost),
        "realized_gross_pnl": None
        if book.realized_gross_pnl is None
        else _number(book.realized_gross_pnl),
        "base_fee_cost_in_quote": None
        if book.base_fee_cost_in_quote is None
        else _number(book.base_fee_cost_in_quote),
        "fees": [[asset, _number(amount)] for asset, amount in book.fees],
        "realized_pnl": [[asset, _number(amount)] for asset, amount in book.realized_pnl],
        "applied_fills": [fill_document(fill, format_version=2) for fill in book.applied_fills],
    }


@dataclass(frozen=True, slots=True)
class ExecutionAccountReceipt:
    book: ExecutionAccountBook
    record_sha256: str
    state: str
    matched: bool
    recovery_required: bool = True
    manual_resume_ready: bool = False


@dataclass(frozen=True, slots=True)
class ConfirmedExecutionAccountInputs:
    """Detached current financial inputs, never control or history-complete authority."""

    receipt: ExecutionAccountReceipt
    account: AccountSnapshot
    observed_at_ms: int
    originals: tuple


class ExecutionAccountRecovery:
    def __init__(self, coordinator, account_reader, sink, *, opening, on_incident=None):
        if (
            type(coordinator) is not OrderCoordinator
            or type(opening) is not ExecutionAccountOpening
            or coordinator.account_id != opening.account.account_id
            or coordinator.sink is not sink
            or not callable(getattr(account_reader, "account", None))
        ):
            raise ValidationError("account recovery requires its original typed owner/sink/opening")
        if on_incident is not None and not callable(on_incident):
            raise ValidationError("account incident callback must be callable")
        self.coordinator, self.account_reader, self.sink = coordinator, account_reader, sink
        self.opening, self.account_id = replace(opening), opening.account.account_id
        self._lock, self._on_incident = RLock(), on_incident
        self._records, self._book, self._matched = [], None, None
        self._phase = self._context = self._pending = self._pages = None
        self._reack = False
        self._confirmed_adoption = None
        self._state, self._error, self._notification_failed = "unavailable", None, False
        self._storage_error_type = None
        self._orders = ExecutionObservationRecovery(self.account_id, coordinator, sink)
        self._base = self._source_inputs()
        self._opening = _opening_document(self.opening)
        self._scan = {
            "account_id": self.account_id,
            "instrument": opening.instrument.key,
            "initial_from_id": str(opening.initial_from_id),
            "format_version": 2,
        }
        with self._lock:
            self._replay()
        coordinator._bind_account_recovery(self, self._validated_adoption)
        self._confirmed_inputs_binding = (
            coordinator,
            sink,
            coordinator.broker,
            coordinator.gate,
            account_reader,
            self.opening,
            self._orders,
            self.account_id,
        )

    def _source_inputs(self):
        ExecutionDecisionBindings(
            self.account_id, self.sink
        )  # Existing immutable cycle conflict checks.
        rows, intents, bindings, bound = self.sink.records(), {}, {}, set()
        for row in rows:
            if row.get("kind") == "OrderIntent" and row.get("account_id") == self.account_id:
                if row["instrument"] != self.opening.instrument.key:
                    raise ValidationError("first account recovery supports one instrument")
                identity = row["client_order_id"]
                if identity in intents and _encoded(intents[identity]) != _encoded(row):
                    raise ValidationError("account original order identity conflicts")
                intents[identity] = deepcopy(row)
            elif row.get("kind") == "ExecutionDecisionBound":
                decision, _ = _read_document(row)
                if decision.cycle.account_id == self.account_id:
                    bindings[_hash(row)] = deepcopy(row)
                    bound.update(intent.request.client_order_id for intent in decision.intents)
        if len(intents) > 32 or len(bindings) > 64:
            raise ValidationError("account source capacity reached")
        return {
            "orders": [intents[key] for key in sorted(intents)],
            "bindings": [bindings[key] for key in sorted(bindings)],
            "bound": sorted(set(intents) & bound),
        }

    def _index(self):
        result = {}
        for row in self.sink.records():
            kind = row.get("kind")
            relevant = (
                kind in {"OrderIntent", "OrderObserved", "ExecutionRecoveryObserved"}
                and row.get("account_id") == self.account_id
            )
            if kind == "ExecutionDecisionBound":
                relevant = row.get("cycle", {}).get("account_id") == self.account_id
            elif kind in {"ExecutionFillFact", "ExecutionFillPage"}:
                relevant = (
                    type(row.get("scan")) is dict
                    and row["scan"].get("account_id") == self.account_id
                )
            if relevant:
                result[_hash(row)] = deepcopy(row)
        if len(result) > MAX_SOURCE_RECORDS:
            raise ValidationError("account source history capacity reached")
        return result

    def _account_rows(self):
        selected = {}
        for row in self.sink.records():
            if row.get("kind") != KIND or row.get("account_id") != self.account_id:
                continue
            sequence = row.get("sequence")
            if type(sequence) is not int or not 1 <= sequence <= MAX_ACCOUNT_RECORDS:
                raise JournalError("invalid account record sequence")
            if sequence in selected and _encoded(selected[sequence]) != _encoded(row):
                raise JournalError("conflicting account record revision")
            selected[sequence] = deepcopy(row)
        if sorted(selected) != list(range(1, len(selected) + 1)):
            raise JournalError("account record prefix is incomplete")
        return [selected[key] for key in sorted(selected)]

    def _make_pages(self, views):
        if not views or any(
            not view.source_bound or view.request.instrument != self.opening.instrument
            for view in views
        ):
            raise TradingBlocked(
                "account original orders require immutable strategy/source bindings"
            )
        return ExecutionFillRecovery(
            self.account_id,
            self.opening.instrument,
            self.coordinator.broker,
            self.sink,
            self.coordinator.gate,
            initial_from_id=self.opening.initial_from_id,
            owned=tuple((view.request, view.observation) for view in views),
            format_version=2,
        )

    def _match(self, book, account):
        if any(fill.timestamp_ms > account.observed_at_ms for fill in book.applied_fills):
            return False  # Numerical coincidence is not a compatible account observation.
        wallets = {row.asset: row.wallet for row in account.balances}
        if not account.can_trade or wallets != dict(book.wallets):
            return False
        if self.opening.instrument.market is Market.SPOT:
            return (
                not any(row.signed_quantity for row in account.positions)
                and wallets.get(self.opening.base_asset) == book.quantity
            )
        quantities = {row.instrument.key: row.signed_quantity for row in account.positions}
        return quantities.get(book.instrument.key, Decimal(0)) == book.quantity and not any(
            value for key, value in quantities.items() if key != book.instrument.key
        )

    def _validate_record(self, event, previous, index, native_facts):
        if (
            set(event)
            != {
                "kind",
                "schema_version",
                "account_id",
                "instrument",
                "opening",
                "sequence",
                "previous_sha256",
                "book",
                "origins",
                "source_index",
                "query_records",
                "page_sha256",
                "account_snapshot",
                "observed_at_ms",
                "matched",
            }
            or type(event["schema_version"]) is not int
            or event["schema_version"] != 1
        ):
            raise JournalError("unsupported account event schema")
        if (
            event["kind"] != KIND
            or event["account_id"] != self.account_id
            or event["instrument"] != self.opening.instrument.key
            or _encoded(event["opening"]) != _encoded(self._opening)
            or _encoded(event["origins"]) != _encoded(self._base)
        ):
            raise JournalError("account event opening/source identity differs")
        references = event["source_index"]
        if (
            type(references) is not list
            or references != sorted(set(references))
            or any(type(key) is not str or len(key) != 64 or key not in index for key in references)
        ):
            raise JournalError("account event source references are missing")
        asof = event["observed_at_ms"]
        if type(asof) is not int or not self.opening.account.observed_at_ms <= asof <= MAX_TIME:
            raise JournalError("account event timestamp is invalid")
        if previous is not None and asof < previous["observed_at_ms"]:
            raise JournalError("account event timestamp regressed")
        pages = [
            (key, row)
            for key, row in index.items()
            if row.get("kind") == "ExecutionFillPage"
            and _encoded(row.get("scan")) == _encoded(self._scan)
        ]
        selected = [(key, row) for key, row in pages if row["sha256"] == event["page_sha256"]]
        if len(selected) != 1 or selected[0][0] not in references:
            raise JournalError("account event fill page is missing")
        through = selected[0][1]["revision"]
        if type(through) is not int or not 1 <= through <= MAX_SOURCE_RECORDS:
            raise JournalError("account fill page revision is invalid")
        prefix = [(key, row) for key, row in pages if row["revision"] <= through]
        prefix.sort(key=lambda item: item[1]["revision"])
        if [row["revision"] for _, row in prefix] != list(range(1, through + 1)):
            raise JournalError("account original fill page prefix is incomplete")
        predecessor = None
        for key, row in prefix:
            if (
                key not in references
                or type(row["revision"]) is not int
                or row["previous_sha256"] != predecessor
                or row["sha256"]
                != _hash({name: value for name, value in row.items() if name != "sha256"})
            ):
                raise JournalError("account original fill page prefix was omitted or altered")
            predecessor = row["sha256"]
        ids = {identity for _, row in prefix for identity in row["trade_ids"]}
        documents = event["book"]["applied_fills"]
        if type(documents) is not list or {row["trade_id"] for row in documents} != ids:
            raise JournalError("account event omitted or invented committed fill facts")
        fills = []
        for document in documents:
            fill = native_facts.get(document["trade_id"])
            if fill is None or _encoded(fill_document(fill, format_version=2)) != _encoded(
                document
            ):
                raise JournalError("account fill financial contents changed")
            if not self.opening.account.observed_at_ms <= fill.timestamp_ms <= asof:
                raise ValidationError("account fill is outside the opening/observation boundary")
            matches = [
                row
                for key, row in index.items()
                if key in references
                and row.get("kind") == "ExecutionFillFact"
                and _encoded(row.get("scan")) == _encoded(self._scan)
                and _encoded(row["fill"]) == _encoded(document)
            ]
            if len(matches) != 1:
                raise JournalError("account fill has no original recorded source")
            fills.append(fill)
        queried = event["query_records"]
        if type(queried) is not list or queried != sorted(set(queried)):
            raise JournalError("account query references are invalid")
        requests = {}
        for binding in self._base["bindings"]:
            decision, _ = _read_document(binding)
            requests.update(
                (intent.request.client_order_id, intent.request) for intent in decision.intents
            )
        observations, covered = {}, set()
        for key in queried:
            if key not in references or index[key].get("kind") != "ExecutionRecoveryObserved":
                raise JournalError("account original observation is missing")
            row = index[key]
            identity = row["request"]["client_order_id"]
            request = requests.get(identity)
            if (
                request is None
                or identity in covered
                or _encoded(row["request"]) != _encoded(request_document(request))
            ):
                raise JournalError("account frozen observation differs from its original request")
            body = row["observation"]
            observation = OrderObservation(
                request.instrument,
                identity,
                body["provider_order_id"],
                OrderStatus(body["status"]),
                _parse(body["executed_quantity"]),
                _parse(body["cumulative_quote"]),
            )
            _validate_observation(request, observation)
            if observation.provider_order_id in observations or row["source_binding"] is None:
                raise JournalError("account observation has conflicting or missing ownership")
            covered.add(identity)
            observations[observation.provider_order_id] = observation
        if covered != {row["client_order_id"] for row in self._base["orders"]}:
            raise JournalError("account original observation coverage differs")
        for provider, quantity in self._quantities(fills).items():
            observation = observations.get(provider)
            if observation is None or quantity > Fraction(observation.executed_quantity):
                raise JournalError("account fill exceeds its frozen original observation")
        base_book = initial_execution_book(self.opening) if previous is None else self._book
        book = apply_execution_fills(base_book, self.opening, tuple(fills))
        if _encoded(_book_document(book)) != _encoded(event["book"]):
            raise JournalError("account book differs from full original financial facts")
        account = _read_account(event["account_snapshot"])
        minimum = (
            self.opening.account.observed_at_ms
            if previous is None
            else previous["account_snapshot"]["observed_at_ms"]
        )
        account = check_execution_account_snapshot(
            self.opening, account, observed_at_ms=asof, minimum_ms=minimum
        )
        if type(event["matched"]) is not bool or event["matched"] is not self._match(book, account):
            raise JournalError("account snapshot comparison was altered")
        return book

    @staticmethod
    def _quantities(fills):
        result = {}
        for fill in fills:
            result[fill.provider_order_id] = result.get(
                fill.provider_order_id, Fraction()
            ) + Fraction(fill.quantity)
        return result

    def _replay(self):
        records = self._account_rows()
        if not records:
            return
        views = self._orders.snapshot()
        pages = self._make_pages(views)
        facts = {fill.trade_id: fill for fill in pages.history.facts}
        index, previous = self._index(), None
        for event in records:
            if event["previous_sha256"] != (None if previous is None else _hash(previous)):
                raise JournalError("account financial chain changed")
            self._book = self._validate_record(event, previous, index, facts)
            previous = event
        self._records, self._matched, self._state = records, records[-1]["matched"], "historical"
        # Combined primary/fallback history cannot prove this process observed PRIMARY.
        self._pending, self._phase = (deepcopy(records[-1]), self._book), "account"
        self._reack = True

    def _check(self):
        if _encoded(_opening_document(self.opening)) != _encoded(self._opening):
            raise TradingBlocked("original account opening/asset mapping changed")
        if self._book is not None and _encoded(_book_document(self._book)) != _encoded(
            self._records[-1]["book"]
        ):
            raise JournalError("published account book changed outside its owner")
        if _encoded(self._source_inputs()) != _encoded(self._base):
            raise TradingBlocked("original account orders/source bindings changed")
        rows = self._account_rows()
        allowed = self._records + (
            [] if self._pending is None or self._reack else [self._pending[0]]
        )
        if (
            len(rows) < len(self._records)
            or len(rows) > len(allowed)
            or any(_encoded(row) != _encoded(allowed[index]) for index, row in enumerate(rows))
        ):
            raise JournalError("account financial history changed externally")
        index = self._index()
        if any(key not in index for row in self._records for key in row["source_index"]):
            raise JournalError("previously applied account source history changed")

    def _finish(self, incidents):
        self._phase = None
        history, context = self._pages.history, self._context
        if history.orphan_trade_ids:
            raise TradingBlocked("account source facts await an acknowledged original page")
        book = apply_execution_fills(
            self._book or initial_execution_book(self.opening), self.opening, history.facts
        )
        account = context["account"]
        index = self._index()
        event = {
            "kind": KIND,
            "schema_version": 1,
            "account_id": self.account_id,
            "instrument": self.opening.instrument.key,
            "opening": deepcopy(self._opening),
            "sequence": len(self._records) + 1,
            "previous_sha256": _hash(self._records[-1]) if self._records else None,
            "book": _book_document(book),
            "origins": deepcopy(self._base),
            "source_index": sorted(index),
            "query_records": sorted(view.record_sha256 for view in context["views"]),
            "page_sha256": history.page_sha256,
            "account_snapshot": _account_document(account),
            "observed_at_ms": context["observed_at_ms"],
            "matched": self._match(book, account),
        }
        if len(self._records) >= MAX_ACCOUNT_RECORDS:
            raise ValidationError("account financial record capacity reached")
        _encoded(event)
        self._validate_record(
            event,
            self._records[-1] if self._records else None,
            index,
            {fill.trade_id: fill for fill in history.facts},
        )
        self._pending, self._phase = (deepcopy(event), book), "account"
        return self._commit(incidents)

    def _commit(self, incidents):
        event, book = self._pending
        current_sources = self._index()
        if self._reack:
            source_changed = any(key not in current_sources for key in event["source_index"])
            if not self._records or _encoded(event) != _encoded(self._records[-1]):
                raise JournalError("historical account reacknowledgement candidate changed")
        else:
            source_changed = sorted(current_sources) != event["source_index"]
        if source_changed or _encoded(_book_document(book)) != _encoded(event["book"]):
            raise TradingBlocked("pending account candidate/source changed")
        try:
            durability = self.sink.record(deepcopy(event))
        except (JournalError, OSError, TimeoutError) as error:
            self._storage_error_type = type(error).__name__
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked(
                "account candidate primary acknowledgement is unavailable"
            ) from error
        if durability is not Durability.PRIMARY:
            self.coordinator.gate.block(BlockReason.STORAGE)
            raise TradingBlocked("account candidate requires a fresh primary acknowledgement")
        fresh_views = None if self._reack or self._context is None else self._context["views"]
        if not self._reack:
            self._records.append(deepcopy(event))
        self._book, self._matched = book, event["matched"]
        self._state, self._error = ("recorded" if self._matched else "held"), None
        self._pending = self._phase = self._context = None
        self._reack = False
        self._confirmed_adoption = (
            None if fresh_views is None else (_hash(event), tuple(fresh_views))
        )
        if not self._matched:
            incidents.append(self._incident("EXECUTION_ACCOUNT_SNAPSHOT_MISMATCH"))
        return ExecutionAccountReceipt(book, _hash(event), "primary_acknowledged", self._matched)

    def _continue_orders(self, incidents):
        views = self._orders.recover()
        self._pages = self._make_pages(views)
        minimum = (
            self._records[-1]["account_snapshot"]["observed_at_ms"]
            if self._records
            else self.opening.account.observed_at_ms
        )
        account = check_execution_account_snapshot(
            self.opening,
            self.account_reader.account(),
            observed_at_ms=self._context["observed_at_ms"],
            minimum_ms=minimum,
        )
        self._context.update(views=views, account=account)
        self._phase = "page"
        self._pages.recover_page()
        return self._finish(incidents)

    def _incident(self, code):
        return {
            "code": code,
            "severity": "critical",
            "account_id": self.account_id,
            "instrument": self.opening.instrument.key,
        }

    def _public(self, action):
        incidents = []
        try:
            with self._lock:
                try:
                    self.coordinator.gate.block(BlockReason.EXECUTION)
                    self._check()
                    return action(incidents)
                except Exception as error:
                    self._confirmed_adoption = None
                    if isinstance(error, (JournalError, OSError, TimeoutError)):
                        self.coordinator.gate.block(BlockReason.STORAGE)
                    self._error, self._state = (
                        type(error).__name__,
                        "pending" if self._phase else "held",
                    )
                    incidents.append(self._incident("EXECUTION_ACCOUNT_RECOVERY_HELD"))
                    raise
        finally:
            for event in incidents:
                if self._on_incident is not None:
                    try:
                        self._on_incident(event)
                    except Exception:
                        with self._lock:
                            self._notification_failed = True

    def recover_once(self, *, observed_at_ms):
        def action(incidents):
            self._confirmed_adoption = None
            if self._phase is not None:
                raise TradingBlocked("retry original pending account recovery first")
            if (
                type(observed_at_ms) is not int
                or not self.opening.account.observed_at_ms <= observed_at_ms <= MAX_TIME
                or (self._records and observed_at_ms < self._records[-1]["observed_at_ms"])
            ):
                raise ValidationError("account recovery observation is stale or invalid")
            if not self._base["orders"] or len(self._base["bound"]) != len(self._base["orders"]):
                raise TradingBlocked("account original orders lack strategy/source bindings")
            self._context, self._phase = {"observed_at_ms": observed_at_ms}, "orders"
            return self._continue_orders(incidents)

        return self._public(action)

    def retry_pending(self):
        def action(incidents):
            if self._phase == "account":
                return self._commit(incidents)
            if self._phase == "page":
                self._pages.recover_page()
                return self._finish(incidents)
            if self._phase == "orders":
                return self._continue_orders(incidents)
            raise TradingBlocked("account recovery has no pending candidate")

        return self._public(action)

    def _validated_adoption(self):
        """Validate this process's exact current PRIMARY book and fresh original queries."""
        self._check()
        if (
            self._confirmed_adoption is None
            or self._phase is not None
            or self._reack
            or self._state != "recorded"
            or self._error is not None
            or self._matched is not True
            or self._book is None
            or self._pages is None
            or self._pages.history.orphan_trade_ids
        ):
            raise TradingBlocked("fresh complete financial recovery is required before adoption")
        digest, views = self._confirmed_adoption
        event = self._records[-1]
        if (
            digest != _hash(event)
            or views != self._orders.snapshot()
            or sorted(self._index()) != event["source_index"]
            or sorted(view.record_sha256 for view in views) != event["query_records"]
            or {view.request.client_order_id for view in views}
            != {row["client_order_id"] for row in self._base["orders"]}
        ):
            raise TradingBlocked("current account adoption evidence changed")
        quantity, quote = {}, {}
        for fill in self._book.applied_fills:
            if fill.quote_quantity is None:
                raise TradingBlocked("terminal adoption requires reported gross quote")
            provider = fill.provider_order_id
            quantity[provider] = quantity.get(provider, Fraction()) + Fraction(fill.quantity)
            quote[provider] = quote.get(provider, Fraction()) + Fraction(fill.quote_quantity)
        providers, candidates = set(), []
        for view in views:
            observation = view.observation
            provider = observation.provider_order_id
            if (
                view.state != "primary_acknowledged"
                or not view.source_bound
                or observation.status not in _TERMINAL
                or provider in providers
                or quantity.get(provider, Fraction()) != Fraction(observation.executed_quantity)
                or quote.get(provider, Fraction()) != Fraction(observation.cumulative_quote)
            ):
                raise TradingBlocked("terminal original lacks exact financially applied coverage")
            _validate_observation(view.request, observation)
            providers.add(provider)
            candidates.append((view.request, observation))
        if set(quantity) - providers:
            raise TradingBlocked("account has fills outside the original terminal set")
        return tuple(candidates)

    def confirmed_inputs(self):
        """Return only this owner's current fresh PRIMARY inputs under its lock.

        Historical re-ACK, numerical match alone and caller-created receipts do
        not supply these inputs. Partial originals may be inspected but cannot
        pass the separate all-terminal adoption validation.
        """

        def action(_incidents):
            coordinator, sink, broker, gate, reader, opening, orders, account = (
                self._confirmed_inputs_binding
            )
            if (
                self.coordinator is not coordinator
                or self.sink is not sink
                or coordinator.sink is not sink
                or coordinator.broker is not broker
                or coordinator.gate is not gate
                or self.account_reader is not reader
                or self.opening is not opening
                or self._orders is not orders
                or self.account_id != account
                or coordinator.account_id != account
                or self._confirmed_adoption is None
                or self._phase is not None
                or self._state != "recorded"
                or self._error is not None
                or self._matched is not True
                or self._book is None
                or self._pages is None
                or self._pages.history.orphan_trade_ids
            ):
                raise TradingBlocked("current original confirmed account inputs are unavailable")
            digest, views = self._confirmed_adoption
            event = self._records[-1]
            if (
                digest != _hash(event)
                or views != self._orders.snapshot()
                or sorted(self._index()) != event["source_index"]
                or sorted(view.record_sha256 for view in views) != event["query_records"]
                or {view.request.client_order_id for view in views}
                != {row["client_order_id"] for row in self._base["orders"]}
                or any(
                    view.state != "primary_acknowledged" or not view.source_bound for view in views
                )
            ):
                raise TradingBlocked("current confirmed account source inputs changed")
            return ConfirmedExecutionAccountInputs(
                ExecutionAccountReceipt(deepcopy(self._book), digest, "primary_acknowledged", True),
                _read_account(deepcopy(event["account_snapshot"])),
                event["observed_at_ms"],
                tuple((deepcopy(view.request), deepcopy(view.observation)) for view in views),
            )

        return self._public(action)

    def adopt_confirmed_originals(self):
        """Maintain known-terminal reservations only; no history/readiness/resume claim.

        Every original must be terminal with exact financially committed quantity
        and gross quote. Partial/UNKNOWN sets stay untouched. The account caller
        still serializes all coordinator access; restart restores UNKNOWN again.
        """
        return self._public(lambda _incidents: self.coordinator._adopt_account_recovery(self))

    def snapshot(self):
        with self._lock:
            return self._book

    def status(self):
        with self._lock:
            return {
                "account_id": self.account_id,
                "instrument": self.opening.instrument.key,
                "state": self._state,
                "pending": self._phase is not None,
                "pending_phase": self._phase,
                "matched": self._matched,
                "recovery_required": True,
                "manual_resume_ready": False,
                "error_type": self._error,
                "notification_failed": self._notification_failed,
                "storage_error_type": self._storage_error_type,
            }
