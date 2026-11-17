"""Read-only projection of the margin owner's bounded, recorded ledger."""

import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

from PySide6.QtCore import QSignalBlocker, Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.application.margin_funding import funding_fact_from_document
from omega.domain.errors import ValidationError

BASE = "/api/margin-trading"


def _number(value):
    if type(value) not in {str, int} or len(str(value)) > 128:
        raise ValueError("Invalid recorded margin amount")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("Invalid recorded margin amount") from None
    if not result.is_finite():
        raise ValueError("Nonfinite recorded margin amount")
    return result


def _time(value):
    if type(value) is not int or not 0 <= value <= 253_402_300_799_000:
        raise ValueError("Invalid recorded margin time")
    return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y/%m/%d %H:%M:%S")


def ledger_accounts(snapshot):
    """Validate display limits and identities; never reconstruct a missing fact."""
    if type(snapshot) is not dict or snapshot.get("scope") != "virtual_margin_v2_ohlcv_proxy":
        raise ValueError("Invalid margin history scope")
    rows = snapshot.get("accounts")
    if type(rows) is not list or len(rows) > 10:
        raise ValueError("Invalid margin account list")
    result = {}
    for row in rows:
        if type(row) is not dict or type(row.get("account_id")) is not str:
            raise ValueError("Invalid margin account identity")
        identity = row["account_id"]
        if not 1 <= len(identity) <= 128 or identity in result:
            raise ValueError("Duplicate or invalid margin account")
        envelope = row.get("account")
        if envelope is None:
            result[identity] = {**row, "ledger_rows": [], "ledger_summary": {}, "asset": "?"}
            continue
        if (
            type(envelope) is not dict
            or type(envelope.get("schema_version")) is not int
            or envelope["schema_version"] not in {1, 2}
        ):
            raise ValueError("Invalid margin account display document")
        account, summary = envelope.get("account"), envelope.get("summary")
        if (
            type(account) is not dict
            or account.get("account_id") != identity
            or type(summary) is not dict
        ):
            raise ValueError("Margin account document identity mismatch")
        ledger, positions = account.get("ledger"), account.get("positions")
        total, limit = summary.get("ledger_total"), summary.get("ledger_limit")
        if (
            type(ledger) is not list
            or len(ledger) > 128
            or type(positions) is not list
            or len(positions) > 100
            or type(total) is not int
            or total < len(ledger)
            or type(limit) is not int
            or not 1 <= limit <= 128
            or len(ledger) > limit
            or type(summary.get("ledger_truncated")) is not bool
            or summary["ledger_truncated"] != (total > len(ledger))
        ):
            raise ValueError("Invalid bounded margin ledger")
        records, previous = [], 0
        owners = {}
        for position in positions:
            if (
                type(position) is not dict
                or type(position.get("owner_id")) is not str
                or type(position.get("instrument")) is not str
            ):
                raise ValueError("Invalid margin position identity")
            owners[position["instrument"]] = position["owner_id"]
        for entry in ledger:
            if (
                type(entry) is not dict
                or type(entry.get("sequence")) is not int
                or not previous < entry["sequence"] <= total
            ):
                raise ValueError("Invalid margin ledger sequence")
            previous = entry["sequence"]
            fact = entry.get("fact")
            if type(fact) is not dict or fact.get("kind") not in {"fill", "transfer", "funding"}:
                raise ValueError("Invalid recorded margin fact")
            if owners.get(fact.get("instrument")) != fact.get("owner_id"):
                raise ValueError("Recorded margin owner mismatch")
            _time(fact.get("event_time_ms"))
            _time(fact.get("available_at_ms"))
            if fact["available_at_ms"] < fact["event_time_ms"]:
                raise ValueError("Recorded margin time order mismatch")
            for name in ("realized_pnl", "fee", "cross_delta", "isolated_delta"):
                if name in entry:
                    _number(entry[name])
            kind = fact["kind"]
            if kind == "funding":
                if envelope["schema_version"] != 2:
                    raise ValueError("Funding requires an explicit versioned account document")
                try:
                    funding_fact_from_document(
                        {key: value for key, value in fact.items() if key != "kind"}
                    )
                except (ValidationError, ValueError, TypeError, KeyError):
                    raise ValueError("Invalid recorded scenario funding fact") from None
            amount = _number(fact.get("delta_qty" if kind == "fill" else "amount"))
            event_id = fact.get(
                {"fill": "fill_id", "transfer": "transfer_id", "funding": "funding_id"}[kind]
            )
            if type(event_id) is not str or not 1 <= len(event_id) <= 128:
                raise ValueError("Missing recorded margin event ID")
            if kind == "fill":
                _number(fact.get("price"))
                _number(fact.get("fee"))
            records.append(
                {
                    **entry,
                    "fact": fact,
                    "event_id": event_id,
                    "direction": ("BUY" if amount > 0 else "SELL")
                    if kind == "fill"
                    else "TRANSFER"
                    if kind == "transfer"
                    else "FUNDING_CREDIT"
                    if amount > 0
                    else "FUNDING_DEBIT"
                    if amount < 0
                    else "FUNDING_ZERO",
                }
            )
        result[identity] = {
            **row,
            "ledger_rows": records,
            "ledger_summary": summary,
            "asset": account.get("settlement_asset", "?"),
        }
    return result


class MarginHistoryWidget(QWidget):
    accountsObserved = Signal(list)
    fullLedgerRequested = Signal(str)

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch, self.pending, self.serial = None, None, 0
        self.accounts, self.capable, self._snapshot_epoch = {}, False, None
        outer = QVBoxLayout(self)
        self.notice, self.status, self.context = QLabel(), QLabel(), QLabel()
        for item in (self.notice, self.status, self.context):
            item.setWordWrap(True)
            item.setTextFormat(Qt.TextFormat.PlainText)
            outer.addWidget(item)
        selectors = QHBoxLayout()
        self.account, self.strategy = QComboBox(), QComboBox()
        self.refresh_button = QPushButton()
        self.full_ledger_button = QPushButton("전체 원장 · 페이지/ZIP / Full ledger")
        for item in (self.account, self.strategy, self.refresh_button):
            selectors.addWidget(item)
        outer.addLayout(selectors)
        outer.addWidget(self.full_ledger_button)
        self.search = QLineEdit()
        outer.addWidget(self.search)
        self.table = QTableWidget(0, 12)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        outer.addWidget(self.table, 1)
        self.account.currentIndexChanged.connect(self._account_changed)
        self.strategy.currentIndexChanged.connect(self.render)
        self.search.textChanged.connect(self.render)
        self.refresh_button.clicked.connect(self.refresh)
        self.full_ledger_button.clicked.connect(
            lambda: self.fullLedgerRequested.emit(self.account.currentData() or "")
        )
        client.resultReceived.connect(self._received)
        client.failed.connect(self._failed)
        client.connectionChanged.connect(self.set_attached)
        self.set_presentation("light", "ko")

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def update_state(self, state):
        core = state.get("core", {})
        epoch = core.get("epoch")
        changed = epoch != self.epoch
        self.epoch = epoch
        self.capable = bool(core.get("capabilities", {}).get("margin_trading"))
        if changed:
            self.pending = None
            self.status.setText(
                self.text(
                    "코어 변경 · 이전 기록은 최신 확인 전입니다",
                    "Core changed; prior rows are not confirmed current",
                )
            )
        self.set_attached(self.client.attached)
        if changed and self.capable:
            self.refresh()

    def refresh(self):
        if self.pending is not None or not self.client.attached or not self.capable:
            return
        self.serial += 1
        tag = f"margin-history:{self.serial}"
        self.pending = tag, self.epoch
        self.status.setText(
            self.text("원장 조회 중 · 이전 기록 표시", "Reading ledger; retained prior records")
        )
        self.client.request("GET", BASE, tag=tag)

    def _received(self, tag, result):
        if self.pending != (tag, self.epoch):
            return
        self.pending = None
        if result.get("core_epoch") != self.epoch:
            self.status.setText(
                self.text("이전 코어의 응답을 제외했습니다", "Stale core response ignored")
            )
            return
        try:
            accounts = ledger_accounts(result)
        except (ValueError, TypeError, KeyError, OverflowError) as error:
            self.status.setText(str(error))
            return
        self.accounts, self._snapshot_epoch = accounts, self.epoch
        self.accountsObserved.emit(list(accounts))
        selected = self.account.currentData()
        blocker = QSignalBlocker(self.account)
        self.account.clear()
        for identity in accounts:
            self.account.addItem(identity, identity)
        index = self.account.findData(selected)
        self.account.setCurrentIndex(max(0, index))
        del blocker
        self.status.setText(
            self.text(
                "조회 확인 · 금융 owner가 제공한 최근 기록",
                "Observed; recent records supplied by the financial owner",
            )
        )
        self._account_changed()

    def _account_changed(self, *_args):
        row = self.accounts.get(self.account.currentData(), {})
        selected = self.strategy.currentData()
        blocker = QSignalBlocker(self.strategy)
        self.strategy.clear()
        self.strategy.addItem(
            self.text("전체 전략 · 담보 이동 포함", "All strategies including transfers"), ""
        )
        account = (row.get("account") or {}).get("account", {})
        owners = {p["owner_id"] for p in account.get("positions", [])}
        for owner in sorted(owners):
            self.strategy.addItem(owner, owner)
        self.strategy.setCurrentIndex(max(0, self.strategy.findData(selected)))
        del blocker
        self.render()

    def render(self, *_args):
        row = self.accounts.get(self.account.currentData(), {})
        owner, query = self.strategy.currentData(), self.search.text().strip().casefold()
        records = [
            r
            for r in row.get("ledger_rows", [])
            if (not owner or r["fact"]["owner_id"] == owner)
            and (not query or query in json.dumps(r, ensure_ascii=False).casefold())
        ]
        self.table.setRowCount(len(records))
        asset = row.get("asset", "?")
        has_funding = any(
            entry["fact"]["kind"] == "funding" for entry in row.get("ledger_rows", [])
        )
        self.table.setColumnCount(14 if has_funding else 12)
        self._headers(has_funding)
        for index, entry in enumerate(reversed(records)):
            fact = entry["fact"]
            funding = fact["kind"] == "funding"
            values = (
                entry["sequence"],
                _time(fact["event_time_ms"]),
                _time(fact["available_at_ms"]),
                fact["owner_id"],
                fact["instrument"],
                entry["direction"],
                f"{fact['amount']} {fact['settlement_asset']}"
                if funding
                else fact.get("delta_qty", fact.get("amount", "—")),
                fact.get("reference_price", "—") if funding else fact.get("price", "—"),
                f"{entry.get('fee', '—')} {fact.get('fee_asset', asset)}",
                f"{entry.get('realized_pnl', '—')} {asset}",
                entry.get("cross_delta", "—"),
                entry.get("isolated_delta", "—"),
            )
            if has_funding:
                values += (
                    ("시나리오 rate / Scenario rate" if funding else "—"),
                    (entry["event_id"] if funding else "—"),
                )
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value)[:500])
                item.setToolTip(
                    f"{entry['event_id']}\n{json.dumps(entry, ensure_ascii=False)}"[:4000]
                )
                self.table.setItem(index, column, item)
        summary = row.get("ledger_summary", {})
        note = " · ".join(
            str(row[key]) for key in ("state", "error", "model_errors") if row.get(key)
        )
        self.context.setText(
            self.text(
                f"원장 표시 {len(records)} / 수신 {len(row.get('ledger_rows', []))} "
                f"/ 전체 {summary.get('ledger_total', '—')} · 최근 최대128건 · {asset}\n{note}",
                f"Showing {len(records)} / received {len(row.get('ledger_rows', []))} "
                f"/ total {summary.get('ledger_total', '—')} · latest128 max · {asset}\n{note}",
            )
        )
        if summary.get("ledger_truncated"):
            self.context.setText(
                self.context.text() + self.text(" · 이전 원장 생략", " · earlier ledger omitted")
            )
        if has_funding:
            self.context.setText(
                self.context.text()
                + self.text(
                    " · Funding은 시나리오 계산이며 거래소 실지급 아님",
                    " · Funding is scenario cashflow, not a venue payment",
                )
            )

    def _failed(self, tag, message):
        if self.pending == (tag, self.epoch):
            self.pending = None
            self.status.setText(
                self.text("조회 실패 · 이전 기록: ", "Read failed; retained rows: ") + message[:300]
            )

    def set_attached(self, attached):
        self.refresh_button.setEnabled(bool(attached and self.capable))
        if not attached:
            self.pending = None
            self.status.setText(
                self.text("연결 끊김 · 이전 조회 기록", "Disconnected; retained previous records")
            )
        elif not self.capable:
            self.status.setText(
                self.text(
                    "이 코어는 증거금 내역을 제공하지 않습니다",
                    "This core does not provide margin history",
                )
            )

    def set_presentation(self, _theme, language):
        self.language = language
        self.notice.setText(
            self.text(
                "가상 증거금 v2 · 실제 원장 기록만 표시 · 거래소 실체결 아님",
                "Virtual margin v2; recorded ledger only; no live exchange fills",
            )
        )
        self.refresh_button.setText(self.text("원장 조회", "Read ledger"))
        self.search.setPlaceholderText(
            self.text("종목 · 이벤트 ID · 시각 검색", "Search instrument, event ID or time")
        )
        self._headers(self.table.columnCount() == 14)
        self.render()

    def _headers(self, funding=False):
        labels = [
            "Seq",
            "UTC",
            "Available UTC",
            "TS",
            "Instrument",
            "Side / fact",
            "Qty / transfer / cashflow" if funding else "Qty / transfer",
            "Price / reference" if funding else "Price",
            "Fee",
            "Recorded PnL",
            "Cross delta",
            "Isolated delta",
        ]
        if funding:
            labels.extend(["Funding source", "Funding ID"])
        self.table.setHorizontalHeaderLabels(labels)
