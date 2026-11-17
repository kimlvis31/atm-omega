"""Account → strategy → observed fills. This client never reconstructs missing PnL."""

# Imports
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

from PySide6.QtCore import QSignalBlocker, Qt
from PySide6.QtGui import QColor
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.gui.presentation import THEMES

MAX_FILLS = 1000
_TEXT = {
    "title": ("계정 거래 내역", "ACCOUNT HISTORY"),
    "accounts": ("계정 목록", "ACCOUNTS"),
    "strategies": ("선택 계정의 전략", "ACCOUNT STRATEGIES"),
    "refresh": ("내역 조회", "Refresh fills"),
    "search": ("종목 · 체결 시각 · 사유 검색", "Search instrument, UTC time or reason"),
    "all": ("전체 방향", "All sides"),
    "empty": (
        "계정과 전략을 선택하면 체결 기록을 조회합니다.",
        "Select an account and strategy to read fills.",
    ),
    "loading": (
        "체결 기록 조회 중 · 표시 중인 이전 기록은 최신 확인 전입니다.",
        "Loading fills · retained rows are not yet confirmed current.",
    ),
    "failed": ("조회 실패 · 이전 기록 표시", "Request failed · retained records"),
    "disconnected": ("연결 끊김 · 이전 조회 기록", "Disconnected · retained records"),
    "limit": (
        "전략별 최근 최대 1,000건 · 이전 전체 이력은 제공되지 않음",
        "Up to 1,000 recent fills per strategy · not all-time history",
    ),
    "paper": ("PAPER 체결 기록 · 거래소 실체결 연결 아님", "PAPER fills · no live exchange fills"),
    "time_note": (
        "시각은 코어의 기록 기준입니다. 분봉 체결은 보수적인 분 마감 시각일 수 있습니다.",
        "Times follow core records; paper fills can use conservative minute-close timestamps.",
    ),
    "fees": ("조회 기록 수수료", "Loaded-record fees"),
    "pnl": ("조회 기록 제공 손익", "Loaded-record PnL"),
    "unavailable": ("미제공", "Not provided"),
    "partial": ("일부 기록만 제공", "Partial records only"),
    "count": (
        "표시 {visible:,} / 조회 {total:,}건",
        "Showing {visible:,} / {total:,} loaded fills",
    ),
    "observed": ("조회 확인", "Observed"),
    "account_info": ("선택 계정", "SELECTED ACCOUNT"),
    "cash": ("잔액", "Cash"),
}

_HEADERS = {
    "accounts": (("계정", "시장", "상태"), ("Account", "Market", "Status")),
    "strategies": (("전략", "종목", "체결 수"), ("Strategy", "Instrument", "Fills")),
    "fills": (
        ("시각 (UTC)", "종목", "방향", "수량", "가격", "수수료", "사유", "제공 손익"),
        ("Time (UTC)", "Instrument", "Side", "Quantity", "Price", "Fee", "Reason", "Recorded PnL"),
    ),
}


def text(key: str, language: str) -> str:
    return _TEXT[key][0 if language == "ko" else 1]


def recorded_decimal(value) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = Decimal(str(value))
    except (ValueError, InvalidOperation):
        return None
    return number if number.is_finite() else None


def recorded_time(value) -> str:
    if type(value) is not int or not 0 <= value <= 253_402_300_799_000:
        return "—"
    return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y/%m/%d %H:%M:%S")


def _table(columns: int) -> QTableWidget:
    table = QTableWidget(0, columns)
    table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    table.setAlternatingRowColors(True)
    table.verticalHeader().hide()
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
    table.horizontalHeader().setStretchLastSection(True)
    return table


class HistoryPage(QWidget):
    """Read-only API view. Account selection keeps unrelated balances and fees separate."""

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.theme, self.language = "light", "ko"
        self._accounts: dict[str, dict] = {}
        self._strategies: dict[str, dict] = {}
        self._selected_account = ""
        self._selected_strategy = ""
        self._fills: list[dict] = []
        self._detail: dict = {}
        self._pending_tag = None
        self._counter = 0
        self._observed = ""
        self._epoch = None
        self._revision = None
        self._state = "empty"
        self._error = ""
        self._attached = bool(client.attached)
        self._pending_revision = None
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        self.history_tabs = QTabWidget()
        outer.addWidget(self.history_tabs)
        paper = QWidget()
        self.history_tabs.addTab(paper, "Paper v1")
        from omega.gui.margin_history import MarginHistoryWidget

        self.margin_history = MarginHistoryWidget(client)
        self.history_tabs.addTab(self.margin_history, "Margin v2")
        from omega.gui.ledger_history import LedgerHistoryWidget

        self.ledger_history = LedgerHistoryWidget(client)
        self.history_tabs.addTab(self.ledger_history, "Margin v2 · Ledger ZIP")
        self.margin_history.accountsObserved.connect(self.ledger_history.set_accounts)
        self.margin_history.fullLedgerRequested.connect(self.open_margin_ledger)
        layout = QVBoxLayout(paper)
        layout.setContentsMargins(0, 0, 0, 0)
        self.title = QLabel()
        self.title.setProperty("role", "title")
        self.refresh_button = QPushButton()
        layout.addWidget(self.title)
        self.source_note = QLabel()
        self.source_note.setProperty("role", "muted")
        self.source_note.setWordWrap(True)
        layout.addWidget(self.source_note)
        split = QSplitter(Qt.Orientation.Horizontal)
        left = QWidget()
        left.setMinimumWidth(220)
        left_layout = QVBoxLayout(left)
        left_layout.setContentsMargins(0, 0, 6, 0)
        self.accounts_title = QLabel()
        self.accounts_table = _table(3)
        self.strategies_title = QLabel()
        self.strategies_table = _table(3)
        self.account_info = QLabel()
        self.account_info.setTextFormat(Qt.TextFormat.PlainText)
        self.account_info.setWordWrap(True)
        for widget in (
            self.accounts_title,
            self.accounts_table,
            self.strategies_title,
            self.strategies_table,
            self.account_info,
        ):
            left_layout.addWidget(widget)
        right = QWidget()
        right_layout = QVBoxLayout(right)
        right_layout.setContentsMargins(6, 0, 0, 0)
        self.selection_label = QLabel()
        self.selection_label.setTextFormat(Qt.TextFormat.PlainText)
        self.selection_label.setWordWrap(True)
        self.status = QLabel()
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        self.status.setWordWrap(True)
        self.status.setProperty("role", "muted")
        right_layout.addWidget(self.selection_label)
        right_layout.addWidget(self.status)
        summary = QHBoxLayout()
        self.fee_summary, self.pnl_summary = QLabel(), QLabel()
        for widget in (self.fee_summary, self.pnl_summary):
            widget.setTextFormat(Qt.TextFormat.PlainText)
            widget.setWordWrap(True)
            summary.addWidget(widget, 1)
        right_layout.addLayout(summary)
        filters = QHBoxLayout()
        self.search = QLineEdit()
        self.side = QComboBox()
        self.side.addItem("", "")
        self.side.addItem("BUY", "BUY")
        self.side.addItem("SELL", "SELL")
        filters.addWidget(self.search, 1)
        filters.addWidget(self.side)
        filters.addWidget(self.refresh_button)
        right_layout.addLayout(filters)
        self.count_label = QLabel()
        self.count_label.setProperty("role", "muted")
        right_layout.addWidget(self.count_label)
        self.fills_table = _table(8)
        self.fills_table.setSortingEnabled(True)
        right_layout.addWidget(self.fills_table, 1)
        self.limit_note = QLabel()
        self.limit_note.setWordWrap(True)
        self.limit_note.setProperty("role", "muted")
        right_layout.addWidget(self.limit_note)
        split.addWidget(left)
        split.addWidget(right)
        split.setChildrenCollapsible(False)
        split.setStretchFactor(1, 1)
        split.setSizes([290, 1000])
        layout.addWidget(split, 1)
        self.accounts_table.itemSelectionChanged.connect(self._account_changed)
        self.strategies_table.itemSelectionChanged.connect(self._strategy_changed)
        self.refresh_button.clicked.connect(self.refresh)
        self.search.textChanged.connect(self._render_fills)
        self.side.currentIndexChanged.connect(self._render_fills)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        client.connectionChanged.connect(self.set_attached)
        self.set_presentation(self.theme, self.language)
        self.set_attached(self._attached)

    def set_shell_embedded(self, embedded: bool) -> None:
        """Keep selection and source context while the main shell owns the heading."""
        self.title.setVisible(not embedded)

    def open_margin_ledger(self, account_id):
        self.history_tabs.setCurrentWidget(self.ledger_history)
        if account_id:
            self.ledger_history.select_account(account_id)

    @staticmethod
    def _identity(item: dict, *keys) -> str:
        return next((str(item[key]) for key in keys if item.get(key) is not None), "")

    def update_state(self, snapshot: dict) -> None:
        self.ledger_history.update_state(snapshot)
        self.margin_history.update_state(snapshot)
        accounts = snapshot.get("accounts", [])
        strategies = snapshot.get("strategies", [])
        self._accounts = {self._identity(row, "account_id", "id"): row for row in accounts}
        self._strategies = {
            self._identity(row, "instance", "strategy_id", "id"): row for row in strategies
        }
        epoch = snapshot.get("core", {}).get("epoch")
        changed_epoch = self._epoch is not None and epoch != self._epoch
        self._epoch = epoch
        account = self._selected_account
        if account not in self._accounts:
            account = next(iter(self._accounts), "")
        self._fill_accounts(account)
        if account != self._selected_account:
            self.select_account(account)
        else:
            self._fill_strategies(self._selected_strategy)
        strategy = self._strategies.get(self._selected_strategy, {})
        marker = (
            epoch,
            self._selected_strategy,
            strategy.get("revision"),
            strategy.get("fill_count"),
        )
        if changed_epoch:
            self._pending_tag = None
        if marker != self._revision or changed_epoch:
            self._revision = marker
            self.refresh()
        self._render_account_info()
        self.set_attached(self.client.attached)

    def _fill_accounts(self, selected: str) -> None:
        blocker = QSignalBlocker(self.accounts_table)
        self.accounts_table.setRowCount(len(self._accounts))
        for index, (identity, row) in enumerate(self._accounts.items()):
            for column, value in enumerate(
                (identity, row.get("market", "—"), row.get("status", "—"))
            ):
                self.accounts_table.setItem(index, column, QTableWidgetItem(str(value)))
            if identity == selected:
                self.accounts_table.selectRow(index)
        del blocker

    def _fill_strategies(self, selected: str) -> None:
        blocker = QSignalBlocker(self.strategies_table)
        rows = [
            (identity, row)
            for identity, row in self._strategies.items()
            if row.get("account_id") == self._selected_account
        ]
        self.strategies_table.setRowCount(len(rows))
        found = False
        for index, (identity, row) in enumerate(rows):
            for column, value in enumerate(
                (identity, row.get("instrument", "—"), row.get("fill_count", "—"))
            ):
                self.strategies_table.setItem(index, column, QTableWidgetItem(str(value)))
            if identity == selected:
                self.strategies_table.selectRow(index)
                found = True
        del blocker
        if not found:
            self.select_strategy(rows[0][0] if rows else "")

    def select_account(self, identity: str) -> None:
        if identity and identity not in self._accounts:
            return
        self._selected_account = identity
        self._fill_accounts(identity)
        self._clear_detail()
        self._selected_strategy = ""
        self._fill_strategies("")
        self._render_account_info()

    def select_strategy(self, identity: str) -> None:
        row = self._strategies.get(identity, {})
        if identity and row.get("account_id") != self._selected_account:
            return
        self.refresh_button.setEnabled(self.client.attached and bool(identity))
        if identity != self._selected_strategy:
            self._selected_strategy = identity
            self._clear_detail()
            self._fill_strategies(identity)
            self._render_account_info()
            self.refresh()

    def _account_changed(self) -> None:
        item = self.accounts_table.item(self.accounts_table.currentRow(), 0)
        if item:
            self.select_account(item.text())

    def _strategy_changed(self) -> None:
        item = self.strategies_table.item(self.strategies_table.currentRow(), 0)
        if item:
            self.select_strategy(item.text())

    def _clear_detail(self) -> None:
        self._fills, self._detail = [], {}
        self._pending_tag = None
        self._pending_revision = None
        self._state, self._observed, self._error = "empty", "", ""
        self._render_fills()
        self._render_status()

    def _render_account_info(self) -> None:
        row = self._accounts.get(self._selected_account, {})
        asset = row.get("quote_asset", "?")
        cash = row.get("cash", row.get("balance", "—"))
        self.account_info.setText(
            f"{text('account_info', self.language)}\n{self._selected_account or '—'}\n"
            f"{text('cash', self.language)}  {cash} {asset}"
        )
        strategy = self._strategies.get(self._selected_strategy, {})
        self.selection_label.setText(
            "  ·  ".join(
                value
                for value in (
                    self._selected_account,
                    self._selected_strategy,
                    str(strategy.get("instrument", "")),
                )
                if value
            )
        )

    def refresh(self) -> None:
        if not self._selected_strategy or not self.client.attached or self._pending_tag:
            return
        self._counter += 1
        self._pending_tag = f"history:{self._selected_strategy}:{self._counter}"
        self._pending_revision = self._strategies.get(self._selected_strategy, {}).get("revision")
        self._state, self._error = "loading", ""
        self._render_status()
        self.client.request(
            "GET",
            "/api/strategies/" + quote(self._selected_strategy, safe=""),
            tag=self._pending_tag,
        )

    def _result(self, tag: str, result: dict) -> None:
        if tag != self._pending_tag:
            return
        self._pending_tag = None
        if (
            result.get("instance") != self._selected_strategy
            or result.get("account_id") != self._selected_account
        ):
            self._state, self._error = "failed", "Strategy/account response mismatch"
            self._render_status()
            return
        rows = result.get("fills", [])
        if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
            self._state, self._error = "failed", "Malformed fill records"
            self._render_status()
            return
        self._detail = result
        self._fills = rows[-MAX_FILLS:]
        self._observed = datetime.now(UTC).strftime("%Y/%m/%d %H:%M:%S UTC")
        self._state, self._error = "observed", ""
        self._render_fills()
        self._render_status()
        current_revision = self._strategies.get(self._selected_strategy, {}).get("revision")
        if current_revision != self._pending_revision:
            self.refresh()

    def _failed(self, tag: str, message: str) -> None:
        if tag != self._pending_tag:
            return
        self._pending_tag = None
        self._state, self._error = "failed", message[:300]
        self._render_status()

    def set_attached(self, attached: bool) -> None:
        was_attached, self._attached = self._attached, bool(attached)
        self.refresh_button.setEnabled(attached and bool(self._selected_strategy))
        if not attached:
            self._state = "disconnected"
            self._pending_tag = None
            self._render_status()
        elif not was_attached:
            self.refresh()

    def _render_status(self) -> None:
        label = text(self._state, self.language)
        if self._observed and self._state != "observed":
            label += " · " + self._observed
        elif self._state == "observed":
            label += " · " + self._observed
        if self._error:
            label += " · " + self._error
        self.status.setText(label)

    def _render_fills(self, *_args) -> None:
        query, side = self.search.text().strip().casefold(), self.side.currentData()
        instrument = self._detail.get("instrument", "—")
        account = self._accounts.get(self._selected_account, {})
        quote_asset = account.get("quote_asset", self._detail.get("quote_asset"))
        rows = []
        for record in reversed(self._fills):
            timestamp = recorded_time(record.get("timestamp_ms"))
            symbol = record.get("instrument", instrument)
            direction = str(record.get("side", "—")).upper()
            reason = str(record.get("reason", "—"))
            searchable = f"{timestamp} {symbol} {direction} {reason}".casefold()
            if (side and direction != side) or (query and query not in searchable):
                continue
            rows.append((record, timestamp, str(symbol), direction, reason))
        self.fills_table.setSortingEnabled(False)
        self.fills_table.setUpdatesEnabled(False)
        self.fills_table.setRowCount(len(rows))
        for index, (record, timestamp, symbol, direction, reason) in enumerate(rows):
            fee = recorded_decimal(record.get("fee"))
            fee_asset = record.get("fee_asset", quote_asset) or "?"
            pnl = next(
                (
                    recorded_decimal(record[key])
                    for key in ("realized_pnl", "pnl", "profit")
                    if key in record
                ),
                None,
            )
            pnl_asset = record.get("pnl_asset", quote_asset) or "?"
            quantity, price = (
                recorded_decimal(record.get("quantity")),
                recorded_decimal(record.get("price")),
            )
            values = (
                timestamp,
                symbol,
                direction,
                quantity if quantity is not None else "—",
                price if price is not None else "—",
                f"{fee} {fee_asset}" if fee is not None else "—",
                reason,
                f"{pnl} {pnl_asset}" if pnl is not None else "—",
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value)[:500])
                item.setToolTip(str(value)[:3000])
                if column == 2 and direction in {"BUY", "SELL"}:
                    item.setForeground(
                        QColor(THEMES[self.theme]["positive" if direction == "BUY" else "negative"])
                    )
                if column == 7 and pnl is not None and pnl:
                    item.setForeground(
                        QColor(THEMES[self.theme]["positive" if pnl > 0 else "negative"])
                    )
                self.fills_table.setItem(index, column, item)
        self.fills_table.setUpdatesEnabled(True)
        self.fills_table.setSortingEnabled(True)
        self.fills_table.sortItems(0, Qt.SortOrder.DescendingOrder)
        self.count_label.setText(
            text("count", self.language).format(visible=len(rows), total=len(self._fills))
        )
        self._render_summaries(quote_asset)

    def _render_summaries(self, quote_asset) -> None:
        totals: dict[str, Decimal] = {}
        missing = False
        for record in self._fills:
            fee, asset = recorded_decimal(record.get("fee")), record.get("fee_asset", quote_asset)
            if fee is None or not asset:
                missing = True
                continue
            totals[str(asset)] = totals.get(str(asset), Decimal(0)) + fee
        fees = " | ".join(f"{value} {asset}" for asset, value in sorted(totals.items()))
        if missing:
            fees += " · " + text("partial", self.language)
        self.fee_summary.setText(text("fees", self.language) + "  " + (fees or "—"))
        pnl_key = next(
            (
                key
                for key in ("realized_pnl", "pnl", "profit")
                if self._fills and all(key in row for row in self._fills)
            ),
            None,
        )
        if pnl_key:
            pnl_totals: dict[str, Decimal] = {}
            complete = True
            for row in self._fills:
                value = recorded_decimal(row[pnl_key])
                asset = row.get("pnl_asset", quote_asset)
                if value is None or not asset:
                    complete = False
                    continue
                pnl_totals[str(asset)] = pnl_totals.get(str(asset), Decimal(0)) + value
            pnl = " | ".join(f"{value} {asset}" for asset, value in sorted(pnl_totals.items()))
            if not complete:
                pnl += " · " + text("partial", self.language)
        elif any(
            any(key in row for key in ("realized_pnl", "pnl", "profit")) for row in self._fills
        ):
            pnl = text("partial", self.language)
        else:
            pnl = text("unavailable", self.language)
        self.pnl_summary.setText(text("pnl", self.language) + "  " + pnl)

    def set_presentation(self, theme: str, language: str) -> None:
        self.ledger_history.set_presentation(theme, language)
        self.margin_history.set_presentation(theme, language)
        self.theme = theme if theme in THEMES else "light"
        self.language = language
        self.title.setText(text("title", language))
        self.accounts_title.setText(text("accounts", language))
        self.strategies_title.setText(text("strategies", language))
        self.refresh_button.setText(text("refresh", language))
        self.source_note.setText(text("paper", language))
        self.search.setPlaceholderText(text("search", language))
        self.side.setItemText(0, text("all", language))
        self.limit_note.setText(text("limit", language) + "\n" + text("time_note", language))
        for table, key in (
            (self.accounts_table, "accounts"),
            (self.strategies_table, "strategies"),
            (self.fills_table, "fills"),
        ):
            table.setHorizontalHeaderLabels(_HEADERS[key][0 if language == "ko" else 1])
        self._render_account_info()
        self._render_status()
        self._render_fills()
