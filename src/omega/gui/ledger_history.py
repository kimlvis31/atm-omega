"""Owner-only stable ledger pages; unknown reads retain explicitly stale rows."""

import json
import os

from PySide6.QtCore import QDateTime, QSignalBlocker, Qt, QTimeZone
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFileDialog,
    QGridLayout,
    QHeaderView,
    QLabel,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.domain.errors import ValidationError
from omega.gui.ledger_export import LedgerExportTask
from omega.gui.margin_history import _time


class LedgerHistoryWidget(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch, self.capable, self.generation, self.serial = None, False, 0, 0
        self.pending, self.page, self.page_current, self.job = None, None, False, None
        self.export_requests = {}
        outer = QVBoxLayout(self)
        self.notice, self.status, self.counts = QLabel(), QLabel(), QLabel()
        for label in (self.notice, self.status, self.counts):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
            outer.addWidget(label)
        form = QGridLayout()
        self.account, self.period = QComboBox(), QCheckBox("UTC 기간 / UTC period")
        self.account.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.account.setMinimumContentsLength(12)
        self.start, self.end = QDateTimeEdit(), QDateTimeEdit()
        now = QDateTime.currentDateTimeUtc()
        for editor, date in ((self.start, now.addDays(-1)), (self.end, now)):
            editor.setTimeZone(QTimeZone(b"UTC"))
            editor.setDisplayFormat("yyyy-MM-dd HH:mm:ss 'UTC'")
            editor.setCalendarPopup(True)
            editor.setDateTime(date)
            editor.dateTimeChanged.connect(self._selection_changed)
        self.refresh_button = QPushButton()
        self.previous_button, self.next_button = QPushButton(), QPushButton()
        self.export_button = QPushButton()
        form.addWidget(self.account, 0, 0, 1, 2)
        form.addWidget(self.refresh_button, 0, 2)
        form.addWidget(self.period, 1, 0, 1, 3)
        form.addWidget(QLabel("시작 포함 / Start inclusive"), 2, 0)
        form.addWidget(self.start, 2, 1, 1, 2)
        form.addWidget(QLabel("끝 제외 / End exclusive"), 3, 0)
        form.addWidget(self.end, 3, 1, 1, 2)
        form.addWidget(self.previous_button, 4, 0)
        form.addWidget(self.next_button, 4, 1)
        form.addWidget(self.export_button, 4, 2)
        outer.addLayout(form)
        self.table = QTableWidget(0, 13)
        self.table.setHorizontalHeaderLabels(
            [
                "Seq",
                "Kind",
                "UTC",
                "Available UTC",
                "TS",
                "Instrument",
                "Qty / signed amount",
                "Price / reference",
                "Fee",
                "Recorded PnL",
                "Cross delta",
                "Isolated delta",
                "Event ID",
            ]
        )
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        outer.addWidget(self.table, 1)
        self.account.currentIndexChanged.connect(self._selection_changed)
        self.period.toggled.connect(self._selection_changed)
        self.refresh_button.clicked.connect(self.refresh)
        self.previous_button.clicked.connect(lambda: self._navigate(-128))
        self.next_button.clicked.connect(lambda: self._navigate(128))
        self.export_button.clicked.connect(self.choose_export)
        client.resultReceived.connect(self._received)
        client.failed.connect(self._failed)
        client.connectionChanged.connect(self.set_attached)
        self.set_presentation("light", "ko")

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def _context(self):
        return self.generation, self.epoch, self.account.currentData()

    def _invalidate(self, message):
        self.generation += 1
        self.pending, self.page_current = None, False
        if self.job:
            self.job.cancel()
        self.export_requests.clear()
        self.status.setText(message)
        self._controls()

    def _selection_changed(self, *_args):
        self._invalidate(
            self.text(
                "선택/기간 변경 · 이전 표는 미확인 · 새 snapshot을 조회하세요",
                "Selection/filter changed; prior rows unconfirmed; read a new snapshot",
            )
        )

    def set_accounts(self, identities):
        selected = self.account.currentData()
        blocker = QSignalBlocker(self.account)
        self.account.clear()
        self.account.addItem(self.text("계정 선택", "Select account"), None)
        for identity in identities:
            self.account.addItem(identity, identity)
        self.account.setCurrentIndex(max(0, self.account.findData(selected)))
        del blocker
        if self.account.currentData() != selected:
            self._selection_changed()

    def select_account(self, identity):
        if self.account.findData(identity) < 0:
            self.account.addItem(identity, identity)
        self.account.setCurrentIndex(self.account.findData(identity))
        self.refresh()

    def update_state(self, state):
        core = state.get("core", {})
        epoch = core.get("epoch")
        capable = core.get("capabilities", {}).get("margin_trading") is True
        if epoch != self.epoch or capable != self.capable:
            self.epoch, self.capable = epoch, capable
            self._invalidate(
                self.text(
                    "코어/지원 변경 · 이전 표는 미확인 · 새 snapshot 필요",
                    "Core/capability changed; prior rows unconfirmed; new snapshot required",
                )
            )
        self._controls()

    def set_attached(self, attached):
        if not attached:
            self._invalidate(
                self.text(
                    "연결 끊김 · 이전 표는 미확인 · 빈 원장을 뜻하지 않습니다",
                    "Disconnected; prior rows unconfirmed; this does not mean an empty ledger",
                )
            )
        self._controls()

    def _filters(self):
        if not self.period.isChecked():
            return {}
        start, end = (
            self.start.dateTime().toMSecsSinceEpoch(),
            self.end.dateTime().toMSecsSinceEpoch(),
        )
        if not 0 <= start < end:
            raise ValueError("UTC start must precede exclusive end")
        return {"start_ms": start, "end_ms": end}

    def refresh(self):
        if not self._enabled() or self.job:
            return
        self._invalidate(
            self.text(
                "새 snapshot 조회 중 · 이전 표 유지", "Reading new snapshot; prior rows retained"
            )
        )
        self._request_page(0)

    def _enabled(self):
        return bool(
            self.client.attached and self.capable and self.epoch and self.account.currentData()
        )

    def _request_page(self, offset, snapshot=None):
        from omega.runtime.ledger_client import ledger_url

        try:
            query = self._filters() | {"offset": offset, "limit": 128}
            if snapshot is not None:
                query["snapshot"] = snapshot
            path = ledger_url(self.account.currentData(), query)
        except (ValidationError, ValueError, TypeError) as error:
            self.status.setText(str(error))
            return
        self.serial += 1
        tag = f"ledger-page:{id(self)}:{self.serial}"
        self.pending = tag, self._context(), query
        self._controls()
        self.client.request("GET", path, tag=tag)

    def _navigate(self, delta):
        if self.page_current and self.page and not self.pending and not self.job:
            offset = self.page["offset"] + delta
            if offset >= 0 and (delta < 0 or self.page["next_offset"] == offset):
                self._request_page(offset, self.page["snapshot"])

    def _received(self, tag, result):
        if tag in self.export_requests:
            task, ticket = self.export_requests.pop(tag)
            if task is self.job and task.context == self._context() and self._enabled():
                task.deliver(ticket, result)
            else:
                task.cancel()
            return
        if self.pending is None or self.pending[:2] != (tag, self._context()):
            return
        _, _, query = self.pending
        self.pending = None
        try:
            from omega.runtime.ledger_client import validate_ledger_page

            page = validate_ledger_page(
                result,
                account_id=self.account.currentData(),
                core_epoch=self.epoch,
                **query,
            )
            if page["state"] != "ready":
                raise ValueError(f"Ledger unavailable: {page.get('reason_code', 'unknown')}")
            if (
                query.get("snapshot") is not None
                and self.page is not None
                and any(
                    page[key] != self.page[key]
                    for key in ("ledger_total", "ledger_capacity", "total")
                )
            ):
                raise ValueError("Ledger snapshot counts changed")
        except (ValidationError, ValueError, TypeError, KeyError) as error:
            self.page_current = False
            self.status.setText(
                self.text("미확인 · 이전 표 유지: ", "Unconfirmed; prior rows retained: ")
                + str(error)[:300]
            )
            self._controls()
            return
        self.page, self.page_current = page, True
        self._render()
        self.status.setText(
            self.text(
                "확인된 원장 snapshot · append는 새 조회 시 반영",
                "Confirmed ledger snapshot; refresh explicitly to include appends",
            )
        )
        self._controls()

    def _failed(self, tag, message):
        if tag in self.export_requests:
            task, ticket = self.export_requests.pop(tag)
            task.deliver(ticket, error=message)
        elif self.pending and self.pending[0] == tag:
            self.pending, self.page_current = None, False
            self.status.setText(
                self.text("조회 실패 · 이전 표 유지: ", "Read failed; prior rows retained: ")
                + message[:300]
            )
            self._controls()

    def _render(self):
        page = self.page
        if not page:
            return
        self.table.setRowCount(len(page["rows"]))
        for index, row in enumerate(page["rows"]):
            fact = row["fact"]
            values = (
                row["sequence"],
                fact["kind"],
                _time(fact["event_time_ms"]),
                _time(fact["available_at_ms"]),
                fact["owner_id"],
                fact["instrument"],
                fact.get("delta_qty", fact.get("amount", "—")),
                fact.get("price", fact.get("reference_price", "—")),
                row.get("fee", "—"),
                row.get("realized_pnl", "—"),
                row.get("cross_delta", "—"),
                row.get("isolated_delta", "—"),
                fact.get("fill_id", fact.get("funding_id", fact.get("transfer_id", "—"))),
            )
            for column, value in enumerate(values):
                item = QTableWidgetItem(str(value))
                item.setToolTip(json.dumps(row, ensure_ascii=False)[:8000])
                self.table.setItem(index, column, item)
        self.counts.setText(
            f"Rows {page['offset']}–{page['offset'] + len(page['rows'])} / "
            f"filtered {page['total']} · "
            f"ledger {page['ledger_total']} / capacity {page['ledger_capacity']} · "
            f"snapshot {page['snapshot'][:16]}…"
        )
        self.counts.setToolTip(page["snapshot"])

    def choose_export(self):
        if not self._enabled() or self.job or self.pending:
            return
        destination, _ = QFileDialog.getSaveFileName(
            self, "Ledger ZIP", "ledger.zip", "ZIP (*.zip)"
        )
        if destination:
            self.start_export(destination)

    def start_export(self, destination):
        if not self._enabled() or self.job or self.pending:
            return
        try:
            filters = self._filters()
        except ValueError as error:
            self.status.setText(str(error))
            return
        task = LedgerExportTask(
            destination,
            account_id=self.account.currentData(),
            filters=filters,
            context=self._context(),
            parent=self,
        )
        task.readRequested.connect(
            lambda ticket: self._export_read(task, ticket), Qt.ConnectionType.QueuedConnection
        )
        task.completed.connect(self._export_done, Qt.ConnectionType.QueuedConnection)
        self.job = task
        self.status.setText(
            self.text(
                "별도 snapshot으로 ZIP 작성 중 · 금융 원장만",
                "Exporting a separate stable snapshot; financial ledger only",
            )
        )
        self._controls()
        task.start()

    def _export_read(self, task, ticket):
        if (
            task.cancelled
            or task is not self.job
            or task.context != self._context()
            or not self._enabled()
        ):
            task.cancel()
            return
        from omega.runtime.ledger_client import ledger_url

        try:
            path = ledger_url(ticket["account_id"], ticket["query"])
        except (ValidationError, ValueError, TypeError) as error:
            task.deliver(ticket, error=str(error))
            return
        self.serial += 1
        tag = f"ledger-export:{id(self)}:{self.serial}"
        self.export_requests[tag] = task, ticket
        self.client.request("GET", path, tag=tag)

    def _export_done(self, task, _result, error):
        current = (
            task is self.job
            and task.context == self._context()
            and self._enabled()
            and not task.cancelled
        )
        self.export_requests = {
            tag: row for tag, row in self.export_requests.items() if row[0] is not task
        }
        if task is self.job:
            self.job = None
        try:
            if current and not error:
                # A selection/epoch change even after the last page prevents final publication.
                os.replace(task.staging, task.destination)
                self.status.setText(
                    self.text("금융 원장 ZIP 저장 완료: ", "Financial ledger ZIP saved: ")
                    + str(task.destination)
                )
            elif current:
                self.status.setText(
                    self.text(
                        "내보내기 실패 · 기존 ZIP 유지: ", "Export failed; prior ZIP preserved: "
                    )
                    + error
                )
        except OSError as exc:
            self.status.setText(str(exc)[:300])
        finally:
            try:
                task.discard()
            except OSError as exc:
                self.status.setText(f"Staging cleanup failed: {str(exc)[:200]}")
            task.deleteLater()
            self._controls()

    def _controls(self):
        enabled, idle = self._enabled(), self.pending is None and self.job is None
        self.refresh_button.setEnabled(enabled and idle)
        self.export_button.setEnabled(enabled and idle)
        self.previous_button.setEnabled(
            enabled and idle and self.page_current and self.page["offset"] > 0
        )
        self.next_button.setEnabled(
            enabled and idle and self.page_current and self.page["next_offset"] is not None
        )
        self.start.setEnabled(self.period.isChecked())
        self.end.setEnabled(self.period.isChecked())

    def set_presentation(self, _theme, language):
        self.language = language
        self.notice.setText(
            self.text(
                "가상 증거금 v2 · 128행씩 · UTC 시작 포함/끝 제외. "
                "전략/검색은 최근 기록 탭에서 제공합니다. "
                "ZIP은 같은 계정/기간의 별도 snapshot으로 전체 금융 원장만 저장합니다. "
                "Funding은 시나리오 현금흐름이며 실지급이 아닙니다. "
                "미확인 상태는 빈 원장이 아닙니다.",
                "Virtual margin v2 · 128 rows/page · UTC inclusive start/exclusive end. "
                "Strategy/search filters remain in Recent records. "
                "ZIP reads a separate stable snapshot of this account/period "
                "and contains financial ledger only. Scenario funding is not a "
                "venue payment. Unknown is not an empty ledger.",
            )
        )
        self.refresh_button.setText(self.text("새 snapshot 조회", "Read new snapshot"))
        self.previous_button.setText(self.text("이전 128행", "Previous 128"))
        self.next_button.setText(self.text("다음 128행", "Next 128"))
        self.export_button.setText(self.text("전체 기간 ZIP 저장", "Export period ZIP"))
        self._controls()

    def closeEvent(self, event):
        self._invalidate(self.text("조회 종료 · 이전 표", "Closed; prior rows"))
        super().closeEvent(event)
