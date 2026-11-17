"""Manual bounded public-data jobs; no database or exchange access in the GUI."""

# Imports
import json
import re
from datetime import UTC, datetime
from uuid import uuid4

from PySide6.QtCore import QDateTime, Qt, QTimer, QTimeZone
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QDateTimeEdit,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.domain.models import is_valid_symbol

KINDS = ("trade_flow", "current_oi", "history_oi", "long_short_ratio")
JOB_KINDS = frozenset(KINDS) | {"trade_flow_repair"}
ACTIVE = {"queued", "running", "cancelling"}
STATES = ACTIVE | {"completed", "partial", "cancelled", "unknown", "error"}


def _valid_instrument(value):
    parts = value.split(":") if type(value) is str else ()
    return (
        len(parts) == 3
        and parts[0] == "binance"
        and parts[1] in {"spot", "usdm"}
        and is_valid_symbol(parts[2])
    )


def _row(value):
    if not isinstance(value, dict):
        raise ValueError("Invalid job")
    if (
        not isinstance(value.get("id"), str)
        or re.fullmatch(r"[A-Za-z0-9_-]{1,96}", value["id"]) is None
        or value.get("kind") not in JOB_KINDS
        or value.get("state") not in STATES
        or not isinstance(value.get("parameters"), dict)
        or not _valid_instrument(value["parameters"].get("instrument"))
        or re.fullmatch(r"[a-f0-9]{32}", str(value.get("execution_id"))) is None
        or type(value.get("generation")) is not int
        or value["generation"] < 0
        or type(value.get("cancel_requested")) is not bool
        or type(value.get("worker_started")) is not bool
        or re.fullmatch(r"[a-f0-9]{64}", str(value.get("destination_id"))) is None
        or value.get("archive_id") is not None
        and re.fullmatch(r"[a-f0-9]{64}", str(value["archive_id"])) is None
        or value.get("result") is not None
        and not isinstance(value["result"], dict)
    ):
        raise ValueError("Invalid job identity or state")
    # Bound result rendering before touching Qt; never interpret result as observed OHLCV.
    if len(json.dumps(value, allow_nan=False)) > 100_000:
        raise ValueError("Job detail exceeds display capacity")
    return value


class AuxiliaryPanel(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.instrument, self.epoch = None, None
        self.attached, self.capable = bool(client.attached), False
        self.closed, self.storage_failed = False, False
        self.jobs, self._requests = {}, {}
        self._serial, self._version = 0, 0
        self._pending = None
        self._repair_key = None
        self.setCheckable(True)
        self.setChecked(False)
        root = QVBoxLayout(self)
        self.content = QWidget()
        root.addWidget(self.content)
        layout = QVBoxLayout(self.content)
        self.hint, self.kind_hint, self.target, self.status, self.summary = (
            QLabel() for _ in range(5)
        )
        for label in (self.hint, self.kind_hint, self.target, self.status, self.summary):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.hint)
        layout.addWidget(self.target)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.kind = QComboBox()
        for kind in KINDS:
            self.kind.addItem(kind, kind)
        self.start, self.end = QDateTimeEdit(), QDateTimeEdit()
        end = int(datetime.now(UTC).timestamp()) // 60 * 60000
        for edit, stamp in ((self.start, end - 5 * 60000), (self.end, end)):
            edit.setTimeZone(QTimeZone(b"UTC"))
            edit.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
            edit.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
            edit.setCalendarPopup(True)
        self.limit, self.pages = QSpinBox(), QSpinBox()
        self.limit.setRange(1, 1000)
        self.limit.setValue(500)
        self.pages.setRange(1, 20)
        self.pages.setValue(20)
        self._form_labels = []
        for words, field in (
            (("자료 종류", "Data kind"), self.kind),
            (("시작 (포함)", "Start (inclusive)"), self.start),
            (("끝 (제외)", "End (exclusive)"), self.end),
            (("응답 건수 상한", "Response limit"), self.limit),
            (("최대 페이지", "Maximum pages"), self.pages),
        ):
            label = QLabel()
            self._form_labels.append((label, words))
            form.addRow(label, field)
        layout.addLayout(form)
        layout.addWidget(self.kind_hint)
        grid = QGridLayout()
        self.submit_button, self.refresh_button, self.cancel_button, self.resume_button = (
            QPushButton() for _ in range(4)
        )
        for i, button in enumerate(
            (self.submit_button, self.refresh_button, self.cancel_button, self.resume_button)
        ):
            grid.addWidget(button, i // 2, i % 2)
        layout.addLayout(grid)
        self.table = QTableWidget(0, 4)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setMinimumHeight(140)
        self.table.setMaximumHeight(210)
        layout.addWidget(self.table)
        self.repair_box = QGroupBox()
        repair_form = QFormLayout(self.repair_box)
        repair_form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.repair_minute = QComboBox()
        self.repair_label = QLabel()
        repair_form.addRow(self.repair_label, self.repair_minute)
        self.repair_limit, self.repair_pages = QSpinBox(), QSpinBox()
        self.repair_limit.setRange(1, 1000)
        self.repair_limit.setValue(500)
        self.repair_pages.setRange(1, 20)
        self.repair_pages.setValue(20)
        self.repair_limit_label, self.repair_pages_label = QLabel(), QLabel()
        repair_form.addRow(self.repair_limit_label, self.repair_limit)
        repair_form.addRow(self.repair_pages_label, self.repair_pages)
        self.repair_button, self.repair_hint = QPushButton(), QLabel()
        self.repair_hint.setWordWrap(True)
        self.repair_hint.setTextFormat(Qt.TextFormat.PlainText)
        repair_form.addRow(self.repair_hint)
        repair_form.addRow(self.repair_button)
        layout.addWidget(self.repair_box)
        self.repair_button.clicked.connect(self.repair)
        layout.addWidget(self.summary)
        self.results = QTableWidget(0, 2)
        self.results.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.results.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.results.setMinimumHeight(100)
        self.results.setMaximumHeight(180)
        layout.addWidget(self.results)
        layout.addWidget(self.status)
        self.kind.currentIndexChanged.connect(self._kind_changed)
        self.submit_button.clicked.connect(self.submit)
        self.refresh_button.clicked.connect(self.refresh)
        self.cancel_button.clicked.connect(lambda: self.action("cancel"))
        self.resume_button.clicked.connect(lambda: self.action("resume"))
        self.table.itemSelectionChanged.connect(self._selection_changed)
        self.toggled.connect(self._expanded)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.timer = QTimer(self)
        self.timer.setInterval(5000)
        self.timer.timeout.connect(
            lambda: self.refresh() if self.isVisible() and self.isChecked() else None
        )
        self.timer.start()
        self.set_presentation("light", "ko")
        self.content.hide()
        self._kind_changed()

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def _expanded(self, checked):
        self.content.setVisible(checked)
        if checked:
            self.refresh()

    def set_instrument(self, instrument):
        if instrument == self.instrument:
            return
        self.instrument = instrument
        self._version += 1
        self._render()
        self._kind_changed()
        if self.isChecked():
            self.refresh()

    def update_state(self, state):
        core = state.get("core") or {}
        epoch = core.get("epoch") or state.get("core_epoch")
        if isinstance(epoch, str) and epoch and epoch != self.epoch:
            self.epoch = epoch
            self._version += 1
            self.jobs.clear()
            self._pending = None
            self.closed, self.storage_failed = False, False
            self._render()
        self.capable = (core.get("capabilities") or {}).get("auxiliary_jobs") is True
        if not self.capable:
            self.status.setText(
                self.text(
                    "코어 보조 수집 기능을 사용할 수 없습니다",
                    "Core auxiliary collection is unavailable",
                )
            )
        self._controls()

    def set_attached(self, attached):
        self.attached = bool(attached)
        if not attached:
            self._version += 1
            if self._pending:
                self._pending["unknown"] = True
            self.status.setText(
                self.text(
                    "연결 끊김 · 마지막 결과는 오래됐을 수 있습니다",
                    "Disconnected · last results may be stale",
                )
            )
        self._controls()

    def _kind_changed(self):
        kind = self.kind.currentData()
        current = kind == "current_oi"
        for widget in (self.start, self.end, self.limit):
            widget.setEnabled(not current)
        self.limit.setMaximum(1000 if kind == "trade_flow" else 500)
        self.pages.setEnabled(kind == "trade_flow")
        self.kind_hint.setText(
            self.text(
                "과거 통계는 최대 500건의 native 5분 batch 한 번을 저장합니다. "
                "completed는 수신 자료 저장 완료이며 "
                "요청 전체 기간의 coverage 검증 완료가 아닙니다.",
                "Historical statistics store one received native 5m batch, up to 500 rows. "
                "Completed means received data was stored; "
                "full requested-range coverage is unverified.",
            )
            if kind in {"history_oi", "long_short_ratio"}
            else self.text(
                "현재 OI는 실제 관측 시점 하나를 저장하며 기간 coverage를 뜻하지 않습니다.",
                "Current OI stores one actual observation, without establishing range coverage.",
            )
            if current
            else ""
        )
        self._controls()

    def _selected(self):
        rows = self.table.selectionModel().selectedRows()
        if len(rows) != 1:
            return None
        item = self.table.item(rows[0].row(), 0)
        row = self.jobs.get(item.data(Qt.ItemDataRole.UserRole)) if item else None
        return row if row and row["parameters"]["instrument"] == self.instrument else None

    def _controls(self):
        active = (
            self.attached
            and self.capable
            and bool(self.epoch)
            and not self.closed
            and not self.storage_failed
        )
        selected = self._selected()
        allowed = _valid_instrument(self.instrument)
        allowed = allowed and (
            not self.instrument.startswith("binance:spot:")
            or self.kind.currentData() == "trade_flow"
        )
        self.submit_button.setEnabled(active and allowed and not self._pending)
        self.refresh_button.setEnabled(self.attached and self.capable and bool(self.epoch))
        self.cancel_button.setEnabled(
            active and not self._pending and bool(selected and selected["state"] in ACTIVE)
        )
        resumable = selected and selected["state"] in {"partial", "cancelled", "unknown", "error"}
        if (
            resumable
            and selected["kind"] not in {"trade_flow", "trade_flow_repair"}
            and selected["worker_started"]
            and not selected.get("archive_id")
        ):
            resumable = False
        self.resume_button.setEnabled(active and not self._pending and bool(resumable))
        repairable = self._repair_options(selected)
        self.repair_button.setEnabled(
            active
            and not self._pending
            and bool(repairable)
            and self.repair_minute.currentData() in {row["open_time_ms"] for row in repairable}
        )
        self.target.setText(
            self.instrument or self.text("종목 한 개를 선택하세요", "Select exactly one instrument")
        )

    def parameters(self):
        kind = self.kind.currentData()
        params = {"instrument": self.instrument}
        if kind == "current_oi":
            return params
        start, end = (edit.dateTime().toMSecsSinceEpoch() for edit in (self.start, self.end))
        closed = int(datetime.now(UTC).timestamp()) // 60 * 60000
        if not 0 <= start < end <= closed or start % 60000 or end % 60000:
            raise ValueError(
                self.text("닫힌 UTC 분 범위를 지정하세요", "Choose a closed UTC minute range")
            )
        if end - start > (10 * 60000 if kind == "trade_flow" else 28 * 86400000):
            raise ValueError(
                self.text(
                    "체결은 최대 10분, 통계는 최대 28일입니다",
                    "Trades allow at most 10 minutes; statistics 28 days",
                )
            )
        params.update(start_ms=start, end_ms=end, limit=self.limit.value())
        if kind == "trade_flow":
            params["max_pages"] = self.pages.value()
        return params

    def _request(self, method, path, payload=None, *, context):
        if len(self._requests) >= 64:
            self._requests = {
                tag: value
                for tag, value in self._requests.items()
                if value[0] == self.epoch and value[1] == self._version
            }
        self._serial += 1
        tag = f"auxiliary:{id(self)}:{self._serial}"
        self._requests[tag] = (self.epoch, self._version, context)
        self.client.request(method, path, payload, tag=tag)

    def submit(self):
        self._controls()
        if not self.submit_button.isEnabled():
            return
        try:
            params = self.parameters()
        except ValueError as exc:
            self.status.setText(str(exc))
            return
        self._submit_job(self.kind.currentData(), params)

    def _submit_job(self, kind, params):
        identity = uuid4().hex
        self._pending = {
            "id": identity,
            "kind": kind,
            "parameters": params,
            "action": "submit",
            "unknown": False,
        }
        self._version += 1
        self._request(
            "POST",
            "/api/auxiliary-jobs",
            {"id": identity, "kind": kind, "parameters": params},
            context=("mutation", identity),
        )
        self.status.setText(self.text("작업 접수 확인 중", "Confirming job acceptance"))
        self._controls()

    @staticmethod
    def _repair_options(row):
        if not row or row["kind"] != "trade_flow" or row["state"] != "partial":
            return []
        result = row.get("result")
        values = result.get("repairable_minutes") if isinstance(result, dict) else None
        params = row["parameters"]
        start, end = params.get("start_ms"), params.get("end_ms")
        if (
            type(values) is not list
            or len(values) > 10
            or type(start) is not int
            or type(end) is not int
            or not 0 <= start < end <= 253_402_300_799_000
            or start % 60000
            or end % 60000
        ):
            return []
        openings = set()
        for value in values:
            if (
                type(value) is not dict
                or value.get("quality") not in {"partial", "missing"}
                or type(value.get("open_time_ms")) is not int
                or not start <= value["open_time_ms"] < end
                or value["open_time_ms"] % 60000
                or value["open_time_ms"] in openings
                or type(value.get("revision")) is not int
                or not 1 <= value["revision"] < 2**63
                or re.fullmatch(r"[a-f0-9]{64}", str(value.get("checksum"))) is None
            ):
                return []
            openings.add(value["open_time_ms"])
        return sorted(values, key=lambda value: value["open_time_ms"])

    def repair(self):
        self._controls()
        if not self.repair_button.isEnabled():
            return
        parent = self._selected()
        opening = self.repair_minute.currentData()
        if opening not in {row["open_time_ms"] for row in self._repair_options(parent)}:
            return
        self._submit_job(
            "trade_flow_repair",
            {
                "instrument": parent["parameters"]["instrument"],
                "parent_job_id": parent["id"],
                "parent_execution_id": parent["execution_id"],
                "open_time_ms": opening,
                "limit": self.repair_limit.value(),
                "max_pages": self.repair_pages.value(),
            },
        )

    def action(self, action):
        self._controls()
        button = self.cancel_button if action == "cancel" else self.resume_button
        if action not in {"cancel", "resume"} or not button.isEnabled():
            return
        row = self._selected()
        self._pending = {
            "id": row["id"],
            "kind": row["kind"],
            "parameters": row["parameters"],
            "action": action,
            "execution_id": row["execution_id"],
            "generation": row["generation"],
            "unknown": False,
        }
        self._version += 1
        self._request(
            "POST",
            f"/api/auxiliary-jobs/{row['id']}/{action}",
            {"execution_id": row["execution_id"]},
            context=("mutation", row["id"]),
        )
        self._controls()

    def refresh(self):
        if not self.attached or not self.capable or not self.epoch:
            return
        # Bound overlapping timer requests; an unknown mutation is resolved by GET only.
        if any(
            epoch == self.epoch and version == self._version
            for epoch, version, _ in self._requests.values()
        ):
            return
        pending = self._pending
        if pending:
            if pending["unknown"]:
                self._request(
                    "GET",
                    "/api/auxiliary-jobs/" + pending["id"],
                    context=("resolve", pending["id"]),
                )
            return
        self._request("GET", "/api/auxiliary-jobs", context=("list", None))

    def _check_row(self, row):
        previous = self.jobs.get(row["id"])
        if previous and (
            row["generation"] < previous["generation"]
            or row["generation"] == previous["generation"]
            and row["execution_id"] != previous["execution_id"]
        ):
            raise ValueError("Stale attempt")
        if previous and any(
            row[key] != previous[key] for key in ("kind", "parameters", "destination_id")
        ):
            raise ValueError("Changed job plan")

    def _accept_row(self, row):
        self._check_row(row)
        self.jobs[row["id"]] = row

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if not request or request[0] != self.epoch or not self.attached:
            return
        epoch, version, (kind, identity) = request
        try:
            if kind == "list":
                if version != self._version:
                    return
                if (
                    value.get("schema_version") != 1
                    or value.get("core_epoch") != epoch
                    or not isinstance(value.get("jobs"), list)
                    or len(value["jobs"]) > 128
                ):
                    raise ValueError("Invalid snapshot")
                rows = [_row(row) for row in value["jobs"]]
                if len({row["id"] for row in rows}) != len(rows):
                    raise ValueError("Duplicate jobs")
                for row in rows:
                    self._check_row(row)
                for row in rows:
                    self._accept_row(row)
                self.closed, self.storage_failed = (
                    value.get("closed") is True,
                    value.get("storage_failed") is True,
                )
            else:
                pending = self._pending
                if not pending or pending["id"] != identity:
                    return
                row = _row(value)
                if (
                    row["id"] != identity
                    or row["kind"] != pending["kind"]
                    or row["parameters"] != pending["parameters"]
                ):
                    raise ValueError("Unexpected job plan")
                if pending["action"] == "cancel" and row["execution_id"] != pending["execution_id"]:
                    raise ValueError("Stale cancellation")
                if pending["action"] == "resume" and row["generation"] < pending["generation"]:
                    raise ValueError("Stale resumption")
                self._accept_row(row)
                self._pending = None
                self._version += 1
            self.status.setText(
                self.text(
                    "기록 저장 실패 · 새 작업/재개 불가",
                    "Recording failed · new jobs/resumption blocked",
                )
                if self.storage_failed
                else self.text("보조 수집 서비스 종료됨", "Auxiliary collection service closed")
                if self.closed
                else self.text(
                    "수동 공개 수집 상태 갱신", "Manual public collection status updated"
                )
            )
            self._render()
        except (ValueError, TypeError, KeyError, OverflowError):
            if self._pending:
                self._pending["unknown"] = True
            self.status.setText(
                self.text(
                    "결과 미확인 · 새 요청을 보내지 않고 조회하세요",
                    "Outcome unconfirmed · refresh without reposting",
                )
            )
        self._controls()

    def _failed(self, tag, _message):
        request = self._requests.pop(tag, None)
        if not request or request[0] != self.epoch:
            return
        if request[2][0] != "list" and self._pending:
            self._pending["unknown"] = True
        self.status.setText(
            self.text(
                "조회 실패 / 결과 미확인 · 새 요청 자동 재전송 없음",
                "Read failed / outcome unconfirmed · no automatic repost",
            )
        )
        self._controls()

    def _render(self):
        selected = self._selected()
        selected_id = selected["id"] if selected else None
        rows = [
            row for row in self.jobs.values() if row["parameters"]["instrument"] == self.instrument
        ]
        self.table.blockSignals(True)
        self.table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            for column, text in enumerate(
                (row["kind"], row["state"], str(row["generation"]), row["id"][:12])
            ):
                item = QTableWidgetItem(text)
                item.setData(Qt.ItemDataRole.UserRole, row["id"])
                item.setToolTip(row["id"] + " / " + row["execution_id"])
                self.table.setItem(index, column, item)
            if row["id"] == selected_id:
                self.table.selectRow(index)
        if selected_id is None:
            self.table.clearSelection()
        self.table.blockSignals(False)
        self._selection_changed()

    def _selection_changed(self):
        row = self._selected()
        self._render_repair(row)
        self.results.setRowCount(0)
        self.summary.clear()
        if row:
            result = row.get("result") or {}
            coverage = ""
            if row["kind"] in {"history_oi", "long_short_ratio"}:
                # Older saved results have no coverage metadata. Never infer coverage
                # from job completion, row count or the requested start/end duration.
                received = result.get("received_rows") if isinstance(result, dict) else None
                count = str(received) if type(received) is int and received >= 0 else "?"
                coverage = self.text(
                    f"\n범위 상태: 전체 요청 기간 미검증 · 수신 batch {count}건 (최대 500건)",
                    f"\nCoverage: full requested range unverified · received batch {count} rows "
                    "(up to 500)",
                )
            elif row["kind"] == "current_oi":
                coverage = self.text(
                    "\n범위 상태: 단일 시점 관측 · 기간 coverage 해당 없음",
                    "\nCoverage: single point observation · range coverage not applicable",
                )
            elif row["kind"] == "trade_flow_repair":
                coverage = self.text(
                    "\n복구 재개는 고정된 pending 기록 재생입니다. "
                    "새 관측으로 다시 복구하려면 원래 partial 부모 작업을 선택하세요.",
                    "\nRepair resume replays frozen pending data. "
                    "For a new observation attempt, select the original partial parent job.",
                )
            self.summary.setText(
                f"{row['state']} · attempt {row['generation']} · "
                f"cancel_requested={row['cancel_requested']}\n"
                + self.text(
                    "취소는 협력적이며 완료 후 취소 요청도 남을 수 있습니다. "
                    "재개는 같은 계획을 사용합니다.",
                    "Cancellation is cooperative; completion may retain a cancellation request. "
                    "Resume keeps the same plan.",
                )
                + coverage
            )
            self.summary.setToolTip(
                json.dumps(
                    {
                        key: row.get(key)
                        for key in ("parameters", "destination_id", "archive_id", "execution_id")
                    },
                    ensure_ascii=False,
                )
            )
            values = list(result.items())[:64] if isinstance(result, dict) else []
            if row.get("error"):
                values.insert(0, ("error", row["error"]))
            self.results.setRowCount(len(values))
            for index, (key, value) in enumerate(values):
                for column, text in enumerate((key, json.dumps(value, ensure_ascii=False))):
                    item = QTableWidgetItem(str(text)[:1500])
                    item.setToolTip(str(text)[:3000])
                    self.results.setItem(index, column, item)
        self._controls()

    def _render_repair(self, row):
        parent = row and row["kind"] == "trade_flow" and row["state"] == "partial"
        self.repair_box.setVisible(bool(parent))
        key = (row["id"], row["execution_id"]) if parent else None
        selected = self.repair_minute.currentData() if key == self._repair_key else None
        self._repair_key = key
        options = self._repair_options(row)
        self.repair_minute.clear()
        for value in options:
            date = datetime.fromtimestamp(value["open_time_ms"] / 1000, UTC).strftime(
                "%Y-%m-%d %H:%M UTC"
            )
            self.repair_minute.addItem(
                f"{date} · {value['quality']} · r{value['revision']}", value["open_time_ms"]
            )
            self.repair_minute.setItemData(
                self.repair_minute.count() - 1, value["checksum"], Qt.ItemDataRole.ToolTipRole
            )
        index = self.repair_minute.findData(selected)
        if index >= 0:
            self.repair_minute.setCurrentIndex(index)
        self.repair_minute.setEnabled(bool(options))
        self.repair_hint.setText(
            self.text(
                "원문을 새로 수집해 선택한 1분만 명시 복구합니다. "
                "원래 부모 attempt와 기존 revision을 확인하며 "
                "성공/범위 품질은 결과에서 확인하세요.",
                "Explicitly fetch observations to repair only the selected minute. "
                "The parent attempt and existing revision are checked; "
                "inspect the outcome/quality.",
            )
            if options
            else self.text(
                "복구 가능 분 metadata가 없거나 유효하지 않습니다. "
                "구형 결과를 임의로 복구 가능하다고 가정하지 않습니다. 최신 조회를 확인하세요.",
                "Repairable-minute metadata is missing or invalid. "
                "Legacy results are not assumed repairable; check the latest status.",
            )
        )

    def set_presentation(self, _theme, language):
        self.language = language if language in {"ko", "en"} else "ko"
        self.setTitle(
            self.text("보조 자료 · 수동 공개 수집", "Auxiliary data · manual public collection")
        )
        self.hint.setText(
            self.text(
                "실시간 자동 구독이 아닙니다. 체결: 닫힌 1~10분 / 최대20페이지. "
                "OI·비율: USDM만, 과거 통계는 native 5분·최대28일이며 "
                "반환 개수 제한으로 부분 수집될 수 있습니다. "
                "원문 보관과 정규화 저장은 OHLCV 준비 여부와 별개입니다.",
                "Not a live subscription. Trades: 1–10 closed minutes / at most 20 pages. "
                "OI/ratios: USDM only; native 5m history up to 28 days may be partial "
                "due to response limits. Raw archives and normalized storage "
                "are separate from OHLCV readiness.",
            )
        )
        for index, words in enumerate(
            (
                ("체결 흐름 (trade_flow)", "Trade flow"),
                ("현재 미결제약정 (current_oi)", "Current open interest"),
                ("과거 미결제약정 (history_oi)", "Historical open interest (5m)"),
                ("롱/숏 비율 (long_short_ratio)", "Long/short ratio (5m)"),
            )
        ):
            self.kind.setItemText(index, words[0 if self.language == "ko" else 1])
        for label, words in self._form_labels:
            label.setText(words[0 if self.language == "ko" else 1])
        for button, words in (
            (self.submit_button, ("새 수집 작업", "New collection job")),
            (self.refresh_button, ("조회 / 미확인 결과 확인", "Refresh / resolve outcome")),
            (self.cancel_button, ("선택 작업 취소 요청", "Request selected cancellation")),
            (self.resume_button, ("선택 계획 수동 재개", "Manually resume selected plan")),
        ):
            button.setText(words[0 if self.language == "ko" else 1])
        self.table.setHorizontalHeaderLabels(["Kind", "State", "Attempt", "Job ID"])
        self.results.setHorizontalHeaderLabels(["Field", "Result (not OHLCV readiness)"])
        self.repair_box.setTitle(
            self.text(
                "선택 partial 작업 · 1분 명시 복구", "Selected partial job · explicit 1m repair"
            )
        )
        self.repair_label.setText(self.text("복구할 분", "Minute to repair"))
        self.repair_limit_label.setText(self.text("복구 응답 상한", "Repair response limit"))
        self.repair_pages_label.setText(self.text("복구 최대 페이지", "Repair maximum pages"))
        self.repair_button.setText(
            self.text("선택한 1분 복구 작업 생성", "Create repair job for the selected minute")
        )
        self._kind_changed()
        self._selection_changed()
        self._controls()
