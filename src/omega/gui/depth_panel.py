"""Collapsed explicit live-depth controls; no exchange, storage or trading imports."""

from copy import deepcopy
from datetime import UTC, datetime
from uuid import uuid4

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.domain.models import is_valid_symbol

PATH = "/api/depth"
SUFFIX = ":binance_depth_ws_v1"
_STATES = {
    "queued",
    "connecting",
    "synced",
    "backoff",
    "blocked",
    "unknown",
    "stopped",
    "stopping_http",
}


def _instrument(value):
    parts = value.split(":") if type(value) is str else ()
    return (
        len(parts) == 3
        and parts[0] == "binance"
        and parts[1] in {"spot", "usdm"}
        and is_valid_symbol(parts[2])
    )


def _snapshot(value):
    if (
        type(value) is not dict
        or type(value.get("core_epoch")) is not str
        or not 1 <= len(value["core_epoch"]) <= 128
        or type(value.get("generation")) is not int
        or not 0 <= value["generation"] <= 2**63 - 1
        or any(type(value.get(key)) is not bool for key in ("running", "closed"))
        or type(value.get("instruments")) is not list
        or len(value["instruments"]) > 8
        or type(value.get("close_errors")) is not list
        or len(value["close_errors"]) > 24
        or any(type(item) is not str or len(item) > 160 for item in value["close_errors"])
        or type(value.get("limitation")) is not str
        or len(value["limitation"]) > 1024
    ):
        raise ValueError("invalid depth state")
    rows, seen = [], set()
    for row in value["instruments"]:
        if (
            type(row) is not dict
            or not {"instrument", "state", "source", "cursor_ms", "quality", "reason"} <= row.keys()
            or not _instrument(row.get("instrument"))
            or row["instrument"] in seen
            or row.get("state") not in _STATES
            or row.get("source") != row["instrument"] + SUFFIX
            or row.get("quality") not in {None, "observed", "partial", "missing"}
            or row.get("reason") is not None
            and (type(row["reason"]) is not str or len(row["reason"]) > 160)
            or row.get("cursor_ms") is not None
            and (
                type(row["cursor_ms"]) is not int
                or not 0 <= row["cursor_ms"] <= 253_402_300_799_000
                or row["cursor_ms"] % 60000
            )
        ):
            raise ValueError("invalid depth instrument status")
        seen.add(row["instrument"])
        rows.append(
            {
                key: row.get(key)
                for key in ("instrument", "state", "source", "cursor_ms", "quality", "reason")
            }
        )
    return {
        "core_epoch": value["core_epoch"],
        "generation": value["generation"],
        "running": value["running"],
        "closed": value["closed"],
        "instruments": rows,
        "close_errors": list(value["close_errors"]),
        "limitation": value["limitation"],
    }


class DepthPanel(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.attached, self.capable = bool(client.attached), False
        self.instrument = self.epoch = self.current = None
        self.draft, self._requests = [], {}
        self.dirty, self.requires_reload = False, False
        self._pending, self._notice = None, None
        self._serial, self._accepted = 0, 0
        self.setCheckable(True)
        self.setChecked(False)
        root = QVBoxLayout(self)
        self.content = QWidget()
        root.addWidget(self.content)
        layout = QVBoxLayout(self.content)
        self.hint, self.target, self.status = (QLabel() for _ in range(3))
        for label in (self.hint, self.target, self.status):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
            label.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
            layout.addWidget(label)
        controls = QGridLayout()
        self.price_precision, self.quote_precision = QSpinBox(), QSpinBox()
        for editor in (self.price_precision, self.quote_precision):
            editor.setRange(0, 18)
            editor.setValue(8)
        self.price_label, self.quote_label = QLabel(), QLabel()
        controls.addWidget(self.price_label, 0, 0)
        controls.addWidget(self.price_precision, 0, 1)
        controls.addWidget(self.quote_label, 1, 0)
        controls.addWidget(self.quote_precision, 1, 1)
        self.assumptions, self.resume = QCheckBox(), QCheckBox()
        controls.addWidget(self.assumptions, 2, 0, 1, 2)
        controls.addWidget(self.resume, 3, 0, 1, 2)
        layout.addLayout(controls)
        buttons = QGridLayout()
        (
            self.add_button,
            self.remove_button,
            self.start_button,
            self.stop_button,
            self.refresh_button,
            self.reload_button,
        ) = (QPushButton() for _ in range(6))
        for index, button in enumerate(
            (
                self.add_button,
                self.remove_button,
                self.start_button,
                self.stop_button,
                self.refresh_button,
                self.reload_button,
            )
        ):
            buttons.addWidget(button, index // 2, index % 2)
        layout.addLayout(buttons)
        self.draft_table = QTableWidget(0, 3)
        self.table = QTableWidget(0, 4)
        for table in (self.draft_table, self.table):
            table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
            table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
            table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
            table.horizontalHeader().setStretchLastSection(True)
            table.verticalHeader().hide()
            table.setMinimumHeight(76)
            table.setMaximumHeight(132)
            layout.addWidget(table)
        layout.addStretch(1)
        self.toggled.connect(self._expanded)
        self.assumptions.toggled.connect(self._controls)
        self.resume.toggled.connect(self._controls)
        self.draft_table.itemSelectionChanged.connect(self._controls)
        for button, callback in (
            (self.add_button, self.add_selected),
            (self.remove_button, self.remove_selected),
            (self.start_button, self.start),
            (self.stop_button, self.stop),
            (self.refresh_button, self.refresh),
            (self.reload_button, self.reload_current),
        ):
            button.clicked.connect(callback)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.timer = QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.content.hide()
        self.set_presentation("light", "ko")

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_presentation(self, theme, language):
        self.language = language if language in {"ko", "en"} else "ko"
        self.setTitle(self.text("호가 실시간 수집 · 선택 기능", "Live depth collection · optional"))
        self.hint.setText(
            self.text(
                "관측 시작 이후의 최신 호가만 저장하며 과거 호가를 채우지 않습니다. "
                "±5% 범위·연결이 불완전하면 partial입니다. 수집 실행과 분석 준비는 다릅니다. "
                "원문/기록 한도가 있으며 일반 거래의 정지 설정과 별개입니다.",
                "Stores latest live snapshots, without historical depth fill. "
                "Incomplete ±5%/connection coverage is partial. Running differs from readiness. "
                "Raw/journal quotas apply separately from general trading controls.",
            )
        )
        self.price_label.setText(self.text("가격 소수 자릿수", "Price decimal places"))
        self.quote_label.setText(self.text("금액 소수 자릿수", "Notional decimal places"))
        self.assumptions.setText(self.text("수동 정밀도 가정 확인", "Confirm assumed precision"))
        self.assumptions.setToolTip(
            self.text(
                "거래소에서 확인한 메타데이터가 아닙니다. 입력값의 소수 자릿수를 직접 확인하세요.",
                "These values are manual assumptions, not verified exchange metadata.",
            )
        )
        self.resume.setText(
            self.text("미확인 기록 대조 후 명시 재개", "Resume fixed pending records")
        )
        for button, ko, en in (
            (self.add_button, "선택 종목을 초안에 추가", "Add selected to draft"),
            (self.remove_button, "선택 초안 제거", "Remove draft row"),
            (self.start_button, "초안으로 수집 시작", "Start draft collection"),
            (self.stop_button, "호가 수집 중지 요청", "Request depth stop"),
            (self.refresh_button, "요청·현재 상태 조회", "Read request/current state"),
            (self.reload_button, "현재 코어 상태 수동 불러오기", "Manually reload current core"),
        ):
            button.setText(self.text(ko, en))
        self.draft_table.setHorizontalHeaderLabels(
            self.text(
                ["종목 초안", "가격 정밀도(가정)", "금액 정밀도(가정)"],
                ["Draft instrument", "Price precision (assumed)", "Notional precision (assumed)"],
            )
        )
        self.table.setHorizontalHeaderLabels(
            self.text(
                ["수집 종목", "실행 / 관측 품질", "저장 끝(UTC)", "상태 설명"],
                ["Instrument", "Worker / observed quality", "Committed end (UTC)", "Reason"],
            )
        )
        self._render()

    def _expanded(self, checked):
        self.content.setVisible(checked)
        if checked:
            self.refresh()

    def _ready(self):
        return self.attached and self.capable and self.epoch is not None

    def _controls(self):
        ready = self._ready()
        mutate = (
            ready
            and self.current is not None
            and not self.current["closed"]
            and not self.requires_reload
            and self._pending is None
            and not self.current["close_errors"]
        )
        running = bool(self.current and self.current["running"])
        self.add_button.setEnabled(
            _instrument(self.instrument)
            and self.assumptions.isChecked()
            and self._pending is None
            and len(self.draft) < 8
        )
        self.remove_button.setEnabled(
            self._pending is None and len(self.draft_table.selectionModel().selectedRows()) == 1
        )
        self.start_button.setEnabled(
            mutate and not running and bool(self.draft) and self.assumptions.isChecked()
        )
        self.stop_button.setEnabled(mutate and running)
        self.refresh_button.setEnabled(ready and not self.requires_reload)
        self.reload_button.setEnabled(
            ready and not any(item[2] == "post" for item in self._requests.values())
        )
        self.resume.setEnabled(self._pending is None and not running)

    def set_instrument(self, value):
        selected = value if _instrument(value) else None
        if selected != self.instrument:
            self.assumptions.setChecked(False)
        self.instrument = selected
        self._render()

    def set_attached(self, attached):
        self.attached = bool(attached)
        if not self.attached:
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
            self.requires_reload = True
        self._render()

    def update_state(self, state):
        core = state.get("core", {}) if type(state) is dict else {}
        epoch = core.get("epoch")
        self.capable = core.get("capabilities", {}).get("depth_collection") is True
        if type(epoch) is str and epoch and epoch != self.epoch:
            changed = self.epoch is not None
            self.epoch, self.current = epoch, None
            self._requests.clear()
            self._accepted = 0
            if changed:
                self.requires_reload = True
                self._notice = self.text(
                    "새 코어 · 초안 유지, 수동 상태 확인 전 변경 차단",
                    "New core · draft retained; manually reload before changes",
                )
            elif self.isChecked():
                self.refresh()
        self._render()

    def _render(self):
        self.target.setText(
            self.text("선택 종목: ", "Selected instrument: ") + (self.instrument or "—")
        )
        selected = self.draft_table.currentRow()
        self.draft_table.setRowCount(len(self.draft))
        for index, row in enumerate(self.draft):
            current = self.draft_table.item(index, 0)
            same = current is not None and current.text() == row["instrument"]
            if not same:
                self.draft_table.setItem(index, 0, QTableWidgetItem(row["instrument"]))
            for column, key in ((1, "price_precision"), (2, "quote_precision")):
                editor = self.draft_table.cellWidget(index, column) if same else None
                if editor is None:
                    editor = QSpinBox()
                    editor.setRange(0, 18)
                    editor.setValue(row[key])
                    editor.valueChanged.connect(
                        lambda value, identity=row["instrument"], field=key: self._edit(
                            identity, field, value
                        )
                    )
                    self.draft_table.setCellWidget(index, column, editor)
                elif editor.value() != row[key] and not editor.hasFocus():
                    editor.blockSignals(True)
                    editor.setValue(row[key])
                    editor.blockSignals(False)
                editor.setEnabled(self._pending is None)
        if 0 <= selected < len(self.draft):
            self.draft_table.selectRow(selected)
        rows = [] if self.current is None else self.current["instruments"]
        self.table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            stamp = (
                "—"
                if row["cursor_ms"] is None
                else datetime.fromtimestamp(row["cursor_ms"] / 1000, UTC).strftime("%m-%d %H:%M:%S")
            )
            for column, value in enumerate(
                (
                    row["instrument"],
                    row["state"] + " / " + (row["quality"] or "—"),
                    stamp,
                    row["reason"] or "—",
                )
            ):
                item = QTableWidgetItem(value)
                item.setToolTip(row["source"] if column == 0 else value)
                self.table.setItem(index, column, item)
        for table in (self.draft_table, self.table):
            height = (
                table.horizontalHeader().height()
                + min(3, max(1, table.rowCount())) * table.verticalHeader().defaultSectionSize()
                + table.frameWidth() * 2
                + 12
            )
            table.setFixedHeight(min(132, max(76, height)))
        if not self.attached:
            message = self.text(
                "코어 연결 끊김 · 표시 상태가 오래됐을 수 있습니다",
                "Disconnected · displayed state may be stale",
            )
        elif not self.capable:
            message = self.text(
                "이 코어의 호가 수집 기능 미지원", "Depth collection unavailable in this core"
            )
        elif self.requires_reload:
            message = self.text(
                "수동으로 현재 코어 상태를 불러오세요. 이전 요청 성공은 추정하지 않습니다.",
                "Manually reload this core. Prior request success is not inferred.",
            )
        elif self.current and self.current["close_errors"]:
            message = self.text(
                "자원 종료 미확인 · 변경 차단", "Resource shutdown unconfirmed · changes blocked"
            )
        elif self._pending:
            message = (
                self.text(
                    "요청 미확인 · 같은 ID 조회만 수행하며 자동 재전송하지 않습니다.",
                    "Request unconfirmed · only query the same ID; never repost automatically.",
                )
                if self._pending["unknown"]
                else self.text(
                    "요청 접수 확인 · 실제 실행/정지 상태 확인 중",
                    "Request accepted · confirming actual start/stop state",
                )
                if self._pending["ack"]
                else self.text("요청 응답 대기", "Waiting for request acknowledgement")
            )
        elif self.current:
            message = (
                self.text(
                    "실행 중 · 관측 품질은 표에서 별도 확인",
                    "Running · inspect observation quality separately",
                )
                if self.current["running"]
                else self.text("수집 worker 정지 확인", "Collection worker stopped")
            )
            if self.current["close_errors"]:
                message = self.text(
                    "자원 종료 미확인 · 변경 차단",
                    "Resource shutdown unconfirmed · changes blocked",
                )
        else:
            message = self.text("호가 수집 상태 미수신", "Depth state not received")
        self.status.setText(
            message
            if not self.attached
            or not self.capable
            or self.requires_reload
            or self.current
            and self.current["close_errors"]
            else self._notice or message
        )
        self._controls()

    def _edit(self, instrument, key, value):
        for row in self.draft:
            if row["instrument"] == instrument:
                row[key] = value
                self.dirty = True

    def add_selected(self):
        if not self.add_button.isEnabled():
            return
        row = {
            "instrument": self.instrument,
            "price_precision": self.price_precision.value(),
            "quote_precision": self.quote_precision.value(),
        }
        self.draft = [item for item in self.draft if item["instrument"] != self.instrument] + [row]
        self.dirty = True
        self._render()

    def remove_selected(self):
        if self.remove_button.isEnabled():
            del self.draft[self.draft_table.currentRow()]
            self.dirty = True
            self._render()

    def _request(self, method, suffix="", payload=None, *, kind):
        self._serial += 1
        tag = f"depth:{id(self)}:{self._serial}"
        self._requests[tag] = (self.epoch, self._serial, kind)
        self.client.request(method, PATH + suffix, payload, tag=tag)
        self._controls()

    def refresh(self):
        if (
            not self._ready()
            or self.requires_reload
            or any(
                item[2] in {"lookup", "snapshot", "reload", "post"}
                for item in self._requests.values()
            )
        ):
            return
        if self._pending and self._pending["unknown"]:
            self._request("GET", "/" + self._pending["id"], kind="lookup")
        else:
            self._request("GET", kind="snapshot")

    def reload_current(self):
        if self._ready() and not any(
            item[2] in {"reload", "post"} for item in self._requests.values()
        ):
            self._request("GET", kind="reload")

    def _tick(self):
        if self.isVisible() and self.isChecked():
            self.refresh()

    def _post(self, kind, parameters):
        identity = uuid4().hex
        payload = {
            "id": identity,
            "core_epoch": self.epoch,
            "expected_generation": self.current["generation"],
            "kind": kind,
            "parameters": parameters,
        }
        self._pending = {
            "id": identity,
            "kind": kind,
            "generation": self.current["generation"],
            "payload": deepcopy(payload),
            "ack": False,
            "unknown": False,
            "accepted_generation": None,
        }
        self._notice = None
        self._render()
        self._request("POST", payload=payload, kind="post")

    def start(self):
        if self.start_button.isEnabled():
            self._post(
                "start", {"subscriptions": deepcopy(self.draft), "resume": self.resume.isChecked()}
            )

    def stop(self):
        if self.stop_button.isEnabled():
            self._post("stop", {})

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch or not self.attached:
            return
        try:
            if request[2] in {"post", "lookup"}:
                pending = self._pending
                if pending is None:
                    return
                expected = pending["generation"] + (1 if pending["kind"] == "start" else 0)
                if (
                    type(value) is not dict
                    or value.get("id") != pending["id"]
                    or value.get("core_epoch") != self.epoch
                    or value.get("kind") != pending["kind"]
                    or value.get("state") != "requested"
                    or type(value.get("generation")) is not int
                    or value["generation"] != expected
                ):
                    raise ValueError("invalid depth request acknowledgement")
                pending.update(ack=True, unknown=False, accepted_generation=expected)
                self._render()
                self.refresh()
                return
            current = _snapshot(value)
            if (
                current["core_epoch"] != self.epoch
                or request[1] < self._accepted
                or self.current is not None
                and current["generation"] < self.current["generation"]
            ):
                raise ValueError("stale depth snapshot")
            if (
                request[2] != "reload"
                and self._pending
                and self._pending["ack"]
                and self._pending["kind"] == "start"
                and current["generation"] == self._pending["accepted_generation"]
            ):
                expected = {
                    row["instrument"]
                    for row in self._pending["payload"]["parameters"]["subscriptions"]
                }
                if {row["instrument"] for row in current["instruments"]} != expected:
                    raise ValueError("depth request differs from actual subscriptions")
            self.current, self._accepted = current, request[1]
            self._notice = None
            if request[2] == "reload":
                self.requires_reload, self._pending = False, None
                self._notice = self.text(
                    "현재 상태 확인 · 이전 요청 성공을 확정하지 않고 새 명령을 준비합니다",
                    "Current state read · preparing new commands without confirming prior success",
                )
            elif self._pending and self._pending["ack"]:
                pending = self._pending
                if current["generation"] > pending["accepted_generation"]:
                    self.requires_reload = True
                elif current["generation"] == pending["accepted_generation"]:
                    if pending["kind"] == "start":
                        self._pending = None
                        self._notice = (
                            self.text(
                                "시작 요청 확인 · worker가 실행 중입니다 (분석 준비와 별개)",
                                "Start confirmed · running (analysis readiness is separate)",
                            )
                            if current["running"]
                            else self.text(
                                "시작 요청은 확인됐으나 worker가 종료됐습니다. 원인을 확인하세요.",
                                "Start request is known, but the worker ended; inspect its reason.",
                            )
                        )
                    elif not current["running"] and not current["close_errors"]:
                        self._pending = None
                        self._notice = self.text(
                            "호가 수집 worker 중지 확인", "Depth collection worker stop confirmed"
                        )
            self._render()
        except (ValueError, TypeError, KeyError, OverflowError):
            if self._pending:
                self._pending["unknown"] = True
            self._notice = self.text(
                "잘못되거나 오래된 응답 · 초안과 미확인 요청 유지",
                "Invalid/stale response · draft and unconfirmed request retained",
            )
            self._render()

    def _failed(self, tag, reason):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch:
            return
        if self._pending:
            self._pending["unknown"] = True
        self._notice = self.text(
            "상태/요청 미확인 · 같은 ID 조회만 하며 재전송하지 않습니다",
            "State/request unconfirmed · query the same ID without reposting",
        )
        self._render()
