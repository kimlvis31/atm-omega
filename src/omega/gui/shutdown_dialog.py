"""Explicit core-exit choices; closing this client never stops its core."""

# Imports
import re
from copy import deepcopy
from uuid import uuid4

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

PATH = "/api/shutdown"
KINDS = {"review", "save_exit", "flatten_exit", "cancel"}
STATES = {
    "starting",
    "idle",
    "reviewing",
    "pausing",
    "draining",
    "blocked",
    "review_required",
    "exit_requested",
    "canceled",
    "interrupted",
    "failed",
    "closed",
}
OP_STATES = {
    "queued",
    "running",
    "waiting_fill",
    "blocked",
    "review_required",
    "exit_requested",
    "completed",
    "canceled",
    "interrupted",
    "failed",
}
ACTIVE = {"queued", "running", "waiting_fill"}


def _identity(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _summary(value):
    if (
        type(value) is not dict
        or any(
            type(value.get(key)) is not int or not 0 <= value[key] <= 100000
            for key in ("positions_count", "pending_count", "unknown_count")
        )
        or any(type(value.get(key)) is not bool for key in ("durable", "supports_flatten"))
        or type(value.get("notes")) is not list
        or len(value["notes"]) > 100
        or any(type(note) is not str or len(note) > 2048 for note in value["notes"])
    ):
        raise ValueError("종료 상태를 확인할 수 없습니다 / Invalid exit summary")
    return deepcopy(value)


def _operation(value):
    if (
        type(value) is not dict
        or not _identity(value.get("id"))
        or value.get("kind") not in KINDS
        or value.get("state") not in OP_STATES
        or type(value.get("parameters")) is not dict
        or type(value.get("expected_revision")) is not int
        or value["expected_revision"] < 0
        or type(value.get("operation_core_epoch", value.get("core_epoch"))) is not str
    ):
        raise ValueError("종료 요청 기록을 확인할 수 없습니다 / Invalid exit receipt")
    params = value["parameters"]
    if (
        value["kind"] == "save_exit"
        and (
            set(params) != {"acknowledge_unmanaged_positions"}
            or type(params["acknowledge_unmanaged_positions"]) is not bool
        )
        or value["kind"] != "save_exit"
        and params != {}
    ):
        raise ValueError("Invalid shutdown request parameters")
    if value.get("summary") is not None:
        _summary(value["summary"])
    return deepcopy(value)


class ShutdownDialog(QDialog):
    def __init__(self, client, state, *, language="ko", parent=None):
        super().__init__(parent)
        self.client, self.language = client, language
        self.epoch = self.snapshot = None
        self.capable, self.attached = False, bool(client.attached)
        self._serial, self._requests = 0, {}
        self._pending, self._watch_id, self._watch_row = None, None, None
        self._fresh, self._initial_review = False, True
        self._explicit_review = False
        self.setWindowTitle(self.text("코어 종료", "Stop core"))
        self.resize(760, 720)
        outer = QVBoxLayout(self)
        scroll, body = QScrollArea(), QWidget()
        scroll.setWidgetResizable(True)
        scroll.setWidget(body)
        outer.addWidget(scroll)
        layout = QVBoxLayout(body)
        layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.warning = self.label(
            self.text(
                "GUI를 닫는 것과 코어를 종료하는 것은 다릅니다. "
                "아래 선택은 코어 전체에 적용됩니다.",
                "Closing the GUI and stopping the core are separate. "
                "These choices affect the entire core.",
            )
        )
        layout.addWidget(self.warning)
        self.status, self.counts, self.notes, self.outcome = (self.label() for _ in range(4))
        for label in (self.status, self.counts, self.notes):
            layout.addWidget(label)
        self.participants = QTableWidget(0, 4)
        self.participants.setHorizontalHeaderLabels(
            [
                self.text("담당", "Owner"),
                self.text("확인", "Ready"),
                self.text("기록", "Durable"),
                self.text("정리 지원", "Can flatten"),
            ]
        )
        self.participants.setEditTriggers(QTableWidget.EditTrigger.NoEditTriggers)
        self.participants.setMaximumHeight(135)
        self.participants.horizontalHeader().setStretchLastSection(True)
        layout.addWidget(self.participants)
        self.acknowledge = QCheckBox(
            self.text(
                "포지션·주문이 미관리 상태로 남을 수 있음에 동의합니다",
                "I acknowledge positions/orders may be unmanaged",
            )
        )
        layout.addWidget(self.acknowledge)
        self.acknowledge.toggled.connect(self.controls)
        self.save_button = self.button(
            self.text("상태를 저장하고 종료", "Save state and exit"), self.save_exit
        )
        self.cancel_button = self.button(
            self.text("종료 취소", "Cancel core shutdown"), lambda: self.submit("cancel", {})
        )
        self.flatten_button = self.button(
            self.text(
                "포지션·대기 주문을 정리하고 종료", "Close positions and pending orders, then exit"
            ),
            lambda: self.submit("flatten_exit", {}),
        )
        for button in (self.save_button, self.cancel_button, self.flatten_button):
            layout.addWidget(button)
        layout.addWidget(
            self.label(
                self.text(
                    "정리 요청 접수는 종료 완료가 아닙니다. "
                    "체결을 기다리는 동안 코어·데이터 공급은 계속됩니다. "
                    "종료 취소는 이미 제출된 정리 주문을 되돌리거나 "
                    "신규 진입을 자동 재개하지 않습니다. "
                    "증거금 v2에 포지션·주문이 있으면 이번 버전의 전체 정리는 지원되지 않습니다.",
                    "Acceptance is not confirmed exit. "
                    "The core and feed continue while fills are pending. "
                    "Cancel preserves submitted reductions and never resumes entries "
                    "automatically. "
                    "This version cannot flatten active margin-v2 positions/orders.",
                )
            )
        )
        lookup = QHBoxLayout()
        self.operation_id = QLineEdit()
        self.operation_id.setMaxLength(128)
        self.operation_id.setPlaceholderText(
            self.text("현재 또는 이전 종료 요청 번호", "Current or previous exit request ID")
        )
        self.query_button = self.button(self.text("요청 조회", "Read request"), self.query)
        lookup.addWidget(self.operation_id, 1)
        lookup.addWidget(self.query_button)
        layout.addLayout(lookup)
        layout.addWidget(self.outcome)
        bottom = QHBoxLayout()
        self.refresh_button = self.button(
            self.text("포지션 다시 확인", "Review current positions"), self.review_now
        )
        self.close_button = self.button(
            self.text("이 창만 닫기", "Close this dialog only"), self.reject
        )
        bottom.addWidget(self.refresh_button)
        bottom.addWidget(self.close_button)
        layout.addLayout(bottom)
        layout.addStretch(1)
        self.update_state(state)
        client.resultReceived.connect(self.received)
        client.failed.connect(self.failed)
        client.connectionChanged.connect(self.set_attached)
        client.stateReceived.connect(self.update_state)
        self.timer = QTimer(self)
        self.timer.setInterval(1000)
        self.timer.timeout.connect(self.poll)
        self.timer.start()
        self.controls()
        self.refresh()

    def text(self, korean, english):
        return korean if self.language == "ko" else english

    @staticmethod
    def label(text=""):
        label = QLabel(text)
        label.setTextFormat(Qt.TextFormat.PlainText)
        label.setWordWrap(True)
        label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        return label

    def button(self, text, callback):
        button = QPushButton(text)
        button.clicked.connect(callback)
        return button

    def ready(self):
        return self.attached and self.capable and bool(self.epoch)

    def update_state(self, state):
        core = state.get("core", {})
        epoch = core.get("epoch")
        if type(epoch) is str and epoch and epoch != self.epoch:
            if self.epoch is not None:
                self._initial_review = False
                self.outcome.setText(
                    self.text(
                        "코어가 재시작됐습니다. 이전 요청을 재전송하지 말고 번호로 조회하세요.",
                        "The core restarted. Look up the old request ID; never resend it.",
                    )
                )
            self.epoch, self.snapshot, self._fresh = epoch, None, False
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
            self.acknowledge.setChecked(False)
        self.capable = core.get("capabilities", {}).get("shutdown_management") is True
        if hasattr(self, "save_button"):
            self.controls()

    def set_attached(self, value):
        self.attached = bool(value)
        if not self.attached:
            self._requests.clear()
            self._fresh = False
            if self._pending:
                self._pending["unknown"] = True
            self.status.setText(
                self.text(
                    "코어 연결이 끊겼습니다. 종료 완료 여부는 확인되지 않았습니다.",
                    "Core disconnected. Completed shutdown has not been confirmed.",
                )
            )
        self.controls()

    def controls(self):
        known = (
            self.ready()
            and self._fresh
            and self.snapshot is not None
            and self.snapshot.get("ready", True) is True
            and self.snapshot.get("closed", False) is False
        )
        summary = self.snapshot["summary"] if self.snapshot else None
        busy = (
            self._watch_row is not None
            and self._watch_row["kind"] == "review"
            and self._watch_row["state"] in ACTIVE
        )
        changing = {"starting", "reviewing", "pausing", "draining", "exit_requested", "closed"}
        can_choose = (
            known
            and not busy
            and self._pending is None
            and self.snapshot["state"] not in changing
            and summary["durable"]
            and summary["unknown_count"] == 0
        )
        exposed = summary is not None and summary["positions_count"] + summary["pending_count"] > 0
        self.save_button.setEnabled(can_choose and (not exposed or self.acknowledge.isChecked()))
        self.flatten_button.setEnabled(can_choose and summary["supports_flatten"])
        self.cancel_button.setEnabled(
            known
            and self._pending is None
            and self.snapshot["state"] not in {"starting", "exit_requested", "closed"}
        )
        self.acknowledge.setEnabled(bool(exposed))
        self.refresh_button.setEnabled(self.ready() and self._pending is None and not busy)
        self.query_button.setEnabled(self.ready())
        if not self.capable:
            self.status.setText(
                self.text(
                    "코어 종료 관리 기능이 연결되지 않았습니다",
                    "Core shutdown management is unavailable",
                )
            )

    def request(self, method, path, payload=None, *, context):
        if not self.ready() or any(row[1] == context for row in self._requests.values()):
            return
        self._serial += 1
        tag = f"shutdown:{id(self)}:{self._serial}"
        self._requests[tag] = self.epoch, context
        try:
            self.client.request(method, path, payload, tag=tag)
        except Exception as error:
            self.failed(tag, type(error).__name__)

    def refresh(self):
        self.request("GET", PATH, context=("snapshot", None))

    def review_now(self):
        if self.refresh_button.isEnabled():
            self._initial_review, self._explicit_review = True, True
            self.refresh()

    def query(self):
        identity = self.operation_id.text().strip()
        if _identity(identity):
            self.request("GET", PATH + "/" + identity, context=("operation", identity))
        else:
            self.outcome.setText(self.text("요청 번호를 확인하세요", "Check the request ID"))

    def submit(self, kind, parameters):
        buttons = {
            "save_exit": self.save_button,
            "flatten_exit": self.flatten_button,
            "cancel": self.cancel_button,
        }
        if (
            not self.ready()
            or self.snapshot is None
            or self._pending is not None
            or kind in buttons
            and not buttons[kind].isEnabled()
        ):
            return
        identity = uuid4().hex
        payload = {
            "id": identity,
            "core_epoch": self.epoch,
            "expected_revision": self.snapshot["revision"],
            "kind": kind,
            "parameters": parameters,
        }
        self._pending = {"payload": deepcopy(payload), "unknown": False}
        self._watch_id, self._watch_row, self._fresh = identity, None, False
        self.operation_id.setText(identity)
        self.outcome.setText(
            self.text(
                "요청 접수 확인 중 · 아직 종료된 것이 아닙니다",
                "Confirming acceptance; the core has not exited",
            )
        )
        self.controls()
        self.request("POST", PATH, payload, context=("submit", identity))

    def save_exit(self):
        self.submit("save_exit", {"acknowledge_unmanaged_positions": self.acknowledge.isChecked()})

    def poll(self):
        if not self.isVisible() or not self.ready():
            return
        if self._pending:
            identity = self._pending["payload"]["id"]
            self.request("GET", PATH + "/" + identity, context=("operation", identity))
        elif self._watch_id and (
            self._watch_row is None or self._watch_row["state"] in ACTIVE | {"exit_requested"}
        ):
            self.request("GET", PATH + "/" + self._watch_id, context=("operation", self._watch_id))
        self.refresh()

    def accept_snapshot(self, value):
        if (
            type(value) is not dict
            or type(value.get("schema_version")) is not int
            or value["schema_version"] != 1
            or type(value.get("revision")) is not int
            or value["revision"] < 0
            or value.get("state") not in STATES
            or "operation_id" not in value
            or value.get("operation_id") is not None
            and not _identity(value["operation_id"])
            or type(value.get("participants")) is not list
            or len(value["participants"]) > 20
            or any(type(row) is not dict for row in value["participants"])
            or type(value.get("operations")) is not list
            or len(value["operations"]) > 128
        ):
            raise ValueError("Invalid shutdown snapshot")
        summary = _summary(value.get("summary"))
        for operation in value["operations"]:
            _operation(operation)
        if self.snapshot is not None and value["revision"] < self.snapshot["revision"]:
            return
        signature = (
            summary["positions_count"],
            summary["pending_count"],
            summary["unknown_count"],
            [
                {key: row.get(key) for key in ("owner", "positions", "pending_orders", "unknown")}
                for row in value["participants"]
            ],
        )
        if getattr(self, "_exposure_signature", signature) != signature:
            self.acknowledge.setChecked(False)
        self._exposure_signature = deepcopy(signature)
        self.snapshot, self._fresh = deepcopy(value), True
        self.counts.setText(
            self.text(
                f"열린 포지션 {summary['positions_count']} · "
                f"대기 주문 {summary['pending_count']} · 미확인 {summary['unknown_count']}",
                f"Open positions {summary['positions_count']} · "
                f"Pending orders {summary['pending_count']} · Unknown {summary['unknown_count']}",
            )
        )
        self.notes.setText(
            "\n".join(summary["notes"])
            + (
                self.text(
                    "\n현재 구성은 전체 정리를 지원하지 않습니다. "
                    "상태 저장 또는 종료 취소를 선택하세요.",
                    "\nThis configuration cannot flatten everything. "
                    "Choose save state or cancel shutdown.",
                )
                if not summary["supports_flatten"]
                else ""
            )
        )
        self.participants.setRowCount(len(value["participants"]))
        self.participants.setVisible(bool(value["participants"]))
        self.participants.setFixedHeight(min(135, 30 + 28 * len(value["participants"])))
        for index, row in enumerate(value["participants"]):
            for column, key in enumerate(("owner", "ready", "durable", "supports_flatten")):
                field = row.get(key, "?")
                if type(field) is bool:
                    field = self.text("예", "Yes") if field else self.text("아니오", "No")
                elif key == "owner":
                    field = {
                        "paper_v1": self.text("가상 거래 v1", "Paper v1"),
                        "margin_v2": self.text("가상 증거금 v2", "Virtual margin v2"),
                    }.get(field, field)
                self.participants.setItem(index, column, QTableWidgetItem(str(field)[:160]))
        states = {
            "idle": ("종료 방법을 선택하세요", "Choose how to stop"),
            "draining": ("정리 주문 체결을 기다리고 있습니다", "Waiting for reduction fills"),
            "exit_requested": (
                "종료 요청이 접수됐습니다. 실제 종료 완료는 아직 확인되지 않았습니다",
                "Exit requested; actual completed exit is not yet confirmed",
            ),
        }
        self.status.setText(
            self.text(
                *states.get(
                    value["state"],
                    ("코어 종료 상태: " + value["state"], "Core exit state: " + value["state"]),
                )
            )
        )
        if not self.operation_id.text() and value["operation_id"]:
            self.operation_id.setText(value["operation_id"])
            self._watch_id = value["operation_id"]

    def accept_operation(self, value, identity):
        row = _operation(value)
        if row["id"] != identity:
            raise ValueError("Shutdown receipt identity changed")
        if self._pending and identity == self._pending["payload"]["id"]:
            submitted = self._pending["payload"]
            if (
                row["kind"] != submitted["kind"]
                or row["parameters"] != submitted["parameters"]
                or row["expected_revision"] != submitted["expected_revision"]
                or row.get("operation_core_epoch", row["core_epoch"]) != submitted["core_epoch"]
            ):
                raise ValueError("Shutdown receipt differs from its frozen request")
            self._pending = None
            self._fresh = False
        if identity == self._watch_id:
            self._watch_row = row
        origin = row.get("operation_core_epoch", row.get("core_epoch"))
        confirmed = (
            row["state"] == "completed"
            and type(row.get("result")) is dict
            and row["result"].get("core_exit_confirmed") is True
        )
        if confirmed and origin != self.epoch:
            message = self.text(
                "이전 코어의 종료가 저장 기록으로 확인됐습니다",
                "The previous core exit is confirmed by its saved receipt",
            )
        elif row["state"] == "exit_requested":
            message = self.text(
                "종료 요청 접수 · 실제 종료 완료 미확인",
                "Exit requested; completed exit unconfirmed",
            )
        else:
            labels = {
                "queued": ("요청이 접수됐습니다", "Request accepted"),
                "running": ("요청을 처리하고 있습니다", "Request processing"),
                "waiting_fill": ("정리 주문 체결 대기", "Waiting for reduction fills"),
                "completed": (
                    "요청 처리가 끝났습니다. 코어 종료 확인은 별도입니다",
                    "Request processed; core exit confirmation is separate",
                ),
                "canceled": (
                    "종료가 취소됐습니다. 신규 진입은 자동 재개하지 않습니다",
                    "Shutdown canceled; entries never resume automatically",
                ),
                "blocked": (
                    "종료 조건을 확인할 수 없어 중단됐습니다",
                    "Stopped because exit conditions cannot be confirmed",
                ),
                "review_required": (
                    "바뀐 포지션·주문을 확인하고 다시 선택하세요",
                    "Review changed positions/orders and choose again",
                ),
                "interrupted": (
                    "이전 요청은 재시작으로 중단됐습니다",
                    "The prior request was interrupted by restart",
                ),
                "failed": (
                    "요청을 처리하지 못했습니다. 현재 상태를 다시 확인하세요",
                    "Request failed; review current state",
                ),
            }
            message = self.text(*labels[row["state"]])
        self.outcome.setText(message)
        self.refresh()

    def received(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch or not self.ready():
            return
        try:
            if type(value) is not dict or value.get("core_epoch") != self.epoch:
                raise ValueError("Shutdown response belongs to another core")
            if request[1][0] == "snapshot":
                self.accept_snapshot(value)
                if (
                    self._initial_review
                    and self.snapshot.get("ready", True) is True
                    and not self.snapshot.get("closed", False)
                ):
                    self._initial_review = False
                    if self.snapshot["operation_id"] is None or self._explicit_review:
                        self._explicit_review = False
                        self.submit("review", {})
            else:
                self.accept_operation(value, request[1][1])
        except (ValueError, TypeError, KeyError) as error:
            if self._pending:
                self._pending["unknown"] = True
            self._fresh = False
            self.outcome.setText(str(error)[:300])
        self.controls()

    def failed(self, tag, _message):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch:
            return
        if self._pending:
            self._pending["unknown"] = True
        self._fresh = False
        self.outcome.setText(
            self.text(
                "결과를 확인하지 못했습니다. 같은 요청 번호만 조회합니다.",
                "Outcome unconfirmed. Only the same request ID will be queried.",
            )
        )
        self.controls()

    def closeEvent(self, event):
        self.timer.stop()
        self._requests.clear()
        super().closeEvent(event)

    def reject(self):
        self.timer.stop()
        self._requests.clear()
        super().reject()
