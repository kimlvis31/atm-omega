"""Explicit cold-start NN adoption through the owner client; no host imports."""

import re
import time
from copy import deepcopy
from uuid import uuid4

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QCheckBox, QGridLayout, QGroupBox, QLabel, QPushButton, QWidget

from omega.runtime.causal_adoption_client import (
    adoption_body,
    adoption_url,
    validate_adoption_snapshot,
)

MAX_POLLS = 32


def _identity(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _sha(value):
    return type(value) is str and re.fullmatch(r"[a-f0-9]{64}", value) is not None


class CausalAdoptionControls(QGroupBox):
    adoptionCompleted = Signal(object)

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch, self.snapshot, self.receipt = None, None, None
        self.attached, self.capable = bool(client.attached), False
        self._requests, self._target = {}, None
        self._received_at, self._serial, self._accepted_serial = None, 0, 0
        self._polls, self._last_poll, self._outcome = 0, 0.0, ""
        self.setCheckable(True)
        self.setChecked(False)
        layout = QGridLayout(self)
        self.content = QWidget()
        layout.addWidget(self.content)
        body = QGridLayout(self.content)
        self.hint, self.status, self.request_label = self._label(), self._label(), self._label()
        self.refresh_button, self.query_button = QPushButton(), QPushButton()
        self.confirm, self.adopt_button = QCheckBox(), QPushButton()
        body.addWidget(self.hint, 0, 0, 1, 2)
        body.addWidget(self.status, 1, 0, 1, 2)
        body.addWidget(self.request_label, 2, 0, 1, 2)
        body.addWidget(self.refresh_button, 3, 0)
        body.addWidget(self.query_button, 3, 1)
        body.addWidget(self.confirm, 4, 0, 1, 2)
        body.addWidget(self.adopt_button, 5, 0, 1, 2)
        self.toggled.connect(self._expanded)
        self.confirm.toggled.connect(self._controls)
        self.refresh_button.clicked.connect(self.refresh)
        self.query_button.clicked.connect(self.query)
        self.adopt_button.clicked.connect(self.adopt)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.content.hide()
        self.set_presentation(None, "ko")

    @staticmethod
    def _label():
        label = QLabel()
        label.setWordWrap(True)
        label.setTextFormat(Qt.TextFormat.PlainText)
        return label

    def _text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_presentation(self, _theme, language):
        self.language = language
        self.setTitle(self._text("중단 후 NN 작업 준비", "Prepare NN task after interruption"))
        self.hint.setText(
            self._text(
                "이전 실행의 준비 상태를 확인한 뒤 새 NN 작업을 수동 초기화 상태로 준비합니다. "
                "이전 요청의 미확인 결과는 이력으로 남습니다. "
                "추론·계정 거래를 자동 재개하지 않습니다.",
                "Verify the previous startup and prepare a new NN task for manual initialization. "
                "Unconfirmed previous requests remain in history. Inference and account trading "
                "do not resume automatically.",
            )
        )
        self.refresh_button.setText(self._text("준비 가능 상태 확인", "Check startup status"))
        self.query_button.setText(self._text("동일 요청 확인", "Check same request"))
        self.confirm.setText(
            self._text(
                "확인된 이전 준비 상태를 바탕으로 새 NN 작업을 준비하는 데 동의합니다",
                "I confirm preparing a new NN task from the verified previous startup",
            )
        )
        self.adopt_button.setText(self._text("새 NN 작업 준비", "Prepare new NN task"))
        self._controls()

    def update_state(self, state):
        core = state.get("core", {}) if type(state) is dict else {}
        epoch = core.get("epoch")
        if _identity(epoch) and epoch != self.epoch:
            self.epoch, self.snapshot, self.receipt, self._received_at = epoch, None, None, None
            self._requests.clear()
            self._accepted_serial = 0
            self.confirm.setChecked(False)
            if self._target:
                self._target["unknown"] = True
        self.capable = core.get("capabilities", {}).get("causal_inference") is True
        self._controls()

    def set_attached(self, attached):
        self.attached = bool(attached)
        if not self.attached:
            self._requests.clear()
            self._received_at = None
            if self._target:
                self._target["unknown"] = True
        self._controls()

    def _expanded(self, checked):
        self.content.setVisible(checked)
        self.confirm.setChecked(False)
        if checked:
            self.refresh()
        self._controls()

    def _connected(self):
        return bool(self.attached and self.capable and self.epoch)

    def _fresh(self):
        return bool(
            self.snapshot
            and self._received_at is not None
            and self.snapshot["core_epoch"] == self.epoch
            and time.monotonic() - self._received_at <= 10
        )

    def _writable(self):
        return bool(
            self.isChecked()
            and self.isVisible()
            and self._connected()
            and self._fresh()
            and self.snapshot["eligible"]
            and self.confirm.isChecked()
            and self._target is None
            and not self._requests
        )

    def _request(self, method, path, payload=None, *, context):
        self._serial += 1
        tag = f"causal-adoption:{self._serial}"
        self._requests[tag] = {
            "context": context,
            "epoch": self.epoch,
            "serial": self._serial,
            "started_at": time.monotonic(),
            "id": (payload or {}).get("id") if method == "POST" else path.rsplit("/", 1)[-1],
        }
        try:
            self.client.request(method, path, payload, tag=tag)
        except Exception as error:
            self._failed(tag, str(error))
        self._controls()

    def refresh(self, _checked=False):
        if self.isChecked() and self._connected() and not self._requests:
            self._request("GET", adoption_url(), context="snapshot")

    def adopt(self, _checked=False):
        if not self._writable():
            return
        identity = uuid4().hex
        try:
            payload = adoption_body(identity, self.snapshot)
        except (ValueError, TypeError, KeyError):
            self._unconfirmed()
            self._controls()
            return
        self._target = {"id": identity, "epoch": self.epoch, "unknown": False, "settled": False}
        self._polls, self.receipt, self._outcome = 0, None, ""
        self.confirm.setChecked(False)
        self._received_at = None
        self._request("POST", adoption_url(), payload, context="operation")

    def query(self, _checked=False, *, automatic=False):
        if (
            self.isChecked()
            and self._connected()
            and self._target
            and not self._requests
            and (not automatic or not self._target["unknown"] and self._polls < MAX_POLLS)
        ):
            if automatic:
                self._polls += 1
            self._last_poll = time.monotonic()
            self._request("GET", adoption_url(self._target["id"]), context="operation")

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request["epoch"] != self.epoch:
            return
        try:
            if request["context"] == "snapshot":
                current = validate_adoption_snapshot(value)
                if current["core_epoch"] != self.epoch or request["serial"] < self._accepted_serial:
                    return
                if (
                    self.snapshot is None
                    or current["observation_sha256"] != self.snapshot["observation_sha256"]
                ):
                    self.confirm.setChecked(False)
                self.snapshot, self._received_at = current, request["started_at"]
                self._accepted_serial = request["serial"]
            else:
                self._operation_result(request, value)
        except (ValueError, TypeError, KeyError):
            self._unconfirmed()
        self._controls()

    def _operation_result(self, request, value):
        target = self._target
        if (
            target is None
            or request["id"] != target["id"]
            or type(value) is not dict
            or value.get("id") != target["id"]
            or value.get("core_epoch") != self.epoch
            or target["epoch"] != self.epoch
            or value.get("kind", "cold_adoption") != "cold_adoption"
            or value.get("state") not in {"admitted", "running", "blocked", "unknown", "succeeded"}
            or type(value.get("durable")) is not bool
            or "request_digest" in value
            and not _sha(value["request_digest"])
        ):
            raise ValueError("Invalid cold adoption response")
        if request["serial"] < target.get("accepted_serial", 0):
            return
        target["accepted_serial"] = request["serial"]
        self.receipt = deepcopy(value)
        state = value["state"]
        target["unknown"] = state in {"unknown", "blocked"} or (
            state == "succeeded" and not value["durable"]
        )
        if state == "succeeded" and value["durable"]:
            result = value.get("result")
            if (
                type(result) is not dict
                or result.get("core_epoch") != self.epoch
                or type(result.get("generation")) is not int
                or not 0 <= result["generation"] < 2**63
                or not _identity(result.get("new_actor_token"))
                or not _identity(result.get("prior_core_epoch"))
                or result["prior_core_epoch"] == self.epoch
                or result.get("manual_required") is not True
                or not _sha(result.get("receipt_sha256"))
                or any(
                    result.get(key) is not False
                    for key in (
                        "automatic_inference_started",
                        "financial_resumed",
                        "old_error_cleared",
                        "old_request_replayed",
                        "old_physical_close_claimed",
                    )
                )
            ):
                raise ValueError("Cold adoption completion is unconfirmed")
            target["settled"], target["unknown"] = True, False
            self._received_at = None
            if target.get("completion_sha256") not in {None, result["receipt_sha256"]}:
                raise ValueError("Original adoption receipt changed")
            if target.get("completion_sha256") is None:
                target["completion_sha256"] = result["receipt_sha256"]
                self.adoptionCompleted.emit(deepcopy(result))

    def _unconfirmed(self):
        self._received_at = None
        if self._target:
            self._target["unknown"] = True
        self._outcome = self._text(
            "결과 미확인 · 동일 요청을 확인하세요.", "Result unconfirmed. Check the same request."
        )

    def _failed(self, tag, _message):
        if self._requests.pop(tag, None) is not None:
            self._unconfirmed()
            self._controls()

    def tick(self):
        if (
            self.isChecked()
            and self.isVisible()
            and self._connected()
            and self._target
            and not self._target["settled"]
            and not self._target["unknown"]
            and self._target["epoch"] == self.epoch
            and time.monotonic() - self._last_poll >= 1
        ):
            self.query(automatic=True)
        self._controls()

    def _controls(self, *_args):
        connected = bool(self.isChecked() and self._connected())
        self.refresh_button.setEnabled(bool(connected and not self._requests))
        self.query_button.setEnabled(bool(connected and self._target and not self._requests))
        self.confirm.setEnabled(
            bool(connected and self._fresh() and self._target is None and self.snapshot["eligible"])
        )
        self.adopt_button.setEnabled(bool(self._writable()))
        status = self._text("새 상태 조회 필요", "Refresh required")
        if self._fresh():
            status = (
                self._text("수동 준비 가능", "Ready for manual preparation")
                if self.snapshot["eligible"]
                else self._text("보류 · 준비 조건 미확인", "Held; startup unconfirmed")
            )
        if self._target:
            if self._target["unknown"]:
                status = self._text(
                    "결과 미확인 · 동일 요청을 확인하세요",
                    "Result unconfirmed; check same request",
                )
            elif self._target["settled"]:
                status = self._text(
                    "새 작업 준비됨 · 별도 수동 초기화 필요",
                    "Prepared; manual initialization required",
                )
            else:
                status = self._text("요청 진행 중", "Request pending")
            if self._polls >= MAX_POLLS:
                status += self._text(
                    " · 자동 조회 종료, 수동 확인 필요",
                    " · Automatic checks ended; check manually",
                )
        self.status.setText(status + ("\n" + self._outcome if self._outcome else ""))
        self.request_label.setText(
            self._text("요청: ", "Request: ") + self._target["id"] if self._target else ""
        )
