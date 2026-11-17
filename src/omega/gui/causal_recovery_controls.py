"""Small explicit NN recovery view; transport uncertainty keeps the original ID."""

import re
import time
from copy import deepcopy
from uuid import uuid4

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import QCheckBox, QGridLayout, QGroupBox, QLabel, QPushButton, QWidget

PATH = "/api/causal-inference/recovery"
MAX_POLLS = 32
STATES = {
    "STARTING",
    "MANUAL_REQUIRED",
    "ACTIVE",
    "RECOVERY_HELD",
    "OLD_RETIRED",
    "HELD",
    "UNKNOWN",
    "CLOSED",
}


def _identity(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _number(value):
    return type(value) is int and 0 <= value <= 2**63 - 1


def _scope(value):
    return (
        type(value) is dict
        and value.get("scope") == "committed_inference_service"
        and value.get("live_orders_enabled") is False
    )


def _summary(value):
    if (
        not _scope(value)
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or not _identity(value.get("core_epoch"))
        or value.get("state") not in STATES
        or value.get("generation") is not None
        and not _number(value["generation"])
        or not _number(value.get("control_revision"))
        or value.get("actor_token") is not None
        and not _identity(value["actor_token"])
        or (value.get("generation") is None) != (value.get("actor_token") is None)
        or type(value.get("held")) is not bool
        or value.get("current_recovery_id") is not None
        and not _identity(value["current_recovery_id"])
        or type(value.get("blockers")) is not list
        or len(value["blockers"]) > 64
        or any(not _identity(code) for code in value["blockers"])
        or value.get("retirement_sha256") is not None
        and (
            type(value["retirement_sha256"]) is not str
            or re.fullmatch(r"[a-f0-9]{64}", value["retirement_sha256"]) is None
        )
        or (value.get("recovery_scope"), value.get("historical_carry_available"))
        not in {
            ("verified_pins0_reservations0", False),
            ("verified_historical_pin_carry", True),
        }
        or type(value.get("historical_carry_available")) is not bool
        or type(value.get("cold_interrupted_adoption_available")) is not bool
        or any(
            value.get(key) is not False
            for key in (
                "automatic_inference_started",
                "financial_resumed",
            )
        )
    ):
        raise ValueError("Invalid NN recovery summary")
    return deepcopy(value)


class CausalRecoveryControls(QGroupBox):
    authorityChanged = Signal(object)
    recoveryCompleted = Signal(object)

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch, self.snapshot, self.receipt = None, None, None
        self.attached, self.capable = bool(client.attached), False
        self.generation, self.actor_token = None, None
        self._received_at, self._serial, self._accepted_serial = None, 0, 0
        self._requests, self._target = {}, None
        self._polls, self._last_poll, self._outcome = 0, 0.0, ""
        self.setCheckable(True)
        self.setChecked(False)
        layout = QGridLayout(self)
        self.content = QWidget()
        layout.addWidget(self.content)
        body = QGridLayout(self.content)
        self.hint, self.status, self.request_label = self._label(), self._label(), self._label()
        self.refresh_button, self.query_button = QPushButton(), QPushButton()
        self.confirm, self.recover_button = QCheckBox(), QPushButton()
        body.addWidget(self.hint, 0, 0, 1, 2)
        body.addWidget(self.status, 1, 0, 1, 2)
        body.addWidget(self.request_label, 2, 0, 1, 2)
        body.addWidget(self.refresh_button, 3, 0)
        body.addWidget(self.query_button, 3, 1)
        body.addWidget(self.confirm, 4, 0, 1, 2)
        body.addWidget(self.recover_button, 5, 0, 1, 2)
        self.toggled.connect(self._expanded)
        self.confirm.toggled.connect(self._controls)
        self.refresh_button.clicked.connect(self.refresh)
        self.query_button.clicked.connect(self.query)
        self.recover_button.clicked.connect(self.recover)
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

    def set_presentation(self, _theme, language):
        self.language = language
        ko = language == "ko"
        self.setTitle("NN 작업 복구" if ko else "NN task recovery")
        self.hint.setText(
            "현재 연결된 NN 작업만 정리하고 새 작업으로 교체합니다. 이전 상태 이어받기는 "
            "지원하지 않습니다. 복구 요청은 추론·계정 거래를 자동 재개하지 않습니다. "
            "추론 재개는 별도 수동 초기화가 필요합니다."
            if ko
            else "Clean up and replace the currently connected NN task. Previous state "
            "cannot be carried forward. Inference requires separate manual initialization; "
            "recovery does not automatically resume inference or account trading."
        )
        self.refresh_button.setText("정리 상태 확인" if ko else "Check cleanup status")
        self.query_button.setText("동일 요청 확인" if ko else "Check same request")
        self.confirm.setText(
            "이전 NN 상태를 초기화하고 교체하는 데 동의합니다"
            if ko
            else "I confirm replacement with a reset NN state"
        )
        self.recover_button.setText("NN 작업 정리·교체" if ko else "Clean up and replace NN task")
        self._controls()

    def update_state(self, state):
        core = state.get("core", {}) if type(state) is dict else {}
        epoch = core.get("epoch")
        if _identity(epoch) and epoch != self.epoch:
            self.epoch = epoch
            self.snapshot, self.receipt, self._received_at = None, None, None
            self.generation, self.actor_token = None, None
            self._requests.clear()
            self._accepted_serial = 0
            self.confirm.setChecked(False)
            if self._target:
                self._target["unknown"] = True
                self._target["historical"] = True
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

    def observe_authority(self, epoch, generation, token):
        """A newer normal snapshot invalidates this view without granting freshness."""
        if epoch != self.epoch or self.generation == generation and self.actor_token == token:
            return
        if self.generation is not None and (generation is None or generation < self.generation):
            return
        if self.generation == generation and self.actor_token is not None:
            raise ValueError("NN task identity changed without a new generation")
        self.generation, self.actor_token = generation, token
        self.snapshot, self.receipt, self._received_at = None, None, None
        self.confirm.setChecked(False)
        if self._target and self._target["generation"] != generation:
            self._target["historical"] = True
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
            and time.monotonic() - self._received_at <= 10
            and self.snapshot["core_epoch"] == self.epoch
        )

    def _writable(self):
        return bool(
            self.isChecked()
            and self.isVisible()
            and self._connected()
            and self._fresh()
            and self.confirm.isChecked()
            and (
                self._target is None or self._target.get("settled") and not self._target["unknown"]
            )
            and not self._requests
            and self.snapshot["generation"] is not None
            and self.snapshot["state"] in {"MANUAL_REQUIRED", "ACTIVE", "HELD"}
            and not self.snapshot["blockers"]
            and self.snapshot["current_recovery_id"] is None
        )

    def _request(self, method, path, payload=None, *, kind, manual=False):
        self._serial += 1
        tag = f"causal-recovery:{self._serial}"
        self._requests[tag] = {
            "kind": kind,
            "epoch": self.epoch,
            "generation": self.generation,
            "serial": self._serial,
            "started_at": time.monotonic(),
            "manual": manual,
            "id": (payload or {}).get("id") if method == "POST" else path.rsplit("/", 1)[-1],
        }
        self.client.request(method, path, payload, tag=tag)
        self._controls()

    def refresh(self):
        if self.isChecked() and self._connected() and not self._requests:
            self._request("GET", PATH, kind="summary")

    def query(self, *, automatic=False):
        if self.isChecked() and self._connected() and self._target and not self._requests:
            if automatic and (self._polls >= MAX_POLLS or self._target["unknown"]):
                return
            if automatic:
                self._polls += 1
            self._last_poll = time.monotonic()
            self._request(
                "GET", PATH + "/" + self._target["id"], kind="operation", manual=not automatic
            )

    def recover(self):
        if not self._writable():
            return
        current = self.snapshot
        identity = uuid4().hex
        self._target = {
            "id": identity,
            "epoch": self.epoch,
            "generation": self.generation,
            "actor_token": self.actor_token,
            "unknown": False,
            "historical": False,
        }
        self._polls, self.receipt = 0, None
        self._outcome = ""
        self.confirm.setChecked(False)
        self._received_at = None
        self._request(
            "POST",
            PATH,
            {
                "id": identity,
                "parameters": {
                    "core_epoch": self.epoch,
                    "expected_generation": current["generation"],
                    "expected_control_revision": current["control_revision"],
                    "expected_actor_token": current["actor_token"],
                    "expected_retirement_sha256": current["retirement_sha256"],
                    "state_policy": "reset",
                    "confirm": True,
                },
            },
            kind="operation",
        )

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request["epoch"] != self.epoch:
            return
        try:
            if request["kind"] == "summary":
                current = _summary(value)
                generation = current["generation"]
                if (
                    current["core_epoch"] != self.epoch
                    or request["serial"] < self._accepted_serial
                    or self.generation is not None
                    and (generation is None or generation < self.generation)
                ):
                    return
                changed = (generation, current["actor_token"]) != (
                    self.generation,
                    self.actor_token,
                )
                if (
                    not changed
                    and self.snapshot
                    and current["control_revision"] < self.snapshot["control_revision"]
                ):
                    return
                if changed:
                    if self.generation == generation and self.actor_token is not None:
                        raise ValueError("NN task identity changed without a new generation")
                    self.receipt = None
                    self.confirm.setChecked(False)
                    if self._target and self._target["generation"] != generation:
                        self._target["historical"] = True
                    self.generation, self.actor_token = generation, current["actor_token"]
                    self.authorityChanged.emit(current)
                self.snapshot, self._received_at = current, request["started_at"]
                self._accepted_serial = request["serial"]
                self._completed_binding()
                if self._target is None and current["current_recovery_id"]:
                    self._target = {
                        "id": current["current_recovery_id"],
                        "epoch": self.epoch,
                        "generation": generation,
                        "unknown": True,
                        "historical": False,
                    }
            else:
                self._operation_result(request, value)
        except (ValueError, TypeError, KeyError):
            self._received_at = None
            self._outcome = self._text(
                "응답을 확인할 수 없습니다. 동일 요청을 확인하세요.",
                "Response unconfirmed. Check the same request.",
            )
            if self._target:
                self._target["unknown"] = True
        self._controls()

    def _operation_result(self, request, value):
        target = self._target
        if (
            not target
            or request["id"] != target["id"]
            or type(value) is not dict
            or value.get("core_epoch") != self.epoch
            or value.get("id") != target["id"]
            or value.get("state") not in {"admitted", "running", "blocked", "unknown", "succeeded"}
            or type(value.get("durable")) is not bool
            or "request_digest" in value
            and (
                type(value["request_digest"]) is not str
                or re.fullmatch(r"[a-f0-9]{64}", value["request_digest"]) is None
            )
        ):
            raise ValueError("Invalid recovery operation")
        if target["epoch"] != self.epoch or request["generation"] != self.generation:
            target["historical"], target["unknown"] = True, True
            return
        if request["serial"] < target.get("accepted_serial", 0):
            return
        target["accepted_serial"] = request["serial"]
        # Operation progress never replaces a fresh cleanup summary or grants authority.
        self.receipt = deepcopy(value)
        state = value["state"]
        target["unknown"] = (
            state == "unknown"
            or not value["durable"]
            and state
            in {
                "blocked",
                "succeeded",
            }
        )
        if state in {"blocked", "succeeded"} and value["durable"]:
            result = value.get("result")
            if state == "succeeded":
                if (
                    type(result) is not dict
                    or not _number(result.get("generation"))
                    or result.get("manual_required") is not True
                    or type(result.get("receipt_sha256")) is not str
                    or re.fullmatch(r"[a-f0-9]{64}", result["receipt_sha256"]) is None
                ):
                    raise ValueError("Recovery completion remains unconfirmed")
                target["completion"] = deepcopy(result)
            self._outcome = self._text(
                "NN 작업 교체 결과를 확인했습니다. 현재 정리 상태를 다시 조회하세요.",
                "Replacement result received. Check the current cleanup status.",
            )
            target["settled"] = True
            self.refresh()

    def _completed_binding(self):
        target = self._target or {}
        proof = target.get("completion", {})
        current = self.snapshot or {}
        if (
            target.get("settled")
            and not target.get("unknown")
            and self._fresh()
            and target.get("epoch") == self.epoch
            and current.get("state") == "MANUAL_REQUIRED"
            and current.get("generation") == proof.get("generation")
            and _identity(current.get("actor_token"))
            and not current.get("blockers")
            and current.get("current_recovery_id") is None
            and type(target.get("generation")) is int
            and proof["generation"] > target["generation"]
            and _identity(target.get("actor_token"))
        ):
            self.recoveryCompleted.emit(
                {
                    "previous": (self.epoch, target["generation"], target["actor_token"]),
                    "current": (self.epoch, proof["generation"], current["actor_token"]),
                    "receipt_sha256": proof["receipt_sha256"],
                }
            )

    def _failed(self, tag, _message):
        request = self._requests.pop(tag, None)
        if request is None:
            return
        self._received_at = None
        self._outcome = self._text(
            "결과 미확인 · 동일 요청을 확인하세요.", "Result unconfirmed. Check the same request."
        )
        if self._target:
            self._target["unknown"] = True
        self._controls()

    def tick(self):
        if (
            self.isChecked()
            and self.isVisible()
            and self._connected()
            and self._target
            and not self._target.get("settled")
            and not self._target.get("historical")
            and time.monotonic() - self._last_poll >= 1
        ):
            self.query(automatic=True)
        self._controls()

    def _text(self, ko, en):
        return ko if self.language == "ko" else en

    def _controls(self, *_args):
        connected = self.isChecked() and self._connected()
        self.refresh_button.setEnabled(connected and not self._requests)
        self.query_button.setEnabled(connected and bool(self._target) and not self._requests)
        self.confirm.setEnabled(
            bool(
                connected
                and self._fresh()
                and (
                    self._target is None
                    or self._target.get("settled")
                    and not self._target["unknown"]
                )
            )
        )
        self.recover_button.setEnabled(self._writable())
        names = {
            "STARTING": ("준비 중", "Starting"),
            "MANUAL_REQUIRED": ("수동 초기화 필요", "Manual initialization required"),
            "ACTIVE": ("연결됨", "Connected"),
            "RECOVERY_HELD": ("정리 중", "Cleanup pending"),
            "OLD_RETIRED": ("이전 작업 정리됨", "Previous task cleaned up"),
            "HELD": ("보류", "Held"),
            "UNKNOWN": ("미확인", "Unconfirmed"),
            "CLOSED": ("종료됨", "Closed"),
        }
        state = names.get((self.snapshot or {}).get("state"), ("상태 조회 필요", "Check status"))
        text = state[0 if self.language == "ko" else 1]
        if not self._fresh():
            text += self._text(" · 새 상태 조회 필요", " · Refresh required")
        if (self.snapshot or {}).get("blockers"):
            text += self._text(
                " · 정리가 확인되지 않아 교체할 수 없습니다",
                " · Cleanup blocks replacement",
            )
        if self._target:
            state = (self.receipt or {}).get("state")
            text += " · " + self._text(
                "결과 미확인"
                if self._target["unknown"]
                else "요청 결과 확인됨"
                if self._target.get("settled")
                else "요청 진행 중",
                "Result unconfirmed"
                if self._target["unknown"]
                else "Request result received"
                if self._target.get("settled")
                else "Request pending",
            )
            if state == "blocked":
                text += self._text(" · 교체 보류", " · Replacement blocked")
            if self._polls >= MAX_POLLS:
                text += self._text(
                    " · 자동 조회 종료, 동일 요청을 수동 확인하세요",
                    " · Automatic checks ended; check the same request manually",
                )
        if (self.snapshot or {}).get("historical_carry_available"):
            self.hint.setText(
                self._text(
                    "검증된 기존 입력 연결만 이어받습니다. 복구 요청은 추론·계정 거래를 "
                    "자동 재개하지 않으며, 추론은 별도 수동 초기화가 필요합니다.",
                    "Only verified previous input bindings can be carried forward. Recovery does "
                    "not automatically resume inference or account trading; "
                    "initialize inference manually.",
                )
            )
        else:
            self.hint.setText(
                self._text(
                    "현재 연결된 NN 작업만 정리하고 새 작업으로 교체합니다. 이전 상태 이어받기는 "
                    "지원하지 않습니다. 복구 요청은 추론·계정 거래를 자동 재개하지 않습니다. "
                    "추론 재개는 별도 수동 초기화가 필요합니다.",
                    "Clean up and replace the currently connected NN task. Previous state cannot "
                    "be carried forward. Inference requires separate manual initialization; "
                    "recovery "
                    "does not automatically resume inference or account trading.",
                )
            )
        self.status.setText(text + ("\n" + self._outcome if self._outcome else ""))
        self.request_label.setText(
            self._text("요청: ", "Request: ") + self._target["id"] if self._target else ""
        )
