"""Explicit account retirement and archive loading through owner HTTP contracts."""

import json
import re
from copy import deepcopy
from hashlib import sha256
from uuid import uuid4

from PySide6.QtCore import Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QLabel,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from omega.runtime.margin_retirement_client import (
    history_job_url,
    history_load_body,
    history_load_url,
    retirement_body,
    retirement_url,
    validate_retired_history,
    validate_retirement_observation,
)

_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_SHA = re.compile(r"[a-f0-9]{64}\Z")
_META = {
    "account_id",
    "state",
    "operation_id",
    "receipt_sha256",
    "registered_parameters_sha256",
    "session_config_hash",
    "ledger_namespace",
    "archive_sha256",
    "ledger_total",
    "reason_code",
}


def retired_accounts(value):
    """Bounded metadata only; absence never invents an archived balance/session."""
    if type(value) is not list or len(value) > 128:
        raise ValueError("invalid retired account list")
    result = {}
    for row in value:
        if type(row) is not dict or set(row) != _META:
            raise ValueError("invalid retired account metadata")
        identity = row["account_id"]
        if type(identity) is not str or _ID.fullmatch(identity) is None or identity in result:
            raise ValueError("invalid retired account identity")
        if type(row["state"]) is not str or row["state"] not in {"retiring", "retired"}:
            raise ValueError("invalid account lifecycle")
        if type(row["operation_id"]) is not str or _ID.fullmatch(row["operation_id"]) is None:
            raise ValueError("invalid retirement operation identity")
        for name in (
            "receipt_sha256",
            "registered_parameters_sha256",
            "session_config_hash",
            "ledger_namespace",
            "archive_sha256",
        ):
            item = row[name]
            if item is not None and (type(item) is not str or _SHA.fullmatch(item) is None):
                raise ValueError("invalid retirement metadata checksum")
        if row["state"] == "retired" and any(
            row[name] is None
            for name in (
                "receipt_sha256",
                "registered_parameters_sha256",
                "session_config_hash",
                "ledger_namespace",
                "archive_sha256",
                "ledger_total",
            )
        ):
            raise ValueError("retired metadata lacks durable archive identity")
        if row["ledger_total"] is not None and (
            type(row["ledger_total"]) is not int or not 0 <= row["ledger_total"] <= 10000
        ):
            raise ValueError("invalid retired ledger count")
        reason = row["reason_code"]
        if reason is not None and (
            type(reason) is not str or re.fullmatch(r"[A-Za-z0-9_]{1,128}", reason) is None
        ):
            raise ValueError("invalid retired reason")
        result[identity] = deepcopy(row)
    return result


class MarginRetirement(QGroupBox):
    changed = Signal()

    def __init__(self, owner):
        super().__init__(owner)
        self.owner = owner
        self.key, self.generation, self.observation = None, 0, None
        self.pending = self.history = None
        self.loaded, self.requests = set(), set()
        self.supported = self.available = False
        self.row = self.metadata = None
        self.archive = False
        self.setCheckable(True)
        outer = QVBoxLayout(self)
        self.content = QWidget()
        layout = QVBoxLayout(self.content)
        self.note, self.status = QLabel(), QLabel()
        for label in (self.note, self.status):
            label.setWordWrap(True)
            layout.addWidget(label)
        controls = QGridLayout()
        (
            self.inspect_button,
            self.retire_button,
            self.query_button,
            self.retry_button,
            self.load_button,
        ) = (QPushButton() for _ in range(5))
        for index, button in enumerate(
            (
                self.inspect_button,
                self.retire_button,
                self.query_button,
                self.retry_button,
                self.load_button,
            )
        ):
            controls.addWidget(button, index // 2, index % 2)
        layout.addLayout(controls)
        self.consent = QCheckBox()
        layout.addWidget(self.consent)
        outer.addWidget(self.content)
        self.toggled.connect(self.content.setVisible)
        self.inspect_button.clicked.connect(self.inspect)
        self.retire_button.clicked.connect(self.retire)
        self.query_button.clicked.connect(self.query)
        self.retry_button.clicked.connect(self.retry)
        self.load_button.clicked.connect(self.load)
        self.consent.toggled.connect(self.render)
        self.setChecked(False)
        self.content.hide()

    @property
    def busy(self):
        return self.pending is not None and self.pending["state"] not in {
            "completed",
            "failed",
            "error",
        }

    def blocks(self, identity):
        return bool(
            self.pending
            and self.pending["body"]["parameters"]["account_id"] == identity
            and self.pending["state"] not in {"failed", "error"}
        )

    def can_read(self, row):
        return bool(
            row and (self.owner.epoch, row["account_id"], row.get("receipt_sha256")) in self.loaded
        )

    def invalidate(self):
        self.generation += 1
        self.key = self.observation = None
        self.requests.clear()
        self.loaded.clear()
        self.consent.setChecked(False)
        if self.pending and self.busy:
            self.pending["state"] = "unknown"
        if self.history:
            self.history["state"] = "unknown"

    def sync(self, row, metadata, archive, supported, available):
        key = (
            self.owner.epoch,
            row["account_id"] if row else None,
            archive,
            metadata.get("state") if metadata else None,
            metadata.get("receipt_sha256") if metadata else None,
        )
        if key != self.key:
            self.generation += 1
            self.key, self.observation = key, None
            self.requests.clear()
            self.consent.setChecked(False)
        self.row, self.metadata, self.archive = row, metadata, archive
        self.supported, self.available = supported, available
        self.render()

    def _same_pending(self):
        return bool(
            self.pending
            and self.row
            and self.pending["body"]["core_epoch"] == self.owner.epoch
            and self.pending["body"]["parameters"]["account_id"] == self.row["account_id"]
        )

    def _pending_target(self):
        return bool(
            self.pending
            and self.row
            and self.pending["body"]["parameters"]["account_id"] == self.row["account_id"]
        )

    def _proof_matches(self, observed):
        return bool(
            self.pending
            and self.pending.get("proof")
            and all(
                observed[name] == self.pending["proof"][name]
                for name in ("registered_parameters_sha256", "session_config_hash")
            )
        )

    def render(self, *_args):
        text = self.owner.text
        self.setTitle(text("계정 보관 전환 · 과거 자료", "Account retirement / archived history"))
        self.note.setText(
            text(
                "활성 계정을 자동 정지·청산하지 않습니다. "
                "정지·포지션/주문/미확정 0 확인 후 보관합니다. "
                "보관 잔액은 유지되며 이체가 아닙니다. 현재 금액은 미확인입니다. "
                "과거 자료는 명시 불러오기만 읽습니다.",
                "No implicit pause, cancel or flatten. "
                "Retirement requires confirmed paused/flat/no pending state. "
                "Balances remain archived; this is not a transfer. "
                "Current amounts are unconfirmed. History loads only explicitly.",
            )
        )
        self.consent.setText(
            text(
                "최신 관측을 다시 확인한 뒤 선택 계정을 보관합니다",
                "Recheck the fresh observation and retire this account",
            )
        )
        for button, ko, en in (
            (self.inspect_button, "보관 가능 여부 조회", "Inspect retirement"),
            (self.retire_button, "선택 계정 보관 확인", "Confirm retirement"),
            (self.query_button, "원래 요청 조회", "Query original request"),
            (self.retry_button, "원래 ID/내용으로 명시 재시도", "Explicit same-ID retry"),
            (self.load_button, "보관 자료 불러오기", "Load archived history"),
        ):
            button.setText(text(ko, en))
        ready = self.available and self.supported and self.row is not None
        waiting = bool(self.requests)
        self.inspect_button.setEnabled(
            ready
            and self.owner._snapshot_epoch == self.owner.epoch
            and not waiting
            and not self.busy
            and not self.archive
            and self.metadata is None
        )
        self.retire_button.setEnabled(
            bool(
                ready
                and self.owner._snapshot_epoch == self.owner.epoch
                and not waiting
                and not self.busy
                and not self.archive
                and self.metadata is None
                and self.observation
                and self.observation["state"] == "ready"
                and self.consent.isChecked()
            )
        )
        self.query_button.setEnabled(ready and not waiting and self._pending_target())
        self.retry_button.setEnabled(
            bool(
                ready
                and not waiting
                and self._same_pending()
                and self.pending["state"] == "unknown"
            )
        )
        self.load_button.setEnabled(
            bool(
                ready
                and not waiting
                and self.archive
                and self.metadata
                and self.metadata["state"] == "retired"
                and (not self.history or self.history["state"] not in {"queued", "running"})
            )
        )
        self.consent.setEnabled(ready and not waiting and not self.busy and not self.archive)
        if not self.supported:
            self.status.setText(
                text("이 코어는 보관 전환을 지원하지 않습니다", "Core does not support retirement")
            )
        elif self.busy and self.pending["body"]["core_epoch"] != self.owner.epoch:
            self.status.setText(
                self.owner.text(
                    "이전 코어 요청 미확인 · 재전송 금지: ",
                    "Previous-core operation unconfirmed; do not replay: ",
                )
                + self.pending["body"]["id"]
            )

    def send(self, action, method, path, payload=None):
        ticket = (self.generation, action, uuid4().hex)
        self.requests.add(ticket)
        self.owner._request(method, path, payload, context=("lifecycle", ticket))
        self.changed.emit()

    def inspect(self):
        if not self.inspect_button.isEnabled():
            return
        self.observation = None
        self.consent.setChecked(False)
        self.send("inspect", "GET", retirement_url(self.row["account_id"]))

    def retire(self):
        if self.retire_button.isEnabled():
            self.send("confirm", "GET", retirement_url(self.row["account_id"]))

    def query(self):
        if self.query_button.isEnabled():
            if self._same_pending():
                self.send("operation", "GET", "/api/margin-trading/" + self.pending["body"]["id"])
            else:
                self.send("reconcile", "GET", retirement_url(self.row["account_id"]))

    def retry(self):
        if self.retry_button.isEnabled():
            self.send("retry", "GET", retirement_url(self.row["account_id"]))

    def _post(self, body):
        self.consent.setChecked(False)
        proof = (
            self.pending["proof"]
            if self.pending and self.pending["body"]["id"] == body["id"]
            else {
                name: self.observation[name]
                for name in ("registered_parameters_sha256", "session_config_hash")
            }
        )
        self.pending = {"body": deepcopy(body), "state": "unknown", "proof": deepcopy(proof)}
        self.owner._snapshot_epoch = None
        self.send("operation", "POST", "/api/margin-trading", body)

    def load(self):
        if not self.load_button.isEnabled():
            return
        body = history_load_body(
            uuid4().hex, self.row["account_id"], self.metadata["receipt_sha256"], self.owner.epoch
        )
        self.history = {"body": body, "state": "queued"}
        self.send("history", "POST", history_load_url(), body)

    def tick(self):
        if not self.available or self.requests or not self.row:
            return
        if self._same_pending() and self.pending["state"] in {"queued", "running"}:
            self.query()
            return
        if not self.history:
            return
        body = self.history["body"]
        if (
            self.history["state"] in {"queued", "running"}
            and body["core_epoch"] == self.owner.epoch
            and body["account_id"] == self.row["account_id"]
        ):
            self.send("history", "GET", history_job_url(body["id"]))

    def failed(self, ticket, message):
        self.requests.discard(ticket)
        if ticket[0] != self.generation:
            return
        if ticket[1] in {"inspect", "confirm"}:
            self.observation = None
            self.consent.setChecked(False)
        if ticket[1] in {"operation", "retry", "verify", "reconcile"} and self.pending:
            self.pending["state"] = "unknown"
        if ticket[1] == "history" and self.history:
            self.history["state"] = "unknown"
        self.status.setText(
            self.owner.text("미확인 · 자동 재전송 없음: ", "Unconfirmed; no automatic POST retry: ")
            + str(message)[:160]
        )
        self.changed.emit()

    def received(self, ticket, value):
        if ticket not in self.requests or ticket[0] != self.generation or not self.row:
            return
        self.requests.discard(ticket)
        action = ticket[1]
        if type(value) is not dict:
            raise ValueError("invalid lifecycle response")
        if action in {"inspect", "confirm", "retry", "verify", "reconcile"}:
            observed = validate_retirement_observation(
                value, account_id=self.row["account_id"], core_epoch=self.owner.epoch
            )
            if action == "inspect":
                self.observation = observed
                self.status.setText(observed["state"] + " · " + str(observed["reason_code"] or ""))
            elif (
                observed["state"] == "retired"
                and self._pending_target()
                and observed["operation_id"] == self.pending["body"]["id"]
                and self._proof_matches(observed)
            ):
                self.pending["state"] = "completed"
                self.status.setText(
                    self.owner.text(
                        "보관 완료 · 잔액 보존 · 이체 없음",
                        "Retirement confirmed; balances retained, no transfer",
                    )
                )
                self.owner.refresh()
            elif action == "confirm":
                if (
                    observed["state"] == "ready"
                    and self.observation
                    and observed["observation_sha256"] == self.observation["observation_sha256"]
                ):
                    self.observation = observed
                    self._post(
                        retirement_body(
                            uuid4().hex,
                            self.row["account_id"],
                            observed["observation_sha256"],
                            self.owner.epoch,
                        )
                    )
                else:
                    self.observation = observed
                    self.consent.setChecked(False)
                    self.status.setText(
                        self.owner.text(
                            "관측 변경 · 다시 확인하세요", "Observation changed; confirm again"
                        )
                    )
            elif (
                action == "retry"
                and self._same_pending()
                and observed["state"] in {"ready", "retiring"}
                and (
                    observed["state"] == "ready"
                    or observed["operation_id"] == self.pending["body"]["id"]
                )
                and observed["observation_sha256"]
                == self.pending["body"]["parameters"]["expected_observation_sha256"]
                and self._proof_matches(observed)
            ):
                self._post(self.pending["body"])
            else:
                self.status.setText(
                    self.owner.text(
                        "미확인 · 원래 요청 유지", "Unconfirmed; original request retained"
                    )
                )
        elif action == "operation":
            body = self.pending["body"]
            expected = sha256(
                json.dumps(
                    (body["kind"], body["parameters"]),
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            if (
                value.get("id") != body["id"]
                or value.get("kind") != body["kind"]
                or value.get("operation_core_epoch", value.get("core_epoch")) != body["core_epoch"]
                or value.get("request_digest") != expected
                or type(value.get("state")) is not str
                or value["state"]
                not in {"queued", "running", "completed", "unknown", "failed", "error"}
            ):
                raise ValueError("retirement operation identity mismatch")
            self.pending["state"] = value["state"] if value["state"] != "completed" else "running"
            self.status.setText(body["id"] + " · " + value["state"])
            if value["state"] == "completed":
                self.send("verify", "GET", retirement_url(body["parameters"]["account_id"]))
            elif value["state"] in {"failed", "error"}:
                self.owner._snapshot_epoch = None
                self.owner.refresh()
        elif action == "history":
            body = self.history["body"]
            job = validate_retired_history(value, identity=body["id"], core_epoch=self.owner.epoch)
            if (
                job["account_id"] != body["account_id"]
                or job["receipt_sha256"] != body["expected_receipt_sha256"]
            ):
                raise ValueError("archive job identity mismatch")
            self.history["state"] = job["state"]
            if job["state"] == "completed" and job["cached"]:
                if len(self.loaded) >= 128:
                    self.loaded.clear()
                self.loaded.add(
                    (self.owner.epoch, body["account_id"], body["expected_receipt_sha256"])
                )
            self.status.setText(self.owner.text("보관 자료: ", "Archived history: ") + job["state"])
        self.changed.emit()
