"""Explicit CPU committed-input controls; no model, database or dataset access."""

import json
import re
import time
from copy import deepcopy
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from PySide6.QtCore import QDateTime, Qt, QTimer, QTimeZone
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.domain.errors import ValidationError
from omega.domain.inference import InferenceOutputBinding
from omega.domain.model_contract import ShadowModelContract
from omega.domain.models import is_valid_symbol
from omega.gui.causal_adoption_controls import CausalAdoptionControls
from omega.gui.causal_recovery_controls import CausalRecoveryControls
from omega.gui.evidence_status import evidence_status

PATH = "/api/causal-inference"
SCOPE = "committed_inference_service"
KINDS = {"activate", "reset_stream", "predict_committed", "pause", "retire"}
TERMINAL = {"complete", "failed", "unknown", "cancelled"}
SNAPSHOT_TTL_S = 10


def _identity(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is not None


def _hex(value, size=32):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{size}}}", value) is not None


def _operation_id(value):
    return type(value) is str and re.fullmatch(r"[A-Za-z0-9_-]{1,96}", value) is not None


def _integer(value):
    return type(value) is int and 0 <= value <= 253_402_300_799_000


def _instrument(value):
    parts = value.split(":") if type(value) is str else ()
    return (
        len(parts) == 3
        and parts[0] == "binance"
        and parts[1] in {"spot", "usdm"}
        and is_valid_symbol(parts[2])
    )


def _copy(value):
    if len(json.dumps(value, allow_nan=False).encode()) > 2_097_152:
        raise ValueError("Causal response exceeds display capacity")
    return deepcopy(value)


def _committed_scope(value):
    return (
        type(value) is dict
        and value.get("scope") == SCOPE
        and value.get("live_orders_enabled") is False
        and (
            value.get("strategy_delivery_enabled") is False
            or value.get("strategy_delivery_enabled") is True
            and value.get("strategy_delivery_scope") == "virtual_margin_v2_opt_in"
        )
    )


def _operation(value):
    if (
        type(value) is not dict
        or not _operation_id(value.get("id"))
        or value.get("kind") not in KINDS
        or not _identity(value.get("core_epoch"))
        or not _identity(value.get("operation_core_epoch", value.get("core_epoch")))
        or value.get("state") not in {"queued", "running"} | TERMINAL
        or not _integer(value.get("submitted_revision"))
        or value.get("revision") is not None
        and not _integer(value["revision"])
        or type(value.get("durable")) is not bool
        or not _hex(value.get("request_digest"), 64)
    ):
        raise ValueError("Invalid causal operation receipt")
    return _copy(value | {"core_epoch": value.get("operation_core_epoch", value["core_epoch"])})


def _snapshot(value):
    if (
        type(value) is not dict
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or not _committed_scope(value)
        or not _identity(value.get("core_epoch"))
        or not _integer(value.get("revision"))
        or not _integer(value.get("server_time_ms"))
        or value.get("state")
        not in {
            "disabled",
            "ready",
            "paused",
            "manual_required",
            "recording_unconfirmed",
            "retired",
            "closed",
        }
        or any(
            type(value.get(key)) is not bool
            for key in ("ready", "storage_available", "actor_alive")
        )
        or value.get("active_deployment_id") is not None
        and not _identity(value["active_deployment_id"])
        or type(value.get("deployments")) is not list
        or len(value["deployments"]) > 128
        or type(value.get("streams")) is not list
        or len(value["streams"]) > 128
        or type(value.get("operations")) is not list
        or len(value["operations"]) > 512
    ):
        raise ValueError("Invalid committed-inference snapshot")
    deployments, streams = set(), set()
    for row in value["deployments"]:
        if (
            type(row) is not dict
            or not _identity(row.get("id"))
            or row["id"] in deployments
            or not _hex(row.get("training_job_id"))
            or row.get("state")
            not in {"preparing", "ready", "paused", "manual_required", "retired", "failed"}
            or type(row.get("manual_required")) is not bool
        ):
            raise ValueError("Invalid causal deployment")
        deployments.add(row["id"])
    for row in value["streams"]:
        if (
            type(row) is not dict
            or not _identity(row.get("deployment_id"))
            or not _identity(row.get("stream"))
            or not _identity(row.get("stream_epoch"))
            or not _instrument(row.get("instrument"))
            or row.get("state") not in {"ready", "blocked", "manual_required"}
            or type(row.get("manual_required")) is not bool
            or not _integer(row.get("sequence"))
            or row.get("last_open_ms") is not None
            and not _integer(row["last_open_ms"])
            or row.get("pending_operation_id") is not None
            and not _operation_id(row["pending_operation_id"])
        ):
            raise ValueError("Invalid causal stream")
        key = row["deployment_id"], row["stream"], row["instrument"]
        if key in streams:
            raise ValueError("Duplicate causal stream")
        streams.add(key)
        feature = row.get("feature")
        if feature is not None:
            if (
                type(feature) is not dict
                or not _identity(feature.get("request_id"))
                or not _hex(feature.get("input_sha256"), 64)
                or not _hex(feature.get("frame_sha256"), 64)
                or any(
                    not _integer(feature.get(key))
                    for key in (
                        "open_time_ms",
                        "decision_at_ms",
                        "accepted_at_ms",
                        "valid_until_ms",
                    )
                )
                or type(feature.get("values")) is not dict
                or len(feature["values"]) > 64
                or feature["open_time_ms"] % 60000
                or not feature["open_time_ms"] + 60000
                <= feature["decision_at_ms"]
                <= feature["accepted_at_ms"]
                <= feature["valid_until_ms"]
            ):
                raise ValueError("Invalid causal feature summary")
            for name, number in feature["values"].items():
                if (
                    type(name) is not str
                    or re.fullmatch(r"NN\.[A-Za-z][A-Za-z0-9_.]{0,123}", name) is None
                    or type(number) is not str
                    or len(number) > 128
                    or not Decimal(number).is_finite()
                ):
                    raise ValueError("Invalid namespaced causal feature")
    for operation in value["operations"]:
        _operation(operation)
    return _copy(value)


def _generation(value, key="actor_generation", token_key="current_actor_token"):
    generation, token = value.get(key), value.get(token_key)
    if (
        generation is not None
        and not _integer(generation)
        or token is not None
        and not _identity(token)
        or (generation is None) != (token is None)
    ):
        raise ValueError("Invalid NN task generation")
    return generation, token


class CausalControls(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__("확정 관측 CPU 추론 / Committed inference", parent)
        self.client, self.language = client, "ko"
        self.epoch, self.snapshot, self.training_id = None, None, None
        self.attached, self.capable = bool(client.attached), False
        self._received_at, self._serial, self._accepted_serial = None, 0, 0
        self._minimum_revision = 0
        self._generation_known, self._actor_generation, self._actor_token = False, None, None
        self._requests, self._pending, self._unresolved = {}, None, {}
        self._historical, self._recovery_releases = {}, {}
        self._adoption_releases = {}
        self._deploy_selection, self._visible_streams = None, []
        self._outcome = ""
        self.setCheckable(True)
        self.setChecked(False)
        root = QVBoxLayout(self)
        self.content = QWidget()
        root.addWidget(self.content)
        layout = QVBoxLayout(self.content)
        self.hint = self._label(
            "실제 PostgreSQL의 commit된 확정봉과 실제 수신·현재 시각을 코어가 검증합니다. "
            "CPU 전용이며 회고 CSV/MC Shadow와 별개입니다. 입력값·상태·hash·모델 경로를 "
            "직접 지정하지 않습니다. 활성화 뒤 예측은 별도 수동 요청입니다. "
            "전략 입력 전달은 가상 증거금 v2에서 명시 선택한 연결만 사용하며 "
            "실주문은 꺼져 있습니다."
        )
        self.status, self.selection_label = self._label(), self._label()
        layout.addWidget(self.hint)
        layout.addWidget(self.status)
        self.evidence_status = self._label()
        layout.addWidget(self.evidence_status)
        layout.addWidget(self.selection_label)
        top = QGridLayout()
        self.refresh_button, self.reconcile_button = (
            QPushButton("상태 조회"),
            QPushButton("동일 요청 확인"),
        )
        top.addWidget(self.refresh_button, 0, 0)
        top.addWidget(self.reconcile_button, 0, 1)
        layout.addLayout(top)
        self.recovery = CausalRecoveryControls(client, self)
        self.recovery.authorityChanged.connect(self._recovery_authority)
        self.recovery.recoveryCompleted.connect(self._recovery_completed)
        layout.addWidget(self.recovery)
        self.adoption = CausalAdoptionControls(client, self)
        self.adoption.adoptionCompleted.connect(self._adoption_completed)
        layout.addWidget(self.adoption)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        self._build_activation()
        self._build_streams()
        self.refresh_button.clicked.connect(self.refresh)
        self.reconcile_button.clicked.connect(self.reconcile)
        self.toggled.connect(self._expanded)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        if hasattr(client, "stateReceived"):
            client.stateReceived.connect(self.update_state)
        self.timer = QTimer(self)
        self.timer.setInterval(500)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.content.hide()
        self.update_state(getattr(client, "state", {}))

    @staticmethod
    def _label(text=""):
        label = QLabel(text)
        label.setWordWrap(True)
        label.setTextFormat(Qt.TextFormat.PlainText)
        return label

    @staticmethod
    def _table(headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setStretchLastSection(True)
        table.setMaximumHeight(170)
        return table

    def _build_activation(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        self.deployments = QComboBox()
        self.deployments.addItem("새 배포 / New deployment", None)
        layout.addWidget(self.deployments)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.schema, self.features = QLineEdit(), QLineEdit()
        self.schema.setPlaceholderText("학습 모듈의 실제 schema ID")
        self.schema.setMaxLength(128)
        self.features.setPlaceholderText("모듈이 선언한 특성 순서 · comma separated")
        self.features.setMaxLength(33024)
        self.lookback, self.output_width = QSpinBox(), QSpinBox()
        self.lookback.setRange(1, 4096)
        self.output_width.setRange(1, 256)
        self.input_mode = QComboBox()
        for text, mode in (
            ("[batch]", "scalar"),
            ("[batch, features]", "vector"),
            ("[batch, lookback, features]", "sequence"),
        ):
            self.input_mode.addItem(text, mode)
        self.recurrent, self.no_signal = (
            QCheckBox("명시 RNN 상태 계약"),
            QCheckBox("전체 null 출력을 보류로 허용"),
        )
        self.state_shape, self.state_version = QLineEdit(), QLineEdit()
        self.state_shape.setPlaceholderText("선언 shape · 예: 1, 1, 8 (실제 state 값 입력 아님)")
        self.state_shape.setMaxLength(32)
        self.state_version.setMaxLength(128)
        for name, widget in (
            ("Schema", self.schema),
            ("특성 순서", self.features),
            ("Lookback", self.lookback),
            ("입력 shape", self.input_mode),
            ("출력 폭", self.output_width),
            ("RNN", self.recurrent),
            ("상태 shape", self.state_shape),
            ("상태 버전", self.state_version),
        ):
            form.addRow(name, widget)
        form.addRow(self.no_signal)
        self.binding_name, self.binding_index = QLineEdit("NN.score"), QSpinBox()
        self.binding_name.setMaxLength(128)
        self.binding_index.setRange(0, 255)
        form.addRow("출력 특성 이름", self.binding_name)
        form.addRow("출력 열 (vector일 때)", self.binding_index)
        layout.addLayout(form)
        actions = QGridLayout()
        self.add_binding_button, self.remove_binding_button = (
            QPushButton("출력 연결 추가"),
            QPushButton("선택 연결 삭제"),
        )
        actions.addWidget(self.add_binding_button, 0, 0)
        actions.addWidget(self.remove_binding_button, 0, 1)
        layout.addLayout(actions)
        self.bindings = self._table(["Feature", "Output path"])
        self._bindings = []
        layout.addWidget(self.bindings)
        self.shape_confirm, self.trusted = (
            QCheckBox("실제 모듈의 특성·shape 계약을 확인했습니다"),
            QCheckBox("선택한 Trusted Python 실행에 동의합니다"),
        )
        layout.addWidget(self.shape_confirm)
        layout.addWidget(self.trusted)
        self.activate_button = QPushButton("선택 완료 학습을 명시 활성화 · CPU")
        layout.addWidget(self.activate_button)
        self.tabs.addTab(page, "배포 계약")
        self.add_binding_button.clicked.connect(self.add_binding)
        self.remove_binding_button.clicked.connect(self.remove_binding)
        self.activate_button.clicked.connect(self.activate)
        self.deployments.currentIndexChanged.connect(self._deployment_changed)
        for edit in (self.schema, self.features, self.state_shape, self.state_version):
            edit.textChanged.connect(self._contract_changed)
        for spin in (self.lookback, self.output_width):
            spin.valueChanged.connect(self._contract_changed)
        for control in (self.recurrent, self.no_signal):
            control.toggled.connect(self._contract_changed)
        self.input_mode.currentIndexChanged.connect(self._contract_changed)
        self.shape_confirm.toggled.connect(self._controls)
        self.trusted.toggled.connect(self._controls)

    def _build_streams(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        self.streams = self._table(["Stream / instrument", "State", "Feature / expiry"])
        self.streams.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Interactive)
        self.streams.setColumnWidth(0, 150)
        layout.addWidget(self.streams)
        self.feature_summary = self._label()
        self.feature_summary.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.feature_summary)
        self.lineage = self._label()
        self.lineage.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.lineage)
        self.load_stream_button = QPushButton("선택 stream을 요청 폼으로")
        layout.addWidget(self.load_stream_button)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.stream_name, self.symbol = QLineEdit(), QLineEdit()
        self.stream_name.setMaxLength(128)
        self.symbol.setMaxLength(40)
        self.market = QComboBox()
        self.market.addItems(["usdm", "spot"])
        self.state_policy = QComboBox()
        for text, value in (
            ("정책을 명시 선택하세요", None),
            ("새 상태로 reset", "reset"),
            ("검증된 기존 상태 preserve", "preserve"),
        ):
            self.state_policy.addItem(text, value)
        self.open_time = QDateTimeEdit()
        self.open_time.setTimeZone(QTimeZone(b"UTC"))
        self.open_time.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
        self.open_time.setCalendarPopup(True)
        stamp = (time.time_ns() // 1_000_000 // 60000 - 1) * 60000
        self.open_time.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
        self.timeout = QSpinBox()
        self.timeout.setRange(1000, 30000)
        self.timeout.setSingleStep(1000)
        self.timeout.setValue(15000)
        for name, widget in (
            ("Stream", self.stream_name),
            ("시장", self.market),
            ("종목", self.symbol),
            ("상태 정책", self.state_policy),
            ("요청 확정분 UTC", self.open_time),
            ("기한 ms", self.timeout),
        ):
            form.addRow(name, widget)
        layout.addLayout(form)
        actions = QGridLayout()
        self.reset_button, self.predict_button = (
            QPushButton("Stream 명시 설정"),
            QPushButton("Commit된 확정봉으로 한 번 예측"),
        )
        self.pause_button, self.retire_button = (
            QPushButton("선택 배포 정지"),
            QPushButton("선택 배포 종료"),
        )
        for index, button in enumerate(
            (self.reset_button, self.predict_button, self.pause_button, self.retire_button)
        ):
            actions.addWidget(button, index // 2, index % 2)
        layout.addLayout(actions)
        self.operations = self._table(["Operation", "Kind", "State"])
        layout.addWidget(self.operations)
        self.tabs.addTab(page, "확정분 · 결과")
        self.load_stream_button.clicked.connect(self.load_stream)
        self.reset_button.clicked.connect(self.reset_stream)
        self.predict_button.clicked.connect(self.predict)
        self.pause_button.clicked.connect(lambda: self._deployment_operation("pause"))
        self.retire_button.clicked.connect(lambda: self._deployment_operation("retire"))
        for edit in (self.stream_name, self.symbol):
            edit.textChanged.connect(self._controls)
        self.market.currentIndexChanged.connect(self._controls)
        self.state_policy.currentIndexChanged.connect(self._controls)
        self.streams.itemSelectionChanged.connect(self._controls)

    def _contract_changed(self, *_args):
        self.shape_confirm.setChecked(False)
        self.trusted.setChecked(False)
        self._render_bindings()
        self._controls()

    def contract(self):
        features = tuple(item.strip() for item in self.features.text().split(","))
        lookback, mode = self.lookback.value(), self.input_mode.currentData()
        shape = (
            (None,)
            if mode == "scalar"
            else (None, len(features))
            if mode == "vector"
            else (None, lookback, len(features))
        )
        state_shape, state_version = None, None
        if self.recurrent.isChecked():
            parts = [part.strip() for part in self.state_shape.text().split(",")]
            if not 1 <= len(parts) <= 5 or any(
                re.fullmatch(r"[1-9][0-9]{0,3}", part) is None for part in parts
            ):
                raise ValueError("명시 상태 shape의 양수 차원을 확인하세요.")
            state_shape, state_version = (
                tuple(int(part) for part in parts),
                self.state_version.text().strip(),
            )
        return ShadowModelContract(
            self.schema.text().strip(),
            features,
            lookback,
            shape,
            (None,) if self.output_width.value() == 1 else (None, self.output_width.value()),
            state_shape,
            state_version,
            self.no_signal.isChecked(),
        ).document()

    def add_binding(self):
        try:
            name, index = self.binding_name.text().strip(), self.binding_index.value()
            path = ("values", 0) if self.output_width.value() == 1 else ("values", 0, index)
            InferenceOutputBinding(name, path)
            if len(self._bindings) >= 64 or name in {item[0] for item in self._bindings}:
                raise ValueError("출력 연결은 중복 없는 최대 64개 이름이어야 합니다.")
            if self.output_width.value() > 1 and index >= self.output_width.value():
                raise ValueError("출력 열이 선언 폭을 벗어납니다.")
            self._bindings.append((name, index))
            self._contract_changed()
        except (ValueError, ValidationError) as error:
            self._outcome = str(error)
            self._controls()

    def remove_binding(self):
        row = self.bindings.currentRow()
        if 0 <= row < len(self._bindings):
            self._bindings.pop(row)
            self._contract_changed()

    def _binding_documents(self):
        if not self._bindings:
            raise ValueError("NN.* 출력 연결을 명시 추가하세요.")
        result = []
        for name, index in self._bindings:
            if self.output_width.value() > 1 and index >= self.output_width.value():
                raise ValueError("출력 연결이 현재 선언 폭을 벗어납니다.")
            path = ("values", 0) if self.output_width.value() == 1 else ("values", 0, index)
            InferenceOutputBinding(name, path)
            result.append({"name": name, "path": list(path)})
        return result

    def _render_bindings(self):
        self.bindings.setRowCount(len(self._bindings))
        for row, (name, index) in enumerate(self._bindings):
            path = "values[0]" if self.output_width.value() == 1 else f"values[0][{index}]"
            for column, value in enumerate((name, path)):
                self.bindings.setItem(row, column, QTableWidgetItem(value))

    def set_training_selection(self, identity, *, core_epoch):
        selected = identity if _hex(identity) and core_epoch == self.epoch else None
        if selected != self.training_id:
            self.training_id = selected
            self.shape_confirm.setChecked(False)
            self.trusted.setChecked(False)
        self._controls()

    def update_state(self, state):
        core = state.get("core", {}) if isinstance(state, dict) else {}
        epoch = core.get("epoch")
        if _identity(epoch) and epoch != self.epoch:
            self.epoch, self.snapshot, self._received_at, self.training_id = epoch, None, None, None
            self._accepted_serial = 0
            self._minimum_revision = 0
            self._generation_known, self._actor_generation, self._actor_token = False, None, None
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
            for row in self._unresolved.values():
                row["unknown"] = True
            self.shape_confirm.setChecked(False)
            self.trusted.setChecked(False)
        self.capable = core.get("capabilities", {}).get("causal_inference") is True
        self.recovery.update_state(state)
        self.adoption.update_state(state)
        self._render()

    def set_attached(self, attached):
        self.attached = bool(attached)
        self.recovery.set_attached(attached)
        self.adoption.set_attached(attached)
        if not self.attached:
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
        self._render_streams()
        self._controls()

    def set_presentation(self, _theme, language):
        self.language = language
        self.recovery.set_presentation(_theme, language)
        self.adoption.set_presentation(_theme, language)
        self.setTitle("확정 관측 CPU 추론" if language == "ko" else "Committed-input CPU inference")

    def _expanded(self, checked):
        self.content.setVisible(checked)
        if not checked:
            self.recovery.setChecked(False)
            self.adoption.setChecked(False)
        if checked:
            self.refresh()

    def _fresh(self):
        return bool(
            self.snapshot is not None
            and self._received_at is not None
            and time.monotonic() - self._received_at <= SNAPSHOT_TTL_S
            and self.snapshot["core_epoch"] == self.epoch
        )

    def _recovery_authority(self, summary):
        if summary["core_epoch"] == self.epoch:
            self._accept_generation(summary["generation"], summary["actor_token"])
            self._render()

    def _recovery_completed(self, proof):
        self._recovery_releases[proof["current"]] = proof["previous"]
        self._partition_recovered_history()
        self._render()

    def _adoption_completed(self, proof):
        if proof["core_epoch"] != self.epoch:
            return
        current = (self.epoch, proof["generation"], proof["new_actor_token"])
        self._adoption_releases[current] = {
            "previous_epoch": proof["prior_core_epoch"],
            "received_at": time.monotonic(),
        }
        # A receipt grants no revision or current control state. Require a GET
        # begun after this positive receipt before separating the old NN history.
        self.snapshot, self._received_at, self.training_id = None, None, None
        self.trusted.setChecked(False)
        self.shape_confirm.setChecked(False)
        self.state_policy.setCurrentIndex(0)
        self._requests = {
            tag: row for tag, row in self._requests.items() if row["context"] != "snapshot"
        }
        self.refresh()
        self._render()

    def _partition_recovered_history(self):
        current = (self.epoch, self._actor_generation, self._actor_token)
        previous = self._recovery_releases.get(current)
        adoption = self._adoption_releases.get(current)
        if (
            not self._fresh()
            or self.snapshot is None
            or "actor_generation" not in self.snapshot
            or _generation(self.snapshot) != current[1:]
        ):
            return
        for identity, row in tuple(self._unresolved.items()):
            cold_previous = bool(
                adoption
                and row["epoch"] == adoption["previous_epoch"]
                and self._received_at >= adoption["received_at"]
            )
            if (row["epoch"], row.get("generation"), row.get("actor_token")) == previous or (
                cold_previous
            ):
                self._historical[identity] = self._unresolved.pop(identity)

    def _accept_generation(self, generation, token):
        if self._generation_known:
            if self._actor_generation is not None and (
                generation is None or generation < self._actor_generation
            ):
                return False
            if generation == self._actor_generation and token != self._actor_token:
                raise ValueError("NN task identity changed without a new generation")
        if not self._generation_known or generation != self._actor_generation:
            self._generation_known = True
            self._actor_generation, self._actor_token = generation, token
            self.snapshot, self._received_at, self.training_id = None, None, None
            self._minimum_revision = 0
            self._deploy_selection = None
            if self._pending:
                self._pending["unknown"] = True
                self._unresolved[self._pending["id"]] = self._pending
                self._pending = None
            self.trusted.setChecked(False)
            self.shape_confirm.setChecked(False)
            self.state_policy.setCurrentIndex(0)
            self.recovery.observe_authority(self.epoch, generation, token)
        return True

    def _writable(self, *, protective=False):
        return bool(
            self.attached
            and self.capable
            and self._fresh()
            and (
                self._pending is None or protective and self._pending["kind"] == "predict_committed"
            )
            and (not self._unresolved or protective)
            and self.snapshot["ready"]
            and self.snapshot["storage_available"]
            and self.snapshot["actor_alive"]
            and self.snapshot["state"] not in {"recording_unconfirmed", "closed"}
        )

    def _deployment(self):
        return next(
            (
                row
                for row in (self.snapshot or {}).get("deployments", [])
                if row["id"] == self.deployments.currentData()
            ),
            None,
        )

    def _stream(self):
        name, symbol = self.stream_name.text().strip(), self.symbol.text().strip()
        if not _identity(name) or not is_valid_symbol(symbol):
            raise ValueError("유효한 stream과 종목을 명시하세요.")
        return name, "binance:" + self.market.currentText() + ":" + symbol

    def _matching_stream(self):
        try:
            stream, instrument = self._stream()
        except ValueError:
            return None
        return next(
            (
                row
                for row in (self.snapshot or {}).get("streams", [])
                if row["deployment_id"] == self.deployments.currentData()
                and row["stream"] == stream
                and row["instrument"] == instrument
            ),
            None,
        )

    def _deployment_changed(self, *_args):
        self._deploy_selection = self.deployments.currentData()
        self.state_policy.setCurrentIndex(0)
        self._render_streams()
        self._controls()

    def refresh(self):
        if (
            self.attached
            and self.capable
            and self.epoch
            and not any(row["context"] == "snapshot" for row in self._requests.values())
        ):
            self._request("GET", PATH, context="snapshot")

    def reconcile(self):
        item = self.operations.item(self.operations.currentRow(), 0)
        selected = None if item is None else item.text()
        target = (
            self._historical.get(selected)
            or self._unresolved.get(selected)
            or self._pending
            or next(iter(self._unresolved.values()), None)
            or next(iter(self._historical.values()), None)
        )
        if target and self.attached and self.capable:
            self._request("GET", PATH + "/" + target["id"], context="operation", manual=True)

    def _request(self, method, path, payload=None, *, context, manual=False):
        self._serial += 1
        tag = f"causal:{self._serial}:{context}"
        self._requests[tag] = {
            "context": context,
            "epoch": self.epoch,
            "generation": self._actor_generation,
            "serial": self._serial,
            "started_at": time.monotonic(),
            "manual": manual,
            "operation_id": (payload or {}).get("id")
            if method == "POST"
            else path.rsplit("/", 1)[-1],
        }
        self.client.request(method, path, payload, tag=tag)
        self._controls()

    def _submit(self, kind, parameters):
        protective = kind in {"pause", "retire"}
        if not self._writable(protective=protective):
            raise ValueError("현재 코어의 새 상태를 조회하고 미확인 요청을 먼저 확인하세요.")
        identity = uuid4().hex
        payload = {
            "id": identity,
            "kind": kind,
            "parameters": {
                "core_epoch": self.epoch,
                "revision": self.snapshot["revision"],
                **parameters,
            },
        }
        if self._pending is not None:
            self._unresolved[self._pending["id"]] = self._pending
        self._pending = {
            "id": identity,
            "kind": kind,
            "epoch": self.epoch,
            "unknown": False,
            "generation": self._actor_generation,
            "actor_token": self._actor_token,
        }
        self._received_at = None
        self._outcome = "요청 접수 확인 중 · " + identity
        self._request("POST", PATH, payload, context="operation")

    def activate(self):
        try:
            if (
                not self.training_id
                or not self.trusted.isChecked()
                or not self.shape_confirm.isChecked()
            ):
                raise ValueError(
                    "완료 학습을 선택하고 실제 계약과 Trusted Python 실행을 확인하세요."
                )
            self._submit(
                "activate",
                {
                    "training_job_id": self.training_id,
                    "deployment_id": self.deployments.currentData(),
                    "trusted_module": True,
                    "contract": self.contract(),
                    "bindings": self._binding_documents(),
                },
            )
        except (ValueError, TypeError) as error:
            self._outcome = str(error)
            self._controls()

    def reset_stream(self):
        try:
            stream, instrument = self._stream()
            if self.deployments.currentData() is None or self.state_policy.currentData() not in {
                "reset",
                "preserve",
            }:
                raise ValueError("배포와 reset/preserve 정책을 명시 선택하세요.")
            current = self._matching_stream()
            if self.state_policy.currentData() == "preserve" and (
                current is None or current["state"] != "ready" or current["manual_required"]
            ):
                raise ValueError(
                    "미확인/없는 stream은 preserve할 수 없습니다. 명시 reset이 필요합니다."
                )
            self._submit(
                "reset_stream",
                {
                    "deployment_id": self.deployments.currentData(),
                    "stream": stream,
                    "instrument": instrument,
                    "state_policy": self.state_policy.currentData(),
                },
            )
        except ValueError as error:
            self._outcome = str(error)
            self._controls()

    def predict(self):
        try:
            stream, instrument = self._stream()
            row, deployment = self._matching_stream(), self._deployment()
            if (
                row is None
                or row["state"] != "ready"
                or row["manual_required"]
                or row["pending_operation_id"] is not None
                or deployment is None
                or deployment["state"] != "ready"
                or deployment["manual_required"]
                or not self.snapshot
                or self.snapshot["state"] != "ready"
            ):
                raise ValueError("ready 배포와 명시 설정한 ready stream이 필요합니다.")
            opening = self.open_time.dateTime().toMSecsSinceEpoch()
            if opening < 0 or opening % 60000:
                raise ValueError("UTC 1분에 정렬된 확정분을 선택하세요.")
            self._submit(
                "predict_committed",
                {
                    "deployment_id": deployment["id"],
                    "stream": stream,
                    "instrument": instrument,
                    "open_time_ms": opening,
                    "timeout_ms": self.timeout.value(),
                },
            )
        except ValueError as error:
            self._outcome = str(error)
            self._controls()

    def _deployment_operation(self, kind):
        try:
            if self.deployments.currentData() is None:
                raise ValueError("저장된 배포를 선택하세요.")
            self._submit(kind, {"deployment_id": self.deployments.currentData()})
        except ValueError as error:
            self._outcome = str(error)
            self._controls()

    def load_stream(self):
        index = self.streams.currentRow()
        if not 0 <= index < len(self._visible_streams):
            return
        row = self._visible_streams[index]
        provider, market, symbol = row["instrument"].split(":")
        if provider != "binance" or market not in {"spot", "usdm"}:
            return
        self.stream_name.setText(row["stream"])
        self.market.setCurrentText(market)
        self.symbol.setText(symbol)
        self.state_policy.setCurrentIndex(0)
        if row["last_open_ms"] is not None:
            self.open_time.setDateTime(
                QDateTime.fromMSecsSinceEpoch(row["last_open_ms"] + 60000, QTimeZone(b"UTC"))
            )
        self._controls()

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request["epoch"] != self.epoch:
            return
        try:
            if request["context"] == "snapshot":
                snapshot = _snapshot(value)
                if (
                    snapshot["core_epoch"] != self.epoch
                    or request["serial"] < self._accepted_serial
                ):
                    return
                if "actor_generation" in snapshot:
                    generation, token = _generation(snapshot)
                    if not self._accept_generation(generation, token):
                        return
                elif self._generation_known:
                    raise ValueError("NN task generation is missing")
                if self.snapshot is not None and snapshot["revision"] < self.snapshot["revision"]:
                    return
                if snapshot["revision"] < self._minimum_revision:
                    return
                # The server sampled its clock during this request. Including
                # the complete round trip gives a conservative age/expiry bound.
                self.snapshot, self._received_at = snapshot, request["started_at"]
                self._accepted_serial = request["serial"]
                self._partition_recovered_history()
                for receipt in snapshot["operations"]:
                    self._receipt(receipt)
                self._render()
            else:
                if type(value) is not dict:
                    raise ValueError("Invalid committed operation response")
                if not _committed_scope(value):
                    raise ValueError("Operation is not a committed-inference receipt")
                receipt = _operation(value)
                if receipt["id"] != request["operation_id"]:
                    return
                self._receipt(receipt, acknowledge_unknown=request["manual"])
                self.refresh()
        except (ValueError, TypeError, KeyError, InvalidOperation) as error:
            self._outcome = str(error)
            self._received_at = None
            if self._pending:
                self._pending["unknown"] = True
            self._render_streams()
        self._controls()

    def _receipt(self, receipt, *, acknowledge_unknown=False):
        target = (
            self._pending
            if self._pending and receipt["id"] == self._pending["id"]
            else self._unresolved.get(receipt["id"]) or self._historical.get(receipt["id"])
        )
        if target is None or receipt["core_epoch"] != target["epoch"]:
            return

        def historical():
            target["unknown"] = True
            if receipt["id"] in self._historical:
                target["historical_state"] = receipt["state"]
            self._outcome = (
                "이전 NN 작업의 요청 기록 · 현재 작업 제어에는 사용하지 않습니다."
                if self.language == "ko"
                else "Previous NN task request record; it cannot control the current task."
            )
            if acknowledge_unknown and receipt["state"] in TERMINAL and receipt["durable"]:
                self._unresolved.pop(receipt["id"], None)
                if target is self._pending:
                    self._pending = None

        if receipt["core_epoch"] != self.epoch:
            historical()
            return

        if self._generation_known:
            if "operation_actor_generation" not in receipt:
                target["unknown"] = True
                return
            generation, token = _generation(
                receipt, "operation_actor_generation", "operation_actor_token"
            )
            if (generation, token) != (self._actor_generation, self._actor_token):
                historical()
                return
            if (generation, token) != (target.get("generation"), target.get("actor_token")):
                target["unknown"] = True
                return

        def finished():
            if self._pending is not None and self._pending["id"] == receipt["id"]:
                self._pending = None
            self._unresolved.pop(receipt["id"], None)

        if receipt["state"] in TERMINAL:
            if (
                receipt["core_epoch"] == self.epoch
                and receipt["durable"]
                and receipt["revision"] is not None
            ):
                self._minimum_revision = max(self._minimum_revision, receipt["revision"])
            if receipt["state"] == "complete" and receipt["durable"]:
                result = receipt.get("result") or {}
                if receipt["kind"] == "activate" and _identity(result.get("deployment_id")):
                    self._deploy_selection = result["deployment_id"]
                self._outcome = receipt["kind"] + " · complete · " + receipt["id"]
                if (
                    receipt["kind"] == "predict_committed"
                    and result.get("no_signal") is True
                    and result.get("feature") is None
                ):
                    self._outcome = "정상 보류 · no_signal · " + receipt["id"]
                finished()
                self.trusted.setChecked(False)
                self.shape_confirm.setChecked(False)
                self.state_policy.setCurrentIndex(0)
            elif receipt["state"] in {"failed", "cancelled"} and receipt["durable"]:
                self._outcome = (
                    receipt["kind"]
                    + " · "
                    + receipt["state"]
                    + " · "
                    + str(receipt.get("error"))[:512]
                )
                finished()
            elif receipt["state"] == "unknown" and receipt["durable"] and acknowledge_unknown:
                finished()
                self._outcome = (
                    "원래 요청의 unknown 기록 확인됨 · stream은 명시 reset이 필요합니다."
                )
                self.state_policy.setCurrentIndex(0)
                self.trusted.setChecked(False)
                self.shape_confirm.setChecked(False)
            else:
                target["unknown"] = True
                self._outcome = "요청 결과 미확인 · 동일 요청을 조회하세요."
        else:
            self._outcome = receipt["kind"] + " · " + receipt["state"]

    def _failed(self, tag, message):
        request = self._requests.pop(tag, None)
        if request is not None:
            self._outcome = str(message)[:512]
            self._received_at = None
            if request["context"] == "operation":
                target = (
                    self._pending
                    if self._pending and self._pending["id"] == request["operation_id"]
                    else self._unresolved.get(request["operation_id"])
                )
                if target is not None:
                    target["unknown"] = True
            self._render_streams()
            self._controls()

    def _tick(self):
        self.recovery.tick()
        self.adoption.tick()
        if self.isChecked() and self.isVisible() and self.attached and self.capable:
            interval = 1 if self._pending or self._unresolved else 3
            if (
                not self._fresh()
                or self._received_at is not None
                and time.monotonic() - self._received_at >= interval
            ):
                self.refresh()
        self._render_streams()
        self._controls()

    def _render(self):
        self.deployments.blockSignals(True)
        self.deployments.clear()
        self.deployments.addItem("새 배포 / New deployment", None)
        for row in (self.snapshot or {}).get("deployments", []):
            self.deployments.addItem(row["id"] + " · " + row["state"], row["id"])
        index = self.deployments.findData(self._deploy_selection)
        self.deployments.setCurrentIndex(max(0, index))
        self.deployments.blockSignals(False)
        self._render_streams()
        rows = list((self.snapshot or {}).get("operations", []))
        rows.extend(
            {
                "id": row["id"],
                "kind": row["kind"],
                "state": ("이전 작업 · " if self.language == "ko" else "Previous task · ")
                + row.get("historical_state", "unknown"),
            }
            for row in self._historical.values()
        )
        self.operations.setRowCount(len(rows))
        for index, row in enumerate(rows):
            for column, value in enumerate((row["id"], row["kind"], row["state"])):
                self.operations.setItem(index, column, QTableWidgetItem(value))
        self._controls()

    def _render_streams(self):
        self._visible_streams = [
            row
            for row in (self.snapshot or {}).get("streams", [])
            if row["deployment_id"] == self.deployments.currentData()
        ]
        now = (
            None
            if self._received_at is None or self.snapshot is None
            else self.snapshot["server_time_ms"]
            + int((time.monotonic() - self._received_at) * 1000)
        )
        self.streams.setRowCount(len(self._visible_streams))
        summaries = []
        for index, row in enumerate(self._visible_streams):
            feature = row.get("feature")
            valid = bool(
                self.attached
                and self._fresh()
                and feature
                and now is not None
                and feature["valid_until_ms"] > now
                and feature["accepted_at_ms"] <= self.snapshot["server_time_ms"]
                and row["state"] == "ready"
                and not row["manual_required"]
                and self.snapshot["state"] == "ready"
            )
            text = (
                ", ".join(f"{key}={value}" for key, value in feature["values"].items())
                + f" · until {feature['valid_until_ms']}"
                if valid
                else "만료/미준비 · 사용 가능한 feature 없음"
            )
            if (
                feature is None
                and row["state"] == "ready"
                and not row["manual_required"]
                and row.get("reason") == "no_signal"
            ):
                text = "정상 보류(no_signal) · 수치 feature 없음"
            summaries.append(text)
            for column, value in enumerate(
                (row["stream"] + " / " + row["instrument"], row["state"], text)
            ):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                self.streams.setItem(index, column, item)
        selected = self.streams.currentRow()
        summary_index = (
            selected if 0 <= selected < len(summaries) else 0 if len(summaries) == 1 else None
        )
        self.feature_summary.setText(summaries[summary_index] if summary_index is not None else "")
        feature = (
            self._visible_streams[selected].get("feature")
            if 0 <= selected < len(self._visible_streams)
            else None
        )
        self.lineage.setText(
            f"Request {feature['request_id']} · open {feature['open_time_ms']} · "
            f"decision {feature['decision_at_ms']} · accepted {feature['accepted_at_ms']}\n"
            f"input {feature['input_sha256']}\nframe {feature['frame_sha256']}"
            if feature
            else ""
        )

    def _controls(self, *_args):
        connected = self.attached and self.capable and self.epoch is not None
        writable = self._writable()
        self.refresh_button.setEnabled(
            connected and not any(row["context"] == "snapshot" for row in self._requests.values())
        )
        self.reconcile_button.setEnabled(
            connected and bool(self._pending or self._unresolved or self._historical)
        )
        self.selection_label.setText(
            "완료 학습: " + (self.training_id or "작업 목록에서 선택하세요")
        )
        self.activate_button.setEnabled(
            writable
            and self.training_id is not None
            and self.trusted.isChecked()
            and self.shape_confirm.isChecked()
        )
        deployment, stream = self._deployment(), self._matching_stream()
        configured = deployment is not None and deployment["state"] not in {
            "preparing",
            "retired",
            "failed",
        }
        try:
            self._stream()
            valid_stream = True
        except ValueError:
            valid_stream = False
        self.reset_button.setEnabled(
            writable
            and configured
            and valid_stream
            and self.state_policy.currentData() in {"reset", "preserve"}
            and (
                self.state_policy.currentData() == "reset"
                or stream is not None
                and stream["state"] == "ready"
                and not stream["manual_required"]
            )
        )
        self.predict_button.setEnabled(
            writable
            and configured
            and deployment["state"] == "ready"
            and not deployment["manual_required"]
            and stream is not None
            and stream["state"] == "ready"
            and not stream["manual_required"]
            and stream["pending_operation_id"] is None
            and self.snapshot["state"] == "ready"
        )
        protective = self._writable(protective=True)
        self.pause_button.setEnabled(protective and configured and deployment["state"] == "ready")
        self.retire_button.setEnabled(
            protective and deployment is not None and deployment["state"] != "retired"
        )
        self.load_stream_button.setEnabled(
            0 <= self.streams.currentRow() < len(self._visible_streams)
        )
        state = (self.snapshot or {}).get("state", "unavailable")
        fresh = "현재 상태" if self._fresh() else "상태 재조회 필요"
        pending = " · 미확인/진행 요청 있음" if self._pending or self._unresolved else ""
        self.status.setText(f"CPU · {state} · {fresh}{pending}\n{self._outcome}")
        self.evidence_status.setText(
            evidence_status(
                (self.snapshot or {}).get("evidence_storage"),
                current=self.attached and self.capable and self._fresh(),
                language=self.language,
            )
        )
