"""Structured CPU shadow controls; no NN module, artifact or database imports."""

# Imports
import json
import math
import re
import time
from copy import deepcopy
from hashlib import sha256
from uuid import uuid4

from PySide6.QtCore import QDateTime, Qt, QTimer, QTimeZone
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSizePolicy,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.domain.models import is_valid_symbol

PATH = "/api/shadow-models"
KINDS = {
    "inspect_training",
    "prepare_fixture",
    "activate",
    "reset_stream",
    "resume",
    "pause",
    "predict",
    "predict_raw",
    "refresh",
}
STATES = {"queued", "running", "waiting", "completed", "failed", "unknown", "expired"}
ACTIVE = {"queued", "running", "waiting"}


def _hex(value, size=32):
    return type(value) is str and re.fullmatch(f"[a-f0-9]{{{size}}}", value) is not None


def _json(value):
    text = json.dumps(value, allow_nan=False, sort_keys=True, separators=(",", ":"))
    if len(text.encode()) > 4_194_304:
        raise ValueError("Shadow response exceeds GUI capacity")
    return text


def _operation(value):
    if (
        type(value) is not dict
        or type(value.get("id")) is not str
        or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value["id"]) is None
        or value.get("kind") not in KINDS
        or value.get("state") not in STATES
        or type(value.get("parameters")) is not dict
        or not _hex(value.get("execution_id"))
        or type(value.get("operation_core_epoch", value.get("core_epoch"))) is not str
    ):
        raise ValueError("Invalid shadow operation identity/state")
    _json(value)
    return deepcopy(value)


def _snapshot(value):
    if (
        type(value) is not dict
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or value.get("scope") != "paper_shadow_only"
        or type(value.get("core_epoch")) is not str
        or value.get("state") not in {"ready", "starting", "unavailable"}
        or type(value.get("closed")) is not bool
        or type(value.get("operations")) is not list
        or len(value["operations"]) > 128
        or type(value.get("candidates")) is not list
        or len(value["candidates"]) > 128
    ):
        raise ValueError("Invalid shadow snapshot")
    result = deepcopy(value)
    result["operations"] = [_operation(row) for row in value["operations"]]
    if len({row["id"] for row in result["operations"]}) != len(result["operations"]):
        raise ValueError("Duplicate shadow operation")
    for row in result["candidates"]:
        if (
            type(row) is not dict
            or not _hex(row.get("candidate_id"), 64)
            or not _hex(row.get("training_job_id"))
            or type(row.get("fixture_verified")) is not bool
        ):
            raise ValueError("Invalid cached shadow candidate")
    if len({row["candidate_id"] for row in result["candidates"]}) != len(result["candidates"]):
        raise ValueError("Duplicate cached shadow candidate")
    deployment = value.get("deployment")
    if deployment is not None:
        if type(deployment) is not dict or type(deployment.get("document")) is not dict:
            raise ValueError("Invalid shadow deployment")
        doc = deployment["document"]
        if (
            doc.get("scope") != "paper_shadow_only"
            or type(doc.get("revision")) is not int
            or doc["revision"] < 0
            or type(doc.get("paused")) is not bool
            or type(doc.get("streams")) is not dict
            or len(doc["streams"]) > 128
        ):
            raise ValueError("Invalid shadow deployment revision/streams")
        if doc.get("candidate") is not None and (
            type(doc["candidate"]) is not dict or not _hex(doc["candidate"].get("candidate_id"), 64)
        ):
            raise ValueError("Invalid active shadow candidate")
        for key, row in doc["streams"].items():
            if (
                type(key) is not str
                or "|" not in key
                or type(row) is not dict
                or type(row.get("sequence")) is not int
                or row["sequence"] < 0
                or row.get("health") not in {"ready", "blocked"}
                or type(row.get("reason")) is not str
                or row.get("open_ms") is not None
                and type(row["open_ms"]) is not int
            ):
                raise ValueError("Invalid shadow stream")
    _json(result)
    return result


class ShadowControls(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch, self.snapshot = None, None
        self.attached, self.capable = bool(client.attached), False
        self.research_available, self._last_outcome = True, None
        self.training_id, self._selection_version = None, 0
        self.metadata, self._metadata_epoch, self._loaded_training = None, None, None
        self._metadata_source = "training"
        self._serial, self._accepted_serial, self._form_version = 0, 0, 0
        self._requests, self._operations = {}, {}
        self._pending, self._grid_signature = None, None
        self._dataset_source = ""
        self._labels, self._buttons = [], []
        self.setCheckable(True)
        self.setChecked(False)
        root = QVBoxLayout(self)
        self.content = QWidget()
        root.addWidget(self.content)
        layout = QVBoxLayout(self.content)
        self.hint, self.status, self.selection_label = self._label(), self._label(), self._label()
        for label in (self.hint, self.status, self.selection_label):
            layout.addWidget(label)
        top = QGridLayout()
        self.inspect_button = self._button(
            "선택 완료 학습 계약 조회", "Inspect selected completed training", self.inspect_training
        )
        self.refresh_button = self._button("상태 조회", "Read state", self.refresh)
        self.reconcile_button = self._button(
            "미확인 요청 확인", "Acknowledge unknown request", self.reconcile
        )
        top.addWidget(self.inspect_button, 0, 0)
        top.addWidget(self.refresh_button, 0, 1)
        top.addWidget(self.reconcile_button, 1, 0, 1, 2)
        layout.addLayout(top)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        self._build_candidate()
        self._build_input()
        self._build_stream()
        self._build_results()
        self._build_raw_input()
        self.toggled.connect(self._expanded)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        if hasattr(client, "stateReceived"):
            client.stateReceived.connect(self.update_state)
        self.timer = QTimer(self)
        self.timer.setInterval(3000)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.set_presentation("light", "ko")
        self.content.hide()

    def _label(self, ko=None, en=None):
        label = QLabel()
        label.setWordWrap(True)
        label.setTextFormat(Qt.TextFormat.PlainText)
        if ko is not None:
            self._labels.append((label, ko, en))
        return label

    def _button(self, ko, en, slot):
        button = QPushButton()
        self._buttons.append((button, ko, en))
        button.clicked.connect(slot)
        return button

    def _row(self, form, ko, en, widget):
        form.addRow(self._label(ko, en), widget)

    @staticmethod
    def _table(columns, *, editable=False):
        table = QTableWidget(0, columns)
        if not editable:
            table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setMinimumHeight(100)
        table.setMaximumHeight(210)
        return table

    def _build_candidate(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        self.metadata_label = self._label()
        layout.addWidget(self.metadata_label)
        self.metadata_details = QGroupBox()
        self.metadata_details.setCheckable(True)
        self.metadata_details.setChecked(False)
        details_layout = QVBoxLayout(self.metadata_details)
        self.metadata_table = self._table(2)
        self.metadata_table.setHorizontalHeaderLabels(["Field", "Actual saved value"])
        details_layout.addWidget(self.metadata_table)
        self.metadata_table.hide()
        self.metadata_details.toggled.connect(self.metadata_table.setVisible)
        layout.addWidget(self.metadata_details)
        self.apply_metadata_button = self._button(
            "조회한 계약을 폼에 적용", "Apply inspected contract to form", self.apply_metadata
        )
        layout.addWidget(self.apply_metadata_button)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.schema = QLineEdit()
        self.schema.setReadOnly(True)
        self.features = QLineEdit()
        self.features.setPlaceholderText("Declared feature order, comma separated")
        self.lookback = QSpinBox()
        self.lookback.setRange(1, 256)
        self.input_mode = QComboBox()
        for text, mode in (
            ("[batch]", "scalar"),
            ("[batch, features]", "vector"),
            ("[batch, lookback, features]", "sequence"),
        ):
            self.input_mode.addItem(text, mode)
        self.output_width = QSpinBox()
        self.output_width.setRange(1, 256)
        self.recurrent = QCheckBox()
        self.state_shape, self.state_version = QLineEdit(), QLineEdit()
        self.state_shape.setPlaceholderText("1, 1, hidden_size")
        self.state_version.setPlaceholderText("explicit module state version")
        self.no_signal = QCheckBox()
        for ko, en, widget in (
            ("실제 schema ID", "Actual schema ID", self.schema),
            ("특성 순서", "Feature order", self.features),
            ("입력 봉 수", "Lookback", self.lookback),
            ("입력 shape", "Input shape", self.input_mode),
            ("출력 폭", "Output width", self.output_width),
            ("RNN 상태", "RNN state", self.recurrent),
            ("상태 shape", "State shape", self.state_shape),
            ("상태 버전", "State version", self.state_version),
        ):
            self._row(form, ko, en, widget)
        form.addRow(self.no_signal)
        self.shape_confirm = QCheckBox()
        self.trusted = QCheckBox()
        self._labels.extend(
            [
                (self.recurrent, "명시 RNN 상태 이어받기", "Carry explicit RNN state"),
                (
                    self.no_signal,
                    "전체 null 출력을 정상 보류로 허용",
                    "Allow all-null output as no signal",
                ),
                (
                    self.shape_confirm,
                    "입력 shape·특성 계약을 확인했습니다",
                    "I confirm the shape / feature contract",
                ),
                (
                    self.trusted,
                    "Trusted Python을 연구 child에서 실행합니다",
                    "Execute trusted Python in a research child",
                ),
            ]
        )
        form.addRow(self.shape_confirm)
        form.addRow(self.trusted)
        layout.addLayout(form)
        self.fixture_button = self._button(
            "후보 동결 · fixture 검증 실행",
            "Freeze candidate · run fixture validation",
            self.prepare_fixture,
        )
        layout.addWidget(self.fixture_button)
        self.candidates = QComboBox()
        layout.addWidget(self.candidates)
        candidate_actions = QGridLayout()
        self.inspect_candidate_button = self._button(
            "선택 후보의 학습 계약 조회",
            "Inspect selected candidate training",
            self.inspect_candidate,
        )
        self.activate_button = self._button(
            "선택 검증 후보 명시 활성화", "Explicitly activate verified candidate", self.activate
        )
        candidate_actions.addWidget(self.inspect_candidate_button, 0, 0)
        candidate_actions.addWidget(self.activate_button, 1, 0)
        layout.addLayout(candidate_actions)
        self.tabs.addTab(page, "Candidate")
        for edit in (self.features, self.state_shape, self.state_version):
            edit.textEdited.connect(self._edited)
        for spin in (self.lookback, self.output_width):
            spin.valueChanged.connect(self._edited)
        self.input_mode.currentIndexChanged.connect(self._edited)
        for check in (self.recurrent, self.no_signal, self.shape_confirm, self.trusted):
            check.toggled.connect(self._edited)
        self.candidates.currentIndexChanged.connect(self._candidate_selection_changed)

    def _build_input(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.addWidget(
            self._label(
                "직접 준비한 수치 입력입니다. 별도의 원시 데이터 탭에서 "
                "회고 전처리를 요청할 수 있습니다. "
                "예제 입력은 합성이며 실관측이라고 취급하지 않습니다.",
                "Manual prepared values. "
                "Use the separate raw-data tab for retrospective preprocessing. "
                "Example inputs are synthetic, never observed market data.",
            )
        )
        actions = QGridLayout()
        self.build_input_button = self._button(
            "계약으로 빈 입력표 만들기", "Rebuild empty input table from contract", self.build_input
        )
        self.example_button = self._button(
            "합성 예제 수치 넣기", "Fill explicit synthetic example values", self.fill_example
        )
        actions.addWidget(self.build_input_button, 0, 0)
        actions.addWidget(self.example_button, 1, 0)
        layout.addLayout(actions)
        self.input_table = self._table(1, editable=True)
        self.input_table.itemChanged.connect(self._edited)
        layout.addWidget(self.input_table)
        self.input_hint = self._label()
        layout.addWidget(self.input_hint)
        self.tabs.addTab(page, "Prepared input")

    def _build_stream(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        self.active_label = self._label()
        layout.addWidget(self.active_label)
        self.streams = QComboBox()
        layout.addWidget(self.streams)
        self.load_stream_button = self._button(
            "선택 stream의 다음 분 불러오기", "Load selected stream next minute", self.load_stream
        )
        layout.addWidget(self.load_stream_button)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.stream_name = QLineEdit("gui-preview")
        self.market = QComboBox()
        self.market.addItems(["usdm", "spot"])
        self.symbol = QLineEdit("BTCUSDT")
        self.open_time = QDateTimeEdit()
        self.open_time.setTimeZone(QTimeZone(b"UTC"))
        self.open_time.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
        self.open_time.setCalendarPopup(True)
        stamp = (time.time_ns() // 1_000_000 // 60000 - 1) * 60000
        self.open_time.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
        self.deadline = QSpinBox()
        self.deadline.setRange(1, 60)
        self.deadline.setValue(15)
        for ko, en, widget in (
            ("Stream 이름", "Stream name", self.stream_name),
            ("시장", "Market", self.market),
            ("종목", "Symbol", self.symbol),
            ("입력 확정 분", "Closed input minute", self.open_time),
            ("기한 (초)", "Deadline (seconds)", self.deadline),
        ):
            self._row(form, ko, en, widget)
        layout.addLayout(form)
        actions = QGridLayout()
        self.reset_button = self._button(
            "Stream 명시 초기화", "Explicitly reset stream", self.reset_stream
        )
        self.resume_button = self._button("Shadow 수동 재개", "Manually resume shadow", self.resume)
        self.pause_button = self._button("Shadow 정지", "Pause shadow", self.pause)
        self.predict_button = self._button(
            "준비 입력으로 Shadow 예측", "Predict prepared input in shadow", self.predict
        )
        for index, button in enumerate(
            (self.reset_button, self.resume_button, self.pause_button, self.predict_button)
        ):
            actions.addWidget(button, index // 2, index % 2)
        layout.addLayout(actions)
        self.tabs.addTab(page, "Shadow stream")
        for edit in (self.stream_name, self.symbol):
            edit.textEdited.connect(self._edited)
        self.market.currentIndexChanged.connect(self._edited)
        self.open_time.dateTimeChanged.connect(self._edited)
        self.deadline.valueChanged.connect(self._edited)

    def _build_results(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        self.output_label = self._label()
        self.output = self._table(2)
        self.output.setHorizontalHeaderLabels(["Index", "Value"])
        self.operations = self._table(3)
        self.operations.setHorizontalHeaderLabels(["Operation", "Kind", "State / error"])
        self.resolve_owner_button = self._button(
            "코어에서 미확인 작업 재조회",
            "Explicitly recheck unknown jobs in core",
            self.refresh_unknown_jobs,
        )
        self.show_result_button = self._button(
            "선택 작업의 저장 예측 보기",
            "View selected stored prediction",
            self.show_stored_prediction,
        )
        layout.addWidget(self.output_label)
        layout.addWidget(self.output)
        layout.addWidget(self.operations)
        layout.addWidget(self.show_result_button)
        layout.addWidget(self.resolve_owner_button)
        self.tabs.addTab(page, "Results / operations")
        self.operations.itemSelectionChanged.connect(self._controls)

    def _build_raw_input(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.addWidget(
            self._label(
                "원본 dataset manifest 또는 폴더에서 모듈이 입력을 전처리합니다. "
                "CSV에는 실제 수신 시각이 없어 회고 Shadow 전용입니다. "
                "실시간 인지/TS 주문이 아니며 MC·보간 dataset은 거절합니다. "
                "과거 입력 분과 실제 snapshot/as-of를 구분합니다.",
                "The module preprocesses a source dataset manifest or folder. "
                "CSV lacks actual received "
                "times, so this is retrospective shadow only, never live knowledge or TS orders. "
                "Monte Carlo/interpolated datasets are rejected. "
                "Reference time and actual snapshot "
                "availability/as-of remain separate.",
            )
        )
        self.raw_dataset = QLineEdit()
        self.raw_dataset.setMaxLength(4096)
        self.raw_dataset.setPlaceholderText("Source dataset.json or dataset folder; not a CSV file")
        layout.addWidget(self.raw_dataset)
        actions = QGridLayout()
        self.raw_file_button = self._button(
            "Manifest 선택", "Choose manifest", self._browse_raw_manifest
        )
        self.raw_folder_button = self._button(
            "Dataset 폴더", "Dataset folder", self._browse_raw_folder
        )
        self.raw_source_button = self._button(
            "NN 페이지 선택 경로 가져오기", "Use NN-page selected dataset", self.use_dataset_source
        )
        actions.addWidget(self.raw_file_button, 0, 0)
        actions.addWidget(self.raw_folder_button, 0, 1)
        actions.addWidget(self.raw_source_button, 1, 0, 1, 2)
        layout.addLayout(actions)
        self.raw_context, self.raw_support = self._label(), self._label()
        layout.addWidget(self.raw_context)
        layout.addWidget(self.raw_support)
        self.raw_confirm = QCheckBox()
        self._buttons.append(
            (self.raw_confirm, "회고 추론 전용임에 동의", "Confirm retrospective shadow only")
        )
        layout.addWidget(self.raw_confirm)
        self.raw_predict_button = self._button(
            "원시 데이터로 회고 추론", "Predict raw data retrospectively", self.predict_raw
        )
        layout.addWidget(self.raw_predict_button)
        layout.addStretch()
        self.tabs.addTab(page, "Raw data / retrospective inference")
        self.raw_dataset.textChanged.connect(self._raw_dataset_changed)
        self.raw_confirm.toggled.connect(self._edited)
        self.raw_lineage = self._label()
        self.raw_lineage.setSizePolicy(QSizePolicy.Policy.Ignored, QSizePolicy.Policy.Preferred)
        self.raw_lineage.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        self.raw_lineage.hide()
        self.tabs.widget(3).layout().insertWidget(1, self.raw_lineage)

    def set_dataset_source(self, value):
        # The parent selection is a suggestion, never an asynchronous draft replacement.
        self._dataset_source = value if type(value) is str and len(value) <= 4096 else ""
        self.raw_source_button.setToolTip(self._dataset_source)
        self._controls()

    def use_dataset_source(self):
        if self._dataset_source:
            self.raw_dataset.setText(self._dataset_source)

    def _browse_raw_manifest(self):
        name, _ = QFileDialog.getOpenFileName(
            self, "Source dataset manifest", "", "Dataset manifest (*.json)"
        )
        if name:
            self.raw_dataset.setText(name)

    def _browse_raw_folder(self):
        name = QFileDialog.getExistingDirectory(self, "Source dataset folder")
        if name:
            self.raw_dataset.setText(name)

    def _raw_dataset_changed(self):
        self.raw_confirm.setChecked(False)
        self._edited()

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_presentation(self, _theme, language):
        self.language = language if language in {"ko", "en"} else "ko"
        self.setTitle(
            self.text("NN 후보 · 배포 · Shadow 추론", "NN CANDIDATES · ACTIVATION · SHADOW")
        )
        self.metadata_details.setTitle(
            self.text(
                "실제 저장 모듈 · 학습 설정 · checkpoint", "Saved module / config / checkpoint"
            )
        )
        self.hint.setText(
            self.text(
                "CPU 연구 child의 준비 입력/회고 원시 입력 Shadow 전용입니다. "
                "학습 완료로 자동 활성화하지 않고, "
                "TS 주문에 반영하지 않습니다. 외부 모듈은 GUI에서 import하지 않습니다.",
                "CPU research-child prepared-input/retrospective-raw shadow only. "
                "Training never auto-activates a model; "
                "outputs never place TS orders. The GUI never imports the external module.",
            )
        )
        for widget, ko, en in self._labels:
            widget.setText(self.text(ko, en))
        for widget, ko, en in self._buttons:
            widget.setText(self.text(ko, en))
        for index, words in enumerate(
            (
                ("후보 · 명시 적용", "Candidate / activation"),
                ("준비 입력", "Prepared input"),
                ("Shadow stream", "Shadow stream"),
                ("결과 · 작업", "Results / operations"),
                ("원시 데이터 → 회고 추론", "Raw data / retrospective inference"),
            )
        ):
            self.tabs.setTabText(index, self.text(*words))
        self._render()

    def _edited(self, *_args):
        self._form_version += 1
        self._controls()

    def _candidate_selection_changed(self, *_args):
        self._selection_version += 1
        self._controls()

    def set_training_selection(self, identity, *, core_epoch):
        selected = (
            identity
            if _hex(identity) and self.epoch is not None and core_epoch == self.epoch
            else None
        )
        if selected != self.training_id:
            self.training_id = selected
            self._selection_version += 1
        self._render()

    def update_state(self, state):
        core = state.get("core") or {}
        epoch = core.get("epoch") or state.get("core_epoch")
        changed = type(epoch) is str and bool(epoch) and epoch != self.epoch
        if changed:
            self.epoch, self.snapshot = epoch, None
            self.training_id = None
            self._accepted_serial = 0
            self._requests.clear()
            self._operations.clear()
            self._metadata_epoch = None
            self._last_outcome = None
            self._selection_version += 1
            if self._pending:
                self._pending["unknown"] = True
            self.output.setRowCount(0)
            self.output_label.clear()
        self.capable = (core.get("capabilities") or {}).get("shadow_inference") is True
        self._render()
        if changed and self.isChecked():
            self.refresh()

    def set_attached(self, attached):
        previous, self.attached = self.attached, bool(attached)
        if not self.attached:
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
        self._render()
        if self.attached and not previous and self.isChecked():
            self.refresh()

    def _expanded(self, checked):
        self.content.setVisible(checked)
        if checked:
            self.refresh()

    def set_research_available(self, available):
        self.research_available = available is True
        self._controls()

    def _tick(self):
        if self.isChecked() and self.isVisible():
            self.refresh()

    def _ready(self):
        return self.attached and self.capable and self.epoch is not None

    def _document(self):
        return (
            (self.snapshot or {}).get("deployment", {}).get("document", {})
            if (self.snapshot or {}).get("deployment")
            else {}
        )

    def _active_candidate(self):
        reference = self._document().get("candidate") or {}
        return next(
            (
                row
                for row in (self.snapshot or {}).get("candidates", [])
                if row["candidate_id"] == reference.get("candidate_id")
            ),
            None,
        )

    def _controls(self):
        known = self._ready() and self.snapshot is not None
        writable = (
            known
            and self.snapshot["state"] == "ready"
            and not self.snapshot["closed"]
            and self._pending is None
        )
        valid_metadata = self.metadata is not None and self._metadata_epoch == self.epoch
        device = (self.metadata or {}).get("device")
        cpu_metadata = device == "cpu" or type(device) is dict and device.get("used") == "cpu"
        self.inspect_button.setEnabled(writable and self.training_id is not None)
        self.refresh_button.setEnabled(self._ready())
        self.reconcile_button.setEnabled(
            self._ready() and self._pending is not None and self._pending.get("unknown", False)
        )
        self.apply_metadata_button.setEnabled(valid_metadata and self._pending is None)
        selected = self.candidates.currentData()
        candidate = next(
            (
                r
                for r in (self.snapshot or {}).get("candidates", [])
                if r["candidate_id"] == selected
            ),
            None,
        )
        source_matches = (
            self._metadata_source == "training"
            and self._loaded_training == self.training_id
            or self._metadata_source == "candidate"
            and candidate is not None
            and self._loaded_training == candidate["training_job_id"]
        )
        self.fixture_button.setEnabled(
            writable
            and self.research_available
            and valid_metadata
            and cpu_metadata
            and source_matches
            and self._loaded_training == self.metadata.get("training_job_id")
            and self.shape_confirm.isChecked()
            and self.trusted.isChecked()
        )
        self.inspect_candidate_button.setEnabled(writable and candidate is not None)
        self.activate_button.setEnabled(
            writable and candidate is not None and candidate["fixture_verified"]
        )
        active = self._active_candidate()
        self.reset_button.setEnabled(writable and active is not None)
        self.resume_button.setEnabled(
            writable and active is not None and self._document().get("paused") is True
        )
        self.pause_button.setEnabled(
            writable and active is not None and self._document().get("paused") is False
        )
        self.predict_button.setEnabled(
            writable
            and self.research_available
            and active is not None
            and self._document().get("paused") is False
            and valid_metadata
            and self._loaded_training == active["training_job_id"]
        )
        capabilities = (self.metadata or {}).get("module", {}).get("capabilities")
        raw_supported = (
            valid_metadata
            and active is not None
            and self._loaded_training == active["training_job_id"]
            and type(capabilities) is dict
            and capabilities.get("prepare_inference") is True
        )
        self.raw_predict_button.setEnabled(
            writable
            and self.research_available
            and active is not None
            and self._document().get("paused") is False
            and raw_supported
            and self.raw_confirm.isChecked()
            and bool(self.raw_dataset.text().strip())
        )
        self.raw_source_button.setEnabled(bool(self._dataset_source))
        self.raw_support.setText(
            self.text(
                "활성 모듈의 원시 전처리 지원 확인됨 · 코어가 실제 입력/계약을 최종 검증합니다"
                if raw_supported
                else "활성 후보 계약을 조회·적용하세요. 원시 전처리 미지원 모듈은 "
                "새 지원 모듈로 재학습·후보 검증해야 합니다. 기존 준비 입력 방식은 유지됩니다.",
                "Active module declares raw preprocessing; the core validates actual input/contract"
                if raw_supported
                else "Inspect/apply the active candidate. A module without raw preparation "
                "needs retraining and candidate verification with a supporting module. "
                "Prepared input remains available.",
            )
        )
        try:
            name, instrument = self._stream()
            stream = self._document().get("streams", {}).get(name + "|" + instrument)
            next_sequence = stream["sequence"] + 1 if stream is not None else "reset required"
        except (TypeError, ValueError):
            name, instrument, next_sequence = "?", "?", "reset required"
        self.raw_context.setText(
            f"Shadow stream: {name} · {instrument} · next sequence={next_sequence}\n"
            f"reference open (UTC ms)={self.open_time.dateTime().toMSecsSinceEpoch()} · "
            f"deadline={self.deadline.value()}s · "
            + self.text(
                "위 값은 Shadow stream 탭에서 명시 설정합니다",
                "Edit these values in the Shadow stream tab",
            )
        )
        self.resolve_owner_button.setEnabled(
            known and (self._pending is None or self._pending.get("unknown", False))
        )
        selected = self.operations.selectionModel().selectedRows()
        item = self.operations.item(selected[0].row(), 0) if len(selected) == 1 else None
        result_row = self._operations.get(item.text()) if item is not None else None
        self.show_result_button.setEnabled(
            result_row is not None
            and result_row["kind"] in {"predict", "predict_raw"}
            and result_row["state"] == "completed"
            and type(result_row.get("result")) is dict
        )

    def _render(self):
        self.selection_label.setText(
            self.text("선택 완료 학습: ", "Selected completed training: ")
            + (self.training_id or "—")
        )
        selected = self.candidates.currentData()
        self.candidates.blockSignals(True)
        self.candidates.clear()
        self.candidates.addItem(self.text("후보 선택", "Select candidate"), None)
        for row in (self.snapshot or {}).get("candidates", []):
            self.candidates.addItem(
                f"{row['candidate_id'][:16]} · verified={row['fixture_verified']} · "
                f"train={row['training_job_id'][:8]}",
                row["candidate_id"],
            )
        self.candidates.setCurrentIndex(max(0, self.candidates.findData(selected)))
        self.candidates.blockSignals(False)
        stream = self.streams.currentData()
        self.streams.clear()
        self.streams.addItem(self.text("저장 stream 선택", "Select saved stream"), None)
        for key, row in self._document().get("streams", {}).items():
            self.streams.addItem(
                f"{key} · seq={row['sequence']} · {row['health']} / {row['reason']}", key
            )
        self.streams.setCurrentIndex(max(0, self.streams.findData(stream)))
        active = self._document().get("candidate") or {}
        active_id = active.get("candidate_id")
        self.active_label.setText(
            self.text("활성 후보: ", "Active candidate: ")
            + (active_id[:16] + "…" if active_id else "—")
            + f" · paused={self._document().get('paused', '?')} · "
            + f"revision={self._document().get('revision', '?')}"
        )
        self.active_label.setToolTip(active_id or "")
        selected_rows = self.operations.selectionModel().selectedRows()
        selected_item = (
            self.operations.item(selected_rows[0].row(), 0) if len(selected_rows) == 1 else None
        )
        selected_operation = selected_item.text() if selected_item else None
        self.operations.blockSignals(True)
        self.operations.setRowCount(len(self._operations))
        self.operations.clearSelection()
        self.operations.setCurrentCell(-1, -1)
        for index, row in enumerate(self._operations.values()):
            for column, text in enumerate(
                (row["id"], row["kind"], row["state"] + " / " + str(row.get("error") or ""))
            ):
                item = QTableWidgetItem(text)
                item.setToolTip(text)
                self.operations.setItem(index, column, item)
            if row["id"] == selected_operation:
                self.operations.selectRow(index)
        self.operations.blockSignals(False)
        if not self.attached:
            self.status.setText(
                self.text(
                    "연결 끊김 · 표시 상태는 오래됐을 수 있습니다",
                    "Disconnected · displayed state may be stale",
                )
            )
        elif not self.capable:
            self.status.setText(
                self.text("코어 Shadow 기능 미지원", "Core shadow workflow unavailable")
            )
        elif self._pending:
            self.status.setText(
                self.text(
                    "결과 미확인 · 같은 ID의 상태만 조회하며 자동 재전송하지 않습니다",
                    "Outcome unconfirmed · checking the same ID, never automatically reposting",
                )
                if self._pending.get("unknown")
                else self.text("코어 작업 처리 중", "Core operation pending")
            )
        elif self.snapshot:
            self.status.setText(
                f"Shadow owner: {self.snapshot['state']} · closed={self.snapshot['closed']}"
                + (" · " + self._last_outcome if self._last_outcome else "")
            )
        self._controls()

    def _request(self, method, path, payload=None, *, context):
        self._serial += 1
        tag = f"shadow-gui:{id(self)}:{self._serial}"
        self._requests[tag] = (self.epoch, self._serial, context)
        self.client.request(method, path, payload, tag=tag)

    def refresh(self, *, acknowledge=False):
        if not self._ready():
            return
        if any(entry[0] == self.epoch for entry in self._requests.values()):
            return
        if self._pending and not acknowledge:
            self._request("GET", PATH + "/" + self._pending["id"], context="operation")
        else:
            self._request("GET", PATH, context="acknowledge" if acknowledge else "snapshot")

    def reconcile(self):
        if self.reconcile_button.isEnabled():
            self.refresh(acknowledge=True)

    def _submit(self, kind, parameters, *, metadata_target=None, metadata_source="training"):
        if not self._ready() or self._pending is not None:
            return
        identity = uuid4().hex
        self._last_outcome = None
        self._pending = {
            "id": identity,
            "kind": kind,
            "parameters": deepcopy(parameters),
            "epoch": self.epoch,
            "selection": self._selection_version,
            "form": self._form_version,
            "metadata_target": metadata_target,
            "metadata_source": metadata_source,
            "unknown": False,
        }
        self._render()
        self._request(
            "POST",
            PATH,
            {"id": identity, "kind": kind, "parameters": parameters},
            context="operation",
        )

    def inspect_training(self):
        if self.inspect_button.isEnabled():
            self._submit(
                "inspect_training",
                {"training_job_id": self.training_id},
                metadata_target=self.training_id,
            )

    def inspect_candidate(self):
        if not self.inspect_candidate_button.isEnabled():
            return
        row = next(
            r
            for r in self.snapshot["candidates"]
            if r["candidate_id"] == self.candidates.currentData()
        )
        self._submit(
            "inspect_training",
            {"training_job_id": row["training_job_id"]},
            metadata_target=row["training_job_id"],
            metadata_source="candidate",
        )

    def apply_metadata(self):
        if not self.apply_metadata_button.isEnabled():
            return
        try:
            value = self.metadata
            inputs = value["module"].get("input") or {}
            defaults = value.get("contract_defaults")
            if type(inputs) is not dict or defaults is not None and type(defaults) is not dict:
                raise ValueError("Invalid actual module input declaration")
            contract = defaults or {
                "schema_id": inputs.get("schema_id"),
                "features": inputs.get("features", []),
                "lookback": value["config"].get("lookback", 1),
                "input_shape": None,
                "output_shape": [None],
                "state_shape": None,
                "state_version": None,
                "allow_no_signal": False,
            }
            features, lookback = contract["features"], contract["lookback"]
            if (
                type(contract["schema_id"]) is not str
                or not contract["schema_id"]
                or type(features) is not list
                or len(features) > 32
                or any(type(item) is not str or not item or len(item) > 128 for item in features)
                or len(set(features)) != len(features)
                or type(lookback) is not int
                or not 1 <= lookback <= 256
                or contract["schema_id"] != inputs.get("schema_id")
                or inputs.get("features") is not None
                and features != inputs["features"]
            ):
                raise ValueError("Contract exceeds GUI shape limits or lacks an actual schema")
            shape = contract["input_shape"]
            if shape is not None and (
                type(shape) is not list
                or not 1 <= len(shape) <= 3
                or shape[0] is not None
                or any(type(v) is not int or v <= 0 for v in shape[1:])
                or len(shape) == 1
                and (len(features) != 1 or lookback != 1)
                or len(shape) == 2
                and (lookback != 1 or shape[1] != len(features))
                or len(shape) == 3
                and shape[1:] != [lookback, len(features)]
            ):
                raise ValueError("Input shape needs a supported explicit GUI contract")
            mode = (
                "scalar"
                if shape == [None]
                else "vector"
                if shape is not None and len(shape) == 2
                else "sequence"
            )
            output = contract.get("output_shape")
            if (
                type(output) is not list
                or len(output) not in {1, 2}
                or output[0] is not None
                or len(output) == 2
                and (type(output[1]) is not int or not 1 <= output[1] <= 256)
            ):
                raise ValueError("Output shape requires a supported explicit GUI contract")
            state_shape, state_version = contract.get("state_shape"), contract.get("state_version")
            if state_shape is not None and (
                type(state_shape) is not list
                or not 1 <= len(state_shape) <= 5
                or any(type(v) is not int or not 1 <= v <= 4096 for v in state_shape)
                or math.prod(state_shape) > 100000
                or type(state_version) is not str
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", state_version) is None
                or (value["module"].get("capabilities") or {}).get("stateful_inference") is not True
            ):
                raise ValueError("Invalid declared recurrent state contract")
            if state_shape is None and state_version is not None:
                raise ValueError("State version requires a declared state shape")
            self.schema.setText(contract["schema_id"])
            self.features.setText(", ".join(features))
            self.features.setCursorPosition(0)
            self.features.setToolTip(", ".join(features))
            self.lookback.setValue(lookback)
            self.input_mode.setCurrentIndex(self.input_mode.findData(mode))
            self.output_width.setValue(1 if len(output) == 1 else output[1])
            self.recurrent.setChecked(state_shape is not None)
            self.recurrent.setEnabled(
                (value["module"].get("capabilities") or {}).get("stateful_inference") is True
            )
            self.state_shape.setText(", ".join(str(v) for v in (state_shape or [])))
            self.state_version.setText(state_version or "")
            self.no_signal.setChecked(contract.get("allow_no_signal") is True)
            self.shape_confirm.setChecked(False)
            self.trusted.setChecked(False)
            self._loaded_training = value["training_job_id"]
            self._form_version += 1
            self.status.setText(
                self.text(
                    "계약을 읽었습니다. shape를 확인하고 입력표를 명시 구성하세요.",
                    "Contract loaded. Confirm shape and explicitly rebuild the input table.",
                )
            )
        except (ValueError, TypeError, KeyError, IndexError) as error:
            self.status.setText(str(error))
        self._controls()

    def contract(self):
        features = tuple(part.strip() for part in self.features.text().split(","))
        if (
            not self.schema.text()
            or not 1 <= len(features) <= 32
            or any(not v or len(v) > 128 for v in features)
            or len(set(features)) != len(features)
        ):
            raise ValueError("Confirm the actual schema and unique ordered features")
        lookback, mode = self.lookback.value(), self.input_mode.currentData()
        if (
            mode == "scalar"
            and (len(features) != 1 or lookback != 1)
            or mode == "vector"
            and lookback != 1
        ):
            raise ValueError("Scalar/vector input requires lookback=1; scalar requires one feature")
        shape = (
            [None]
            if mode == "scalar"
            else [None, len(features)]
            if mode == "vector"
            else [None, lookback, len(features)]
        )
        state_shape, state_version = None, None
        if self.recurrent.isChecked():
            parts = [part.strip() for part in self.state_shape.text().split(",")]
            if (
                not parts
                or len(parts) > 5
                or any(re.fullmatch(r"[1-9][0-9]{0,3}", part) is None for part in parts)
            ):
                raise ValueError("State shape requires positive integer dimensions")
            state_shape, state_version = (
                [int(part) for part in parts],
                self.state_version.text().strip(),
            )
            if (
                any(v > 4096 for v in state_shape)
                or math.prod(state_shape) > 100000
                or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", state_version) is None
            ):
                raise ValueError("Confirm a bounded state shape and explicit state version")
        return {
            "schema_id": self.schema.text(),
            "features": list(features),
            "lookback": lookback,
            "input_shape": shape,
            "output_shape": [None]
            if self.output_width.value() == 1
            else [None, self.output_width.value()],
            "state_shape": state_shape,
            "state_version": state_version,
            "allow_no_signal": self.no_signal.isChecked(),
        }

    def build_input(self):
        try:
            contract = self.contract()
            mode = self.input_mode.currentData()
            rows, columns = (
                (contract["lookback"] if mode == "sequence" else 1),
                len(contract["features"]),
            )
            if rows * columns > 4096:
                raise ValueError("Prepared input exceeds the 4096-cell GUI limit")
            self.input_table.blockSignals(True)
            self.input_table.clear()
            self.input_table.setRowCount(rows)
            self.input_table.setColumnCount(columns)
            self.input_table.setHorizontalHeaderLabels(contract["features"])
            self.input_table.blockSignals(False)
            self._grid_signature = (mode, tuple(contract["features"]), contract["lookback"])
            self._form_version += 1
            self.input_hint.setText(
                self.text(
                    "빈 칸은0으로 채우지 않습니다. 준비 수치를 입력하거나 합성 예제를 선택하세요.",
                    "Blank cells never become zero. "
                    "Enter prepared values or explicitly fill a synthetic example.",
                )
            )
        except (TypeError, ValueError) as error:
            self.status.setText(str(error))

    def fill_example(self):
        if self._grid_signature is None:
            self.build_input()
        if self._grid_signature is None:
            return
        for row in range(self.input_table.rowCount()):
            for column in range(self.input_table.columnCount()):
                self.input_table.setItem(row, column, QTableWidgetItem(str(0.01 * (column + 1))))
        self.input_hint.setText(
            self.text(
                "합성 prepared-input 예제입니다 · 실관측/수익성 검증이 아닙니다",
                "Synthetic prepared-input example · not observed data or a profitability test",
            )
        )

    def batch(self):
        contract = self.contract()
        signature = (
            self.input_mode.currentData(),
            tuple(contract["features"]),
            contract["lookback"],
        )
        if self._grid_signature != signature:
            raise ValueError("Rebuild the input table after changing feature order/shape/lookback")
        rows = []
        for row in range(self.input_table.rowCount()):
            values = []
            for column in range(self.input_table.columnCount()):
                item = self.input_table.item(row, column)
                if item is None or not item.text().strip():
                    raise ValueError("Every prepared input cell needs an explicit finite value")
                value = float(item.text())
                if not math.isfinite(value):
                    raise ValueError("Prepared values must be finite")
                values.append(value)
            rows.append(values)
        return (
            [rows[0][0]]
            if signature[0] == "scalar"
            else [rows[0]]
            if signature[0] == "vector"
            else [rows]
        )

    def prepare_fixture(self):
        if not self.fixture_button.isEnabled():
            return
        try:
            self._submit(
                "prepare_fixture",
                {
                    "training_job_id": self._loaded_training,
                    "contract": self.contract(),
                    "fixture_batch": self.batch(),
                    "trusted_module": True,
                },
            )
        except (ValueError, TypeError) as error:
            self.status.setText(str(error))

    def activate(self):
        if self.activate_button.isEnabled():
            self._submit(
                "activate",
                {
                    "candidate_id": self.candidates.currentData(),
                    "expected_revision": self._document()["revision"],
                },
            )

    def _stream(self):
        name, symbol = self.stream_name.text().strip(), self.symbol.text().strip()
        if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", name) is None or not is_valid_symbol(symbol):
            raise ValueError("Enter a valid stream name and native instrument symbol")
        return name, f"binance:{self.market.currentText()}:{symbol}"

    def reset_stream(self):
        if not self.reset_button.isEnabled():
            return
        try:
            name, instrument = self._stream()
            self._submit(
                "reset_stream",
                {
                    "stream": name,
                    "instrument": instrument,
                    "expected_revision": self._document()["revision"],
                    "reason": "gui_explicit_reset",
                },
            )
        except ValueError as error:
            self.status.setText(str(error))

    def load_stream(self):
        key = self.streams.currentData()
        row = self._document().get("streams", {}).get(key)
        if row is None:
            return
        name, instrument = key.split("|", 1)
        provider, market, symbol = instrument.split(":")
        if provider != "binance" or market not in {"spot", "usdm"} or not is_valid_symbol(symbol):
            return
        self.stream_name.setText(name)
        self.market.setCurrentText(market)
        self.symbol.setText(symbol)
        if row["open_ms"] is not None:
            self.open_time.setDateTime(
                QDateTime.fromMSecsSinceEpoch(row["open_ms"] + 60000, QTimeZone(b"UTC"))
            )
        self._edited()

    def resume(self):
        if self.resume_button.isEnabled():
            self._submit("resume", {"expected_revision": self._document()["revision"]})

    def pause(self):
        if self.pause_button.isEnabled():
            self._submit(
                "pause",
                {"expected_revision": self._document()["revision"], "reason": "gui_user_pause"},
            )

    def predict(self):
        if not self.predict_button.isEnabled():
            return
        try:
            name, instrument = self._stream()
            row = self._document()["streams"].get(name + "|" + instrument)
            if row is None or row["health"] != "ready":
                raise ValueError("Explicitly initialize/reset a ready stream first")
            active = self._active_candidate()
            if active is None or active["training_job_id"] != self._loaded_training:
                raise ValueError("Inspect/apply the active candidate's training contract first")
            contract, batch = self.contract(), self.batch()
            opening = self.open_time.dateTime().toMSecsSinceEpoch()
            issued = time.time_ns() // 1_000_000
            if opening % 60000 or opening + 60000 > issued:
                raise ValueError("Select a closed, UTC-aligned input minute")
            digest = sha256(
                _json(
                    {
                        "scope": "manual_prepared_shadow",
                        "instrument": instrument,
                        "open_ms": opening,
                        "contract": contract,
                        "batch": batch,
                    }
                ).encode()
            ).hexdigest()
            self._submit(
                "predict",
                {
                    "expected_revision": self._document()["revision"],
                    "stream": name,
                    "instrument": instrument,
                    "sequence": row["sequence"] + 1,
                    "open_time_ms": opening,
                    "as_of_ms": issued,
                    "deadline_ms": issued + self.deadline.value() * 1000,
                    "input_digest": digest,
                    "batch": batch,
                    "schema_id": contract["schema_id"],
                    "features": contract["features"],
                    "lookback": contract["lookback"],
                },
            )
        except (ValueError, TypeError, KeyError) as error:
            self.status.setText(str(error))

    def refresh_unknown_jobs(self):
        if self._pending is None and self.resolve_owner_button.isEnabled():
            self._submit("refresh", {})
        elif self._pending and self._pending.get("unknown"):
            self.status.setText(
                self.text(
                    "먼저 현재 상태 확인으로 로컬 미확인 요청을 해제하세요",
                    "Acknowledge the local unknown request before an explicit owner recheck",
                )
            )

    def predict_raw(self):
        if not self.raw_predict_button.isEnabled():
            return
        try:
            dataset = self.raw_dataset.text().strip()
            if not dataset or len(dataset) > 4096 or any(char in dataset for char in "\x00\r\n"):
                raise ValueError("Select a source dataset manifest or folder")
            name, instrument = self._stream()
            stream = self._document()["streams"].get(name + "|" + instrument)
            if stream is None or stream["health"] != "ready":
                raise ValueError("Explicitly reset/load a ready stream first")
            opening = self.open_time.dateTime().toMSecsSinceEpoch()
            issued = time.time_ns() // 1_000_000
            if (
                opening < 0
                or opening % 60000
                or opening + 60000 > issued
                or stream["open_ms"] is not None
                and opening != stream["open_ms"] + 60000
            ):
                raise ValueError(
                    "Select the next closed UTC minute; load the stream to inspect its cursor"
                )
            sequence = stream["sequence"] + 1
            if not 1 <= sequence <= 2**63 - 1:
                raise ValueError("Shadow sequence capacity exceeded")
            self._submit(
                "predict_raw",
                {
                    "expected_revision": self._document()["revision"],
                    "stream": name,
                    "instrument": instrument,
                    "sequence": sequence,
                    "open_time_ms": opening,
                    "deadline_ms": issued + self.deadline.value() * 1000,
                    "dataset": dataset,
                    "retrospective_confirmed": True,
                },
            )
        except (ValueError, TypeError, KeyError) as error:
            self.status.setText(str(error))

    def _accept_operation(self, row):
        previous = self._operations.get(row["id"])
        if previous and previous["execution_id"] != row["execution_id"]:
            raise ValueError("Shadow operation attempt changed")
        if previous and previous["state"] == "completed" and row["state"] in ACTIVE:
            return
        pending = self._pending
        if (
            pending
            and row["id"] == pending["id"]
            and (row["kind"] != pending["kind"] or row["parameters"] != pending["parameters"])
        ):
            pending["unknown"] = True
            raise ValueError("Shadow operation differs from frozen submission")
        self._operations[row["id"]] = row
        if pending is None or row["id"] != pending["id"]:
            return
        if row["state"] in ACTIVE:
            pending["unknown"] = False
            return
        if row["state"] == "unknown":
            pending["unknown"] = True
            return
        origin = row.get("operation_core_epoch", row.get("core_epoch"))
        self._last_outcome = f"{row['kind']}: {row['state']}"
        if row.get("error"):
            self._last_outcome += " / " + str(row["error"])
        current = (
            pending["epoch"] == self.epoch
            and origin == self.epoch
            and pending["selection"] == self._selection_version
        )
        if row["state"] == "completed" and current:
            result = row.get("result")
            if row["kind"] == "inspect_training":
                if (
                    type(result) is not dict
                    or type(result.get("schema_version")) is not int
                    or result["schema_version"] != 1
                    or result.get("training_job_id") != pending["metadata_target"]
                    or type(result.get("module")) is not dict
                    or type(result.get("config")) is not dict
                    or type(result.get("checkpoint")) is not dict
                    or type(result["checkpoint"].get("file")) is not str
                    or not result["checkpoint"]["file"]
                    or len(result["checkpoint"]["file"]) > 512
                    or not _hex(result["checkpoint"].get("sha256"), 64)
                    or type(result.get("shape_confirmation_required")) is not bool
                ):
                    raise ValueError("Invalid actual training metadata")
                self.metadata, self._metadata_epoch = deepcopy(result), self.epoch
                self._metadata_source = pending["metadata_source"]
                self.metadata_label.setText(
                    f"train={result['training_job_id']} · "
                    f"module={result['module'].get('module_id')} · device={result.get('device')} · "
                    f"shape_confirmation_required={result.get('shape_confirmation_required')}"
                )
                fields = [
                    ("module_id", result["module"].get("module_id")),
                    ("module_version", result["module"].get("module_version")),
                    ("api_version", result["module"].get("api_version")),
                    ("core_version", result["module"].get("core_version")),
                    ("framework", result["module"].get("framework")),
                    ("checkpoint.file", result["checkpoint"]["file"]),
                    ("checkpoint.sha256", result["checkpoint"]["sha256"]),
                ] + [("config." + key, value) for key, value in list(result["config"].items())[:32]]
                self.metadata_table.setRowCount(len(fields))
                for index, (key, value) in enumerate(fields):
                    self.metadata_table.setItem(index, 0, QTableWidgetItem(key))
                    item = QTableWidgetItem(str(value)[:512])
                    item.setToolTip(str(value)[:2048])
                    self.metadata_table.setItem(index, 1, item)
            elif (
                row["kind"] in {"predict", "predict_raw"} and pending["form"] == self._form_version
            ):
                self._show_prediction(result, retrospective=row["kind"] == "predict_raw")
        self._pending = None

    def show_stored_prediction(self):
        selected = self.operations.selectionModel().selectedRows()
        if len(selected) != 1 or not self.show_result_button.isEnabled():
            return
        identity = self.operations.item(selected[0].row(), 0).text()
        row = self._operations.get(identity)
        if row is None:
            return
        try:
            self._show_prediction(row["result"], retrospective=row["kind"] == "predict_raw")
            self.output_label.setText(
                self.text("저장 결과 · ", "Stored result · ")
                + f"operation={identity} · "
                + f"origin={row.get('operation_core_epoch', row.get('core_epoch'))} · "
                + self.output_label.text()
            )
        except (TypeError, ValueError) as error:
            self.status.setText(str(error))

    def _show_prediction(self, value, *, retrospective=False):
        if (
            type(value) is not dict
            or value.get("scope")
            != ("offline_retrospective_shadow_only" if retrospective else "paper_shadow_only")
            or type(value.get("values")) is not list
            or type(value.get("no_signal")) is not bool
        ):
            raise ValueError("Invalid shadow prediction result")
        raw = retrospective
        proof = self._raw_result_proof(value) if raw else None
        leaves = []

        def flatten(item):
            if type(item) is list:
                for child in item:
                    flatten(child)
            elif item is None or type(item) in {int, float} and math.isfinite(item):
                leaves.append(item)
            else:
                raise ValueError("Invalid shadow output value")

        flatten(value["values"])
        self.raw_lineage.setVisible(raw)
        if raw:
            lineage = json.dumps(proof["lineage"], ensure_ascii=False, sort_keys=True)
            self.raw_lineage.setText(
                f"Historical reference close={proof['reference_time_ms']} UTC ms\n"
                f"Actual snapshot availability={proof['snapshot_available_at_ms']} UTC ms · "
                f"actual as-of={proof['as_of_ms']} UTC ms\n"
                + "\n".join(
                    f"{key}={proof[key]}"
                    for key in ("dataset_sha256", "rows_sha256", "window_sha256", "prepared_sha256")
                )
                + "\nlineage="
                + lineage[:4000]
                + (" [display truncated]" if len(lineage) > 4000 else "")
            )
        self.output.setRowCount(min(len(leaves), 128))
        for index, item in enumerate(leaves[:128]):
            self.output.setItem(index, 0, QTableWidgetItem(str(index)))
            self.output.setItem(
                index, 1, QTableWidgetItem("— / no signal" if item is None else str(item))
            )
        self.output_label.setText(
            ("Offline retrospective shadow" if raw else "Paper shadow")
            + f" · minute={value.get('open_time_ms')} · "
            f"sequence={value.get('sequence')} · no_signal={value['no_signal']} · "
            f"{min(len(leaves), 128)}/{len(leaves)} values · TS orders: 0"
        )

    @staticmethod
    def _raw_result_proof(value):
        proof = value.get("preparation")
        if (
            type(proof) is not dict
            or proof.get("scope") != "offline_retrospective_shadow_only"
            or type(value.get("open_time_ms")) is not int
            or not _hex(value.get("candidate_id"), 64)
            or not _hex(value.get("deployment_epoch"))
            or not _hex(value.get("stream_epoch"))
            or type(value.get("sequence")) is not int
            or not 1 <= value["sequence"] <= 2**63 - 1
            or not 0 <= value["open_time_ms"] <= 253_402_300_799_000 - 60000
            or value["open_time_ms"] % 60000
            or any(
                type(proof.get(key)) is not int
                for key in ("reference_time_ms", "snapshot_available_at_ms", "as_of_ms")
            )
            or not value["open_time_ms"] + 60000
            == proof["reference_time_ms"]
            <= proof["snapshot_available_at_ms"]
            == proof["as_of_ms"]
            <= 253_402_300_799_000
            or any(
                not _hex(proof.get(key), 64)
                for key in (
                    "dataset_sha256",
                    "rows_sha256",
                    "window_sha256",
                    "prepared_sha256",
                    "batch_sha256",
                )
            )
            or value.get("input_digest") != proof.get("prepared_sha256")
        ):
            raise ValueError(
                "Invalid retrospective reference/actual snapshot clocks or source pins"
            )
        lineage = proof.get("lineage")
        if (
            type(lineage) is not dict
            or lineage.get("dataset_kind") != "normalized_source"
            or type(lineage.get("source")) is not dict
            or type(lineage.get("quality")) is not dict
            or type(lineage["quality"].get("interpolated_rows")) is not int
            or lineage["quality"]["interpolated_rows"] != 0
            or lineage.get("availability")
            != "actual_snapshot_only; historical received times unavailable"
        ):
            raise ValueError("Invalid retrospective source lineage")
        request = proof.get("request")
        if type(request) is not dict or any(
            request.get(key) != value.get(key) or type(request.get(key)) is not type(value.get(key))
            for key in (
                "candidate_id",
                "sequence",
                "open_time_ms",
                "deployment_epoch",
                "stream_epoch",
            )
        ):
            raise ValueError("Raw output does not match its preparation context")
        return proof

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch or not self.attached:
            return
        try:
            if type(value) is not dict:
                raise ValueError("Shadow response must be an object")
            if value.get("core_epoch") != self.epoch:
                raise ValueError("Shadow response belongs to another core")
            context = request[2]
            if context in {"snapshot", "acknowledge"}:
                snapshot = _snapshot(value)
                old_revision, new_revision = (
                    self._document().get("revision", -1),
                    (snapshot.get("deployment") or {}).get("document", {}).get("revision", -1),
                )
                if request[1] < self._accepted_serial or new_revision < old_revision:
                    return
                for row in snapshot["operations"]:
                    previous = self._operations.get(row["id"])
                    if previous and previous["execution_id"] != row["execution_id"]:
                        raise ValueError("Shadow operation attempt changed")
                    if (
                        self._pending
                        and row["id"] == self._pending["id"]
                        and (
                            row["kind"] != self._pending["kind"]
                            or row["parameters"] != self._pending["parameters"]
                        )
                    ):
                        raise ValueError("Shadow snapshot changed frozen submission")
                self.snapshot, self._accepted_serial = snapshot, request[1]
                for row in snapshot["operations"]:
                    self._accept_operation(row)
                if context == "acknowledge" and self._pending and self._pending.get("unknown"):
                    self._pending = None
            else:
                row = _operation(value)
                if self._pending is None or row["id"] != self._pending["id"]:
                    return
                self._accept_operation(row)
                self._accepted_serial = max(self._accepted_serial, request[1])
            self._render()
            if self._pending is None and context == "operation":
                self.refresh()
        except (TypeError, ValueError, KeyError, OverflowError, RecursionError) as error:
            if self._pending:
                self._pending["unknown"] = True
            self._render()
            self.status.setText(str(error))

    def _failed(self, tag, _message):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch:
            return
        if self._pending:
            self._pending["unknown"] = True
        self._render()
        self.status.setText(
            self.text(
                "결과 미확인 · 상태 조회만 하며 자동 재전송하지 않습니다",
                "Outcome unconfirmed · status lookup only, no automatic repost",
            )
        )
