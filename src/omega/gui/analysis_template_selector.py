"""Explicit copies of saved CA definitions; no template execution or financial commands."""

import json
from copy import deepcopy

from PySide6.QtCore import QSignalBlocker, Qt, Signal
from PySide6.QtWidgets import QComboBox, QGroupBox, QHBoxLayout, QLabel, QPushButton, QVBoxLayout

from omega.gui.analysis_page import configuration_rows, form_configuration


def analysis_copy(value):
    """Validate the installed structured form while preserving copied values."""
    if type(value) is not dict or len(json.dumps(value, allow_nan=False).encode()) > 65536:
        raise ValueError("분석 설정이 너무 크거나 잘못되었습니다 / Invalid analysis document")
    form_configuration(configuration_rows(value))
    return deepcopy(value)


class AnalysisTemplateSelector(QGroupBox):
    refreshRequested = Signal()
    changed = Signal()

    def __init__(self, parent=None):
        super().__init__("저장 CA 분석 복사 / Copy saved CA analysis", parent)
        self._epoch, self._instrument, self._attached = None, None, False
        self._revision, self._records, self._applied, self._recorded = None, {}, None, None
        self._needs_apply = False
        layout = QVBoxLayout(self)
        self.choice = QComboBox()
        self.choice.addItem("기존 EMA/MMACD 기본값 / Legacy defaults", None)
        self.choice.setMinimumContentsLength(12)
        self.choice.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        layout.addWidget(self.choice)
        actions = QHBoxLayout()
        self.refresh_button = QPushButton("CA 목록 조회 / Refresh")
        self.apply_button = QPushButton("선택 적용 / Apply selection")
        actions.addWidget(self.refresh_button)
        actions.addWidget(self.apply_button)
        layout.addLayout(actions)
        self.status = QLabel(
            "기존 설정 유지. CA는 분석 페이지에서 작성합니다 / Edit CA in Analysis."
        )
        self.status.setWordWrap(True)
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.status)
        self.choice.currentIndexChanged.connect(self._changed)
        self.refresh_button.clicked.connect(self.refreshRequested.emit)
        self.apply_button.clicked.connect(self.apply_selection)
        self.set_context(None, core_epoch=None, attached=False)

    def _changed(self, *_args):
        self._needs_apply = True
        self.status.setText(
            "선택을 적용하세요. 기존 전략 행은 변하지 않습니다 / Apply before adding."
        )
        self.changed.emit()

    def uses_profile(self):
        return self.choice.currentData() is not None or self._applied is not None

    def _populate(self, selected=None):
        with QSignalBlocker(self.choice):
            self.choice.clear()
            self.choice.addItem("기존 EMA/MMACD 기본값 / Legacy defaults", None)
            if self._recorded is not None:
                self.choice.addItem("기록된 분석 초안 / Recorded analysis snapshot", "<recorded>")
            for identity, row in self._records.items():
                self.choice.addItem(
                    f"{row['name']} · {row['instrument']} · {identity} r{row['revision']}", identity
                )
            index = self.choice.findData(selected)
            self.choice.setCurrentIndex(max(index, 0))

    def invalidate(self, message="CA 목록을 다시 조회/적용하세요 / Refresh and apply CA again"):
        held = (
            self._applied is not None or self.choice.currentData() is not None or self._needs_apply
        )
        self._records, self._revision, self._applied = {}, None, None
        self._needs_apply = held
        self._populate("<recorded>" if self._recorded is not None and held else None)
        self.status.setText(message)
        self.changed.emit()

    def set_context(self, instrument, *, core_epoch, attached):
        if (self._epoch, self._instrument) != (core_epoch, instrument) or not attached:
            self.invalidate()
        self._epoch, self._instrument = core_epoch, instrument
        self._attached = attached is True
        self.refresh_button.setEnabled(self._attached and bool(core_epoch))

    def set_analysis_library(self, snapshot, *, core_epoch):
        if not self._attached or not core_epoch or core_epoch != self._epoch:
            raise ValueError("이전 코어 CA 응답 제외 / Stale CA response")
        if (
            type(snapshot) is not dict
            or len(json.dumps(snapshot, allow_nan=False).encode()) > 2_097_152
            or type(snapshot.get("schema_version")) is not int
            or snapshot["schema_version"] != 1
            or type(snapshot.get("revision")) is not int
            or not 0 <= snapshot["revision"] < 2**63
            or snapshot.get("persistence", "ready") != "ready"
            or type(snapshot.get("instances")) is not list
            or len(snapshot["instances"]) > 512
            or snapshot.get("core_epoch", core_epoch) != core_epoch
            or self._revision is not None
            and snapshot["revision"] < self._revision
        ):
            raise ValueError("CA 목록을 검증할 수 없습니다 / Invalid CA library")
        records = {}
        for row in snapshot["instances"]:
            if (
                type(row) is not dict
                or any(
                    type(row.get(key)) is not str
                    or not 1 <= len(row[key]) <= 128
                    or not row[key].isprintable()
                    for key in ("id", "name", "instrument")
                )
                or type(row.get("revision")) is not int
                or not 1 <= row["revision"] < 2**63
                or row["id"] in records
                or row["id"] == "<recorded>"
            ):
                raise ValueError("잘못된 CA 항목 / Invalid CA record")
            records[row["id"]] = deepcopy(row)
            records[row["id"]]["configuration"] = analysis_copy(row["configuration"])
        selected = self.choice.currentData()
        if self._revision is not None and (
            self._revision != snapshot["revision"] or self._records != records
        ):
            self._applied = None
            self._needs_apply = self._needs_apply or selected is not None
        self._records, self._revision = records, snapshot["revision"]
        self._populate(selected)
        self.status.setText("CA 목록 수신. 선택 후 적용하세요 / Select and apply a saved CA.")
        self.changed.emit()

    def apply_selection(self):
        try:
            selected = self.choice.currentData()
            if selected is None:
                self.reset()
                return
            if not self._attached or not self._epoch:
                raise ValueError("현재 코어에 연결하세요 / Connect to the current core")
            row = self._recorded if selected == "<recorded>" else self._records.get(selected)
            if row is None or row["instrument"] != self._instrument:
                raise ValueError("CA 종목이 다릅니다 / CA instrument must match the strategy")
            self._applied = deepcopy(row)
            self._needs_apply = False
            label = (
                "기록된 분석 초안 / Recorded snapshot"
                if selected == "<recorded>"
                else (f"CA {row['id']} r{row['revision']}")
            )
            self.status.setText(label + " · 복사 적용 / Copy applied; existing rows unchanged")
            self.changed.emit()
        except (ValueError, TypeError, KeyError) as error:
            self._needs_apply = True
            self.status.setText(str(error))

    def analysis_document(self, instrument, *, core_epoch):
        if self._needs_apply:
            raise ValueError("CA 선택을 다시 적용하세요 / Apply the analysis selection first")
        if self._applied is None:
            return None
        if (
            not self._attached
            or not core_epoch
            or core_epoch != self._epoch
            or instrument != self._instrument
            or self._applied["instrument"] != instrument
        ):
            raise ValueError("CA 코어/종목이 변경됐습니다 / CA context changed")
        return analysis_copy(self._applied["configuration"])

    def reset(self):
        self._applied, self._recorded, self._needs_apply = None, None, False
        self._populate()
        self.status.setText("기존 EMA/MMACD 기본값 유지 / Legacy defaults retained")
        self.changed.emit()

    def load_document(self, configuration, instrument):
        """Keep an existing copied draft without inventing its missing CA identity."""
        value = analysis_copy(configuration)
        self._recorded = {"instrument": instrument, "configuration": value}
        self._applied, self._needs_apply = deepcopy(self._recorded), False
        self._populate("<recorded>")
        self.status.setText(
            "기록된 분석 초안 유지 · CA ID 추정 없음 / Recorded snapshot; no inferred CA ID"
        )
        self.changed.emit()
