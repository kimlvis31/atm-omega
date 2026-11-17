"""Dataset inspection and explicit observation selections through research jobs."""

import json
import re
from copy import deepcopy
from urllib.parse import quote

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

OHLCV_SCHEMA = "omega.ohlcv.1m.v1"
AUXILIARY_SCHEMA = "omega.market-observations.1m.v1"
AUXILIARY_SCHEMA_V2 = "omega.market-observations.1m.v2"
CHANNELS = ("trade_flow", "open_interest", "long_short_ratio", "funding_rate")


def _native_bundle(value):
    auxiliary = value.get("auxiliary") if type(value) is dict else None
    return (
        type(auxiliary) is dict
        and value.get("schema_id") in {AUXILIARY_SCHEMA, AUXILIARY_SCHEMA_V2}
        and (
            value["schema_id"] != AUXILIARY_SCHEMA_V2
            or auxiliary.get("scope") == "recorded_native_inputs_v2"
        )
        and "transform" in auxiliary
        and auxiliary["transform"] is None
        and all(
            type(auxiliary.get(name)) is str
            and re.fullmatch(r"[a-f0-9]{64}", auxiliary[name]) is not None
            for name in ("manifest_sha256", "observations_sha256")
        )
    )


def _summary(value):
    if (
        type(value) is not dict
        or value.get("schema_id") not in {OHLCV_SCHEMA, AUXILIARY_SCHEMA, AUXILIARY_SCHEMA_V2}
        or type(value.get("rows")) is not int
        or not 1 <= value["rows"] <= 1_000_000
        or type(value.get("instruments")) is not list
        or not 1 <= len(value["instruments"]) <= 1000
        or type(value.get("time_range")) is not list
        or len(value["time_range"]) != 2
        or any(
            type(stamp) is not int or stamp < 0 or stamp % 60000 for stamp in value["time_range"]
        )
        or value["time_range"][0] >= value["time_range"][1]
        or (value["schema_id"] != OHLCV_SCHEMA) != isinstance(value.get("auxiliary"), dict)
        or value["schema_id"] == OHLCV_SCHEMA
        and value.get("auxiliary") is not None
    ):
        raise ValueError("Invalid dataset inspection response")
    seen = set()
    for key in value["instruments"]:
        if type(key) is not str or not 1 <= len(key) <= 128 or not key.isprintable() or key in seen:
            raise ValueError("Invalid inspected instrument")
        seen.add(key)
    ranges = value.get("instrument_ranges")
    if type(ranges) is not dict or set(ranges) != seen:
        raise ValueError("Invalid inspected instrument ranges")
    for bounds in ranges.values():
        if (
            type(bounds) is not list
            or len(bounds) != 2
            or any(type(stamp) is not int or stamp < 0 or stamp % 60000 for stamp in bounds)
            or not value["time_range"][0] <= bounds[0] < bounds[1] <= value["time_range"][1]
        ):
            raise ValueError("Invalid inspected instrument time range")
    if len(json.dumps(value, allow_nan=False).encode()) > 262144:
        raise ValueError("Dataset inspection exceeds display capacity")
    auxiliary = value.get("auxiliary")
    if auxiliary is not None and (
        auxiliary.get("schema_id") != value["schema_id"]
        or auxiliary.get("scope")
        != (
            "recorded_native_inputs_v2"
            if value["schema_id"] == AUXILIARY_SCHEMA_V2
            else "retrospective_training_only"
        )
        or type(auxiliary.get("channels")) is not dict
    ):
        raise ValueError("Invalid retrospective observation identity")
    if value["schema_id"] == AUXILIARY_SCHEMA_V2 and not _native_bundle(value):
        raise ValueError("Invalid original V2 observation identity")
    return deepcopy(value)


class ResearchInputs(QGroupBox):
    """Never read a market DB, dataset file or trusted Python module in the GUI."""

    inspectionChanged = Signal()

    def __init__(self, client, source, parent=None):
        super().__init__("Dataset 검증 / Inspect input", parent)
        self.client, self.source = client, source
        self.metadata = None
        self._version, self._serial = 0, 0
        self._binding = None
        self._pending = None
        self._job = None
        self._query = None
        self._failed = False
        self._attached, self._available = bool(client.attached), True
        self._epoch = self._core_epoch()
        layout = QVBoxLayout(self)
        self.inspect_button = QPushButton("선택 dataset 검증 / Validate selected dataset")
        layout.addWidget(self.inspect_button)
        self.notice = QLabel("CSV 원본은 먼저 export로 dataset을 만든 뒤 검증하세요.")
        self.notice.setWordWrap(True)
        self.notice.setTextFormat(Qt.TextFormat.PlainText)
        self.notice.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(self.notice)
        self.export_group = QGroupBox("저장 보조 관측 추가 / Export stored observations")
        form = QFormLayout(self.export_group)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.instrument, self.channel = QComboBox(), QComboBox()
        self.channel.addItems(CHANNELS)
        self.channel.addItem("depth")
        self.export_format = QComboBox()
        self.export_format.addItem("V1 · 기존 선택 관측 / Selected observations", 1)
        self.export_format.addItem("V2 · 원본 revision / Retained native revisions", 2)
        self.metric_lookback_ms = QSpinBox()
        self.metric_lookback_ms.setRange(0, 86400000)
        self.metric_lookback_ms.setSuffix(" ms")
        self.lookback_label = QLabel("V2 metric lookback · 0 = seed 없음 / No seed")
        self.observation_source = QLineEdit()
        self.observation_source.setMaxLength(96)
        self.observation_source.setPlaceholderText("binance:usdm:BTCUSDT:open_interest_history")
        self.as_of = QLineEdit()
        self.as_of.setMaxLength(15)
        self.as_of.setPlaceholderText("선택 UTC ms · 비우면 현재 저장 시점")
        form.addRow("검증된 종목 / Instrument", self.instrument)
        form.addRow("묶음 형식 / Bundle format", self.export_format)
        form.addRow(self.lookback_label, self.metric_lookback_ms)
        form.addRow("채널 / Channel", self.channel)
        form.addRow("명시 저장 source / Source", self.observation_source)
        form.addRow("실제 availability cutoff", self.as_of)
        buttons = QHBoxLayout()
        self.add_button, self.remove_button = QPushButton("선택 추가"), QPushButton("선택 삭제")
        buttons.addWidget(self.add_button)
        buttons.addWidget(self.remove_button)
        form.addRow(buttons)
        self.selections = QTableWidget(0, 3)
        self.selections.setHorizontalHeaderLabels(["Instrument", "Channel", "Source"])
        self.selections.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.selections.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.selections.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.selections.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.selections.horizontalHeader().setStretchLastSection(True)
        self.selections.setMaximumHeight(180)
        form.addRow(self.selections)
        self.export_notice = QLabel(
            "최대 8개 source · 합계 10,000분. 저장된 관측만 출력합니다. "
            "수집·결측 채움은 하지 않습니다. "
            "실제 availability/선택 revision을 유지하는 회고 학습·MC 입력입니다. "
            "원본 묶음은 지원 코어에서 공유 증거금 native 재생 입력으로도 선택할 수 있습니다. "
            "Original bundle only for native replay; no generated/MC/proxy input. "
            "인과 추론 준비·배포의 근거로 사용할 수 없습니다."
        )
        self.export_notice.setWordWrap(True)
        self.export_notice.setTextFormat(Qt.TextFormat.PlainText)
        self._legacy_export_notice = self.export_notice.text()
        form.addRow(self.export_notice)
        layout.addWidget(self.export_group)
        self.export_group.hide()
        self.inspect_button.clicked.connect(self.inspect)
        self.add_button.clicked.connect(self.add_selection)
        self.remove_button.clicked.connect(self.remove_selection)
        self.export_format.currentIndexChanged.connect(self._export_format_changed)
        source.textChanged.connect(self._source_changed)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failure)
        self.set_context(client.attached)

    def _core_epoch(self):
        state = getattr(self.client, "state", {})
        core = state.get("core") if isinstance(state, dict) else None
        return core.get("epoch") if isinstance(core, dict) else None

    def _current(self):
        return self.source.text().strip(), self._version, self._core_epoch()

    def _source_changed(self, *_args):
        self._version += 1
        self.metadata, self._binding, self._pending, self._job, self._query = (None,) * 5
        self._failed = False
        self.instrument.clear()
        self.selections.setRowCount(0)
        self.notice.setText("입력이 바뀌었습니다. 선택 dataset을 검증하세요.")
        self.set_context(self._attached, self._available)
        self.inspectionChanged.emit()

    def set_context(self, attached, available=True):
        if self._epoch != self._core_epoch():
            self._epoch = self._core_epoch()
            self._source_changed()
        self._attached, self._available = bool(attached), bool(available)
        ready = self._attached and self._available
        self.inspect_button.setEnabled(ready and self._pending is None and self._job is None)
        self.export_format.model().item(1).setEnabled(
            self._v2_capability("native_observation_bundle")
        )
        self._export_format_changed()

    def _v2_capability(self, name):
        state = getattr(self.client, "state", {})
        core = state.get("core", {}) if type(state) is dict else {}
        if type(core) is not dict:
            return False
        caps = core.get("capabilities", {})
        scope = (
            "recorded_native_inputs_v2"
            if name == "native_observation_bundle"
            else "native_auxiliary_available_cutoff_v2"
        )
        return (
            self._attached
            and self.client.attached is True
            and self._available
            and type(core.get("epoch")) is str
            and 1 <= len(core["epoch"]) <= 128
            and type(caps) is dict
            and caps.get(name + "_v2") is True
            and caps.get(name + "_v2_scope") == scope
        )

    def _export_format_changed(self, *_args):
        v2 = self.export_format.currentData() == 2
        self.metric_lookback_ms.setVisible(v2)
        self.lookback_label.setVisible(v2)
        self.channel.model().item(self.channel.findText("depth")).setEnabled(
            v2 and self._v2_capability("native_observation_bundle")
        )
        self.channel.model().item(self.channel.findText("funding_rate")).setEnabled(not v2)
        self.export_notice.setText(
            "V2 · 저장 원본 revision과 실제 availability, 희소 metric seed를 보존합니다. "
            "공유 증거금 native 재생 전용 / Native shared margin only; NN training/MC unsupported. "
            "Metric lookback 0 = seed 없음. 선택 범위/관측만 출력합니다. "
            "수집·결측 채움은 하지 않습니다. "
            "Normalized records do not prove provider/raw completeness."
            if v2
            else self._legacy_export_notice
        )

    def set_kind(self, kind):
        self.export_group.setVisible(kind == "export_auxiliary")

    def inspect(self):
        if not self._attached or not self._available:
            return
        try:
            path = self.source.text().strip()
            if not path or len(path) > 4096 or any(char in path for char in "\x00\r\n"):
                raise ValueError("Dataset manifest 또는 폴더를 선택하세요.")
            self.metadata, self._failed = None, False
            self.instrument.clear()
            self.selections.setRowCount(0)
            self._binding = self._current()
            self._serial += 1
            self._pending = self.client.submit_job("inspect_dataset", {"dataset": path})
            self.notice.setText("연구 worker에서 파일·hash·schema를 검증 중입니다…")
            self.set_context(self._attached, self._available)
        except (ValueError, TypeError) as error:
            self._failed = True
            self.notice.setText(str(error))

    def update_jobs(self, jobs):
        self.set_context(self._attached, self._available)
        if self._job is None or self._binding != self._current():
            return
        job = next(
            (item for item in jobs if isinstance(item, dict) and item.get("id") == self._job), None
        )
        if (
            job
            and job.get("state")
            in {"complete", "failed", "cancelled", "interrupted", "unknown", "quarantined"}
            and self._query is None
        ):
            self._query = f"dataset_inspection:{self._job}:{self._serial}"
            self.client.request("GET", "/api/jobs/" + quote(self._job, safe=""), tag=self._query)

    def _result(self, tag, result):
        if tag not in {self._pending, self._query} or self._binding != self._current():
            return
        try:
            if not isinstance(result, dict):
                raise ValueError("Invalid inspection result")
            if tag == self._pending:
                self._pending = None
                identity = result.get("id")
                if type(identity) is not str or not re.fullmatch(r"[a-f0-9]{32}", identity):
                    raise ValueError("Invalid inspection job identity")
                self._job = identity
                if result.get("state") != "complete":
                    self.set_context(self._attached, self._available)
                    return
            elif result.get("id") != self._job:
                raise ValueError("Dataset inspection job identity changed")
            if result.get("state") != "complete" or result.get("kind") != "inspect_dataset":
                raise ValueError(str(result.get("error", "Dataset 검증에 실패했습니다.")))
            self.metadata = _summary(result.get("result"))
            self.instrument.clear()
            self.instrument.addItems(self.metadata["instruments"])
            auxiliary = self.metadata.get("auxiliary")
            self.notice.setText(
                f"{self.metadata['schema_id']} · {self.metadata['rows']} rows · "
                f"{len(self.metadata['instruments'])} instruments\n"
                + (
                    "보조 관측 채널: "
                    + ", ".join(auxiliary.get("channels", {}))
                    + " · 실제 availability 보존 · 회고 학습/MC 전용 · 인과 추론 준비/배포 불가"
                    if auxiliary
                    else "OHLCV 검증 완료 · 저장 보조 관측을 별도 선택할 수 있습니다."
                )
            )
            if _native_bundle(self.metadata):
                v2 = self.metadata["schema_id"] == AUXILIARY_SCHEMA_V2
                inputs_notice = (
                    "V2 원본 flow/depth/희소 OI·LS 및 기록 revision · "
                    "Original sparse metrics and retained revisions. "
                    if v2
                    else "공유 증거금 native 모드에서 원본 trade_flow/NES 재생 가능. "
                )
                self.notice.setText(
                    f"{self.metadata['schema_id']} · {self.metadata['rows']} rows\n"
                    + inputs_notice
                    + "Original availability retained; generated/MC/proxy replay unsupported. "
                    "코어 지원과 명시 native 선택이 필요합니다. 인과 추론 준비/배포 불가."
                )
            self._job, self._query = None, None
        except (ValueError, TypeError, KeyError) as error:
            self.metadata, self._job, self._query, self._pending = (None,) * 4
            self._failed = True
            self.notice.setText(str(error))
        self.set_context(self._attached, self._available)
        self.inspectionChanged.emit()

    def _failure(self, tag, message):
        if tag in {self._pending, self._query}:
            self._pending, self._query, self._job = None, None, None
            self.metadata, self._failed = None, True
            self.notice.setText(str(message))
            self.set_context(self._attached, self._available)
            self.inspectionChanged.emit()

    def validate_usage(self, kind):
        if self._pending or self._job:
            raise ValueError("Dataset 검증 완료 후 제출하세요.")
        if self._failed:
            raise ValueError("Dataset 검증에 실패했습니다. 입력을 확인한 뒤 다시 검증하세요.")
        if self.metadata and self.metadata["schema_id"] == AUXILIARY_SCHEMA_V2:
            raise ValueError(
                "V2는 명시 native 공유 증거금 재생 전용 / V2 requires native shared margin; "
                "NN training and MC are unsupported"
            )
        if (
            self.metadata
            and self.metadata["schema_id"] == AUXILIARY_SCHEMA
            and kind not in {"train", "monte_carlo"}
        ):
            raise ValueError("보조 관측 dataset은 회고 학습·MC 전용입니다.")

    def native_replay_parameters(self, decision_latency_ms=0):
        if type(decision_latency_ms) is not int or not 0 <= decision_latency_ms <= 60000:
            raise ValueError("결정 지연은 0..60000ms 정수 / Integer decision latency required")
        if not self._attached or not self._available or self._pending or self._job or self._failed:
            raise ValueError("원본 dataset 검증 완료 후 제출 / Complete input validation first")
        if self._binding != self._current() or not _native_bundle(self.metadata):
            raise ValueError(
                "원본 선택 관측 bundle을 먼저 검증하세요 / Validate original observations; "
                "generated/MC/proxy inputs are unsupported"
            )
        if self.metadata["schema_id"] == AUXILIARY_SCHEMA_V2 and not self._v2_capability(
            "margin_native_observation_replay"
        ):
            raise ValueError("V2 native replay capability required / V2 코어 지원 필요")
        return {"mode": "native", "decision_latency_ms": decision_latency_ms}

    def add_selection(self):
        try:
            if (
                not self.metadata
                or self._binding != self._current()
                or self.metadata["schema_id"] != OHLCV_SCHEMA
            ):
                raise ValueError("원본 OHLCV dataset을 먼저 검증하세요.")
            source = self.observation_source.text().strip()
            if not 1 <= len(source) <= 96 or not source.isprintable():
                raise ValueError("실제 저장 source를 명시하세요 (1..96 printable characters).")
            selection = [self.instrument.currentText(), self.channel.currentText(), source]
            existing = self._selected()
            if dict(zip(("instrument", "channel", "source"), selection, strict=True)) in existing:
                raise ValueError("같은 source 선택이 이미 있습니다.")
            if len(existing) >= 8:
                raise ValueError("최대 8개 source를 선택할 수 있습니다.")
            row = self.selections.rowCount()
            self.selections.insertRow(row)
            for column, value in enumerate(selection):
                self.selections.setItem(row, column, QTableWidgetItem(value))
        except ValueError as error:
            self.notice.setText(str(error))

    def remove_selection(self):
        if self.selections.currentRow() >= 0:
            self.selections.removeRow(self.selections.currentRow())

    def _selected(self):
        return [
            dict(
                zip(
                    ("instrument", "channel", "source"),
                    (self.selections.item(row, col).text() for col in range(3)),
                    strict=True,
                )
            )
            for row in range(self.selections.rowCount())
        ]

    def export_parameters(self):
        self.validate_usage("export_auxiliary")
        if (
            not self.metadata
            or self._binding != self._current()
            or self.metadata["schema_id"] != OHLCV_SCHEMA
        ):
            raise ValueError("원본 OHLCV dataset을 먼저 검증하세요.")
        selections = self._selected()
        if not 1 <= len(selections) <= 8:
            raise ValueError("1..8개 보조 관측 source를 명시하세요.")
        version = self.export_format.currentData()
        if version == 2 and not self._v2_capability("native_observation_bundle"):
            raise ValueError("V2 export capability required / V2 코어 지원 필요")
        if any(row["channel"] == "depth" for row in selections) and version != 2:
            raise ValueError("Depth source requires explicit V2 / depth는 V2 전용")
        if version == 2 and any(row["channel"] == "funding_rate" for row in selections):
            raise ValueError("V2 native funding replay is unsupported / funding은 V1만 지원")
        minutes = 0
        for selection in selections:
            if selection["instrument"] not in self.metadata["instrument_ranges"]:
                raise ValueError("선택 종목이 검증된 dataset에 없습니다. 다시 검증하세요.")
            begin, end = self.metadata["instrument_ranges"][selection["instrument"]]
            if version == 2 and selection["channel"] in {"open_interest", "long_short_ratio"}:
                begin = max(0, begin - self.metric_lookback_ms.value()) // 60000 * 60000
            minutes += (end - begin) // 60000
        if minutes > 10000:
            raise ValueError("합계 10,000분을 초과합니다. 더 짧은 candle dataset을 선택하세요.")
        result = {"candle_dataset": self.source.text().strip(), "selections": selections}
        if version == 2:
            result["format_version"] = 2
            if any(row["channel"] in {"open_interest", "long_short_ratio"} for row in selections):
                result["metric_lookback_ms"] = self.metric_lookback_ms.value()
        cutoff = self.as_of.text().strip()
        if cutoff:
            if not re.fullmatch(r"[0-9]{1,15}", cutoff) or int(cutoff) > 253_402_300_799_000:
                raise ValueError("as-of는 유효한 UTC millisecond 정수여야 합니다.")
            result["as_of_ms"] = int(cutoff)
        return result
