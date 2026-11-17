"""Explicit local scenario-file selection; only validated request JSON crosses the API."""

import json
import os
import stat
from copy import deepcopy
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path

from PySide6.QtCore import Qt, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QFileDialog,
    QGroupBox,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
)

from omega.application.margin_funding import FundingSchedule
from omega.domain.errors import ValidationError

MAX_FILE_BYTES = 2_097_152
PREVIEW_ROWS = 200


def _read_file(path):
    selected = Path(path).lstat()
    if stat.S_ISLNK(selected.st_mode) or getattr(selected, "st_file_attributes", 0) & 0x400:
        raise ValueError("Funding source cannot be a linked file")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        before = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_size > MAX_FILE_BYTES
            or getattr(before, "st_file_attributes", 0) & 0x400
        ):
            raise ValueError("Select a bounded regular JSON file")
        raw = stream.read(MAX_FILE_BYTES + 1)
        after = os.fstat(stream.fileno())
    current = Path(path).lstat()

    def identity(value):
        return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns

    if (
        len(raw) > MAX_FILE_BYTES
        or identity(before) != identity(after)
        or identity(after) != identity(current)
    ):
        raise ValueError("Funding file changed while reading; select it again")
    return json.loads(raw.decode("utf-8")), sha256(raw).hexdigest()


class FundingEditor(QGroupBox):
    changed = Signal()

    def __init__(self, parent=None, *, validate_candidate=None, max_request_bytes=131072):
        super().__init__(parent)
        self.validate_candidate, self.max_request_bytes = validate_candidate, max_request_bytes
        self.raw = self.schedule = self.source_path = self.source_sha256 = None
        self.supported, self.language = False, "ko"
        self.setCheckable(True)
        self.setChecked(False)
        layout = QVBoxLayout(self)
        self.body = QGroupBox()
        content = QVBoxLayout(self.body)
        self.note, self.summary, self.status = QLabel(), QLabel(), QLabel()
        for label in (self.note, self.summary, self.status):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
        self.path = QLineEdit()
        self.path.setReadOnly(True)
        self.choose = QPushButton()
        self.choose.setEnabled(False)
        self.choose.clicked.connect(self.select_file)
        self.preview = QTableWidget(0, 5)
        self.preview.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.preview.horizontalHeader().setSectionResizeMode(
            QHeaderView.ResizeMode.ResizeToContents
        )
        self.preview.setMaximumHeight(200)
        for widget in (self.note, self.choose, self.path, self.summary, self.preview, self.status):
            content.addWidget(widget)
        layout.addWidget(self.body)
        self.body.hide()
        self.toggled.connect(self._toggled)
        self.set_language("ko")

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_supported(self, supported):
        if self.supported == (supported is True):
            return
        self.supported = supported is True
        self.choose.setEnabled(self.supported)
        self.set_language(self.language)

    def set_language(self, language):
        self.language = language
        self.setTitle(
            self.text("고급 비용 · Funding 시나리오", "Advanced costs · Funding scenario")
            + ("" if self.supported else self.text(" (코어 미지원)", " (unsupported core)"))
        )
        self.choose.setText(self.text("Funding JSON 파일 선택", "Select funding JSON file"))
        self.note.setText(
            self.text(
                "기본 미적용 · 시나리오 rate로 계산하며 거래소 실지급이 아닙니다. "
                "일정의 분 경계·가용 시각·참조 가격을 확인하세요. "
                "슬리피지·별도 체결 지연은 미지원입니다. "
                "요청 JSON 값만 전송하며 원본 파일을 바꾸지 않습니다.",
                "Off by default. Calculated scenario rates, not actual venue payments. "
                "Check minute boundaries, source availability and reference prices. "
                "Slippage/extra fill delay are "
                "unsupported. Only request JSON values are sent; the source file is unchanged.",
            )
        )
        self.preview.setHorizontalHeaderLabels(
            ["Settlement UTC", "Instrument", "Rate", "Reference price", "Source available UTC"]
        )
        self._render()

    def _toggled(self, checked):
        self.body.setVisible(checked)
        self.changed.emit()

    def reset(self):
        self.raw = self.schedule = self.source_path = self.source_sha256 = None
        self.path.clear()
        self.status.clear()
        self.setChecked(False)
        self._render()
        self.changed.emit()

    def _render(self):
        points = () if self.schedule is None else self.schedule.points
        self.preview.setRowCount(min(PREVIEW_ROWS, len(points)))

        def utc(value):
            return (
                datetime.fromtimestamp(value // 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")
                + f".{value % 1000:03d}"
            )

        for index, point in enumerate(points[:PREVIEW_ROWS]):
            for column, value in enumerate(
                (
                    utc(point.event_time_ms),
                    point.instrument.key,
                    str(point.rate),
                    str(point.reference_price),
                    utc(point.source_available_at_ms),
                )
            ):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                self.preview.setItem(index, column, item)
        self.summary.setText(
            self.text("일정 미선택", "No schedule selected")
            if not points
            else f"{min(PREVIEW_ROWS, len(points))} / {len(points)} · "
            f"SHA256 {self.schedule.sha256} · "
            + self.text(
                "request_json_snapshot · 정책 USDT 1e-8 / HALF_EVEN. "
                "파서 10000점 상한과 별개로 전체 API 요청 한도가 적용됩니다.",
                "request_json_snapshot · policy USDT 1e-8 / HALF_EVEN. "
                "The full API request limit applies independently of the 10000-point parser limit.",
            )
        )

    def validate_document(self, value, *, request=None):
        if not self.supported:
            raise ValueError("Connected core does not support scenario funding")
        raw = json.loads(json.dumps(value, allow_nan=False))
        try:
            schedule = FundingSchedule.parse(raw)
        except ValidationError as error:
            raise ValueError(str(error)) from None
        if request is not None:
            candidate = request
        elif self.validate_candidate is not None:
            candidate = self.validate_candidate(raw)
        else:
            candidate = {"funding": raw}
        if type(candidate) is not dict or candidate.get("funding") != raw:
            raise ValueError("Candidate request differs from selected funding")
        if type(candidate) is dict and "account" in candidate and "start_ms" in candidate:
            allowed = {point["instrument"] for point in candidate["account"]["positions"]}
            try:
                schedule.validate_for(
                    tuple(
                        point.instrument
                        for point in schedule.points
                        if point.instrument.key in allowed
                    ),
                    candidate["start_ms"],
                )
            except ValidationError as error:
                raise ValueError(str(error)) from None
        if (
            len(
                json.dumps(
                    candidate, sort_keys=True, allow_nan=False, separators=(",", ":")
                ).encode()
            )
            > self.max_request_bytes
        ):
            raise ValueError(
                f"Full funding request exceeds {self.max_request_bytes} bytes; "
                "previous draft preserved"
            )
        return raw, schedule

    def load_document(
        self, value, *, label="Recorded request", source_path=None, source_sha256=None, request=None
    ):
        raw, schedule = self.validate_document(value, request=request)
        self.raw, self.schedule = raw, schedule
        self.source_path, self.source_sha256 = source_path, source_sha256
        self.path.setText(str(source_path) if source_path is not None else label)
        self.status.clear()
        self.setChecked(True)
        self._render()
        self.changed.emit()

    def select_file(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            self.text("시나리오 Funding 일정", "Scenario funding schedule"),
            "",
            "JSON (*.json)",
        )
        if not path:
            return
        try:
            raw, checksum = _read_file(path)
            self.load_document(raw, source_path=path, source_sha256=checksum)
        except (OSError, ValueError, TypeError, KeyError, ValidationError) as error:
            message = (
                self.text(
                    "새 파일을 불러오지 못해 이전 선택을 유지합니다: ",
                    "Could not load the new file; the previous selection is retained: ",
                )
                if self.raw is not None
                else self.text("파일을 불러오지 못했습니다: ", "Could not load the file: ")
            )
            self.status.setText(message + str(error)[:400])

    def document(self):
        if not self.isChecked():
            return None
        if not self.supported or self.raw is None:
            raise ValueError("Select a funding schedule on a supported core, or turn funding off")
        if self.source_path is not None:
            try:
                current, checksum = _read_file(self.source_path)
            except OSError:
                raise ValueError("Funding source is no longer readable; select it again") from None
            if checksum != self.source_sha256 or current != self.raw:
                raise ValueError("Funding source changed after preview; explicitly select it again")
        return deepcopy(self.raw)

    def can_submit(self):
        return not self.isChecked() or self.supported and self.raw is not None
