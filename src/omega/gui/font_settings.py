"""GUI-local font drafts and trusted file registration; never install an OS font."""

# Imports
import json
from pathlib import Path

from PySide6.QtCore import QIODevice, QSaveFile, Qt, Signal
from PySide6.QtGui import QFont, QFontDatabase
from PySide6.QtWidgets import (
    QComboBox,
    QFileDialog,
    QFontComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
)

# Constants
MAX_FONT_BYTES = 32 * 1024 * 1024


def default_font_preferences():
    return {"mode": "default", "family": "", "path": "", "size_px": 12}


def validate_font_preferences(value):
    if not isinstance(value, dict) or set(value) - {"mode", "family", "path", "size_px"}:
        raise ValueError("Font preferences must contain mode, family, path and size_px only")
    result = default_font_preferences() | value
    if not isinstance(result["mode"], str) or result["mode"] not in {
        "default",
        "installed",
        "file",
    }:
        raise ValueError("Unknown GUI font source")
    if type(result["size_px"]) is not int or not 10 <= result["size_px"] <= 18:
        raise ValueError("GUI body font size must be 10..18 pixels")
    for name, maximum in (("family", 256), ("path", 4096)):
        if (
            not isinstance(result[name], str)
            or len(result[name]) > maximum
            or any(ord(character) < 32 for character in result[name])
        ):
            raise ValueError(f"Invalid GUI font {name}")
        result[name] = result[name].strip()
    if result["mode"] == "installed" and not result["family"]:
        raise ValueError("Select an installed font family")
    if result["mode"] == "file" and not result["path"]:
        raise ValueError("Select a trusted local TTF or OTF file")
    if result["mode"] == "default":
        result["family"], result["path"] = "", ""
    elif result["mode"] == "installed":
        result["path"] = ""
    return result


def prepare_font(value, default_font, *, installed_families=None):
    """Validate/register first; return a candidate without changing QApplication."""
    preferences = validate_font_preferences(value)
    registration = None
    if preferences["mode"] == "default":
        candidate = QFont(default_font)
    elif preferences["mode"] == "installed":
        available = QFontDatabase.families() if installed_families is None else installed_families
        if preferences["family"] not in available:
            raise ValueError(
                "This family was not installed when the GUI started. "
                "Select its TTF/OTF file, or restart after installing it in the OS."
            )
        if preferences["family"] not in QFontDatabase.families():
            raise ValueError("The selected installed font family is unavailable")
        candidate = QFont(preferences["family"])
    else:
        raw = preferences["path"]
        if raw.startswith(("\\\\", "//")):
            raise ValueError("Select a local font file, not a network path")
        path = Path(raw)
        if not path.is_absolute() or path.suffix.lower() not in {".ttf", ".otf"}:
            raise ValueError("Font file must be an absolute local TTF or OTF path")
        path = path.resolve(strict=True)
        if not path.is_file() or not 0 < path.stat().st_size <= MAX_FONT_BYTES:
            raise ValueError("Font file must be a nonempty local file no larger than 32 MiB")
        registration = QFontDatabase.addApplicationFont(str(path))
        if registration < 0:
            raise ValueError("Qt could not load this font file; the current font was retained")
        families = QFontDatabase.applicationFontFamilies(registration)
        family = preferences["family"] or (families[0] if families else "")
        if family not in families:
            QFontDatabase.removeApplicationFont(registration)
            raise ValueError("The selected font family is not available in this font file")
        try:
            preferences = validate_font_preferences(
                preferences | {"path": str(path), "family": family}
            )
        except ValueError:
            QFontDatabase.removeApplicationFont(registration)
            raise
        candidate = QFont(family)
    candidate.setPixelSize(preferences["size_px"])
    return candidate, preferences, registration


def save_gui_preferences(root, value):
    encoded = json.dumps(value, ensure_ascii=False, allow_nan=False).encode("utf-8")
    if len(encoded) > 32768:
        raise ValueError("GUI preferences exceed 32 KiB")
    path = Path(root) / "state" / "gui.preferences.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    output = QSaveFile(str(path))
    output.setDirectWriteFallback(False)
    if not output.open(QIODevice.OpenModeFlag.WriteOnly):
        raise OSError(output.errorString())
    if output.write(encoded) != len(encoded):
        output.cancelWriting()
        raise OSError(output.errorString())
    if not output.commit():
        raise OSError(output.errorString())


class FontSettings(QGroupBox):
    applyRequested = Signal(dict)

    def __init__(self, preferences, parent=None):
        super().__init__(parent)
        self.language = "ko"
        self._applied = default_font_preferences()
        layout = QFormLayout(self)
        layout.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.mode = QComboBox()
        for mode in ("default", "installed", "file"):
            self.mode.addItem(mode, mode)
        self.family = QFontComboBox()
        self.family.setEditable(False)
        self.file = QLineEdit()
        self.file.setMaxLength(4096)
        self.file.setPlaceholderText("Trusted local .ttf / .otf")
        self.file_family = QLineEdit()
        self.file_family.setReadOnly(True)
        self.browse_button = QPushButton("…")
        file_row = QHBoxLayout()
        file_row.addWidget(self.file, 1)
        file_row.addWidget(self.browse_button)
        self.font_size = QSpinBox()
        self.font_size.setRange(10, 18)
        self.font_size.setSuffix(" px")
        self.preview = QLabel("가나다 · ATM Omega · AaBb 0123456789")
        self.preview.setTextFormat(Qt.TextFormat.PlainText)
        self.preview.setWordWrap(True)
        self.notice = QLabel()
        self.notice.setTextFormat(Qt.TextFormat.PlainText)
        self.notice.setWordWrap(True)
        self.status = QLabel()
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        self.status.setWordWrap(True)
        self.apply_button, self.reset_button = QPushButton(), QPushButton()
        actions = QHBoxLayout()
        actions.addWidget(self.apply_button)
        actions.addWidget(self.reset_button)
        self._labels = []
        for key, field in (
            ("source", self.mode),
            ("family", self.family),
            ("file", file_row),
            ("file_family", self.file_family),
            ("size", self.font_size),
        ):
            label = QLabel()
            self._labels.append((key, label))
            layout.addRow(label, field)
        layout.addRow(self.preview)
        layout.addRow(self.notice)
        layout.addRow(actions)
        layout.addRow(self.status)
        self.mode.currentIndexChanged.connect(self._draft_changed)
        self.family.currentFontChanged.connect(self._draft_changed)
        self.file.textChanged.connect(self._file_changed)
        self.font_size.valueChanged.connect(self._draft_changed)
        self.browse_button.clicked.connect(self._browse)
        self.apply_button.clicked.connect(self._request_apply)
        self.reset_button.clicked.connect(
            lambda: self.applyRequested.emit(default_font_preferences())
        )
        try:
            self.set_applied(validate_font_preferences(preferences))
        except ValueError as error:
            self.set_applied(default_font_preferences())
            self.status.setText(str(error))
        self.set_language("ko")

    def set_applied(self, preferences):
        self._applied = dict(preferences)
        self.mode.setCurrentIndex(self.mode.findData(preferences["mode"]))
        self.family.setCurrentFont(QFont(preferences["family"]))
        self.file.setText(preferences["path"])
        self.file_family.setText(preferences["family"] if preferences["mode"] == "file" else "")
        self.font_size.setValue(preferences["size_px"])
        self._draft_changed()

    def _file_changed(self, *_args):
        if self.file.text().strip() != self._applied["path"]:
            self.file_family.clear()
        self._draft_changed()

    def _draft_changed(self, *_args):
        mode = self.mode.currentData()
        self.family.setEnabled(mode == "installed")
        self.file.setEnabled(mode == "file")
        self.browse_button.setEnabled(mode == "file")
        self.file_family.setEnabled(mode == "file")
        candidate = QFont(self.family.currentFont()) if mode == "installed" else QFont(self.font())
        candidate.setPixelSize(self.font_size.value())
        self.preview.setFont(candidate)

    def draft(self):
        return validate_font_preferences(
            {
                "mode": self.mode.currentData(),
                "family": self.family.currentFont().family()
                if self.mode.currentData() == "installed"
                else self.file_family.text()
                if self.mode.currentData() == "file"
                else "",
                "path": self.file.text(),
                "size_px": self.font_size.value(),
            }
        )

    def _request_apply(self):
        try:
            self.applyRequested.emit(self.draft())
        except ValueError as error:
            self.status.setText(str(error))

    def _browse(self):
        selected, _ = QFileDialog.getOpenFileName(
            self, "Trusted GUI font", self.file.text(), "Font (*.ttf *.otf)"
        )
        if selected:
            self.mode.setCurrentIndex(self.mode.findData("file"))
            self.file.setText(selected)

    def set_language(self, language):
        self.language = language
        ko = language == "ko"
        self.setTitle("GUI 글꼴 / TTF" if ko else "GUI font / TTF")
        for index, title in enumerate(
            (
                "기본 글꼴" if ko else "Default font",
                "설치된 글꼴" if ko else "Installed font",
                "로컬 TTF/OTF 파일" if ko else "Local TTF/OTF file",
            )
        ):
            self.mode.setItemText(index, title)
        labels = {
            "source": ("글꼴 소스", "Font source"),
            "family": ("설치된 글꼴", "Installed family"),
            "file": ("신뢰하는 글꼴 파일", "Trusted font file"),
            "file_family": ("불러온 글꼴", "Loaded family"),
            "size": ("본문 크기", "Body size"),
        }
        for key, label in self._labels:
            label.setText(labels[key][0 if ko else 1])
        self.apply_button.setText("적용하고 로컬 저장" if ko else "Apply and save locally")
        self.reset_button.setText("기본 글꼴 복원" if ko else "Restore defaults")
        self.notice.setText(
            "현재 GUI에만 적용합니다. 파일은 선택한 경로를 참조하며 복사·배포하지 않습니다. "
            "신뢰하는 TTF/OTF를 선택하세요. "
            "일부 글꼴에는 한글이 없어 대체 글꼴이 표시될 수 있습니다. "
            "음성 알림은 별도 알림/음성 설정에서 변경합니다."
            if ko
            else "Applies to this GUI only. "
            "Font files are referenced, never copied or redistributed. "
            "Choose trusted TTF/OTF files. Missing Korean glyphs may use a fallback font. "
            "Spoken alerts have separate notification/speech settings."
        )
