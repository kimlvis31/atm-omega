"""Eta's four-plus-five icon dashboard, independent of trading implementations."""

# Imports
import json

from PySide6.QtCore import QSize, Qt, Signal
from PySide6.QtWidgets import (
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QPlainTextEdit,
    QSizePolicy,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from omega.gui.icons import icon
from omega.gui.presentation import THEMES, translate

# Original page destinations; merged Omega workspaces keep their explicit subtab.
DESTINATIONS = (
    ("accounts", "accounts_tile", "accounts"),
    ("trading", "trader_tile", "trader"),
    ("charts", "analysis_tile", "analysis"),
    ("history", "history_tile", "history"),
    ("market", "market_tile", "market"),
    ("simulation", "simulation_tile", "simulation"),
    ("artifacts", "results_tile", "results"),
    ("database", "database_tile", "database"),
    ("neural", "neural_tile", "neural"),
)


class DashboardPage(QWidget):
    navigate = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.theme, self.language = "light", "ko"
        self._state = {}
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 0, 16, 0)
        layout.addStretch(2)
        self.tile_panel = QWidget()
        self.grid = QGridLayout(self.tile_panel)
        self.grid.setContentsMargins(0, 0, 0, 0)
        self.grid.setHorizontalSpacing(18)
        self.grid.setVerticalSpacing(20)
        self.tiles = []
        for index, (glyph, label, destination) in enumerate(DESTINATIONS):
            tile = QWidget()
            tile.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            column = QVBoxLayout(tile)
            column.setContentsMargins(0, 0, 0, 0)
            column.setSpacing(8)
            button = QToolButton()
            button.setProperty("role", "dashboardTile")
            button.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            button.setCursor(Qt.CursorShape.PointingHandCursor)
            button.clicked.connect(lambda _checked=False, key=destination: self.navigate.emit(key))
            caption = QLabel()
            caption.setProperty("role", "tileCaption")
            caption.setAlignment(Qt.AlignmentFlag.AlignCenter)
            caption.setWordWrap(True)
            column.addWidget(button, 1)
            column.addWidget(caption)
            row, start = (0, 1 + index * 2) if index < 4 else (1, (index - 4) * 2)
            self.grid.addWidget(tile, row, start, 1, 2)
            self.tiles.append((tile, button, caption, glyph, label))
        for column in range(10):
            self.grid.setColumnStretch(column, 1)
        layout.addWidget(self.tile_panel, 0, Qt.AlignmentFlag.AlignHCenter)
        layout.addStretch(2)
        self.activity_toggle = QToolButton()
        self.activity_toggle.setCheckable(True)
        self.activity_toggle.toggled.connect(self._show_activity)
        layout.addWidget(self.activity_toggle, 0, Qt.AlignmentFlag.AlignRight)
        self.activity_panel = QWidget()
        footer = QHBoxLayout(self.activity_panel)
        footer.setContentsMargins(0, 0, 0, 0)
        self.summary = QLabel()
        self.summary.setProperty("role", "muted")
        self.summary.setWordWrap(True)
        self.details = QToolButton()
        self.details.setCheckable(True)
        self.details.toggled.connect(self._show_details)
        footer.addWidget(self.summary, 1)
        footer.addWidget(self.details)
        layout.addWidget(self.activity_panel)
        self.activity_panel.hide()
        self.health = QPlainTextEdit()
        self.health.setReadOnly(True)
        self.health.setMaximumHeight(105)
        self.health.hide()
        layout.addWidget(self.health)
        self.set_presentation(self.theme, self.language)

    def _show_details(self, checked):
        self.health.setVisible(checked)

    def _show_activity(self, checked):
        self.activity_panel.setVisible(checked)
        self.health.setVisible(checked and self.details.isChecked())

    def resizeEvent(self, event):
        self.tile_panel.setFixedWidth(min(1100, max(640, int(self.width() * 0.7))))
        side = max(68, min(160, int(self.width() * 0.11), int(self.height() / 3.1)))
        for tile, button, _caption, _glyph, _label in self.tiles:
            tile.setMaximumWidth(side + 28)
            button.setFixedHeight(side)
            button.setIconSize(QSize(int(side * 0.82), int(side * 0.82)))
        super().resizeEvent(event)

    def update_state(self, state):
        self._state = state
        self.summary.setText(
            translate("dashboard_summary", self.language).format(
                accounts=len(state.get("accounts", [])),
                strategies=len(state.get("strategies", [])),
                jobs=len(state.get("jobs", [])),
            )
        )
        self.health.setPlainText(
            json.dumps(
                {
                    "core": state.get("core", {}),
                    "market": state.get("market", {}),
                    "research": state.get("research", {}),
                },
                ensure_ascii=False,
                indent=2,
            )
        )

    def set_presentation(self, theme, language):
        self.theme, self.language = theme, language
        for _tile, button, caption, glyph, label in self.tiles:
            text = translate(label, language)
            caption.setText(text)
            button.setAccessibleName(text)
            button.setToolTip(text)
            button.setIcon(icon(glyph, THEMES[theme]["text"], 160))
        self.details.setText(translate("diagnostics", language))
        self.activity_toggle.setText(translate("dashboard_activity", language))
        self.update_state(self._state)
