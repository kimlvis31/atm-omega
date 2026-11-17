"""Independent resizable desktop shell, attached to the core by API only."""

# Imports
import json
from datetime import UTC, datetime
from pathlib import Path

from PySide6.QtCore import QByteArray, QEvent, QSignalBlocker, QSize, Qt, QTimer
from PySide6.QtGui import QCloseEvent, QFont, QFontDatabase
from PySide6.QtWidgets import (
    QApplication,
    QDockWidget,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStackedWidget,
    QStyle,
    QTabWidget,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from omega.gui.analysis_page import AnalysisPage
from omega.gui.connection import CoreClient
from omega.gui.dashboard import DashboardPage
from omega.gui.database_page import DatabasePage
from omega.gui.font_settings import (
    default_font_preferences,
    prepare_font,
    save_gui_preferences,
    validate_font_preferences,
)
from omega.gui.history_page import HistoryPage
from omega.gui.icons import icon
from omega.gui.incident_presentation import (
    incident_acknowledgement_text,
    incident_connection_text,
    incident_critical_summary,
    incident_details,
    incident_guidance,
    incident_headers,
    incident_row,
)
from omega.gui.margin_page import MarginPage
from omega.gui.notifications import NotificationController
from omega.gui.pages import (
    ArtifactPage,
    ChartsPage,
    ResearchPage,
    SettingsPage,
    TradingPage,
    data_table,
    plain_json,
    update_table,
)
from omega.gui.presentation import (
    THEMES,
    apply_form_language,
    configure_fonts,
    stylesheet,
    translate,
)
from omega.gui.shutdown_dialog import ShutdownDialog
from omega.gui.simulation_results import SimulationResultsPage

PAGE_IDS = (
    "dashboard",
    "trading",
    "charts",
    "data",
    "simulation",
    "neural",
    "artifacts",
    "settings",
    "database",
    "history",
    "analysis",
    "simulation_results",
    "margin_v2",
)


def load_preferences(root: Path) -> dict:
    path = root / "state" / "gui.preferences.json"
    try:
        if path.stat().st_size > 32_768:
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, UnicodeError):
        return {}


def entry_gate_text(core: dict, language: str) -> str:
    """A global gate is not a promise that every strategy can submit an entry."""
    ko = language == "ko"
    reasons = []
    if core.get("control_recording") is False:
        reasons.append("제어 기록 불가" if ko else "control recording unavailable")
    if core.get("paper_recording") is False:
        reasons.append("거래 기록 불가" if ko else "paper recording unavailable")
    if core.get("paper_manual_resume_required") is True:
        reasons.append(
            "대조 후 수동 재개 필요" if ko else "manual resume after reconciliation required"
        )
    control_paused = core.get("control_paused", core.get("paused", True))
    if control_paused:
        reasons.append(translate("paused_state", language))
    elif core.get("paper_paused") is True:
        reasons.append("거래 엔진 정지" if ko else "paper engine paused")
    if core.get("data_gate") is False:
        reason = str(core.get("data_gate_reason", "unavailable"))[:80]
        reasons.append(("데이터 제한: " if ko else "data restricted: ") + reason)
    if reasons:
        return " · ".join(reasons)
    if core.get("effective_entries_allowed") is True:
        return (
            "전체 진입 게이트 통과 · 전략별 준비/주문/예산은 별도"
            if ko
            else "Global entry gates passed · strategy readiness/orders/budgets are separate"
        )
    return (
        "신규 진입 제한 · 전체 게이트 상태 미확인"
        if ko
        else "Entries restricted · global gates unconfirmed"
    )


class OmegaWindow(QMainWindow):
    def __init__(self, root: Path, *, client=None):
        super().__init__()
        configure_fonts(QApplication.instance())
        self._default_font = QFont(QApplication.instance().font())
        # Qt's live combo also includes later application-only registrations.
        self._installed_font_families = frozenset(QFontDatabase.families())
        self._font_registration = None
        self._font_error = ""
        self._active_font_size = 12
        self.root = root.resolve()
        self.preferences = load_preferences(self.root)
        self.theme = self.preferences.get("theme", "light")
        self.language = self.preferences.get("language", "ko")
        self.font_preferences = default_font_preferences()
        try:
            self.font_preferences = validate_font_preferences(self.preferences.get("font", {}))
            font, self.font_preferences, self._font_registration = prepare_font(
                self.font_preferences,
                self._default_font,
                installed_families=self._installed_font_families,
            )
            QApplication.instance().setFont(font)
            self._active_font_size = self.font_preferences["size_px"]
        except (OSError, ValueError) as error:
            self._font_error = str(error)
        self.snapshot: dict = {}
        self._poll_count = 0
        self._commands: dict[str, tuple[str, dict]] = {}
        self._last_state_at = None
        self._incident_attached = False
        self._incident_ack_targets = {}
        self._incident_ack_feedback = {}
        self._uncertain_operation: str | None = None
        self._closing = False
        self._shutdown_dialog = None
        self.client = client or CoreClient(self.root, self)
        self.setWindowTitle("ATM Omega · Paper")
        self.setWindowIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_ComputerIcon))
        self.resize(1360, 860)
        self.setMinimumSize(800, 560)
        base = QWidget()
        layout = QVBoxLayout(base)
        layout.setContentsMargins(10, 7, 10, 2)
        layout.setSpacing(8)
        self.header = QFrame()
        self.header.setProperty("role", "header")
        header = self.header_layout = QGridLayout(self.header)
        header.setContentsMargins(0, 0, 0, 0)
        self.header_icons = QWidget()
        controls = QHBoxLayout(self.header_icons)
        controls.setContentsMargins(0, 0, 0, 0)
        self.home_button = QToolButton()
        self.menu_button = QToolButton()
        self.menu_button.setCheckable(True)
        self.settings_button = QToolButton()
        for button in (self.home_button, self.menu_button, self.settings_button):
            button.setProperty("role", "headerIcon")
            button.setIconSize(QSize(28, 28))
            button.setFixedSize(38, 38)
            controls.addWidget(button)
        controls.addStretch()
        self.title = QLabel()
        self.title.setProperty("role", "title")
        self.title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.clock_panel = QFrame()
        self.clock_panel.setProperty("role", "clockPanel")
        clocks = QHBoxLayout(self.clock_panel)
        clocks.setContentsMargins(8, 3, 8, 3)
        clocks.setSpacing(10)
        self.clock_fields = {}
        for zone in ("LOCAL", "UTC"):
            column = QVBoxLayout()
            column.setSpacing(1)
            label = QLabel(zone)
            label.setProperty("role", "clockCaption")
            field = QLineEdit()
            field.setReadOnly(True)
            field.setFocusPolicy(Qt.FocusPolicy.ClickFocus)
            field.setAlignment(Qt.AlignmentFlag.AlignCenter)
            field.setMinimumWidth(135)
            field.setMaximumWidth(165)
            field.setProperty("role", "clock")
            field.setAccessibleName(zone)
            column.addWidget(label)
            column.addWidget(field)
            clocks.addLayout(column)
            self.clock_fields[zone] = field
        # Keep the combined value available to accessibility/integration clients.
        self.clock = QLabel(self.clock_panel)
        self.clock.hide()
        self.close_button = QToolButton()
        self.close_button.setProperty("role", "headerIcon")
        self.close_button.setIconSize(QSize(28, 28))
        self.close_button.setFixedSize(38, 38)
        self.power_menu = QMenu(self.close_button)
        self.close_gui_action = self.power_menu.addAction("GUI 닫기")
        self.shutdown_action = self.power_menu.addAction("코어 종료…")
        self.shutdown_action.setEnabled(False)
        self.close_button.setMenu(self.power_menu)
        self.close_button.setPopupMode(QToolButton.ToolButtonPopupMode.InstantPopup)
        self.header_actions = QWidget()
        toolbar = QHBoxLayout(self.header_actions)
        toolbar.setContentsMargins(0, 0, 0, 0)
        self.connection_status = QLabel()
        self.connection_status.setProperty("role", "stateBadge")
        self.connection_status.setWordWrap(True)
        self.bell = QPushButton()
        self.bell.setIcon(self.style().standardIcon(QStyle.StandardPixmap.SP_MessageBoxInformation))
        self.pause_button = QPushButton()
        self.resume_button = QPushButton()
        toolbar.addWidget(self.pause_button)
        toolbar.addWidget(self.resume_button)
        toolbar.addWidget(self.bell)
        toolbar.addWidget(self.close_button)
        self._header_compact = None
        self._layout_header(False)
        layout.addWidget(self.header)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.navigation = QListWidget()
        self.navigation.setMinimumWidth(165)
        self.navigation.setMaximumWidth(270)
        self.navigation.addItems(PAGE_IDS)
        self.stack = QStackedWidget()
        self.dashboard = DashboardPage()
        self.trading = TradingPage(self.client)
        self.charts = ChartsPage(self.client, root=self.root)
        self.data = ResearchPage(self.client, ["export", "monte_carlo"])
        self.simulation = ResearchPage(self.client, ["simulate"])
        self.neural_job = ResearchPage(self.client, ["train"])
        self.neural_viewer = ArtifactPage()
        neural = QTabWidget()
        neural.addTab(self.neural_job, "Instance / dataset / training")
        neural.addTab(self.neural_viewer, "Run / metrics / 3D")
        self.artifacts = ArtifactPage()
        self.settings = SettingsPage(
            self.client, self.preferences | {"font": self.font_preferences}
        )
        self.settings.font_settings.applyRequested.connect(self._apply_font)
        if self._font_error:
            self.settings.font_settings.status.setText(self._font_error)
        self.database = DatabasePage(self.client)
        self.history = HistoryPage(self.client)
        self.analysis_library = AnalysisPage(self.client)
        self.simulation_results = SimulationResultsPage(self.client)
        self.margin_v2 = MarginPage(self.client)
        self.margin_v2.ledgerRequested.connect(self._open_margin_ledger)
        for page in (
            self.charts,
            self.database,
            self.history,
            self.analysis_library,
            self.simulation_results,
            self.margin_v2,
        ):
            page.set_shell_embedded(True)
        for page in (
            self.dashboard,
            self.trading,
            self.charts,
            self.data,
            self.simulation,
            neural,
            self.artifacts,
            self.settings,
            self.database,
            self.history,
            self.analysis_library,
            self.simulation_results,
            self.margin_v2,
        ):
            self.stack.addWidget(page)
        self.splitter.addWidget(self.navigation)
        self.splitter.addWidget(self.stack)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setChildrenCollapsible(False)
        self.splitter.setSizes([195, 1120])
        self.navigation.hide()
        layout.addWidget(self.splitter, 1)
        self.disclaimer = QLabel()
        self.disclaimer.setProperty("role", "muted")
        self.disclaimer.setWordWrap(True)
        layout.addWidget(self.disclaimer)
        self.setCentralWidget(base)
        self.verify_button = QPushButton("Verify command result")
        self.verify_button.hide()
        self.statusBar().addPermanentWidget(self.verify_button)
        self.verify_button.clicked.connect(self._verify_operation)
        self.incident_dock = QDockWidget(self)
        self._incident_dock_sized = False
        incident_widget = QWidget()
        incident_layout = QVBoxLayout(incident_widget)
        self.incident_connection = QLabel()
        self.incident_connection.setWordWrap(True)
        self.incident_connection.setTextFormat(Qt.TextFormat.PlainText)
        incident_layout.addWidget(self.incident_connection)
        self.incident_summary = QLabel()
        self.incident_summary.setWordWrap(True)
        self.incident_summary.setTextFormat(Qt.TextFormat.PlainText)
        incident_layout.addWidget(self.incident_summary)
        self.incident_table = data_table(incident_headers(self.language))
        self.incident_table.setColumnHidden(0, True)  # Stable identity still owns selection/ACK.
        header = self.incident_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Stretch)
        self.incident_table.setColumnHidden(1, self.incident_table.width() < 950)
        self.incident_table.installEventFilter(self)
        incident_layout.addWidget(self.incident_table)
        self.incident_guidance = QLabel()
        self.incident_guidance.setWordWrap(True)
        self.incident_guidance.setTextFormat(Qt.TextFormat.PlainText)
        self.incident_guidance.setTextInteractionFlags(
            Qt.TextInteractionFlag.TextSelectableByMouse
            | Qt.TextInteractionFlag.TextSelectableByKeyboard
        )
        self.incident_guidance.setMinimumWidth(0)
        self.incident_guidance_scroll = QScrollArea()
        self.incident_guidance_scroll.setWidgetResizable(True)
        self.incident_guidance_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.incident_guidance_scroll.setHorizontalScrollBarPolicy(
            Qt.ScrollBarPolicy.ScrollBarAlwaysOff
        )
        self.incident_guidance_scroll.setMinimumHeight(72)
        self.incident_guidance_scroll.setMaximumHeight(128)
        self.incident_guidance_scroll.setWidget(self.incident_guidance)
        self.incident_guidance_scroll.hide()
        incident_layout.addWidget(self.incident_guidance_scroll)
        self.incident_table.itemSelectionChanged.connect(self._update_incident_guidance)
        self.ack_button = QPushButton()
        incident_layout.addWidget(self.ack_button)
        self.incident_dock.setWidget(incident_widget)
        self.addDockWidget(Qt.DockWidgetArea.BottomDockWidgetArea, self.incident_dock)
        self.incident_dock.hide()
        self.notifications = NotificationController(self)
        self.pairing_timer = QTimer(self)
        self.pairing_timer.setSingleShot(True)
        self.pairing_timer.timeout.connect(self.settings.pairing.clear)
        self.navigation.currentRowChanged.connect(self._page_changed)
        self.dashboard.navigate.connect(self._dashboard_destination)
        self.home_button.clicked.connect(lambda: self.navigation.setCurrentRow(0))
        self.settings_button.clicked.connect(
            lambda: self.navigation.setCurrentRow(PAGE_IDS.index("settings"))
        )
        self.menu_button.toggled.connect(self.navigation.setVisible)
        self.close_gui_action.triggered.connect(self.close)
        self.shutdown_action.triggered.connect(self._open_shutdown)
        self.settings.presentationChanged.connect(self.set_presentation)
        self.bell.clicked.connect(self._toggle_incidents)
        self.pause_button.clicked.connect(lambda: self._command("pause"))
        self.resume_button.clicked.connect(self._resume)
        self.ack_button.clicked.connect(self._acknowledge)
        self.client.stateReceived.connect(self.accept_state)
        self.client.connectionChanged.connect(self._connection_changed)
        self.client.failed.connect(self._failed)
        self.client.resultReceived.connect(self._result)
        self.data.openArtifact.connect(lambda path: self.open_artifact(Path(path)))
        self.simulation.openArtifact.connect(self._open_simulation_result)
        self.simulation_results.configurationDraft.connect(self._simulation_draft)
        self.neural_job.openArtifact.connect(self._open_neural_artifact)
        self.navigation.setCurrentRow(0)
        self.set_presentation(self.theme, self.language)
        self._connection_changed(False)
        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)
        self._update_clock()
        geometry = self.preferences.get("geometry", "")
        if isinstance(geometry, str) and len(geometry) < 16_384:
            self.restoreGeometry(QByteArray.fromHex(geometry.encode("ascii", errors="ignore")))

    def start(self) -> None:
        self.client.start()

    def set_presentation(self, theme: str, language: str) -> None:
        self.theme = theme if theme in THEMES else "light"
        self.language = language if language in {"ko", "en"} else "en"
        self.setStyleSheet(stylesheet(self.theme, body_font_px=self._active_font_size))
        apply_form_language(self, self.language)
        for index, key in enumerate(PAGE_IDS):
            label = self.margin_v2.label() if key == "margin_v2" else translate(key, self.language)
            self.navigation.item(index).setText(label)
            self.navigation.item(index).setIcon(
                icon("trading" if key == "margin_v2" else key, THEMES[self.theme]["text"])
            )
        self.bell.setIcon(icon("notifications", THEMES[self.theme]["text"]))
        for button, glyph, label in (
            (self.home_button, "home", "home"),
            (self.settings_button, "settings", "settings"),
            (self.close_button, "close", "close_gui"),
        ):
            button.setIcon(icon(glyph, THEMES[self.theme]["text"], 32))
            button.setToolTip(translate(label, self.language))
            button.setAccessibleName(translate(label, self.language))
        self.menu_button.setToolTip(translate("navigation", self.language))
        self.menu_button.setIcon(icon("menu", THEMES[self.theme]["text"]))
        self.menu_button.setAccessibleName(translate("navigation", self.language))
        self.close_button.setToolTip("종료 메뉴" if self.language == "ko" else "Exit menu")
        self.close_button.setAccessibleName(self.close_button.toolTip())
        self.close_gui_action.setText("GUI 닫기" if self.language == "ko" else "Close GUI")
        self.shutdown_action.setText("코어 종료…" if self.language == "ko" else "Stop core…")
        self.title.setText(
            self.navigation.item(max(0, self.navigation.currentRow())).text().upper()
        )
        self.pause_button.setText(translate("pause", self.language))
        self.resume_button.setText(translate("resume", self.language))
        self.ack_button.setText(translate("ack", self.language))
        self.incident_dock.setWindowTitle(translate("incidents", self.language))
        self._update_incidents()
        self.disclaimer.setText(
            "ATM-OMEGA 0.1.0.dev0   ·   " + translate("paper_hint", self.language)
        )
        self.disclaimer.setToolTip(translate("close_hint", self.language))
        for page in (
            self.dashboard,
            self.database,
            self.history,
            self.charts,
            self.data,
            self.simulation,
            self.neural_job,
            self.neural_viewer,
            self.artifacts,
            self.analysis_library,
            self.simulation_results,
            self.margin_v2,
        ):
            page.set_presentation(self.theme, self.language)
        self.navigation.item(PAGE_IDS.index("margin_v2")).setText(self.margin_v2.label())
        if self.navigation.currentRow() == PAGE_IDS.index("margin_v2"):
            self.title.setText(self.margin_v2.label().upper())
        self.settings.mobile_hint.setText(translate("mobile_hint", self.language))
        self.settings.notification_settings.set_presentation(self.theme, self.language)
        self.settings.font_settings.set_language(self.language)
        self.trading.feed_controls.set_presentation(self.theme, self.language)
        self._connection_changed(self.client.attached)

    def _page_changed(self, index: int) -> None:
        self.stack.setCurrentIndex(index)
        self.title.setText(self.navigation.item(index).text().upper())
        self.menu_button.setChecked(False)
        if index == PAGE_IDS.index("charts"):
            self.charts.refresh()
        if index == PAGE_IDS.index("settings") and self.client.attached:
            self.client.request("GET", "/api/devices", tag="devices")
        if index == PAGE_IDS.index("database"):
            self.database.refresh()
        if index == PAGE_IDS.index("history"):
            self.history.refresh()
        if index == PAGE_IDS.index("analysis"):
            self.analysis_library.refresh()
        if index == PAGE_IDS.index("simulation_results"):
            self.simulation_results.refresh()
        if index == PAGE_IDS.index("margin_v2"):
            self.margin_v2.refresh()

    def _open_margin_ledger(self, account_id):
        self.navigation.setCurrentRow(PAGE_IDS.index("history"))
        self.history.open_margin_ledger(account_id)

    def _dashboard_destination(self, destination):
        page = {
            "accounts": "trading",
            "trader": "trading",
            "analysis": "analysis",
            "history": "history",
            "market": "charts",
            "simulation": "simulation",
            "results": "simulation_results",
            "database": "database",
            "neural": "neural",
        }.get(destination, "dashboard")
        self.navigation.setCurrentRow(PAGE_IDS.index(page))
        if page == "trading":
            self.trading.tabs.setCurrentIndex(
                {"accounts": 0, "trader": 2, "history": 1}[destination]
            )

    def _update_clock(self):
        now = datetime.now().astimezone()
        utc = now.astimezone(UTC)
        self.clock.setText(f"[LOCAL] {now:%Y/%m/%d %H:%M:%S}\n[UTC]   {utc:%Y/%m/%d %H:%M:%S}")
        self.clock_fields["LOCAL"].setText(f"{now:%Y-%m-%d %H:%M:%S}")
        self.clock_fields["UTC"].setText(f"{utc:%Y-%m-%d %H:%M:%S}")

    def _layout_header(self, compact):
        if compact == self._header_compact:
            return
        self._header_compact = compact
        for widget in (
            self.header_icons,
            self.title,
            self.header_actions,
            self.clock_panel,
            self.connection_status,
        ):
            self.header_layout.removeWidget(widget)
        grid = self.header_layout
        grid.addWidget(self.header_icons, 0, 0)
        if compact:
            grid.addWidget(self.header_actions, 0, 1, 1, 2, Qt.AlignmentFlag.AlignRight)
            grid.addWidget(self.title, 1, 0)
            grid.addWidget(self.clock_panel, 1, 1, 1, 2, Qt.AlignmentFlag.AlignRight)
            grid.addWidget(self.connection_status, 2, 0, 1, 3)
        else:
            grid.addWidget(self.title, 0, 1)
            grid.addWidget(self.header_actions, 0, 2, Qt.AlignmentFlag.AlignRight)
            grid.addWidget(self.connection_status, 1, 0, 1, 2)
            grid.addWidget(self.clock_panel, 1, 2, Qt.AlignmentFlag.AlignRight)
        grid.setColumnStretch(0, 1)
        grid.setColumnStretch(1, 2)
        grid.setColumnStretch(2, 1)

    def eventFilter(self, watched, event):
        if watched is getattr(self, "incident_table", None) and event.type() == QEvent.Type.Resize:
            watched.setColumnHidden(1, event.size().width() < 950)
        return super().eventFilter(watched, event)

    def resizeEvent(self, event):
        if hasattr(self, "header_layout"):
            self._layout_header(self.width() < 1150)
        super().resizeEvent(event)

    def _connection_changed(self, attached: bool) -> None:
        self._incident_attached = bool(attached)
        self.incident_connection.setText(
            incident_connection_text(
                self.snapshot.get("core", {}),
                attached=attached,
                last_received=self._last_state_at,
                language=self.language,
            )
        )
        self.shutdown_action.setEnabled(
            attached
            and self.snapshot.get("core", {}).get("capabilities", {}).get("shutdown_management")
            is True
        )
        text = translate("attached" if attached else "disconnected", self.language)
        if attached:
            core = self.snapshot.get("core", {})
            source = "DEMO · " if self.snapshot.get("market", {}).get("demo") else ""
            text = source + text + " · " + entry_gate_text(core, self.language)
        self.connection_status.setText(text)
        self.pause_button.setEnabled(attached)
        self.resume_button.setEnabled(
            attached and self.snapshot.get("mode") == "paper" and not self._uncertain_operation
        )
        self.ack_button.setEnabled(attached)
        self._update_incident_guidance()
        self.settings.set_attached(attached)
        self.database.set_attached(attached)
        self.analysis_library.set_attached(attached)
        self.simulation_results.set_attached(attached)
        self.margin_v2.set_attached(attached)
        self.trading.set_attached(attached and self.snapshot.get("mode") == "paper")
        for page in (self.data, self.simulation, self.neural_job):
            page.set_attached(attached)

    def accept_state(self, state: dict) -> None:
        self.snapshot = state
        self.shutdown_action.setEnabled(
            self.client.attached
            and state.get("core", {}).get("capabilities", {}).get("shutdown_management") is True
        )
        self._last_state_at = datetime.now(UTC)
        self._poll_count += 1
        self.dashboard.update_state(state)
        self.trading.update_state(state)
        self.database.update_state(state)
        self.history.update_state(state)
        self.charts.update_state(state)
        self.analysis_library.update_state(state)
        self.simulation_results.update_state(state)
        self.margin_v2.update_state(state)
        jobs = state.get("jobs", [])
        for page in (self.data, self.simulation, self.neural_job):
            page.update_jobs(jobs)
            page.update_health(state.get("research", {}))
        settings = state.get("settings", {})
        self.settings.update_settings(settings)
        for page in (self.data, self.simulation, self.neural_job):
            page.apply_settings(settings)
        notify = settings.get("notifications", {})
        core = state.get("core", {})
        # A headless core notification worker can explicitly own audio to avoid duplicates.
        gui_audio = core.get("notification_audio_owner", "gui") == "gui"
        self.notifications.configure(
            sound=bool(notify.get("sound", True)) and gui_audio,
            voice=bool(notify.get("speech", False))
            and core.get("notification_speech_owner", "gui") == "gui",
            language=self.language,
            repeat_s=int(notify.get("repeat_critical_s", 30)),
        )
        incidents = state.get("incidents", [])
        self.settings.notification_settings.update_speech(core.get("speech", {}))
        self.notifications.update(incidents)
        self._update_incidents()
        self.bell.setText(f"{translate('incidents', self.language)} ({len(incidents)})")
        self._connection_changed(True)
        if self._poll_count == 1 or self._poll_count % 20 == 0:
            self.client.request("GET", "/api/modules", tag="modules")
            self.client.request("GET", "/api/analysis-library", tag="analysis-binding")
        if self._poll_count % 5 == 0 and self.navigation.currentRow() == PAGE_IDS.index("charts"):
            self.charts.refresh()
        if self._poll_count % 10 == 0 and self.navigation.currentRow() == PAGE_IDS.index(
            "settings"
        ):
            self.client.request("GET", "/api/devices", tag="devices")

    def _update_incidents(self) -> None:
        self.incident_table.setHorizontalHeaderLabels(incident_headers(self.language))
        incidents = self.snapshot.get("incidents", [])
        selected = self.incident_table.item(self.incident_table.currentRow(), 0)
        selected_id = selected.text() if selected else None
        blocker = QSignalBlocker(self.incident_table)
        update_table(self.incident_table, [incident_row(item, self.language) for item in incidents])
        for row, incident in enumerate(incidents):
            details = incident_details(incident, self.language)
            for column in range(self.incident_table.columnCount()):
                self.incident_table.item(row, column).setToolTip(details)
        if selected_id is not None:
            selected_row = next(
                (index for index, item in enumerate(incidents) if item.get("id") == selected_id),
                None,
            )
            if selected_row is None:
                self.incident_table.clearSelection()
                self.incident_table.setCurrentCell(-1, -1)
            else:
                self.incident_table.selectRow(selected_row)
        del blocker
        self.incident_summary.setText(incident_critical_summary(incidents, self.language))
        retained = {item.get("id") for item in incidents if isinstance(item.get("id"), str)}
        self._incident_ack_feedback = {
            identity: value
            for identity, value in self._incident_ack_feedback.items()
            if identity in retained
        }
        self._update_incident_guidance()

    def _selected_incident(self):
        cell = self.incident_table.item(self.incident_table.currentRow(), 0)
        return next(
            (
                item
                for item in self.snapshot.get("incidents", [])
                if cell and item.get("id") == cell.text()
            ),
            None,
        )

    @staticmethod
    def _incident_signature(item):
        return {
            key: item.get(key, default)
            for key, default in (("id", "unknown"), ("revision", 1), ("severity", "warning"))
        }

    def _update_incident_guidance(self):
        incident = self._selected_incident()
        detail = incident_guidance(
            incident,
            self.snapshot.get("core", {}),
            attached=self._incident_attached,
            last_received=self._last_state_at,
            language=self.language,
        )
        pending = False
        if incident:
            feedback = self._incident_ack_feedback.get(incident.get("id"))
            if feedback:
                same = feedback["signature"] == self._incident_signature(incident) and feedback[
                    "epoch"
                ] == self.snapshot.get("core", {}).get("epoch")
                state = feedback["state"] if same else "changed"
                pending = (
                    same
                    and state in {"pending", "unconfirmed"}
                    and "command:" + feedback["operation"] in self._incident_ack_targets
                )
                detail += "\n\n" + incident_acknowledgement_text(state, self.language)
        self.incident_guidance.setText(detail)
        self.incident_guidance_scroll.setVisible(incident is not None)
        self.incident_guidance.setAccessibleName(
            "선택한 알림 설명" if self.language == "ko" else "Selected incident explanation"
        )
        self.incident_summary.setToolTip(
            incident_guidance(None, {}, attached=False, last_received=None, language=self.language)
        )
        self.ack_button.setEnabled(self._incident_attached and incident is not None and not pending)

    def _toggle_incidents(self) -> None:
        if self.incident_dock.isHidden():
            self._show_incidents()
        else:
            self.incident_dock.hide()

    def _show_incidents(self) -> None:
        self.incident_dock.show()
        if not self._incident_dock_sized:
            QTimer.singleShot(0, self._size_incidents_once)

    def _size_incidents_once(self) -> None:
        if self._closing or self.incident_dock.isHidden() or self._incident_dock_sized:
            return
        self._incident_dock_sized = True
        if not self.incident_dock.isFloating():
            # Initial layout must settle first. Later toggles preserve the user's resize.
            height = min(260, max(160, self.height() // 2))
            self.resizeDocks([self.incident_dock], [height], Qt.Orientation.Vertical)
            QTimer.singleShot(0, self._check_incident_space)

    def _check_incident_space(self) -> None:
        dock, table = self.incident_dock, self.incident_table
        if self._closing or dock.isHidden() or dock.isFloating():
            return
        required = sum(table.rowHeight(row) for row in range(min(3, table.rowCount())))
        if table.viewport().height() >= required:
            return
        # Some central pages cannot shrink further. Reuse this resizable dock rather
        # than forcing a larger main window or leaving only one incident readable.
        screen = self.screen()
        available = screen.availableGeometry() if screen is not None else self.geometry()
        dock.setFloating(True)
        dock.setWindowModality(Qt.WindowModality.NonModal)
        dock.resize(
            min(1100, max(1, available.width() - 40)), min(320, max(1, available.height() - 40))
        )
        dock.move(available.center() - dock.rect().center())
        dock.show()

    def _command(self, kind: str, payload: dict | None = None) -> None:
        if not self.client.attached:
            return
        payload = payload or {}
        operation = self.client.command(kind, payload)
        self._commands["command:" + operation] = (kind, payload)
        if kind == "incident.acknowledge":
            incident = next(
                (
                    item
                    for item in self.snapshot.get("incidents", [])
                    if item.get("id") == payload.get("incident_id")
                ),
                None,
            )
            if incident is not None:
                target = {
                    "signature": self._incident_signature(incident),
                    "epoch": self.snapshot.get("core", {}).get("epoch"),
                    "state": "pending",
                    "operation": operation,
                }
                self._incident_ack_targets["command:" + operation] = target
                self._incident_ack_feedback[incident["id"]] = dict(target)
                self._update_incident_guidance()
        self.statusBar().showMessage(
            f"Operation {operation} submitted; waiting for core result", 10_000
        )

    def _resume(self) -> None:
        answer = QMessageBox.question(
            self,
            translate("resume", self.language),
            "가상 거래 신규 진입을 재개할까요? 현재 코어의 복구 조건을 먼저 확인합니다."
            if self.language == "ko"
            else "Resume new paper entries after core safety checks?",
        )
        if answer == QMessageBox.StandardButton.Yes:
            self._command("resume")

    def _acknowledge(self) -> None:
        item = self.incident_table.item(self.incident_table.currentRow(), 0)
        if item:
            self._command("incident.acknowledge", {"incident_id": item.text()})

    def _result(self, tag: str, result: dict) -> None:
        if tag == "analysis-binding":
            self.trading.set_analysis_library(result)
            return
        if tag == "devices":
            self.settings.set_devices(result)
            return
        if tag.startswith(("command:", "operation:")):
            command_id = tag.split(":", 1)[1]
            operation = self._commands.get("command:" + command_id, ("", {}))
            ack_target = self._incident_ack_targets.get("command:" + command_id)
            if operation[0] == "incident.acknowledge" and (
                type(result.get("id")) is not str or result["id"] != command_id
            ):
                # A tag routes delivery; only the matching receipt can confirm its ACK.
                if ack_target:
                    current_feedback = self._incident_ack_feedback.get(
                        ack_target["signature"]["id"]
                    )
                    if (
                        current_feedback is None
                        or current_feedback["operation"] == ack_target["operation"]
                    ):
                        self._incident_ack_feedback[ack_target["signature"]["id"]] = dict(
                            ack_target, state="unconfirmed"
                        )
                self._uncertain_operation = command_id
                self.verify_button.show()
                self.statusBar().showMessage(
                    incident_acknowledgement_text("unconfirmed", self.language), 15_000
                )
                self._connection_changed(self.client.attached)
                return
            status = result.get("status", "unknown")
            if status in {"completed", "rejected"}:
                self._commands.pop("command:" + command_id, None)
                if self._uncertain_operation == command_id:
                    self._uncertain_operation = None
                    self.verify_button.hide()
            self.statusBar().showMessage(f"Operation {result.get('id', tag)} · {status}", 15_000)
            data = result.get("result", {})
            if operation[0] == "incident.acknowledge" and ack_target:
                durable = (
                    status == "completed"
                    and isinstance(data, dict)
                    and data.get("acknowledged") is True
                    and data.get("durable") is True
                )
                if durable:
                    ack_state = "durable"
                elif (
                    status == "volatile"
                    and isinstance(data, dict)
                    and data.get("acknowledged") is True
                    and data.get("durable") is False
                ):
                    ack_state = "volatile"
                elif status == "rejected":
                    ack_state = "rejected"
                else:
                    ack_state = "unconfirmed"
                feedback = dict(ack_target, state=ack_state)
                current_feedback = self._incident_ack_feedback.get(ack_target["signature"]["id"])
                if (
                    current_feedback is None
                    or current_feedback["operation"] == ack_target["operation"]
                ):
                    self._incident_ack_feedback[ack_target["signature"]["id"]] = feedback
                if durable and ack_target["epoch"] == self.snapshot.get("core", {}).get("epoch"):
                    self.notifications.tracker.acknowledge(ack_target["signature"])
                if status in {"completed", "rejected", "volatile"}:
                    self._incident_ack_targets.pop("command:" + command_id, None)
                self._update_incident_guidance()
            if isinstance(data, dict) and "code" in data and "expires_at_ms" in data:
                self.settings.pairing.setText(data["code"])
                self.pairing_timer.start(120_000)
            if (
                status == "completed"
                and isinstance(data, dict)
                and "notifications" in data
                and "logging" in data
            ):
                # A different settings form, or edits made while HTTP was in
                # flight, must not be erased by this completion. The structured
                # notification form tracks its own command and edit generation.
                def reflected(draft, current):
                    return isinstance(draft, dict) and all(
                        key in current
                        and (
                            reflected(value, current[key])
                            if isinstance(value, dict)
                            else current[key] == value
                        )
                        for key, value in draft.items()
                    )

                if data.get("revision", 0) >= self.settings.current.get("revision", 0):
                    if self.settings.dirty:
                        try:
                            draft = json.loads(self.settings.settings_json.toPlainText())
                            if reflected(draft, data):
                                self.settings.dirty = False
                        except (TypeError, ValueError):
                            pass  # Keep incomplete JSON as an editable draft.
                    self.settings.update_settings(data)
            if status in {"unknown", "volatile"}:
                self._uncertain_operation = command_id
                self.verify_button.show()
                self._connection_changed(self.client.attached)
                self._show_incidents()
                self.dashboard.health.setPlainText(plain_json(result))
            self.client.poll()

    def _failed(self, tag: str, message: str) -> None:
        if self._closing:
            return
        if tag.startswith("command:"):
            ack_target = self._incident_ack_targets.get(tag)
            if ack_target:
                current_feedback = self._incident_ack_feedback.get(ack_target["signature"]["id"])
                if (
                    current_feedback is None
                    or current_feedback["operation"] == ack_target["operation"]
                ):
                    self._incident_ack_feedback[ack_target["signature"]["id"]] = dict(
                        ack_target, state="unconfirmed"
                    )
                self._update_incident_guidance()
            self._uncertain_operation = tag.removeprefix("command:")
            self.verify_button.show()
            self._connection_changed(self.client.attached)
            message = (
                f"{message}. Operation {tag.removeprefix('command:')}: "
                "outcome not confirmed; not retried."
            )
        self.statusBar().showMessage(message, 10_000)
        for page in (self.data, self.simulation, self.neural_job):
            if tag.startswith(("new_job:", "job:")):
                page.result.setPlainText(message)

    def _verify_operation(self) -> None:
        if self._uncertain_operation and self.client.attached:
            self.client.request(
                "GET",
                "/api/operations/" + self._uncertain_operation,
                tag="operation:" + self._uncertain_operation,
            )

    def open_artifact(self, path: Path) -> bool:
        self.navigation.setCurrentRow(PAGE_IDS.index("artifacts"))
        return self.artifacts.open_path(path)

    def _open_neural_artifact(self, path: str) -> None:
        self.navigation.setCurrentRow(PAGE_IDS.index("neural"))
        self.stack.widget(PAGE_IDS.index("neural")).setCurrentIndex(1)
        self.neural_viewer.open_path(Path(path))

    def _open_simulation_result(self, path: str):
        self.navigation.setCurrentRow(PAGE_IDS.index("simulation_results"))
        selected = Path(path).resolve()
        if (
            selected.name == "simulation.json"
            and selected.parent.parent == self.root / "artifacts"
            and len(selected.parent.name) == 32
            and all(c in "0123456789abcdef" for c in selected.parent.name)
        ):
            self.simulation_results.select_run(selected.parent.name)
        else:
            self.simulation_results.open_path(selected)

    def _simulation_draft(self, parameters):
        self.navigation.setCurrentRow(PAGE_IDS.index("simulation"))
        self.simulation.apply_simulation_draft(dict(parameters))

    def closeEvent(self, event: QCloseEvent) -> None:
        self._closing = True
        if self._shutdown_dialog is not None:
            self._shutdown_dialog.close()
        self.client.stop()
        self.history.ledger_history.close()
        self.margin_v2.close()
        self.notifications.close()
        try:
            save_gui_preferences(self.root, self._preferences_snapshot())
        except (OSError, ValueError):
            pass
        if self._font_registration is not None:
            QFontDatabase.removeApplicationFont(self._font_registration)
            self._font_registration = None
        event.accept()

    def _open_shutdown(self):
        if not self.shutdown_action.isEnabled():
            return
        if self._shutdown_dialog is None:
            dialog = ShutdownDialog(self.client, self.snapshot, language=self.language, parent=self)
            self._shutdown_dialog = dialog
            dialog.finished.connect(lambda _result: self._shutdown_closed(dialog))
        self._shutdown_dialog.show()
        self._shutdown_dialog.raise_()
        self._shutdown_dialog.activateWindow()

    def _shutdown_closed(self, dialog):
        if self._shutdown_dialog is dialog:
            self._shutdown_dialog = None
        dialog.deleteLater()

    def _preferences_snapshot(self, font=None):
        return self.preferences | {
            "schema_version": 1,
            "theme": self.theme,
            "language": self.language,
            "geometry": bytes(self.saveGeometry().toHex()).decode("ascii"),
            "font": dict(self.font_preferences if font is None else font),
        }

    def _apply_font(self, value):
        registration = None
        try:
            font, preferences, registration = prepare_font(
                value, self._default_font, installed_families=self._installed_font_families
            )
            snapshot = self._preferences_snapshot(preferences)
            save_gui_preferences(self.root, snapshot)
        except (OSError, ValueError) as error:
            if registration is not None:
                QFontDatabase.removeApplicationFont(registration)
            self.settings.font_settings.status.setText(str(error))
            return
        previous = self._font_registration
        QApplication.instance().setFont(font)
        self._font_registration = registration
        self._active_font_size = preferences["size_px"]
        self.font_preferences, self.preferences = preferences, snapshot
        self._font_error = ""
        self.set_presentation(self.theme, self.language)
        self.settings.font_settings.set_applied(preferences)
        self.settings.font_settings.status.setText(
            "GUI 글꼴 적용·로컬 저장 완료"
            if self.language == "ko"
            else "GUI font applied and saved locally"
        )
        if previous is not None:
            QFontDatabase.removeApplicationFont(previous)
