"""Eta-style database workspace; every query and mutation belongs to the core API."""

# Imports
import ipaddress
import math
from datetime import UTC, datetime
from urllib.parse import quote
from uuid import uuid4

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.gui.auxiliary_panel import AuxiliaryPanel
from omega.gui.auxiliary_schedule_panel import AuxiliarySchedulePanel
from omega.gui.depth_panel import DepthPanel

TEXT = {
    "title": ("데이터베이스", "DATABASE"),
    "peer": ("로컬 네트워크에서 실데이터 가져오기", "IMPORT OBSERVED DATA FROM LOCAL NETWORK"),
    "host": ("IP 주소", "IP address"),
    "port": ("포트", "Port"),
    "database": ("DB 이름", "Database"),
    "user": ("사용자", "User"),
    "password": ("비밀번호", "Password"),
    "import": ("선택 종목의 결측 구간 가져오기", "Import selected instrument gaps"),
    "peer_hint": (
        "Omega v2 스키마의 실제 관측만 가져옵니다. 비밀번호는 저장하지 않습니다.",
        "Import observed rows from Omega v2 only. Passwords are never saved.",
    ),
    "collection": ("데이터 수집 현황", "COLLECTION PROCESS"),
    "worker_state": ("수집 프로세스 / 데이터 준비", "Collector / data readiness"),
    "last_fetched": ("최근 수집", "Last fetched"),
    "speed": ("평균 속도", "Average speed"),
    "remaining": ("남은 구간", "Remaining ranges"),
    "eta": ("남은 예상 시간", "ETA"),
    "db_status": ("데이터베이스 상태", "DATABASE STATUS"),
    "engine": ("저장 엔진", "Storage engine"),
    "state": ("상태", "State"),
    "size": ("DB 사용량", "Database size"),
    "free": ("사용 가능 공간", "Free space"),
    "compression": ("압축 전 / 후", "Before / after compression"),
    "refresh": ("DB 상태 조회", "Read DB status"),
    "compress": ("DB 압축 · 미지원", "Compress · unavailable"),
    "symbols": ("종목 목록", "INSTRUMENT LIST"),
    "search": ("종목 검색", "Search instruments"),
    "select_all": ("표시 목록 선택", "Select displayed"),
    "release_all": ("선택 해제", "Release all"),
    "catalog_refresh": ("거래소 종목 정보 갱신", "Refresh exchange catalog"),
    "detail": ("선택 종목 정보 · 가용 구간", "SELECTED INSTRUMENT · AVAILABLE RANGES"),
    "stream": ("실시간 수집", "Collect stream"),
    "historical": ("과거 데이터도 수집", "Collect historical data"),
    "apply": ("수집 설정 적용", "Apply collection"),
    "lookback": ("조회 / 복구 범위 (1분봉)", "Lookback / recovery (1m bars)"),
    "refetch": ("결측 재수집", "Refetch gaps"),
    "reset": ("원본 초기화 · 미지원", "Reset source data · unavailable"),
    "repair_hint": (
        "결측 복구는 실제 관측을 재수집합니다. 연구용 보간·Monte Carlo와 별개입니다.",
        "Gap recovery fetches observed data, separately from research interpolation / Monte Carlo.",
    ),
    "unknown": ("미수신", "Not received"),
    "unsupported": ("미지원", "Unavailable"),
    "disconnected": (
        "코어 연결 끊김 · 표시 데이터가 오래됐을 수 있습니다",
        "Core disconnected · displayed data may be stale",
    ),
    "waiting": (
        "코어에서 데이터베이스 정보를 기다리는 중",
        "Waiting for database information from core",
    ),
    "none": ("종목을 선택하세요", "Select an instrument"),
    "queued": ("요청 접수 대기", "Waiting for request acceptance"),
    "action_unknown": (
        "결과 미확인 · 같은 요청을 재전송하지 않고 상태를 조회합니다",
        "Outcome unconfirmed · checking status without reposting",
    ),
    "market_usdm": ("USDⓈ-M 선물", "USDⓈ-M futures"),
    "market_spot": ("현물", "Spot"),
    "source_demo": ("DEMO · 합성 데이터", "DEMO · synthetic data"),
    "single_only": (
        "결측 재수집·피어 가져오기는 현재 한 종목씩 지원합니다.",
        "Gap refetch and peer import currently support one instrument at a time.",
    ),
    "mixed_mode": (
        "수집 모드가 혼합되었거나 미수신입니다. 적용할 모드를 명시적으로 선택하세요.",
        "Collection modes are mixed or unknown. Explicitly choose the modes to apply.",
    ),
    "availability_scope": (
        "비율은 저장된 최초~최신 관측 사이의 내부 결측 기준입니다. "
        "상장일부터 현재까지의 완전성이나 최신 수집 여부를 뜻하지 않습니다.",
        "Ratios describe internal gaps between first and latest stored observations. "
        "They do not establish completeness since listing or freshness up to now.",
    ),
    "rate_window": ("비율 대상", "Ratio window"),
    "latest_observed": ("최신 관측", "Latest observation"),
    "range_window": ("구간 목록 대상", "Range list window"),
    "range_limit": (
        "최신 관측 기준 최대 20,000분 · 시작 포함, 끝 제외",
        "At most 20,000 minutes ending at latest observation · start included, end excluded",
    ),
    "more_ranges": ("그 외 {count}구간", "{count} more ranges"),
    "repair_result": (
        "추가 {inserted}봉 · 미복구 {remaining}분",
        "Inserted {inserted} bars · {remaining} missing minutes remain",
    ),
    "tab_symbols": ("종목·수집 설정", "Instruments / collection"),
    "tab_detail": ("선택 종목 상세", "Selected instrument"),
    "tab_connection": ("연결·DB 상태", "Connection / DB status"),
}
HEADERS = {
    "ko": [
        "종목",
        "상태",
        "최초 1분봉 (UTC)",
        "저장 봉 수",
        "결측 봉 수",
        "희망 수집 모드",
        "데이터 준비",
    ],
    "en": [
        "Instrument",
        "Status",
        "First 1m (UTC)",
        "Stored bars",
        "Missing bars",
        "Requested collection",
        "Data readiness",
    ],
}
CHANNEL_HEADERS = {
    "ko": ["데이터", "최초 / 최근 (UTC)", "실관측 가용률 / 미저장 결측률", "가용 / 결측 구간"],
    "en": ["Channel", "First / latest (UTC)", "Observed / unstored gaps", "Available / gap ranges"],
}


class DatabasePage(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.language, self.theme = "ko", "light"
        self.snapshot = {}
        self._attached = bool(client.attached)
        self._have_snapshot = False
        self._expected_tag = None
        self._action_id = None
        self._action_kind = None
        self._busy = False
        self._labels = []
        self._instruments = {}
        self._selected_detail = {}
        self._selection_key = ()
        self._mode_dirty = False
        self._lookback_dirty = False
        self._submitted_lookback = None
        self._syncing_modes = False
        self._compact = False
        self._shell_embedded = False
        self._build()
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.timer = QTimer(self)
        self.timer.setInterval(5000)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.set_presentation("light", "ko")
        self.set_attached(self._attached)
        if self._attached:
            self.refresh()

    def tr_text(self, key):
        return TEXT[key][0 if self.language == "ko" else 1]

    def _label(self, key, *, group=False, button=False):
        widget = QGroupBox() if group else QPushButton() if button else QLabel()
        self._labels.append((widget, key, group))
        if isinstance(widget, QLabel):
            widget.setWordWrap(True)
            widget.setTextFormat(Qt.TextFormat.PlainText)
        return widget

    def _form_value(self, form, key):
        value = QLabel()
        value.setWordWrap(True)
        value.setTextFormat(Qt.TextFormat.PlainText)
        value.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        form.addRow(self._label(key), value)
        return value

    def _build(self):
        root = QVBoxLayout(self)
        self.title = self._label("title")
        self.title.setProperty("role", "title")
        root.addWidget(self.title)
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        left_widget = QWidget()
        left = QVBoxLayout(left_widget)
        self.peer_group = self._label("peer", group=True)
        peer = QFormLayout(self.peer_group)
        self.host = QLineEdit("127.0.0.1")
        self.host.setMaxLength(64)
        self.port = QSpinBox()
        self.port.setRange(1, 65535)
        self.port.setValue(5432)
        self.database_name = QLineEdit("omega")
        self.user = QLineEdit("omega_reader")
        self.password = QLineEdit()
        self.password.setEchoMode(QLineEdit.EchoMode.Password)
        for widget in (self.database_name, self.user):
            widget.setMaxLength(128)
        self.password.setMaxLength(1024)
        for key, widget in (
            ("host", self.host),
            ("port", self.port),
            ("database", self.database_name),
            ("user", self.user),
            ("password", self.password),
        ):
            peer.addRow(self._label(key), widget)
        self.import_button = self._label("import", button=True)
        peer.addRow(self.import_button)
        peer.addRow(self._label("peer_hint"))
        left.addWidget(self.peer_group)
        process = self._label("collection", group=True)
        process_form = QFormLayout(process)
        self.last_fetched = self._form_value(process_form, "last_fetched")
        self.fetch_speed = self._form_value(process_form, "speed")
        self.remaining = self._form_value(process_form, "remaining")
        self.eta = self._form_value(process_form, "eta")
        self.worker_state = self._form_value(process_form, "worker_state")
        left.addWidget(process)
        status = self._label("db_status", group=True)
        status_form = QFormLayout(status)
        self.db_engine = self._form_value(status_form, "engine")
        self.db_state = self._form_value(status_form, "state")
        self.db_size = self._form_value(status_form, "size")
        self.db_free = self._form_value(status_form, "free")
        self.compression = self._form_value(status_form, "compression")
        self.refresh_button = self._label("refresh", button=True)
        self.compress_button = self._label("compress", button=True)
        self.compress_button.setEnabled(False)
        status_form.addRow(self.refresh_button)
        status_form.addRow(self.compress_button)
        left.addWidget(status)
        left.addStretch()
        left_scroll = QScrollArea()
        left_scroll.setWidgetResizable(True)
        left_scroll.setWidget(left_widget)
        left_scroll.setMinimumWidth(240)
        self.left_panel = left_scroll
        self.splitter.addWidget(left_scroll)

        right_widget = QWidget()
        # Font metrics differ by OS. Keep the complete instrument/action panel reachable
        # by scrolling rather than forcing a small window beyond its requested height.
        self.right_panel = QScrollArea()
        self.right_panel.setWidgetResizable(True)
        self.right_panel.setWidget(right_widget)
        right = QVBoxLayout(right_widget)
        right.addWidget(self._label("symbols"))
        controls = QHBoxLayout()
        self.market = QComboBox()
        self.market.addItem("", "usdm")
        self.market.addItem("", "spot")
        self.search = QLineEdit()
        self.search.setClearButtonEnabled(True)
        self.select_all = self._label("select_all", button=True)
        self.release_all = self._label("release_all", button=True)
        controls.addWidget(self.market)
        controls.addWidget(self.search, 1)
        controls.addWidget(self.select_all)
        controls.addWidget(self.release_all)
        right.addLayout(controls)
        self.catalog_button = self._label("catalog_refresh", button=True)
        right.addWidget(self.catalog_button)
        self.content_splitter = QSplitter(Qt.Orientation.Vertical)
        self.table = QTableWidget(0, 7)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.verticalHeader().hide()
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.setMinimumHeight(140)
        self.content_splitter.addWidget(self.table)
        detail_widget = QWidget()
        detail = QVBoxLayout(detail_widget)
        detail.addWidget(self._label("detail"))
        self.selection_info = QLabel()
        self.selection_info.setWordWrap(True)
        self.selection_info.setTextFormat(Qt.TextFormat.PlainText)
        self.metadata = QLabel()
        self.metadata.setWordWrap(True)
        self.metadata.setTextFormat(Qt.TextFormat.PlainText)
        detail.addWidget(self.selection_info)
        detail.addWidget(self.metadata)
        self.quality_scope = QLabel()
        self.quality_scope.setWordWrap(True)
        self.quality_scope.setTextFormat(Qt.TextFormat.PlainText)
        detail.addWidget(self.quality_scope)
        self.channels = QTableWidget(4, 4)
        self.channels.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.channels.verticalHeader().hide()
        self.channels.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        self.channels.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.channels.setMinimumHeight(150)
        detail.addWidget(self.channels)
        self.auxiliary = AuxiliaryPanel(self.client)
        detail.addWidget(self.auxiliary)
        self.auxiliary_schedule = AuxiliarySchedulePanel(self.client)
        detail.addWidget(self.auxiliary_schedule)
        self.depth = DepthPanel(self.client)
        detail.addWidget(self.depth)
        mode = QHBoxLayout()
        self.stream = QCheckBox()
        self.historical = QCheckBox()
        self._labels.extend(
            ((self.stream, "stream", False), (self.historical, "historical", False))
        )
        self.lookback = QSpinBox()
        self.lookback.setRange(1, 20000)
        self.lookback.setValue(500)
        self.lookback.valueChanged.connect(lambda _value: setattr(self, "_lookback_dirty", True))
        mode.addWidget(self.stream)
        mode.addWidget(self.historical)
        mode.addWidget(self._label("lookback"))
        mode.addWidget(self.lookback)
        actions = QHBoxLayout()
        self.apply_button = self._label("apply", button=True)
        self.refetch_button = self._label("refetch", button=True)
        self.reset_button = self._label("reset", button=True)
        self.reset_button.setEnabled(False)
        for button in (self.apply_button, self.refetch_button, self.reset_button):
            actions.addWidget(button)
        detail_scroll = QScrollArea()
        detail_scroll.setWidgetResizable(True)
        detail_scroll.setWidget(detail_widget)
        self.detail_panel = detail_scroll
        self.content_splitter.addWidget(detail_scroll)
        self.content_splitter.setSizes([460, 310])
        self.content_splitter.setChildrenCollapsible(False)
        right.addWidget(self.content_splitter, 1)
        right.addLayout(mode)
        right.addLayout(actions)
        right.addWidget(self._label("repair_hint"))
        self.splitter.addWidget(self.right_panel)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([330, 950])
        self.splitter.setChildrenCollapsible(False)
        self.workspace = QStackedWidget()
        self.workspace.addWidget(self.splitter)
        self.compact_tabs = QTabWidget()
        self.workspace.addWidget(self.compact_tabs)
        root.addWidget(self.workspace, 1)
        self.status = QLabel()
        self.status.setWordWrap(True)
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        root.addWidget(self.status)
        self.refresh_button.clicked.connect(self.refresh)
        self.catalog_button.clicked.connect(
            lambda: self._action("catalog.refresh", {"market": self.market.currentData()})
        )
        self.apply_button.clicked.connect(self.apply_collection)
        self.refetch_button.clicked.connect(self.refetch)
        self.import_button.clicked.connect(self.import_peer)
        self.select_all.clicked.connect(self.table.selectAll)
        self.release_all.clicked.connect(self.table.clearSelection)
        self.search.textChanged.connect(self._render_table)
        self.market.currentIndexChanged.connect(self._market_changed)
        self.table.itemSelectionChanged.connect(self._selection_changed)
        self.stream.checkStateChanged.connect(self._mode_changed)
        self.historical.checkStateChanged.connect(self._mode_changed)

    def set_shell_embedded(self, embedded):
        """The application shell already supplies the page title."""
        self._shell_embedded = bool(embedded)
        self.title.setVisible(not self._shell_embedded)

    def _set_compact(self, compact):
        if compact == self._compact:
            return
        self._compact = compact
        if compact:
            # Reuse the same controls and selection; only their containers change.
            for widget, key in (
                (self.right_panel, "tab_symbols"),
                (self.detail_panel, "tab_detail"),
                (self.left_panel, "tab_connection"),
            ):
                self.compact_tabs.addTab(widget, self.tr_text(key))
            self.workspace.setCurrentWidget(self.compact_tabs)
            self.compact_tabs.setCurrentIndex(0)
        else:
            for widget in (self.left_panel, self.right_panel, self.detail_panel):
                self.compact_tabs.removeTab(self.compact_tabs.indexOf(widget))
            self.splitter.addWidget(self.left_panel)
            self.splitter.addWidget(self.right_panel)
            self.content_splitter.addWidget(self.detail_panel)
            self.content_splitter.setSizes([460, 310])
            self.splitter.setSizes([330, 950])
            self.workspace.setCurrentWidget(self.splitter)
        if not compact:
            for widget in (self.left_panel, self.right_panel, self.detail_panel):
                widget.show()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._set_compact(self.width() < 1050)

    def _value(self, value):
        return self.tr_text("unknown") if value is None else str(value)

    def _time(self, value):
        if type(value) is not int:
            return self.tr_text("unknown")
        try:
            return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y/%m/%d %H:%M")
        except (ValueError, OSError, OverflowError):
            return self.tr_text("unknown")

    def _bytes(self, value):
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            return self.tr_text("unknown")
        for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
            if value < 1024 or unit == "TiB":
                return f"{value:,.2f} {unit}"
            value /= 1024
        return self.tr_text("unknown")

    def _rate(self, value):
        if value is None or isinstance(value, bool):
            return self.tr_text("unknown")
        try:
            value = float(value)
            return (
                f"{value * 100:.3f}%"
                if math.isfinite(value) and 0 <= value <= 1
                else self.tr_text("unknown")
            )
        except (TypeError, ValueError):
            return self.tr_text("unknown")

    def selected_instruments(self):
        return [
            self.table.item(index.row(), 0).data(Qt.ItemDataRole.UserRole)
            for index in self.table.selectionModel().selectedRows()
        ]

    def _market_changed(self):
        self._have_snapshot = False
        self.snapshot = {}
        self._instruments = {}
        self._selected_detail = {}
        self._render_snapshot()
        self.refresh()

    def _sync_collection_mode(self, selected):
        self._syncing_modes = True
        try:
            for checkbox, field in (
                (self.stream, "collecting_stream"),
                (self.historical, "collecting_historical"),
            ):
                requested = (
                    "collection_requested"
                    if field == "collecting_stream"
                    else "historical_requested"
                )
                modes = [
                    self._instruments[key].get(requested, self._instruments[key].get(field))
                    for key in selected
                ]
                resolved = bool(modes) and all(type(value) is bool for value in modes)
                resolved = resolved and len(set(modes)) == 1
                checkbox.setTristate(not resolved)
                checkbox.setCheckState(
                    Qt.CheckState.Checked
                    if resolved and modes[0]
                    else Qt.CheckState.Unchecked
                    if resolved
                    else Qt.CheckState.PartiallyChecked
                )
        finally:
            self._syncing_modes = False
        self._mode_dirty = False

    def _modes_resolved(self):
        return all(
            checkbox.checkState() != Qt.CheckState.PartiallyChecked
            for checkbox in (self.stream, self.historical)
        )

    def _mode_changed(self, _state):
        if self._syncing_modes:
            return
        self._mode_dirty = True
        if self.stream.checkState() == Qt.CheckState.Unchecked:
            self.historical.blockSignals(True)
            self.historical.setTristate(False)
            self.historical.setChecked(False)
            self.historical.blockSignals(False)
        self._update_controls()

    def refresh(self):
        if not self._attached:
            return
        selected = self.selected_instruments()
        instrument = selected[0] if len(selected) == 1 else ""
        market = self.market.currentData()
        self._expected_tag = f"database-snapshot:{market}:{instrument}"
        path = "/api/database?market=" + market
        if instrument:
            path += "&instrument=" + quote(instrument, safe="")
        self.client.request("GET", path, tag=self._expected_tag)
        if self._action_id and self._busy:
            self.client.request(
                "GET",
                "/api/database/actions/" + self._action_id,
                tag="database-action:" + self._action_id,
            )

    def _tick(self):
        if self.isVisible():
            self.refresh()

    def update_state(self, state):
        self.auxiliary.update_state(state)
        self.auxiliary_schedule.update_state(state)
        self.depth.update_state(state)
        if isinstance(state.get("database"), dict) and "instruments" in state:
            self.accept_snapshot(state)

    def accept_snapshot(self, snapshot):
        self.snapshot = snapshot
        self._have_snapshot = True
        self._instruments = {
            item["instrument"]: item
            for item in snapshot.get("instruments", [])
            if isinstance(item, dict) and isinstance(item.get("instrument"), str)
        }
        self._selected_detail = snapshot.get("selected") or {}
        self._render_snapshot()
        source = self._value(snapshot.get("source"))
        if snapshot.get("source") == "demo":
            source = self.tr_text("source_demo")
        self.status.setText(f"{source} · {self._time(snapshot.get('updated_at_ms'))} UTC")

    def _render_snapshot(self):
        database = self.snapshot.get("database") or {}
        collection = self.snapshot.get("collection") or {}
        worker = collection.get("worker")
        if isinstance(worker, dict):
            ko = self.language == "ko"
            process = (
                ("실행" if ko else "running")
                if worker.get("running") is True
                else (
                    ("정지" if ko else "stopped")
                    if worker.get("running") is False
                    else self.tr_text("unknown")
                )
            )
            ready = (
                ("준비됨" if ko else "ready")
                if worker.get("ready") is True
                else (
                    ("미준비" if ko else "not ready")
                    if worker.get("ready") is False
                    else self.tr_text("unknown")
                )
            )
            self.worker_state.setText(f"{process} / {ready} · {worker.get('state', 'unknown')}")
        else:
            self.worker_state.setText(self.tr_text("unknown"))
        if collection.get("manual_apply_required") is True:
            self.worker_state.setText(
                self.worker_state.text()
                + "\n"
                + (
                    "희망 설정 복원됨 · 수집 설정 적용 후 재개"
                    if self.language == "ko"
                    else "Requested settings restored · apply collection to resume"
                )
            )
        self.db_engine.setText(self._value(database.get("engine")))
        self.db_state.setText(self._value(database.get("state")))
        self.db_size.setText(self._bytes(database.get("size_bytes")))
        self.db_free.setText(self._bytes(database.get("free_bytes")))
        self.compression.setText(
            self._bytes(database.get("compression_before_bytes"))
            + " / "
            + self._bytes(database.get("compression_after_bytes"))
        )
        fetched = collection.get("last_fetched")
        self.last_fetched.setText(self._time(fetched) + (" UTC" if type(fetched) is int else ""))
        speed = collection.get("rows_per_second")
        self.fetch_speed.setText(
            f"{speed:,.3f} / s"
            if type(speed) in (int, float) and math.isfinite(speed) and speed >= 0
            else self.tr_text("unknown")
        )
        self.remaining.setText(self._value(collection.get("remaining_ranges")))
        eta = collection.get("eta_ms")
        self.eta.setText(
            f"{eta // 3_600_000:02d}:{eta // 60_000 % 60:02d}:{eta // 1000 % 60:02d}"
            if type(eta) is int and eta >= 0
            else self.tr_text("unknown")
        )
        self._render_table()

    def _collection_mode(self, item):
        stream = item.get("collection_requested", item.get("collecting_stream"))
        history = item.get("historical_requested", item.get("collecting_historical"))
        if type(stream) is not bool or type(history) is not bool:
            return self.tr_text("unknown")
        return "ALL" if stream and history else "STREAM" if stream else "NONE"

    def _readiness(self, item):
        ko = self.language == "ko"
        state = str(item.get("collection_state", ""))[:80]
        value = item.get("data_ready")
        status = (
            ("준비됨" if ko else "ready")
            if value is True
            else (("미준비" if ko else "not ready") if value is False else self.tr_text("unknown"))
        )
        return status + (f" · {state}" if state else "")

    def _render_table(self):
        selected = set(self.selected_instruments())
        search = self.search.text().strip().casefold()
        entries = [
            item
            for key, item in sorted(self._instruments.items())
            if (not search or search in key.casefold())
            and item.get("market", key.split(":")[1] if ":" in key else None)
            == self.market.currentData()
        ]
        self.table.blockSignals(True)
        self.table.setUpdatesEnabled(False)
        self.table.clearSelection()
        self.table.setRowCount(len(entries))
        for index, item in enumerate(entries):
            key = item["instrument"]
            row = [
                item.get("symbol", key),
                item.get("status"),
                self._time(item.get("first_open_ms")),
                item.get("rows"),
                item.get("gap_rows"),
                self._collection_mode(item),
                self._readiness(item),
            ]
            for column, value in enumerate(row):
                cell = QTableWidgetItem(self._value(value))
                cell.setData(Qt.ItemDataRole.UserRole, key)
                cell.setToolTip(
                    key
                    + "\n"
                    + self._readiness(item)
                    + "\n"
                    + (
                        "프로세스 실행은 데이터 준비를 보장하지 않습니다."
                        if self.language == "ko"
                        else "A running process does not establish data readiness."
                    )
                )
                self.table.setItem(index, column, cell)
            if key in selected:
                self.table.selectionModel().select(
                    self.table.model().index(index, 0),
                    self.table.selectionModel().SelectionFlag.Select
                    | self.table.selectionModel().SelectionFlag.Rows,
                )
        self.table.setUpdatesEnabled(True)
        self.table.blockSignals(False)
        self._selection_changed(query=False)

    def _ranges(self, values):
        if not isinstance(values, list):
            return self.tr_text("unknown")
        if not values:
            return "—"
        text = "; ".join(
            f"{self._time(pair[0])} ~ {self._time(pair[1])}"
            if isinstance(pair, (list, tuple)) and len(pair) == 2
            else self.tr_text("unknown")
            for pair in values[:4]
        )
        if len(values) > 4:
            text += "; " + self.tr_text("more_ranges").format(count=len(values) - 4)
        return text

    def _quality_scope(self, values):
        window = values.get("range_window")
        if not isinstance(window, dict):
            window = {}
        return (
            f"{self.tr_text('rate_window')}: {self._time(values.get('first_open_ms'))} ~ "
            f"{self._time(values.get('last_open_ms'))} UTC · "
            f"{self.tr_text('latest_observed')}: {self._time(values.get('last_open_ms'))} UTC\n"
            f"{self.tr_text('range_window')}: {self._time(window.get('start_ms'))} ~ "
            f"{self._time(window.get('end_ms'))} UTC "
            f"({self.tr_text('range_limit')})\n{self.tr_text('availability_scope')}"
        )

    def _selection_changed(self, *, query=True):
        selected = self.selected_instruments()
        self.auxiliary.set_instrument(selected[0] if len(selected) == 1 else None)
        self.auxiliary_schedule.set_instrument(selected[0] if len(selected) == 1 else None)
        self.depth.set_instrument(selected[0] if len(selected) == 1 else None)
        identity = tuple(sorted(selected))
        if identity != self._selection_key or not self._mode_dirty:
            self._sync_collection_mode(selected)
        if identity != self._selection_key or not self._lookback_dirty:
            values = [self._instruments[key].get("history_lookback_minutes") for key in selected]
            if (
                values
                and all(type(value) is int and 1 <= value <= 20000 for value in values)
                and len(set(values)) == 1
            ):
                self.lookback.blockSignals(True)
                self.lookback.setValue(values[0])
                self.lookback.blockSignals(False)
                self.lookback.setToolTip("")
            else:
                self.lookback.setToolTip(
                    "희망 범위 혼합/미수신 · 적용 시 현재 입력값을 사용합니다."
                    if self.language == "ko"
                    else "Mixed/unavailable saved lookbacks; Apply uses the current field value."
                )
            self._lookback_dirty = False
        self._selection_key = identity
        if len(selected) != 1:
            self.selection_info.setText(
                self.tr_text("none")
                if not selected
                else f"{len(selected)} instruments selected · {self.tr_text('single_only')}"
            )
            self.metadata.clear()
            detail = {}
        else:
            key = selected[0]
            item = self._instruments.get(key, {})
            detail = (
                self._selected_detail if self._selected_detail.get("instrument") == key else item
            )
            self.selection_info.setText(
                f"{key} · {self._value(item.get('status'))} · {self._collection_mode(item)}"
                + " · "
                + self._readiness(item)
            )
            metadata = detail.get("metadata") or item.get("metadata") or {}
            fields = ("base_asset", "quote_asset", "price_tick", "quantity_step", "min_notional")
            names = {
                "base_asset": ("기초자산", "Base"),
                "quote_asset": ("기준통화", "Quote"),
                "price_tick": ("가격 단위", "Price tick"),
                "quantity_step": ("수량 단위", "Quantity step"),
                "min_notional": ("최소 주문 금액", "Min notional"),
            }
            self.metadata.setText(
                "  ·  ".join(
                    f"{names[field][0 if self.language == 'ko' else 1]}: "
                    f"{self._value(metadata.get(field))}"
                    for field in fields
                )
            )
        channels = detail.get("channels") or {}
        if len(selected) == 1 and "ohlcv" not in channels:
            channels = {"ohlcv": self._instruments.get(selected[0], {})}
        ohlcv = channels.get("ohlcv", {})
        self.quality_scope.setText(self._quality_scope(ohlcv))
        self.quality_scope.setToolTip(str(ohlcv.get("scope", ""))[:1000])
        for row, channel in enumerate(("ohlcv", "depth", "agg_trade", "metrics")):
            values = channels.get(channel, {})
            supported = values.get("supported", channel == "ohlcv" and bool(values))
            info = [
                channel.upper(),
                self.tr_text("unsupported"),
                self.tr_text("unsupported"),
                self.tr_text("unsupported"),
            ]
            if supported:
                info = [
                    channel.upper(),
                    self._time(values.get("first_open_ms"))
                    + " / "
                    + self._time(values.get("last_open_ms")),
                    self._rate(values.get("availability"))
                    + " / "
                    + self._rate(values.get("gap_rate", values.get("dummy_rate"))),
                    self._ranges(values.get("available_ranges"))
                    + " / "
                    + self._ranges(values.get("gap_ranges")),
                ]
            elif channel == "ohlcv":
                info = [
                    channel.upper(),
                    self.tr_text("unknown"),
                    self.tr_text("unknown"),
                    self.tr_text("unknown"),
                ]
            for column, value in enumerate(info):
                cell = QTableWidgetItem(value)
                cell.setToolTip(value)
                self.channels.setItem(row, column, cell)
        self._update_controls()
        if query and len(selected) == 1:
            self.refresh()

    def _update_controls(self):
        active = self._attached and self._have_snapshot and not self._busy
        selected = self.selected_instruments()
        caps = self.snapshot.get("capabilities") or {}
        for button, allowed in (
            (self.catalog_button, caps.get("catalog_refresh")),
            (
                self.apply_button,
                caps.get("collection_update") is True and bool(selected) and self._modes_resolved(),
            ),
            (self.refetch_button, caps.get("gap_refetch") is True and len(selected) == 1),
            (self.import_button, caps.get("local_network_import") is True and len(selected) == 1),
        ):
            button.setEnabled(active and allowed is True)
        self.apply_button.setToolTip("" if self._modes_resolved() else self.tr_text("mixed_mode"))
        for button in (self.refetch_button, self.import_button):
            button.setToolTip("" if len(selected) == 1 else self.tr_text("single_only"))
        self.refresh_button.setEnabled(self._attached)
        self.stream.setEnabled(active and bool(selected) and caps.get("collection_update") is True)
        self.historical.setEnabled(
            self.stream.isEnabled() and self.stream.checkState() == Qt.CheckState.Checked
        )

    def _action(self, kind, parameters):
        if not self._attached or self._busy or not self._have_snapshot:
            return
        capability = {
            "catalog.refresh": "catalog_refresh",
            "collection.set": "collection_update",
            "gaps.refetch": "gap_refetch",
            "peer.import": "local_network_import",
        }.get(kind)
        if self.snapshot.get("capabilities", {}).get(capability) is not True:
            return
        self._action_id = uuid4().hex
        self._action_kind = kind
        self._submitted_lookback = (
            parameters.get("lookback_minutes") if kind == "collection.set" else None
        )
        self._busy = True
        self._update_controls()
        self.status.setText(self.tr_text("queued"))
        self.client.request(
            "POST",
            "/api/database/actions",
            {
                "id": self._action_id,
                "kind": kind,
                "parameters": parameters,
            },
            tag="database-action:" + self._action_id,
        )

    def apply_collection(self):
        selected = self.selected_instruments()
        if selected and self._modes_resolved():
            stream = self.stream.checkState() == Qt.CheckState.Checked
            self._action(
                "collection.set",
                {
                    "instruments": selected,
                    "stream": stream,
                    "historical": stream and self.historical.isChecked(),
                    "lookback_minutes": self.lookback.value(),
                },
            )

    def refetch(self):
        selected = self.selected_instruments()
        if len(selected) == 1:
            self._action(
                "gaps.refetch",
                {"instrument": selected[0], "lookback_minutes": self.lookback.value()},
            )

    def import_peer(self):
        selected = self.selected_instruments()
        if len(selected) != 1:
            return
        try:
            address = ipaddress.ip_address(self.host.text().strip())
            if (
                not (address.is_private or address.is_loopback)
                or address.is_unspecified
                or address.is_multicast
            ):
                raise ValueError("Use a literal private or loopback IP address")
            if not self.database_name.text().strip() or not self.user.text().strip():
                raise ValueError("Database and user are required")
            self._action(
                "peer.import",
                {
                    "instrument": selected[0],
                    "host": str(address),
                    "port": self.port.value(),
                    "database": self.database_name.text().strip(),
                    "user": self.user.text().strip(),
                    "password": self.password.text(),
                    "lookback_minutes": self.lookback.value(),
                },
            )
        except ValueError as error:
            self.status.setText(str(error))
        finally:
            self.password.clear()

    def _result(self, tag, result):
        if tag.startswith("database-snapshot:"):
            if tag == self._expected_tag:
                self.accept_snapshot(result)
        elif self._action_id and tag == "database-action:" + self._action_id:
            state = result.get("state", "unknown")
            text = f"{self._action_id[:8]} · {state}"
            progress = result.get("progress")
            if type(progress) in (int, float) and math.isfinite(progress) and 0 <= progress <= 1:
                text += f" · {progress * 100:.1f}%"
            if result.get("error"):
                text += " · " + (
                    "Peer import failed"
                    if self._action_kind == "peer.import"
                    else str(result["error"])[:200]
                )
            details = result.get("result")
            if isinstance(details, dict) and all(
                type(details.get(key)) is int and details[key] >= 0
                for key in ("inserted_rows", "remaining_minutes")
            ):
                text += " · " + self.tr_text("repair_result").format(
                    inserted=details["inserted_rows"], remaining=details["remaining_minutes"]
                )
            self.status.setText(text)
            if state in {"complete", "completed", "failed", "rejected", "cancelled"}:
                self._busy = False
                if state in {"complete", "completed"}:
                    self._mode_dirty = False
                    if (
                        self._action_kind == "collection.set"
                        and self.lookback.value() == self._submitted_lookback
                    ):
                        self._lookback_dirty = False
                self.refresh()
            self._update_controls()

    def _failed(self, tag, _message):
        # Display no payload/server exception here: peer passwords must never echo into widgets.
        if tag.startswith("database-snapshot:") and tag == self._expected_tag:
            self._have_snapshot = False
            self.status.setText(self.tr_text("unknown"))
            self._update_controls()
        elif self._action_id and tag == "database-action:" + self._action_id:
            self.status.setText(self.tr_text("action_unknown"))
            self._busy = True
            self._update_controls()

    def set_attached(self, attached):
        previously = self._attached
        self._attached = bool(attached)
        self.auxiliary.set_attached(attached)
        self.auxiliary_schedule.set_attached(attached)
        self.depth.set_attached(attached)
        if not attached:
            self.password.clear()
            self._have_snapshot = False
            self.status.setText(self.tr_text("disconnected"))
        self._update_controls()
        if attached and not previously:
            self.refresh()

    def set_presentation(self, theme, language):
        self.theme = theme
        self.language = language if language in {"ko", "en"} else "ko"
        self.auxiliary.set_presentation(theme, self.language)
        self.auxiliary_schedule.set_presentation(theme, self.language)
        self.depth.set_presentation(theme, self.language)
        for widget, key, group in self._labels:
            (widget.setTitle if group else widget.setText)(self.tr_text(key))
        self.market.setItemText(0, self.tr_text("market_usdm"))
        self.market.setItemText(1, self.tr_text("market_spot"))
        self.search.setPlaceholderText(self.tr_text("search"))
        self.table.setHorizontalHeaderLabels(HEADERS[self.language])
        self.channels.setHorizontalHeaderLabels(CHANNEL_HEADERS[self.language])
        for index, key in enumerate(("tab_symbols", "tab_detail", "tab_connection")):
            self.compact_tabs.setTabText(index, self.tr_text(key))
        self._render_snapshot()
        if not self._have_snapshot:
            self.status.setText(
                self.tr_text("waiting") if self._attached else self.tr_text("disconnected")
            )
