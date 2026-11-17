"""Eta-style simulation run selection and inert, bounded result inspection."""

# Imports
import bisect
import html
import json
import math
import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from PySide6.QtCore import QObject, QPointF, QRunnable, Qt, QThreadPool, Signal
from PySide6.QtGui import QColor, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QAbstractItemView,
    QComboBox,
    QFileDialog,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QTreeWidget,
    QTreeWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.gui.presentation import THEMES
from omega.research.result_summary import _cost_quote_record, read_simulation_result

COST_QUOTE_PREVIEW_ROWS = 200
_TEXT = {
    "title": ("시뮬레이션 결과", "SIMULATION RESULTS"),
    "refresh": ("완료 작업 조회", "Refresh completed runs"),
    "open": ("결과 JSON 열기", "Open result JSON"),
    "copy": ("설정 복사", "Copy setup"),
    "copy_unavailable": ("설정 복사 · 기록 없음", "Copy setup · unavailable"),
    "copy_note": (
        "저장된 실행 요청을 새 초안으로 복사합니다. 현재 설치된 TS를 사용하며 "
        "당시 코드·모델까지 동일한 재현은 보장하지 않습니다. 별도 실행 확인이 필요합니다.",
        "Copy the recorded request into a new draft using currently installed TS modules. "
        "Historical code/model reproduction is not guaranteed. Submit requires a separate action.",
    ),
    "search_runs": ("실행 ID 검색", "Search run ID"),
    "search_records": ("종목·사유·시각 검색", "Search instrument, reason or time"),
    "summary": ("결과 요약", "Summary"),
    "curve": ("자산·낙폭 곡선", "Equity / drawdown"),
    "fills": ("체결 기록", "Fills"),
    "events": ("거래 사건", "Events"),
    "settings": ("설정·가정", "Settings / assumptions"),
    "unavailable": ("기록 없음", "Unavailable"),
    "empty": (
        "완료된 시뮬레이션을 선택하거나 결과 JSON을 여세요.",
        "Select a completed simulation or open its result JSON.",
    ),
    "loading": (
        "결과 읽는 중 · 이전 결과는 지웠습니다",
        "Loading result · previous result cleared",
    ),
    "disconnected": (
        "코어 연결 끊김 · 로컬 결과 열기는 가능합니다",
        "Core disconnected · local results remain available",
    ),
    "limit": (
        "최근 완료 작업 · 체결/사건 최대 2,000건 · 금액 표시 8자리, 정확값은 툴팁 · 원본 유지",
        "Recent completed jobs · tables up to 2,000 rows · "
        "amounts shown to 8 decimals; exact tooltip · source unchanged",
    ),
    "equity": ("평가자산", "Equity"),
    "wallet": ("지갑 잔고", "Wallet"),
    "funding_net": ("시나리오 Funding 순현금흐름", "Scenario funding net cashflow"),
    "drawdown_pct": ("초기 자산 포함 낙폭 (%)", "Drawdown including initial cash (%)"),
    "initial_wallet": ("초기 지갑", "Initial wallet"),
    "final_wallet": ("최종 지갑", "Final wallet"),
    "final_equity": ("최종 평가자산", "Final equity"),
    "net_wallet_change": ("지갑 변화", "Wallet change"),
    "unrealized_pnl": ("최종 미실현손익", "Final unrealized PnL"),
    "equity_return_pct": ("평가자산 수익률 (%)", "Equity return (%)"),
    "max_drawdown_pct": ("최대 표본 낙폭 (%)", "Maximum sampled drawdown (%)"),
    "fills_count": ("체결 횟수 (왕복 거래 아님)", "Fill count (not round trips)"),
    "pending_orders": ("미체결 주문", "Pending orders"),
    "reconcile": ("지갑 검산 차이", "Wallet reconciliation difference"),
    "curve_note": (
        "실제 UTC 시간축 · 표시만 최대 2,000점 축약 · 낙폭은 초기 현금과 저장 표본 기준",
        "UTC time axis · display reduced to 2,000 points · "
        "drawdown uses initial cash and recorded samples",
    ),
}


def _utc(value):
    if value is None:
        return "—"
    try:
        return datetime.fromtimestamp(float(value) / 1000, UTC).strftime("%Y/%m/%d %H:%M:%S")
    except (ValueError, TypeError, OverflowError, OSError):
        return "—"


def _label():
    widget = QLabel()
    widget.setWordWrap(True)
    widget.setTextFormat(Qt.TextFormat.PlainText)
    return widget


class _Signals(QObject):
    finished = Signal(int, object, str)


class _LoadResult(QRunnable):
    def __init__(self, generation, path, root):
        super().__init__()
        self.generation, self.path, self.root = generation, path, root
        self.signals = _Signals()

    def run(self):
        try:
            result = read_simulation_result(self.path, self.root)
            self.signals.finished.emit(self.generation, result, "")
        except (OSError, ValueError, TypeError, KeyError, UnicodeError, ArithmeticError) as error:
            self.signals.finished.emit(self.generation, None, str(error)[:300])


class ResultCurve(QWidget):
    """UTC-based line plot; original summary points remain available for cursor inspection."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.theme = "light"
        self.points, self.geometry_points, self.times = [], [], []
        self.label = ""
        self.gap_times = []
        self.setMinimumHeight(180)
        self.setMouseTracking(True)

    def set_points(self, label, rows, field):
        self.label = label
        self.points = []
        self.gap_times = [row["timestamp_ms"] for row in rows if row.get(field) is None]
        for row in rows:
            value = row.get(field)
            if value is not None:
                number = float(value)
                if math.isfinite(number):
                    self.points.append((row["timestamp_ms"], number))
        self.times = [point[0] for point in self.points]
        points = self.points
        if len(points) > 2000:
            selected = {0, len(points) - 1}
            for bucket in range(999):
                start, end = bucket * len(points) // 999, (bucket + 1) * len(points) // 999
                selected.add(min(range(start, end), key=lambda index: points[index][1]))
                selected.add(max(range(start, end), key=lambda index: points[index][1]))
            points = [points[index] for index in sorted(selected)]
        self.geometry_points = points
        self.update()

    def paintEvent(self, _event):
        colors = THEMES[self.theme]
        painter = QPainter(self)
        painter.fillRect(self.rect(), QColor(colors["surface"]))
        painter.setPen(QColor(colors["text"]))
        painter.drawText(10, 20, self.label)
        if not self.geometry_points:
            painter.drawText(10, 45, "—")
            return
        start, stop = self.times[0], self.times[-1]
        low = min(point[1] for point in self.geometry_points)
        high = max(point[1] for point in self.geometry_points)
        spread = max(high - low, abs(high) * 0.001, 1e-12)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        path = QPainterPath()
        for index, (timestamp, value) in enumerate(self.geometry_points):
            point = QPointF(
                16 + (timestamp - start) / max(1, stop - start) * max(1, self.width() - 32),
                self.height() - 42 - (value - low) / spread * max(1, self.height() - 80),
            )
            gap = index and bisect.bisect_right(self.gap_times, timestamp) > bisect.bisect_right(
                self.gap_times, self.geometry_points[index - 1][0]
            )
            path.moveTo(point) if index == 0 or gap else path.lineTo(point)
        painter.setPen(QPen(QColor(colors["accent"]), 2))
        painter.drawPath(path)
        painter.setPen(QColor(colors["text"]))
        painter.drawText(12, 40, f"min {low:.8g} / max {high:.8g}")
        painter.drawText(12, self.height() - 23, _utc(start) + " UTC")
        painter.drawText(
            self.rect().adjusted(12, 0, -12, -4),
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignBottom,
            _utc(stop) + " UTC",
        )

    def mouseMoveEvent(self, event):
        if self.points:
            fraction = min(1, max(0, (event.position().x() - 16) / max(1, self.width() - 32)))
            timestamp = self.times[0] + fraction * (self.times[-1] - self.times[0])
            index = min(bisect.bisect_left(self.times, timestamp), len(self.points) - 1)
            if index and abs(self.times[index - 1] - timestamp) < abs(
                self.times[index] - timestamp
            ):
                index -= 1
            point = self.points[index]
            self.setToolTip(html.escape(f"{_utc(point[0])} UTC · {point[1]:.12g}"))
        super().mouseMoveEvent(event)


class SimulationResultsPage(QWidget):
    configurationDraft = Signal(dict)

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        self.theme, self.language = "light", "ko"
        self.jobs, self.summary = {}, None
        self.path = None
        self._copy_parameters = None
        self._selected_id, self._expected_job = None, None
        self._generation, self._loading, self._next_load = 0, False, None
        self._tasks = {}
        self.pool = QThreadPool(self)
        self.pool.setMaxThreadCount(1)
        self._build()
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.set_presentation("light", "ko")
        self.set_attached(bool(client.attached))

    def text(self, key):
        return _TEXT[key][0 if self.language == "ko" else 1]

    def value(self, value):
        return self.text("unavailable") if value is None else str(value)

    def _table(self, headers):
        table = QTableWidget(0, len(headers))
        table.setHorizontalHeaderLabels(headers)
        table.verticalHeader().hide()
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.ResizeToContents)
        table.horizontalHeader().setStretchLastSection(True)
        return table

    def _build(self):
        layout = QVBoxLayout(self)
        self.title = _label()
        self.title.setProperty("role", "title")
        layout.addWidget(self.title)
        actions = QHBoxLayout()
        self.refresh_button, self.open_button = QPushButton(), QPushButton()
        self.copy_button = QPushButton()
        self.search_runs = QLineEdit()
        self.search_runs.setMaxLength(128)
        actions.addWidget(self.refresh_button)
        actions.addWidget(self.open_button)
        actions.addWidget(self.copy_button)
        actions.addWidget(self.search_runs, 1)
        layout.addLayout(actions)
        self.run_combo = QComboBox()
        layout.addWidget(self.run_combo)
        self.run_combo.hide()
        self.splitter = QSplitter(Qt.Orientation.Horizontal)
        self.runs = self._table(["Run", "Created (UTC)"])
        self.runs.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.runs.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.runs.setMinimumWidth(190)
        self.splitter.addWidget(self.runs)
        self.tabs = QTabWidget()
        summary = QWidget()
        summary_layout = QVBoxLayout(summary)
        self.metrics = self._table(["Metric", "Recorded / derived value"])
        self.costs = self._table(["Currency", "Fees", "Funding cost", "Realized PnL"])
        self.positions = self._table(["Instrument", "Open quantity"])
        for table in (self.metrics, self.costs, self.positions):
            table.setMinimumHeight(130)
            summary_layout.addWidget(table)
        summary_scroll = QScrollArea()
        summary_scroll.setWidgetResizable(True)
        summary_scroll.setWidget(summary)
        self.tabs.addTab(summary_scroll, "")
        curve = QWidget()
        curve_layout = QVBoxLayout(curve)
        self.curve_field = QComboBox()
        for field in ("equity", "wallet", "drawdown_pct"):
            self.curve_field.addItem("", field)
        self.plot = ResultCurve()
        self.curve_note = _label()
        curve_layout.addWidget(self.curve_field)
        curve_layout.addWidget(self.plot, 1)
        curve_layout.addWidget(self.curve_note)
        curve_scroll = QScrollArea()
        curve_scroll.setWidgetResizable(True)
        curve_scroll.setWidget(curve)
        self.tabs.addTab(curve_scroll, "")
        self.search_records = QLineEdit()
        self.search_records.setMaxLength(128)
        self.instrument = QComboBox()
        self.fills = self._table(
            ["UTC", "Instrument", "Quantity", "Price", "Fee", "Realized PnL", "Reason"]
        )
        fills_page = QWidget()
        fills_layout = QVBoxLayout(fills_page)
        filters = QHBoxLayout()
        filters.addWidget(self.instrument)
        filters.addWidget(self.search_records, 1)
        fills_layout.addLayout(filters)
        fills_layout.addWidget(self.fills)
        self.fill_count = _label()
        fills_layout.addWidget(self.fill_count)
        self.cost_quote_group = QGroupBox()
        quote_layout = QVBoxLayout(self.cost_quote_group)
        self.cost_quote_note = _label()
        self.cost_quotes = self._table(
            [
                "Fill UTC",
                "Instrument",
                "Side",
                "Quantity",
                "Raw price",
                "Final price",
                "Fee USDT",
                "Scenario difference USDT",
                "Issued UTC",
                "Eligible UTC",
                "Available UTC",
                "Fill ID",
                "Order ID",
                "Policy SHA256",
            ]
        )
        self.cost_quote_count = _label()
        quote_layout.addWidget(self.cost_quote_note)
        quote_layout.addWidget(self.cost_quotes)
        quote_layout.addWidget(self.cost_quote_count)
        self.cost_quote_group.hide()
        fills_layout.addWidget(self.cost_quote_group)
        self.tabs.addTab(fills_page, "")
        self.events = self._table(["UTC", "Kind", "Instrument", "Cost / reason"])
        event_page = QWidget()
        event_layout = QVBoxLayout(event_page)
        event_layout.addWidget(self.events)
        self.event_count = _label()
        event_layout.addWidget(self.event_count)
        self.tabs.addTab(event_page, "")
        self.settings = QTreeWidget()
        self.settings.setColumnCount(2)
        self.settings.setHeaderLabels(["Recorded field", "Value"])
        self.tabs.addTab(self.settings, "")
        self.splitter.addWidget(self.tabs)
        self.splitter.setStretchFactor(1, 1)
        self.splitter.setSizes([300, 900])
        layout.addWidget(self.splitter, 1)
        self.status = _label()
        self.limit_note = _label()
        layout.addWidget(self.status)
        layout.addWidget(self.limit_note)
        self.refresh_button.clicked.connect(self.refresh)
        self.open_button.clicked.connect(self.browse)
        self.copy_button.clicked.connect(self.copy_configuration)
        self.search_runs.textChanged.connect(self._render_jobs)
        self.runs.itemSelectionChanged.connect(self._selected)
        self.search_records.textChanged.connect(self._render_records)
        self.instrument.currentIndexChanged.connect(self._render_records)
        self.curve_field.currentIndexChanged.connect(self._render_curve)
        self.run_combo.activated.connect(self._combo_selected)

    def set_shell_embedded(self, embedded):
        self.title.setVisible(not embedded)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        compact = self.width() < 1000
        self.runs.setVisible(not compact)
        self.run_combo.setVisible(compact)

    def refresh(self):
        if self.client.attached:
            self.client.request("GET", "/api/jobs", tag="simulation-results:jobs")
            if self._selected_id:
                self._expected_job = "simulation-results:job:" + self._selected_id
                self.client.request("GET", "/api/jobs/" + self._selected_id, tag=self._expected_job)

    def update_state(self, state):
        self.update_jobs(state.get("jobs", []))

    def update_jobs(self, jobs):
        if not isinstance(jobs, list) or len(jobs) > 1000:
            return
        self.jobs = {
            row["id"]: row
            for row in jobs
            if isinstance(row, dict)
            and isinstance(row.get("id"), str)
            and re.fullmatch(r"[a-f0-9]{32}", row["id"])
            and row.get("kind") in {"simulate", "simulate_margin"}
            and row.get("state") == "complete"
        }
        self._render_jobs()

    def _render_jobs(self):
        query = self.search_runs.text().strip().casefold()
        rows = sorted(
            [row for key, row in self.jobs.items() if query in key.casefold()],
            key=lambda row: (
                row.get("created_at_ms") if type(row.get("created_at_ms")) is int else -1,
                row["id"],
            ),
            reverse=True,
        )
        self.runs.blockSignals(True)
        self.runs.clearSelection()
        self.runs.setRowCount(len(rows))
        for index, row in enumerate(rows):
            created = row.get("created_at_ms", row.get("created_ms"))
            self._cells(self.runs, index, [row["id"], _utc(created)])
            self.runs.item(index, 0).setData(Qt.ItemDataRole.UserRole, row["id"])
            if row["id"] == self._selected_id:
                self.runs.selectRow(index)
        self.runs.blockSignals(False)
        self.run_combo.blockSignals(True)
        self.run_combo.clear()
        for row in rows:
            self.run_combo.addItem(row["id"] + " · " + _utc(row.get("created_at_ms")), row["id"])
        self.run_combo.setCurrentIndex(self.run_combo.findData(self._selected_id))
        self.run_combo.blockSignals(False)

    def _selected(self):
        rows = self.runs.selectionModel().selectedRows()
        if not rows:
            return
        identity = self.runs.item(rows[0].row(), 0).data(Qt.ItemDataRole.UserRole)
        self._select_run(identity)

    def _combo_selected(self, _index):
        identity = self.run_combo.currentData()
        if identity:
            self._select_run(identity)

    def _select_run(self, identity):
        if identity == self._selected_id:
            return
        self._selected_id = identity
        self._generation += 1
        self._next_load = None
        self._expected_job = "simulation-results:job:" + identity
        self._clear()
        self.status.setText(self.text("loading"))
        if self.client.attached:
            self.client.request("GET", "/api/jobs/" + identity, tag=self._expected_job)

    def select_run(self, identity):
        """A job-ID route preserves path checks when linked from the simulation setup page."""
        if not isinstance(identity, str) or re.fullmatch(r"[a-f0-9]{32}", identity) is None:
            raise ValueError("simulation run ID must be 32 lowercase hexadecimal characters")
        if identity == self._selected_id:
            self.refresh()
        else:
            self._select_run(identity)

    def _result(self, tag, result):
        if tag == "simulation-results:jobs":
            self.update_jobs(result.get("jobs"))
        elif tag == self._expected_job and result.get("id") == self._selected_id:
            if (
                result.get("kind") not in {"simulate", "simulate_margin"}
                or result.get("state") != "complete"
            ):
                self.status.setText(self.text("unavailable"))
                return
            try:
                root = Path(self.client.root).resolve() / "artifacts"
                path = Path(result["result_file"])
                if path.resolve() != (root / self._selected_id / "simulation.json").resolve():
                    raise ValueError("completed simulation must reference its own simulation.json")
                self.open_path(path, allowed_root=root, parameters=result.get("parameters"))
            except (ValueError, KeyError, TypeError, AttributeError) as error:
                self.status.setText(str(error)[:300])

    def _failed(self, tag, _message):
        if tag == "simulation-results:jobs" or tag == self._expected_job:
            self.status.setText(self.text("unavailable"))

    def set_attached(self, attached):
        self.refresh_button.setEnabled(bool(attached))
        if not attached:
            self._expected_job = None
            self.status.setText(self.text("disconnected"))

    def browse(self):
        name, _ = QFileDialog.getOpenFileName(
            self, self.text("open"), "", "Simulation result (*.json)"
        )
        if name:
            self._expected_job = None
            self.open_path(Path(name))

    def open_path(self, path, *, allowed_root=None, parameters=None):
        if allowed_root is None:
            self._selected_id, self._expected_job = None, None
            self._render_jobs()
        self._copy_parameters = None
        if isinstance(parameters, dict):
            try:
                serialized = json.dumps(parameters, allow_nan=False)
                if len(serialized.encode("utf-8")) <= 1024 * 1024:
                    self._copy_parameters = json.loads(serialized)
            except (ValueError, TypeError, RecursionError):
                pass  # A valid result remains viewable without a copyable request snapshot.
        self._generation += 1
        request = self._generation, Path(path), allowed_root
        self._clear()
        self.status.setText(self.text("loading"))
        if self._loading:
            self._next_load = request  # Keep one latest selection, never an unbounded task queue.
        else:
            self._start_load(request)

    def _start_load(self, request):
        generation, path, root = request
        task = _LoadResult(generation, path, root)
        self._loading = True
        self._tasks[generation] = task
        task.signals.finished.connect(self._loaded)
        self.pool.start(task)

    def _loaded(self, generation, packet, error):
        self._tasks.pop(generation, None)
        self._loading = False
        if generation == self._generation:
            if error:
                self.status.setText(error)
            else:
                self.accept_result(packet)
        if self._next_load:
            request, self._next_load = self._next_load, None
            self._start_load(request)

    def _clear(self):
        self.summary, self.path = None, None
        for table in (
            self.metrics,
            self.costs,
            self.positions,
            self.fills,
            self.events,
            self.cost_quotes,
        ):
            table.setRowCount(0)
        self.cost_quote_group.hide()
        self.settings.clear()
        self.plot.set_points("", [], "equity")
        self._update_copy_button()

    def accept_result(self, packet):
        summary = packet["summary"]
        if "cost_quotes" in summary:
            try:
                _cost_quote_record(summary["cost_quotes"], summary["cost_policy"], summary["fills"])
            except (ValueError, TypeError, KeyError):
                self._clear()
                self.status.setText(
                    "비용 설명과 실제 체결 불일치 / Cost quotes do not match applied fills"
                )
                return
        self.summary, self.path = packet["summary"], Path(packet["path"])
        self.status.setText(self.path.name + " · SHA256 " + packet["sha256"][:16] + "…")
        if str(self.summary.get("model", "")).startswith("shared-virtual-margin"):
            self.status.setText(
                self.status.text() + " · Shared margin v2 / OHLCV proxy / full fills / CPU"
            )
        self.status.setToolTip(html.escape(str(self.path) + " · SHA256 " + packet["sha256"]))
        self.instrument.blockSignals(True)
        self.instrument.clear()
        self.instrument.addItem("ALL", "")
        self.instrument.addItems(
            sorted({str(row.get("instrument", "")) for row in self.summary["fills"] or []})
        )
        self.instrument.blockSignals(False)
        self._render_summary()
        self._render_records()
        self._render_settings()
        self._render_curve()
        self._update_copy_button()

    def _update_copy_button(self):
        available = self.summary is not None and self._copy_parameters is not None
        self.copy_button.setEnabled(available)
        self.copy_button.setText(self.text("copy" if available else "copy_unavailable"))
        self.copy_button.setToolTip(self.text("copy_note"))

    def copy_configuration(self):
        if self.summary is not None and self._copy_parameters is not None:
            self.configurationDraft.emit(json.loads(json.dumps(self._copy_parameters)))

    def _cells(self, table, row, values):
        for column, value in enumerate(values):
            exact = self.value(value)
            shown = exact
            numeric_column = (
                (table is self.metrics and column == 1)
                or (table is self.costs and column > 0)
                or (table is self.positions and column == 1)
                or (table is self.fills and column in {2, 3, 4, 5})
                or (table is self.events and column == 3)
            )
            tokens = exact.split(" ", 1)
            try:
                if numeric_column and re.fullmatch(
                    r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?", tokens[0]
                ):
                    number = Decimal(tokens[0])
                    if number and abs(number) < Decimal("0.00000001"):
                        text = f"{number:.8g}"
                    else:
                        text = f"{number if number else Decimal(0):,.8f}".rstrip("0").rstrip(".")
                    shown = text + (" " + tokens[1] if len(tokens) == 2 else "")
            except (InvalidOperation, ValueError):
                pass
            item = QTableWidgetItem(shown)
            item.setToolTip(html.escape(exact))
            table.setItem(row, column, item)

    def _render_summary(self):
        if self.summary is None:
            return
        currency = self.value(self.summary["wallet_currency"])
        rows = []
        for key, value in self.summary["metrics"].items():
            unit = "%" if key.endswith("_pct") else currency
            rows.append(
                [self.text(key), self.value(value) + (" " + unit if value is not None else "")]
            )
        for key in ("fills_count", "pending_orders"):
            rows.append([self.text(key), self.summary.get(key)])
        rows.append([self.text("reconcile"), self.summary["reconciliation"]["difference"]])
        self.metrics.setRowCount(len(rows))
        for row, values in enumerate(rows):
            self._cells(self.metrics, row, values)
        self.costs.setRowCount(len(self.summary["costs"]))
        fields = ("currency", "fee", "funding", "realized_pnl")
        labels = ["Currency", "Fees", "Funding cost", "Realized PnL"]
        if "funding" in self.summary:
            fields = ("currency", "fee", "funding", "funding_net", "realized_pnl")
            labels = [
                "Currency",
                "Fees",
                "Funding cost",
                "Signed scenario funding net",
                "Realized PnL",
            ]
        self.costs.setColumnCount(len(fields))
        self.costs.setHorizontalHeaderLabels(labels)
        for row, values in enumerate(self.summary["costs"]):
            self._cells(
                self.costs,
                row,
                [values.get(key) for key in fields],
            )
        positions = self.summary["positions"]
        self.positions.setRowCount(len(positions) if positions is not None else 1)
        for row, pair in enumerate(positions.items() if positions is not None else [(None, None)]):
            self._cells(self.positions, row, pair)
        for table in (self.metrics, self.costs, self.positions):
            height = table.horizontalHeader().height() + 8
            height += sum(table.rowHeight(row) for row in range(table.rowCount()))
            table.setFixedHeight(max(75, height))

    def _render_records(self):
        if self.summary is None:
            return
        query, instrument = (
            self.search_records.text().strip().casefold(),
            self.instrument.currentData(),
        )
        fills = [
            row
            for row in self.summary["fills"] or []
            if (not instrument or row.get("instrument") == instrument)
            and (not query or query in (str(row) + _utc(row.get("timestamp_ms"))).casefold())
        ]
        self.fills.setRowCount(min(2000, len(fills)))
        for index, row in enumerate(fills[:2000]):
            currency = self.summary["quote_assets"].get(
                row.get("instrument"), self.summary["wallet_currency"]
            )
            fee_currency = row.get("fee_asset", row.get("fee_currency", currency))
            pnl_currency = row.get("pnl_asset", currency)

            def money(key, asset, record=row):
                return (
                    self.value(record.get(key)) + " " + self.value(asset)
                    if record.get(key) is not None
                    else self.text("unavailable")
                )

            self._cells(
                self.fills,
                index,
                [
                    _utc(row.get("timestamp_ms")),
                    row.get("instrument"),
                    row.get("quantity"),
                    row.get("price"),
                    money("fee", fee_currency),
                    money("realized_pnl", pnl_currency),
                    row.get("tag"),
                ],
            )
        self.fill_count.setText(
            self.text("unavailable")
            if self.summary["fills"] is None
            else f"{min(2000, len(fills))} / {len(fills)} · " + self.text("fills_count")
        )
        self._render_cost_quotes(query, instrument)
        events = self.summary["events"] or []
        self.event_count.setText(
            self.text("unavailable")
            if self.summary["events"] is None
            else f"{min(2000, len(events))} / {len(events)}"
        )
        self.events.setRowCount(min(2000, len(events)))
        for index, row in enumerate(events[:2000]):
            self._cells(
                self.events,
                index,
                [
                    _utc(row.get("timestamp_ms")),
                    row.get("kind"),
                    row.get("instrument"),
                    (
                        f"{row.get('amount')} {row.get('settlement_asset')} · scenario cashflow"
                        if row.get("kind") == "funding_cashflow"
                        else row.get("cost", row.get("policy", row.get("tag")))
                    ),
                ],
            )

    def _render_cost_quotes(self, query, instrument):
        document = self.summary.get("cost_quotes")
        self.cost_quote_group.setVisible(document is not None)
        if document is None:
            self.cost_quotes.setRowCount(0)
            return
        rows = [
            row
            for row in document["rows"]
            if (not instrument or row["instrument"] == instrument)
            and (not query or query in (str(row) + _utc(row["event_time_ms"])).casefold())
        ]
        shown = rows[-COST_QUOTE_PREVIEW_ROWS:]
        self.cost_quotes.setRowCount(len(shown))
        for index, row in enumerate(shown):
            self._cells(
                self.cost_quotes,
                index,
                [
                    _utc(row["event_time_ms"]),
                    row["instrument"],
                    row["side"],
                    row["quantity"],
                    row["raw_price"],
                    row["final_price"],
                    row["fee"],
                    row["explanatory_slippage_amount"],
                    _utc(row["issued_at_ms"]),
                    _utc(row["eligible_at_ms"]),
                    _utc(row["available_at_ms"]),
                    row["fill_id"],
                    row["order_id"],
                    row["policy_sha256"],
                ],
            )
        self.cost_quote_count.setText(
            f"recent {len(shown)} / {len(rows)} · total {document['total']}"
        )

    def _render_curve(self):
        field = self.curve_field.currentData()
        currency = self.summary.get("wallet_currency") if self.summary else None
        label = (
            self.text(field) + " · " + ("%" if field == "drawdown_pct" else self.value(currency))
        )
        self.plot.set_points(label, self.summary["curve"] if self.summary else [], field)

    def _render_settings(self):
        self.settings.clear()
        budget = [1000]

        def add(parent, key, value):
            if budget[0] <= 0:
                return
            budget[0] -= 1
            nested = isinstance(value, (dict, list))
            item = QTreeWidgetItem([str(key)[:200], "" if nested else self.value(value)[:2000]])
            parent.addTopLevelItem(item) if parent is self.settings else parent.addChild(item)
            for child_key, child_value in (
                value.items()
                if isinstance(value, dict)
                else enumerate(value)
                if isinstance(value, list)
                else []
            ):
                add(item, child_key, child_value)

        for key in ("model", "backend", "spec", "input", "assumptions", "limitations"):
            add(self.settings, key, self.summary.get(key))
        if "funding" in self.summary:
            add(self.settings, "scenario funding", self.summary["funding"])
        if "cost_policy" in self.summary:
            add(self.settings, "execution scenario policy", self.summary["cost_policy"])
        if budget[0] == 0:
            self.settings.addTopLevelItem(QTreeWidgetItem(["…", "1,000 displayed fields limit"]))
        self.settings.expandToDepth(1)
        self.settings.resizeColumnToContents(0)

    def set_presentation(self, theme, language):
        self.theme = theme if theme in THEMES else "light"
        self.language = language if language in {"ko", "en"} else "ko"
        self.title.setText(self.text("title"))
        self.refresh_button.setText(self.text("refresh"))
        self.open_button.setText(self.text("open"))
        self._update_copy_button()
        self.search_runs.setPlaceholderText(self.text("search_runs"))
        self.search_records.setPlaceholderText(self.text("search_records"))
        for index, key in enumerate(("summary", "curve", "fills", "events", "settings")):
            self.tabs.setTabText(index, self.text(key))
        for index, field in enumerate(("equity", "wallet", "drawdown_pct")):
            self.curve_field.setItemText(index, self.text(field))
        self.curve_note.setText(self.text("curve_note"))
        self.cost_quote_group.setTitle("확정 체결 시나리오 설명 / Confirmed fill scenario quotes")
        self.cost_quote_note.setText(
            "원본 source와 적용된 FillEvent가 일치하는 설명입니다. 가격 차이는 시나리오 값이며 "
            "추가 현금 차감이 아닙니다. / Confirmed source/applied fills only; "
            "scenario price differences are not an additional wallet debit."
        )
        self.limit_note.setText(self.text("limit"))
        self.plot.theme = self.theme
        headers = (
            (
                ["실행 ID", "생성 (UTC)"],
                ["항목", "기록 / 계산 값"],
                ["통화", "수수료", "Funding 비용", "실현손익"],
                ["종목", "열린 수량"],
                [
                    "시각 (UTC)",
                    "종목",
                    "부호 있는 수량",
                    "가격",
                    "수수료·통화",
                    "실현손익·통화",
                    "사유",
                ],
                ["시각 (UTC)", "종류", "종목", "비용 / 사유"],
                ["기록 항목", "값"],
            )
            if self.language == "ko"
            else (
                ["Run", "Created (UTC)"],
                ["Metric", "Recorded / derived value"],
                ["Currency", "Fees", "Funding cost", "Realized PnL"],
                ["Instrument", "Open quantity"],
                [
                    "UTC",
                    "Instrument",
                    "Signed quantity",
                    "Price",
                    "Fee / currency",
                    "Realized PnL / currency",
                    "Reason",
                ],
                ["UTC", "Kind", "Instrument", "Cost / reason"],
                ["Recorded field", "Value"],
            )
        )
        for table, names in zip(
            (self.runs, self.metrics, self.costs, self.positions, self.fills, self.events),
            headers[:6],
            strict=True,
        ):
            table.setHorizontalHeaderLabels(names)
        self.settings.setHeaderLabels(headers[6])
        self._render_summary()
        self._render_records()
        self._render_curve()
        if self.summary is None:
            self.status.setText(self.text("empty"))
