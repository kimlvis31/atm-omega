"""Desktop pages consume API DTOs; local files are opened only by the inert viewer."""

# Imports
import json
import time
from pathlib import Path
from urllib.parse import quote

from PySide6.QtCore import QRegularExpression, QSignalBlocker, Qt, QUrl, Signal
from PySide6.QtGui import QDesktopServices, QRegularExpressionValidator
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFileDialog,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.gui.charts import ChartDrawer, MetricPlot, NetworkStructureView
from omega.gui.charts_page import ChartsPage  # noqa: F401
from omega.gui.configuration import ConfigurationEditor
from omega.gui.dashboard import DashboardPage  # noqa: F401
from omega.gui.feed_controls import FeedControls
from omega.gui.font_settings import FontSettings
from omega.gui.models import parse_candles, read_artifact
from omega.gui.presentation import translate
from omega.gui.runtime_settings import RuntimeSettingsPage
from omega.gui.settings_notifications import NotificationSettingsPage


def data_table(headers: list[str], parent=None) -> QTableWidget:
    table = QTableWidget(0, len(headers), parent)
    table.setHorizontalHeaderLabels(headers)
    table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    table.verticalHeader().hide()
    return table


def update_table(table: QTableWidget, rows: list[list]) -> None:
    selected = table.item(table.currentRow(), 0)
    selectedID = selected.text() if selected else None
    table.setUpdatesEnabled(False)
    table.setRowCount(min(len(rows), 1000))
    for rowID, row in enumerate(rows[:1000]):
        for columnID, value in enumerate(row[: table.columnCount()]):
            text = str(value)[:1000]
            item = table.item(rowID, columnID)
            if item is None or item.text() != text:
                table.setItem(rowID, columnID, QTableWidgetItem(text))
        if selectedID is not None and row and str(row[0]) == selectedID:
            table.selectRow(rowID)
    table.setUpdatesEnabled(True)


def plain_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, default=str)


class SeedInput(QLineEdit):
    """Preserve 64-bit reproducibility seeds without QSpinBox's 32-bit truncation."""

    def __init__(self, parent=None):
        super().__init__("0", parent)
        self.setMaxLength(19)
        self.setValidator(QRegularExpressionValidator(QRegularExpression("[0-9]{0,19}"), self))

    def value(self) -> int:
        text = self.text()
        if not text or not text.isascii() or not text.isdecimal():
            raise ValueError("Seed must be a nonnegative integer")
        seed = int(text)
        if seed >= 2**63:
            raise ValueError("Seed must be below 2^63")
        return seed

    def setValue(self, seed: int) -> None:
        if type(seed) is not int or not 0 <= seed < 2**63:
            raise ValueError("Seed must be a nonnegative integer below 2^63")
        self.setText(str(seed))


class TradingPage(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client = client
        layout = QVBoxLayout(self)
        self.accounts = data_table(["Account", "Market", "Balance / cash", "Status"])
        self.positions = data_table(["Account", "Instrument", "Quantity", "Entry", "Control"])
        self.strategies = data_table(["Strategy", "Module", "Instrument", "Status"])
        tabs = self.tabs = QTabWidget()
        tabs.addTab(self.accounts, "Accounts")
        tabs.addTab(self.positions, "Positions")
        tabs.addTab(self.strategies, "Strategies")
        setup = QWidget()
        form = QFormLayout(setup)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        title = QLabel(translate("account_setup", "ko"))
        title.setProperty("role", "section")
        title.setProperty("omega_message_key", "account_setup")
        form.addRow(title)
        self.account_id = QLineEdit("paper-main")
        self.balance = QLineEdit("10000")
        self.create_button = QPushButton("Create paper account")
        form.addRow("Account ID", self.account_id)
        form.addRow("Initial balance", self.balance)
        form.addRow(self.create_button)
        self.instance = QLineEdit("tef-01")
        self.module = QComboBox()
        self.module.addItem("tef", "tef")
        self.instrument = QLineEdit("binance:usdm:BTCUSDT")
        self.budget = QLineEdit("1000")
        self.configuration = QPlainTextEdit("{}")
        self.configuration.setMaximumHeight(100)
        self.configuration_editor = ConfigurationEditor(self.configuration)
        self.analysis = QPlainTextEdit("{}")
        self.analysis.setMaximumHeight(100)
        self.analysis_editor = ConfigurationEditor(self.analysis)
        self.analysis_instances = []
        self.analysis_choice = QComboBox()
        self.analysis_choice.addItem("직접 설정 / Manual configuration", None)
        self.analysis_editor.set_schema(
            {
                "type": "object",
                "properties": {
                    "ema_period": {
                        "type": "integer",
                        "title": "EMA samples",
                        "default": 20,
                        "minimum": 1,
                    },
                    "mmacd_periods": {
                        "type": "array",
                        "title": "MMACD fast / slow",
                        "default": [12, 26],
                    },
                    "signal_period": {
                        "type": "integer",
                        "title": "MMACD signal",
                        "default": 9,
                        "minimum": 1,
                    },
                    "momentum_samples": {
                        "type": "integer",
                        "title": "MFI / DMI samples",
                        "default": 14,
                        "minimum": 1,
                    },
                },
            }
        )
        self.add_button = QPushButton("Attach paper strategy")
        strategy_title = QLabel(translate("strategy_setup", "ko"))
        strategy_title.setProperty("role", "section")
        strategy_title.setProperty("omega_message_key", "strategy_setup")
        form.addRow(strategy_title)
        form.addRow("Strategy instance", self.instance)
        form.addRow("TS module", self.module)
        form.addRow("Instrument", self.instrument)
        form.addRow("Budget", self.budget)
        form.addRow(QLabel("Strategy configuration"))
        form.addRow(self.configuration_editor)
        form.addRow(QLabel("Analysis configuration"))
        form.addRow("Saved instrument CA", self.analysis_choice)
        form.addRow(self.analysis_editor)
        self.advanced = QGroupBox(translate("advanced_spec", "ko"))
        self.advanced.setProperty("omega_message_key", "advanced_spec")
        self.advanced.setCheckable(True)
        self.advanced.setChecked(False)
        documents = QWidget()
        documents_layout = QVBoxLayout(documents)
        documents_layout.addWidget(self.configuration)
        documents_layout.addWidget(self.analysis)
        advanced_layout = QVBoxLayout(self.advanced)
        advanced_layout.addWidget(documents)
        documents.hide()
        self.advanced.toggled.connect(documents.setVisible)
        form.addRow(self.advanced)
        form.addRow(self.add_button)
        timestamp = (int(time.time() * 1000) // 60000 - 1) * 60000
        self.candle = QPlainTextEdit(
            plain_json(
                {
                    "open_time_ms": timestamp,
                    "open": "100",
                    "high": "103",
                    "low": "99",
                    "close": "102",
                    "volume": "10",
                }
            )
        )
        self.candle.setMaximumHeight(160)
        self.step_button = QPushButton("Process paper candle")
        manual_title = QLabel("Manual paper candle (closed 1m; no real order)")
        manual_title.setWordWrap(True)
        form.addRow(manual_title)
        form.addRow(self.candle)
        form.addRow(self.step_button)
        self.feed_controls = FeedControls(client)
        form.addRow(self.feed_controls)
        self.command_result = QLabel()
        self.command_result.setWordWrap(True)
        form.addRow(self.command_result)
        strategy_actions = QGroupBox("선택 전략 관리 / Selected paper strategy")
        actions_layout = QFormLayout(strategy_actions)
        actions_layout.addRow(
            QLabel("전략 목록에서 선택하세요. 복구 후 신규 진입은 별도로 재개해야 합니다.")
        )
        self.recover_button = QPushButton("저장 상태 복구 / Recover checkpoint")
        self.reset_model_button = QPushButton("모델 초기화 후 복구 / Reset model and recover")
        self.local_resume_button = QPushButton("전략 손절 정지 해제 / Reset local stop latch")
        self.cancel_order_button = QPushButton("선택 전략 미체결 취소 / Cancel pending paper order")
        self.remove_strategy_button = QPushButton("선택 전략 제거 / Remove strategy")
        for button in (
            self.recover_button,
            self.reset_model_button,
            self.local_resume_button,
            self.cancel_order_button,
            self.remove_strategy_button,
        ):
            actions_layout.addRow(button)
        form.addRow(strategy_actions)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(setup)
        self.workspace = QSplitter(Qt.Orientation.Horizontal)
        self.workspace.addWidget(tabs)
        self.workspace.addWidget(scroll)
        self.workspace.setChildrenCollapsible(False)
        self.workspace.setSizes([760, 460])
        self.workspace.setStretchFactor(0, 1)
        layout.addWidget(self.workspace)
        self.create_button.clicked.connect(
            lambda: self._send(
                "paper.account.create",
                {
                    "account_id": self.account_id.text().strip(),
                    "balance": self.balance.text().strip(),
                },
            )
        )
        self.add_button.clicked.connect(self._add_strategy)
        self.step_button.clicked.connect(self._step)
        self.recover_button.clicked.connect(lambda: self._selected_action("paper.strategy.recover"))
        self.reset_model_button.clicked.connect(
            lambda: self._selected_action("paper.strategy.recover", reset=True)
        )
        self.local_resume_button.clicked.connect(
            lambda: self._selected_action("paper.strategy.resume")
        )
        self.cancel_order_button.clicked.connect(
            lambda: self._selected_action("paper.order.cancel")
        )
        self.remove_strategy_button.clicked.connect(
            lambda: self._selected_action("paper.strategy.remove")
        )
        client.modulesReceived.connect(self._set_modules)
        self._module_definitions = {}
        self._module_drafts = {}
        self._active_module = "tef"
        self.module.currentIndexChanged.connect(self._module_selected)
        self.instrument.textChanged.connect(self._update_analysis_choice)
        self.analysis_choice.currentIndexChanged.connect(
            lambda: self.analysis_editor.setEnabled(self.analysis_choice.currentData() is None)
        )
        self.set_attached(False)

    def resizeEvent(self, event):
        self.workspace.setOrientation(
            Qt.Orientation.Vertical if self.width() < 950 else Qt.Orientation.Horizontal
        )
        super().resizeEvent(event)

    def _send(self, kind: str, payload: dict) -> None:
        self.client.command(kind, payload)
        self.command_result.setText("Submitted to the paper core; check the command result/status.")

    def _selected_action(self, kind, *, reset=False):
        identity = self._selected_strategy_id()
        if identity is None or not self.client.attached:
            self.command_result.setText(
                "전략 목록에서 먼저 전략을 선택하세요. / Select a strategy first."
            )
            return
        if (
            reset
            and QMessageBox.question(
                self,
                "모델 초기화 / Reset model",
                "분석 누적 상태와 전략의 손절·재진입 기억을 초기화합니다. "
                "포지션·체결·잔고는 유지하고 신규 진입을 정지합니다. 계속할까요?",
            )
            != QMessageBox.StandardButton.Yes
        ):
            return
        # State polling continues inside modal dialogs. Never retain a C++ item
        # across one or transfer approval to a replacement selection.
        if not self.client.attached or self._selected_strategy_id() != identity:
            self.command_result.setText("연결 또는 선택이 바뀌었습니다. 다시 선택하세요.")
            return
        self._send(kind, {"instance": identity, **({"reset_model": True} if reset else {})})

    def _selected_strategy_id(self):
        selected = self.strategies.selectionModel().selectedRows()
        if len(selected) != 1:
            return None
        item = self.strategies.item(selected[0].row(), 0)
        return item.text() if item is not None else None

    def _add_strategy(self) -> None:
        try:
            config = json.loads(self.configuration.toPlainText())
            if not isinstance(config, dict):
                raise ValueError("Strategy configuration must be a JSON object")
            binding = self.analysis_choice.currentData()
            analysis = {} if binding else json.loads(self.analysis.toPlainText())
            if not isinstance(analysis, dict):
                raise ValueError("Analysis configuration must be a JSON object")
            self._send(
                "paper.strategy.add",
                {
                    "instance": self.instance.text().strip(),
                    "account_id": self.account_id.text().strip(),
                    "module_id": self.module.currentData() or self.module.currentText(),
                    "instrument": self.instrument.text().strip(),
                    "configuration": config,
                    "analysis": analysis,
                    "budget": self.budget.text().strip(),
                    **(
                        {"analysis_id": binding["id"], "analysis_revision": binding["revision"]}
                        if binding
                        else {}
                    ),
                },
            )
        except ValueError as error:
            self.command_result.setText(str(error))

    def _step(self) -> None:
        try:
            candle = json.loads(self.candle.toPlainText())
            if not isinstance(candle, dict):
                raise ValueError("Candle must be a JSON object")
            parse_candles([candle])
            self._send(
                "paper.step", {"instrument": self.instrument.text().strip(), "candle": candle}
            )
        except (ValueError, KeyError, TypeError) as error:
            self.command_result.setText(str(error))

    def set_analysis_library(self, library):
        self.analysis_instances = library.get("instances", [])
        self._update_analysis_choice()

    def _update_analysis_choice(self, *_args):
        selected = self.analysis_choice.currentData()
        with QSignalBlocker(self.analysis_choice):
            self.analysis_choice.clear()
            self.analysis_choice.addItem("직접 설정 / Manual configuration", None)
            for row in self.analysis_instances:
                if row.get("instrument") == self.instrument.text().strip():
                    self.analysis_choice.addItem(
                        f"{row['name']} · rev {row['revision']}",
                        {
                            "id": row["id"],
                            "revision": row["revision"],
                            "instrument": row["instrument"],
                        },
                    )
            if selected:
                index = self.analysis_choice.findData(selected)
                if index >= 0:
                    self.analysis_choice.setCurrentIndex(index)
                elif selected.get("instrument") == self.instrument.text().strip():
                    self.analysis_choice.addItem(
                        "Stale selection · reload CA / 수동 재선택 필요", selected
                    )
                    self.analysis_choice.setCurrentIndex(self.analysis_choice.count() - 1)
        self.analysis_editor.setEnabled(self.analysis_choice.currentData() is None)

    def _set_modules(self, response: dict) -> None:
        current = self.module.currentData()
        self._module_definitions = {}
        with QSignalBlocker(self.module):
            self.module.clear()
            for module in response.get("ts", []):
                if isinstance(module, dict):
                    identity = module.get("module_id", module.get("id", ""))
                    if identity:
                        self._module_definitions[identity] = module
                        self.module.addItem(str(module.get("label", identity)), identity)
            index = self.module.findData(current)
            if index >= 0:
                self.module.setCurrentIndex(index)
        self._module_selected()

    def _module_selected(self, *_args):
        identity = self.module.currentData()
        definition = self._module_definitions.get(identity, {})
        if identity != self._active_module:
            self._module_drafts[self._active_module] = self.configuration.toPlainText()
            self.configuration.setPlainText(
                self._module_drafts.get(
                    identity, plain_json(definition.get("default_configuration", {}))
                )
            )
            self._active_module = identity
        self.configuration_editor.set_schema(definition.get("configuration_schema", {}))

    def set_attached(self, attached: bool) -> None:
        self.feed_controls.set_attached(attached)
        for button in (
            self.create_button,
            self.add_button,
            self.step_button,
            self.recover_button,
            self.reset_model_button,
            self.local_resume_button,
            self.cancel_order_button,
            self.remove_strategy_button,
        ):
            button.setEnabled(attached)
        self.step_button.setEnabled(attached and not getattr(self, "_feed_active", False))

    def update_state(self, state: dict) -> None:
        self.feed_controls.update_state(state)
        self._feed_active = state.get("feed", {}).get("enabled", False)
        self.step_button.setEnabled(
            self.client.attached and not state.get("feed", {}).get("enabled", False)
        )
        accounts = state.get("accounts", [])
        update_table(
            self.accounts,
            [
                [
                    account.get("id", account.get("account_id", "?")),
                    account.get("market", account.get("venue", "paper")),
                    f"{account.get('cash', account.get('balance', '—'))} "
                    f"{account.get('quote_asset', '')}".strip(),
                    account.get("status", "paper"),
                ]
                for account in accounts
                if isinstance(account, dict)
            ],
        )
        rows = []
        for account in accounts:
            if not isinstance(account, dict):
                continue
            positions = account.get("positions", [])
            if isinstance(positions, dict):
                positions = [
                    dict(value, instrument=key)
                    if isinstance(value, dict)
                    else {"instrument": key, "quantity": value}
                    for key, value in positions.items()
                ]
            for position in positions:
                if isinstance(position, dict):
                    rows.append(
                        [
                            account.get("id", account.get("account_id", "?")),
                            position.get("instrument", "?"),
                            position.get("quantity", "?"),
                            position.get("entry_price", position.get("average_price", "—")),
                            position.get("status", position.get("control", "—")),
                        ]
                    )
        update_table(self.positions, rows)
        selected_strategy = self._selected_strategy_id()
        update_table(
            self.strategies,
            [
                [
                    strategy.get("id", strategy.get("strategy_id", strategy.get("instance", "?"))),
                    strategy.get("module", strategy.get("module_id", "?")),
                    strategy.get("instrument", "—"),
                    strategy.get("status", "—"),
                ]
                for strategy in state.get("strategies", [])
                if isinstance(strategy, dict)
            ],
        )
        matches = [
            i
            for i in range(self.strategies.rowCount())
            if self.strategies.item(i, 0).text() == selected_strategy
        ]
        if selected_strategy is not None and matches:
            self.strategies.selectRow(matches[0])
        else:
            self.strategies.clearSelection()
            self.strategies.setCurrentCell(-1, -1)


class ArtifactPage(QWidget):
    """Read-only data/run viewer also usable without a core or exchange credentials."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.language = "ko"
        self.path = None
        self.records = []
        layout = QVBoxLayout(self)
        controls = QHBoxLayout()
        self.open_button = QPushButton("Open data artifact")
        self.path_label = QLabel("JSON / JSONL / CSV · inert data only · max 16 MiB")
        self.path_label.setWordWrap(True)
        controls.addWidget(self.open_button)
        controls.addWidget(self.path_label, 1)
        layout.addLayout(controls)
        self.tabs = QTabWidget()
        self.raw = QPlainTextEdit()
        self.raw.setReadOnly(True)
        self.tabs.addTab(self.raw, "Recorded data / lineage")
        metric_page = QWidget()
        metric_layout = QVBoxLayout(metric_page)
        self.metric_select = QComboBox()
        self.metric_plot = MetricPlot()
        metric_layout.addWidget(self.metric_select)
        metric_layout.addWidget(self.metric_plot)
        self.tabs.addTab(metric_page, "Metrics")
        self.graph = NetworkStructureView()
        self.structure_records = []
        self.structure_total = 0
        graph_page = QWidget()
        graph_layout = QVBoxLayout(graph_page)
        self.structure_select = QComboBox()
        self.structure_note = QLabel()
        self.structure_note.setWordWrap(True)
        self.structure_note.setTextFormat(Qt.TextFormat.PlainText)
        self.node_detail = QLabel()
        self.node_detail.setWordWrap(True)
        self.node_detail.setTextFormat(Qt.TextFormat.PlainText)
        self.node_detail.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        graph_layout.addWidget(self.structure_select)
        graph_layout.addWidget(self.structure_note)
        graph_layout.addWidget(self.graph, 1)
        graph_layout.addWidget(self.node_detail)
        self.tabs.addTab(graph_page, "3D structure summary")
        self.structure_select.currentIndexChanged.connect(self._structure_changed)
        self.graph.nodeSelected.connect(self._node_selected)
        self._set_structures([])
        self.candles = ChartDrawer()
        self.tabs.addTab(self.candles, "OHLCV")
        layout.addWidget(self.tabs)
        self.open_button.clicked.connect(self.browse)
        self.metric_select.currentTextChanged.connect(self._metric_changed)

    def browse(self) -> None:
        name, _ = QFileDialog.getOpenFileName(
            self, "Open data artifact", "", "Recorded data (*.json *.jsonl *.csv)"
        )
        if name:
            self.open_path(Path(name))

    def open_path(self, path: Path) -> bool:
        try:
            data, text = read_artifact(path)
            self.path = path
            self.path_label.setText(str(path))
            self.raw.setPlainText(
                text[:256_000] + ("\n[Preview truncated]" if len(text) > 256_000 else "")
            )
            self.records = data if isinstance(data, list) else [data]
            if isinstance(data, dict) and isinstance(data.get("equity"), list):
                self.records = [
                    {"metric": metric, "value": row[metric], "split": "simulation"}
                    for row in data["equity"]
                    if isinstance(row, dict)
                    for metric in ("wallet", "equity")
                    if metric in row
                ]
            self.metric_select.clear()
            names = sorted(
                {
                    f"{row['metric']} / {row.get('split', 'train')}"
                    for row in self.records
                    if isinstance(row, dict) and "metric" in row
                }
            )
            self.metric_select.addItems(names)
            self._metric_changed(self.metric_select.currentText())
            structures = [
                row
                for row in self.records
                if isinstance(row, dict)
                and ("structure" in row or "layers" in row or "nodes" in row)
            ]
            self._set_structures(structures)
            rows = [
                row
                for row in self.records
                if isinstance(row, dict)
                and {"open", "high", "low", "close", "open_time_ms", "volume"} <= row.keys()
            ]
            # Multi-symbol exports are separated by instrument before display.
            if rows:
                instrument = rows[0].get("instrument")
                rows = [row for row in rows if row.get("instrument") == instrument]
            self.candles.set_candles(parse_candles(rows))
            if path.name == "run.json":
                self._open_run_companions(path.parent)
            return True
        except (OSError, ValueError, TypeError, KeyError, UnicodeError) as error:
            QMessageBox.warning(self, "Artifact viewer", str(error))
            return False

    def _open_run_companions(self, root: Path) -> None:
        # Fixed companion names only; a manifest cannot make the viewer import code.
        metrics = root / "metrics.jsonl"
        if metrics.is_file():
            data, _ = read_artifact(metrics)
            if isinstance(data, list):
                self.records = data
                self.metric_select.clear()
                self.metric_select.addItems(
                    sorted(
                        {
                            f"{row['metric']} / {row.get('split', 'train')}"
                            for row in data
                            if isinstance(row, dict) and "metric" in row
                        }
                    )
                )
        structures = root / "structures.jsonl"
        if structures.is_file():
            data, _ = read_artifact(structures)
            if isinstance(data, list):
                self._set_structures(data)

    def _set_structures(self, records):
        self.structure_total = len(records)
        self.structure_records = []
        start = max(0, len(records) - 1000)
        for index, row in enumerate(records[start:], start=start):
            if not isinstance(row, dict):
                continue
            structure = row.get("structure", row)
            if not isinstance(structure, dict) or not ({"nodes", "layers"} & structure.keys()):
                continue
            step = row.get("step")
            step = step if type(step) is int and step >= 0 else None
            self.structure_records.append((index + 1, step, structure))
        self.structure_select.blockSignals(True)
        self.structure_select.clear()
        for index, (record, step, _structure) in enumerate(self.structure_records):
            label = f"step {step if step is not None else '?'} · record {record}"
            self.structure_select.addItem(label, index)
        self.structure_select.setCurrentIndex(len(self.structure_records) - 1)
        self.structure_select.blockSignals(False)
        self._structure_changed(self.structure_select.currentIndex())

    def _structure_changed(self, index):
        if 0 <= index < len(self.structure_records):
            record, step, structure = self.structure_records[index]
            self.graph.set_structure(structure)
            self.structure_note.setText(
                f"Recorded step={step if step is not None else 'unknown'} · "
                f"record {record}/{self.structure_total} · "
                f"visible history={len(self.structure_records)} (max 1000) · "
                "layer summary, not tensors"
            )
        else:
            self.graph.set_structure({})
            self.structure_note.setText(
                "기록된 구조 없음 / No recorded structure · no model code is loaded"
            )

    def _node_selected(self, details):
        if not details:
            self.node_detail.setText(
                "표시 점을 클릭하면 기록된 node/layer 정보를 보여줍니다 / "
                "Click a point for recorded node details"
            )
            return
        self.node_detail.setText(
            f"id={details.get('id') or 'not recorded'} · type={details.get('type')} · "
            f"shape={details.get('shape')} · parameters={details.get('parameters')} · "
            f"reported units={details.get('reported_units')} · "
            f"display points={details.get('display_points')}\n"
            "Recorded metadata only · display points summarize layers, not learned weights"
        )

    def _metric_changed(self, label: str) -> None:
        values = [
            row.get("value")
            for row in self.records
            if isinstance(row, dict)
            and "metric" in row
            and f"{row['metric']} / {row.get('split', 'train')}" == label
        ]
        self.metric_plot.set_values(label, values)

    def set_presentation(self, theme: str, language: str) -> None:
        self.language = language
        self.open_button.setText(translate("open", language))
        self.graph.theme, self.graph.language = theme, language
        self.graph.update()
        self.metric_plot.theme = theme
        self.metric_plot.update()
        self.candles.set_presentation(theme, language)


class ResearchPage(QWidget):
    """Explicit job specs keep UI lifetime independent of dataset/simulation/training jobs."""

    openArtifact = Signal(str)

    def __init__(self, client, kinds: list[str], parent=None):
        from PySide6.QtWidgets import QGridLayout, QLayout

        super().__init__(parent)
        self.client = client
        self.language = "ko"
        self._submitted: set[str] = set()
        self._submission_epochs: dict[str, str | None] = {}
        self._jobs: dict[str, dict] = {}
        self._selected_job_detail: dict = {}
        self._inspect_tags: set[str] = set()
        self._inspection_job_id: str | None = None
        self._inspection_accepted_id: str | None = None
        self._inspection_selection: dict = {}
        self._selected_detail_epoch = None
        self._job_query_epochs: dict[str, str | None] = {}
        self._last_core_epoch = self._core_epoch()
        self._analysis_request, self._analysis_serial = None, 0
        self._settings_revision: int | None = None
        self._core_settings: dict = {}
        self._research_health: dict = {}
        self._research_available = True
        layout = QVBoxLayout(self)
        self.health_notice = QLabel()
        self.health_notice.setWordWrap(True)
        self.health_notice.setTextFormat(Qt.TextFormat.PlainText)
        self.health_notice.hide()
        layout.addWidget(self.health_notice)
        form_widget = QWidget()
        form = QFormLayout(form_widget)
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.kind = QComboBox()
        self.kind.addItems(kinds)
        if "export" in kinds and "export_auxiliary" not in kinds:
            self.kind.addItem("export_auxiliary")
        self.dataset = QLineEdit()
        self.dataset.setPlaceholderText("Dataset manifest or source CSV / JSONL path")
        self.module = QLineEdit()
        self.module.setPlaceholderText("Trusted external neuralnetwork.py; never loaded by the GUI")
        self.module_catalog = QComboBox()
        self.module_catalog.addItem("Select available module…", "")
        self.seed = SeedInput()
        form.addRow("Job kind", self.kind)
        if "simulate" in kinds:
            from omega.gui.margin_replay_controls import MarginReplayControls

            self.simulation_mode = QComboBox()
            self.simulation_mode.addItem("기존 선형 재생 / Linear replay", "linear")
            self.simulation_mode.addItem("공유 증거금 v2 / Shared margin v2", "margin")
            self.margin_replay = MarginReplayControls()
            self.margin_replay.analysis_selector.refreshRequested.connect(
                self.refresh_analysis_library
            )
            self.margin_replay.funding.changed.connect(self._simulation_mode_changed)
            self.margin_replay.cost_policy.changed.connect(self._simulation_mode_changed)
            self.margin_replay.auxiliary_mode.currentIndexChanged.connect(
                self._simulation_mode_changed
            )
            form.addRow("Simulation engine", self.simulation_mode)
            self.margin_replay.hide()
        source_row = QHBoxLayout()
        source_row.addWidget(self.dataset)
        browse = QPushButton("…")
        source_row.addWidget(browse)
        form.addRow("Dataset / source", source_row)
        if {"export", "export_auxiliary", "monte_carlo", "train", "simulate"}.intersection(kinds):
            from omega.gui.research_inputs import ResearchInputs

            self.dataset_controls = ResearchInputs(client, self.dataset)
            form.addRow(self.dataset_controls)
        if hasattr(self, "margin_replay"):
            self.dataset_controls.inspectionChanged.connect(self._simulation_mode_changed)
            form.addRow(self.margin_replay)
        if "train" in kinds:
            module_row = QHBoxLayout()
            module_row.addWidget(self.module)
            browse_module = QPushButton("…")
            module_row.addWidget(browse_module)
            form.addRow("Instance module", module_row)
            form.addRow("Module catalog", self.module_catalog)
            browse_module.clicked.connect(self._browse_module)
            self.mc_training = QCheckBox("Monte Carlo diversify training split before training")
            self.mc_training.setToolTip(
                "Original dataset is preserved. Validation/test stay separate."
            )
            self.mc_blocks = QSpinBox()
            self.mc_blocks.setRange(1, 10000)
            self.mc_blocks.setValue(60)
            form.addRow(self.mc_training)
            form.addRow("MC block bars", self.mc_blocks)
            self.demo_instance = QCheckBox("Use built-in host demo instance (not a trading model)")
            self.profile = QComboBox()
            self.profile.addItems(["standard", "lightweight", "detailed"])
            self.inspect_button = QPushButton("Inspect selected module")
            form.addRow(self.demo_instance)
            form.addRow("Recording profile", self.profile)
            form.addRow(self.inspect_button)
            self.inspect_button.clicked.connect(self.inspect_module)
        if "train" in kinds or "simulate" in kinds:
            self.device = QComboBox()
            self.device.addItems(["auto", "cpu", "gpu"])
            self.device.setToolTip("Actual backend/fallback is recorded by the worker or module.")
            form.addRow("Compute device", self.device)
        form.addRow("Seed", self.seed)
        form_scroll = QScrollArea()
        self.form_scroll = form_scroll
        form_scroll.setWidgetResizable(True)
        form_scroll.setWidget(form_widget)
        form_scroll.setMinimumHeight(80)
        configuration = QWidget()
        configuration_layout = QVBoxLayout(configuration)
        configuration_layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        configuration_layout.setContentsMargins(0, 0, 0, 0)
        setup_title = QLabel(translate("job_setup", self.language))
        setup_title.setProperty("role", "section")
        setup_title.setProperty("omega_message_key", "job_setup")
        configuration_layout.addWidget(setup_title)
        configuration_layout.addWidget(form_scroll, 1)
        self.parameters = QPlainTextEdit()
        self.parameters.setMaximumHeight(200)
        if "train" in kinds:
            from omega.gui.neural_controls import NeuralControls

            self.neural_controls = NeuralControls(self.parameters)
            form.addRow("NN configuration", self.neural_controls)
            self.neural_controls.source.currentIndexChanged.connect(self._nn_source_changed)
            self.demo_instance.toggled.connect(self._nn_demo_changed)
            self.module.textEdited.connect(self._nn_external_selected)
        self.advanced = QGroupBox(translate("advanced_spec", self.language))
        self.advanced.setProperty("omega_message_key", "advanced_spec")
        self.advanced.setCheckable(True)
        self.advanced.setChecked(False)
        advanced_layout = QVBoxLayout(self.advanced)
        advanced_layout.addWidget(self.parameters)
        self.parameters.hide()
        self.advanced.toggled.connect(self.parameters.setVisible)
        configuration_layout.addWidget(self.advanced)
        if "train" in kinds:
            from omega.gui.shadow_controls import ShadowControls

            self.shadow_controls = ShadowControls(client)
            self.shadow_controls.update_state(getattr(client, "state", {}))
            self.shadow_controls.set_dataset_source(self.dataset.text())
            self.dataset.textChanged.connect(self.shadow_controls.set_dataset_source)
            configuration_layout.addWidget(self.shadow_controls)
            from omega.gui.causal_controls import CausalControls

            self.causal_controls = CausalControls(client)
            configuration_layout.addWidget(self.causal_controls)
        controls = QGridLayout()
        self.run_button = QPushButton("Submit job")
        self.run_button.setEnabled(False)
        self.cancel_button = QPushButton("Cancel selected job")
        self.cancel_button.setEnabled(False)
        self.open_button = QPushButton("Open selected job artifact")
        self.open_button.setEnabled(False)
        controls.addWidget(self.run_button, 0, 0)
        controls.addWidget(self.cancel_button, 0, 1)
        controls.addWidget(self.open_button, 1, 0)
        if hasattr(self, "dataset_controls"):
            self.use_dataset_button = QPushButton("선택 결과를 입력으로 / Use selected dataset")
            self.use_dataset_button.setEnabled(False)
            self.use_dataset_button.clicked.connect(self._use_selected_dataset)
            controls.addWidget(self.use_dataset_button, 2, 0, 1, 2)
        if "train" in kinds:
            self.resume_selected_button = QPushButton("선택 학습 이어하기 / Resume")
            self.resume_selected_button.setEnabled(False)
            self.resume_selected_button.clicked.connect(self._resume_selected_training)
            controls.addWidget(self.resume_selected_button, 1, 1)
        controls.setColumnStretch(0, 1)
        controls.setColumnStretch(1, 1)
        configuration_layout.addLayout(controls)
        self.jobs = data_table(["Job ID", "Kind", "State", "Progress / error"])
        self.result = QPlainTextEdit()
        self.result.setReadOnly(True)
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.addWidget(self.jobs)
        splitter.addWidget(self.result)
        activity = QWidget()
        activity_layout = QVBoxLayout(activity)
        activity_layout.setContentsMargins(0, 0, 0, 0)
        activity_title = QLabel(translate("job_activity", self.language))
        activity_title.setProperty("role", "section")
        activity_title.setProperty("omega_message_key", "job_activity")
        activity_layout.addWidget(activity_title)
        activity_layout.addWidget(splitter, 1)
        workspace = self.workspace = QSplitter(Qt.Orientation.Horizontal)
        self.configuration_scroll = QScrollArea()
        self.configuration_scroll.setWidgetResizable(True)
        self.configuration_scroll.setMinimumHeight(100)
        self.configuration_scroll.setWidget(configuration)
        workspace.addWidget(self.configuration_scroll)
        workspace.addWidget(activity)
        workspace.setChildrenCollapsible(False)
        workspace.setSizes([430, 820])
        workspace.setStretchFactor(1, 1)
        layout.addWidget(workspace)
        browse.clicked.connect(self._browse_dataset)
        self.kind.currentTextChanged.connect(self._defaults)
        if hasattr(self, "simulation_mode"):
            self.simulation_mode.currentIndexChanged.connect(self._simulation_mode_changed)
        self.run_button.clicked.connect(self.submit)
        self.cancel_button.clicked.connect(self.cancel)
        self.open_button.clicked.connect(self.open_selected)
        self.jobs.itemSelectionChanged.connect(self.inspect_job)
        self.module_catalog.currentIndexChanged.connect(self._select_module)
        client.modulesReceived.connect(self.set_modules)
        client.resultReceived.connect(self._handle_result)
        client.failed.connect(self._handle_failure)
        self._defaults(self.kind.currentText())

    def resizeEvent(self, event):
        self.workspace.setOrientation(
            Qt.Orientation.Vertical if self.width() < 950 else Qt.Orientation.Horizontal
        )
        super().resizeEvent(event)

    def refresh_analysis_library(self):
        if not hasattr(self, "margin_replay") or not self.client.attached or not self._core_epoch():
            return
        if self._analysis_request is not None:
            return
        self._analysis_serial += 1
        tag = f"research-ca:{id(self)}:{self._analysis_serial}"
        self._analysis_request = (tag, self._core_epoch())
        try:
            self.client.request("GET", "/api/analysis-library", tag=tag)
        except Exception as error:
            self._analysis_request = None
            self.margin_replay.analysis_selector.invalidate(type(error).__name__)

    def _core_epoch(self):
        state = getattr(self.client, "state", {})
        core = state.get("core", {}) if isinstance(state, dict) else {}
        return core.get("epoch") if isinstance(core, dict) else None

    def _selected_actions(self):
        item = self.jobs.item(self.jobs.currentRow(), 0)
        job = self._jobs.get(item.text(), {}) if item else {}
        complete = job.get("state") == "complete"
        if hasattr(self, "shadow_controls"):
            self.shadow_controls.set_training_selection(
                job.get("id") if complete and job.get("kind") == "train" else None,
                core_epoch=self._core_epoch(),
            )
        if hasattr(self, "causal_controls"):
            self.causal_controls.set_training_selection(
                job.get("id") if complete and job.get("kind") == "train" else None,
                core_epoch=self._core_epoch(),
            )
        self.open_button.setEnabled(complete and bool(job.get("result_file")))
        if hasattr(self, "use_dataset_button"):
            self.use_dataset_button.setEnabled(
                complete
                and job.get("kind") in {"export", "export_auxiliary", "monte_carlo"}
                and bool(job.get("result_file"))
                and self.client.attached
            )
        if hasattr(self, "resume_selected_button"):
            detail = self._selected_job_detail
            self.resume_selected_button.setEnabled(
                complete
                and job.get("kind") == "train"
                and item is not None
                and detail.get("id") == item.text()
                and detail.get("state") == "complete"
                and self._selected_detail_epoch == self._core_epoch()
            )

    def _defaults(self, kind: str) -> None:
        if hasattr(self, "dataset_controls"):
            self.dataset_controls.set_kind(kind)
        self.advanced.setVisible(kind != "export_auxiliary")
        self.seed.setEnabled(kind != "export_auxiliary")
        defaults = {
            "export": {"split_ratios": [0.7, 0.15, 0.15], "purge_bars": 0},
            "monte_carlo": {"block_bars": 60, "splits": ["train"], "scenario_index": 0},
            "simulate": {
                "strategy": "buy_and_hold_demo",
                "quantity": "1",
                "spec": {
                    "profile": "validation",
                    "initial_wallet": "10000",
                    "path": "O-L-H-C",
                    "seed": 0,
                    "fee_rate": "0.0004",
                    "slippage_rate": "0",
                    "latency_ms": 0,
                    "backend": "auto",
                },
            },
            "train": {
                "config": {"epochs": 10},
                "recording": {"profile": "standard", "optional_detail_failure": "warn_continue"},
                "online_learning": False,
            },
        }
        self.parameters.setPlainText(plain_json(defaults.get(kind, {})))
        if self._core_settings:
            self._apply_settings_parameters()
        self.set_attached(self.client.attached)

    def apply_settings(self, settings: dict) -> None:
        """Apply global defaults once per revision, preserving edits during normal polling."""
        revision = settings.get("revision")
        if type(revision) is not int or revision == self._settings_revision:
            return
        self._settings_revision = revision
        self._core_settings = settings
        kind = self.kind.currentText()
        if kind == "simulate":
            defaults = settings.get("simulation", {})
            if defaults.get("backend") in {"auto", "cpu", "gpu"}:
                self.device.setCurrentText(defaults["backend"])
            seed = defaults.get("seed")
            if type(seed) is int and 0 <= seed < 2**63:
                self.seed.setValue(seed)
        elif kind == "train":
            profile = settings.get("training", {}).get("recording")
            if profile in {"light", "standard", "detailed"}:
                self.profile.setCurrentText("lightweight" if profile == "light" else profile)
        self._apply_settings_parameters()

    def _apply_settings_parameters(self) -> None:
        kind = self.kind.currentText()
        if kind not in {"simulate", "train"}:
            return
        try:
            parameters = json.loads(self.parameters.toPlainText())
            if not isinstance(parameters, dict):
                raise ValueError("parameters must be a JSON object")
            if kind == "simulate":
                target = parameters.setdefault("spec", {})
                defaults = self._core_settings.get("simulation", {})
                if not isinstance(target, dict):
                    raise ValueError("simulation spec must be a JSON object")
                if defaults.get("backend") in {"auto", "cpu", "gpu"}:
                    target["backend"] = defaults["backend"]
                seed = defaults.get("seed")
                if type(seed) is int and 0 <= seed < 2**63:
                    target["seed"] = seed
            else:
                target = parameters.setdefault("recording", {})
                defaults = self._core_settings.get("training", {})
                if not isinstance(target, dict):
                    raise ValueError("recording must be a JSON object")
                profile = defaults.get("recording")
                if profile in {"light", "standard", "detailed"}:
                    target["profile"] = "lightweight" if profile == "light" else profile
                policy = defaults.get("optional_recording_failure")
                if policy in {"continue", "stop"}:
                    target["optional_detail_failure"] = (
                        "warn_continue" if policy == "continue" else policy
                    )
            self.parameters.setPlainText(plain_json(parameters))
        except ValueError:
            # Preserve an unfinished draft; never erase it merely because a snapshot arrived.
            self.result.setPlainText(
                "Global defaults changed; the unfinished JSON draft was preserved. "
                "Check recording/device settings after completing the draft."
            )

    def _browse_dataset(self) -> None:
        name, _ = QFileDialog.getOpenFileName(
            self, "Dataset / source", "", "Dataset (*.json *.csv *.jsonl)"
        )
        if name:
            self.dataset.setText(name)

    def _browse_module(self) -> None:
        name, _ = QFileDialog.getOpenFileName(
            self, "Trusted instance module", "", "Python instance module (*.py)"
        )
        if name:
            self.module.setText(name)
            self._nn_external_selected()

    def _nn_source_changed(self, *_args) -> None:
        self.demo_instance.setChecked(self.neural_controls.source.currentData() == "demo")

    def _nn_demo_changed(self, checked: bool) -> None:
        if checked:
            self.neural_controls.source.setCurrentIndex(
                self.neural_controls.source.findData("demo")
            )
        elif self.neural_controls.source.currentData() == "demo":
            self._nn_external_selected()

    def _nn_external_selected(self, *_args) -> None:
        if hasattr(self, "neural_controls"):
            source = self.neural_controls.source
            if source.currentData() == "external":
                self.neural_controls.reset_descriptor()
            else:
                source.setCurrentIndex(source.findData("external"))

    def _select_module(self, index: int) -> None:
        path = self.module_catalog.itemData(index)
        if path:
            changed = self.module.text() != str(path)
            self.module.setText(str(path))
            if changed:
                self._nn_external_selected()

    def set_modules(self, modules: dict) -> None:
        if hasattr(self, "margin_replay"):
            self.margin_replay.set_modules(modules)
        selected = self.module_catalog.currentData()
        self.module_catalog.clear()
        self.module_catalog.addItem("Select available module…", "")
        for module in modules.get("nn", []):
            if isinstance(module, dict):
                path = module.get("path", module.get("module_path", ""))
                self.module_catalog.addItem(str(module.get("module_id", path)), path)
        index = self.module_catalog.findData(selected)
        if index >= 0:
            self.module_catalog.setCurrentIndex(index)

    def submit(self) -> None:
        if not self._research_available:
            return
        try:
            if hasattr(self, "simulation_mode") and self.simulation_mode.currentData() == "margin":
                if not self._margin_supported():
                    raise ValueError("The connected core does not provide shared margin replay")
                if not self.dataset.text().strip():
                    raise ValueError("Select a dataset first")
                parameters = self.margin_replay.parameters()
                if "auxiliary_replay" in parameters:
                    parameters["auxiliary_replay"] = self.dataset_controls.native_replay_parameters(
                        parameters["auxiliary_replay"]["decision_latency_ms"]
                    )
                else:
                    self.dataset_controls.validate_usage("simulate_margin")
                parameters["dataset"] = self.dataset.text().strip()
                tag = self.client.submit_job("simulate_margin", parameters)
                self._submitted.add(tag)
                self._submission_epochs[tag] = self._core_epoch()
                self.set_attached(self.client.attached)
                self.result.setPlainText("Submitting shared margin replay…")
                return
            parameters = json.loads(self.parameters.toPlainText())
            if not isinstance(parameters, dict):
                raise ValueError("parameters must be a JSON object")
            json.dumps(parameters, allow_nan=False)
            if not self.dataset.text().strip():
                raise ValueError("Select a dataset or source file first")
            kind = self.kind.currentText()
            if hasattr(self, "dataset_controls"):
                self.dataset_controls.validate_usage(kind)
            if kind == "export_auxiliary":
                if not self._auxiliary_export_supported():
                    raise ValueError("연결된 코어에서 보조 관측 export를 지원하지 않습니다.")
                parameters = self.dataset_controls.export_parameters()
            parameters["source_file" if kind == "export" else "dataset"] = (
                self.dataset.text().strip()
            )
            if kind == "export_auxiliary":
                parameters.pop("dataset", None)
            if kind != "export_auxiliary":
                parameters["seed"] = self.seed.value()
            if kind == "train":
                for key in ("demo", "module", "builtin"):
                    parameters.pop(key, None)
                parameters.update(
                    self.neural_controls.selection(
                        self.module.text(), demo_checked=self.demo_instance.isChecked()
                    )
                )
                parameters["device"] = self.device.currentText()
                parameters.setdefault("recording", {})["profile"] = self.profile.currentText()
            if kind == "simulate" and isinstance(parameters.get("spec"), dict):
                parameters["spec"]["seed"] = self.seed.value()
                parameters["spec"]["backend"] = self.device.currentText()
            if kind == "train" and self.mc_training.isChecked():
                parameters["monte_carlo"] = {
                    "enabled": True,
                    "seed": self.seed.value(),
                    "block_bars": self.mc_blocks.value(),
                    "splits": ["train"],
                }
            if kind == "train":
                parameters = self.neural_controls.apply(parameters)
            tag = self.client.submit_job(kind, parameters)
            self._submitted.add(tag)
            self._submission_epochs[tag] = self._core_epoch()
            self.set_attached(self.client.attached)
            self.result.setPlainText("Submitting…")
        except (ValueError, TypeError) as error:
            self.result.setPlainText(str(error))

    def update_jobs(self, jobs: list[dict]) -> None:
        if hasattr(self, "dataset_controls"):
            self.dataset_controls.update_jobs(jobs)
        if hasattr(self, "simulation_mode"):
            self._simulation_mode_changed()
        if hasattr(self, "shadow_controls"):
            self.shadow_controls.update_state(getattr(self.client, "state", {}))
        epoch = self._core_epoch()
        previous_jobs = self._jobs if epoch == self._last_core_epoch else {}
        if epoch != self._last_core_epoch:
            self._last_core_epoch = epoch
            self._selected_job_detail = {}
            self._selected_detail_epoch = None
            self._inspection_accepted_id = None
            self._inspection_job_id = None
        self._jobs = {
            str(job.get("id", job.get("job_id", "?"))): job for job in jobs if isinstance(job, dict)
        }
        for identity, previous in previous_jobs.items():
            if identity in self._jobs and previous.get("state") in {"unknown", "quarantined"}:
                # Quarantine is released by owner recovery, not a delayed poll.
                self._jobs[identity] = self._jobs[identity] | previous
        detail = self._selected_job_detail
        identity = detail.get("id")
        if self._jobs.get(identity, {}).get("state") in {"unknown", "quarantined"}:
            self._selected_job_detail = detail = {}
            self._selected_detail_epoch = None
            identity = None
        if (
            identity in self._jobs
            and detail.get("state") == "complete"
            and self._selected_detail_epoch == epoch
            and self._jobs[identity].get("state") in {"queued", "running", "complete"}
        ):
            # UUIDs are immutable within an owner epoch. A delayed snapshot cannot
            # undo an already-observed completed detail; unknown/quarantine can.
            self._jobs[identity] = self._jobs[identity] | detail
        update_table(
            self.jobs,
            [
                [
                    job.get("id", job.get("job_id", "?")),
                    job.get("kind", "?"),
                    job.get("state", job.get("status", "?")),
                    job.get("error", job.get("progress", "—")),
                ]
                for job in self._jobs.values()
            ],
        )
        self._selected_actions()
        item = self.jobs.item(self.jobs.currentRow(), 0)
        if (
            item
            and self._jobs.get(item.text(), {}).get("state") == "complete"
            and (
                self._selected_job_detail.get("id") != item.text()
                or self._selected_job_detail.get("state") != "complete"
                or self._selected_detail_epoch != epoch
            )
        ):
            self.inspect_job()
        inspected = self._jobs.get(self._inspection_job_id or "", {})
        if inspected.get("state") == "complete" and self.client.attached:
            identity = self._inspection_job_id
            self._inspection_job_id = None
            self.client.request(
                "GET", "/api/jobs/" + quote(identity, safe=""), tag="nn_descriptor:" + identity
            )

    def _handle_result(self, tag: str, result: dict) -> None:
        if self._analysis_request is not None and tag == self._analysis_request[0]:
            _, epoch = self._analysis_request
            self._analysis_request = None
            if epoch != self._core_epoch() or not self.client.attached:
                return
            try:
                self.margin_replay.analysis_selector.set_analysis_library(result, core_epoch=epoch)
            except (ValueError, TypeError, KeyError) as error:
                self.margin_replay.analysis_selector.invalidate(str(error))
            return
        if tag in self._submitted:
            epoch = self._submission_epochs.pop(tag, self._core_epoch())
            if epoch != self._core_epoch():
                self._inspect_tags.discard(tag)
                self._submitted.remove(tag)
                self.set_attached(self.client.attached)
                self.result.setPlainText(
                    "코어가 바뀌어 이전 제출 응답을 적용하지 않았습니다. 작업 목록을 확인하세요."
                )
                return
            if tag in self._inspect_tags:
                self._inspect_tags.remove(tag)
                self._inspection_job_id = result.get("id")
                self._inspection_accepted_id = self._inspection_job_id
            self._submitted.remove(tag)
            self.set_attached(self.client.attached)
            self.result.setPlainText(plain_json(result))
        elif tag.startswith("job:"):
            item = self.jobs.item(self.jobs.currentRow(), 0)
            query_epoch = self._job_query_epochs.pop(tag, self._core_epoch())
            if (
                item
                and tag == "job:" + item.text()
                and result.get("id") == item.text()
                and query_epoch == self._core_epoch()
            ):
                current_state = self._jobs.get(item.text(), {}).get("state")
                if (
                    current_state in {"unknown", "quarantined"}
                    and result.get("state") != current_state
                ):
                    return
                if self._jobs.get(item.text(), {}).get("state") == "complete" and result.get(
                    "state"
                ) in {"queued", "running"}:
                    return
                self._selected_job_detail = result
                self._selected_detail_epoch = query_epoch
                self._jobs[item.text()] = self._jobs.get(item.text(), {}) | result
                self._selected_actions()
                self._apply_nn_descriptor(result)
                self.result.setPlainText(plain_json(result)[:256_000])
        elif tag.startswith("nn_descriptor:"):
            self._apply_nn_descriptor(result)
            self.result.setPlainText(plain_json(result)[:256_000])

    def _apply_nn_descriptor(self, result: dict) -> None:
        if not hasattr(self, "neural_controls") or result.get("kind") != "nn_inspect":
            return
        identity = result.get("id")
        if not isinstance(identity, str) or self._jobs.get(identity, {}).get("state") in {
            "unknown",
            "quarantined",
        }:
            return
        try:
            selected = self.neural_controls.selection(
                self.module.text(), demo_checked=self.demo_instance.isChecked()
            )
        except ValueError:
            return
        if (
            result.get("id") == self._inspection_accepted_id
            and result.get("state") == "complete"
            and selected == self._inspection_selection
            and isinstance(result.get("result"), dict)
        ):
            self.neural_controls.apply_descriptor(result["result"])

    def _handle_failure(self, tag: str, message: str) -> None:
        if self._analysis_request is not None and tag == self._analysis_request[0]:
            _, epoch = self._analysis_request
            self._analysis_request = None
            if epoch == self._core_epoch():
                self.margin_replay.analysis_selector.invalidate(str(message)[:220])
            return
        self._job_query_epochs.pop(tag, None)
        if tag in self._submitted:
            self._submission_epochs.pop(tag, None)
            self._inspect_tags.discard(tag)
            self._submitted.remove(tag)
            self.set_attached(self.client.attached)
            self.result.setPlainText(
                f"{message}\nSubmission outcome is unknown. Check the job list before retrying."
            )

    def inspect_job(self) -> None:
        item = self.jobs.item(self.jobs.currentRow(), 0)
        self._selected_actions()
        if item and self.client.attached:
            tag = "job:" + item.text()
            # CoreClient deduplicates pending tags. Do not retag an old request as
            # new-epoch merely because the same row was selected again.
            if tag not in getattr(self.client, "_pending", {}):
                self._job_query_epochs[tag] = self._core_epoch()
                self.client.request("GET", "/api/jobs/" + quote(item.text(), safe=""), tag=tag)

    def _resume_selected_training(self) -> None:
        item = self.jobs.item(self.jobs.currentRow(), 0)
        job = self._selected_job_detail
        if (
            not item
            or job.get("id") != item.text()
            or job.get("kind") != "train"
            or job.get("state") != "complete"
        ):
            self.result.setPlainText("완료된 학습 작업을 선택하고 상세 조회를 기다리세요.")
            return
        parameters = job.get("parameters", {})
        summary = job.get("result", {})
        if not isinstance(parameters, dict) or not isinstance(summary, dict):
            self.result.setPlainText("이전 학습 설정을 읽을 수 없습니다.")
            return
        seed = parameters.get("seed", 0)
        if (
            type(seed) is not int
            or not 0 <= seed < 2**63
            or not isinstance(parameters.get("config", {}), dict)
        ):
            self.result.setPlainText("이전 seed/config를 이 화면에서 재사용할 수 없습니다.")
            return
        checkpoint = summary.get("checkpoint")
        path = job.get("result_file")
        dataset = summary.get("training_dataset_file", parameters.get("dataset"))
        if not all(isinstance(value, str) and value for value in (checkpoint, path, dataset)):
            self.result.setPlainText(
                "이 작업은 자동 재개 정보가 없습니다. training 폴더와 checkpoint를 직접 선택하세요."
            )
            return
        # [1]: Prepare a new editable draft, never resubmit or mutate the old job.
        draft = json.loads(json.dumps(parameters))
        draft.pop("monte_carlo", None)
        draft["resume"] = {"run": str(Path(path).parent), "checkpoint": checkpoint}
        completed = summary.get("completed_epochs")
        if type(completed) is int and completed < 10000:
            draft.setdefault("config", {})["epochs"] = completed + 1
        self.kind.setCurrentText("train")
        self.dataset.setText(dataset)
        self.mc_training.setChecked(False)
        self.seed.setValue(seed)
        self.module.setText(str(parameters.get("module", "")))
        self.demo_instance.setChecked(bool(parameters.get("demo", False)))
        source = (
            "demo"
            if parameters.get("demo", False)
            else "pytorch_return"
            if parameters.get("builtin") == "pytorch_return"
            else "external"
        )
        self.neural_controls.source.setCurrentIndex(self.neural_controls.source.findData(source))
        if parameters.get("device") in {"auto", "cpu", "gpu"}:
            self.device.setCurrentText(parameters["device"])
        self.parameters.setPlainText(plain_json(draft))
        self.result.setPlainText(
            "재개 설정을 새 초안으로 불러왔습니다. 총 epochs와 설정을 확인한 뒤 실행하세요."
        )

    def inspect_module(self) -> None:
        if not self._research_available or self._submitted:
            return
        try:
            parameters = self.neural_controls.selection(
                self.module.text(), demo_checked=self.demo_instance.isChecked()
            )
        except ValueError as error:
            self.result.setPlainText(str(error))
            return
        self._inspection_selection = parameters
        self._inspection_accepted_id = None
        self._inspection_job_id = None
        tag = self.client.submit_job("nn_inspect", parameters)
        self._submitted.add(tag)
        self._submission_epochs[tag] = self._core_epoch()
        self._inspect_tags.add(tag)
        self.set_attached(self.client.attached)

    def open_selected(self) -> None:
        item = self.jobs.item(self.jobs.currentRow(), 0)
        job = self._jobs.get(item.text(), {}) if item else {}
        if job.get("state") == "complete" and job.get("result_file"):
            self.openArtifact.emit(str(job["result_file"]))

    def _use_selected_dataset(self) -> None:
        item = self.jobs.item(self.jobs.currentRow(), 0)
        job = self._jobs.get(item.text(), {}) if item else {}
        if (
            job.get("state") == "complete"
            and job.get("kind") in {"export", "export_auxiliary", "monte_carlo"}
            and job.get("result_file")
        ):
            self.dataset.setText(str(job["result_file"]))
            if self.kind.currentText() in {"export", "export_auxiliary"}:
                self.kind.setCurrentText("monte_carlo")
            self.dataset_controls.inspect()

    def cancel(self) -> None:
        item = self.jobs.item(self.jobs.currentRow(), 0)
        if item:
            self.client.command("job.cancel", {"job_id": item.text()})

    def update_health(self, health: dict) -> None:
        self._research_health = health if isinstance(health, dict) else {}
        blocked = self._research_health.get("state") in {"degraded", "closed", "unavailable"}
        quarantined = self._research_health.get("quarantined", False) is True
        self._research_available = not (blocked or quarantined)
        if hasattr(self, "shadow_controls"):
            self.shadow_controls.set_research_available(self._research_available)
        self.health_notice.setHidden(self._research_available)
        ko = self.language == "ko"
        self.health_notice.setText(
            "연구 작업 제한 · 이전 작업의 종료 확인 후 수동 복구가 필요합니다."
            if ko and quarantined
            else "연구 작업을 사용할 수 없습니다. 연결 상태와 작업 기록을 확인하세요."
            if ko
            else "Research restricted: verify the previous worker has stopped before recovery."
            if quarantined
            else "Research is unavailable. Check connection and job records."
        )
        self.set_attached(self.client.attached)

    def set_attached(self, attached: bool) -> None:
        if hasattr(self, "causal_controls"):
            self.causal_controls.set_attached(attached)
        if hasattr(self, "dataset_controls"):
            self.dataset_controls.set_context(attached, self._research_available)
        if hasattr(self, "shadow_controls"):
            self.shadow_controls.set_attached(attached)
        self.run_button.setEnabled(attached and not self._submitted and self._research_available)
        if self.kind.currentText() == "export_auxiliary":
            supported = self._auxiliary_export_supported()
            self.run_button.setEnabled(self.run_button.isEnabled() and supported)
            self.run_button.setToolTip(
                "보조 관측 export는 연결된 코어의 auxiliary_export 지원이 필요합니다."
                if not supported
                else ""
            )
        if hasattr(self, "simulation_mode"):
            self._simulation_mode_changed()
            if not attached:
                self.margin_replay.funding.set_supported(False)
                self.margin_replay.cost_policy.set_supported(False)
                self.margin_replay.set_auxiliary_supported(False)
                self.run_button.setEnabled(False)
        self.cancel_button.setEnabled(attached)
        if hasattr(self, "inspect_button"):
            self.inspect_button.setEnabled(
                attached and not self._submitted and self._research_available
            )

    def _margin_supported(self):
        state = getattr(self.client, "state", {})
        return bool(state.get("core", {}).get("capabilities", {}).get("margin_simulation"))

    def _auxiliary_export_supported(self):
        state = getattr(self.client, "state", {})
        return state.get("core", {}).get("capabilities", {}).get("auxiliary_export") is True

    def _simulation_mode_changed(self, *_args):
        self.margin_replay.set_analysis_context(
            core_epoch=self._core_epoch(), attached=self.client.attached is True
        )
        if self._analysis_request is not None and (
            self._analysis_request[1] != self._core_epoch() or not self.client.attached
        ):
            self._analysis_request = None
        shared = self.simulation_mode.currentData() == "margin"
        supported = self._margin_supported()
        capabilities = getattr(self.client, "state", {}).get("core", {}).get("capabilities", {})
        self.margin_replay.set_analysis_execution_supported(
            self.client.attached is True
            and capabilities.get("margin_analysis_execution") is True
            and capabilities.get("margin_analysis_execution_scope") == "required_fields_v1"
        )
        self.margin_replay.funding.set_supported(
            self.client.attached is True
            and capabilities.get("margin_funding") is True
            and capabilities.get("margin_funding_scope") == "minute_boundary_scenario_rate_v1"
        )
        self.margin_replay.cost_policy.set_supported(
            self.client.attached is True
            and capabilities.get("margin_cost_policy") is True
            and capabilities.get("margin_cost_policy_scope") == "fixed_adverse_full_fill_delay_v1"
        )
        self.margin_replay.set_auxiliary_supported(
            self.client.attached is True
            and capabilities.get("margin_native_observation_replay") is True
            and capabilities.get("margin_native_observation_replay_scope")
            == "native_trade_flow_available_cutoff_v1",
            v2_supported=(
                self.client.attached is True
                and capabilities.get("margin_native_observation_replay_v2") is True
                and capabilities.get("margin_native_observation_replay_v2_scope")
                == "native_auxiliary_available_cutoff_v2"
            ),
            source_family=(
                {"omega.market-observations.1m.v1": 1, "omega.market-observations.1m.v2": 2}.get(
                    (self.dataset_controls.metadata or {}).get("schema_id")
                )
            ),
        )
        self.simulation_mode.model().item(1).setEnabled(supported)
        self.margin_replay.setVisible(shared)
        self.seed.setEnabled(not shared)
        self.device.setEnabled(not shared)
        self.advanced.setVisible(not shared)
        self.run_button.setEnabled(
            self.client.attached
            and (not shared or supported)
            and not self._submitted
            and self._research_available
            and (not shared or self.margin_replay.funding.can_submit())
            and (not shared or self.margin_replay.cost_policy.can_submit())
            and (not shared or self.margin_replay.auxiliary_can_submit())
            and (not shared or self.margin_replay.analysis_execution_can_submit())
        )
        self.simulation_mode.setToolTip(
            "This mode requires core capability margin_simulation; CPU reference only."
        )

    def apply_simulation_draft(self, parameters):
        """Copy into the correct engine without submitting or copying user consent."""
        parameters = json.loads(json.dumps(parameters, allow_nan=False))
        dataset = str(parameters.pop("dataset", ""))
        if {"account", "strategies", "start_ms", "brackets"} <= parameters.keys():
            if not self._margin_supported():
                raise ValueError("This core cannot load a shared margin replay draft")
            self.margin_replay.load_parameters(parameters)
            self.simulation_mode.setCurrentIndex(1)
        else:
            self.simulation_mode.setCurrentIndex(0)
            self.parameters.setPlainText(plain_json(parameters))
            spec = parameters.get("spec", {})
            seed = spec.get("seed", parameters.get("seed", 0))
            if type(seed) is int and 0 <= seed < 2**63:
                self.seed.setValue(seed)
            self.device.setCurrentText(str(spec.get("backend", "auto")))
        self.dataset.setText(dataset)
        self.result.setPlainText(
            "Copied as a draft using installed modules. "
            "Review assumptions before a separate submit."
        )

    def set_presentation(self, _theme: str, language: str) -> None:
        self.language = language
        if hasattr(self, "causal_controls"):
            self.causal_controls.set_presentation(_theme, language)
        if hasattr(self, "shadow_controls"):
            self.shadow_controls.set_presentation(_theme, language)
        self.run_button.setText(translate("run", language))
        self.update_health(self._research_health)


class SettingsPage(QWidget):
    presentationChanged = Signal(str, str)

    def __init__(self, client, preferences: dict, parent=None):
        super().__init__(parent)
        self.client = client
        self.current = {}
        self.revision = 0
        self.dirty = False
        layout = QVBoxLayout(self)
        tabs = QTabWidget()
        display = QWidget()
        form = QFormLayout(display)
        self.theme = QComboBox()
        self.theme.addItems(["dark", "light"])
        self.theme.setCurrentText(preferences.get("theme", "light"))
        self.language = QComboBox()
        self.language.addItem("한국어", "ko")
        self.language.addItem("English", "en")
        self.language.setCurrentIndex(1 if preferences.get("language") == "en" else 0)
        form.addRow("Theme", self.theme)
        form.addRow("Language", self.language)
        self.font_settings = FontSettings(preferences.get("font", {}))
        form.addRow(self.font_settings)
        self.mobile_hint = QLabel(translate("mobile_hint", self.language.currentData()))
        self.mobile_hint.setWordWrap(True)
        form.addRow(self.mobile_hint)
        self.mobile_button = QPushButton("Open mobile browser preview")
        self.mobile_button.clicked.connect(self._open_mobile)
        form.addRow(self.mobile_button)
        display_scroll = QScrollArea()
        display_scroll.setWidgetResizable(True)
        display_scroll.setWidget(display)
        tabs.addTab(display_scroll, "Display")
        self.runtime_settings = RuntimeSettingsPage(client)
        self.runtime_settings.set_presentation("light", self.language.currentData())
        tabs.addTab(self.runtime_settings, "Runtime settings")
        if hasattr(client, "stateReceived"):
            client.stateReceived.connect(self.runtime_settings.update_state)
        runtime = QWidget()
        runtime_layout = QVBoxLayout(runtime)
        self.revision_label = QLabel("Core settings · disconnected")
        self.settings_json = QPlainTextEdit()
        self.settings_json.setPlaceholderText("Core runtime settings appear when connected")
        controls = QHBoxLayout()
        self.reload_button = QPushButton("Reload current settings")
        self.apply_button = QPushButton("Apply changes at displayed revision")
        self.apply_button.setEnabled(False)
        controls.addWidget(self.reload_button)
        controls.addWidget(self.apply_button)
        runtime_layout.addWidget(self.revision_label)
        runtime_layout.addWidget(self.settings_json)
        runtime_layout.addLayout(controls)
        tabs.addTab(runtime, "Runtime / logging / notifications")
        self.notification_settings = NotificationSettingsPage(client)
        tabs.addTab(self.notification_settings, "Notifications / speech")
        device = QWidget()
        device_layout = QVBoxLayout(device)
        row = QHBoxLayout()
        self.device_name = QLineEdit()
        self.device_name.setPlaceholderText("Device name")
        self.device_name.setMaxLength(64)
        self.allow_pause = QCheckBox("Pause")
        self.allow_resume = QCheckBox("Resume")
        self.allow_pause.setChecked(True)
        self.allow_resume.setChecked(True)
        self.pair_button = QPushButton("Issue 2-minute pairing code")
        row.addWidget(self.device_name)
        row.addWidget(self.allow_pause)
        row.addWidget(self.allow_resume)
        row.addWidget(self.pair_button)
        device_layout.addLayout(row)
        self.pairing = QLineEdit()
        self.pairing.setReadOnly(True)
        self.pairing.setPlaceholderText("Pairing code is shown once here")
        device_layout.addWidget(self.pairing)
        self.mobile_url = QLineEdit()
        self.mobile_url.setReadOnly(True)
        self.mobile_url.setPlaceholderText("Mobile URL · no access token in the address")
        device_layout.addWidget(self.mobile_url)
        self.devices = data_table(["Device ID", "Name", "Permissions", "Revoked"])
        device_layout.addWidget(self.devices)
        self.revoke_button = QPushButton("Revoke selected device")
        device_layout.addWidget(self.revoke_button)
        tabs.addTab(device, "Registered devices")
        layout.addWidget(tabs)
        self.theme.currentTextChanged.connect(self._presentation)
        self.language.currentIndexChanged.connect(self._presentation)
        self.settings_json.textChanged.connect(self._mark_dirty)
        self.reload_button.clicked.connect(self._reload)
        self.apply_button.clicked.connect(self._apply)
        self.pair_button.clicked.connect(self._pair)
        self.revoke_button.clicked.connect(self._revoke)
        self.set_attached(False)

    def _presentation(self, *_args) -> None:
        self.runtime_settings.set_presentation(
            self.theme.currentText(), self.language.currentData()
        )
        self.presentationChanged.emit(self.theme.currentText(), self.language.currentData())

    def _open_mobile(self) -> None:
        if self.client.attached:
            QDesktopServices.openUrl(QUrl(self.client.url + "/mobile/"))

    def _mark_dirty(self) -> None:
        self.dirty = True

    def update_settings(self, settings: dict) -> None:
        self.current = settings
        from omega.domain.errors import ValidationError
        from omega.runtime.mobile_origin import canonical_mobile_origin

        try:
            origin = canonical_mobile_origin(settings.get("mobile", {}).get("https_origin", ""))
            base = origin or getattr(self.client, "url", "")
            self.mobile_url.setText(base + "/mobile/" if base else "")
        except (AttributeError, ValidationError):
            self.mobile_url.clear()
        self.runtime_settings.update_settings(settings)
        self.notification_settings.update_settings(settings)
        if not self.dirty:
            self._reload()

    def _reload(self) -> None:
        self.revision = self.current.get("revision", 0)
        self.revision_label.setText(f"Core settings revision {self.revision}")
        self.settings_json.setPlainText(
            plain_json(
                {
                    key: value
                    for key, value in self.current.items()
                    if key not in {"schema_version", "revision"}
                }
            )
        )
        self.dirty = False

    def _apply(self) -> None:
        try:
            changes = json.loads(self.settings_json.toPlainText())
            if not isinstance(changes, dict):
                raise ValueError("settings changes must be a JSON object")
            self.client.command("settings.patch", {"changes": changes, "revision": self.revision})
        except ValueError as error:
            QMessageBox.warning(self, "Settings", str(error))

    def _pair(self) -> None:
        permissions = ["read"]
        if self.allow_pause.isChecked():
            permissions.append("pause")
        if self.allow_resume.isChecked():
            permissions.append("resume")
        self.client.command(
            "issue_pairing", {"name": self.device_name.text().strip(), "permissions": permissions}
        )

    def _revoke(self) -> None:
        item = self.devices.item(self.devices.currentRow(), 0)
        if item:
            self.client.command("revoke_device", {"device_id": item.text()})

    def set_devices(self, response: dict) -> None:
        update_table(
            self.devices,
            [
                [
                    item.get("id", item.get("device_id", "?")),
                    item.get("name", "?"),
                    ", ".join(item.get("permissions", [])),
                    item.get("revoked", False),
                ]
                for item in response.get("devices", [])
            ],
        )

    def set_attached(self, attached: bool) -> None:
        self.runtime_settings.set_attached(attached)
        self.notification_settings.set_attached(attached)
        for button in (self.apply_button, self.pair_button, self.revoke_button, self.mobile_button):
            button.setEnabled(attached)
