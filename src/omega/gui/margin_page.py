"""Structured opt-in virtual margin controls; all execution stays behind core API."""

import json
import re
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, localcontext
from hashlib import sha256
from uuid import uuid4

from PySide6.QtCore import QDateTime, QSignalBlocker, Qt, QTimer, QTimeZone, Signal
from PySide6.QtWidgets import (
    QAbstractItemView,
    QBoxLayout,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLayout,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.domain.errors import ValidationError
from omega.domain.models import is_valid_symbol
from omega.gui.analysis_template_selector import AnalysisTemplateSelector
from omega.gui.configuration import ConfigurationEditor
from omega.gui.cost_editor import COST_SCOPE, CostEditor
from omega.gui.external_feature_bindings import PATH as BINDINGS_PATH
from omega.gui.external_feature_bindings import (
    ExternalFeatureBindings,
    required_fields,
    validate_wait_ms,
)
from omega.gui.funding_editor import FundingEditor
from omega.gui.margin_config_copy import (
    MarginConfigurationCopy,
    analysis_execution_policy,
    new_parameters,
    prepare_source,
)
from omega.gui.margin_retirement import MarginRetirement, retired_accounts
from omega.runtime.margin_configuration_client import configuration_url, validate_configuration

BASE = "/api/margin-trading"
_ID = re.compile(r"[A-Za-z0-9_.:-]{1,128}\Z")
_ACTIVE = {"queued", "running"}
_CURRENT_FUNDING = object()
_CURRENT_COST_POLICY = object()
_LOCAL_RESUME_NOTE = (
    "계정 재개와 다른 조작입니다. 계정은 정지 상태로 유지되며 "
    "신규 진입은 별도로 재개합니다. 이미 접수한 주문은 취소하지 않습니다.",
    "This clears only the selected strategy's local stop. The account stays paused; "
    "resume entries separately. Already accepted orders remain active.",
)


def _money(text, *, positive=False):
    if type(text) not in {str, int}:
        raise ValueError("십진 문자열로 입력하세요 / Enter a decimal string")
    try:
        value = Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("유한한 십진수를 입력하세요 / Enter a finite decimal") from None
    if (
        not value.is_finite()
        or len(value.as_tuple().digits) > 18
        or not -18 <= value.as_tuple().exponent <= 18
        or positive
        and value <= 0
    ):
        raise ValueError("숫자의 범위 또는 정밀도를 확인하세요 / Check decimal range and precision")
    return value


def _identity(text):
    if type(text) is not str or _ID.fullmatch(text) is None:
        raise ValueError("ID는 영문·숫자·_ . : - 로 입력하세요 / Invalid identity")
    return text


def _document(editor):
    text = editor.toPlainText()
    if len(text.encode()) > 65536:
        raise ValueError("모듈 설정이 너무 큽니다 / Module configuration exceeds capacity")
    value = json.loads(text)
    if type(value) is not dict:
        raise ValueError("모듈 설정은 object여야 합니다 / Module configuration must be an object")
    json.dumps(value, allow_nan=False)
    return value


def _utc_text(value):
    if type(value) is not int:
        return "—"
    try:
        return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y-%m-%d %H:%M UTC")
    except (ValueError, OverflowError, OSError):
        return "?"


def _table(columns):
    result = QTableWidget(0, columns)
    result.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
    result.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
    result.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
    result.verticalHeader().hide()
    result.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
    result.setMinimumHeight(145)
    return result


class MarginPage(QWidget):
    ledgerRequested = Signal(str)

    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.epoch = self._snapshot_epoch = None
        self.attached, self.capable, self.closed = bool(client.attached), False, False
        self.accounts, self.rows, self._definitions = {}, [], {}
        self.retired_accounts, self._retirement_supported = {}, False
        self._requests, self._serial, self._version = {}, 0, 0
        self._pending, self._loading, self._editing = None, False, None
        self._module_drafts, self._active_module = {}, None
        self._binding_sent_at = None
        self._strategy_account = None
        self._copy_selection = None
        self._copy_module_epoch = None
        self._analysis_serial = 0
        self._texts, self._buttons = [], []
        self._global_allowed = False
        self._runtime_ready, self._core_state = False, None
        self._module_local_resume = False
        outer = QVBoxLayout(self)
        self.heading, self.notice, self.global_gate, self.status = (QLabel() for _ in range(4))
        self.heading.setProperty("role", "title")
        for label in (self.notice, self.global_gate, self.status):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
        outer.addWidget(self.heading)
        outer.addWidget(self.notice)
        outer.addWidget(self.global_gate)
        outer.addWidget(self.status)
        self.tabs = QTabWidget()
        outer.addWidget(self.tabs, 1)
        self._operations_tab()
        self._configuration_tab()
        self.client.resultReceived.connect(self._received)
        self.client.failed.connect(self._failed)
        self.client.connectionChanged.connect(self.set_attached)
        if hasattr(self.client, "modulesReceived"):
            self.client.modulesReceived.connect(self.set_modules)
        self.timer = QTimer(self)
        self.timer.setInterval(2000)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.set_presentation("light", "ko")
        self.new_strategy()

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def label(self):
        return self.text("가상 증거금 v2", "Virtual margin v2")

    def _button(self, ko, en, callback):
        button = QPushButton()
        self._buttons.append((button, ko, en))
        button.clicked.connect(callback)
        return button

    def _field(self, form, ko, en, widget):
        label = QLabel()
        self._texts.append((label, ko, en))
        form.addRow(label, widget)

    @staticmethod
    def _edit(value=""):
        edit = QLineEdit(value)
        edit.setMaxLength(128)
        return edit

    @staticmethod
    def _utc():
        edit = QDateTimeEdit()
        edit.setTimeZone(QTimeZone(b"UTC"))
        edit.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
        edit.setCalendarPopup(True)
        stamp = int(datetime.now(UTC).timestamp()) // 60 * 60000
        edit.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
        return edit

    def _operations_tab(self):
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        self.lifecycle = QComboBox()
        self.lifecycle.addItem("활성 / Active", "active")
        self.lifecycle.addItem("보관 / Retired", "retired")
        self.lifecycle.currentIndexChanged.connect(self._lifecycle_changed)
        layout.addWidget(self.lifecycle)
        actions = QGridLayout()
        self.refresh_button = self._button("새로 고침", "Refresh", self.refresh)
        self.pause_button = self._button(
            "선택 계정 신규 진입 정지",
            "Pause selected account entries",
            lambda: self.control("pause"),
        )
        self.resume_button = self._button(
            "선택 계정 수동 재개", "Resume selected account", lambda: self.control("resume")
        )
        self.reconcile_button = self._button(
            "선택 계정 대조", "Reconcile selected account", lambda: self.control("reconcile")
        )
        self.brackets_button = self._button(
            "선택 계정 브래킷 가정 갱신",
            "Update selected bracket assumptions",
            self.update_brackets,
        )
        self.ledger_button = self._button(
            "선택 계정 전체 원장 · ZIP", "Selected account ledger / ZIP", self.open_ledger
        )
        self.copy_configuration_button = self._button(
            "선택 계정 설정 → 새 초안", "Selected settings → new draft", self.copy_configuration
        )
        for index, button in enumerate(
            (
                self.refresh_button,
                self.pause_button,
                self.resume_button,
                self.reconcile_button,
                self.brackets_button,
                self.ledger_button,
                self.copy_configuration_button,
            )
        ):
            actions.addWidget(button, index // 3, index % 3)
        layout.addLayout(actions)
        self.account_table = _table(6)
        self.account_table.setMaximumHeight(230)
        self.account_table.itemSelectionChanged.connect(self._account_selected)
        layout.addWidget(self.account_table)
        self.account_summary = QLabel()
        self.account_summary.setWordWrap(True)
        self.account_summary.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.account_summary)
        self.retirement = MarginRetirement(self)
        self.retirement.changed.connect(self._controls)
        layout.addWidget(self.retirement)
        local_controls = QHBoxLayout()
        self.local_strategy = QComboBox()
        self.local_strategy.setMinimumContentsLength(16)
        self.local_strategy.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.local_strategy.currentIndexChanged.connect(self._controls)
        self.resume_strategy_button = self._button(
            "선택 전략 로컬 정지 해제",
            "Clear selected strategy local stop",
            self.resume_strategy,
        )
        local_controls.addWidget(self.local_strategy, 1)
        local_controls.addWidget(self.resume_strategy_button)
        layout.addLayout(local_controls)
        local_note = self.local_resume_note = QLabel()
        local_note.setWordWrap(True)
        self._texts.append((local_note, *_LOCAL_RESUME_NOTE))
        layout.addWidget(local_note)
        self.position_table = _table(7)
        layout.addWidget(self.position_table, 1)
        query = QHBoxLayout()
        self.operation_id = self._edit()
        self.operation_id.textChanged.connect(lambda: self._controls())
        self.operation_id.setPlaceholderText("Operation ID")
        self.query_button = self._button(
            "요청 ID로 결과 확인", "Query operation ID", self.query_operation
        )
        query.addWidget(self.operation_id, 1)
        query.addWidget(self.query_button)
        layout.addLayout(query)
        self.tabs.addTab(page, "")

    def _configuration_tab(self):
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        self.config_copy = MarginConfigurationCopy()
        self.config_copy.applyRequested.connect(self.apply_configuration_copy)
        for control in self.config_copy.consents.values():
            control.toggled.connect(self._controls)
        layout.addWidget(self.config_copy)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.account_id, self.balance, self.fee_rate = (
            self._edit("virtual_1"),
            self._edit("1000"),
            self._edit("0.001"),
        )
        self.start = self._utc()
        for ko, en, edit in (
            ("가상 계정 ID", "Virtual account ID", self.account_id),
            ("초기 잔액 (USDT)", "Opening balance (USDT)", self.balance),
            ("시작할 확정 1분봉 UTC", "First closed minute UTC", self.start),
            ("수수료율 (0.001 = 0.1%)", "Fee rate (0.001 = 0.1%)", self.fee_rate),
        ):
            self._field(form, ko, en, edit)
        layout.addLayout(form)
        self.strategy_table = _table(6)
        self.strategy_table.setMaximumHeight(190)
        self.strategy_table.itemSelectionChanged.connect(self._strategy_selected)
        layout.addWidget(self.strategy_table)
        row_actions = QHBoxLayout()
        self.new_button = self._button("새 전략 입력", "New strategy draft", self.new_strategy)
        self.save_button = self._button(
            "전략 목록에 반영", "Apply strategy to list", self.save_strategy
        )
        self.remove_button = self._button(
            "선택 전략 제외", "Remove selected strategy", self.remove_strategy
        )
        for button in (self.new_button, self.save_button, self.remove_button):
            row_actions.addWidget(button)
        layout.addLayout(row_actions)
        self.editor_panel = QWidget()
        self.editor_layout = QBoxLayout(QBoxLayout.Direction.LeftToRight, self.editor_panel)
        self.editor_layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        self.editor_layout.setContentsMargins(0, 0, 0, 0)
        basic = QGroupBox()
        self._texts.append(
            (
                basic,
                "선택 전략 · 담보 · 종목 단위",
                "Selected strategy, collateral and instrument grid",
            )
        )
        self.editor_layout.addWidget(basic, 1)
        details = QFormLayout(basic)
        details.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        details.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.strategy_id, self.symbol = self._edit(), self._edit("BTCUSDT")
        self.module, self.margin_mode = QComboBox(), QComboBox()
        self.module.addItem("TEF", "tef")
        self.module.addItem("Target quantity", "target_quantity")
        self.margin_mode.addItem("CROSS", "cross")
        self.margin_mode.addItem("ISOLATED", "isolated")
        self.leverage = QSpinBox()
        self.leverage.setRange(1, 1000)
        self.leverage.setValue(10)
        self.budget, self.collateral = self._edit("100"), self._edit("100")
        self.quantity_step, self.price_tick = self._edit("0.001"), self._edit("0.01")
        self.min_quantity, self.min_notional = self._edit("0.001"), self._edit("0")
        self.can_short = QCheckBox()
        self.can_short.setChecked(True)
        for ko, en, edit in (
            ("전략 ID", "Strategy ID", self.strategy_id),
            ("USDM 종목", "USDM symbol", self.symbol),
            ("TS 모듈", "TS module", self.module),
            ("담보 방식", "Margin mode", self.margin_mode),
            ("레버리지", "Leverage", self.leverage),
            ("배분할 증거금 (USDT)", "Allocated margin (USDT)", self.budget),
            ("격리 지갑 담보 (USDT)", "Isolated collateral (USDT)", self.collateral),
            ("수량 단위", "Quantity step", self.quantity_step),
            ("가격 단위", "Price tick", self.price_tick),
            ("최소 수량", "Minimum quantity", self.min_quantity),
            ("최소 명목 금액", "Minimum notional", self.min_notional),
            ("신규 숏 허용", "Allow opening shorts", self.can_short),
        ):
            self._field(details, ko, en, edit)
        self.margin_mode.currentIndexChanged.connect(
            lambda: self.collateral.setEnabled(self.margin_mode.currentData() == "isolated")
        )
        module_box = QGroupBox()
        self._texts.append(
            (module_box, "모듈 설정 · 기본 분석", "Module configuration and basic analysis")
        )
        self.editor_layout.addWidget(module_box, 1)
        module_layout = QVBoxLayout(module_box)
        module_layout.setSizeConstraint(QLayout.SizeConstraint.SetMinimumSize)
        self.configuration = QPlainTextEdit()
        self.configuration.setMaximumHeight(130)
        self.configuration_editor = ConfigurationEditor(self.configuration)
        module_layout.addWidget(self.configuration_editor)
        self.advanced_toggle = QCheckBox()
        self._texts.append(
            (self.advanced_toggle, "고급 모듈 JSON 표시", "Show advanced module JSON")
        )
        self.advanced_toggle.toggled.connect(self.configuration.setVisible)
        module_layout.addWidget(self.advanced_toggle)
        module_layout.addWidget(self.configuration)
        self.configuration.hide()
        self.analysis_panel = QWidget()
        analysis_form = QFormLayout(self.analysis_panel)
        self.ema, self.fast, self.slow, self.signal = (QSpinBox() for _ in range(4))
        for spin, value in zip(
            (self.ema, self.fast, self.slow, self.signal), (20, 12, 26, 9), strict=True
        ):
            spin.setRange(2, 20000)
            spin.setValue(value)
        for ko, en, spin in (
            ("EMA 표본", "EMA samples", self.ema),
            ("MMACD fast", "MMACD fast", self.fast),
            ("MMACD slow", "MMACD slow", self.slow),
            ("Signal 표본", "Signal samples", self.signal),
        ):
            self._field(analysis_form, ko, en, spin)
        module_layout.addWidget(self.analysis_panel)
        self.analysis_selector = AnalysisTemplateSelector()
        module_layout.addWidget(self.analysis_selector)
        self.analysis_selector.changed.connect(
            lambda: self.analysis_panel.setEnabled(not self.analysis_selector.uses_profile())
        )
        self.analysis_selector.refreshRequested.connect(self.refresh_analysis_library)
        self.required_analysis = QCheckBox()
        self._texts.append(
            (self.required_analysis, "필수 지표만 실행", "Execute required indicators only")
        )
        self.required_analysis.setToolTip(
            "전체 분석 설정은 보존합니다. 지원 코어와 적용한 구조화 CA가 필요합니다. / "
            "Keep the full profile; requires core support and an applied typed CA."
        )
        self.required_analysis.setEnabled(False)
        self.analysis_selector.changed.connect(self._analysis_execution_context)
        module_layout.addWidget(self.required_analysis)
        self.symbol.textChanged.connect(self._analysis_context)
        self.external_inputs = ExternalFeatureBindings()
        self.external_inputs.refreshRequested.connect(self.refresh)
        self.external_inputs.selection.currentIndexChanged.connect(self._controls)
        module_layout.addWidget(self.external_inputs)
        self.external_wait_panel = QWidget()
        wait_form = QFormLayout(self.external_wait_panel)
        wait_form.setContentsMargins(0, 0, 0, 0)
        wait_form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.external_feature_wait = QSpinBox()
        self.external_feature_wait.setRange(0, 30000)
        self.external_feature_wait.setValue(15000)
        self.external_feature_wait.setSuffix(" ms")
        self._field(
            wait_form, "NN 결과 최대 대기", "Maximum NN result wait", self.external_feature_wait
        )
        wait_note = QLabel()
        wait_note.setWordWrap(True)
        self._texts.append(
            (
                wait_note,
                "보호·기존 주문은 즉시 처리하며 그 외에는 설정 시간까지만 NN 결과를 기다립니다. "
                "계정 등록 후에는 이 설정을 바꾸지 않습니다.",
                "Protection and accepted orders are processed immediately. Otherwise wait for NN "
                "results up to this limit. This setting is fixed when the account is registered.",
            )
        )
        wait_form.addRow(wait_note)
        module_layout.addWidget(self.external_wait_panel)
        self.configuration.textChanged.connect(self._external_context)
        self.symbol.textChanged.connect(self._external_context)
        self.module.currentIndexChanged.connect(self._module_changed)
        layout.addWidget(self.editor_panel)
        bracket_box = QGroupBox()
        self._texts.append(
            (
                bracket_box,
                "수동 브래킷 가정 · 모든 등록 종목에 단일 구간 적용",
                "Manual bracket assumptions: one tier per registered instrument",
            )
        )
        bracket_form = QFormLayout(bracket_box)
        self.bracket_cap, self.maintenance = self._edit("1000000"), self._edit("0.005")
        self.max_leverage = QSpinBox()
        self.max_leverage.setRange(1, 1000)
        self.max_leverage.setValue(20)
        self.bracket_time = self._utc()
        for ko, en, edit in (
            ("명목 금액 상한 (미만)", "Exclusive notional cap", self.bracket_cap),
            ("유지 증거금률", "Maintenance rate", self.maintenance),
            ("구간 최대 레버리지", "Tier maximum leverage", self.max_leverage),
            ("가정 기록 기준 UTC", "Assumption timestamp UTC", self.bracket_time),
        ):
            self._field(bracket_form, ko, en, edit)
        self.manual_assumptions = QCheckBox()
        self._texts.append(
            (
                self.manual_assumptions,
                "거래소 실측 자료가 아닌 수동 가정임을 확인했습니다",
                "I understand these are manual assumptions, not exchange observations",
            )
        )
        bracket_form.addRow(self.manual_assumptions)
        layout.addWidget(bracket_box)
        self.funding = FundingEditor(
            validate_candidate=lambda value: self.parameters(_funding=value, _preview=True)
        )
        self.funding.changed.connect(
            lambda: self._controls() if hasattr(self, "configure_button") else None
        )
        layout.addWidget(self.funding)
        self.cost_policy = CostEditor(
            validate_candidate=lambda value: self.parameters(_cost_policy=value, _preview=True)
        )
        self.cost_policy.changed.connect(
            lambda: self._controls() if hasattr(self, "configure_button") else None
        )
        layout.addWidget(self.cost_policy)
        self.configure_button = self._button(
            "새 가상 계정 등록 (자동 재개 안 함)",
            "Register virtual account (starts paused)",
            self.configure,
        )
        layout.addWidget(self.configure_button)
        layout.addStretch()
        scroll.setWidget(page)
        self.tabs.addTab(scroll, "")

    def _schema(self, module):
        definition = self._definitions.get(module, {})
        if definition:
            return definition.get("configuration_schema", {})
        if module == "target_quantity":
            return {
                "properties": {
                    "quantity": {
                        "type": "string",
                        "title": "Signed target quantity",
                        "default": "1",
                    }
                }
            }
        if module != "tef":
            return {}
        return {
            "properties": {
                "model": {"type": "string", "enum": ["MMACDDEFAULT"], "default": "MMACDDEFAULT"},
                "direction": {
                    "type": "string",
                    "enum": ["BOTH", "LONG", "SHORT"],
                    "default": "BOTH",
                },
                "order_type": {
                    "type": "string",
                    "enum": ["MARKET", "LIMIT", "ADAPTIVE"],
                    "default": "MARKET",
                },
                "order_offset": {"type": "string", "default": "0", "x-display-percent": True},
                "fullStopLossImmediate": {
                    "type": ["string", "null"],
                    "default": None,
                    "x-display-percent": True,
                },
                "fullStopLossClose": {
                    "type": ["string", "null"],
                    "default": None,
                    "x-display-percent": True,
                },
                "stopTradeOnFSL": {"type": "boolean", "default": True},
                "postStopLossReentry": {"type": "boolean", "default": False},
            }
        }

    def set_modules(self, data):
        definitions = {}
        for row in data.get("ts", [])[:100] if type(data.get("ts")) is list else []:
            if (
                type(row) is dict
                and type(row.get("module_id", row.get("id"))) is str
                and row.get("status", "available") == "available"
                and row.get("trusted", True) is True
            ):
                definitions[row.get("module_id", row.get("id"))] = row
        self._definitions = definitions
        self._copy_module_epoch = self.epoch if self.attached else None
        with QSignalBlocker(self.module):
            for identity, row in definitions.items():
                if self.module.findData(identity) < 0:
                    self.module.addItem(str(row.get("label", identity))[:128], identity)
        if any(
            control.hasFocus()
            for control, _definition in self.configuration_editor.controls.values()
        ):
            return  # Keep an edit which has not committed its draft yet.
        self.configuration_editor.set_schema(self._schema(self.module.currentData()))
        self._external_context()

    def _external_context(self):
        if not hasattr(self, "external_inputs"):
            return
        try:
            if self.module.currentData() not in {"tef", "target_quantity"} and not self._schema(
                self.module.currentData()
            ):
                raise ValueError("External input metadata is unavailable")
            fields = required_fields(
                self._schema(self.module.currentData()), _document(self.configuration)
            )
        except (ValueError, TypeError):
            fields = ("<invalid external input metadata>",)
        self.external_inputs.set_context("binance:usdm:" + self.symbol.text().strip(), fields)
        self.external_wait_panel.setVisible(bool(fields))
        if hasattr(self, "configure_button"):
            self._controls()

    def _module_changed(self):
        if self._loading:
            return
        if self._active_module is not None:
            self._module_drafts[self._active_module] = self.configuration.toPlainText()
        module = self.module.currentData()
        self._active_module = module
        default = {"model": "MMACDDEFAULT"} if module == "tef" else {"quantity": "1"}
        default = self._definitions.get(module, {}).get("default_configuration", default)
        self.configuration.setPlainText(self._module_drafts.get(module, json.dumps(default)))
        self.configuration_editor.set_schema(self._schema(module))
        for edit in (self.ema, self.fast, self.slow, self.signal):
            edit.setEnabled(module == "tef")
        self.analysis_panel.setVisible(module == "tef")
        self._external_context()

    def _analysis_context(self, *_args):
        self.analysis_selector.set_context(
            "binance:usdm:" + self.symbol.text().strip(),
            core_epoch=self.epoch,
            attached=self.attached,
        )
        self._analysis_execution_context()

    def _analysis_execution_supported(self):
        core = (self._core_state or {}).get("core", {})
        caps = core.get("capabilities", {})
        return (
            self.attached
            and not self.closed
            and bool(self.epoch)
            and type(caps) is dict
            and caps.get("margin_analysis_execution") is True
            and caps.get("margin_analysis_execution_scope") == "required_fields_v1"
        )

    def _analysis_execution_context(self, *_args):
        if not hasattr(self, "required_analysis"):
            return
        try:
            analysis = self.analysis_selector.analysis_document(
                "binance:usdm:" + self.symbol.text().strip(), core_epoch=self.epoch
            )
            analysis_execution_policy(
                {"version": 1, "policy": "required_fields"},
                analysis,
                supported=self._analysis_execution_supported(),
            )
        except (ValueError, TypeError, KeyError):
            self.required_analysis.setEnabled(False)
        else:
            self.required_analysis.setEnabled(True)

    def refresh_analysis_library(self):
        if not self.attached or not self.epoch:
            return
        self._analysis_serial += 1
        self._request(
            "GET", "/api/analysis-library", context=("analysis_library", self._analysis_serial)
        )

    def new_strategy(self):
        self._editing = None
        self.strategy_table.clearSelection()
        self.strategy_id.setText(f"ts_{len(self.rows) + 1}")
        used = {row["position"]["instrument"] for row in self.rows}
        self.symbol.setText(
            next(
                (
                    symbol
                    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")
                    if "binance:usdm:" + symbol not in used
                ),
                "ASSETUSDT",
            )
        )
        self._module_drafts, self._active_module = {}, None
        self.module.setCurrentIndex(0)
        self._module_changed()
        self.external_inputs.select_documents([])
        self.margin_mode.setCurrentIndex(0)
        self.collateral.setEnabled(False)
        self.analysis_selector.reset()
        self.required_analysis.setChecked(False)
        self._analysis_context()

    def _strategy_selected(self):
        selected = self.strategy_table.selectionModel().selectedRows()
        if len(selected) != 1:
            return
        self._editing = selected[0].row()
        row = self.rows[self._editing]
        position, strategy = row["position"], row["strategy"]
        self._loading = True
        try:
            self.strategy_id.setText(strategy["id"])
            self.symbol.setText(strategy["instrument"].split(":")[-1])
            self.module.setCurrentIndex(self.module.findData(strategy["module"]))
            self.margin_mode.setCurrentIndex(self.margin_mode.findData(position["margin_mode"]))
            self.leverage.setValue(position["leverage"])
            self.budget.setText(position["budget_margin"])
            self.collateral.setText(position.get("isolated_collateral", "100"))
            self.collateral.setEnabled(position["margin_mode"] == "isolated")
            constraints = strategy["constraints"]
            for key in ("quantity_step", "price_tick", "min_quantity", "min_notional"):
                getattr(self, key).setText(constraints[key])
            self.can_short.setChecked(constraints["can_short"])
            self.configuration.setPlainText(
                json.dumps(strategy["configuration"], ensure_ascii=False)
            )
            self._active_module = strategy["module"]
            self._module_drafts = {self._active_module: self.configuration.toPlainText()}
            self.configuration_editor.set_schema(self._schema(self._active_module))
            analysis = strategy["analysis"]
            self.ema.setValue(analysis.get("ema_period", 20))
            periods = analysis.get("mmacd_periods", [12, 26])
            self.fast.setValue(periods[0])
            self.slow.setValue(periods[1])
            self.signal.setValue(analysis.get("signal_period", 9))
            self.analysis_selector.reset()
            if analysis and not set(analysis) <= {"ema_period", "mmacd_periods", "signal_period"}:
                self.analysis_selector.load_document(analysis, strategy["instrument"])
            self.required_analysis.setChecked("analysis_execution" in strategy)
        finally:
            self._loading = False
        self.analysis_panel.setVisible(self.module.currentData() == "tef")
        self._analysis_execution_context()
        self._external_context()
        self.external_inputs.select_documents(strategy.get("external_features", []))
        for control in (self.ema, self.fast, self.slow, self.signal):
            control.setEnabled(self.module.currentData() == "tef")

    def strategy_document(self):
        identity = _identity(self.strategy_id.text().strip())
        symbol = self.symbol.text().strip()
        if not is_valid_symbol(symbol) or not symbol.endswith("USDT"):
            raise ValueError("USDT 선물 종목을 입력하세요 / Enter a linear USDT symbol")
        instrument = "binance:usdm:" + symbol
        budget = _money(self.budget.text())
        collateral = (
            _money(self.collateral.text())
            if self.margin_mode.currentData() == "isolated"
            else Decimal(0)
        )
        if budget < 0 or collateral < 0:
            raise ValueError("예산과 담보는 음수일 수 없습니다 / Negative budget or collateral")
        rules = {
            key: str(
                _money(getattr(self, key).text(), positive=key in {"quantity_step", "price_tick"})
            )
            for key in ("quantity_step", "price_tick", "min_quantity", "min_notional")
        }
        if _money(rules["min_quantity"]) < 0 or _money(rules["min_notional"]) < 0:
            raise ValueError("최소 단위는 0 이상이어야 합니다 / Minimums must be nonnegative")
        rules.update(leverage=str(self.leverage.value()), can_short=self.can_short.isChecked())
        configuration = _document(self.configuration)
        if self.module.currentData() == "target_quantity":
            _money(configuration.get("quantity", "0"))
        analysis = self.analysis_selector.analysis_document(instrument, core_epoch=self.epoch)
        if analysis is None and self.module.currentData() == "tef":
            if self.fast.value() >= self.slow.value():
                raise ValueError("Fast 표본은 slow보다 작아야 합니다 / Fast must be below slow")
            analysis = {
                "ema_period": self.ema.value(),
                "mmacd_periods": [self.fast.value(), self.slow.value()],
                "signal_period": self.signal.value(),
            }
        elif analysis is None:
            analysis = {}
        position = {
            "instrument": instrument,
            "owner_id": identity,
            "margin_mode": self.margin_mode.currentData(),
            "leverage": self.leverage.value(),
            "budget_margin": str(budget),
        }
        if position["margin_mode"] == "isolated":
            position["isolated_collateral"] = str(collateral)
        strategy = {
            "id": identity,
            "instrument": instrument,
            "module": self.module.currentData(),
            "configuration": configuration,
            "analysis": analysis,
            "constraints": rules,
        }
        if self.required_analysis.isChecked():
            strategy["analysis_execution"] = analysis_execution_policy(
                {"version": 1, "policy": "required_fields"},
                analysis,
                supported=self._analysis_execution_supported(),
            )
        self._external_context()
        bindings = self.external_inputs.documents(self.epoch)
        if bindings:
            strategy["external_features"] = bindings
        return {"position": position, "strategy": strategy}

    def save_strategy(self):
        try:
            row = self.strategy_document()
            for index, previous in enumerate(self.rows):
                if index != self._editing and (
                    row["strategy"]["id"] == previous["strategy"]["id"]
                    or row["strategy"]["instrument"] == previous["strategy"]["instrument"]
                ):
                    raise ValueError(
                        "종목과 전략 ID는 중복할 수 없습니다 / Duplicate instrument or strategy ID"
                    )
            if self._editing is None:
                if len(self.rows) >= 100:
                    raise ValueError("전략은 최대 100개입니다 / At most 100 strategies")
                self.rows.append(row)
                selected = len(self.rows) - 1
            else:
                selected = self._editing
                self.rows[selected] = row
            self._render_strategies()
            self.strategy_table.selectRow(selected)
            self.status.setText(
                self.text(
                    "전략을 로컬 초안 목록에 반영했습니다",
                    "Strategy applied to the local draft list",
                )
            )
        except (ValueError, TypeError, KeyError) as error:
            self.status.setText(str(error))

    def remove_strategy(self):
        selected = self.strategy_table.selectionModel().selectedRows()
        if len(selected) == 1:
            self.rows.pop(selected[0].row())
            self._render_strategies()
            self.new_strategy()

    def _render_strategies(self):
        with QSignalBlocker(self.strategy_table):
            self.strategy_table.setRowCount(len(self.rows))
            for index, row in enumerate(self.rows):
                position, strategy = row["position"], row["strategy"]
                for column, value in enumerate(
                    (
                        strategy["id"],
                        strategy["instrument"].split(":")[-1],
                        strategy["module"],
                        position["margin_mode"],
                        position["leverage"],
                        position["budget_margin"],
                    )
                ):
                    self.strategy_table.setItem(index, column, QTableWidgetItem(str(value)))

    def parameters(
        self, *, _funding=_CURRENT_FUNDING, _cost_policy=_CURRENT_COST_POLICY, _preview=False
    ):
        identity = _identity(self.account_id.text().strip())
        balance, fee = _money(self.balance.text()), _money(self.fee_rate.text())
        if balance < 0 or not 0 <= fee <= 1 or not self.rows:
            raise ValueError(
                "잔액·수수료·전략 목록을 확인하세요 / Check balance, fee and strategy list"
            )
        if self._editing is None or self.strategy_document() != self.rows[self._editing]:
            raise ValueError(
                "전략 초안을 목록에 먼저 반영하세요 / Apply strategy edits to the list first"
            )
        if not _preview and not self.manual_assumptions.isChecked():
            raise ValueError(
                "수동 브래킷 가정임을 확인하세요 / Confirm the manual bracket assumptions"
            )
        if not _preview and not self._copy_consents_ready():
            raise ValueError(
                "복사한 비용/Funding/NN 설정에 새로 동의하세요 / Confirm copied assumptions"
            )
        # Recheck every saved row against the current core list before registration.
        # Editing a different row must not keep a retired deployment authorized.
        context = (self.external_inputs.instrument, self.external_inputs.required)
        selected = self.external_inputs.selection.currentData()
        try:
            for row in self.rows:
                strategy = row["strategy"]
                if "analysis_execution" in strategy:
                    analysis_execution_policy(
                        strategy["analysis_execution"],
                        strategy["analysis"],
                        supported=self._analysis_execution_supported(),
                    )
                fields = required_fields(
                    self._schema(strategy["module"]), strategy["configuration"]
                )
                self.external_inputs.set_context(strategy["instrument"], fields)
                self.external_inputs.documents(
                    self.epoch, saved=strategy.get("external_features", [])
                )
        finally:
            self.external_inputs.set_context(*context)
            self.external_inputs.select_documents([{"binding_sha256": selected}])
        start = self.start.dateTime().toMSecsSinceEpoch()
        stamp = self.bracket_time.dateTime().toMSecsSinceEpoch()
        if start < 0 or start % 60000 or stamp < 0 or stamp > start + 60000:
            raise ValueError(
                "시작 분과 브래킷 가정 UTC를 확인하세요 / Check start minute and assumption UTC"
            )
        cap, rate = _money(self.bracket_cap.text(), positive=True), _money(self.maintenance.text())
        if not 0 <= rate <= 1:
            raise ValueError("유지 증거금률은 0~1입니다 / Maintenance rate must be in [0,1]")
        positions = [deepcopy(row["position"]) for row in self.rows]
        with localcontext() as calculation:
            calculation.prec = 256
            excessive = (
                sum((_money(row["budget_margin"]) for row in positions), Decimal(0)) > balance
                or sum(
                    (_money(row.get("isolated_collateral", "0")) for row in positions), Decimal(0)
                )
                > balance
            )
        if excessive:
            raise ValueError(
                "배분 예산과 격리 담보는 초기 잔액 이하여야 합니다 / "
                "Allocations and collateral exceed opening balance"
            )
        if any(row["leverage"] > self.max_leverage.value() for row in positions):
            raise ValueError(
                "전략 배율이 브래킷 상한을 초과합니다 / Strategy leverage exceeds tier maximum"
            )
        tables = [
            {
                "instrument": row["instrument"],
                "observed_at_ms": stamp,
                "available_at_ms": stamp,
                "rows": [
                    {
                        "notional_floor": "0",
                        "notional_cap": str(cap),
                        "maintenance_rate": str(rate),
                        "maintenance_amount": "0",
                        "max_leverage": self.max_leverage.value(),
                    }
                ],
            }
            for row in positions
        ]
        result = {
            "account": {"account_id": identity, "balance": str(balance), "positions": positions},
            "strategies": [deepcopy(row["strategy"]) for row in self.rows],
            "start_ms": start,
            "brackets": tables,
            "fee_rate": str(fee),
        }
        if any(row["strategy"].get("external_features") for row in self.rows):
            result["external_feature_wait_ms"] = validate_wait_ms(
                self.external_feature_wait.value()
            )
        funding = self.funding.document() if _funding is _CURRENT_FUNDING else _funding
        if funding is not None:
            result["funding"] = deepcopy(funding)
            self.funding.validate_document(funding, request=result)
        costs = (
            self.cost_policy.document() if _cost_policy is _CURRENT_COST_POLICY else _cost_policy
        )
        if costs is not None:
            result["cost_policy"] = deepcopy(costs)
            self.cost_policy.validate_document(costs, request=result)
        if (
            len(json.dumps(result, sort_keys=True, allow_nan=False, separators=(",", ":")).encode())
            > 131072
        ):
            raise ValueError(
                "계정 설정이 코어 요청 한도를 초과합니다 / "
                "Account configuration exceeds core capacity"
            )
        return result

    def configure(self):
        if not self.configure_button.isEnabled():
            return
        try:
            parameters = self.parameters()
            if (
                parameters["account"]["account_id"] in self.accounts
                or parameters["account"]["account_id"] in self.retired_accounts
            ):
                raise ValueError(
                    "등록된 계정은 새 잔액으로 덮어쓰지 않습니다 / "
                    "Existing account cannot be reset through registration"
                )
        except (ValueError, TypeError, KeyError) as error:
            self.status.setText(str(error))
            return
        self._submit("configure", parameters)

    def selected_account(self):
        selected = self.account_table.selectionModel().selectedRows()
        if len(selected) != 1:
            return None
        item = self.account_table.item(selected[0].row(), 0)
        rows = self.retired_accounts if self.lifecycle.currentData() == "retired" else self.accounts
        return rows.get(item.data(Qt.ItemDataRole.UserRole)) if item else None

    def open_ledger(self):
        row = self.selected_account()
        if row and self.ledger_button.isEnabled():
            self.ledgerRequested.emit(row["account_id"])

    def copy_configuration(self):
        row = self.selected_account()
        if row is None or not self.copy_configuration_button.isEnabled():
            return
        identity = row["account_id"]
        context = self.config_copy.begin(identity, self.epoch)
        self.config_copy.setChecked(True)
        self.tabs.setCurrentIndex(1)
        self._request("GET", configuration_url(identity), context=("configuration", context))
        self._controls()

    def _copy_current(self, context):
        row = self.selected_account()
        return bool(
            row
            and context == (row["account_id"], self.config_copy.generation)
            and self.config_copy.source_epoch == self.epoch
        )

    def _copy_source(self, result, context):
        if not self._copy_current(context):
            return
        document = validate_configuration(result, account_id=context[0], core_epoch=self.epoch)
        if document["state"] != "ready":
            raise ValueError("Configuration unavailable: " + document["reason_code"])
        source = prepare_source(
            document["parameters"],
            modules=self._copy_modules(),
            schema_for=self._schema,
            number=_money,
            identity=_identity,
            analysis_execution_supported=self._analysis_execution_supported(),
        )
        self._validate_copy_policies(document["parameters"])
        self.config_copy.accept(source, document["configuration_sha256"])

    def _copy_modules(self):
        if self._copy_module_epoch != self.epoch:
            raise ValueError("Refresh the current core TS module list before copying")
        return set(self._definitions)

    def _validate_copy_policies(self, parameters):
        if "funding" in parameters:
            self.funding.validate_document(parameters["funding"], request=parameters)
        if "cost_policy" in parameters:
            self.cost_policy.validate_document(parameters["cost_policy"], request=parameters)

    def _copy_consents_ready(self):
        return self.config_copy.consent_ready(
            {
                "funding": self.funding.isChecked(),
                "cost": self.cost_policy.isChecked(),
                "nn": any(row["strategy"].get("external_features") for row in self.rows),
            }
        )

    def apply_configuration_copy(self):
        copy = self.config_copy
        try:
            if (
                not self._enabled()
                or self._snapshot_epoch != self.epoch
                or not self._runtime_ready
                or self._pending
                or not copy.source
                or not self._copy_current((copy.source_id, copy.generation))
                or not copy.explicit_start.isChecked()
            ):
                raise ValueError("Read the selected source and explicitly confirm new capital/UTC")
            # Revalidate all source fields/modules/caps and final request bytes before mutation.
            source = prepare_source(
                copy.source["raw"],
                modules=self._copy_modules(),
                schema_for=self._schema,
                number=_money,
                identity=_identity,
                analysis_execution_supported=self._analysis_execution_supported(),
            )
            source["checksum"] = copy.source["checksum"]
            parameters = new_parameters(
                source,
                new_id=copy.new_id.text().strip(),
                balance=copy.capital.text().strip(),
                start_ms=copy.start.dateTime().toMSecsSinceEpoch(),
                known_ids=set(self.accounts) | set(self.retired_accounts),
                number=_money,
                identity=_identity,
            )
            self._validate_copy_policies(parameters)
        except (ValidationError, ValueError, TypeError, KeyError) as error:
            copy.status.setText("기존 초안 유지 / Previous draft retained: " + str(error)[:250])
            return
        self.account_id.setText(parameters["account"]["account_id"])
        self.balance.setText(parameters["account"]["balance"])
        self.start.setDateTime(copy.start.dateTime())
        self.fee_rate.setText(source["fee"])
        self.rows = deepcopy(source["rows"])
        cap, rate, maximum, stamp = source["tier"]
        self.bracket_cap.setText(cap)
        self.maintenance.setText(rate)
        self.max_leverage.setValue(maximum)
        self.bracket_time.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
        self.external_feature_wait.setValue(source["wait"])
        self.external_inputs.invalidate()
        self._render_strategies()
        self.strategy_table.selectRow(0)
        self._strategy_selected()
        if "funding" in parameters:
            self.funding.load_document(
                parameters["funding"], label="Copied registered request", request=parameters
            )
        else:
            self.funding.reset()
        if "cost_policy" in parameters:
            self.cost_policy.load_document(parameters["cost_policy"], request=parameters)
        else:
            self.cost_policy.reset()
        self.manual_assumptions.setChecked(False)
        copy.reset_consents(source)
        copy.status.setText("새 초안만 적용됨 · 동의/NN 재검증 후 별도 등록 · 재개/주문 전송 없음")
        self._controls()

    def control(self, kind):
        button = {
            "pause": self.pause_button,
            "resume": self.resume_button,
            "reconcile": self.reconcile_button,
        }[kind]
        row = self.selected_account()
        if row and button.isEnabled():
            self._submit(kind, {"account_id": row["account_id"]})

    def resume_strategy(self):
        if not self._enabled() or not self.resume_strategy_button.isEnabled():
            return
        row = self.selected_account()
        identity = self.local_strategy.currentData()
        if row is None or not self._local_resume_ready(row, identity):
            return
        self._submit("resume_strategy", {"account_id": row["account_id"], "strategy_id": identity})

    def _local_resume_ready(self, row, identity):
        shutdown = row.get("shutdown", {})
        return (
            type(identity) is str
            and row["account_id"] not in self.retired_accounts
            and self._module_local_resume
            and type(row.get("strategies")) is list
            and type(shutdown) is dict
            and row.get("state") == "ready"
            and not row.get("pending_frame")
            and not row.get("manual_recovery_required")
            and not any(
                shutdown.get(key) for key in ("active", "recovery_required", "needs_settlement")
            )
            and self._strategy_account == (self.epoch, row["account_id"])
            and any(
                item.get("id") == identity and item.get("state") == "ready"
                for item in row.get("strategies", [])
                if type(item) is dict
            )
        )

    def _local_strategies(self, row):
        target = None if row is None else (self.epoch, row["account_id"])
        previous = self.local_strategy.currentData() if target == self._strategy_account else None
        self._strategy_account = target
        strategies = row.get("strategies", []) if row else []
        if (
            type(strategies) is not list
            or len(strategies) > 100
            or any(
                type(item) is not dict
                or type(item.get("id")) is not str
                or _ID.fullmatch(item["id"]) is None
                for item in strategies
            )
            or len({item["id"] for item in strategies}) != len(strategies)
        ):
            strategies = []
        with QSignalBlocker(self.local_strategy):
            self.local_strategy.clear()
            self.local_strategy.addItem(
                self.text("로컬 정지를 해제할 전략 선택", "Select strategy to clear local stop"),
                None,
            )
            for item in strategies:
                self.local_strategy.addItem(item["id"], item["id"])
            index = self.local_strategy.findData(previous) if previous else -1
            self.local_strategy.setCurrentIndex(index if index > 0 else 0)

    def update_brackets(self):
        row = self.selected_account()
        if row is None or not self.brackets_button.isEnabled():
            return
        try:
            if not self.manual_assumptions.isChecked():
                raise ValueError(
                    "새 가상 계정 탭에서 수동 가정 확인을 선택하세요 / "
                    "Confirm manual assumptions in the draft tab"
                )
            cap, rate = (
                _money(self.bracket_cap.text(), positive=True),
                _money(self.maintenance.text()),
            )
            stamp = self.bracket_time.dateTime().toMSecsSinceEpoch()
            if not 0 <= rate <= 1 or stamp < 0:
                raise ValueError("브래킷 가정값을 확인하세요 / Check bracket assumptions")
            instruments = row.get("instruments")
            if type(instruments) is not list or not 1 <= len(instruments) <= 100:
                raise ValueError(
                    "계정 종목을 확인할 수 없습니다 / Account instrument list unavailable"
                )
            tables = [
                {
                    "instrument": instrument,
                    "observed_at_ms": stamp,
                    "available_at_ms": stamp,
                    "rows": [
                        {
                            "notional_floor": "0",
                            "notional_cap": str(cap),
                            "maintenance_rate": str(rate),
                            "maintenance_amount": "0",
                            "max_leverage": self.max_leverage.value(),
                        }
                    ],
                }
                for instrument in instruments
            ]
            self._submit("update_brackets", {"account_id": row["account_id"], "brackets": tables})
        except (ValueError, TypeError) as error:
            self.status.setText(str(error))

    def _request(self, method, path, payload=None, *, context):
        if len(self._requests) >= 64:
            self.status.setText(
                self.text(
                    "응답 대기 요청이 많습니다. 잠시 후 조회하세요",
                    "Too many outstanding requests; query after responses finish",
                )
            )
            if method == "POST" and self._pending:
                self._pending["unknown"] = True
            if context[0] == "lifecycle":
                self.retirement.failed(context[1], "request capacity")
            return
        self._serial += 1
        tag = f"margin-v2:{id(self)}:{self._serial}"
        self._requests[tag] = (self.epoch, self._version, context)
        if context == ("bindings", None):
            self._binding_sent_at = self.external_inputs.clock()
        try:
            self.client.request(method, path, payload, tag=tag)
        except Exception as error:
            self._failed(tag, type(error).__name__)

    def _submit(self, kind, parameters):
        if self.retirement.busy:
            return
        identity = uuid4().hex
        self._version += 1
        self._requests.clear()
        self._pending = {
            "id": identity,
            "kind": kind,
            "parameters": deepcopy(parameters),
            "epoch": self.epoch,
            "unknown": False,
        }
        self.operation_id.setText(identity)
        self.status.setText(
            self.text(
                "요청 접수 확인 중 · 자동 재전송하지 않습니다",
                "Confirming acceptance; no automatic POST retry",
            )
        )
        self._controls()
        self._request(
            "POST",
            BASE,
            {"core_epoch": self.epoch, "id": identity, "kind": kind, "parameters": parameters},
            context=("operation", identity),
        )
        self._controls()

    def refresh(self):
        if not self._enabled():
            return
        if not any(context[2] == ("registry", None) for context in self._requests.values()):
            self._request("GET", BASE, context=("registry", None))
        if not self._definitions and not any(
            context[2] == ("modules", None) for context in self._requests.values()
        ):
            self._request("GET", "/api/modules", context=("modules", None))
        if (
            self.external_inputs.required
            or any(row["strategy"].get("external_features") for row in self.rows)
        ) and not any(context[2] == ("bindings", None) for context in self._requests.values()):
            self._request("GET", BINDINGS_PATH, context=("bindings", None))

    def query_operation(self):
        if not self._enabled():
            return
        identity = self.operation_id.text().strip()
        try:
            _identity(identity)
        except ValueError as error:
            self.status.setText(str(error))
            return
        self._request("GET", BASE + "/" + identity, context=("operation", identity))

    def _enabled(self):
        return self.attached and self.capable and bool(self.epoch) and not self.closed

    def _controls(self):
        enabled = self._enabled() and self._snapshot_epoch == self.epoch and self._runtime_ready
        mutable = (
            enabled
            and self._pending is None
            and not self.retirement.busy
            and not self.retirement.requests
        )
        selected = self.selected_account()
        archived = self.lifecycle.currentData() == "retired"
        readable = selected is not None and (
            not archived or self._retirement_supported and self.retirement.can_read(selected)
        )
        account_mutable = bool(
            mutable
            and selected
            and not archived
            and selected["account_id"] not in self.retired_accounts
            and not self.retirement.blocks(selected["account_id"])
        )
        self.retirement.sync(
            selected,
            self.retired_accounts.get(selected["account_id"]) if selected else None,
            archived,
            self._retirement_supported,
            self._enabled() and self._runtime_ready and self._pending is None,
        )
        self.ledger_button.setEnabled(self._enabled() and readable)
        binding_ready = True
        try:
            self.external_inputs.documents(self.epoch)
        except ValueError:
            binding_ready = False
        self.configure_button.setEnabled(
            mutable
            and binding_ready
            and self.funding.can_submit()
            and self.cost_policy.can_submit()
            and self._copy_consents_ready()
        )
        self.copy_configuration_button.setEnabled(
            mutable
            and readable
            and not any(
                row[2][0] == "configuration" and row[2][1][0] == selected["account_id"]
                for row in self._requests.values()
            )
        )
        self.config_copy.apply_button.setEnabled(
            mutable
            and self.config_copy.source is not None
            and self._copy_current((self.config_copy.source_id, self.config_copy.generation))
        )
        self.external_inputs.refresh_button.setEnabled(self._enabled())
        for button in (self.pause_button, self.reconcile_button, self.brackets_button):
            button.setEnabled(account_mutable)
        self.resume_button.setEnabled(account_mutable and selected.get("state") == "ready")
        local_ready = (
            account_mutable
            and selected is not None
            and self._local_resume_ready(selected, self.local_strategy.currentData())
        )
        self.resume_strategy_button.setEnabled(local_ready)
        self.local_strategy.setEnabled(account_mutable)
        self.local_resume_note.setText(
            (
                ""
                if self._module_local_resume
                else self.text(
                    "이 코어는 로컬 정지 해제를 지원하지 않습니다. ",
                    "This core does not support clearing local stops. ",
                )
            )
            + self.text(*_LOCAL_RESUME_NOTE)
        )
        self.refresh_button.setEnabled(self._enabled())
        self.query_button.setEnabled(self._enabled() and bool(self.operation_id.text()))

    def set_attached(self, attached):
        self.attached = bool(attached)
        self._analysis_context()
        if not self.attached:
            self.retirement.invalidate()
            self.config_copy.invalidate()
            self._copy_module_epoch = None
            self.external_inputs.invalidate()
            self.funding.set_supported(False)
            self.cost_policy.set_supported(False)
            self._version += 1
            self._requests.clear()
            self._snapshot_epoch = None
            self._runtime_ready = False
            self._module_local_resume = False
            self._local_strategies(None)
            if self._pending:
                self._pending["unknown"] = True
                self.status.setText(
                    self.text(
                        "접속이 끊겼습니다. 요청 ID로 확인하세요. 자동 재전송하지 않습니다",
                        "Disconnected: query the operation ID; it will not be resent",
                    )
                )
        self._controls()

    def update_state(self, state):
        core = state.get("core", {})
        self._core_state = {"core": deepcopy(core)}
        epoch = core.get("epoch")
        if type(epoch) is str and epoch and epoch != self.epoch:
            old = self.epoch
            self.epoch = epoch
            self.retirement.invalidate()
            self.config_copy.invalidate()
            self._copy_module_epoch = None
            self.external_inputs.invalidate()
            self.funding.set_supported(False)
            self.cost_policy.set_supported(False)
            self._version += 1
            self._requests.clear()
            self._snapshot_epoch = None
            self._runtime_ready = False
            self._module_local_resume = False
            self.accounts = {}
            self.retired_accounts, self._retirement_supported = {}, False
            self._local_strategies(None)
            self.account_table.setRowCount(0)
            self.position_table.setRowCount(0)
            if old and self._pending:
                self.status.setText(
                    self.text(
                        "코어가 변경됐습니다. 이전 요청을 재전송하지 말고 ID로 확인하세요",
                        "Core changed: query the previous operation ID; do not resend",
                    )
                )
                self._pending = None
        self.capable = core.get("capabilities", {}).get("margin_trading") is True
        self._analysis_context()
        self._global_allowed = core.get("effective_entries_allowed") is True
        self.global_gate.setText(
            self.text(
                "전체 코어 진입 허용"
                if self._global_allowed
                else "전체 코어 진입 정지/미확인 · "
                "이 페이지의 계정 재개만으로 전체 정지가 해제되지 않습니다",
                "Global core entries allowed"
                if self._global_allowed
                else "Global core entries paused/unconfirmed; "
                "local account resume does not lift the global pause",
            )
        )
        self._controls()

    def _received(self, tag, result):
        request = self._requests.pop(tag, None)
        if request is None or self.closed or request[:2] != (self.epoch, self._version):
            return
        context, identity = request[2]
        if context == "analysis_library":
            if identity != self._analysis_serial:
                return
            try:
                # This existing endpoint has no epoch field; bind its request locally.
                self.analysis_selector.set_analysis_library(result, core_epoch=request[0])
            except (ValueError, TypeError, KeyError) as error:
                self.analysis_selector.invalidate(str(error))
            return
        if context == "modules":
            self.set_modules(result)
            return
        if result.get("core_epoch") != self.epoch:
            if context == "lifecycle":
                self.retirement.failed(identity, "stale core")
            if context == "configuration":
                self.config_copy.status.setText(
                    "이전 코어 응답 제외 · 기존 초안 유지 / Stale core ignored"
                )
                self._controls()
            return
        try:
            if context == "bindings":
                self.external_inputs.apply_snapshot(
                    result, self.epoch, requested_at=self._binding_sent_at
                )
            elif context == "registry":
                self.apply_snapshot(result)
            elif context == "configuration":
                self._copy_source(result, identity)
            elif context == "lifecycle":
                self.retirement.received(identity, result)
            else:
                self._operation(result, identity)
        except (ValidationError, ValueError, TypeError, KeyError) as error:
            if context == "registry":
                self._snapshot_epoch = None
            self.status.setText(
                self.text("코어 응답을 검증할 수 없습니다: ", "Cannot validate core response: ")
                + str(error)[:160]
            )
            if context == "configuration":
                self.config_copy.status.setText(
                    "기존 초안 유지 / Previous draft retained: " + str(error)[:250]
                )
            if context == "lifecycle":
                self.retirement.failed(identity, error)
        self._controls()

    def _operation(self, result, identity):
        if result.get("id") != identity or result.get("state") not in _ACTIVE | {
            "completed",
            "unknown",
            "failed",
            "error",
        }:
            raise ValueError("operation identity/state mismatch")
        if self._pending and self._pending["id"] == identity:
            if (
                result.get("operation_core_epoch", result.get("core_epoch"))
                != self._pending["epoch"]
            ):
                raise ValueError("operation belongs to another core")
            if result.get("kind") != self._pending["kind"]:
                raise ValueError("operation kind mismatch")
            expected = sha256(
                json.dumps(
                    (self._pending["kind"], self._pending["parameters"]),
                    sort_keys=True,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode()
            ).hexdigest()
            if result.get("request_digest") != expected:
                raise ValueError("operation payload digest mismatch")
            if result["state"] not in _ACTIVE:
                self._pending = None
                self._snapshot_epoch = None
                self.refresh()
        self.status.setText(
            f"{identity} · {result['state']}"
            + (" · " + str(result["error"])[:160] if result.get("error") else "")
        )

    def apply_snapshot(self, result):
        rows = result.get("accounts")
        if result.get("core_epoch") != self.epoch or type(rows) is not list or len(rows) > 10:
            raise ValueError("invalid margin registry")
        accounts = {}
        for row in rows:
            if (
                type(row) is not dict
                or type(row.get("account_id")) is not str
                or _ID.fullmatch(row["account_id"]) is None
                or row["account_id"] in accounts
            ):
                raise ValueError("invalid account identity")
            document = row.get("account")
            if document is not None:
                if (
                    type(document) is not dict
                    or type(document.get("schema_version")) is not int
                    or document["schema_version"] not in {1, 2}
                    or type(document.get("account")) is not dict
                ):
                    raise ValueError("invalid account document")
                positions = document["account"].get("positions")
                summary = document.get("summary", {})
                if (
                    type(positions) is not list
                    or len(positions) > 100
                    or any(type(position) is not dict for position in positions)
                    or type(summary) is not dict
                    or "ledger_total" in summary
                    and (type(summary["ledger_total"]) is not int or summary["ledger_total"] < 0)
                ):
                    raise ValueError("invalid bounded account detail")
            accounts[row["account_id"]] = row
        retired = retired_accounts(result.get("retired_accounts", []))
        if any(
            identity in accounts and row["state"] == "retired" for identity, row in retired.items()
        ):
            raise ValueError("retired account still appears active")
        selected = self.selected_account()
        previous = None if selected is None else selected["account_id"]
        self.accounts, self._snapshot_epoch = accounts, self.epoch
        self.retired_accounts = retired
        self._runtime_ready = result.get("state") == "ready"
        capabilities = result.get("capabilities", {})
        self.funding.set_supported(
            type(capabilities) is dict
            and capabilities.get("funding") is True
            and capabilities.get("funding_scope") == "minute_boundary_scenario_rate_v1"
        )
        self.cost_policy.set_supported(
            type(capabilities) is dict
            and capabilities.get("cost_policy") is True
            and capabilities.get("cost_policy_scope") == COST_SCOPE
        )
        self._module_local_resume = (
            type(capabilities) is dict and capabilities.get("module_local_resume") is True
        )
        self._retirement_supported = (
            type(capabilities) is dict and capabilities.get("retirement") is True
        )
        if not self._runtime_ready:
            self.status.setText(
                self.text("가상 증거금 서비스 준비 안 됨: ", "Virtual margin service unavailable: ")
                + str(result.get("error") or result.get("state", "unknown"))[:160]
            )
        self._render_accounts(previous)
        if self._pending and not self._pending["unknown"]:
            for row in (
                result.get("operations", [])[:128] if type(result.get("operations")) is list else []
            ):
                if row.get("id") == self._pending["id"]:
                    self._operation(
                        row
                        | {"core_epoch": self.epoch, "operation_core_epoch": row.get("core_epoch")},
                        row["id"],
                    )
                    break
        self._controls()

    def _lifecycle_changed(self):
        if not hasattr(self, "retirement"):
            return
        self._render_accounts()

    def _render_accounts(self, previous=None):
        archive = self.lifecycle.currentData() == "retired"
        accounts = self.retired_accounts if archive else self.accounts
        self.lifecycle.setItemText(0, self.text("활성", "Active") + f" ({len(self.accounts)}/10)")
        self.lifecycle.setItemText(
            1,
            self.text("보관 / 전환중", "Retired / retiring")
            + f" ({len(self.retired_accounts)}/128)",
        )
        with QSignalBlocker(self.account_table):
            self.account_table.clearSelection()
            self.account_table.setCurrentCell(-1, -1)
            self.account_table.setRowCount(len(accounts))
            for index, (identity, row) in enumerate(accounts.items()):
                for column, value in enumerate(
                    (
                        identity,
                        row.get("state", "?"),
                        "read-only" if archive else row.get("feed_state", "?"),
                        "retained" if archive else row.get("paused", "?"),
                        _utc_text(row.get("cursor_ms")),
                        len(row["pending_orders"])
                        if type(row.get("pending_orders")) is list
                        else "?",
                    )
                ):
                    item = QTableWidgetItem(str(value))
                    item.setData(Qt.ItemDataRole.UserRole, identity)
                    self.account_table.setItem(index, column, item)
            for index, identity in enumerate(accounts):
                if identity == previous:
                    self.account_table.selectRow(index)
        self._account_selected()

    def _account_selected(self):
        row = self.selected_account()
        identity = row["account_id"] if row else None
        if identity != self._copy_selection:
            self._copy_selection = identity
            self.config_copy.invalidate(identity)
        self._local_strategies(row)
        document = row.get("account") if row else None
        account = document.get("account", {}) if type(document) is dict else {}
        positions = account.get("positions", [])
        if type(positions) is not list or len(positions) > 100:
            positions = []
        self.position_table.setRowCount(len(positions))
        for index, position in enumerate(positions):
            for column, key in enumerate(
                (
                    "instrument",
                    "margin_mode",
                    "leverage",
                    "quantity",
                    "entry_price",
                    "isolated_wallet",
                    "realized_pnl",
                )
            ):
                value = str(position.get(key, "?"))
                item = QTableWidgetItem(value.split(":")[-1] if key == "instrument" else value)
                item.setToolTip(value)
                self.position_table.setItem(index, column, item)
        summary = document.get("summary", {}) if type(document) is dict else {}
        total = summary.get(
            "ledger_total", len(account["ledger"]) if type(account.get("ledger")) is list else "?"
        )
        self.account_summary.setText(
            self.text("계정을 선택하세요", "Select an account")
            if row is None
            else f"{row['account_id']} · Cross wallet: {account.get('cross_wallet', '?')} USDT · "
            f"Ledger: {total} · Next: {_utc_text(row.get('next_open_ms'))} · "
            f"{row.get('feed_reason') or row.get('error') or ''}"
        )
        if row is not None and self.lifecycle.currentData() == "retired":
            self.account_summary.setText(
                f"{row['account_id']} · {row['state']} · Ledger: {row.get('ledger_total')} · "
                + self.text(
                    "잔액 보존 · 현재 금액 미확인 · 이체 아님",
                    "Balances retained; current amounts unconfirmed; no transfer",
                )
            )
        self._controls()

    def _failed(self, tag, message):
        request = self._requests.pop(tag, None)
        if request is None or self.closed or request[:2] != (self.epoch, self._version):
            return
        if request[2][0] == "analysis_library":
            if request[2][1] == self._analysis_serial:
                self.analysis_selector.invalidate(str(message)[:220])
            return
        if request[2][0] == "bindings":
            self.external_inputs.invalidate()
        if request[2][0] == "configuration":
            self.config_copy.status.setText(
                "설정 조회 실패 · 기존 초안 유지 / Read failed; draft retained"
            )
        if request[2][0] == "lifecycle":
            self.retirement.failed(request[2][1], message)
        if request[2][0] == "operation" and self._pending and self._pending["id"] == request[2][1]:
            self._pending["unknown"] = True
            self.status.setText(
                self.text(
                    "요청 결과 미확인 · ID로 조회하세요 · 자동 재전송하지 않습니다",
                    "Operation outcome unknown: query its ID; no automatic POST retry",
                )
            )
        else:
            self.status.setText(str(message)[:220])
        self._controls()

    def _tick(self):
        if self.isVisible() and self._enabled():
            self.refresh()
            if self._pending and not self._pending["unknown"]:
                self.query_operation()
            self.retirement.tick()

    def set_shell_embedded(self, embedded):
        self.heading.setVisible(not embedded)

    def set_presentation(self, theme, language):
        self.language = language if language in {"ko", "en"} else "en"
        self.external_inputs.set_presentation(self.language)
        self.funding.set_language(self.language)
        self.cost_policy.set_language(self.language)
        self.retirement.render()
        self.heading.setText(self.label())
        self.notice.setText(
            self.text(
                "별도 가상 계정 · 1분 OHLCV 대용 가격 · 전량 체결 · 기본 슬리피지/추가 지연 0 · "
                "청산 미지원. 지원 코어의 비용/Funding 시나리오는 명시 선택할 때만 적용하며 "
                "거래소 실지급이 아닙니다. 수동 브래킷/종목 제약 가정이며 기존 v1과 분리됩니다.",
                "Separate virtual accounts: 1m OHLCV proxy marks, full fills, default zero "
                "slippage/extra delay, no liquidation. "
                "Supported cost/funding scenarios are opt-in, "
                "not a venue payment. Brackets and grids are manual assumptions; "
                "v1 is separate.",
            )
        )
        for widget, ko, en in self._texts:
            if isinstance(widget, QGroupBox):
                widget.setTitle(self.text(ko, en))
            else:
                widget.setText(self.text(ko, en))
        for button, ko, en in self._buttons:
            button.setText(self.text(ko, en))
        self._local_strategies(self.selected_account())
        self.tabs.setTabText(0, self.text("계정 운영", "Account operations"))
        self.tabs.setTabText(1, self.text("새 가상 계정", "New virtual account"))
        self.account_table.setHorizontalHeaderLabels(
            self.text(
                ["계정", "상태", "수집", "진입 정지", "소비한 분(UTC)", "미체결"],
                ["Account", "State", "Feed", "Paused", "Consumed minute", "Pending"],
            )
        )
        self.strategy_table.setHorizontalHeaderLabels(
            self.text(
                ["전략", "종목", "모듈", "담보", "배율", "증거금"],
                ["Strategy", "Symbol", "Module", "Margin", "Leverage", "Budget"],
            )
        )
        self.position_table.setHorizontalHeaderLabels(
            self.text(
                ["종목", "담보", "배율", "수량", "진입가", "격리 지갑", "실현손익"],
                [
                    "Instrument",
                    "Margin",
                    "Leverage",
                    "Quantity",
                    "Entry",
                    "Isolated wallet",
                    "Realized PnL",
                ],
            )
        )
        if self._core_state is not None:
            self.update_state(self._core_state)
        self._controls()

    def resizeEvent(self, event):
        direction = (
            QBoxLayout.Direction.TopToBottom
            if self.width() < 1050
            else QBoxLayout.Direction.LeftToRight
        )
        changed = self.editor_layout.direction() != direction
        self.editor_layout.setDirection(direction)
        super().resizeEvent(event)
        if changed:
            # Reflow the following groups before painting with the old horizontal height.
            self.editor_panel.updateGeometry()
            parent_layout = self.editor_panel.parentWidget().layout()
            parent_layout.invalidate()
            parent_layout.activate()

    def closeEvent(self, event):
        self.closed = True
        self.timer.stop()
        self._version += 1
        self._requests.clear()
        self.retirement.invalidate()
        super().closeEvent(event)
