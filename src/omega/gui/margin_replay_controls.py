"""Structured draft for offline shared-margin replay; never calls a trading API."""

import json
import re
from copy import deepcopy
from decimal import Decimal, InvalidOperation, localcontext

from PySide6.QtCore import QDateTime, Qt, QTimeZone
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.gui.analysis_template_selector import AnalysisTemplateSelector
from omega.gui.configuration import ConfigurationEditor
from omega.gui.cost_editor import CostEditor
from omega.gui.funding_editor import FundingEditor
from omega.gui.margin_config_copy import analysis_execution_policy

_CURRENT_FUNDING = object()
_CURRENT_COST_POLICY = object()


def _auxiliary_document(value):
    if value is None:
        return None
    if (
        type(value) is not dict
        or set(value) != {"mode", "decision_latency_ms"}
        or type(value["mode"]) is not str
        or value["mode"] != "native"
        or type(value["decision_latency_ms"]) is not int
        or not 0 <= value["decision_latency_ms"] <= 60000
    ):
        raise ValueError("원본 관측 native 모드와 지연 0..60000ms / Native replay latency required")
    return dict(value)


def _amount(value, *, positive=False):
    if type(value) is not str or len(value) > 128:
        raise ValueError("Enter a bounded decimal string")
    try:
        result = Decimal(value)
    except InvalidOperation:
        raise ValueError("Enter a finite decimal string") from None
    if (
        not result.is_finite()
        or len(result.as_tuple().digits) > 18
        or not -18 <= result.as_tuple().exponent <= 18
    ):
        raise ValueError("Decimal exceeds supported precision")
    if result < 0 or positive and not result:
        raise ValueError("Amount must be positive" if positive else "Amount must be nonnegative")
    return result


def _identity(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValueError("Identity requires1..128 letters, digits or _ . : -")
    return value


class MarginReplayControls(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.rows, self.definitions = [], {}
        self._analysis_epoch, self._analysis_attached = None, False
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        self.notice = QLabel(
            "공유 증거금 재생 / Shared margin replay: closed1m, next-minute full fills, "
            "cross/isolated, CPU. Default zero slippage/extra order delay; explicit cost scenarios "
            "require core support. No intrabar randomization, liquidation or GPU. "
            "Funding is an explicitly selected scenario on supported cores, not a venue payment. "
            "브래킷은 수동 합성 가정이며 거래소 관측이 아닙니다."
        )
        self.notice.setWordWrap(True)
        outer.addWidget(self.notice)
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.account_id, self.balance, self.fee = (
            QLineEdit("replay"),
            QLineEdit("10000"),
            QLineEdit("0.001"),
        )
        self.start = QDateTimeEdit()
        self.start.setTimeZone(QTimeZone(b"UTC"))
        self.start.setDisplayFormat("yyyy-MM-dd HH:mm 'UTC'")
        self.start.setDateTime(QDateTime.fromMSecsSinceEpoch(0, QTimeZone(b"UTC")))
        self.start.setCalendarPopup(True)
        for label, edit in (
            ("Replay account ID", self.account_id),
            ("Opening balance USDT", self.balance),
            ("Fee ratio (0..1)", self.fee),
            ("First dataset minute", self.start),
        ):
            form.addRow(label, edit)
        outer.addLayout(form)
        native = QGroupBox("원본 보조 관측 / Original auxiliary observations")
        native_form = QFormLayout(native)
        native_form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self._auxiliary_supported = False
        self.auxiliary_mode = QComboBox()
        self.auxiliary_mode.addItem("기존 OHLCV / Plain OHLCV", None)
        self.auxiliary_mode.addItem("원본 관측 native / Native original observations", "native")
        self.decision_latency_ms = QSpinBox()
        self.decision_latency_ms.setRange(0, 60000)
        self.decision_latency_ms.setSuffix(" ms")
        native_form.addRow("입력 방식 / Input mode", self.auxiliary_mode)
        native_form.addRow("결정 지연 / Decision latency", self.decision_latency_ms)
        self.auxiliary_notice = QLabel(
            "원본 선택 관측만 사용하며 실제 availability와 revision을 보존합니다. "
            "Native trade_flow/NES only; generated/MC/proxy inputs are unsupported. "
            "WOI/depth/metric 전략은 이 모드에서 지원하지 않습니다."
        )
        self.auxiliary_notice.setWordWrap(True)
        self.auxiliary_notice.setTextFormat(Qt.TextFormat.PlainText)
        native_form.addRow(self.auxiliary_notice)
        outer.addWidget(native)
        self.auxiliary_mode.currentIndexChanged.connect(self._auxiliary_changed)
        self.set_auxiliary_supported(False)
        strategy = QGroupBox("전략 추가 / Add strategy")
        fields = QFormLayout(strategy)
        self._strategy_form = fields
        fields.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.strategy_id, self.symbol = QLineEdit("ts_1"), QLineEdit("BTCUSDT")
        self.module, self.margin_mode = QComboBox(), QComboBox()
        self.module.addItem("Target quantity", "target_quantity")
        self.module.addItem("TEF", "tef")
        self.margin_mode.addItems(["cross", "isolated"])
        self.leverage = QSpinBox()
        self.leverage.setRange(1, 125)
        self.leverage.setValue(10)
        self.budget, self.collateral = QLineEdit("100"), QLineEdit("100")
        for label, edit in (
            ("Strategy ID", self.strategy_id),
            ("Binance USD-M symbol", self.symbol),
            ("TS family", self.module),
            ("Margin mode", self.margin_mode),
            ("Leverage", self.leverage),
            ("Budget USDT", self.budget),
            ("Isolated collateral USDT", self.collateral),
        ):
            fields.addRow(label, edit)
        self.configuration = QPlainTextEdit('{"quantity":"1"}')
        self.configuration.setMaximumHeight(100)
        self.editor = ConfigurationEditor(self.configuration)
        fields.addRow("TS parameters", self.editor)
        self.advanced = QGroupBox("Advanced TS JSON")
        self.advanced.setCheckable(True)
        self.advanced.setChecked(False)
        self.advanced.setMaximumHeight(36)
        advanced_layout = QVBoxLayout(self.advanced)
        advanced_layout.addWidget(self.configuration)
        self.configuration.hide()
        self.advanced.toggled.connect(self.configuration.setVisible)
        self.advanced.toggled.connect(
            lambda checked: self.advanced.setMaximumHeight(150 if checked else 36)
        )
        fields.addRow(self.advanced)
        self.ema, self.fast, self.slow, self.signal = (QSpinBox() for _ in range(4))
        for item, default in zip(
            (self.ema, self.fast, self.slow, self.signal), (20, 12, 26, 9), strict=True
        ):
            item.setRange(1, 100000)
            item.setValue(default)
        for label, edit in (
            ("TEF EMA", self.ema),
            ("TEF MACD fast", self.fast),
            ("TEF MACD slow", self.slow),
            ("TEF MACD signal", self.signal),
        ):
            fields.addRow(label, edit)
        self.analysis_selector = AnalysisTemplateSelector()
        fields.addRow(self.analysis_selector)
        self._analysis_execution_supported = False
        self.required_analysis = QCheckBox("필수 지표만 실행 / Execute required indicators only")
        self.required_analysis.setEnabled(False)
        self.required_analysis.setToolTip(
            "전체 프로필은 보존합니다. 지원 코어와 적용한 구조화 CA가 필요합니다. / "
            "Keep the full profile; requires core support and an applied typed CA."
        )
        fields.addRow(self.required_analysis)
        self.analysis_selector.changed.connect(self._analysis_controls)
        self.symbol.textChanged.connect(self._analysis_context)
        self.add_button = QPushButton("목록에 추가 / Add to list")
        self.remove_button = QPushButton("선택 삭제 / Remove selected")
        buttons = QHBoxLayout()
        buttons.addWidget(self.add_button)
        buttons.addWidget(self.remove_button)
        fields.addRow(buttons)
        outer.addWidget(strategy)
        self.table = QTableWidget(0, 4)
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.table.setHorizontalHeaderLabels(["TS", "Instrument", "Module", "Margin / leverage"])
        self.table.setMinimumHeight(115)
        outer.addWidget(self.table)
        assumptions = QFormLayout()
        assumptions.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.bracket_cap, self.maintenance = QLineEdit("1000000"), QLineEdit("0.01")
        self.max_leverage = QSpinBox()
        self.max_leverage.setRange(1, 125)
        self.max_leverage.setValue(20)
        self.manual = QCheckBox("합성 브래킷 확인 / Confirm synthetic tiers")
        self.manual.setToolTip(
            "These manual single-tier assumptions are synthetic, not exchange observations."
        )
        self.noncausal = QCheckBox("비인과 보간 허용 / Allow noncausal input")
        self.noncausal.setToolTip(
            "Allow noncausally repaired input only for offline stress replay."
        )
        for label, edit in (
            ("Tier notional cap", self.bracket_cap),
            ("Maintenance ratio", self.maintenance),
            ("Tier maximum leverage", self.max_leverage),
        ):
            assumptions.addRow(label, edit)
        assumptions.addRow(self.manual)
        assumptions.addRow(self.noncausal)
        outer.addLayout(assumptions)
        self.funding = FundingEditor(
            validate_candidate=lambda value: self.parameters(_funding=value, _preview=True)
        )
        outer.addWidget(self.funding)
        self.cost_policy = CostEditor(
            validate_candidate=lambda value: self.parameters(_cost_policy=value, _preview=True)
        )
        outer.addWidget(self.cost_policy)
        self.status = QLabel()
        self.status.setWordWrap(True)
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        outer.addWidget(self.status)
        self.add_button.clicked.connect(self.add_strategy)
        self.remove_button.clicked.connect(self.remove_strategy)
        self.module.currentIndexChanged.connect(self._module_changed)
        self.margin_mode.currentTextChanged.connect(
            lambda mode: self.collateral.setEnabled(mode == "isolated")
        )
        self.collateral.setEnabled(False)
        self._module_changed()

    def set_analysis_context(self, *, core_epoch, attached):
        self._analysis_epoch, self._analysis_attached = core_epoch, attached is True
        self._analysis_context()

    def _analysis_context(self, *_args):
        self.analysis_selector.set_context(
            "binance:usdm:" + self.symbol.text().strip(),
            core_epoch=self._analysis_epoch,
            attached=self._analysis_attached,
        )

    def _analysis_controls(self):
        for edit in (self.ema, self.fast, self.slow, self.signal):
            edit.setEnabled(
                self.module.currentData() == "tef" and not self.analysis_selector.uses_profile()
            )
        try:
            analysis = self.analysis_selector.analysis_document(
                "binance:usdm:" + self.symbol.text().strip(), core_epoch=self._analysis_epoch
            )
            self._execution_policy({"version": 1, "policy": "required_fields"}, analysis)
        except (ValueError, TypeError, KeyError):
            self.required_analysis.setEnabled(False)
        else:
            self.required_analysis.setEnabled(True)

    def set_analysis_execution_supported(self, supported):
        self._analysis_execution_supported = supported is True
        self._analysis_controls()

    def _execution_policy(self, value, analysis):
        return analysis_execution_policy(
            value,
            analysis,
            supported=(
                self._analysis_execution_supported
                and self._analysis_attached
                and bool(self._analysis_epoch)
            ),
        )

    def analysis_execution_can_submit(self):
        return not any("analysis_execution" in row["strategy"] for row in self.rows) or (
            self._analysis_execution_supported
            and self._analysis_attached
            and bool(self._analysis_epoch)
        )

    def set_auxiliary_supported(self, supported, *, v2_supported=False, source_family=None):
        self._auxiliary_supported = (
            v2_supported is True if source_family == 2 else supported is True
        )
        self.auxiliary_mode.model().item(1).setEnabled(
            self._auxiliary_supported or source_family is None and v2_supported is True
        )
        self.auxiliary_notice.setText(
            "V2 원본 flow/depth/희소 OI·LS · 원래 availability/revision 유지. "
            "Native original inputs; graph policy must fit the recorded seed range. "
            "NN training/MC/generated/proxy replay unsupported."
            if source_family == 2
            else "원본 선택 관측만 사용하며 실제 availability와 revision을 보존합니다. "
            "Native trade_flow/NES only; generated/MC/proxy inputs are unsupported. "
            "WOI/depth/metric 전략은 이 V1 모드에서 지원하지 않습니다."
        )
        self.auxiliary_mode.setToolTip(
            ""
            if self._auxiliary_supported
            else "연결된 코어의 원본 관측 재생 지원 필요 / Core native replay support required"
        )
        self._auxiliary_changed()

    def _auxiliary_changed(self, *_args):
        self.decision_latency_ms.setEnabled(
            self._auxiliary_supported and self.auxiliary_mode.currentData() == "native"
        )

    def auxiliary_can_submit(self):
        return self.auxiliary_mode.currentData() is None or self._auxiliary_supported

    def auxiliary_parameters(self):
        mode = self.auxiliary_mode.currentData()
        if mode is None:
            return None
        if not self._auxiliary_supported:
            raise ValueError("코어의 원본 관측 재생 지원 필요 / Native replay unavailable")
        return _auxiliary_document(
            {"mode": mode, "decision_latency_ms": self.decision_latency_ms.value()}
        )

    def set_modules(self, data):
        self.definitions = {
            r.get("module_id", r.get("id")): r
            for r in data.get("ts", [])
            if type(r) is dict and r.get("module_id", r.get("id")) in {"tef", "target_quantity"}
        }
        self._set_schema()

    def _set_schema(self):
        module = self.module.currentData()
        fallback = {
            "type": "object",
            "properties": {
                "quantity": {"type": "string", "title": "Signed target quantity", "default": "1"}
            },
        }
        self.editor.set_schema(
            self.definitions.get(module, {}).get(
                "configuration_schema", fallback if module == "target_quantity" else {}
            )
        )

    def _module_changed(self, *_args):
        tef = self.module.currentData() == "tef"
        self.configuration.setPlainText('{"model":"MMACDDEFAULT"}' if tef else '{"quantity":"1"}')
        self._set_schema()
        for edit in (self.ema, self.fast, self.slow, self.signal):
            edit.setEnabled(tef)
            self._strategy_form.setRowVisible(edit, tef)
        self._analysis_controls()

    def add_strategy(self):
        try:
            identity = _identity(self.strategy_id.text().strip())
            symbol = self.symbol.text().strip()
            if re.fullmatch(r"[A-Z0-9_]{5,64}", symbol) is None or not symbol.endswith("USDT"):
                raise ValueError("Choose a canonical Binance USD-M USDT symbol")
            instrument = "binance:usdm:" + symbol
            if len(self.rows) >= 100 or any(
                r["position"]["instrument"] == instrument or r["strategy"]["id"] == identity
                for r in self.rows
            ):
                raise ValueError("A session needs unique instruments and strategy IDs; maximum100")
            budget = _amount(self.budget.text(), positive=True)
            raw_configuration = self.configuration.toPlainText()
            if len(raw_configuration.encode()) > 65536:
                raise ValueError("TS configuration exceeds capacity")
            configuration = json.loads(raw_configuration)
            if (
                type(configuration) is not dict
                or len(json.dumps(configuration, allow_nan=False).encode()) > 65536
            ):
                raise ValueError("TS configuration must be a bounded object")
            mode = self.margin_mode.currentText()
            position = {
                "instrument": instrument,
                "owner_id": identity,
                "margin_mode": mode,
                "leverage": self.leverage.value(),
                "budget_margin": str(budget),
            }
            if mode == "isolated":
                position["isolated_collateral"] = str(
                    _amount(self.collateral.text(), positive=True)
                )
            tef = self.module.currentData() == "tef"
            analysis = self.analysis_selector.analysis_document(
                instrument, core_epoch=self._analysis_epoch
            )
            if analysis is None:
                if tef and self.fast.value() >= self.slow.value():
                    raise ValueError("Fast period must be below slow period")
                analysis = (
                    {
                        "ema_period": self.ema.value(),
                        "mmacd_periods": [self.fast.value(), self.slow.value()],
                        "signal_period": self.signal.value(),
                    }
                    if tef
                    else {}
                )
            execution = {}
            if self.required_analysis.isChecked():
                execution["analysis_execution"] = self._execution_policy(
                    {"version": 1, "policy": "required_fields"}, analysis
                )
            self.rows.append(
                {
                    "position": position,
                    "strategy": {
                        "id": identity,
                        "instrument": instrument,
                        "module": self.module.currentData(),
                        "configuration": configuration,
                        "analysis": analysis,
                        "constraints": {"leverage": str(self.leverage.value()), "can_short": True},
                        **execution,
                    },
                }
            )
            self.table.setRowCount(len(self.rows))
            for index, row in enumerate(self.rows):
                p, s = row["position"], row["strategy"]
                for column, value in enumerate(
                    (s["id"], p["instrument"], s["module"], f"{p['margin_mode']} / {p['leverage']}")
                ):
                    self.table.setItem(index, column, QTableWidgetItem(str(value)))
                if "analysis_execution" in s:
                    self.table.item(index, 2).setToolTip(
                        "필수 지표만 실행 / Execute required indicators only"
                    )
            self.strategy_id.setText(f"ts_{len(self.rows) + 1}")
            self.status.setText(
                "Draft added; edit fields and add another unique instrument if needed."
            )
        except (ValueError, TypeError, InvalidOperation, RecursionError) as error:
            self.status.setText(str(error))

    def remove_strategy(self):
        index = self.table.currentRow()
        if 0 <= index < len(self.rows):
            del self.rows[index]
            self.table.removeRow(index)

    def parameters(
        self, *, _funding=_CURRENT_FUNDING, _cost_policy=_CURRENT_COST_POLICY, _preview=False
    ):
        account_id = _identity(self.account_id.text().strip())
        balance, fee = _amount(self.balance.text(), positive=True), _amount(self.fee.text())
        start = self.start.dateTime().toMSecsSinceEpoch()
        if (
            fee > 1
            or not self.rows
            or not _preview
            and not self.manual.isChecked()
            or start < 0
            or start % 60000
        ):
            raise ValueError(
                "Check fee0..1, first dataset minute, strategy list and manual assumptions consent"
            )
        positions = [deepcopy(row["position"]) for row in self.rows]
        with localcontext() as context:
            context.prec = 256
            if (
                sum((_amount(p["budget_margin"]) for p in positions), Decimal(0)) > balance
                or sum((_amount(p.get("isolated_collateral", "0")) for p in positions), Decimal(0))
                > balance
            ):
                raise ValueError("Allocated budgets or isolated collateral exceed opening balance")
        cap, rate = (
            _amount(self.bracket_cap.text(), positive=True),
            _amount(self.maintenance.text()),
        )
        if rate > 1 or any(p["leverage"] > self.max_leverage.value() for p in positions):
            raise ValueError("Check maintenance0..1 and tier leverage limit")
        result = {
            "account": {"account_id": account_id, "balance": str(balance), "positions": positions},
            "strategies": [deepcopy(row["strategy"]) for row in self.rows],
            "start_ms": start,
            "brackets": [
                {
                    "instrument": p["instrument"],
                    "observed_at_ms": start,
                    "available_at_ms": start,
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
                for p in positions
            ],
            "spec": {"fee_rate": str(fee), "backend": "cpu"},
            "allow_noncausal_research_input": self.noncausal.isChecked(),
        }
        for strategy in result["strategies"]:
            if "analysis_execution" in strategy:
                self._execution_policy(strategy["analysis_execution"], strategy["analysis"])
        funding = self.funding.document() if _funding is _CURRENT_FUNDING else _funding
        if funding is not None:
            result["funding"] = funding
            self.funding.validate_document(funding, request=result)
        costs = (
            self.cost_policy.document() if _cost_policy is _CURRENT_COST_POLICY else _cost_policy
        )
        if costs is not None:
            result["cost_policy"] = costs
            self.cost_policy.validate_document(costs, request=result)
        auxiliary = self.auxiliary_parameters()
        if auxiliary is not None:
            if self.noncausal.isChecked():
                raise ValueError("Native 원본 관측은 비인과/생성 입력을 허용하지 않습니다.")
            result["auxiliary_replay"] = auxiliary
        if len(json.dumps(result, allow_nan=False).encode()) > 131072:
            raise ValueError("Replay configuration exceeds capacity")
        return result

    def load_parameters(self, parameters):
        """Load a recorded draft; consent is always a new explicit user action."""
        raw = json.loads(json.dumps(parameters, allow_nan=False))
        auxiliary = _auxiliary_document(raw.get("auxiliary_replay"))
        account = raw["account"]
        positions, strategies = account["positions"], raw["strategies"]
        if (
            type(positions) is not list
            or type(strategies) is not list
            or not 1 <= len(positions) == len(strategies) <= 100
        ):
            raise ValueError("Invalid recorded shared-margin draft")
        by_owner = {_identity(s["id"]): s for s in strategies}
        if len(by_owner) != len(strategies):
            raise ValueError("Recorded strategy IDs must be unique")
        rows = []
        for position in positions:
            strategy = by_owner[position["owner_id"]]
            if strategy["instrument"] != position["instrument"]:
                raise ValueError("Recorded draft owner/instrument mismatch")
            if "analysis_execution" in strategy:
                self._execution_policy(strategy["analysis_execution"], strategy["analysis"])
            rows.append({"position": position, "strategy": strategy})
        account_id = _identity(account["account_id"])
        balance = str(_amount(account["balance"], positive=True))
        fee = str(_amount(raw.get("spec", {}).get("fee_rate", "0.001")))
        start = raw["start_ms"]
        if type(start) is not int or not 0 <= start <= 253_402_300_799_000 or start % 60000:
            raise ValueError("Recorded draft first minute is invalid")
        brackets = raw["brackets"]
        # This form emits one uniform synthetic tier; refuse to silently flatten a
        # recorded multi-tier or heterogeneous reference into different assumptions.
        if (
            type(brackets) is not list
            or len(brackets) != len(rows)
            or any(len(b["rows"]) != 1 for b in brackets)
        ):
            raise ValueError("Recorded tier shape needs advanced/manual preparation")
        tier = brackets[0]["rows"][0]
        if any(
            b["rows"][0] != tier or b["observed_at_ms"] != start or b["available_at_ms"] != start
            for b in brackets
        ):
            raise ValueError("Recorded heterogeneous references cannot be changed by this form")
        if (
            _amount(tier["notional_floor"]) != 0
            or _amount(tier["maintenance_amount"]) != 0
            or {b["instrument"] for b in brackets} != {p["instrument"] for p in positions}
            or len({p["instrument"] for p in positions}) != len(positions)
            or type(tier["max_leverage"]) is not int
            or not 1 <= tier["max_leverage"] <= 125
        ):
            raise ValueError("Recorded references exceed the structured single-tier form")
        if "funding" in raw:
            self.funding.validate_document(raw["funding"], request=raw)
        if "cost_policy" in raw:
            self.cost_policy.validate_document(raw["cost_policy"], request=raw)
        # Commit the copied draft only after all reference/identity checks succeed.
        self.account_id.setText(account_id)
        self.balance.setText(balance)
        self.fee.setText(fee)
        self.start.setDateTime(QDateTime.fromMSecsSinceEpoch(start, QTimeZone(b"UTC")))
        self.bracket_cap.setText(str(tier["notional_cap"]))
        self.maintenance.setText(str(tier["maintenance_rate"]))
        self.max_leverage.setValue(tier["max_leverage"])
        self.rows = rows
        self.analysis_selector.reset()
        self.table.setRowCount(len(rows))
        for index, row in enumerate(rows):
            p, s = row["position"], row["strategy"]
            for column, value in enumerate(
                (s["id"], p["instrument"], s["module"], f"{p['margin_mode']} / {p['leverage']}")
            ):
                self.table.setItem(index, column, QTableWidgetItem(str(value)))
            if "analysis_execution" in s:
                self.table.item(index, 2).setToolTip(
                    "필수 지표만 실행 / Execute required indicators only"
                )
        self.manual.setChecked(False)
        self.noncausal.setChecked(False)
        self.auxiliary_mode.setCurrentIndex(0 if auxiliary is None else 1)
        self.decision_latency_ms.setValue(
            0 if auxiliary is None else auxiliary["decision_latency_ms"]
        )
        if "funding" in raw:
            self.funding.load_document(raw["funding"], request=raw)
        else:
            self.funding.reset()
        if "cost_policy" in raw:
            self.cost_policy.load_document(raw["cost_policy"], request=raw)
        else:
            self.cost_policy.reset()
        self.status.setText(
            "Recorded draft loaded; review inputs and explicitly reconfirm assumptions."
        )
