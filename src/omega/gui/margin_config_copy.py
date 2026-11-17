"""Structured registered-settings copy; no financial/model state or module execution."""

import json
from copy import deepcopy
from decimal import Decimal, localcontext

from PySide6.QtCore import QDateTime, Qt, QTimeZone, Signal
from PySide6.QtWidgets import (
    QCheckBox,
    QDateTimeEdit,
    QFormLayout,
    QGroupBox,
    QLabel,
    QLineEdit,
    QPushButton,
    QVBoxLayout,
    QWidget,
)

from omega.domain.external_features import ExternalFeatureBinding
from omega.domain.models import is_valid_symbol
from omega.gui.analysis_template_selector import analysis_copy
from omega.gui.external_feature_bindings import required_fields, validate_wait_ms


def _object(value, required, optional=()):
    if type(value) is not dict or not set(required) <= set(value) <= set(required) | set(optional):
        raise ValueError("Settings are outside this structured copy form's supported fields")


def _integer(value, lower, upper):
    if type(value) is not int or not lower <= value <= upper:
        raise ValueError("Settings contain an unsupported integer or time")
    return value


def analysis_execution_policy(value, analysis, *, supported=False):
    """Validate an explicit execution choice without changing its complete profile."""
    if (
        type(value) is not dict
        or set(value) != {"version", "policy"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or type(value["policy"]) is not str
        or value["policy"] != "required_fields"
    ):
        raise ValueError("Invalid analysis_execution policy / 분석 실행 정책이 잘못되었습니다")
    if supported is not True:
        raise ValueError("Core required_fields_v1 support required / 코어 지원이 필요합니다")
    if type(analysis) is not dict or not analysis:
        raise ValueError("Explicit typed analysis profile required / 구조화 분석 설정이 필요합니다")
    analysis_copy(analysis)
    return deepcopy(value)


def prepare_source(
    parameters, *, modules, schema_for, number, identity, analysis_execution_supported=False
):
    """Validate the entire representable form before any current draft is replaced."""
    raw = deepcopy(parameters)
    _object(
        raw,
        {"account", "strategies", "start_ms", "brackets"},
        {"fee_rate", "external_feature_wait_ms", "funding", "cost_policy"},
    )
    account = raw["account"]
    _object(account, {"account_id", "balance", "positions"})
    source_id = identity(account["account_id"])
    if number(account["balance"]) < 0:
        raise ValueError("Negative registered starting capital")
    _integer(raw["start_ms"], 0, 253402300799000)
    fee = number(raw.get("fee_rate", "0.001"))
    if not 0 <= fee <= 1 or raw["start_ms"] % 60000:
        raise ValueError("Invalid fee or unaligned start minute")
    positions, strategies = account["positions"], raw["strategies"]
    if (
        type(positions) is not list
        or type(strategies) is not list
        or not 1 <= len(positions) == len(strategies) <= 100
    ):
        raise ValueError("One configured position per strategy is required")
    rows, instruments, identities, has_nn = [], set(), set(), False
    for position, strategy in zip(positions, strategies, strict=True):
        strategy = deepcopy(strategy)
        _object(
            position,
            {"instrument", "owner_id", "margin_mode", "leverage", "budget_margin"},
            {"isolated_collateral"},
        )
        _object(
            strategy,
            {"id", "instrument", "module", "configuration", "analysis", "constraints"},
            {"external_features", "analysis_execution"},
        )
        key = strategy["instrument"]
        parts = key.split(":") if type(key) is str else []
        strategy_id = identity(strategy["id"])
        module = strategy["module"]
        if (
            len(parts) != 3
            or parts[:2] != ["binance", "usdm"]
            or not is_valid_symbol(parts[2])
            or not parts[2].endswith("USDT")
            or key in instruments
            or strategy_id in identities
            or position["instrument"] != key
            or position["owner_id"] != strategy_id
        ):
            raise ValueError("Position/strategy order and unique USDT owner identities must match")
        if type(module) is not str or module not in modules:
            raise ValueError("Refresh the current core TS module list before copying")
        instruments.add(key)
        identities.add(strategy_id)
        leverage = _integer(position["leverage"], 1, 1000)
        mode = position["margin_mode"]
        if type(mode) is not str or mode not in {"cross", "isolated"}:
            raise ValueError("Unsupported margin mode")
        budget = number(position["budget_margin"])
        collateral = number(position.get("isolated_collateral", "0"))
        if budget < 0 or collateral < 0 or mode == "cross" and "isolated_collateral" in position:
            raise ValueError("Unsupported collateral or negative allocation")
        rules = strategy["constraints"]
        defaults = {
            "quantity_step": "0.001",
            "price_tick": "0.01",
            "min_quantity": "0.001",
            "min_notional": "0",
            "leverage": str(leverage),
            "can_short": True,
        }
        _object(rules, set(), set(defaults))
        rules = defaults | rules
        if type(rules["can_short"]) is not bool or number(rules["leverage"]) != leverage:
            raise ValueError("Unsupported execution constraint or leverage mismatch")
        for name in ("quantity_step", "price_tick", "min_quantity", "min_notional"):
            value = number(rules[name], positive=name in {"quantity_step", "price_tick"})
            if value < 0:
                raise ValueError("Negative execution minimum")
            rules[name] = str(value)
        rules["leverage"] = str(leverage)
        configuration, analysis = strategy["configuration"], strategy["analysis"]
        if type(configuration) is not dict or len(json.dumps(configuration).encode()) > 32768:
            raise ValueError("Unsupported TS configuration document")
        if module == "target_quantity":
            number(configuration.get("quantity", "0"))
        if module == "tef" and not analysis:
            _object(analysis, {"ema_period", "mmacd_periods", "signal_period"})
        if (
            module == "tef"
            and type(analysis) is dict
            and set(analysis) == {"ema_period", "mmacd_periods", "signal_period"}
        ):
            _object(analysis, {"ema_period", "mmacd_periods", "signal_period"})
            periods = analysis["mmacd_periods"]
            if type(periods) is not list or len(periods) != 2:
                raise ValueError("Unsupported TEF analysis periods")
            for value in (analysis["ema_period"], *periods, analysis["signal_period"]):
                _integer(value, 2, 20000)
            if periods[0] >= periods[1]:
                raise ValueError("Fast period must be below slow period")
        elif analysis != {}:
            analysis_copy(analysis)  # Full typed profile, never filter it to legacy MA keys.
        if "analysis_execution" in strategy:
            analysis_execution_policy(
                strategy["analysis_execution"], analysis, supported=analysis_execution_supported
            )
        fields = required_fields(schema_for(module), configuration)
        bindings = strategy.get("external_features", [])
        if fields:
            if type(bindings) is not list or len(bindings) != 1:
                raise ValueError("This form requires one explicit registered NN binding")
            binding = ExternalFeatureBinding.parse(bindings[0])
            if binding.instrument.key != key or set(binding.fields) != set(fields):
                raise ValueError("Saved NN binding does not match the required fields")
            has_nn = True
        elif bindings:
            raise ValueError(
                "External binding requirements are not represented by this module schema"
            )
        elif "external_features" in strategy:
            del strategy["external_features"]
        position = {
            "instrument": key,
            "owner_id": strategy_id,
            "margin_mode": mode,
            "leverage": leverage,
            "budget_margin": str(budget),
        }
        if mode == "isolated":
            position["isolated_collateral"] = str(collateral)
        rows.append({"position": position, "strategy": {**strategy, "constraints": rules}})
    tables = raw["brackets"]
    if type(tables) is not list or len(tables) != len(rows):
        raise ValueError("Copy requires explicit matching manual single-tier tables")
    common = None
    seen = set()
    for table in tables:
        _object(table, {"instrument", "observed_at_ms", "available_at_ms", "rows"})
        key = table["instrument"]
        if type(key) is not str or key not in instruments or key in seen:
            raise ValueError("Bracket instrument mismatch")
        seen.add(key)
        stamp = _integer(table["observed_at_ms"], 0, 253402300799000)
        if type(table["available_at_ms"]) is not int or table["available_at_ms"] != stamp:
            raise ValueError(
                "Different observation/availability times cannot be copied by this form"
            )
        tiers = table["rows"]
        if type(tiers) is not list or len(tiers) != 1:
            raise ValueError("Multi-tier brackets are unsupported here; previous draft retained")
        tier = tiers[0]
        _object(
            tier,
            {
                "notional_floor",
                "notional_cap",
                "maintenance_rate",
                "maintenance_amount",
                "max_leverage",
            },
        )
        if number(tier["notional_floor"]) != 0 or number(tier["maintenance_amount"]) != 0:
            raise ValueError("This form requires zero floor/maintenance amount")
        cap = number(tier["notional_cap"], positive=True)
        rate = number(tier["maintenance_rate"])
        maximum = _integer(tier["max_leverage"], 1, 1000)
        if not 0 <= rate <= 1:
            raise ValueError("Invalid maintenance rate")
        value = str(cap), str(rate), maximum, stamp
        if common is not None and common != value:
            raise ValueError(
                "Per-instrument tier differences are unsupported; previous draft retained"
            )
        common = value
    if any(row["position"]["leverage"] > common[2] for row in rows):
        raise ValueError("Configured leverage exceeds the manual tier maximum")
    if not has_nn and "external_feature_wait_ms" in raw:
        raise ValueError("NN wait without represented NN inputs is unsupported")
    wait = validate_wait_ms(raw.get("external_feature_wait_ms", 15000)) if has_nn else 15000
    return {
        "source_id": source_id,
        "raw": raw,
        "rows": rows,
        "tier": common,
        "fee": str(fee),
        "has_nn": has_nn,
        "wait": wait,
    }


def new_parameters(source, *, new_id, balance, start_ms, known_ids, number, identity):
    identity(new_id)
    if new_id == source["source_id"] or new_id in known_ids:
        raise ValueError("Enter a distinct, unused new account ID")
    capital = number(balance)
    _integer(start_ms, 0, 253402300799000)
    if capital < 0 or start_ms % 60000 or source["tier"][3] > start_ms + 60000:
        raise ValueError("Check new capital, aligned UTC start and the original tier timestamp")
    positions = [deepcopy(row["position"]) for row in source["rows"]]
    with localcontext() as calculation:
        calculation.prec = 256
        if (
            sum((number(row["budget_margin"]) for row in positions), Decimal(0)) > capital
            or sum((number(row.get("isolated_collateral", "0")) for row in positions), Decimal(0))
            > capital
        ):
            raise ValueError("New starting capital does not cover copied allocations/collateral")
    result = {
        **deepcopy(source["raw"]),
        "account": {"account_id": new_id, "balance": str(capital), "positions": positions},
        "strategies": [deepcopy(row["strategy"]) for row in source["rows"]],
        "start_ms": start_ms,
        "fee_rate": source["fee"],
    }
    if (
        len(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False).encode())
        > 131072
    ):
        raise ValueError("Copied configuration exceeds core request capacity")
    return result


class MarginConfigurationCopy(QGroupBox):
    applyRequested = Signal()

    def __init__(self, parent=None):
        super().__init__("등록 설정 → 새 계정 초안 / Registered settings → new draft", parent)
        self.source, self.generation, self.source_id, self.source_epoch = None, 0, None, None
        self.requirements = {"funding": False, "cost": False, "nn": False}
        self.setCheckable(True)
        self.setChecked(False)
        outer = QVBoxLayout(self)
        self.body = QWidget()
        outer.addWidget(self.body)
        layout = QVBoxLayout(self.body)
        self.toggled.connect(self.body.setVisible)
        self.body.hide()
        self.note, self.provenance, self.status = QLabel(), QLabel(), QLabel()
        self.source_summary = QLabel()
        self.applied_provenance = QLabel()
        for label in (
            self.note,
            self.provenance,
            self.source_summary,
            self.applied_provenance,
            self.status,
        ):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
            layout.addWidget(label)
        self.note.setText(
            "선택 계정은 변경하지 않습니다. 새 ID·초기 자본·UTC 시작을 직접 지정합니다. "
            "잔고/포지션/주문/모델 상태/재개를 복사하지 않습니다. "
            "단일 동일 수동 브래킷만 지원하며 기존 비용/Funding의 절대 UTC 값은 유지합니다."
            " / Settings only: choose new ID/capital/UTC. "
            "No balances, orders, model state or resume are copied. "
            "Only identical manual single tiers are supported; scenario UTC stays fixed."
        )
        form = QFormLayout()
        form.setRowWrapPolicy(QFormLayout.RowWrapPolicy.WrapLongRows)
        self.new_id, self.capital = QLineEdit(), QLineEdit()
        self.start = QDateTimeEdit()
        self.start.setTimeZone(QTimeZone(b"UTC"))
        self.start.setDisplayFormat("yyyy-MM-dd HH:mm:ss 'UTC'")
        self.start.setCalendarPopup(True)
        stamp = QDateTime.currentDateTimeUtc().toMSecsSinceEpoch() // 60000 * 60000
        self.start.setDateTime(QDateTime.fromMSecsSinceEpoch(stamp, QTimeZone(b"UTC")))
        form.addRow("새 계정 ID / New ID", self.new_id)
        form.addRow("새 초기 자본 USDT / New capital", self.capital)
        form.addRow("새 시작 UTC / New start", self.start)
        layout.addLayout(form)
        self.explicit_start = QCheckBox(
            "새 자본과 시작 UTC를 직접 확인했습니다 / Confirm new capital/start"
        )
        self.apply_button = QPushButton("검증 후 새 초안에 적용 / Validate and apply new draft")
        self.apply_button.setEnabled(False)
        self.apply_button.clicked.connect(self.applyRequested)
        layout.addWidget(self.explicit_start)
        layout.addWidget(self.apply_button)
        self.consents = {}
        for key, text in (
            ("funding", "복사한 Funding 시나리오에 새로 동의 / Confirm copied funding scenario"),
            ("cost", "복사한 비용/지연 가정에 새로 동의 / Confirm copied cost/delay assumptions"),
            ("nn", "현 코어 NN 연결 재조회·선택 확인 / Confirm refreshed current-core NN bindings"),
        ):
            self.consents[key] = QCheckBox(text)
            self.consents[key].hide()
            layout.addWidget(self.consents[key])

    def invalidate(self, selected=None):
        self.generation += 1
        self.source, self.source_id, self.source_epoch = None, selected, None
        self.apply_button.setEnabled(False)
        self.provenance.clear()
        self.source_summary.clear()
        self.status.setText(
            "선택/연결 변경 · 조회 미확인 · 기존 초안 유지 / Selection changed; draft retained"
        )

    def begin(self, identity, epoch):
        self.invalidate(identity)
        self.source_epoch = epoch
        self.new_id.clear()
        self.capital.clear()
        self.explicit_start.setChecked(False)
        self.status.setText("설정 조회 중 · 기존 초안 유지 / Reading; current draft retained")
        return identity, self.generation

    def accept(self, source, checksum):
        self.source = source
        source["checksum"] = checksum
        self.provenance.setText(
            f"Source preview {source['source_id'][:32]} · SHA256 {checksum[:16]}…"
        )
        self.provenance.setToolTip(f"{source['source_id']} / {checksum}")
        raw, tier = source["raw"], source["tier"]
        instruments = ", ".join(
            row["strategy"]["instrument"].split(":")[-1][:24] for row in source["rows"][:3]
        )
        self.source_summary.setText(
            f"Source opening capital {raw['account']['balance']} USDT (not current wallet) · "
            f"strategies {len(source['rows'])}: {instruments} · fee {source['fee']}\n"
            f"Manual tier cap {tier[0]} / rate {tier[1]} / max leverage {tier[2]} · "
            f"Funding {'funding' in raw} / cost {'cost_policy' in raw} / NN {source['has_nn']}"
        )
        self.status.setText(
            "조회 확인 · 새 ID/자본/UTC 지정 후 적용하세요 / Enter new ID/capital/start"
        )
        self.apply_button.setEnabled(True)

    def reset_consents(self, source):
        self.applied_provenance.setText(
            f"Applied draft source {source['source_id'][:32]} / {source['checksum'][:16]}…"
        )
        self.requirements = {
            "funding": "funding" in source["raw"],
            "cost": "cost_policy" in source["raw"],
            "nn": source["has_nn"],
        }
        for key, control in self.consents.items():
            control.setChecked(False)
            control.setVisible(self.requirements[key])

    def consent_ready(self, enabled=None):
        return all(
            not required
            or enabled is not None
            and not enabled[key]
            or self.consents[key].isChecked()
            for key, required in self.requirements.items()
        )
