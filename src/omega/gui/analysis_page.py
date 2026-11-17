"""CAC → instrument CA configuration library, accessed only through the core API."""

# Imports
import json
import math
import re
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import quote

from PySide6.QtCore import QSignalBlocker, Qt
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
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

# Text and display interval IDs; installed capabilities are confirmed by the core.
_TEXT = {
    "title": ("분석 설정 · CAC / CA", "ANALYSIS CONFIGURATIONS · CAC / CA"),
    "notice": (
        "CAC를 저장한 뒤 종목별 CA를 생성하세요. 전략은 선택 당시 설정을 복사합니다. "
        "저장 완료는 실행 분석 준비 완료가 아닙니다. 차트의 화면용 EMA와 별개입니다.",
        "Save a CAC, then create an instrument CA. Strategies bind a frozen snapshot. "
        "Saved does not mean analyzer-ready; this is separate from chart display EMA.",
    ),
    "templates": ("CAC 템플릿", "CAC templates"),
    "instances": ("종목별 CA", "Instrument CAs"),
    "refresh": ("목록 갱신", "Refresh library"),
    "resolve": ("불확실 요청 확인", "Check uncertain request"),
    "new": ("새 설정", "New"),
    "save": ("저장", "Save"),
    "clone": ("다른 이름으로 복제", "Clone with name"),
    "rename": ("이름 변경", "Rename"),
    "delete": ("삭제", "Delete"),
    "name": ("이름 / 코드", "Name / code"),
    "interval": ("시간봉", "Interval"),
    "kind": ("분석 지표", "Indicator"),
    "samples": ("기간 / nSamples", "Period / nSamples"),
    "periods": ("MMACD 기간 목록", "MMACD periods"),
    "signal": ("MMACD signal 기간", "MMACD signal period"),
    "precision": ("가격 소수 자릿수", "Price precision"),
    "line": ("선 번호 / line index", "Line index"),
    "psar_start": ("PSAR AF0 · 초기 가속 계수", "PSAR AF0 · initial acceleration"),
    "psar_acceleration": ("PSAR AF+ · 가속 증가량", "PSAR AF+ · acceleration increment"),
    "psar_maximum": ("PSAR AFmax · 최대 가속 계수", "PSAR AFmax · maximum acceleration"),
    "swing_range": (
        "SWING 반전 범위 (%) · 0.5%=비율0.005",
        "SWING reversal (%) · 0.5%=ratio 0.005",
    ),
    "add": ("지표 추가", "Add indicator"),
    "apply": ("선택 지표 수정", "Update indicator"),
    "remove": ("선택 지표 삭제", "Remove indicator"),
    "advanced": ("고급 JSON · 보조 입력", "Advanced JSON · supplementary input"),
    "apply_json": ("JSON을 구조화 폼으로 가져오기", "Load JSON into structured form"),
    "market": ("시장", "Market"),
    "symbol": ("종목", "Symbol"),
    "template": ("원본 CAC", "Source CAC"),
    "create": ("종목 CA 생성", "Create instrument CA"),
    "rebase": ("선택 CAC 최신판을 명시 적용", "Explicitly copy selected CAC revision"),
    "configured": (
        "설정됨 · 자동 실행/구독 아님",
        "Configured · not automatically running/subscribed",
    ),
    "rebase_note": (
        "CAC 수정은 기존 CA/전략에 자동 적용되지 않습니다. "
        "명시 적용도 기존 전략의 복사본을 바꾸지 않습니다.",
        "CAC edits never update existing CAs/strategies automatically. "
        "Explicit copying also leaves bound strategy snapshots unchanged.",
    ),
    "unsupported": (
        "EMA / MMACD / SMA / WMA / BOL / MFI / DMIxADX / SWING / PSAR / "
        "NES / WOI / MST / STS / IVP / TPD "
        "설정을 지원합니다. "
        "NES·WOI·MST·STS에는 보조 자료가 필요하며 현재 CA는 보조 채널을 자동 구독하지 않습니다. "
        "IVP는 해석된 OHLCV 프로파일이며 NumPy/SciPy 선택 설치가 필요합니다. "
        "SMA/WMA/BOL은 저장 분석 설정이며 MAIN 차트 표시 연결은 별도입니다. "
        "주·월 분석은 미지원입니다.",
        "EMA / MMACD / SMA / WMA / BOL / MFI / DMIxADX / SWING / PSAR / "
        "NES / WOI / MST / STS / IVP / TPD "
        "are configurable. "
        "NES/WOI/MST/STS need auxiliary data; current CAs do not subscribe automatically. "
        "IVP is an interpreted OHLCV profile and requires optional NumPy/SciPy. "
        "SMA/WMA/BOL are saved analysis definitions; MAIN chart rendering is separate. "
        "Calendar analysis is unsupported.",
    ),
    "auxiliary_unavailable": (
        "NES·WOI·MST·STS에는 보조 자료가 필요하며 현재 CA는 보조 채널을 자동 구독하지 않습니다.",
        "NES/WOI/MST/STS need auxiliary data; current CAs do not subscribe automatically.",
    ),
    "auxiliary_available": (
        "NES/MST는 저장된 DB 보조 관측을 자동 연결할 수 있습니다. "
        "실제 자료/허용 나이/품질이 준비돼야 합니다. "
        "depth가 필요한 WOI/STS 수집과 실거래 전략 자동 구독은 아직 미지원입니다.",
        "NES/MST can connect committed DB auxiliary observations automatically, "
        "subject to available data, age and quality. Depth collection for WOI/STS "
        "and automatic live-trading strategy subscriptions remain unsupported.",
    ),
    "loading": ("코어에서 목록 조회 중", "Reading library from core"),
    "loaded": ("코어 목록 갱신 완료", "Core library refreshed"),
    "disconnected": (
        "연결 끊김 · 이전 목록 / 편집 내용 유지",
        "Disconnected · retaining last library and edits",
    ),
    "unsaved": ("편집 중 · 아직 저장하지 않음", "Editing · not saved"),
    "uncertain": (
        "결과 불확실 · 재전송하지 않고 요청 상태를 확인하세요.",
        "Outcome uncertain · check operation status; do not resend.",
    ),
    "saved": ("코어 저장 확인", "Core confirmed saved"),
    "select": (
        "왼쪽 목록에서 선택하거나 새 설정을 만드세요.",
        "Select a library item or create a new one.",
    ),
}
_INTERVALS = (
    ("1m", 0),
    ("3m", 1),
    ("5m", 2),
    ("15m", 3),
    ("30m", 4),
    ("1h", 5),
    ("2h", 6),
    ("4h", 7),
    ("6h", 8),
    ("8h", 9),
    ("12h", 10),
    ("1d", 11),
    ("3d", 12),
)
_MARKET_GROUPS = {
    "NES": "order_flow",
    "WOI": "order_flow",
    "MST": "market_sentiment",
    "STS": "short_term_scalar",
    "IVP": "volume_profile",
    "TPD": "temporal_probability",
    "SMA": "window_averages",
    "WMA": "window_averages",
    "BOL": "bollinger",
}
_MARKET_ENUMS = {"BOL": {"MAType": ("SMA", "WMA", "EMA")}}
# Explicit display contracts mirror installed producer bounds without importing analysis code.
_MARKET_FIELDS = {
    **{
        kind: {
            "nSamples": (10, 2, 20000, int),
            "line_index": (0, 0, 9, int),
            "price_precision": (8, 0, 16, int),
        }
        for kind in ("SMA", "WMA")
    },
    "BOL": {
        "nSamples": (10, 2, 20000, int),
        "MAType": ("SMA", None, None, str),
        "bandWidth": (2.0, None, None, float),
        "line_index": (0, 0, 9, int),
        "price_precision": (8, 0, 16, int),
    },
    "TPD": {
        "viewLength": (10, 2, 20000, int),
        "nSamples": (100, 2, 20000, int),
        "nSamplesMA": (20, 2, 20000, int),
        "line_index": (0, 0, 9, int),
    },
    "NES": {
        "nSamples": (14, 2, 100000, int),
        "line_index": (0, 0, 9, int),
        "quote_precision": (8, 0, 16, int),
    },
    "WOI": {
        "nSamples": (14, 2, 100000, int),
        "line_index": (0, 0, 9, int),
        "quote_precision": (8, 0, 16, int),
    },
    "MST": {
        "nSamples": (14, 2, 20000, int),
        "line_index": (0, 0, 9, int),
        "quote_precision": (8, 0, 16, int),
        "maximum_metric_age_ms": (330000, 0, 86400000, int),
    },
    "STS": {
        **{
            name: (default, 2, 20000, int)
            for name, default in (
                ("MMACDFast", 10),
                ("MMACDSlow", 20),
                ("MMACDSignal", 9),
                ("DMIxADXNSamples", 20),
                ("MFINSamples", 20),
                ("WOINSamples", 5),
                ("WOIBaseNSamples", 240),
            )
        },
        **{
            name: (default, 0, None, float)
            for name, default in (
                ("OSCScale", 1.0),
                ("WMMACD", 1.0),
                ("WMFI", 1.0),
                ("WDMIxADX", 0.5),
                ("WWOI", 0.5),
            )
        },
        "price_precision": (8, 0, 16, int),
        "quote_precision": (8, 0, 16, int),
    },
    "IVP": {
        "nSamples": (500, 2, 20000, int),
        "gammaFactor": (0.01, 0.005, 0.1, float),
        "deltaFactor": (1.0, 0.1, 10, float),
        "prominence": (0.1, 0.01, 1, float),
        "distance": (5, 1, 100, int),
        "height": (0.5, 0, 1, float),
        "price_precision": (8, 0, 16, int),
        "max_divisions": (8192, 2, 32768, int),
    },
}


def _market_row(kind, row):
    specs = _MARKET_FIELDS[kind]
    if not isinstance(row, dict) or set(row) - (set(specs) | {"interval_id"}):
        raise ValueError("Unsupported market indicator parameters")
    interval = row.get("interval_id")
    if type(interval) is not int or not 0 <= interval <= 11:
        raise ValueError("Market indicators require interval 0..11")
    result = {"interval_id": interval}
    for name, (default, lower, upper, dtype) in specs.items():
        value = row.get(name, default)
        if dtype is str:
            choices = _MARKET_ENUMS.get(kind, {}).get(name, ())
            if type(value) is not str or value not in choices:
                raise ValueError(f"{name} requires one of {', '.join(choices)}")
            result[name] = value
            continue
        elif dtype is int:
            if type(value) is not int:
                raise ValueError(f"{name} requires an integer")
        else:
            if (
                isinstance(value, bool)
                or not isinstance(value, (str, int, float, Decimal))
                or len(str(value)) > 80
            ):
                raise ValueError(f"{name} requires a finite number")
            try:
                value = float(value)
            except (ValueError, OverflowError):
                raise ValueError(f"{name} requires a finite number") from None
            if not math.isfinite(value):
                raise ValueError(f"{name} requires a finite number")
        if lower is not None and value < lower or upper is not None and value > upper:
            raise ValueError(f"{name} is outside the installed indicator bounds")
        result[name] = value
    if kind == "STS" and (
        result["MMACDFast"] >= result["MMACDSlow"]
        or result["OSCScale"] <= 0
        or not math.isfinite(sum(result[name] for name in ("WMMACD", "WMFI", "WDMIxADX", "WWOI")))
        or not sum(result[name] for name in ("WMMACD", "WMFI", "WDMIxADX", "WWOI")) > 0
    ):
        raise ValueError("STS needs Fast < Slow, positive scale and at least one positive weight")
    return result


def _spin(lower, upper, value):
    widget = QSpinBox()
    widget.setRange(lower, upper)
    widget.setValue(value)
    return widget


def _scroll(content):
    widget = QScrollArea()
    widget.setWidgetResizable(True)
    widget.setWidget(content)
    return widget


def _swing_value(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal)):
        raise ValueError("Invalid SWING ratio")
    if len(str(value)) > 512:
        raise ValueError("SWING ratio exceeds the editable precision limit")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        raise ValueError("Invalid SWING ratio") from None
    if not result.is_finite() or result < Decimal("0.0001") or result.adjusted() > 307:
        raise ValueError("SWING ratio must be finite and >=0.0001 (0.01%)")
    return result


def _decimal_shift(value, places):
    """Power-of-ten UI conversion without the process Decimal rounding context."""
    parts = value.as_tuple()
    return Decimal((parts.sign, parts.digits, parts.exponent + places))


def _parabolic_row(row):
    if set(row) - {
        "interval_id",
        "line_index",
        "start",
        "acceleration",
        "maximum",
        "price_precision",
    }:
        raise ValueError("Unsupported PSAR parameters")
    intervalID, line, precision = (
        row.get("interval_id"),
        row.get("line_index", 0),
        row.get("price_precision", 8),
    )
    if (
        type(intervalID) is not int
        or not 0 <= intervalID <= 11
        or type(line) is not int
        or not 0 <= line <= 9
        or type(precision) is not int
        or not 0 <= precision <= 16
    ):
        raise ValueError("PSAR requires interval 0..11, line 0..9, price precision 0..16")
    values = {
        key: row.get(key, default)
        for key, default in (("start", 0.02), ("acceleration", 0.005), ("maximum", 0.2))
    }
    try:
        if any(
            isinstance(value, bool) or not isinstance(value, (str, int, float, Decimal))
            for value in values.values()
        ):
            raise ValueError
        values = {key: float(value) for key, value in values.items()}
        finite = all(math.isfinite(value) for value in values.values())
    except (ValueError, TypeError, OverflowError):
        finite = False
    if not finite:
        raise ValueError("PSAR AF values must be finite numbers")
    if not 0 < values["start"] < values["maximum"] or values["acceleration"] <= 0:
        raise ValueError("PSAR requires 0 < AF0 < AFmax and AF+ > 0")
    return {"interval_id": intervalID, "line_index": line, "price_precision": precision, **values}


def form_configuration(definitions: list[dict]) -> dict:
    """Translate explicit rows into JSON; arithmetic validation stays in the core."""
    moving, momentum, swing, parabolic, outputs = {}, [], [], [], set()
    market = {}
    for definition in definitions:
        row, kind = dict(definition), definition["kind"]
        row.pop("kind")
        intervalID = row["interval_id"]
        identity = (intervalID, kind, row.get("line_index", 0))
        if identity in outputs:
            raise ValueError("Duplicate indicator/interval/line")
        outputs.add(identity)
        if kind in {"EMA", "MMACD"}:
            entry = moving.setdefault(
                intervalID, {"interval_id": intervalID, "price_precision": row["price_precision"]}
            )
            if entry["price_precision"] != row["price_precision"]:
                raise ValueError("EMA/MMACD sharing an interval must use the same price precision")
            entry.update({key: value for key, value in row.items() if key != "interval_id"})
        elif kind in {"MFI", "DMIxADX"}:
            momentum.append(row | {"analysis_code": kind})
        elif kind == "SWING":
            _swing_value(row.get("swing_range"))
            if (
                type(intervalID) is not int
                or not 0 <= intervalID <= 11
                or type(row.get("line_index")) is not int
                or not 0 <= row["line_index"] <= 9
            ):
                raise ValueError("SWING needs an installed interval 0..11 and line 0..9")
            swing.append(row)
        elif kind == "PSAR":
            parabolic.append(_parabolic_row(row))
        elif kind in _MARKET_GROUPS:
            encoded = _market_row(kind, row)
            if kind in {"NES", "WOI", "SMA", "WMA"}:
                encoded["analysis_code"] = kind
            market.setdefault(_MARKET_GROUPS[kind], []).append(encoded)
        else:
            raise ValueError("Unsupported analysis indicator")
    result = {"moving_averages": list(moving.values()), "momentum": momentum}
    if swing:
        result["swing"] = swing
    if parabolic:
        result["parabolic"] = parabolic
    result.update(market)
    price_rows = [*market.get("window_averages", []), *market.get("bollinger", [])]
    if len(price_rows) > 64 or sum(row["nSamples"] + 3 for row in price_rows) > 100000:
        raise ValueError("Price windows require at most 64 definitions and 100000 retained samples")
    return result


def configuration_rows(configuration: dict) -> list[dict]:
    """Unknown data remains an error rather than disappearing during form loading."""
    if not isinstance(configuration, dict) or set(configuration) - {
        "moving_averages",
        "momentum",
        "swing",
        "parabolic",
        *_MARKET_GROUPS.values(),
    }:
        raise ValueError("Unsupported analysis configuration fields")
    result = []

    def integer(value, lower, upper, label):
        if type(value) is not int or not lower <= value <= upper:
            raise ValueError(f"Invalid {label}; expected integer {lower}–{upper}")
        return value

    for group in ("moving_averages", "momentum", "swing", "parabolic"):
        if (
            not isinstance(configuration.get(group, []), list)
            or len(configuration.get(group, [])) > 64
        ):
            raise ValueError("Analysis definitions must be a bounded list")
    for row in configuration.get("moving_averages", []):
        if not isinstance(row, dict) or set(row) - {
            "interval_id",
            "ema_period",
            "mmacd_periods",
            "signal_period",
            "price_precision",
        }:
            raise ValueError("Unsupported moving-average parameters")
        common = {
            "interval_id": integer(row["interval_id"], 0, 12, "interval_id"),
            "price_precision": integer(row.get("price_precision", 8), 0, 16, "price_precision"),
        }
        signal_period = integer(row.get("signal_period", 9), 2, 100000, "signal_period")
        periods = row.get("mmacd_periods", [])
        if not isinstance(periods, list):
            raise ValueError("MMACD periods must be a JSON list")
        if row.get("ema_period") is not None:
            result.append(
                common
                | {"kind": "EMA", "ema_period": integer(row["ema_period"], 2, 100000, "EMA period")}
            )
        if row.get("mmacd_periods"):
            if (
                not 2 <= len(periods) <= 10
                or any(type(period) is not int or not 2 <= period <= 100000 for period in periods)
                or len(set(periods)) != len(periods)
            ):
                raise ValueError("Invalid MMACD period list")
            result.append(
                common
                | {
                    "kind": "MMACD",
                    "mmacd_periods": row["mmacd_periods"],
                    "signal_period": signal_period,
                }
            )
        if not row.get("mmacd_periods") and row.get("ema_period") is None:
            raise ValueError("Moving average has neither EMA nor MMACD")
    for row in configuration.get("momentum", []):
        if (
            not isinstance(row, dict)
            or set(row) - {"interval_id", "analysis_code", "nSamples", "line_index"}
            or row.get("analysis_code") not in {"MFI", "DMIxADX"}
        ):
            raise ValueError("Unsupported momentum parameters")
        result.append(
            {
                "kind": row["analysis_code"],
                "interval_id": integer(row["interval_id"], 0, 12, "interval_id"),
                "nSamples": integer(row.get("nSamples", 14), 2, 100000, "nSamples"),
                "line_index": integer(row.get("line_index", 0), 0, 9, "line_index"),
            }
        )
    for row in configuration.get("swing", []):
        if not isinstance(row, dict) or set(row) - {"interval_id", "line_index", "swing_range"}:
            raise ValueError("Unsupported SWING parameters")
        value = row.get("swing_range", 0.005)
        _swing_value(value)
        result.append(
            {
                "kind": "SWING",
                "interval_id": integer(row["interval_id"], 0, 11, "interval_id"),
                "line_index": integer(row.get("line_index", 0), 0, 9, "line_index"),
                "swing_range": value,
            }
        )
    for row in configuration.get("parabolic", []):
        if not isinstance(row, dict):
            raise ValueError("PSAR definition must be an object")
        result.append({"kind": "PSAR", **_parabolic_row(row)})
    for group in dict.fromkeys(_MARKET_GROUPS.values()):
        rows = configuration.get(group, [])
        if not isinstance(rows, list) or len(rows) > 64:
            raise ValueError("Market indicator definitions must be a bounded list")
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError("Market indicator definition must be an object")
            kind = (
                row.get("analysis_code")
                if group in {"order_flow", "window_averages"}
                else next(code for code, name in _MARKET_GROUPS.items() if name == group)
            )
            if (
                not isinstance(kind, str)
                or kind not in _MARKET_GROUPS
                or _MARKET_GROUPS[kind] != group
            ):
                raise ValueError("Unsupported grouped analysis indicator")
            fields = {
                key: value
                for key, value in row.items()
                if key != "analysis_code" or group not in {"order_flow", "window_averages"}
            }
            result.append({"kind": kind, **_market_row(kind, fields)})
    if any(
        row["interval_id"] not in dict((iid, label) for label, iid in _INTERVALS) for row in result
    ):
        raise ValueError("Unsupported analysis interval")
    return result


class AnalysisExecutionControls(QGroupBox):
    """Read-only execution results and explicit commands, separate from CAC drafts."""

    _states = {"queued", "preparing", "running", "blocked", "stopped", "error"}
    _active = {"queued", "preparing", "running", "blocked"}

    def __init__(self, owner):
        super().__init__()
        self.owner, self.client = owner, owner.client
        self.target, self.row = None, None
        self._selection = self._version = self._serial = 0
        self._core_epoch = None
        self._request = self._request_context = None
        self._pending = self._uncertain = self._command_context = None
        self._command_execution_id = None
        self._supported = None
        self.setCheckable(True)
        outer = QVBoxLayout(self)
        self.content = QWidget()
        layout = QVBoxLayout(self.content)
        actions = QHBoxLayout()
        self.start_button, self.stop_button, self.refresh_button, self.resolve_button = (
            QPushButton() for _ in range(4)
        )
        for button in (
            self.start_button,
            self.stop_button,
            self.refresh_button,
            self.resolve_button,
        ):
            actions.addWidget(button)
        layout.addLayout(actions)
        setup = QHBoxLayout()
        self.warmup_label = QLabel()
        self.warmup = _spin(1, 525600, 240)
        self.restart = QCheckBox()
        setup.addWidget(self.warmup_label)
        setup.addWidget(self.warmup)
        setup.addWidget(self.restart)
        setup.addStretch()
        layout.addLayout(setup)
        self.summary, self.quality, self.message = QLabel(), QLabel(), QLabel()
        for widget in (self.summary, self.quality, self.message):
            widget.setWordWrap(True)
            widget.setTextFormat(Qt.TextFormat.PlainText)
            layout.addWidget(widget)
        self.outputs = QTableWidget(0, 2)
        self.outputs.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.outputs.horizontalHeader().setStretchLastSection(True)
        self.outputs.setMinimumHeight(160)
        self.outputs.setMaximumHeight(300)
        layout.addWidget(self.outputs)
        outer.addWidget(self.content)
        self.start_button.clicked.connect(self.start)
        self.stop_button.clicked.connect(self.stop)
        self.refresh_button.clicked.connect(self.refresh)
        self.resolve_button.clicked.connect(self.resolve)
        self.restart.toggled.connect(self._controls)
        self.toggled.connect(self.content.setVisible)
        self.toggled.connect(lambda checked: self.refresh() if checked else None)
        self.client.resultReceived.connect(self._result)
        self.client.failed.connect(self._failed)
        self.client.connectionChanged.connect(self._connection)
        self.setChecked(False)
        self.content.hide()
        self.set_presentation()

    def _text(self, ko, en):
        return ko if self.owner.language == "ko" else en

    def _context(self):
        return (
            self.target["id"] if self.target else "",
            self.target["instrument"] if self.target else "",
            self._selection,
            self._core_epoch,
        )

    def select(self, target):
        before = (self.target["id"], self.target["instrument"]) if self.target else None
        after = (target["id"], target["instrument"]) if target else None
        if before != after:
            self._selection += 1
            self._request = self._request_context = None
            self.row = None
            self.message.clear()
        self.target = deepcopy(target)
        self._render()
        if before != after and target is not None:
            self.refresh()

    def update_state(self, snapshot):
        core = snapshot.get("core", {})
        epoch = core.get("epoch")
        if isinstance(epoch, str) and epoch:
            if self._core_epoch not in (None, epoch):
                self.row = None
                self._version += 1
                self._request = self._request_context = None
                if self._pending:
                    self._uncertain, self._pending = self._pending, None
                self.owner._execution_states = {
                    key: "unknown" for key in self.owner._execution_states
                }
                self.owner._filter_instances()
            self._core_epoch = epoch
        supported = core.get("capabilities", {}).get("analysis_execution")
        if type(supported) is bool:
            self._supported = supported
        self._render()
        if self.isChecked():
            self.refresh()

    def refresh(self):
        if not self.client.attached or not self.target or self._request:
            return
        self._serial += 1
        self._request = f"ca-execution:{id(self)}:{self._serial}"
        self._request_context = self._context(), self._version
        self.client.request(
            "GET",
            "/api/analysis-execution?analysis_id=" + quote(self.target["id"], safe=""),
            tag=self._request,
        )

    def _controls(self):
        available = bool(
            self.client.attached
            and self.target
            and self._core_epoch
            and self._supported is not False
            and not self._pending
            and not self._uncertain
        )
        active = self.row is not None and self.row["state"] in self._active
        changed = (
            self.row is not None
            and self.target is not None
            and self.row["analysis_revision"] != self.target["revision"]
        )
        self.start_button.setEnabled(
            available and (not active and not changed or self.restart.isChecked())
        )
        self.stop_button.setEnabled(available and active)
        self.refresh_button.setEnabled(bool(self.client.attached and self.target))
        self.resolve_button.setEnabled(bool(self.client.attached and self._uncertain))

    def start(self):
        self._controls()
        if not self.start_button.isEnabled():
            return
        self._command(
            "analysis.execution.start",
            {
                "analysis_id": self.target["id"],
                "expected_revision": self.target["revision"],
                "warmup_minutes": self.warmup.value(),
                "restart": self.restart.isChecked(),
            },
        )

    def stop(self):
        self._controls()
        if not self.stop_button.isEnabled():
            return
        self._command(
            "analysis.execution.stop",
            {
                "analysis_id": self.target["id"],
                "execution_id": self.row["execution_id"],
            },
        )

    def _command(self, kind, payload):
        self._command_context = self._context()
        self._command_execution_id = payload.get("execution_id")
        self._pending = self.client.command(kind, payload)
        self.message.setText(self._text("코어 응답 대기", "Awaiting core response"))
        self._controls()

    def resolve(self):
        if self.client.attached and self._uncertain:
            self.client.request(
                "GET",
                "/api/operations/" + self._uncertain,
                tag=f"ca-execution-operation:{id(self)}:{self._uncertain}",
            )

    @classmethod
    def _validate_row(cls, row, identity, instrument):
        if (
            not isinstance(row, dict)
            or row.get("analysis_id") != identity
            or row.get("instrument") != instrument
            or not isinstance(row.get("execution_id"), str)
            or not re.fullmatch(r"[a-f0-9]{32}", row["execution_id"])
            or row.get("state") not in cls._states
            or type(row.get("ready")) is not bool
            or type(row.get("analysis_revision")) is not int
            or row["analysis_revision"] < 1
            or not isinstance(row.get("configuration_hash"), str)
            or not re.fullmatch(r"[a-f0-9]{64}", row["configuration_hash"])
            or not isinstance(row.get("reason"), str)
            or len(row["reason"]) > 512
        ):
            raise ValueError("Invalid analysis execution identity/state")
        last = row.get("last_closed_ms")
        if last is not None and (
            type(last) is not int or not 0 <= last <= 253402300799000 or last % 60000
        ):
            raise ValueError("Invalid closed analysis timestamp")
        if row["ready"] and (row["state"] != "running" or last is None):
            raise ValueError("Inconsistent analysis readiness")
        outputs = row.get("outputs")
        if not isinstance(outputs, dict) or len(outputs) > 2048:
            raise ValueError("Analysis outputs exceed the display bound")
        for key, value in outputs.items():
            if (
                not isinstance(key, str)
                or len(key) > 160
                or value is not None
                and (
                    type(value) not in {str, int, float, bool}
                    or type(value) is float
                    and not math.isfinite(value)
                    or type(value) is str
                    and len(value) > 512
                )
            ):
                raise ValueError("Invalid analysis output value")
        for key in ("warmup", "input_quality"):
            if (
                not isinstance(row.get(key), dict)
                or len(json.dumps(row[key], allow_nan=False)) > 32768
            ):
                raise ValueError("Invalid or excessive analysis quality")
        for key in ("processed_minutes", "total_minutes"):
            if key in row["warmup"] and (
                type(row["warmup"][key]) is not int or row["warmup"][key] < 0
            ):
                raise ValueError("Invalid analysis warmup count")
        if row["warmup"].get("processed_minutes", 0) > row["warmup"].get("total_minutes", 525600):
            raise ValueError("Inconsistent analysis warmup progress")
        return deepcopy(row)

    def _result(self, tag, result):
        if tag == self._request:
            context = self._request_context
            self._request = self._request_context = None
            if context is None or context != (self._context(), self._version):
                return
            try:
                epoch, rows = result.get("core_epoch"), result.get("instances")
                if (
                    type(result.get("schema_version")) is not int
                    or result["schema_version"] != 1
                    or not isinstance(epoch, str)
                    or not epoch
                    or self._core_epoch not in (None, epoch)
                    or not isinstance(rows, list)
                    or len(rows) > 1
                    or not isinstance(result.get("load_errors", []), list)
                ):
                    raise ValueError("Invalid analysis execution response/epoch")
                self._core_epoch = epoch
                self.row = self._validate_row(rows[0], *self._context()[:2]) if rows else None
                if not self._uncertain:
                    self.message.setText(
                        str(result.get("load_errors", []))[:1000]
                        if result.get("load_errors")
                        else ""
                    )
            except (ValueError, TypeError, KeyError) as error:
                self.message.setText(str(error)[:300])
            self._render()
            return
        identity = self._pending or self._uncertain
        if not identity or tag not in {
            "command:" + identity,
            f"ca-execution-operation:{id(self)}:{identity}",
        }:
            return
        status, outcome = result.get("status"), result.get("result")
        if (
            status == "completed"
            and isinstance(outcome, dict)
            and outcome.get("status") == "rejected"
        ):
            status = "rejected"
        if status == "completed":
            try:
                row = self._validate_row(outcome, *self._command_context[:2])
                if (
                    self._command_execution_id is not None
                    and row["execution_id"] != self._command_execution_id
                ):
                    raise ValueError("Analysis stop response changed execution identity")
            except (ValueError, TypeError, KeyError):
                self._uncertain, self._pending = identity, None
                self.message.setText(
                    self._text(
                        "결과 불확실 · 요청 상태를 확인하세요",
                        "Outcome uncertain · check operation status",
                    )
                )
                self._controls()
                return
            self._pending = self._uncertain = None
            self._version += 1
            self._request = self._request_context = None
            if self._command_context == self._context():
                self.row = row
            self.message.setText(
                self._text("코어 실행 상태 확인", "Core confirmed execution status")
            )
            self._render()
            self.refresh()
        elif status == "rejected":
            self._pending = self._uncertain = None
            self.message.setText(str(result.get("code", "Rejected"))[:300])
            self._controls()
        else:
            self._uncertain, self._pending = identity, None
            self.message.setText(
                self._text(
                    "결과 불확실 · 요청 상태를 확인하세요",
                    "Outcome uncertain · check operation status",
                )
            )
            self._controls()

    def _failed(self, tag, message):
        identity = self._pending or self._uncertain
        if tag == self._request:
            self._request = self._request_context = None
            self.message.setText(str(message)[:300])
        elif identity and tag in {
            "command:" + identity,
            f"ca-execution-operation:{id(self)}:{identity}",
        }:
            self._uncertain, self._pending = identity, None
            self._version += 1
            self._request = self._request_context = None
            self.message.setText(
                self._text(
                    "결과 불확실 · 요청 상태를 확인하세요",
                    "Outcome uncertain · check operation status",
                )
            )
        self._controls()

    def _connection(self, attached):
        if not attached:
            self._request = self._request_context = None
            if self._pending:
                self._uncertain, self._pending = self._pending, None
            self.message.setText(
                self._text("연결 끊김 · 마지막 조회 결과", "Disconnected · last queried results")
            )
        self._controls()

    def _render(self):
        if self.row is not None:
            self.owner._set_execution_state(self.row["analysis_id"], self.row["state"])
        if not self.target:
            self.summary.setText(self._text("저장된 CA를 선택하세요", "Select a saved CA"))
        elif self.row is None:
            self.summary.setText(
                self._text(
                    "실행 기록 없음 · 상태 조회 후 시작",
                    "No execution record · query status then start",
                )
            )
        else:
            row, warmup = self.row, self.row["warmup"]
            timestamp = (
                "—"
                if row["last_closed_ms"] is None
                else datetime.fromtimestamp(row["last_closed_ms"] / 1000, UTC).strftime(
                    "%Y-%m-%d %H:%M UTC"
                )
            )
            ready = (
                self._text("분석값 준비 완료", "Outputs ready")
                if row["ready"]
                else self._text("분석값 준비 부족", "Outputs not ready")
            )
            self.summary.setText(
                f"{row['state']} · {ready} · {row['reason']}\n"
                f"CA r{self.target['revision']} / snapshot r{row['analysis_revision']} · "
                f"{timestamp}\n"
                f"{warmup.get('processed_minutes', '?')} / {warmup.get('total_minutes', '?')} min"
            )
            self.summary.setToolTip(row["execution_id"] + "\n" + row["configuration_hash"])
        if self.row is None:
            self.summary.setToolTip("")
        if self.row:
            quality = self.row["input_quality"]
            source = quality.get("source", self._text("미확인", "unknown"))
            if source == "committed_ohlcv":
                source = self._text("DB 확정 OHLCV", "DB closed OHLCV")
            elif source == "committed_frames":
                source = self._text(
                    "DB 확정 OHLCV + 고정 보조 입력", "DB closed OHLCV + pinned auxiliary"
                )
            labels = [str(source)]
            for field, ko, en in (
                ("fresh", "최신 자료", "Fresh data"),
                ("contiguous", "연속 자료", "Contiguous data"),
            ):
                if type(quality.get(field)) is bool:
                    labels.append(f"{self._text(ko, en)}: {quality[field]}")
            missing = quality.get("missing_outputs")
            if isinstance(missing, list):
                labels.append(
                    self._text(f"미준비 필드 {len(missing)}개", f"{len(missing)} unready fields")
                )
            if quality.get("auxiliary_subscription") is False:
                labels.append(self._text("보조 채널 구독 없음", "No auxiliary subscription"))
            elif quality.get("auxiliary_subscription") is True:
                labels.append(
                    self._text("DB 보조 관측 연결", "DB auxiliary observations connected")
                )
                if quality.get("knowledge_mode") == "warmup_retrospective_then_live":
                    labels.append(
                        self._text(
                            "과거 준비는 사후 관측 · 이후 실제 인지 시각 적용",
                            "Retrospective warmup; live uses actual knowledge cutoff",
                        )
                    )
            self.quality.setText(" · ".join(labels)[:2000])
            self.quality.setToolTip(json.dumps(quality, ensure_ascii=False)[:4000])
        else:
            self.quality.clear()
            self.quality.setToolTip("")
        values = sorted(self.row["outputs"].items())[:256] if self.row else []
        total = len(self.row["outputs"]) if self.row else 0
        field_title = self._text("분석 필드", "Analysis field")
        if total > 256:
            field_title += f" · 256/{total}"
        self.outputs.setHorizontalHeaderLabels([field_title, self._text("값", "Value")])
        self.outputs.setRowCount(len(values))
        for index, (key, value) in enumerate(values):
            shown = (
                self._text("미준비 / 관측 없음", "Not ready / unavailable")
                if value is None
                else str(value)
            )
            self.outputs.setItem(index, 0, QTableWidgetItem(key))
            self.outputs.setItem(index, 1, QTableWidgetItem(shown))
        self._controls()

    def set_presentation(self):
        self.setTitle(self._text("CA 독립 분석", "Independent CA execution"))
        for button, ko, en in (
            (self.start_button, "시작", "Start"),
            (self.stop_button, "중단", "Stop"),
            (self.refresh_button, "상태 갱신", "Refresh status"),
            (self.resolve_button, "불확실 요청 확인", "Check uncertain request"),
        ):
            button.setText(self._text(ko, en))
        self.restart.setText(
            self._text("새 고정 구간으로 재시작", "Restart with a new frozen range")
        )
        self.warmup_label.setText(self._text("준비 범위 (분)", "Warmup range (minutes)"))
        self.outputs.setHorizontalHeaderLabels(
            [self._text("분석 필드", "Analysis field"), self._text("값", "Value")]
        )
        self._render()


class AnalysisPage(QWidget):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language, self.theme = client, "ko", "light"
        self.library = {"revision": 0, "templates": [], "instances": []}
        self._selected_template = self._selected_instance = ""
        self._edit_revision = self._instance_edit_revision = 0
        self._definitions = []
        self._dirty = False
        self._instance_dirty = False
        self._draft_ids = {"templates": 0, "instances": 0}
        self._edit_serials = {"templates": 0, "instances": 0}
        self._submitted_context = None
        self._loading_form = False
        self._have_snapshot = False
        self._pending = None
        self._pending_kind = None
        self._reload_group = None
        self._uncertain = None
        self._epoch = None
        self._attached = bool(client.attached)
        self._execution_states = {}
        self._texts = []
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.title = QLabel()
        self.title.setProperty("role", "title")
        layout.addWidget(self.title)
        self.notice = QLabel()
        self.notice.setWordWrap(True)
        layout.addWidget(self.notice)
        controls = QHBoxLayout()
        self.refresh_button = self._button("refresh", self.refresh)
        self.resolve_button = self._button("resolve", self.resolve_operation)
        controls.addWidget(self.refresh_button)
        controls.addWidget(self.resolve_button)
        controls.addStretch()
        layout.addLayout(controls)
        self.status = QLabel()
        self.status.setWordWrap(True)
        self.status.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.status)
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs, 1)
        self._template_tab()
        self._instance_tab()
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        client.connectionChanged.connect(self.set_attached)
        self.new_template()
        self.set_presentation(self.theme, self.language)
        self.set_attached(client.attached)

    def tr_text(self, key):
        return _TEXT[key][0 if self.language == "ko" else 1]

    def _button(self, key, callback):
        button = QPushButton()
        button.clicked.connect(callback)
        self._texts.append((button, key))
        return button

    def _label(self, key):
        label = QLabel()
        label.setWordWrap(True)
        self._texts.append((label, key))
        return label

    def _template_tab(self):
        split = QSplitter()
        self.template_list = QListWidget()
        split.addWidget(self.template_list)
        content = QWidget()
        layout = QVBoxLayout(content)
        buttons = QHBoxLayout()
        self.template_actions = {}
        for key, callback in (
            ("new", self.new_template),
            ("save", self.save_template),
            ("clone", self.clone_template),
            ("rename", self.rename_template),
            ("delete", self.delete_template),
        ):
            button = self._button(key, callback)
            self.template_actions[key] = button
            buttons.addWidget(button)
        layout.addLayout(buttons)
        name_form = QFormLayout()
        self.template_name = QLineEdit()
        self.template_name.setMaxLength(80)
        name_form.addRow(self._label("name"), self.template_name)
        layout.addLayout(name_form)
        self.definition_table = QTableWidget(0, 3)
        self.definition_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.definition_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.definition_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.definition_table.horizontalHeader().setStretchLastSection(True)
        self.definition_table.setMinimumHeight(140)
        layout.addWidget(self.definition_table)
        editor = QFormLayout()
        self.interval = QComboBox()
        for label, intervalID in _INTERVALS:
            self.interval.addItem(label, intervalID)
        self.kind = QComboBox()
        self.kind.addItems(["EMA", "MMACD", "MFI", "DMIxADX", "SWING", "PSAR", *_MARKET_GROUPS])
        self.samples = _spin(2, 100000, 20)
        self.periods = QLineEdit("12, 26")
        self.periods.setMaxLength(80)
        self.signal = _spin(2, 100000, 9)
        self.precision = _spin(0, 16, 8)
        self.line = _spin(0, 9, 0)
        self.swing_range = QLineEdit("0.5")
        self.swing_range.setMaxLength(512)
        self.swing_range.setToolTip("Percent input; stored as ratio. 0.5% = 0.005. Minimum 0.01%.")
        self.psar_start, self.psar_acceleration, self.psar_maximum = (
            QLineEdit("0.02"),
            QLineEdit("0.005"),
            QLineEdit("0.2"),
        )
        for widget in (self.psar_start, self.psar_acceleration, self.psar_maximum):
            widget.setMaxLength(80)
            widget.setToolTip("Direct AF value. Require 0 < AF0 < AFmax, AF+ > 0.")
        for key, widget in (
            ("interval", self.interval),
            ("kind", self.kind),
            ("samples", self.samples),
            ("periods", self.periods),
            ("signal", self.signal),
            ("precision", self.precision),
            ("line", self.line),
            ("swing_range", self.swing_range),
            ("psar_start", self.psar_start),
            ("psar_acceleration", self.psar_acceleration),
            ("psar_maximum", self.psar_maximum),
        ):
            editor.addRow(self._label(key), widget)
        layout.addLayout(editor)
        self._definition_editor = editor
        self.market_details = QGroupBox("Market indicator parameters")
        self.market_details.setCheckable(True)
        self.market_details.setChecked(True)
        details_layout = QVBoxLayout(self.market_details)
        self.market_content = QWidget()
        content_layout = QVBoxLayout(self.market_content)
        self.market_forms, self.market_inputs = {}, {}
        shared = {
            "nSamples": self.samples,
            "line_index": self.line,
            "price_precision": self.precision,
        }
        for kind, fields in _MARKET_FIELDS.items():
            market_content, form = QWidget(), QFormLayout()
            market_content.setLayout(form)
            values = {}
            for name, (default, lower, upper, dtype) in fields.items():
                if name in shared:
                    values[name] = shared[name]
                    continue
                if dtype is str:
                    widget = QComboBox()
                    widget.addItems(_MARKET_ENUMS[kind][name])
                    widget.setCurrentText(default)
                else:
                    widget = (
                        _spin(lower, upper, default) if dtype is int else QLineEdit(str(default))
                    )
                if isinstance(widget, QLineEdit):
                    widget.setMaxLength(80)
                    if kind == "BOL" and name == "bandWidth":
                        widget.setToolTip(
                            "Finite signed width; zero is valid. Negative width inverts the named "
                            "LOW/HIGH bands. Saved analysis only; no MAIN chart overlay."
                        )
                form.addRow(name, widget)
                values[name] = widget
            content_layout.addWidget(market_content)
            self.market_forms[kind], self.market_inputs[kind] = market_content, values
        details_layout.addWidget(self.market_content)
        self.market_details.toggled.connect(self.market_content.setVisible)
        layout.addWidget(self.market_details)
        edit_buttons = QHBoxLayout()
        edit_buttons.addWidget(self._button("add", self.add_definition))
        edit_buttons.addWidget(self._button("apply", self.update_definition))
        edit_buttons.addWidget(self._button("remove", self.remove_definition))
        layout.addLayout(edit_buttons)
        self.support_hint = self._label("unsupported")
        layout.addWidget(self.support_hint)
        self.advanced = QGroupBox()
        self.advanced.setCheckable(True)
        self.advanced.setChecked(False)
        advanced_layout = QVBoxLayout(self.advanced)
        self.advanced_content = QWidget()
        inner = QVBoxLayout(self.advanced_content)
        self.json_input = QPlainTextEdit()
        self.json_input.setMaximumBlockCount(500)
        self.json_input.setMinimumHeight(140)
        inner.addWidget(self.json_input)
        inner.addWidget(self._button("apply_json", self.apply_json))
        advanced_layout.addWidget(self.advanced_content)
        self.advanced_content.hide()
        self.advanced.toggled.connect(self.advanced_content.setVisible)
        layout.addWidget(self.advanced)
        layout.addStretch()
        split.addWidget(_scroll(content))
        split.setSizes([240, 900])
        split.setChildrenCollapsible(False)
        self.tabs.addTab(split, "")
        self.template_list.currentItemChanged.connect(self._template_selected)
        self.definition_table.itemSelectionChanged.connect(self._definition_selected)
        self.template_name.textEdited.connect(self._mark_dirty)
        self.kind.currentTextChanged.connect(self._kind_changed)
        self._kind_changed()

    def _instance_tab(self):
        split = QSplitter()
        sidebar = QWidget()
        sidebar_layout = QVBoxLayout(sidebar)
        sidebar_layout.setContentsMargins(0, 0, 0, 0)
        self.instance_search = QLineEdit()
        self.instance_sort = QComboBox()
        for mode in ("name", "instrument", "state"):
            self.instance_sort.addItem(mode, mode)
        self.instance_list = QListWidget()
        sidebar_layout.addWidget(self.instance_search)
        sidebar_layout.addWidget(self.instance_sort)
        sidebar_layout.addWidget(self.instance_list)
        split.addWidget(sidebar)
        content = QWidget()
        layout = QVBoxLayout(content)
        actions = QHBoxLayout()
        self.instance_actions = {}
        for key, callback in (
            ("new", self.new_instance),
            ("create", self.create_instance),
            ("rename", self.rename_instance),
            ("rebase", self.rebase_instance),
            ("delete", self.delete_instance),
        ):
            button = self._button(key, callback)
            self.instance_actions[key] = button
            actions.addWidget(button)
        layout.addLayout(actions)
        form = QFormLayout()
        self.instance_name, self.symbol = QLineEdit(), QLineEdit("BTCUSDT")
        self.instance_name.setMaxLength(80)
        self.symbol.setMaxLength(40)
        self.market = QComboBox()
        self.market.addItem("Binance USDⓈ-M", "usdm")
        self.market.addItem("Binance Spot", "spot")
        self.template_choice = QComboBox()
        for key, widget in (
            ("name", self.instance_name),
            ("market", self.market),
            ("symbol", self.symbol),
            ("template", self.template_choice),
        ):
            form.addRow(self._label(key), widget)
        layout.addLayout(form)
        layout.addWidget(self._label("rebase_note"))
        self.instance_info = QLabel()
        self.instance_info.setWordWrap(True)
        self.instance_info.setTextFormat(Qt.TextFormat.PlainText)
        layout.addWidget(self.instance_info)
        self.instance_configuration = QPlainTextEdit()
        self.instance_configuration.setReadOnly(True)
        self.instance_parameters = QTableWidget(0, 3)
        self.instance_parameters.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.instance_parameters.horizontalHeader().setStretchLastSection(True)
        self.instance_parameters.setMinimumHeight(180)
        layout.addWidget(self.instance_parameters, 1)
        self.instance_advanced = QGroupBox()
        self.instance_advanced.setCheckable(True)
        self.instance_advanced.setChecked(False)
        advanced_layout = QVBoxLayout(self.instance_advanced)
        advanced_layout.addWidget(self.instance_configuration)
        self.instance_configuration.hide()
        self.instance_advanced.toggled.connect(self.instance_configuration.setVisible)
        layout.addWidget(self.instance_advanced)
        self.execution = AnalysisExecutionControls(self)
        layout.addWidget(self.execution)
        split.addWidget(_scroll(content))
        split.setSizes([240, 900])
        split.setChildrenCollapsible(False)
        self.tabs.addTab(split, "")
        self.instance_list.currentItemChanged.connect(self._instance_selected)
        self.instance_list.itemSelectionChanged.connect(self._sync_execution_selection)
        self.instance_search.textChanged.connect(self._filter_instances)
        self.instance_sort.currentIndexChanged.connect(self._filter_instances)
        self.instance_name.textEdited.connect(self._mark_instance_dirty)
        self.symbol.textEdited.connect(self._mark_instance_dirty)
        self.market.currentIndexChanged.connect(self._mark_instance_dirty)
        self.template_choice.currentIndexChanged.connect(self._mark_instance_dirty)

    def _kind_changed(self, *_):
        kind = self.kind.currentText()
        self.samples.setEnabled(
            kind
            in {"EMA", "MFI", "DMIxADX", "NES", "WOI", "MST", "IVP", "TPD", "SMA", "WMA", "BOL"}
        )
        self.samples.setMaximum(
            20000 if kind in {"MST", "IVP", "TPD", "SMA", "WMA", "BOL"} else 100000
        )
        if (
            kind in _MARKET_FIELDS
            and kind != getattr(self, "_editor_kind", None)
            and "nSamples" in _MARKET_FIELDS[kind]
        ):
            self.samples.setValue(_MARKET_FIELDS[kind]["nSamples"][0])
        self._editor_kind = kind
        self.periods.setEnabled(kind == "MMACD")
        self.signal.setEnabled(kind == "MMACD")
        self.precision.setEnabled(
            kind in {"EMA", "MMACD", "PSAR", "STS", "IVP", "SMA", "WMA", "BOL"}
        )
        self.line.setEnabled(
            kind
            in {"MFI", "DMIxADX", "SWING", "PSAR", "NES", "WOI", "MST", "TPD", "SMA", "WMA", "BOL"}
        )
        self.swing_range.setEnabled(kind == "SWING")
        for widget in (self.psar_start, self.psar_acceleration, self.psar_maximum):
            widget.setEnabled(kind == "PSAR")
        if hasattr(self, "market_forms"):
            for code, widget in self.market_forms.items():
                widget.setVisible(code == kind)
            self.market_details.setVisible(kind in _MARKET_FIELDS)
            self.market_details.setTitle(
                f"{kind} · " + ("상세 파라미터" if self.language == "ko" else "Parameters")
            )
            for widget in (
                self.samples,
                self.periods,
                self.signal,
                self.precision,
                self.line,
                self.swing_range,
                self.psar_start,
                self.psar_acceleration,
                self.psar_maximum,
            ):
                self._definition_editor.setRowVisible(
                    widget, kind not in _MARKET_FIELDS or widget.isEnabled()
                )

    def _read_definition(self):
        kind = self.kind.currentText()
        row = {"kind": kind, "interval_id": self.interval.currentData()}
        if kind == "EMA":
            row.update(ema_period=self.samples.value(), price_precision=self.precision.value())
        elif kind == "MMACD":
            periods = [int(value.strip()) for value in self.periods.text().split(",")]
            if (
                not 2 <= len(periods) <= 10
                or len(set(periods)) != len(periods)
                or any(not 2 <= period <= 100000 for period in periods)
            ):
                raise ValueError("MMACD requires 2–10 unique periods between 2 and 100000")
            row.update(
                mmacd_periods=sorted(periods),
                signal_period=self.signal.value(),
                price_precision=self.precision.value(),
            )
        elif kind == "SWING":
            try:
                percent = Decimal(self.swing_range.text().strip())
            except InvalidOperation:
                raise ValueError("SWING reversal must be a finite percent value") from None
            if not percent.is_finite():
                raise ValueError("SWING reversal must be a finite percent value")
            ratio = _swing_value(_decimal_shift(percent, -2))
            row.update(swing_range=format(ratio, "f"), line_index=self.line.value())
        elif kind == "PSAR":
            row.update(
                _parabolic_row(
                    {
                        "interval_id": self.interval.currentData(),
                        "line_index": self.line.value(),
                        "price_precision": self.precision.value(),
                        "start": float(self.psar_start.text()),
                        "acceleration": float(self.psar_acceleration.text()),
                        "maximum": float(self.psar_maximum.text()),
                    }
                )
            )
        elif kind in _MARKET_FIELDS:
            for name, widget in self.market_inputs[kind].items():
                row[name] = (
                    widget.value()
                    if isinstance(widget, QSpinBox)
                    else widget.currentText()
                    if isinstance(widget, QComboBox)
                    else widget.text().strip()
                )
            return {
                "kind": kind,
                **_market_row(kind, {key: value for key, value in row.items() if key != "kind"}),
            }
        else:
            row.update(nSamples=self.samples.value(), line_index=self.line.value())
        return row

    def add_definition(self):
        try:
            if len(self._definitions) >= 128:
                raise ValueError("Analysis definition limit reached")
            rows = [*self._definitions, self._read_definition()]
            form_configuration(rows)
            self._definitions = rows
            self._render_definitions()
            self._mark_dirty()
        except (ValueError, TypeError) as exc:
            self.status.setText(str(exc))

    def update_definition(self):
        index = self.definition_table.currentRow()
        if index < 0:
            return
        try:
            rows = [dict(row) for row in self._definitions]
            rows[index] = self._read_definition()
            form_configuration(rows)
            self._definitions = rows
            self._render_definitions()
            self._mark_dirty()
        except (ValueError, TypeError) as exc:
            self.status.setText(str(exc))

    def remove_definition(self):
        index = self.definition_table.currentRow()
        if index >= 0:
            self._definitions.pop(index)
            self._render_definitions()
            self._mark_dirty()

    def _definition_selected(self):
        index = self.definition_table.currentRow()
        if not 0 <= index < len(self._definitions):
            return
        row = self._definitions[index]
        self.interval.setCurrentIndex(self.interval.findData(row["interval_id"]))
        self.kind.setCurrentText(row["kind"])
        self.samples.setValue(row.get("ema_period", row.get("nSamples", 14)))
        self.periods.setText(", ".join(map(str, row.get("mmacd_periods", [12, 26]))))
        self.signal.setValue(row.get("signal_period", 9))
        self.precision.setValue(row.get("price_precision", 8))
        self.line.setValue(row.get("line_index", 0))
        if row["kind"] == "SWING":
            self.swing_range.setText(
                format(_decimal_shift(_swing_value(row["swing_range"]), 2), "f")
            )
        if row["kind"] == "PSAR":
            self.psar_start.setText(str(row["start"]))
            self.psar_acceleration.setText(str(row["acceleration"]))
            self.psar_maximum.setText(str(row["maximum"]))
        if row["kind"] in _MARKET_FIELDS:
            kind = row["kind"]
            for name, widget in self.market_inputs[kind].items():
                value = row.get(name, _MARKET_FIELDS[kind][name][0])
                if isinstance(widget, QSpinBox):
                    widget.setValue(value)
                elif isinstance(widget, QComboBox):
                    widget.setCurrentText(value)
                else:
                    widget.setText(str(value))

    def _render_definitions(self):
        blocker = QSignalBlocker(self.definition_table)
        self._render_parameters(self.definition_table, self._definitions)
        del blocker
        self.json_input.setPlainText(json.dumps(form_configuration(self._definitions), indent=2))

    @staticmethod
    def _render_parameters(table, definitions):
        table.setRowCount(len(definitions))
        for index, row in enumerate(definitions):
            interval = next(label for label, iid in _INTERVALS if iid == row["interval_id"])
            parameters = " · ".join(
                f"{key}={value}" for key, value in row.items() if key not in {"kind", "interval_id"}
            )
            if row["kind"] == "SWING":
                ratio = _swing_value(row["swing_range"])
                percent = format(_decimal_shift(ratio, 2), "f")
                parameters = f"line_index={row['line_index']} · reversal={percent}% (ratio {ratio})"
            elif row["kind"] == "PSAR":
                parameters = (
                    f"line_index={row['line_index']} · AF0={row['start']} · "
                    f"AF+={row['acceleration']} · AFmax={row['maximum']} · "
                    f"precision={row['price_precision']}"
                )
            for column, value in enumerate((interval, row["kind"], parameters)):
                table.setItem(index, column, QTableWidgetItem(str(value)))

    def _mark_dirty(self, *_):
        if not self._loading_form:
            self._edit_serials["templates"] += 1
            self._dirty = True
            self.status.setText(self.tr_text("unsaved"))

    def _mark_instance_dirty(self, *_):
        if not self._loading_form:
            self._edit_serials["instances"] += 1
            self._instance_dirty = True

    def apply_json(self):
        try:
            if len(self.json_input.toPlainText()) > 100000:
                raise ValueError("Analysis JSON exceeds limit")
            rows = configuration_rows(json.loads(self.json_input.toPlainText()))
            form_configuration(rows)
            self._definitions = rows
            self._render_definitions()
            self._mark_dirty()
        except (ValueError, TypeError, KeyError) as exc:
            self.status.setText(str(exc))

    def new_template(self):
        self._draft_ids["templates"] += 1
        self._selected_template = ""
        self._edit_revision = self.library["revision"]
        self.template_list.setCurrentRow(-1)
        self.template_name.clear()
        self._definitions = [
            {"kind": "EMA", "interval_id": 0, "ema_period": 20, "price_precision": 8},
            {
                "kind": "MMACD",
                "interval_id": 0,
                "mmacd_periods": [12, 26],
                "signal_period": 9,
                "price_precision": 8,
            },
        ]
        self._render_definitions()
        self._dirty = False
        self._update_controls()

    def _template_selected(self, item, _previous):
        if item is None:
            return
        identity = item.data(Qt.ItemDataRole.UserRole)
        row = next((row for row in self.library["templates"] if row["id"] == identity), None)
        if row is None:
            return
        self._draft_ids["templates"] += 1
        try:
            rows = configuration_rows(row["configuration"])
        except (KeyError, ValueError, TypeError) as exc:
            self.status.setText(str(exc))
            return
        self._selected_template, self._edit_revision = identity, self.library["revision"]
        self.template_name.setText(row["name"])
        self._definitions = rows
        self._render_definitions()
        self._dirty = False
        self._update_controls()

    def new_instance(self):
        self._draft_ids["instances"] += 1
        self._selected_instance = ""
        self._instance_edit_revision = self.library["revision"]
        self.instance_list.setCurrentRow(-1)
        self.instance_name.clear()
        self.instance_info.setText(self.tr_text("select"))
        self.instance_configuration.clear()
        self.instance_parameters.setRowCount(0)
        self.market.setEnabled(True)
        self.symbol.setEnabled(True)
        self._instance_dirty = False
        self._sync_execution_selection()
        self._update_controls()

    def _instance_selected(self, item, _previous):
        if item is None:
            self._sync_execution_selection()
            return
        identity = item.data(Qt.ItemDataRole.UserRole)
        row = next((row for row in self.library["instances"] if row["id"] == identity), None)
        if row is None:
            return
        self._draft_ids["instances"] += 1
        self._selected_instance, self._instance_edit_revision = identity, self.library["revision"]
        blockers = [
            QSignalBlocker(widget)
            for widget in (
                self.instance_name,
                self.market,
                self.symbol,
                self.template_choice,
            )
        ]
        self.instance_name.setText(row["name"])
        _, market, symbol = row["instrument"].split(":")
        self.market.setCurrentIndex(self.market.findData(market))
        self.symbol.setText(symbol)
        self.market.setEnabled(False)
        self.symbol.setEnabled(False)
        self.template_choice.setCurrentIndex(self.template_choice.findData(row["template_id"]))
        self.instance_info.setText(
            f"{self.tr_text('configured')}\n{row['instrument']}\n"
            f"CA r{row['revision']} · CAC snapshot r{row['template_revision']}"
        )
        self.instance_configuration.setPlainText(json.dumps(row["configuration"], indent=2))
        self._render_parameters(self.instance_parameters, configuration_rows(row["configuration"]))
        del blockers
        self._instance_dirty = False
        self._sync_execution_selection()
        self._update_controls()

    def _sync_execution_selection(self):
        if not hasattr(self, "execution"):
            return
        item = self.instance_list.currentItem()
        identity = (
            item.data(Qt.ItemDataRole.UserRole)
            if item is not None and item.isSelected() and not item.isHidden()
            else ""
        )
        row = next((row for row in self.library["instances"] if row["id"] == identity), None)
        self.execution.select(row if identity == self._selected_instance else None)

    def _instance_label(self, row):
        state = self._execution_states.get(row["id"], "configured")
        parts = {"name": row["name"], "instrument": row["instrument"], "state": state}
        first = self.instance_sort.currentData() or "name"
        return " · ".join(
            [
                parts[first],
                *(value for key, value in parts.items() if key != first),
                f"r{row['revision']}",
            ]
        )

    def _filter_instances(self, *_):
        query = self.instance_search.text().strip().casefold()
        blocker = QSignalBlocker(self.instance_list)
        for index in range(self.instance_list.count()):
            item = self.instance_list.item(index)
            row = next(
                (
                    row
                    for row in self.library["instances"]
                    if row["id"] == item.data(Qt.ItemDataRole.UserRole)
                ),
                None,
            )
            if row is not None:
                item.setText(self._instance_label(row))
                hidden = query not in (row["name"] + " " + row["instrument"]).casefold()
                item.setHidden(hidden)
                if hidden and item.isSelected():
                    self.instance_list.clearSelection()
                    self.instance_list.setCurrentRow(-1)
        self.instance_list.sortItems()
        del blocker
        self._sync_execution_selection()
        self._update_controls()

    def _set_execution_state(self, identity, state):
        if self._execution_states.get(identity) == state:
            return
        self._execution_states[identity] = state
        for index in range(self.instance_list.count()):
            item = self.instance_list.item(index)
            if item.data(Qt.ItemDataRole.UserRole) == identity:
                row = next(
                    (row for row in self.library["instances"] if row["id"] == identity), None
                )
                if row is not None:
                    item.setText(self._instance_label(row))
                    item.setToolTip(
                        row["instrument"]
                        + self.execution._text(
                            "\n마지막 조회 실행 상태", "\nLast queried execution state"
                        )
                    )
                break
        if self.instance_sort.currentData() == "state":
            self.instance_list.sortItems()

    def save_template(self):
        try:
            configuration = form_configuration(self._definitions)
            if not self._definitions:
                raise ValueError("At least one installed indicator is required")
            action = "update" if self._selected_template else "create"
            payload = {"name": self.template_name.text(), "configuration": configuration}
            if self._selected_template:
                payload["id"] = self._selected_template
            self._command("analysis.template." + action, payload, self._edit_revision)
        except (ValueError, TypeError) as exc:
            self.status.setText(str(exc))

    def clone_template(self):
        self._command(
            "analysis.template.clone",
            {"id": self._selected_template, "name": self.template_name.text()},
            self._edit_revision,
        )

    def rename_template(self):
        self._command(
            "analysis.template.rename",
            {"id": self._selected_template, "name": self.template_name.text()},
            self._edit_revision,
        )

    def delete_template(self):
        self._command(
            "analysis.template.delete", {"id": self._selected_template}, self._edit_revision
        )

    def create_instance(self):
        instrument = f"binance:{self.market.currentData()}:{self.symbol.text().strip().upper()}"
        self._command(
            "analysis.instance.create",
            {
                "name": self.instance_name.text(),
                "instrument": instrument,
                "template_id": self.template_choice.currentData(),
            },
            self._instance_edit_revision,
        )

    def rename_instance(self):
        self._command(
            "analysis.instance.rename",
            {"id": self._selected_instance, "name": self.instance_name.text()},
            self._instance_edit_revision,
        )

    def rebase_instance(self):
        self._command(
            "analysis.instance.rebase",
            {"id": self._selected_instance, "template_id": self.template_choice.currentData()},
            self._instance_edit_revision,
        )

    def delete_instance(self):
        self._command(
            "analysis.instance.delete",
            {"id": self._selected_instance},
            self._instance_edit_revision,
        )

    def _command(self, kind, payload, revision):
        if not self.client.attached or self._pending or self._uncertain or not self._have_snapshot:
            return
        payload["expected_revision"] = revision
        self._pending_kind = kind
        group = "templates" if ".template." in kind else "instances"
        self._submitted_context = self._draft_ids[group], self._edit_serials[group]
        self._pending = self.client.command(kind, payload)
        self._update_controls()

    def refresh(self):
        if self.client.attached:
            self.client.request("GET", "/api/analysis-library", tag="analysis-library")
            self.status.setText(self.tr_text("loading"))

    def resolve_operation(self):
        if self._uncertain and self.client.attached:
            self.client.request(
                "GET",
                "/api/operations/" + self._uncertain,
                tag="analysis-operation:" + self._uncertain,
            )

    def _result(self, tag, result):
        if tag == "analysis-library":
            self.apply_snapshot(result)
            return
        identity = (
            tag.removeprefix("command:")
            if tag.startswith("command:")
            else tag.removeprefix("analysis-operation:")
        )
        if identity not in {self._pending, self._uncertain} or not identity:
            return
        outcome = result.get("result")
        if (
            result.get("status") == "completed"
            and isinstance(outcome, dict)
            and outcome.get("status") == "rejected"
        ):
            result = {"status": "rejected", "code": outcome.get("code", "Rejected")}
        if result.get("status") == "completed":
            item = outcome.get("item") if isinstance(outcome, dict) else None
            if not isinstance(item, dict) or not isinstance(item.get("id"), str):
                self._uncertain, self._pending = identity, None
                self.status.setText(self.tr_text("uncertain"))
                self._update_controls()
                return
            group = "templates" if ".template." in (self._pending_kind or "") else "instances"
            selected = "" if item.get("deleted") else item.get("id", "")
            same_draft = (
                self._submitted_context is not None
                and self._draft_ids[group] == self._submitted_context[0]
            )
            unchanged = same_draft and self._edit_serials[group] == self._submitted_context[1]
            if same_draft:
                if group == "templates":
                    self._selected_template, self._dirty = selected, not unchanged
                    self._edit_revision = outcome.get("revision", self._edit_revision)
                else:
                    self._selected_instance, self._instance_dirty = selected, not unchanged
                    self._instance_edit_revision = outcome.get(
                        "revision", self._instance_edit_revision
                    )
            elif group == "templates" and not self._selected_template:
                self._edit_revision = outcome.get("revision", self._edit_revision)
            elif group == "instances" and not self._selected_instance:
                self._instance_edit_revision = outcome.get("revision", self._instance_edit_revision)
            self._reload_group = group if unchanged else None
            self._pending = self._uncertain = None
            self.status.setText(self.tr_text("saved"))
            self._have_snapshot = False
            self.refresh()
        elif result.get("status") == "rejected":
            self._pending = self._uncertain = None
            self.status.setText(str(result.get("code", "Rejected")))
        else:
            self._uncertain, self._pending = identity, None
            self.status.setText(self.tr_text("uncertain"))
        self._update_controls()

    def _failed(self, tag, message):
        if tag == "analysis-library":
            self.status.setText(str(message)[:300])
        elif tag in {"command:" + str(self._pending), "analysis-operation:" + str(self._uncertain)}:
            self._uncertain, self._pending = self._pending or self._uncertain, None
            self.status.setText(self.tr_text("uncertain") + " · " + str(message)[:300])
        self._update_controls()

    def apply_snapshot(self, snapshot):
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("schema_version") != 1
            or not isinstance(snapshot.get("templates"), list)
            or not isinstance(snapshot.get("instances"), list)
            or type(snapshot.get("revision")) is not int
        ):
            self.status.setText("Invalid analysis library response")
            return
        try:
            if len(snapshot["templates"]) > 128 or len(snapshot["instances"]) > 512:
                raise ValueError("Analysis library response exceeds item limit")
            for group in ("templates", "instances"):
                for row in snapshot[group]:
                    if (
                        not isinstance(row, dict)
                        or not isinstance(row.get("id"), str)
                        or not isinstance(row.get("name"), str)
                        or type(row.get("revision")) is not int
                        or row["revision"] < 1
                    ):
                        raise ValueError("Invalid analysis library record")
                    form_configuration(configuration_rows(row["configuration"]))
                    if group == "instances" and (
                        not isinstance(row.get("instrument"), str)
                        or len(row["instrument"].split(":")) != 3
                        or not isinstance(row.get("template_id"), str)
                        or type(row.get("template_revision")) is not int
                    ):
                        raise ValueError("Invalid CA response")
        except (ValueError, TypeError, KeyError) as exc:
            self.status.setText(str(exc))
            return
        self.library = snapshot
        self._have_snapshot = snapshot.get("persistence", "ready") == "ready"
        for widget, group, selected in (
            (self.template_list, "templates", self._selected_template),
            (self.instance_list, "instances", self._selected_instance),
        ):
            blocker = QSignalBlocker(widget)
            widget.clear()
            for row in snapshot[group]:
                item = QListWidgetItem(f"{row['name']} · r{row['revision']}")
                item.setData(Qt.ItemDataRole.UserRole, row["id"])
                item.setToolTip(row.get("instrument", row["id"]))
                widget.addItem(item)
                if row["id"] == selected:
                    widget.setCurrentItem(item)
            del blocker
        selected = self.template_choice.currentData()
        blocker = QSignalBlocker(self.template_choice)
        self.template_choice.clear()
        for row in snapshot["templates"]:
            self.template_choice.addItem(f"{row['name']} · r{row['revision']}", row["id"])
        index = self.template_choice.findData(selected)
        if index >= 0:
            self.template_choice.setCurrentIndex(index)
        del blocker
        clean_templates = not self._dirty and not self._pending and not self._uncertain
        clean_instances = not self._instance_dirty and not self._pending and not self._uncertain
        if clean_templates or self._reload_group == "templates":
            if self._selected_template and self.template_list.currentItem():
                self._template_selected(self.template_list.currentItem(), None)
            else:
                self.new_template()
        if clean_instances or self._reload_group == "instances":
            if self._selected_instance and self.instance_list.currentItem():
                self._instance_selected(self.instance_list.currentItem(), None)
            else:
                self.new_instance()
        self._reload_group = None
        self._filter_instances()
        self._sync_execution_selection()
        self._update_controls()
        if not self._pending and not self._uncertain and self.status.text() in _TEXT["loading"]:
            self.status.setText(
                self.tr_text("unsaved" if self._dirty or self._instance_dirty else "loaded")
            )

    def update_state(self, snapshot):
        self._auxiliary_inputs = (snapshot.get("core", {}).get("capabilities") or {}).get(
            "analysis_auxiliary_inputs"
        ) is True
        self._update_support_hint()
        epoch = snapshot.get("core", {}).get("epoch")
        if epoch != self._epoch:
            self._epoch = epoch
            self.refresh()
        library = snapshot.get("analysis_library")
        if isinstance(library, dict) and library.get("revision") != self.library["revision"]:
            self.apply_snapshot(library)
        self.execution.update_state(snapshot)

    def set_attached(self, attached):
        previous, self._attached = self._attached, bool(attached)
        if not attached:
            self.status.setText(self.tr_text("disconnected"))
            if self._pending:
                self._uncertain, self._pending = self._pending, None
        elif not previous:
            self.refresh()
        self._update_controls()

    def _update_controls(self):
        if not hasattr(self, "instance_actions"):
            return
        available = (
            self.client.attached
            and self._have_snapshot
            and not self._pending
            and not self._uncertain
        )
        selected_item = self.instance_list.currentItem()
        instance_selected = bool(
            selected_item is not None
            and selected_item.isSelected()
            and not selected_item.isHidden()
        )
        for key, button in self.template_actions.items():
            button.setEnabled(
                key == "new" or (available and (key == "save" or bool(self._selected_template)))
            )
        for key, button in self.instance_actions.items():
            button.setEnabled(
                key == "new"
                or (
                    available
                    and (
                        key == "create"
                        and not self._selected_instance
                        and bool(self.library["templates"])
                        or key != "create"
                        and bool(self._selected_instance)
                        and instance_selected
                    )
                )
            )
        self.refresh_button.setEnabled(self.client.attached)
        self.resolve_button.setEnabled(self.client.attached and bool(self._uncertain))

    def set_shell_embedded(self, embedded):
        self.title.setVisible(not embedded)

    def set_presentation(self, theme, language):
        self.theme, self.language = theme, language
        self.title.setText(self.tr_text("title"))
        self.notice.setText(self.tr_text("notice"))
        for widget, key in self._texts:
            widget.setText(self.tr_text(key))
        self._update_support_hint()
        self.tabs.setTabText(0, self.tr_text("templates"))
        self.tabs.setTabText(1, self.tr_text("instances"))
        self.advanced.setTitle(self.tr_text("advanced"))
        self.instance_advanced.setTitle(self.tr_text("advanced"))
        self.instance_search.setPlaceholderText(
            "CA 이름 / 종목 검색" if language == "ko" else "Search CA name / instrument"
        )
        labels = (
            ("이름", "종목", "실행 상태")
            if language == "ko"
            else ("Name", "Instrument", "Execution state")
        )
        for index, label in enumerate(labels):
            self.instance_sort.setItemText(index, label)
        self.execution.set_presentation()
        for table in (self.definition_table, self.instance_parameters):
            table.setHorizontalHeaderLabels(
                (
                    self.tr_text("interval"),
                    self.tr_text("kind"),
                    "Parameters" if language != "ko" else "파라미터",
                )
            )

    def _update_support_hint(self):
        message = self.tr_text("unsupported")
        if getattr(self, "_auxiliary_inputs", False):
            message = message.replace(
                self.tr_text("auxiliary_unavailable"), self.tr_text("auxiliary_available")
            )
        self.support_hint.setText(message)
