"""Eta-inspired chart workspace, consuming immutable core snapshots only."""

# Imports
import json
import math
import re
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from urllib.parse import quote
from uuid import uuid4

from PySide6.QtCore import QEvent, QSignalBlocker, Qt, QTimer
from PySide6.QtWidgets import (
    QButtonGroup,
    QCheckBox,
    QComboBox,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSplitter,
    QStyle,
    QVBoxLayout,
    QWidget,
)

from omega.domain.errors import ValidationError
from omega.domain.models import is_valid_symbol
from omega.gui.calendar_bars import CALENDAR_PERIODS, calendar_bounds, covered_minutes
from omega.gui.chart_auxiliary import AuxiliaryChartRequest
from omega.gui.chart_derived_series import DerivedSeriesRequest
from omega.gui.chart_indicators import IndicatorPlot, PriceOverlayRequest, display_indicators
from omega.gui.chart_lookup_policy import ChartLookupPolicy
from omega.gui.chart_price_windows import (
    PriceWindowsRequest,
    exact_snapshot_pin,
)
from omega.gui.chart_price_windows import (
    definitions as price_window_definitions,
)
from omega.gui.chart_profiles import (
    ANALYSIS_KINDS,
    AUXILIARY_INDICATORS,
    DERIVED_INDICATORS,
    ChartWorkspaceStore,
    default_profile,
    display_date,
    indicator_enabled,
    validate_profile,
)
from omega.gui.chart_settings import ChartSettings
from omega.gui.chart_volume_profile import VolumeProfileRequest
from omega.gui.charts import TIMEFRAMES, ChartDrawer, aggregate_utc
from omega.gui.models import Candle, parse_candles

# Constants
_TEXT = {
    "title": ("마켓 분석", "MARKET ANALYSIS"),
    "snapshot": (
        "확정 1분봉 스냅샷 · 상위 시간봉은 화면용 UTC 완전봉 집계 · 전략 분석과 분리",
        "Closed 1m snapshots · complete UTC display aggregation · separate from strategy analysis",
    ),
    "calendar_limit": (
        "주봉은 UTC 월요일, 월봉은 UTC 매월 1일입니다. 확정 1분봉 전체가 있어야 표시합니다.",
        "Weeks begin Monday UTC; months begin on day 1 UTC. "
        "Every closed 1m source bar is required.",
    ),
    "calendar_capacity": (
        "{frame}은 완전한 달력 구간이 필요합니다. 현재 API 조회 상한은 1분봉 10,000개이며 "
        "주봉은 10,080개, 월봉은 40,320–44,640개가 필요해 이 조회만으로는 만들 수 없습니다.",
        "{frame} needs a complete calendar interval. The API returns up to 10,000 minute bars; "
        "a week needs 10,080 and a month 40,320–44,640. "
        "This query cannot supply a complete bar.",
    ),
    "calendar_loading": ("{frame} DB 달력 봉 조회 중", "Loading {frame} calendar bars from DB"),
    "calendar_unavailable": (
        "{frame} 달력 봉 조회 불가 · DB 또는 코어의 달력 조회 지원을 확인하세요.",
        "{frame} calendar bars unavailable · check DB and core calendar-query support.",
    ),
    "calendar_counts": (
        "DB 1m {minutes:,}개 → {frame} 완전봉 {bars:,} · "
        "결측 {missing:,}분 · 부분 구간 {partial:,}개",
        "DB {minutes:,} source minutes → {frame} {bars:,} complete bars · "
        "{missing:,} missing minutes · {partial:,} partial periods",
    ),
    "add": ("+ 비교 차트", "+ Compare"),
    "refresh": ("새로고침", "Refresh"),
    "reset": ("최근 구간", "Latest view"),
    "linked": ("시간축 / 커서 연동", "Link time / cursor"),
    "grid": ("자동 격자", "Adaptive grid"),
    "columns": ("가로 분할", "Side by side"),
    "rows": ("세로 분할", "Stacked"),
    "fit": ("전체", "Fit"),
    "volume": ("거래량", "Volume"),
    "awaiting": ("스냅샷 대기", "Awaiting snapshot"),
    "demo": ("DEMO · 합성", "DEMO · synthetic"),
    "database": ("DATABASE", "DATABASE"),
    "manual_paper": ("PAPER · 수동 입력", "PAPER · manual input"),
    "disconnected": ("연결 끊김 · 마지막 스냅샷", "Disconnected · retained snapshot"),
    "empty": (
        "표시할 확정 1분봉이 없습니다. 수집기와 DB 상태를 확인하세요.",
        "No closed 1m candles. Check collector and database status.",
    ),
    "incomplete": (
        "{frame} 완전봉 없음 · 연속된 확정 1분봉 {minutes}개가 필요합니다.",
        "No complete {frame} bars · requires {minutes} consecutive closed 1m candles.",
    ),
    "counts": (
        "1m {raw:,} → {frame} 완전봉 {bars:,} · 불완전 구간 {omitted:,}분 제외",
        "1m {raw:,} → {frame} {bars:,} complete bars · {omitted:,} incomplete minutes omitted",
    ),
    "ema_hint": (
        "화면용 EMA20 · 완전봉 20개 SMA seed · 전략 입력에 사용하지 않음",
        "Display EMA20 · SMA seed after 20 complete bars · never strategy input",
    ),
    "fit_hint": (
        "전체 스냅샷과 가격/거래량 범위 맞춤",
        "Fit snapshot and price/volume ranges",
    ),
    "interaction_hint": (
        "Eta 조작: 먼저 영역을 클릭한 뒤 휠을 사용하세요.\n"
        "가격 plot: 가로/세로 이동 · 휠 시간 확대/축소(오른쪽 끝 고정)\n"
        "가격축: 세로 드래그로 가격 배율 · 휠 없음\n"
        "시간축: 가로 드래그로 시간 배율 · 휠 시간 이동\n"
        "거래량 plot: 시간 이동/배율 · 거래량축: 독립 배율\n"
        "좌클릭: 시각 선택/해제 · 우클릭: 해당 세로 범위 맞춤\n"
        "축 더블클릭: 해당 축 맞춤 · 차트 밖으로 나가면 휠 영역 재선택",
        "Eta controls: click a section before using the wheel.\n"
        "Price plot: H/V pan; wheel time zoom anchored at the right edge.\n"
        "Price axis: drag vertically to scale; wheel does nothing.\n"
        "Time axis: horizontal drag to zoom; wheel pans time.\n"
        "Volume plot: time pan/zoom; volume axis scales independently.\n"
        "Left click toggles a fixed timestamp; right click fits that Y range.\n"
        "Double-click an axis to reset it; leaving the chart clears wheel focus.",
    ),
    "symbol_hint": (
        "종목 입력 후 Enter · 목록은 빠른 선택 예시",
        "Type a symbol and press Enter · suggestions only",
    ),
    "failed": ("스냅샷 조회 실패", "Snapshot request failed"),
    "gap": ("결측 {count}분", "{count} missing minutes"),
    "interpolated": ("보간 {ratio:.1%}", "interpolated {ratio:.1%}"),
}


def text(key: str, language: str) -> str:
    return _TEXT[key][0 if language == "ko" else 1]


class ChartPane(QFrame):
    def __init__(self, owner: "ChartsPage", instrument: str):
        super().__init__()
        self.owner = owner
        self.setObjectName("chartPane")
        self.setMinimumSize(320, 355)
        self.raw: tuple[Candle, ...] = ()
        self.source_kind = "awaiting"
        self.quality: dict = {}
        self.minutes = 1
        self.profile = default_profile()
        self._series = {}
        self._series_key = None
        self._raw_revision, self._raw_epoch = 0, None
        self._price_content, self._price_raw_epoch = None, None
        self._dmi_key = self._dmi_id = self._dmi_hash = self._dmi_tag = None
        self._dmi_parameters = None
        self._dmi_state, self._dmi_serial = "off", 0
        self._dmi_lookup_policy = ChartLookupPolicy()
        self._dmi_lines, self._dmi_plot_key, self._dmi_plot_lines = {}, None, {}
        self._dmi_metadata = {}
        self._strategies = {}
        self._fill_tag = None
        self._fill_generation = 0
        self._fill_revision = None
        self._epoch = None
        self._raw_source, self._raw_quality = "awaiting", {}
        self._calendar_generation = 0
        self._calendar_data = {}
        self._calendar_state, self._calendar_error = "awaiting", ""
        self.selection = QLineEdit(instrument, self)
        self.selection.hide()  # Canonical instrument value retained for client/test contracts.
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 6)
        layout.setSpacing(5)
        header = QHBoxLayout()
        self.market = QComboBox()
        self.market.addItem("USDⓈ-M", "usdm")
        self.market.addItem("SPOT", "spot")
        self.market.setMaximumWidth(115)
        self.symbol = QComboBox()
        self.symbol.setEditable(True)
        self.symbol.addItems(["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT"])
        self.symbol.lineEdit().setMaxLength(40)
        self.source_label = QLabel()
        self.source_label.setProperty("role", "muted")
        self.close_button = QPushButton("×")
        self.close_button.setFixedWidth(28)
        header.addWidget(self.market)
        header.addWidget(self.symbol, 1)
        header.addWidget(self.source_label)
        header.addWidget(self.close_button)
        layout.addLayout(header)
        period_row = QHBoxLayout()
        self.period_scroll = QScrollArea()
        self.period_scroll.setFrameShape(QFrame.Shape.NoFrame)
        self.period_scroll.setWidgetResizable(True)
        self.period_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.period_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        period_content = QWidget()
        periods = QHBoxLayout(period_content)
        periods.setContentsMargins(0, 0, 0, 0)
        periods.setSpacing(4)
        self.timeframes = QButtonGroup(self)
        self.timeframes.setExclusive(True)
        self.period_buttons: dict[str, QPushButton] = {}
        for frame, minutes in TIMEFRAMES.items():
            button = QPushButton(frame)
            button.setCheckable(True)
            button.setMinimumWidth(27)
            button.setProperty("role", "timeframe")
            self.timeframes.addButton(button, self._button_id(minutes))
            self.period_buttons[frame] = button
            periods.addWidget(button)
        self.period_buttons["1m"].setChecked(True)
        self.period_scroll.setWidget(period_content)
        self._fit_period_strip()
        period_row.addWidget(self.period_scroll, 1)
        self.fit = QPushButton()
        self.settings_button = QPushButton()
        self.settings_button.setCheckable(True)
        period_row.addWidget(self.fit)
        period_row.addWidget(self.settings_button)
        layout.addLayout(period_row)
        self.chart = ChartDrawer()
        self.chart.set_options(volume=False)
        self.chart.set_presentation(owner.theme, owner.language)
        layout.addWidget(self.chart, 1)
        self.si_layout = QVBoxLayout()
        self.si_layout.setContentsMargins(0, 0, 0, 0)
        self.si_layout.setSpacing(3)
        self.si_plots = [IndicatorPlot(self.chart, parent=self) for _ in range(3)]
        for plot in self.si_plots:
            self.si_layout.addWidget(plot)
        layout.addLayout(self.si_layout)
        options = QHBoxLayout()
        self.ema = QCheckBox("EMA20")
        self.volume = QCheckBox()
        self.volume.setChecked(True)
        self.quality_badge = QLabel()
        self.quality_badge.setProperty("role", "muted")
        options.addWidget(self.ema)
        options.addWidget(self.volume)
        options.addStretch()
        options.addWidget(self.quality_badge)
        layout.addLayout(options)
        producer = QHBoxLayout()
        self.producer_status, self.producer_retry = QLabel(), QPushButton()
        self.producer_status.setWordWrap(True)
        self.producer_status.setProperty("role", "muted")
        producer.addWidget(self.producer_status, 1)
        producer.addWidget(self.producer_retry)
        layout.addLayout(producer)
        self.producer_retry.clicked.connect(self._retry_dmi)
        self.price_requests = {}
        for kind in ("PSAR", "SWING"):
            row = QHBoxLayout()
            label, retry = QLabel(), QPushButton()
            label.setWordWrap(True)
            label.setProperty("role", "muted")
            row.addWidget(label, 1)
            row.addWidget(retry)
            layout.addLayout(row)
            self.price_requests[kind] = PriceOverlayRequest(self, kind, label, retry)
        row = QHBoxLayout()
        ivp_label, ivp_retry = QLabel(), QPushButton()
        ivp_label.setWordWrap(True)
        ivp_label.setProperty("role", "muted")
        row.addWidget(ivp_label, 1)
        row.addWidget(ivp_retry)
        layout.addLayout(row)
        self.ivp_request = VolumeProfileRequest(self, ivp_label, ivp_retry)
        row = QHBoxLayout()
        windows_label, windows_retry = QLabel(), QPushButton()
        windows_label.setWordWrap(True)
        windows_label.setProperty("role", "muted")
        row.addWidget(windows_label, 1)
        row.addWidget(windows_retry)
        layout.addLayout(row)
        self.price_windows_request = PriceWindowsRequest(self, windows_label, windows_retry)
        self.derived_requests = {}
        for kind in DERIVED_INDICATORS:
            row = QHBoxLayout()
            label, retry = QLabel(), QPushButton()
            label.setWordWrap(True)
            label.setProperty("role", "muted")
            row.addWidget(label, 1)
            row.addWidget(retry)
            layout.addLayout(row)
            self.derived_requests[kind] = DerivedSeriesRequest(self, kind, label, retry)
        self.auxiliary_requests = {}
        for kind in AUXILIARY_INDICATORS:
            row = QHBoxLayout()
            label, retry = QLabel(), QPushButton()
            label.setWordWrap(True)
            label.setProperty("role", "muted")
            row.addWidget(label, 1)
            row.addWidget(retry)
            layout.addLayout(row)
            self.auxiliary_requests[kind] = AuxiliaryChartRequest(self, kind, label, retry)
        self.trade_overlay = QComboBox()
        self.trade_overlay.addItem("체결 표시 없음", "")
        self.trade_overlay.setSizeAdjustPolicy(
            QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon
        )
        self.trade_overlay.setMinimumContentsLength(15)
        layout.addWidget(self.trade_overlay)
        self.overlay_status = QLabel()
        self.overlay_status.setWordWrap(True)
        self.overlay_status.setProperty("role", "muted")
        layout.addWidget(self.overlay_status)
        self.selected_readout = QLabel()
        self.selected_readout.setWordWrap(True)
        self.selected_readout.setProperty("role", "muted")
        self.selected_readout.hide()
        layout.addWidget(self.selected_readout)
        self.settings_panel = ChartSettings(self)
        self.settings_scroll = QScrollArea()
        self.settings_scroll.setWidgetResizable(True)
        self.settings_scroll.setWidget(self.settings_panel)
        self.settings_scroll.setMaximumHeight(275)
        self.settings_scroll.setMinimumHeight(190)
        self.settings_scroll.hide()
        layout.insertWidget(2, self.settings_scroll)
        self.detail = QLabel()
        self.detail.setWordWrap(True)
        self.detail.setProperty("role", "muted")
        layout.addWidget(self.detail)
        self.timeframes.idClicked.connect(
            lambda identity: self.set_timeframe({-100: "1w", -101: "1M"}.get(identity, identity))
        )
        self.ema.toggled.connect(self._toggle_ema)
        self.volume.toggled.connect(self._toggle_volume)
        self.fit.clicked.connect(self.fit_all)
        self.settings_button.toggled.connect(self.settings_scroll.setVisible)
        self.settings_button.toggled.connect(lambda _checked: self._update_minimum_height())
        self.trade_overlay.currentIndexChanged.connect(self._select_overlay)
        if hasattr(owner.client, "resultReceived"):
            owner.client.resultReceived.connect(self._fill_result)
            owner.client.resultReceived.connect(self._dmi_result)
        owner.client.failed.connect(self._dmi_failed)
        self._dmi_timer = QTimer(self)
        self._dmi_timer.setInterval(1000)
        self._dmi_timer.timeout.connect(self._poll_dmi)
        self.chart.selectionChanged.connect(self._selection_readout)
        self.market.currentIndexChanged.connect(self._commit_symbol)
        self.symbol.activated.connect(self._commit_symbol)
        self.symbol.lineEdit().returnPressed.connect(self._commit_symbol)
        self.selection.textChanged.connect(self._selection_changed)
        self.close_button.clicked.connect(lambda: owner._remove_pane(self))
        self.chart.rangeChanged.connect(
            lambda start, stop: owner._link_range(self.chart, start, stop)
        )
        self.chart.cursorChanged.connect(
            lambda timestamp: owner._link_cursor(self.chart, timestamp)
        )
        self._sync_selector(instrument)
        self.apply_profile(self.profile)
        self.set_presentation()
        self._update_minimum_height()

    def _update_minimum_height(self):
        self.layout().invalidate()
        self.setMinimumHeight(max(355, self.layout().minimumSize().height()))
        if hasattr(self.owner, "_split_rows"):
            self.owner._refresh_minimum_geometry()

    def _fit_period_strip(self) -> None:
        """Style padding must not squeeze labels when a shell applies its theme later."""
        if not hasattr(self, "period_scroll") or not hasattr(self, "period_buttons"):
            return
        for button in self.period_buttons.values():
            button.setMinimumWidth(max(27, button.sizeHint().width()))
        periods = self.period_scroll.widget().layout()
        width = sum(button.minimumWidth() for button in self.period_buttons.values())
        self.period_scroll.widget().setMinimumWidth(
            width + periods.spacing() * (len(self.period_buttons) - 1)
        )
        height = max(button.sizeHint().height() for button in self.period_buttons.values())
        self.period_scroll.setFixedHeight(
            height + self.style().pixelMetric(QStyle.PixelMetric.PM_ScrollBarExtent)
        )

    def changeEvent(self, event) -> None:
        super().changeEvent(event)
        if event.type() in {QEvent.Type.StyleChange, QEvent.Type.FontChange}:
            QTimer.singleShot(0, self._fit_period_strip)
            if hasattr(self, "si_plots"):
                QTimer.singleShot(0, self._update_minimum_height)

    def _sync_selector(self, instrument: str) -> None:
        parts = instrument.split(":")
        if len(parts) != 3:
            return
        market_block = QSignalBlocker(self.market)
        symbol_block = QSignalBlocker(self.symbol)
        index = self.market.findData(parts[1])
        if index >= 0:
            self.market.setCurrentIndex(index)
        self.symbol.setCurrentText(parts[2])
        del market_block, symbol_block

    def _commit_symbol(self, *_args) -> None:
        symbol = self.symbol.currentText().strip().upper()
        if not is_valid_symbol(symbol):
            self.detail.setText("Invalid Binance symbol")
            return
        instrument = f"binance:{self.market.currentData()}:{symbol}"
        if instrument != self.selection.text():
            self.selection.setText(instrument)
        else:
            self.owner.refresh()

    def _selection_changed(self, instrument: str) -> None:
        self._sync_selector(instrument)
        self.raw = ()
        self._price_content, self._price_raw_epoch = None, None
        self.source_kind, self.quality = "awaiting", {}
        self._raw_source, self._raw_quality = "awaiting", {}
        self._calendar_generation += 1
        self._calendar_data.clear()
        self._calendar_state, self._calendar_error = "awaiting", ""
        self.chart.set_candles(())
        self.chart.clear_selection()
        self.chart.reset_view()
        self.chart.set_fill_markers([])
        self.update_strategies(self._strategies)
        self._refresh_indicators()
        self._update_status()
        self.owner.refresh()

    @staticmethod
    def _button_id(period: int | str) -> int:
        return {"1w": -100, "1M": -101}.get(period, period)

    def set_timeframe(self, minutes: int | str) -> None:
        if minutes not in TIMEFRAMES.values():
            raise ValueError("unsupported timeframe")
        if self.minutes == minutes:
            self.timeframes.button(self._button_id(minutes)).setChecked(True)
            return
        self.chart._geometry()
        right = self.chart._time_bounds()[1] if self.chart.candles else None
        self.minutes = minutes
        self._calendar_generation += 1
        self._calendar_state, self._calendar_error = "awaiting", ""
        self.timeframes.button(self._button_id(minutes)).setChecked(True)
        if minutes in CALENDAR_PERIODS:
            self.chart.set_calendar_period(minutes)
        else:
            self.chart.set_interval_minutes(minutes)
        self._render_interval()
        self.chart.reset_view(end_ms=right)
        if minutes in CALENDAR_PERIODS:
            self.owner._request_calendar(self)

    def set_snapshot(
        self,
        rows: tuple[Candle, ...],
        source: str,
        quality: dict,
        *,
        content=None,
        content_epoch=None,
    ) -> None:
        verified = (
            content is not None
            and type(content_epoch) is str
            and bool(content_epoch)
            and content_epoch == self.owner._series_epoch
        )
        if not verified:
            content = None
        changed = rows != self.raw or content != self._price_content
        self._price_content = content
        # The body epoch was captured before provider.read, not when an old GET
        # happened to finish. Legacy display freshness keeps its prior contract.
        self._price_raw_epoch = content_epoch if verified else None
        if changed:
            self.raw = rows
            self._raw_revision += 1
        self._raw_epoch = self.owner._series_epoch
        self._raw_source, self._raw_quality = source, quality
        if self.minutes not in CALENDAR_PERIODS or not self.owner._calendar_api:
            self.source_kind, self.quality = source, quality
        if changed:
            self._render_interval()
        else:
            self._refresh_indicators()
            self._update_status()

    def _render_interval(self) -> None:
        try:
            if self.minutes in CALENDAR_PERIODS and self.owner._calendar_api:
                value = self._calendar_data.get(self.minutes)
                rows, self.source_kind, self.quality = value or ((), "awaiting", {})
            else:
                self.source_kind, self.quality = self._raw_source, self._raw_quality
                rows = aggregate_utc(self.raw, self.minutes)
            self.chart.set_candles(rows)
            self._refresh_indicators()
            self._update_status()
        except ValueError as error:
            self.chart.set_candles(())
            self.chart.empty_message = str(error)
            self.detail.setText(str(error))
            self.chart.update()

    def _refresh_indicators(self):
        # SI formulas depend on accepted data/boundaries and numerical settings,
        # independently of slot allocation, timezone, EMA overlay and visibility.
        key = (
            self.chart._data_revision,
            self.chart.interval_ms,
            self.chart.calendar_period,
            self.profile["mfi_period"],
            tuple(self.profile["mmacd_periods"]),
            self.profile["signal_period"],
            tuple((code, self.profile["enabled"][code]) for code in ("VOL", "MFI", "MMACD")),
        )
        if key != self._series_key:
            series = display_indicators(self.chart.candles, self.profile, self.chart._bar_end)
            self._series, self._series_key = series, key
        self._ensure_dmi()
        for request in self.price_requests.values():
            request.ensure()
        self.ivp_request.ensure()
        self.price_windows_request.ensure()
        field = self.profile["dmi_display"]
        plot_key = id(self._dmi_lines), field, len(self.chart.candles)
        if plot_key != self._dmi_plot_key:
            values = self._dmi_lines.get(field, (None,) * len(self.chart.candles))
            self._dmi_plot_lines = {field: values}
            self._dmi_plot_key = plot_key
        self._series["DMIxADX"] = self._dmi_plot_lines
        for kind, request in self.derived_requests.items():
            request.ensure()
            if indicator_enabled(self.profile, kind):
                self._series[kind] = request.lines()
            else:
                self._series.pop(kind, None)
        for kind, request in self.auxiliary_requests.items():
            request.ensure()
            if self.profile["enabled"][kind]:
                self._series[kind] = request.lines()
            else:
                self._series.pop(kind, None)
        for slot, plot in zip(self.profile["slots"], self.si_plots, strict=True):
            code = slot["indicator"]
            request = self.auxiliary_requests.get(code) or self.derived_requests.get(code)
            lines = request.lines() if request is not None else self._series[code]
            enabled = indicator_enabled(self.profile, code)
            plot.unit_label = request.unit() if request is not None else ""
            plot.set_styles(
                request.styles() if code in self.derived_requests else {},
                histogram=code == "STS",
            )
            plot.setToolTip(request.label.toolTip() if request is not None else "")
            # Only this pane owns these generated tuples. Keep the public set_series
            # behavior for callers that explicitly resubmit a mutable lines object.
            if plot.code != code or plot.lines is not lines or plot.enabled != enabled:
                plot.set_series(code, lines, enabled)
            plot.setVisible(slot["visible"])
        self._selection_readout(self.chart.selected_timestamp)
        self._update_minimum_height()

    def apply_profile(self, profile):
        self.profile = validate_profile(profile)
        self.chart.set_display_preferences(
            timezone=self.profile["timezone"], ema_period=self.profile["ema_period"]
        )
        self.chart.set_options(ema=self.profile["ema"], volume=False)
        ema_block, volume_block = QSignalBlocker(self.ema), QSignalBlocker(self.volume)
        self.ema.setText(f"EMA{self.profile['ema_period']}")
        self.ema.setChecked(self.profile["ema"])
        self.volume.setChecked(
            any(slot["visible"] for slot in self.profile["slots"] if slot["indicator"] == "VOL")
        )
        self.volume.setEnabled(any(slot["indicator"] == "VOL" for slot in self.profile["slots"]))
        del ema_block, volume_block
        self._refresh_indicators()

    def _dmi_context(self):
        return self._producer_context(
            "DMIxADX", {"nSamples": self.profile["dmi_period"]}, self.profile["enabled"]["DMIxADX"]
        )

    def _producer_context(self, kind, settings, enabled):
        if not enabled:
            return None, "off"
        if self._raw_source != "database":
            return None, "source_unavailable"
        if self.minutes not in {1, 3, 5, 15, 30, 60, 120, 240, 360, 480, 720, 1440}:
            return None, "period_unavailable"
        if not self.owner._series_supported or not self.owner._series_epoch:
            return None, "api_unavailable"
        if not self.owner.client.attached:
            return None, "disconnected"
        if self._raw_epoch != self.owner._series_epoch:
            return None, "awaiting_data"
        if not self.raw or len(self.raw) > 10000:
            return None, "range_unavailable"
        start, end = self.raw[0].open_time_ms, self.raw[-1].open_time_ms + 60000
        if not 0 < end - start or kind not in ANALYSIS_KINDS and end - start > 10000 * 60000:
            return None, "range_unavailable"
        params = {
            "instrument": self.selection.text().strip(),
            "start_ms": start,
            "end_ms": end,
            "period_minutes": self.minutes,
            "kind": kind,
            **settings,
        }
        key = (self.owner._series_epoch, self._raw_revision, tuple(params.items()))
        return (key, params), "ready"

    def _ensure_dmi(self):
        context, reason = self._dmi_context()
        if context is None:
            if reason != "disconnected":
                self._dmi_key = self._dmi_id = self._dmi_hash = self._dmi_tag = None
            self._dmi_state = reason
            if self._dmi_lines:
                self._dmi_lines = {}
            self._dmi_status()
            return
        key, params = context
        if key != self._dmi_key:
            self._dmi_key, self._dmi_parameters = key, params
            self._dmi_id, self._dmi_hash = uuid4().hex, None
            self._dmi_lines, self._dmi_state = {}, "queued"
            self._send_dmi("POST")
        elif self._dmi_state == "disconnected":
            self._dmi_state = (
                "unknown" if self._dmi_lookup_policy.permits_auto("unknown") else "lookup_paused"
            )
        self._dmi_status()

    def _send_dmi(self, method):
        if (
            self.owner._series_closed
            or self not in self.owner.panes
            or not self.owner.client.attached
        ):
            return
        self._dmi_serial += 1
        self._dmi_lookup_policy.bind(self._dmi_id)
        tag = f"chart-series:{id(self)}:{self._dmi_serial}"
        self._dmi_tag = tag
        self._sync_dmi_polling()
        if method == "POST":
            self.owner.client.request(
                "POST",
                "/api/chart-series",
                {"id": self._dmi_id, "parameters": self._dmi_parameters},
                tag=tag,
            )
        else:
            self.owner.client.request("GET", "/api/chart-series/" + self._dmi_id, tag=tag)

    def _poll_dmi(self):
        if (
            self._dmi_lookup_policy.permits_auto(self._dmi_state)
            and not self._dmi_tag
            and self._dmi_id
            and self._dmi_state in {"queued", "running", "unknown"}
            and self.isVisible()
            and self.owner.client.attached
            and not self.owner._series_closed
        ):
            context, _ = self._dmi_context()
            if context and context[0] == self._dmi_key:
                self._send_dmi("GET")

    def _retry_dmi(self):
        if self._dmi_state == "lookup_paused":
            if (
                self._dmi_id
                and not self._dmi_tag
                and not self.owner._series_closed
                and self in self.owner.panes
                and self.owner.client.attached
            ):
                context, _ = self._dmi_context()
                if context and context[0] == self._dmi_key:
                    self._send_dmi("GET")
            return
        if self._dmi_state not in {"failed", "cancelled", "expired"}:
            return
        self._dmi_key = None
        self._refresh_indicators()

    def _dmi_result(self, tag, response):
        if (
            tag != self._dmi_tag
            or self.owner._series_closed
            or self not in self.owner.panes
            or not self.owner.client.attached
        ):
            return
        self._dmi_tag = None
        context, _ = self._dmi_context()
        if not context or context[0] != self._dmi_key:
            return
        try:
            params = response.get("parameters")
            expected_hash = sha256(
                json.dumps(
                    self._dmi_parameters, sort_keys=True, separators=(",", ":"), allow_nan=False
                ).encode()
            ).hexdigest()
            if (
                response.get("id") != self._dmi_id
                or response.get("core_epoch") != self.owner._series_epoch
                or params != self._dmi_parameters
                or not isinstance(params, dict)
                or any(
                    type(params.get(field)) is not int
                    for field in ("start_ms", "end_ms", "period_minutes", "nSamples")
                )
                or response.get("state")
                not in {"queued", "running", "complete", "failed", "cancelled", "expired"}
                or response.get("spec_hash") != expected_hash
                or self._dmi_hash not in (None, response["spec_hash"])
            ):
                raise ValueError("chart series identity changed")
            self._dmi_state = response["state"]
            if self._dmi_state == "complete":
                self._dmi_lines = self._parse_dmi_result(response.get("result"))
            elif self._dmi_state in {"failed", "cancelled", "expired"}:
                self._dmi_lines = {}
        except (ValueError, TypeError, KeyError, OverflowError):
            self._dmi_state = (
                "lookup_paused" if self._dmi_lookup_policy.record_failure() else "unknown"
            )
            self._dmi_lines = {}
        else:
            self._dmi_hash = response["spec_hash"]
            self._dmi_lookup_policy.accept_response()
        self._refresh_indicators()

    def _parse_dmi_result(self, result):
        params = self._dmi_parameters
        if (
            not isinstance(result, dict)
            or result.get("source") != "database"
            or result.get("instrument") != params["instrument"]
            or type(result.get("period_minutes")) is not int
            or result["period_minutes"] != params["period_minutes"]
            or type(result.get("nSamples")) is not int
            or result["nSamples"] != params["nSamples"]
            or result.get("knowledge_mode") != "retrospective_display"
            or type(result.get("as_of_ms")) is not int
            or type(result.get("produced_at_ms")) is not int
            or not params["end_ms"]
            <= result["as_of_ms"]
            <= result["produced_at_ms"]
            <= 253_402_300_799_000
            or re.fullmatch(r"[a-f0-9]{64}", str(result.get("source_checksum"))) is None
            or not isinstance(result.get("quality"), dict)
            or type(result.get("rows")) is not list
            or len(result["rows"]) > 10000
        ):
            raise ValueError("invalid retrospective chart series")
        values, previous = {}, -1
        width = params["period_minutes"] * 60000
        for row in result["rows"]:
            opening = row.get("open_time_ms") if isinstance(row, dict) else None
            if (
                type(opening) is not int
                or opening <= previous
                or opening % width
                or not params["start_ms"] <= opening < opening + width <= params["end_ms"]
                or type(row.get("close_time_ms")) is not int
                or row["close_time_ms"] != opening + width
                or row["close_time_ms"] > result["as_of_ms"]
                or type(row.get("available_at_ms")) is not int
                or row["available_at_ms"] != result["produced_at_ms"]
            ):
                raise ValueError("invalid closed target chart candle")
            for field in ("DMIxADX", "ABSMA", "ABSMAREL"):
                if field not in row:
                    raise ValueError("missing chart series field")
                value = row.get(field)
                if value is not None and (
                    type(value) not in {int, float} or not math.isfinite(value)
                ):
                    raise ValueError("invalid chart series number")
            values[opening] = row
            previous = opening
        self._dmi_metadata = {
            key: result[key]
            for key in (
                "source_checksum",
                "as_of_ms",
                "produced_at_ms",
                "knowledge_mode",
                "quality",
            )
        }
        return {
            field: tuple(
                values.get(timestamp, {}).get(field) for timestamp in self.chart.timestamps
            )
            for field in ("DMIxADX", "ABSMA", "ABSMAREL")
        }

    def _dmi_failed(self, tag, _message):
        if tag != self._dmi_tag or self.owner._series_closed or self not in self.owner.panes:
            return
        self._dmi_tag = None
        self._dmi_state = "lookup_paused" if self._dmi_lookup_policy.record_failure() else "unknown"
        self._dmi_lines = {}
        self._refresh_indicators()

    def _sync_dmi_polling(self):
        timer = getattr(self, "_dmi_timer", None)
        if timer is None:
            return
        if self._dmi_id:
            self._dmi_lookup_policy.bind(self._dmi_id)
        if (
            self._dmi_state in {"queued", "running", "unknown"}
            and self._dmi_id
            and not self._dmi_lookup_policy.permits_auto(self._dmi_state)
        ):
            self._dmi_state = "lookup_paused"
        active = (
            self._dmi_lookup_policy.permits_auto(self._dmi_state)
            and not self._dmi_tag
            and not self.owner._series_closed
            and self in self.owner.panes
            and self.owner.client.attached
        )
        if active:
            if not timer.isActive():
                timer.start()
        else:
            timer.stop()

    def _dmi_status(self):
        self._sync_dmi_polling()
        enabled = self.profile["enabled"]["DMIxADX"]
        self.producer_status.setVisible(enabled)
        self.producer_retry.setVisible(
            enabled and self._dmi_state in {"failed", "cancelled", "expired", "lookup_paused"}
        )
        self.producer_retry.setText(
            ("같은 요청 조회" if self.owner.language == "ko" else "Check same request")
            if self._dmi_state == "lookup_paused"
            else ("DMI 다시 계산" if self.owner.language == "ko" else "Recalculate DMI")
        )
        labels = {
            "source_unavailable": (
                "DB 관측만 지원 · 합성/demo 차트에 혼합하지 않음",
                "DB observations only; never mixed into synthetic/demo charts",
            ),
            "period_unavailable": (
                "DMI는 1m~1d 지원 기간만 가능 · 3d/주/월 미지원",
                "DMI supports installed 1m–1d periods; 3d/week/month unavailable",
            ),
            "api_unavailable": ("코어 DMI 시계열 API 미지원", "Core DMI series API unavailable"),
            "awaiting_data": ("새 코어의 봉 스냅샷 대기", "Awaiting candles from the fresh core"),
            "range_unavailable": (
                "최대 10,000개 원본 1분 범위 필요",
                "A bounded range of at most 10,000 source minutes is required",
            ),
            "disconnected": ("코어 연결 끊김", "Core disconnected"),
            "queued": ("코어 계산 대기", "Queued for core calculation"),
            "running": ("코어 계산 중", "Core calculation running"),
            "unknown": (
                "결과 미확인 · 같은 작업 조회 중",
                "Outcome unconfirmed; checking the same job",
            ),
            "lookup_paused": (
                "결과 확인 실패 · 자동 조회 중지, 같은 요청을 다시 조회할 수 있습니다",
                "Lookup failed; automatic checks paused. Check the same request explicitly.",
            ),
            "complete": (
                "DB 과거 표시 계산 완료 · 당시 인지 자료를 뜻하지 않음",
                "Retrospective DB display calculation complete; "
                "not knowledge at the historical close",
            ),
            "failed": (
                "계산 실패 · 명시 재요청 가능",
                "Calculation failed; explicit retry available",
            ),
            "cancelled": ("계산 취소됨", "Calculation cancelled"),
            "expired": ("결과 만료 · 기존 값 제거됨", "Result expired; previous values cleared"),
        }
        words = labels.get(self._dmi_state, ("DMI 꺼짐", "DMI disabled"))
        message = "DMIxADX · " + words[0 if self.owner.language == "ko" else 1]
        if self._dmi_state == "complete" and not any(
            value is not None for value in self._dmi_lines.get(self.profile["dmi_display"], ())
        ):
            message += (
                " · 준비 표본 부족 / 값 없음"
                if self.owner.language == "ko"
                else " · Warmup / no values in this range"
            )
        self.producer_status.setText(message)
        self.producer_status.setToolTip(
            json.dumps(self._dmi_metadata, ensure_ascii=False)[:4000]
            if self._dmi_state == "complete"
            else ""
        )

    def _toggle_ema(self, checked):
        profile = deepcopy(self.profile)
        profile["ema"] = checked
        self.apply_profile(profile)
        self.settings_panel.ema.setChecked(checked)

    def _toggle_volume(self, checked):
        profile = deepcopy(self.profile)
        for slot in profile["slots"]:
            if slot["indicator"] == "VOL":
                slot["visible"] = checked
        self.apply_profile(profile)
        for index, slot in enumerate(self.settings_panel._slots):
            if slot["indicator"] == "VOL":
                self.settings_panel.displays[index].setChecked(checked)

    def fit_all(self):
        self.chart.fit_all()
        for plot in self.si_plots:
            plot._manual_range = None
            plot.refresh_view()

    def _selection_readout(self, timestamp):
        self.selected_readout.setVisible(timestamp is not None)
        rows = self.chart.candles
        index = next(
            (
                i
                for i, row in enumerate(rows)
                if timestamp is not None
                and row.open_time_ms <= timestamp < self.chart._bar_end(row.open_time_ms)
            ),
            None,
        )
        if index is None:
            self.selected_readout.setText(
                "선택 시각의 봉 없음"
                if self.owner.language == "ko"
                else "No candle at the selected timestamp"
            )
            return
        row = rows[index]
        label = display_date(row.open_time_ms, self.profile["timezone"]).strftime(
            "%Y/%m/%d %H:%M %Z"
        )
        values = [label, f"O {row.open:g} H {row.high:g} L {row.low:g} C {row.close:g}"]
        for code, lines in self._series.items():
            if not indicator_enabled(self.profile, code):
                continue
            for name, line in lines.items():
                value = line[index]
                request = self.auxiliary_requests.get(code) or self.derived_requests.get(code)
                unit = " " + request.unit() if request is not None and request.unit() else ""
                missing = (
                    request.missing_label(row.open_time_ms, name)
                    if code in self.derived_requests
                    else "missing"
                    if request is not None
                    else "warmup"
                )
                values.append(
                    f"{name} {value:.6g}{unit}" if value is not None else f"{name} {missing}{unit}"
                )
        values.extend(self.price_windows_request.readout(row.open_time_ms))
        self.selected_readout.setText(" · ".join(values))
        self._update_minimum_height()

    def update_strategies(self, strategies, epoch=None):
        if epoch is not None and epoch != self._epoch:
            self._epoch = epoch
            self._fill_tag, self._fill_revision = None, None
            self.chart.set_fill_markers([])
        self._strategies = dict(strategies)
        selected = self.trade_overlay.currentData()
        blocker = QSignalBlocker(self.trade_overlay)
        self.trade_overlay.clear()
        self.trade_overlay.addItem(
            "체결 표시 없음" if self.owner.language == "ko" else "No fill overlay", ""
        )
        for identity, row in self._strategies.items():
            if row.get("instrument") == self.selection.text():
                self.trade_overlay.addItem(
                    f"PAPER · {row.get('account_id', '')} / {identity}", identity
                )
        index = self.trade_overlay.findData(selected)
        self.trade_overlay.setCurrentIndex(max(0, index))
        del blocker
        if self.trade_overlay.currentData() != selected:
            self._select_overlay()
        else:
            self._refresh_overlay()

    def _select_overlay(self, *_args):
        self._fill_tag, self._fill_revision = None, None
        self.chart.set_fill_markers([])
        self.overlay_status.clear()
        self._refresh_overlay()

    def _refresh_overlay(self):
        identity = self.trade_overlay.currentData()
        if not identity or not self.owner.client.attached or self._fill_tag:
            return
        row = self._strategies.get(identity, {})
        revision = (identity, row.get("revision"), row.get("fill_count"))
        if revision == self._fill_revision:
            return
        self._fill_revision = revision
        self._fill_generation += 1
        self._fill_tag = f"chart-fills:{id(self)}:{self._fill_generation}"
        self.overlay_status.setText(
            "PAPER 체결 조회 중" if self.owner.language == "ko" else "Loading paper fills"
        )
        self.owner.client.request(
            "GET", "/api/strategies/" + quote(identity, safe=""), tag=self._fill_tag
        )

    def _fill_result(self, tag, response):
        if tag != self._fill_tag:
            return
        self._fill_tag = None
        identity = self.trade_overlay.currentData()
        expected = self._strategies.get(identity, {})
        try:
            if (
                response.get("instance") != identity
                or response.get("instrument") != self.selection.text()
                or response.get("account_id") != expected.get("account_id")
            ):
                raise ValueError("chart fill response strategy/account/instrument mismatch")
            self.chart.set_fill_markers(response.get("fills", []))
        except (TypeError, ValueError) as error:
            self.chart.set_fill_markers([])
            self._fill_revision = None
            self.overlay_status.setText(str(error))
            return
        self.overlay_status.setText(
            f"PAPER 체결 {len(self.chart.fill_markers)}개 · 최근 최대 1,000개 · 실체결 아님"
            if self.owner.language == "ko"
            else f"PAPER {len(self.chart.fill_markers)} fills · latest 1,000 max · no live fills"
        )
        self._refresh_overlay()

    def _update_status(self) -> None:
        language = self.owner.language
        self.source_label.setText(
            text(self.source_kind, language)
            if self.source_kind in _TEXT
            else self.source_kind.upper()
        )
        self.source_label.setToolTip(self.source_kind)
        frame = next(frame for frame, value in TIMEFRAMES.items() if value == self.minutes)
        count = len(self.chart.candles)
        calendar = self.minutes in CALENDAR_PERIODS
        capacity = text("calendar_capacity", language).format(frame=frame) if calendar else ""
        if calendar and self.owner._calendar_api:
            if self._calendar_state == "failed" or self.source_kind == "unavailable":
                message = text("calendar_unavailable", language).format(frame=frame)
                self.source_label.setText(
                    text("failed", language) if self._calendar_state == "failed" else "UNAVAILABLE"
                )
                if self._calendar_error:
                    message += " " + self._calendar_error[:200]
            elif self.minutes not in self._calendar_data:
                message = text("calendar_loading", language).format(frame=frame)
            else:
                message = text("calendar_counts", language).format(
                    frame=frame,
                    bars=count,
                    minutes=self.quality.get("source_minutes", 0),
                    missing=self.quality.get("missing_minutes", 0),
                    partial=self.quality.get("partial_periods", 0),
                )
            self.detail.setText(message)
            self.chart.empty_message = message if not count else ""
        elif not self.raw:
            message = capacity if calendar else text("empty", language)
            self.chart.empty_message = message
            self.detail.setText(message)
        elif not count:
            message = (
                capacity
                if calendar
                else text("incomplete", language).format(frame=frame, minutes=self.minutes)
            )
            self.chart.empty_message = message
            self.detail.setText(message)
        else:
            self.chart.empty_message = ""
            self.detail.setText(
                text("counts", language).format(
                    raw=len(self.raw),
                    frame=frame,
                    bars=count,
                    omitted=len(self.raw) - covered_minutes(self.chart.candles, self.minutes),
                )
            )
        badges = []
        missing = self.quality.get("missing_minutes", 0)
        if isinstance(missing, int) and missing:
            badges.append(text("gap", language).format(count=missing))
        try:
            ratio = float(self.quality.get("interpolation_rate", 0))
        except (ValueError, TypeError):
            ratio = 0
        if 0 < ratio <= 1:
            badges.append(text("interpolated", language).format(ratio=ratio))
        self.quality_badge.setText(" · ".join(badges))
        self.detail.setToolTip(json.dumps(self.quality, ensure_ascii=False, indent=2)[:4000])
        self.chart.update()

    def mark_disconnected(self) -> None:
        self.source_label.setText(text("disconnected", self.owner.language))
        self.source_label.setToolTip(text("disconnected", self.owner.language))
        self._fill_tag, self._fill_revision = None, None
        if self.trade_overlay.currentData():
            self.overlay_status.setText(
                "연결 끊김 · 마지막 PAPER 체결 기록"
                if self.owner.language == "ko"
                else "Disconnected · retained PAPER fills"
            )

    def set_presentation(self) -> None:
        language = self.owner.language
        self.fit.setText(text("fit", language))
        self.settings_button.setText("설정" if language == "ko" else "Settings")
        self.fit.setToolTip(text("fit_hint", language))
        self.volume.setText(text("volume", language))
        self.ema.setToolTip(text("ema_hint", language))
        self.symbol.setToolTip(text("symbol_hint", language))
        self.period_scroll.setToolTip(text("calendar_limit", language))
        self.chart.set_presentation(self.owner.theme, language)
        self.chart.setToolTip(text("interaction_hint", language))
        self.settings_panel.set_presentation()
        for plot in self.si_plots:
            plot.update()
        self._selection_readout(self.chart.selected_timestamp)
        self._update_status()
        self._dmi_status()
        for request in self.auxiliary_requests.values():
            request.status()
        for request in self.price_requests.values():
            request.status()
        for request in self.derived_requests.values():
            request.status()
        self.ivp_request.status()


class ChartsPage(QWidget):
    """Flexible split workspace; adding a pane never starts data collection or trading."""

    def __init__(self, client, parent=None, *, root: Path | None = None):
        super().__init__(parent)
        self.client = client
        self._calendar_api = hasattr(client, "resultReceived")
        self._calendar_sequence = 0
        self._calendar_pending = {}
        self._calendar_requests = {}
        self._calendar_epoch = None
        self._series_epoch, self._series_supported, self._series_closed = None, False, False
        self._analysis_kinds = frozenset()
        self._auxiliary_supported = False
        self.theme, self.language = "light", "ko"
        self.profiles = {}
        self.store = ChartWorkspaceStore(root) if root is not None else None
        self._workspace_warning = ""
        self.rows: list[tuple[QWidget, QLineEdit, QLabel, ChartDrawer]] = []
        self.panes: list[ChartPane] = []
        self._split_rows: list[QSplitter] = []
        self._layout_signature = None
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        self.title = QLabel()
        self.title.setProperty("role", "title")
        self.layout_mode = QComboBox()
        for mode in ("grid", "columns", "rows"):
            self.layout_mode.addItem(mode, mode)
        layout.addWidget(self.title)
        toolbar = QGridLayout()
        self.add_button, self.refresh_button, self.reset_button = (QPushButton() for _ in range(3))
        self.save_button = QPushButton()
        self.linked = QCheckBox()
        self.linked.setChecked(True)
        for index, button in enumerate((self.add_button, self.refresh_button, self.reset_button)):
            toolbar.addWidget(button, 0, index)
        toolbar.setColumnStretch(3, 1)
        toolbar.addWidget(self.linked, 1, 0, 1, 2)
        toolbar.addWidget(self.layout_mode, 1, 2)
        toolbar.addWidget(self.save_button, 1, 3)
        layout.addLayout(toolbar)
        self.notice = QLabel()
        self.notice.setWordWrap(True)
        self.notice.setProperty("role", "muted")
        layout.addWidget(self.notice)
        self.splitter = QSplitter(Qt.Orientation.Vertical)
        self.splitter.setChildrenCollapsible(False)
        self.scroll = QScrollArea()
        self.scroll.setWidgetResizable(True)
        self.scroll.setWidget(self.splitter)
        layout.addWidget(self.scroll, 1)
        self.add_button.clicked.connect(lambda: self.add_chart())
        self.refresh_button.clicked.connect(self.refresh)
        self.reset_button.clicked.connect(self.reset_views)
        self.save_button.clicked.connect(self.save_workspace)
        self.layout_mode.currentIndexChanged.connect(self._reflow)
        client.candlesReceived.connect(self.accept_candles)
        client.failed.connect(self._failed)
        client.connectionChanged.connect(self._connection_changed)
        if self._calendar_api:
            client.resultReceived.connect(self._calendar_result)
        saved = None
        try:
            saved = self.store.load() if self.store else None
        except (OSError, ValueError, TypeError, UnicodeError) as error:
            self._workspace_warning = f"Saved chart workspace ignored: {error}"
        if saved:
            self.profiles = saved["profiles"]
            self.layout_mode.setCurrentIndex(self.layout_mode.findData(saved["layout"]))
            self.linked.setChecked(saved["linked"])
            for value in saved["panes"]:
                self.add_chart(value["instrument"])
                pane = self.panes[-1]
                pane.apply_profile(value["profile"])
                pane.settings_panel.set_draft(value["profile"])
                pane.set_timeframe(value["timeframe"])
        else:
            self.add_chart("binance:usdm:BTCUSDT")
        self.set_presentation("light", "ko")

    def set_shell_embedded(self, embedded: bool) -> None:
        """The shell already supplies the page title; retain controls and source notes."""
        self.title.setVisible(not embedded)

    def add_chart(self, instrument="binance:spot:BTCUSDT") -> None:
        if len(self.panes) >= 6:
            return
        pane = ChartPane(self, instrument)
        self.panes.append(pane)
        self.rows.append((pane, pane.selection, pane.source_label, pane.chart))
        self._reflow()
        self.add_button.setEnabled(len(self.rows) < 6)
        self.refresh()

    def _remove_pane(self, pane: ChartPane) -> None:
        if len(self.panes) == 1:
            return
        pane._dmi_timer.stop()
        pane.ivp_request.timer.stop()
        pane.price_windows_request.timer.stop()
        for request in pane.derived_requests.values():
            request.cancel()
            request.timer.stop()
        for request in pane.price_requests.values():
            request.timer.stop()
        self.panes.remove(pane)
        self.rows = [row for row in self.rows if row[0] is not pane]
        pane.setParent(None)
        pane.deleteLater()
        self._reflow()
        self.add_button.setEnabled(True)

    def _remove(self, record) -> None:
        self._remove_pane(record[0])

    def _reflow(self, *_args) -> None:
        mode = self.layout_mode.currentData()
        if mode == "grid":
            width = self.scroll.viewport().width()
            columns = 3 if width >= 1250 else 2 if width >= 700 else 1
            columns = min(columns, math.ceil(math.sqrt(len(self.panes))))
        else:
            columns = len(self.panes) if mode == "columns" else 1
        columns = max(1, min(columns, len(self.panes)))
        signature = mode, columns, len(self.panes)
        if signature == self._layout_signature:
            return
        self._layout_signature = signature
        for pane in self.panes:
            pane.setParent(None)
        for row in self._split_rows:
            row.setParent(None)
            row.deleteLater()
        self._split_rows.clear()
        for start in range(0, len(self.panes), columns):
            row = QSplitter(Qt.Orientation.Horizontal)
            row.setChildrenCollapsible(False)
            for pane in self.panes[start : start + columns]:
                row.addWidget(pane)
                pane.show()
                pane.close_button.setEnabled(len(self.panes) > 1)
            row.setSizes([1000] * row.count())
            self.splitter.addWidget(row)
            self._split_rows.append(row)
        self.splitter.setMinimumWidth(320 * columns)
        self._refresh_minimum_geometry()
        self.splitter.setSizes([1000] * len(self._split_rows))

    def _refresh_minimum_geometry(self):
        for row in self._split_rows:
            row.setMinimumHeight(
                max((row.widget(i).minimumHeight() for i in range(row.count())), default=355)
            )
        self.splitter.setMinimumHeight(
            sum(row.minimumHeight() for row in self._split_rows)
            + self.splitter.handleWidth() * max(0, len(self._split_rows) - 1)
        )

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        QTimer.singleShot(0, self._reflow)

    def _link_range(self, source, start, stop) -> None:
        if self.linked.isChecked():
            for pane in self.panes:
                if pane.chart is not source:
                    pane.chart.set_time_range(start, stop)
                    # set_time_range deliberately does not re-emit rangeChanged
                    # (which would recursively link panes). Its SI needs one update.
                    for plot in pane.si_plots:
                        plot.refresh_view()

    def _link_cursor(self, source, timestamp) -> None:
        if self.linked.isChecked():
            for pane in self.panes:
                if pane.chart is not source:
                    pane.chart.set_cursor(timestamp)
                    for plot in pane.si_plots:
                        plot.update()

    def update_state(self, snapshot):
        previous_epoch = self._series_epoch
        previous_supported = self._series_supported
        previous_auxiliary = self._auxiliary_supported
        previous_analysis = self._analysis_kinds
        self._series_epoch = snapshot.get("core", {}).get("epoch")
        self._series_supported = (snapshot.get("core", {}).get("capabilities") or {}).get(
            "chart_series"
        ) is True and self._calendar_api
        declared = (snapshot.get("core", {}).get("capabilities") or {}).get("chart_analysis_kinds")
        self._analysis_kinds = (
            frozenset(declared)
            if (
                type(declared) in {list, tuple}
                and len(declared) <= len(ANALYSIS_KINDS)
                and all(type(kind) is str and kind in ANALYSIS_KINDS for kind in declared)
                and len(set(declared)) == len(declared)
            )
            else frozenset()
        )
        self._auxiliary_supported = (snapshot.get("core", {}).get("capabilities") or {}).get(
            "chart_observations"
        ) is True and self._calendar_api
        epoch = snapshot.get("core", {}).get("epoch")
        if epoch is not None and self._calendar_epoch not in (None, epoch):
            self._invalidate_calendar()
        if epoch is not None:
            self._calendar_epoch = epoch
        strategies = {
            row.get("instance", row.get("id", "")): row for row in snapshot.get("strategies", [])
        }
        for pane in self.panes:
            pane.update_strategies(strategies, snapshot.get("core", {}).get("epoch"))
            if (
                previous_epoch != self._series_epoch
                or previous_supported != self._series_supported
                or previous_auxiliary != self._auxiliary_supported
                or previous_analysis != self._analysis_kinds
            ):
                pane._refresh_indicators()
        if previous_epoch != self._series_epoch and any(
            pane.profile["enabled"]["DMIxADX"]
            or pane.profile["ivp"]["enabled"]
            or any(indicator_enabled(pane.profile, kind) for kind in DERIVED_INDICATORS)
            or any(pane.profile[group]["enabled"] for group in ("psar", "swing"))
            or any(
                pane.profile["price_windows"][group]["enabled"] for group in ("sma", "wma", "bol")
            )
            or any(pane.profile["enabled"][kind] for kind in AUXILIARY_INDICATORS)
            for pane in self.panes
        ):
            self.refresh()

    def workspace_snapshot(self):
        return {
            "schema_version": 1,
            "profiles": deepcopy(self.profiles),
            "layout": self.layout_mode.currentData(),
            "linked": self.linked.isChecked(),
            "panes": [
                {
                    "instrument": pane.selection.text(),
                    "timeframe": pane.minutes,
                    "profile": deepcopy(pane.profile),
                }
                for pane in self.panes
            ],
        }

    def _save(self, value):
        if self.store is None:
            raise ValueError("Chart workspace storage is not configured")
        self.store.save(value)

    def save_workspace(self):
        try:
            self._save(self.workspace_snapshot())
        except (OSError, ValueError) as error:
            self.notice.setText(str(error))
            return False
        self.notice.setText(
            "차트 작업 공간 저장됨" if self.language == "ko" else "Chart workspace saved"
        )
        return True

    def save_profile(self, pane, name, profile):
        snapshot = self.workspace_snapshot()
        snapshot["profiles"][name] = validate_profile(profile)
        snapshot["panes"][self.panes.index(pane)]["profile"] = profile
        self._save(snapshot)
        self.profiles = snapshot["profiles"]
        for item in self.panes:
            item.settings_panel.refresh_profiles()

    def delete_profile(self, name):
        snapshot = self.workspace_snapshot()
        snapshot["profiles"].pop(name, None)
        self._save(snapshot)
        self.profiles = snapshot["profiles"]
        for item in self.panes:
            item.settings_panel.refresh_profiles()

    def reset_views(self) -> None:
        targets = self.panes[:1] if self.linked.isChecked() else self.panes
        for pane in targets:
            pane.chart.reset_view()

    def refresh(self) -> None:
        if not self.client.attached:
            return
        for pane in self.panes:
            if pane.minutes in CALENDAR_PERIODS and self._calendar_api:
                self._request_calendar(pane)
        instruments = {
            pane.selection.text().strip()
            for pane in self.panes
            if pane.minutes not in CALENDAR_PERIODS or not self._calendar_api
        }
        for instrument in instruments:
            if instrument:
                include_epoch = any(
                    pane.selection.text().strip() == instrument
                    and price_window_definitions(pane.profile["price_windows"])
                    for pane in self.panes
                )
                suffix = "&include_core_epoch=1" if include_epoch else ""
                self.client.request(
                    "GET",
                    f"/api/candles?instrument={quote(instrument, safe='')}&limit=10000{suffix}",
                    tag="candles:" + instrument,
                )

    def _invalidate_calendar(self):
        self._calendar_pending.clear()
        self._calendar_requests.clear()
        for pane in self.panes:
            pane._calendar_generation += 1
            pane._calendar_data.clear()
            pane._calendar_state, pane._calendar_error = "awaiting", ""
            if pane.minutes in CALENDAR_PERIODS:
                pane._render_interval()

    def _request_calendar(self, pane):
        if (
            not self._calendar_api
            or not self.client.attached
            or pane.minutes not in CALENDAR_PERIODS
        ):
            return
        key = pane.selection.text().strip(), pane.minutes
        waiter = pane, pane._calendar_generation
        pane._calendar_state, pane._calendar_error = "loading", ""
        pane._update_status()
        pending = self._calendar_pending.get(key)
        if pending is not None:
            waiters = self._calendar_requests[pending][1]
            if waiter not in waiters:
                waiters.append(waiter)
            return
        self._calendar_sequence += 1
        tag = f"chart-calendar:{id(self)}:{self._calendar_sequence}"
        self._calendar_pending[key] = tag
        self._calendar_requests[tag] = key, [waiter]
        instrument, period = key
        limit = 52 if period == "1w" else 12
        self.client.request(
            "GET",
            f"/api/chart-candles?instrument={quote(instrument, safe='')}"
            f"&period={period}&limit={limit}",
            tag=tag,
        )

    def _calendar_waiters(self, tag):
        request = self._calendar_requests.pop(tag, None)
        if request is None:
            return None, []
        key, waiters = request
        if self._calendar_pending.get(key) == tag:
            self._calendar_pending.pop(key, None)
        active = [
            pane
            for pane, generation in waiters
            if pane in self.panes
            and pane._calendar_generation == generation
            and (pane.selection.text().strip(), pane.minutes) == key
        ]
        return key, active

    @staticmethod
    def _parse_calendar(response, key):
        instrument, period = key
        if (
            type(response.get("schema_version")) is not int
            or response.get("schema_version") != 1
            or response.get("instrument") != instrument
            or response.get("period") != period
            or response.get("source") not in {"database", "unavailable"}
        ):
            raise ValueError("calendar response identity/source mismatch")
        begin, end, cutoff = (
            response.get(name) for name in ("begin_ms", "end_ms", "closed_until_ms")
        )
        if (
            any(type(value) is not int or value % 60000 for value in (begin, end, cutoff))
            or end < begin
        ):
            raise ValueError("calendar response bounds are invalid")
        rows = parse_candles(response.get("candles", []))
        if len(rows) > (52 if period == "1w" else 12):
            raise ValueError("calendar response exceeds requested bar count")
        quality = response.get("quality")
        if not isinstance(quality, dict) or quality.get("status") not in {
            "observed",
            "gapped",
            "partial",
            "unavailable",
        }:
            raise ValueError("calendar response quality is invalid")
        for field in ("source_minutes", "missing_minutes", "partial_periods"):
            if type(quality.get(field)) is not int or quality[field] < 0:
                raise ValueError("calendar response counts are invalid")
        bars = quality.get("bars")
        if not isinstance(bars, list) or len(bars) > 512:
            raise ValueError("calendar response period quality is unbounded")
        complete = set()
        previous = None
        for bar in bars:
            if not isinstance(bar, dict):
                raise ValueError("calendar period quality must be an object")
            start, stop = bar.get("start_ms"), bar.get("end_ms")
            if (
                type(start) is not int
                or type(stop) is not int
                or calendar_bounds(start, period) != (start, stop)
            ):
                raise ValueError("calendar period quality boundaries are invalid")
            if previous is not None and start != previous:
                raise ValueError("calendar period quality must cover contiguous calendar intervals")
            previous = stop
            if not start < end or not stop > begin:
                raise ValueError("calendar period quality is outside the request")
            fields = (
                "expected_minutes",
                "requested_minutes",
                "closed_minutes",
                "observed_minutes",
                "missing_minutes",
            )
            if any(type(bar.get(field)) is not int or bar[field] < 0 for field in fields):
                raise ValueError("calendar period quality counts are invalid")
            if any(
                type(bar.get(field)) is not bool
                for field in ("prefix_partial", "suffix_partial", "is_closed", "complete")
            ):
                raise ValueError("calendar period quality flags are invalid")
            expected = (stop - start) // 60000
            requested = (min(end, stop) - max(begin, start)) // 60000
            closed = max(0, min(end, stop, cutoff) - max(begin, start)) // 60000
            observed = bar["observed_minutes"]
            if (
                bar["expected_minutes"] != expected
                or bar["requested_minutes"] != requested
                or bar["closed_minutes"] != closed
                or observed > closed
                or bar["missing_minutes"] != closed - observed
                or bar["prefix_partial"] != (begin > start)
                or bar["suffix_partial"] != (end < stop)
                or bar["is_closed"] != (stop <= cutoff)
                or bar["complete"] != (stop <= cutoff and observed == expected)
            ):
                raise ValueError("calendar period quality is inconsistent")
            if bar["complete"]:
                complete.add(start)
        if response["source"] == "database" and begin < end:
            if (
                not bars
                or bars[0]["start_ms"] != calendar_bounds(begin, period)[0]
                or bars[-1]["end_ms"] < end
            ):
                raise ValueError("calendar period quality does not cover the requested range")
        if {row.open_time_ms for row in rows} != complete:
            raise ValueError("calendar candles do not match complete period quality")
        if quality["source_minutes"] != sum(bar["observed_minutes"] for bar in bars) or quality[
            "missing_minutes"
        ] != sum(bar["missing_minutes"] for bar in bars):
            raise ValueError("calendar summary counts do not match period quality")
        if response["source"] == "unavailable" and (rows or quality["status"] != "unavailable"):
            raise ValueError("unavailable calendar response contains observed bars")
        return rows, response["source"], deepcopy(quality)

    def _calendar_result(self, tag, response):
        key, panes = self._calendar_waiters(tag)
        if not panes:
            return
        try:
            value = self._parse_calendar(response, key)
        except (TypeError, ValueError, KeyError, OverflowError) as error:
            for pane in panes:
                pane._calendar_state, pane._calendar_error = "failed", str(error)
                pane._update_status()
            return
        for pane in panes:
            pane._calendar_data[key[1]] = value
            pane._calendar_state, pane._calendar_error = "ready", ""
            pane._render_interval()

    def accept_candles(self, instrument: str, response: dict) -> None:
        content = None
        content_epoch = response.get("core_epoch")
        if type(content_epoch) is str and content_epoch and content_epoch == self._series_epoch:
            try:
                # Preserve exact wire content before float parsing, accepting only
                # a provider read explicitly tied to the current core epoch.
                content = exact_snapshot_pin(instrument, response.get("candles", []))
            except (ValidationError, ValueError, TypeError, KeyError, OverflowError):
                # Legacy snapshots remain displayable, but cannot verify this overlay.
                pass
        try:
            if response.get("instrument", instrument) != instrument:
                raise ValueError("candle response instrument mismatch")
            candles = parse_candles(response.get("candles", []))
            quality = response.get("quality", {})
            if not isinstance(quality, dict):
                raise ValueError("candle quality must be an object")
            source = str(response.get("source", "unknown"))[:64]
        except (ValueError, TypeError, KeyError) as error:
            self._failed("candles:" + instrument, str(error))
            return
        for pane in self.panes:
            if pane.selection.text().strip() == instrument:
                pane.set_snapshot(
                    candles, source, quality, content=content, content_epoch=content_epoch
                )

    def _failed(self, tag: str, message: str) -> None:
        _, calendar_panes = self._calendar_waiters(tag)
        for pane in calendar_panes:
            pane._calendar_state, pane._calendar_error = "failed", message[:300]
            pane._update_status()
        for pane in self.panes:
            if tag == pane._fill_tag:
                pane._fill_tag, pane._fill_revision = None, None
                pane.overlay_status.setText(message[:300])
        if not tag.startswith("candles:"):
            return
        for pane in self.panes:
            if pane.selection.text() == tag.removeprefix("candles:") and not (
                self._calendar_api and pane.minutes in CALENDAR_PERIODS
            ):
                pane.source_label.setText(text("failed", self.language))
                pane.detail.setText(message[:300])

    def _connection_changed(self, attached: bool) -> None:
        if not attached:
            self._calendar_pending.clear()
            self._calendar_requests.clear()
            for pane in self.panes:
                pane._calendar_generation += 1
                pane._price_raw_epoch = None
            for pane in self.panes:
                pane._dmi_tag = None
                pane._dmi_state, pane._dmi_lines = "unknown" if pane._dmi_id else "disconnected", {}
                pane._refresh_indicators()
                pane.mark_disconnected()
        else:
            for pane in self.panes:
                if pane._dmi_id:
                    pane._dmi_state = "unknown"
                pane._refresh_indicators()

    def closeEvent(self, event):
        for pane in self.panes:
            for request in pane.derived_requests.values():
                request.cancel()
        self._series_closed = True
        for pane in self.panes:
            pane._dmi_timer.stop()
            pane.ivp_request.timer.stop()
            pane.price_windows_request.timer.stop()
            for request in pane.derived_requests.values():
                request.timer.stop()
            for request in pane.price_requests.values():
                request.timer.stop()
        super().closeEvent(event)

    def set_presentation(self, theme: str, language: str) -> None:
        self.theme, self.language = theme, language
        self.title.setText(text("title", language))
        self.notice.setText(self._workspace_warning or text("snapshot", language))
        self.add_button.setText(text("add", language))
        self.refresh_button.setText(text("refresh", language))
        self.reset_button.setText(text("reset", language))
        self.save_button.setText("구성 저장" if language == "ko" else "Save layout")
        self.linked.setText(text("linked", language))
        for index in range(self.layout_mode.count()):
            self.layout_mode.setItemText(index, text(self.layout_mode.itemData(index), language))
        for pane in self.panes:
            pane.set_presentation()
