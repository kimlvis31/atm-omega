"""Eta-style structured chart draft, apply and explicit named-profile storage."""

# Imports
from PySide6.QtCore import QSignalBlocker
from PySide6.QtWidgets import (
    QCheckBox,
    QComboBox,
    QFormLayout,
    QGridLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSpinBox,
    QTabWidget,
    QVBoxLayout,
    QWidget,
)

from omega.gui.chart_price_windows import PriceWindowsEditor
from omega.gui.chart_profiles import (
    ANALYSIS_FIELDS,
    ANALYSIS_KINDS,
    AUXILIARY_FIELDS,
    AUXILIARY_INDICATORS,
    INDICATORS,
    LEGACY_DERIVED_INDICATORS,
    LEGACY_INDICATORS,
    SI_SLOTS,
    analysis_profile,
    profile_name,
    swap_slot,
    validate_profile,
)


class ChartSettings(QWidget):
    def __init__(self, pane):
        super().__init__(pane)
        self.pane = pane
        self.tabs = QTabWidget()
        self._loading = False
        self._draft_version = 6
        self._slots = []
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.tabs)
        general, indicators, overlays, profiles, auxiliary = (QWidget() for _ in range(5))
        self.tabs.addTab(general, "Chart")
        self.tabs.addTab(indicators, "SI slots")
        self.tabs.addTab(overlays, "Price overlays")
        self.tabs.addTab(profiles, "Profiles")
        self.tabs.addTab(auxiliary, "Observations")
        form = QFormLayout(general)
        self.zone = QComboBox()
        self.zone.setEditable(True)
        self.zone.addItems(
            ["UTC", "LOCAL", *[f"UTC{hour:+03d}:00" for hour in range(-12, 15) if hour]]
        )
        self.ema = QCheckBox("EMA")
        self.ema_period = self._period(20)
        ema_row = QHBoxLayout()
        ema_row.addWidget(self.ema)
        ema_row.addWidget(self.ema_period)
        self.mfi_period = self._period(14)
        self.mmacd_periods = QLineEdit()
        self.mmacd_periods.setMaxLength(80)
        self.signal_period = self._period(9)
        self.dmi_period = self._period(14)
        self.dmi_display = QComboBox()
        self.dmi_display.addItems(["DMIxADX", "ABSMA", "ABSMAREL"])
        overlay_form = QFormLayout(overlays)
        self.price_windows = PriceWindowsEditor(self)
        overlay_form.addRow(self.price_windows)
        self.overlay_controls = {}
        for group, title, fields in (
            (
                "psar",
                "PSAR (line 0)",
                ("start", "acceleration", "maximum", "price_precision", "color", "width"),
            ),
            ("swing", "SWING (line 0)", ("swing_range", "color", "width")),
        ):
            switch = QCheckBox(title)
            self.overlay_controls[(group, "enabled")] = switch
            overlay_form.addRow(switch)
            for key in fields:
                control = QSpinBox() if key == "price_precision" else QLineEdit()
                if key == "price_precision":
                    control.setRange(0, 16)
                else:
                    control.setMaxLength(64)
                self.overlay_controls[(group, key)] = control
                overlay_form.addRow(f"{title} · {key}", control)
        self.overlay_note = QLabel()
        self.overlay_note.setWordWrap(True)
        overlay_form.addRow(self.overlay_note)
        self.ivp_controls = {}
        for key in (
            "enabled",
            "visible",
            "nSamples",
            "gammaFactor",
            "deltaFactor",
            "prominence",
            "distance",
            "height",
            "price_precision",
            "max_divisions",
            "origin_ms",
            "color",
            "width",
        ):
            control = QCheckBox() if key in {"enabled", "visible"} else QLineEdit()
            if isinstance(control, QLineEdit):
                control.setMaxLength(64)
            self.ivp_controls[key] = control
            overlay_form.addRow("IVP · " + key, control)
        self.ivp_origin_button = QPushButton("IVP 시작점 = 첫 분봉 / Origin = first loaded minute")
        self.ivp_origin_button.clicked.connect(self._ivp_origin)
        overlay_form.addRow(self.ivp_origin_button)
        self.ivp_note = QLabel(
            "IVP: 고정 선택 봉의 OHLCV 해석 분포입니다. origin_ms는 UTC밀리초이며 빈칸은 "
            "종목/주기별 첫 분봉에 고정됩니다. 최대10000분. 경계 band는 미지원.\n"
            "Fixed-origin selected-bar interpreted OHLCV profile, not actual trade-price volume. "
            "Blank origin_ms anchors to the first loaded minute per instrument/period. "
            "Maximum10000minutes; per-bar boundary bands unavailable."
        )
        self.ivp_note.setWordWrap(True)
        overlay_form.addRow(self.ivp_note)
        self.labels = []
        for ko, en, field in (
            ("표시 시간대", "Display timezone", self.zone),
            ("가격 EMA", "Price EMA", ema_row),
            ("MFI 표본 수", "MFI samples", self.mfi_period),
            ("MMACD EMA 목록", "MMACD EMA periods", self.mmacd_periods),
            ("MMACD 신호 표본 수", "MMACD signal samples", self.signal_period),
            ("DMIxADX 표본 수 (line 0)", "DMIxADX samples (line 0)", self.dmi_period),
            ("DMIxADX 표시값", "DMIxADX display field", self.dmi_display),
        ):
            label = QLabel()
            self.labels.append((label, ko, en))
            form.addRow(label, field)
        si_form = QFormLayout(indicators)
        self.enabled, self.allocations, self.displays = {}, [], []
        for code in INDICATORS:
            switch = QCheckBox(code)
            self.enabled[code] = switch
            si_form.addRow("Calculation", switch)
            if code in ANALYSIS_KINDS:
                switch.toggled.connect(self._promote_analysis_draft)
        for index in range(SI_SLOTS):
            allocation, display = QComboBox(), QCheckBox()
            allocation.addItems(LEGACY_INDICATORS)
            self.allocations.append(allocation)
            self.displays.append(display)
            row = QHBoxLayout()
            row.addWidget(display)
            row.addWidget(allocation, 1)
            si_form.addRow(f"SI {index + 1}", row)
            allocation.currentTextChanged.connect(lambda code, index=index: self._swap(index, code))
        self.derived_controls = {}
        for code, fields in (
            ("NES", ("nSamples", "quote_precision", "origin_ms")),
            ("TPD", ("viewLength", "nSamples", "nSamplesMA", "origin_ms")),
        ):
            self.derived_controls[code] = {}
            for key in fields:
                field = QLineEdit()
                field.setMaxLength(64)
                self.derived_controls[code][key] = field
                si_form.addRow(code + " · " + key, field)
            display = QComboBox()
            display.addItems([code, "ABSMA", "ABSMAREL"])
            self.derived_controls[code]["display"] = display
            si_form.addRow(code + " · display", display)
            origin = QPushButton(
                "NES: 최근1440분을 시작점으로 / Anchor last1440minutes"
                if code == "NES"
                else "TPD: 첫 분봉 시작점 / Anchor first loaded minute"
            )
            origin.clicked.connect(lambda _checked=False, code=code: self._derived_origin(code))
            si_form.addRow(origin)
        derived_note = QLabel(
            "NES 출처는 종목:aggTrade로 고정됩니다. 최대1440분, TPD는10000분. "
            "origin_ms 빈칸은 최초 계산 시작점에 고정되며 자동으로 이동하지 않습니다. "
            "NES/ABSMA는 quote금액, REL은 무차원입니다. "
            "TPD는 확률이 아닌 signed return bias입니다.\n"
            "NES uses canonical instrument:aggTrade only. Fixed origin; no automatic truncation. "
            "Choose an explicit origin for a shorter range. Select one field/unit at a time."
        )
        derived_note.setWordWrap(True)
        si_form.addRow(derived_note)
        self._build_analysis_controls(si_form)
        auxiliary_form = QFormLayout(auxiliary)
        self.auxiliary_sources, self.auxiliary_metrics = {}, {}
        self.auxiliary_note = QLabel()
        self.auxiliary_note.setWordWrap(True)
        auxiliary_form.addRow(self.auxiliary_note)
        for code in AUXILIARY_INDICATORS:
            source, metric = QLineEdit(), QComboBox()
            source.setMaxLength(96)
            self.auxiliary_sources[code], self.auxiliary_metrics[code] = source, metric
            presets = [[name] for name in AUXILIARY_FIELDS[code]]
            if code == "AGGTRADE":
                presets = [list(AUXILIARY_FIELDS[code][i : i + 2]) for i in (0, 2, 4)]
            elif code == "DEPTH":
                presets = [list(AUXILIARY_FIELDS[code])]
            elif code == "LONGSHORTRATIO":
                presets.append(["METRIC_LONGACCOUNT", "METRIC_SHORTACCOUNT"])
            for fields in presets:
                metric.addItem(" / ".join(fields), fields)
            auxiliary_form.addRow(code + " · source", source)
            auxiliary_form.addRow(code + " · metric", metric)
        profile_form = QFormLayout(profiles)
        self.profile_list = QComboBox()
        self.profile_name = QLineEdit("Default")
        self.profile_name.setMaxLength(64)
        self.load_button, self.delete_button = QPushButton(), QPushButton()
        row = QHBoxLayout()
        row.addWidget(self.profile_list, 1)
        row.addWidget(self.load_button)
        row.addWidget(self.delete_button)
        profile_form.addRow(row)
        profile_form.addRow("Name", self.profile_name)
        self.note = QLabel()
        self.note.setWordWrap(True)
        self.note.setProperty("role", "muted")
        profile_form.addRow(self.note)
        self.apply_button, self.save_button, self.reset_button = (QPushButton() for _ in range(3))
        row = QHBoxLayout()
        for button in (self.apply_button, self.save_button, self.reset_button):
            row.addWidget(button)
        layout.addLayout(row)
        self.status = QLabel()
        self.status.setWordWrap(True)
        layout.addWidget(self.status)
        self.apply_button.clicked.connect(self.apply)
        self.save_button.clicked.connect(self.save)
        self.reset_button.clicked.connect(lambda: self.set_draft(self.pane.profile))
        self.load_button.clicked.connect(self.load_profile)
        self.delete_button.clicked.connect(self.delete_profile)
        self.set_draft(pane.profile)
        self.set_presentation()

    @staticmethod
    def _period(value):
        field = QSpinBox()
        field.setRange(2, 2000)
        field.setValue(value)
        return field

    def _ivp_origin(self):
        if self.pane.raw:
            self.ivp_controls["origin_ms"].setText(str(self.pane.raw[0].open_time_ms))

    def _derived_origin(self, code):
        if self.pane.raw:
            start = self.pane.raw[0].open_time_ms
            if code == "NES":
                start = max(start, self.pane.raw[-1].open_time_ms + 60000 - 1440 * 60000)
            self.derived_controls[code]["origin_ms"].setText(str(start))

    def _promote_analysis_draft(self, *_args):
        if not self._loading:
            self._draft_version = 7
            self._set_allocation_choices()

    def _set_allocation_choices(self):
        choices = INDICATORS if self._draft_version == 7 else LEGACY_INDICATORS
        for allocation in self.allocations:
            current = allocation.currentText()
            if tuple(allocation.itemText(index) for index in range(allocation.count())) == choices:
                continue
            blocker = QSignalBlocker(allocation)
            allocation.clear()
            allocation.addItems(choices)
            allocation.setCurrentText(current)
            del blocker

    def _assign_analysis(self, kind):
        self._promote_analysis_draft()
        index = self.analysis_allocation_slots[kind].currentIndex()
        self.allocations[index].setCurrentText(kind)

    def _build_analysis_controls(self, si_form):
        self.analysis_controls = {}
        self.analysis_allocation_slots = {}
        self.analysis_tabs = QTabWidget()
        si_form.addRow(self.analysis_tabs)
        self.analysis_note = QLabel()
        self.analysis_note.setWordWrap(True)
        si_form.addRow(self.analysis_note)
        captions = {
            "origin_ms": ("고정 시작점 (UTC ms)", "Fixed origin (UTC ms)"),
            "display": ("표시 성분", "Display component"),
            "quote_precision": ("금액 정밀도", "Quote precision"),
            "price_precision": ("가격 정밀도", "Price precision"),
            "maximum_metric_age_ms": ("최대 자료 경과시간 (ms)", "Maximum metric age (ms)"),
            "visible": ("히스토그램 표시", "Display histogram"),
            "positive_color": ("양수 색상", "Positive color"),
            "negative_color": ("음수 색상", "Negative color"),
            "opacity": ("불투명도", "Opacity"),
        }
        for kind in ANALYSIS_KINDS:
            config = analysis_profile({}, kind)
            controls = self.analysis_controls[kind] = {}
            body, scroll = QWidget(), QScrollArea()
            form = QFormLayout(body)
            scroll.setWidget(body)
            scroll.setWidgetResizable(True)
            scroll.setMinimumHeight(280)
            scroll.setMaximumHeight(410)
            self.analysis_tabs.addTab(scroll, kind)
            assignment = QHBoxLayout()
            target, assign = QComboBox(), QPushButton()
            target.addItems([f"SI {index + 1}" for index in range(SI_SLOTS)])
            self.analysis_allocation_slots[kind] = target
            self.labels.append((assign, "선택 SI에 배정", "Assign to selected SI"))
            assign.clicked.connect(lambda _checked=False, kind=kind: self._assign_analysis(kind))
            assignment.addWidget(target)
            assignment.addWidget(assign)
            form.addRow(assignment)
            for key, value in config.items():
                if key == "lines":
                    continue
                if key == "display":
                    control = QComboBox()
                    for name in ANALYSIS_FIELDS[kind]:
                        control.addItem(name, name)
                    control.currentIndexChanged.connect(self._promote_analysis_draft)
                elif type(value) is bool:
                    control = QCheckBox()
                    control.toggled.connect(self._promote_analysis_draft)
                else:
                    control = QLineEdit()
                    control.setMaxLength(64)
                    control.textChanged.connect(self._promote_analysis_draft)
                controls[key] = control
                label = QLabel()
                ko, en = captions.get(key, (key, key))
                self.labels.append((label, ko, en))
                form.addRow(label, control)
            origin_button = QPushButton()
            self.labels.append(
                (origin_button, "128 완전봉 시작점으로 고정", "Anchor 128 complete bars")
            )
            origin_button.clicked.connect(
                lambda _checked=False, kind=kind: self._analysis_origin(kind)
            )
            form.addRow(origin_button)
            if kind == "STS":
                continue
            grid = QGridLayout()
            fields = ("active", "nSamples", "visible", "color", "opacity", "width")
            headings = (
                ("선 계산", "Calculate line"),
                ("표본 수", "Samples"),
                ("표시", "Display"),
                ("색상", "Color"),
                ("불투명도", "Opacity"),
                ("선 굵기", "Line width"),
            )
            for column, (ko, en) in enumerate(headings, 1):
                label = QLabel()
                self.labels.append((label, ko, en))
                grid.addWidget(label, 0, column)
            controls["lines"] = []
            for index in range(10):
                line = {}
                controls["lines"].append(line)
                grid.addWidget(QLabel(str(index)), index + 1, 0)
                for column, key in enumerate(fields, 1):
                    control = QCheckBox() if key in {"active", "visible"} else QLineEdit()
                    if isinstance(control, QLineEdit):
                        control.setMaxLength(64)
                        control.setMinimumWidth(65)
                        control.setMaximumWidth(100)
                        control.textChanged.connect(self._promote_analysis_draft)
                    else:
                        control.toggled.connect(self._promote_analysis_draft)
                    line[key] = control
                    grid.addWidget(control, index + 1, column)
            form.addRow(grid)

    def _analysis_origin(self, kind):
        if self.pane.raw and type(self.pane.minutes) is int:
            width = self.pane.minutes * 60000
            end = (self.pane.raw[-1].open_time_ms + 60000) // width * width
            self.analysis_controls[kind]["origin_ms"].setText(str(max(0, end - 128 * width)))

    def _swap(self, index, code):
        if self._loading or not self._slots:
            return
        if code in ANALYSIS_KINDS:
            self._promote_analysis_draft()
        for slot, display in zip(self._slots, self.displays, strict=True):
            slot["visible"] = display.isChecked()
        self._slots = swap_slot(self._slots, index, code)
        self._fill_slots()

    def _fill_slots(self):
        for slot, allocation, display in zip(
            self._slots, self.allocations, self.displays, strict=True
        ):
            blocker = QSignalBlocker(allocation)
            allocation.setCurrentText(slot["indicator"])
            display.setChecked(slot["visible"])
            del blocker

    def set_draft(self, profile):
        profile = validate_profile(profile)
        self._loading = True
        self._draft_version = profile["schema_version"]
        self._set_allocation_choices()
        self.price_windows.set_draft(profile["price_windows"])
        self.zone.setCurrentText(profile["timezone"])
        self.ema.setChecked(profile["ema"])
        self.ema_period.setValue(profile["ema_period"])
        self.mfi_period.setValue(profile["mfi_period"])
        self.mmacd_periods.setText(", ".join(map(str, profile["mmacd_periods"])))
        self.signal_period.setValue(profile["signal_period"])
        self.dmi_period.setValue(profile["dmi_period"])
        self.dmi_display.setCurrentText(profile["dmi_display"])
        for code, controls in self.derived_controls.items():
            for key, field in controls.items():
                value = profile[code.lower()][key]
                if key == "display":
                    field.setCurrentText(value)
                else:
                    field.setText("" if value is None else str(value))
        for kind, controls in self.analysis_controls.items():
            config = analysis_profile(profile, kind)
            for key, control in controls.items():
                if key == "lines":
                    for row, values in zip(control, config[key], strict=True):
                        for name, field in row.items():
                            if isinstance(field, QCheckBox):
                                field.setChecked(values[name])
                            else:
                                field.setText(str(values[name]))
                elif isinstance(control, QComboBox):
                    control.setCurrentIndex(control.findData(config[key]))
                elif isinstance(control, QCheckBox):
                    control.setChecked(config[key])
                else:
                    control.setText("" if config[key] is None else str(config[key]))
        for key, field in self.ivp_controls.items():
            value = profile["ivp"][key]
            if key in {"enabled", "visible"}:
                field.setChecked(value)
            else:
                field.setText("" if value is None else str(value))
        for (group, key), field in self.overlay_controls.items():
            value = profile[group][key]
            if key == "enabled":
                field.setChecked(value)
            elif key == "price_precision":
                field.setValue(value)
            else:
                field.setText(str(value))
        for code, switch in self.enabled.items():
            switch.setChecked(profile["enabled"].get(code, False))
        for code in AUXILIARY_INDICATORS:
            options = profile["auxiliary"][code]
            self.auxiliary_sources[code].setText(options["source"])
            selector = self.auxiliary_metrics[code]
            index = selector.findData(options["fields"])
            if index < 0:
                selector.addItem(" / ".join(options["fields"]), options["fields"])
                index = selector.count() - 1
            selector.setCurrentIndex(index)
        self._slots = profile["slots"]
        self._fill_slots()
        self._loading = False
        self.status.clear()

    def draft(self):
        return validate_profile(
            {
                "schema_version": self._draft_version,
                "price_windows": self.price_windows.draft(),
                "timezone": self.zone.currentText().strip(),
                "ema": self.ema.isChecked(),
                "ema_period": self.ema_period.value(),
                "mfi_period": self.mfi_period.value(),
                "mmacd_periods": [
                    int(part.strip()) for part in self.mmacd_periods.text().split(",")
                ],
                "signal_period": self.signal_period.value(),
                "dmi_period": self.dmi_period.value(),
                "dmi_display": self.dmi_display.currentText(),
                **{
                    code.lower(): {
                        key: (
                            field.currentText()
                            if key == "display"
                            else None
                            if key == "origin_ms" and not field.text().strip()
                            else int(field.text())
                        )
                        for key, field in self.derived_controls[code].items()
                    }
                    for code in LEGACY_DERIVED_INDICATORS
                },
                **(self._analysis_draft() if self._draft_version == 7 else {}),
                "ivp": {
                    key: (
                        field.isChecked()
                        if key in {"enabled", "visible"}
                        else field.text().strip()
                        if key == "color"
                        else None
                        if key == "origin_ms" and not field.text().strip()
                        else int(field.text())
                        if key
                        in {"nSamples", "distance", "price_precision", "max_divisions", "origin_ms"}
                        else float(field.text())
                    )
                    for key, field in self.ivp_controls.items()
                },
                **{
                    group: {
                        key: (
                            field.isChecked()
                            if key == "enabled"
                            else field.value()
                            if key == "price_precision"
                            else field.text().strip()
                            if key == "color"
                            else float(field.text())
                        )
                        for (owner, key), field in self.overlay_controls.items()
                        if owner == group
                    }
                    for group in ("psar", "swing")
                },
                "enabled": {
                    code: self.enabled[code].isChecked()
                    for code in (INDICATORS if self._draft_version == 7 else LEGACY_INDICATORS)
                },
                "auxiliary": {
                    code: {
                        "source": self.auxiliary_sources[code].text().strip(),
                        "fields": self.auxiliary_metrics[code].currentData(),
                    }
                    for code in AUXILIARY_INDICATORS
                },
                "slots": [
                    {"indicator": allocation.currentText(), "visible": display.isChecked()}
                    for allocation, display in zip(self.allocations, self.displays, strict=True)
                ],
            }
        )

    def _analysis_draft(self):
        result = {}
        for kind, controls in self.analysis_controls.items():
            config = {}
            for key, control in controls.items():
                if key == "lines":
                    config[key] = [
                        {
                            name: field.isChecked()
                            if isinstance(field, QCheckBox)
                            else field.text().strip()
                            if name == "color"
                            else float(field.text())
                            if name == "width"
                            else int(field.text())
                            for name, field in row.items()
                        }
                        for row in control
                    ]
                elif isinstance(control, QComboBox):
                    config[key] = control.currentData()
                elif isinstance(control, QCheckBox):
                    config[key] = control.isChecked()
                elif key in {"positive_color", "negative_color"}:
                    config[key] = control.text().strip()
                elif key == "origin_ms" and not control.text().strip():
                    config[key] = None
                elif key in {"OSCScale", "WMMACD", "WMFI", "WDMIxADX", "WWOI"}:
                    config[key] = float(control.text())
                else:
                    config[key] = int(control.text())
            result[kind.lower()] = config
        return result

    def apply(self):
        try:
            self.pane.apply_profile(self.draft())
        except ValueError as error:
            self.status.setText(str(error))
            return False
        self.status.setText(
            "적용됨 · 아직 저장하지 않음"
            if self.pane.owner.language == "ko"
            else "Applied · not saved yet"
        )
        return True

    def refresh_profiles(self):
        selected = self.profile_list.currentText()
        self.profile_list.clear()
        self.profile_list.addItems(sorted(self.pane.owner.profiles))
        self.profile_list.setCurrentText(selected)

    def load_profile(self):
        profile = self.pane.owner.profiles.get(self.profile_list.currentText())
        if profile is not None:
            self.set_draft(profile)
            self.profile_name.setText(self.profile_list.currentText())
            self.status.setText(
                "초안 불러옴 · 적용 필요"
                if self.pane.owner.language == "ko"
                else "Draft loaded · apply to chart"
            )

    def save(self):
        try:
            name, profile = profile_name(self.profile_name.text()), self.draft()
            self.pane.owner.save_profile(self.pane, name, profile)
        except (OSError, ValueError) as error:
            self.status.setText(str(error))
            return
        self.pane.apply_profile(profile)
        self.status.setText(
            "프로필 / 작업 공간 저장됨"
            if self.pane.owner.language == "ko"
            else "Profile / workspace saved"
        )

    def delete_profile(self):
        name = self.profile_list.currentText()
        if name:
            try:
                self.pane.owner.delete_profile(name)
            except (OSError, ValueError) as error:
                self.status.setText(str(error))
                return
            self.status.setText(
                "프로필 삭제됨" if self.pane.owner.language == "ko" else "Profile deleted"
            )

    def set_presentation(self):
        ko = self.pane.owner.language == "ko"
        self.analysis_note.setText(
            "WOI/MST는 활성선만 계산합니다. 표시·색상·성분 변경은 재계산하지 않습니다. "
            "빈 시작점은 종목/주기별 128 완전봉에 한 번 고정됩니다. 이후 자동 이동하지 않습니다. "
            "실제 입력이 없으면 사유와 결측을 표시합니다."
            if ko
            else "WOI/MST calculate active lines only. "
            "Display, style and component changes use cached values. "
            "Blank origin anchors 128 complete bars once per instrument/period; it never slides. "
            "Missing native inputs remain gaps with their actual reason."
        )
        mst_labels = {
            "PRICESTR": "PRICE_STR",
            "OISTR": "OI_STR",
            "LSSTR": "LS_STR",
            "TAKERSTR": "TAKER_STR",
            "MSTBASE": "MST_BASE",
        }
        for kind, controls in self.analysis_controls.items():
            combo = controls["display"]
            for index in range(combo.count()):
                name = combo.itemData(index)
                caption = (
                    mst_labels.get(name, name)
                    if kind == "MST"
                    else (name + (" · 종합" if ko else " · Composite") if name == "S" else name)
                )
                combo.setItemText(index, caption)
        self.price_windows.set_presentation(self.pane.owner.language)
        self.pane.price_windows_request.status()
        self.tabs.setTabText(0, "차트" if ko else "Chart")
        self.tabs.setTabText(1, "보조지표 / 슬롯" if ko else "SI / slots")
        self.tabs.setTabText(2, "가격 지표" if ko else "Price overlays")
        self.tabs.setTabText(4, "보조 관측" if ko else "Observations")
        for source in self.auxiliary_sources.values():
            source.setPlaceholderText("코어 기본 출처" if ko else "Core default source")
        self.auxiliary_note.setText(
            "저장된 실제 관측만 표시합니다. 결측을 채우지 않으며, "
            "과거 시점에 알려졌다는 뜻은 아닙니다. "
            "DEPTH는 저장된 호가 구간의 quote-notional 합계이며 전체 호가 열지도가 아닙니다."
            if ko
            else "Display stored observations only, without filling gaps or claiming historical "
            "availability. DEPTH shows recorded quote-notional bucket totals, "
            "not a full order-book heatmap."
        )
        self.tabs.setTabText(3, "프로필" if ko else "Profiles")
        self.overlay_note.setText(
            "DB 과거 표시용 계산입니다. PSAR는 봉 중앙 가로선, SWING은 선택 봉 마감까지 "
            "확인된 pivot만 연결합니다. 선택 해제 시 SWING을 숨깁니다. 1m~1d만 지원합니다."
            if ko
            else "Retrospective DB display only. PSAR draws centered dashes; SWING connects only "
            "pivots confirmed by the selected bar close. Clear selection to hide SWING. 1m–1d only."
        )
        for label, korean, english in self.labels:
            label.setText(korean if ko else english)
        self.apply_button.setText("적용" if ko else "Apply")
        self.save_button.setText("이름으로 저장" if ko else "Save as")
        self.reset_button.setText("초안 되돌리기" if ko else "Revert draft")
        self.load_button.setText("불러오기" if ko else "Load")
        self.delete_button.setText("삭제" if ko else "Delete")
        for display in self.displays:
            display.setText("표시" if ko else "Display")
        self.note.setText(
            "계산과 표시를 구분합니다. 슬롯 변경은 기존 배정과 교환합니다. "
            "적용만 하면 재시작 뒤 유지되지 않습니다. 저장하면 전체 차트 구성이 함께 저장됩니다. "
            "지표는 화면용이며 전략 분석과 별개입니다."
            if ko
            else "Calculation and display are separate. Assigning an SI swaps its previous slot. "
            "Apply is temporary; Save also stores the full workspace. "
            "Indicators are display-only, separate from strategy analysis."
        )
        self.refresh_profiles()
