"""Explicit continuous public-data collection controls; all IO belongs to the core."""

from copy import deepcopy
from datetime import UTC, datetime

from PySide6.QtCore import Qt, QTimer
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QGridLayout,
    QGroupBox,
    QHeaderView,
    QLabel,
    QPushButton,
    QSizePolicy,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from omega.domain.models import is_valid_symbol

PATH = "/api/auxiliary-schedule"
CHANNELS = {"trade_flow", "open_interest_history", "long_short_ratio"}
STATES = {"queued", "running", "backoff", "blocked", "unknown", "stopped"}


def _instrument(value):
    parts = value.split(":") if type(value) is str else ()
    return (
        len(parts) == 3
        and parts[0] == "binance"
        and parts[1] in {"spot", "usdm"}
        and is_valid_symbol(parts[2])
    )


def _subscriptions(values):
    if type(values) is not list or len(values) > 512:
        raise ValueError("Invalid subscriptions")
    merged = {}
    for row in values:
        if (
            type(row) is not dict
            or not _instrument(row.get("instrument"))
            or type(row.get("channels")) is not list
            or not 1 <= len(row["channels"]) <= 2
            or any(type(c) is not str or c not in {"trade_flow", "metric"} for c in row["channels"])
            or ":spot:" in row["instrument"]
            and "metric" in row["channels"]
        ):
            raise ValueError("Invalid subscription identity or channel")
        merged.setdefault(row["instrument"], set()).update(row["channels"])
    if len(merged) > 128:
        raise ValueError("Too many subscription instruments")
    return [
        {"instrument": key, "channels": sorted(channels)}
        for key, channels in sorted(merged.items())
    ]


def _snapshot(value):
    if (
        type(value) is not dict
        or type(value.get("schema_version")) is not int
        or value["schema_version"] != 1
        or type(value.get("core_epoch")) is not str
        or not value["core_epoch"]
        or any(type(value.get(k)) is not int or value[k] < 0 for k in ("revision", "generation"))
        or any(
            type(value.get(k)) is not bool
            for k in (
                "enabled",
                "closed",
                "storage_available",
                "worker_alive",
                "manual_apply_required",
            )
        )
        or type(value.get("tasks")) is not dict
        or len(value["tasks"]) > 384
    ):
        raise ValueError("Invalid scheduler snapshot")
    result = deepcopy(value)
    result["subscriptions"] = _subscriptions(value.get("subscriptions"))
    result["strategy_demand"] = _subscriptions(value.get("strategy_demand", []))
    unsupported = value.get("unsupported_demand", [])
    if type(unsupported) is not list or len(unsupported) > 1000:
        raise ValueError("Invalid unsupported demand")
    for row in unsupported:
        if (
            type(row) is not dict
            or type(row.get("instance")) is not str
            or len(row["instance"]) > 128
            or not _instrument(row.get("instrument"))
            or type(row.get("channels")) is not list
            or len(row["channels"]) > 16
            or any(type(c) is not str or len(c) > 64 for c in row["channels"])
        ):
            raise ValueError("Invalid unsupported demand identity")
    for key, row in value["tasks"].items():
        if (
            type(row) is not dict
            or not _instrument(row.get("instrument"))
            or row.get("channel") not in CHANNELS
            or key != row["instrument"] + "|" + row["channel"]
            or row.get("state") not in STATES
            or type(row.get("next_ms")) is not int
            or not 0 <= row["next_ms"] <= 253_402_300_799_000
            or type(row.get("reason")) is not str
            or len(row["reason"]) > 160
            or row["next_ms"] % (60000 if row["channel"] == "trade_flow" else 300000)
            or ":spot:" in row["instrument"]
            and row["channel"] != "trade_flow"
        ):
            raise ValueError("Invalid scheduler task")
    expected_tasks = {
        row["instrument"] + "|" + channel
        for row in result["subscriptions"]
        for channel in (
            (["trade_flow"] if "trade_flow" in row["channels"] else [])
            + (["open_interest_history", "long_short_ratio"] if "metric" in row["channels"] else [])
        )
    }
    if set(value["tasks"]) != expected_tasks:
        raise ValueError("Scheduler tasks differ from requested subscriptions")
    return result


def _utc(value):
    return datetime.fromtimestamp(value / 1000, UTC).strftime("%Y-%m-%d %H:%M:%S")


class AuxiliarySchedulePanel(QGroupBox):
    def __init__(self, client, parent=None):
        super().__init__(parent)
        self.client, self.language = client, "ko"
        self.instrument, self.epoch = None, None
        self.attached, self.capable = bool(client.attached), False
        self.snapshot, self.draft = None, []
        self.dirty, self._pending = False, None
        self._serial, self._accepted_serial = 0, 0
        self._requests = {}
        self.setCheckable(True)
        self.setChecked(False)
        root = QVBoxLayout(self)
        self.content = QWidget()
        root.addWidget(self.content)
        layout = QVBoxLayout(self.content)
        self.hint, self.target, self.status, self.unsupported = (QLabel() for _ in range(4))
        for label in (self.hint, self.target, self.status, self.unsupported):
            label.setWordWrap(True)
            label.setTextFormat(Qt.TextFormat.PlainText)
            label.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
            layout.addWidget(label)
        controls = QGridLayout()
        self.trade_flow, self.metric_channel = (
            QCheckBox("trade_flow"),
            QCheckBox("metric (OI / LS)"),
        )
        self.trade_flow.setChecked(True)
        controls.addWidget(self.trade_flow, 0, 0)
        controls.addWidget(self.metric_channel, 0, 1)
        self.add_button, self.remove_button, self.demand_button = (QPushButton() for _ in range(3))
        self.apply_button, self.stop_button, self.resume_button = (QPushButton() for _ in range(3))
        self.refresh_button, self.resolve_button = QPushButton(), QPushButton()
        for index, button in enumerate(
            (
                self.add_button,
                self.remove_button,
                self.demand_button,
                self.apply_button,
                self.stop_button,
                self.resume_button,
                self.refresh_button,
                self.resolve_button,
            )
        ):
            controls.addWidget(button, 1 + index // 2, index % 2)
        layout.addLayout(controls)
        self.draft_table = self._table(2)
        self.tasks = self._table(5)
        layout.addWidget(self.draft_table)
        layout.addWidget(self.tasks)
        self.add_button.clicked.connect(self.add_selected)
        self.remove_button.clicked.connect(self.remove_selected)
        self.demand_button.clicked.connect(self.load_demand)
        self.apply_button.clicked.connect(self.apply)
        self.stop_button.clicked.connect(self.stop)
        self.resume_button.clicked.connect(self.resume)
        self.refresh_button.clicked.connect(self.refresh)
        self.resolve_button.clicked.connect(self.resolve_unknown)
        self.tasks.itemSelectionChanged.connect(self._controls)
        self.draft_table.itemSelectionChanged.connect(self._controls)
        self.toggled.connect(self._expanded)
        client.resultReceived.connect(self._result)
        client.failed.connect(self._failed)
        self.timer = QTimer(self)
        self.timer.setInterval(5000)
        self.timer.timeout.connect(self._tick)
        self.timer.start()
        self.set_presentation("light", "ko")
        self.content.hide()

    @staticmethod
    def _table(columns):
        table = QTableWidget(0, columns)
        table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        table.verticalHeader().hide()
        table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        table.setMinimumHeight(110)
        table.setMaximumHeight(185)
        return table

    def text(self, ko, en):
        return ko if self.language == "ko" else en

    def set_presentation(self, _theme, language):
        self.language = language if language in {"ko", "en"} else "ko"
        self.setTitle(self.text("연속 보조 데이터 수집", "CONTINUOUS AUXILIARY COLLECTION"))
        self.hint.setText(
            self.text(
                "공개 REST 연속 수집입니다. 희망 구독과 실제 데이터 준비는 다릅니다. "
                "작업 상태는 분석 품질/최신성 준비를 보장하지 않습니다. "
                "전략 수요를 불러온 뒤 명시적으로 적용하세요. "
                "코어 재시작/unknown 작업은 명시 재개가 필요합니다. "
                "metric은 native 5분 OI/LS이며 depth 수집은 미지원입니다.",
                "Continuous public REST collection. Requested subscriptions and data readiness "
                "differ: task state does not establish analysis quality/freshness. "
                "Load strategy demand, then explicitly apply. "
                "Core restart/unknown tasks require explicit resumption. "
                "Metric uses native 5m OI/LS; depth collection is unsupported.",
            )
        )
        for button, ko, en in (
            (self.add_button, "선택 종목을 초안에 추가", "Add selected instrument to draft"),
            (self.remove_button, "선택 초안 제거", "Remove draft row"),
            (
                self.demand_button,
                "전략 수요를 초안으로 불러오기",
                "Load strategy demand into draft",
            ),
            (self.apply_button, "초안 적용 · 수집 시작", "Apply draft · start collection"),
            (self.stop_button, "연속 수집 정지", "Stop continuous collection"),
            (self.resume_button, "선택 작업 명시 재개", "Explicitly resume selected task"),
            (self.refresh_button, "수집 상태 조회", "Read collection state"),
            (
                self.resolve_button,
                "현재 상태 확인 · 미확인 요청 해제",
                "Read current state · acknowledge unknown request",
            ),
        ):
            button.setText(self.text(ko, en))
        self.draft_table.setHorizontalHeaderLabels(
            [
                self.text("희망 종목 (초안)", "Requested instrument (draft)"),
                self.text("희망 채널", "Requested channels"),
            ]
        )
        self.tasks.setHorizontalHeaderLabels(
            [
                self.text("종목", "Instrument"),
                self.text("실제 채널", "Native channel"),
                self.text("다음 cursor (UTC)", "Next cursor (UTC)"),
                self.text("작업 상태", "Task state"),
                self.text("사유", "Reason"),
            ]
        )
        self._render()

    def _expanded(self, checked):
        self.content.setVisible(checked)
        if checked:
            self.refresh()

    def _tick(self):
        if self.isChecked() and self.isVisible():
            self.refresh()

    def set_instrument(self, instrument):
        self.instrument = instrument if _instrument(instrument) else None
        if self.instrument and ":spot:" in self.instrument:
            self.metric_channel.setChecked(False)
        self._render()

    def update_state(self, state):
        core = state.get("core") or {}
        epoch = core.get("epoch") or state.get("core_epoch")
        changed = type(epoch) is str and bool(epoch) and epoch != self.epoch
        if changed:
            self.epoch, self.snapshot = epoch, None
            self._requests.clear()
            self._accepted_serial = 0
            if self._pending:
                self._pending["unknown"] = True
        self.capable = (core.get("capabilities") or {}).get("auxiliary_schedule") is True
        self._render()
        if changed and self.isChecked():
            self.refresh()

    def set_attached(self, attached):
        previous, self.attached = self.attached, bool(attached)
        if not self.attached:
            self._requests.clear()
            if self._pending:
                self._pending["unknown"] = True
            self.status.setText(
                self.text(
                    "연결 끊김 · 표시 상태는 오래됐을 수 있습니다",
                    "Disconnected · displayed state may be stale",
                )
            )
        self._controls()
        if self.attached and not previous and self.isChecked():
            self.refresh()

    def _ready(self):
        return self.attached and self.capable and self.epoch is not None

    def _selected_task(self):
        rows = self.tasks.selectionModel().selectedRows()
        if len(rows) != 1 or self.snapshot is None:
            return None
        item = self.tasks.item(rows[0].row(), 0)
        return self.snapshot["tasks"].get(item.data(Qt.ItemDataRole.UserRole)) if item else None

    def _controls(self):
        known = self._ready() and self.snapshot is not None
        writable = (
            known
            and not self._pending
            and not self.snapshot["closed"]
            and self.snapshot["storage_available"]
        )
        self.refresh_button.setEnabled(
            self._ready() and (not self._pending or self._pending.get("unknown", False))
        )
        self.resolve_button.setEnabled(
            self._ready() and self._pending is not None and self._pending.get("unknown", False)
        )
        self.apply_button.setEnabled(writable)
        self.stop_button.setEnabled(writable and self.snapshot["enabled"])
        task = self._selected_task()
        self.resume_button.setEnabled(
            writable
            and task is not None
            and task["state"] in {"unknown", "blocked", "stopped", "backoff"}
        )
        self.add_button.setEnabled(not self._pending and self.instrument is not None)
        self.remove_button.setEnabled(
            not self._pending and len(self.draft_table.selectionModel().selectedRows()) == 1
        )
        self.demand_button.setEnabled(known and not self._pending)
        self.metric_channel.setEnabled(
            bool(self.instrument and ":usdm:" in self.instrument) and not self._pending
        )
        self.trade_flow.setEnabled(not self._pending)

    def _render(self):
        self.target.setText(
            self.text("선택 종목: ", "Selected instrument: ") + (self.instrument or "—")
        )
        selection = self._selected_task()
        selected_key = (
            None if selection is None else selection["instrument"] + "|" + selection["channel"]
        )
        self.draft_table.setRowCount(len(self.draft))
        for index, row in enumerate(self.draft):
            for column, value in enumerate((row["instrument"], ", ".join(row["channels"]))):
                self.draft_table.setItem(index, column, QTableWidgetItem(value))
        tasks = {} if self.snapshot is None else self.snapshot["tasks"]
        self.tasks.setRowCount(len(tasks))
        self.tasks.clearSelection()
        for index, (key, row) in enumerate(sorted(tasks.items())):
            for column, value in enumerate(
                (
                    row["instrument"],
                    row["channel"],
                    _utc(row["next_ms"]),
                    row["state"],
                    row["reason"],
                )
            ):
                item = QTableWidgetItem(value)
                item.setToolTip(value)
                if column == 0:
                    item.setData(Qt.ItemDataRole.UserRole, key)
                self.tasks.setItem(index, column, item)
            if key == selected_key:
                self.tasks.selectRow(index)
        self.tasks.resizeRowsToContents()
        unsupported = [] if self.snapshot is None else self.snapshot.get("unsupported_demand", [])
        self.unsupported.setText(
            self.text("미지원 전략 수요: ", "Unsupported strategy demand: ")
            + (
                "; ".join(
                    f"{r['instance']} · {r['instrument']} · {','.join(r['channels'])}"
                    for r in unsupported
                )
                or "—"
            )
        )
        if not self.attached:
            self.status.setText(
                self.text(
                    "연결 끊김 · 표시 상태는 오래됐을 수 있습니다",
                    "Disconnected · displayed state may be stale",
                )
            )
        elif self._pending:
            self.status.setText(
                self.text(
                    "요청 결과 미확인 · 자동 재전송하지 않습니다. "
                    "현재 상태 확인 후 명시적으로 해제하세요.",
                    "Request outcome unconfirmed · never reposted automatically. "
                    "Read current state and explicitly acknowledge.",
                )
                if self._pending.get("unknown")
                else self.text("요청 처리 중", "Request pending")
            )
        elif not self.capable:
            self.status.setText(
                self.text("코어 연속 수집 기능 미지원", "Core continuous collection unavailable")
            )
        elif self.snapshot and (self.snapshot["closed"] or not self.snapshot["storage_available"]):
            self.status.setText(
                self.text(
                    "수집 owner 종료 또는 저장 불가 · 변경 차단",
                    "Collection owner closed or storage unavailable · changes blocked",
                )
            )
        elif self.snapshot:
            self.status.setText(
                f"revision {self.snapshot['revision']} · "
                f"generation {self.snapshot['generation']} · "
                + self.text(
                    "수집 희망 활성" if self.snapshot["enabled"] else "정지 · 명시 적용 필요",
                    "Collection armed"
                    if self.snapshot["enabled"]
                    else "Stopped · explicit apply required",
                )
                + f" · worker={self.snapshot['worker_alive']}"
            )
        self._controls()

    def add_selected(self):
        if not self.add_button.isEnabled():
            return
        channels = (["trade_flow"] if self.trade_flow.isChecked() else []) + (
            ["metric"] if self.metric_channel.isChecked() else []
        )
        if not channels:
            self.status.setText(self.text("추가할 채널을 선택하세요", "Select channels to add"))
            return
        try:
            self.draft = _subscriptions(
                self.draft + [{"instrument": self.instrument, "channels": channels}]
            )
        except ValueError:
            self.status.setText(
                self.text(
                    "구독 수 제한 초과 · 초안 유지", "Subscription limit reached · draft retained"
                )
            )
            return
        self.dirty = True
        self._render()

    def remove_selected(self):
        rows = self.draft_table.selectionModel().selectedRows()
        if self.remove_button.isEnabled() and len(rows) == 1:
            del self.draft[rows[0].row()]
            self.dirty = True
            self._render()

    def load_demand(self):
        if self.demand_button.isEnabled():
            self.draft = deepcopy(self.snapshot["strategy_demand"])
            self.dirty = True
            self._render()

    def _request(self, method, suffix="", payload=None, *, acknowledge=False):
        self._serial += 1
        tag = f"aux-schedule:{id(self)}:{self._serial}"
        self._requests[tag] = (self.epoch, self._serial, method, acknowledge)
        self.client.request(method, PATH + suffix, payload, tag=tag)

    def refresh(self, *, acknowledge=False):
        if not self._ready() or self._pending and not self._pending.get("unknown"):
            return
        if any(v[0] == self.epoch and v[2] == "GET" for v in self._requests.values()):
            return
        self._request("GET", acknowledge=acknowledge)

    def resolve_unknown(self):
        if self.resolve_button.isEnabled():
            self.refresh(acknowledge=True)

    def _post(self, suffix, payload):
        self._pending = {"unknown": False, "payload": deepcopy(payload), "suffix": suffix}
        self._render()
        self._request("POST", suffix, payload)

    def apply(self):
        if self.apply_button.isEnabled():
            self._post(
                "/configure",
                {
                    "expected_revision": self.snapshot["revision"],
                    "subscriptions": deepcopy(self.draft),
                },
            )

    def stop(self):
        if self.stop_button.isEnabled():
            self._post("/stop", {"expected_generation": self.snapshot["generation"]})

    def resume(self):
        task = self._selected_task()
        if self.resume_button.isEnabled() and task:
            self._post(
                "/resume",
                {
                    "instrument": task["instrument"],
                    "channel": task["channel"],
                    "expected_generation": self.snapshot["generation"],
                },
            )

    def _result(self, tag, value):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch or not self.attached:
            return
        try:
            snapshot = _snapshot(value)
            if snapshot["core_epoch"] != self.epoch:
                self._unknown_reply(request)
                return
            if self.snapshot is not None and (
                snapshot["revision"] < self.snapshot["revision"]
                or snapshot["generation"] < self.snapshot["generation"]
            ):
                self._unknown_reply(request)
                return
            if request[1] < self._accepted_serial:
                self._unknown_reply(request)
                return
            self.snapshot, self._accepted_serial = snapshot, request[1]
            if request[2] == "POST" and not self._matches_pending(snapshot):
                self._unknown_reply(request)
                return
            if request[2] == "POST" or request[3]:
                if (
                    request[2] == "POST"
                    and self._pending
                    and self._pending["suffix"] == "/configure"
                ):
                    self.dirty = False
                self._pending = None
            if not self.dirty:
                self.draft = deepcopy(snapshot["subscriptions"])
            self._render()
            if request[3]:
                self.status.setText(
                    self.text(
                        "현재 상태 확인 완료 · 이전 명령의 성공 여부는 확정하지 않습니다",
                        "Current state read · prior command success remains unconfirmed",
                    )
                )
        except (TypeError, ValueError, KeyError, OverflowError):
            if request[2] == "POST" and self._pending:
                self._pending["unknown"] = True
            self._render()
            self.status.setText(
                self.text(
                    "잘못된 수집 상태 응답 · 기존 초안/상태 유지",
                    "Invalid collection response · draft/state retained",
                )
            )

    def _matches_pending(self, snapshot):
        if self._pending is None:
            return False
        payload, suffix = self._pending["payload"], self._pending["suffix"]
        if suffix == "/configure":
            return (
                snapshot["enabled"]
                and snapshot["revision"] > payload["expected_revision"]
                and snapshot["subscriptions"] == payload["subscriptions"]
            )
        if suffix == "/stop":
            return (
                not snapshot["enabled"] and snapshot["generation"] > payload["expected_generation"]
            )
        return (
            snapshot["enabled"]
            and snapshot["generation"] == payload["expected_generation"]
            and payload["instrument"] + "|" + payload["channel"] in snapshot["tasks"]
        )

    def _unknown_reply(self, request):
        if request[2] == "POST" and self._pending:
            self._pending["unknown"] = True
            self._render()

    def _failed(self, tag, _message):
        request = self._requests.pop(tag, None)
        if request is None or request[0] != self.epoch:
            return
        if request[2] == "POST" and self._pending:
            self._pending["unknown"] = True
        self._render()
        self.status.setText(
            self.text(
                "요청 결과 미확인 · 상태를 조회하세요. 자동 재전송하지 않습니다.",
                "Outcome unconfirmed · read current state. Never reposted automatically.",
            )
        )
