"""Qt-only schedule interactions; actual scheduler/HTTP collection is separate."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)
ETH, BTC = "binance:usdm:ETHUSDT", "binance:usdm:BTCUSDT"


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(["auxiliary-schedule-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/malgun.ttf"
    if not QFontDatabase.families() and font.is_file():
        identity = QFontDatabase.addApplicationFont(str(font))
        if identity >= 0:
            app.setFont(QFont(QFontDatabase.applicationFontFamilies(identity)[0], 10))
    return app


@pytest.fixture
def client(qt_app):
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.attached, self.calls = True, []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

    return Client()


@pytest.fixture
def panel(qt_app, client):
    from omega.gui.auxiliary_schedule_panel import AuxiliarySchedulePanel

    view = AuxiliarySchedulePanel(client)
    view.update_state(core())
    view.set_instrument(ETH)
    view.setChecked(True)
    yield view
    view.timer.stop()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def core(epoch="epoch-a", capable=True):
    return {"core": {"epoch": epoch, "capabilities": {"auxiliary_schedule": capable}}}


def snapshot(*, instrument=ETH, channels=None, **changes):
    channels = channels or ["trade_flow"]
    tasks = {}
    for channel in channels:
        for native in (
            [channel] if channel == "trade_flow" else ["open_interest_history", "long_short_ratio"]
        ):
            tasks[instrument + "|" + native] = {
                "instrument": instrument,
                "channel": native,
                "next_ms": 1700000100000,
                "state": "unknown",
                "attempt": "b" * 32,
                "failures": 0,
                "retry_at_ms": 0,
                "reason": "commit_outcome_unknown",
                "last_run_ms": 1700000100000,
            }
    result = {
        "schema_version": 1,
        "core_epoch": "epoch-a",
        "revision": 4,
        "generation": 8,
        "enabled": False,
        "closed": False,
        "storage_available": True,
        "worker_alive": True,
        "manual_apply_required": True,
        "subscriptions": [{"instrument": instrument, "channels": channels}],
        "strategy_demand": [{"instrument": BTC, "channels": ["metric", "trade_flow"]}],
        "unsupported_demand": [{"instance": "sts-one", "instrument": ETH, "channels": ["depth"]}],
        "tasks": tasks,
    }
    result.update(changes)
    return result


def reply(panel, value=None):
    panel.refresh()
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], snapshot() if value is None else value
    )


def test_default_collapsed_no_boot_collection_and_explicit_demand_preview(panel):
    panel.setChecked(False)
    panel.client.calls.clear()
    panel._requests.clear()
    assert not panel.isChecked()
    assert panel.client.calls == []
    panel.setChecked(True)
    assert panel.client.calls[-1][:2] == ("GET", "/api/auxiliary-schedule")
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot())
    panel.load_demand()
    assert panel.draft == [{"instrument": BTC, "channels": ["metric", "trade_flow"]}]
    assert not any(call[0] == "POST" for call in panel.client.calls)
    assert "명시 적용" in panel.status.text()
    assert "희망 구독과 실제 데이터 준비는 다릅니다" in panel.hint.text()
    assert "depth" in panel.unsupported.text()


def test_apply_uses_preview_revision_and_never_live_demand_override(panel):
    reply(panel)
    panel.load_demand()
    # A later core snapshot must not replace the already edited preview.
    reply(panel, snapshot(revision=5, strategy_demand=[]))
    panel.apply()
    method, path, payload, tag = panel.client.calls[-1]
    assert (method, path) == ("POST", "/api/auxiliary-schedule/configure")
    assert payload == {
        "expected_revision": 5,
        "subscriptions": [{"instrument": BTC, "channels": ["metric", "trade_flow"]}],
    }
    result = snapshot(
        instrument=BTC, channels=["metric", "trade_flow"], revision=6, generation=9, enabled=True
    )
    panel.client.resultReceived.emit(tag, result)
    assert panel._pending is None and not panel.dirty
    assert panel.snapshot["enabled"]


def test_add_merge_remove_selected_and_spot_disables_metric(panel):
    reply(panel)
    panel.metric_channel.setChecked(True)
    panel.add_selected()
    assert panel.draft[0]["channels"] == ["metric", "trade_flow"]
    panel.add_selected()
    assert len(panel.draft) == 1
    panel.set_instrument("binance:spot:BTCUSDT")
    assert not panel.metric_channel.isChecked() and not panel.metric_channel.isEnabled()
    panel.add_selected()
    assert len(panel.draft) == 2
    panel.draft_table.selectRow(0)
    panel.remove_selected()
    assert len(panel.draft) == 1


def test_unknown_post_only_get_and_explicit_reconciliation(panel):
    reply(panel)
    panel.apply()
    panel.client.failed.emit(panel.client.calls[-1][3], "timeout")
    panel.apply()
    assert len([c for c in panel.client.calls if c[0] == "POST"]) == 1
    assert not panel.apply_button.isEnabled()
    reply(panel, snapshot(revision=5, generation=9, enabled=True))
    assert panel._pending["unknown"]  # A timer GET cannot claim the command succeeded.
    panel.resolve_unknown()
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], snapshot(revision=5, generation=9, enabled=True)
    )
    assert panel._pending is None
    assert "성공 여부는 확정하지" in panel.status.text()
    assert len([c for c in panel.client.calls if c[0] == "POST"]) == 1


def test_stop_and_resume_fence_exact_generation_and_native_task(panel):
    reply(panel, snapshot(enabled=True))
    panel.stop()
    assert panel.client.calls[-1][1:3] == (
        "/api/auxiliary-schedule/stop",
        {"expected_generation": 8},
    )
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(generation=9))
    panel.tasks.selectRow(0)
    panel.resume()
    assert panel.client.calls[-1][1:3] == (
        "/api/auxiliary-schedule/resume",
        {"instrument": ETH, "channel": "trade_flow", "expected_generation": 9},
    )
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], snapshot(generation=9, enabled=True)
    )
    assert panel._pending is None


def test_new_epoch_rejects_old_reply_and_preserves_unknown_draft(panel):
    reply(panel)
    panel.load_demand()
    panel.apply()
    old = panel.client.calls[-1][3]
    panel.update_state(core("epoch-b"))
    panel.client.resultReceived.emit(old, snapshot(revision=10, enabled=True))
    assert panel.snapshot is None and panel._pending["unknown"]
    assert panel.draft[0]["instrument"] == BTC
    reply(panel, snapshot(core_epoch="epoch-b", revision=0, generation=0))
    assert not panel.apply_button.isEnabled()
    panel.resolve_unknown()
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], snapshot(core_epoch="epoch-b", revision=0, generation=0)
    )
    assert panel.apply_button.isEnabled()
    assert panel.draft[0]["instrument"] == BTC


@pytest.mark.parametrize(
    "field,value", [("revision", 3), ("generation", 7), ("core_epoch", "epoch-old")]
)
def test_late_snapshot_cannot_regress_authoritative_state(panel, field, value):
    reply(panel)
    expected = deepcopy(panel.snapshot)
    reply(panel, snapshot(**{field: value}))
    assert panel.snapshot == expected


@pytest.mark.parametrize(
    "mutation",
    [
        "boolean_revision",
        "unsafe_symbol",
        "invalid_channel",
        "missing_task",
        "bad_cursor",
        "task_identity",
    ],
)
def test_malformed_response_is_atomic_and_keeps_dirty_draft(panel, mutation):
    reply(panel)
    panel.load_demand()
    original, draft = deepcopy(panel.snapshot), deepcopy(panel.draft)
    broken = snapshot(revision=5)
    key = next(iter(broken["tasks"]))
    if mutation == "boolean_revision":
        broken["revision"] = True
    elif mutation == "unsafe_symbol":
        broken["subscriptions"][0]["instrument"] = "binance:usdm:BAD\nUSDT"
    elif mutation == "invalid_channel":
        broken["subscriptions"][0]["channels"] = ["depth"]
    elif mutation == "missing_task":
        broken["tasks"] = {}
    elif mutation == "bad_cursor":
        broken["tasks"][key]["next_ms"] += 1
    else:
        broken["tasks"][key]["instrument"] = BTC
    reply(panel, broken)
    assert panel.snapshot == original and panel.draft == draft
    assert "잘못된" in panel.status.text()


def test_mismatched_post_success_retains_unknown_and_draft(panel):
    reply(panel)
    panel.load_demand()
    panel.apply()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(revision=5, enabled=True))
    assert panel._pending["unknown"] and panel.dirty
    assert panel.draft[0]["instrument"] == BTC


def test_disconnect_and_capability_prevent_mutations_and_late_result(panel):
    reply(panel)
    panel.apply()
    old = panel.client.calls[-1][3]
    panel.set_attached(False)
    panel.client.resultReceived.emit(old, snapshot(revision=5, enabled=True))
    assert not panel.apply_button.isEnabled()
    assert panel.snapshot["revision"] == 4
    panel.set_attached(True)
    panel.update_state(core(capable=False))
    calls = len(panel.client.calls)
    panel.apply()
    panel.stop()
    panel.resume()
    panel.refresh()
    assert len(panel.client.calls) == calls


@pytest.mark.parametrize("changes", [{"storage_available": False}, {"closed": True}])
def test_unavailable_owner_blocks_all_writes(panel, changes):
    reply(panel, snapshot(**changes))
    panel.tasks.selectRow(0)
    assert not panel.apply_button.isEnabled()
    assert not panel.stop_button.isEnabled()
    assert not panel.resume_button.isEnabled()


def test_database_page_forwards_core_attachment_language(client, qt_app):
    from omega.gui.database_page import DatabasePage

    view = DatabasePage(client)
    try:
        assert not view.auxiliary_schedule.isChecked()
        view.update_state(core())
        assert view.auxiliary_schedule.capable
        view.accept_snapshot(
            {
                "source": "database",
                "database": {},
                "collection": {},
                "capabilities": {},
                "instruments": [
                    {"instrument": item, "symbol": item.split(":")[-1], "market": "usdm"}
                    for item in (ETH, BTC)
                ],
            }
        )
        view.table.selectRow(0)
        assert view.auxiliary_schedule.instrument == view.selected_instruments()[0]
        view.table.selectAll()
        assert view.auxiliary_schedule.instrument is None
        view.set_presentation("dark", "en")
        assert view.auxiliary_schedule.language == "en"
        view.set_attached(False)
        assert not view.auxiliary_schedule.attached
    finally:
        view.timer.stop()
        view.auxiliary.timer.stop()
        view.auxiliary_schedule.timer.stop()
        view.close()
        view.deleteLater()
        qt_app.processEvents()


def test_task_selection_identity_survives_poll_and_disappearing_row_clears(panel):
    reply(panel)
    panel.tasks.selectRow(0)
    reply(panel, snapshot(channels=["metric", "trade_flow"]))
    assert panel._selected_task()["channel"] == "trade_flow"
    reply(panel, snapshot(channels=["metric"]))
    assert panel._selected_task() is None
    assert not panel.resume_button.isEnabled()


def test_stale_post_reply_does_not_leave_permanent_pending_lock(panel):
    reply(panel)
    panel.apply()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(revision=3))
    assert panel._pending["unknown"] and panel.resolve_button.isEnabled()


def test_superseded_get_cannot_overwrite_new_post_result(panel):
    reply(panel)
    panel.refresh()
    old = panel.client.calls[-1][3]
    panel.apply()
    panel.client.resultReceived.emit(panel.client.calls[-1][3], snapshot(revision=5, enabled=True))
    panel.client.resultReceived.emit(old, snapshot())
    assert panel.snapshot["revision"] == 5 and panel._pending is None


def test_refresh_deduplicates_inflight_query_and_collapsed_tick_is_idle(panel):
    reply(panel)
    panel.refresh()
    count = len(panel.client.calls)
    panel.refresh()
    assert len(panel.client.calls) == count
    panel.setChecked(False)
    panel._tick()
    assert len(panel.client.calls) == count


def test_unicode_instrument_is_preserved_in_subscription_payload(panel):
    symbol = "binance:usdm:\u5e01\u5b89\u4eba\u751fUSDT"
    reply(panel)
    panel.set_instrument(symbol)
    panel.add_selected()
    panel.apply()
    assert any(row["instrument"] == symbol for row in panel.client.calls[-1][2]["subscriptions"])


@pytest.mark.parametrize("width", [800, 1360])
def test_actual_qt_layout_accessible_and_screenshot(panel, qt_app, tmp_path, width):
    from PySide6.QtWidgets import QScrollArea

    host = QScrollArea()
    host.setWidgetResizable(True)
    host.setWidget(panel)
    panel.setChecked(True)
    panel.client.resultReceived.emit(
        panel.client.calls[-1][3], snapshot(channels=["metric", "trade_flow"])
    )
    host.resize(width, 900)
    host.show()
    qt_app.processEvents()
    try:
        assert panel.width() <= host.viewport().width()
        assert panel.apply_button.geometry().right() <= panel.content.width()
        assert panel.resolve_button.geometry().right() <= panel.content.width()
        assert panel.tasks.columnCount() == 5
        assert host.grab().save(str(tmp_path / f"aux-schedule-{width}.png"))
    finally:
        host.takeWidget()
        host.close()
        host.deleteLater()
