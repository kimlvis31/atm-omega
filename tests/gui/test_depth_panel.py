"""Qt depth request fences and compact drafts, using a signal-only fake core."""

import importlib.util
import os
from copy import deepcopy
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)
ETH, BTC = "binance:usdm:ETHUSDT", "binance:spot:BTCUSDT"


@pytest.fixture(scope="module")
def qt_app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtGui import QFont, QFontDatabase
    from PySide6.QtWidgets import QApplication

    app = QApplication.instance() or QApplication(["depth-panel-tests"])
    font = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/segoeui.ttf"
    if not QFontDatabase.families() and font.is_file():
        registered = QFontDatabase.addApplicationFont(str(font))
        if registered >= 0:
            app.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    korean = Path(os.environ.get("WINDIR", "C:/Windows")) / "Fonts/malgun.ttf"
    if korean.is_file():
        registered = QFontDatabase.addApplicationFont(str(korean))
        if registered >= 0:
            app.setFont(QFont(QFontDatabase.applicationFontFamilies(registered)[0], 10))
    return app


def client():
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
def panel(qt_app):
    from omega.gui.depth_panel import DepthPanel

    view = DepthPanel(client())
    view.update_state({"core": {"epoch": "epoch-a", "capabilities": {"depth_collection": True}}})
    view.setChecked(True)
    view.client.resultReceived.emit(view.client.calls[-1][3], snapshot())
    view.client.calls.clear()
    yield view
    view.timer.stop()
    view.close()
    view.deleteLater()
    qt_app.processEvents()


def snapshot(generation=0, running=False, instruments=(), epoch="epoch-a", **extra):
    return {
        "core_epoch": epoch,
        "generation": generation,
        "running": running,
        "closed": False,
        "instruments": [
            {
                "instrument": value,
                "state": "synced" if running else "stopped",
                "source": value + ":binance_depth_ws_v1",
                "cursor_ms": 60000 if running else None,
                "quality": "partial" if running else None,
                "reason": "insufficient_price_coverage" if running else None,
            }
            for value in instruments
        ],
        "close_errors": [],
        "limitation": "live observations only",
        **extra,
    }


def reply(view, value):
    view.client.resultReceived.emit(view.client.calls[-1][3], value)


def load(view, value=None):
    view.refresh()
    reply(view, snapshot() if value is None else value)


def add(view, instrument=ETH):
    view.set_instrument(instrument)
    view.price_precision.setValue(3)
    view.quote_precision.setValue(2)
    view.assumptions.setChecked(True)
    view.add_selected()


def acknowledge(view, post):
    payload = post[2]
    generation = payload["expected_generation"] + (payload["kind"] == "start")
    view.client.resultReceived.emit(
        post[3],
        {
            "id": payload["id"],
            "kind": payload["kind"],
            "core_epoch": payload["core_epoch"],
            "state": "requested",
            "generation": generation,
        },
    )


def test_default_collapsed_manual_precision_and_no_automatic_post(qt_app):
    from omega.gui.depth_panel import DepthPanel

    panel = DepthPanel(client())
    panel.timer.stop()
    panel.update_state({"core": {"epoch": "epoch-a", "capabilities": {"depth_collection": True}}})
    assert not panel.isChecked() and panel.content.isHidden() and not panel.client.calls
    panel.setChecked(True)
    reply(panel, snapshot())
    panel.set_instrument(ETH)
    assert not panel.add_button.isEnabled()
    add(panel)
    assert panel.draft == [{"instrument": ETH, "price_precision": 3, "quote_precision": 2}]
    assert "가정" in panel.assumptions.text()
    assert not any(method == "POST" for method, *_ in panel.client.calls)
    panel.close()
    panel.deleteLater()


def test_start_is_frozen_fenced_and_requested_does_not_mean_prepared(panel):
    load(panel)
    add(panel)
    panel.start()
    post = panel.client.calls[-1]
    assert post[:2] == ("POST", "/api/depth")
    assert post[2]["core_epoch"] == "epoch-a" and post[2]["expected_generation"] == 0
    assert post[2]["parameters"] == {"subscriptions": panel.draft, "resume": False}
    assert panel._pending is not None and not panel.reload_button.isEnabled()
    acknowledge(panel, post)
    assert panel._pending is not None and panel.client.calls[-1][:2] == ("GET", "/api/depth")
    reply(panel, snapshot(1, True, [ETH]))
    assert panel._pending is None and panel.stop_button.isEnabled()
    assert "분석 준비와 별개" in panel.status.text()
    assert "partial" in panel.table.item(0, 1).text()


def test_stop_waits_for_actual_running_false_and_resource_close_confirmation(panel):
    load(panel, snapshot(1, True, [ETH]))
    panel.stop()
    post = panel.client.calls[-1]
    assert post[2]["kind"] == "stop" and post[2]["parameters"] == {}
    acknowledge(panel, post)
    reply(panel, snapshot(1, True, [ETH]))
    assert panel._pending is not None
    panel.refresh()
    reply(panel, snapshot(1, False, [ETH], close_errors=["resource_close_failed"]))
    assert panel._pending is not None and not panel.start_button.isEnabled()
    assert "자원 종료 미확인" in panel.status.text()
    panel.refresh()
    reply(panel, snapshot(1, False, [ETH]))
    assert panel._pending is None and "중지 확인" in panel.status.text()


def test_unknown_post_only_queries_original_id_without_repost(panel):
    load(panel)
    add(panel)
    panel.start()
    post = panel.client.calls[-1]
    panel.client.failed.emit(post[3], "timeout")
    panel.refresh()
    assert panel.client.calls[-1][:2] == ("GET", "/api/depth/" + post[2]["id"])
    acknowledge(panel, panel.client.calls[-1][:2] + (post[2], panel.client.calls[-1][3]))
    reply(panel, snapshot(1, True, [ETH]))
    assert panel._pending is None
    assert sum(row[0] == "POST" for row in panel.client.calls) == 1


def test_new_core_requires_manual_reload_and_preserves_dirty_precision_draft(panel):
    load(panel)
    add(panel)
    editor = panel.draft_table.cellWidget(0, 1)
    editor.setValue(4)
    panel.start()
    old = panel.client.calls[-1]
    panel.update_state({"core": {"epoch": "epoch-b", "capabilities": {"depth_collection": True}}})
    assert panel.requires_reload and panel.draft[0]["price_precision"] == 4
    panel.client.resultReceived.emit(old[3], {"id": old[2]["id"]})
    assert panel.current is None and not panel.start_button.isEnabled()
    panel.reload_current()
    reply(panel, snapshot(1, True, [BTC], epoch="epoch-b"))
    assert (
        not panel.requires_reload
        and panel._pending is None
        and panel.draft[0]["price_precision"] == 4
    )
    assert sum(row[0] == "POST" for row in panel.client.calls) == 1


def test_poll_preserves_editor_identity_and_draft_values(panel):
    load(panel)
    add(panel)
    editor = panel.draft_table.cellWidget(0, 1)
    editor.setValue(5)
    panel.refresh()
    reply(panel, snapshot())
    assert panel.draft_table.cellWidget(0, 1) is editor and editor.value() == 5
    assert panel.draft[0]["price_precision"] == 5 and panel.dirty


@pytest.mark.parametrize(
    "mutation", ["generation_bool", "source", "cursor_bool", "identity", "wrong_epoch"]
)
def test_invalid_snapshot_retains_last_valid_state_and_draft(panel, mutation):
    load(panel)
    add(panel)
    previous = deepcopy(panel.current)
    value = snapshot(1, True, [ETH])
    if mutation == "generation_bool":
        value["generation"] = True
    elif mutation == "source":
        value["instruments"][0]["source"] = "fake"
    elif mutation == "cursor_bool":
        value["instruments"][0]["cursor_ms"] = True
    elif mutation == "identity":
        value["instruments"][0]["instrument"] = "binance:usdm:../escape"
    else:
        value["core_epoch"] = "other"
    panel.refresh()
    reply(panel, value)
    assert panel.current == previous and len(panel.draft) == 1
    assert "초안" in panel.status.text()


def test_start_subscription_mismatch_never_replaces_valid_snapshot(panel):
    load(panel)
    add(panel)
    panel.start()
    post = panel.client.calls[-1]
    previous = deepcopy(panel.current)
    acknowledge(panel, post)
    reply(panel, snapshot(1, True, [BTC]))
    assert panel.current == previous and panel._pending["unknown"]


def test_disconnect_never_displays_old_confirmation_as_current(panel):
    load(panel)
    panel._notice = "호가 수집 worker 중지 확인"
    panel.set_attached(False)
    assert "연결 끊김" in panel.status.text()
    assert not panel.start_button.isEnabled() and not panel.stop_button.isEnabled()


def test_unicode_selection_eight_row_limit_and_resume_explicit_only(panel):
    load(panel)
    for index in range(8):
        add(panel, "binance:usdm:" + ("币安人生USDT" if index == 0 else f"COIN{index}USDT"))
    assert len(panel.draft) == 8 and not panel.add_button.isEnabled()
    panel.resume.setChecked(True)
    panel.start()
    assert panel.client.calls[-1][2]["parameters"]["resume"] is True


@pytest.mark.parametrize("width", [800, 1360])
def test_compact_render_at_supported_widths(panel, qt_app, tmp_path, width):
    load(panel)
    add(panel)
    panel.resize(width, 720)
    panel.show()
    qt_app.processEvents()
    assert panel.draft_table.maximumHeight() <= 132 and panel.table.maximumHeight() <= 132
    assert panel.width() == width and panel.start_button.isVisible()
    assert panel.grab().save(str(tmp_path / f"depth-{width}.png"))


def test_database_page_insertion_is_collapsed_and_selection_is_forwarded(qt_app):
    from omega.gui.database_page import DatabasePage

    page = DatabasePage(client())
    try:
        assert not page.depth.isChecked()
        page.update_state(
            {"core": {"epoch": "epoch-a", "capabilities": {"depth_collection": True}}}
        )
        page.selected_instruments = lambda: [ETH]
        page._instruments[ETH] = {"instrument": ETH, "market": "usdm"}
        page._selection_changed(query=False)
        assert page.depth.instrument == ETH
        page.set_presentation("dark", "en")
        assert page.depth.language == "en"
        page.set_attached(False)
        assert page.depth.attached is False
    finally:
        page.timer.stop()
        page.close()
        page.deleteLater()
        qt_app.processEvents()
