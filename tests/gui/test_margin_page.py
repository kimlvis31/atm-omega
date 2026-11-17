"""Structured margin GUI/operation fences; no broker/DB/network calls."""

import json
import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QDateTime, QObject, QPoint, QTimeZone, Signal
from PySide6.QtWidgets import QApplication

from omega.gui.margin_page import BASE, MarginPage
from omega.gui.presentation import configure_fonts, stylesheet

START = 1700000040000


class Client(QObject):
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)
    stateReceived = Signal(dict)
    candlesReceived = Signal(str, dict)
    modulesReceived = Signal(dict)
    attached = True
    state = {}

    def __init__(self):
        super().__init__()
        self.calls, self.commands = [], []

    def request(self, method, path, payload=None, *, tag):
        self.calls.append((method, path, deepcopy(payload), tag))

    def command(self, kind, payload):
        self.commands.append((kind, payload))
        return "a" * 32

    def poll(self):
        pass

    def start(self):
        pass

    def stop(self):
        pass


def state(epoch="core-a", allowed=False):
    return {
        "mode": "paper",
        "core": {
            "epoch": epoch,
            "capabilities": {"margin_trading": True},
            "effective_entries_allowed": allowed,
        },
        "accounts": [],
        "strategies": [],
        "jobs": [],
    }


def account(identity="virtual_1"):
    return {
        "account_id": identity,
        "state": "ready",
        "paused": True,
        "cursor_ms": START,
        "next_open_ms": START + 60000,
        "feed_state": "paused",
        "pending_orders": ["order-1"],
        "instruments": ["binance:usdm:BTCUSDT"],
        "account": {
            "schema_version": 1,
            "summary": {"ledger_total": 1000, "ledger_retained": 128},
            "account": {
                "cross_wallet": "899.9",
                "ledger": [{}] * 128,
                "positions": [
                    {
                        "instrument": "binance:usdm:BTCUSDT",
                        "margin_mode": "cross",
                        "leverage": 10,
                        "quantity": "1",
                        "entry_price": "100",
                        "isolated_wallet": "0",
                        "realized_pnl": "0",
                    }
                ],
            },
        },
    }


def registry(epoch="core-a", rows=None):
    return {
        "schema_version": 1,
        "core_epoch": epoch,
        "state": "ready",
        "accounts": [] if rows is None else rows,
        "operations": [],
    }


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    value = QApplication.instance() or QApplication(["margin-v2-page"])
    configure_fonts(value)
    return value


@pytest.fixture
def page(app):
    client = Client()
    page = MarginPage(client)
    page.setStyleSheet(stylesheet("light"))
    page.resize(1360, 900)
    page.update_state(state())
    page.apply_snapshot(registry())
    for edit in (page.start, page.bracket_time):
        edit.setDateTime(QDateTime.fromMSecsSinceEpoch(START, QTimeZone(b"UTC")))
    page.show()
    app.processEvents()
    yield page
    page.close()
    page.deleteLater()
    app.processEvents()


def save_target(page, identity="btc", symbol="BTCUSDT", *, isolated=False):
    page.new_strategy()
    page.strategy_id.setText(identity)
    page.symbol.setText(symbol)
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    if isolated:
        page.margin_mode.setCurrentIndex(page.margin_mode.findData("isolated"))
        page.collateral.setText("150")
    page.save_strategy()
    assert page.rows[-1]["strategy"]["id"] == identity, page.status.text()


def posts(page):
    return [call for call in page.client.calls if call[0] == "POST"]


def response(page, *, status="completed", **changes):
    pending = page._pending
    row = {
        "id": pending["id"],
        "kind": pending["kind"],
        "core_epoch": page.epoch,
        "operation_core_epoch": pending["epoch"],
        "state": status,
        "error": None,
        "request_digest": sha256(
            json.dumps(
                (pending["kind"], pending["parameters"]),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode()
        ).hexdigest(),
    }
    row.update(changes)
    return row


def test_structured_two_strategies_build_exact_virtual_payload_with_manual_reference(page):
    save_target(page)
    save_target(page, "eth", "ETHUSDT", isolated=True)
    page.manual_assumptions.setChecked(True)
    params = page.parameters()
    assert params["account"]["balance"] == "1000" and params["start_ms"] == START
    assert [row["margin_mode"] for row in params["account"]["positions"]] == ["cross", "isolated"]
    assert params["account"]["positions"][1]["isolated_collateral"] == "150"
    assert [row["configuration"]["quantity"] for row in params["strategies"]] == ["1", "1"]
    assert all(row["constraints"]["leverage"] == "10" for row in params["strategies"])
    assert all(
        row["observed_at_ms"] == START == row["available_at_ms"] for row in params["brackets"]
    )
    assert all(row["rows"][0]["notional_floor"] == "0" for row in params["brackets"])
    page.configure()
    assert len(posts(page)) == 1
    method, path, body, _ = posts(page)[0]
    assert (method, path, body["kind"], body["core_epoch"]) == ("POST", BASE, "configure", "core-a")
    assert body["parameters"] == params
    assert not any(item[2] and item[2].get("kind") == "resume" for item in posts(page))


def test_tef_configuration_editor_and_basic_analysis_are_actual_structured_fields(page, app):
    page.strategy_id.setText("tef-a")
    control = page.configuration_editor.controls[("fullStopLossImmediate",)][0]
    control.setText("5")
    control.editingFinished.emit()
    app.processEvents()
    page.fast.setValue(2)
    page.slow.setValue(3)
    page.signal.setValue(2)
    page.save_strategy()
    strategy = page.rows[0]["strategy"]
    assert strategy["module"] == "tef"
    assert strategy["configuration"]["fullStopLossImmediate"] == "0.05"
    assert strategy["analysis"]["mmacd_periods"] == [2, 3]
    assert strategy["analysis"]["signal_period"] == 2


@pytest.mark.parametrize("problem", ["unconfirmed", "negative", "budget", "cap", "unsaved"])
def test_invalid_or_unsaved_form_never_posts(page, problem):
    save_target(page)
    page.manual_assumptions.setChecked(True)
    if problem == "unconfirmed":
        page.manual_assumptions.setChecked(False)
    elif problem == "negative":
        page.balance.setText("-1")
    elif problem == "budget":
        page.balance.setText("50")
    elif problem == "cap":
        page.max_leverage.setValue(2)
    else:
        page.budget.setText("101")
    page.configure()
    assert not posts(page) and page.status.text()


def test_duplicate_ids_and_instruments_preserve_original_strategy_list(page):
    save_target(page)
    old = deepcopy(page.rows)
    page.new_strategy()
    page.strategy_id.setText("btc")
    page.symbol.setText("ETHUSDT")
    page.save_strategy()
    assert page.rows == old
    page.strategy_id.setText("new")
    page.symbol.setText("BTCUSDT")
    page.save_strategy()
    assert page.rows == old


def test_unknown_post_is_never_reposted_and_explicit_id_query_unblocks_confirmed_result(page):
    save_target(page)
    page.manual_assumptions.setChecked(True)
    page.configure()
    tag = posts(page)[0][3]
    identity = page._pending["id"]
    page.client.failed.emit(tag, "synthetic timeout")
    assert page._pending["unknown"] and not page.configure_button.isEnabled()
    for _ in range(3):
        page._tick()
    assert len(posts(page)) == 1 and identity == page.operation_id.text()
    page.query_operation()
    call = page.client.calls[-1]
    assert call[:2] == ("GET", BASE + "/" + identity)
    page.client.resultReceived.emit(call[3], response(page))
    assert page._pending is None and len(posts(page)) == 1


@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "wrong"),
        ("kind", "resume"),
        ("request_digest", "0" * 64),
        ("operation_core_epoch", "other"),
    ],
)
def test_operation_response_requires_exact_identity_payload_and_origin_epoch(page, field, value):
    save_target(page)
    page.manual_assumptions.setChecked(True)
    page.configure()
    tag = posts(page)[0][3]
    page.client.resultReceived.emit(tag, response(page, **{field: value}))
    assert page._pending is not None and not page.configure_button.isEnabled()
    assert len(posts(page)) == 1


def test_epoch_change_discards_old_callbacks_preserves_draft_and_never_resends(page):
    save_target(page)
    page.manual_assumptions.setChecked(True)
    page.configure()
    tag = posts(page)[0][3]
    old_response = response(page)
    draft = deepcopy(page.rows)
    page.update_state(state("core-b"))
    page.client.resultReceived.emit(tag, old_response)
    assert page.accounts == {} and page.rows == draft
    assert page._snapshot_epoch is None and not page.configure_button.isEnabled()
    assert len(posts(page)) == 1
    page.apply_snapshot(registry("core-b"))
    assert page.configure_button.isEnabled()


def test_account_actions_and_global_pause_are_distinct_and_use_current_epoch(page):
    page.apply_snapshot(registry(rows=[account()]))
    page.account_table.selectRow(0)
    assert page.resume_button.isEnabled() and "전체" in page.global_gate.text()
    page.control("resume")
    body = posts(page)[-1][2]
    assert body["kind"] == "resume" and body["parameters"] == {"account_id": "virtual_1"}
    assert not page.client.commands
    assert "Ledger: 1000" in page.account_summary.text()
    assert page.position_table.item(0, 3).text() == "1"


def test_refresh_does_not_replace_unsaved_draft_or_reset_existing_balance(page):
    save_target(page)
    page.balance.setText("1234")
    page.budget.setText("77")
    page.apply_snapshot(registry(rows=[account()]))
    assert page.balance.text() == "1234" and page.budget.text() == "77"
    page.budget.setText("100")
    page.manual_assumptions.setChecked(True)
    page.configure()
    assert not posts(page) and "reset" in page.status.text()


def test_explicit_bracket_update_has_original_manual_timestamp(page):
    page.apply_snapshot(registry(rows=[account()]))
    page.account_table.selectRow(0)
    page.manual_assumptions.setChecked(True)
    page.update_brackets()
    body = posts(page)[0][2]
    assert body["kind"] == "update_brackets"
    assert body["parameters"]["brackets"][0]["observed_at_ms"] == START
    assert body["parameters"]["brackets"][0]["available_at_ms"] == START


def test_disconnection_locks_mutations_but_keeps_operation_id(page):
    save_target(page)
    page.manual_assumptions.setChecked(True)
    page.configure()
    identity = page.operation_id.text()
    page.client.connectionChanged.emit(False)
    assert not page.configure_button.isEnabled() and page.operation_id.text() == identity
    page.client.connectionChanged.emit(True)
    page.apply_snapshot(registry())
    assert page.query_button.isEnabled() and not page.configure_button.isEnabled()


@pytest.mark.parametrize(
    "bad",
    [
        ["wrong"],
        {"summary": {"ledger_total": True}, "schema_version": 1, "account": {"positions": []}},
    ],
)
def test_malformed_account_response_does_not_enable_control(page, bad):
    row = account()
    row["account"] = bad
    with pytest.raises(ValueError):
        page.apply_snapshot(registry(rows=[row]))
    assert page.accounts == {}


def test_shell_navigation_adds_page_without_changing_eta_nine_tiles(app, tmp_path):
    from omega.gui.window import PAGE_IDS, OmegaWindow

    client = Client()
    window = OmegaWindow(tmp_path, client=client)
    try:
        assert len(window.dashboard.tiles) == 9 and PAGE_IDS[-1] == "margin_v2"
        assert window.navigation.item(PAGE_IDS.index("margin_v2")).text() == "가상 증거금 v2"
        window.accept_state(state())
        window.navigation.setCurrentRow(PAGE_IDS.index("margin_v2"))
        assert window.stack.currentWidget() is window.margin_v2
        assert any(call[1] == BASE for call in client.calls)
    finally:
        window.close()
        window.deleteLater()
        app.processEvents()


def test_actual_qt_responsive_forms_and_operation_snapshots(page, app):
    output = Path(__file__).resolve().parents[3] / ".tools/gui-margin-v2"
    output.mkdir(parents=True, exist_ok=True)
    save_target(page)
    save_target(page, "eth", "ETHUSDT", isolated=True)
    page.tabs.setCurrentIndex(1)
    for width in (800, 1360):
        page.resize(width, 900)
        scroll = page.tabs.widget(1)
        scroll.verticalScrollBar().setValue(0)
        app.processEvents()
        assert page.strategy_id.geometry().bottom() < page.symbol.geometry().top()
        assert page.budget.geometry().bottom() < page.collateral.geometry().top()
        assert page.grab().save(str(output / f"configure-{width}.png"))
        assert page.width() == width
        for _ in range(2):
            app.processEvents()
            scroll.verticalScrollBar().setValue(scroll.verticalScrollBar().maximum())
        app.processEvents()
        bottom = page.configure_button.mapTo(
            scroll.viewport(), QPoint(0, page.configure_button.height())
        )
        assert 0 <= bottom.y() <= scroll.viewport().height()
        assert page.grab().save(str(output / f"configure-bottom-{width}.png"))
    page.tabs.setCurrentIndex(0)
    page.apply_snapshot(registry(rows=[account()]))
    page.account_table.selectRow(0)
    page.resize(800, 900)
    app.processEvents()
    assert page.grab().save(str(output / "operations-800.png"))


def test_pre_mutation_registry_callback_cannot_undo_completed_operation_fence(page):
    page.refresh()
    old = next(call[3] for call in page.client.calls if call[1] == BASE)
    save_target(page)
    page.manual_assumptions.setChecked(True)
    page.configure()
    post = posts(page)[0]
    page.client.resultReceived.emit(post[3], response(page))
    assert page._pending is None and not page.configure_button.isEnabled()
    page.client.resultReceived.emit(old, registry())
    assert not page.configure_button.isEnabled()
    fresh = next(
        call for call in reversed(page.client.calls) if call[1] == BASE and call[0] == "GET"
    )
    page.client.resultReceived.emit(fresh[3], registry(rows=[account()]))
    assert page._snapshot_epoch == page.epoch


def test_unavailable_runtime_never_enables_mutations_and_query_remains_available(page):
    page.operation_id.setText("a" * 32)
    page.apply_snapshot(registry() | {"state": "unavailable", "error": "StorageError"})
    assert not page.configure_button.isEnabled() and page.query_button.isEnabled()
    assert "StorageError" in page.status.text()


def test_language_updates_global_gate_without_erasing_draft(page):
    save_target(page)
    page.balance.setText("4321")
    page.set_presentation("light", "en")
    assert page.label() == "Virtual margin v2"
    assert "Global core" in page.global_gate.text() and page.balance.text() == "4321"


def test_close_retires_callbacks_and_timer_without_sending_control(page):
    page.refresh()
    tag = page.client.calls[-1][3]
    page.close()
    page.client.resultReceived.emit(tag, registry(rows=[account()]))
    assert not page.timer.isActive() and page.accounts == {} and not posts(page)


def test_removed_selected_account_does_not_silently_select_new_row_identity(page):
    page.apply_snapshot(registry(rows=[account("A"), account("B")]))
    page.account_table.selectRow(0)
    assert page.selected_account()["account_id"] == "A"
    page.apply_snapshot(registry(rows=[account("B"), account("C")]))
    assert page.selected_account() is None
    assert not page.pause_button.isEnabled() and not page.resume_button.isEnabled()
    assert not page.reconcile_button.isEnabled() and not page.brackets_button.isEnabled()
    page.control("resume")
    assert not posts(page)
    page.account_table.selectRow(0)
    page.apply_snapshot(registry(rows=[account("C"), account("B")]))
    assert page.selected_account()["account_id"] == "B"
