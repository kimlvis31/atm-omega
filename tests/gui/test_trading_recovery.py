"""Selected paper recovery must not retarget during polling or a confirmation dialog."""

import importlib.util
import os

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["trading-recovery-tests"])


def snapshot(*names):
    return {
        "core": {"epoch": "test", "server_time_ms": 1000},
        "accounts": [{"account_id": "paper", "balance": "10000"}],
        "strategies": [
            {
                "instance": name,
                "account_id": "paper",
                "module_id": "tef",
                "instrument": "binance:usdm:BTCUSDT",
                "status": "recovery_required",
            }
            for name in names
        ],
        "feed": {"enabled": False},
    }


@pytest.fixture
def page(app):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.pages import TradingPage

    class Client(QObject):
        modulesReceived = Signal(dict)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def command(self, kind, payload):
            self.calls.append((kind, payload))
            return "command:1"

    client = Client()
    view = TradingPage(client)
    view.set_attached(True)
    view.update_state(snapshot("alpha", "beta"))
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


@pytest.mark.parametrize(
    "button,kind",
    [
        ("recover_button", "paper.strategy.recover"),
        ("local_resume_button", "paper.strategy.resume"),
        ("cancel_order_button", "paper.order.cancel"),
        ("remove_strategy_button", "paper.strategy.remove"),
    ],
)
def test_actions_use_selected_instance_not_new_strategy_draft(page, button, kind):
    page.instance.setText("unrelated-new-draft")
    page.strategies.selectRow(1)
    getattr(page, button).click()
    assert page.client.calls == [(kind, {"instance": "beta"})]


def test_no_strategy_selected_rejects_action_without_submission(page):
    page.strategies.setCurrentCell(-1, -1)
    page.strategies.clearSelection()
    page._selected_action("paper.strategy.recover")
    assert not page.client.calls
    assert "Select a strategy" in page.command_result.text()


def test_cleared_selection_with_retained_current_row_is_not_a_selected_strategy(page):
    page.strategies.selectRow(0)
    page.strategies.clearSelection()
    assert page.strategies.currentRow() == 0
    page._selected_action("paper.strategy.recover")
    assert not page.client.calls


@pytest.mark.parametrize("accepted", [False, True])
def test_reset_requires_explicit_confirmation_and_only_then_adds_reset_flag(
    page, monkeypatch, accepted
):
    from PySide6.QtWidgets import QMessageBox

    page.strategies.selectRow(0)
    prompts = []

    def confirm(*args):
        prompts.append(args)
        return QMessageBox.StandardButton.Yes if accepted else QMessageBox.StandardButton.No

    monkeypatch.setattr(QMessageBox, "question", confirm)
    page.reset_model_button.click()
    assert len(prompts) == 1
    assert page.client.calls == (
        [("paper.strategy.recover", {"instance": "alpha", "reset_model": True})] if accepted else []
    )


def test_disconnected_buttons_and_direct_action_do_not_submit(page):
    page.strategies.selectRow(0)
    page.client.attached = False
    page.set_attached(False)
    for name in (
        "recover_button",
        "reset_model_button",
        "local_resume_button",
        "cancel_order_button",
        "remove_strategy_button",
    ):
        button = getattr(page, name)
        assert not button.isEnabled()
        button.click()
    page._selected_action("paper.strategy.recover")
    assert not page.client.calls


def test_poll_reorder_keeps_selected_identity_and_deletion_clears_implicit_target(page):
    page.strategies.selectRow(0)
    page.update_state(snapshot("beta", "alpha"))
    page.recover_button.click()
    assert page.client.calls[-1] == ("paper.strategy.recover", {"instance": "alpha"})
    page.client.calls.clear()
    page.update_state(snapshot("beta", "gamma"))
    page._selected_action("paper.strategy.recover")
    assert not page.client.calls
    assert not page.strategies.selectionModel().selectedRows()


def test_poll_replaces_selected_item_during_reset_dialog_rejects_without_crashing(
    page, monkeypatch
):
    from PySide6.QtWidgets import QMessageBox

    page.strategies.selectRow(0)

    def confirm(*_args):
        page.update_state(snapshot("gamma", "beta"))
        return QMessageBox.StandardButton.Yes

    monkeypatch.setattr(QMessageBox, "question", confirm)
    page._selected_action("paper.strategy.recover", reset=True)
    assert not page.client.calls


def test_disconnect_during_reset_confirmation_rejects_after_dialog(page, monkeypatch):
    from PySide6.QtWidgets import QMessageBox

    page.strategies.selectRow(0)

    def confirm(*_args):
        page.client.attached = False
        page.set_attached(False)
        return QMessageBox.StandardButton.Yes

    monkeypatch.setattr(QMessageBox, "question", confirm)
    page._selected_action("paper.strategy.recover", reset=True)
    assert not page.client.calls


def test_strategy_management_remains_scroll_reachable_at_small_and_large_sizes(page, app):
    from PySide6.QtWidgets import QScrollArea

    page.show()
    for width, height in ((1360, 860), (800, 560)):
        page.resize(width, height)
        app.processEvents()
        assert page.width() == width and page.height() == height
        scroll = next(
            area
            for area in page.findChildren(QScrollArea)
            if area.widget() is not None and area.widget().isAncestorOf(page.recover_button)
        )
        scroll.ensureWidgetVisible(page.recover_button)
        app.processEvents()
        assert scroll.verticalScrollBar().maximum() > 0
        assert not page.grab().isNull()
