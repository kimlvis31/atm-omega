"""Feed settings drafts and uncertain commands remain independent of data readiness."""

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)


@pytest.fixture
def panel():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtCore import QObject, Signal
    from PySide6.QtWidgets import QApplication

    from omega.gui.feed_controls import FeedControls

    app = QApplication.instance() or QApplication(["feed-draft-tests"])

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)

        def __init__(self):
            super().__init__()
            self.commands, self.requests = [], []

        def command(self, kind, payload):
            operation = f"op-{len(self.commands)}"
            self.commands.append((operation, kind, payload))
            return operation

        def request(self, *args, **kwargs):
            self.requests.append((args, kwargs))

    instance = FeedControls(Client())
    instance.setChecked(True)
    instance.set_presentation("light", "en")
    instance.update_state(snapshot())
    instance.set_attached(True)
    yield instance
    instance.close()
    instance.deleteLater()
    app.processEvents()


def snapshot(revision=0, warmup=240, epoch="epoch-a"):
    return {
        "core": {"server_time_ms": 180_000, "epoch": epoch},
        "feed": {
            "revision": revision,
            "enabled": False,
            "warmup_minutes": warmup,
            "state": "blocked",
            "reason": "missing committed data",
        },
    }


def complete(panel, result=None, tag="command:"):
    panel.client.resultReceived.emit(
        tag + panel.pending,
        result
        or {
            "status": "completed",
            "result": {"revision": 1},
        },
    )


def test_first_snapshot_enables_apply_and_later_edits_survive_confirmation(panel):
    assert panel.apply_button.isEnabled()
    panel.warmup.setValue(300)
    panel.apply()
    assert panel.client.commands[-1][2]["warmup_minutes"] == 300
    panel.warmup.setValue(400)
    complete(panel)
    panel.update_state(snapshot(1, 300))
    assert panel.warmup.value() == 400 and panel.dirty
    assert "preserved" in panel.operation_status.text()
    assert "blocked" in panel.status.text()  # saving is not analyzer readiness


def test_unchanged_saved_draft_accepts_fresh_state_but_ignores_older_snapshot(panel):
    panel.warmup.setValue(300)
    panel.apply()
    complete(panel)
    panel.update_state(snapshot(0, 100))
    assert panel.warmup.value() == 300
    panel.update_state(snapshot(1, 300))
    assert not panel.dirty and panel.warmup.value() == 300
    panel.update_state(snapshot(0, 120, epoch="epoch-b"))
    assert panel.warmup.value() == 120  # new core has an independent revision


def test_known_rejection_keeps_draft_and_allows_a_deliberate_new_request(panel):
    panel.warmup.setValue(300)
    panel.apply()
    complete(panel, {"status": "rejected", "code": "BAD_CONFIG"})
    assert panel.pending is None and panel.dirty and panel.apply_button.isEnabled()
    assert "BAD_CONFIG" in panel.operation_status.text()


def test_transport_failure_retains_operation_and_queries_without_reposting(panel):
    panel.apply()
    operation = panel.pending
    panel.client.failed.emit("command:" + operation, "connection lost")
    panel.apply()
    assert len(panel.client.commands) == 1 and not panel.apply_button.isEnabled()
    panel.resolve_operation()
    assert panel.client.requests[-1] == (
        ("GET", "/api/operations/" + operation),
        {"tag": "operation:" + operation},
    )
    complete(panel, {"status": "unknown"}, tag="operation:")
    assert panel.pending == operation and panel.resolve_button.isEnabled()
    complete(panel, tag="operation:")
    assert panel.pending is None and panel.apply_button.isEnabled()


def test_disconnect_keeps_unconfirmed_operation_until_reconnection_query(panel):
    panel.apply()
    operation = panel.pending
    panel.set_attached(False)
    assert panel.pending == operation and not panel.resolve_button.isEnabled()
    panel.set_attached(True)
    assert panel.resolve_button.isEnabled()
    complete(panel, tag="operation:")
    assert not panel.apply_button.isEnabled()  # refresh server clock before another command
    panel.update_state(snapshot(1))
    assert panel.apply_button.isEnabled()


@pytest.mark.parametrize("data", [None, {}, {"revision": True}])
def test_incomplete_completed_receipt_does_not_claim_configuration_saved(panel, data):
    panel.apply()
    operation = panel.pending
    complete(panel, {"status": "completed", "result": data})
    assert panel.pending == operation and not panel.apply_button.isEnabled()
    assert "unconfirmed" in panel.operation_status.text()


def long_snapshot(*, quantity="0", pending=False, paused=True, plan_state="warming"):
    value = snapshot(revision=2, warmup=30000)
    value["core"].update(control_paused=paused, paper_paused=paused)
    value["strategies"] = [
        {"instance": "tef-daily", "quantity": quantity, "has_pending_order": pending}
    ]
    value["feed"]["warmup_plans"] = {
        "tef-daily": {
            "plan_id": "fixed-plan",
            "state": plan_state,
            "start_ms": 0,
            "target_end_ms": 30000 * 60000,
            "next_open_ms": 1024 * 60000,
            "processed_minutes": 1024,
            "total_minutes": 30000,
            "alignment_minutes": 1440,
            "chunk_minutes": 512,
            "checkpoint": {"private": "must never appear in the GUI"},
        }
    }
    return value


def test_long_prepare_uses_new_target_while_continue_preserves_existing_fixed_plan_and_draft(panel):
    panel.update_state(long_snapshot())
    panel.warmup.setValue(40000)
    panel.enabled.setChecked(True)
    assert panel.apply_button.isEnabled() and panel.continue_button.isEnabled()
    panel.continue_warmup()
    operation, kind, payload = panel.client.commands[-1]
    assert kind == "paper.feed.continue" and payload == {}
    assert "continuation" in panel.operation_status.text()
    complete(panel, {"status": "completed", "result": {"revision": 3}})
    assert panel.pending is None and panel.dirty
    panel.update_state(long_snapshot())
    assert panel.warmup.value() == 40000 and panel.enabled.isChecked()
    assert "continuation confirmed" in panel.operation_status.text()
    panel.apply()
    assert panel.client.commands[-1][1:] == (
        "paper.feed.configure",
        {
            "enabled": True,
            "start_ms": 120000,
            "warmup_minutes": 40000,
        },
    )
    assert panel.client.commands[-1][0] != operation


@pytest.mark.parametrize(
    "quantity,pending,paused",
    [
        ("1", False, True),
        ("-0.1", False, True),
        ("0", True, True),
        ("NaN", False, True),
        ("0", None, True),
        ("0", False, False),
    ],
)
def test_long_prepare_and_continue_require_global_pause_and_every_strategy_flat(
    panel, quantity, pending, paused
):
    panel.update_state(long_snapshot(quantity=quantity, pending=pending, paused=paused))
    panel.enabled.setChecked(True)
    assert not panel.apply_button.isEnabled() and not panel.continue_button.isEnabled()
    panel.apply()
    panel.continue_warmup()
    assert not panel.client.commands
    assert panel.safety_hint.text()
    panel.enabled.setChecked(False)
    assert panel.apply_button.isEnabled()  # Disabling a feed remains available.


def test_unknown_safety_fields_do_not_authorize_long_prepare(panel):
    state = long_snapshot()
    state["core"].pop("paper_paused")
    panel.update_state(state)
    panel.enabled.setChecked(True)
    panel.apply()
    assert not panel.client.commands
    state["core"]["paper_paused"] = True
    state.pop("strategies")
    panel.update_state(state)
    assert not panel.continue_button.isEnabled()


def test_continue_unconfirmed_uses_same_operation_lookup_and_never_reposts(panel):
    panel.update_state(long_snapshot())
    panel.continue_warmup()
    operation = panel.pending
    panel.client.failed.emit("command:" + operation, "lost")
    panel.continue_warmup()
    panel.apply()
    assert len(panel.client.commands) == 1
    assert panel.pending == operation and not panel.continue_button.isEnabled()
    panel.resolve_operation()
    assert panel.client.requests[-1][1]["tag"] == "operation:" + operation
    complete(panel, {"status": "unknown"}, tag="operation:")
    assert panel.pending == operation
    complete(panel, {"status": "completed", "result": {"revision": 3}}, tag="operation:")
    assert panel.pending is None and panel.continue_button.isEnabled()


def test_plan_summary_shows_utc_fixed_bounds_cursor_and_chunk_without_checkpoint(panel):
    panel.update_state(long_snapshot())
    assert "0/1" in panel.plan_summary.text()
    details = panel.plan_details.toPlainText()
    assert "fixed-plan" in details and "1,024/30,000" in details
    assert "1970/01/01 00:00 UTC" in details and "chunk 512 min" in details
    assert "alignment 1440" in details and "next" in details
    assert "private" not in details and "checkpoint" not in details
    assert panel.warmup.maximum() == 525600
    panel.update_state(long_snapshot(plan_state="complete"))
    assert "1/1" in panel.plan_summary.text() and not panel.continue_button.isEnabled()


def test_exhausted_not_ready_plan_requires_new_range_without_automatic_continuation(panel):
    state = long_snapshot(plan_state="not_ready")
    state["feed"]["warmup_plans"]["tef-daily"]["processed_minutes"] = 30000
    panel.update_state(state)
    assert not panel.continue_button.isEnabled()
    assert "Apply a longer/new plan" in panel.plan_summary.text()
    assert "does not extend" in panel.plan_summary.text()
    panel.continue_warmup()
    assert not panel.client.commands
    panel.enabled.setChecked(True)
    assert panel.apply_button.isEnabled()  # An explicitly chosen new range is available.


def test_unfinished_warming_plan_can_continue_alongside_exhausted_unready_plan(panel):
    state = long_snapshot(plan_state="not_ready")
    state["feed"]["warmup_plans"]["second-strategy"] = {
        **state["feed"]["warmup_plans"]["tef-daily"],
        "state": "warming",
        "plan_id": "other-fixed",
    }
    panel.update_state(state)
    assert panel.continue_button.isEnabled()
    panel.continue_warmup()
    assert panel.client.commands[-1][1:] == ("paper.feed.continue", {})


def test_large_warmup_page_remains_reachable_by_scrolling_at_both_sizes(panel):
    from PySide6.QtWidgets import QApplication, QScrollArea

    wrapper = QScrollArea()
    wrapper.setWidgetResizable(True)
    wrapper.setWidget(panel)
    panel.update_state(long_snapshot())
    wrapper.show()
    for width, height in ((1360, 860), (800, 560)):
        wrapper.resize(width, height)
        QApplication.instance().processEvents()
        assert wrapper.width() == width and wrapper.height() == height
        wrapper.ensureWidgetVisible(panel.continue_button)
        QApplication.instance().processEvents()
        assert panel.continue_button.geometry().bottom() < panel.hint.geometry().top()
        assert panel.plan_summary.geometry().bottom() < panel.plan_details.geometry().top()
        assert not wrapper.grab().isNull()
    wrapper.takeWidget()
    wrapper.close()
