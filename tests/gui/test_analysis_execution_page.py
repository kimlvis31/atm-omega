"""Independent CA execution controls retain structured library editing and identity."""

import os
from copy import deepcopy

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QObject, Signal
from PySide6.QtWidgets import QApplication

from omega.gui.analysis_page import AnalysisPage
from omega.gui.presentation import configure_fonts, stylesheet
from omega.runtime.analysis_library import AnalysisLibrary

START = 1_700_000_040_000


def test_pinned_auxiliary_quality_names_retrospective_knowledge_mode(page):
    controls = page.execution
    controls.row = row(
        page,
        input_quality={
            "source": "committed_frames",
            "auxiliary_subscription": True,
            "knowledge_mode": "warmup_retrospective_then_live",
            "manifest_head": True,
        },
    )
    controls._render()
    assert "pinned auxiliary" in controls.quality.text()
    assert "DB auxiliary observations connected" in controls.quality.text()
    assert "Retrospective warmup" in controls.quality.text()


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    value = QApplication.instance() or QApplication(["ca-execution-controls"])
    configure_fonts(value)
    return value


@pytest.fixture
def page(app, tmp_path):
    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls, self.commands = [], []

        def request(self, method, path, *, tag):
            self.calls.append((method, path, tag))

        def command(self, kind, payload):
            identity = f"{len(self.commands) + 1:032x}"
            self.commands.append((identity, kind, deepcopy(payload)))
            return identity

    library = AnalysisLibrary(tmp_path / "library.json", references_provider=lambda: [])
    created = library.command(
        "analysis.template.create",
        {
            "name": "Base",
            "expected_revision": 0,
            "configuration": {
                "moving_averages": [{"interval_id": 0, "ema_period": 2, "price_precision": 8}],
                "momentum": [],
            },
        },
        operation_id="1" * 32,
    )
    for name, symbol in (("Alpha", "ETHUSDT"), ("Beta", "BTCUSDT")):
        library.command(
            "analysis.instance.create",
            {
                "name": name,
                "instrument": "binance:usdm:" + symbol,
                "template_id": created["item"]["id"],
                "expected_revision": library.snapshot()["revision"],
            },
            operation_id=f"{library.snapshot()['revision'] + 1:032x}",
        )
    value = AnalysisPage(Client())
    value.set_presentation("light", "en")
    value.apply_snapshot(library.snapshot())
    value.update_state({"core": {"epoch": "core-a", "capabilities": {"analysis_execution": True}}})
    value.resize(1360, 900)
    value.show()
    select(value, 0)
    value.execution.setChecked(True)
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def select(page, index):
    page.instance_list.setCurrentRow(index)
    return page.execution.target


def row(page, **changes):
    target = page.execution.target
    return {
        "analysis_id": target["id"],
        "instrument": target["instrument"],
        "execution_id": "e" * 32,
        "state": "preparing",
        "analysis_revision": target["revision"],
        "configuration_hash": "c" * 64,
        "ready": False,
        "last_closed_ms": START + 60000,
        "reason": "preparing_snapshot",
        "outputs": {"0_EMA_0_EMA": None, "CLOSED_0": True, "zero": 0},
        "warmup": {"processed_minutes": 1, "total_minutes": 240},
        "input_quality": {
            "fresh": True,
            "source": "committed_ohlcv",
            "missing_outputs": ["0_EMA_0_EMA"],
        },
        **changes,
    }


def get_tag(page):
    return [
        tag for _, path, tag in page.client.calls if path.startswith("/api/analysis-execution?")
    ][-1]


def respond(page, value=None, *, tag=None, epoch="core-a"):
    page.client.resultReceived.emit(
        tag or get_tag(page),
        {
            "schema_version": 1,
            "core_epoch": epoch,
            "instances": [] if value is None else [value],
            "load_errors": [],
        },
    )


def complete(page, value):
    identity = page.client.commands[-1][0]
    page.client.resultReceived.emit("command:" + identity, {"status": "completed", "result": value})


def test_start_uses_saved_selected_revision_and_keeps_cac_and_instance_drafts(page):
    target = deepcopy(page.execution.target)
    page.instance_name.setText("Unsubmitted name")
    page.instance_name.textEdited.emit("Unsubmitted name")
    page.symbol.setText("WRONGDRAFT")
    page.execution.warmup.setValue(512)
    page.execution.start()
    _, kind, payload = page.client.commands[-1]
    assert kind == "analysis.execution.start"
    assert payload == {
        "analysis_id": target["id"],
        "expected_revision": target["revision"],
        "warmup_minutes": 512,
        "restart": False,
    }
    assert page.instance_name.text() == "Unsubmitted name" and page._instance_dirty
    assert page.instance_parameters.rowCount() == 1 and page.definition_table.rowCount() > 0


def test_empty_execution_list_is_configured_and_can_start_after_fresh_epoch(page):
    respond(page)
    assert page.execution.row is None and page.execution.start_button.isEnabled()
    assert "No execution record" in page.execution.summary.text()
    assert not page.execution.stop_button.isEnabled()


def test_new_draft_and_cleared_selection_cannot_execute_previous_ca(page):
    page.instance_list.clearSelection()
    page.execution.start()
    assert not page.client.commands and page.execution.target is None
    select(page, 1)
    page.new_instance()
    page.execution.start()
    assert not page.client.commands and page.execution.target is None


def test_start_result_displays_server_values_none_and_real_zero_separately(page):
    page.execution.start()
    complete(page, row(page))
    assert page.execution.row["state"] == "preparing"
    table = {
        page.execution.outputs.item(i, 0).text(): page.execution.outputs.item(i, 1).text()
        for i in range(page.execution.outputs.rowCount())
    }
    assert table["0_EMA_0_EMA"] == "Not ready / unavailable" and table["zero"] == "0"
    assert (
        "UTC" in page.execution.summary.text() and "closed OHLCV" in page.execution.quality.text()
    )
    assert page.execution.stop_button.isEnabled() and not page.execution.start_button.isEnabled()


def test_stop_is_bound_to_exact_execution_and_restart_is_explicit(page):
    respond(page, row(page, state="running", ready=True))
    page.execution.start()
    assert not page.client.commands
    page.execution.restart.setChecked(True)
    page.execution.start()
    assert page.client.commands[-1][2]["restart"] is True
    complete(page, row(page, execution_id="a" * 32))
    page.execution.stop()
    assert page.client.commands[-1][1:] == (
        "analysis.execution.stop",
        {"analysis_id": page.execution.target["id"], "execution_id": "a" * 32},
    )
    complete(page, row(page, execution_id="a" * 32, state="stopped"))
    assert page.execution.row["state"] == "stopped"


def test_frozen_running_revision_does_not_silently_follow_rebased_ca(page):
    old = row(page, state="running", ready=True)
    respond(page, old)
    changed = deepcopy(page.library)
    changed["revision"] += 1
    for item in changed["instances"]:
        if item["id"] == page.execution.target["id"]:
            item["revision"] += 1
    page.apply_snapshot(changed)
    assert page.execution.row["analysis_revision"] == old["analysis_revision"]
    assert page.execution.target["revision"] > old["analysis_revision"]
    assert f"snapshot r{old['analysis_revision']}" in page.execution.summary.text()
    assert not page.execution.start_button.isEnabled()


def test_late_different_ca_reply_and_command_do_not_pollute_new_selection(page):
    old_row, old_tag = row(page), get_tag(page)
    page.execution.start()
    select(page, 1)
    new_target = page.execution.target["id"]
    respond(page, old_row, tag=old_tag)
    complete(page, old_row)
    assert page.execution.target["id"] == new_target and page.execution.row is None
    respond(page, row(page, execution_id="f" * 32))
    assert page.execution.row["analysis_id"] == new_target


def test_get_started_before_command_cannot_restore_an_old_execution(page):
    old_tag = get_tag(page)
    page.execution.start()
    newer = row(page, execution_id="a" * 32)
    complete(page, newer)
    respond(page, row(page, execution_id="b" * 32, state="stopped"), tag=old_tag)
    assert page.execution.row["execution_id"] == "a" * 32


def test_core_restart_rejects_old_request_and_old_command_result(page):
    old_tag, old_row = get_tag(page), row(page)
    page.execution.start()
    page.update_state({"core": {"epoch": "core-b"}})
    respond(page, old_row, tag=old_tag)
    complete(page, old_row)
    assert page.execution.row is None
    respond(page, row(page), epoch="core-b")
    assert page.execution.row["analysis_id"] == page.execution.target["id"]


def test_disconnect_keeps_last_result_and_unknown_command_only_checks_original_operation(page):
    respond(page, row(page, state="running", ready=True))
    page.execution.stop()
    operation = page.client.commands[-1][0]
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    assert page.execution.row is not None and not page.execution.stop_button.isEnabled()
    assert page.execution._uncertain == operation
    page.client.attached = True
    page.client.connectionChanged.emit(True)
    page.execution.start()
    assert len(page.client.commands) == 1
    page.execution.resolve()
    _, path, tag = page.client.calls[-1]
    assert path == "/api/operations/" + operation
    page.client.resultReceived.emit(
        tag, {"status": "completed", "result": row(page, state="stopped")}
    )
    assert page.execution._uncertain is None and page.execution.row["state"] == "stopped"


def test_malformed_completed_or_wrong_stop_identity_stays_uncertain(page):
    respond(page, row(page, state="running", ready=True))
    page.execution.stop()
    complete(page, row(page, execution_id="f" * 32, state="stopped"))
    assert page.execution._uncertain == page.client.commands[-1][0]
    page.execution.start()
    assert len(page.client.commands) == 1


def test_execution_poll_preserves_unsubmitted_template_instance_and_warmup_controls(page):
    page.instance_name.setText("Draft")
    page.instance_name.textEdited.emit("Draft")
    page.template_name.setText("Draft CAC")
    page.template_name.textEdited.emit("Draft CAC")
    page.execution.warmup.setValue(10080)
    page.execution.restart.setChecked(True)
    respond(page, row(page))
    assert page.instance_name.text() == "Draft" and page.template_name.text() == "Draft CAC"
    assert page._instance_dirty and page._dirty
    assert page.execution.warmup.value() == 10080 and page.execution.restart.isChecked()


def test_search_and_state_sort_keep_identity_and_hidden_selection_has_no_execution_authority(page):
    first = page.execution.target["id"]
    page.instance_search.setText("BTCUSDT")
    page.execution.start()
    assert page.execution.target is None and not page.client.commands
    assert not page.instance_actions["rename"].isEnabled()
    page.instance_search.clear()
    page.instance_sort.setCurrentIndex(page.instance_sort.findData("instrument"))
    selected = select(page, 0)
    assert selected["id"] != first and selected["instrument"].endswith("BTCUSDT")
    respond(page, row(page, state="blocked"))
    page.instance_sort.setCurrentIndex(page.instance_sort.findData("state"))
    assert page.execution.target["id"] == selected["id"]
    assert "blocked" in page.instance_list.currentItem().text()


def test_large_valid_output_is_bounded_and_indicates_omitted_display_rows(page):
    respond(page, row(page, outputs={f"value_{index:04d}": index for index in range(512)}))
    assert page.execution.outputs.rowCount() == 256
    assert "256/512" in page.execution.outputs.horizontalHeaderItem(0).text()


@pytest.mark.parametrize(
    "field,value",
    [
        ("analysis_id", "wrong"),
        ("instrument", "binance:spot:SOLUSDT"),
        ("execution_id", "../invalid"),
        ("state", "ready"),
        ("ready", 1),
        ("last_closed_ms", START + 1),
        ("outputs", {"bad": float("inf")}),
        ("input_quality", []),
        ("warmup", {"processed_minutes": 5, "total_minutes": 4}),
    ],
)
def test_invalid_execution_rows_do_not_replace_last_valid_result(page, field, value):
    respond(page, row(page))
    before = deepcopy(page.execution.row)
    page.execution.refresh()
    respond(page, row(page, **{field: value}))
    assert page.execution.row == before
    assert (
        "Invalid" in page.execution.message.text()
        or "Inconsistent" in page.execution.message.text()
    )


def test_wrong_epoch_unsupported_endpoint_and_explicit_capability_remain_honest(page):
    respond(page, row(page), epoch="other-core")
    assert page.execution.row is None and "epoch" in page.execution.message.text()
    page.execution.refresh()
    page.client.failed.emit(get_tag(page), "Core request failed (404)")
    assert "404" in page.execution.message.text()
    page.update_state({"core": {"epoch": "core-a", "capabilities": {"analysis_execution": False}}})
    assert not page.execution.start_button.isEnabled()


@pytest.mark.parametrize("width", [800, 1360])
def test_collapsible_execution_controls_are_accessible_with_structured_form(
    page, app, tmp_path, width
):
    page.resize(width, 900)
    page.setStyleSheet(stylesheet("light"))
    page.tabs.setCurrentIndex(1)
    page.execution.setChecked(True)
    respond(page, row(page))
    app.processEvents()
    scroll = page.tabs.widget(1).widget(1)
    scroll.verticalScrollBar().setValue(scroll.verticalScrollBar().maximum())
    app.processEvents()
    assert page.execution.content.isVisible()
    assert page.execution.outputs.rowCount() == 3
    assert page.instance_parameters.rowCount() == 1
    assert (
        page.execution.start_button.geometry().right()
        < page.execution.stop_button.geometry().left()
    )
    assert page.execution.summary.wordWrap()
    page.grab().save(str(tmp_path / f"ca-execution-{width}.png"))
    page.execution.setChecked(False)
    assert page.execution.content.isHidden() and page.instance_parameters.rowCount() == 1
