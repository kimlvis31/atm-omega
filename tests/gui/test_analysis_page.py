"""Structured CAC editing → durable API contract → instrument CA snapshot flow."""

import importlib.util

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt dependency is not installed"
)


@pytest.fixture(scope="module")
def qt_app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    from omega.gui.presentation import configure_fonts

    app = QApplication.instance() or QApplication(["analysis-library-tests"])
    configure_fonts(app)
    return app


@pytest.fixture
def page(qt_app, tmp_path):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.analysis_page import AnalysisPage
    from omega.runtime.analysis_library import AnalysisLibrary

    class Client(QObject):
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        connectionChanged = Signal(bool)

        def __init__(self):
            super().__init__()
            self.attached, self.calls, self.commands = True, [], []
            self.library = AnalysisLibrary(
                tmp_path / "analysis.json", references_provider=lambda: []
            )

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

        def command(self, kind, payload):
            operation = f"op-{len(self.commands)}"
            self.commands.append((operation, kind, payload))
            return operation

        def deliver_library(self):
            self.resultReceived.emit("analysis-library", self.library.snapshot())

        def complete(self):
            from omega.domain.errors import ValidationError

            operation, kind, payload = self.commands[-1]
            try:
                result = {
                    "status": "completed",
                    "result": self.library.command(kind, payload, operation_id=operation),
                }
            except ValidationError as exc:
                result = {"status": "rejected", "code": str(exc)}
            self.resultReceived.emit("command:" + operation, result)
            self.deliver_library()

    instance = AnalysisPage(Client())
    instance.set_presentation("light", "en")
    instance.resize(1200, 850)
    instance.show()
    instance.client.deliver_library()
    qt_app.processEvents()
    yield instance
    instance.close()
    instance.deleteLater()
    qt_app.processEvents()


def create_template(page, name="MMACD CAC"):
    page.template_name.setText(name)
    page.save_template()
    page.client.complete()
    return page.client.library.snapshot()["templates"][-1]


def test_structured_template_save_clone_and_rename_retain_internal_identity(page):
    first = create_template(page)
    assert first["configuration"]["moving_averages"][0]["ema_period"] == 20
    assert page._selected_template == first["id"]
    assert page.definition_table.rowCount() == 2
    page.template_name.setText("Clone")
    page.clone_template()
    page.client.complete()
    clone = page.client.library.snapshot()["templates"][-1]
    assert page._selected_template == clone["id"] != first["id"]
    page.template_name.setText("Renamed clone")
    page.rename_template()
    page.client.complete()
    assert page._selected_template == clone["id"]
    assert page.template_name.text() == "Renamed clone"
    assert page.client.commands[-1][1] == "analysis.template.rename"


def test_indicator_forms_preserve_per_interval_parameters_and_reject_duplicates(page):
    page.interval.setCurrentIndex(page.interval.findData(2))
    page.kind.setCurrentText("MFI")
    page.samples.setValue(23)
    page.line.setValue(2)
    page.add_definition()
    assert page._definitions[-1] == {
        "kind": "MFI",
        "interval_id": 2,
        "nSamples": 23,
        "line_index": 2,
    }
    page.add_definition()
    assert len(page._definitions) == 3
    assert "Duplicate" in page.status.text()
    template = create_template(page)
    assert template["configuration"]["momentum"][0]["nSamples"] == 23
    page.definition_table.selectRow(2)
    assert page.kind.currentText() == "MFI" and page.line.value() == 2
    page.samples.setValue(17)
    page.update_definition()
    assert page._definitions[-1]["nSamples"] == 17


def test_invalid_mmaccd_or_advanced_json_never_discards_current_form(page):
    before = list(page._definitions)
    page.kind.setCurrentText("MMACD")
    page.periods.setText("12, 12")
    page.add_definition()
    assert page._definitions == before
    page.json_input.setPlainText('{"PSAR": {}}')
    page.apply_json()
    assert page._definitions == before
    assert page.json_input.toPlainText() == '{"PSAR": {}}'
    assert "Unsupported" in page.status.text()
    page.json_input.setPlainText(
        '{"momentum": [{"interval_id": 0, "analysis_code": "MFI", "nSamples": "bad"}]}'
    )
    page.apply_json()
    assert page._definitions == before
    assert "Invalid nSamples" in page.status.text()
    page.json_input.setPlainText(
        '{"momentum": [{"interval_id": 0, "analysis_code": "MFI", "nSamples": 12}]}'
    )
    page.apply_json()
    assert len(page._definitions) == 1 and page._definitions[0]["kind"] == "MFI"


def test_refresh_does_not_overwrite_a_user_draft_or_its_base_revision(page):
    first = create_template(page)
    page.template_name.setText("Unsaved name")
    page._mark_dirty()
    previous_revision = page._edit_revision
    page.client.library.command(
        "analysis.template.rename",
        {"id": first["id"], "name": "Elsewhere", "expected_revision": previous_revision},
        operation_id="elsewhere",
    )
    page.client.deliver_library()
    assert page.template_name.text() == "Unsaved name"
    assert page._edit_revision == previous_revision
    page.save_template()
    page.client.complete()
    assert "revision conflict" in page.status.text()
    assert page.template_name.text() == "Unsaved name"


def test_instrument_ca_uses_saved_template_and_future_edits_do_not_follow(page):
    template = create_template(page)
    page.instance_name.setText("BTC CA")
    page.create_instance()
    page.client.complete()
    ca = page.client.library.snapshot()["instances"][0]
    assert page._selected_instance == ca["id"]
    assert "not automatically running" in page.instance_info.text()
    assert not page.market.isEnabled() and not page.symbol.isEnabled()
    bound = page.client.library.snapshot_for_strategy(ca["id"], ca["instrument"])
    page.definition_table.selectRow(0)
    page.samples.setValue(31)
    page.update_definition()
    # Selecting the CAC again adopts the current library revision; preserve edited rows.
    page._edit_revision = page.library["revision"]
    page.save_template()
    page.client.complete()
    assert page.client.library.snapshot()["instances"][0]["template_revision"] == 1
    page.instance_list.setCurrentRow(0)
    page.rebase_instance()
    page.client.complete()
    latest = page.client.library.snapshot()["instances"][0]
    assert latest["template_revision"] == 2
    assert bound["configuration"]["moving_averages"][0]["ema_period"] == 20
    assert template["id"] == latest["template_id"]


def test_uncertain_command_blocks_resend_and_uses_readonly_operation_query(page):
    page.template_name.setText("Pending")
    page.save_template()
    operation = page.client.commands[-1][0]
    page.client.failed.emit("command:" + operation, "connection lost")
    assert page._uncertain == operation
    page.save_template()
    assert len(page.client.commands) == 1
    page.resolve_operation()
    assert page.client.calls[-1][0] == ("GET", "/api/operations/" + operation)
    page.client.resultReceived.emit("analysis-operation:" + operation, {"status": "unknown"})
    assert page._uncertain == operation
    assert not page.template_actions["save"].isEnabled()


def test_delete_reference_rejection_stays_visible_and_keeps_the_cac(page):
    template = create_template(page)
    page.instance_name.setText("CA")
    page.create_instance()
    page.client.complete()
    page.template_list.setCurrentRow(0)
    page.delete_template()
    page.client.complete()
    assert "referenced by a CA" in page.status.text()
    assert page.client.library.snapshot()["templates"][0]["id"] == template["id"]


def test_disconnected_or_degraded_library_cannot_mutate_and_embedded_title_hides(page):
    page.set_shell_embedded(True)
    assert page.title.isHidden() and not page.notice.isHidden()
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    assert not page.template_actions["save"].isEnabled()
    page.client.attached = True
    page.apply_snapshot(page.client.library.snapshot() | {"persistence": "degraded"})
    assert not page.template_actions["save"].isEnabled()


def test_malformed_snapshot_keeps_previous_library_and_editor(page):
    first = create_template(page)
    previous = page.library
    page.apply_snapshot(previous | {"templates": [{"id": first["id"]}]})
    assert page.library is previous
    assert "Invalid analysis library record" in page.status.text()


def test_reconnect_requests_fresh_library_without_reposting_operations(page):
    page.client.attached = False
    page.client.connectionChanged.emit(False)
    page.client.attached = True
    page.client.connectionChanged.emit(True)
    assert page.client.calls[-1][0] == ("GET", "/api/analysis-library")
    assert not page.client.commands


def test_provider_rejection_inside_completed_envelope_is_not_saved(page):
    page.template_name.setText("Rejected")
    page.save_template()
    page.client.resultReceived.emit(
        "command:" + page._pending,
        {"status": "completed", "result": {"status": "rejected", "code": "REVISION_CONFLICT"}},
    )
    assert page._pending is None and page._uncertain is None
    assert page.status.text() == "REVISION_CONFLICT"
    assert page._selected_template == ""


def test_completed_response_without_receipt_does_not_claim_success(page):
    page.template_name.setText("Unconfirmed")
    page.save_template()
    operation = page._pending
    page.client.resultReceived.emit("command:" + operation, {"status": "completed", "result": {}})
    assert page._uncertain == operation
    assert "Outcome uncertain" in page.status.text()


def test_new_draft_can_reselect_the_same_single_saved_cac_and_ca(page, qt_app):
    from PySide6.QtCore import Qt
    from PySide6.QtTest import QTest

    template = create_template(page)
    page.new_template()
    assert page.template_list.currentRow() == -1
    QTest.mouseClick(
        page.template_list.viewport(),
        Qt.MouseButton.LeftButton,
        pos=page.template_list.visualItemRect(page.template_list.item(0)).center(),
    )
    assert page._selected_template == template["id"]
    page.instance_name.setText("CA")
    page.create_instance()
    page.client.complete()
    ca = page.client.library.snapshot()["instances"][0]
    page.tabs.setCurrentIndex(1)
    qt_app.processEvents()
    page.new_instance()
    assert page.instance_list.currentRow() == -1
    QTest.mouseClick(
        page.instance_list.viewport(),
        Qt.MouseButton.LeftButton,
        pos=page.instance_list.visualItemRect(page.instance_list.item(0)).center(),
    )
    assert page._selected_instance == ca["id"]


@pytest.mark.parametrize("new_draft", [False, True])
def test_template_receipt_preserves_later_edits_and_binds_only_its_own_draft(page, new_draft):
    page.template_name.setText("Submitted")
    page.save_template()
    if new_draft:
        page.new_template()
    page.template_name.setText("Later draft")
    page.template_name.textEdited.emit("Later draft")
    page.client.complete()
    saved = page.client.library.snapshot()["templates"][0]
    assert saved["name"] == "Submitted"
    assert page.template_name.text() == "Later draft" and page._dirty
    assert page._edit_revision == 1
    assert page._selected_template == ("" if new_draft else saved["id"])
    page.save_template()
    assert page.client.commands[-1][1] == "analysis.template." + (
        "create" if new_draft else "update"
    )
    page.client.complete()
    records = page.client.library.snapshot()["templates"]
    assert len(records) == (2 if new_draft else 1)
    assert records[-1]["name"] == "Later draft"


@pytest.mark.parametrize("new_draft", [False, True])
def test_instance_receipt_never_replaces_a_newer_ca_draft(page, new_draft):
    create_template(page)
    page.instance_name.setText("Submitted CA")
    page.create_instance()
    if new_draft:
        page.new_instance()
    page.instance_name.setText("Later CA")
    page.instance_name.textEdited.emit("Later CA")
    page.client.complete()
    saved = page.client.library.snapshot()["instances"][0]
    assert page.instance_name.text() == "Later CA" and page._instance_dirty
    assert page._instance_edit_revision == 2
    assert page._selected_instance == ("" if new_draft else saved["id"])
    if new_draft:
        page.create_instance()
    else:
        page.rename_instance()
    page.client.complete()
    records = page.client.library.snapshot()["instances"]
    assert len(records) == (2 if new_draft else 1)
    assert records[-1]["name"] == "Later CA"


def test_clean_refresh_adopts_current_form_and_revision_but_instance_edits_are_preserved(page):
    template = create_template(page)
    page.client.library.command(
        "analysis.template.rename",
        {
            "id": template["id"],
            "name": "External rename",
            "expected_revision": 1,
        },
        operation_id="external-rename",
    )
    page.client.deliver_library()
    assert page.template_name.text() == "External rename" and page._edit_revision == 2
    page.instance_name.setText("Unsaved CA")
    page.instance_name.textEdited.emit("Unsaved CA")
    base_revision = page._instance_edit_revision
    page.client.library.command(
        "analysis.template.rename",
        {
            "id": template["id"],
            "name": "External again",
            "expected_revision": 2,
        },
        operation_id="external-again",
    )
    page.client.deliver_library()
    assert page.instance_name.text() == "Unsaved CA"
    assert page._instance_edit_revision == base_revision


def test_swing_percent_form_roundtrip_preserves_exact_ratio_and_disables_period_fields(page):
    page.kind.setCurrentText("SWING")
    assert page.swing_range.isEnabled() and page.line.isEnabled()
    assert (
        not page.samples.isEnabled()
        and not page.periods.isEnabled()
        and not page.precision.isEnabled()
    )
    page.swing_range.setText("0.512345678901234567890123456789")
    page.line.setValue(3)
    page.add_definition()
    expected = "0.00512345678901234567890123456789"
    assert page._definitions[-1]["swing_range"] == expected
    assert "nSamples" not in page._definitions[-1]
    page.definition_table.selectRow(2)
    assert page.swing_range.text() == "0.512345678901234567890123456789"
    assert "%" in page.definition_table.item(2, 2).text()
    assert "ratio" in page.definition_table.item(2, 2).text()
    page.kind.setCurrentText("MFI")
    assert page.samples.isEnabled() and not page.swing_range.isEnabled()


def test_psar_direct_af_form_roundtrip_uses_eta_names_and_independent_line_precision(page):
    from omega.gui.analysis_page import configuration_rows, form_configuration

    page.kind.setCurrentText("PSAR")
    assert page.psar_start.isEnabled() and page.precision.isEnabled() and page.line.isEnabled()
    assert not page.samples.isEnabled() and not page.swing_range.isEnabled()
    assert (page.psar_start.text(), page.psar_acceleration.text(), page.psar_maximum.text()) == (
        "0.02",
        "0.005",
        "0.2",
    )
    page.interval.setCurrentIndex(page.interval.findData(11))
    page.line.setValue(3)
    page.precision.setValue(5)
    page.psar_start.setText("0.03")
    page.psar_acceleration.setText("0.007")
    page.psar_maximum.setText("0.4")
    page.add_definition()
    expected = {
        "kind": "PSAR",
        "interval_id": 11,
        "line_index": 3,
        "price_precision": 5,
        "start": 0.03,
        "acceleration": 0.007,
        "maximum": 0.4,
    }
    assert page._definitions[-1] == expected
    configuration = form_configuration(page._definitions)
    assert configuration_rows(configuration)[-1] == expected
    page.definition_table.selectRow(2)
    assert page.psar_acceleration.text() == "0.007"
    assert "AF+=" in page.definition_table.item(2, 2).text()
    assert "%" not in page.definition_table.item(2, 2).text()


@pytest.mark.parametrize(
    "start,acceleration,maximum",
    [
        ("0", "0.005", "0.2"),
        ("0.2", "0.005", "0.2"),
        ("0.02", "0", "0.2"),
        ("nan", "0.005", "0.2"),
        ("0.02", "inf", "0.2"),
        ("0.02", "0.005", "-0.2"),
    ],
)
def test_invalid_psar_draft_does_not_replace_existing_definitions(
    page, start, acceleration, maximum
):
    before = list(page._definitions)
    page.kind.setCurrentText("PSAR")
    page.psar_start.setText(start)
    page.psar_acceleration.setText(acceleration)
    page.psar_maximum.setText(maximum)
    page.add_definition()
    assert page._definitions == before and not page.client.commands


def test_psar_rejects_unsupported_interval_duplicate_and_nonnumeric_json(page):
    from omega.gui.analysis_page import configuration_rows

    page.kind.setCurrentText("PSAR")
    page.interval.setCurrentIndex(page.interval.findData(12))
    before = list(page._definitions)
    page.add_definition()
    assert page._definitions == before and "interval 0..11" in page.status.text()
    page.interval.setCurrentIndex(0)
    page.add_definition()
    count = len(page._definitions)
    page.add_definition()
    assert len(page._definitions) == count and "Duplicate" in page.status.text()
    for key, value in (("start", True), ("maximum", "not a number"), ("line_index", True)):
        with pytest.raises(ValueError):
            configuration_rows({"parabolic": [{"interval_id": 0, key: value}]})


def test_psar_absence_does_not_add_empty_group_to_existing_cac(page):
    from omega.gui.analysis_page import configuration_rows, form_configuration

    existing = form_configuration(page._definitions)
    assert "parabolic" not in existing
    assert form_configuration(configuration_rows(existing)) == existing


def test_psar_numeric_strings_normalize_without_inventing_afmax_one_cap():
    from omega.gui.analysis_page import configuration_rows, form_configuration

    values = {
        "parabolic": [{"interval_id": 0, "start": "0.02", "acceleration": "0.005", "maximum": "2"}]
    }
    rows = configuration_rows(values)
    actual = form_configuration(rows)["parabolic"][0]
    assert actual["maximum"] == 2 and actual["acceleration"] == 0.005
    with pytest.raises(ValueError):
        configuration_rows({"parabolic": [{"interval_id": 0, "maximum": 10**1000}]})


def test_psar_fields_remain_scroll_reachable_and_distinct_at_both_sizes(page, qt_app):
    from PySide6.QtWidgets import QScrollArea

    page.kind.setCurrentText("PSAR")
    for width, height in ((1360, 860), (800, 560)):
        page.resize(width, height)
        qt_app.processEvents()
        assert page.width() == width and page.height() == height
        scroll = next(
            area
            for area in page.findChildren(QScrollArea)
            if area.widget() is not None and area.widget().isAncestorOf(page.psar_maximum)
        )
        scroll.ensureWidgetVisible(page.psar_maximum)
        qt_app.processEvents()
        assert page.psar_start.geometry().bottom() < page.psar_acceleration.geometry().top()
        assert page.psar_acceleration.geometry().bottom() < page.psar_maximum.geometry().top()
        assert scroll.verticalScrollBar().maximum() > 0
        assert not page.grab().isNull()


def test_psar_template_ca_and_strategy_snapshot_configuration_stay_explicit(page):
    page._definitions = []
    page._render_definitions()
    page.kind.setCurrentText("PSAR")
    page.add_definition()
    template = create_template(page, "PSAR CAC")
    assert template["configuration"]["parabolic"][0]["acceleration"] == 0.005
    assert "PSAR" in page.library["capabilities"]["features"]
    page.instance_name.setText("BTC PSAR")
    page.create_instance()
    page.client.complete()
    ca = page.client.library.snapshot()["instances"][0]
    assert ca["configuration"] == template["configuration"]
    assert page.instance_parameters.item(0, 1).text() == "PSAR"
    page.definition_table.selectRow(0)
    page.psar_acceleration.setText("0.015")
    page.update_definition()
    page._edit_revision = page.library["revision"]
    page.save_template()
    page.client.complete()
    assert page.client.library.snapshot()["instances"][0]["configuration"] == ca["configuration"]


def test_swing_template_and_ca_save_preserve_line_range_and_do_not_follow_later_edits(page):
    page._definitions = []
    page._render_definitions()
    page.kind.setCurrentText("SWING")
    page.swing_range.setText("0.5")
    page.line.setValue(0)
    page.add_definition()
    template = create_template(page, "SWING CAC")
    assert template["configuration"]["swing"][0]["swing_range"] == 0.005
    assert "SWING" in page.library["capabilities"]["features"]
    page.instance_name.setText("SWING BTC CA")
    page.create_instance()
    page.client.complete()
    ca = page.client.library.snapshot()["instances"][0]
    assert ca["configuration"]["swing"][0]["line_index"] == 0
    assert page.instance_parameters.item(0, 1).text() == "SWING"
    page.definition_table.selectRow(0)
    assert page.swing_range.text() == "0.5"
    page.swing_range.setText("1")
    page.update_definition()
    page._edit_revision = page.library["revision"]
    page.save_template()
    page.client.complete()
    assert page.client.library.snapshot()["instances"][0]["configuration"] == ca["configuration"]


@pytest.mark.parametrize("text", ["", "NaN", "sNaN", "Infinity", "-0.5", "0.001", "1e999999"])
def test_invalid_swing_percent_retains_draft_without_sending_commands(page, text):
    before = list(page._definitions)
    page.kind.setCurrentText("SWING")
    page.swing_range.setText(text)
    page.add_definition()
    assert page._definitions == before and not page.client.commands
    assert page.swing_range.text() == text


def test_advanced_invalid_swing_never_discards_json_or_existing_rows(page):
    before = list(page._definitions)
    for value in (None, "NaN", "0.00001"):
        import json

        payload = json.dumps({"swing": [{"interval_id": 0, "line_index": 0, "swing_range": value}]})
        page.json_input.setPlainText(payload)
        page.apply_json()
        assert page._definitions == before and page.json_input.toPlainText() == payload


def test_advanced_swing_form_loading_keeps_value_and_rejects_duplicate_line(page):
    from omega.gui.analysis_page import configuration_rows, form_configuration

    payload = {"swing": [{"interval_id": 2, "line_index": 9, "swing_range": "0.003"}]}
    rows = configuration_rows(payload)
    assert form_configuration(rows)["swing"] == payload["swing"]
    with pytest.raises(ValueError, match="Duplicate"):
        form_configuration([*rows, *rows])
