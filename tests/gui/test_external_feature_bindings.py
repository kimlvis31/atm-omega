"""Registered input selection stays behind the core and fails closed on stale lists."""

import json
import os
from copy import deepcopy

import pytest

pytest.importorskip("PySide6")
from PySide6.QtCore import QDateTime, QObject, QTimeZone, Signal
from PySide6.QtWidgets import QApplication

from omega.domain.external_features import ExternalFeatureBinding
from omega.domain.models import Instrument, Market
from omega.gui.external_feature_bindings import (
    PATH,
    ExternalFeatureBindings,
    required_fields,
    validate_wait_ms,
)
from omega.gui.margin_page import MarginPage
from omega.strategies.registry import StrategyRegistry

BTC = Instrument("binance", Market.USDM, "BTCUSDT")
ETH = Instrument("binance", Market.USDM, "ETHUSDT")
START = 1700000040000


class Client(QObject):
    resultReceived = Signal(str, dict)
    failed = Signal(str, str)
    connectionChanged = Signal(bool)
    modulesReceived = Signal(dict)
    attached = True

    def __init__(self):
        super().__init__()
        self.calls = []

    def request(self, method, path, payload=None, *, tag):
        self.calls.append((method, path, deepcopy(payload), tag))


def binding(instrument=BTC, fields=("NN.score",), identity="model"):
    return ExternalFeatureBinding(
        "causal_nn", identity, "main", instrument, "a" * 64, "b" * 64, fields
    )


def document(*bindings, epoch="core"):
    return {
        "bindings": [row.document() for row in bindings],
        "server_time_ms": START,
        "core_epoch": epoch,
        "strategy_delivery_enabled": True,
        "strategy_delivery_scope": "virtual_margin_v2_opt_in",
        "live_orders_enabled": False,
    }


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    return QApplication.instance() or QApplication(["registered-inputs"])


@pytest.fixture
def page(app):
    page = MarginPage(Client())
    page.timer.stop()
    page.set_modules({"ts": StrategyRegistry().list_modules()})
    page.update_state({"core": {"epoch": "core", "capabilities": {"margin_trading": True}}})
    page.apply_snapshot({"core_epoch": "core", "state": "ready", "accounts": [], "operations": []})
    for edit in (page.start, page.bracket_time):
        edit.setDateTime(QDateTime.fromMSecsSinceEpoch(START, QTimeZone(b"UTC")))
    page.module.setCurrentIndex(page.module.findData("neural_signal"))
    page.manual_assumptions.setChecked(True)
    yield page
    page.close()
    page.deleteLater()
    app.processEvents()


def deliver(page, *bindings):
    page.refresh()
    call = next(call for call in reversed(page.client.calls) if call[1] == PATH)
    page.client.resultReceived.emit(call[3], document(*bindings))
    return call


def select(page, value):
    page.external_inputs.selection.setCurrentIndex(
        page.external_inputs.selection.findData(value.binding_sha256)
    )


def test_neural_manifest_annotation_resolves_instance_field_and_non_nn_module_uses_same_metadata(
    page,
):
    schema = page._schema("neural_signal")
    assert required_fields(schema, {}) == ("NN.score",)
    assert required_fields(schema, {"signal_field": "NN.custom"}) == ("NN.custom",)
    generic = deepcopy(page._definitions["neural_signal"])
    generic.update(module_id="another_demo", id="another_demo")
    page.set_modules({"ts": [generic]})
    page.module.setCurrentIndex(page.module.findData("another_demo"))
    assert page.external_inputs.required == ("NN.score",)
    assert page.analysis_panel.isHidden()


@pytest.mark.parametrize("field", ["score", "OTHER.score", 1, None, "NN." + "x" * 125])
def test_metadata_namespace_or_missing_dynamic_value_never_guessed(page, field):
    with pytest.raises(ValueError):
        required_fields(page._schema("neural_signal"), {"signal_field": field})


def test_only_current_instrument_required_fields_and_explicit_selection_are_allowed(page):
    right = binding()
    deliver(
        page,
        binding(ETH),
        binding(fields=("NN.other",), identity="other"),
        binding(fields=("NN.score", "NN.extra"), identity="extra"),
        right,
    )
    assert page.external_inputs.selection.count() == 2
    assert page.external_inputs.selection.currentIndex() == 0
    assert not page.configure_button.isEnabled()
    page.save_strategy()
    assert page.rows == []
    select(page, right)
    assert page.configure_button.isEnabled()
    page.save_strategy()
    assert page.rows[0]["strategy"]["external_features"] == [right.document()]
    assert page.rows[0]["strategy"]["analysis"] == {}
    assert not page.analysis_panel.isVisible()
    assert not any(call[0] == "POST" for call in page.client.calls)
    page.configure()
    post = [call for call in page.client.calls if call[0] == "POST"]
    assert len(post) == 1
    assert post[0][2]["parameters"]["strategies"][0]["external_features"] == [right.document()]
    assert post[0][2]["parameters"]["external_feature_wait_ms"] == 15000


def test_change_signal_field_or_instrument_clears_incompatible_selection(page):
    right, custom = binding(), binding(fields=("NN.custom",), identity="custom")
    deliver(page, right, custom)
    select(page, right)
    config = json.loads(page.configuration.toPlainText())
    config["signal_field"] = "NN.custom"
    page.configuration.setPlainText(json.dumps(config))
    assert page.external_inputs.required == ("NN.custom",)
    assert page.external_inputs.selection.count() == 2
    assert not page.configure_button.isEnabled()
    select(page, custom)
    page.symbol.setText("ETHUSDT")
    assert page.external_inputs.selection.count() == 1
    assert not page.configure_button.isEnabled()


def test_epoch_disconnect_failed_response_and_retirement_drop_selection_without_posts(page):
    right = binding()
    deliver(page, right)
    select(page, right)
    deliver(page, right)
    assert page.external_inputs.selection.currentData() == right.binding_sha256
    deliver(page)
    assert page.external_inputs.selection.currentIndex() == 0
    deliver(page, right)
    select(page, right)
    page.refresh()
    call = page.client.calls[-1]
    assert call[1] == PATH
    page.client.failed.emit(call[3], "offline")
    assert not page.external_inputs.bindings
    deliver(page, right)
    select(page, right)
    page.set_attached(False)
    assert not page.external_inputs.bindings
    page.set_attached(True)
    deliver(page, right)
    page.update_state({"core": {"epoch": "new", "capabilities": {"margin_trading": True}}})
    assert page.external_inputs.epoch is None
    assert not any(call[0] == "POST" for call in page.client.calls)


def test_ttl_masks_authority_and_old_epoch_response_cannot_restore_cache(page):
    now = [10.0]
    page.external_inputs.clock = lambda: now[0]
    right = binding()
    deliver(page, right)
    select(page, right)
    now[0] += 10.001
    page._controls()
    assert not page.configure_button.isEnabled()
    with pytest.raises(ValueError, match="Refresh"):
        page.strategy_document()
    page.refresh()
    old = page.client.calls[-1]
    page.update_state({"core": {"epoch": "other", "capabilities": {"margin_trading": True}}})
    page.client.resultReceived.emit(old[3], document(right))
    assert not page.external_inputs.bindings


@pytest.mark.parametrize("problem", ["hash", "scope", "live", "duplicate", "size"])
def test_malformed_or_wrong_scope_cached_document_never_offers_unverified_binding(app, problem):
    widget = ExternalFeatureBindings()
    value = document(binding())
    if problem == "hash":
        value["bindings"][0]["producer_sha256"] = "c" * 64
    elif problem == "scope":
        value["strategy_delivery_scope"] = "live"
    elif problem == "live":
        value["live_orders_enabled"] = True
    elif problem == "duplicate":
        value["bindings"] *= 2
    else:
        value["bindings"] *= 129
    with pytest.raises(ValueError):
        widget.apply_snapshot(value, "core")
    assert not widget.bindings
    widget.deleteLater()


def test_legacy_target_draft_keeps_exact_shape_and_never_requests_nn_inputs(page):
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    page.client.calls.clear()
    row = page.strategy_document()
    assert set(row["strategy"]) == {
        "id",
        "instrument",
        "module",
        "configuration",
        "analysis",
        "constraints",
    }
    assert row["strategy"]["configuration"] == {"quantity": "1"}
    page.refresh()
    assert not any(call[1] == PATH for call in page.client.calls)
    page.save_strategy()
    assert "external_feature_wait_ms" not in page.parameters()
    assert page.external_wait_panel.isHidden()


def test_saved_nn_row_is_revalidated_even_while_editing_legacy_row(page):
    right = binding()
    deliver(page, right)
    select(page, right)
    page.save_strategy()
    page.new_strategy()
    page.module.setCurrentIndex(page.module.findData("target_quantity"))
    page.save_strategy()
    deliver(page)
    page.configure()
    assert not any(call[0] == "POST" for call in page.client.calls)
    assert "registered NN input" in page.status.text()


def test_saved_selection_survives_edit_and_parameters_validation_without_hash_editors(page):
    right = binding()
    deliver(page, right)
    select(page, right)
    page.save_strategy()
    assert page.parameters()["strategies"][0]["external_features"] == [right.document()]
    assert page.external_inputs.selection.currentData() == right.binding_sha256
    assert page.parameters()["strategies"][0]["external_features"] == [right.document()]
    assert not page.external_inputs.selection.isEditable()


def test_missing_metadata_blocks_unknown_module_without_guessing_nn_fields(page):
    page.set_modules({"ts": [{"module_id": "unknown", "configuration_schema": {}}]})
    page.module.setCurrentIndex(page.module.findData("unknown"))
    assert not page.configure_button.isEnabled()
    assert "Unsupported input metadata" in page.external_inputs.selection.currentText()
    page.save_strategy()
    assert page.rows == []


def test_input_refresh_button_only_requests_core_get_and_preserves_draft(page):
    before = page.configuration.toPlainText()
    page.external_inputs.refresh_button.click()
    assert any(call[0] == "GET" and call[1] == PATH for call in page.client.calls)
    assert not any(call[0] == "POST" for call in page.client.calls)
    assert page.configuration.toPlainText() == before


def test_slow_get_uses_request_start_age_instead_of_giving_stale_list_new_lifetime(page):
    now = [10.0]
    page.external_inputs.clock = lambda: now[0]
    page.refresh()
    call = next(call for call in reversed(page.client.calls) if call[1] == PATH)
    now[0] = 21.0
    right = binding()
    page.client.resultReceived.emit(call[3], document(right))
    select(page, right)
    assert page.external_inputs.received_at == 10.0
    assert not page.configure_button.isEnabled()
    with pytest.raises(ValueError, match="Refresh"):
        page.strategy_document()


def test_long_nn_field_matches_core_capacity_and_remains_selectable(page):
    field = "NN." + "x" * 124
    config = json.loads(page.configuration.toPlainText())
    config["signal_field"] = field
    page.configuration.setPlainText(json.dumps(config))
    assert page.external_inputs.required == (field,)
    long = binding(fields=(field,))
    deliver(page, long)
    select(page, long)
    assert page.strategy_document()["strategy"]["external_features"] == [long.document()]


@pytest.mark.parametrize("value", [0, 15000, 30000])
def test_nn_wait_structured_control_serializes_account_limit_without_changing_binding(page, value):
    right = binding()
    deliver(page, right)
    select(page, right)
    page.external_feature_wait.setValue(value)
    page.save_strategy()
    params = page.parameters()
    assert params["external_feature_wait_ms"] == value
    assert type(params["external_feature_wait_ms"]) is int
    assert params["strategies"][0]["external_features"] == [right.document()]
    assert validate_wait_ms(value) == value


@pytest.mark.parametrize("value", [-1, 30001, True, False, "15000", 15000.0, None])
def test_nn_wait_rejects_out_of_range_or_non_native_integer(value):
    with pytest.raises(ValueError):
        validate_wait_ms(value)
