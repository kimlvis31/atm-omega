"""NES/TPD SI settings, source/context fences and unit-safe core-only rendering."""

import importlib.util
import json
import os
from copy import deepcopy
from dataclasses import asdict
from hashlib import sha256

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["derived-si-tests"])


@pytest.fixture
def page(app, tmp_path):
    from PySide6.QtCore import QObject, Signal

    from omega.gui.charts_page import ChartsPage

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, method, path, payload=None, *, tag):
            self.calls.append((method, path, deepcopy(payload), tag))

    widget = ChartsPage(Client(), root=tmp_path)
    widget.resize(1360, 900)
    widget.show()
    widget.update_state({"core": {"epoch": "a", "capabilities": {"chart_series": True}}})
    raw(widget)
    app.processEvents()
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def raw(page, count=30, source="database"):
    from omega.gui.models import Candle

    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "source": source,
            "candles": [asdict(Candle(i * 60000, 100, 104, 98, 102, 5)) for i in range(count)],
        },
    )


def enable(page, kind):
    from omega.gui.chart_profiles import swap_slot

    pane = page.panes[0]
    draft = deepcopy(pane.profile)
    draft["enabled"][kind] = True
    draft["slots"] = swap_slot(draft["slots"], 1, kind)
    draft["slots"][1]["visible"] = True
    pane.apply_profile(draft)
    return page.client.calls[-1] if page.client.calls else None


def reply(post):
    params = post[2]["parameters"]
    kind, period = params["kind"], params["period_minutes"]
    produced = params["end_ms"] + 2000
    rows = []
    for opening in range(params["start_ms"], params["end_ms"], period * 60000):
        row = {
            "open_time_ms": opening,
            "close_time_ms": opening + period * 60000,
            "available_at_ms": produced,
            "segment": 0,
            kind: None if opening == 0 else 3.0,
            "ABSMA": None if opening == 0 else 3.0,
            "ABSMAREL": None if opening == 0 else 1.0,
        }
        if kind == "NES":
            row.update(
                input_ready=True,
                input_reason="observed",
                observed_minutes=period,
                required_minutes=period,
                source_available_at_ms=opening + period * 60000 + 1,
                auxiliary_checksum="b" * 64,
            )
        rows.append(row)
    requested = (params["end_ms"] - params["start_ms"]) // 60000
    result = {
        **{key: value for key, value in params.items() if key not in {"start_ms", "end_ms"}},
        "source": "database",
        "as_of_ms": produced - 1000,
        "produced_at_ms": produced,
        "replay_start_ms": params["start_ms"],
        "knowledge_mode": "retrospective_display",
        "source_checksum": "c" * 64,
        "rows": rows,
        "quality": {
            "requested_minutes": requested,
            "observed_minutes": requested,
            "missing_minutes": 0,
            "reset_count": 0,
            "complete_bars": len(rows),
        },
    }
    if kind == "NES":
        result.update(
            auxiliary_source=params["instrument"] + ":aggTrade",
            source_checksum_scope="committed_candles_trade_flow_revisions_v1",
        )
        result["quality"].update(
            auxiliary_observed_minutes=requested,
            auxiliary_partial_minutes=0,
            auxiliary_missing_minutes=0,
        )
    return {
        "id": post[2]["id"],
        "parameters": params,
        "state": "complete",
        "core_epoch": "a",
        "spec_hash": sha256(
            json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "result": result,
    }


@pytest.mark.parametrize("kind", ["NES", "TPD"])
def test_enabled_requests_common_core_only_and_single_field_unit_switch_no_new_io(page, kind):
    post = enable(page, kind)
    assert post[:2] == ("POST", "/api/chart-series")
    page.client.resultReceived.emit(post[3], reply(post))
    pane = page.panes[0]
    request = pane.derived_requests[kind]
    assert request.state == "complete"
    assert pane.si_plots[1].code == kind
    assert pane.si_plots[1].lines == {kind: (None, *([3.0] * 29))}
    assert pane.si_plots[1].unit_label == ("quote_asset" if kind == "NES" else "signed_return_bias")
    before = len(page.client.calls)
    draft = deepcopy(pane.profile)
    draft[kind.lower()]["display"] = "ABSMAREL"
    draft["timezone"] = "UTC+09:00"
    pane.apply_profile(draft)
    assert len(page.client.calls) == before and pane.si_plots[1].unit_label == "dimensionless"
    assert pane.si_plots[1].lines == {"ABSMAREL": (None, *([1.0] * 29))}


def test_old_v4_exact_enabled_map_migrates_default_off_without_write(page, tmp_path):
    from omega.gui.chart_profiles import validate_profile

    old = deepcopy(page.panes[0].profile)
    old["schema_version"] = 4
    for code in ("NES", "TPD"):
        del old[code.lower()]
        del old["enabled"][code]
    before = deepcopy(old)
    migrated = validate_profile(old)
    assert old == before and migrated["schema_version"] == 6
    assert not migrated["enabled"]["NES"] and not migrated["enabled"]["TPD"]
    assert migrated["slots"] == old["slots"]
    page.panes[0].apply_profile(migrated)
    assert page.client.calls == [] and not (tmp_path / "state/chart.workspace.json").exists()


def test_nes_range_is_not_silently_truncated_and_user_anchor_is_explicit(page):
    raw(page, 1500)
    enable(page, "NES")
    pane = page.panes[0]
    assert pane.derived_requests["NES"].state == "origin_range"
    assert not any(call[0] == "POST" for call in page.client.calls)
    settings = pane.settings_panel
    settings.set_draft(pane.profile)
    settings._derived_origin("NES")
    assert settings.derived_controls["NES"]["origin_ms"].text() == str(60 * 60000)
    assert settings.apply()
    post = page.client.calls[-1]
    assert post[2]["parameters"]["start_ms"] == 60 * 60000
    raw(page, 1501)
    assert pane.derived_requests["NES"].state == "origin_range"


@pytest.mark.parametrize("kind", ["NES", "TPD"])
@pytest.mark.parametrize("change", ["epoch", "origin", "instrument", "disconnect", "parameters"])
def test_late_result_never_repopulates_new_context(page, kind, change):
    post = enable(page, kind)
    pane = page.panes[0]
    if change == "epoch":
        page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    elif change in {"origin", "parameters"}:
        draft = deepcopy(pane.profile)
        draft[kind.lower()]["origin_ms" if change == "origin" else "nSamples"] = (
            60000 if change == "origin" else 40
        )
        pane.apply_profile(draft)
    elif change == "instrument":
        pane.selection.setText("binance:usdm:ETHUSDT")
    else:
        page.client.attached = False
        page.client.connectionChanged.emit(False)
    page.client.resultReceived.emit(post[3], reply(post))
    assert pane.derived_requests[kind].rows == ()
    assert all(value is None for values in pane.si_plots[1].lines.values() for value in values)


@pytest.mark.parametrize("kind", ["NES", "TPD"])
def test_fixed_origin_not_viewport_seed_and_identical_new_core_data_requeries(page, kind):
    post = enable(page, kind)
    page.client.resultReceived.emit(post[3], reply(post))
    pane = page.panes[0]
    count = len(page.client.calls)
    pane.chart._pan_time(20, pane.chart._time_bounds())
    assert len(page.client.calls) == count
    raw(page, 40)
    assert page.client.calls[-1][2]["parameters"]["start_ms"] == post[2]["parameters"]["start_ms"]
    page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    assert any(
        call[0] == "GET" and call[1].startswith("/api/candles?")
        for call in page.client.calls[count:]
    )
    raw(page, 40)
    assert pane.derived_requests[kind].state == "queued"


@pytest.mark.parametrize("change", ["held", "checksum", "source", "coverage", "future", "nan"])
def test_malformed_nes_snapshot_is_unknown_not_flat_or_zero(page, change):
    post = enable(page, "NES")
    data = reply(post)
    row = data["result"]["rows"][2]
    if change == "held":
        row["input_ready"] = False
    elif change == "checksum":
        row["auxiliary_checksum"] = "bad"
    elif change == "source":
        data["result"]["auxiliary_source"] = "different"
    elif change == "coverage":
        row["observed_minutes"] = 0
    elif change == "future":
        row["source_available_at_ms"] = data["result"]["as_of_ms"] + 1
    else:
        row["NES"] = float("nan")
    page.client.resultReceived.emit(post[3], data)
    assert page.panes[0].derived_requests["NES"].state == "unknown"
    assert page.panes[0].derived_requests["NES"].rows == ()


def test_missing_gap_and_isolated_observed_point_remain_visible(page, app):
    post = enable(page, "NES")
    data = reply(post)
    for row in data["result"]["rows"]:
        if row["open_time_ms"] != 10 * 60000:
            row.update(
                NES=None,
                ABSMA=None,
                ABSMAREL=None,
                input_ready=False,
                input_reason="missing",
                observed_minutes=0,
                source_available_at_ms=None,
            )
    data["result"]["quality"].update(auxiliary_observed_minutes=1, auxiliary_missing_minutes=29)
    page.client.resultReceived.emit(post[3], data)
    plot = page.panes[0].si_plots[1]
    app.processEvents()
    plot._geometry()
    assert not plot._sample_paths["NES"].isEmpty()
    assert plot._paths[0][1].elementCount() == 1


def test_disconnected_same_id_lookup_and_off_preserve_three_slot_contract(page):
    post = enable(page, "NES")
    request = page.panes[0].derived_requests["NES"]
    identity = request.identity
    page.client.failed.emit(post[3], "timeout")
    request.poll()
    assert request.identity == identity and page.client.calls[-1][:2] == (
        "GET",
        "/api/chart-series/" + identity,
    )
    draft = deepcopy(page.panes[0].profile)
    draft["enabled"]["NES"] = False
    page.panes[0].apply_profile(draft)
    assert request.rows == () and request.state == "off"
    assert len(page.panes[0].si_plots) == 3


def test_selected_missing_observation_is_not_labeled_warmup_and_keeps_unit(page):
    post = enable(page, "NES")
    data = reply(post)
    data["result"]["rows"][10].update(
        NES=None,
        ABSMA=None,
        ABSMAREL=None,
        input_ready=False,
        input_reason="missing",
        observed_minutes=0,
        source_available_at_ms=None,
    )
    data["result"]["quality"].update(auxiliary_observed_minutes=29, auxiliary_missing_minutes=1)
    page.client.resultReceived.emit(post[3], data)
    pane = page.panes[0]
    pane._selection_readout(10 * 60000)
    assert "NES missing [0/1] quote_asset" in pane.selected_readout.text()
    assert "NES warmup" not in pane.selected_readout.text()
    pane._selection_readout(0)
    assert "NES warmup quote_asset" in pane.selected_readout.text()
    draft = deepcopy(pane.profile)
    draft["nes"]["display"] = "ABSMAREL"
    pane.apply_profile(draft)
    pane._selection_readout(10 * 60000)
    assert "ABSMAREL missing [0/1] dimensionless" in pane.selected_readout.text()
