"""Selected IVP keeps the Eta right-fixed price placement and causal UI fences."""

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

    return QApplication.instance() or QApplication(["selected-ivp-tests"])


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


def raw(page, count=30):
    from omega.gui.models import Candle

    page.accept_candles(
        page.panes[0].selection.text(),
        {
            "candles": [asdict(Candle(i * 60000, 100, 110, 90, 103, 5)) for i in range(count)],
            "source": "database",
        },
    )


def enable(page):
    pane = page.panes[0]
    draft = deepcopy(pane.profile)
    draft["ivp"]["enabled"] = True
    draft["ivp"]["nSamples"] = 3
    pane.apply_profile(draft)
    assert pane.ivp_request.state == "select_bar"
    pane.chart.selected_timestamp = 20 * 60000
    pane.chart.selectionChanged.emit(20 * 60000)
    return page.client.calls[-1]


def response(post, state="ready"):
    params = post[2]["parameters"]
    produced = params["end_ms"] + 2000
    row = {
        "open_time_ms": params["end_ms"] - params["period_minutes"] * 60000,
        "close_time_ms": params["end_ms"],
        "available_at_ms": produced,
        "state": state,
        "gammaFactor": params["gammaFactor"],
        "betaFactor": 1.0,
        "divisionHeight": 10.0,
        "filtered": [float(i + 1) for i in range(20)],
        "maximum": 20.0,
        "divisions": 20,
        "boundaries": [3, 15],
        "near_boundaries": [None] * 10,
    }
    if state in {"seed", "missing", "zero"}:
        row.update(filtered=None, maximum=None, boundaries=None)
        if state != "zero":
            row.update(betaFactor=None, divisionHeight=None, divisions=None)
        if state == "missing":
            row["near_boundaries"] = None
    return {
        "id": post[2]["id"],
        "parameters": deepcopy(params),
        "state": "complete",
        "spec_hash": sha256(
            json.dumps(params, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
        "core_epoch": "a",
        "result": {
            **{key: value for key, value in params.items() if key not in {"start_ms", "end_ms"}},
            "replay_start_ms": params["start_ms"],
            "selected_close_ms": params["end_ms"],
            "source": "database",
            "as_of_ms": produced - 1000,
            "produced_at_ms": produced,
            "knowledge_mode": "retrospective_display",
            "source_checksum": "c" * 64,
            "quality": {
                "requested_minutes": (params["end_ms"] - params["start_ms"]) // 60000,
                "observed_minutes": (params["end_ms"] - params["start_ms"]) // 60000,
                "missing_minutes": 0,
                "reset_count": 0,
                "complete_bars": (params["end_ms"] - params["start_ms"])
                // (params["period_minutes"] * 60000),
            },
            "rows": [row],
        },
    }


def test_exact_selected_cutoff_and_later_data_viewport_style_do_not_recompute(page):
    post = enable(page)
    assert post[:2] == ("POST", "/api/chart-series")
    assert post[2]["parameters"]["start_ms"] == 0
    assert post[2]["parameters"]["end_ms"] == 21 * 60000
    page.client.resultReceived.emit(post[3], response(post))
    pane = page.panes[0]
    assert pane.ivp_request.state == "complete"
    before = len(page.client.calls)
    raw(page, 40)
    pane.chart._pan_time(20, pane.chart._time_bounds())
    draft = deepcopy(pane.profile)
    draft["ivp"]["color"] = "#ffeecc"
    draft["ivp"]["visible"] = False
    draft["timezone"] = "UTC+09:00"
    pane.apply_profile(draft)
    assert len(page.client.calls) == before
    assert pane.chart.volume_profile[-1] is False
    assert len(pane.si_plots) == 3


def test_right_fixed_price_rectangles_and_clear_selection(page):
    post = enable(page)
    page.client.resultReceived.emit(post[3], response(post))
    chart = page.panes[0].chart
    chart._geometry()
    rects = chart.volume_profile_rectangles()
    assert rects and all(abs(rect.right() - chart._plot().right()) < 1e-8 for rect in rects)
    widths = [rect.width() for rect in rects]
    chart._pan_time(20, chart._time_bounds())
    chart._geometry()
    assert [rect.width() for rect in chart.volume_profile_rectangles()] == widths
    chart.clear_selection()
    assert chart.volume_profile is None and chart.volume_profile_rectangles() == ()


@pytest.mark.parametrize("state", ["seed", "zero", "missing"])
def test_unready_states_explicit_and_no_rectangles(page, state):
    post = enable(page)
    page.client.resultReceived.emit(post[3], response(post, state))
    request = page.panes[0].ivp_request
    assert request.state == "complete" and request.rows[0]["state"] == state
    assert page.panes[0].chart.volume_profile_rectangles() == ()


@pytest.mark.parametrize("change", ["epoch", "selection", "clear", "origin"])
def test_stale_completion_not_displayed_after_context_change(page, change):
    post = enable(page)
    pane = page.panes[0]
    if change == "epoch":
        page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    elif change == "selection":
        pane.chart.selected_timestamp = 22 * 60000
        pane.chart.selectionChanged.emit(22 * 60000)
    elif change == "clear":
        pane.chart.clear_selection()
    else:
        draft = deepcopy(pane.profile)
        draft["ivp"]["origin_ms"] = 5 * 60000
        pane.apply_profile(draft)
    page.client.resultReceived.emit(post[3], response(post))
    assert pane.chart.volume_profile is None


@pytest.mark.parametrize(
    "field,value",
    [
        ("filtered", [float("nan")] * 20),
        ("divisions", True),
        ("maximum", 19.0),
        ("boundaries", [4, 3]),
        ("state", []),
        ("available_at_ms", 1),
    ],
)
def test_malformed_profile_cleared_without_drawing(page, field, value):
    post = enable(page)
    reply = response(post)
    reply["result"]["rows"][0][field] = value
    page.client.resultReceived.emit(post[3], reply)
    assert page.panes[0].ivp_request.state == "unknown"
    assert page.panes[0].chart.volume_profile is None


def test_old_profile_migrates_without_write_and_structured_apply(tmp_path, page):
    from omega.gui.chart_profiles import validate_profile

    pane = page.panes[0]
    old = deepcopy(pane.profile)
    del old["ivp"]
    assert validate_profile(old)["ivp"]["enabled"] is False
    settings = pane.settings_panel
    settings.ivp_controls["enabled"].setChecked(True)
    settings.ivp_controls["nSamples"].setText("20")
    settings._ivp_origin()
    assert settings.apply()
    assert pane.profile["ivp"]["origin_ms"] == 0
    assert pane.profile["ivp"]["nSamples"] == 20
    assert not (tmp_path / "state/chart.workspace.json").exists()


def test_finite_large_volumes_never_overflow_right_fixed_rectangles(page):
    import math

    post = enable(page)
    reply = response(post)
    reply["result"]["rows"][0].update(filtered=[1e308] * 20, maximum=1e308)
    page.client.resultReceived.emit(post[3], reply)
    chart = page.panes[0].chart
    chart._geometry()
    rectangles = chart.volume_profile_rectangles()
    assert rectangles
    assert all(
        math.isfinite(value)
        for rect in rectangles
        for value in (rect.x(), rect.y(), rect.width(), rect.height(), rect.right(), rect.bottom())
    )
    assert all(abs(rect.right() - chart._plot().right()) < 1e-8 for rect in rectangles)


@pytest.mark.parametrize(
    "changes",
    [
        {"missing_minutes": 1},
        {"requested_minutes": True},
        {"reset_count": 10001},
        {"complete_bars": 10000},
        {"unknown": 0},
    ],
)
def test_contradictory_quality_metadata_rejected(page, changes):
    post = enable(page)
    reply = response(post)
    reply["result"]["quality"].update(changes)
    page.client.resultReceived.emit(post[3], reply)
    assert page.panes[0].ivp_request.state == "unknown"
    assert page.panes[0].chart.volume_profile is None


def test_ivp_only_epoch_change_reloads_and_identical_new_core_rows_resume(page):
    old_post = enable(page)
    pane = page.panes[0]
    page.client.resultReceived.emit(old_post[3], response(old_post))
    before = len(page.client.calls)
    page.update_state({"core": {"epoch": "b", "capabilities": {"chart_series": True}}})
    assert pane.ivp_request.state == "awaiting_data" and pane.chart.volume_profile is None
    assert any(
        call[0] == "GET" and call[1].startswith("/api/candles?")
        for call in page.client.calls[before:]
    )
    revision = pane._raw_revision
    raw(page)
    assert pane._raw_revision == revision  # The new epoch's snapshot is byte-identical.
    new_post = page.client.calls[-1]
    assert new_post[:2] == ("POST", "/api/chart-series")
    assert new_post[2]["id"] != old_post[2]["id"]
    assert new_post[2]["parameters"] == old_post[2]["parameters"]
    page.client.resultReceived.emit(old_post[3], response(old_post))
    assert pane.chart.volume_profile is None
    fresh = response(new_post)
    fresh["core_epoch"] = "b"
    page.client.resultReceived.emit(new_post[3], fresh)
    assert pane.ivp_request.state == "complete"
    assert pane.chart.volume_profile is not None


def test_oversized_profile_number_is_a_normal_draft_error(page):
    from omega.gui.chart_profiles import validate_profile

    draft = deepcopy(page.panes[0].profile)
    draft["ivp"]["gammaFactor"] = 10**400
    with pytest.raises(ValueError, match="finite display"):
        validate_profile(draft)
