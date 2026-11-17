"""Observed auxiliary SI contracts, missing-data rendering and draft persistence."""

import copy
import importlib.util
import json
from dataclasses import asdict
from urllib.parse import parse_qs, urlsplit

import pytest

pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("PySide6") is None, reason="optional Qt is not installed"
)


@pytest.fixture(scope="module")
def app():
    import os

    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-auxiliary-tests"])


def client():
    from PySide6.QtCore import QObject, Signal

    class Client(QObject):
        candlesReceived = Signal(str, dict)
        connectionChanged = Signal(bool)
        resultReceived = Signal(str, dict)
        failed = Signal(str, str)
        attached = True

        def __init__(self):
            super().__init__()
            self.calls = []

        def request(self, *args, **kwargs):
            self.calls.append((args, kwargs))

    return Client()


@pytest.fixture
def page(app, tmp_path):
    from omega.gui.charts_page import ChartsPage
    from omega.gui.models import Candle

    value = ChartsPage(client(), root=tmp_path)
    value._series_epoch, value._auxiliary_supported = "epoch-a", True
    rows = [Candle(i * 60000, 10, 12, 9, 11, 2) for i in range(5)]
    value.accept_candles(
        value.panes[0].selection.text(),
        {"candles": [asdict(row) for row in rows], "source": "database", "quality": {}},
    )
    yield value
    value.close()
    value.deleteLater()
    app.processEvents()


def enable(page, kind="OPENINTEREST", source=""):
    pane = page.panes[0]
    profile = copy.deepcopy(pane.profile)
    profile["enabled"][kind] = True
    profile["slots"][0] = {"indicator": kind, "visible": True}
    profile["auxiliary"][kind]["source"] = source
    pane.apply_profile(profile)
    return pane.auxiliary_requests[kind]


def response(request, *, field="METRIC_OPENINTEREST", unit="contracts", state="partial"):
    params = request.parameters
    return {
        "schema_version": 1,
        "scope": "retrospective_auxiliary_chart",
        "core_epoch": "epoch-a",
        **{key: value for key, value in params.items() if key != "source"},
        "as_of_ms": 900000,
        "source": params.get("source", params["instrument"] + ":observed"),
        "state": state,
        "series": {field: [[0, "12.5"], [120000, "15.25"]]},
        "units": {field: unit},
        "quality": {"expected_bars": 5, "observed_bars": 2, "missing_bars": 3, "partial_bars": 0},
        "gaps": [[60000, 120000], [180000, 300000]],
        "lineage": [
            {
                "open_time_ms": 0,
                "available_at_ms": 60000,
                "revision": 1,
                "checksum": "a" * 64,
                "quality": "observed",
                "coverage": "1",
            }
        ],
        "warnings": [],
    }


def deliver(page, request, payload=None):
    page.client.resultReceived.emit(request.tag, payload or response(request))


@pytest.mark.parametrize("version", [1, 2, 3])
def test_old_profile_migrates_only_in_memory(app, tmp_path, version):
    from omega.gui.chart_profiles import default_profile, validate_profile
    from omega.gui.charts_page import ChartsPage

    old = default_profile()
    old.pop("auxiliary")
    old["schema_version"] = version
    names = ("VOL", "MFI", "MMACD") if version == 1 else ("VOL", "MFI", "MMACD", "DMIxADX")
    old["enabled"] = {name: old["enabled"][name] for name in names}
    if version == 1:
        for key in ("dmi_period", "dmi_display", "psar", "swing"):
            old.pop(key)
    elif version == 2:
        old.pop("psar")
        old.pop("swing")
    before = copy.deepcopy(old)
    migrated = validate_profile(old)
    assert old == before and migrated["schema_version"] == 6
    assert not any(migrated["enabled"][name] for name in migrated["auxiliary"])
    # Discover the supported store path without assuming a production namespace.
    probe = ChartsPage(client(), root=tmp_path)
    try:
        path = probe.store.path
    finally:
        probe.close()
        probe.deleteLater()
    path.parent.mkdir(parents=True, exist_ok=True)
    workspace = {
        "schema_version": 1,
        "profiles": {"old": old},
        "layout": "grid",
        "linked": True,
        "panes": [{"instrument": "binance:usdm:BTCUSDT", "timeframe": 1, "profile": old}],
    }
    path.write_text(json.dumps(workspace), encoding="utf-8")
    contents = path.read_bytes()
    reopened = ChartsPage(client(), root=tmp_path)
    try:
        assert path.read_bytes() == contents
        assert reopened.profiles["old"]["schema_version"] == 6
    finally:
        reopened.close()
        reopened.deleteLater()


@pytest.mark.parametrize(
    "kind,fields",
    [
        ("OPENINTEREST", ["METRIC_OPENINTEREST", "METRIC_OPENINTERESTVALUE"]),
        ("LONGSHORTRATIO", ["METRIC_LONGSHORTRATIO", "METRIC_LONGACCOUNT"]),
        ("AGGTRADE", ["AGGTRADE_NOTIONALBUY", "AGGTRADE_QUANTITYSELL"]),
        ("DEPTH", ["UNKNOWN"]),
        ("FUNDING", ["FUNDING_RATE", "FUNDING_RATE"]),
    ],
)
def test_profile_rejects_unknown_or_mixed_units(app, kind, fields):
    from omega.gui.chart_profiles import default_profile, validate_profile

    profile = default_profile()
    profile["auxiliary"][kind]["fields"] = fields
    with pytest.raises(ValueError):
        validate_profile(profile)


def test_structured_choice_apply_and_explicit_save(page):
    editor = page.panes[0].settings_panel
    editor.enabled["AGGTRADE"].setChecked(True)
    editor.allocations[0].setCurrentText("AGGTRADE")
    editor.auxiliary_sources["AGGTRADE"].setText("explicit:flow")
    editor.auxiliary_metrics["AGGTRADE"].setCurrentIndex(1)
    assert editor.apply()
    assert not page.store.path.exists()
    assert page.panes[0].profile["auxiliary"]["AGGTRADE"] == {
        "source": "explicit:flow",
        "fields": ["AGGTRADE_QUANTITYBUY", "AGGTRADE_QUANTITYSELL"],
    }
    editor.profile_name.setText("observed")
    editor.save()
    assert page.store.load()["profiles"]["observed"] == page.panes[0].profile


def test_exact_api_query_sparse_alignment_readout_and_line_cache(page):
    request = enable(page, source="explicit:oi")
    args, kwargs = page.client.calls[-1]
    assert args[0] == "GET" and kwargs["tag"] == request.tag
    assert urlsplit(args[1]).path == "/api/chart-observations"
    assert parse_qs(urlsplit(args[1]).query) == {
        "instrument": [page.panes[0].selection.text()],
        "kind": ["OPENINTEREST"],
        "start_ms": ["0"],
        "end_ms": ["300000"],
        "period_minutes": ["1"],
        "source": ["explicit:oi"],
    }
    deliver(page, request)
    pane = page.panes[0]
    lines = request.lines()
    assert lines["METRIC_OPENINTEREST"] == (12.5, None, 15.25, None, None)
    assert pane.si_plots[0].unit_label == "contracts"
    calls = len(page.client.calls)
    pane._selection_readout(60000)
    assert (
        "missing" in pane.selected_readout.text()
        and "warmup" not in pane.selected_readout.text().split("METRIC_OPENINTEREST")[1]
    )
    pane._selection_readout(0)
    assert "12.5 contracts" in pane.selected_readout.text()
    pane._refresh_indicators()
    assert request.lines() is lines and len(page.client.calls) == calls
    assert request.metadata["lineage"][0]["available_at_ms"] == 60000


@pytest.mark.parametrize("change", ["epoch", "instrument", "source", "disconnected", "closed"])
def test_stale_reply_cannot_restore_cleared_observations(page, change):
    request = enable(page)
    tag, payload = request.tag, response(request)
    if change == "epoch":
        page._series_epoch = "epoch-b"
    elif change == "instrument":
        page.panes[0].selection.setText("binance:spot:ETHUSDT")
    elif change == "source":
        page.panes[0].profile["auxiliary"]["OPENINTEREST"]["source"] = "changed:oi"
    elif change == "disconnected":
        page.client.attached = False
    else:
        page._series_closed = True
    request.ensure()
    page.client.resultReceived.emit(tag, payload)
    assert not request.points


@pytest.mark.parametrize("change", ["demo", "api", "calendar", "range", "awaiting"])
def test_unavailable_context_never_queries_observation_api(page, change):
    pane = page.panes[0]
    if change == "demo":
        pane._raw_source = "demo"
    elif change == "api":
        page._auxiliary_supported = False
    elif change == "calendar":
        pane.minutes = "1w"
    elif change == "range":
        pane.raw = ()
    else:
        pane._raw_epoch = "old"
    before = sum("chart-observations" in args[1] for args, _ in page.client.calls)
    request = enable(page)
    assert request.state != "loading"
    assert sum("chart-observations" in args[1] for args, _ in page.client.calls) == before


@pytest.mark.parametrize(
    "corruption",
    ["nan", "overflow", "bool", "schema", "gap", "future", "hash", "order", "identity", "unit"],
)
def test_malformed_or_future_lineage_is_not_plotted(page, corruption):
    request = enable(page)
    value = response(request)
    if corruption in {"nan", "overflow"}:
        value["series"]["METRIC_OPENINTEREST"][0][1] = "NaN" if corruption == "nan" else "1e999"
    elif corruption == "bool":
        value["period_minutes"] = True
    elif corruption == "schema":
        value["schema_version"] = True
    elif corruption == "gap":
        value["gaps"] = [[0, 900000]]
    elif corruption == "future":
        value["lineage"][0]["available_at_ms"] = 900001
    elif corruption == "hash":
        value["lineage"][0]["checksum"] = "bad"
    elif corruption == "order":
        value["series"]["METRIC_OPENINTEREST"].reverse()
    elif corruption == "unit":
        value["units"]["METRIC_OPENINTEREST"] = "quote_asset"
    else:
        value["instrument"] = "binance:spot:ETHUSDT"
    deliver(page, request, value)
    assert request.state == "failed" and not request.points
    assert request.lines()["METRIC_OPENINTEREST"] == (None,) * 5


def test_retry_is_manual_and_source_change_invalidates(page):
    request = enable(page)
    page.client.failed.emit(request.tag, "failure")
    calls = len(page.client.calls)
    page.panes[0]._refresh_indicators()
    assert request.state == "failed" and len(page.client.calls) == calls
    request.retry.click()
    assert request.state == "loading" and len(page.client.calls) == calls + 1
    enable(page, source="different:oi")
    assert request.parameters["source"] == "different:oi" and not request.points


def test_depth_units_truthful_totals_and_funding_unavailable(page):
    request = enable(page, "DEPTH")
    payload = response(request, field="DEPTH_BIDS", unit="quote_asset")
    payload["series"]["DEPTH_ASKS"] = [[0, "15"]]
    payload["units"]["DEPTH_ASKS"] = "quote_asset"
    deliver(page, request, payload)
    assert "heatmap" in request.label.text() and request.unit() == "quote_asset"
    assert request.lines()["DEPTH_ASKS"] == (15.0, None, None, None, None)
    funding = enable(page, "FUNDING")
    payload = response(funding, field="FUNDING_RATE", unit="signed_rate", state="unsupported")
    payload["series"] = {}
    payload["warnings"] = ["Funding not available for spot"]
    deliver(page, funding, payload)
    assert funding.state == "unsupported" and not funding.points
    assert "Funding not available" in funding.label.toolTip()


def test_capability_transition_and_removed_pane_ignore_reply(page):
    page.update_state({"core": {"epoch": "epoch-a", "capabilities": {"chart_observations": True}}})
    request = enable(page)
    tag, payload = request.tag, response(request)
    page.add_chart("binance:spot:ETHUSDT")
    page._remove_pane(page.panes[0])
    page.client.resultReceived.emit(tag, payload)
    assert not request.points


def test_sparse_paths_never_connect_across_missing_observation(page):
    request = enable(page)
    payload = response(request)
    payload["series"]["METRIC_OPENINTEREST"] = [
        [0, "1"],
        [60000, "2"],
        [180000, "3"],
        [240000, "4"],
    ]
    payload["gaps"] = [[120000, 180000]]
    deliver(page, request, payload)
    plot = page.panes[0].si_plots[0]
    plot.resize(800, 160)
    plot._geometry()
    path = plot._paths[0][1]
    assert path.elementCount() == 4
    assert [path.elementAt(i).isMoveTo() for i in range(4)] == [True, False, True, False]


def test_metric_change_uses_existing_response_and_default_units(page):
    request = enable(page)
    payload = response(request)
    payload["series"]["METRIC_OPENINTERESTVALUE"] = [[0, "500"]]
    payload["units"]["METRIC_OPENINTERESTVALUE"] = "quote_asset"
    deliver(page, request, payload)
    count = len(page.client.calls)
    profile = copy.deepcopy(page.panes[0].profile)
    profile["auxiliary"]["OPENINTEREST"]["fields"] = ["METRIC_OPENINTERESTVALUE"]
    page.panes[0].apply_profile(profile)
    assert len(page.client.calls) == count
    assert request.lines()["METRIC_OPENINTERESTVALUE"] == (500.0, None, None, None, None)
    assert request.unit() == "quote_asset"


def test_unknown_enabled_flag_is_still_rejected(app):
    from omega.gui.chart_profiles import default_profile, validate_profile

    profile = default_profile()
    profile["schema_version"] = 3
    profile["enabled"]["not-a-real-si"] = False
    with pytest.raises(ValueError):
        validate_profile(profile)


def test_isolated_observations_have_cached_visible_markers_without_a_bridge(page, app):
    from PySide6.QtCore import QPointF

    request = enable(page)
    deliver(page, request)
    page.resize(800, 950)
    page.show()
    app.processEvents()
    plot = page.panes[0].si_plots[0]
    plot._geometry()
    markers = plot._sample_paths
    path = markers["METRIC_OPENINTEREST"]
    for timestamp, value in ((0, 12.5), (120000, 15.25)):
        center = QPointF(plot._x(plot.chart._bar_center(timestamp)), plot._y(value))
        assert path.contains(center)
        image = plot.grab().toImage()
        assert (
            image.pixelColor(round(center.x()), round(center.y())).name()
            == plot.chart._colors()["chart_ema"]
        )
    assert not path.contains(QPointF(plot._x(plot.chart._bar_center(60000)), plot._y(13.875)))
    plot._geometry()
    assert plot._sample_paths is markers
    assert not any(
        plot._paths[0][1].elementAt(i).isLineTo() for i in range(plot._paths[0][1].elementCount())
    )


@pytest.mark.parametrize("code", ["VOL", "MFI", "MMACD", "DMIxADX"])
def test_existing_si_geometry_has_no_auxiliary_point_markers(page, code):
    plot = page.panes[0].si_plots[0]
    plot.set_series(code, {"field": (1, None, 2, None, None)})
    plot._geometry()
    assert all(path.isEmpty() for path in plot._sample_paths.values())
