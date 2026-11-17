"""Bound same-ID uncertainty and stop idle timers in all inherited chart jobs."""

import importlib.util
import json
import os
from copy import deepcopy
from hashlib import sha256

import pytest

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")
KINDS = ("PSAR", "SWING", "NES", "TPD", "IVP", "PRICE_WINDOWS")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["chart-lookup-polling"])


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
    widget.accept_candles(
        widget.panes[0].selection.text(),
        {
            "core_epoch": "a",
            "source": "database",
            "candles": [
                dict(
                    open_time_ms=index * 60000,
                    open="100",
                    high="104",
                    low="98",
                    close="102",
                    volume="5",
                )
                for index in range(30)
            ],
        },
    )
    app.processEvents()
    widget.client.calls.clear()
    yield widget
    widget.close()
    widget.deleteLater()
    app.processEvents()


def controller(pane, kind):
    if kind in {"PSAR", "SWING"}:
        return pane.price_requests[kind]
    if kind in {"NES", "TPD"}:
        return pane.derived_requests[kind]
    return pane.ivp_request if kind == "IVP" else pane.price_windows_request


def enable(page, kind):
    pane = page.panes[0]
    profile = deepcopy(pane.profile)
    if kind == "PRICE_WINDOWS":
        profile["price_windows"]["sma"]["enabled"] = True
        profile["price_windows"]["sma"]["lines"][0].update(active=True, nSamples=2)
    elif kind in {"NES", "TPD"}:
        profile["enabled"][kind] = True
    else:
        profile[kind.lower()]["enabled"] = True
        if kind == "IVP":
            pane.chart.selected_timestamp = 20 * 60000
    pane.apply_profile(profile)
    return controller(pane, kind), page.client.calls[-1]


def response(post, state="queued"):
    parameters = post[2]["parameters"]
    return {
        "id": post[2]["id"],
        "parameters": deepcopy(parameters),
        "state": state,
        "core_epoch": "a",
        "spec_hash": sha256(
            json.dumps(parameters, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
        ).hexdigest(),
        "result": None,
    }


def exhaust(page, request):
    for count in range(8):
        assert request.tag is not None
        assert not request.timer.isActive()
        page.client.failed.emit(request.tag, "unclassified transport/API failure")
        if count < 7:
            assert request.state == "unknown" and request.timer.isActive()
            request.poll()
    assert request.state == "lookup_paused" and not request.timer.isActive()


@pytest.mark.parametrize("kind", KINDS)
def test_initial_and_terminal_poll_is_idle_without_context_work(page, monkeypatch, kind):
    request = controller(page.panes[0], kind)
    assert not request.timer.isActive()
    calls = []
    monkeypatch.setattr(request, "context", lambda: calls.append("context"))
    for state in (
        "off",
        "complete",
        "failed",
        "cancelled",
        "expired",
        "disconnected",
        "lookup_paused",
    ):
        request.state = state
        request.poll()
    assert not calls and not page.client.calls


@pytest.mark.parametrize("kind", KINDS)
def test_pending_inflight_hidden_and_disconnected_guards_precede_context(page, monkeypatch, kind):
    request, post = enable(page, kind)
    calls, context = [], request.context

    def tracked():
        calls.append("context")
        return context()

    monkeypatch.setattr(request, "context", tracked)
    request.poll()
    assert not calls and len(page.client.calls) == 1
    page.client.resultReceived.emit(request.tag, response(post, "running"))
    calls.clear()
    page.panes[0].hide()
    request.poll()
    assert not calls and len(page.client.calls) == 1
    page.client.attached = False
    request.poll()
    assert not calls and not request.timer.isActive()


@pytest.mark.parametrize("kind", KINDS)
def test_eight_failures_pause_manual_lookup_keeps_id_and_valid_response_resumes(page, kind):
    request, post = enable(page, kind)
    assert not request.timer.isActive()  # POST in flight
    exhaust(page, request)
    assert len(page.client.calls) == 8
    assert sum(call[0] == "POST" for call in page.client.calls) == 1
    assert all(call[1] == "/api/chart-series/" + post[2]["id"] for call in page.client.calls[1:])
    identity, parameters = request.identity, deepcopy(request.params)
    for _ in range(20):
        request.poll()
        request.ensure()
        request.status()
    assert len(page.client.calls) == 8
    assert request.retry.isVisible() and request.rows == ()
    request.request_again()
    assert len(page.client.calls) == 9 and page.client.calls[-1][0] == "GET"
    assert page.client.calls[-1][1] == "/api/chart-series/" + identity
    assert request.identity == identity and request.params == parameters
    page.client.failed.emit(request.tag, "manual lookup also unavailable")
    request.poll()
    assert len(page.client.calls) == 9 and request.state == "lookup_paused"
    request.request_again()
    page.client.resultReceived.emit(request.tag, response(post, "running"))
    assert request.state == "running" and request.timer.isActive()
    request.poll()
    assert not request.timer.isActive()
    # Valid matching status restored a full failure budget for the same ID.
    exhaust(page, request)
    assert request.identity == identity


@pytest.mark.parametrize("kind", KINDS)
def test_paused_style_locale_reconnect_do_not_refill_or_repost(page, kind):
    pane = page.panes[0]
    request, _post = enable(page, kind)
    exhaust(page, request)
    identity = request.identity
    profile = deepcopy(pane.profile)
    profile["timezone"] = "UTC+09:00"
    if kind == "PRICE_WINDOWS":
        profile["price_windows"]["sma"]["lines"][0].update(visible=False, color="#123456")
    elif kind in {"PSAR", "SWING", "IVP"}:
        profile[kind.lower()]["color"] = "#123456"
    pane.apply_profile(profile)
    page.set_presentation("light", "en")
    assert len(page.client.calls) == 8 and request.state == "lookup_paused"
    assert "Check same request" == request.retry.text()
    assert not request.timer.isActive()
    # Preserve the same ID across a transport disconnect without replacing data.
    # New-kind epoch/source freshness may remain ineligible after reconnect.
    page.client.attached = False
    request.ensure()
    assert not request.timer.isActive()
    page.client.attached = True
    request.ensure()
    request.poll()
    assert request.identity == identity and request.state == "lookup_paused"
    assert len(page.client.calls) == 8 and not request.timer.isActive()


@pytest.mark.parametrize("kind", KINDS)
def test_valid_terminal_response_stops_timer_then_explicit_recalculation_uses_new_id(page, kind):
    request, post = enable(page, kind)
    page.client.resultReceived.emit(request.tag, response(post, "running"))
    assert request.timer.isActive()
    request.poll()
    page.client.resultReceived.emit(request.tag, response(post, "expired"))
    assert request.state == "expired" and not request.timer.isActive()
    request.poll()
    count = len(page.client.calls)
    request.request_again()
    assert len(page.client.calls) == count + 1
    assert page.client.calls[-1][0] == "POST" and request.identity != post[2]["id"]


@pytest.mark.parametrize("kind", KINDS)
def test_wrong_tag_cannot_reset_budget_and_close_cannot_rearm(page, kind):
    request, post = enable(page, kind)
    page.client.resultReceived.emit(request.tag, response(post, "running"))
    request.timer.stop()
    page.client.resultReceived.emit("foreign-response", response(post, "running"))
    page.client.failed.emit("foreign-failure", "not this controller")
    assert not request.timer.isActive()
    request.poll()
    for _ in range(7):
        page.client.failed.emit(request.tag, "failure")
        request.poll()
    page.client.resultReceived.emit("wrong-tag", response(post, "running"))
    page.client.failed.emit(request.tag, "eighth failure")
    assert request.state == "lookup_paused"
    count = len(page.client.calls)
    request.request_again()
    active_tag = request.tag
    page.close()
    assert not request.timer.isActive()
    page.client.resultReceived.emit(active_tag, response(post, "running"))
    request.poll()
    assert not request.timer.isActive() and len(page.client.calls) == count + 1


@pytest.mark.parametrize("kind", KINDS)
def test_new_numerical_context_resets_but_invalid_status_does_not(page, kind):
    pane = page.panes[0]
    request, post = enable(page, kind)
    exhaust(page, request)
    profile = deepcopy(pane.profile)
    if kind == "PRICE_WINDOWS":
        profile["price_windows"]["sma"]["lines"][0]["nSamples"] = 3
    elif kind == "SWING":
        profile["swing"]["swing_range"] = 0.006
    elif kind == "PSAR":
        profile["psar"]["start"] = 0.03
    else:
        profile[kind.lower()]["nSamples"] += 1
    pane.apply_profile(profile)
    fresh = page.client.calls[-1]
    assert fresh[0] == "POST" and request.identity != post[2]["id"]
    if kind == "PRICE_WINDOWS":
        # Its existing malformed DTO contract remains terminal failed.
        malformed = response(fresh, "complete")
        page.client.resultReceived.emit(request.tag, malformed)
        assert request.state == "failed" and not request.timer.isActive()
    else:
        for count in range(8):
            malformed = response(fresh)
            malformed["core_epoch"] = "wrong-core"
            page.client.resultReceived.emit(request.tag, malformed)
            if count < 7:
                assert request.state == "unknown"
                request.poll()
        assert request.state == "lookup_paused" and not request.timer.isActive()
