"""DMI shares bounded lookup semantics without changing its display snapshot."""

import importlib.util
import os
from copy import deepcopy

import pytest
from test_chart_dmi_series import client, enable, raw, response

pytestmark = pytest.mark.skipif(importlib.util.find_spec("PySide6") is None, reason="optional Qt")


@pytest.fixture(scope="module")
def app():
    os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    from PySide6.QtWidgets import QApplication

    return QApplication.instance() or QApplication(["dmi-lookup-tests"])


@pytest.fixture
def page(app, tmp_path):
    from omega.gui.charts_page import ChartsPage

    view = ChartsPage(client(), root=tmp_path)
    view.resize(1360, 900)
    view.show()
    view.set_presentation("light", "en")
    view.update_state({"core": {"epoch": "a", "capabilities": {"chart_series": True}}})
    raw(view)
    app.processEvents()
    view.client.calls.clear()
    yield view
    view.close()
    view.deleteLater()
    app.processEvents()


def series_calls(page):
    return [call for call in page.client.calls if call[1].startswith("/api/chart-series")]


def exhaust(page):
    post = enable(page)
    pane = page.panes[0]
    assert post[0] == "POST" and not pane._dmi_timer.isActive()
    for index in range(8):
        if index:
            pane._poll_dmi()
            assert series_calls(page)[-1][0:2] == ("GET", "/api/chart-series/" + post[2]["id"])
            assert not pane._dmi_timer.isActive()
        page.client.failed.emit(pane._dmi_tag, "deliberate lookup failure")
    assert pane._dmi_state == "lookup_paused" and not pane._dmi_timer.isActive()
    assert len(series_calls(page)) == 8
    return post


def test_dmi_idle_poll_never_builds_context_or_starts_a_timer(page, monkeypatch):
    pane = page.panes[0]
    assert not pane._dmi_timer.isActive()

    def forbidden():
        pytest.fail("idle DMI must not construct a numerical context")

    monkeypatch.setattr(pane, "_dmi_context", forbidden)
    pane._poll_dmi()
    assert series_calls(page) == []


def test_eight_dmi_failures_stop_automatic_requests_and_pause_rows(page, monkeypatch):
    exhaust(page)
    pane = page.panes[0]
    before = len(series_calls(page))

    def forbidden():
        pytest.fail("paused DMI must not construct a polling context")

    monkeypatch.setattr(pane, "_dmi_context", forbidden)
    for _ in range(20):
        pane._poll_dmi()
    assert len(series_calls(page)) == before and pane._dmi_lines == {}
    assert pane.producer_retry.isVisible()


def test_manual_dmi_lookup_uses_same_id_and_only_a_valid_reply_resumes(page):
    post = exhaust(page)
    pane = page.panes[0]
    pane._retry_dmi()
    assert series_calls(page)[-1][0:2] == ("GET", "/api/chart-series/" + post[2]["id"])
    assert sum(call[0] == "POST" for call in series_calls(page)) == 1
    page.client.failed.emit(pane._dmi_tag, "still unavailable")
    assert pane._dmi_state == "lookup_paused" and not pane._dmi_timer.isActive()
    before = len(series_calls(page))
    pane._poll_dmi()
    assert len(series_calls(page)) == before
    pane._retry_dmi()
    page.client.resultReceived.emit(pane._dmi_tag, response(post, state="running"))
    assert pane._dmi_state == "running" and pane._dmi_timer.isActive()
    pane._poll_dmi()
    page.client.failed.emit(pane._dmi_tag, "one new failure")
    assert pane._dmi_state == "unknown" and pane._dmi_timer.isActive()
    assert pane._dmi_id == post[2]["id"]


@pytest.mark.parametrize("state", ["complete", "failed", "cancelled", "expired"])
def test_terminal_dmi_stops_timer_and_direct_poll_is_context_free(page, monkeypatch, state):
    post = enable(page)
    pane = page.panes[0]
    page.client.resultReceived.emit(pane._dmi_tag, response(post, state=state))
    assert pane._dmi_state == state and not pane._dmi_timer.isActive()
    before = len(series_calls(page))

    def forbidden():
        pytest.fail("terminal DMI must not construct a polling context")

    monkeypatch.setattr(pane, "_dmi_context", forbidden)
    pane._poll_dmi()
    assert len(series_calls(page)) == before


def test_dmi_locale_style_and_same_context_reconnect_do_not_replenish_budget(page):
    post = exhaust(page)
    pane = page.panes[0]
    parameters = deepcopy(pane._dmi_parameters)
    page.set_presentation("dark", "ko")
    page.set_presentation("light", "en")
    pane._refresh_indicators()
    page.client.attached = False
    page._connection_changed(False)
    assert not pane._dmi_timer.isActive()
    page.client.attached = True
    page._connection_changed(True)
    pane._ensure_dmi()
    assert pane._dmi_state == "lookup_paused" and not pane._dmi_timer.isActive()
    assert pane._dmi_id == post[2]["id"] and pane._dmi_parameters == parameters
    before = len(series_calls(page))
    pane._poll_dmi()
    assert len(series_calls(page)) == before


def test_meaningful_dmi_context_change_binds_a_new_request_without_old_reply_credit(page):
    original = exhaust(page)
    pane = page.panes[0]
    old_tag = series_calls(page)[-1][3]
    profile = deepcopy(pane.profile)
    profile["dmi_period"] = 4
    pane.apply_profile(profile)
    current = series_calls(page)[-1]
    assert current[0] == "POST" and current[2]["id"] != original[2]["id"]
    for index in range(7):
        if index:
            pane._poll_dmi()
        page.client.failed.emit(pane._dmi_tag, "new context failure")
    assert pane._dmi_state == "unknown"
    # A valid old job cannot replenish the newer job's remaining one failure.
    page.client.resultReceived.emit(old_tag, response(original, state="running"))
    pane._poll_dmi()
    page.client.failed.emit(pane._dmi_tag, "eighth failure")
    assert pane._dmi_state == "lookup_paused" and not pane._dmi_timer.isActive()
    assert sum(call[0] == "POST" for call in series_calls(page)) == 2


def test_malformed_dmi_replies_are_bounded_and_cannot_publish_rows(page):
    post = enable(page)
    pane = page.panes[0]
    for index in range(8):
        if index:
            pane._poll_dmi()
        invalid = response(post, state="running")
        invalid["core_epoch"] = "foreign"
        page.client.resultReceived.emit(pane._dmi_tag, invalid)
    assert pane._dmi_state == "lookup_paused" and not pane._dmi_timer.isActive()
    assert pane._dmi_lines == {}


def test_wrong_dmi_hash_cannot_replenish_or_poison_manual_lookup(page):
    post = exhaust(page)
    pane = page.panes[0]
    pane._retry_dmi()
    invalid = response(post, state="running", spec_hash="f" * 64)
    page.client.resultReceived.emit(pane._dmi_tag, invalid)
    assert pane._dmi_state == "lookup_paused" and pane._dmi_hash is None
    assert not pane._dmi_timer.isActive()
    pane._retry_dmi()
    page.client.resultReceived.emit(pane._dmi_tag, response(post, state="running"))
    assert pane._dmi_state == "running" and pane._dmi_timer.isActive()
    assert pane._dmi_hash == response(post)["spec_hash"]
    assert sum(call[0] == "POST" for call in series_calls(page)) == 1


def test_malformed_dmi_completion_does_not_commit_hash_or_reset_failure_budget(page):
    post = exhaust(page)
    pane = page.panes[0]
    pane._retry_dmi()
    invalid = response(post)
    invalid["result"]["rows"][0].pop("ABSMA")
    page.client.resultReceived.emit(pane._dmi_tag, invalid)
    assert pane._dmi_state == "lookup_paused" and pane._dmi_hash is None
    assert pane._dmi_lines == {} and not pane._dmi_timer.isActive()
    pane._retry_dmi()
    page.client.resultReceived.emit(pane._dmi_tag, response(post))
    assert pane._dmi_state == "complete" and pane._dmi_hash == response(post)["spec_hash"]
    assert not pane._dmi_timer.isActive()
    assert pane._dmi_lines["DMIxADX"][-1] is not None
    assert sum(call[0] == "POST" for call in series_calls(page)) == 1


def test_closed_dmi_pane_ignores_late_response_and_never_rearms(page, monkeypatch):
    post = enable(page)
    pane = page.panes[0]
    tag = pane._dmi_tag
    page.close()

    def forbidden():
        pytest.fail("closed DMI must not construct a polling context")

    monkeypatch.setattr(pane, "_dmi_context", forbidden)
    page.client.resultReceived.emit(tag, response(post, state="running"))
    page.client.failed.emit(tag, "late failure")
    pane._poll_dmi()
    assert not pane._dmi_timer.isActive()
