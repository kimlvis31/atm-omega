"""Execution health reaches the core only after releasing paper state locks."""

from copy import deepcopy
from threading import Lock
from types import SimpleNamespace

from omega.runtime.paper_feed import PaperFeedSupervisor

INCIDENT = {
    "account_id": "paper1",
    "instance": "strategy1",
    "instrument": "binance:usdm:ETHUSDT",
    "code": "POSITION_PROTECTION_UNAVAILABLE",
    "severity": "critical",
}


def supervisor(callback=None):
    state = {"owner_retired": False, "incidents": [deepcopy(INCIDENT)]}
    paper_lock = Lock()

    def health():
        with paper_lock:
            return deepcopy(state)

    paper = SimpleNamespace(execution_health=health)
    feed = PaperFeedSupervisor(paper, None, on_execution_incident=callback)
    return feed, state, paper_lock


def test_delivery_holds_neither_paper_nor_supervisor_state_lock():
    delivered = []
    feed, _, paper_lock = supervisor()

    def callback(incident):
        assert paper_lock.acquire(blocking=False)
        paper_lock.release()
        assert feed.lock.acquire(blocking=False)
        feed.lock.release()
        delivered.append(incident)

    feed.on_execution_incident = callback
    feed._record_execution_health()
    feed._record_execution_health()
    assert delivered == [INCIDENT] and not feed.status()["execution_alert_unavailable"]


def test_failed_delivery_retries_and_recovery_allows_a_new_incident():
    calls = []

    def callback(incident):
        calls.append(deepcopy(incident))
        if len(calls) == 1:
            raise OSError("synthetic delivery fault")

    feed, state, _ = supervisor(callback)
    feed._record_execution_health()
    assert feed.status()["execution_alert_unavailable"]
    feed._record_execution_health()
    assert len(calls) == 2 and not feed.status()["execution_alert_unavailable"]
    feed._record_execution_health()
    assert len(calls) == 2
    state["incidents"] = []
    feed._record_execution_health()
    state["incidents"] = [deepcopy(INCIDENT)]
    feed._record_execution_health()
    assert len(calls) == 3


def test_retired_or_stopped_owner_never_delivers_late_incident():
    calls = []
    feed, state, _ = supervisor(calls.append)
    state["owner_retired"] = True
    feed._record_execution_health()
    state["owner_retired"] = False
    feed.stop.set()
    feed._record_execution_health()
    assert calls == []


def test_severity_escalation_is_a_distinct_transition():
    calls = []
    feed, state, _ = supervisor(calls.append)
    state["incidents"][0]["severity"] = "warning"
    feed._record_execution_health()
    state["incidents"][0]["severity"] = "critical"
    feed._record_execution_health()
    assert [value["severity"] for value in calls] == ["warning", "critical"]
