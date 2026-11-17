"""Saved acknowledgements silence delivery without resolving incident history."""

from copy import deepcopy

import pytest

from omega.gui.models import IncidentTracker


def incident(**changes):
    return {
        "id": "saved-incident",
        "revision": 1,
        "severity": "critical",
        "acknowledged": False,
        "resolved": False,
        "status": "active",
        "created_at_ms": 1_000,
        "last_seen_at_ms": 2_000,
        **changes,
    }


@pytest.mark.parametrize("severity", ["warning", "critical", "fatal"])
def test_initial_server_acknowledged_history_is_retained_without_delivery(severity):
    tracker = IncidentTracker()
    saved = incident(severity=severity, acknowledged=True)
    original = deepcopy(saved)
    assert tracker.update([saved]) == []
    assert list(tracker.active.values()) == [saved]
    assert tracker.seen == {tracker.key(saved)}
    assert tracker.acknowledged == set()  # No invented local acknowledgement.
    assert tracker.critical() == []
    assert tracker.update([deepcopy(saved)]) == []
    assert saved == original and saved["resolved"] is False


def test_unacknowledged_history_delivers_once_and_stays_critical_until_explicit_ack():
    tracker = IncidentTracker()
    saved = incident()
    original = deepcopy(saved)
    assert tracker.update([saved]) == [saved]
    assert tracker.critical() == [saved]
    assert tracker.update([deepcopy(saved)]) == []
    assert tracker.critical() == [saved]  # Existing repeat behavior remains available.
    tracker.acknowledge(saved)
    assert tracker.critical() == []
    assert tracker.update([saved]) == []
    assert tracker.active[tracker.key(saved)] == saved
    assert saved == original and saved["acknowledged"] is False


def test_local_acknowledgement_before_first_update_silences_only_that_occurrence():
    tracker = IncidentTracker()
    saved = incident()
    tracker.acknowledge(saved)
    assert tracker.seen == set() and tracker.active == {}
    assert tracker.update([saved]) == []
    assert tracker.critical() == []
    assert tracker.seen == {tracker.key(saved)}
    assert list(tracker.active.values()) == [saved]
    assert saved["acknowledged"] is False and saved["resolved"] is False


@pytest.mark.parametrize("ack_source", ["local", "server"])
@pytest.mark.parametrize("change", [{"revision": 2}, {"severity": "fatal"}])
def test_new_revision_or_escalation_of_same_id_alerts_when_unacknowledged(ack_source, change):
    tracker = IncidentTracker()
    first = incident(acknowledged=ack_source == "server")
    if ack_source == "local":
        tracker.acknowledge(first)
    assert tracker.update([first]) == []
    current = first | {"acknowledged": False} | change
    assert tracker.update([current]) == [current]
    assert tracker.critical() == [current]
    assert tracker.update([deepcopy(current)]) == []
    assert current["created_at_ms"] == first["created_at_ms"]
    assert current["last_seen_at_ms"] == first["last_seen_at_ms"]


def test_warning_escalates_to_unacknowledged_critical_without_revision_change():
    tracker = IncidentTracker()
    first = incident(severity="warning", acknowledged=True)
    assert tracker.update([first]) == []
    escalated = first | {"severity": "critical", "acknowledged": False}
    assert tracker.update([escalated]) == [escalated]
    assert tracker.critical() == [escalated]


def test_new_revision_already_acknowledged_by_server_is_not_a_new_delivery():
    tracker = IncidentTracker()
    first = incident(acknowledged=True)
    assert tracker.update([first]) == []
    acknowledged_revision = first | {"revision": 2}
    assert tracker.update([acknowledged_revision]) == []
    assert tracker.critical() == []
    assert list(tracker.active.values()) == [acknowledged_revision]


def test_mixed_history_keeps_unresolved_entries_and_only_delivers_unacknowledged():
    tracker = IncidentTracker()
    server_ack = incident(id="server", acknowledged=True)
    local_ack = incident(id="local")
    unacknowledged = incident(id="needs-review")
    resolved = incident(id="resolved", resolved=True)
    closed = incident(id="closed", status="closed")
    tracker.acknowledge(local_ack)
    history = [server_ack, local_ack, unacknowledged, resolved, closed]
    original = deepcopy(history)
    assert tracker.update(history) == [unacknowledged]
    assert set(tracker.active) == {
        tracker.key(server_ack),
        tracker.key(local_ack),
        tracker.key(unacknowledged),
    }
    assert tracker.critical() == [unacknowledged]
    assert tracker.update(history) == []
    assert history == original


def test_reconnect_on_same_tracker_does_not_redeliver_seen_history():
    tracker = IncidentTracker()
    saved = incident()
    assert tracker.update([saved]) == [saved]
    assert tracker.update([]) == []
    assert tracker.active == {} and tracker.critical() == []
    assert tracker.update([deepcopy(saved)]) == []
    assert tracker.critical() == [saved]
    tracker.acknowledge(saved)
    assert tracker.update([]) == []
    assert tracker.update([deepcopy(saved)]) == []
    assert tracker.critical() == []


@pytest.mark.parametrize("resolution", [{"resolved": True}, {"status": "resolved"}])
def test_resolution_removes_active_repeat_without_rewriting_original_history(resolution):
    tracker = IncidentTracker()
    saved = incident()
    original = deepcopy(saved)
    assert tracker.update([saved]) == [saved]
    resolved = saved | resolution
    assert tracker.update([resolved]) == []
    assert tracker.active == {} and tracker.critical() == []
    assert tracker.key(saved) in tracker.seen
    assert saved == original
