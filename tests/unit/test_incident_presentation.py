"""Notification wording preserves currentness, uncertainty and sound-only ACKs."""

from datetime import UTC, datetime

import pytest

from omega.gui.incident_presentation import (
    incident_connection_text,
    incident_details,
    incident_headers,
    incident_row,
)


@pytest.mark.parametrize("language", ["ko", "en"])
def test_friendly_transport_summary_keeps_code_in_escaped_details(language):
    incident = {
        "id": "one",
        "code": "CONTROL_TRANSPORT_OR_STORAGE_FAILURE",
        "severity": "critical",
        "timestamp": 100,
        "last_seen_at": 120,
        "observation_scope": "previous_core",
        "acknowledged": True,
        "occurrences": 2,
        "context": {
            "stage": "operation",
            "operation": "POST",
            "error_kind": "timeout",
            "raw": "SECRET",
        },
    }
    row = incident_row(incident, language)
    assert len(row) == len(incident_headers(language)) == 7 and row[0] == "one"
    assert "UTC" in row[1] and "UTC" in row[2]
    assert incident["code"] not in row[5] and row[5] != "?"
    details = incident_details(incident, language)
    assert incident["code"] in details and "timeout" in details and "SECRET" not in details
    assert row[1] in details and row[2] in details  # Hidden compact columns retain both times.
    assert ("이전 코어" if language == "ko" else "Previous core") in row[3]
    assert ("해결 미확인" if language == "ko" else "resolution unconfirmed") in row[3]
    assert ("심각" if language == "ko" else "Critical") == row[4]


def test_legacy_bad_timestamp_and_html_are_not_claimed_current_or_rendered_as_markup():
    row = incident_row({"id": "x", "code": "<img src=x>", "timestamp": float("nan")}, "en")
    assert row[1] == "?" and row[2] == "?" and "unknown" in row[3]
    details = incident_details({"id": "<script>", "code": "<img src=x>"}, "en")
    assert "&lt;script&gt;" in details and "&lt;img" in details and "<img" not in details


def test_disconnect_does_not_reuse_last_recording_available_as_current_recovery():
    now = datetime(2026, 11, 2, tzinfo=UTC)
    stale = incident_connection_text(
        {"storage_available": True}, attached=False, last_received=now, language="en"
    )
    assert "current status unavailable" in stale and "2026-11-02" in stale
    assert "recording available" not in stale
    current = incident_connection_text(
        {"storage_available": True}, attached=True, last_received=now, language="en"
    )
    assert "recording available" in current and "recovery unconfirmed" in current
