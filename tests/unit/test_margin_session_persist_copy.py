"""Detached session commits retain ownership and real uncertain-write recovery."""

from copy import deepcopy

import pytest
from test_margin_session import new

from omega.domain.errors import StorageError
from omega.runtime import margin_session


def candidate_for(session):
    candidate = deepcopy(session._meta)
    candidate["graph_errors"]["btc-ts"] = "ProviderError"
    return candidate


def test_successful_real_commit_has_no_alias_to_the_callers_nested_candidate(tmp_path):
    session = new(tmp_path)
    try:
        candidate = candidate_for(session)
        expected = deepcopy(candidate)
        session._persist(candidate)
        candidate["graph_errors"].clear()
        candidate["graphs"].clear()
        assert session._meta == session._read() == expected
        assert session._uncertain is None
    finally:
        session.close()


def test_caller_mutation_during_write_cannot_change_the_frozen_data_or_checksum(
    tmp_path, monkeypatch
):
    session = new(tmp_path)
    original = margin_session.atomic_json
    try:
        candidate = candidate_for(session)
        expected = deepcopy(candidate)

        def write(path, envelope):
            # Model a caller retaining its own input while persistence blocks.
            candidate["graph_errors"]["btc-ts"] = "ChangedAfterSnapshot"
            original(path, envelope)

        with monkeypatch.context() as patch:
            patch.setattr(margin_session, "atomic_json", write)
            session._persist(candidate)
        assert candidate != expected
        assert session._meta == session._read() == expected
        session._validate_meta(session._read())
    finally:
        session.close()


@pytest.mark.parametrize("committed", [False, True])
def test_uncertain_real_write_keeps_both_frozen_snapshots_and_reconciles_paused(
    tmp_path, monkeypatch, committed
):
    session = new(tmp_path)
    original = margin_session.atomic_json
    try:
        before = deepcopy(session._meta)
        candidate = candidate_for(session)
        expected = deepcopy(candidate)

        def fail(path, envelope):
            if committed:
                original(path, envelope)
            candidate["graph_errors"].clear()
            raise OSError("injected write or acknowledgement loss")

        with monkeypatch.context() as patch:
            patch.setattr(margin_session, "atomic_json", fail)
            with pytest.raises(StorageError, match="outcome is unknown"):
                session._persist(candidate)
        assert session._state == "unknown" and session._error == "session_commit_unknown"
        assert session._meta == before
        assert session._uncertain == (before, expected)
        assert session._read() == (expected if committed else before)
        result = session.reconcile()
        assert result["paused"]
        assert session._meta == session._read() == expected
        assert session._uncertain is None
    finally:
        session.close()


def test_unchanged_metadata_keeps_original_bytes_without_a_new_write(tmp_path, monkeypatch):
    session = new(tmp_path)
    try:
        before = session.path.read_bytes()

        def forbidden(*args):
            pytest.fail("an unchanged snapshot should not create a new write outcome")

        with monkeypatch.context() as patch:
            patch.setattr(margin_session, "atomic_json", forbidden)
            session._persist(deepcopy(session._meta))
        assert session.path.read_bytes() == before and session._uncertain is None
    finally:
        session.close()
