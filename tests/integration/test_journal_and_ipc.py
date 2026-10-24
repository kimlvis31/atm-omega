import json
import multiprocessing as mp

import pytest

from omega.domain.errors import JournalError, ProtocolError
from omega.infrastructure.journal import JsonlJournal
from omega.runtime.ipc import receive, send
from omega.runtime.process import ControlProcess


def test_journal_checksum_and_torn_tail_are_not_silently_accepted(tmp_path):
    path = tmp_path / "journal.jsonl"
    journal = JsonlJournal(path)
    journal.append({"kind": "fill", "quantity": "1"})
    assert journal.records() == ({"kind": "fill", "quantity": "1"},)
    data = json.loads(path.read_bytes())
    data["event"]["quantity"] = "2"
    path.write_text(json.dumps(data) + "\n")
    with pytest.raises(JournalError):
        journal.records()
    path.write_bytes(b'{"event":')
    with pytest.raises(JournalError):
        journal.append({"kind": "next"})


def test_ipc_byte_limit_and_timeout():
    left, right = mp.Pipe()
    try:
        with pytest.raises(TimeoutError):
            receive(right, 0)
        with pytest.raises(ProtocolError):
            send(left, {"id": "x", "huge": "x" * 70_000})
        send(left, {"id": "x", "value": "ok"})
        assert receive(right, 1)["value"] == "ok"
        with pytest.raises(ProtocolError):
            send(left, {"id": "x", "nan": float("nan")})
    finally:
        left.close()
        right.close()


def test_spawn_control_owner_pause_snapshot_and_clean_stop():
    process = ControlProcess()
    try:
        ready = process.start(timeout=10)
        assert ready["status"] == "DEGRADED" and not ready["gui_started"]
        before = process.command("GetSnapshot")
        assert before["entry_reasons"] == ["RESTART_MANUAL"]
        assert process.command("PauseNewRisk")["applied"]
        snapshot = process.command("GetSnapshot")
        assert "MANUAL_PAUSE" in snapshot["entry_reasons"]
        assert not snapshot["allows_new_risk"]
        assert process.command("ResumeTrading")["error"] == "UNSUPPORTED_COMMAND"
        assert process.command("ShutdownIdle")["stopped"]
    finally:
        process.close()
