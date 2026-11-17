"""Explicit terminal consent, current evidence and recoverable original IDs."""

from copy import deepcopy

import pytest
from test_margin_retirement_api_client import SHA, Backend, ready

from omega.runtime.console import ControlConsole
from omega.runtime.margin_retirement_client import retirement_response, retirement_url


class Console(ControlConsole):
    def __init__(self, root, epoch="epoch"):
        self.messages, self.calls = [], []
        super().__init__(root, output=self.messages.append)
        self.epoch, self.observed = epoch, ready(epoch, "account")
        self.fail = False
        self.reply_epoch = epoch

    def request(self, method, path, body=None):
        self.calls.append((method, path, deepcopy(body)))
        if method == "GET" and path == "/api/margin-trading":
            return {"core_epoch": self.epoch}
        if method == "GET" and path == retirement_url("account"):
            return deepcopy(self.observed)
        if method == "POST":
            if self.fail:
                raise OSError("response lost")
            if path == "/api/margin-trading":
                return {"id": body["id"], "core_epoch": self.reply_epoch, "state": "queued"}
            return {**Backend._job(body["id"], "account", SHA, "queued"), "core_epoch": self.epoch}
        return {**Backend._job("read", "account", SHA, "completed"), "core_epoch": self.epoch}


def test_retirement_inspection_is_read_only(tmp_path):
    console = Console(tmp_path)
    assert console.execute("margin-retirement account")["state"] == "ready"
    assert [row[0] for row in console.calls] == ["GET", "GET"]


@pytest.mark.parametrize("tail", ["", "account", "account " + SHA, "account " + SHA + " --force"])
def test_terminal_never_supplies_implicit_retirement_consent(tmp_path, tail):
    console = Console(tmp_path)
    with pytest.raises(ValueError):
        console.execute("margin-retire " + tail)
    assert console.calls == []


def test_explicit_retirement_binds_fresh_evidence_and_reports_original_id(tmp_path):
    console = Console(tmp_path)
    result = console.execute("margin-retire account " + SHA + " --confirm --id retire")
    assert result["state"] == "queued"
    assert console.calls[-1] == (
        "POST",
        "/api/margin-trading",
        {
            "id": "retire",
            "kind": "retire",
            "core_epoch": "epoch",
            "parameters": {"account_id": "account", "expected_observation_sha256": SHA},
        },
    )
    assert console.messages == ["Retirement operation retire"]


def test_changed_observation_does_not_submit_after_user_confirmation(tmp_path):
    console = Console(tmp_path)
    console.observed["observation_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="changed"):
        console.execute("margin-retire account " + SHA + " --confirm --id retire")
    assert all(method == "GET" for method, _, _ in console.calls)


def test_explicit_new_core_retry_keeps_old_id_parameters_and_fence_hash(tmp_path):
    console = Console(tmp_path, "new-epoch")
    console.observed = retirement_response(
        "account",
        "new-epoch",
        state="retiring",
        phase="prepared",
        operation_id="original",
        observation_sha256=SHA,
        registered_parameters_sha256="b" * 64,
        session_config_hash="c" * 64,
        ledger_namespace="d" * 64,
    )
    console.execute("margin-retire account " + SHA + " --confirm --id original")
    posted = console.calls[-1][2]
    assert posted["id"] == "original" and posted["core_epoch"] == "new-epoch"
    assert posted["parameters"]["expected_observation_sha256"] == SHA
    console.calls.clear()
    with pytest.raises(ValueError, match="original"):
        console.execute("margin-retire account " + SHA + " --confirm --id different")
    assert all(method == "GET" for method, _, _ in console.calls)


@pytest.mark.parametrize("network_loss", [True, False])
def test_unknown_response_is_not_automatically_reposted(tmp_path, network_loss):
    console = Console(tmp_path)
    console.fail, console.reply_epoch = network_loss, "old-epoch"
    with pytest.raises((OSError, ValueError)):
        console.execute("margin-retire account " + SHA + " --confirm --id original")
    assert sum(method == "POST" for method, _, _ in console.calls) == 1
    assert any("GET /api/margin-trading/original" in line for line in console.messages)


def test_archive_load_and_status_are_explicit_bounded_read_operations(tmp_path):
    console = Console(tmp_path)
    assert console.execute("margin-history account " + SHA + " --id read")["state"] == "queued"
    assert console.calls[-1] == (
        "POST",
        "/api/margin-trading/history",
        {
            "id": "read",
            "account_id": "account",
            "expected_receipt_sha256": SHA,
            "core_epoch": "epoch",
        },
    )
    assert console.execute("margin-history-status read")["cached"]
    assert all("/retirement" not in path for _, path, _ in console.calls)
