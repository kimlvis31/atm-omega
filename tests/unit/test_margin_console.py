"""Manual scenario CLI and real local v2 files; no network or market database."""

import json
from copy import deepcopy
from decimal import Decimal as D

import pytest

from omega.domain.errors import StorageError, TradingBlocked, ValidationError
from omega.runtime import margin_console as module

BTC = "binance:usdm:BTCUSDT"
ETH = "binance:usdm:ETHUSDT"


def initial():
    return {
        "account_id": "scenario:1",
        "balance": "1000.00",
        "positions": [
            {
                "instrument": BTC,
                "owner_id": "btc",
                "margin_mode": "cross",
                "leverage": 10,
                "budget_margin": "100",
            },
            {
                "instrument": ETH,
                "owner_id": "eth",
                "margin_mode": "isolated",
                "leverage": 5,
                "budget_margin": "100",
                "isolated_collateral": "100",
            },
        ],
    }


def marks(at=0):
    return [
        {"instrument": item, "price": "100", "event_time_ms": at, "available_at_ms": at}
        for item in (BTC, ETH)
    ]


def brackets(at=0):
    return [
        {
            "instrument": item,
            "observed_at_ms": at,
            "available_at_ms": at,
            "rows": [
                {
                    "notional_floor": "0",
                    "notional_cap": "1000000",
                    "maintenance_rate": ".01",
                    "maintenance_amount": "0",
                    "max_leverage": 20,
                }
            ],
        }
        for item in (BTC, ETH)
    ]


def submit(instrument=BTC, identity="order", at=0):
    return {
        "order_id": identity,
        "instrument": instrument,
        "side": "BUY",
        "quantity": "1.00",
        "kind": "MARKET",
        "price": None,
        "reducing": False,
        "reason": "manual_scenario",
        "as_of_ms": at,
        "worst_execution_price": "100",
        "fee_rate": ".001",
        "marks": marks(at),
        "brackets": brackets(at),
    }


def step(opening=60000, price="100"):
    return {
        "candles": [
            {
                "instrument": item,
                "open_time_ms": opening,
                "open": price,
                "high": "120",
                "low": "80",
                "close": "110",
                "volume": "10",
            }
            for item in (BTC, ETH)
        ],
        "as_of_ms": opening + 60000,
        "phase": "open",
        "brackets": brackets(),
    }


def write(tmp_path, name, document):
    path = tmp_path / name
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def create(tmp_path):
    return module.MarginConsole(
        tmp_path / "state-root", create=write(tmp_path, "create.json", initial())
    )


def test_flat_initial_partition_and_manual_provenance():
    account = module.initial_account(initial())
    assert account.cross_wallet == D(900) and account.opening_balance == D(1000)
    assert account.positions[1].isolated_wallet == D(100)
    assert not account.ledger and all(item.quantity == 0 for item in account.positions)
    assert all(item.source == "manual_scenario" for item in module.scenario_marks(marks()))
    assert all(
        item.source == "manual_scenario" and len(item.checksum) == 64
        for item in module.scenario_brackets(brackets())
    )


@pytest.mark.parametrize(
    "change",
    [
        "unknown",
        "budget",
        "collateral",
        "leverage_bool",
        "cross_collateral",
        "spot",
        "duplicate",
        "balance_float",
    ],
)
def test_creation_rejects_bad_partition_types_and_unsupported_identity(change):
    data = initial()
    if change == "unknown":
        data["positions"][0]["quantity"] = "1"
    elif change == "budget":
        data["positions"][0]["budget_margin"] = "1001"
    elif change == "collateral":
        data["positions"][1]["isolated_collateral"] = "1001"
    elif change == "leverage_bool":
        data["positions"][0]["leverage"] = True
    elif change == "cross_collateral":
        data["positions"][0]["isolated_collateral"] = "1"
    elif change == "spot":
        data["positions"][0]["instrument"] = "binance:spot:BTCUSDT"
    elif change == "duplicate":
        data["positions"][1] = deepcopy(data["positions"][0])
    else:
        data["balance"] = 1000.0
    with pytest.raises((ValidationError, ValueError)):
        module.initial_account(data)


@pytest.mark.parametrize(
    "raw", [b'{"x":1,"x":2}', b'{"x":1.1}', b'{"x":NaN}', b'{"x":Infinity}', b"[]", b"\xff", b"["]
)
def test_user_file_rejects_duplicate_float_nonfinite_and_wrong_shape(tmp_path, raw):
    path = tmp_path / "bad.json"
    path.write_bytes(raw)
    with pytest.raises(ValidationError):
        module.read_scenario(path)


def test_scenario_file_size_and_native_row_cap(tmp_path, monkeypatch):
    path = write(tmp_path, "input.json", initial())
    monkeypatch.setattr(module, "MAX_INPUT_BYTES", 16)
    with pytest.raises(ValidationError):
        module.read_scenario(path)
    with pytest.raises(ValidationError):
        module.scenario_marks([marks()[0]] * 101)


def test_create_real_file_ack_reuse_then_conflicting_create_and_missing_account(tmp_path):
    console = create(tmp_path)
    try:
        assert console.status()["paused"] and console.status()["state"] == "ready"
        assert console.status()["wallet"]["cross"] == "900.00"
    finally:
        console.close()
    reused = create(tmp_path)
    reused.close()
    changed = initial()
    changed["balance"] = "2000"
    with pytest.raises(ValidationError, match="conflicts"):
        module.MarginConsole(
            tmp_path / "state-root", create=write(tmp_path, "different.json", changed)
        )
    with pytest.raises(StorageError, match="missing"):
        module.MarginConsole(tmp_path / "missing-root", account_id="missing")
    assert not list((tmp_path / "missing-root").rglob("*.json"))


def test_restart_entry_pause_pending_preserved_cancel_and_new_request_ids(tmp_path):
    console = create(tmp_path)
    try:
        with pytest.raises(TradingBlocked):
            console.submit(submit())
        console.execute("resume")
        console.submit(submit())
        console.execute("pause")
        assert len(console.status()["pending"]) == 1
    finally:
        console.close()
    reopened = module.MarginConsole(tmp_path / "state-root", account_id="scenario:1")
    try:
        assert reopened.status()["paused"] and len(reopened.status()["pending"]) == 1
        count = len(reopened.journal.read("scenario:1"))
        reopened.execute("resume")
        assert len(reopened.journal.read("scenario:1")) == count + 1
        reopened.execute("pause")
        reopened.execute("cancel order")
        assert reopened.status()["pending"] == [] and reopened.status()["paused"]
    finally:
        reopened.close()


def test_real_cross_isolated_full_fill_while_paused_and_restart(tmp_path):
    console = create(tmp_path)
    try:
        console.execute("resume")
        console.submit(submit())
        console.submit(submit(ETH, "eth-order"))
        console.execute("pause")
        result = console.step(step())
        assert result["paused"] and not result["pending"]
        assert result["sequence"] == 2 and result["batch"]["fills"] == 2
        assert D(result["wallet"]["cross"]) == D("899.9")
        assert D(result["positions"][1]["isolated_collateral"]) == D("99.9")
        assert all(D(item["quantity"]) == 1 for item in result["positions"])
        assert "ledger" not in json.dumps(result)
    finally:
        console.close()
    restarted = module.MarginConsole(tmp_path / "state-root", account_id="scenario:1")
    try:
        assert restarted.status()["paused"] and restarted.status()["sequence"] == 2
        assert restarted.execute("reconcile")["sequence"] == 2
    finally:
        restarted.close()
    with pytest.raises(ValidationError, match="conflicts"):
        create(tmp_path)


def test_native_creation_time_waits_for_next_full_minute(tmp_path):
    console = create(tmp_path)
    try:
        console.execute("resume")
        assert console.submit(submit(at=1))["pending"][0]["eligible_at_ms"] == 60000
        first = console.step(step(0))
        assert first["sequence"] == 0 and first["batch"]["fills"] == 0 and first["pending"]
        second = console.step(step())
        assert second["sequence"] == 1 and second["batch"]["fills"] == 1
    finally:
        console.close()


def test_gap_bound_rejection_is_recorded_without_filling_or_expanding_risk(tmp_path):
    console = create(tmp_path)
    try:
        console.execute("resume")
        console.submit(submit())
        result = console.step(step(price="110"))
        assert result["sequence"] == 0 and result["pending"] == []
        assert result["batch"]["fills"] == 0
        assert result["batch"]["admissions"][0]["outcome"] == "rejected"
        assert "bound" in result["batch"]["admissions"][0]["reason"]
    finally:
        console.close()


def test_unknown_input_fields_wrong_boolean_or_missing_refs_do_not_mutate(tmp_path):
    console = create(tmp_path)
    try:
        console.execute("resume")
        for mutation in ("unknown", "bool", "missing_marks"):
            data = submit()
            if mutation == "unknown":
                data["owner_id"] = "override"
            elif mutation == "bool":
                data["reducing"] = 0
            else:
                data["marks"] = []
            with pytest.raises((ValidationError, TradingBlocked)):
                console.submit(data)
            assert console.status()["pending"] == [] and console.status()["sequence"] == 0
        bad = step()
        bad["candles"][0]["close_time_ms"] = 123
        with pytest.raises(ValidationError):
            console.step(bad)
    finally:
        console.close()


def test_uncertain_journal_commit_requires_explicit_reconcile_and_no_auto_repeat(
    tmp_path, monkeypatch
):
    console = create(tmp_path)
    try:
        console.execute("resume")
        original = console.journal._publish
        calls = []

        def lose_ack(directory, record, payload):
            calls.append(record.record_id)
            original(directory, record, payload)
            raise OSError("synthetic ACK loss")

        monkeypatch.setattr(console.journal, "_publish", lose_ack)
        with pytest.raises(StorageError):
            console.submit(submit())
        assert len(calls) == 2 and console.status()["state"] == "unknown"
        console.status()
        assert len(calls) == 2
        monkeypatch.setattr(console.journal, "_publish", original)
        assert console.execute("reconcile")["state"] == "ready"
        assert len(console.status()["pending"]) == 1
    finally:
        console.close()


def test_cli_execute_and_interactive_help_paths_spaces_and_release(tmp_path):
    create_file = write(tmp_path, "create file.json", initial())
    submit_file = write(tmp_path, "order file.json", submit())
    step_file = write(tmp_path, "step file.json", step())
    outputs = []
    args = ["--root", str(tmp_path / "batch-root"), "--create", str(create_file)]
    for command in (
        "help",
        "resume",
        f'submit "{submit_file}"',
        "pause",
        f'step "{step_file}"',
        "status",
    ):
        args.extend(("--execute", command))
    assert module.main(args, output=outputs.append) == 0
    rows = [json.loads(value) for value in outputs]
    assert rows[-1]["sequence"] == 1 and rows[-1]["paused"]
    assert "help" in rows[1]["commands"] and all("ledger" not in row for row in rows)
    commands = iter(("help", "status", "exit"))
    outputs.clear()
    assert (
        module.main(
            ["--root", str(tmp_path / "batch-root"), "--account", "scenario:1"],
            input_fn=lambda _: next(commands),
            output=outputs.append,
        )
        == 0
    )
    assert json.loads(outputs[0])["paused"] is True
    with pytest.raises(SystemExit) as raised:
        module.main(["--help"])
    assert raised.value.code == 0


def test_cli_error_response_keeps_loop_available_and_nonzero_batch_status(tmp_path):
    create_file = write(tmp_path, "create.json", initial())
    outputs = []
    assert (
        module.main(
            [
                "--root",
                str(tmp_path / "root"),
                "--create",
                str(create_file),
                "--execute",
                "bad",
                "--execute",
                "status",
            ],
            output=outputs.append,
        )
        == 2
    )
    assert json.loads(outputs[1])["error"] == "ValidationError"
    assert json.loads(outputs[2])["state"] == "ready"
    outputs.clear()
    assert (
        module.main(
            ["--root", str(tmp_path / "missing"), "--account", "absent", "--execute", "status"],
            output=outputs.append,
        )
        == 2
    )
    assert json.loads(outputs[0])["error"] == "StorageError"


def test_second_owner_is_rejected_without_closing_first(tmp_path):
    first = create(tmp_path)
    try:
        outputs = []
        assert (
            module.main(
                [
                    "--root",
                    str(tmp_path / "state-root"),
                    "--account",
                    "scenario:1",
                    "--execute",
                    "status",
                ],
                output=outputs.append,
            )
            == 2
        )
        assert json.loads(outputs[0])["error"] == "RuntimeError"
        assert first.status()["state"] == "ready"
    finally:
        first.close()


def test_explicit_initial_registration_ack_loss_is_reconciled_without_second_wallet(
    tmp_path, monkeypatch
):
    original = module.FileMarginStateStore.save
    lost = []

    def save(store, account, *, expected_sequence):
        original(store, account, expected_sequence=expected_sequence)
        if expected_sequence is None and not lost:
            lost.append(account)
            raise StorageError("synthetic initial wallet ACK loss")

    monkeypatch.setattr(module.FileMarginStateStore, "save", save)
    console = create(tmp_path)
    try:
        assert console.status()["state"] == "unknown"
        assert console.status()["wallet"] is None
        recovered = console.execute("reconcile")
        assert recovered["state"] == "ready" and recovered["paused"]
        assert D(recovered["wallet"]["cross"]) == 900
        assert len(lost) == 1 and console.store.load("scenario:1") == lost[0]
    finally:
        console.close()


def test_input_cannot_create_order_before_known_ledger_or_pending_cutoff(tmp_path):
    console = create(tmp_path)
    try:
        console.execute("resume")
        console.submit(submit(at=1))
        with pytest.raises(ValidationError, match="precedes"):
            console.submit(submit(ETH, "past", at=0))
        console.step(step())
        with pytest.raises(ValidationError, match="precedes"):
            console.submit(submit(ETH, "past-after-fill", at=119999))
    finally:
        console.close()


def test_cli_batch_cap_does_not_create_state_and_close_error_releases_both(tmp_path, monkeypatch):
    path = write(tmp_path, "create.json", initial())
    outputs = []
    root = tmp_path / "too-many"
    args = ["--root", str(root), "--create", str(path)]
    for _ in range(129):
        args.extend(("--execute", "status"))
    assert module.main(args, output=outputs.append) == 2
    assert not root.exists()
    original = module.FileVirtualMarginJournal.close
    owners = []

    def close(journal):
        original(journal)
        owners.append(journal)
        raise OSError("synthetic close error")

    monkeypatch.setattr(module.FileVirtualMarginJournal, "close", close)
    outputs.clear()
    root = tmp_path / "close-root"
    assert (
        module.main(
            ["--root", str(root), "--create", str(path), "--execute", "status"],
            output=outputs.append,
        )
        == 2
    )
    assert owners[0].closed and json.loads(outputs[-1])["message"].endswith("could not be closed")
    monkeypatch.setattr(module.FileVirtualMarginJournal, "close", original)
    reopened = module.MarginConsole(root, account_id="scenario:1")
    reopened.close()


def test_missing_journal_is_visible_unavailable_and_batch_never_reports_success(tmp_path):
    console = create(tmp_path)
    target = next(console.journal.primary.glob("*.json"))
    console.close()
    target.unlink()  # Explicit synthetic loss; ordinary restart must not create an anchor.
    outputs = []
    assert (
        module.main(
            [
                "--root",
                str(tmp_path / "state-root"),
                "--account",
                "scenario:1",
                "--execute",
                "status",
            ],
            output=outputs.append,
        )
        == 2
    )
    assert json.loads(outputs[0])["state"] == "unavailable"
    assert json.loads(outputs[0])["error"] == "StorageError"
    assert not list(target.parent.glob("*.json"))
