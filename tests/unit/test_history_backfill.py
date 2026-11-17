"""History scans use fake immutable ports: no exchange, credentials or DB lifecycle."""

import json
import threading
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from itertools import repeat
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.application.history_backfill import (
    MAX_GAP_RANGES,
    MINUTE,
    BackfillPlan,
    HistoryBackfill,
)
from omega.domain.errors import (
    ProtocolError,
    RateLimited,
    StorageError,
    TransportError,
    ValidationError,
)
from omega.domain.models import Candle, Instrument, Market, decimal
from omega.infrastructure.postgres.config import DatabaseSettings
from omega.runtime import backfill_cli as cli

INSTRUMENT = Instrument("binance", Market.USDM, "ETHUSDT")
DESTINATION = "a" * 64


def candle(index, *, close="10", instrument=INSTRUMENT):
    return Candle(
        instrument,
        index * MINUTE,
        (index + 1) * MINUTE,
        decimal("10"),
        decimal("20"),
        decimal("5"),
        decimal(close),
        decimal("4"),
    )


class History:
    def __init__(self, rows):
        self.rows, self.calls = list(rows), []
        self.hook = None

    def closed_minutes(self, instrument, start_ms, end_ms, *, now_ms):
        self.calls.append((instrument, start_ms, end_ms, now_ms))
        if self.hook:
            self.hook()
        return tuple(c for c in self.rows if start_ms <= c.open_time_ms < end_ms)


class Store:
    def __init__(self):
        self.rows, self.calls = {}, []
        self.hook = None
        self.closed = False

    def put(self, observation):
        self.calls.append(observation)
        value = observation.candle
        key = (value.instrument, value.open_time_ms)
        if key in self.rows and self.rows[key] != value:
            raise StorageError("immutable conflict")
        inserted = key not in self.rows
        self.rows[key] = value
        if self.hook:
            self.hook()
        return inserted

    def get(self, instrument, open_time_ms):
        return self.rows.get((instrument, open_time_ms))

    def close(self):
        self.closed = True


def setup_scan(n=5, *, rows=None, chunk=2, persist=None, state=None, store=None):
    plan = BackfillPlan(INSTRUMENT, 0, n * MINUTE, DESTINATION, chunk)
    history = History([candle(i) for i in range(n)] if rows is None else rows)
    store = store or Store()
    saved = []
    runner = HistoryBackfill(
        plan,
        history,
        store,
        persist=persist or (lambda data: saved.append(deepcopy(data))),
        clock_ms=lambda: (n + 2) * MINUTE,
        state=state,
    )
    return runner, history, store, saved


def finish(runner):
    while (state := runner.step())["state"] == "scanning":
        pass
    return state


def rehash(state):
    state["sha256"] = sha256(
        json.dumps(
            {k: v for k, v in state.items() if k != "sha256"},
            sort_keys=True,
            allow_nan=False,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    return state


def test_pages_can_scan_beyond_old_20000_minute_limit_without_large_requests():
    runner, history, store, saved = setup_scan(20_001, chunk=1000)
    state = finish(runner)
    assert state["observed_minutes"] == len(store.rows) == 20_001
    assert state["state"] == "complete"
    assert len(saved) == 22  # initial state plus 21 durable pages
    assert all(end - start <= 1000 * MINUTE for _, start, end, _ in history.calls)
    assert all(row.raw is None and row.source == "rest" for row in store.calls)
    assert "inserted" not in state


def test_gaps_remain_explicit_and_are_not_filled_or_reported_as_complete():
    runner, _, store, _ = setup_scan(8, rows=[candle(1), candle(2), candle(6)])
    state = finish(runner)
    assert state["state"] == "complete_with_gaps"
    assert state["processed_minutes"] == 8
    assert state["observed_minutes"] == 3
    assert state["missing_minutes"] == 5
    assert state["missing_ranges"] == [
        [0, MINUTE],
        [3 * MINUTE, 6 * MINUTE],
        [7 * MINUTE, 8 * MINUTE],
    ]
    assert len(store.rows) == 3
    assert state["raw_payload"] == "unavailable_from_history_port"


def test_empty_history_and_bounded_gap_preview():
    runner, _, store, _ = setup_scan(3, rows=[])
    assert finish(runner)["missing_ranges"] == [[0, 3 * MINUTE]]
    assert not store.calls
    runner, _, _, _ = setup_scan(400, rows=[candle(i) for i in range(0, 400, 2)], chunk=1000)
    state = finish(runner)
    assert len(state["missing_ranges"]) == MAX_GAP_RANGES
    assert state["missing_minutes"] == 200
    assert state["omitted_missing_minutes"] == 200 - MAX_GAP_RANGES


@pytest.mark.parametrize(
    "changed", ["source_deleted", "source_changed", "db_deleted", "db_changed"]
)
def test_resume_rechecks_committed_anchor_in_both_source_and_store(changed):
    runner, history, store, _ = setup_scan()
    state = runner.step()
    if changed == "source_deleted":
        history.rows = [row for row in history.rows if row.open_time_ms != MINUTE]
    elif changed == "source_changed":
        history.rows[1] = candle(1, close="11")
    elif changed == "db_deleted":
        del store.rows[(INSTRUMENT, MINUTE)]
    else:
        store.rows[(INSTRUMENT, MINUTE)] = candle(1, close="11")
    restored = HistoryBackfill(
        runner.plan,
        history,
        store,
        persist=lambda _: None,
        clock_ms=lambda: 7 * MINUTE,
        state=state,
    )
    with pytest.raises(ProtocolError, match="anchor"):
        restored.step()
    assert restored.snapshot() == state
    assert len(store.calls) == 2


def test_resume_continues_fixed_range_and_rechecks_even_terminal_anchor():
    runner, history, store, _ = setup_scan()
    state = runner.step()
    restored = HistoryBackfill(
        runner.plan,
        history,
        store,
        persist=lambda _: None,
        clock_ms=lambda: 7 * MINUTE,
        state=state,
    )
    assert finish(restored)["observed_minutes"] == 5
    assert len(store.rows) == 5
    del store.rows[(INSTRUMENT, 4 * MINUTE)]
    with pytest.raises(ProtocolError, match="anchor"):
        restored.step()


def test_failed_checkpoint_never_advances_cursor_and_repeat_page_accepts_duplicates():
    saves = []

    def persist(state):
        if state["next_open_ms"]:
            raise OSError("disk unavailable")
        saves.append(state)

    runner, history, store, _ = setup_scan(persist=persist)
    with pytest.raises(OSError):
        runner.step()
    assert runner.snapshot()["next_open_ms"] == 0
    assert len(store.rows) == 2  # DB commits preceded the failed cursor publication.
    restored = HistoryBackfill(
        runner.plan,
        history,
        store,
        persist=lambda _: None,
        clock_ms=lambda: 7 * MINUTE,
        state=saves[0],
    )
    assert finish(restored)["observed_minutes"] == 5
    assert len(store.rows) == 5
    assert len(store.calls) == 7  # Two idempotent retries, never an inflated inserted metric.


def test_unknown_commit_replays_same_immutable_key_and_conflict_preserves_original():
    runner, _, store, _ = setup_scan()

    def uncertain():
        store.hook = None
        raise StorageError("connection lost after commit")

    store.hook = uncertain
    with pytest.raises(StorageError):
        runner.step()
    assert runner.snapshot()["next_open_ms"] == 0
    assert len(store.rows) == 1
    assert finish(runner)["observed_minutes"] == 5
    assert store.rows[(INSTRUMENT, 0)] == candle(0)
    other, _, store, _ = setup_scan(store=store, rows=[candle(0, close="11")])
    with pytest.raises(StorageError):
        other.step()
    assert store.rows[(INSTRUMENT, 0)] == candle(0)
    assert other.snapshot()["next_open_ms"] == 0


@pytest.mark.parametrize(
    "case", ["wrong_instrument", "outside_range", "conflicting", "oversized", "not_candle"]
)
def test_invalid_source_page_is_rejected_before_any_write(case):
    runner, history, store, _ = setup_scan()
    values = {
        "wrong_instrument": [candle(0, instrument=Instrument("binance", Market.SPOT, "ETHUSDT"))],
        "outside_range": [candle(2)],
        "conflicting": [candle(0), candle(0, close="11")],
        "oversized": repeat(candle(0)),
        "not_candle": [None],
    }
    history.closed_minutes = lambda *a, **k: values[case]
    with pytest.raises(ProtocolError):
        runner.step()
    assert not store.calls
    assert runner.snapshot()["next_open_ms"] == 0


def test_equal_source_duplicates_are_counted_once_and_ordered():
    runner, history, store, _ = setup_scan(2)
    history.closed_minutes = lambda *a, **k: [candle(1), candle(0), candle(1)]
    assert runner.step()["observed_minutes"] == 2
    assert [row.candle.open_time_ms for row in store.calls] == [0, MINUTE]


def test_domain_compatible_noninteger_timestamp_cannot_reach_storage():
    runner, history, store, _ = setup_scan()
    # The generic Candle currently permits numeric equality at zero; this boundary
    # still requires exact integer milliseconds before an immutable write.
    row = replace(candle(0), open_time_ms=False)
    history.closed_minutes = lambda *a, **k: (row,)
    with pytest.raises(ProtocolError):
        runner.step()
    assert not store.calls


@pytest.mark.parametrize("where", ["source", "put"])
def test_retired_owner_drops_late_result_and_does_not_publish_cursor(where):
    runner, history, store, saved = setup_scan()
    if where == "source":
        history.hook = runner.close
    else:
        store.hook = runner.close
    with pytest.raises(InterruptedError):
        runner.step()
    assert len(store.calls) == (1 if where == "put" else 0)
    assert runner.snapshot()["next_open_ms"] == 0
    assert len(saved) == 1


@pytest.mark.parametrize("binding", ["history", "store", "plan"])
def test_rebinding_during_source_read_never_commits_old_generation(binding):
    runner, history, store, _ = setup_scan()
    history.hook = lambda: setattr(runner, binding, object())
    with pytest.raises(ValidationError, match="binding"):
        runner.step()
    assert not store.calls
    assert runner.snapshot()["next_open_ms"] == 0


def test_concurrent_step_is_rejected_and_actual_clock_rewind_stops_writes():
    runner, history, store, _ = setup_scan()

    def overlap():
        with pytest.raises(ValidationError, match="already"):
            runner.step()
        runner.clock_ms = lambda: MINUTE

    history.hook = overlap
    with pytest.raises(ValidationError, match="closed"):
        runner.step()
    assert not store.calls


def test_close_while_history_is_blocked_rejects_late_response():
    runner, history, store, _ = setup_scan()
    entered, release = threading.Event(), threading.Event()
    errors = []

    def block():
        entered.set()
        assert release.wait(2)

    def worker():
        try:
            runner.step()
        except Exception as exc:
            errors.append(exc)

    history.hook = block
    thread = threading.Thread(target=worker)
    thread.start()
    try:
        assert entered.wait(2)
        runner.close()
    finally:
        release.set()
        thread.join(2)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], InterruptedError)
    assert not store.calls and runner.snapshot()["next_open_ms"] == 0


def test_admitted_checkpoint_can_finish_but_closed_owner_does_not_publish_in_memory():
    runner, _, _, saved = setup_scan()

    def persist(state):
        saved.append(deepcopy(state))
        runner.close()

    runner.persist = persist
    with pytest.raises(InterruptedError):
        runner.step()
    assert saved[-1]["next_open_ms"] == 2 * MINUTE
    assert runner.snapshot()["next_open_ms"] == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"start_ms": True},
        {"start_ms": 1},
        {"end_ms": 0},
        {"end_ms": 525601 * MINUTE},
        {"chunk_minutes": 1001},
        {"chunk_minutes": True},
        {"destination_id": "password"},
        {"instrument": Instrument("other", Market.SPOT, "ETHUSDT")},
    ],
)
def test_plan_bounds_are_strict(changes):
    with pytest.raises(ValidationError):
        replace(BackfillPlan(INSTRUMENT, 0, 5 * MINUTE, DESTINATION), **changes)


@pytest.mark.parametrize("change", ["checksum", "count", "anchor", "state", "gaps"])
def test_malformed_checkpoint_is_rejected(change):
    runner, _, _, _ = setup_scan()
    state = runner.step()
    if change == "checksum":
        state["next_open_ms"] += MINUTE
    elif change == "count":
        state["observed_minutes"] = True
    elif change == "anchor":
        state["last_candle"]["open_time_ms"] = state["next_open_ms"]
    elif change == "state":
        state["state"] = "complete"
    else:
        state["missing_ranges"] = [[0, MINUTE]]
    if change != "checksum":
        rehash(state)
    with pytest.raises(ValidationError):
        HistoryBackfill.validate_state(runner.plan, state)


def test_future_target_rejected_before_initial_checkpoint_and_ports():
    plan = BackfillPlan(INSTRUMENT, 0, 5 * MINUTE, DESTINATION)
    with pytest.raises(ValidationError, match="closed"):
        HistoryBackfill(
            plan,
            None,
            None,
            persist=lambda _: pytest.fail("saved future plan"),
            clock_ms=lambda: 5 * MINUTE - 1,
        )


def test_checkpoint_never_overwrites_external_changes_or_deletion(tmp_path):
    path = tmp_path / "state.json"
    cp = cli.Checkpoint(path)
    cp.save({"first": True})
    path.write_text('{"external":true}')
    with pytest.raises(ValidationError, match="changed"):
        cp.save({"second": True})
    assert json.loads(path.read_text()) == {"external": True}
    cp = cli.Checkpoint(path)
    path.unlink()
    with pytest.raises(ValidationError, match="disappeared"):
        cp.save({"second": True})


def test_checkpoint_initial_collision_size_and_invalid_json(tmp_path):
    path = tmp_path / "state.json"
    cp = cli.Checkpoint(path)
    path.write_text("{}")
    with pytest.raises(ValidationError, match="changed"):
        cp.save({})
    path.write_text("{" * 65537)
    with pytest.raises(ValidationError, match="bound"):
        cli.Checkpoint(path)
    path.write_text("{")
    with pytest.raises(ValidationError, match="JSON"):
        cli.Checkpoint(path)


def test_post_commit_external_write_is_not_adopted_as_our_next_generation(tmp_path, monkeypatch):
    path = tmp_path / "state.json"
    checkpoint = cli.Checkpoint(path)
    checkpoint.save({"revision": 1})
    before = checkpoint.expected
    atomic = cli.atomic_json

    def replaced_after_commit(target, state):
        atomic(target, state)
        target.write_text('{"foreign":true}', encoding="utf-8")

    monkeypatch.setattr(cli, "atomic_json", replaced_after_commit)
    with pytest.raises(ValidationError, match="immediately after commit"):
        checkpoint.save({"revision": 2})
    assert checkpoint.state == {"revision": 1} and checkpoint.expected == before
    with pytest.raises(ValidationError, match="changed"):
        checkpoint.save({"revision": 3})
    assert json.loads(path.read_text(encoding="utf-8")) == {"foreign": True}


def test_linked_state_directory_is_rejected_before_writing(monkeypatch, tmp_path):
    monkeypatch.setattr(Path, "is_symlink", lambda path: path == tmp_path / "state")
    with pytest.raises(ValidationError, match="linked"):
        cli._safe_path(tmp_path, Path("state/backfill") / ("a" * 64 + ".json"))
    assert not (tmp_path / "state").exists()


@pytest.mark.parametrize(
    "stamp", ["2026-10-01T00:00:00", "2026-10-01T00:00:00.001Z", "bad", "1960-01-01T00:00:00Z"]
)
def test_cli_requires_aware_exact_minute_times(stamp):
    with pytest.raises(ValidationError):
        cli.utc_ms(stamp)


def test_utc_conversion_is_exact_and_identity_hash_excludes_password():
    assert cli.utc_ms("2026-10-01T09:00:00+09:00") == cli.utc_ms("2026-10-01T00:00:00Z")
    settings = DatabaseSettings("127.0.0.1", 55432, "omega", "app", "secret")
    assert cli.destination_id(settings) == cli.destination_id(replace(settings, password="other"))
    assert cli.destination_id(settings) != cli.destination_id(replace(settings, dbname="different"))


@pytest.mark.parametrize("error", [TransportError("private message"), RateLimited(0.1)])
def test_public_read_retries_are_bounded_and_never_mutations(error):
    calls, waits = [], []

    def fail(*args, **kwargs):
        calls.append(1)
        raise error

    retry = cli.RetryHistory(
        SimpleNamespace(closed_minutes=fail),
        threading.Event(),
        wait=lambda delay: waits.append(delay),
    )
    with pytest.raises(type(error)):
        retry.closed_minutes(INSTRUMENT, 0, MINUTE, now_ms=MINUTE)
    assert len(calls) == 3 and len(waits) == 2


def test_retry_after_is_never_shortened_and_wait_can_be_interrupted():
    def fail(*args, **kwargs):
        raise RateLimited(61)

    retry = cli.RetryHistory(
        SimpleNamespace(closed_minutes=fail),
        threading.Event(),
        wait=lambda _: pytest.fail("waited/retried too early"),
    )
    with pytest.raises(RateLimited):
        retry.closed_minutes()
    retry.source = SimpleNamespace(closed_minutes=lambda: (_ for _ in ()).throw(TransportError()))
    retry.wait = lambda _: True
    with pytest.raises(InterruptedError):
        retry.closed_minutes()


def fake_cli(monkeypatch, tmp_path, *, rows=None):
    settings = DatabaseSettings("127.0.0.1", 55432, "omega", "app", "NEVER-PRINT-THIS")
    store, history = Store(), History([candle(i) for i in range(5)] if rows is None else rows)
    monkeypatch.setattr(cli, "load_database_settings", lambda _: settings)
    monkeypatch.setattr(cli, "PostgresCandleStore", lambda _: store)
    monkeypatch.setattr(cli, "SharedRequestBudget", lambda path: path)
    monkeypatch.setattr(cli, "BinanceClient", lambda *a, **k: SimpleNamespace(kwargs=k))
    monkeypatch.setattr(cli, "BinanceMarketData", lambda *a: history)
    argv = [
        "--root",
        str(tmp_path),
        "--db-config",
        str(tmp_path / "db.toml"),
        "--market",
        "usdm",
        "--symbol",
        "ETHUSDT",
        "--from-ms",
        "0",
        "--to-ms",
        str(5 * MINUTE),
        "--chunk-minutes",
        "2",
    ]
    return argv, settings, store, history


def test_cli_help_has_no_environment_or_network_side_effects(monkeypatch, capsys):
    monkeypatch.setattr(cli, "run", lambda _: pytest.fail("help touched runtime"))
    with pytest.raises(SystemExit) as result:
        cli.main(["--help"])
    assert result.value.code == 0
    assert "--from-utc" in capsys.readouterr().out


def test_cli_fixed_plan_reuse_resume_and_shared_budget(monkeypatch, tmp_path, capsys):
    argv, settings, store, history = fake_cli(monkeypatch, tmp_path)
    budget_paths = []
    monkeypatch.setattr(cli, "SharedRequestBudget", lambda path: budget_paths.append(path))
    assert cli.main(argv) == 0
    (statefile,) = (tmp_path / "state/backfill").glob("*.json")
    state = json.loads(statefile.read_text())
    assert state["plan"]["destination_id"] == cli.destination_id(settings)
    assert statefile.stem == state["plan_id"]
    assert budget_paths == [tmp_path / "state/binance-public-budget.json"]
    assert store.closed and "NEVER-PRINT-THIS" not in capsys.readouterr().out
    calls = len(store.calls)
    assert cli.main(argv) == 0  # exact range transparently resumes; terminal anchor is checked
    assert len(store.calls) == calls
    resume = argv[:4] + ["--resume", statefile.stem]
    assert cli.main(resume) == 0
    history.rows[-1] = candle(4, close="11")
    assert cli.main(resume) == 1


def test_cli_gap_exit_and_source_failures_preserve_resume_without_secrets(
    monkeypatch, tmp_path, capsys
):
    argv, _, store, history = fake_cli(monkeypatch, tmp_path, rows=[])
    assert cli.main(argv) == 3
    (statefile,) = (tmp_path / "state/backfill").glob("*.json")
    assert json.loads(statefile.read_text())["state"] == "complete_with_gaps"
    # Use a different range so a fresh plan is requested.
    argv[argv.index("--to-ms") + 1] = str(4 * MINUTE)
    history.hook = lambda: (_ for _ in ()).throw(ProtocolError("NEVER-PRINT-THIS"))
    assert cli.main(argv) == 1
    assert store.closed
    assert "NEVER-PRINT-THIS" not in capsys.readouterr().out


def test_cli_ctrl_c_releases_plan_owner_and_keeps_initial_cursor(monkeypatch, tmp_path):
    argv, _, store, history = fake_cli(monkeypatch, tmp_path)
    history.hook = lambda: (_ for _ in ()).throw(KeyboardInterrupt())
    assert cli.main(argv) == 130
    assert store.closed
    (statefile,) = (tmp_path / "state/backfill").glob("*.json")
    assert json.loads(statefile.read_text())["next_open_ms"] == 0
    history.hook = None
    assert cli.main(argv) == 0  # lock was released


@pytest.mark.parametrize(
    "extra",
    [
        ["--resume", "../outside"],
        ["--resume", "f" * 64],
        ["--chunk-minutes", "1001"],
        ["--to-ms", str(525601 * MINUTE)],
    ],
)
def test_cli_invalid_plans_never_open_store(monkeypatch, tmp_path, extra):
    argv, _, _, _ = fake_cli(monkeypatch, tmp_path)
    monkeypatch.setattr(cli, "PostgresCandleStore", lambda _: pytest.fail("opened invalid plan"))
    assert cli.main(argv + extra) == 1


def test_resume_destination_and_page_size_must_match(monkeypatch, tmp_path):
    argv, settings, _, _ = fake_cli(monkeypatch, tmp_path)
    assert cli.main(argv) == 0
    (statefile,) = (tmp_path / "state/backfill").glob("*.json")
    resume = argv[:4] + ["--resume", statefile.stem]
    monkeypatch.setattr(
        cli, "PostgresCandleStore", lambda _: pytest.fail("opened wrong destination")
    )
    assert cli.main(resume + ["--chunk-minutes", "1"]) == 1
    monkeypatch.setattr(cli, "load_database_settings", lambda _: replace(settings, dbname="other"))
    assert cli.main(resume) == 1


def test_same_plan_owner_exclusion(monkeypatch, tmp_path):
    argv, _, _, history = fake_cli(monkeypatch, tmp_path)
    nested = []
    history.hook = lambda: nested.append(cli.main(argv))
    assert cli.main(argv) == 0
    assert nested and set(nested) == {1}
