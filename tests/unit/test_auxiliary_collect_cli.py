"""CLI configuration, safe composition and summaries without network or PostgreSQL."""

import json
from types import SimpleNamespace

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime import auxiliary_collect_cli as cli
from omega.runtime.auxiliary_journal import AuxiliaryJobJournal


def settings(**changes):
    return SimpleNamespace(
        **{
            "host": "127.0.0.1",
            "port": 55433,
            "dbname": "omega_test",
            "user": "omega_app",
            "password": "synthetic-private-value",
            **changes,
        }
    )


def arguments(tmp_path, *extra):
    return cli.parser().parse_args(["--root", str(tmp_path), *extra])


def new_arguments(tmp_path, *extra):
    return arguments(
        tmp_path,
        "--market",
        "usdm",
        "--symbol",
        "ETHUSDT",
        "--from-ms",
        "60000",
        "--to-ms",
        "180000",
        *extra,
    )


def test_new_plan_has_fixed_defaults_and_destination_does_not_depend_on_password(tmp_path):
    args = new_arguments(tmp_path)
    plan = cli.collection_plan(args, settings())
    assert plan["limit"] == 500 and plan["max_pages"] == 20
    assert plan["source"] == "binance:usdm:ETHUSDT:aggTrade"
    assert plan["destination_id"] == cli.destination_id(settings(password="different-synthetic"))
    assert plan["destination_id"] != cli.destination_id(settings(dbname="omega"))
    assert "synthetic-private-value" not in json.dumps(plan)


def test_resume_loads_verified_original_plan_and_refuses_overrides_or_other_destination(tmp_path):
    plan = cli.collection_plan(new_arguments(tmp_path), settings())
    journal = AuxiliaryJobJournal(tmp_path / "state/auxiliary-jobs", plan)
    assert cli.collection_plan(arguments(tmp_path, "--resume", journal.job_id), settings()) == plan
    with pytest.raises(ValidationError, match="overrides"):
        cli.collection_plan(
            arguments(tmp_path, "--resume", journal.job_id, "--limit", "100"), settings()
        )
    with pytest.raises(ValidationError, match="destination"):
        cli.collection_plan(arguments(tmp_path, "--resume", journal.job_id), settings(port=55432))
    with pytest.raises(ValidationError):
        cli.collection_plan(arguments(tmp_path, "--resume", "../escape"), settings())


@pytest.mark.parametrize(
    "options", [[], ["--market", "usdm"], ["--market", "usdm", "--symbol", "ETHUSDT"]]
)
def test_incomplete_new_job_is_rejected_before_any_connection(tmp_path, options):
    with pytest.raises(ValidationError):
        cli.collection_plan(arguments(tmp_path, *options), settings())


@pytest.mark.parametrize("close_failure", [False, True])
def test_collection_composition_owns_symbol_source_lock_and_always_closes_store(
    tmp_path, monkeypatch, close_failure
):
    monkeypatch.setattr(cli, "load_database_settings", lambda _path: settings())
    calls = {}

    class Resource:
        def close(self):
            calls["closed_store"] = True
            if close_failure:
                raise StorageError("synthetic close failure")

    class Lock:
        def __init__(self, path):
            calls["lock"] = path

        def close(self):
            calls["closed_lock"] = True

    class Runner:
        def __init__(self, plan, client, archive, store, journal, **options):
            calls["plan"] = plan
            calls["client"] = client
            calls["journal"] = journal
            calls["options"] = options

        def run(self):
            return {"complete": False, "reason": "stopped"}

    monkeypatch.setattr(cli, "InstanceLock", Lock)
    monkeypatch.setattr(cli, "PostgresAuxiliaryStore", lambda _settings: Resource())
    monkeypatch.setattr(cli, "FileRawArchive", lambda _path: object())
    monkeypatch.setattr(cli, "AuxiliaryPublicBudget", lambda _path: object())
    monkeypatch.setattr(cli, "UrllibTransport", object)
    monkeypatch.setattr(cli, "AuxiliaryCollection", Runner)
    if close_failure:
        with pytest.raises(StorageError):
            cli.collect(new_arguments(tmp_path), clock_ms=lambda: 300000)
    else:
        result = cli.collect(new_arguments(tmp_path), clock_ms=lambda: 300000)
        assert result["reason"] == "stopped"
    assert calls["closed_store"] and calls["closed_lock"]
    assert calls["lock"].parent == tmp_path / "state/locks"
    assert len(calls["lock"].stem) == 64
    assert calls["client"].client.credentials is None
    assert calls["client"].client.enable_trading is False
    assert calls["journal"].snapshot()["plan"] == calls["plan"]


@pytest.mark.parametrize(
    "identity", [{"dbname": "atmEta"}, {"port": 5432}, {"host": "remote.example"}]
)
def test_initial_cli_refuses_legacy_or_unapproved_db_targets_before_store_creation(
    tmp_path, monkeypatch, identity
):
    monkeypatch.setattr(cli, "load_database_settings", lambda _path: settings(**identity))
    monkeypatch.setattr(cli, "PostgresAuxiliaryStore", lambda _: pytest.fail("must not open DB"))
    with pytest.raises(ValidationError, match="local Omega"):
        cli.collect(new_arguments(tmp_path), clock_ms=lambda: 300000)


def test_lock_is_released_when_database_constructor_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(cli, "load_database_settings", lambda _: settings())
    closed = []
    monkeypatch.setattr(
        cli, "InstanceLock", lambda _: SimpleNamespace(close=lambda: closed.append(True))
    )
    monkeypatch.setattr(cli, "AuxiliaryPublicBudget", lambda _: object())
    monkeypatch.setattr(cli, "UrllibTransport", object)

    def fail(_settings):
        raise StorageError("synthetic database unavailable")

    monkeypatch.setattr(cli, "PostgresAuxiliaryStore", fail)
    with pytest.raises(StorageError):
        cli.collect(new_arguments(tmp_path), clock_ms=lambda: 300000)
    assert closed == [True]


@pytest.mark.parametrize(
    "complete,reason,exit_code",
    [(True, "complete", 0), (False, "stopped", 130), (False, "collection_failed", 2)],
)
def test_main_prints_only_one_summary_and_uses_nonzero_exit_for_incomplete_job(
    monkeypatch, capsys, complete, reason, exit_code
):
    monkeypatch.setattr(cli.signal, "signal", lambda *_: 0)
    monkeypatch.setattr(
        cli, "collect", lambda *_args, **_kw: {"complete": complete, "reason": reason}
    )
    assert cli.main([]) == exit_code
    assert json.loads(capsys.readouterr().out) == {"complete": complete, "reason": reason}


def test_exception_details_or_raw_bodies_do_not_reach_stdout(monkeypatch, capsys):
    monkeypatch.setattr(cli.signal, "signal", lambda *_: 0)

    def fail(*_args, **_options):
        raise StorageError("synthetic-private-value: do not log response body")

    monkeypatch.setattr(cli, "collect", fail)
    assert cli.main([]) == 2
    output = capsys.readouterr().out
    assert "synthetic-private-value" not in output and "response body" not in output
    assert json.loads(output)["error"] == "StorageError"
