"""Reader-only sealed fixtures plus one real Session/helper retirement integration."""

import json
import os
import threading
from dataclasses import replace
from hashlib import sha256

import pytest
from test_margin_ledger_pages import mixed
from test_margin_session import configuration, new

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.runtime import margin_retired_history as module
from omega.runtime.margin_cleanup import confirmed_cleanup
from omega.runtime.margin_configuration import configuration_parameters
from omega.runtime.margin_ledger import LedgerPages, LedgerQuery
from omega.runtime.margin_retired_history import MarginRetiredHistory
from omega.runtime.margin_retirement import (
    MarginRetirement,
    registered_session_configuration,
    retirement_sha256,
)
from omega.runtime.margin_store import account_document


def wait_job(reader, job_id):
    with reader.condition:
        assert reader.condition.wait_for(
            lambda: reader.status(job_id)["state"] in {"completed", "failed"}, timeout=5
        )
    return reader.status(job_id)


def seal(body):
    return {**body, "sha256": retirement_sha256(body)}


def write(path, document):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")), encoding="utf-8")


def sealed_fixture(root, identity="demo"):
    # Isolated reader authority fixtures: no claim that these bytes establish
    # a real financial retirement. Production helper still validates every read.
    account = replace(mixed(150), account_id=identity)
    parameters, _ = configuration()
    parameters["account_id"] = identity
    _, rows = configuration()
    raw = {"account": parameters, "strategies": rows, "start_ms": 0, "brackets": []}
    _, digest = configuration_parameters(raw)
    config_hash = retirement_sha256(
        registered_session_configuration(raw, "ohlcv_margin_session_v1")
    )
    anchor = "a" * 64
    namespace = retirement_sha256((os.path.normcase(str(root.resolve())), config_hash))
    original = LedgerPages(namespace).page(anchor, account, LedgerQuery())
    names = sorted(
        [
            "session_lease",
            "journal",
            "account_store",
            *("strategy_store:" + row["id"] for row in rows),
        ]
    )
    observation = {
        "schema_version": 1,
        "scope": "virtual_margin_retirement_observation",
        "account_id": identity,
        "registered_parameters_sha256": digest,
        "session_config_hash": config_hash,
        "ledger_namespace": namespace,
        "account_sha256": retirement_sha256(account_document(account)),
        "source": {
            "anchor_sha256": anchor,
            "tip_sha256": "d" * 64,
            "count": 1,
            "chain_sha256": "e" * 64,
        },
        "ledger_snapshot": original["snapshot"],
        "ledger_total": 150,
        "session_metadata_sha256": retirement_sha256({"synthetic": "reader_session_metadata"}),
        "shutdown_sha256": retirement_sha256({"synthetic": "reader_shutdown_observation"}),
        "shutdown_metadata_sha256": retirement_sha256(None),
        "strategies": [
            {
                "id": row["id"],
                "revision": 1,
                "bootstrap_sha256": retirement_sha256(("synthetic_bootstrap", row["id"])),
                "checkpoint_sha256": retirement_sha256(("synthetic_checkpoint", row["id"])),
                "pending": False,
            }
            for row in rows
        ],
        "paused": True,
        "orders": 0,
        "staged": 0,
        "cleanup_generation": "f" * 32,
        "cleanup_resource_names": names,
    }
    archive = seal(
        {
            "schema_version": 1,
            "scope": "virtual_margin_retirement_archive",
            "account_id": identity,
            "registered_parameters": raw,
            "registered_parameters_sha256": digest,
            "session_config_hash": config_hash,
            "ledger_namespace": namespace,
            "observation": observation,
            "account": account_document(account),
            "source_files": [],
            "source_inventory_sha256": retirement_sha256([]),
        }
    )
    receipt = seal(
        {
            "schema_version": 1,
            "scope": "virtual_margin_retirement",
            "account_id": identity,
            "state": "retired",
            "request": {"operation_id": "retire", "core_epoch": "epoch", "issued_at_ms": 0},
            "registered_parameters_sha256": digest,
            "session_config_hash": config_hash,
            "observation_sha256": retirement_sha256(observation),
            "archive_sha256": archive["sha256"],
            "cleanup_generation": "f" * 32,
            "release_evidence": {
                "method": "owned_native_cleanup",
                "generation": "f" * 32,
                "resources": [
                    {"name": name, "state": "complete", "error_type": None, "attempts": 1}
                    for name in names
                ],
            },
        }
    )
    helper = MarginRetirement(root, identity)
    write(helper.archive_path, archive)
    write(helper.path, receipt)
    # Every corruption test starts from an independently accepted strict
    # baseline, so a stale fixture cannot manufacture a false negative pass.
    assert helper.read() == receipt
    assert helper.archive() == archive
    return helper, receipt, archive, original


def test_cold_gets_and_status_have_no_io_and_exact_unknown_shape(tmp_path, monkeypatch):
    reader = MarginRetiredHistory(tmp_path)

    def forbidden(*args):
        pytest.fail("cold GET attempted archive IO")

    monkeypatch.setattr(module, "_read_archive", forbidden)
    try:
        assert reader.ledger_page("demo", "a" * 64)["reason_code"] == "archive_not_loaded"
        assert reader.configuration("demo", "a" * 64)["reason_code"] == "archive_not_loaded"
        assert reader.status("unknown") == {
            "schema_version": 1,
            "scope": module.SCOPE,
            "job_id": "unknown",
            "account_id": None,
            "receipt_sha256": None,
            "state": "unavailable",
            "reason_code": "job_unknown",
            "cached": False,
            "ledger_total": None,
        }
        assert reader._thread is None
    finally:
        reader.close()
    assert confirmed_cleanup(reader.cleanup_status())


def test_actual_helper_files_original_tokens_config_and_single_cache_replacement(
    tmp_path, monkeypatch
):
    _, receipt, archive, original = sealed_fixture(tmp_path)
    _, other, _, _ = sealed_fixture(tmp_path, "other")
    before = {path: sha256(path.read_bytes()).hexdigest() for path in tmp_path.rglob("*.json")}
    native_read = MarginRetirement._bytes
    reads = []

    def read_only_files(helper, path, **arguments):
        assert threading.current_thread() is not threading.main_thread()
        assert path in {helper.path, helper.archive_path}
        reads.append(path)
        return native_read(helper, path, **arguments)

    monkeypatch.setattr(MarginRetirement, "_bytes", read_only_files)
    reader = MarginRetiredHistory(tmp_path)
    try:
        reader.load("first", "demo", receipt["sha256"])
        assert wait_job(reader, "first")["cached"] is True
        read_count = len(reads)
        page = reader.ledger_page("demo", receipt["sha256"], snapshot=original["snapshot"])
        assert page == original
        ending = reader.ledger_page(
            "demo", receipt["sha256"], offset=128, snapshot=original["snapshot"]
        )
        assert len(ending["rows"]) == 22 and ending["next_offset"] is None
        config = reader.configuration("demo", receipt["sha256"])
        assert config["parameters"] == archive["registered_parameters"]
        config["parameters"]["account"]["balance"] = "modified caller copy"
        assert (
            reader.configuration("demo", receipt["sha256"])["parameters"]
            == archive["registered_parameters"]
        )
        assert len(reads) == read_count
        reader.load("second", "other", other["sha256"])
        assert wait_job(reader, "second")["state"] == "completed"
        assert reader.status("first")["cached"] is False
        assert reader.ledger_page("demo", receipt["sha256"])["reason_code"] == "archive_not_loaded"
        assert reader.configuration("other", "0" * 64)["state"] == "unavailable"
        reader.load("reload", "demo", receipt["sha256"])
        assert wait_job(reader, "reload")["state"] == "completed"
        assert (
            reader.ledger_page("demo", receipt["sha256"], snapshot=original["snapshot"]) == original
        )
    finally:
        reader.close()
    assert before == {path: sha256(path.read_bytes()).hexdigest() for path in before}


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "corrupt",
        "directory",
        "oversize",
        "receipt",
        "prepared",
        "source_anchor",
        "prefix",
    ],
)
def test_actual_helper_rejects_missing_nonregular_tampered_and_bounded_files(tmp_path, damage):
    helper, receipt, archive, _ = sealed_fixture(tmp_path)
    if damage == "missing":
        helper.archive_path.unlink()
    elif damage == "corrupt":
        helper.archive_path.write_bytes(b'{"secret/path":')
    elif damage == "directory":
        helper.archive_path.unlink()
        helper.archive_path.mkdir()
    elif damage == "oversize":
        with helper.archive_path.open("wb") as stream:
            stream.truncate(16_000_001)
    elif damage == "receipt":
        receipt["sha256"] = "0" * 64
        write(helper.path, receipt)
    elif damage == "prepared":
        receipt = seal(
            {
                **{k: v for k, v in receipt.items() if k != "sha256"},
                "state": "prepared",
                "release_evidence": None,
            }
        )
        write(helper.path, receipt)
    else:
        if damage == "source_anchor":
            archive["observation"]["source"]["anchor_sha256"] = "0" * 64
        else:
            archive["observation"]["ledger_total"] = 149
        archive = seal({k: v for k, v in archive.items() if k != "sha256"})
        receipt = seal(
            {
                **{k: v for k, v in receipt.items() if k != "sha256"},
                "archive_sha256": archive["sha256"],
                "observation_sha256": retirement_sha256(archive["observation"]),
            }
        )
        write(helper.archive_path, archive)
        write(helper.path, receipt)
    reader = MarginRetiredHistory(tmp_path)
    try:
        reader.load("load", "demo", receipt["sha256"])
        status = wait_job(reader, "load")
        assert status["state"] == "failed" and status["cached"] is False
        assert status["reason_code"] in {"archive_conflict", "archive_unavailable"}
        assert "secret" not in json.dumps(status) and str(tmp_path) not in json.dumps(status)
        assert reader.ledger_page("demo", receipt["sha256"])["state"] == "unavailable"
    finally:
        reader.close()


def test_actual_helper_linked_archive_is_rejected(tmp_path):
    helper, receipt, _, _ = sealed_fixture(tmp_path)
    target = tmp_path / "original.json"
    helper.archive_path.rename(target)
    try:
        helper.archive_path.symlink_to(target)
    except OSError:
        pytest.skip("OS does not grant symbolic link creation")
    reader = MarginRetiredHistory(tmp_path)
    try:
        reader.load("link", "demo", receipt["sha256"])
        assert wait_job(reader, "link")["state"] == "failed"
    finally:
        reader.close()


def test_cache_generation_is_pinned_during_page_and_failed_load_keeps_old_cache(
    tmp_path, monkeypatch
):
    _, receipt, _, original = sealed_fixture(tmp_path)
    _, other, _, _ = sealed_fixture(tmp_path, "other")
    reader = MarginRetiredHistory(tmp_path)
    decode_entered, decode_release = threading.Event(), threading.Event()
    page_entered, page_release = threading.Event(), threading.Event()
    actual = module._read_archive
    results = []

    def blocked_decode(*args):
        result = actual(*args)
        decode_entered.set()
        assert decode_release.wait(5)
        return result

    try:
        reader.load("initial", "demo", receipt["sha256"])
        wait_job(reader, "initial")
        cache = reader._cache
        actual_page = cache.pages.page

        def blocked_page(*args):
            page_entered.set()
            assert page_release.wait(5)
            return actual_page(*args)

        monkeypatch.setattr(cache.pages, "page", blocked_page)
        monkeypatch.setattr(module, "_read_archive", blocked_decode)
        reader.load("replacement", "other", other["sha256"])
        assert decode_entered.wait(5)
        page_thread = threading.Thread(
            target=lambda: results.append(reader.ledger_page("demo", receipt["sha256"]))
        )
        page_thread.start()
        assert page_entered.wait(5)
        decode_release.set()
        assert reader._cache is cache
        page_release.set()
        page_thread.join(5)
        assert not page_thread.is_alive() and results == [original]
        assert wait_job(reader, "replacement")["cached"] is True
        monkeypatch.setattr(module, "_read_archive", actual)
        reader.load("missing", "missing", "0" * 64)
        assert wait_job(reader, "missing")["state"] == "failed"
        assert reader.configuration("other", other["sha256"])["state"] == "ready"
    finally:
        decode_release.set()
        page_release.set()
        reader.close()


def test_worker_start_failure_is_closed_and_native_cleanup_can_be_confirmed(tmp_path, monkeypatch):
    def fail(*args):
        raise RuntimeError("private thread information")

    monkeypatch.setattr(threading.Thread, "start", fail)
    reader = MarginRetiredHistory(tmp_path)
    result = reader.load("start", "demo", "a" * 64)
    assert result["state"] == "failed" and result["reason_code"] == "reader_unavailable"
    assert reader.load("next", "demo", "a" * 64)["reason_code"] == "reader_closed"
    reader.close()
    assert confirmed_cleanup(reader.cleanup_status())


def test_eight_ephemeral_jobs_bounded_conflicts_idempotence_and_completed_eviction(
    tmp_path, monkeypatch
):
    entered, release = threading.Event(), threading.Event()

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        raise StorageError("private/path and raw parameters")

    monkeypatch.setattr(module, "_read_archive", blocked)
    reader = MarginRetiredHistory(tmp_path)
    try:
        reader.load("job0", "demo", "a" * 64)
        assert entered.wait(5)
        for index in range(1, 8):
            assert reader.load(f"job{index}", "demo", "a" * 64)["state"] == "queued"
        assert len(reader._jobs) == 8 and len(reader._queue) == 7
        assert reader.load("job8", "demo", "a" * 64)["reason_code"] == "job_capacity"
        assert reader.load("job0", "demo", "a" * 64)["state"] == "running"
        with pytest.raises(ProtocolError):
            reader.load("job0", "other", "a" * 64)
        release.set()
        assert wait_job(reader, "job7")["state"] == "failed"
        reader.load("job8", "demo", "a" * 64)
        assert wait_job(reader, "job8")["state"] == "failed"
        assert len(reader._jobs) == 8 and reader.status("job0")["state"] == "unavailable"
    finally:
        release.set()
        reader.close()


def test_close_timeout_keeps_original_thread_cache_and_late_result_cannot_publish(
    tmp_path, monkeypatch
):
    _, receipt, _, _ = sealed_fixture(tmp_path)
    entered, release = threading.Event(), threading.Event()
    actual = module._read_archive

    def blocked(*args):
        entered.set()
        assert release.wait(5)
        return actual(*args)

    reader = MarginRetiredHistory(tmp_path)
    reader.load("old", "demo", receipt["sha256"])
    wait_job(reader, "old")
    cache = reader._cache
    monkeypatch.setattr(module, "_read_archive", blocked)
    monkeypatch.setattr(module, "CLOSE_TIMEOUT", 0.01)
    try:
        reader.load("late", "demo", receipt["sha256"])
        assert entered.wait(5)
        original_thread = reader._thread
        with pytest.raises(StorageError):
            reader.close()
        assert reader._thread is original_thread and reader._cache is cache
        assert not confirmed_cleanup(reader.cleanup_status())
        assert reader.status("late")["reason_code"] == "reader_closed"
        assert reader.load("new", "demo", receipt["sha256"])["reason_code"] == "reader_closed"
        release.set()
        original_thread.join(5)
        assert not original_thread.is_alive()
        assert reader._cache is cache and not confirmed_cleanup(reader.cleanup_status())
        reader.close()
        assert confirmed_cleanup(reader.cleanup_status()) and reader._cache is None
        assert reader.cleanup_status()["resources"][0]["error_type"] == "StorageError"
        assert reader.status("late")["state"] == "failed"
    finally:
        release.set()
        reader.close()


@pytest.mark.parametrize(
    "identity,receipt",
    [("../escape", "a" * 64), ("a" * 129, "a" * 64), ("demo", "A" * 64), ("demo", "f" * 63)],
)
def test_strict_bounded_ids_and_receipts(tmp_path, identity, receipt):
    reader = MarginRetiredHistory(tmp_path)
    try:
        for action in (
            lambda: reader.load("job", identity, receipt),
            lambda: reader.ledger_page(identity, receipt),
            lambda: reader.configuration(identity, receipt),
        ):
            with pytest.raises(ValidationError):
                action()
        assert reader._thread is None
    finally:
        reader.close()


def test_real_session_helper_retirement_then_history_load_without_session_reopen(
    tmp_path, monkeypatch
):
    session = new(tmp_path)
    account, rows = configuration()
    parameters = {"account": account, "strategies": rows, "start_ms": 0, "brackets": []}
    helper = MarginRetirement(tmp_path, account["account_id"])
    try:
        observation = session.retirement_snapshot(parameters)
        original = session.ledger_page()
        plan = helper.make_plan(
            {"operation_id": "retire", "core_epoch": "epoch", "issued_at_ms": 0},
            parameters,
            observation,
        )
        helper.prepare(plan)
        helper.close(session)
        receipt = helper.finish()
    finally:
        session.close()
    from omega.runtime import margin_session

    monkeypatch.setattr(
        margin_session.MarginTradingSession,
        "__init__",
        lambda *a, **k: pytest.fail("retired history opened Session"),
    )
    reader = MarginRetiredHistory(tmp_path)
    try:
        reader.load("real", account["account_id"], receipt["sha256"])
        assert wait_job(reader, "real")["state"] == "completed"
        assert (
            reader.ledger_page(
                account["account_id"], receipt["sha256"], snapshot=original["snapshot"]
            )
            == original
        )
    finally:
        reader.close()
