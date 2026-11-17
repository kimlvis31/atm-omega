"""Actual synthetic Session/helper orchestration and bounded owner-port faults."""

import threading
from collections import deque
from copy import deepcopy

import pytest
from test_margin_session import configuration, new

from omega.domain.errors import ProtocolError, StorageError, ValidationError
from omega.runtime import margin_retirement_runtime as module
from omega.runtime.margin_cleanup import confirmed_cleanup
from omega.runtime.margin_retirement import MarginRetirement, retirement_sha256
from omega.runtime.margin_retirement_client import retirement_body
from omega.runtime.margin_retirement_runtime import RetirementRuntime, validate_registry


class Owner:
    def __init__(self, root, *, disk=None):
        self.root = root
        self.condition = threading.Condition(threading.RLock())
        self.thread = threading.current_thread()
        self.closed, self.ready, self.error = False, True, None
        self.core_epoch = "epoch"
        self.clock_ms = lambda: 123000
        self.operations, self.queue, self.sessions, self.views = {}, deque(), {}, {}
        self.blocked_accounts = set()
        self._command_started = False
        self.on_incident = None
        self.writes = []
        if disk is None:
            account, rows = configuration()
            parameters = {"account": account, "strategies": rows, "start_ms": 0, "brackets": []}
            session = new(root)
            self.sessions[account["account_id"]] = session
            self.views[account["account_id"]] = {"account": "large prior view"}
            disk = {
                "schema_version": 1,
                "accounts": {
                    account["account_id"]: {
                        "parameters": parameters,
                        "creation_state": "created",
                        "feed_enabled": True,
                    }
                },
                "operations": {},
            }
        self.disk, self.data = deepcopy(disk), deepcopy(disk)
        self.operations = deepcopy(disk["operations"])

    def _guard(self):
        if self.closed:
            raise StorageError("closed owner")

    def _write(self, data):
        self._guard()
        self.writes.append(deepcopy(data))
        self.disk = deepcopy(data)
        self.data = deepcopy(data)

    def _read(self):
        return deepcopy(self.disk)


@pytest.fixture
def pair(tmp_path):
    owner = Owner(tmp_path)
    runtime = RetirementRuntime(owner)
    original = list(owner.sessions.values())
    yield owner, runtime
    for _, resource in runtime.resources():
        resource.close()
    for session in original:
        session.close()


def observe(owner, runtime, account_id="session-test"):
    results = []
    thread = threading.Thread(target=lambda: results.append(runtime.inspect(account_id)))
    thread.start()
    with owner.condition:
        assert owner.condition.wait_for(lambda: bool(runtime.queue), timeout=5)
    runtime.serve_one()
    thread.join(5)
    assert not thread.is_alive()
    return results[0]


def operation(owner, response, identity="retire"):
    parameters = {
        "account_id": response["account_id"],
        "expected_observation_sha256": response["observation_sha256"],
    }
    owner.operations[identity] = {
        "id": identity,
        "kind": "retire",
        "request_digest": retirement_sha256(("retire", parameters)),
        "core_epoch": owner.core_epoch,
        "state": "queued",
        "result": None,
        "error": None,
    }
    return parameters


def sources(root):
    return {
        path: path.read_bytes()
        for path in root.rglob("*.json")
        if "margin-retired" not in path.parts
    }


def test_schema1_stays_exact_and_observation_is_owner_only_io_free(pair, monkeypatch):
    owner, runtime = pair
    before, files = deepcopy(owner.data), sources(owner.root)
    monkeypatch.setattr(
        owner, "_guard", lambda: pytest.fail("inspection performed filesystem guard")
    )
    result = observe(owner, runtime)
    assert result["state"] == "ready" and result["core_epoch"] == "epoch"
    assert result["paused"] is True and result["source_pending"] is False
    assert result["source_record_count"] >= 1 and result["strategy_count"] == 2
    assert result["open_positions"] == result["pending_orders"] == result["staged"] == 0
    assert owner.data == before and owner.writes == [] and sources(owner.root) == files
    assert validate_registry(owner.data) is owner.data
    assert runtime.metadata() == [] and not runtime.gated("session-test")
    assert runtime.inspect("session-test")["reason_code"] == "owner_reentrant_read"


def test_actual_retirement_fence_before_archive_and_final_ack_frees_only_then(pair, monkeypatch):
    owner, runtime = pair
    response = observe(owner, runtime)
    parameters = operation(owner, response)
    files = sources(owner.root)
    original = owner.sessions["session-test"]
    prepare = MarginRetirement.prepare

    def prepared_after_fence(helper, plan):
        assert runtime.gated("session-test") and owner._command_started is True
        control = owner.disk["accounts"]["session-test"]["retirement_control"]
        assert control["plan"] == plan.document and control["parameters"] == parameters
        assert owner.disk["accounts"]["session-test"]["feed_enabled"] is False
        assert owner.disk["operations"]["retire"]["state"] == "running"
        assert len(owner.writes) == 1 and "session-test" in owner.sessions
        return prepare(helper, plan)

    monkeypatch.setattr(MarginRetirement, "prepare", prepared_after_fence)
    result = runtime.execute("retire", parameters)
    assert result["state"] == "completed" and result["result"]["retired"] is True
    assert len(owner.writes) == 2 and owner.data["schema_version"] == 2
    assert owner.data["accounts"] == {} and owner.sessions == {} and owner.views == {}
    assert confirmed_cleanup(original.cleanup_status())
    tombstone = runtime.retired("session-test")
    assert tombstone["receipt_sha256"] == result["result"]["receipt_sha256"]
    assert tombstone["feed_enabled_before"] is True
    assert set(runtime.metadata()[0]) == module._META
    assert runtime.helpers == {} and runtime._contexts == {} and runtime._observation is None
    assert [name for name, _ in runtime.resources()] == ["retired_history"]
    assert not runtime.gated("session-test") and sources(owner.root) == files
    assert validate_registry(owner.data) is owner.data
    with pytest.raises(ValidationError):
        runtime.execute("retire-again", parameters)


@pytest.mark.parametrize(
    "change", ["source_control", "expired", "different_account", "invalid_fields"]
)
def test_fresh_preflight_rejection_has_no_fence_close_or_durable_mutation(
    pair, monkeypatch, change
):
    owner, runtime = pair
    response = observe(owner, runtime)
    parameters = operation(owner, response)
    if change == "source_control":
        owner.sessions["session-test"].pause("new-source-tip")
    elif change == "expired":
        monkeypatch.setattr(module.time, "monotonic", lambda: runtime._observation[2] + 1)
    elif change == "different_account":
        parameters["account_id"] = "other"
    else:
        parameters["unrecognized"] = True
    with pytest.raises(ValidationError):
        runtime.execute("retire", parameters)
    assert owner._command_started is False and owner.writes == []
    assert not runtime.gated("session-test") and runtime.helpers == {}
    assert owner.sessions["session-test"].cleanup_status()["logical_closed"] is False


def test_first_registry_ack_loss_exact_target_continues_without_reclosing(pair, monkeypatch):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    write = owner._write

    def ack_loss(data):
        write(data)
        raise StorageError("ack lost after exact replace")

    monkeypatch.setattr(owner, "_write", ack_loss)
    assert runtime.execute("retire", parameters)["state"] == "completed"
    assert not runtime.registry_uncertain and len(owner.writes) == 2


def test_known_original_registry_requires_same_id_retry_frozen_time_and_rebases_other_account(
    pair, monkeypatch
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    write = owner._write
    monkeypatch.setattr(
        owner, "_write", lambda data: (_ for _ in ()).throw(StorageError("before replace"))
    )
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    context = runtime._contexts["session-test"]
    plan, session = deepcopy(context.descriptor), context.session
    assert runtime.gated("session-test") and not runtime.registry_uncertain
    assert not (owner.root / "state/margin-retired").exists()
    assert session.cleanup_status()["logical_closed"] is False
    assert runtime.retryable("retire", parameters)
    assert not runtime.retryable("another", parameters)
    changed = deepcopy(owner.data)
    slot = deepcopy(changed["accounts"]["session-test"])
    slot["parameters"]["account"]["account_id"] = "other"
    changed["accounts"]["other"] = slot
    write(changed)  # Another lineage's permitted registry write between explicit retries.
    owner.clock_ms = lambda: 999000
    monkeypatch.setattr(owner, "_write", write)
    assert runtime.execute("retire", parameters)["state"] == "completed"
    assert owner.data["accounts"]["other"] == slot
    assert runtime.retired("session-test")["request"]["issued_at_ms"] == 123000
    assert runtime.retired("session-test")["feed_enabled_before"] is True
    assert plan["request"]["issued_at_ms"] == 123000


@pytest.mark.parametrize("outcome", ["foreign", "unreadable"])
def test_unknown_registry_outcome_is_fatal_and_never_prepares_or_closes(pair, monkeypatch, outcome):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    if outcome == "foreign":

        def foreign(data):
            owner.disk = deepcopy(data)
            owner.disk["accounts"]["session-test"]["foreign"] = True
            raise StorageError("foreign registry")

        monkeypatch.setattr(owner, "_write", foreign)
    else:
        monkeypatch.setattr(
            owner, "_read", lambda: (_ for _ in ()).throw(StorageError("unreadable"))
        )
    with pytest.raises(StorageError):
        runtime.execute("retire", parameters)
    assert runtime.registry_uncertain and runtime.gated("session-test")
    assert not runtime.retryable("retire", parameters)
    assert owner.operations["retire"]["state"] == "unknown"
    assert owner.sessions["session-test"].cleanup_status()["logical_closed"] is False
    assert not (owner.root / "state/margin-retired").exists()


def test_native_close_after_release_error_stays_unknown_until_original_explicit_retry(
    pair, monkeypatch
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    session, calls = owner.sessions["session-test"], []
    close = session.close

    def after_release_once():
        calls.append(1)
        close()
        if len(calls) == 1:
            raise StorageError("first close acknowledgement lost")

    monkeypatch.setattr(session, "close", after_release_once)
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    assert confirmed_cleanup(session.cleanup_status()) and "session-test" in owner.data["accounts"]
    assert runtime._contexts["session-test"].session is session
    assert len(runtime.resources()) == 2 and runtime.metadata()[0]["state"] == "retiring"
    assert runtime.execute("retire", parameters)["state"] == "completed"
    assert len(calls) == 2


def test_closed_authority_ack_loss_reuses_exact_backend_candidate_without_reclosing(
    pair, monkeypatch
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    session, calls = owner.sessions["session-test"], []
    close, cas = session.close, MarginRetirement._cas
    failed = []

    def tracked_close():
        calls.append(1)
        close()

    def closed_failure(helper, path, scope, previous, candidate):
        if candidate.get("state") == "closed" and not failed:
            failed.append(deepcopy(candidate))
            raise StorageError("before closed receipt write")
        if candidate.get("state") == "closed":
            assert candidate == failed[0]
        return cas(helper, path, scope, previous, candidate)

    monkeypatch.setattr(session, "close", tracked_close)
    monkeypatch.setattr(MarginRetirement, "_cas", closed_failure)
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    assert runtime.execute("retire", parameters)["state"] == "completed"
    assert calls == [1]


def test_final_index_before_write_failure_keeps_active_slot_and_original_refs_until_retry(
    pair, monkeypatch
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    write, failed = owner._write, []
    session = owner.sessions["session-test"]

    def index_failure(data):
        if data.get("retired") and not failed:
            failed.append(deepcopy(data))
            raise StorageError("before final index write")
        return write(data)

    monkeypatch.setattr(owner, "_write", index_failure)
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    assert runtime.gated("session-test") and runtime.retired("session-test") is None
    assert owner.sessions["session-test"] is session and owner.views["session-test"]
    assert runtime._contexts["session-test"].plan is None
    assert runtime.metadata()[0]["receipt_sha256"] is not None
    monkeypatch.setattr(session, "close", lambda: pytest.fail("final-index retry reclosed Session"))
    assert runtime.execute("retire", parameters)["state"] == "completed"
    monkeypatch.undo()


@pytest.mark.parametrize("phase", ["fence", "prepared"])
def test_restart_recovers_before_session_open_with_original_epoch_and_lease_only_helper(
    pair, monkeypatch, phase
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    session = owner.sessions["session-test"]
    method = "prepare" if phase == "fence" else "close"
    actual = getattr(MarginRetirement, method)
    monkeypatch.setattr(
        MarginRetirement,
        method,
        lambda *a: (_ for _ in ()).throw(StorageError("process interruption")),
    )
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    disk = deepcopy(owner.disk)
    monkeypatch.setattr(MarginRetirement, method, actual)
    session.close()  # Explicitly release old native resources, simulating process exit.
    restarted = Owner(owner.root, disk=disk)
    restarted.ready, restarted.core_epoch = False, "next-epoch"
    next_runtime = RetirementRuntime(restarted)
    from omega.runtime import margin_session

    monkeypatch.setattr(
        margin_session.MarginTradingSession,
        "__init__",
        lambda *a, **k: pytest.fail("recovery opened Session"),
    )
    try:
        next_runtime.recover()
        assert restarted.data["accounts"] == {} and restarted.sessions == {}
        result = next_runtime.retired("session-test")
        assert result["request"]["core_epoch"] == "epoch"
        assert restarted.operations["retire"]["core_epoch"] == "epoch"
        assert restarted.operations["retire"]["state"] == "completed"
        helper = MarginRetirement(owner.root, "session-test")
        assert helper.read()["release_evidence"]["method"] == "recovery_exclusive_reacquire"
    finally:
        for _, resource in next_runtime.resources():
            resource.close()


def test_restart_validates_retired_pointer_and_missing_receipt_never_allows_id_reuse(pair):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    runtime.execute("retire", parameters)
    restarted = Owner(owner.root, disk=owner.disk)
    next_runtime = RetirementRuntime(restarted)
    try:
        next_runtime.recover()
        assert next_runtime.retired("session-test") is not None
        helper = MarginRetirement(owner.root, "session-test")
        helper.path.unlink()
        unknown = RetirementRuntime(restarted)
        try:
            unknown.recover()
            assert unknown.retired("session-test") is None and unknown.gated("session-test")
            assert unknown.metadata()[0]["state"] == "retiring"
            assert "session-test" in restarted.data["retired"]
        finally:
            unknown.history.close()
    finally:
        next_runtime.history.close()


def test_expired_inspection_performs_no_owner_callback_and_fail_wakes_tickets(pair, monkeypatch):
    owner, runtime = pair
    monkeypatch.setattr(
        owner.sessions["session-test"],
        "retirement_snapshot",
        lambda *a: pytest.fail("expired callback"),
    )
    results = []
    thread = threading.Thread(
        target=lambda: results.append(runtime.inspect("session-test", timeout=0.01))
    )
    thread.start()
    thread.join(5)
    assert results[0]["reason_code"] == "read_timeout" and not runtime.queue
    runtime.serve_one()
    thread = threading.Thread(target=lambda: results.append(runtime.inspect("session-test")))
    thread.start()
    with owner.condition:
        assert owner.condition.wait_for(lambda: bool(runtime.queue), timeout=5)
    runtime.fail("owner_closed")
    thread.join(5)
    assert results[-1]["reason_code"] == "owner_closed" and not runtime.waiters


def test_eight_inspection_slots_and_queued_control_blocks_observation(pair):
    owner, runtime = pair
    results, threads = [], []
    for _ in range(8):
        thread = threading.Thread(target=lambda: results.append(runtime.inspect("session-test")))
        thread.start()
        threads.append(thread)
    with owner.condition:
        assert owner.condition.wait_for(lambda: len(runtime.waiters) == 8, timeout=5)
    ninth = threading.Thread(target=lambda: results.append(runtime.inspect("session-test")))
    ninth.start()
    ninth.join(5)
    assert results == [runtime._unavailable("session-test", "read_queue_full")]
    runtime.fail("owner_closed")
    for thread in threads:
        thread.join(5)
    assert len(results) == 9 and not runtime.queue
    owner.queue.append(("pause", "pause", {"account_id": "session-test"}))
    assert observe(owner, runtime)["state"] == "unavailable"


@pytest.mark.parametrize(
    "change", ["schema", "bool_version", "extra", "accounts11", "operations129", "feed_bool"]
)
def test_registry_shapes_and_real_capacity_limits_are_not_monkeypatched(pair, change):
    owner, _ = pair
    data = deepcopy(owner.data)
    if change == "schema":
        data["schema_version"] = 3
    elif change == "bool_version":
        data["schema_version"] = True
    elif change == "extra":
        data["retired"] = {}
    elif change == "feed_bool":
        data.update(schema_version=2, retired={})
        data["accounts"]["session-test"]["feed_enabled"] = 1
    elif change == "accounts11":
        for index in range(10):
            slot = deepcopy(data["accounts"]["session-test"])
            slot["parameters"]["account"]["account_id"] = f"other{index}"
            data["accounts"][f"other{index}"] = slot
    else:
        data["operations"] = {str(index): {} for index in range(129)}
    with pytest.raises(ValidationError):
        validate_registry(data)


def test_same_id_wrong_parameters_and_other_retirement_never_replace_frozen_context(
    pair, monkeypatch
):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    monkeypatch.setattr(
        MarginRetirement,
        "prepare",
        lambda *a: (_ for _ in ()).throw(StorageError("archive unavailable")),
    )
    runtime.execute("retire", parameters)
    context = runtime._contexts["session-test"]
    with pytest.raises(ValidationError):
        runtime.execute("other", parameters)
    changed = {**parameters, "expected_observation_sha256": "0" * 64}
    assert not runtime.retryable("retire", changed)
    with pytest.raises(ValidationError):
        runtime.execute("retire", changed)
    assert runtime._contexts["session-test"] is context
    with pytest.raises(ProtocolError):
        runtime._fence_candidate(
            module._Context(
                context.account_id,
                "foreign",
                context.parameters,
                context.descriptor,
                context.helper,
                context.resource,
                context.row,
                context.session,
                context.plan,
            )
        )


def test_legacy_schema1_injected_configuration_is_unchanged_and_does_no_retirement_io(
    pair, monkeypatch
):
    owner, runtime = pair
    owner.data["accounts"]["session-test"]["parameters"] = {
        "account": {"account_id": "session-test"},
        "strategies": [],
    }
    before = deepcopy(owner.data)
    monkeypatch.setattr(
        module,
        "MarginRetirement",
        lambda *a: pytest.fail("legacy recovery opened retirement helper"),
    )
    assert validate_registry(owner.data) is owner.data
    runtime.recover()
    assert owner.data == before and owner.writes == [] and runtime.helpers == {}


def test_feed_false_provenance_and_metadata_generation_never_duplicates_final_account(
    pair, monkeypatch
):
    owner, runtime = pair
    owner.data["accounts"]["session-test"]["feed_enabled"] = False
    owner.disk = deepcopy(owner.data)
    parameters = operation(owner, observe(owner, runtime))
    commit = runtime._commit
    observed = []

    def metadata_between_commit_and_release(candidate):
        result = commit(candidate)
        if candidate.get("retired") and result:
            observed.extend(runtime.metadata())
        return result

    monkeypatch.setattr(runtime, "_commit", metadata_between_commit_and_release)
    assert runtime.execute("retire", parameters)["state"] == "completed"
    assert len(observed) == 1 and set(observed[0]) == module._META
    assert runtime.retired("session-test")["feed_enabled_before"] is False
    assert owner.writes[0]["accounts"]["session-test"]["retirement_feed_enabled_before"] is False


def test_retired_receipt_cannot_override_unconfirmed_original_native_session(pair, monkeypatch):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    write = owner._write

    def no_final_index(data):
        if data.get("retired"):
            raise StorageError("final index not acknowledged")
        write(data)

    monkeypatch.setattr(owner, "_write", no_final_index)
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    session = owner.sessions["session-test"]
    status = session.cleanup_status
    monkeypatch.setattr(owner, "_write", write)
    monkeypatch.setattr(session, "cleanup_status", lambda: {**status(), "state": "pending"})
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    assert owner.sessions["session-test"] is session and "session-test" in owner.data["accounts"]
    monkeypatch.setattr(session, "cleanup_status", status)
    assert runtime.execute("retire", parameters)["state"] == "completed"


def test_exact_128_tombstone_structure_and_129_rejection_without_capacity_patch(pair):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    runtime.execute("retire", parameters)
    data = deepcopy(owner.data)
    original = deepcopy(data["retired"]["session-test"])
    for index in range(127):
        account_id, operation_id = f"retired{index}", f"operation{index}"
        row = deepcopy(original)
        row.update(account_id=account_id, operation_id=operation_id)
        row["request"]["operation_id"] = operation_id
        data["retired"][account_id] = row
        data["operations"][operation_id] = {
            "id": operation_id,
            "kind": "retire",
            "request_digest": retirement_sha256(
                (
                    "retire",
                    {
                        "account_id": account_id,
                        "expected_observation_sha256": row["observation_sha256"],
                    },
                )
            ),
            "core_epoch": row["request"]["core_epoch"],
            "state": "completed",
            "error": None,
            "result": {
                "account_id": account_id,
                "retired": True,
                "receipt_sha256": row["receipt_sha256"],
            },
        }
    # These are scalar registry shape fixtures, not128 actual authority receipts.
    assert len(data["retired"]) == len(data["operations"]) == 128
    assert validate_registry(data) is data
    data["retired"]["overflow"] = deepcopy(original)
    with pytest.raises(ValidationError):
        validate_registry(data)


def test_recovery_rejects_missing_original_native_feed_provenance(pair, monkeypatch):
    owner, runtime = pair
    parameters = operation(owner, observe(owner, runtime))
    monkeypatch.setattr(
        MarginRetirement,
        "prepare",
        lambda *a: (_ for _ in ()).throw(StorageError("archive unavailable")),
    )
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    data = deepcopy(owner.disk)
    del data["accounts"]["session-test"]["retirement_feed_enabled_before"]
    with pytest.raises(ValidationError):
        validate_registry(data)


@pytest.mark.parametrize("fault", ["native_close", "first_registry"])
def test_unknown_inspection_preserves_original_observation_for_strict_wire_retry(
    pair, monkeypatch, fault
):
    owner, runtime = pair
    original_view = observe(owner, runtime)
    parameters = operation(owner, original_view)
    files = sources(owner.root)
    session = owner.sessions["session-test"]
    if fault == "native_close":
        close = session.close

        def fail_after_close():
            close()
            raise StorageError("after native release")

        monkeypatch.setattr(session, "close", fail_after_close)
    else:
        write = owner._write
        monkeypatch.setattr(
            owner, "_write", lambda data: (_ for _ in ()).throw(StorageError("before fence write"))
        )
    assert runtime.execute("retire", parameters)["state"] == "unknown"
    plan = deepcopy(runtime._contexts["session-test"].descriptor)
    results = []
    thread = threading.Thread(target=lambda: results.append(runtime.inspect("session-test")))
    thread.start()
    thread.join(5)
    assert not thread.is_alive()
    view = results[0]
    assert view["state"] == "retiring" and view["operation_id"] == "retire"
    for name in (
        "observation_sha256",
        "registered_parameters_sha256",
        "session_config_hash",
        "ledger_namespace",
    ):
        assert view[name] == original_view[name]
    wire = retirement_body(
        view["operation_id"], view["account_id"], view["observation_sha256"], view["core_epoch"]
    )
    assert wire["parameters"] == parameters and runtime.retryable(wire["id"], wire["parameters"])
    # The durable-control fallback carries the same proof even when no context
    # exists yet during startup's cache projection.
    if fault == "native_close":
        context = runtime._contexts.pop("session-test")
        try:
            assert (
                runtime._cached_response("session-test")["observation_sha256"]
                == original_view["observation_sha256"]
            )
        finally:
            runtime._contexts["session-test"] = context
        monkeypatch.setattr(session, "close", close)
    else:
        monkeypatch.setattr(owner, "_write", write)
    owner.clock_ms = lambda: pytest.fail("explicit retry requested a new issue time")
    monkeypatch.setattr(
        MarginRetirement,
        "make_plan",
        lambda *a: pytest.fail("explicit retry constructed a new plan"),
    )
    assert runtime.execute(wire["id"], wire["parameters"])["state"] == "completed"
    assert runtime.retired("session-test")["plan_sha256"] == plan["sha256"]
    assert sources(owner.root) == files
