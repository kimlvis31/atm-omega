"""Persisted CAC/CA references, optimistic edits and immutable strategy snapshots."""

import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from omega.domain.errors import ValidationError
from omega.runtime.analysis_library import AnalysisLibrary


def configuration(period=20):
    return {
        "moving_averages": [
            {"interval_id": 0, "ema_period": period, "mmacd_periods": [12, 26], "signal_period": 9}
        ],
        "momentum": [{"interval_id": 2, "analysis_code": "MFI", "nSamples": 14}],
    }


@pytest.fixture
def library(tmp_path):
    return AnalysisLibrary(tmp_path / "analysis.json", references_provider=lambda: [])


def apply(library, kind, *, operation=None, **payload):
    revision = library.snapshot()["revision"]
    return library.command(
        kind,
        {"expected_revision": revision, **payload},
        operation_id=operation or f"operation-{revision}",
    )["item"]


def create(library, name="Shared CAC"):
    return apply(library, "analysis.template.create", name=name, configuration=configuration())


def instance(library, template, name="BTC CA"):
    return apply(
        library,
        "analysis.instance.create",
        name=name,
        instrument="binance:usdm:BTCUSDT",
        template_id=template["id"],
    )


def test_template_clone_rename_and_duplicate_names_use_stable_ids(library):
    template = create(library)
    cloned = apply(library, "analysis.template.clone", id=template["id"], name="Copy")
    assert cloned["configuration"] == template["configuration"]
    assert cloned["id"] != template["id"] and cloned["revision"] == 1
    renamed = apply(library, "analysis.template.rename", id=template["id"], name="Renamed")
    assert renamed["id"] == template["id"] and renamed["revision"] == 2
    with pytest.raises(ValidationError, match="already exists"):
        create(library, "copy")
    apply(library, "analysis.template.delete", id=template["id"])
    reused = create(library, "Renamed")
    assert reused["id"] != template["id"]


def test_template_ca_and_strategy_snapshots_never_follow_future_edits(library):
    template = create(library)
    ca = instance(library, template)
    frozen = library.snapshot_for_strategy(ca["id"], ca["instrument"], expected_revision=1)
    apply(library, "analysis.template.update", id=template["id"], configuration=configuration(50))
    assert library.snapshot()["instances"][0]["configuration"] == ca["configuration"]
    rebased = apply(library, "analysis.instance.rebase", id=ca["id"], template_id=template["id"])
    assert rebased["configuration"]["moving_averages"][0]["ema_period"] == 50
    assert frozen["configuration"]["moving_averages"][0]["ema_period"] == 20
    assert frozen["template_revision"] == 1 and rebased["template_revision"] == 2
    later = library.snapshot_for_strategy(ca["id"], ca["instrument"])
    assert frozen["configuration_hash"] != later["configuration_hash"]
    assert ca["status"] == "configured"
    assert library.capabilities()["execution_subscription"] is False


def test_reference_protection_counts_strategy_positions_not_only_accounts(library):
    template = create(library)
    ca = instance(library, template)
    references = [
        {"analysis_id": ca["id"], "strategy_id": "account:position-one"},
        {"analysis_id": ca["id"], "strategy_id": "account:position-two"},
    ]
    library.references_provider = lambda: references
    with pytest.raises(ValidationError, match="referenced by a CA"):
        apply(library, "analysis.template.delete", id=template["id"])
    with pytest.raises(ValidationError, match="position-one"):
        apply(library, "analysis.instance.delete", id=ca["id"])
    references.pop(0)
    with pytest.raises(ValidationError, match="position-two"):
        apply(library, "analysis.instance.delete", id=ca["id"])
    references.clear()
    apply(library, "analysis.instance.delete", id=ca["id"])
    apply(library, "analysis.template.delete", id=template["id"])


@pytest.mark.parametrize("provider", [None, lambda: None, lambda: [{"account_id": "only-account"}]])
def test_missing_reference_authority_blocks_ca_delete(library, provider):
    ca = instance(library, create(library))
    library.references_provider = provider
    with pytest.raises(ValidationError, match="authority"):
        apply(library, "analysis.instance.delete", id=ca["id"])


def test_operation_receipt_is_atomic_and_idempotent_across_restart(library):
    payload = {"expected_revision": 0, "name": "One", "configuration": configuration()}
    first = library.command("analysis.template.create", payload, operation_id="same-id")
    payload_copy = deepcopy(payload)
    assert (
        library.command("analysis.template.create", payload_copy, operation_id="same-id") == first
    )
    reopened = AnalysisLibrary(library.path)
    assert (
        reopened.command("analysis.template.create", payload_copy, operation_id="same-id") == first
    )
    assert len(reopened.snapshot()["templates"]) == 1
    with pytest.raises(ValidationError, match="reused"):
        reopened.command(
            "analysis.template.create", payload | {"name": "Other"}, operation_id="same-id"
        )


def test_write_failure_preserves_previous_file_and_blocks_followup_mutation(library, monkeypatch):
    import omega.runtime.analysis_library as module

    create(library)
    old, old_snapshot = library.path.read_bytes(), library.snapshot()

    def unavailable(*_):
        raise OSError("test storage unavailable")

    monkeypatch.setattr(module, "atomic_json", unavailable)
    with pytest.raises(OSError):
        create(library, "Second")
    assert library.path.read_bytes() == old
    assert library.snapshot()["revision"] == old_snapshot["revision"]
    assert library.snapshot()["persistence"] == "degraded"
    with pytest.raises(OSError, match="reconcile"):
        create(library, "Third")


def test_parallel_same_revision_allows_only_one_writer(library):
    def attempt(index):
        try:
            library.command(
                "analysis.template.create",
                {"expected_revision": 0, "name": f"T{index}", "configuration": configuration()},
                operation_id=f"op-{index}",
            )
            return True
        except ValidationError:
            return False

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(attempt, range(4)))
    assert results.count(True) == 1
    assert library.snapshot()["revision"] == 1


@pytest.mark.parametrize(
    "config",
    [
        {},
        {"PSAR": {}},
        {"moving_averages": [{"interval_id": 13, "ema_period": 20}]},
        {"momentum": [{"interval_id": 0, "analysis_code": "MST"}]},
        {"moving_averages": [{"interval_id": 0, "ema_period": True}]},
        {
            "moving_averages": [
                {"interval_id": 0, "ema_period": 20},
                {"interval_id": 0, "ema_period": 30},
            ]
        },
    ],
)
def test_unsupported_or_ambiguous_analysis_never_persists(library, config):
    with pytest.raises(ValidationError):
        apply(library, "analysis.template.create", name="Invalid", configuration=config)
    assert library.snapshot()["revision"] == 0
    assert not library.path.exists()


def test_snapshot_guards_instrument_revision_and_deep_copy(library):
    ca = instance(library, create(library))
    with pytest.raises(ValidationError, match="instruments differ"):
        library.snapshot_for_strategy(ca["id"], "binance:spot:BTCUSDT")
    with pytest.raises(ValidationError, match="revision conflict"):
        library.snapshot_for_strategy(ca["id"], ca["instrument"], expected_revision=True)
    snap = library.snapshot_for_strategy(ca["id"], ca["instrument"])
    snap["configuration"]["moving_averages"][0]["ema_period"] = 99
    assert (
        library.snapshot()["instances"][0]["configuration"]["moving_averages"][0]["ema_period"]
        == 20
    )


def test_corrupt_file_and_future_schema_are_rejected(library):
    create(library)
    candidate = json.loads(library.path.read_text(encoding="utf-8"))
    candidate["schema_version"] = 99
    library.path.write_text(json.dumps(candidate), encoding="utf-8")
    with pytest.raises(ValidationError, match="schema/version"):
        AnalysisLibrary(library.path)


def test_receipt_capacity_is_explicit_not_silent_idempotence_eviction(library, monkeypatch):
    import omega.runtime.analysis_library as module

    monkeypatch.setattr(module, "MAX_OPERATIONS", 1)
    template = create(library)
    with pytest.raises(ValidationError, match="capacity"):
        apply(library, "analysis.template.rename", id=template["id"], name="Other")


def core_command(core, identity, kind, payload):
    value = core.request(
        "POST", "/api/commands", core.token, {"id": identity, "kind": kind, "payload": payload}
    )
    assert value["status"] == "completed", value
    return value["result"]


def swing_ca(core, prefix="swing"):
    template = core_command(
        core,
        prefix + "-template",
        "analysis.template.create",
        {
            "expected_revision": 0,
            "name": "SWING CAC",
            "configuration": {
                "swing": [{"interval_id": 0, "line_index": 0, "swing_range": "0.005"}]
            },
        },
    )["item"]
    ca = core_command(
        core,
        prefix + "-instance",
        "analysis.instance.create",
        {
            "expected_revision": 1,
            "name": "BTC SWING CA",
            "instrument": "binance:usdm:BTCUSDT",
            "template_id": template["id"],
        },
    )["item"]
    core_command(core, prefix + "-account", "paper.account.create", {"account_id": "a"})
    core_command(
        core,
        prefix + "-strategy",
        "paper.strategy.add",
        {
            "instance": "s",
            "account_id": "a",
            "instrument": "binance:usdm:BTCUSDT",
            "module_id": "tef",
            "configuration": {"model": "SPDDEFAULT"},
            "analysis_id": ca["id"],
            "analysis_revision": ca["revision"],
        },
    )
    return template, ca


def test_swing_template_ca_binding_through_actual_core_contract_and_real_producer(tmp_path):
    from omega.runtime.app import build_core

    core = build_core(tmp_path)
    try:
        _template, ca = swing_ca(core)
        detail = core.request("GET", "/api/strategies/s", core.token)
        assert detail["analysis_snapshot"]["analysis_id"] == ca["id"]
        assert detail["analysis"]["swing"][0]["swing_range"] == 0.005
        assert (
            "SWING"
            in core.request("GET", "/api/analysis-library", core.token)["capabilities"]["features"]
        )
        for index in range(2):
            core_command(
                core,
                f"step-{index}",
                "paper.step",
                {
                    "instrument": ca["instrument"],
                    "candle": {
                        "open_time_ms": index * 60000,
                        "open": "105",
                        "high": "110",
                        "low": "100",
                        "close": "105",
                        "volume": "1",
                    },
                },
            )
        assert core.paper.engines["s"].module.model.state["la_swing0_lsType_prev"] == -1
        assert core.paper.detail("s")["status"] == "ready"
    finally:
        core.close()


def test_swing_bound_snapshot_and_delete_guard_remain_stable_after_template_update(tmp_path):
    from omega.runtime.app import build_core

    core = build_core(tmp_path)
    try:
        template, ca = swing_ca(core)
        frozen = core.paper.detail("s")["analysis_snapshot"]
        core_command(
            core,
            "edit-swing",
            "analysis.template.update",
            {
                "expected_revision": 2,
                "id": template["id"],
                "configuration": {
                    "swing": [{"interval_id": 0, "line_index": 0, "swing_range": "0.01"}]
                },
            },
        )
        assert core.paper.detail("s")["analysis_snapshot"] == frozen
        assert (
            core.analysis_library.snapshot_for_strategy(ca["id"], ca["instrument"])[
                "configuration"
            ]["swing"][0]["swing_range"]
            == 0.005
        )
        with pytest.raises(ValidationError, match="referenced"):
            core.analysis_library.command(
                "analysis.instance.delete",
                {
                    "id": ca["id"],
                    "expected_revision": 3,
                },
                operation_id="delete-bound",
            )
    finally:
        core.close()
