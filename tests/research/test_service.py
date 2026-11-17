import json
import threading
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from omega.research.service import ResearchService


def completed(service, job, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = service.status(job["id"])
        if result["state"] in {"complete", "failed", "cancelled", "interrupted"}:
            return result
        time.sleep(0.05)
    raise AssertionError(f"research worker did not finish: {result}")


def test_process_pipeline_export_mc_train_and_simulation(source_file, tmp_path):
    service = ResearchService(tmp_path / "research")
    try:
        exported = completed(service, service.submit("export", {"source_file": str(source_file)}))
        assert exported["state"] == "complete", exported
        dataset = str(Path(exported["result_file"]).parent)
        generated = completed(
            service,
            service.submit("monte_carlo", {"dataset": dataset, "block_bars": 2, "seed": 42}),
        )
        assert generated["state"] == "complete", generated
        trained = completed(
            service,
            service.submit(
                "train",
                {
                    "dataset": str(Path(generated["result_file"]).parent),
                    "demo": True,
                    "config": {"epochs": 2},
                },
            ),
        )
        assert trained["state"] == "complete", trained
        assert trained["result"]["module_id"] == "omega.demo.linear_neuron"
        simulated = completed(
            service, service.submit("simulate", {"dataset": dataset, "spec": {"backend": "gpu"}})
        )
        assert simulated["state"] == "complete", simulated
        assert simulated["result"]["backend"]["selected"] == "cpu"
        assert len(service.list_jobs()) == 4
        assert not list((tmp_path / "research/artifacts").glob(".partial-*"))
    finally:
        service.close()


def test_failed_job_never_publishes_artifact_and_next_job_works(tmp_path):
    service = ResearchService(tmp_path / "research")
    try:
        failed = completed(
            service, service.submit("export", {"source_file": str(tmp_path / "missing.csv")})
        )
        assert failed["state"] == "failed"
        assert "artifact" not in failed
        assert not (tmp_path / "research/artifacts" / failed["id"]).exists()
        good = completed(service, service.submit("nn_inspect", {"demo": True}))
        assert good["state"] == "complete"
    finally:
        service.close()


def test_cancel_uncooperative_plugin_and_worker_timeout(tmp_path):
    plugin = tmp_path / "slow.py"
    plugin.write_text("import time\ntime.sleep(60)\n")
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit("nn_inspect", {"module": str(plugin)})
        deadline = time.monotonic() + 5
        while service.status(job["id"])["state"] == "queued" and time.monotonic() < deadline:
            time.sleep(0.01)
        service.cancel(job["id"])
        result = completed(service, job)
        assert result["state"] == "cancelled", result
        assert "artifact" not in result
        expired = completed(
            service, service.submit("nn_inspect", {"module": str(plugin), "timeout_seconds": 0.1})
        )
        assert expired["state"] == "failed"
        assert "deadline" in expired["error"]
    finally:
        service.close()


def test_bad_requests_and_closed_service(tmp_path):
    service = ResearchService(tmp_path / "research")
    try:
        for kind, parameters in (
            ("live_order", {}),
            ("train", {"timeout_seconds": -1}),
            ("train", {"value": float("nan")}),
        ):
            with pytest.raises(ValueError):
                service.submit(kind, parameters)
        with pytest.raises(ValueError):
            service.status("../escape")
    finally:
        service.close()
    with pytest.raises(RuntimeError):
        service.submit("nn_inspect", {"demo": True})


def test_mc_and_training_are_one_gui_independent_job(dataset, tmp_path):
    from omega.research.artifacts import read_json

    service = ResearchService(tmp_path / "research")
    try:
        result = completed(
            service,
            service.submit(
                "train",
                {
                    "dataset": str(dataset),
                    "demo": True,
                    "config": {"epochs": 1},
                    "monte_carlo": {"enabled": True, "block_bars": 2, "seed": 42},
                },
            ),
        )
        assert result["state"] == "complete", result
        artifact = Path(result["artifact"])
        assert (artifact / "generated_dataset/source.original.csv").is_file()
        assert read_json(artifact / "training/run.json")["dataset"]["transform"]["seed"] == 42
    finally:
        service.close()


def test_restart_distinguishes_final_artifact_from_interrupted_partial(tmp_path):
    from omega.research.artifacts import content_hash, write_json

    root = tmp_path / "research"
    (root / "jobs").mkdir(parents=True)
    complete_id, interrupted_id = "a" * 32, "b" * 32
    for job_id in (complete_id, interrupted_id):
        write_json(
            root / "jobs" / f"{job_id}.json",
            {
                "id": job_id,
                "state": "running",
                "kind": "nn_inspect",
                "created_at_ms": 1,
            },
        )
    final = root / "artifacts" / complete_id
    final.mkdir(parents=True)
    write_json(final / "module.json", {"module_id": "synthetic.fixture"})
    write_json(
        final / "artifact.json",
        {
            "job_id": complete_id,
            "state": "complete",
            "result_file": "module.json",
            "summary": {"module_id": "synthetic.fixture"},
            "finished_at_ms": 2,
            "files": [{"file": "module.json", "sha256": content_hash(final / "module.json")}],
        },
    )
    service = ResearchService(root)
    try:
        assert service.status(complete_id)["state"] == "complete"
        interrupted = service.status(interrupted_id)
        assert interrupted["state"] == "interrupted"
        assert "artifact" not in interrupted
    finally:
        service.close()


def test_simulation_uses_registry_ts_not_only_research_demo(dataset, tmp_path):
    service = ResearchService(tmp_path / "research")
    try:
        result = completed(
            service,
            service.submit(
                "simulate",
                {
                    "dataset": str(dataset),
                    "ts_module_id": "target_quantity",
                    "strategy_config": {"quantity": "2"},
                    "spec": {"fee_rate": "0"},
                },
            ),
        )
        assert result["state"] == "complete", result
        assert result["result"]["fills"] == 2
    finally:
        service.close()


def test_corrupt_offline_job_does_not_block_core_research_start(tmp_path):
    root = tmp_path / "research"
    jobs = root / "jobs"
    jobs.mkdir(parents=True)
    job_id = "c" * 32
    (jobs / f"{job_id}.json").write_text("{partial json")
    service = ResearchService(root)
    try:
        assert service.status(job_id)["state"] == "failed"
        assert list(jobs.glob("*.corrupt"))
        assert (
            completed(service, service.submit("nn_inspect", {"demo": True}))["state"] == "complete"
        )
    finally:
        service.close()


def test_storage_transition_failure_is_explicit_and_rejects_new_work(tmp_path, monkeypatch):
    import omega.research.service as service_module

    original = service_module.write_json

    def failing_transition(path, data):
        if data.get("state") == "running":
            raise OSError("synthetic disk unavailable")
        return original(path, data)

    monkeypatch.setattr(service_module, "write_json", failing_transition)
    service = ResearchService(tmp_path / "research")
    try:
        job = service.submit("nn_inspect", {"demo": True})
        result = completed(service, job)
        assert result["state"] == "failed"
        assert result["persistence"] == "memory_only"
        assert service.health()["state"] == "degraded"
        with pytest.raises(RuntimeError, match="unavailable"):
            service.submit("nn_inspect", {"demo": True})
        assert service.list_job_summaries()[0]["state"] == "failed"
    finally:
        service.close()


def test_job_summaries_read_bounded_bodies_and_omit_parameters(tmp_path, monkeypatch):
    import omega.research.service as service_module
    from omega.research.artifacts import write_json

    service = ResearchService(tmp_path / "research")
    try:
        for index in range(5):
            job_id = f"{index:032x}"
            write_json(
                service.root / "jobs" / f"{job_id}.json",
                {
                    "id": job_id,
                    "state": "complete",
                    "kind": "train",
                    "created_at_ms": index,
                    "parameters": {"heavy": "x" * 10000},
                    "error": "e" * 1000,
                },
            )
        original = service_module.read_status_json
        reads = []

        def counted(path):
            reads.append(path)
            return original(path)

        monkeypatch.setattr(service_module, "read_status_json", counted)
        summaries = service.list_job_summaries(limit=2)
        assert len(summaries) == len(reads) == 2
        assert all("parameters" not in item and len(item["error"]) == 512 for item in summaries)
    finally:
        service.close()


def test_interpolated_export_and_noncausal_replay_opt_in(gapped_source, tmp_path):
    service = ResearchService(tmp_path / "research")
    try:
        exported = completed(
            service,
            service.submit(
                "export",
                {
                    "source_file": str(gapped_source),
                    "interpolation": {"enabled": True},
                },
            ),
        )
        assert exported["state"] == "complete", exported
        assert exported["result"]["interpolation"]["rows"] == 1
        assert Path(exported["result"]["original_dataset_file"]).is_file()
        dataset_path = str(Path(exported["result_file"]).parent)
        denied = completed(service, service.submit("simulate", {"dataset": dataset_path}))
        assert denied["state"] == "failed"
        assert "noncausal" in denied["error"]
        allowed = completed(
            service,
            service.submit(
                "simulate",
                {
                    "dataset": dataset_path,
                    "allow_noncausal_research_input": True,
                },
            ),
        )
        assert allowed["state"] == "complete", allowed
        trained = completed(
            service,
            service.submit(
                "train",
                {
                    "dataset": dataset_path,
                    "demo": True,
                    "config": {"epochs": 1},
                    "monte_carlo": {"enabled": True, "seed": 42, "block_bars": 2},
                },
            ),
        )
        assert trained["state"] == "complete", trained
    finally:
        service.close()


class UnconfirmedProcess:
    """Synthetic handle only; never invokes any OS process operation."""

    pid = 12345
    exitcode = None

    def __init__(self, *, raise_on_stop=False):
        self.alive, self.closed = True, False
        self.raise_on_stop = raise_on_stop
        self.calls = []

    def start(self):
        self.calls.append("start")

    def is_alive(self):
        assert not self.closed
        return self.alive

    def join(self, timeout):
        self.calls.append(("join", timeout))

    def terminate(self):
        self.calls.append("terminate")
        if self.raise_on_stop:
            raise OSError("synthetic terminate failure")

    def kill(self):
        self.calls.append("kill")
        if self.raise_on_stop:
            raise OSError("synthetic kill failure")

    def close(self):
        assert not self.alive
        self.closed = True


def unconfirmed_job(tmp_path, monkeypatch, *, raise_on_stop=False, marker_failure=False):
    from omega.research import service as module

    service = ResearchService(tmp_path / "research")
    process, spawned, tick = UnconfirmedProcess(raise_on_stop=raise_on_stop), [], [0]

    def monotonic():
        tick[0] += 3
        return tick[0]

    def spawn(**kwargs):
        spawned.append(kwargs)
        return process

    monkeypatch.setattr(module, "time", SimpleNamespace(time=time.time, monotonic=monotonic))
    if marker_failure:
        write = module.write_json

        def failing(path, data):
            if path.name == "research-unconfirmed.json":
                raise OSError("synthetic marker disk failure")
            return write(path, data)

        monkeypatch.setattr(module, "write_json", failing)
    with service._condition:
        service._context = SimpleNamespace(Event=threading.Event, Process=spawn)
        active = service.submit("nn_inspect", {"demo": True, "timeout_seconds": 0.01})
        pending = service.submit("nn_inspect", {"demo": True})
    service._thread.join(2)
    assert not service._thread.is_alive()
    return service, process, spawned, active, pending


@pytest.mark.parametrize("raise_on_stop", [False, True])
def test_unconfirmed_worker_is_quarantined_without_false_terminal_state_or_new_dispatch(
    tmp_path, monkeypatch, raise_on_stop
):
    service, process, spawned, active, pending = unconfirmed_job(
        tmp_path, monkeypatch, raise_on_stop=raise_on_stop
    )
    try:
        assert len(spawned) == 1 and "kill" in process.calls
        assert process.alive and service._unconfirmed_process is process
        assert service._active[0] == active["id"]
        assert service.health()["state"] == "degraded"
        assert service.health()["quarantined"] is True
        status = service.status(active["id"])
        assert status["state"] == status["computation_state"] == "unknown"
        assert not {"artifact", "result", "result_file"} & status.keys()
        assert json.loads(service._job_path(active["id"]).read_text())["state"] == "running"
        assert service.status(pending["id"])["state"] == "queued"
        assert service.status(pending["id"])["dispatch_state"] == "blocked"
        assert "RESEARCH_QUARANTINE" in service.status(pending["id"])["error"]
        for _ in range(2):
            with pytest.raises(RuntimeError, match="exit remains unconfirmed"):
                service.close()
        with pytest.raises(RuntimeError, match="closed|unavailable"):
            service.submit("nn_inspect", {"demo": True})
        marker = json.loads(service._quarantine_path.read_text())
        assert marker["job_id"] == active["id"] and marker["pid"] == process.pid
        assert marker["reason"] == "WORKER_EXIT_UNCONFIRMED"
    finally:
        process.alive = False
        service.close()
    assert process.closed and service._quarantine_path.exists()


def test_quarantine_restart_masks_late_complete_and_does_not_reconcile_any_files(
    tmp_path, monkeypatch
):
    from omega.research import service as module
    from omega.research.artifacts import write_json

    old, process, _, active, _ = unconfirmed_job(tmp_path, monkeypatch)
    new = None
    try:
        # A surviving former worker could publish this after its parent's timeout.
        write_json(
            old._job_path(active["id"]),
            {**active, "state": "complete", "result_file": "untrusted-late-result.json"},
        )
        partial = old.root / "artifacts" / (".partial-" + active["id"])
        partial.mkdir()
        (partial / "untouched.txt").write_text("worker-owned partial")
        expected = {
            path.relative_to(old.root): path.read_bytes()
            for path in old.root.rglob("*")
            if path.is_file()
        }

        def forbidden(*_args):
            raise AssertionError("quarantined files must never be reconciled")

        monkeypatch.setattr(module, "_recover_final", forbidden)
        new = ResearchService(old.root)
        assert new.health()["state"] == "degraded" and new.health()["quarantined"]
        assert new._unconfirmed_process is None  # A stored PID is never adopted.
        assert new.status(active["id"])["state"] == "unknown"
        assert "result_file" not in new.status(active["id"])
        assert (
            next(row for row in new.list_jobs() if row["id"] == active["id"])["state"] == "unknown"
        )
        with pytest.raises(RuntimeError, match="unavailable"):
            new.submit("nn_inspect", {"demo": True})
        for relative, content in expected.items():
            assert (new.root / relative).read_bytes() == content
    finally:
        if new:
            new.close()
        process.alive = False
        old.close()
    assert old._quarantine_path.exists()


@pytest.mark.parametrize(
    "marker",
    [
        b"{",
        b"x" * 4097,
        b"[]",
        b"\xff",
        b'{"schema_version":true}',
        json.dumps(
            {
                "schema_version": 1,
                "job_id": "invalid",
                "pid": 1,
                "reason": "WORKER_EXIT_UNCONFIRMED",
                "recorded_at_ms": 1,
            }
        ).encode(),
    ],
)
def test_malformed_quarantine_degrades_research_without_rewriting_job_files(tmp_path, marker):
    root = tmp_path / "research"
    path = root / "state/research-unconfirmed.json"
    path.parent.mkdir(parents=True)
    path.write_bytes(marker)
    jobs = root / "jobs"
    jobs.mkdir()
    identity = "a" * 32
    job_file = jobs / f"{identity}.json"
    original = json.dumps(
        {"id": identity, "state": "running", "kind": "train", "created_at_ms": 1}
    ).encode()
    job_file.write_bytes(original)
    service = ResearchService(root)
    try:
        assert service.health()["state"] == "degraded"
        assert service.status(identity)["state"] == "unknown"
        assert job_file.read_bytes() == original
        assert path.read_bytes() == marker
        assert not list(jobs.glob("*.corrupt"))
        with pytest.raises(RuntimeError, match="unavailable"):
            service.submit("nn_inspect", {"demo": True})
    finally:
        service.close()


def test_unconfirmed_worker_marker_failure_keeps_memory_gate_and_close_failure(
    tmp_path, monkeypatch
):
    service, process, _, active, _ = unconfirmed_job(tmp_path, monkeypatch, marker_failure=True)
    try:
        assert service.status(active["id"])["state"] == "unknown"
        assert "quarantine persistence unavailable" in service.health()["error"]
        assert not service._quarantine_path.exists()
        with pytest.raises(RuntimeError, match="unconfirmed"):
            service.close()
    finally:
        process.alive = False
        service.close()


def test_supervisor_join_deadline_records_quarantine_before_returning_failure(tmp_path):
    from omega.research.artifacts import write_json

    service = ResearchService(tmp_path / "research")
    service.close()  # The real idle supervisor is gone; replace only its test handle.
    process = UnconfirmedProcess()
    identity = "b" * 32
    service._closed = False
    service._active = identity, threading.Event()
    service._running_process = identity, process
    alive = [True]
    service._thread = SimpleNamespace(join=lambda _timeout: None, is_alive=lambda: alive[0])
    write_json(service._job_path(identity), {"id": identity, "state": "running"})
    try:
        for _ in range(2):
            with pytest.raises(RuntimeError, match="shutdown deadline"):
                service.close()
        assert service._quarantine_path.exists()
        assert service._unconfirmed_process is process
        assert service.status(identity)["state"] == "unknown"
    finally:
        alive[0] = process.alive = False
        service.close()


@pytest.mark.parametrize("contents", [b"[]", b"null", b"{partial"])
def test_quarantine_preserves_unrelated_corrupt_job_and_returns_bounded_summaries(
    tmp_path, monkeypatch, contents
):
    old, process, _, active, _ = unconfirmed_job(tmp_path, monkeypatch)
    new = None
    identity = "c" * 32
    path = old._job_path(identity)
    path.write_bytes(contents)
    try:
        new = ResearchService(old.root)
        assert new.status(identity)["state"] == "unknown"
        assert "unreadable job metadata" in new.status(identity)["error"]
        summaries = new.list_job_summaries()
        assert next(row for row in summaries if row["id"] == identity)["state"] == "unknown"
        assert (
            next(row for row in summaries if row["id"] == active["id"])["computation_state"]
            == "unknown"
        )
        assert len(new.list_jobs()) == 3
        assert path.read_bytes() == contents
        assert not list(path.parent.glob("*.corrupt"))
    finally:
        if new:
            new.close()
        process.alive = False
        old.close()


@pytest.mark.parametrize("retirement", ["close", "quarantine"])
def test_dispatch_observes_retirement_before_start_without_spawning(tmp_path, retirement):
    service = ResearchService(tmp_path / "research")
    process = UnconfirmedProcess()
    process.alive, process.pid = False, None

    def construct(**_kwargs):
        if retirement == "close":
            service._closed = True
        else:
            service._quarantine_worker(service._active[0], process)
        return process

    try:
        with service._condition:
            service._context = SimpleNamespace(Event=threading.Event, Process=construct)
            job = service.submit("nn_inspect", {"demo": True})
        service._thread.join(2)
        assert not service._thread.is_alive()
        assert "start" not in process.calls
        assert service.status(job["id"])["state"] == (
            "cancelled" if retirement == "close" else "unknown"
        )
        if retirement == "quarantine":
            assert json.loads(service._quarantine_path.read_text())["pid"] is None
    finally:
        service.close()
