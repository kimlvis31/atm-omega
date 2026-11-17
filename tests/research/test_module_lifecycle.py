"""External module registrations live through use, then only their owner releases."""

import os
import sys
import threading
from pathlib import Path
from types import ModuleType
from uuid import uuid4

import pytest

from omega.research import demo_instance, neural
from omega.research import service as research_service
from omega.research.artifacts import content_hash, read_json, write_json
from omega.research.prediction import PredictionSession, predict_run


def external_modules():
    return {
        name: module
        for name, module in sys.modules.copy().items()
        if name.startswith("omega_external_")
    }


def test_training_releases_its_own_external_module_after_completed_close(dataset, tmp_path):
    before = external_modules()
    neural.train(
        dataset, Path(demo_instance.__file__), tmp_path / "training", {"config": {"epochs": 1}}
    )
    assert external_modules() == before


@pytest.fixture
def probe_module(tmp_path):
    """Actual dataclass/string annotations require registration through close."""
    text = Path(demo_instance.__file__).read_text()
    text = text.replace(
        "import json",
        "import json\nimport sys\nfrom dataclasses import dataclass\n"
        "from typing import get_type_hints",
    )
    text = text.replace(
        "class LinearNeuron:",
        """@dataclass
class _ModuleProbe:
    value: 'float'

def _module_check():
    assert sys.modules[__name__].__dict__ is globals()
    assert get_type_hints(_ModuleProbe)["value"] is float
    assert _ModuleProbe(1.0).value == 1.0

class LinearNeuron:""",
    )
    text = text.replace(
        "def create(config, context):\n    return LinearNeuron(config)",
        "def create(config, context):\n    _module_check()\n    return LinearNeuron(config)",
    )
    text = text.replace(
        "    def close(self, context):\n        pass",
        "    def close(self, context):\n        _module_check()\n"
        "        context.emit_event('registered_at_close', module_name=__name__)",
    )
    module = tmp_path / "probe.py"
    module.write_text(text)
    return module


@pytest.mark.parametrize("failure", [None, "precreate", "create", "train", "close"])
def test_training_success_and_failures_release_only_their_imports(
    dataset, probe_module, tmp_path, failure
):
    foreign = tmp_path / "foreign.py"
    foreign.write_text("VALUE = 7\n")
    retained = neural.load_module(foreign)
    try:
        before = external_modules()
        text = probe_module.read_text()
        if failure == "precreate":
            text = text.replace('"train": True', '"train": False')
        elif failure == "create":
            text = text.replace(
                "return LinearNeuron(config)", "raise RuntimeError('fixture create failed')"
            )
        elif failure == "train":
            text = text.replace(
                "    def train(self, train, validation, config, context):",
                "    def train(self, train, validation, config, context):\n"
                "        _module_check()\n        raise RuntimeError('fixture train failed')",
            )
        elif failure == "close":
            text = text.replace(
                "        context.emit_event('registered_at_close', module_name=__name__)",
                "        context.emit_event('registered_at_close', module_name=__name__)\n"
                "        raise RuntimeError('fixture close failed')",
            )
        probe_module.write_text(text)
        if failure is None:
            neural.train(dataset, probe_module, tmp_path / "training", {"config": {"epochs": 1}})
        else:
            with pytest.raises((ValueError, RuntimeError)):
                neural.train(
                    dataset, probe_module, tmp_path / "training", {"config": {"epochs": 1}}
                )
        assert external_modules() == before
        assert sys.modules[retained.__name__] is retained and retained.VALUE == 7
    finally:
        neural.release_module(retained)


def test_release_guard_does_not_remove_a_replacement_or_unrelated_module(tmp_path):
    path = tmp_path / "bare.py"
    path.write_text("VALUE = 1\n")
    loaded = neural.load_module(path)
    name = loaded.__name__
    replacement = ModuleType(name)
    sys.modules[name] = replacement
    try:
        assert neural.release_module(loaded) is False
        assert sys.modules[name] is replacement
        assert neural.release_module(sys) is False and sys.modules["sys"] is sys
    finally:
        if sys.modules.get(name) is replacement:
            del sys.modules[name]


def test_module_execution_error_releases_its_exact_registration(tmp_path):
    before = external_modules()
    path = tmp_path / "broken.py"
    path.write_text("raise RuntimeError('fixture exec failed')\n")
    with pytest.raises(RuntimeError, match="exec failed"):
        with neural.external_module_scope():
            neural.load_module(path)
    assert external_modules() == before


def test_nested_and_concurrent_scopes_do_not_release_another_owner(tmp_path):
    path = tmp_path / "bare.py"
    path.write_text("VALUE = 1\n")
    before = external_modules()
    with neural.external_module_scope():
        outer = neural.load_module(path)
        with neural.external_module_scope():
            inner = neural.load_module(path)
        assert inner.__name__ not in sys.modules and sys.modules[outer.__name__] is outer
    assert external_modules() == before
    ready = [threading.Event(), threading.Event()]
    leave = [threading.Event(), threading.Event()]
    loaded, errors = {}, []

    def own(index):
        try:
            with neural.external_module_scope():
                module = neural.load_module(path)
                loaded[index] = module
                ready[index].set()
                if not leave[index].wait(5):
                    raise AssertionError("fixture owner wait expired")
                assert sys.modules[module.__name__] is module
        except BaseException as error:
            errors.append(error)
            ready[index].set()

    threads = [threading.Thread(target=own, args=(i,)) for i in range(2)]
    try:
        for thread in threads:
            thread.start()
        assert all(event.wait(5) for event in ready)
        assert not errors
        leave[0].set()
        threads[0].join(5)
        assert not threads[0].is_alive()
        assert loaded[0].__name__ not in sys.modules
        assert sys.modules[loaded[1].__name__] is loaded[1]
    finally:
        for event in leave:
            event.set()
        for thread in threads:
            thread.join(5)
    assert not errors and external_modules() == before


@pytest.fixture
def probe_training(dataset, probe_module, tmp_path):
    root = tmp_path / "training"
    neural.train(dataset, probe_module, root, {"config": {"epochs": 1}})
    return root


def test_prediction_retains_dataclass_registration_until_explicit_close(probe_training, tmp_path):
    before = external_modules()
    with neural.external_module_scope():
        session = PredictionSession(probe_training, tmp_path / "prediction", trusted_module=True)
        module = session.module
        assert sys.modules[module.__name__] is module
    assert sys.modules[module.__name__] is module
    assert session.predict([0.1])["prediction"]["values"]
    session.close()
    session.close()
    assert external_modules() == before


@pytest.mark.parametrize("failure", ["descriptor", "create", "restore", "predict", "close"])
def test_prediction_constructor_and_cleanup_failures_release_registration(
    probe_training, tmp_path, failure
):
    before = external_modules()
    path = probe_training / "instance_module.py"
    run = read_json(probe_training / "run.json")
    text = path.read_text()
    if failure == "descriptor":
        run["module"]["module_version"] = "wrong"
    elif failure == "create":
        text = text.replace(
            "return LinearNeuron(config)", "raise RuntimeError('fixture create failed')"
        )
    elif failure == "restore":
        text = text.replace(
            "    def restore(self, checkpoint, context):",
            "    def restore(self, checkpoint, context):\n        _module_check()\n"
            "        raise RuntimeError('fixture restore failed')",
        )
    elif failure == "predict":
        text = text.replace(
            "    def predict(self, batch, state=None):",
            "    def predict(self, batch, state=None):\n        _module_check()\n"
            "        raise RuntimeError('fixture predict failed')",
        )
    else:
        text = text.replace(
            "        context.emit_event('registered_at_close', module_name=__name__)",
            "        context.emit_event('registered_at_close', module_name=__name__)\n"
            "        raise RuntimeError('fixture close failed')",
        )
    path.write_text(text)
    run["module_source_sha256"] = content_hash(path)
    write_json(probe_training / "run.json", run)
    with pytest.raises((ValueError, RuntimeError)):
        predict_run(
            probe_training, tmp_path / "prediction", {"batch": [0.1], "trusted_module": True}
        )
    assert external_modules() == before
    assert not (tmp_path / "prediction/prediction.json").exists()


@pytest.mark.skipif(os.name == "nt", reason="POSIX FIFO regression")
@pytest.mark.parametrize("owner", ["train", "prediction"])
def test_closed_frozen_source_can_become_fifo_without_a_cached_module(
    probe_training, tmp_path, owner
):
    before = external_modules()
    frozen = probe_training / "instance_module.py"
    if owner == "prediction":
        session = PredictionSession(probe_training, tmp_path / "prediction", trusted_module=True)
        session.predict([0.1])
        session.close()
        frozen = tmp_path / "prediction/instance_module.py"
    assert external_modules() == before
    frozen.unlink()
    os.mkfifo(frozen)
    # Leave the FIFO intact; no reader/partner/cleanup workaround can hide a leak.
    assert all(
        getattr(module, "__file__", None) != str(frozen) for module in external_modules().values()
    )


@pytest.mark.parametrize("kind", ["nn_inspect", "simulate"])
@pytest.mark.parametrize("failure", [False, True])
def test_worker_scope_releases_inspection_and_custom_strategy_modules(
    dataset, probe_module, tmp_path, kind, failure
):
    before = external_modules()
    params = {"module": str(probe_module)}
    if kind == "nn_inspect" and failure:
        probe_module.write_text(
            probe_module.read_text().replace('"api_version": 1,', '"api_version": 999,')
        )
    if kind == "simulate":
        with probe_module.open("a") as stream:
            stream.write("""
def create_strategy(config):
    _module_check()
    def strategy(view):
        _module_check()
        return []
    return strategy
""")
        params = {"dataset": str(dataset), "strategy_module": str(probe_module)}
        if failure:
            probe_module.write_text(
                probe_module.read_text().replace(
                    "        return []", "        raise RuntimeError('fixture strategy failed')"
                )
            )
    root = tmp_path / "research"
    (root / "jobs").mkdir(parents=True)
    (root / "artifacts").mkdir()
    identity = uuid4().hex
    write_json(
        root / "jobs" / f"{identity}.json",
        {"id": identity, "state": "running", "kind": kind, "parameters": params},
    )
    research_service._run_job(str(root), identity, kind, params, None)
    assert read_json(root / "jobs" / f"{identity}.json")["state"] == (
        "failed" if failure else "complete"
    )
    assert external_modules() == before
