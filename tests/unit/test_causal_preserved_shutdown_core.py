"""Core validates the preservation receipt; injected NN has no real NN resources."""

from copy import deepcopy

import pytest

from omega.domain.errors import StorageError
from omega.runtime.service import CoreService


class PreservedBackend:
    def __init__(self, receipt):
        self.receipt, self.calls = receipt, []

    def snapshot(self):
        return {"state": "disabled", "revision": 0}

    def can_close_preserving_cold_namespace(self):
        return True

    def close_preserving_cold_namespace(self):
        self.calls.append("preserve")
        return deepcopy(self.receipt)

    def close(self):
        self.calls.append("ordinary")
        raise StorageError("ordinary close must not bless the old namespace")


def preserved_receipt():
    return {
        "closed": True,
        "durable": False,
        "worker_alive": False,
        "actor_alive": False,
        "namespace_preserved": True,
        "namespace_clean": False,
        "current_physical_cleanup_confirmed": True,
        "current_required_recording_state": "none_attempted",
        "old_resource_close_claimed": False,
    }


def test_core_can_release_only_its_current_resources_without_blessing_old_nn_data(tmp_path):
    backend = PreservedBackend(preserved_receipt())
    core = CoreService(tmp_path, causal_inference=backend)
    old = tmp_path / "state/old-nn-record.json"
    old.write_text('{"phase":"proof_pending"}', encoding="utf-8")
    before = old.read_bytes()
    core.close()
    assert backend.calls == ["preserve"]
    assert core.instance.released and old.read_bytes() == before
    assert backend.receipt["durable"] is False and backend.receipt["namespace_clean"] is False


@pytest.mark.parametrize(
    "key,value",
    [
        ("durable", True),
        ("namespace_clean", True),
        ("current_physical_cleanup_confirmed", False),
        ("current_required_recording_state", "unknown"),
        ("old_resource_close_claimed", True),
        ("worker_alive", True),
    ],
)
def test_invalid_preservation_receipt_keeps_the_core_root_held(tmp_path, key, value):
    receipt = preserved_receipt()
    receipt[key] = value
    core = CoreService(tmp_path, causal_inference=PreservedBackend(receipt))
    try:
        with pytest.raises(StorageError, match="shutdown is not confirmed"):
            core.close()
        assert not core.instance.released
    finally:
        # Fixture-owned lock only: this injected backend has NO NN/kernel/store
        # resources. This disposal is not a product recovery or cleanup ACK.
        core.instance.close()
