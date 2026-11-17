"""Database actions require the local owner and never enter the trading journal."""

import json

import pytest

from omega.domain.errors import StorageError, ValidationError
from omega.runtime.service import CoreService


class MarketData:
    def __init__(self):
        self.calls = []
        self.closed = False

    def snapshot(self, market, instrument):
        self.calls.append((market, instrument))
        return {"source": "unavailable", "instruments": []}

    def submit(self, identity, kind, parameters):
        self.calls.append((identity, kind, parameters))
        return {"id": identity, "kind": kind, "state": "queued"}

    def status(self, identity):
        return {"id": identity, "state": "unknown"}

    def close(self):
        self.closed = True


@pytest.fixture
def core(tmp_path):
    service = CoreService(tmp_path, market_data=MarketData())
    yield service
    service.close()


def test_owner_snapshot_and_action_lookup_use_the_dedicated_provider(core):
    core.request("GET", "/api/database?market=spot&instrument=binance%3Aspot%3ABTCUSDT", core.token)
    assert core.market_data.calls == [("spot", "binance:spot:BTCUSDT")]
    assert core.request("GET", "/api/database/actions/example", core.token) == {
        "id": "example",
        "state": "unknown",
    }
    assert core.state()["core"]["capabilities"]["database"]


def test_peer_secret_never_enters_control_journal_settings_or_state_response(core):
    password = "test-peer-only"
    result = core.request(
        "POST",
        "/api/database/actions",
        core.token,
        {"id": "peer-1", "kind": "peer.import", "parameters": {"password": password}},
    )
    assert result["state"] == "queued"
    assert password not in json.dumps(core.state())
    for path in core.root.rglob("*.json"):
        assert password not in path.read_text(encoding="utf-8")


def test_device_read_permission_cannot_reach_database_or_peer_credentials(core):
    pairing = core.control.create_pairing("phone", ["read", "pause"])
    token = core.control.pair(pairing["code"])["token"]
    for method, path, body in (
        ("GET", "/api/database", None),
        ("GET", "/api/database/actions/peer-1", None),
        ("POST", "/api/database/actions", {"id": "x", "kind": "peer.import", "parameters": {}}),
    ):
        with pytest.raises(PermissionError):
            core.request(method, path, token, body)
    assert not core.market_data.calls


@pytest.mark.parametrize("body", [None, {}, {"id": "x", "kind": "peer.import"}])
def test_invalid_database_envelope_cannot_dispatch(core, body):
    with pytest.raises(ValidationError):
        core.request("POST", "/api/database/actions", core.token, body)
    assert not core.market_data.calls


def test_database_shutdown_failure_still_closes_research_and_releases_core_lock(tmp_path):
    database, research = MarketData(), MarketData()
    core = CoreService(tmp_path, market_data=database, research=research)
    recording_available, attempts = [False], []

    def fail():
        database.closed = True
        attempts.append(recording_available[0])
        if not recording_available[0]:
            raise StorageError("cannot persist shutdown summary")

    database.close = fail
    try:
        with pytest.raises(StorageError, match="cannot persist shutdown summary"):
            core.close()
        assert database.closed and research.closed
        assert attempts == [False] and core.instance.released is False
        assert core.market_data is database and core.research is research
        with pytest.raises(RuntimeError, match="already owns this root"):
            CoreService(tmp_path)
        # The provider's closed flag did not confirm its failed durable summary.
        # Only a successful retry on that same provider releases the root lease.
        recording_available[0] = True
        core.close()
        assert attempts == [False, True] and database.closed and research.closed
        assert core.instance.released is True
        successor = CoreService(tmp_path)
        successor.close()
    finally:
        recording_available[0] = True
        core.close()


def test_validation_rejection_is_known_and_does_not_echo_peer_credentials(core):
    password = "test-invalid-peer"

    def reject(*_args):
        raise ValidationError("invalid peer password: " + password)

    core.market_data.submit = reject
    result = core.request(
        "POST",
        "/api/database/actions",
        core.token,
        {"id": "peer-invalid", "kind": "peer.import", "parameters": {"password": password}},
    )
    assert result == {
        "id": "peer-invalid",
        "kind": "peer.import",
        "state": "rejected",
        "error": "INVALID_DATABASE_ACTION",
    }
    assert password not in json.dumps(result)
