"""Original-process snapshots are bounded evidence, never adopted by PID."""

import json
from dataclasses import replace

import pytest
from test_protection_observation import BTC, observation

from omega.domain.errors import StorageError
from omega.domain.models import Instrument, Market
from omega.runtime.market_data_worker import OwnedCollectors


class OriginalProcess:
    def __init__(self):
        self.code = None

    def poll(self):
        return self.code


def owned(tmp_path):
    owner = OwnedCollectors(tmp_path)
    folder = tmp_path / "logs/collection"
    folder.mkdir(parents=True)
    log = folder / "owned.log"
    process = OriginalProcess()
    owner.children["usdm"] = (process, (BTC.key,), log)
    owner.protection_generations["usdm"] = (process, "a" * 32)
    owner.protection_seen["usdm"] = {}
    return owner, process, log.with_suffix(".protection.json")


def write(path, obs, **changes):
    sequence = (
        json.loads(path.read_text(encoding="utf-8"))["envelope_sequence"] + 1
        if path.exists()
        else 1
    )
    value = {
        "version": 1,
        "generation": "a" * 32,
        "market": "usdm",
        "instruments": [BTC.key],
        "connected": True,
        "connection": obs.connection,
        "envelope_sequence": sequence,
        "observations": {BTC.key: obs.document()},
    }
    value.update(changes)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_original_child_subscription_connection_receipt_and_detach(tmp_path):
    owner, process, path = owned(tmp_path)
    original = observation()
    write(path, original)
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == (original,)
    assert owner.protection_observations((BTC.key,), as_of_ms=20000) == ()
    write(path, original, connected=False, observations={})
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == ()
    write(path, replace(original, connection=2, sequence=1, received_at_ms=10002))
    assert owner.protection_observations((BTC.key,), as_of_ms=10003)[0].connection == 2
    process.code = 0
    assert owner.protection_observations((BTC.key,), as_of_ms=10003) == ()
    # A descriptor/process substitute is not the Popen object originally created here.
    owner.children["usdm"] = (OriginalProcess(), (BTC.key,), path.with_suffix(".log"))
    assert owner.protection_observations((BTC.key,), as_of_ms=10003) == ()


def test_observed_identity_payload_conflict_remains_held_even_if_next_snapshot_looks_valid(
    tmp_path,
):
    owner, _, path = owned(tmp_path)
    original = observation()
    write(path, original)
    owner.protection_observations((BTC.key,), as_of_ms=10002)
    write(path, replace(original, low=original.low - 1))
    with pytest.raises(StorageError):
        owner.protection_observations((BTC.key,), as_of_ms=10002)
    write(path, replace(original, sequence=2))
    with pytest.raises(StorageError, match="held"):
        owner.protection_observations((BTC.key,), as_of_ms=10002)


@pytest.mark.parametrize(
    "changes",
    [
        {"generation": "b" * 32},
        {"instruments": []},
        {"version": True},
        {"market": "spot"},
        {"connected": False},
    ],
)
def test_foreign_or_malformed_transport_is_held(tmp_path, changes):
    owner, _, path = owned(tmp_path)
    write(path, observation(), **changes)
    with pytest.raises(StorageError):
        owner.protection_observations((BTC.key,), as_of_ms=10002)


def test_other_symbol_from_retired_connection_cannot_hide_under_new_envelope(tmp_path):
    owner, process, path = owned(tmp_path)
    eth = Instrument("binance", Market.USDM, "ETHUSDT")
    _, _, log = owner.children["usdm"]
    owner.children["usdm"] = process, (BTC.key, eth.key), log
    old = replace(observation(), instrument=eth)
    write(path, old, instruments=[BTC.key, eth.key], observations={eth.key: old.document()})
    assert owner.protection_observations((BTC.key, eth.key), as_of_ms=10002) == (old,)
    current = replace(observation(), connection=2, received_at_ms=10002)
    write(
        path,
        current,
        instruments=[BTC.key, eth.key],
        observations={BTC.key: current.document(), eth.key: old.document()},
    )
    with pytest.raises(StorageError):
        owner.protection_observations((BTC.key, eth.key), as_of_ms=10003)


def test_failed_before_first_connect_empty_zero_generation_does_not_resurrect_data(tmp_path):
    owner, _, path = owned(tmp_path)
    sample = observation()
    write(path, sample, connection=0, connected=False, observations={})
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == ()
    write(path, sample)
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == (sample,)


def test_disconnected_socket_cannot_reactivate_same_connection_with_higher_publication(tmp_path):
    owner, _, path = owned(tmp_path)
    original = observation()
    write(path, original, envelope_sequence=19)
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == (original,)
    write(path, original, connected=False, observations={}, envelope_sequence=20)
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == ()
    later = replace(original, sequence=2, event_time_ms=10001, received_at_ms=10002)
    write(path, later, envelope_sequence=21)
    with pytest.raises(StorageError):
        owner.protection_observations((BTC.key,), as_of_ms=10003)
