"""Main-run peer cases; collect beside the unchanged unit helpers/import path.

R1 owned source/tests remain frozen. Helpers supply the original fake Popen,
generation and envelope; replay uses exact saved bytes, never a refreshed receipt.
"""

from dataclasses import replace

from test_collector_protection_feed import owned, write
from test_protection_observation import BTC, observation

from omega.domain.errors import StorageError
from omega.domain.models import Instrument, Market


def rejected(owner, keys, *, as_of_ms):
    try:
        rows = owner.protection_observations(keys, as_of_ms=as_of_ms)
    except StorageError:
        return  # Sticky held or explicit stale-unavailable are both safe rejection.
    assert rows == (), "retired socket evidence must never become protection input again"


def test_observed_disconnect_rejects_exact_prior_connected_snapshot_within_ttl(tmp_path):
    owner, _process, path = owned(tmp_path)
    sample = observation()
    write(path, sample)
    original_connected = path.read_bytes()
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == (sample,)
    write(path, sample, connected=False, observations={})
    assert owner.protection_observations((BTC.key,), as_of_ms=10002) == ()
    path.write_bytes(original_connected)  # Delayed replace, same nonce/socket/sequence/payload.
    rejected(owner, (BTC.key,), as_of_ms=10003)


def test_new_market_socket_rejects_prior_socket_snapshot_for_other_symbol_within_ttl(tmp_path):
    owner, process, path = owned(tmp_path)
    eth = Instrument("binance", Market.USDM, "ETHUSDT")
    _original, _instruments, log = owner.children["usdm"]
    owner.children["usdm"] = (process, (BTC.key, eth.key), log)
    subscriptions = [BTC.key, eth.key]
    old_eth = replace(observation(), instrument=eth, connection=1)
    write(path, old_eth, instruments=subscriptions, observations={eth.key: old_eth.document()})
    old_socket_snapshot = path.read_bytes()
    assert owner.protection_observations(tuple(subscriptions), as_of_ms=10002) == (old_eth,)
    current_btc = replace(observation(), connection=2, received_at_ms=10002)
    write(
        path,
        current_btc,
        instruments=subscriptions,
        observations={BTC.key: current_btc.document()},
    )
    assert owner.protection_observations(tuple(subscriptions), as_of_ms=10003) == (current_btc,)
    path.write_bytes(old_socket_snapshot)  # No new BTC row to expose per-symbol rollback.
    rejected(owner, tuple(subscriptions), as_of_ms=10004)
