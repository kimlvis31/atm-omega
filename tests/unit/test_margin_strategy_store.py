"""Real file CAS and recovery boundary, independent of a running trade host."""

from dataclasses import replace

import pytest

from omega.application.margin_strategy import MarginStrategyState
from omega.domain.errors import ProtocolError, StorageError
from omega.domain.models import Instrument, Market
from omega.runtime import margin_strategy_store
from omega.runtime.margin_strategy_store import FileMarginStrategyStore


def initial():
    return MarginStrategyState(
        "account",
        "strategy",
        Instrument("binance", Market.USDM, "BTCUSDT"),
        "strategy",
        "target_quantity",
        "1",
        "a" * 64,
        0,
        "{}",
    )


def test_actual_file_cas_survives_ack_loss_without_new_revision(tmp_path, monkeypatch):
    store = FileMarginStrategyStore(tmp_path, "strategy")
    first = initial()
    try:
        store.save(first, expected_revision=None)
        candidate = replace(first, revision=1, module_checkpoint='{"x":1}')
        real = margin_strategy_store.atomic_json

        def lost_ack(path, data):
            real(path, data)
            raise OSError("saved but caller lost ACK")

        monkeypatch.setattr(margin_strategy_store, "atomic_json", lost_ack)
        with pytest.raises(OSError):
            store.save(candidate, expected_revision=0)
        assert store.load("strategy") == candidate
        store.save(candidate, expected_revision=0)
        with pytest.raises(ProtocolError):
            store.save(replace(candidate, revision=2), expected_revision=0)
        assert store.load("strategy") == candidate
    finally:
        store.close()
    reopened = FileMarginStrategyStore(tmp_path, "strategy")
    try:
        assert reopened.load("strategy") == candidate
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "payload", [b"{", b'{"data":{},"sha256":"bad"}', b'{"data":1,"data":2,"sha256":"bad"}']
)
def test_corrupt_file_is_never_missing_or_replaced(tmp_path, payload):
    store = FileMarginStrategyStore(tmp_path, "strategy")
    try:
        store.path.write_bytes(payload)
        with pytest.raises(StorageError):
            store.load("strategy")
        assert store.path.read_bytes() == payload
    finally:
        store.close()


def test_exclusive_owner_and_missing_wallet_are_separate(tmp_path):
    store = FileMarginStrategyStore(tmp_path, "strategy")
    try:
        assert store.load("strategy") is None
        with pytest.raises(Exception, match="already"):
            FileMarginStrategyStore(tmp_path, "strategy")
    finally:
        store.close()


def test_regular_file_parent_and_directory_snapshot_rejected(tmp_path):
    store = FileMarginStrategyStore(tmp_path, "strategy")
    try:
        store.path.mkdir()
        with pytest.raises(StorageError):
            store.load("strategy")
    finally:
        store.close()
