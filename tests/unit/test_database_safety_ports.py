"""Test-only port selection never connects or relaxes database identity guards."""

import importlib.util
from pathlib import Path

import pytest


@pytest.fixture
def ports(monkeypatch):
    monkeypatch.delenv("OMEGA_TEST_DB_PORT", raising=False)
    monkeypatch.delenv("OMEGA_TEST_PEER_DB_PORT", raising=False)
    path = Path(__file__).resolve().parents[1] / "integration" / "database_safety.py"
    spec = importlib.util.spec_from_file_location("omega_test_database_safety_ports", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.isolated_test_port


def test_legacy_defaults_and_explicit_independent_ports(ports, monkeypatch):
    assert ports() == 55433 and ports(peer=True) == 55434
    monkeypatch.setenv("OMEGA_TEST_DB_PORT", "54329")
    monkeypatch.setenv("OMEGA_TEST_PEER_DB_PORT", "54330")
    assert ports() == 54329 and ports(peer=True) == 54330


@pytest.mark.parametrize("value", ["1024", "65535"])
def test_native_port_boundaries_are_allowed(ports, monkeypatch, value):
    monkeypatch.setenv("OMEGA_TEST_DB_PORT", value)
    assert ports() == int(value)


@pytest.mark.parametrize("name", ["OMEGA_TEST_DB_PORT", "OMEGA_TEST_PEER_DB_PORT"])
@pytest.mark.parametrize(
    "value",
    [
        "",
        "0",
        "1023",
        "65536",
        "99999",
        "100000",
        "-54329",
        "+54329",
        " 54329",
        "54329 ",
        "54329\n",
        "054329",
        "54329.0",
        "5e4",
        "True",
        "５４３２９",
    ],
)
def test_malformed_or_out_of_range_explicit_values_never_fall_back(ports, monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError, match=name):
        ports()
    with pytest.raises(ValueError, match=name):
        ports(peer=True)


@pytest.mark.parametrize("main,peer", [("54329", "54329"), ("55434", None), (None, "55433")])
def test_peer_main_collision_is_rejected_for_both_callers(ports, monkeypatch, main, peer):
    if main is not None:
        monkeypatch.setenv("OMEGA_TEST_DB_PORT", main)
    if peer is not None:
        monkeypatch.setenv("OMEGA_TEST_PEER_DB_PORT", peer)
    with pytest.raises(ValueError, match="must differ"):
        ports()
    with pytest.raises(ValueError, match="must differ"):
        ports(peer=True)


@pytest.mark.parametrize("peer", [0, 1, None, "true"])
def test_peer_selection_requires_a_native_boolean(ports, peer):
    with pytest.raises(ValueError, match="explicit boolean"):
        ports(peer=peer)
